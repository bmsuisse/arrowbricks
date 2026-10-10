//! Shared test-only helpers for the integration tests in `tests/` --
//! `tests/common/mod.rs` (not `tests/common.rs`) is the standard way to
//! share code between separate integration-test binaries without Cargo
//! treating this file as its own test binary (a bare `tests/common.rs`
//! would be auto-discovered and compiled/run as one, showing "0 tests" but
//! still paying the compile cost). This directory otherwise has no shared-
//! infrastructure convention (see AGENTS.md's own note on the equivalent
//! Python side) -- added because `wait_for_calls` below was found
//! duplicated verbatim in both `wiremock_pipeline.rs` and
//! `wiremock_thrift.rs`'s own cancellation tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Polls briefly for `calls` to reach `want` -- the cancel RPC/REST call
/// these tests assert on is fire-and-forget (spawned by `pipeline::cancel_hook`,
/// never awaited), so asserting on it immediately after a timeout/drop
/// would race it. Same "poll briefly rather than assume synchronous-with-
/// the-caller" pattern this crate's own
/// `thrift_pool_exhaustion_falls_back_to_a_throwaway_session_that_still_succeeds`
/// test uses for its own background session-close call.
pub async fn wait_for_calls(calls: &AtomicUsize, want: usize) {
    for _ in 0..100 {
        if calls.load(Ordering::SeqCst) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A `TokenProvider` that checks every token request runs in the call
/// context the test set up -- what `lib.rs`'s `PyTokenProvider` relies on to
/// find the caller's asyncio event loop. Counts all requests and the ones
/// that came without (or with a different) context.
#[allow(dead_code)] // not every test binary that includes `common` uses it
pub struct ContextCheckingToken {
    pub expected: u64,
    pub calls: AtomicUsize,
    pub without_context: AtomicUsize,
}

#[allow(dead_code)]
impl ContextCheckingToken {
    pub fn new(expected: u64) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            expected,
            calls: AtomicUsize::new(0),
            without_context: AtomicUsize::new(0),
        })
    }

    pub fn context(&self) -> arrowbricks_core::client::CallContext {
        std::sync::Arc::new(self.expected)
    }

    /// Panics unless there was at least `min_calls` token request and every
    /// one of them saw the expected context.
    pub fn assert_all_in_context(&self, min_calls: usize) {
        let calls = self.calls.load(Ordering::SeqCst);
        assert!(
            calls >= min_calls,
            "expected at least {min_calls} token requests, saw {calls}"
        );
        assert_eq!(
            self.without_context.load(Ordering::SeqCst),
            0,
            "{calls} token requests, some from a task that lost the call context"
        );
    }
}

impl arrowbricks_core::client::TokenProvider for ContextCheckingToken {
    fn get_token(&self) -> arrowbricks_core::client::TokenFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let seen = arrowbricks_core::client::current_call_context().and_then(|c| c.downcast_ref::<u64>().copied());
        if seen != Some(self.expected) {
            self.without_context.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async { Ok("ctx-token".to_string()) })
    }
}

/// Drives `execute_ndjson_stream` the way `lib.rs`'s `stream_ndjson_lines`
/// iterator does -- the submit inside a `HeartbeatWait` (a task spawned on
/// another runtime), then every chunk -- all inside `ctx`, the call context
/// the Python entry point sets. Returns every NDJSON line.
#[allow(dead_code)]
pub async fn stream_ndjson_in_context(
    client: std::sync::Arc<arrowbricks_core::client::DbClient>,
    ctx: arrowbricks_core::client::CallContext,
) -> Vec<String> {
    use arrowbricks_core::heartbeat::{HeartbeatWait, Tick};
    arrowbricks_core::client::with_call_context(Some(ctx), async move {
        let mut wait = HeartbeatWait::new(
            async move {
                arrowbricks_core::pipeline::execute_ndjson_stream(client, "SELECT * FROM t", None, None, None, false)
                    .await
            },
            Some(30.0),
        );
        let mut stream = loop {
            match wait.tick().await.unwrap() {
                Some(Tick::Ready(stream)) => break stream,
                Some(Tick::Heartbeat) => {}
                None => unreachable!("HeartbeatWait yields Ready exactly once"),
            }
        };
        let mut lines = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.unwrap() {
            lines.extend(chunk);
        }
        lines
    })
    .await
}
