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
