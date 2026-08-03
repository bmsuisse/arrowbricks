//! Generic heartbeat-wait primitive, port of `_streaming.py`'s
//! `await_with_heartbeat`: wraps a single slow operation, periodically
//! reports "still waiting" instead of blocking silently, then reports the
//! real result once. Reused for both `execute_streamed` (waiting on
//! submit/poll) and `fetchall_arrow_streamed` (waiting on the chunk
//! download) -- same as the Python original, where both are thin wrappers
//! around one shared function.

use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use crate::client::{ApiError, join_error};

/// Matches `_streaming.py`'s `_HEARTBEAT_INTERVAL_S` -- see its own comment
/// for why 15s (well under typical PaaS idle-connection ceilings for a
/// caller forwarding these as SSE keep-alives).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub enum Tick<T> {
    Heartbeat,
    Ready(T),
}

/// Runs the wrapped future on the shared tokio runtime (not a bare
/// `tokio::spawn`, which requires already being inside a task the runtime is
/// driving -- this needs to work from a plain synchronous PyO3 method too,
/// e.g. `execute_streamed` itself isn't async, only its returned iterator's
/// `__anext__` is) so it keeps making progress between separate `tick()`
/// calls, exactly like Python's `asyncio.ensure_future(aw)` schedules the
/// wrapped awaitable to run independently of when it's next polled.
pub struct HeartbeatWait<T> {
    handle: Option<JoinHandle<Result<T, ApiError>>>,
    deadline: Option<Instant>,
    total_timeout_s: Option<f64>,
    heartbeat_interval: Duration,
}

impl<T: Send + 'static> HeartbeatWait<T> {
    pub fn new<F>(fut: F, total_timeout_s: Option<f64>) -> Self
    where
        F: std::future::Future<Output = Result<T, ApiError>> + Send + 'static,
    {
        Self::with_interval(fut, total_timeout_s, HEARTBEAT_INTERVAL)
    }

    /// Same as `new`, but with an injectable heartbeat interval -- tests use
    /// a short real duration instead of the real 15s, since the wrapped
    /// future always runs on `pyo3_async_runtimes`'s own separate global
    /// runtime (needed so this works from a synchronous PyO3 method with no
    /// ambient tokio context), not whatever runtime is polling `tick()` --
    /// `tokio::time::pause()` in a `#[tokio::test]` wouldn't affect that
    /// other runtime's clock, so shortening the real interval is what
    /// actually keeps tests fast, not virtual time.
    pub fn with_interval<F>(fut: F, total_timeout_s: Option<f64>, heartbeat_interval: Duration) -> Self
    where
        F: std::future::Future<Output = Result<T, ApiError>> + Send + 'static,
    {
        Self {
            handle: Some(pyo3_async_runtimes::tokio::get_runtime().spawn(fut)),
            deadline: total_timeout_s.map(|s| Instant::now() + Duration::from_secs_f64(s)),
            total_timeout_s,
            heartbeat_interval,
        }
    }

    /// One step: `Ok(Some(Tick::Heartbeat))` if still waiting,
    /// `Ok(Some(Tick::Ready(value)))` exactly once when the wrapped future
    /// completes, `Ok(None)` if already exhausted (caller should raise
    /// StopAsyncIteration), `Err` on the wrapped future's own error or a
    /// `total_timeout_s` overrun.
    pub async fn tick(&mut self) -> Result<Option<Tick<T>>, ApiError> {
        let Some(handle) = self.handle.as_mut() else {
            return Ok(None);
        };

        let wait_for = match self.deadline {
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    handle.abort();
                    self.handle = None;
                    let secs = self.total_timeout_s.unwrap_or(0.0);
                    return Err(ApiError {
                        message: format!("Query exceeded {secs}s timeout"),
                        transient: false,
                    });
                }
                self.heartbeat_interval.min(deadline - now)
            }
            None => self.heartbeat_interval,
        };

        tokio::select! {
            res = handle => {
                self.handle = None;
                match res {
                    Ok(inner) => inner.map(|v| Some(Tick::Ready(v))),
                    Err(join_err) => Err(join_error(join_err)),
                }
            }
            _ = tokio::time::sleep(wait_for) => Ok(Some(Tick::Heartbeat)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_INTERVAL: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn heartbeats_then_ready() {
        let mut wait = HeartbeatWait::with_interval(
            async {
                tokio::time::sleep(Duration::from_millis(70)).await;
                Ok(42)
            },
            None,
            TEST_INTERVAL,
        );

        // ~3 heartbeat intervals (60ms) elapse before the 70ms task
        // finishes -- must report Heartbeat each time, not skip ahead.
        for _ in 0..3 {
            match wait.tick().await.unwrap() {
                Some(Tick::Heartbeat) => {}
                Some(Tick::Ready(_)) => panic!("finished earlier than expected"),
                None => panic!("exhausted earlier than expected"),
            }
        }
        match wait.tick().await.unwrap() {
            Some(Tick::Ready(v)) => assert_eq!(v, 42),
            _ => panic!("expected Ready(42) once the wrapped future completes"),
        }
        // Exhausted -- must not tick again.
        assert!(wait.tick().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn total_timeout_raises_before_completion() {
        // deadline (50ms) is checked at the *start* of each tick, not
        // continuously -- so this must actually loop across several ticks
        // (like real usage does) rather than expect the very first call to
        // already be past the deadline.
        let mut wait = HeartbeatWait::with_interval(
            async {
                tokio::time::sleep(Duration::from_secs(120)).await;
                Ok(())
            },
            Some(0.05),
            TEST_INTERVAL,
        );

        let mut last_err = None;
        for _ in 0..10 {
            match wait.tick().await {
                Ok(Some(Tick::Heartbeat)) => continue,
                Ok(_) => panic!("must not become Ready/None before the timeout fires"),
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }
        let err = last_err.expect("timeout must fire within 10 ticks of a 20ms interval against a 50ms deadline");
        assert!(
            err.message.contains("0.05"),
            "error should mention the configured timeout: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn wrapped_future_error_propagates() {
        let mut wait: HeartbeatWait<()> = HeartbeatWait::with_interval(
            async {
                Err(ApiError {
                    message: "boom".into(),
                    transient: false,
                })
            },
            None,
            TEST_INTERVAL,
        );
        let err = wait.tick().await.unwrap_err();
        assert_eq!(err.message, "boom");
    }
}
