//! Generic heartbeat-wait primitive, port of `_streaming.py`'s
//! `await_with_heartbeat`: wraps a single slow operation, periodically
//! reports "still waiting" instead of blocking silently, then reports the
//! real result once. Reused for both `execute_streamed` (waiting on
//! submit/poll) and `fetchall_arrow_streamed` (waiting on the chunk
//! download) -- same as the Python original, where both are thin wrappers
//! around one shared function.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use crate::client::{ApiError, join_error};

/// Matches `_streaming.py`'s `_HEARTBEAT_INTERVAL_S` -- well under typical
/// PaaS idle-connection ceilings for a caller forwarding these as SSE
/// keep-alives.
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
    /// runtime (see this struct's own doc comment), not whatever runtime is
    /// polling `tick()` -- `tokio::time::pause()` in a `#[tokio::test]`
    /// wouldn't affect that other runtime's clock, so shortening the real
    /// interval is what actually keeps tests fast, not virtual time.
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
                    // `abort()` only *requests* cancellation -- the task
                    // (running on pyo3-async-runtimes' own persistent
                    // background runtime, independent of whatever's calling
                    // `tick()`) keeps running until its next await point,
                    // where tokio actually drops it. Awaiting the handle
                    // here blocks until that drop has genuinely happened
                    // before this error ever reaches the caller -- found by
                    // testing a real timeout against a live warehouse from a
                    // short-lived script: without this, the caller can see
                    // QueryTimeout, decide the program is done, and exit
                    // while the orphaned task is still mid-drop; if that
                    // drop needs to touch a Python object (e.g. dropping a
                    // token_provider callback reference) after the
                    // interpreter has started finalizing, it panics with
                    // "The Python interpreter is not initialized". A
                    // long-running server never hits this (the interpreter
                    // stays alive), but nothing should depend on that.
                    let _ = handle.await;
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

impl<T> Drop for HeartbeatWait<T> {
    /// Best-effort: if this is dropped with a task still in flight for any
    /// reason *other* than `tick()`'s own `total_timeout_s` path (which
    /// already awaits the abort before returning -- see its comment) --
    /// Python-side cancellation (`asyncio.wait_for`, `task.cancel()`) being
    /// the real one, found the same way as that fix, by testing against a
    /// live warehouse -- at least *requests* cancellation immediately.
    /// `JoinHandle::drop` alone does not abort the task: tokio explicitly
    /// leaves a dropped-but-unaborted task running fully detached to
    /// completion, so without this, an early drop for any reason would leave
    /// the task running with nothing left to observe or join it, at all,
    /// ever. `Drop::drop` can't `.await`, so this can't guarantee the task
    /// has actually finished by the time this returns -- only `tick()`'s own
    /// timeout path gives that stronger guarantee -- it only shrinks how
    /// long an orphaned task keeps running (and how long it can panic
    /// touching Python state after the interpreter starts finalizing)
    /// afterward.
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Future factory for `HeartbeatStream::tick` -- produces the future that
/// pulls the next item, boxed so it can be spawned onto the shared runtime.
type NextItemFuture<T> = Pin<Box<dyn Future<Output = Result<Option<T>, ApiError>> + Send>>;

/// Port of `_streaming.py`'s `heartbeat_over_stream`: like `HeartbeatWait`,
/// but for a *stream* of items rather than one final result. Ticks
/// repeatedly, sharing ONE `total_timeout_s` deadline across the whole
/// stream (computed once at construction) rather than restarting it on every
/// item -- a pathological sequence of individually-fast-enough items must
/// still trip the overall budget, matching Python's single shared `deadline`
/// variable rather than per-item timeouts.
pub struct HeartbeatStream<T> {
    current: Option<JoinHandle<Result<Option<T>, ApiError>>>,
    deadline: Option<Instant>,
    total_timeout_s: Option<f64>,
    heartbeat_interval: Duration,
}

impl<T: Send + 'static> HeartbeatStream<T> {
    pub fn new(total_timeout_s: Option<f64>) -> Self {
        Self::with_interval(total_timeout_s, HEARTBEAT_INTERVAL)
    }

    pub fn with_interval(total_timeout_s: Option<f64>, heartbeat_interval: Duration) -> Self {
        Self {
            current: None,
            deadline: total_timeout_s.map(|s| Instant::now() + Duration::from_secs_f64(s)),
            total_timeout_s,
            heartbeat_interval,
        }
    }

    /// One step: `Ok(Some(Tick::Heartbeat))` while still waiting on the
    /// current item, `Ok(Some(Tick::Ready(item)))` once it arrives,
    /// `Ok(None)` once the source itself is exhausted (caller should raise
    /// StopAsyncIteration, matching Python's `except StopAsyncIteration:
    /// return`), `Err` on the source's own error or a `total_timeout_s`
    /// overrun. `spawn_next` is only called to start a *new* pull when none
    /// is already in flight -- a tick that only heartbeated resumes the same
    /// in-flight pull on the next call rather than starting a redundant one.
    pub async fn tick<F>(&mut self, spawn_next: F) -> Result<Option<Tick<T>>, ApiError>
    where
        F: FnOnce() -> NextItemFuture<T>,
    {
        if self.current.is_none() {
            self.current = Some(pyo3_async_runtimes::tokio::get_runtime().spawn(spawn_next()));
        }
        let handle = self.current.as_mut().expect("just set above if it was None");

        let wait_for = match self.deadline {
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    handle.abort();
                    // See HeartbeatWait::tick's identical comment on why this
                    // await matters.
                    let _ = handle.await;
                    self.current = None;
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
                self.current = None;
                match res {
                    Ok(inner) => inner.map(|opt| opt.map(Tick::Ready)),
                    Err(join_err) => Err(join_error(join_err)),
                }
            }
            _ = tokio::time::sleep(wait_for) => Ok(Some(Tick::Heartbeat)),
        }
    }
}

impl<T> Drop for HeartbeatStream<T> {
    /// Same as `HeartbeatWait`'s `Drop`, for the chunk-download phase -- see
    /// its own doc comment.
    fn drop(&mut self) {
        if let Some(handle) = self.current.take() {
            handle.abort();
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

    /// Regression test for `tick()`'s abort-then-await -- see its own
    /// comment for the real panic this guards against. Proven here by a task
    /// that sets a shared flag in its own `Drop` impl, asserted `true`
    /// immediately after `tick()` returns, not merely "eventually".
    #[tokio::test]
    async fn total_timeout_waits_for_the_aborted_task_to_actually_drop() {
        struct SetOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = SetOnDrop(dropped.clone());
        let mut wait = HeartbeatWait::with_interval(
            async move {
                let _guard = guard; // moved in, dropped only when this future is dropped
                tokio::time::sleep(Duration::from_secs(120)).await;
                Ok(())
            },
            Some(0.03),
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
        assert!(last_err.is_some(), "timeout must fire within 10 ticks");
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "the aborted task's future must already be dropped by the time tick() returns the timeout error"
        );
    }

    /// Regression test for a *second*, more general real panic found the
    /// same way: `Drop for HeartbeatWait` needs a real, reproduced bug (see
    /// its own doc comment) -- reproduced against a live warehouse:
    /// intermittent (1 run in 3). This only proves cancellation is requested
    /// (`JoinHandle::is_finished()` eventually becomes true), not that it
    /// happens synchronously within `drop()` itself -- `Drop::drop` can't
    /// `.await`, so it can't prove more than that.
    #[tokio::test]
    async fn dropping_heartbeat_wait_directly_requests_cancellation() {
        let mut wait: HeartbeatWait<()> = HeartbeatWait::with_interval(
            async {
                tokio::time::sleep(Duration::from_secs(120)).await;
                Ok(())
            },
            None, // no total_timeout_s at all -- this drop is not going through tick()'s timeout branch
            TEST_INTERVAL,
        );
        // One tick to prove the task is genuinely running first (not
        // already finished for some unrelated reason).
        assert!(matches!(wait.tick().await.unwrap(), Some(Tick::Heartbeat)));
        assert!(
            !wait.handle.as_ref().unwrap().is_finished(),
            "the 120s sleep must not have finished on its own yet"
        );
        let abort_handle = wait.handle.as_ref().unwrap().abort_handle();

        drop(wait);
        // Abort is requested synchronously in Drop, but the task only
        // actually finishes at its next await point -- give the runtime a
        // moment to schedule that, matching the "best-effort, not
        // immediate" contract this Drop impl actually provides (unlike
        // tick()'s own timeout path, which awaits the join directly).
        for _ in 0..20 {
            if abort_handle.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("dropping HeartbeatWait must still abort its spawned task, even outside tick()'s own timeout path");
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

    /// A source of items, each optionally preceded by a delay -- `spawn_next`
    /// closures below pop off the front of this to simulate a slow chunk
    /// arriving mid-stream, not just a slow first item.
    type QueueItem = (Duration, Option<u32>);

    fn spawn_next_from(
        queue: std::sync::Arc<tokio::sync::Mutex<std::collections::VecDeque<QueueItem>>>,
    ) -> NextItemFuture<u32> {
        Box::pin(async move {
            let (delay, value) = queue.lock().await.pop_front().expect("test queue exhausted");
            tokio::time::sleep(delay).await;
            Ok(value)
        })
    }

    #[tokio::test]
    async fn heartbeat_stream_heartbeats_then_yields_each_item_in_turn() {
        let queue = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from([
            (Duration::from_millis(70), Some(1)),
            (Duration::from_millis(5), Some(2)),
            (Duration::from_millis(0), None),
        ])));
        let mut stream: HeartbeatStream<u32> = HeartbeatStream::with_interval(None, TEST_INTERVAL);

        // First item is slow (~70ms against a 20ms interval) -- must
        // heartbeat a few times before it's ready, not skip ahead.
        let mut heartbeats = 0;
        let first = loop {
            let q = queue.clone();
            match stream.tick(move || spawn_next_from(q)).await.unwrap() {
                Some(Tick::Heartbeat) => heartbeats += 1,
                Some(Tick::Ready(v)) => break v,
                None => panic!("exhausted before first item"),
            }
        };
        assert_eq!(first, 1);
        assert!(heartbeats >= 2, "expected several heartbeats, got {heartbeats}");

        // Second item resolves fast -- a fresh pull must still be started
        // (not stuck replaying the first, already-consumed handle).
        let q = queue.clone();
        match stream.tick(move || spawn_next_from(q)).await.unwrap() {
            Some(Tick::Ready(v)) => assert_eq!(v, 2),
            other => panic!("expected Ready(2), got {other:?}"),
        }

        // Source signals exhaustion (Ok(None)) -- must surface as `Ok(None)`
        // (StopAsyncIteration equivalent), not an error.
        let q = queue.clone();
        assert!(stream.tick(move || spawn_next_from(q)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn heartbeat_stream_timeout_is_shared_across_items_not_reset_per_item() {
        // Each item individually resolves in 15ms (under a naive per-item
        // timeout), but a 45ms *shared* budget must still trip by the 4th
        // pull -- proves the deadline is computed once at construction, not
        // restarted on every `tick` call.
        let queue = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::from([
            (Duration::from_millis(15), Some(1)),
            (Duration::from_millis(15), Some(2)),
            (Duration::from_millis(15), Some(3)),
            (Duration::from_millis(15), Some(4)),
        ])));
        let mut stream: HeartbeatStream<u32> = HeartbeatStream::with_interval(Some(0.045), TEST_INTERVAL);

        let mut last_err = None;
        'outer: for _ in 0..4 {
            loop {
                let q = queue.clone();
                match stream.tick(move || spawn_next_from(q)).await {
                    Ok(Some(Tick::Heartbeat)) => continue,
                    Ok(Some(Tick::Ready(_))) => continue 'outer,
                    Ok(None) => panic!("must not exhaust before the shared timeout fires"),
                    Err(e) => {
                        last_err = Some(e);
                        break 'outer;
                    }
                }
            }
        }
        let err = last_err.expect("shared 45ms timeout must fire within 4 items of 15ms each");
        assert!(
            err.message.contains("0.045"),
            "error should mention the configured timeout: {}",
            err.message
        );
    }

    /// `HeartbeatStream` counterpart to
    /// `total_timeout_waits_for_the_aborted_task_to_actually_drop` -- same
    /// fix, same reasoning, separate code path (the chunk-fetch phase,
    /// rather than the initial submit/poll wait).
    #[tokio::test]
    async fn heartbeat_stream_timeout_waits_for_the_aborted_task_to_actually_drop() {
        struct SetOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream: HeartbeatStream<u32> = HeartbeatStream::with_interval(Some(0.03), TEST_INTERVAL);

        // A fresh closure is built each loop iteration (tick() takes FnOnce),
        // but `tick()` only actually calls it on the first pull (self.current
        // is None) -- every later iteration's closure is just dropped
        // unused, so only one SetOnDrop guard is ever really constructed.
        let mut last_err = None;
        for _ in 0..10 {
            let dropped_for_task = dropped.clone();
            let spawn_next = move || -> NextItemFuture<u32> {
                Box::pin(async move {
                    let _guard = SetOnDrop(dropped_for_task);
                    tokio::time::sleep(Duration::from_secs(120)).await;
                    Ok(Some(1))
                })
            };
            match stream.tick(spawn_next).await {
                Ok(Some(Tick::Heartbeat)) => continue,
                Ok(_) => panic!("must not become Ready/None before the timeout fires"),
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }
        assert!(last_err.is_some(), "timeout must fire within 10 ticks");
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "the aborted task's future must already be dropped by the time tick() returns the timeout error"
        );
    }
}
