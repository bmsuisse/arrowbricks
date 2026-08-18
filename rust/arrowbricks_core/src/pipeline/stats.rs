//! Observability/stats-accumulation plumbing shared by every backend and
//! consumer (`ResultStream`, `NdjsonStream`): `cancel_hook` builds the
//! closure `heartbeat.rs`'s timeout/cancellation triggers fire,
//! `StatsReporter` builds and dispatches exactly one `QueryStatsData` event
//! per query, and `PoisonOnDrop`/`ReportOnDrop` are `Drop`-based guards that
//! make sure that dispatch (and, for `PoisonOnDrop`, poisoning an abandoned
//! `ResultStream`) still happens even when a fetch is abandoned mid-flight
//! (cancelled or timed out) rather than reaching its own normal return.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::client::{ApiError, CancelHandle, DbClient, QueryStatsAccumulator, QueryStatsData};

/// Builds the closure `heartbeat::HeartbeatWait::with_cancel`/
/// `heartbeat::HeartbeatStream::with_cancel` fire on `total_timeout_s`/
/// cancellation -- shared by `lib.rs`'s `PyResultSet::fetchall_arrow_streamed`
/// and `PyNdjsonStreamIter`'s `Running` state, the two Rust-side wrappers
/// this design's cancellation feature plugs into (see `heartbeat.rs`'s own
/// doc comments for exactly which two triggers that is). Lives here, not in
/// `lib.rs`, specifically so this -- the actual mechanism those two triggers
/// fire -- is constructible (and therefore testable against a real mock
/// HTTP/Thrift server) from a plain `#[tokio::test]` with no PyO3/Python
/// involved at all; see `tests/wiremock_pipeline.rs`/`tests/wiremock_thrift.rs`'s
/// own cancellation tests.
///
/// Records the outcome hint into `stats` *before* spawning the actual cancel
/// RPC/REST call -- both must happen synchronously, in that order, inside
/// the closure itself (not after awaiting anything), since `with_cancel`'s
/// own contract requires this to run before the wrapped future/task is
/// aborted -- see that method's doc comment for why.
pub fn cancel_hook(
    client: Arc<DbClient>,
    handle: CancelHandle,
    stats: Arc<QueryStatsAccumulator>,
) -> impl FnOnce(bool) + Send + 'static {
    move |is_timeout: bool| {
        stats.store_outcome_if_unset(is_timeout);
        // Bare statement, not `let _ = ...` -- see `PyEventSink::on_event`'s
        // identical comment on why (clippy's `let_underscore_future`).
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            client.cancel_statement(&handle).await;
        });
    }
}

/// Per-`ResultStream`/`NdjsonStream` bookkeeping needed to build and
/// dispatch exactly one `QueryStatsData` per query, at completion -- see
/// `client::QueryStatsAccumulator`/`EventSink` for the counters/dispatch
/// mechanism this wraps. Everything here is plain, single-owner data (this
/// struct is never shared across a `tokio::spawn` boundary); the counters
/// that genuinely need concurrent access from spawned chunk-fetch workers
/// live in `QueryStatsAccumulator` instead, passed into `finish` explicitly
/// rather than duplicated here.
pub(crate) struct StatsReporter {
    pub(crate) client: Arc<DbClient>,
    pub(crate) statement_id: String,
    pub(crate) protocol: &'static str,
    pub(crate) warehouse_wait_s: f64,
    pub(crate) submit_to_ready_s: f64,
    pub(crate) fetch_s: f64,
    pub(crate) fetch_started_at: Option<Instant>,
    /// `Some` for SEA, whose whole chunk manifest (and therefore its count)
    /// is known upfront -- `chunk_metas.len()`, the same number
    /// `ResultStream.num_chunks`/`NdjsonStream.num_chunks` already carry.
    /// `None` for Thrift, which doesn't know its chunk count until it's
    /// fully discovered them; `finish` falls back to
    /// `QueryStatsAccumulator::chunks_seen` in that case instead.
    pub(crate) static_num_chunks: Option<usize>,
    /// Self-once guard: an explicit `finish("success"/"error")` call and
    /// `PoisonOnDrop`/`ReportOnDrop`'s own fallback call can both race to be
    /// the one that actually reports (a genuine error already calls
    /// `finish` explicitly, then the guard's own `Drop` fires too, on the
    /// far side of the `return`) -- only the first one to run wins.
    pub(crate) reported: bool,
}

impl StatsReporter {
    pub(crate) fn begin_fetch(&mut self) {
        self.fetch_started_at.get_or_insert_with(Instant::now);
    }

    pub(crate) fn end_fetch(&mut self) {
        if let Some(t) = self.fetch_started_at.take() {
            self.fetch_s += t.elapsed().as_secs_f64();
        }
    }

    pub(crate) fn finish(&mut self, outcome: &'static str, stats: &QueryStatsAccumulator) {
        self.end_fetch();
        if self.reported {
            return;
        }
        self.reported = true;
        let Some(sink) = self.client.on_event() else {
            return;
        };
        let data = QueryStatsData {
            statement_id: self.statement_id.clone(),
            protocol: self.protocol,
            warehouse_wait_s: self.warehouse_wait_s,
            submit_to_ready_s: self.submit_to_ready_s,
            fetch_s: self.fetch_s,
            num_chunks: self
                .static_num_chunks
                .unwrap_or_else(|| stats.chunks_seen.load(Ordering::Relaxed)),
            bytes_downloaded: stats.bytes_downloaded.load(Ordering::Relaxed),
            retry_count: stats.retry_count.load(Ordering::Relaxed),
            concurrency_used: self.client.chunk_fetch_concurrency,
            outcome,
        };
        sink.on_event(data);
    }
}

/// Arms on construction, poisons `*poisoned` on `Drop` unless `defuse()` was
/// called first. `fetch_at_least` arms one of these at the top and only
/// defuses it right before its own final `Ok(())` -- so *any* early exit
/// (a genuine `?`-propagated error, or the whole future being dropped
/// mid-`.await` by Python-side cancellation: `task.cancel()`/
/// `asyncio.wait_for` timing out) leaves the stream poisoned.
///
/// This matters because `fetch_at_least` pulls chunks off `self.reorder`
/// (removing them from it) into a *local* `decode_handles` list before
/// decoding and only appending decoded batches to `self.pending`/
/// `self.pending_rows` afterwards -- an early exit (any early exit, not
/// just cancellation) abandons whatever was in `decode_handles` at that
/// moment. Those chunks are gone for good (already consumed out of the
/// reorder buffer, decode results discarded with nothing left holding
/// their `JoinHandle`s), but `self.exhausted`/`self.reorder`'s own internal
/// bookkeeping has already moved past them. Found in code review and
/// reproduced: a `fetchall()` cancelled or timed out mid-download, then
/// retried on the *same* `Cursor`/`ResultStream`, silently returned a real
/// but truncated row count (20 of 30 expected) with no error at all --
/// `fetch_at_least` checking `poisoned` up front turns that into a loud,
/// immediate error on the next use instead.
///
/// Also doubles as the completion signal for observability's `QueryStats`:
/// a `Drop` that fires while still armed means this call never reached its
/// own normal end for *any* reason -- most commonly the same abandonment
/// this guard already exists to poison against. `QueryStatsAccumulator::
/// pending_outcome()` tells "the total_timeout_s/cancellation this
/// abandonment actually is" apart from a genuine mid-batch data/network
/// error (which already called `finish("error", ..)` explicitly, and
/// therefore already reported, before propagating its own `Err` up through
/// this guard's own `Drop`) -- see that method's own doc comment for why
/// this is the only point from which that distinction can be made at all.
pub(crate) struct PoisonOnDrop<'a> {
    poisoned: &'a mut bool,
    pub(crate) reporter: &'a mut StatsReporter,
    pub(crate) stats: &'a QueryStatsAccumulator,
    armed: bool,
}

impl<'a> PoisonOnDrop<'a> {
    pub(crate) fn new(
        poisoned: &'a mut bool,
        reporter: &'a mut StatsReporter,
        stats: &'a QueryStatsAccumulator,
    ) -> Self {
        reporter.begin_fetch();
        Self {
            poisoned,
            reporter,
            stats,
            armed: true,
        }
    }

    pub(crate) fn defuse(&mut self) {
        self.armed = false;
    }

    /// Reports `outcome="error"` and hands `e` back wrapped in `Err`, so a
    /// call site can just `return guard.fail(e);` instead of repeating
    /// `guard.reporter.finish("error", guard.stats); return Err(e);` at
    /// every one of `fetch_at_least`'s several error-producing points
    /// (found in review: that exact pair was duplicated 3x in this one
    /// function alone). Does *not* defuse -- an explicit `Err` here is a
    /// genuine data/network failure, which must still poison the stream the
    /// same way an abandoned-mid-batch `Drop` would (see this guard's own
    /// doc comment on why a genuine error is poisoned too, not just
    /// cancellation).
    pub(crate) fn fail<T>(&mut self, e: ApiError) -> Result<T, ApiError> {
        self.reporter.finish("error", self.stats);
        Err(e)
    }
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.poisoned = true;
            let outcome = self.stats.pending_outcome().unwrap_or("cancelled");
            self.reporter.finish(outcome, self.stats);
        }
    }
}

/// `NdjsonStream`'s counterpart to `PoisonOnDrop`, minus the poisoning --
/// `next_chunk` pulls and hands off exactly one already-fully-dequeued item
/// at a time (no local, multi-item `decode_handles` list an early exit could
/// abandon mid-batch the way `fetch_at_least` could), so there's no
/// analogous correctness bug to guard against here. Exists purely so
/// observability's "abandoned mid-fetch" `QueryStats` dispatch (see
/// `PoisonOnDrop`'s own doc comment for why this has to be a `Drop` impl at
/// all) covers `stream_ndjson_lines`/`stream_query_json` too, not just the
/// `ResultStream`-backed fetch methods.
pub(crate) struct ReportOnDrop<'a> {
    pub(crate) reporter: &'a mut StatsReporter,
    pub(crate) stats: &'a QueryStatsAccumulator,
    armed: bool,
}

impl<'a> ReportOnDrop<'a> {
    pub(crate) fn new(reporter: &'a mut StatsReporter, stats: &'a QueryStatsAccumulator) -> Self {
        reporter.begin_fetch();
        Self {
            reporter,
            stats,
            armed: true,
        }
    }

    pub(crate) fn defuse(&mut self) {
        self.armed = false;
    }

    /// See `PoisonOnDrop::fail`'s identical doc comment -- same
    /// deduplication, no poisoning concern here (see this struct's own doc
    /// comment for why).
    pub(crate) fn fail<T>(&mut self, e: ApiError) -> Result<T, ApiError> {
        self.reporter.finish("error", self.stats);
        Err(e)
    }
}

impl Drop for ReportOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            let outcome = self.stats.pending_outcome().unwrap_or("cancelled");
            self.reporter.finish(outcome, self.stats);
        }
    }
}

/// Reports a `QueryStats` event (outcome `"error"`) for the case a query
/// fails *before* a `ResultStream`/`NdjsonStream` -- and therefore a
/// `StatsReporter` -- ever exists: `ensure_warehouse_running`/submit/poll
/// itself failing (a FAILED/CANCELED statement, a network error, or a
/// stopped/unreachable warehouse). Without this, an `on_event` caller would
/// never hear about the single most common real "error" case (e.g. bad
/// SQL, or the warehouse being down) at all, since that never reaches the
/// point `StatsReporter` is normally constructed at. `warehouse_wait_s` is
/// read back from `stats` (see `QueryStatsAccumulator::warehouse_wait_s`),
/// not passed in separately -- `submit_and_poll`/`submit_thrift_and_start_fetch`
/// record it there themselves, including on their own failure paths, so
/// it's always current by the time any caller of this function has an
/// error in hand.
///
/// `statement_id` is best-effort empty (`""`) here: `ApiError` doesn't carry
/// it, so a failure that happened after the server *did* assign one (a
/// polled FAILED/CANCELED statement) still can't be attributed to it from
/// here -- a real, accepted gap, not something this call site can close
/// without a wider change to `ApiError` itself.
///
/// Deliberately does **not** attempt to catch task-cancellation/
/// `total_timeout_s` *during* this submit/poll phase the way `PoisonOnDrop`/
/// `ReportOnDrop` do for the fetch phase -- no `heartbeat.rs` wrapper covers
/// this phase either (see this design's own scope note: both `on_event` and
/// the cancellation feature share the identical blind spot here), so there
/// is nothing for a `Drop`-based guard to distinguish it from `finish`'s own
/// "already reported" idempotency without a real, working hint. A caller
/// who cancels a `Cursor.execute()`/`stream_query_json` call mid-submit
/// today gets no `on_event` at all for that attempt, same as before this
/// feature existed.
pub(crate) fn report_submit_error(
    client: &Arc<DbClient>,
    protocol: &'static str,
    submit_to_ready_s: f64,
    stats: &QueryStatsAccumulator,
) {
    let Some(sink) = client.on_event() else {
        return;
    };
    sink.on_event(QueryStatsData {
        statement_id: String::new(),
        protocol,
        warehouse_wait_s: stats.warehouse_wait_s(),
        submit_to_ready_s,
        fetch_s: 0.0,
        num_chunks: stats.chunks_seen.load(Ordering::Relaxed),
        bytes_downloaded: stats.bytes_downloaded.load(Ordering::Relaxed),
        retry_count: stats.retry_count.load(Ordering::Relaxed),
        concurrency_used: client.chunk_fetch_concurrency,
        outcome: "error",
    });
}
