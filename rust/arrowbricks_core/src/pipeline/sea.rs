//! The REST Statement-Execution-API (SEA) backed execute paths: the
//! incremental `ResultStream` (buffers Arrow `RecordBatch`es at the level a
//! `fetchmany`/`fetchall` caller actually asks for), its `execute_lazy`
//! entry point, the eager one-shot `run_pipeline` (this crate's own test
//! suite only -- not reachable from Python), and `execute_lazy_prefer_inline`
//! (the opt-in `disposition: INLINE`/`JSON_ARRAY` fast path, including its
//! fallback to a normal `ResultStream` on the SEA byte-limit-exceeded case).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::client::{
    ApiError, ApiErrorKind, CancelHandle, ColumnDescription, DbClient, InlineOrExternal, QueryStatsAccumulator,
    QueryStatsData, StatementSubmitResult, join_error,
};

use super::reorder::{ReorderBuffer, decode_chunk_item};
use super::stats::{PoisonOnDrop, StatsReporter, report_submit_error};

pub struct ExecuteResult {
    pub statement_id: String,
    pub num_chunks: usize,
    pub batches: Vec<RecordBatch>,
    pub schema: Option<SchemaRef>,
    pub columns: Vec<ColumnDescription>,
}

impl ExecuteResult {
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

/// Caps how many chunks `ResultStream::fetch_at_least` will pull/decode
/// ahead of a `fetchmany` request before checking real (decoded) row counts
/// against what was asked for. Chunk metas carry a `row_count` estimate from
/// the manifest that's normally used for this instead, but it can be absent
/// -- this bounds worst-case over-fetch when it is, same reasoning as
/// Python's own `chunk_fetch_concurrency` default: I/O-bound work doesn't
/// benefit past a modest amount of look-ahead.
const MAX_CHUNKS_PER_FETCH_BATCH: usize = 32;

/// Lazy, incremental counterpart to `run_pipeline`/`ExecuteResult`: pulls and
/// decodes only as many chunks as a `fetchmany`-style caller actually asks
/// for, buffering the rest at the Arrow `RecordBatch` level -- the same
/// architecture as `cursor.py`'s `_ResultSet` (buffer at the Arrow level, not
/// materialized rows, so Arrow-native fetches stay zero-copy and a
/// `fetchmany(100)` loop over a huge result never pulls more chunks than it
/// consumes).
pub struct ResultStream {
    pub statement_id: String,
    pub num_chunks: usize,
    pub schema: Option<SchemaRef>,
    pub columns: Vec<ColumnDescription>,
    pub(crate) reorder: ReorderBuffer,
    pub(crate) pending: VecDeque<RecordBatch>,
    pub(crate) pending_rows: usize,
    pub(crate) exhausted: bool,
    /// Set (via `PoisonOnDrop`) if a `fetch_at_least` call ever exits
    /// without reaching its own end -- see that guard's doc comment.
    pub(crate) poisoned: bool,
    /// Enough to fire a best-effort server-side cancel -- see
    /// `client::CancelHandle`'s own doc comment. Read (cloned) by `lib.rs`
    /// at construction time to build the `on_cancel` hook `heartbeat.rs`'s
    /// `HeartbeatWait`/`HeartbeatStream` fire on `total_timeout_s`/
    /// cancellation, without needing to lock this stream's own
    /// `Arc<AsyncMutex<..>>` to get at it.
    pub cancel_handle: CancelHandle,
    /// Shared with `heartbeat.rs`'s own `on_cancel` hook the same way --
    /// see `QueryStatsAccumulator`'s own doc comment for why sharing this
    /// Arc (rather than this struct's own private counters) is what lets a
    /// `total_timeout_s`/cancellation trigger tell `StatsReporter` which
    /// outcome to report from *inside* a `Drop` impl, where nothing else
    /// reachable from this struct's own fields would still be valid.
    pub stats: Arc<QueryStatsAccumulator>,
    pub(crate) reporter: StatsReporter,
}

impl ResultStream {
    /// Pulls/decodes chunks until at least `want_rows` are buffered (or the
    /// source is exhausted). Chunks are pulled in bounded batches -- using
    /// each chunk's manifest `row_count` estimate to decide how many to pull
    /// before decoding, capped per batch at `MAX_CHUNKS_PER_FETCH_BATCH` --
    /// and each batch's `spawn_blocking` decode handles are awaited
    /// together, same fetch/decode overlap reasoning as `run_pipeline`. The
    /// outer loop repeats batches until `want_rows` is actually met (not
    /// just one bounded batch -- `want_rows = usize::MAX` must still drain a
    /// result with more than `MAX_CHUNKS_PER_FETCH_BATCH` chunks, so a
    /// single capped batch isn't enough).
    async fn fetch_at_least(&mut self, want_rows: usize) -> Result<(), ApiError> {
        if self.poisoned {
            return Err(ApiError {
                message: "this result was left incomplete by a previous cancelled, timed-out, or failed fetch -- \
                          re-run the query instead of continuing to use this cursor/result"
                    .to_string(),
                transient: false,
                kind: ApiErrorKind::Other,
            });
        }
        let mut guard = PoisonOnDrop::new(&mut self.poisoned, &mut self.reporter, self.stats.as_ref());
        while self.pending_rows < want_rows && !self.exhausted {
            let mut decode_handles = Vec::new();
            let mut estimated_new_rows = 0usize;
            while self.pending_rows + estimated_new_rows < want_rows
                && !self.exhausted
                && decode_handles.len() < MAX_CHUNKS_PER_FETCH_BATCH
            {
                match self.reorder.next().await {
                    Ok(Some(item)) => {
                        estimated_new_rows += item.row_count.unwrap_or(0).max(0) as usize;
                        let truncate_to = item.truncate_to;
                        decode_handles.push(tokio::task::spawn_blocking(move || {
                            decode_chunk_item(&item.blob, truncate_to)
                        }));
                    }
                    Ok(None) => self.exhausted = true,
                    Err(e) => return guard.fail(e),
                }
            }
            for handle in decode_handles {
                let batches = match handle.await {
                    Ok(Ok(batches)) => batches,
                    Ok(Err(e)) => return guard.fail(e),
                    Err(join_err) => return guard.fail(join_error(join_err)),
                };
                for batch in batches {
                    if self.schema.is_none() {
                        self.schema = Some(batch.schema());
                    }
                    self.pending_rows += batch.num_rows();
                    self.pending.push_back(batch);
                }
            }
        }
        guard.defuse();
        guard.reporter.end_fetch();
        if self.exhausted {
            guard.reporter.finish("success", guard.stats);
        }
        Ok(())
    }

    /// Takes up to `n` rows off the front of the buffer, splitting a batch
    /// with `RecordBatch::slice` if it straddles the boundary -- the
    /// remainder stays buffered for the next call. Assumes `fetch_at_least`
    /// already ran for this call; may return fewer than `n` rows (or none)
    /// if the source was exhausted first.
    fn take(&mut self, n: usize) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        let mut remaining = n;
        while remaining > 0 {
            let Some(front) = self.pending.pop_front() else { break };
            if front.num_rows() <= remaining {
                remaining -= front.num_rows();
                self.pending_rows -= front.num_rows();
                out.push(front);
            } else {
                let head = front.slice(0, remaining);
                let tail = front.slice(remaining, front.num_rows() - remaining);
                self.pending_rows -= remaining;
                self.pending.push_front(tail);
                out.push(head);
                remaining = 0;
            }
        }
        out
    }

    pub async fn fetchmany_arrow(&mut self, n: usize) -> Result<(Vec<RecordBatch>, Option<SchemaRef>), ApiError> {
        self.fetch_at_least(n).await?;
        Ok((self.take(n), self.schema.clone()))
    }

    pub async fn fetchall_arrow(&mut self) -> Result<(Vec<RecordBatch>, Option<SchemaRef>), ApiError> {
        self.fetch_at_least(usize::MAX).await?;
        let all = self.pending_rows;
        Ok((self.take(all), self.schema.clone()))
    }
}

/// Shared by `execute_lazy` and `execute_ndjson_stream`'s SEA branch --
/// submits via `execute_arrow_statement`, timing `submit_to_ready_s` and
/// reporting `on_event`'s `"error"` outcome (via `report_submit_error`) if
/// it fails. `warehouse_wait_s` is *not* timed here at all -- it's recorded
/// once, internally, by `client::DbClient::submit_and_poll` (which this
/// calls into exactly once), and read back from `stats.warehouse_wait_s()`
/// by this function's own caller. Extracting this one shared helper (rather
/// than repeating "time it, call it, report_submit_error on failure" at
/// each call site) is what makes fixing that error-reporting path
/// consistent across every SEA call site instead of three separate,
/// easily-desynced copies -- found in code review as real duplication, not
/// just a style nit, since one of the three copies used to also silently
/// re-time `ensure_warehouse_running` a second time per query. Doesn't
/// cover `execute_arrow_statement_prefer_inline`'s own submission --
/// `execute_lazy_prefer_inline` needs its own copy of this shape, since
/// `InlineOrExternal`'s return type differs and its own fallback (a JSON
/// conversion failure, not a submission failure) needs to keep timing
/// `submit_to_ready_s` across a second, additional submission rather than
/// just once.
pub(crate) async fn submit_sea_and_report(
    client: &Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
    stats: &QueryStatsAccumulator,
) -> Result<(StatementSubmitResult, f64), ApiError> {
    let submit_t0 = Instant::now();
    match client
        .execute_arrow_statement(statement, catalog, schema, parameters, stats)
        .await
    {
        Ok(s) => Ok((s, submit_t0.elapsed().as_secs_f64())),
        Err(e) => {
            report_submit_error(client, "sea", submit_t0.elapsed().as_secs_f64(), stats);
            Err(e)
        }
    }
}

/// Submit -> poll -> start background chunk fetching, without draining
/// anything yet -- pairs with `ResultStream`'s `fetchmany_arrow`/
/// `fetchall_arrow` for on-demand pulling.
pub async fn execute_lazy(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ResultStream, ApiError> {
    let stats = Arc::new(QueryStatsAccumulator::default());
    let (submitted, submit_to_ready_s) =
        submit_sea_and_report(&client, statement, catalog, schema, parameters, &stats).await?;
    let warehouse_wait_s = stats.warehouse_wait_s();
    Ok(result_stream_from_submitted(
        client,
        submitted,
        stats,
        warehouse_wait_s,
        submit_to_ready_s,
    ))
}

fn result_stream_from_submitted(
    client: Arc<DbClient>,
    submitted: StatementSubmitResult,
    stats: Arc<QueryStatsAccumulator>,
    warehouse_wait_s: f64,
    submit_to_ready_s: f64,
) -> ResultStream {
    let num_chunks = submitted.chunk_metas.len();
    let rx = client.clone().fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
        stats.clone(),
    );
    ResultStream {
        statement_id: submitted.statement_id.clone(),
        num_chunks,
        schema: None,
        columns: submitted.columns,
        reorder: ReorderBuffer::new(rx),
        pending: VecDeque::new(),
        pending_rows: 0,
        exhausted: false,
        poisoned: false,
        cancel_handle: CancelHandle::Sea {
            statement_id: submitted.statement_id.clone(),
        },
        stats,
        reporter: StatsReporter {
            client,
            statement_id: submitted.statement_id,
            protocol: "sea",
            warehouse_wait_s,
            submit_to_ready_s,
            fetch_s: 0.0,
            fetch_started_at: None,
            static_num_chunks: Some(num_chunks),
            reported: false,
        },
    }
}

/// Like `execute_lazy`, but first tries `disposition: INLINE` + `format:
/// JSON_ARRAY` via `DbClient::execute_arrow_statement_prefer_inline` -- for
/// a small result, this skips the chunk-fetch round trip entirely (see that
/// function's own doc comment for the full reasoning, including its own
/// **safe-to-double-execute** fallback for a statement that reached a
/// FAILED state, e.g. the INLINE byte-limit-exceeded case -- nothing
/// committed server-side, so a fresh submission is a distinct, harmless
/// execution). Converts the returned JSON_ARRAY rows into a `RecordBatch`
/// via `json_convert`; if that conversion fails, this does **not** fall
/// back to a fresh `execute_lazy`/resubmission the way the byte-limit case
/// does -- found in code review (2026-08-11) that this call site's own
/// statement already reached SUCCEEDED (unlike the byte-limit case), so any
/// DML side effects (INSERT/MERGE/UPDATE/DELETE) already happened;
/// resubmitting the identical SQL here would silently duplicate them for
/// non-idempotent SQL, with nothing surfaced to the caller. See the
/// conversion-failure arm below for what this does instead. Not the
/// default -- opt-in only, via `Cursor.execute(..., prefer_inline=True)`.
pub async fn execute_lazy_prefer_inline(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ResultStream, ApiError> {
    let stats = Arc::new(QueryStatsAccumulator::default());
    // Not `submit_sea_and_report` -- `execute_arrow_statement_prefer_inline`'s
    // own return shape (`InlineOrExternal`) differs from plain
    // `execute_arrow_statement`'s, and the JSON-conversion fallback below
    // needs to keep timing this same `submit_t0` across a *second*
    // submission rather than starting fresh -- see that fallback's own doc
    // comment.
    let submit_t0 = Instant::now();
    let outcome = match client
        .execute_arrow_statement_prefer_inline(statement, catalog, schema, parameters.clone(), &stats)
        .await
    {
        Ok(o) => o,
        Err(e) => {
            report_submit_error(&client, "sea", submit_t0.elapsed().as_secs_f64(), &stats);
            return Err(e);
        }
    };
    let submit_to_ready_s = submit_t0.elapsed().as_secs_f64();

    let (statement_id, rows, columns) = match outcome {
        InlineOrExternal::External(submitted) => {
            return Ok(result_stream_from_submitted(
                client,
                submitted,
                stats.clone(),
                stats.warehouse_wait_s(),
                submit_to_ready_s,
            ));
        }
        InlineOrExternal::Inline {
            statement_id,
            rows,
            columns,
        } => (statement_id, rows, columns),
    };

    match crate::json_convert::json_array_to_record_batch(&rows, &columns) {
        Ok(batch) => {
            let pending_rows = batch.num_rows();
            let schema = Some(batch.schema());
            // Sender dropped immediately -- never touched. `exhausted:
            // true` and `pending` already fully populated below mean
            // `fetch_at_least`'s own loop condition (`!self.exhausted`)
            // never calls `self.reorder.next()` at all for this stream.
            let (_tx, rx) = mpsc::channel(1);
            Ok(ResultStream {
                statement_id: statement_id.clone(),
                num_chunks: 1,
                schema,
                columns,
                reorder: ReorderBuffer::new(rx),
                pending: VecDeque::from([batch]),
                pending_rows,
                exhausted: true,
                poisoned: false,
                // An INLINE result is already fully returned and terminal by
                // the time this statement_id even exists -- there is
                // nothing left running server-side to cancel. Kept as a
                // real `CancelHandle` anyway (rather than a special no-op
                // variant) purely for type-uniformity; firing it here would
                // just be a harmless no-op against an already-finished
                // statement, same as the "belt-and-suspenders" reasoning
                // `cancel_statement`'s own doc comment describes.
                cancel_handle: CancelHandle::Sea {
                    statement_id: statement_id.clone(),
                },
                stats: stats.clone(),
                reporter: StatsReporter {
                    client,
                    statement_id,
                    protocol: "sea",
                    warehouse_wait_s: stats.warehouse_wait_s(),
                    submit_to_ready_s,
                    // No separate download phase for an INLINE result --
                    // the rows are already in hand from the submit/poll
                    // response itself.
                    fetch_s: 0.0,
                    fetch_started_at: None,
                    static_num_chunks: Some(1),
                    reported: false,
                },
            })
        }
        // **Does NOT resubmit `statement`** -- found in code review
        // (2026-08-11) that an earlier version of this arm called
        // `execute_arrow_statement(statement, ...)` here, on the same
        // "distinct execution, safe to double-run" reasoning
        // `execute_arrow_statement_prefer_inline`'s own byte-limit-exceeded
        // fallback (`client/sea.rs`) uses -- but that reasoning doesn't transfer:
        // the byte-limit case only ever fires for a statement that reached
        // a FAILED state server-side (nothing committed), while this arm
        // only runs after the statement already reached SUCCEEDED -- rows
        // genuinely came back, so any DML side effects (INSERT/MERGE/
        // UPDATE/DELETE) already happened. Blindly resubmitting the
        // identical SQL here would silently duplicate those side effects
        // for non-idempotent SQL, with nothing surfaced to the caller --
        // reproduced directly: `protocol="sea"`, `prefer_inline=True`, an
        // INSERT whose INLINE JSON result has a column type
        // `json_convert` can't handle, and the mock statements route was
        // hit exactly twice instead of once. There is also no way to fetch
        // *this same* statement's data a different way instead (an INLINE
        // submission has no `external_links`/manifest chunks to fall back
        // to -- returning the data inline in the response is the entire
        // point of INLINE, so there is nothing left to resolve for this
        // `statement_id`) -- the only safe option is a clear error, not a
        // silent retry-shaped write. The `on_event` dispatch below is
        // built inline rather than via `report_submit_error` specifically
        // so it can report the *real* `statement_id` (which does exist --
        // the statement genuinely succeeded) instead of that helper's own
        // `statement_id: String::new()`, meant for a submission that never
        // produced one at all.
        Err(e) => {
            let submit_to_ready_s = submit_t0.elapsed().as_secs_f64();
            if let Some(sink) = client.on_event() {
                sink.on_event(QueryStatsData {
                    statement_id: statement_id.clone(),
                    protocol: "sea",
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
            Err(ApiError::permanent(format!(
                "prefer_inline: statement {statement_id} succeeded and returned an INLINE result, but it could \
                 not be converted to Arrow ({e}) -- refusing to automatically re-run the query to avoid \
                 duplicating any write it performed; pass prefer_inline=False (or a column-type-compatible \
                 projection) if you need this result, or re-submit it yourself if you know it's safe to re-run"
            )))
        }
    }
}

/// Full submit -> poll -> fetch -> reorder -> decode pipeline. Returns the
/// assembled batches in logical (chunk_index) order plus their schema, ready
/// to hand to `pyo3_arrow::PyTable` for a zero-copy Arrow C Data Interface
/// handoff back to Python (consumable by DuckDB/pyarrow/arro3 directly).
///
/// Decode of each reordered chunk is pushed onto `spawn_blocking` rather
/// than run inline: Arrow-IPC decode is CPU work, and running it on the
/// same task that's driving `reorder.next()` would serialize it against
/// that task's own progress. Spawning it lets the blocking-pool thread
/// decode chunk N while the async-pool threads keep fetching/reordering
/// chunk N+1+ in parallel -- real overlap between network and CPU work,
/// not just concurrent network fetches. Handles are pushed in the order
/// `reorder.next()` releases them (the correct logical order) and awaited
/// in that same order, so concurrent decode doesn't reshuffle row order.
pub async fn run_pipeline(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ExecuteResult, ApiError> {
    // `run_pipeline` is the eager, one-shot path -- not reachable from
    // Python (`lib.rs` never calls it; only this crate's own test suite
    // does), so it has no `CancelHandle`/`StatsReporter`/`on_event` wiring
    // of its own. It still needs *a* `QueryStatsAccumulator` to satisfy
    // `execute_arrow_statement`/`fetch_chunks_with_backpressure`'s own
    // signatures -- built and discarded here, never read back.
    let stats = Arc::new(QueryStatsAccumulator::default());
    let submitted = client
        .execute_arrow_statement(statement, catalog, schema, parameters, &stats)
        .await?;
    let num_chunks = submitted.chunk_metas.len();

    let rx = client.clone().fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
        stats,
    );
    let mut reorder = ReorderBuffer::new(rx);

    let mut decode_handles = Vec::with_capacity(num_chunks);
    while let Some(item) = reorder.next().await? {
        let truncate_to = item.truncate_to;
        decode_handles.push(tokio::task::spawn_blocking(move || {
            decode_chunk_item(&item.blob, truncate_to)
        }));
    }

    let mut batches = Vec::new();
    for handle in decode_handles {
        batches.extend(handle.await.map_err(join_error)??);
    }
    let schema = batches.first().map(|b| b.schema());

    Ok(ExecuteResult {
        statement_id: submitted.statement_id,
        num_chunks,
        batches,
        schema,
        columns: submitted.columns,
    })
}
