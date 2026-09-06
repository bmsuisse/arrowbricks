//! The Thrift/TCLIService backed execute path: `execute_lazy_thrift` and
//! everything it depends on -- session handling (mirroring the SEA session
//! pool with one necessary difference, see `execute_lazy_thrift`'s own doc
//! comment), the sequential `FetchResults(orientation: FETCH_NEXT)` discovery
//! loop paired with a bounded-concurrency link-download worker pool, and
//! inline `arrowBatches` decode -- producing the exact same `ResultStream`
//! shape `pipeline::sea`'s SEA path does, so `PyResultSet` and everything
//! above it needs zero Thrift-specific handling.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arrow_buffer::Buffer as ArrowBuffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::client::{
    ApiError, CancelHandle, ChunkItem, ColumnDescription, DbClient, QueryStatsAccumulator, join_error,
};
use crate::thrift;

use super::reorder::ReorderBuffer;
use super::sea::ResultStream;
use super::stats::{StatsReporter, report_submit_error};

/// Encodes 16 raw bytes (a THandleIdentifier's guid) as lowercase hex --
/// used only to give a Thrift result stream a human-readable `statement_id`
/// string for `ResultSet.statement_id`/logging parity with the SEA path
/// (which gets a real `statement_id` straight from the REST API). Not
/// pulling in a `hex` crate for this: it's a handful of lines and the only
/// place this crate needs hex encoding at all.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// What's known once a Thrift statement has reached a terminal state and is
/// ready to hand off to `drive_thrift_fetch_loop` -- a small bundle so
/// `execute_lazy_thrift`'s own error paths (see its doc comment) have one
/// single point to close a throwaway session on, instead of repeating that
/// cleanup at every `return Err(...)`.
struct ThriftStatementReady {
    operation: thrift::OperationHandle,
    schema_bytes: Option<bytes::Bytes>,
    compression: Option<bool>,
    initial_rowset: Option<(thrift::RowSet, bool)>,
    already_closed: bool,
}

async fn submit_and_await_thrift_statement(
    client: &Arc<DbClient>,
    session: &thrift::SessionHandle,
    statement: &str,
    parameters: Option<&Value>,
    stats: &QueryStatsAccumulator,
) -> Result<ThriftStatementReady, ApiError> {
    let resp = client
        .thrift_execute_statement_raw(session, statement, parameters, stats)
        .await?;

    let operation = resp
        .operation_handle
        .ok_or_else(|| ApiError::permanent("Thrift ExecuteStatement succeeded with no operationHandle".to_string()))?;

    let mut schema_bytes: Option<bytes::Bytes> = None;
    // Preserve whether metadata actually confirmed compression, so direct
    // links can start downloading before the next FetchResults response.
    let mut compression = None;
    let mut initial_rowset: Option<(thrift::RowSet, bool)> = None;
    let mut already_finished = false;
    let mut already_closed = false;

    if let Some(direct) = resp.direct_results {
        already_closed = direct.already_closed;
        if let Some(meta) = &direct.result_set_metadata {
            schema_bytes = meta.arrow_schema.clone();
            compression = Some(meta.lz4_compressed);
        }
        if let Some(op_status) = &direct.operation_status {
            if let Some(e) = op_status.terminal_error() {
                return Err(ApiError::statement_failed(format!("Thrift statement failed: {e}")));
            }
            already_finished = op_status.is_finished();
        }
        if let Some(fr) = direct.result_set {
            if let Some(e) = fr.status.error() {
                return Err(ApiError::permanent(format!("Thrift FetchResults (direct) failed: {e}")));
            }
            if let Some(meta) = &fr.result_set_metadata {
                if schema_bytes.is_none() {
                    schema_bytes = meta.arrow_schema.clone();
                }
                compression = Some(meta.lz4_compressed);
            }
            let has_more = fr.has_more_rows;
            if let Some(rs) = fr.results {
                initial_rowset = Some((rs, has_more));
            }
        }
    }

    if !already_finished {
        loop {
            let status = client.thrift_get_operation_status_raw(&operation, stats).await?;
            if let Some(e) = status.terminal_error() {
                return Err(ApiError::statement_failed(format!("Thrift statement failed: {e}")));
            }
            if status.is_finished() {
                break;
            }
            tokio::time::sleep(crate::client::THRIFT_POLL_INTERVAL).await;
        }
    }

    Ok(ThriftStatementReady {
        operation,
        schema_bytes,
        compression,
        initial_rowset,
        already_closed,
    })
}

/// Submit -> (maybe poll) -> start background chunk fetching for the
/// `protocol="thrift"` backend -- see `client::Protocol::Thrift`'s own doc
/// comment for the wire-format background and why this is faster than SEA
/// for small queries. Produces the exact same `ResultStream` shape as
/// `execute_lazy`/`execute_lazy_prefer_inline` (same `fetchmany_arrow`/
/// `fetchall_arrow`/`schema` contract, same `ReorderBuffer`/`decode_chunk`
/// underneath), so `PyResultSet` and everything above it needs zero
/// Thrift-specific handling.
///
/// Session handling mirrors the SEA session pool (`client::Pool<String>`)
/// exactly, with one necessary difference: Thrift's `TExecuteStatementReq`
/// *requires* a `sessionHandle` (unlike SEA's optional `session_id`), so
/// pool exhaustion/creation failure can't fall back to a session-less
/// submission the way SEA does -- instead, a fresh, unpooled session is
/// opened just for this one call and closed again immediately afterward.
/// This is a genuinely necessary fallback, not a shortcut: without it, a
/// caller who exhausts `MAX_SESSIONS_PER_KEY` concurrent statements would
/// get a hard failure instead of the same "a bit more session-creation
/// overhead, but still works" degradation SEA gets.
///
/// A session is only needed for the initial `ExecuteStatement` call --
/// every subsequent RPC (`GetOperationStatus`/`FetchResults`/
/// `CloseOperation`) addresses the operation directly by its own handle, so
/// the session is checked back in (or closed, if it was a throwaway) right
/// after `ExecuteStatement` returns, not held for the statement's whole
/// lifetime the way one might expect from a "session" name.
///
/// **A session checked out for this call (whether pooled or a throwaway,
/// see `client::Pool<thrift::SessionHandle>`'s doc comment) must stay open until the
/// operation it created is fully drained and closed -- not just until the
/// statement reaches a terminal "finished" state.** Found the hard way by
/// testing genuine concurrent load against the real warehouse: closing a
/// throwaway session immediately after `ExecuteStatement` returned (the
/// original version of this function) intermittently (~20-30% of runs, once
/// concurrency exceeded `MAX_SESSIONS_PER_KEY`) crashed a *different*,
/// still-in-flight fetch for the operation that same session had just
/// created, with `RESOURCE_DOES_NOT_EXIST: Command ... does not exist` --
/// closing a Thrift session invalidates every operation still open under
/// it, including its own, regardless of whether that operation has
/// literally finished fetching yet. Fixed by deferring the throwaway
/// session's close (and the operation's own close) to
/// `drive_thrift_fetch_loop`'s single cleanup point, which always runs
/// after the fetch loop is fully done, on every exit path -- not right
/// after submission. A *pooled* session is still checked in eagerly, right
/// after `ExecuteStatement` returns, same as before this fix: that only
/// returns it to the idle pool for potential reuse by a *different*
/// operation, it does not close the session, and a HiveServer2-compatible
/// session is designed to support multiple independently-addressed
/// operations at once -- the crash above was specifically about *closing*
/// a session out from under one of its own still-open operations, not
/// about a session merely being idle/shared.
pub async fn execute_lazy_thrift(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ResultStream, ApiError> {
    let ThriftSubmitResult {
        statement_id,
        schema_bytes,
        rx,
        operation,
        stats,
        warehouse_wait_s,
        submit_to_ready_s,
    } = submit_thrift_and_start_fetch(client.clone(), statement, catalog, schema, parameters).await?;
    // Populate `schema`/`columns` up front from Thrift's own result-set
    // metadata, the same way SEA's `manifest.schema.columns` already does --
    // otherwise both stay empty until a batch is actually decoded, which for
    // a zero-row result never happens, leaving `cursor.description == []`.
    let schema = schema_bytes.as_ref().and_then(decode_ipc_schema);
    let columns = schema
        .as_ref()
        .map(|s| {
            s.fields()
                .iter()
                .map(|f| ColumnDescription {
                    name: f.name().clone(),
                    type_name: Some(f.data_type().to_string()),
                    type_precision: None,
                    type_scale: None,
                    type_text: None,
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(ResultStream {
        statement_id: statement_id.clone(),
        num_chunks: 0,
        schema,
        columns,
        reorder: ReorderBuffer::new(rx),
        pending: VecDeque::new(),
        pending_rows: 0,
        exhausted: false,
        poisoned: false,
        cancel_handle: CancelHandle::Thrift { operation },
        stats,
        reporter: StatsReporter {
            client,
            statement_id,
            protocol: "thrift",
            warehouse_wait_s,
            submit_to_ready_s,
            fetch_s: 0.0,
            fetch_started_at: None,
            // Thrift doesn't know its chunk count upfront -- see
            // `StatsReporter::static_num_chunks`'s own doc comment.
            static_num_chunks: None,
            reported: false,
        },
    })
}

/// `submit_thrift_and_start_fetch`'s return shape -- a named struct instead
/// of a positional tuple (found in review: the original 7-element tuple
/// needed `#[allow(clippy::type_complexity)]` and forced every caller to
/// destructure-then-reassemble it positionally, risking the two same-typed
/// `f64` timing fields silently swapping if the tuple were ever reordered).
pub(crate) struct ThriftSubmitResult {
    pub(crate) statement_id: String,
    pub(crate) schema_bytes: Option<bytes::Bytes>,
    pub(crate) rx: mpsc::Receiver<Result<ChunkItem, ApiError>>,
    pub(crate) operation: thrift::OperationHandle,
    pub(crate) stats: Arc<QueryStatsAccumulator>,
    pub(crate) warehouse_wait_s: f64,
    pub(crate) submit_to_ready_s: f64,
}

/// The submit/session/poll/fetch-start tail of `execute_lazy_thrift`,
/// factored out so `execute_ndjson_stream` can drive the exact same Thrift
/// path instead of unconditionally submitting via SEA. Session handling and
/// the cleanup-ordering invariant are unchanged by the split -- see
/// `execute_lazy_thrift`'s own doc comment for both. Also returns
/// everything observability needs: a clone of the `OperationHandle` (for
/// `CancelHandle::Thrift`, taken *before* `ready` moves into the background
/// fetch loop below -- once there, it's only reachable on the far side of
/// that loop's own cleanup, too late for `heartbeat.rs` to ever use it),
/// the per-query `QueryStatsAccumulator`, and the two timings this function
/// itself is positioned to measure directly (`ensure_warehouse_running`
/// (`client.rs`) is already this function's own first call, unlike SEA's
/// `submit_and_poll` (`client/sea.rs`), which buries it several calls
/// deeper).
pub(crate) async fn submit_thrift_and_start_fetch(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ThriftSubmitResult, ApiError> {
    let stats = Arc::new(QueryStatsAccumulator::default());
    let warehouse_t0 = Instant::now();
    // Explicit match, not a bare `?` -- found in review: a stopped/
    // unreachable warehouse used to propagate correctly to the caller but
    // never reached `report_submit_error`, so `on_event` silently never
    // fired for that query at all, contradicting the "once per query, at
    // completion, including outcome=error" contract.
    if let Err(e) = client.ensure_warehouse_running().await {
        report_submit_error(&client, "thrift", 0.0, &stats);
        return Err(e);
    }
    stats.add_warehouse_wait_s(warehouse_t0.elapsed().as_secs_f64());

    let submit_t0 = Instant::now();
    let pooled = client.thrift_checkout_session(catalog, schema).await;
    let (session, from_pool) = match pooled {
        Some(s) => (s, true),
        None => match client.thrift_open_session_raw(catalog, schema).await {
            Ok(s) => (s, false),
            Err(e) => {
                report_submit_error(&client, "thrift", submit_t0.elapsed().as_secs_f64(), &stats);
                return Err(e);
            }
        },
    };

    let ready = submit_and_await_thrift_statement(&client, &session, statement, parameters.as_ref(), &stats).await;
    let submit_to_ready_s = submit_t0.elapsed().as_secs_f64();

    // Exactly one of these two arms ever touches `session` -- a pooled
    // session is checked in right away (safe: that only makes it available
    // for a *different* operation, see `execute_lazy_thrift`'s own doc
    // comment); a throwaway session that failed to even reach a terminal
    // state is closed right here, since it'll never be handed to
    // `drive_thrift_fetch_loop`; a throwaway session that succeeded is
    // carried forward for that function to close once the fetch loop is
    // actually done with it.
    let throwaway_session = if from_pool {
        client.thrift_checkin_session(catalog, schema, session, ready.is_ok());
        None
    } else {
        match &ready {
            Ok(_) => Some(session),
            Err(_) => {
                client.thrift_close_session_raw(&session).await;
                None
            }
        }
    };
    let ready = match ready {
        Ok(r) => r,
        Err(e) => {
            report_submit_error(&client, "thrift", submit_to_ready_s, &stats);
            return Err(e);
        }
    };

    let statement_id = hex_encode(&ready.operation.operation_id.guid);
    // Cheap: `Bytes::clone()`/`OperationHandle::clone()` are refcount bumps/
    // small struct copies, not deep copies. `ready` itself is about to move
    // into the loop below; `execute_lazy_thrift` needs these clones to
    // populate `description`/`schema()`/`CancelHandle` up front.
    let schema_bytes = ready.schema_bytes.clone();
    let operation = ready.operation.clone();

    let concurrency = client.chunk_fetch_concurrency.max(1);
    let (tx, rx) = mpsc::channel::<Result<ChunkItem, ApiError>>(concurrency);
    tokio::spawn(drive_thrift_fetch_loop(
        client,
        ready,
        throwaway_session,
        tx,
        stats.clone(),
    ));

    let warehouse_wait_s = stats.warehouse_wait_s();
    Ok(ThriftSubmitResult {
        statement_id,
        schema_bytes,
        rx,
        operation,
        stats,
        warehouse_wait_s,
        submit_to_ready_s,
    })
}

/// Decodes just the Arrow-IPC *schema message* out of `blob` -- no batches
/// required, unlike `decode_chunk`. Used by `execute_lazy_thrift` to give
/// Thrift a `description`/`schema()` before any row has been fetched.
///
/// `blob` here is *exactly* one bare schema message and nothing else --
/// unlike every other caller of `StreamDecoder` (`pipeline/reorder.rs`'s
/// `decode_chunk`), which always feeds it a schema followed by a batch or
/// an explicit end-of-stream marker. Found in code review and confirmed directly against
/// `arrow_ipc::reader::StreamDecoder`'s own source: its internal state
/// machine only finalizes a message (`self.schema = Some(..)`) on the
/// buffer-emptiness check for the *next* message, so a buffer that ends
/// exactly at this message's last byte leaves `decode()` returning `Ok(None)`
/// with the schema silently never set, not an error -- appending the same
/// 8-byte end-of-stream marker `StreamWriter::finish()` itself writes
/// (continuation marker `0xFFFFFFFF` + zero length) gives it the trailing
/// byte it needs to notice the schema message is actually complete.
fn decode_ipc_schema(blob: &Bytes) -> Option<SchemaRef> {
    let mut bytes = Vec::with_capacity(blob.len() + 8);
    bytes.extend_from_slice(blob);
    bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
    bytes.extend_from_slice(&0i32.to_le_bytes());
    let mut buffer = ArrowBuffer::from(Bytes::from(bytes));
    let mut decoder = StreamDecoder::new();
    while !buffer.is_empty() {
        if decoder.decode(&mut buffer).is_err() {
            return None;
        }
    }
    decoder.schema()
}

/// Drives the sequential `FetchResults(orientation: FETCH_NEXT)` loop for
/// one Thrift statement, translating each response's `arrowBatches`
/// (decoded inline -- see below) or `resultLinks` (cloud-fetch, downloaded
/// concurrently) into `ChunkItem`s fed to the same `ReorderBuffer`/
/// `decode_chunk` machinery every other backend uses. Chunk indices are
/// assigned sequentially in the exact order items are produced here, which
/// is also true row order (Thrift's `FetchResults` calls are strictly
/// sequential, unlike SEA's independently-resolved chunk set) -- so
/// `ReorderBuffer` never actually has to reorder anything on this path, but
/// reusing it costs nothing and keeps `ResultStream` backend-agnostic.
/// Owns the two cleanup steps every exit path of `run_thrift_fetch_loop`
/// needs (closing the operation, and closing a throwaway session if this
/// call didn't use a pooled one) -- a bare `return` inside that loop must
/// never bypass either, see `execute_lazy_thrift`'s own doc comment for why
/// the session-close specifically matters.
async fn drive_thrift_fetch_loop(
    client: Arc<DbClient>,
    ready: ThriftStatementReady,
    throwaway_session: Option<thrift::SessionHandle>,
    tx: mpsc::Sender<Result<ChunkItem, ApiError>>,
    stats: Arc<QueryStatsAccumulator>,
) {
    let ThriftStatementReady {
        operation,
        schema_bytes,
        compression,
        initial_rowset,
        already_closed,
    } = ready;
    run_thrift_fetch_loop(
        &client,
        &operation,
        schema_bytes,
        compression,
        initial_rowset,
        &tx,
        &stats,
    )
    .await;
    // Closes the channel *before* the two cleanup RPCs below, not after this
    // whole function returns -- `tx` is otherwise dropped at the end of this
    // scope, which is on the far side of a `CloseOperation` round trip.
    //
    // This is the fix for a real, measured problem: `ReorderBuffer::next`'s
    // final `rx.recv().await` -- the one that returns `None` to tell
    // `ResultStream::fetch_at_least` the result is fully drained -- can only
    // return once every `Sender` clone is gone, and this one outlived the
    // fetch loop it belongs to. So *every* Thrift query blocked its caller
    // for the full duration of `thrift_close_operation_best_effort`'s
    // network round trip after the last chunk had already been downloaded,
    // decompressed and decoded. Traced against a real warehouse
    // (`benchmark_table`, `LIMIT 10000`, one reused connection, warm runs): the
    // time `fetch_at_least` spent in that last `recv()` matched the
    // `CloseOperation` RPC's own duration to within 0.1ms on every single
    // run (116-183ms, 8/8 runs), and an interleaved A/B over 16 warm runs
    // moved the median end-to-end query from 1043.7ms to 853.8ms. The
    // cleanup RPCs still run, and still run to completion -- they just run
    // on this detached task after the caller already has its rows, which is
    // what "best effort" already meant.
    //
    // Safe because nothing below sends: `run_thrift_fetch_loop` joins every
    // download worker (each of which held its own `tx.clone()`) before
    // returning, and the cleanup calls after this point are both
    // fire-and-forget with no path back to the consumer.
    drop(tx);
    if !already_closed {
        client.thrift_close_operation_best_effort(&operation).await;
    }
    if let Some(session) = throwaway_session {
        client.thrift_close_session_raw(&session).await;
    }
}

/// One `resultLinks` entry plus the `chunk_index` it was assigned at
/// discovery time -- see `run_thrift_fetch_loop`'s own doc comment for why
/// that ordering matters.
struct ThriftLinkWork {
    chunk_index: i64,
    row_count: i64,
    file_link: String,
    split_limit: usize,
}

/// Downloads one `resultLinks` entry -- decode and truncation both happen
/// later, in `decode_chunk_item`, not here, since decoding just to *count*
/// rows and then possibly re-encoding was pure wasted work for the common
/// case (a file that already matches its declared count, i.e. every chunk
/// except the last one of a `LIMIT`-bounded query): every downloaded chunk
/// used to get a full Arrow-IPC decode here, discarded, and then a second,
/// real decode downstream in the consumer (`ResultStream::fetch_at_least`
/// et al.) -- found in review, a pure duplicate-work removal with no
/// trade-off, not a correctness fix. `ChunkItem::truncate_to` carries the
/// declared bound forward instead, so truncation (when actually needed)
/// happens exactly once, on the one decode that was always going to happen
/// anyway.
async fn fetch_thrift_link(
    client: &Arc<DbClient>,
    work: ThriftLinkWork,
    compressed: bool,
    stats: &Arc<QueryStatsAccumulator>,
) -> Result<ChunkItem, ApiError> {
    let blob = client
        .fetch_link_bytes_budgeted(&work.file_link, compressed, work.split_limit, stats)
        .await?;
    Ok(ChunkItem {
        blob,
        row_count: Some(work.row_count),
        chunk_index: work.chunk_index,
        truncate_to: Some(work.row_count),
    })
}

/// Drives the sequential `FetchResults(orientation: FETCH_NEXT)` loop
/// (Thrift's own cursor semantics require this side to stay strictly
/// sequential -- concurrent `FetchResults` calls on one operation aren't a
/// thing this protocol supports) while a separate, bounded worker pool
/// downloads previously-discovered `resultLinks` concurrently, across
/// *every* batch discovered so far, not just the current one.
///
/// This is the fix for a real, measured problem: the first version of this
/// function fully awaited one batch's downloads before ever asking for the
/// next batch's links, capping effective download concurrency at "however
/// many links one `FetchResults` response happens to contain" instead of
/// `chunk_fetch_concurrency` -- confirmed against a real workspace
/// (`benchmark_table`, `LIMIT 500000`, 4 warm runs each): SEA (which knows its
/// whole chunk manifest upfront and fans out `chunk_fetch_concurrency`
/// downloads across the *entire* result immediately, see
/// `client/sea.rs`'s `fetch_chunks_with_backpressure`) averaged 11.8s; this
/// batch-serialized Thrift loop averaged 20.5s for the identical query, a
/// consistent ~1.7x slower across every run, not noise.
///
/// The producer (this function's own `FetchResults` loop) pushes each
/// discovered link into a bounded `mpsc` channel instead of downloading it
/// directly -- `Sender::send` naturally backpressures once the buffer is
/// full, so the producer can still race ahead discovering more batches
/// (a cheap metadata-only round trip) while a fixed pool of
/// `chunk_fetch_concurrency` workers pulls from the *same* channel (shared
/// via `Arc<tokio::sync::Mutex<Receiver>>`, the standard way to turn one
/// `mpsc::Receiver` into an effective multi-consumer queue) and downloads
/// concurrently across however many batches have been discovered so far.
/// Downloads completing out of order (across or within a batch) is exactly
/// what `ReorderBuffer` already exists to handle -- `chunk_index` is
/// assigned once, deterministically, at discovery time in the producer,
/// never at download-completion time.
async fn run_thrift_fetch_loop(
    client: &Arc<DbClient>,
    operation: &thrift::OperationHandle,
    mut schema_bytes: Option<bytes::Bytes>,
    compression: Option<bool>,
    initial_rowset: Option<(thrift::RowSet, bool)>,
    tx: &mpsc::Sender<Result<ChunkItem, ApiError>>,
    stats: &Arc<QueryStatsAccumulator>,
) {
    let mut metadata_confirmed = compression.is_some();
    let mut lz4_compressed = compression.unwrap_or_else(|| client.compress_results());
    let concurrency = client.chunk_fetch_concurrency.max(1);
    let (link_tx, link_rx) = mpsc::channel::<ThriftLinkWork>(concurrency);
    let link_rx = Arc::new(tokio::sync::Mutex::new(link_rx));

    // Shared, not captured by value at spawn time: found in review (this
    // session's own concurrency restructuring introduced it) -- workers are
    // all spawned before the discovery loop below has necessarily seen an
    // authoritative `TGetResultSetMetadataResp` yet (that only happens once
    // `already_finished` was false in `submit_and_await_thrift_statement`,
    // i.e. the query didn't finish inside its own `ExecuteStatement` RPC
    // window), so a plain `bool` captured once at spawn time would freeze
    // every worker on `client.compress_results()`'s initial *guess* even
    // after the loop below learns the real value from the first
    // `FetchResults` response's metadata -- silently corrupting/failing
    // decompression for the whole statement if the guess were ever wrong.
    // Same silent-truncation failure shape as the multi-frame LZ4 bug
    // already documented above. Workers now read this fresh per work item
    // instead of once at spawn.
    let compressed_flag = Arc::new(std::sync::atomic::AtomicBool::new(lz4_compressed));

    let mut worker_handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let link_rx = link_rx.clone();
        let out_tx = tx.clone();
        let compressed_flag = compressed_flag.clone();
        let worker_stats = stats.clone();
        worker_handles.push(tokio::spawn(async move {
            loop {
                let work = { link_rx.lock().await.recv().await };
                let Some(work) = work else { return };
                let compressed = compressed_flag.load(std::sync::atomic::Ordering::Relaxed);
                let result = fetch_thrift_link(&client, work, compressed, &worker_stats).await;
                if out_tx.send(result).await.is_err() {
                    return;
                }
            }
        }));
    }

    let mut chunk_index: i64 = 0;
    let mut pending = initial_rowset;
    // Found in an independent review pass of the `compressed_flag` fix
    // above: sharing the flag closes the *captured-once-at-spawn* race, but
    // leaves a narrower one -- `resultSetMetadata` is its own optional field
    // on `TFetchResultsResp`, independent of `results.resultLinks`, so
    // nothing guarantees a response carrying links also carries the
    // metadata that confirms their real compression. If an earlier response
    // has links but no metadata, its links would otherwise be queued (and
    // possibly downloaded) against `client.compress_results()`'s initial
    // *guess* before a later response ever confirms the real value.
    // Buffering here, instead of queueing immediately, means no link is
    // handed to a download worker before compression is authoritatively
    // known at least once -- if metadata never arrives at all across the
    // whole statement (legal, if unusual), the buffer is flushed at loop end
    // using the request's own `canDecompressLZ4Result` value, same fallback
    // `lz4_compressed` already starts from.
    // Direct results may already have confirmed it during submission. In
    // that case downloads can overlap the very first FetchResults RPC too.
    let mut pending_until_confirmed: Vec<ThriftLinkWork> = Vec::new();
    loop {
        let (row_set, has_more) = if let Some(v) = pending.take() {
            v
        } else {
            match client.thrift_fetch_results_raw(operation, stats).await {
                Ok(fr) => {
                    if let Some(e) = fr.status.error() {
                        let _ = tx
                            .send(Err(ApiError::permanent(format!("Thrift FetchResults failed: {e}"))))
                            .await;
                        break;
                    }
                    if let Some(meta) = &fr.result_set_metadata {
                        if schema_bytes.is_none() {
                            schema_bytes = meta.arrow_schema.clone();
                        }
                        lz4_compressed = meta.lz4_compressed;
                        compressed_flag.store(lz4_compressed, std::sync::atomic::Ordering::Relaxed);
                        metadata_confirmed = true;
                    }
                    (fr.results.unwrap_or_default(), fr.has_more_rows)
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        };

        if !row_set.arrow_batches.is_empty() {
            let schema_for_blob = schema_bytes.clone();
            let batches = row_set.arrow_batches;
            let decode_result =
                tokio::task::spawn_blocking(move || build_inline_blob(schema_for_blob, batches, lz4_compressed))
                    .await
                    .map_err(join_error);
            match decode_result {
                Ok(Ok((blob, row_count))) => {
                    let item = ChunkItem {
                        blob,
                        row_count: Some(row_count),
                        chunk_index,
                        // Same "server rarely prepares the exact number of
                        // rows requested" truncation `decode_chunk_item`
                        // already does for `resultLinks` chunks (see its
                        // own doc comment) -- inline `arrowBatches` are
                        // subject to the identical server-side row
                        // generation and can just as easily overshoot their
                        // own declared `rowCount`. Confirmed missing, not
                        // hypothetical: `SELECT id, name FROM benchmark_table
                        // WHERE is_one_off = true LIMIT 5000` came back
                        // with 5037 rows end to end via this inline path
                        // before this fix, while the identical query via
                        // SEA -- and via databricks-sql-connector on either
                        // protocol -- came back with exactly 5000.
                        truncate_to: Some(row_count),
                    };
                    chunk_index += 1;
                    stats.chunks_seen.fetch_add(1, Ordering::Relaxed);
                    if tx.send(Ok(item)).await.is_err() {
                        break;
                    }
                }
                Ok(Err(e)) | Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }

        // Share the budget across all links already known in this batch.
        // Otherwise the first workers can claim eight slots each while
        // the remaining files wait without even starting a request.
        let link_count = row_set.result_links.len().max(1);
        for (link_position, link) in row_set.result_links.into_iter().enumerate() {
            // Distribute the remainder too: 35 files under a 64-slot budget
            // get 29 two-slot shares and six one-slot shares, rather than
            // leaving 29 slots unused. Every file still gets at least one;
            // the semaphore queues files when there are more than slots.
            let split_limit = (concurrency / link_count + usize::from(link_position < concurrency % link_count)).max(1);
            let idx = chunk_index;
            chunk_index += 1;
            stats.chunks_seen.fetch_add(1, Ordering::Relaxed);
            let work = ThriftLinkWork {
                chunk_index: idx,
                row_count: link.row_count,
                file_link: link.file_link,
                split_limit,
            };
            if metadata_confirmed {
                // Backpressure, not an error path: a full buffer just means
                // every worker is currently busy, so this await is exactly
                // the same "peak buffered stays at ~concurrency" trade-off
                // `fetch_chunks_with_backpressure`'s own doc comment
                // describes. The receiving end only ever closes once every
                // worker returns, which only happens after this sender side
                // is dropped -- so a closed-channel send here would mean
                // every worker already exited (e.g. all panicked), not a
                // normal condition; still handled without panicking
                // regardless.
                if link_tx.send(work).await.is_err() {
                    break;
                }
            } else {
                // See this function's own comment above `metadata_confirmed`.
                pending_until_confirmed.push(work);
            }
        }

        if metadata_confirmed {
            for work in pending_until_confirmed.drain(..) {
                if link_tx.send(work).await.is_err() {
                    break;
                }
            }
        }

        if !has_more {
            break;
        }
    }

    // Final flush for the never-confirmed case -- see `metadata_confirmed`'s
    // own comment above.
    for work in pending_until_confirmed.drain(..) {
        if link_tx.send(work).await.is_err() {
            break;
        }
    }

    drop(link_tx); // lets every worker's `recv()` return `None` once the queue drains
    if let Some(e) = first_worker_panic(worker_handles).await {
        let _ = tx.send(Err(e)).await;
    }
}

/// A panicked download worker has already pulled its `ThriftLinkWork` off
/// the queue and sends nothing on `out_tx` -- silently discarding that (a
/// bare `let _ = handle.await`) would mean the channel closing normally
/// looks to the consumer exactly like a complete, successful result instead
/// of a truncated one. Same failure mode `client/sea.rs`'s `join_first_error`
/// exists to prevent on the SEA side; this is the Thrift-side analogue so
/// both protocols have the same defense, not because a real panic was ever
/// observed here (these workers return `()`, not a `Result`, since a
/// download error is sent through `out_tx` from inside the loop rather than
/// returned from the task -- so unlike `join_first_error`, only a genuine
/// panic is possible here, never an `Ok(Err(..))`).
async fn first_worker_panic(handles: Vec<tokio::task::JoinHandle<()>>) -> Option<ApiError> {
    let mut first = None;
    for handle in handles {
        if let Err(join_err) = handle.await {
            first.get_or_insert_with(|| join_error(join_err));
        }
    }
    first
}

/// Concatenates `schema_bytes` (the stream's Arrow-IPC schema message,
/// captured once from whichever response first carried
/// `TGetResultSetMetadataResp.arrowSchema`) with every arrow batch's
/// (LZ4-unwrapped, if `lz4_compressed`) bytes -- producing exactly the same
/// self-contained "schema message + N record-batch messages" shape
/// `decode_chunk` already knows how to decode, confirmed against
/// `databricks-sql-connector`'s own `convert_arrow_based_set_to_arrow_table`
/// (which does the identical concatenation before calling
/// `pyarrow.ipc.open_stream`). Runs on `spawn_blocking` -- LZ4 decompression
/// is real CPU work, same reasoning as `client/download.rs`'s `fetch_link_bytes`.
fn build_inline_blob(
    schema_bytes: Option<bytes::Bytes>,
    batches: Vec<thrift::ArrowBatch>,
    lz4_compressed: bool,
) -> Result<(bytes::Bytes, i64), ApiError> {
    // Pre-size the output buffer to avoid repeated allocations. Schema is
    // typically a few KB; batches are typically 10s-100s of KB uncompressed.
    // For compressed data, use the same * 4 heuristic as decompress_lz4_frame
    // (Arrow IPC data typically compresses several-fold); for uncompressed,
    // the exact sizes are known.
    let schema_size = schema_bytes.as_ref().map(|s| s.len()).unwrap_or(0);
    let batches_size: usize = batches
        .iter()
        .map(|b| {
            if lz4_compressed {
                b.batch.len() * 4
            } else {
                b.batch.len()
            }
        })
        .sum();
    let mut out = Vec::with_capacity(schema_size + batches_size);

    if let Some(s) = &schema_bytes {
        out.extend_from_slice(s);
    }
    let mut row_count = 0i64;
    for b in batches {
        row_count += b.row_count;
        if lz4_compressed {
            // Decompress directly into the output buffer using FrameDecoder,
            // avoiding a separate intermediate allocation and copy. The
            // FrameDecoder's read_to_end appends to the existing buffer.
            use std::io::Read;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(&b.batch[..]);
            while !decoder.get_ref().is_empty() {
                decoder
                    .read_to_end(&mut out)
                    .map_err(|e| ApiError::permanent(format!("LZ4 frame decompress failed: {e}")))?;
            }
        } else {
            out.extend_from_slice(&b.batch);
        }
    }
    Ok((bytes::Bytes::from(out), row_count))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_worker() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async {})
    }

    fn panicking_worker() -> tokio::task::JoinHandle<()> {
        tokio::spawn(async { panic!("simulated download-worker panic") })
    }

    #[tokio::test]
    async fn first_worker_panic_is_none_when_all_workers_finish_cleanly() {
        let handles = vec![ok_worker(), ok_worker(), ok_worker()];
        assert!(first_worker_panic(handles).await.is_none());
    }

    #[tokio::test]
    async fn first_worker_panic_surfaces_a_real_panic() {
        let handles = vec![ok_worker(), panicking_worker(), ok_worker()];
        let err = first_worker_panic(handles).await;
        assert!(
            err.is_some(),
            "a panicked worker must surface as an error, not be silently dropped"
        );
    }
}
