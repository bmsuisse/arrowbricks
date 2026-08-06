//! Reorder buffer (port of `_ResultSet._pull_one_chunk_table`) + Arrow-IPC
//! decode of the reordered chunk stream via arrow-rs.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::buffer::Buffer as ArrowBuffer;
use arrow::datatypes::{DataType, Float32Type, Float64Type, SchemaRef};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::client::{
    ApiError, ChunkItem, ColumnDescription, DbClient, InlineOrExternal, StatementSubmitResult, join_error,
};
use crate::thrift;

/// Same dict-of-lists-keyed-by-index shape as `_ResultSet._pending`: a
/// `chunk_index` can carry more than one blob (multiple `external_links` per
/// chunk), so a plain map from index to single item would silently drop the
/// first blob when a second arrives for the same index.
struct ReorderBuffer {
    rx: mpsc::Receiver<Result<ChunkItem, ApiError>>,
    pending: HashMap<i64, VecDeque<ChunkItem>>,
    next_idx: i64,
    exhausted: bool,
}

impl ReorderBuffer {
    fn new(rx: mpsc::Receiver<Result<ChunkItem, ApiError>>) -> Self {
        Self {
            rx,
            pending: HashMap::new(),
            next_idx: 0,
            exhausted: false,
        }
    }

    fn pop_pending(&mut self, idx: i64) -> ChunkItem {
        let items = self
            .pending
            .get_mut(&idx)
            .expect("pop_pending called with missing index");
        let item = items.pop_front().expect("pop_pending called on empty deque");
        if items.is_empty() {
            self.pending.remove(&idx);
        }
        item
    }

    /// Prefers `next_idx` from `pending` first (an earlier call may have
    /// pulled several out-of-order chunks off the channel already). Once the
    /// source is exhausted, a genuine gap (an index that never arrives)
    /// drains the lowest remaining index instead of stranding everything
    /// buffered past it -- costs row order past that point, never rows.
    async fn next(&mut self) -> Result<Option<ChunkItem>, ApiError> {
        loop {
            if self.pending.contains_key(&self.next_idx) {
                let item = self.pop_pending(self.next_idx);
                if !self.pending.contains_key(&self.next_idx) {
                    self.next_idx += 1;
                }
                return Ok(Some(item));
            }
            if self.exhausted {
                if let Some(&min_idx) = self.pending.keys().min() {
                    return Ok(Some(self.pop_pending(min_idx)));
                }
                return Ok(None);
            }
            match self.rx.recv().await {
                Some(Ok(item)) => {
                    self.pending.entry(item.chunk_index).or_default().push_back(item);
                }
                Some(Err(e)) => return Err(e),
                None => self.exhausted = true,
            }
        }
    }
}

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

/// Decodes a chunk's raw Arrow-IPC stream bytes into batches. Uses
/// `StreamDecoder`'s push-based interface fed by an `arrow::buffer::Buffer`
/// built directly from `blob` (`Buffer::from(bytes::Bytes)`, confirmed
/// zero-copy in arrow-buffer's own source -- `bytes.rs`'s
/// `impl From<bytes::Bytes> for Bytes` stores the original `bytes::Bytes` via
/// `Deallocation::Custom`, no memcpy) instead of the higher-level
/// `StreamReader` (reads via `std::io::Read` into freshly allocated buffers,
/// copying every column's data out of `blob` on every decode -- what this
/// used before). For properly aligned IPC data (the normal case -- Databricks
/// writes it, not this crate), decoded batches now slice directly into the
/// same allocation `blob` already held since the network fetch, cutting out
/// a second full copy of every chunk's bytes; `require_alignment` stays at
/// its default `false`, so a misaligned *fixed-width* buffer still falls back
/// to a copy automatically rather than erroring (arrow-ipc's own documented
/// behavior) -- variable-width values, null bitmaps, and nested/dictionary
/// children all stay zero-copy regardless. One exception this doesn't cover:
/// if a `RecordBatch` message declared IPC *buffer*-level compression (a
/// different, unrelated feature from this crate's own cloud-fetch
/// `result_compression` unwrap in `client.rs`, which already ran before this
/// function ever sees the bytes), `arrow-ipc`'s own reader always
/// decompresses into fresh buffers there -- not something Databricks has
/// been observed to use in this format, but not something this crate
/// controls either.
///
/// A zero-length `blob` is rejected explicitly rather than handed to
/// `StreamDecoder`: found in code review that an empty buffer makes the
/// `while` loop below a no-op and `decoder.finish()` sees a still-pristine
/// decoder state, which its own `Ok(())` arm treats as a *clean, empty*
/// stream -- silently returning zero batches with no error at all, the same
/// silent-truncation failure mode as the real multi-frame LZ4 bug this crate
/// already shipped once (see the `result_compression` invariant above). The
/// old `StreamReader`-based version failed loudly on this input instead
/// ("Expected schema message, found empty stream"); this restores that.
fn decode_chunk(blob: &Bytes) -> Result<Vec<RecordBatch>, ApiError> {
    if blob.is_empty() {
        return Err(ApiError {
            message: "empty Arrow IPC chunk: expected at least a schema message".to_string(),
            transient: false,
        });
    }
    let mut buffer = ArrowBuffer::from(blob.clone());
    let mut decoder = StreamDecoder::new();
    let mut batches = Vec::new();
    while !buffer.is_empty() {
        match decoder.decode(&mut buffer) {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => {}
            Err(e) => {
                return Err(ApiError {
                    message: format!("Arrow IPC decode error: {e}"),
                    transient: false,
                });
            }
        }
    }
    decoder.finish().map_err(|e| ApiError {
        message: format!("bad Arrow IPC stream: {e}"),
        transient: false,
    })?;
    Ok(batches)
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
    reorder: ReorderBuffer,
    pending: VecDeque<RecordBatch>,
    pending_rows: usize,
    exhausted: bool,
    /// Set (via `PoisonOnDrop`) if a `fetch_at_least` call ever exits
    /// without reaching its own end -- see that guard's doc comment.
    poisoned: bool,
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
struct PoisonOnDrop<'a> {
    poisoned: &'a mut bool,
    armed: bool,
}

impl<'a> PoisonOnDrop<'a> {
    fn new(poisoned: &'a mut bool) -> Self {
        Self { poisoned, armed: true }
    }

    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.poisoned = true;
        }
    }
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
            });
        }
        let mut guard = PoisonOnDrop::new(&mut self.poisoned);
        while self.pending_rows < want_rows && !self.exhausted {
            let mut decode_handles = Vec::new();
            let mut estimated_new_rows = 0usize;
            while self.pending_rows + estimated_new_rows < want_rows
                && !self.exhausted
                && decode_handles.len() < MAX_CHUNKS_PER_FETCH_BATCH
            {
                match self.reorder.next().await? {
                    Some(item) => {
                        estimated_new_rows += item.row_count.unwrap_or(0).max(0) as usize;
                        let truncate_to = item.truncate_to;
                        decode_handles.push(tokio::task::spawn_blocking(move || {
                            decode_chunk_item(&item.blob, truncate_to)
                        }));
                    }
                    None => self.exhausted = true,
                }
            }
            for handle in decode_handles {
                for batch in handle.await.map_err(join_error)?? {
                    if self.schema.is_none() {
                        self.schema = Some(batch.schema());
                    }
                    self.pending_rows += batch.num_rows();
                    self.pending.push_back(batch);
                }
            }
        }
        guard.defuse();
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
    let submitted = client
        .execute_arrow_statement(statement, catalog, schema, parameters)
        .await?;
    Ok(result_stream_from_submitted(client, submitted))
}

fn result_stream_from_submitted(client: Arc<DbClient>, submitted: StatementSubmitResult) -> ResultStream {
    let num_chunks = submitted.chunk_metas.len();
    let rx = client.fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
    );
    ResultStream {
        statement_id: submitted.statement_id,
        num_chunks,
        schema: None,
        columns: submitted.columns,
        reorder: ReorderBuffer::new(rx),
        pending: VecDeque::new(),
        pending_rows: 0,
        exhausted: false,
        poisoned: false,
    }
}

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

/// Submit -> (maybe poll) -> start background chunk fetching for the
/// `protocol="thrift"` backend -- see `client::Protocol::Thrift`'s own doc
/// comment for the wire-format background and why this is faster than SEA
/// for small queries. Produces the exact same `ResultStream` shape as
/// `execute_lazy`/`execute_lazy_prefer_inline` (same `fetchmany_arrow`/
/// `fetchall_arrow`/`schema` contract, same `ReorderBuffer`/`decode_chunk`
/// underneath), so `PyResultSet` and everything above it needs zero
/// Thrift-specific handling.
///
/// Session handling mirrors the SEA session pool (`client::SessionPool`)
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
/// What's known once a Thrift statement has reached a terminal state and is
/// ready to hand off to `drive_thrift_fetch_loop` -- a small bundle so
/// `execute_lazy_thrift`'s own error paths (see its doc comment) have one
/// single point to close a throwaway session on, instead of repeating that
/// cleanup at every `return Err(...)`.
struct ThriftStatementReady {
    operation: thrift::OperationHandle,
    schema_bytes: Option<bytes::Bytes>,
    lz4_compressed: bool,
    initial_rowset: Option<(thrift::RowSet, bool)>,
    already_closed: bool,
}

async fn submit_and_await_thrift_statement(
    client: &Arc<DbClient>,
    session: &thrift::SessionHandle,
    statement: &str,
    parameters: Option<&Value>,
) -> Result<ThriftStatementReady, ApiError> {
    let resp = client
        .thrift_execute_statement_raw(session, statement, parameters)
        .await?;

    let operation = resp
        .operation_handle
        .ok_or_else(|| ApiError::permanent("Thrift ExecuteStatement succeeded with no operationHandle".to_string()))?;

    let mut schema_bytes: Option<bytes::Bytes> = None;
    // Fallback guess before any `TGetResultSetMetadataResp` has actually
    // confirmed compression -- overwritten the moment one arrives, same
    // "trust the response's stated value over what we asked for" pattern as
    // `client.rs`'s own `execute_statement`.
    let mut lz4_compressed = client.compress_results();
    let mut initial_rowset: Option<(thrift::RowSet, bool)> = None;
    let mut already_finished = false;
    let mut already_closed = false;

    if let Some(direct) = resp.direct_results {
        already_closed = direct.already_closed;
        if let Some(meta) = &direct.result_set_metadata {
            schema_bytes = meta.arrow_schema.clone();
            lz4_compressed = meta.lz4_compressed;
        }
        if let Some(op_status) = &direct.operation_status {
            if let Some(e) = op_status.terminal_error() {
                return Err(ApiError::permanent(format!("Thrift statement failed: {e}")));
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
                lz4_compressed = meta.lz4_compressed;
            }
            let has_more = fr.has_more_rows;
            if let Some(rs) = fr.results {
                initial_rowset = Some((rs, has_more));
            }
        }
    }

    if !already_finished {
        loop {
            let status = client.thrift_get_operation_status_raw(&operation).await?;
            if let Some(e) = status.terminal_error() {
                return Err(ApiError::permanent(format!("Thrift statement failed: {e}")));
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
        lz4_compressed,
        initial_rowset,
        already_closed,
    })
}

/// Submit -> (maybe poll) -> start background chunk fetching for the
/// `protocol="thrift"` backend. See `client::Protocol::Thrift`'s own doc
/// comment for the wire-format background.
///
/// **A session checked out for this call (whether pooled or a throwaway,
/// see `client::ThriftSessionPool`'s doc comment) must stay open until the
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
    let pooled = client.thrift_checkout_session(catalog, schema).await;
    let (session, from_pool) = match pooled {
        Some(s) => (s, true),
        None => (client.thrift_open_session_raw(catalog, schema).await?, false),
    };

    let ready = submit_and_await_thrift_statement(&client, &session, statement, parameters.as_ref()).await;

    // Exactly one of these two arms ever touches `session` -- a pooled
    // session is checked in right away (safe: that only makes it available
    // for a *different* operation, see this function's own doc comment);
    // a throwaway session that failed to even reach a terminal state is
    // closed right here, since it'll never be handed to
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
    let ready = ready?;

    let statement_id = hex_encode(&ready.operation.operation_id.guid);

    let concurrency = client.chunk_fetch_concurrency.max(1);
    let (tx, rx) = mpsc::channel::<Result<ChunkItem, ApiError>>(concurrency);
    tokio::spawn(drive_thrift_fetch_loop(
        client,
        ready.operation,
        ready.schema_bytes,
        ready.lz4_compressed,
        ready.initial_rowset,
        ready.already_closed,
        throwaway_session,
        tx,
    ));

    Ok(ResultStream {
        statement_id,
        num_chunks: 0,
        schema: None,
        columns: Vec::new(),
        reorder: ReorderBuffer::new(rx),
        pending: VecDeque::new(),
        pending_rows: 0,
        exhausted: false,
        poisoned: false,
    })
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
    operation: thrift::OperationHandle,
    schema_bytes: Option<bytes::Bytes>,
    lz4_compressed: bool,
    initial_rowset: Option<(thrift::RowSet, bool)>,
    already_closed: bool,
    throwaway_session: Option<thrift::SessionHandle>,
    tx: mpsc::Sender<Result<ChunkItem, ApiError>>,
) {
    run_thrift_fetch_loop(&client, &operation, schema_bytes, lz4_compressed, initial_rowset, &tx).await;
    if !already_closed {
        client.thrift_close_operation_best_effort(&operation).await;
    }
    if let Some(session) = throwaway_session {
        client.thrift_close_session_raw(&session).await;
    }
}

/// One `resultLinks` entry plus the `chunk_index` it was assigned at
/// discovery time (in strict `FetchResults` order, which is also true row
/// order -- see this module's own reasoning on `ReorderBuffer` not needing
/// to reorder anything on the *discovery* side, only the download side now
/// that downloads happen out of order across batches).
struct ThriftLinkWork {
    chunk_index: i64,
    row_count: i64,
    file_link: String,
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
    client: Arc<DbClient>,
    work: ThriftLinkWork,
    compressed: bool,
) -> Result<ChunkItem, ApiError> {
    let blob = client.fetch_link_bytes(&work.file_link, compressed).await?;
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
/// (`dim_article`, `LIMIT 500000`, 4 warm runs each): SEA (which knows its
/// whole chunk manifest upfront and fans out `chunk_fetch_concurrency`
/// downloads across the *entire* result immediately, see
/// `client.rs`'s `fetch_chunks_with_backpressure`) averaged 11.8s; this
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
    mut lz4_compressed: bool,
    initial_rowset: Option<(thrift::RowSet, bool)>,
    tx: &mpsc::Sender<Result<ChunkItem, ApiError>>,
) {
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
        worker_handles.push(tokio::spawn(async move {
            loop {
                let work = { link_rx.lock().await.recv().await };
                let Some(work) = work else { return };
                let compressed = compressed_flag.load(std::sync::atomic::Ordering::Relaxed);
                let result = fetch_thrift_link(client.clone(), work, compressed).await;
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
    let mut metadata_confirmed = false;
    let mut pending_until_confirmed: Vec<ThriftLinkWork> = Vec::new();
    loop {
        let (row_set, has_more) = if let Some(v) = pending.take() {
            v
        } else {
            match client.thrift_fetch_results_raw(operation).await {
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
                        truncate_to: None,
                    };
                    chunk_index += 1;
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

        for link in row_set.result_links {
            let idx = chunk_index;
            chunk_index += 1;
            let work = ThriftLinkWork {
                chunk_index: idx,
                row_count: link.row_count,
                file_link: link.file_link,
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
                // See this function's own comment above `metadata_confirmed`
                // -- held back until compression is authoritatively known at
                // least once, rather than risking a download against a
                // still-unconfirmed guess.
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

    // Metadata never arrived across the whole statement (legal, if
    // unusual) -- flush whatever's left using `lz4_compressed`'s own
    // fallback value (the request's own `canDecompressLZ4Result`), the same
    // one `compressed_flag` was initialized from. Nothing more authoritative
    // is ever coming once the loop above has already seen `has_more_rows ==
    // false`.
    for work in pending_until_confirmed.drain(..) {
        if link_tx.send(work).await.is_err() {
            break;
        }
    }

    drop(link_tx); // lets every worker's `recv()` return `None` once the queue drains
    for handle in worker_handles {
        let _ = handle.await;
    }
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
/// is real CPU work, same reasoning as `client.rs`'s `fetch_link_bytes`.
fn build_inline_blob(
    schema_bytes: Option<bytes::Bytes>,
    batches: Vec<thrift::ArrowBatch>,
    lz4_compressed: bool,
) -> Result<(bytes::Bytes, i64), ApiError> {
    let mut out = Vec::new();
    if let Some(s) = &schema_bytes {
        out.extend_from_slice(s);
    }
    let mut row_count = 0i64;
    for b in batches {
        row_count += b.row_count;
        if lz4_compressed {
            let decoded = crate::client::decompress_lz4_frame(&b.batch)?;
            out.extend_from_slice(&decoded);
        } else {
            out.extend_from_slice(&b.batch);
        }
    }
    Ok((bytes::Bytes::from(out), row_count))
}

/// Decodes one chunk's blob and, only if `truncate_to` is `Some(n)` and the
/// decode produced more than `n` rows, slices the batches down to exactly
/// `n` (in order, no re-encode) -- the single decode every `ChunkItem`
/// consumer needs, whether or not truncation actually applies. Replaces a
/// previous design that decoded twice per Thrift cloud-fetch chunk (once
/// just to *count* rows for truncation, immediately discarding the result
/// and re-encoding back to bytes if untruncated; once again here, for
/// real, downstream) -- found in review, a pure duplicate-work removal
/// with no behavior change, see `fetch_thrift_link`'s own doc comment.
///
/// The truncation itself is a real, confirmed-against-a-live-workspace
/// requirement, not a hypothetical: `SELECT * FROM dim_article LIMIT
/// 500000` came back with 502879 rows end to end (2879 extra) via the
/// Thrift cloud-fetch path before this existed, while the *same* query via
/// SEA's `EXTERNAL_LINKS` chunking came back with exactly 500000. This
/// isn't a bug in this crate's decode -- `databricks-sql-connector`'s own
/// `ResultSetDownloadHandler.run` has an identical check with the identical
/// justification in its own comment: "The server rarely prepares the exact
/// number of rows requested by the client in cloud fetch. Subsequently, we
/// drop the extraneous rows in the last file if more rows are retrieved
/// than requested." Silently handing back more rows than a `LIMIT` (or any
/// other row-count-bounded query) asked for is exactly the "silent
/// incorrectness" this crate's own testing discipline exists to catch.
///
/// Only ever *drops* rows, never guesses at which ones to keep beyond "the
/// first `truncate_to`, in order" -- a `truncate_to` of `None` or `<= 0`
/// (not populated, or genuinely zero) skips truncation entirely rather than
/// assumed; a chunk with fewer or exactly as many rows as declared is
/// returned unchanged (the overwhelmingly common case).
fn decode_chunk_item(blob: &Bytes, truncate_to: Option<i64>) -> Result<Vec<RecordBatch>, ApiError> {
    let batches = decode_chunk(blob)?;
    let Some(declared_row_count) = truncate_to.filter(|n| *n > 0) else {
        return Ok(batches);
    };
    let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
    if total_rows <= declared_row_count {
        return Ok(batches);
    }

    let mut kept = Vec::with_capacity(batches.len());
    let mut remaining = declared_row_count;
    for batch in batches {
        if remaining <= 0 {
            break;
        }
        if (batch.num_rows() as i64) <= remaining {
            remaining -= batch.num_rows() as i64;
            kept.push(batch);
        } else {
            kept.push(batch.slice(0, remaining as usize));
            remaining = 0;
        }
    }
    // `declared_row_count > 0` (the early `filter` above) guarantees
    // `remaining > 0` going into the loop's first iteration, so `kept`
    // always gets at least one push there -- this can't be empty with the
    // logic above, but found in review that the old version's explicit
    // "no batches survived truncation" error (needed back when this
    // returned re-encoded bytes and had to get a schema from `kept.first()`)
    // quietly disappeared when this switched to returning batches directly.
    // A `debug_assert` costs nothing in release builds and still catches a
    // future edit to the loop above (e.g. the `remaining <= 0` break
    // condition) that breaks this invariant, during testing rather than
    // silently in production.
    debug_assert!(
        !kept.is_empty(),
        "decode_chunk_item: truncation produced zero batches despite a positive declared_row_count -- \
         the loop above's invariant was violated"
    );
    Ok(kept)
}

/// Like `execute_lazy`, but first tries `disposition: INLINE` + `format:
/// JSON_ARRAY` via `DbClient::execute_arrow_statement_prefer_inline` -- for
/// a small result, this skips the chunk-fetch round trip entirely (see that
/// function's own doc comment for the full reasoning and the confirmed
/// real-workspace behavior it's based on). Converts the returned JSON_ARRAY
/// rows into a `RecordBatch` via `json_convert`; if that conversion hits a
/// column type it doesn't handle, falls back to a **fresh** `execute_lazy`
/// call (a distinct statement execution, not a retry -- see
/// `execute_arrow_statement_prefer_inline`'s own doc comment for why that's
/// safe). Not the default -- opt-in only, via `Cursor.execute(...,
/// prefer_inline=True)`.
pub async fn execute_lazy_prefer_inline(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<ResultStream, ApiError> {
    let outcome = client
        .execute_arrow_statement_prefer_inline(statement, catalog, schema, parameters.clone())
        .await?;

    let (statement_id, rows, columns) = match outcome {
        InlineOrExternal::External(submitted) => return Ok(result_stream_from_submitted(client, submitted)),
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
                statement_id,
                num_chunks: 1,
                schema,
                columns,
                reorder: ReorderBuffer::new(rx),
                pending: VecDeque::from([batch]),
                pending_rows,
                exhausted: true,
                poisoned: false,
            })
        }
        Err(_) => execute_lazy(client, statement, catalog, schema, parameters).await,
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
    let submitted = client
        .execute_arrow_statement(statement, catalog, schema, parameters)
        .await?;
    let num_chunks = submitted.chunk_metas.len();

    let rx = client.clone().fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
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

/// Converts one chunk's decoded batches into NDJSON lines, one per row, in
/// arro3-`write_ndjson(explicit_nulls=True)`-compatible format: null-valued
/// keys stay present as JSON `null` rather than being omitted, and a
/// UTC-aware timestamp column renders as full ISO-8601 with a trailing `Z`
/// (arrow-json's own default -- no custom format string needed, verified
/// against arrow-json's own `write_timestamps_with_tz` test producing
/// `"2018-11-13T17:11:10Z"`-shaped output, same as arro3).
///
/// `non_finite_as_string`: `arrow-json` (this crate's own JSON writer, not
/// something arrowbricks wrote) hardcodes NaN/+-Infinity to JSON `null` --
/// valid JSON, but indistinguishable from a real NULL once it's out (found
/// by testing a wide real query against a live warehouse: a NaN and a NULL
/// column came back identically as `null`). When this is `true`, those
/// specific cells are patched to the JSON strings `"NaN"`/`"Infinity"`/
/// `"-Infinity"` after encoding -- see `patch_non_finite_floats`. Only
/// top-level Float32/Float64 columns are covered; a non-finite float nested
/// inside a STRUCT/ARRAY/MAP still comes back as `null` either way.
fn encode_ndjson_lines(batches: &[RecordBatch], non_finite_as_string: bool) -> Result<Vec<String>, ApiError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    {
        let builder = arrow_json::WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = builder.build::<_, arrow_json::writer::LineDelimited>(&mut buf);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        writer.write_batches(&refs).map_err(|e| ApiError {
            message: format!("NDJSON encode error: {e}"),
            transient: false,
        })?;
        writer.finish().map_err(|e| ApiError {
            message: format!("NDJSON encode error: {e}"),
            transient: false,
        })?;
    }
    let mut lines: Vec<String> = String::from_utf8(buf)
        .map_err(|e| ApiError {
            message: format!("NDJSON encode produced invalid UTF-8: {e}"),
            transient: false,
        })?
        .lines()
        .map(|line| line.to_string())
        .collect();

    if non_finite_as_string {
        patch_non_finite_floats(batches, &mut lines);
    }
    Ok(lines)
}

/// `Some(token)` (already-quoted JSON, e.g. `"\"NaN\""`) if `v` is NaN or
/// +-infinite, `None` for any finite value (including `-0.0`).
fn non_finite_token(v: f64) -> Option<&'static str> {
    if v.is_nan() {
        Some("\"NaN\"")
    } else if v == f64::INFINITY {
        Some("\"Infinity\"")
    } else if v == f64::NEG_INFINITY {
        Some("\"-Infinity\"")
    } else {
        None
    }
}

/// One top-level Float32/Float64 column, downcast once per batch rather than
/// once per (row, field) -- see `patch_non_finite_floats`.
enum FloatColumn<'a> {
    F64(usize, &'a arrow::array::Float64Array),
    F32(usize, &'a arrow::array::Float32Array),
}

/// Rewrites each affected line's `null` (arrow-json's fixed encoding for a
/// non-finite float, see `encode_ndjson_lines`) to a `"NaN"`/`"Infinity"`/
/// `"-Infinity"` JSON string, in place. Only scans top-level Float32/Float64
/// columns -- one row of `lines` per row of the batches, in the same order.
/// Precomputes each batch's float columns once (schema scan + downcast) up
/// front instead of redoing both per row -- for a wide, non-float-heavy
/// schema (this feature's own motivating case: a 120-column real table) that
/// was a dynamic downcast attempt on every column for every row, the large
/// majority immediately discarded, and a schema with no float columns at all
/// still paid for the full row x column scan for nothing.
fn patch_non_finite_floats(batches: &[RecordBatch], lines: &mut [String]) {
    let mut global_row = 0usize;
    for batch in batches {
        let schema = batch.schema();
        let float_columns: Vec<FloatColumn> = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(field_index, field)| match field.data_type() {
                DataType::Float64 => Some(FloatColumn::F64(
                    field_index,
                    batch.column(field_index).as_primitive::<Float64Type>(),
                )),
                DataType::Float32 => Some(FloatColumn::F32(
                    field_index,
                    batch.column(field_index).as_primitive::<Float32Type>(),
                )),
                _ => None,
            })
            .collect();

        if float_columns.is_empty() {
            global_row += batch.num_rows();
            continue;
        }

        for row_in_batch in 0..batch.num_rows() {
            for col in &float_columns {
                let (field_index, token) = match col {
                    FloatColumn::F64(field_index, arr) => (
                        *field_index,
                        arr.is_valid(row_in_batch)
                            .then(|| non_finite_token(arr.value(row_in_batch)))
                            .flatten(),
                    ),
                    FloatColumn::F32(field_index, arr) => (
                        *field_index,
                        arr.is_valid(row_in_batch)
                            .then(|| non_finite_token(arr.value(row_in_batch) as f64))
                            .flatten(),
                    ),
                };
                if let Some(token) = token {
                    lines[global_row] = replace_nth_top_level_null(&lines[global_row], field_index, token);
                }
            }
            global_row += 1;
        }
    }
}

/// Replaces the value at the `field_index`-th top-level key (0-based, in
/// schema field order -- matching arrow-json's own emission order, which
/// writes keys in field order rather than sorting them) of one NDJSON line
/// with `replacement` (already valid JSON). Only ever called where the
/// existing value is known -- from the source Arrow array, not by inspecting
/// the JSON text -- to be exactly the 4-byte `null` arrow-json writes for a
/// non-finite float, so this never touches a real NULL and, by counting
/// keys positionally rather than by name, is correct even when two top-level
/// columns share the same name (a real, supported case in this crate).
fn replace_nth_top_level_null(line: &str, field_index: usize, replacement: &str) -> String {
    let bytes = line.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut current_field = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth -= 1,
            b':' if depth == 1 => {
                if current_field == field_index {
                    let value_start = i + 1;
                    debug_assert_eq!(
                        bytes.get(value_start..value_start + 4),
                        Some(&b"null"[..]),
                        "replace_nth_top_level_null called on a field whose value wasn't null"
                    );
                    let mut out = String::with_capacity(line.len() + replacement.len());
                    out.push_str(&line[..value_start]);
                    out.push_str(replacement);
                    out.push_str(&line[value_start + 4..]);
                    return out;
                }
                current_field += 1;
            }
            _ => {}
        }
    }
    line.to_string()
}

/// Chunk-granularity (not row-count-granularity) counterpart to
/// `ResultStream`: pulls, decodes, and NDJSON-encodes exactly one reordered
/// chunk per `next_chunk()` call rather than buffering ahead to satisfy a
/// row count. Backs `stream_query_json` end to end -- unlike the Arrow-Table
/// pipelines above, there's no further Python-side conversion step.
pub struct NdjsonStream {
    pub statement_id: String,
    pub num_chunks: usize,
    reorder: ReorderBuffer,
    non_finite_as_string: bool,
}

impl NdjsonStream {
    /// Pulls the next chunk in logical (chunk_index) order, decodes it, and
    /// NDJSON-encodes it (all on a blocking thread, since both decode and
    /// JSON encoding are CPU work) -- `None` once the source is exhausted.
    /// One network chunk in, one line per row out, matching
    /// `fetch_arrow_chunks_for_statement`'s old per-chunk yield.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<String>>, ApiError> {
        match self.reorder.next().await? {
            Some(item) => {
                let non_finite_as_string = self.non_finite_as_string;
                let truncate_to = item.truncate_to;
                let lines = tokio::task::spawn_blocking(move || {
                    let batches = decode_chunk_item(&item.blob, truncate_to)?;
                    encode_ndjson_lines(&batches, non_finite_as_string)
                })
                .await
                .map_err(join_error)??;
                Ok(Some(lines))
            }
            None => Ok(None),
        }
    }
}

/// Submit -> poll -> start background chunk fetching for the chunk-at-a-time
/// stream above. Matches Python's `fetch_arrow_chunks_with_manifest`: this
/// await itself is never heartbeat-wrapped (only the per-chunk pulls that
/// follow are) -- `stream_query_json` only wraps its chunk iterator, not
/// this initial submit/poll wait, so this crate preserves that same gap
/// rather than "fixing" it during the port.
pub async fn execute_ndjson_stream(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
    non_finite_as_string: bool,
) -> Result<NdjsonStream, ApiError> {
    let submitted = client
        .execute_arrow_statement(statement, catalog, schema, parameters)
        .await?;
    let num_chunks = submitted.chunk_metas.len();
    let rx = client.fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
    );
    Ok(NdjsonStream {
        statement_id: submitted.statement_id,
        num_chunks,
        reorder: ReorderBuffer::new(rx),
        non_finite_as_string,
    })
}

pub struct JsonResult {
    pub statement_id: String,
    pub num_chunks: usize,
    pub rows: Vec<Vec<Option<String>>>,
    pub columns: Vec<ColumnDescription>,
}

/// JSON_ARRAY's contract (Databricks' own, not ours): each chunk is a JSON
/// array of rows, each row an array of values where every non-null value is
/// a *string* regardless of its real column type; null stays JSON null.
/// Casting by the manifest's column type_name, if wanted, is left to the
/// caller -- same pass-through the Python original leaves to its own
/// caller.
fn decode_json_chunk(blob: &Bytes) -> Result<Vec<Vec<Option<String>>>, ApiError> {
    serde_json::from_slice(&blob[..]).map_err(|e| ApiError {
        message: format!("bad JSON_ARRAY chunk: {e}"),
        transient: false,
    })
}

/// JSON_ARRAY counterpart to `run_pipeline` -- same submit/poll/fetch/
/// reorder machinery (chunk bytes are chunk bytes regardless of format),
/// decoding each chunk as a JSON array of rows instead of Arrow-IPC.
pub async fn run_json_pipeline(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
) -> Result<JsonResult, ApiError> {
    let submitted = client
        .execute_json_statement(statement, catalog, schema, parameters)
        .await?;
    let num_chunks = submitted.chunk_metas.len();

    let rx = client.clone().fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
    );
    let mut reorder = ReorderBuffer::new(rx);

    let mut decode_handles = Vec::with_capacity(num_chunks);
    while let Some(item) = reorder.next().await? {
        decode_handles.push(tokio::task::spawn_blocking(move || decode_json_chunk(&item.blob)));
    }

    let mut rows = Vec::new();
    for handle in decode_handles {
        rows.extend(handle.await.map_err(join_error)??);
    }

    Ok(JsonResult {
        statement_id: submitted.statement_id,
        num_chunks,
        rows,
        columns: submitted.columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(idx: i64) -> ChunkItem {
        ChunkItem {
            blob: Bytes::new(),
            row_count: None,
            chunk_index: idx,
            truncate_to: None,
        }
    }

    async fn drain_indices(sent: Vec<Result<ChunkItem, ApiError>>) -> Result<Vec<i64>, String> {
        let (tx, rx) = mpsc::channel(sent.len().max(1));
        for r in sent {
            tx.send(r).await.unwrap();
        }
        drop(tx);
        let mut buf = ReorderBuffer::new(rx);
        let mut out = Vec::new();
        loop {
            match buf.next().await {
                Ok(Some(it)) => out.push(it.chunk_index),
                Ok(None) => return Ok(out),
                Err(e) => return Err(e.message),
            }
        }
    }

    #[tokio::test]
    async fn preserves_order_despite_out_of_order_arrival() {
        // Arrives 2, 0, 1 -- must still be yielded 0, 1, 2.
        let sent = vec![Ok(item(2)), Ok(item(0)), Ok(item(1))];
        assert_eq!(drain_indices(sent).await.unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn duplicate_index_keeps_both_blobs() {
        // Two blobs for index 0 (multiple external_links for one chunk).
        // The guaranteed invariant is "never lost" (both show up), not
        // strict adjacency: consuming the first blob for index 0 advances
        // next_idx past 0 immediately, so a still-in-flight second blob for
        // that same index becomes a straggler rescued later by the
        // exhausted-gap-drain path -- same behavior as the Python original
        // (_ResultSet._pull_one_chunk_table), not a Rust-specific quirk.
        let sent = vec![Ok(item(0)), Ok(item(0)), Ok(item(1))];
        let out = drain_indices(sent).await.unwrap();
        assert_eq!(
            out.iter().filter(|&&i| i == 0).count(),
            2,
            "both index-0 blobs must survive: {out:?}"
        );
        assert_eq!(
            out.iter().filter(|&&i| i == 1).count(),
            1,
            "index-1 blob must survive: {out:?}"
        );
        assert_eq!(out.len(), 3, "no extra/lost items: {out:?}");
    }

    #[tokio::test]
    async fn gap_drains_lowest_remaining_instead_of_stranding() {
        // Index 1 never arrives at all -- once the source is exhausted,
        // draining the lowest remaining (2) instead of waiting forever.
        let sent = vec![Ok(item(2)), Ok(item(0))];
        assert_eq!(drain_indices(sent).await.unwrap(), vec![0, 2]);
    }

    #[tokio::test]
    async fn error_surfaces_after_already_yielded_items() {
        let sent = vec![
            Ok(item(0)),
            Ok(item(1)),
            Err(ApiError {
                message: "boom".into(),
                transient: false,
            }),
        ];
        let (tx, rx) = mpsc::channel(sent.len());
        for r in sent {
            tx.send(r).await.unwrap();
        }
        drop(tx);
        let mut buf = ReorderBuffer::new(rx);
        assert_eq!(buf.next().await.unwrap().unwrap().chunk_index, 0);
        assert_eq!(buf.next().await.unwrap().unwrap().chunk_index, 1);
        assert_eq!(buf.next().await.unwrap_err().message, "boom");
    }

    #[test]
    fn non_finite_token_covers_nan_and_both_infinities_only() {
        assert_eq!(non_finite_token(f64::NAN), Some("\"NaN\""));
        assert_eq!(non_finite_token(f64::INFINITY), Some("\"Infinity\""));
        assert_eq!(non_finite_token(f64::NEG_INFINITY), Some("\"-Infinity\""));
        assert_eq!(non_finite_token(0.0), None);
        assert_eq!(non_finite_token(-0.0), None);
        assert_eq!(non_finite_token(123.456), None);
        assert_eq!(non_finite_token(-123.456), None);
    }

    #[test]
    fn replace_nth_top_level_null_targets_by_position_not_name() {
        // Two fields named "dup" -- the second is the one that's actually
        // null (arrow-json's non-finite-float encoding); the first, same-
        // named field is a real value and must be left alone. Proves the
        // positional approach (not name matching) is what makes this safe.
        let line = r#"{"dup":1,"dup":null}"#;
        let patched = replace_nth_top_level_null(line, 1, "\"NaN\"");
        assert_eq!(patched, r#"{"dup":1,"dup":"NaN"}"#);
    }

    #[test]
    fn replace_nth_top_level_null_ignores_nested_nulls_at_deeper_depth() {
        // A nested object's own "null" must not be mistaken for the
        // top-level field being targeted -- depth tracking (not a naive
        // first-`null`-wins scan) is what keeps this correct.
        let line = r#"{"a":{"inner":null},"b":null}"#;
        let patched = replace_nth_top_level_null(line, 1, "\"Infinity\"");
        assert_eq!(patched, r#"{"a":{"inner":null},"b":"Infinity"}"#);
    }

    #[test]
    fn replace_nth_top_level_null_skips_colons_inside_string_values() {
        // A colon inside a quoted string value (e.g. a timestamp or URL)
        // must not be mistaken for a field separator.
        let line = r#"{"label":"12:34:56","dup":1,"dup":null}"#;
        let patched = replace_nth_top_level_null(line, 2, "\"NaN\"");
        assert_eq!(patched, r#"{"label":"12:34:56","dup":1,"dup":"NaN"}"#);
    }

    fn make_batch(id: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(id)), Arc::new(Float64Array::from(values))],
        )
        .unwrap()
    }

    fn write_stream(batches: &[RecordBatch]) -> Bytes {
        use arrow::ipc::writer::StreamWriter;
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batches[0].schema()).unwrap();
            for b in batches {
                writer.write(b).unwrap();
            }
            writer.finish().unwrap();
        }
        Bytes::from(buf)
    }

    /// Regression test for a real behavior found by testing this crate's own
    /// Thrift path against a real workspace: `SELECT * FROM dim_article
    /// LIMIT 500000` came back with 502879 rows (2879 too many) via a
    /// cloud-fetch `resultLink` whose own declared `rowCount` was 500000 --
    /// `databricks-sql-connector` has an identical truncation step for the
    /// identical documented reason (see this function's own doc comment).
    /// Straddles a batch boundary on purpose (declared count lands
    /// mid-batch) -- the simpler "drop whole extra batches only" bug would
    /// pass a test where the boundary landed exactly on a batch edge.
    #[test]
    fn decode_chunk_item_slices_the_straddling_batch() {
        let batch_a = make_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        let batch_b = make_batch(vec![4, 5, 6], vec![4.0, 5.0, 6.0]);
        let blob = write_stream(&[batch_a, batch_b]);

        let batches = decode_chunk_item(&blob, Some(4)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 4,
            "must keep exactly the declared row count, not the file's real count"
        );

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4],
            "must keep the first N rows in order, not an arbitrary subset"
        );
    }

    /// A chunk that already matches (or undershoots) its declared row count
    /// must come back with every row intact -- the overwhelmingly common
    /// case, and truncation must not be triggered speculatively. Checks
    /// actual cell values, not just a row count -- found in review that the
    /// old byte-identity assertion (`assert_eq!(untouched, blob)`, possible
    /// when this returned re-encoded bytes) got replaced by a row-count-only
    /// sum when this function switched to returning decoded batches
    /// directly, silently losing coverage for a value/column-order bug on
    /// this exact path that a count-only check can't catch.
    #[test]
    fn decode_chunk_item_is_a_no_op_when_not_needed() {
        let batch = make_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        let blob = write_stream(&[batch]);
        let batches = decode_chunk_item(&blob, Some(3)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "must not drop rows when the chunk already matches the declared count"
        );
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "values themselves must survive untouched, not just the row count"
        );
    }

    /// `truncate_to` of `None` or `<= 0` means "no authoritative bound
    /// known" -- must never be treated as "truncate to zero rows." Checks
    /// actual cell values too, same reasoning as
    /// `decode_chunk_item_is_a_no_op_when_not_needed`'s own doc comment.
    #[test]
    fn decode_chunk_item_skips_truncation_when_bound_is_unknown() {
        let batch = make_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        let blob = write_stream(&[batch]);
        let ids_for = |batches: &[RecordBatch]| -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_primitive::<arrow::datatypes::Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect()
        };
        let via_none = decode_chunk_item(&blob, None).unwrap();
        let via_zero = decode_chunk_item(&blob, Some(0)).unwrap();
        assert_eq!(ids_for(&via_none), vec![1, 2, 3]);
        assert_eq!(ids_for(&via_zero), vec![1, 2, 3]);
    }

    /// Regression test for a real behavior found by testing edge-case data
    /// types against a live Databricks warehouse: NaN and Infinity floats
    /// came back from `stream_query_json` as JSON `null`, indistinguishable
    /// from a genuine SQL NULL in the same column -- arrow-json's own fixed
    /// encoding for non-finite floats, not something arrowbricks chose.
    /// `non_finite_as_string=true` must recover the distinction.
    #[test]
    fn encode_ndjson_lines_preserves_non_finite_floats_as_strings_when_requested() {
        let batch = make_batch(vec![1, 2, 3, 4], vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5]);

        let default_lines = encode_ndjson_lines(std::slice::from_ref(&batch), false).unwrap();
        assert_eq!(default_lines[0], r#"{"id":1,"value":null}"#);
        assert_eq!(default_lines[1], r#"{"id":2,"value":null}"#);
        assert_eq!(default_lines[2], r#"{"id":3,"value":null}"#);
        assert_eq!(default_lines[3], r#"{"id":4,"value":1.5}"#);

        let string_lines = encode_ndjson_lines(std::slice::from_ref(&batch), true).unwrap();
        assert_eq!(string_lines[0], r#"{"id":1,"value":"NaN"}"#);
        assert_eq!(string_lines[1], r#"{"id":2,"value":"Infinity"}"#);
        assert_eq!(string_lines[2], r#"{"id":3,"value":"-Infinity"}"#);
        assert_eq!(string_lines[3], r#"{"id":4,"value":1.5}"#); // finite values untouched
    }

    #[test]
    fn encode_ndjson_lines_leaves_a_real_null_alone_when_requested() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![None])), // a genuine SQL NULL, not NaN
            ],
        )
        .unwrap();

        let lines = encode_ndjson_lines(std::slice::from_ref(&batch), true).unwrap();
        assert_eq!(
            lines[0], r#"{"id":1,"value":null}"#,
            "a real NULL must stay null, never become a string"
        );
    }

    /// Regression test for the switch from `StreamReader` to the push-based
    /// `StreamDecoder` (see `decode_chunk`'s doc comment): a single chunk can
    /// contain more than one Arrow-IPC `RecordBatch` message back to back in
    /// the same stream, and `StreamDecoder::decode` only ever returns one
    /// batch per call -- `decode_chunk` must keep calling it until the whole
    /// buffer is drained, not stop after the first. `StreamReader`'s own
    /// `Iterator` impl made this automatic; the lower-level API doesn't, so
    /// this is exactly the kind of thing that regresses silently (a single-
    /// batch-per-chunk bug would still pass every other test in this suite,
    /// since none of them writes more than one batch per chunk).
    #[test]
    fn decode_chunk_reads_every_record_batch_in_a_multi_batch_stream() {
        use arrow::ipc::writer::StreamWriter;

        let batch_a = make_batch(vec![1, 2], vec![1.0, 2.0]);
        let batch_b = make_batch(vec![3, 4, 5], vec![3.0, 4.0, 5.0]);

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch_a.schema()).unwrap();
            writer.write(&batch_a).unwrap();
            writer.write(&batch_b).unwrap();
            writer.finish().unwrap();
        }

        let batches = decode_chunk(&Bytes::from(buf)).unwrap();
        assert_eq!(
            batches.len(),
            2,
            "both record batches in the stream must be decoded, not just the first"
        );
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    /// Regression test for a bug caught in code review before it shipped: an
    /// empty (zero-byte) blob made `StreamDecoder`'s decode loop a no-op and
    /// `decoder.finish()` saw a still-pristine state, which it treats as a
    /// clean empty stream -- silently returning zero batches with no error at
    /// all, instead of the loud failure the old `StreamReader`-based version
    /// gave on the same input ("Expected schema message, found empty
    /// stream"). Same silent-truncation shape as the real multi-frame LZ4 bug
    /// this crate already shipped once (see `client.rs`'s
    /// `decompress_lz4_frame` doc comment) -- a genuinely empty chunk blob
    /// must never be mistaken for "legitimately zero rows".
    #[test]
    fn decode_chunk_rejects_an_empty_blob() {
        let err = decode_chunk(&Bytes::new()).expect_err("an empty blob must error, not silently decode to zero rows");
        assert!(
            err.message.contains("empty"),
            "error should mention the blob was empty: {}",
            err.message
        );
    }

    /// Companion to `decode_chunk_rejects_an_empty_blob` -- proves the empty-
    /// blob check doesn't overcorrect: a *non-empty* stream containing only a
    /// schema message and no `RecordBatch` at all (a legitimate shape for a
    /// genuinely empty query result) must still decode successfully to zero
    /// batches, not error.
    #[test]
    fn decode_chunk_accepts_a_schema_only_stream_with_zero_batches() {
        use arrow::datatypes::{Field, Schema};
        use arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.finish().unwrap();
        }

        let batches = decode_chunk(&Bytes::from(buf)).unwrap();
        assert_eq!(
            batches.len(),
            0,
            "a schema-only stream with no batches is valid, not an error"
        );
    }

    /// Regression/documentation test for a real behavior change found in code
    /// review: `StreamDecoder` (unlike the old `StreamReader`) hard-errors on
    /// any bytes left over after a stream's own EOS marker, instead of
    /// silently ignoring them. Locking this in deliberately -- erroring beats
    /// silently dropping whatever came after the truncation point, same
    /// reasoning as the empty-blob check above -- even though real Databricks
    /// chunks have not been observed to have trailing bytes.
    #[test]
    fn decode_chunk_errors_on_trailing_bytes_after_a_complete_stream() {
        use arrow::ipc::writer::StreamWriter;

        let batch = make_batch(vec![1, 2], vec![1.0, 2.0]);
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buf.extend_from_slice(&[0xAA; 8]);

        decode_chunk(&Bytes::from(buf)).expect_err("trailing bytes after a complete stream's EOS marker must error");
    }

    /// Regression/coverage test: a dictionary-encoded column is written as a
    /// separate `DictionaryBatch` IPC message *before* the `RecordBatch`
    /// message that references it -- `StreamDecoder::decode` consumes that
    /// message internally (updating its own dictionary table) and returns
    /// `Ok(None)` for it, not `Ok(Some(_))`. A prior review verified by
    /// reading `StreamDecoder`'s source that `decode_chunk`'s `Ok(None) => {}`
    /// branch handles this correctly without ending the loop early, but no
    /// test exercised it -- this proves it end to end, not just by source
    /// inspection: round-trips real dictionary keys/values through
    /// `decode_chunk`, not just an empty or single-message stream.
    #[test]
    fn decode_chunk_round_trips_a_dictionary_encoded_column() {
        use arrow::array::{DictionaryArray, Int32Array, StringArray};
        use arrow::datatypes::{Field, Int32Type, Schema};
        use arrow::ipc::writer::StreamWriter;

        let keys = Int32Array::from(vec![0, 1, 0, 2]);
        let values = StringArray::from(vec!["a", "b", "c"]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let batches = decode_chunk(&Bytes::from(buf)).unwrap();
        assert_eq!(
            batches.len(),
            1,
            "the dictionary message itself must not be mistaken for the record batch"
        );
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("column must still be a dictionary array after round-tripping");
        let dict_values = col.values().as_any().downcast_ref::<StringArray>().unwrap();
        let decoded: Vec<&str> = col
            .keys()
            .values()
            .iter()
            .map(|&k| dict_values.value(k as usize))
            .collect();
        assert_eq!(decoded, vec!["a", "b", "a", "c"]);
    }

    /// Diagnostic only, not a correctness check (relative timing is too
    /// flaky for CI) -- `cargo test --release -- --ignored --nocapture
    /// decode_chunk_speed` to compare the current `StreamDecoder`-based
    /// `decode_chunk` against the old `StreamReader`-based approach it
    /// replaced, on a batch shaped like a real chunk (120 columns, mixed
    /// types, 50k rows -- this session's own real-table benchmark).
    #[test]
    #[ignore]
    fn decode_chunk_speed_vs_stream_reader() {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use std::io::Cursor as IoCursor;
        use std::time::Instant;

        const ROWS: usize = 50_000;
        const COLS: usize = 120;

        let mut fields = Vec::with_capacity(COLS);
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(COLS);
        for i in 0..COLS {
            match i % 3 {
                0 => {
                    fields.push(Field::new(format!("c{i}"), DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())));
                }
                1 => {
                    fields.push(Field::new(format!("c{i}"), DataType::Float64, false));
                    columns.push(Arc::new(Float64Array::from(
                        (0..ROWS).map(|r| r as f64 * 1.5).collect::<Vec<_>>(),
                    )));
                }
                _ => {
                    fields.push(Field::new(format!("c{i}"), DataType::Utf8, false));
                    columns.push(Arc::new(StringArray::from(
                        (0..ROWS).map(|r| format!("row-{r}")).collect::<Vec<_>>(),
                    )));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let blob = Bytes::from(buf);

        const ITERS: u32 = 30;

        // Old approach: StreamReader over an IoCursor -- copies every
        // column's data into freshly allocated buffers on every decode.
        let old_start = Instant::now();
        for _ in 0..ITERS {
            let reader = arrow::ipc::reader::StreamReader::try_new(IoCursor::new(&blob[..]), None).unwrap();
            let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches[0].num_rows(), ROWS);
        }
        let old_elapsed = old_start.elapsed();

        // New approach: this file's actual decode_chunk.
        let new_start = Instant::now();
        for _ in 0..ITERS {
            let batches = decode_chunk(&blob).unwrap();
            assert_eq!(batches[0].num_rows(), ROWS);
        }
        let new_elapsed = new_start.elapsed();

        println!(
            "decode_chunk speed ({COLS} cols x {ROWS} rows, {ITERS} iters): \
             StreamReader (old) = {old_elapsed:?} ({:?}/iter), \
             StreamDecoder (new) = {new_elapsed:?} ({:?}/iter)",
            old_elapsed / ITERS,
            new_elapsed / ITERS,
        );
    }
}
