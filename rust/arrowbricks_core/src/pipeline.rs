//! Reorder buffer (port of `_ResultSet._pull_one_chunk_table`) + Arrow-IPC
//! decode of the reordered chunk stream via arrow-rs.

use std::collections::{HashMap, VecDeque};
use std::io::Cursor as IoCursor;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::client::{ApiError, ChunkItem, ColumnDescription, DbClient, join_error};

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

fn decode_chunk(blob: &Bytes) -> Result<Vec<RecordBatch>, ApiError> {
    let reader = StreamReader::try_new(IoCursor::new(&blob[..]), None).map_err(|e| ApiError {
        message: format!("bad Arrow IPC stream: {e}"),
        transient: false,
    })?;
    reader.collect::<Result<Vec<_>, _>>().map_err(|e| ApiError {
        message: format!("Arrow IPC decode error: {e}"),
        transient: false,
    })
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
                        decode_handles.push(tokio::task::spawn_blocking(move || decode_chunk(&item.blob)));
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
    let num_chunks = submitted.chunk_metas.len();
    let rx = client.fetch_chunks_with_backpressure(
        submitted.statement_id.clone(),
        submitted.chunk_metas,
        submitted.compressed,
    );
    Ok(ResultStream {
        statement_id: submitted.statement_id,
        num_chunks,
        schema: None,
        columns: submitted.columns,
        reorder: ReorderBuffer::new(rx),
        pending: VecDeque::new(),
        pending_rows: 0,
        exhausted: false,
    })
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
        decode_handles.push(tokio::task::spawn_blocking(move || decode_chunk(&item.blob)));
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
                let lines = tokio::task::spawn_blocking(move || {
                    let batches = decode_chunk(&item.blob)?;
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
}
