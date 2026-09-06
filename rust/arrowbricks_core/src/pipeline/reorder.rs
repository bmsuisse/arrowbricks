//! Reorder buffer (port of `_ResultSet._pull_one_chunk_table`) + Arrow-IPC
//! decode of the reordered chunk stream via arrow-rs -- the core primitives
//! every backend (SEA, Thrift) and every consumer (`ResultStream`,
//! `NdjsonStream`, the eager `run_pipeline`) shares: `ReorderBuffer` restores
//! logical `chunk_index` order from out-of-order network arrivals, and
//! `decode_chunk`/`decode_chunk_item` turn one chunk's raw Arrow-IPC bytes
//! into `RecordBatch`es (optionally truncated to a declared row-count bound).

use std::collections::{HashMap, VecDeque};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::client::{ApiError, ApiErrorKind, ChunkItem};

/// Same dict-of-lists-keyed-by-index shape as `_ResultSet._pending`: a
/// `chunk_index` can carry more than one blob (multiple `external_links` per
/// chunk), so a plain map from index to single item would silently drop the
/// first blob when a second arrives for the same index.
pub(crate) struct ReorderBuffer {
    rx: mpsc::Receiver<Result<ChunkItem, ApiError>>,
    pending: HashMap<i64, VecDeque<ChunkItem>>,
    next_idx: i64,
    exhausted: bool,
}

impl ReorderBuffer {
    pub(crate) fn new(rx: mpsc::Receiver<Result<ChunkItem, ApiError>>) -> Self {
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
    pub(crate) async fn next(&mut self) -> Result<Option<ChunkItem>, ApiError> {
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

/// Decodes a chunk's raw Arrow-IPC stream bytes into batches. Uses
/// `StreamDecoder`'s push-based interface fed by an `arrow_buffer::Buffer`
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
/// `result_compression` unwrap in `client/download.rs`'s `decompress_lz4_frame`,
/// which already ran before this function ever sees the bytes), `arrow-ipc`'s own reader always
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
    decode_ipc_stream(blob).map(|(batches, _)| batches)
}

/// Also used by cached Python IPC replays, retaining the input allocation
/// through each decoded array. Returning the schema separately preserves
/// schema-only streams that contain no record batches.
pub(crate) fn decode_ipc_stream(blob: &Bytes) -> Result<(Vec<RecordBatch>, SchemaRef), ApiError> {
    if blob.is_empty() {
        return Err(ApiError {
            message: "empty Arrow IPC chunk: expected at least a schema message".to_string(),
            transient: false,
            kind: ApiErrorKind::Other,
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
                    kind: ApiErrorKind::Other,
                });
            }
        }
    }
    decoder.finish().map_err(|e| ApiError {
        message: format!("bad Arrow IPC stream: {e}"),
        transient: false,
        kind: ApiErrorKind::Other,
    })?;
    let schema = decoder
        .schema()
        .ok_or_else(|| ApiError::permanent("Arrow IPC stream has no schema"))?;
    Ok((batches, schema))
}

/// Decodes one chunk's blob and, only if `truncate_to` is `Some(n)` and the
/// decode produced more than `n` rows, slices the batches down to exactly
/// `n` (in order, no re-encode) -- the single decode every `ChunkItem`
/// consumer needs, whether or not truncation actually applies. See
/// `fetch_thrift_link`'s own doc comment for why this replaced a
/// double-decode design.
///
/// The truncation itself is a real, confirmed-against-a-live-workspace
/// requirement, not a hypothetical: `SELECT * FROM benchmark_table LIMIT
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
pub(crate) fn decode_chunk_item(blob: &Bytes, truncate_to: Option<i64>) -> Result<Vec<RecordBatch>, ApiError> {
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
    // `kept` can't be empty here, but found in review that the old version's
    // explicit "no batches survived truncation" error (needed back when this
    // returned re-encoded bytes and had to get a schema from `kept.first()`)
    // quietly disappeared when this switched to returning batches directly.
    // A `debug_assert` costs nothing in release builds and still catches a
    // future edit to the loop above that breaks this invariant, during
    // testing rather than silently in production.
    debug_assert!(
        !kept.is_empty(),
        "decode_chunk_item: truncation produced zero batches despite a positive declared_row_count -- \
         the loop above's invariant was violated"
    );
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    use super::*;
    use crate::pipeline::test_support::make_batch;

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
                kind: ApiErrorKind::Other,
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

    fn write_stream(batches: &[RecordBatch]) -> Bytes {
        use arrow_ipc::writer::StreamWriter;
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

    /// Regression test for `decode_chunk_item`'s truncation -- see its own
    /// doc comment for the real over-delivery incident this guards against.
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
                    .as_primitive::<arrow_array::types::Int64Type>()
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
                    .as_primitive::<arrow_array::types::Int64Type>()
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
                        .as_primitive::<arrow_array::types::Int64Type>()
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
        let batch_a = make_batch(vec![1, 2], vec![1.0, 2.0]);
        let batch_b = make_batch(vec![3, 4, 5], vec![3.0, 4.0, 5.0]);
        let blob = write_stream(&[batch_a, batch_b]);

        let batches = decode_chunk(&blob).unwrap();
        assert_eq!(
            batches.len(),
            2,
            "both record batches in the stream must be decoded, not just the first"
        );
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    /// Regression test for `decode_chunk`'s empty-blob rejection -- see its
    /// own doc comment for the failure mode this guards against.
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
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{Field, Schema};

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
        let batch = make_batch(vec![1, 2], vec![1.0, 2.0]);
        let mut buf = write_stream(&[batch]).to_vec();
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
        use arrow_array::types::Int32Type;
        use arrow_array::{DictionaryArray, Int32Array, StringArray};
        use arrow_schema::{Field, Schema};

        let keys = Int32Array::from(vec![0, 1, 0, 2]);
        let values = StringArray::from(vec!["a", "b", "c"]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();
        let blob = write_stream(&[batch]);

        let batches = decode_chunk(&blob).unwrap();
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
        use arrow_array::{Float64Array, Int64Array, StringArray};
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{Field, Schema};
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
            let reader = arrow_ipc::reader::StreamReader::try_new(IoCursor::new(&blob[..]), None).unwrap();
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
