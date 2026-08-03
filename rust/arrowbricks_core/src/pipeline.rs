//! Reorder buffer (port of `_ResultSet._pull_one_chunk_table`) + Arrow-IPC
//! decode of the reordered chunk stream via arrow-rs.

use std::collections::{HashMap, VecDeque};
use std::io::Cursor as IoCursor;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::client::{join_error, ApiError, ChunkItem, DbClient};

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
        Self { rx, pending: HashMap::new(), next_idx: 0, exhausted: false }
    }

    fn pop_pending(&mut self, idx: i64) -> ChunkItem {
        let items = self.pending.get_mut(&idx).expect("pop_pending called with missing index");
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
    let reader = StreamReader::try_new(IoCursor::new(&blob[..]), None)
        .map_err(|e| ApiError { message: format!("bad Arrow IPC stream: {e}"), transient: false })?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ApiError { message: format!("Arrow IPC decode error: {e}"), transient: false })
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
) -> Result<ExecuteResult, ApiError> {
    let (statement_id, chunk_metas) = client.execute_arrow_statement(statement, catalog, schema).await?;
    let num_chunks = chunk_metas.len();

    let rx = client.clone().fetch_chunks_with_backpressure(statement_id.clone(), chunk_metas);
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

    Ok(ExecuteResult { statement_id, num_chunks, batches, schema })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(idx: i64) -> ChunkItem {
        ChunkItem { blob: Bytes::new(), row_count: None, chunk_index: idx }
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
        assert_eq!(out.iter().filter(|&&i| i == 0).count(), 2, "both index-0 blobs must survive: {out:?}");
        assert_eq!(out.iter().filter(|&&i| i == 1).count(), 1, "index-1 blob must survive: {out:?}");
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
        let sent = vec![Ok(item(0)), Ok(item(1)), Err(ApiError { message: "boom".into(), transient: false })];
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
}
