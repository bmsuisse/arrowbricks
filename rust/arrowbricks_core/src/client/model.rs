//! Shared data-model types passed between the client and pipeline layers:
//! `ColumnDescription` (a manifest column, used for `Cursor.description`
//! before any chunk has been decoded), `CancelHandle` (enough state to fire
//! a best-effort server-side cancel), the per-query `QueryStatsAccumulator`/
//! `QueryStatsData`/`EventSink` observability shapes, and what submitting a
//! statement gets you before any chunk is fetched (`ChunkMeta`,
//! `StatementSubmitResult`, `InlineOrExternal`, `ChunkItem`).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use serde::Deserialize;

use crate::thrift;

/// One manifest column description -- matches `_description_from_manifest`'s
/// `c.get("name")`/`c.get("type_name")`. Only carried for `Cursor.description`
/// compatibility (Python's own fallback for describing a result before any
/// chunk has actually been fetched, since the real Arrow schema isn't known
/// until then); nothing in this crate's own pipeline needs it otherwise.
#[derive(Deserialize, Clone)]
pub struct ColumnDescription {
    pub name: String,
    #[serde(default)]
    pub type_name: Option<String>,
    /// Only present for `type_name == "DECIMAL"` -- needed by
    /// `json_convert`'s INLINE/JSON_ARRAY-to-Arrow conversion to build a
    /// correctly-scaled `Decimal128Array`. Absent (and unused) for every
    /// other type, including on the Arrow-IPC path, which gets its decimal
    /// precision/scale from the IPC schema itself, not from here.
    #[serde(default)]
    pub type_precision: Option<u8>,
    #[serde(default)]
    pub type_scale: Option<i8>,
    /// Only present for `type_name == "STRUCT"` -- a recursive SQL DDL
    /// rendering (e.g. `"STRUCT<a: BIGINT NOT NULL, b: STRING>"`), the only
    /// place the manifest exposes a STRUCT's own field names/types; used by
    /// `json_convert`'s INLINE/JSON_ARRAY-to-Arrow conversion to build the
    /// nested `StructArray`. Note this uses SQL DDL type spelling
    /// (`BIGINT`/`TINYINT`/`SMALLINT`), not this same manifest's own
    /// top-level `type_name` vocabulary (`LONG`/`BYTE`/`SHORT`) -- confirmed
    /// against a real workspace, not assumed; `json_convert::parse_one_field`
    /// remaps the three that differ.
    #[serde(default)]
    pub type_text: Option<String>,
}

// ---- Cancellation + observability (2026-08-11 design doc) --------------

/// Enough state to fire a best-effort server-side cancel against one
/// in-flight statement/operation -- carried by `ResultStream`
/// (`pipeline/sea.rs`)/`NdjsonStream` (`pipeline/ndjson.rs`) so
/// `heartbeat.rs`'s two existing timeout/cancellation
/// detection points (`tick()`'s `total_timeout_s` branch, and
/// `Drop for HeartbeatWait`/`Drop for HeartbeatStream`) can fire
/// `DbClient::cancel_statement` without either of those generic structs
/// needing to know anything protocol-specific themselves.
#[derive(Clone, Debug)]
pub enum CancelHandle {
    Sea { statement_id: String },
    Thrift { operation: thrift::OperationHandle },
}

/// Per-query counters, accumulated across this crate's own retryable network
/// calls and cloud-fetch downloads regardless of whether an `on_event`
/// callback is actually registered to ever read them -- a handful of atomic
/// increments per request is cheap enough not to bother gating behind
/// `on_event.is_some()` (unlike the actual callback dispatch itself, which
/// is skipped entirely when there's nothing to receive it -- see
/// `DbClient::on_event`/`QueryStats`' own doc comment in `lib.rs`).
///
/// `outcome` doubles as the signal `Drop`-based abandonment detection (in
/// `pipeline/stats.rs`'s `PoisonOnDrop`/`ReportOnDrop`) uses to tell a
/// `total_timeout_s` timeout apart from a bare
/// `task.cancel()`/`asyncio.wait_for`-drop, both of which sever the future
/// mid-`.await` with no other way for code running *inside* that future's
/// own `Drop` impl to tell them apart: whichever of `heartbeat.rs`'s two
/// trigger points fires writes "timeout" or "cancelled" into this *before*
/// aborting the task, since that abort is what makes the future's own `Drop`
/// impl run in the first place -- by the time it runs, it's too late to
/// signal anything through the (already being torn down) future itself. Set
/// at most once (`store_outcome_if_unset`) -- the explicit `finish("error")`/
/// `finish("success")` calls on the normal-return paths race the same slot,
/// so the first writer wins and every later one is a no-op.
#[derive(Default, Debug)]
pub struct QueryStatsAccumulator {
    pub bytes_downloaded: AtomicU64,
    pub retry_count: AtomicU32,
    /// Only meaningfully incremented by the Thrift backend, which -- unlike
    /// SEA -- doesn't know its chunk count upfront from a manifest; SEA's
    /// `QueryStats.num_chunks` is taken directly from `chunk_metas.len()`
    /// instead (see `pipeline/stats.rs`'s `StatsReporter`).
    pub chunks_seen: AtomicUsize,
    /// `f64` bits (`f64::to_bits`/`from_bits`) -- accumulated (not simply
    /// overwritten) via `add_warehouse_wait_s`, so a query that calls
    /// `ensure_warehouse_running` more than once (SEA's `submit_and_poll`
    /// runs it exactly once per call, but `execute_arrow_statement_prefer_inline`'s
    /// own fallback -- and `pipeline/sea.rs`'s separate JSON-conversion
    /// fallback in `execute_lazy_prefer_inline` -- can mean `submit_and_poll` itself
    /// runs twice for one logical query) still reports the *total* time
    /// actually spent inside it, not just the last call's (typically ~0,
    /// since the warehouse-running cache is warm by the second call).
    /// Found in code review: an earlier version had `pipeline.rs` time this
    /// externally, once per call site, *in addition to* `submit_and_poll`'s
    /// own unconditional internal call -- not only duplicated the same
    /// boilerplate 4x, but called `ensure_warehouse_running` twice per query
    /// for no reason, and silently lost the timing (and any `on_event`
    /// dispatch at all) whenever the external call's own error path didn't
    /// bother checking it. `submit_and_poll`/`submit_thrift_and_start_fetch`
    /// are now the *only* callers, each reporting into this field exactly
    /// once per attempt, so a caller of e.g. `execute_arrow_statement` just
    /// reads `stats.warehouse_wait_s()` back afterward instead of timing
    /// anything itself.
    warehouse_wait_bits: AtomicU64,
    outcome: AtomicU8,
    /// The statement/operation handle while it's submitted but not yet
    /// terminal -- set by the submit/poll loops (`client/sea.rs`'s
    /// `submit_and_poll_inner`, `pipeline/thrift_exec.rs`'s
    /// `submit_and_await_thrift_statement`) as soon as Databricks hands one
    /// back, cleared once the statement reaches a terminal state. Whatever
    /// is still here when `pipeline/stats.rs`'s `CancelInFlightOnDrop`
    /// drops (the submit future was abandoned -- a `total_timeout_s`, a
    /// Python-side `task.cancel()`/`asyncio.wait_for` -- or a poll failed
    /// mid-wait) gets a best-effort server-side cancel, so a query nobody
    /// is waiting for anymore stops running on the warehouse.
    in_flight: Mutex<Option<CancelHandle>>,
}

const OUTCOME_UNSET: u8 = 0;
const OUTCOME_CANCELLED: u8 = 1;
const OUTCOME_TIMEOUT: u8 = 2;

impl QueryStatsAccumulator {
    /// Called from `heartbeat.rs`'s two trigger points, before they abort
    /// whatever task/future is running this query's fetch -- see this
    /// struct's own doc comment for why the ordering matters.
    pub fn store_outcome_if_unset(&self, timeout: bool) {
        let want = if timeout { OUTCOME_TIMEOUT } else { OUTCOME_CANCELLED };
        let _ = self
            .outcome
            .compare_exchange(OUTCOME_UNSET, want, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Read by `pipeline/stats.rs`'s `StatsReporter::drop` -- `None` means neither
    /// trigger ever fired, so the abandonment it's reacting to must be a bare
    /// cancellation with no `heartbeat.rs` wrapper in play at all (e.g. a
    /// caller doing `asyncio.wait_for(cursor.fetchall(), ...)` directly,
    /// with no `total_timeout_s`/`_streamed` variant involved) -- the caller
    /// defaults that case to `"cancelled"` itself, rather than this method
    /// guessing.
    pub fn pending_outcome(&self) -> Option<&'static str> {
        match self.outcome.load(Ordering::Acquire) {
            OUTCOME_CANCELLED => Some("cancelled"),
            OUTCOME_TIMEOUT => Some("timeout"),
            _ => None,
        }
    }

    /// Called exactly once per `ensure_warehouse_running` call this query
    /// actually makes -- see `warehouse_wait_bits`'s own doc comment for why
    /// this accumulates instead of overwriting. Not contended in practice
    /// (`submit_and_poll`/`submit_thrift_and_start_fetch` call this
    /// strictly sequentially, never concurrently, for one query), but a
    /// CAS loop is used anyway since `AtomicU64` has no native float-add.
    pub fn add_warehouse_wait_s(&self, seconds: f64) {
        let mut current = self.warehouse_wait_bits.load(Ordering::Relaxed);
        loop {
            let new = f64::from_bits(current) + seconds;
            match self.warehouse_wait_bits.compare_exchange_weak(
                current,
                new.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn warehouse_wait_s(&self) -> f64 {
        f64::from_bits(self.warehouse_wait_bits.load(Ordering::Relaxed))
    }

    pub fn set_in_flight(&self, handle: CancelHandle) {
        *self.in_flight.lock().unwrap() = Some(handle);
    }

    pub fn clear_in_flight(&self) {
        *self.in_flight.lock().unwrap() = None;
    }

    pub fn take_in_flight(&self) -> Option<CancelHandle> {
        self.in_flight.lock().unwrap().take()
    }
}

/// One query's worth of timing/counters, handed to `EventSink::on_event`
/// exactly once, at completion -- see `lib.rs`'s `QueryStats` (the
/// PyO3-exposed shape this converts into) for the full field-by-field
/// rationale.
#[derive(Debug, Clone)]
pub struct QueryStatsData {
    pub statement_id: String,
    pub protocol: &'static str,
    pub warehouse_wait_s: f64,
    pub submit_to_ready_s: f64,
    pub fetch_s: f64,
    pub num_chunks: usize,
    pub bytes_downloaded: u64,
    pub retry_count: u32,
    pub concurrency_used: usize,
    pub outcome: &'static str,
}

/// Bridges a Python `on_event` callable (sync or async, matching
/// `token_provider`'s own `PyTokenProvider`/`TokenProvider` pattern in
/// `lib.rs`) into Rust's `client::EventSink` trait. Kept generic (no PyO3
/// here) for the same reason `TokenProvider` is -- the PyO3-specific
/// dispatch lives in `lib.rs`.
///
/// **Fire-and-forget by construction, not by convention**: unlike
/// `TokenProvider::get_token` (awaited inline, so a broken token provider
/// correctly surfaces to the caller), `on_event` must never affect the
/// query it describes -- a slow or raising callback must be invisible to
/// both correctness and latency. `on_event` is therefore a plain, *sync*
/// method: an implementation spawns the actual (possibly async, possibly
/// slow, possibly raising) dispatch onto the background runtime and returns
/// immediately, exactly the same "spawn, don't await" shape as
/// `DbClient::cancel_statement`'s own fire-and-forget RPC.
pub trait EventSink: Send + Sync {
    fn on_event(&self, stats: QueryStatsData);
}

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub chunk_index: i64,
    pub row_count: Option<i64>,
    /// Set when the statement submit/poll response already embedded one or
    /// more of this chunk's presigned URLs (see `StatementResponseBody::result`)
    /// -- the fetch worker downloads them directly instead of first resolving
    /// via `GET .../result/chunks/{i}`. `Vec`, not `Option<String>` -- a
    /// `chunk_index` can carry more than one blob (same reason
    /// `fetch_chunk_index` returns `Vec<Bytes>` and `ReorderBuffer` keys on
    /// `VecDeque`, not a single item: collapsing to one would silently drop
    /// every link but the last for the same index). Empty means "not
    /// pre-resolved, fetch it the normal way."
    pub pre_resolved_links: Vec<String>,
}

/// What submitting a statement gets you before any chunk is fetched:
/// `statement_id` (needed to resolve chunk links), `chunk_metas` (what
/// `fetch_chunks_with_backpressure` needs), and `columns` -- the manifest's
/// own (name, type_name) pairs, used only for `Cursor.description`'s
/// pre-fetch fallback (the real Arrow schema isn't known until a chunk has
/// actually been decoded).
pub struct StatementSubmitResult {
    pub statement_id: String,
    pub chunk_metas: Vec<ChunkMeta>,
    pub columns: Vec<ColumnDescription>,
    /// Whether the server confirmed `LZ4_FRAME` cloud-fetch compression is in
    /// effect for this statement's chunks -- see `ManifestBody::result_compression`.
    pub compressed: bool,
}

/// What `execute_arrow_statement_prefer_inline` gets you -- either the whole
/// result inline as raw JSON_ARRAY rows (every non-null value a string,
/// Databricks' own JSON_ARRAY contract; converting these into a real
/// `RecordBatch` is `pipeline/sea.rs`'s job via `json_convert`, not this
/// Arrow-agnostic client module's -- see `client.rs`'s own module doc
/// comment), or a normal `StatementSubmitResult` to fetch chunks for exactly as if
/// `prefer_inline` had never been asked for. See that function's own doc
/// comment for when each happens.
pub enum InlineOrExternal {
    Inline {
        statement_id: String,
        rows: Vec<Vec<Option<String>>>,
        columns: Vec<ColumnDescription>,
    },
    External(StatementSubmitResult),
}

#[derive(Debug)]
pub struct ChunkItem {
    /// `Bytes` (not `Vec<u8>`) so the bytes reqwest already received off the
    /// socket ride all the way to the Arrow-IPC decoder with zero copies --
    /// `Bytes` is a refcounted view over the same allocation, cheap to clone
    /// and to hand across the `spawn_blocking` boundary in this crate's
    /// `pipeline` module (e.g. `pipeline/reorder.rs`'s `decode_chunk_item`).
    pub blob: Bytes,
    pub row_count: Option<i64>,
    pub chunk_index: i64,
    /// A declared row-count bound this chunk's own decoded batches must be
    /// sliced down to if they exceed it, since a cloud-fetch file can
    /// legitimately contain more rows than its own declared count for a
    /// `LIMIT`-bounded query (see `pipeline/reorder.rs`'s `decode_chunk_item`).
    /// Every real producer sets this now -- Thrift `resultLinks`
    /// (`pipeline/thrift_exec.rs`'s `fetch_thrift_link`), Thrift's inline
    /// `arrowBatches` (`pipeline/thrift_exec.rs`'s `run_thrift_fetch_loop`),
    /// and SEA's own chunk fetch (`client/sea.rs`'s
    /// `fetch_chunks_with_backpressure`, though only when a `chunk_index`
    /// resolves to exactly one blob -- see that function's own comment for
    /// why more than one blob is left untruncated). `None` means no
    /// authoritative bound is known, not "this chunk was never overshot" --
    /// `decode_chunk_item` treats it as a no-op either way.
    pub truncate_to: Option<i64>,
}
