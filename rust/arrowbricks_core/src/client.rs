//! Port of `client.py`'s statement submit/poll/retry logic. No Arrow
//! dependency here on purpose, same reasoning as the Python original: chunk
//! bytes are handed off raw, decoding happens in `pipeline.rs`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::thrift;

/// Typed response shapes -- replaces navigating a dynamic `serde_json::Value`
/// tree with `.get("...").and_then(|v| v.as_str())` chains everywhere. Same
/// data, but serde deserializes straight into these instead of building an
/// intermediate `Value` tree first; matters most for a large manifest (a
/// result with thousands of chunks), which otherwise means allocating a
/// generic map/array node per chunk before ever extracting `chunk_index`.
#[derive(Deserialize)]
struct WarehouseStatusBody {
    state: String,
}

#[derive(Deserialize)]
struct SessionCreateBody {
    session_id: String,
}

#[derive(Deserialize)]
struct StatementStatusBody {
    state: String,
    #[serde(default)]
    error: Option<StatementErrorBody>,
}

#[derive(Deserialize)]
struct StatementErrorBody {
    #[serde(default)]
    error_code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Deserialize)]
struct ChunkMetaRaw {
    chunk_index: i64,
    #[serde(default)]
    row_count: Option<i64>,
}

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

#[derive(Deserialize, Default)]
struct ManifestSchemaBody {
    #[serde(default)]
    columns: Vec<ColumnDescription>,
}

#[derive(Deserialize, Default)]
struct ManifestBody {
    #[serde(default)]
    chunks: Vec<ChunkMetaRaw>,
    #[serde(default)]
    schema: Option<ManifestSchemaBody>,
    /// Echoes back whether the server actually honored our
    /// `result_compression: "LZ4_FRAME"` request (see `execute_statement`) --
    /// decompression is driven by this, not by what we asked for, in case a
    /// disposition/format combination ever doesn't honor it.
    #[serde(default)]
    result_compression: Option<String>,
}

#[derive(Deserialize)]
struct StatementResponseBody {
    statement_id: String,
    status: StatementStatusBody,
    #[serde(default)]
    manifest: Option<ManifestBody>,
    /// SEA embeds whichever chunks are already ready straight in the
    /// submit/poll response body -- confirmed against a real workspace: a
    /// SUCCEEDED response's `result.external_links` already contained chunk
    /// 0's presigned URL, disposition EXTERNAL_LINKS same as always. Used to
    /// skip that chunk's own `GET .../result/chunks/{i}` resolution request
    /// entirely (see `ChunkMeta::pre_resolved_links`) -- a full round trip
    /// saved for whichever chunks land here, which for a fast/small query is
    /// a meaningful fraction of total latency.
    #[serde(default)]
    result: Option<ResultBody>,
}

#[derive(Deserialize)]
struct ResultLinkBody {
    // `#[serde(default)]` (not `Option<i64>`) deliberately: an omitempty-style
    // server serializer would drop a zero-valued `chunk_index` field entirely
    // rather than emit `0` -- exactly the chunk this optimization targets
    // most (chunk 0), and exactly the case that would otherwise turn one
    // optional fast-path field into a hard parse failure for the *whole*
    // statement response (`authed_json` fails the entire `StatementResponseBody`
    // deserialize on any missing required field, no retry). Defaulting to 0
    // reconstructs the omitted value correctly either way.
    #[serde(default)]
    chunk_index: i64,
    // Same reasoning, plus an empty link is filtered out where this is
    // consumed (`execute_statement`) rather than trusted -- a link that
    // somehow came through empty is worse than just re-resolving normally.
    #[serde(default)]
    external_link: String,
}

#[derive(Deserialize, Default)]
struct ResultBody {
    #[serde(default)]
    external_links: Vec<ResultLinkBody>,
    /// Only present for `disposition: INLINE` + `format: JSON_ARRAY` -- each
    /// row a `Vec<Option<String>>` (every non-null value a string,
    /// Databricks' own JSON_ARRAY contract). Consumed by
    /// `json_convert::json_array_to_record_batch` on the `prefer_inline`
    /// fast path.
    #[serde(default)]
    data_array: Option<Vec<Vec<Option<String>>>>,
}

#[derive(Deserialize)]
struct ExternalLinkBody {
    external_link: String,
}

#[derive(Deserialize, Default)]
struct ChunkLinksBody {
    #[serde(default)]
    external_links: Vec<ExternalLinkBody>,
}

/// Bearer token source -- either a static string or a caller-supplied
/// callback, matching Python's `token: str | None` / `token_provider:
/// Callable[[], str | Awaitable[str]] | None`. Kept generic (no PyO3 here)
/// so this module stays Python-agnostic, same reasoning as its own module
/// doc comment; the PyO3-specific bridging for a Python callable lives in
/// `lib.rs`. Called on every request, no caching here -- matches
/// `_bearer_token`'s own contract ("if your provider is expensive to call,
/// cache/refresh inside it").
pub type TokenFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ApiError>> + Send>>;

pub trait TokenProvider: Send + Sync {
    fn get_token(&self) -> TokenFuture;
}

struct StaticToken(String);

impl TokenProvider for StaticToken {
    fn get_token(&self) -> TokenFuture {
        let token = self.0.clone();
        Box::pin(async move { Ok(token) })
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const RETRY_ATTEMPTS: u32 = 6;
const RETRY_MAX_WAIT_S: f64 = 20.0;

/// Coarse classification of *why* an `ApiError` happened, carried across the
/// PyO3 boundary (`lib.rs`'s `api_error_to_pyerr`) so a Python caller can
/// programmatically distinguish "safe to retry" from "don't retry" from "auth
/// problem" instead of regex-parsing `.message` text -- see AGENTS.md's
/// "typed error taxonomy" entry (2026-08-11) for the design discussion.
/// Deliberately small: `transient` (already on `ApiError`) already answers
/// "retryable", so this only adds the two distinctions that need a
/// *different* caller response than a plain retry -- `Auth` (refresh the
/// credential, don't just retry the same one) and `Statement` (the SQL
/// itself failed/was canceled server-side, retrying identically will fail
/// identically). Everything else (network blips, generic parse/decode
/// failures, internal invariant violations) stays `Other` -- `lib.rs` maps
/// `Other` + `transient` to a `TransientError`, `Other` + `!transient` to the
/// plain `ArrowbricksError` base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApiErrorKind {
    #[default]
    Other,
    /// HTTP 401/403 -- see `from_status`. Only set there; every other
    /// construction site (including `from_reqwest`, which has no HTTP status
    /// to inspect at all) leaves this as the `Other` default.
    Auth,
    /// A Databricks statement reached a terminal FAILED/CANCELED state (SEA)
    /// or an operation's `terminal_error()` fired (Thrift) -- see
    /// `ApiError::statement_failed`. Not used for a Thrift RPC's own
    /// transport-level `TStatus` error (e.g. `FetchResults` returning
    /// `INVALID_HANDLE`), which stays `Other` -- that's a protocol/transport
    /// problem, not necessarily evidence the *statement itself* failed.
    Statement,
}

#[derive(Debug)]
pub struct ApiError {
    pub message: String,
    /// 401/403/408/429/5xx -- mirrors `_is_transient_error`. Transport-level
    /// timeouts/connect errors are never transient, same reasoning as the
    /// Python original: a genuinely stalled connection should fail fast on
    /// the caller's own timeout, not be retried here.
    pub transient: bool,
    /// See `ApiErrorKind`'s own doc comment. Defaults to `Other` at every
    /// construction site that doesn't explicitly classify further (the vast
    /// majority -- internal parse/decode/invariant errors have no auth or
    /// statement-failure meaning).
    pub kind: ApiErrorKind,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for ApiError {}

impl ApiError {
    pub(crate) fn permanent(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            transient: false,
            kind: ApiErrorKind::Other,
        }
    }

    /// A Databricks statement/operation reached a terminal FAILED/CANCELED
    /// state -- see `ApiErrorKind::Statement`'s own doc comment for exactly
    /// which call sites use this vs. plain `permanent`.
    pub(crate) fn statement_failed(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            transient: false,
            kind: ApiErrorKind::Statement,
        }
    }

    /// `idempotent` must be `true` only when replaying the *whole* request is
    /// actually safe -- a GET, or a PUT/DELETE whose retried body is byte-for-
    /// byte the same operation either way (`upload_volume_file`'s
    /// overwrite=true PUT, `delete_volume_file`'s DELETE). It must be `false`
    /// for the statement-submit POST: a decode/mid-flight-send failure there
    /// means the server may have already accepted and started executing the
    /// statement before the response broke -- for arbitrary caller SQL
    /// (INSERT/MERGE/COPY INTO), blindly replaying that POST risks a second,
    /// duplicate execution, not just a duplicate read. `is_decode()` alone
    /// isn't the whole idempotent-failure surface either -- `is_request()`
    /// (excluding `is_connect()`/`is_timeout()`, already handled) also covers
    /// hyper's own "connection closed before message completed" pooled-
    /// connection-reuse race, which fires before any body is even read and
    /// is equally safe to retry on an idempotent request. Unlike `is_decode()`
    /// (hit for real on a 400-chunk fetch and reproduced with a directed
    /// test), this specific race is reasoned from reqwest/hyper's own source,
    /// not independently reproduced -- a directed attempt to force it
    /// self-healed 20/20 times (hyper's own idle-connection health check
    /// evidently detects a closed peer and opens a fresh connection before
    /// ever handing the dead one out for reuse, at least under a simple,
    /// low-concurrency test). Kept anyway since it's safe regardless (scoped
    /// to idempotent requests only) and the window may still be reachable
    /// under real production concurrency even though a quick local test
    /// couldn't force it -- treat as a plausible defensive measure, not a
    /// confirmed fix for an observed failure.
    fn from_reqwest(e: reqwest::Error, idempotent: bool) -> Self {
        // `is_decode()` (reqwest's `Kind::Decode`) isn't only content-decoding
        // -- `Response::bytes()`/`.text()`/`.json()` also wrap a body read
        // that fails mid-stream (connection reset, truncated transfer) in
        // the same `Kind::Decode`, message "error decoding response body"
        // (see reqwest's `async_impl::response::Response::do_bytes` and
        // `error::decode`). Confirmed against a real workspace: a 400-chunk/
        // 5.6M-row fetch hit exactly this on one of many large concurrent
        // blob downloads, and it reproduced identically before this change
        // existed -- a genuine transient network blip, not a permanent
        // problem with the request. Connect/timeout errors stay non-transient,
        // unchanged from before -- those mean the endpoint genuinely isn't
        // responding, where failing fast on the caller's own timeout is still
        // right.
        let transient = idempotent && (e.is_decode() || (e.is_request() && !e.is_connect() && !e.is_timeout()));
        Self {
            message: e.to_string(),
            transient,
            kind: ApiErrorKind::Other,
        }
    }

    /// `idempotent` only gates the 5xx case -- 401/403/408/429 mean the
    /// request was rejected before any processing started (auth failure,
    /// rate limit, client-side timeout), safe to retry regardless of method.
    /// A 5xx is murkier: it usually means the same, but it can also mean the
    /// backend already accepted and started the statement before some later
    /// failure (e.g. a gateway timeout) produced the 5xx anyway -- found in
    /// code review that this call was unconditionally transient even for the
    /// statement-submit POST, bypassing the exact idempotency reasoning
    /// `from_reqwest`'s `idempotent` param exists for (retrying that POST
    /// risks a second, duplicate execution of arbitrary caller SQL).
    fn from_status(status: StatusCode, body: &str, idempotent: bool) -> Self {
        let transient = matches!(status.as_u16(), 401 | 403 | 408 | 429) || (idempotent && status.is_server_error());
        let kind = if matches!(status.as_u16(), 401 | 403) {
            ApiErrorKind::Auth
        } else {
            ApiErrorKind::Other
        };
        Self {
            message: format!("HTTP {status}: {body}"),
            transient,
            kind,
        }
    }
}

/// Converts a `JoinError` (a spawned task panicked, or was cancelled) into an
/// `ApiError` instead of letting it be silently dropped. Shared by both the
/// fetch-worker join in `fetch_chunks_with_backpressure` and the decode
/// `spawn_blocking` join in `pipeline.rs` -- a panicking task must surface as
/// an error, not as a quietly-truncated result set.
pub(crate) fn join_error(e: tokio::task::JoinError) -> ApiError {
    ApiError {
        message: format!("task panicked: {e}"),
        transient: false,
        kind: ApiErrorKind::Other,
    }
}

// ---- Cancellation + observability (2026-08-11 design doc) --------------

/// Enough state to fire a best-effort server-side cancel against one
/// in-flight statement/operation -- carried by `ResultStream`/`NdjsonStream`
/// (`pipeline.rs`) so `heartbeat.rs`'s two existing timeout/cancellation
/// detection points (`tick()`'s `total_timeout_s` branch, and
/// `Drop for HeartbeatWait`/`Drop for HeartbeatStream`) can fire
/// `DbClient::cancel_statement` without either of those generic structs
/// needing to know anything protocol-specific themselves.
#[derive(Clone)]
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
/// `pipeline.rs`) uses to tell a `total_timeout_s` timeout apart from a bare
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
    /// instead (see `pipeline.rs`'s `StatsReporter`).
    pub chunks_seen: AtomicUsize,
    /// `f64` bits (`f64::to_bits`/`from_bits`) -- accumulated (not simply
    /// overwritten) via `add_warehouse_wait_s`, so a query that calls
    /// `ensure_warehouse_running` more than once (SEA's `submit_and_poll`
    /// runs it exactly once per call, but `execute_arrow_statement_prefer_inline`'s
    /// own fallback -- and `pipeline.rs`'s separate JSON-conversion fallback
    /// in `execute_lazy_prefer_inline` -- can mean `submit_and_poll` itself
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

    /// Read by `pipeline.rs`'s `StatsReporter::drop` -- `None` means neither
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
            match self
                .warehouse_wait_bits
                .compare_exchange_weak(current, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn warehouse_wait_s(&self) -> f64 {
        f64::from_bits(self.warehouse_wait_bits.load(Ordering::Relaxed))
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
/// `RecordBatch` is `pipeline.rs`'s job via `json_convert`, not this
/// Arrow-agnostic module's -- see this file's own module doc comment), or a
/// normal `StatementSubmitResult` to fetch chunks for exactly as if
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
    /// and to hand across the `spawn_blocking` boundary in pipeline.rs.
    pub blob: Bytes,
    pub row_count: Option<i64>,
    pub chunk_index: i64,
    /// A declared row-count bound this chunk's own decoded batches must be
    /// sliced down to if they exceed it, since a cloud-fetch file can
    /// legitimately contain more rows than its own declared count for a
    /// `LIMIT`-bounded query (see `pipeline.rs`'s `decode_chunk_item`).
    /// Every real producer sets this now -- Thrift `resultLinks`
    /// (`pipeline.rs`'s `fetch_thrift_link`), Thrift's inline `arrowBatches`
    /// (`pipeline.rs`'s `run_thrift_fetch_loop`), and SEA's own chunk fetch
    /// (`fetch_chunks_with_backpressure`, below, though only when a
    /// `chunk_index` resolves to exactly one blob -- see that function's own
    /// comment for why more than one blob is left untruncated). `None`
    /// means no authoritative bound is known, not "this chunk was never
    /// overshot" -- `decode_chunk_item` treats it as a no-op either way.
    pub truncate_to: Option<i64>,
}

/// Unwraps one downloaded chunk file's outer LZ4 Frame compression --
/// server-side, this is a whole-file wrapper applied because we asked for
/// `result_compression: "LZ4_FRAME"`, not Arrow-IPC's own (unrelated,
/// per-buffer) compression option. `reqwest::Response::bytes()` already
/// collected the full body reliably before this runs, so a decode failure
/// here means a genuine format problem, not a network blip -- treated as
/// permanent (not retried), same reasoning as `ApiError::from_reqwest`.
///
/// A real Databricks chunk is not one LZ4 frame -- it's several concatenated
/// back to back (confirmed against a real workspace: a 50k-row/1MB chunk came
/// back as 18 separate frames). `FrameDecoder::read_to_end` stops the moment
/// it hits the first frame's own end marker; calling it once silently
/// produced only that first frame's ~200 bytes (just the Arrow schema
/// message, no data) instead of the full stream, which `pipeline.rs` then
/// decoded as a zero-row/zero-column result with no error at all -- this bug
/// shipped from a design that was only ever verified against synthetic
/// single-frame test data. One `FrameDecoder` resets its own frame state
/// after each `EndMark` and picks up the next concatenated frame on a
/// subsequent `read_to_end` call against the *same* instance (verified: the
/// decoder's position in the underlying byte slice carries over across
/// calls) -- so looping `read_to_end` on one decoder until it stops growing
/// `out` reads every frame without reconstructing a decoder per frame.
pub(crate) fn decompress_lz4_frame(compressed: &Bytes) -> Result<Bytes, ApiError> {
    use std::io::Read;
    // `compressed.len()` is a real lower bound, but LZ4 on Arrow-IPC data
    // (long dictionary/offset-buffer runs, mostly-repeated bytes) typically
    // compresses several-fold -- estimating just the lower bound means the
    // real decompressed size almost always blows past initial capacity,
    // paying for repeated doubling-and-copy growth on every chunk. `* 4` is
    // a heuristic, not a guarantee (`Vec` still grows normally if it's wrong
    // either way) -- just a better starting point than the guaranteed-too-
    // small lower bound.
    let mut out = Vec::with_capacity(compressed.len() * 4);
    let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
    // Terminate on the *reader* being exhausted, not on "output stopped
    // growing" -- found in code review that a frame which happens to decode
    // to zero bytes (a real, valid LZ4 Frame shape: header + immediate
    // EndMark) makes one `read_to_end` call return `Ok(0)` for that frame
    // without erroring and without necessarily advancing into the next
    // frame yet, which the old `out.len() == before` check read as "no more
    // frames" -- silently dropping every subsequent concatenated frame with
    // no error at all. Same silent-truncation shape as the original
    // multi-frame bug this loop exists to fix. Looping while the reader
    // still has bytes left (regardless of whether the last call grew `out`)
    // is the correct fix -- verified against a zero-content frame sandwiched
    // between two real ones, see `decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle`.
    while !decoder.get_ref().is_empty() {
        decoder
            .read_to_end(&mut out)
            .map_err(|e| ApiError::permanent(format!("LZ4 frame decompress failed: {e}")))?;
    }
    Ok(Bytes::from(out))
}

/// Which wire protocol/backend `execute()` talks to Databricks with -- a
/// choice on `Client`/`DatabricksClient` (`protocol: "sea" | "thrift"`),
/// **`Thrift` is the default as of the benchmarking work documented in
/// AGENTS.md's own design-invariant entry** (SEA remains fully supported,
/// explicit `protocol="sea"`) -- it speaks the same HiveServer2-compatible
/// `TCLIService` protocol `databricks-sql-connector` uses by *default*
/// (when its own `use_sea` isn't set) -- plain HTTPS POST,
/// `TBinaryProtocol`-encoded, no framing beyond HTTP itself (see `thrift.rs`).
/// Measurably faster than this crate's own SEA path for small queries
/// (closing the remaining gap `prefer_inline`/SEA-session-pooling didn't --
/// see those entries' own closing notes in `AGENTS.md`), primarily because
/// `TExecuteStatementReq`'s `getDirectResults` can return a small result's
/// data inline in the *same* RPC that submits the statement, where SEA
/// always needs at least a separate poll/fetch round trip. Never slower
/// than SEA on any query shape tested, real or mocked.
///
/// `DbClient::new`'s own internal struct literal initializes `protocol:
/// Protocol::Thrift` too, matching this crate's real user-facing default one
/// layer up (`lib.rs`'s `PyDbClient::new` `#[pyo3(signature = ...)]` and
/// `client.py`'s `DatabricksClient.__init__`, which both default their own
/// `protocol` kwarg to `"thrift"`) -- deliberately kept as one single
/// default rather than two independently-set ones that happened to agree:
/// an earlier version of this had `DbClient::new` default to `Protocol::Sea`
/// while only the PyO3/Python layer defaulted to `"thrift"`, on the
/// reasoning that Rust-only callers (this crate's own test suite) always
/// call `.with_protocol` explicitly anyway -- found in review that this
/// left a real, if narrow, foot-gun for any *future* Rust-only caller who
/// constructs a `DbClient` directly and forgets to call `.with_protocol`,
/// silently getting SEA while believing they're on the new default. Every
/// SEA-testing call site in this crate's own test suite already sets
/// `.with_protocol(Protocol::Sea)` explicitly (see `tests/wiremock_pipeline.rs`),
/// so making this the same default as the public-facing one costs nothing
/// and removes the divergence entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Sea,
    Thrift,
}

impl Protocol {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "sea" => Ok(Protocol::Sea),
            "thrift" => Ok(Protocol::Thrift),
            other => Err(format!("unknown protocol {other:?} -- expected \"sea\" or \"thrift\"")),
        }
    }
}

pub struct DbClient {
    http: Client,
    host: String,
    warehouse_id: String,
    token_provider: Arc<dyn TokenProvider>,
    http_timeout: Duration,
    wait_timeout: String,
    pub chunk_fetch_concurrency: usize,
    warehouse_start_timeout: Duration,
    warehouse_confirmed_running_ttl: Duration,
    warehouse_confirmed_running_at: Mutex<Option<Instant>>,
    /// Whether to request `result_compression: "LZ4_FRAME"` cloud-fetch
    /// compression on every statement -- see `execute_statement`. Runtime
    /// toggle (not a compile-time constant) so a caller who wants to
    /// benchmark or rule out compression as a variable doesn't need to
    /// rebuild the extension to do it. Also doubles, on the Thrift path, as
    /// `TExecuteStatementReq.canDecompressLZ4Result`.
    compress_results: bool,
    session_pool: Pool<String>,
    pub protocol: Protocol,
    /// Same checkout/checkin shape as `session_pool` above, plus: Thrift's
    /// session model is *mandatory* per statement, unlike SEA's optional
    /// `session_id`, and this crate has not independently confirmed whether
    /// concurrent statements on one Thrift session are safe against a real
    /// workspace the way the SEA crash was -- so this pool exists
    /// defensively, following the same proven-safe pattern regardless of
    /// whether the analogous crash reproduces here. Unlike SEA, there is no
    /// "sessionless" fallback available at all -- `TExecuteStatementReq.
    /// sessionHandle` is required by the protocol -- so a pool-exhaustion/
    /// creation-failure `None` from `thrift_checkout_session` means the
    /// caller must open one throwaway session for that single call and
    /// close it again immediately after (see `execute_lazy_thrift`).
    thrift_session_pool: Pool<thrift::SessionHandle>,
    /// Global budget for concurrent cloud-fetch HTTP requests, sized to
    /// `chunk_fetch_concurrency`. Every link download takes one permit; a
    /// download that finds spare permits (few links in flight -- exactly
    /// what a single-chunk result hits, leaving a lone TCP stream idle for
    /// most of the link) also claims up to `MAX_SPLIT_PARTS - 1` extra ones
    /// and splits itself into that many parallel HTTP Range requests. When
    /// many links are already in flight (a large multi-chunk result), the
    /// budget is exhausted and downloads fall back to one stream each --
    /// exactly the shape the existing worker pool was already tuned for, so
    /// this never regresses that case. Measured against a real warehouse:
    /// a 10k-row/1-link query went from a 1299ms median to 672ms; a
    /// 300k-row/19-link query was neutral (8552ms vs 8561ms baseline).
    download_slots: tokio::sync::Semaphore,
    /// Optional observability callback, attached once at construction --
    /// same attachment point as `token_provider`. `None` (the default) means
    /// this feature does nothing at all beyond the cheap, always-on
    /// `QueryStatsAccumulator` counters every query already carries (see
    /// that struct's own doc comment for why those aren't gated too).
    on_event: Option<Arc<dyn EventSink>>,
    /// Runtime-configurable counterparts to the old compile-time
    /// `RETRY_ATTEMPTS`/`RETRY_MAX_WAIT_S` constants (still the defaults --
    /// see `with_token_provider`) -- read by `retry_call`/`retry_call_tracked`
    /// below instead of the constants directly, so a caller can tune retry
    /// behavior (e.g. fail fast in a latency-sensitive path, or retry harder
    /// against a flaky link) without a rebuild. Second instance of the exact
    /// "Rust constant -> `PyDbClient::new` pyo3 default -> `client.py` kwarg
    /// default -> `_core.pyi` stub" threading pattern `chunk_fetch_concurrency`
    /// already uses -- see AGENTS.md's own entry on that one for the footgun
    /// (a default that's silently unused because a different layer's default
    /// always wins) this must not repeat.
    retry_attempts: u32,
    retry_max_wait_s: f64,
}

/// Upper bound on how many parallel Range requests one cloud-fetch link is
/// ever split into -- see `DbClient::download_slots`.
pub const MAX_SPLIT_PARTS: usize = 8;

/// `chunk_fetch_concurrency`'s own default -- see `DbClient::new`'s own
/// comment on it for the measured numbers behind picking 64. Named so
/// `download_slots` (sized to this same number, since it's a budget over
/// the same worker count) can't drift from it the way a second bare `64`
/// silently could.
const DEFAULT_CHUNK_FETCH_CONCURRENCY: usize = 64;

/// How long the Thrift path polls `GetOperationStatus` when a statement
/// doesn't finish within its `getDirectResults` budget (see
/// `execute_lazy_thrift`) -- shorter than SEA's `POLL_INTERVAL` (2s) since
/// this path exists specifically to be fast for small/quick queries; a
/// query slow enough to need many polls pays a modest, bounded amount of
/// extra round trips either way.
pub(crate) const THRIFT_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Request hints on `TExecuteStatementReq.getDirectResults`/`TFetchResultsReq` --
/// how much of the result the server should try to hand back in one RPC.
/// `THRIFT_DIRECT_RESULTS_MAX_BYTES` is honored essentially exactly, up to a
/// hard server-side ceiling of ~1 GiB per response, measured in *uncompressed*
/// `bytesNum` (e.g. a 500k-row result whose LZ4-compressed download is only
/// ~302 MiB still counts as ~669 MiB against this budget) -- **this used to
/// say "the server decides real batch sizes regardless (same as SEA's chunk
/// sizes)," which was wrong and cost real round trips**: raising this from
/// its original 100 MiB (self-inflicted 10x throttle) to 1 GiB dropped a
/// `LIMIT 2000000` query's sequential `FetchResults` discovery calls from 29
/// down to 2, confirmed against a real workspace; values above 1 GiB
/// (tested up to `i64::MAX`) measured identically to 1 GiB, so that's the
/// real ceiling to document, not paper over with an unbounded-looking
/// constant. `THRIFT_DIRECT_RESULTS_MAX_ROWS`, by contrast, **does not
/// govern the `resultLinks` (cloud-fetch) path at all** -- confirmed by
/// requesting as few as 10 rows on a 2M-row query and still getting every
/// link back; it only bounds the small-result *inline* `arrowBatches` path
/// (which the server switches to independently, at roughly 2-3 MiB of
/// actual Arrow bytes, regardless of either hint -- so raising
/// `MAX_BYTES` cannot accidentally turn a medium result into a giant inline
/// payload). Leave `MAX_ROWS` alone; there is nothing to tune there.
///
/// One real, bounded trade-off from raising `MAX_BYTES`: each link's own
/// `expiryTime` is ~900s from the response that issued it, so a much larger
/// batch issues more not-yet-downloaded links earlier, marginally
/// tightening the deadline for a very slow, caller-paced consumer
/// (`Cursor.fetchmany`) -- bounded by the download worker pool's own
/// channel capacity (`chunk_fetch_concurrency`), not unbounded, and there is
/// no re-resolution path for an already-expired Thrift link (the
/// `FETCH_NEXT` cursor has already advanced past it) if this ever bites in
/// practice.
const THRIFT_DIRECT_RESULTS_MAX_ROWS: i64 = 1_000_000;
const THRIFT_DIRECT_RESULTS_MAX_BYTES: i64 = 1024 * 1024 * 1024;

/// A SEA session (`POST /api/2.0/sql/sessions`) pinned to one (catalog,
/// schema) pair, reused across statement submissions instead of the
/// stateless default -- measured against a real workspace: ~20% faster
/// submit-to-terminal-state latency for a small query (mean 495ms -> 404ms,
/// 15 warm runs), which lines up with `databricks-sql-connector`'s own SEA
/// mode (which always creates a session at `connect()`) being consistently
/// faster than this crate's session-less submissions for the same query.
///
/// Two hard constraints, both confirmed against a real workspace, drive this
/// design instead of one shared session per client:
/// - Databricks rejects `session_id` combined with per-statement `catalog`/
///   `schema` (HTTP 400: "The session_id field cannot be set at the same
///   time as the catalog or schema fields") -- a session must be created
///   *for* a specific (catalog, schema) pair, so the pool is keyed on it.
/// - Two statements submitted **concurrently** on the *same* session_id can
///   make the server fail with an internal error (`"Cannot invoke
///   SparkSession.sessionState() because sparkSession is null"`, reproduced
///   directly) -- a session is safe for sequential reuse, not concurrent
///   sharing. So this is a real pool (checkout/checkin), not a single cached
///   id, sized so concurrently-executing `Cursor`s each get their own.
///
/// Pool exhaustion (every session for a key already checked out) and session
/// creation failure both fall back to a plain session-less submission for
/// that one call (catalog/schema passed on the statement itself, exactly
/// today's pre-session behavior) -- this path never blocks waiting for a
/// session and never surfaces a new error class to the caller; worst case is
/// exactly as fast as before this feature existed. Any statement that errors
/// while holding a pooled session has that session discarded rather than
/// returned to the pool -- conservative (a plain query error, e.g. bad SQL,
/// still throws away a perfectly good session), but guarantees a session
/// that might be in the same bad state behind the SparkSession-null crash
/// above is never handed to a second caller.
type SessionKey = (Option<String>, Option<String>);

/// Generic checkout/checkin pool keyed by (catalog, schema) -- shared by
/// SEA's session ids (`Pool<String>`) and Thrift's session handles
/// (`Pool<thrift::SessionHandle>`), which had this exact logic duplicated
/// twice over before this. See `checkout_session`/`thrift_checkout_session`
/// for the constraints that shape it (session pinned to one (catalog,
/// schema) pair, safe for sequential reuse only, checkout never blocks on
/// exhaustion).
struct Pool<T> {
    // ponytail: fixed cap, not a constructor kwarg -- nothing's asked to
    // tune this yet; raise (or expose one) if a workload needs more
    // concurrent sessions per (catalog, schema) pair than this.
    idle: Mutex<HashMap<SessionKey, Vec<T>>>,
    total: Mutex<HashMap<SessionKey, usize>>,
}

// Manual impl instead of `#[derive(Default)]`: the derive would wrongly
// require `T: Default` even though neither field actually needs it.
impl<T> Default for Pool<T> {
    fn default() -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
            total: Mutex::new(HashMap::new()),
        }
    }
}

impl<T> Pool<T> {
    fn take(&self, key: &SessionKey) -> Option<T> {
        self.idle.lock().unwrap().get_mut(key).and_then(Vec::pop)
    }

    /// Reserves a slot for `key` if under `MAX_SESSIONS_PER_KEY`. The
    /// increment happens before the caller's own creation call so two
    /// concurrent callers can't both squeeze past the cap; call `release` if
    /// creation then fails.
    fn reserve(&self, key: &SessionKey) -> bool {
        let mut total = self.total.lock().unwrap();
        let count = total.entry(key.clone()).or_insert(0);
        if *count >= MAX_SESSIONS_PER_KEY {
            return false;
        }
        *count += 1;
        true
    }

    fn release(&self, key: &SessionKey) {
        let mut total = self.total.lock().unwrap();
        if let Some(count) = total.get_mut(key) {
            *count = count.saturating_sub(1);
        }
    }

    fn put(&self, key: SessionKey, item: T) {
        self.idle.lock().unwrap().entry(key).or_default().push(item);
    }

    /// Returns `item` to the pool for reuse (`keep = true`) or discards it,
    /// releasing its reservation instead (`keep = false`).
    fn checkin(&self, key: SessionKey, item: T, keep: bool) {
        if keep {
            self.put(key, item);
        } else {
            self.release(&key);
        }
    }

    fn drain_idle(&self) -> Vec<T> {
        self.idle.lock().unwrap().drain().flat_map(|(_, v)| v).collect()
    }
}

pub const MAX_SESSIONS_PER_KEY: usize = 8;

/// rustls (via reqwest's `rustls-no-provider` feature, see this crate's own
/// Cargo.toml comment on why) has no crypto backend wired in until one is
/// installed process-wide, and building a `reqwest::Client` panics if none
/// has been -- called from every `DbClient` constructor rather than from
/// the PyO3 module's own init, since a plain `cargo test`/pure-Rust caller
/// never runs that init at all. `install_default` only errors if a
/// provider was already installed (a second `DbClient`, or another
/// extension in the same interpreter already installed one) -- not a real
/// failure for us either way, so ignored.
fn install_default_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl DbClient {
    pub fn new(host: &str, warehouse_id: &str, token: &str) -> Self {
        Self::with_token_provider(host, warehouse_id, Arc::new(StaticToken(token.to_string())))
    }

    pub fn with_token_provider(host: &str, warehouse_id: &str, token_provider: Arc<dyn TokenProvider>) -> Self {
        install_default_crypto_provider();
        let host = host.trim_end_matches('/');
        // Only force https:// when no scheme was given at all -- same as
        // Python's check, but relaxed to not clobber an explicit http://
        // (needed to point this at a local wiremock instance for testing;
        // real Databricks workspaces are https-only regardless).
        let host = if host.starts_with("https://") || host.starts_with("http://") {
            host.to_string()
        } else {
            format!("https://{host}")
        };
        Self {
            http: Client::builder().build().expect("failed to build reqwest client"),
            host,
            warehouse_id: warehouse_id.to_string(),
            token_provider,
            http_timeout: Duration::from_secs(60),
            wait_timeout: "30s".to_string(),
            // Python's DatabricksClient defaults to 6, tuned for asyncio+GIL
            // where higher concurrency stops paying off past single digits
            // (see its own comment). This Rust core's real OS-thread
            // parallelism keeps paying off well past that -- measured
            // against a 400-chunk/5.6M-row/120-column table (16=140s,
            // 32=~113s avg of 3, 64=114s, 96=~102s avg of 2, 128=122s). 64
            // was picked as a safe middle-ground bump with no observed
            // downside on real data, not a claim that it's the true optimum.
            //
            // Re-attempted 2026-08-09 after switching away from http2/
            // aws-lc-rs to ring, on the theory that either transport change
            // could have moved the optimum: a first pass reported 96 as
            // ~13-16% faster than 64 on both this table and a second,
            // larger one (fact_sales_order_invoiced, 20M rows/295 cols) --
            // **found on review to be a false positive**. The benchmark
            // script called `connect()`/`arrowbricks.connect()` without
            // ever passing `chunk_fetch_concurrency=` explicitly, so every
            // "level" it claimed to test actually ran at whatever
            // `PyDbClient::new`'s own `#[pyo3(signature = ...)]` default
            // was (unchanged at 64 throughout, since only this Rust-side
            // constant had been edited, and it's overridden unconditionally
            // by `PyDbClient::new`'s explicit `.with_concurrency(...)` call
            // for every real Python caller regardless of this constant's
            // value) -- i.e. every "64 vs 96 vs 128" comparison was actually
            // 64 vs 64 vs 64, and the reported gap was warehouse/network
            // run-to-run noise, not a code effect. Caught by re-running a
            // controlled, interleaved A/B (`chunk_fetch_concurrency=`
            // passed explicitly each time, no rebuild needed) on
            // `dim_article`: 64/96/64/96 measured 125.60s/129.92s/137.50s/
            // 134.30s -- no consistent winner, well within run-to-run
            // noise. Reverted to 64. If re-attempting this again, always
            // pass `chunk_fetch_concurrency=` explicitly in the benchmark
            // script itself rather than relying on rebuilding with a
            // different default -- this exact mistake is easy to repeat
            // otherwise, since the four independent places this default is
            // hardcoded (see AGENTS.md) make "I changed the constant" and "a
            // real Python caller now uses that constant" two different,
            // easily-conflated claims.
            chunk_fetch_concurrency: DEFAULT_CHUNK_FETCH_CONCURRENCY,
            warehouse_start_timeout: Duration::from_secs(300),
            warehouse_confirmed_running_ttl: Duration::from_secs(30),
            warehouse_confirmed_running_at: Mutex::new(None),
            compress_results: true,
            session_pool: Pool::default(),
            protocol: Protocol::Thrift,
            thrift_session_pool: Pool::default(),
            download_slots: tokio::sync::Semaphore::new(DEFAULT_CHUNK_FETCH_CONCURRENCY),
            on_event: None,
            retry_attempts: RETRY_ATTEMPTS,
            retry_max_wait_s: RETRY_MAX_WAIT_S,
        }
    }

    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.chunk_fetch_concurrency = n.max(1);
        self.download_slots = tokio::sync::Semaphore::new(self.chunk_fetch_concurrency);
        self
    }

    /// Selects the wire protocol/backend -- see `Protocol`'s own doc comment.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Matches Python's `DatabricksClient(..., retry_attempts=6)` -- how many
    /// total attempts `retry_call`/`retry_call_tracked` make (the first try
    /// plus `n - 1` retries) before giving up on a transient failure.
    /// Clamped to at least 1 (0 would underflow `self.retry_attempts - 1` in
    /// the retry loop below and, separately, would mean "never even try") --
    /// `PyDbClient::new` also rejects 0 outright with a `ValueError` before
    /// this is ever reached, matching how other constructor args are
    /// validated there, but this clamp is a second, defensive line for any
    /// direct Rust caller that skips that validation.
    pub fn with_retry_attempts(mut self, n: u32) -> Self {
        self.retry_attempts = n.max(1);
        self
    }

    /// Matches Python's `DatabricksClient(..., retry_max_wait_s=20.0)` -- the
    /// ceiling the exponential backoff (`2^attempt` seconds) is capped at.
    /// Clamped to non-negative (a negative wait is meaningless -- `Duration::
    /// from_secs_f64` panics on it).
    pub fn with_retry_max_wait_s(mut self, seconds: f64) -> Self {
        self.retry_max_wait_s = seconds.max(0.0);
        self
    }

    /// Attaches the observability callback -- same attachment point/pattern
    /// as `with_token_provider`, called once at construction, applying to
    /// every query run through this client. See `EventSink`'s own doc
    /// comment for the fire-and-forget dispatch contract this must honor.
    pub fn with_on_event(mut self, on_event: Arc<dyn EventSink>) -> Self {
        self.on_event = Some(on_event);
        self
    }

    pub(crate) fn on_event(&self) -> Option<&Arc<dyn EventSink>> {
        self.on_event.as_ref()
    }

    /// Matches Python's `DatabricksClient(..., compress_results=True)` --
    /// whether to request LZ4-compressed cloud-fetch chunks. On by default
    /// (matches `databricks-sql-connector`'s own
    /// `enable_query_result_lz4_compression=True` default); measured ~2x
    /// faster chunk-fetch time against a real 120-column/100k-row table
    /// (network transfer, not local decode, is the bottleneck for a result
    /// this size). Off trades that back for zero decompression CPU work --
    /// worth it for a caller on a very fast/low-latency link to the
    /// warehouse, where compression's CPU cost stops paying for itself.
    pub fn with_compress_results(mut self, enabled: bool) -> Self {
        self.compress_results = enabled;
        self
    }

    /// Read-back for the Thrift path's fallback `lz4_compressed` guess
    /// (`pipeline.rs`'s `execute_lazy_thrift`) before any
    /// `TGetResultSetMetadataResp` has confirmed the real value.
    pub(crate) fn compress_results(&self) -> bool {
        self.compress_results
    }

    /// Matches Python's `DatabricksClient(..., http_timeout=60.0)` -- the
    /// per-request timeout passed to every `reqwest` call (statement submit/
    /// poll, chunk-index resolution, external-link download, volume file
    /// ops).
    pub fn with_http_timeout(mut self, seconds: f64) -> Self {
        self.http_timeout = Duration::from_secs_f64(seconds);
        self
    }

    /// Matches Python's `wait_timeout="30s"` -- Databricks' own
    /// synchronous-wait budget on the initial statement submit, passed
    /// through verbatim as the API's `wait_timeout` field (its own string
    /// format, e.g. `"10s"`..`"50s"`, not a `Duration`).
    pub fn with_wait_timeout(mut self, wait_timeout: impl Into<String>) -> Self {
        self.wait_timeout = wait_timeout.into();
        self
    }

    /// Matches Python's `warehouse_start_timeout=300.0` -- how long
    /// `ensure_warehouse_running` polls a STOPPED warehouse before giving up
    /// and letting statement submission itself surface whatever's wrong.
    pub fn with_warehouse_start_timeout(mut self, seconds: f64) -> Self {
        self.warehouse_start_timeout = Duration::from_secs_f64(seconds);
        self
    }

    /// Matches Python's `warehouse_confirmed_running_ttl_s=30.0` -- how long
    /// a confirmed-RUNNING result is trusted before `ensure_warehouse_running`
    /// re-checks, so a warm/always-on warehouse doesn't pay a round trip on
    /// every single statement.
    pub fn with_warehouse_confirmed_running_ttl(mut self, seconds: f64) -> Self {
        self.warehouse_confirmed_running_ttl = Duration::from_secs_f64(seconds);
        self
    }

    async fn retry_call<F, Fut, T>(&self, f: F) -> Result<T, ApiError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ApiError>>,
    {
        self.retry_call_tracked(None, f).await
    }

    /// Same retry loop as `retry_call`, plus an optional `retry_count`
    /// counter -- `Some` only for calls that are part of one query's own
    /// submit/poll/chunk-fetch path (see `QueryStats.retry_count`'s own doc
    /// comment in `lib.rs`); session pool management, volume file ops, and
    /// the fire-and-forget cancel/close RPCs all stay on plain `retry_call`
    /// (`None`), since a retry there isn't attributable to any one query the
    /// way a statement's own submit/poll/chunk-fetch retries are. Reads
    /// `self.retry_attempts`/`self.retry_max_wait_s` (constructor-tunable,
    /// see `with_retry_attempts`/`with_retry_max_wait_s`) rather than the old
    /// `RETRY_ATTEMPTS`/`RETRY_MAX_WAIT_S` constants directly, so every
    /// retryable call this client makes honors one client-wide policy.
    async fn retry_call_tracked<F, Fut, T>(&self, stats: Option<&QueryStatsAccumulator>, mut f: F) -> Result<T, ApiError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ApiError>>,
    {
        let mut attempt = 0u32;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt == self.retry_attempts - 1 || !e.transient {
                        return Err(e);
                    }
                    if let Some(s) = stats {
                        s.retry_count.fetch_add(1, Ordering::Relaxed);
                    }
                    let wait = 2f64.powi(attempt as i32).min(self.retry_max_wait_s);
                    tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn authed_json<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
        stats: Option<&QueryStatsAccumulator>,
    ) -> Result<T, ApiError> {
        // Only a GET is safe to blindly replay on a decode/mid-flight-send
        // failure -- see `ApiError::from_reqwest`'s doc. The only POSTs this
        // crate makes through here are statement submission and
        // warehouse-start, neither of which is safe to risk double-executing.
        let idempotent = method == reqwest::Method::GET;
        self.retry_call_tracked(stats, || async {
            // Fetched fresh on every attempt, not just once before the retry
            // loop -- matches Python's _bearer_token being called on every
            // _do() invocation, so a retry after a 401 picks up a
            // just-refreshed token instead of resending the same stale one.
            let token = self.token_provider.get_token().await?;
            let mut req = self
                .http
                .request(method.clone(), url)
                .bearer_auth(&token)
                .timeout(self.http_timeout);
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = req.send().await.map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            if !status.is_success() {
                return Err(ApiError::from_status(status, &text, idempotent));
            }
            serde_json::from_str::<T>(&text).map_err(|e| ApiError::permanent(format!("bad JSON body: {e}")))
        })
        .await
    }

    /// Unauthenticated -- external links are presigned blob-storage URLs,
    /// same as `_fetch_link_bytes` in the Python client. `compressed` decodes
    /// the server's LZ4 Frame wrapper (see `execute_statement`'s
    /// `result_compression` request) before handing bytes onward -- the
    /// downloaded file is smaller over the wire, but its content is opaque
    /// (Arrow-IPC or JSON, depending on `format`) until unwrapped here.
    /// Decompression runs on `spawn_blocking`, same reasoning as
    /// `pipeline.rs`'s Arrow-IPC decode: it's real CPU work (multiple LZ4
    /// frames per chunk, see `decompress_lz4_frame`), and running it inline
    /// would block this task's tokio worker thread from polling anything
    /// else scheduled on it -- other concurrent chunk fetches, heartbeat
    /// timers -- for however long that takes.
    pub(crate) async fn fetch_link_bytes(
        &self,
        url: &str,
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Bytes, ApiError> {
        self.retry_call_tracked(Some(stats), || async {
            let resp = self
                .http
                .get(url)
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text, true));
            }
            let bytes = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))?;
            // Counted here, before decompression -- "downloaded" means bytes
            // actually received off the wire, which is exactly the smaller,
            // (usually) LZ4-compressed size `compress_results` exists to
            // shrink -- see `QueryStats.bytes_downloaded`'s own doc comment
            // in `lib.rs`.
            stats.bytes_downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if !compressed {
                return Ok(bytes);
            }
            tokio::task::spawn_blocking(move || decompress_lz4_frame(&bytes))
                .await
                .map_err(join_error)?
        })
        .await
    }

    /// Downloads one cloud-fetch link, splitting it across parallel HTTP
    /// Range requests when the shared `download_slots` budget has room to
    /// spare -- see that field's own doc comment for the measured win and
    /// why the large-result case is safe.
    pub(crate) async fn fetch_link_bytes_budgeted(
        self: &Arc<Self>,
        url: &str,
        compressed: bool,
        stats: &Arc<QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        // One permit per download is mandatory; the worker pool already
        // bounds concurrent links to `chunk_fetch_concurrency`, so this
        // never blocks in practice -- it just makes the budget accounting
        // exact. `acquire()`'s `Err` case (the semaphore closed) can't
        // happen -- nothing in this crate ever calls `.close()` on
        // `download_slots` -- but propagating it as a real error instead of
        // an `.expect()` costs nothing and avoids a panic if that ever
        // changes.
        let _base = self
            .download_slots
            .acquire()
            .await
            .map_err(|e| ApiError::permanent(format!("download_slots semaphore closed unexpectedly: {e}")))?;
        let want = (MAX_SPLIT_PARTS - 1).min(self.download_slots.available_permits());
        let extra = if want > 0 {
            self.download_slots.try_acquire_many(want as u32).ok()
        } else {
            None
        };
        let parts = 1 + extra.as_ref().map(|p| p.num_permits()).unwrap_or(0);
        if parts <= 1 {
            return self.fetch_link_bytes(url, compressed, stats).await;
        }
        self.fetch_link_bytes_split(url, compressed, 1 << 20, parts as u64, stats)
            .await
    }

    /// Downloads one cloud-fetch link as concurrent HTTP Range requests of
    /// `part_size` bytes each, concatenated in order. The real object size
    /// is learned from the first range response's `Content-Range` header --
    /// the Thrift link's own `bytesNum` is the *uncompressed* row-set size,
    /// not the file's size on blob storage, and using it produces HTTP 416.
    ///
    /// A server that answers the probe with `200 OK` instead of `206
    /// Partial Content` (Range unsupported) falls back to treating the
    /// whole response as the complete file -- correct, since it ignored
    /// Range and sent everything. But a server that *does* answer `206`
    /// with a `Content-Range` header this code can't parse is a different,
    /// unsafe case: silently treating the first `part_size` bytes as the
    /// whole file would truncate the real result with no error. That's
    /// treated as a hard failure instead, not a silent truncation --
    /// confirmed unreachable against real Azure Blob Storage (always
    /// returns a well-formed `bytes start-end/total`), but this is a
    /// third-party response shape, not something this crate controls.
    pub(crate) async fn fetch_link_bytes_split(
        self: &Arc<Self>,
        url: &str,
        compressed: bool,
        part_size: u64,
        max_parts: u64,
        stats: &Arc<QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        // First part doubles as the size probe -- same retry_call wrapping
        // every other download in this crate gets, so a transient failure
        // on the probe itself doesn't skip straight to a hard error.
        let (ranged, total, head) = self.retry_call_tracked(Some(stats), || async {
            let resp = self
                .http
                .get(url)
                .header("Range", format!("bytes=0-{}", part_size - 1))
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text, true));
            }
            let ranged = status == reqwest::StatusCode::PARTIAL_CONTENT;
            let total: Option<u64> = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit('/').next().and_then(|t| t.parse().ok()));
            let head = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))?;
            Ok((ranged, total, head))
        })
        .await?;
        stats.bytes_downloaded.fetch_add(head.len() as u64, Ordering::Relaxed);

        if ranged && total.is_none() {
            return Err(ApiError::permanent(
                "cloud-fetch link answered a Range request with 206 Partial Content but an \
                 unparseable Content-Range header -- refusing to silently return a truncated \
                 file"
                    .to_string(),
            ));
        }

        let mut handles = Vec::new();
        if let Some(total) = ranged.then_some(total).flatten() {
            // Spread everything after the probe part evenly over at most
            // max_parts-1 further requests, so no single tail request
            // dominates the wall clock.
            let remaining = total.saturating_sub(part_size);
            let n_rest = remaining.div_ceil(part_size).min(max_parts.saturating_sub(1));
            let rest_size = if n_rest == 0 { 0 } else { remaining.div_ceil(n_rest) };
            let mut start = part_size;
            let mut n = 1u64;
            while start < total && n <= n_rest {
                let end = (start + rest_size - 1).min(total - 1);
                let this = self.clone();
                let url = url.to_string();
                let part_stats = stats.clone();
                handles.push(tokio::spawn(async move {
                    let bytes = this.retry_call_tracked(Some(&part_stats), || async {
                        let resp = this
                            .http
                            .get(&url)
                            .header("Range", format!("bytes={start}-{end}"))
                            .timeout(this.http_timeout)
                            .send()
                            .await
                            .map_err(|e| ApiError::from_reqwest(e, true))?;
                        let status = resp.status();
                        if !status.is_success() {
                            let text = resp.text().await.unwrap_or_default();
                            return Err(ApiError::from_status(status, &text, true));
                        }
                        resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, true))
                    })
                    .await;
                    if let Ok(b) = &bytes {
                        part_stats.bytes_downloaded.fetch_add(b.len() as u64, Ordering::Relaxed);
                    }
                    bytes
                }));
                start = end + 1;
                n += 1;
            }
        }

        let mut out = bytes::BytesMut::with_capacity(total.unwrap_or(head.len() as u64) as usize);
        out.extend_from_slice(&head);
        // Parts must concatenate in order, so this can't use `JoinSet`
        // (which yields in completion order) without tracking indices --
        // simpler to keep the `Vec` and explicitly `.abort()` every
        // not-yet-awaited sibling the moment one part fails, rather than
        // silently leaving them running (a bare `?` here would return
        // early and just drop the rest, which does NOT cancel them --
        // `JoinHandle::drop` detaches, it doesn't abort -- leaving up to
        // `MAX_SPLIT_PARTS - 1` sibling Range downloads, each with their
        // own `retry_call` backoff, still in flight for a link the caller
        // has already given up on).
        let mut iter = handles.into_iter();
        while let Some(h) = iter.next() {
            match h.await {
                Ok(Ok(part)) => out.extend_from_slice(&part),
                Ok(Err(e)) => {
                    for remaining in iter {
                        remaining.abort();
                    }
                    return Err(e);
                }
                Err(join_err) => {
                    for remaining in iter {
                        remaining.abort();
                    }
                    return Err(join_error(join_err));
                }
            }
        }
        let bytes = out.freeze();
        if let Some(t) = total
            && bytes.len() as u64 != t
        {
            return Err(ApiError::permanent(format!(
                "split download assembled {} bytes, expected {t}",
                bytes.len()
            )));
        }
        if !compressed {
            return Ok(bytes);
        }
        tokio::task::spawn_blocking(move || decompress_lz4_frame(&bytes))
            .await
            .map_err(join_error)?
    }

    /// Shared by both protocols -- plain REST against `/api/2.0/sql/warehouses/{id}`,
    /// nothing SEA- or Thrift-specific about it (see AGENTS.md for why the
    /// Thrift path didn't always call this).
    pub(crate) async fn ensure_warehouse_running(&self) -> Result<(), ApiError> {
        {
            let confirmed = *self.warehouse_confirmed_running_at.lock().unwrap();
            if let Some(at) = confirmed
                && at.elapsed() < self.warehouse_confirmed_running_ttl
            {
                return Ok(());
            }
        }

        let url = format!("{}/api/2.0/sql/warehouses/{}", self.host, self.warehouse_id);
        let data: WarehouseStatusBody = self.authed_json(reqwest::Method::GET, &url, None, None).await?;
        if data.state == "RUNNING" {
            *self.warehouse_confirmed_running_at.lock().unwrap() = Some(Instant::now());
            return Ok(());
        }
        if data.state == "STOPPED" {
            self.authed_json::<IgnoredAny>(reqwest::Method::POST, &format!("{url}/start"), None, None)
                .await?;
        }

        let deadline = Instant::now() + self.warehouse_start_timeout;
        while Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
            let data: WarehouseStatusBody = self.authed_json(reqwest::Method::GET, &url, None, None).await?;
            if data.state == "RUNNING" {
                *self.warehouse_confirmed_running_at.lock().unwrap() = Some(Instant::now());
                return Ok(());
            }
        }
        // Falls through, same as Python: let statement submission surface
        // whatever's actually wrong instead of raising here.
        Ok(())
    }

    async fn create_session(&self, catalog: Option<&str>, schema: Option<&str>) -> Result<String, ApiError> {
        let url = format!("{}/api/2.0/sql/sessions", self.host);
        let mut body = json!({ "warehouse_id": self.warehouse_id });
        if let Some(c) = catalog {
            body["catalog_name"] = json!(c);
        }
        if let Some(s) = schema {
            body["schema_name"] = json!(s);
        }
        let data: SessionCreateBody = self.authed_json(reqwest::Method::POST, &url, Some(&body), None).await?;
        Ok(data.session_id)
    }

    async fn delete_session(&self, session_id: &str) {
        let url = format!("{}/api/2.0/sql/sessions/{session_id}", self.host);
        let body = json!({ "warehouse_id": self.warehouse_id });
        // Best-effort: a failed delete just leaves the session to be reaped
        // by Databricks' own server-side TTL -- not worth surfacing an error
        // for, since this only ever runs during pool cleanup/discard, well
        // after the statement it backed already reached a terminal state.
        let _: Result<IgnoredAny, ApiError> = self
            .authed_json(reqwest::Method::DELETE, &url, Some(&body), None)
            .await;
    }

    /// Hands back a pooled session for (`catalog`, `schema`) if one's idle,
    /// creates one if the pool for that key isn't at `MAX_SESSIONS_PER_KEY`
    /// yet, or `None` if neither -- the caller falls back to a plain
    /// session-less submission in that case, see `session_pool`'s own doc
    /// comment for why this never blocks instead.
    async fn checkout_session(&self, catalog: Option<&str>, schema: Option<&str>) -> Option<String> {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        if let Some(id) = self.session_pool.take(&key) {
            return Some(id);
        }
        if !self.session_pool.reserve(&key) {
            return None;
        }
        match self.create_session(catalog, schema).await {
            Ok(id) => Some(id),
            Err(_) => {
                self.session_pool.release(&key);
                None
            }
        }
    }

    /// Returns a session to the pool for reuse (`keep = true`, the statement
    /// it backed reached SUCCEEDED/FAILED/CANCELED cleanly) or discards it
    /// (`keep = false`) -- see `session_pool`'s doc comment for why any error
    /// discards rather than reuses.
    fn checkin_session(&self, catalog: Option<&str>, schema: Option<&str>, session_id: String, keep: bool) {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        self.session_pool.checkin(key, session_id, keep);
    }

    /// Best-effort cleanup of every currently-idle pooled session -- meant
    /// to be called once, from the Python-facing client's own close/aclose.
    /// A session still checked out (an in-flight statement) at the time this
    /// runs isn't in `idle` and so isn't closed here -- acceptable, same
    /// server-side TTL reaping as a discarded/errored session above; calling
    /// this before every pending statement has finished is a caller
    /// ordering issue, not something this method can fix from inside.
    pub async fn close_all_sessions(&self) {
        for id in self.session_pool.drain_idle() {
            self.delete_session(&id).await;
        }
    }

    // ---- Thrift backend (opt-in `protocol="thrift"`) --------------------

    fn thrift_url(&self) -> String {
        format!("{}/sql/1.0/warehouses/{}", self.host, self.warehouse_id)
    }

    /// Raw Thrift-over-HTTP POST: `Content-Type: application/x-thrift`, the
    /// same Bearer-token auth header as every other request in this crate,
    /// no framing beyond HTTP itself (confirmed against
    /// `databricks-sql-connector`'s own `THttpClient` -- see `thrift.rs`'s
    /// own module doc comment). `idempotent` gates transience exactly like
    /// `authed_json`'s -- `OpenSession`/`ExecuteStatement`/`FetchResults`
    /// (which advances a server-side cursor with `orientation: FETCH_NEXT`,
    /// so blindly replaying it on an ambiguous failure risks silently
    /// skipping or double-fetching rows) must never be retried blindly;
    /// `GetOperationStatus`/`CloseOperation`/`CloseSession`/`CancelOperation`
    /// are safe to retry (read-only or naturally idempotent).
    async fn thrift_call(
        &self,
        body: Bytes,
        idempotent: bool,
        stats: Option<&QueryStatsAccumulator>,
    ) -> Result<Bytes, ApiError> {
        let url = self.thrift_url();
        self.retry_call_tracked(stats, || async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .post(&url)
                .bearer_auth(&token)
                .header("Content-Type", "application/x-thrift")
                .header("User-Agent", "PyDatabricksSqlConnector/4.4.0 arrowbricks-thrift-rs")
                .body(body.clone())
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            let status = resp.status();
            let bytes = resp.bytes().await.map_err(|e| ApiError::from_reqwest(e, idempotent))?;
            if !status.is_success() {
                let text = String::from_utf8_lossy(&bytes).to_string();
                return Err(ApiError::from_status(status, &text, idempotent));
            }
            Ok(bytes)
        })
        .await
    }

    fn thrift_parse_error(e: thrift::ThriftError) -> ApiError {
        ApiError::permanent(format!("bad thrift response: {e}"))
    }

    pub(crate) async fn thrift_open_session_raw(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Result<thrift::SessionHandle, ApiError> {
        let namespace = if catalog.is_some() || schema.is_some() {
            Some(thrift::Namespace {
                catalog_name: catalog,
                schema_name: schema,
            })
        } else {
            None
        };
        let req = thrift::OpenSessionReq {
            namespace,
            configuration: &[("spark.thriftserver.arrowBasedRowSet.timestampAsString", "false")],
        };
        let body = Bytes::from(thrift::build_open_session(&req));
        // Not idempotent: an ambiguous failure here may have already
        // created a session server-side; blindly retrying would leak it
        // (harmless correctness-wise, but not "safe to replay" in the
        // sense this flag means elsewhere in this file).
        let resp_bytes = self.thrift_call(body, false, None).await?;
        let resp = thrift::parse_open_session(&resp_bytes).map_err(Self::thrift_parse_error)?;
        if let Some(e) = resp.status.error() {
            return Err(ApiError::permanent(format!("Thrift OpenSession failed: {e}")));
        }
        resp.session_handle
            .ok_or_else(|| ApiError::permanent("Thrift OpenSession succeeded with no sessionHandle".to_string()))
    }

    pub(crate) async fn thrift_close_session_raw(&self, session: &thrift::SessionHandle) {
        let body = Bytes::from(thrift::build_close_session(session));
        // Best-effort, same as SEA's `delete_session` -- a failed close just
        // leaves the session for Databricks' own server-side TTL to reap.
        let _ = self.thrift_call(body, true, None).await;
    }

    /// Same checkout contract as SEA's `checkout_session` (see
    /// `thrift_session_pool`'s doc comment): `None` means the caller must
    /// open its own throwaway session for this one call (Thrift has no
    /// session-less submission mode to fall back to).
    pub(crate) async fn thrift_checkout_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Option<thrift::SessionHandle> {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        if let Some(handle) = self.thrift_session_pool.take(&key) {
            return Some(handle);
        }
        if !self.thrift_session_pool.reserve(&key) {
            return None;
        }
        match self.thrift_open_session_raw(catalog, schema).await {
            Ok(handle) => Some(handle),
            Err(_) => {
                self.thrift_session_pool.release(&key);
                None
            }
        }
    }

    pub(crate) fn thrift_checkin_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session: thrift::SessionHandle,
        keep: bool,
    ) {
        let key = (catalog.map(str::to_string), schema.map(str::to_string));
        self.thrift_session_pool.checkin(key, session, keep);
    }

    /// Best-effort close of every currently-idle pooled Thrift session --
    /// same contract as `close_all_sessions` (SEA).
    pub async fn close_all_thrift_sessions(&self) {
        for s in self.thrift_session_pool.drain_idle() {
            self.thrift_close_session_raw(&s).await;
        }
    }

    pub(crate) async fn thrift_execute_statement_raw(
        &self,
        session: &thrift::SessionHandle,
        statement: &str,
        parameters: Option<&Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::ExecuteStatementResp, ApiError> {
        let params_vec = parameters.map(thrift::parameters_from_json).unwrap_or_default();
        let req = thrift::ExecuteStatementReq {
            session_handle: session,
            statement,
            can_decompress_lz4: self.compress_results,
            direct_results_max_rows: THRIFT_DIRECT_RESULTS_MAX_ROWS,
            direct_results_max_bytes: THRIFT_DIRECT_RESULTS_MAX_BYTES,
            parameters: &params_vec,
        };
        let body = Bytes::from(thrift::build_execute_statement(&req));
        // Not idempotent -- same double-execution risk as SEA's
        // statement-submit POST (arbitrary caller SQL, e.g. INSERT/MERGE).
        let resp_bytes = self.thrift_call(body, false, Some(stats)).await?;
        let resp = thrift::parse_execute_statement(&resp_bytes).map_err(Self::thrift_parse_error)?;
        if let Some(e) = resp.status.error() {
            return Err(ApiError::permanent(format!("Thrift ExecuteStatement failed: {e}")));
        }
        Ok(resp)
    }

    pub(crate) async fn thrift_get_operation_status_raw(
        &self,
        op: &thrift::OperationHandle,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::OperationStatusResp, ApiError> {
        let body = Bytes::from(thrift::build_get_operation_status(op));
        let resp_bytes = self.thrift_call(body, true, Some(stats)).await?;
        thrift::parse_get_operation_status(&resp_bytes).map_err(Self::thrift_parse_error)
    }

    pub(crate) async fn thrift_fetch_results_raw(
        &self,
        op: &thrift::OperationHandle,
        stats: &QueryStatsAccumulator,
    ) -> Result<thrift::FetchResultsResp, ApiError> {
        let body = Bytes::from(thrift::build_fetch_results(
            op,
            THRIFT_DIRECT_RESULTS_MAX_ROWS,
            THRIFT_DIRECT_RESULTS_MAX_BYTES,
        ));
        // Not idempotent -- see `thrift_call`'s own doc comment: this
        // advances a server-side cursor (`orientation: FETCH_NEXT`).
        let resp_bytes = self.thrift_call(body, false, Some(stats)).await?;
        thrift::parse_fetch_results(&resp_bytes).map_err(Self::thrift_parse_error)
    }

    /// Best-effort -- mirrors `delete_session`'s reasoning: a failed close
    /// just leaves the operation for Databricks' own server-side cleanup.
    pub(crate) async fn thrift_close_operation_best_effort(&self, op: &thrift::OperationHandle) {
        let body = Bytes::from(thrift::build_close_operation(op));
        let _ = self.thrift_call(body, true, None).await;
    }

    /// Fire-and-forget best-effort server-side cancel of one in-flight
    /// statement/operation -- SEA's `POST .../cancel`, or Thrift's
    /// `CancelOperation`. Never awaited before a `QueryTimeout`/cancellation
    /// surfaces to the caller (see `heartbeat.rs`'s own two call sites,
    /// which spawn this rather than awaiting it inline) -- the result is
    /// intentionally ignored either way, same "best effort, Databricks'
    /// own server-side TTL reaps the rest on failure" pattern as
    /// `delete_session`/`thrift_close_operation_best_effort` above.
    ///
    /// Belt-and-suspenders for Thrift specifically: whether `CloseOperation`
    /// on a still-running operation already implicitly cancels it
    /// server-side (common in HiveServer2-compatible implementations) is an
    /// open question this crate's own design doc leaves unresolved either
    /// way -- calling `CancelOperation` explicitly regardless is harmless if
    /// redundant, necessary if not.
    pub(crate) async fn cancel_statement(&self, handle: &CancelHandle) {
        match handle {
            CancelHandle::Sea { statement_id } => {
                let url = format!("{}/api/2.0/sql/statements/{}/cancel", self.host, statement_id);
                // Own request-building, not `authed_json` -- `authed_json`
                // hardcodes `idempotent = method == GET`, but replaying a
                // *cancel* POST is safe (unlike the statement-submit POST
                // elsewhere in this file): cancelling an already-cancelled/
                // already-terminal statement is a no-op, not a second
                // execution of caller SQL.
                let _ = self.retry_call(|| async {
                    let token = self.token_provider.get_token().await?;
                    let resp = self
                        .http
                        .post(&url)
                        .bearer_auth(&token)
                        .timeout(self.http_timeout)
                        .send()
                        .await
                        .map_err(|e| ApiError::from_reqwest(e, true))?;
                    let status = resp.status();
                    if !status.is_success() {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(ApiError::from_status(status, &text, true));
                    }
                    Ok(())
                })
                .await;
            }
            CancelHandle::Thrift { operation } => {
                let body = Bytes::from(thrift::build_cancel_operation(operation));
                let _ = self.thrift_call(body, true, None).await;
            }
        }
    }

    /// Submit + poll an EXTERNAL_LINKS/ARROW_STREAM statement to terminal
    /// state. `parameters`, if given, is Databricks' own named-parameter
    /// format ([{"name":..., "value":..., "type":...}] bound against `:name`
    /// markers in `statement`) -- passed through to the request body
    /// verbatim, same as the Python original does no validation of its own
    /// shape either.
    pub async fn execute_arrow_statement(
        &self,
        statement: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementSubmitResult, ApiError> {
        self.execute_statement(statement, "ARROW_STREAM", catalog, schema, parameters, stats)
            .await
    }

    /// Tries `disposition: INLINE` + `format: JSON_ARRAY` first -- for a
    /// small result, Databricks embeds the whole result directly in this
    /// same submit/poll response (`result.data_array`), skipping the
    /// separate chunk-resolution-and-blob-fetch round trip the normal
    /// `EXTERNAL_LINKS` path always needs. Confirmed against a real
    /// workspace: exceeding INLINE's byte limit (26,214,400 bytes / 25MiB)
    /// fails the statement cleanly with a specific, matchable error
    /// message -- never silent truncation -- and `INLINE_OR_EXTERNAL_LINKS`
    /// ("HYBRID", which would let the server choose per-query) returned a
    /// clean "not a supported disposition" 400 on that same workspace, so
    /// isn't used here. `format` must be `JSON_ARRAY` for `INLINE` --
    /// Databricks rejects `INLINE`+`ARROW_STREAM` outright (also confirmed).
    /// This module stays Arrow-agnostic on purpose (see its own module doc
    /// comment) -- `InlineOrExternal::Inline` carries the raw JSON_ARRAY
    /// rows straight through; converting them into a `RecordBatch` (and
    /// falling back to a fresh `EXTERNAL_LINKS` submission if that
    /// conversion hits a column type it doesn't handle) is `pipeline.rs`'s
    /// job via `json_convert`, same division of responsibility as every
    /// other decode step in this crate.
    ///
    /// Falls back to a **fresh, independent** `execute_arrow_statement` call
    /// (not a retry of this same submission) whenever INLINE doesn't pan
    /// out at the HTTP/statement level: the byte limit was exceeded, or
    /// `data_array` is unexpectedly absent despite SUCCEEDED. This is a
    /// second, distinct statement execution, not a retry of a possibly-
    /// already-run one, so it carries none of the double-execution risk
    /// that made POST retries unsafe elsewhere in this file -- the first
    /// attempt's outcome is fully known (it reached a terminal state)
    /// before the second one is ever submitted. A genuine query error (bad
    /// SQL, permission denied) propagates immediately instead, same as the
    /// normal path -- no reason to mask it behind a pointless second
    /// attempt.
    ///
    /// Not the default -- opt-in only (`prefer_inline` on the Python-facing
    /// `execute()`/`Cursor.execute()`), since a caller who doesn't expect a
    /// small result pays for two full statement executions on the (common,
    /// for them) fallback path instead of one.
    pub async fn execute_arrow_statement_prefer_inline(
        &self,
        statement: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<InlineOrExternal, ApiError> {
        let mut body = json!({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        });
        if let Some(p) = parameters.clone() {
            body["parameters"] = p;
        }

        let outcome = self.submit_and_poll(body, catalog, schema, stats).await;
        let data = match outcome {
            Ok(d) => d,
            Err(e) if e.message.contains("Inline byte limit exceeded") => {
                // Same `stats` accumulator, not a fresh one -- this fallback
                // is invisible to the caller (one `execute()` call in, one
                // result out), so its own submit/poll/retry activity counts
                // toward the same `QueryStats` event as the INLINE attempt
                // that preceded it, not a second, separate one.
                return self
                    .execute_arrow_statement(statement, catalog, schema, parameters, stats)
                    .await
                    .map(InlineOrExternal::External);
            }
            Err(e) => return Err(e),
        };

        let manifest = data.manifest.unwrap_or_default();
        let columns = manifest.schema.map(|s| s.columns).unwrap_or_default();
        let data_array = data.result.and_then(|r| r.data_array);
        match data_array {
            Some(rows) => Ok(InlineOrExternal::Inline {
                statement_id: data.statement_id,
                rows,
                columns,
            }),
            // No data_array despite SUCCEEDED -- shouldn't happen given a
            // non-error status, but this crate never guesses at a missing
            // field. **Does not** fall back to a fresh EXTERNAL_LINKS
            // submission the way the byte-limit-exceeded case above does --
            // found in code review (2026-08-11, alongside the identical bug
            // in `pipeline.rs`'s own JSON-conversion-failure fallback): the
            // byte-limit case is safe to resubmit because that statement
            // reached a FAILED state (nothing committed server-side); this
            // one only runs after SUCCEEDED, meaning any DML side effects
            // already happened, so blindly resubmitting the identical SQL
            // would risk silently duplicating them for non-idempotent SQL.
            // A clean error is the safe choice here regardless of how
            // unlikely this branch is to ever actually fire.
            None => Err(ApiError::permanent(format!(
                "statement {} succeeded with disposition=INLINE but the response had no data_array -- refusing \
                 to automatically re-run the query to avoid duplicating any write it performed",
                data.statement_id
            ))),
        }
    }

    /// Shared by `execute_statement` and `execute_arrow_statement_prefer_inline`:
    /// POST the statement, poll until a terminal state, and turn FAILED/
    /// CANCELED into an `Err` -- everything both callers need before they
    /// diverge on how to interpret a SUCCEEDED response's `result`/`manifest`.
    ///
    /// `body` must *not* already carry `catalog`/`schema`/`session_id` --
    /// this method owns that decision: a pooled session for (`catalog`,
    /// `schema`) if one's available (see `checkout_session`),
    /// falling back to setting `catalog`/`schema` directly on the body
    /// otherwise (Databricks rejects `session_id` combined with either
    /// field). The session, if any, is returned to the pool on a clean
    /// terminal state and discarded on any error.
    async fn submit_and_poll(
        &self,
        mut body: Value,
        catalog: Option<&str>,
        schema: Option<&str>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementResponseBody, ApiError> {
        // Timed and recorded here -- the *only* place this call happens for
        // SEA -- so a caller (e.g. `pipeline.rs`'s `execute_lazy`) reads
        // `stats.warehouse_wait_s()` back afterward instead of also timing
        // its own, second, now-cache-warm call to the same method. See
        // `QueryStatsAccumulator::warehouse_wait_bits`'s own doc comment for
        // the real double-call bug this replaced.
        let warehouse_t0 = Instant::now();
        self.ensure_warehouse_running().await?;
        stats.add_warehouse_wait_s(warehouse_t0.elapsed().as_secs_f64());
        let session_id = self.checkout_session(catalog, schema).await;
        match &session_id {
            Some(id) => body["session_id"] = json!(id),
            None => {
                if let Some(c) = catalog {
                    body["catalog"] = json!(c);
                }
                if let Some(s) = schema {
                    body["schema"] = json!(s);
                }
            }
        }

        let result = self.submit_and_poll_inner(body, stats).await;
        if let Some(id) = session_id {
            self.checkin_session(catalog, schema, id, result.is_ok());
        }
        result
    }

    async fn submit_and_poll_inner(
        &self,
        body: Value,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementResponseBody, ApiError> {
        let url = format!("{}/api/2.0/sql/statements", self.host);
        let mut data: StatementResponseBody = self
            .authed_json(reqwest::Method::POST, &url, Some(&body), Some(stats))
            .await?;

        while !matches!(
            data.status.state.as_str(),
            "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED"
        ) {
            tokio::time::sleep(POLL_INTERVAL).await;
            let poll_url = format!("{}/api/2.0/sql/statements/{}", self.host, data.statement_id);
            data = self
                .authed_json(reqwest::Method::GET, &poll_url, None, Some(stats))
                .await?;
        }

        match data.status.state.as_str() {
            "FAILED" => {
                let err = data.status.error.unwrap_or(StatementErrorBody {
                    error_code: None,
                    message: None,
                });
                Err(ApiError::statement_failed(format!(
                    "Databricks statement failed [{}]: {}",
                    err.error_code.as_deref().unwrap_or(""),
                    err.message.as_deref().unwrap_or(""),
                )))
            }
            "CANCELED" => Err(ApiError::statement_failed("Databricks statement was canceled")),
            _ => Ok(data),
        }
    }

    async fn execute_statement(
        &self,
        statement: &str,
        format: &str,
        catalog: Option<&str>,
        schema: Option<&str>,
        parameters: Option<Value>,
        stats: &QueryStatsAccumulator,
    ) -> Result<StatementSubmitResult, ApiError> {
        let mut body = json!({
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "disposition": "EXTERNAL_LINKS",
            "format": format,
            "wait_timeout": self.wait_timeout,
            "on_wait_timeout": "CONTINUE",
        });
        if self.compress_results {
            // Matches databricks-sql-python's default
            // (enable_query_result_lz4_compression=True) -- trades a cheap
            // client-side LZ4 decompress for meaningfully less data over the
            // wire, which is the actual bottleneck for a large result (local
            // Arrow-IPC decode is already fast; network transfer isn't).
            // Runtime-toggleable via `compress_results=False` -- see
            // `with_compress_results`.
            body["result_compression"] = json!("LZ4_FRAME");
        }
        if let Some(p) = parameters {
            body["parameters"] = p;
        }

        let data = self.submit_and_poll(body, catalog, schema, stats).await?;
        let manifest = data.manifest.unwrap_or_default();
        let compressed = manifest.result_compression.as_deref() == Some("LZ4_FRAME");
        // `Vec` per index, not a plain map entry -- see `ChunkMeta::pre_resolved_links`'s
        // doc for why collapsing to one would silently lose rows.
        let mut pre_resolved: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
        if let Some(r) = data.result {
            for link in r.external_links {
                if link.external_link.is_empty() {
                    continue;
                }
                pre_resolved
                    .entry(link.chunk_index)
                    .or_default()
                    .push(link.external_link);
            }
        }
        let chunk_metas = manifest
            .chunks
            .into_iter()
            .map(|c| ChunkMeta {
                pre_resolved_links: pre_resolved.remove(&c.chunk_index).unwrap_or_default(),
                chunk_index: c.chunk_index,
                row_count: c.row_count,
            })
            .collect();
        let columns = manifest.schema.map(|s| s.columns).unwrap_or_default();
        Ok(StatementSubmitResult {
            statement_id: data.statement_id,
            chunk_metas,
            columns,
            compressed,
        })
    }

    async fn fetch_chunk_index(
        &self,
        statement_id: &str,
        chunk_index: i64,
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Vec<Bytes>, ApiError> {
        let url = format!(
            "{}/api/2.0/sql/statements/{}/result/chunks/{}",
            self.host, statement_id, chunk_index
        );
        let data: ChunkLinksBody = self
            .authed_json(reqwest::Method::GET, &url, None, Some(stats))
            .await?;
        let links: Vec<String> = data.external_links.into_iter().map(|l| l.external_link).collect();
        self.fetch_pre_resolved_links(&links, compressed, stats).await
    }

    /// Same shape as `fetch_chunk_index`, but for links the statement submit/
    /// poll response already embedded -- no resolution GET needed first.
    async fn fetch_pre_resolved_links(
        &self,
        links: &[String],
        compressed: bool,
        stats: &QueryStatsAccumulator,
    ) -> Result<Vec<Bytes>, ApiError> {
        let mut blobs = Vec::with_capacity(links.len());
        for link in links {
            blobs.push(self.fetch_link_bytes(link, compressed, stats).await?);
        }
        Ok(blobs)
    }

    /// Bounded-concurrency worker pool, mirroring
    /// `_fetch_chunks_with_backpressure`: a fixed pool of workers pulls from
    /// a shared queue of chunk metas, each pushing its blobs into a bounded
    /// mpsc channel one at a time -- `Sender::send` blocks (backpressure)
    /// until the consumer takes the previous item, so peak buffered chunks
    /// stays at ~concurrency, not O(whole result). Errors don't cancel
    /// sibling workers; every worker runs to completion, successful chunks
    /// already fetched are still yielded, and the first error (if any) is
    /// delivered as the channel's last item -- same trade-off as Python's
    /// `close_when_done`.
    pub fn fetch_chunks_with_backpressure(
        self: std::sync::Arc<Self>,
        statement_id: String,
        chunk_metas: Vec<ChunkMeta>,
        compressed: bool,
        stats: Arc<QueryStatsAccumulator>,
    ) -> mpsc::Receiver<Result<ChunkItem, ApiError>> {
        let concurrency = self.chunk_fetch_concurrency.max(1);
        let (tx, rx) = mpsc::channel::<Result<ChunkItem, ApiError>>(concurrency);
        let queue = std::sync::Arc::new(Mutex::new(VecDeque::from(chunk_metas)));

        tokio::spawn(async move {
            let mut handles = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let client = self.clone();
                let queue = queue.clone();
                let worker_tx = tx.clone();
                let statement_id = statement_id.clone();
                let stats = stats.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        let meta = { queue.lock().unwrap().pop_front() };
                        let Some(meta) = meta else { return Ok(()) };
                        let fetched = if meta.pre_resolved_links.is_empty() {
                            client
                                .fetch_chunk_index(&statement_id, meta.chunk_index, compressed, &stats)
                                .await
                        } else {
                            client
                                .fetch_pre_resolved_links(&meta.pre_resolved_links, compressed, &stats)
                                .await
                        };
                        match fetched {
                            Ok(blobs) => {
                                // One blob ⇒ `meta.row_count` (the whole chunk_index's
                                // declared count) and "this blob's count" are the same
                                // number -- see `ChunkItem::truncate_to`'s own doc comment.
                                let truncate_to = if blobs.len() == 1 { meta.row_count } else { None };
                                for blob in blobs {
                                    let item = ChunkItem {
                                        blob,
                                        row_count: meta.row_count,
                                        chunk_index: meta.chunk_index,
                                        truncate_to,
                                    };
                                    if worker_tx.send(Ok(item)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }));
            }

            // `tx` itself (not a clone) stays alive across the join below, so
            // the channel can't close until we've had a chance to deliver a
            // terminal error -- dropped implicitly at the end of this block.
            if let Some(e) = join_first_error(handles).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        rx
    }

    /// Uploads `data` to a Unity Catalog volume path via the Files API,
    /// overwriting anything already there. `volume_path` is caller-supplied
    /// in full (e.g. `/Volumes/my_catalog/my_schema/my_volume/some/file.parquet`)
    /// -- this crate has no knowledge of any specific catalog/schema/volume.
    /// `Bytes` (not `Vec<u8>`) so a retry re-sends the same buffer via a
    /// cheap refcount clone, not a real copy.
    pub async fn upload_volume_file(&self, volume_path: &str, data: Bytes) -> Result<(), ApiError> {
        let url = format!("{}/api/2.0/fs/files{}", self.host, volume_path);
        self.retry_call(|| async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .put(&url)
                .query(&[("overwrite", "true")])
                .bearer_auth(&token)
                .header("Content-Type", "application/octet-stream")
                .body(data.clone())
                .timeout(self.http_timeout)
                .send()
                .await
                // `overwrite=true` means retrying this exact PUT is safe --
                // same idempotency reasoning as a GET.
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(ApiError::from_status(status, &text, true));
            }
            Ok(())
        })
        .await
    }

    /// Deletes a file at `volume_path` (see `upload_volume_file`). A 404 is
    /// treated as success -- the file is already gone, which is fine for
    /// idempotent staging cleanup.
    pub async fn delete_volume_file(&self, volume_path: &str) -> Result<(), ApiError> {
        let url = format!("{}/api/2.0/fs/files{}", self.host, volume_path);
        self.retry_call(|| async {
            let token = self.token_provider.get_token().await?;
            let resp = self
                .http
                .delete(&url)
                .bearer_auth(&token)
                .timeout(self.http_timeout)
                .send()
                .await
                .map_err(|e| ApiError::from_reqwest(e, true))?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND || status.is_success() {
                return Ok(());
            }
            let text = resp.text().await.unwrap_or_default();
            Err(ApiError::from_status(status, &text, true))
        })
        .await
    }
}

/// Joins every handle, returning the first error -- whether the task
/// returned `Err(ApiError)` or the task itself panicked (`Err(JoinError)`,
/// e.g. from a poisoned mutex after a sibling panicked first). Missing the
/// panic case would let that worker's unfetched work vanish with no error at
/// all: the channel closing normally looks to the consumer exactly like a
/// complete, successful result instead of a truncated one.
async fn join_first_error(handles: Vec<tokio::task::JoinHandle<Result<(), ApiError>>>) -> Option<ApiError> {
    let mut first_err = None;
    for h in handles {
        let outcome = match h.await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(join_err) => Some(join_error(join_err)),
        };
        if first_err.is_none() {
            first_err = outcome;
        }
    }
    first_err
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the exact value, not just "some big number" -- found in review
    /// that nothing caught an accidental revert (e.g. during a merge
    /// conflict) back toward the old, too-small 100 MiB default, which
    /// would silently reintroduce the round-trip regression documented on
    /// this constant's own doc comment (a 2M-row query needing 29
    /// `FetchResults` calls instead of 2). 1 GiB is the real, measured
    /// server-side ceiling -- see that doc comment for the numbers -- so
    /// this isn't an arbitrary value to protect, it's the actual limit.
    #[test]
    fn thrift_direct_results_max_bytes_is_the_measured_one_gib_ceiling() {
        assert_eq!(THRIFT_DIRECT_RESULTS_MAX_BYTES, 1024 * 1024 * 1024);
    }

    /// Regression test for a real bug found by testing against an actual
    /// Databricks workspace (not just synthetic single-frame test data): a
    /// real chunk's LZ4 compression is several frames concatenated back to
    /// back (a 50k-row chunk came back as 18), and `decompress_lz4_frame`
    /// originally only decoded the first one via one `read_to_end` call --
    /// silently truncating to just that frame's ~200 bytes (the Arrow schema
    /// message, no row data) with no error at all, which `pipeline.rs` then
    /// happily decoded as an empty result.
    #[test]
    fn decompress_lz4_frame_reads_every_concatenated_frame() {
        use std::io::Write;

        fn compress_one_frame(data: &[u8]) -> Vec<u8> {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        let part_a = b"the quick brown fox jumps over the lazy dog ".repeat(50);
        let part_b = b"pack my box with five dozen liquor jugs ".repeat(50);
        let part_c = b"how vexingly quick daft zebras jump ".repeat(50);
        let mut concatenated_frames = Vec::new();
        concatenated_frames.extend(compress_one_frame(&part_a));
        concatenated_frames.extend(compress_one_frame(&part_b));
        concatenated_frames.extend(compress_one_frame(&part_c));

        let decompressed = decompress_lz4_frame(&Bytes::from(concatenated_frames)).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&part_a);
        expected.extend_from_slice(&part_b);
        expected.extend_from_slice(&part_c);
        assert_eq!(decompressed, Bytes::from(expected));
    }

    /// Regression test for a bug found in code review: a real, valid LZ4
    /// Frame that happens to decode to zero bytes (a header immediately
    /// followed by an EndMark -- a legal frame shape, not malformed input)
    /// makes `read_to_end` return `Ok(0)` for that frame without erroring.
    /// The old loop read "output didn't grow" as "no more frames" and
    /// stopped there, silently dropping every frame concatenated after the
    /// empty one. `decompress_lz4_frame` must keep going as long as the
    /// underlying reader still has bytes left, not just as long as output
    /// keeps growing.
    #[test]
    fn decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle() {
        use std::io::Write;

        fn compress_one_frame(data: &[u8]) -> Vec<u8> {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(data).unwrap();
            encoder.finish().unwrap()
        }

        let part_a = b"the quick brown fox jumps over the lazy dog ".repeat(50);
        let part_b = b"pack my box with five dozen liquor jugs ".repeat(50);
        let mut concatenated_frames = Vec::new();
        concatenated_frames.extend(compress_one_frame(&part_a));
        concatenated_frames.extend(compress_one_frame(b"")); // real frame, zero content
        concatenated_frames.extend(compress_one_frame(&part_b));

        let decompressed = decompress_lz4_frame(&Bytes::from(concatenated_frames)).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&part_a);
        expected.extend_from_slice(&part_b);
        assert_eq!(
            decompressed,
            Bytes::from(expected),
            "the frame after the zero-content one must not be silently dropped"
        );
    }

    fn ok_task() -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async { Ok(()) })
    }

    fn err_task(msg: &'static str) -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async move {
            Err(ApiError {
                message: msg.to_string(),
                transient: false,
                kind: ApiErrorKind::Other,
            })
        })
    }

    fn panicking_task() -> tokio::task::JoinHandle<Result<(), ApiError>> {
        tokio::spawn(async { panic!("simulated worker panic (e.g. poisoned mutex)") })
    }

    #[tokio::test]
    async fn join_first_error_none_when_all_succeed() {
        let handles = vec![ok_task(), ok_task(), ok_task()];
        assert!(join_first_error(handles).await.is_none());
    }

    #[tokio::test]
    async fn join_first_error_surfaces_returned_error() {
        let handles = vec![ok_task(), err_task("boom"), ok_task()];
        let err = join_first_error(handles).await.expect("expected an error");
        assert_eq!(err.message, "boom");
    }

    /// Regression test for the bug found in code review: a worker task that
    /// *panics* (not returns Err) must still surface as an error, not vanish
    /// silently. Before the fix, `Err(JoinError)` matched neither `Ok(Err(_))`
    /// nor anything else and was dropped -- the caller would have gotten a
    /// clean, silently-truncated result instead of an error.
    #[tokio::test]
    async fn join_first_error_surfaces_panic_not_silence() {
        let handles = vec![ok_task(), panicking_task(), ok_task()];
        let err = join_first_error(handles)
            .await
            .expect("a panicking task must surface as an error, not vanish");
        assert!(
            err.message.contains("panicked"),
            "error message should mention the panic: {}",
            err.message
        );
    }

    /// Regression test for a real bug found against a real workspace: a
    /// 400-chunk/5.6M-row fetch failed permanently on one of many large
    /// concurrent blob downloads with reqwest's "error decoding response
    /// body" -- a connection that closes early mid-body (`Kind::Decode`,
    /// see `ApiError::from_reqwest`'s comment), not a genuinely dead
    /// endpoint. Before the fix, `transient` was unconditionally `false` for
    /// every reqwest error, so `retry_call` never got a second attempt and
    /// the whole query failed outright. A raw truncated-response server
    /// (rather than wiremock, which doesn't expose a way to violate its own
    /// Content-Length) reproduces the same client-side error reqwest raised
    /// against the real blob storage endpoint.
    #[tokio::test]
    async fn fetch_link_bytes_retries_after_a_connection_closed_mid_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                attempt += 1;
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                if attempt == 1 {
                    // Claims 100 bytes, sends 10, then closes -- reqwest's
                    // `.bytes()` surfaces exactly this as `Kind::Decode`.
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
                        .await;
                    let _ = socket.shutdown().await;
                } else {
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello")
                        .await;
                    let _ = socket.shutdown().await;
                    return;
                }
            }
        });

        let client = DbClient::new(&format!("http://{addr}"), "wh-test", "fake-token");
        let stats = QueryStatsAccumulator::default();
        let bytes = client
            .fetch_link_bytes(&format!("http://{addr}/data"), false, &stats)
            .await
            .expect("must retry past the truncated first attempt and succeed on the second");
        assert_eq!(&bytes[..], b"hello");
        assert_eq!(
            stats.retry_count.load(Ordering::Relaxed),
            1,
            "the one retry after the truncated first attempt must be counted"
        );
        assert_eq!(bytes.len() as u64, stats.bytes_downloaded.load(Ordering::Relaxed));
    }
}

/// Property-based fuzzing for `decompress_lz4_frame` -- the other real,
/// previously-shipped bug class this crate's hand-rolled/third-party-library-
/// adjacent parsing code has already produced (the multi-frame truncation
/// bug documented on this function's own call site in `execute_statement`'s
/// doc comment, and `decompress_lz4_frame_survives_a_zero_content_frame_in_the_middle`
/// above -- both found by real-workspace testing, not fuzzing). Separate
/// `#[cfg(test)]` module from `mod tests` above for the same reason
/// `thrift.rs`/`json_convert.rs`'s own `mod proptests` are split out.
#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Arbitrary/malformed/truncated/empty bytes reinterpreted as an LZ4
        /// Frame: must never panic (most inputs are simply not a valid LZ4
        /// Frame at all and should return `Err`; a few short/empty inputs
        /// are legal-but-trivial frames and should return `Ok` with little
        /// or no content -- either outcome is fine, only a panic is a bug).
        #[test]
        fn decompress_lz4_frame_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = decompress_lz4_frame(&Bytes::from(bytes));
        }

        /// A *truncated* real frame -- compress real data, then chop the
        /// tail off at an arbitrary point -- targeting the multi-frame loop
        /// specifically (a cut mid-frame, mid-block, or exactly on a frame
        /// boundary), which arbitrary random bytes essentially never
        /// produce (LZ4 Frame's magic number alone is 4 specific bytes).
        #[test]
        fn decompress_lz4_frame_never_panics_on_a_truncated_real_frame(
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
            cut_at_fraction in 0.0f64..=1.0,
        ) {
            use std::io::Write;
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(&payload).unwrap();
            let full = encoder.finish().unwrap();
            let cut = ((full.len() as f64) * cut_at_fraction) as usize;
            let _ = decompress_lz4_frame(&Bytes::from(full[..cut].to_vec()));
        }
    }
}
