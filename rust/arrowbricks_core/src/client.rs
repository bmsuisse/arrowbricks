//! Port of `client.py`'s statement submit/poll/retry logic. No Arrow
//! dependency here on purpose, same reasoning as the Python original: chunk
//! bytes are handed off raw, decoding happens in `pipeline.rs`.
//!
//! Split into submodules along this crate's own natural seams:
//!
//! - `error` -- `ApiError`/`ApiErrorKind`, this crate's error type and its
//!   coarse retry/auth/statement-failure classification.
//! - `model` -- shared data-model types passed between this client and
//!   `pipeline.rs` (`CancelHandle`, `QueryStatsAccumulator`/`QueryStatsData`/
//!   `EventSink`, `ChunkMeta`/`StatementSubmitResult`/`InlineOrExternal`/
//!   `ChunkItem`, `ColumnDescription`).
//! - `download` -- downloading/decompressing one cloud-fetch external link,
//!   shared by both backends' chunk-fetch workers.
//! - `sea` -- the REST Statement-Execution-API backend: session pooling,
//!   statement submit/poll, chunk-manifest resolution/fetch.
//! - `thrift_rpc` -- the Thrift/TCLIService backend: RPC framing/dispatch,
//!   session pooling.
//! - `volume` -- Unity Catalog volume file upload/delete.
//!
//! This file itself keeps `DbClient`'s identity/construction, its
//! configuration builders, and the generic authenticated-HTTP/retry/
//! warehouse-availability machinery every backend and feature builds on
//! (`retry_call`/`retry_call_tracked`/`authed_json`/`ensure_warehouse_running`),
//! plus protocol-dispatching infra used by both backends (`cancel_statement`).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde::Deserialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::Value;

use crate::thrift;

mod download;
mod error;
mod model;
mod sea;
mod thrift_rpc;
mod volume;

pub(crate) use error::join_error;
pub use error::{ApiError, ApiErrorKind};
pub use model::{
    CancelHandle, ChunkItem, ChunkMeta, ColumnDescription, EventSink, InlineOrExternal, QueryStatsAccumulator,
    QueryStatsData, StatementSubmitResult,
};

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

pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(2);
const RETRY_ATTEMPTS: u32 = 6;
const RETRY_MAX_WAIT_S: f64 = 20.0;

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
    /// compression on every statement -- see `client/sea.rs`'s
    /// `execute_statement`. Runtime
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
    /// (`pipeline/thrift_exec.rs`'s `execute_lazy_thrift`) before any
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
    async fn retry_call_tracked<F, Fut, T>(
        &self,
        stats: Option<&QueryStatsAccumulator>,
        mut f: F,
    ) -> Result<T, ApiError>
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

    /// Fire-and-forget best-effort server-side cancel of one in-flight
    /// statement/operation -- SEA's `POST .../cancel`, or Thrift's
    /// `CancelOperation`. Never awaited before a `QueryTimeout`/cancellation
    /// surfaces to the caller (see `heartbeat.rs`'s own two call sites,
    /// which spawn this rather than awaiting it inline) -- the result is
    /// intentionally ignored either way, same "best effort, Databricks'
    /// own server-side TTL reaps the rest on failure" pattern as
    /// `client/sea.rs`'s `delete_session`/`client/thrift_rpc.rs`'s
    /// `thrift_close_operation_best_effort`.
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
                // *cancel* POST is safe (unlike the statement-submit POST in
                // `client/sea.rs`'s `execute_statement` -- or Thrift's
                // `ExecuteStatement` RPC in `client/thrift_rpc.rs`'s
                // `thrift_execute_statement_raw`): cancelling an already-
                // cancelled/already-terminal statement is a no-op, not a
                // second execution of caller SQL.
                let _ = self
                    .retry_call(|| async {
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
}
