//! `ApiError`/`ApiErrorKind` -- this crate's error type, its coarse
//! classification (transient/auth/statement-failure, see `ApiErrorKind`'s
//! own doc comment) carried across the PyO3 boundary so a Python caller can
//! distinguish "safe to retry" from "don't retry" from "auth problem"
//! programmatically, and its constructors from a `reqwest::Error`/HTTP
//! status response.

use reqwest::StatusCode;

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
    pub(crate) fn from_reqwest(e: reqwest::Error, idempotent: bool) -> Self {
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
    pub(crate) fn from_status(status: StatusCode, body: &str, idempotent: bool) -> Self {
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
/// fetch-worker join in `client/sea.rs`'s `fetch_chunks_with_backpressure`
/// and the decode `spawn_blocking` joins throughout this crate's `pipeline`
/// module -- a panicking task must surface as an error, not as a
/// quietly-truncated result set.
pub(crate) fn join_error(e: tokio::task::JoinError) -> ApiError {
    ApiError {
        message: format!("task panicked: {e}"),
        transient: false,
        kind: ApiErrorKind::Other,
    }
}
