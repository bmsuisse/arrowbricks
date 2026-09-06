pub mod client;
pub mod heartbeat;
pub mod json_convert;
pub mod pipeline;
pub mod thrift;

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use pyo3_arrow::PyTable;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::Mutex as AsyncMutex;

use client::{
    ApiError, ApiErrorKind, CancelHandle, DbClient, EventSink, Protocol, QueryStatsAccumulator, QueryStatsData,
    TokenFuture, TokenProvider,
};
use heartbeat::{HeartbeatStream, HeartbeatWait, Tick};
use pipeline::{NdjsonStream, ResultStream};

/// Writes any object implementing `__arrow_c_stream__` (a `Table`/
/// `RecordBatchReader` from this crate, arro3, pyarrow, or anything else
/// Arrow-C-Data-Interface-compatible) as Arrow-IPC stream bytes to `buf` (a
/// Python file-like object with a `.write(bytes)` method) -- always
/// uncompressed. See `_streaming.py`'s own `write_ipc_stream` (the
/// user-facing wrapper) for why uncompressed is the safe default.
#[pyfunction]
#[pyo3(signature = (stream, buf))]
fn write_ipc_stream(py: Python<'_>, stream: Bound<'_, PyAny>, buf: Bound<'_, PyAny>) -> PyResult<()> {
    let any_rb: AnyRecordBatch = stream.extract()?;
    let mut reader = any_rb.into_reader()?;
    let schema = reader.schema();
    let mut ipc_buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut ipc_buf, &schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC write error: {e}")))?;
        for batch in reader.by_ref() {
            let batch = batch.map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC write error: {e}")))?;
            writer
                .write(&batch)
                .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC write error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC write error: {e}")))?;
    }
    buf.call_method1("write", (PyBytes::new(py, &ipc_buf),))?;
    Ok(())
}

/// Read-side counterpart to `write_ipc_stream`: parses raw Arrow-IPC stream
/// bytes (e.g. a previously-downloaded chunk, or anything `write_ipc_stream`
/// itself wrote) back into a `Table`. No dependency needed regardless of
/// where the bytes came from -- backs `ReplayableArrowChunk`, which needs to
/// re-parse the same cached bytes on every `__arrow_c_stream__` call.
#[pyfunction]
#[pyo3(signature = (data))]
fn read_ipc_stream(data: Bound<'_, PyBytes>) -> PyResult<PyTable> {
    let py = data.py();
    // The immutable Python bytes own the memory for as long as any decoded
    // array needs it, including arrays exported through the C Data Interface.
    // PyBackedBytes provides that ownership without copying or custom unsafe code.
    let blob = bytes::Bytes::from_owner(pyo3::pybacked::PyBackedBytes::from(data));
    let (batches, schema) = py
        .detach(|| pipeline::decode_ipc_stream(&blob))
        .map_err(|e| PyRuntimeError::new_err(e.message))?;
    PyTable::try_new(batches, schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Wraps a `PyErr` raised by the caller's own `token_provider` callable (its
/// call itself, awaiting its coroutine, or extracting a non-`str` return
/// value) into an `ApiError`. This function's *only* caller is
/// `PyTokenProvider::get_token` -- every `PyErr` reaching it happened while
/// this crate was specifically trying to obtain a bearer token, so `kind:
/// ApiErrorKind::Auth` is justified by that calling context alone, not by
/// inspecting the exception's own type (there's no reliable way to tell
/// "the token provider raised an auth-specific error" from "it raised some
/// other exception" from the exception object itself -- a caller's
/// `token_provider` can raise anything). Found in code review: this used to
/// be unconditionally `ApiErrorKind::Other`, so a `token_provider` that
/// itself fails with an auth error (e.g. a refreshed OAuth token comes back
/// unauthorized) never surfaced as `AuthError` -- exactly the case
/// README.md's `except AuthError: refresh_credentials()` pattern most wants
/// to catch. `transient: false` (unchanged): a `token_provider` failure is
/// not retried by `retry_call_tracked` the way a transient HTTP status is --
/// if the caller's own callable is broken, retrying immediately without
/// giving it a chance to fix itself isn't obviously safe either, so this
/// isn't the fix's concern.
fn py_err_to_api_error(e: PyErr) -> ApiError {
    ApiError {
        message: e.to_string(),
        transient: false,
        kind: ApiErrorKind::Auth,
    }
}

// ---- Typed error taxonomy (2026-08-11 design doc) -------------------------
//
// Every `ApiError` crossing this FFI boundary used to collapse to a plain
// `PyRuntimeError::new_err(e.message)` at ~15 call sites below -- a caller
// couldn't programmatically distinguish "safe to retry" from "don't retry"
// from "auth problem" without regex-parsing `str(exc)`. `ApiError` already
// carried `transient: bool` (and, as of the same change that added this
// hierarchy, `kind: ApiErrorKind`) internally; this section wires both
// across the boundary as real, catchable Python exception *types* instead --
// see `api_error_to_pyerr` below, used at every one of those call sites.
//
// `create_exception!` (not a hand-rolled `#[pyclass]`) is the standard PyO3
// pattern for a new Python exception type -- it registers a real CPython
// exception class with the given base, so `isinstance`/`except` work exactly
// like any built-in exception, and (unlike defining these as plain Python
// classes in `_errors.py` and reaching for them via `py.import(...)` from
// Rust on every error) needs no per-error Python-level module lookup.
//
// Deliberately kept small and flat: a single `ArrowbricksError` base
// (subclassing `PyRuntimeError`, not `PyException` -- see its own doc
// comment for why backward compatibility matters here), then exactly three
// subclasses split by what actually changes a caller's response:
// `TransientError` (backoff and retry -- every internal retry in
// `retry_call_tracked` was already exhausted by the time this reaches
// Python), `AuthError` (401/403 survived every internal retry too, each of
// which re-fetched a token -- the credential itself is bad, not the
// request), and `StatementError` (the SQL statement failed/was canceled
// server-side -- retrying the identical statement fails identically, this
// is not a transport problem at all). Everything else (parse/decode/
// internal-invariant errors, and any permanent non-auth/non-statement HTTP
// status) stays the plain `ArrowbricksError` base.
//
// `QueryTimeout` (`_streaming.py`) is NOT one of these -- it's raised from
// Python, not across this boundary (see `cursor.py`'s own `RuntimeError` ->
// `QueryTimeout` translation, matched on `heartbeat.rs`'s stable
// `"Query exceeded {secs}s timeout"` message prefix), and stays that way.
// It was changed to additionally subclass `ArrowbricksError` (Python side)
// for a consistent single catch-all (`except ArrowbricksError` now catches
// every exception this package raises itself, timeout included) -- see
// `_streaming.py`'s own doc comment on that choice.
pyo3::create_exception!(
    _core,
    ArrowbricksError,
    pyo3::exceptions::PyRuntimeError,
    "Base class for every exception arrowbricks raises itself (as opposed to \
     some other library's exception bubbling up unchanged). Subclasses \
     RuntimeError, not Exception, so an `except RuntimeError` written before \
     this hierarchy existed keeps working unchanged -- see README.md's \
     \"Errors\" section."
);
pyo3::create_exception!(
    _core,
    TransientError,
    ArrowbricksError,
    "A retryable failure (network blip, connection reset, or a 5xx from \
     Databricks) that survived every internal retry (`retry_attempts`, \
     exponential backoff) before ever reaching Python. Backing off further \
     and trying again later is the right response -- retrying immediately \
     just repeats what already failed."
);
pyo3::create_exception!(
    _core,
    AuthError,
    ArrowbricksError,
    "The request was rejected as unauthorized/forbidden (HTTP 401/403), \
     even after every internal retry re-fetched a token from \
     `token_provider`. Treat the credential itself as bad/expired, not a \
     transient blip -- retrying with the same token will fail identically."
);
pyo3::create_exception!(
    _core,
    StatementError,
    ArrowbricksError,
    "The SQL statement itself failed or was canceled server-side (a \
     Databricks FAILED/CANCELED statement state, or a Thrift operation's \
     terminal error) -- bad SQL, a permissions error on the underlying \
     table, or a warehouse-side query failure. Retrying the identical \
     statement will fail identically; this is not a transport problem."
);

/// Maps an `ApiError` crossing the PyO3 boundary to the right exception
/// *type* (see the section doc comment above), not just a message -- used at
/// every `ApiError` -> `PyErr` conversion in this file instead of a bare
/// `PyRuntimeError::new_err(e.message)`. `kind` takes priority over
/// `transient`: a 401/403 is `transient: true` too (internally retryable --
/// each retry re-fetches a token), but once every retry is exhausted and
/// this function actually runs, `AuthError` is the more useful signal to a
/// caller than a generic "retryable" one.
fn api_error_to_pyerr(e: ApiError) -> PyErr {
    match e.kind {
        ApiErrorKind::Auth => AuthError::new_err(e.message),
        ApiErrorKind::Statement => StatementError::new_err(e.message),
        ApiErrorKind::Other if e.transient => TransientError::new_err(e.message),
        ApiErrorKind::Other => ArrowbricksError::new_err(e.message),
    }
}

/// Converts `parameters` (Databricks' own named-parameter format --
/// `[{"name": ..., "value": ..., "type": ...}]`, matching Python's own
/// `list[dict[str, Any]] | None`) from a raw Python object into
/// `serde_json::Value`, passed straight through to the request body -- this
/// crate does no validation of its own shape either, same as the Python
/// original.
fn parameters_to_value(py: Python<'_>, parameters: Option<Py<PyAny>>) -> PyResult<Option<serde_json::Value>> {
    parameters
        .map(|p| pythonize::depythonize::<serde_json::Value>(p.bind(py)))
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("bad `parameters`: {e}")))
}

/// Marker type for the `HEARTBEAT` sentinel -- matches Python's own
/// `_streaming.py` (`class _Heartbeat: ...; HEARTBEAT = _Heartbeat()`): one
/// singleton instance, so a caller's `item is HEARTBEAT` identity check
/// works. `PyOnceLock` lazily creates that single instance on first use and
/// hands out clones of the *same* underlying object thereafter.
#[pyclass(name = "_Heartbeat")]
struct PyHeartbeat;

#[pymethods]
impl PyHeartbeat {
    fn __repr__(&self) -> &'static str {
        "HEARTBEAT"
    }
}

static HEARTBEAT_SINGLETON: PyOnceLock<Py<PyHeartbeat>> = PyOnceLock::new();

fn heartbeat_singleton(py: Python<'_>) -> PyResult<Py<PyHeartbeat>> {
    let cell: &Py<PyHeartbeat> = HEARTBEAT_SINGLETON.get_or_try_init(py, || Py::new(py, PyHeartbeat))?;
    Ok(cell.clone_ref(py))
}

/// Bridges a Python `token_provider` callable (sync or async, matching
/// `TokenProvider = Callable[[], str | Awaitable[str]]`) into Rust's
/// `client::TokenProvider` trait. Calling it re-attaches to the GIL only for
/// the parts that actually touch Python -- the call itself, the awaitable
/// check, and extracting the final string -- not for the whole future,
/// since `future_into_py`'s machinery already detaches the GIL around
/// whatever this future awaits.
///
/// `get_token` isn't only ever called from the outermost task
/// `future_into_py` wraps -- `execute()`'s `ResultSet` spawns chunk-fetch
/// worker tasks (`fetch_chunks_with_backpressure`) that each call an
/// authenticated endpoint (chunk-index resolution) too, and those inner
/// `tokio::spawn`ed tasks don't inherit the outer task's asyncio-event-loop
/// context. Calling `pyo3_async_runtimes::tokio::into_future` from one of
/// them fails with "no running event loop" -- caught by testing an async
/// `token_provider` against a multi-chunk result, not by the eager/sync
/// cases alone. Fix: capture the current task's `TaskLocals` (guaranteed to
/// be the outer context, since `execute_arrow_statement` always needs a
/// token before any worker is spawned) and cache it, so later calls --
/// including from worker tasks -- run inside `pyo3_async_runtimes::tokio::scope`
/// with that same captured context instead of trying to discover one from
/// whatever task happens to call.
///
/// The capture re-runs on **every** call from a context that has its own
/// loop, not just the first ever -- found in code review that caching only
/// once-if-`None` pins the `DbClient` (which persists across many separate
/// `execute()` calls, by design -- see its own doc comment) to whichever
/// event loop happened to be running the very first time a token was ever
/// requested. A second, later `asyncio.run()` (or any fresh loop -- a
/// restarted worker, a new pytest-asyncio test) then scopes the awaited
/// provider onto a loop that's already closed, failing with "Event loop is
/// closed" instead of just using the current one. Re-capturing costs
/// nothing extra from a worker task (its own attempt fails, same as before,
/// falling through to whatever's cached) since the outer call for *this*
/// statement already refreshed the cache before any worker was spawned.
struct PyTokenProvider {
    callable: Py<PyAny>,
    locals: Mutex<Option<TaskLocals>>,
}

impl TokenProvider for PyTokenProvider {
    fn get_token(&self) -> TokenFuture {
        // One `attach` for both: neither call below crosses an `.await`, so
        // there's no need to pay for two separate GIL-attach round trips.
        let (callable, locals) = {
            let mut guard = self.locals.lock().unwrap();
            Python::attach(|py| {
                let callable = self.callable.clone_ref(py);
                // Unconditional, not `if guard.is_none()` -- see this
                // struct's own doc comment for why. Best-effort: if this
                // particular call isn't in a context with a running loop
                // either (a worker task), leave whatever's already cached
                // alone and fall through to using that (or the no-scope
                // path below, if nothing has ever been captured at all).
                if let Ok(captured) = pyo3_async_runtimes::tokio::get_current_locals(py) {
                    *guard = Some(captured);
                }
                (callable, guard.clone())
            })
        };

        Box::pin(async move {
            // Same one-`attach` reasoning as above: calling the token
            // callable and checking `__await__` on its result never cross
            // an `.await` either.
            let (called, is_awaitable): (Py<PyAny>, bool) = Python::attach(|py| {
                let bound = callable.bind(py).call0()?;
                // Mirrors Python's own `inspect.isawaitable(result)` check
                // in `_bearer_token`: a plain sync callable's return value
                // has no `__await__`, an async callable's coroutine/Future
                // does.
                let is_awaitable = bound.hasattr("__await__")?;
                Ok::<_, PyErr>((bound.unbind(), is_awaitable))
            })
            .map_err(py_err_to_api_error)?;

            let result_obj: Py<PyAny> = if is_awaitable {
                let awaited = async move {
                    let fut = Python::attach(|py| pyo3_async_runtimes::tokio::into_future(called.bind(py).clone()))
                        .map_err(py_err_to_api_error)?;
                    fut.await.map_err(py_err_to_api_error)
                };
                match locals {
                    Some(l) => pyo3_async_runtimes::tokio::scope(l, awaited).await?,
                    None => awaited.await?,
                }
            } else {
                called
            };

            Python::attach(|py| result_obj.bind(py).extract::<String>()).map_err(py_err_to_api_error)
        })
    }
}

/// One query's timing/counters, handed to an `on_event` callback exactly
/// once, at completion -- see `README.md`'s own `on_event` section for the
/// user-facing description of each field, and `client::QueryStatsData` (the
/// plain, PyO3-agnostic struct this wraps) for how it's assembled.
/// `#[pyo3(get)]` fields on a real class, not a plain dict -- matching
/// `ResultSet`'s own shape above, the existing convention this crate uses
/// for structured return values crossing the Rust/Python boundary.
#[pyclass(name = "QueryStats")]
struct PyQueryStats {
    #[pyo3(get)]
    statement_id: String,
    #[pyo3(get)]
    protocol: &'static str,
    #[pyo3(get)]
    warehouse_wait_s: f64,
    #[pyo3(get)]
    submit_to_ready_s: f64,
    #[pyo3(get)]
    fetch_s: f64,
    #[pyo3(get)]
    num_chunks: usize,
    #[pyo3(get)]
    bytes_downloaded: u64,
    #[pyo3(get)]
    retry_count: u32,
    #[pyo3(get)]
    concurrency_used: usize,
    #[pyo3(get)]
    outcome: &'static str,
}

impl From<QueryStatsData> for PyQueryStats {
    fn from(d: QueryStatsData) -> Self {
        Self {
            statement_id: d.statement_id,
            protocol: d.protocol,
            warehouse_wait_s: d.warehouse_wait_s,
            submit_to_ready_s: d.submit_to_ready_s,
            fetch_s: d.fetch_s,
            num_chunks: d.num_chunks,
            bytes_downloaded: d.bytes_downloaded,
            retry_count: d.retry_count,
            concurrency_used: d.concurrency_used,
            outcome: d.outcome,
        }
    }
}

#[pymethods]
impl PyQueryStats {
    fn __repr__(&self) -> String {
        format!(
            "QueryStats(statement_id={:?}, protocol={:?}, warehouse_wait_s={:.3}, submit_to_ready_s={:.3}, \
             fetch_s={:.3}, num_chunks={}, bytes_downloaded={}, retry_count={}, concurrency_used={}, outcome={:?})",
            self.statement_id,
            self.protocol,
            self.warehouse_wait_s,
            self.submit_to_ready_s,
            self.fetch_s,
            self.num_chunks,
            self.bytes_downloaded,
            self.retry_count,
            self.concurrency_used,
            self.outcome,
        )
    }
}

/// Bridges a Python `on_event` callable (sync or async, matching
/// `Callable[[QueryStats], None | Awaitable[None]]`) into Rust's
/// `client::EventSink` trait -- mirrors `PyTokenProvider`'s own sync/async
/// detection and `TaskLocals`-caching approach almost exactly, with one
/// deliberate difference driven by `EventSink`'s own fire-and-forget
/// contract: `on_event` here is a plain, *synchronous* method. It captures
/// whatever `TaskLocals` the *calling* context has right now (cheap, no
/// `.await`), then spawns the actual dispatch (which may itself need to
/// await an async callback) onto the background runtime and returns
/// immediately -- a slow or raising `on_event` must never block or fail the
/// query that already has its result.
///
/// **Known limitation, not fully closable without deeper changes:** an
/// *async* `on_event` fired from `pipeline.rs`'s `Drop`-triggered
/// abandonment path (the `total_timeout_s`/cancellation case -- see
/// `PoisonOnDrop`/`ReportOnDrop`'s own doc comments) runs inside a task on
/// `pyo3_async_runtimes`'s background runtime that was never scoped to any
/// asyncio event loop, so capturing `TaskLocals` *at that exact moment*
/// always fails -- same root cause as `PyTokenProvider`'s own doc comment
/// describes for chunk-fetch worker tasks. This falls back to whatever was
/// cached by an *earlier*, successful capture (e.g. a prior query on the
/// same client that reported success/error from a real event-loop context),
/// which works once a client has dispatched at least one such event, but
/// means a *sync* `on_event` (recommended -- it never needs a loop at all)
/// is the only fully reliable choice for observing a client's very first
/// query if that query is also the one that gets cancelled/timed out.
struct PyEventSink {
    callable: Py<PyAny>,
    locals: Mutex<Option<TaskLocals>>,
}

impl PyEventSink {
    async fn dispatch(callable: Py<PyAny>, locals: Option<TaskLocals>, stats: QueryStatsData) {
        // Every failure mode here (bad callable, callback raises, callback
        // returns something unawaitable-but-truthy, etc.) is swallowed --
        // see this struct's own doc comment. No logging crate is a
        // dependency of this workspace (see Cargo.toml), so there's nowhere
        // to record this beyond a `debug_assertions`-only trace, not worth
        // adding a dependency for.
        let result: PyResult<()> = async {
            let (called, is_awaitable): (Py<PyAny>, bool) = Python::attach(|py| {
                let py_stats = Py::new(py, PyQueryStats::from(stats))?;
                let bound = callable.bind(py).call1((py_stats,))?;
                let is_awaitable = bound.hasattr("__await__")?;
                Ok::<_, PyErr>((bound.unbind(), is_awaitable))
            })?;
            if is_awaitable {
                let awaited = async move {
                    let fut = Python::attach(|py| pyo3_async_runtimes::tokio::into_future(called.bind(py).clone()))?;
                    fut.await
                };
                match locals {
                    Some(l) => pyo3_async_runtimes::tokio::scope(l, awaited).await?,
                    None => awaited.await?,
                };
            }
            Ok(())
        }
        .await;
        #[cfg(debug_assertions)]
        if let Err(e) = &result {
            eprintln!("arrowbricks: on_event callback raised, ignored (fire-and-forget): {e}");
        }
        let _ = result;
    }
}

impl EventSink for PyEventSink {
    fn on_event(&self, stats: QueryStatsData) {
        let (callable, locals) = {
            let mut guard = self.locals.lock().unwrap();
            Python::attach(|py| {
                let callable = self.callable.clone_ref(py);
                if let Ok(captured) = pyo3_async_runtimes::tokio::get_current_locals(py) {
                    *guard = Some(captured);
                }
                (callable, guard.clone())
            })
        };
        // Fire-and-forget: intentionally not awaited, and not bound via
        // `let _ = ...` either (that specific pattern trips clippy's
        // `let_underscore_future`, which reasonably worries it's a
        // forgotten `.await` -- a bare statement makes the "detached on
        // purpose" intent unambiguous).
        pyo3_async_runtimes::tokio::get_runtime().spawn(Self::dispatch(callable, locals, stats));
    }
}

/// One Databricks SQL warehouse endpoint -- a persistent `reqwest::Client`
/// (connection pool) reused across every `execute` call, since repeated
/// TCP+TLS handshakes against the same host are pure waste.
#[pyclass(name = "Client")]
struct PyDbClient {
    inner: Arc<DbClient>,
}

#[pymethods]
impl PyDbClient {
    /// Auth is either `token` (a static string) or `token_provider` (a
    /// callable, sync or async, returning a token string -- called on every
    /// request, no caching here), matching `DatabricksClient`'s own
    /// `__init__` validation.
    #[new]
    #[pyo3(signature = (
        host,
        warehouse_id,
        token=None,
        token_provider=None,
        chunk_fetch_concurrency=64,
        http_timeout=60.0,
        wait_timeout="30s".to_string(),
        warehouse_start_timeout=300.0,
        warehouse_confirmed_running_ttl_s=30.0,
        compress_results=true,
        protocol="thrift".to_string(),
        on_event=None,
        // Same literal-defaults-in-four-places pattern as
        // `chunk_fetch_concurrency` above (see AGENTS.md's own entry on that
        // one's footgun, and `client.rs`'s `DbClient` doc comment on
        // `retry_attempts`/`retry_max_wait_s`) -- these two literals must
        // match `client.rs`'s `RETRY_ATTEMPTS`/`RETRY_MAX_WAIT_S` consts,
        // `client.py`'s kwarg defaults, and `_core.pyi`'s stub.
        retry_attempts=6,
        retry_max_wait_s=20.0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: String,
        warehouse_id: String,
        token: Option<String>,
        token_provider: Option<Py<PyAny>>,
        chunk_fetch_concurrency: usize,
        http_timeout: f64,
        wait_timeout: String,
        warehouse_start_timeout: f64,
        warehouse_confirmed_running_ttl_s: f64,
        compress_results: bool,
        protocol: String,
        on_event: Option<Py<PyAny>>,
        // Signed, not `u32` -- found in code review: a `u32` parameter makes
        // PyO3's own argument conversion reject a negative Python int (e.g.
        // `retry_attempts=-1`) with `OverflowError` *before* this
        // constructor body -- and its own `ValueError` below -- ever runs,
        // contradicting the `ValueError`-for-bad-retry-config contract
        // documented in `client.py`/README.md/CHANGELOG.md. `i64` accepts
        // any value a caller could plausibly pass (including negative) and
        // lets the explicit check below turn it into the documented,
        // catchable `ValueError` instead.
        retry_attempts: i64,
        retry_max_wait_s: f64,
    ) -> PyResult<Self> {
        let protocol = Protocol::parse(&protocol).map_err(PyValueError::new_err)?;
        // `Duration::from_secs_f64` panics on negative/NaN/infinite input --
        // reached directly from this constructor via `with_http_timeout`/
        // `with_warehouse_start_timeout`/`with_warehouse_confirmed_running_ttl`
        // below, a Rust panic across the FFI boundary surfaces to a Python
        // caller as an opaque `PanicException` instead of the `ValueError`
        // a bad constructor argument should raise. Found via adversarial
        // testing of hostile constructor args (e.g. `http_timeout=-5.0`),
        // not a real workload -- validated here, once, rather than making
        // every `with_*` setter fallible for every caller including
        // internal Rust ones that only ever pass known-good constants.
        for (name, seconds) in [
            ("http_timeout", http_timeout),
            ("warehouse_start_timeout", warehouse_start_timeout),
            ("warehouse_confirmed_running_ttl_s", warehouse_confirmed_running_ttl_s),
            ("retry_max_wait_s", retry_max_wait_s),
        ] {
            if !seconds.is_finite() || seconds < 0.0 {
                return Err(PyValueError::new_err(format!(
                    "{name} must be a finite, non-negative number of seconds, got {seconds}"
                )));
            }
        }
        // `retry_attempts - 1` in `DbClient::retry_call_tracked`'s loop would
        // underflow at 0 -- and 0 (or negative) attempts would mean "never
        // even try the request," not a sensible retry policy either way.
        // `< 1`, not `== 0`, since `retry_attempts` is signed (see its own
        // doc comment above) and must reject negative values too.
        if retry_attempts < 1 {
            return Err(PyValueError::new_err(format!(
                "retry_attempts must be at least 1, got {retry_attempts}"
            )));
        }
        // `try_from` (not a bare `as u32` cast) -- `retry_attempts` is proven
        // `>= 1` above, but not yet bounded above; a bare `as` cast would
        // silently wrap a pathological value like `u32::MAX as i64 + 1` into
        // some small, unrelated `u32` instead of erroring, the same "wrong
        // exception type" class of surprise this whole fix exists to close.
        let retry_attempts = u32::try_from(retry_attempts)
            .map_err(|_| PyValueError::new_err(format!("retry_attempts is too large: {retry_attempts}")))?;
        let db_client = match (token, token_provider) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "Client needs exactly one of `token` or `token_provider`, not both",
                ));
            }
            (Some(t), None) => DbClient::new(&host, &warehouse_id, &t),
            (None, Some(callable)) => {
                let provider: Arc<dyn TokenProvider> = Arc::new(PyTokenProvider {
                    callable,
                    locals: Mutex::new(None),
                });
                DbClient::with_token_provider(&host, &warehouse_id, provider)
            }
            (None, None) => return Err(PyValueError::new_err("Client needs either `token` or `token_provider`")),
        };
        let mut db_client = db_client
            .with_concurrency(chunk_fetch_concurrency)
            .with_http_timeout(http_timeout)
            .with_wait_timeout(wait_timeout)
            .with_warehouse_start_timeout(warehouse_start_timeout)
            .with_warehouse_confirmed_running_ttl(warehouse_confirmed_running_ttl_s)
            .with_compress_results(compress_results)
            .with_protocol(protocol)
            .with_retry_attempts(retry_attempts)
            .with_retry_max_wait_s(retry_max_wait_s);
        if let Some(callable) = on_event {
            let sink: Arc<dyn EventSink> = Arc::new(PyEventSink {
                callable,
                locals: Mutex::new(None),
            });
            db_client = db_client.with_on_event(sink);
        }
        Ok(Self {
            inner: Arc::new(db_client),
        })
    }

    /// Submits the statement and starts background chunk fetching, without
    /// pulling any of it yet -- returns a `ResultSet` for on-demand
    /// `fetchmany_arrow`/`fetchall_arrow`, mirroring `cursor.py`'s
    /// `execute()` + `_ResultSet` split (chunks fetched lazily as the
    /// caller actually needs them, not all upfront).
    ///
    /// `prefer_inline=True` tries `disposition: INLINE` first (see
    /// `pipeline::execute_lazy_prefer_inline`'s own doc comment) -- for a
    /// result the caller expects to be small, this can skip the chunk-fetch
    /// round trip entirely, at the cost of a second full statement execution
    /// if that expectation turns out wrong (too big, or an unsupported
    /// column type). Default `False`: unconditionally pays for two
    /// executions in that fallback case, which isn't worth it for a caller
    /// with no reason to expect a small result. Ignored (a silent no-op, not
    /// an error) when this client was constructed with `protocol="thrift"`
    /// -- Thrift has no INLINE-disposition equivalent, and its own
    /// `getDirectResults` mechanism already gets small-query latency
    /// without it (see `client::Protocol::Thrift`'s own doc comment).
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None, prefer_inline=false))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
        prefer_inline: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let client_for_result = client.clone();
        let parameters = parameters_to_value(py, parameters)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = if client.protocol == Protocol::Thrift {
                pipeline::execute_lazy_thrift(client, &statement, catalog.as_deref(), schema.as_deref(), parameters)
                    .await
            } else if prefer_inline {
                pipeline::execute_lazy_prefer_inline(
                    client,
                    &statement,
                    catalog.as_deref(),
                    schema.as_deref(),
                    parameters,
                )
                .await
            } else {
                pipeline::execute_lazy(client, &statement, catalog.as_deref(), schema.as_deref(), parameters).await
            }
            .map_err(api_error_to_pyerr)?;
            Ok(PyResultSet {
                statement_id: stream.statement_id.clone(),
                num_chunks: stream.num_chunks,
                columns: column_pairs(&stream.columns),
                cancel_handle: stream.cancel_handle.clone(),
                stats: stream.stats.clone(),
                client: client_for_result,
                inner: Arc::new(AsyncMutex::new(stream)),
            })
        })
    }

    /// Uploads `data` to a Unity Catalog volume path via the Files API,
    /// overwriting anything already there.
    #[pyo3(signature = (volume_path, data))]
    fn upload_volume_file<'py>(
        &self,
        py: Python<'py>,
        volume_path: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let data = bytes::Bytes::from(data);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .upload_volume_file(&volume_path, data)
                .await
                .map_err(api_error_to_pyerr)
        })
    }

    /// Deletes a file at `volume_path` (see `upload_volume_file`). A 404 is
    /// treated as success -- the file is already gone, fine for idempotent
    /// staging cleanup.
    fn delete_volume_file<'py>(&self, py: Python<'py>, volume_path: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .delete_volume_file(&volume_path)
                .await
                .map_err(api_error_to_pyerr)
        })
    }

    /// Best-effort close of every currently-idle pooled session -- both the
    /// SEA pool (`DbClient::close_all_sessions`) and the Thrift pool
    /// (`DbClient::close_all_thrift_sessions`); whichever one this client's
    /// `protocol` never used is simply empty and closes nothing. Meant to be
    /// called from `DatabricksClient.aclose()`. Never raises: a session that
    /// fails to close is simply left for Databricks' own server-side TTL to
    /// reap.
    fn close_sessions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.close_all_sessions().await;
            client.close_all_thrift_sessions().await;
            Ok(())
        })
    }

    /// Chunk-at-a-time counterpart to `execute`+`fetchall_arrow`, entirely
    /// backing `stream_query_json`: yields `HEARTBEAT` while waiting on each
    /// still-in-flight chunk (not just the initial statement wait), then a
    /// `list[str]` of NDJSON lines (one per row, arro3-`write_ndjson(
    /// explicit_nulls=True)`-compatible) per chunk as it arrives in logical
    /// order -- decode and JSON-encoding both happen here, so there's no
    /// further Python-side conversion step. Not async itself, same as
    /// `ResultSet.fetchall_arrow_streamed` -- the returned iterator's
    /// `__anext__` does the real work, including the initial submit/poll on
    /// its very first call.
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None, total_timeout_s=None, non_finite_as_string=false))]
    #[allow(clippy::too_many_arguments)]
    fn stream_ndjson_lines(
        &self,
        py: Python<'_>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
        total_timeout_s: Option<f64>,
        non_finite_as_string: bool,
    ) -> PyResult<PyNdjsonStreamIter> {
        let parameters = parameters_to_value(py, parameters)?;
        Ok(PyNdjsonStreamIter {
            state: Arc::new(AsyncMutex::new(PyNdjsonStreamState::Pending {
                client: self.inner.clone(),
                statement,
                catalog,
                schema,
                parameters,
                total_timeout_s,
                non_finite_as_string,
            })),
        })
    }
}

/// One statement's worth of lazily-fetched result. `fetchmany_arrow`/
/// `fetchall_arrow` pull and decode only as many chunks as needed to satisfy
/// the request, buffering the rest at the Arrow `RecordBatch` level.
#[pyclass(name = "ResultSet")]
struct PyResultSet {
    #[pyo3(get)]
    statement_id: String,
    #[pyo3(get)]
    num_chunks: usize,
    /// (name, type_name) pairs from the manifest -- a pre-fetch estimate
    /// only (Databricks doesn't include every manifest's schema, and this
    /// crate doesn't correct/validate it against the real Arrow schema);
    /// used for `Cursor.description`-style compatibility.
    #[pyo3(get)]
    columns: Vec<(String, Option<String>)>,
    /// Copied out at construction time (same reason `statement_id`/
    /// `num_chunks`/`columns` are, rather than reached through `inner`'s
    /// `AsyncMutex`): `fetchall_arrow_streamed` needs these to build its
    /// `HeartbeatWait::with_cancel` hook synchronously, without locking a
    /// mutex that's about to be locked again by that same call's own
    /// `fetchall_arrow()`.
    cancel_handle: CancelHandle,
    stats: Arc<QueryStatsAccumulator>,
    client: Arc<DbClient>,
    inner: Arc<AsyncMutex<ResultStream>>,
}

fn column_pairs(columns: &[client::ColumnDescription]) -> Vec<(String, Option<String>)> {
    columns.iter().map(|c| (c.name.clone(), c.type_name.clone())).collect()
}

/// `schema` is `None` for a zero-batch result -- falls back to an empty
/// schema rather than a schema-less `Table`.
fn batches_to_pytable(batches: Vec<RecordBatch>, schema: Option<SchemaRef>) -> PyResult<PyTable> {
    let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    PyTable::try_new(batches, schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymethods]
impl PyResultSet {
    /// Returns a `Table` with up to `n` rows -- fewer if the result is
    /// exhausted first (matching `_ResultSet.fetchmany_arrow`'s contract).
    fn fetchmany_arrow<'py>(&self, py: Python<'py>, n: usize) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let (batches, schema) = stream.fetchmany_arrow(n).await.map_err(api_error_to_pyerr)?;
            batches_to_pytable(batches, schema)
        })
    }

    /// Drains everything remaining into one `Table`.
    fn fetchall_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let (batches, schema) = stream.fetchall_arrow().await.map_err(api_error_to_pyerr)?;
            batches_to_pytable(batches, schema)
        })
    }

    /// Like `fetchall_arrow()`, but yields `HEARTBEAT` while pulling chunks
    /// instead of blocking silently -- for a caller bridging e.g. an SSE
    /// connection through the full download, not just `execute_streamed`'s
    /// initial wait for the statement to become ready. Downloading many
    /// chunks for a large result can itself take a while.
    #[pyo3(signature = (total_timeout_s=None))]
    fn fetchall_arrow_streamed(&self, total_timeout_s: Option<f64>) -> PyFetchallArrowStreamedIter {
        let inner = self.inner.clone();
        let fut = async move { inner.lock().await.fetchall_arrow().await };
        let wait = HeartbeatWait::new(fut, total_timeout_s).with_cancel(pipeline::cancel_hook(
            self.client.clone(),
            self.cancel_handle.clone(),
            self.stats.clone(),
        ));
        PyFetchallArrowStreamedIter {
            wait: Arc::new(AsyncMutex::new(Some(wait))),
        }
    }

    /// The real Arrow schema, once known (after at least one chunk has been
    /// fetched and decoded) -- `(name, type_name)` pairs, matching
    /// `Cursor.description`'s shape. `None` before any fetch (the caller
    /// should fall back to `columns`, the manifest-based pre-fetch
    /// estimate). Computed directly from the decoded `arrow_schema::Schema`
    /// here rather than via a returned `Table`'s own `.schema` property --
    /// that property specifically requires a *real* `arro3.core` install to
    /// construct its return value (by pyo3-arrow's own design, so callers
    /// get their own runtime's Schema type back), which would reintroduce
    /// exactly the dependency this crate's callers don't have.
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = inner.lock().await;
            Ok(stream.schema.as_ref().map(|s| {
                s.fields()
                    .iter()
                    .map(|f| (f.name().clone(), f.data_type().to_string()))
                    .collect::<Vec<_>>()
            }))
        })
    }
}

/// Async iterator returned by `ResultSet.fetchall_arrow_streamed`: yields
/// the `HEARTBEAT` singleton zero or more times, then a `Table` exactly
/// once, then stops.
#[pyclass(name = "FetchallArrowStreamedIter")]
struct PyFetchallArrowStreamedIter {
    wait: Arc<AsyncMutex<Option<HeartbeatWait<(Vec<RecordBatch>, Option<SchemaRef>)>>>>,
}

#[pymethods]
impl PyFetchallArrowStreamedIter {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let wait = self.wait.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = wait.lock().await;
            let Some(w) = guard.as_mut() else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match w.tick().await {
                Ok(Some(Tick::Heartbeat)) => Python::attach(|py| heartbeat_singleton(py).map(|h| h.into_any())),
                Ok(Some(Tick::Ready((batches, schema)))) => {
                    *guard = None;
                    let table = batches_to_pytable(batches, schema)?;
                    Python::attach(|py| Py::new(py, table).map(|t| t.into_any()))
                }
                Ok(None) => Err(PyStopAsyncIteration::new_err(())),
                Err(e) => {
                    *guard = None;
                    Err(api_error_to_pyerr(e))
                }
            }
        })
    }
}

/// Not-yet-started vs. running state for `PyNdjsonStreamIter`. The
/// submit/poll/spawn-workers step (`Pending` -> `Running`) happens on the
/// iterator's first `__anext__` call, un-heartbeated -- matching
/// `stream_query_json`'s pre-cutover behavior (its submit/poll wait was never
/// heartbeat-wrapped, only its chunk loop was). The `total_timeout_s` budget
/// starts counting from `Running`, not from construction, for the same
/// reason.
enum PyNdjsonStreamState {
    Pending {
        client: Arc<DbClient>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<serde_json::Value>,
        total_timeout_s: Option<f64>,
        non_finite_as_string: bool,
    },
    Running {
        stream: Arc<AsyncMutex<NdjsonStream>>,
        heartbeat: HeartbeatStream<Vec<String>>,
    },
    Done,
}

/// Async iterator returned by `Client.stream_ndjson_lines`: yields the
/// `HEARTBEAT` singleton while waiting on the statement or any individual
/// chunk, and a `list[str]` of NDJSON lines per chunk (in logical order) as
/// each arrives, until the result is exhausted.
#[pyclass(name = "NdjsonStreamIter")]
struct PyNdjsonStreamIter {
    state: Arc<AsyncMutex<PyNdjsonStreamState>>,
}

#[pymethods]
impl PyNdjsonStreamIter {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state.lock().await;
            loop {
                match &mut *guard {
                    PyNdjsonStreamState::Done => return Err(PyStopAsyncIteration::new_err(())),
                    PyNdjsonStreamState::Pending { .. } => {
                        let PyNdjsonStreamState::Pending {
                            client,
                            statement,
                            catalog,
                            schema,
                            parameters,
                            total_timeout_s,
                            non_finite_as_string,
                        } = std::mem::replace(&mut *guard, PyNdjsonStreamState::Done)
                        else {
                            unreachable!()
                        };
                        let client_for_cancel = client.clone();
                        let stream = pipeline::execute_ndjson_stream(
                            client,
                            &statement,
                            catalog.as_deref(),
                            schema.as_deref(),
                            parameters,
                            non_finite_as_string,
                        )
                        .await
                        .map_err(api_error_to_pyerr)?;
                        let heartbeat = HeartbeatStream::new(total_timeout_s).with_cancel(pipeline::cancel_hook(
                            client_for_cancel,
                            stream.cancel_handle.clone(),
                            stream.stats.clone(),
                        ));
                        *guard = PyNdjsonStreamState::Running {
                            stream: Arc::new(AsyncMutex::new(stream)),
                            heartbeat,
                        };
                    }
                    PyNdjsonStreamState::Running { stream, heartbeat } => {
                        let stream_for_pull = stream.clone();
                        let tick_result = heartbeat
                            .tick(move || Box::pin(async move { stream_for_pull.lock().await.next_chunk().await }))
                            .await;
                        return match tick_result {
                            Ok(Some(Tick::Heartbeat)) => {
                                Python::attach(|py| heartbeat_singleton(py).map(|h| h.into_any()))
                            }
                            Ok(Some(Tick::Ready(lines))) => {
                                Python::attach(|py| Ok(lines.into_pyobject(py)?.into_any().unbind()))
                            }
                            Ok(None) => {
                                *guard = PyNdjsonStreamState::Done;
                                Err(PyStopAsyncIteration::new_err(()))
                            }
                            Err(e) => {
                                *guard = PyNdjsonStreamState::Done;
                                Err(api_error_to_pyerr(e))
                            }
                        };
                    }
                }
            }
        })
    }
}

/// Registers as `arrowbricks._core` -- a compiled submodule bundled inside
/// the same `arrowbricks` wheel, not a separately published package. See
/// `[tool.maturin]` in the repo-root `pyproject.toml` (module-name =
/// "arrowbricks._core", manifest-path pointing back at this crate).
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(write_ipc_stream, m)?)?;
    m.add_function(wrap_pyfunction!(read_ipc_stream, m)?)?;
    m.add_class::<PyDbClient>()?;
    m.add_class::<PyResultSet>()?;
    m.add_class::<PyHeartbeat>()?;
    m.add_class::<PyFetchallArrowStreamedIter>()?;
    m.add_class::<PyNdjsonStreamIter>()?;
    m.add_class::<PyQueryStats>()?;
    m.add("HEARTBEAT", heartbeat_singleton(m.py())?)?;
    m.add("ArrowbricksError", m.py().get_type::<ArrowbricksError>())?;
    m.add("TransientError", m.py().get_type::<TransientError>())?;
    m.add("AuthError", m.py().get_type::<AuthError>())?;
    m.add("StatementError", m.py().get_type::<StatementError>())?;
    Ok(())
}
