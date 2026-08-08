pub mod client;
pub mod heartbeat;
pub mod json_convert;
pub mod pipeline;
pub mod thrift;

use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyBytes;
use pyo3_arrow::PyTable;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::Mutex as AsyncMutex;

use client::{ApiError, DbClient, Protocol, TokenFuture, TokenProvider};
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
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buf, &schema)
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
fn read_ipc_stream(data: &[u8]) -> PyResult<PyTable> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
        .map_err(|e| PyRuntimeError::new_err(format!("bad Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .map_err(|e| PyRuntimeError::new_err(format!("Arrow IPC decode error: {e}")))?;
    PyTable::try_new(batches, schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn py_err_to_api_error(e: PyErr) -> ApiError {
    ApiError {
        message: e.to_string(),
        transient: false,
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
        let callable = Python::attach(|py| self.callable.clone_ref(py));
        let locals = {
            let mut guard = self.locals.lock().unwrap();
            // Unconditional, not `if guard.is_none()` -- see this struct's
            // own doc comment for why. Best-effort: if this particular call
            // isn't in a context with a running loop either (a worker task),
            // leave whatever's already cached alone and fall through to
            // using that (or the no-scope path below, if nothing has ever
            // been captured at all).
            if let Ok(captured) = Python::attach(pyo3_async_runtimes::tokio::get_current_locals) {
                *guard = Some(captured);
            }
            guard.clone()
        };

        Box::pin(async move {
            let called: Py<PyAny> = Python::attach(|py| -> PyResult<Py<PyAny>> {
                let bound = callable.bind(py);
                Ok(bound.call0()?.unbind())
            })
            .map_err(py_err_to_api_error)?;

            // Mirrors Python's own `inspect.isawaitable(result)` check in
            // `_bearer_token`: a plain sync callable's return value has no
            // `__await__`, an async callable's coroutine/Future does.
            let is_awaitable =
                Python::attach(|py| called.bind(py).hasattr("__await__")).map_err(py_err_to_api_error)?;

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
        ] {
            if !seconds.is_finite() || seconds < 0.0 {
                return Err(PyValueError::new_err(format!(
                    "{name} must be a finite, non-negative number of seconds, got {seconds}"
                )));
            }
        }
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
        Ok(Self {
            inner: Arc::new(
                db_client
                    .with_concurrency(chunk_fetch_concurrency)
                    .with_http_timeout(http_timeout)
                    .with_wait_timeout(wait_timeout)
                    .with_warehouse_start_timeout(warehouse_start_timeout)
                    .with_warehouse_confirmed_running_ttl(warehouse_confirmed_running_ttl_s)
                    .with_compress_results(compress_results)
                    .with_protocol(protocol),
            ),
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
            .map_err(|e| PyRuntimeError::new_err(e.message))?;
            Ok(PyResultSet {
                statement_id: stream.statement_id.clone(),
                num_chunks: stream.num_chunks,
                columns: column_pairs(&stream.columns),
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
                .map_err(|e| PyRuntimeError::new_err(e.message))
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
                .map_err(|e| PyRuntimeError::new_err(e.message))
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
            let (batches, schema) = stream
                .fetchmany_arrow(n)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.message))?;
            batches_to_pytable(batches, schema)
        })
    }

    /// Drains everything remaining into one `Table`.
    fn fetchall_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let (batches, schema) = stream
                .fetchall_arrow()
                .await
                .map_err(|e| PyRuntimeError::new_err(e.message))?;
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
        PyFetchallArrowStreamedIter {
            wait: Arc::new(AsyncMutex::new(Some(HeartbeatWait::new(fut, total_timeout_s)))),
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
                    Err(PyRuntimeError::new_err(e.message))
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
                        let stream = pipeline::execute_ndjson_stream(
                            client,
                            &statement,
                            catalog.as_deref(),
                            schema.as_deref(),
                            parameters,
                            non_finite_as_string,
                        )
                        .await
                        .map_err(|e| PyRuntimeError::new_err(e.message))?;
                        *guard = PyNdjsonStreamState::Running {
                            stream: Arc::new(AsyncMutex::new(stream)),
                            heartbeat: HeartbeatStream::new(total_timeout_s),
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
                                Err(PyRuntimeError::new_err(e.message))
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
    m.add("HEARTBEAT", heartbeat_singleton(m.py())?)?;
    Ok(())
}
