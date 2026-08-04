pub mod client;
pub mod heartbeat;
pub mod pipeline;

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

use client::{ApiError, DbClient, TokenFuture, TokenProvider};
use heartbeat::{HeartbeatStream, HeartbeatWait, Tick};
use pipeline::{NdjsonStream, ResultStream};

#[pyfunction]
fn ping() -> &'static str {
    "pong"
}

/// Writes any object implementing `__arrow_c_stream__` (a `Table`/
/// `RecordBatchReader` from this crate, arro3, pyarrow, or anything else
/// Arrow-C-Data-Interface-compatible) as Arrow-IPC stream bytes to `buf` (a
/// Python file-like object with a `.write(bytes)` method) -- always
/// uncompressed. arro3's own default (`compression="LZ4"`) is transparently
/// decompressed by some Arrow readers (e.g. DuckDB's) but not all --
/// `duckdb-wasm`'s browser-side decoder silently fails to parse it. Plain,
/// uncompressed bodies are the safe default for bytes that might end up read
/// by anything.
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
/// `future_into_py` wraps -- `execute()`/`execute_arrow()` spawn chunk-fetch
/// worker tasks (`fetch_chunks_with_backpressure`) that each call an
/// authenticated endpoint (chunk-index resolution) too, and those inner
/// `tokio::spawn`ed tasks don't inherit the outer task's asyncio-event-loop
/// context. Calling `pyo3_async_runtimes::tokio::into_future` from one of
/// them fails with "no running event loop" -- caught by testing an async
/// `token_provider` against a multi-chunk result, not by the eager/sync
/// cases alone. Fix: capture the current task's `TaskLocals` on first use
/// (guaranteed to be the outer context, since `execute_arrow_statement`
/// always needs a token before any worker is spawned) and cache it, so
/// later calls -- including from worker tasks -- run inside
/// `pyo3_async_runtimes::tokio::scope` with that same captured context
/// instead of trying to discover one from whatever task happens to call.
struct PyTokenProvider {
    callable: Py<PyAny>,
    locals: Mutex<Option<TaskLocals>>,
}

impl TokenProvider for PyTokenProvider {
    fn get_token(&self) -> TokenFuture {
        let callable = Python::attach(|py| self.callable.clone_ref(py));
        let locals = {
            let mut guard = self.locals.lock().unwrap();
            if guard.is_none() {
                // Best-effort: if this call isn't in a context with a
                // running loop either, leave it None and fall through to
                // the no-scope path below (matches today's behavior).
                if let Ok(captured) = Python::attach(pyo3_async_runtimes::tokio::get_current_locals) {
                    *guard = Some(captured);
                }
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
/// (connection pool) reused across every `execute`/`execute_arrow` call,
/// same reason Python's own `DatabricksClient` reuses one `httpx.AsyncClient`
/// rather than building a fresh one per statement: repeated TCP+TLS
/// handshakes are pure waste against the same host.
#[pyclass(name = "Client")]
struct PyDbClient {
    inner: Arc<DbClient>,
}

#[pymethods]
impl PyDbClient {
    /// Auth is either `token` (a static string) or `token_provider` (a
    /// callable, sync or async, returning a token string -- called on every
    /// request, no caching here) -- exactly one of the two, matching
    /// `DatabricksClient`'s own `__init__` validation.
    #[new]
    #[pyo3(signature = (
        host,
        warehouse_id,
        token=None,
        token_provider=None,
        chunk_fetch_concurrency=32,
        http_timeout=60.0,
        wait_timeout="30s".to_string(),
        warehouse_start_timeout=300.0,
        warehouse_confirmed_running_ttl_s=30.0,
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
    ) -> PyResult<Self> {
        let db_client = match (token, token_provider) {
            (Some(t), _) => DbClient::new(&host, &warehouse_id, &t),
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
                    .with_warehouse_confirmed_running_ttl(warehouse_confirmed_running_ttl_s),
            ),
        })
    }

    /// Full submit->poll->fetch->reorder->decode pipeline for one
    /// ARROW_STREAM statement, eagerly assembling the whole result. Returns
    /// a `pyo3_arrow` `Table` -- it implements the Arrow C Data Interface
    /// (`__arrow_c_stream__`), so DuckDB/pyarrow/arro3 can all import it
    /// directly, zero-copy. For a large result where you don't want the
    /// whole thing pulled upfront, use `execute()` + `ResultSet.fetchmany_arrow`.
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None))]
    fn execute_arrow<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let parameters = parameters_to_value(py, parameters)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = pipeline::run_pipeline(client, &statement, catalog.as_deref(), schema.as_deref(), parameters)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.message))?;
            let arrow_schema = result.schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            PyTable::try_new(result.batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Submits the statement and starts background chunk fetching, without
    /// pulling any of it yet -- returns a `ResultSet` for on-demand
    /// `fetchmany_arrow`/`fetchall_arrow`, mirroring `cursor.py`'s
    /// `execute()` + `_ResultSet` split (chunks fetched lazily as the
    /// caller actually needs them, not all upfront).
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let parameters = parameters_to_value(py, parameters)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = pipeline::execute_lazy(client, &statement, catalog.as_deref(), schema.as_deref(), parameters)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.message))?;
            Ok(PyResultSet {
                statement_id: stream.statement_id.clone(),
                num_chunks: stream.num_chunks,
                columns: column_pairs(&stream.columns),
                inner: Arc::new(AsyncMutex::new(stream)),
            })
        })
    }

    /// Like `execute()`, but yields `HEARTBEAT` while waiting on Databricks
    /// instead of blocking silently -- for bridging e.g. an SSE connection
    /// during a possible multi-minute cold warehouse start. Yields
    /// `HEARTBEAT` zero or more times, then a `ResultSet` once ready to
    /// fetch. Not async itself (matches `cursor.py`'s own `execute_streamed`,
    /// a sync method returning an async generator) -- the submit/poll starts
    /// running immediately in the background regardless of when the caller
    /// starts iterating.
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None, total_timeout_s=None))]
    fn execute_streamed(
        &self,
        py: Python<'_>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
        total_timeout_s: Option<f64>,
    ) -> PyResult<PyExecuteStreamedIter> {
        let client = self.inner.clone();
        let parameters = parameters_to_value(py, parameters)?;
        let fut = async move {
            pipeline::execute_lazy(client, &statement, catalog.as_deref(), schema.as_deref(), parameters).await
        };
        Ok(PyExecuteStreamedIter {
            wait: Arc::new(AsyncMutex::new(Some(HeartbeatWait::new(fut, total_timeout_s)))),
        })
    }

    /// Full submit->poll->fetch->reorder pipeline for one JSON_ARRAY
    /// statement -- no Arrow parse at all. Returns a plain list of rows,
    /// each row a list of values where every non-null value is a *string*
    /// (Databricks' own JSON_ARRAY contract, not this crate's choice) --
    /// cast by the manifest's column type_name yourself if you want native
    /// Python types.
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None))]
    fn execute_json<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let parameters = parameters_to_value(py, parameters)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result =
                pipeline::run_json_pipeline(client, &statement, catalog.as_deref(), schema.as_deref(), parameters)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.message))?;
            Ok(result.rows)
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

    /// Chunk-at-a-time counterpart to `execute_arrow`, entirely backing
    /// `stream_query_json`: yields `HEARTBEAT` while waiting on each
    /// still-in-flight chunk (not just the initial statement wait), then a
    /// `list[str]` of NDJSON lines (one per row, arro3-`write_ndjson(
    /// explicit_nulls=True)`-compatible) per chunk as it arrives in logical
    /// order -- decode and JSON-encoding both happen here, so there's no
    /// further Python-side conversion step. Not async itself, same as
    /// `execute_streamed` -- the returned iterator's `__anext__` does the
    /// real work, including the initial submit/poll on its very first call.
    #[pyo3(signature = (statement, catalog=None, schema=None, parameters=None, total_timeout_s=None))]
    fn stream_ndjson_lines(
        &self,
        py: Python<'_>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
        parameters: Option<Py<PyAny>>,
        total_timeout_s: Option<f64>,
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
            let arrow_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            PyTable::try_new(batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
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
            let arrow_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            PyTable::try_new(batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
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

/// Async iterator returned by `Client.execute_streamed`: yields the
/// `HEARTBEAT` singleton zero or more times, then a `ResultSet` exactly
/// once, then stops.
#[pyclass(name = "ExecuteStreamedIter")]
struct PyExecuteStreamedIter {
    wait: Arc<AsyncMutex<Option<HeartbeatWait<ResultStream>>>>,
}

#[pymethods]
impl PyExecuteStreamedIter {
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
                Ok(Some(Tick::Ready(stream))) => {
                    *guard = None;
                    let result_set = PyResultSet {
                        statement_id: stream.statement_id.clone(),
                        num_chunks: stream.num_chunks,
                        columns: column_pairs(&stream.columns),
                        inner: Arc::new(AsyncMutex::new(stream)),
                    };
                    Python::attach(|py| Py::new(py, result_set).map(|rs| rs.into_any()))
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
                    let arrow_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
                    let table =
                        PyTable::try_new(batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
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
                        )
                        .await
                        .map_err(|e| PyRuntimeError::new_err(e.message))?;
                        *guard = PyNdjsonStreamState::Running {
                            stream: Arc::new(AsyncMutex::new(stream)),
                            heartbeat: HeartbeatStream::new(total_timeout_s),
                        };
                        // Loop back around to the now-`Running` arm below.
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
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    m.add_function(wrap_pyfunction!(write_ipc_stream, m)?)?;
    m.add_function(wrap_pyfunction!(read_ipc_stream, m)?)?;
    m.add_class::<PyDbClient>()?;
    m.add_class::<PyResultSet>()?;
    m.add_class::<PyHeartbeat>()?;
    m.add_class::<PyExecuteStreamedIter>()?;
    m.add_class::<PyFetchallArrowStreamedIter>()?;
    m.add_class::<PyNdjsonStreamIter>()?;
    m.add("HEARTBEAT", heartbeat_singleton(m.py())?)?;
    Ok(())
}
