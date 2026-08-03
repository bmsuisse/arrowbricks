pub mod client;
pub mod pipeline;

use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::Mutex as AsyncMutex;

use client::{ApiError, DbClient, TokenFuture, TokenProvider};
use pipeline::ResultStream;

#[pyfunction]
fn ping() -> &'static str {
    "pong"
}

fn py_err_to_api_error(e: PyErr) -> ApiError {
    ApiError { message: e.to_string(), transient: false }
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
            let is_awaitable = Python::attach(|py| called.bind(py).hasattr("__await__")).map_err(py_err_to_api_error)?;

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
    #[pyo3(signature = (host, warehouse_id, token=None, token_provider=None, chunk_fetch_concurrency=32))]
    fn new(
        host: String,
        warehouse_id: String,
        token: Option<String>,
        token_provider: Option<Py<PyAny>>,
        chunk_fetch_concurrency: usize,
    ) -> PyResult<Self> {
        let db_client = match (token, token_provider) {
            (Some(t), _) => DbClient::new(&host, &warehouse_id, &t),
            (None, Some(callable)) => {
                let provider: Arc<dyn TokenProvider> = Arc::new(PyTokenProvider { callable, locals: Mutex::new(None) });
                DbClient::with_token_provider(&host, &warehouse_id, provider)
            }
            (None, None) => return Err(PyValueError::new_err("Client needs either `token` or `token_provider`")),
        };
        Ok(Self { inner: Arc::new(db_client.with_concurrency(chunk_fetch_concurrency)) })
    }

    /// Full submit->poll->fetch->reorder->decode pipeline for one
    /// ARROW_STREAM statement, eagerly assembling the whole result. Returns
    /// a `pyo3_arrow` `Table` -- it implements the Arrow C Data Interface
    /// (`__arrow_c_stream__`), so DuckDB/pyarrow/arro3 can all import it
    /// directly, zero-copy. For a large result where you don't want the
    /// whole thing pulled upfront, use `execute()` + `ResultSet.fetchmany_arrow`.
    #[pyo3(signature = (statement, catalog=None, schema=None))]
    fn execute_arrow<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = pipeline::run_pipeline(client, &statement, catalog.as_deref(), schema.as_deref())
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
    #[pyo3(signature = (statement, catalog=None, schema=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = pipeline::execute_lazy(client, &statement, catalog.as_deref(), schema.as_deref())
                .await
                .map_err(|e| PyRuntimeError::new_err(e.message))?;
            Ok(PyResultSet {
                statement_id: stream.statement_id.clone(),
                num_chunks: stream.num_chunks,
                inner: Arc::new(AsyncMutex::new(stream)),
            })
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
    inner: Arc<AsyncMutex<ResultStream>>,
}

#[pymethods]
impl PyResultSet {
    /// Returns a `Table` with up to `n` rows -- fewer if the result is
    /// exhausted first (matching `_ResultSet.fetchmany_arrow`'s contract).
    fn fetchmany_arrow<'py>(&self, py: Python<'py>, n: usize) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let (batches, schema) =
                stream.fetchmany_arrow(n).await.map_err(|e| PyRuntimeError::new_err(e.message))?;
            let arrow_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            PyTable::try_new(batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Drains everything remaining into one `Table`.
    fn fetchall_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = inner.lock().await;
            let (batches, schema) = stream.fetchall_arrow().await.map_err(|e| PyRuntimeError::new_err(e.message))?;
            let arrow_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            PyTable::try_new(batches, arrow_schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pymodule]
fn arrowbricks_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    m.add_class::<PyDbClient>()?;
    m.add_class::<PyResultSet>()?;
    Ok(())
}
