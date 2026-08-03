pub mod client;
pub mod pipeline;

use std::sync::Arc;

use arrow::datatypes::Schema;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use tokio::sync::Mutex as AsyncMutex;

use client::DbClient;
use pipeline::ResultStream;

#[pyfunction]
fn ping() -> &'static str {
    "pong"
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
    #[new]
    #[pyo3(signature = (host, warehouse_id, token, chunk_fetch_concurrency=32))]
    fn new(host: String, warehouse_id: String, token: String, chunk_fetch_concurrency: usize) -> Self {
        Self { inner: Arc::new(DbClient::new(&host, &warehouse_id, &token).with_concurrency(chunk_fetch_concurrency)) }
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
