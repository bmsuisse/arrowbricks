pub mod client;
pub mod pipeline;

use std::sync::Arc;

use arrow::datatypes::Schema;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

use client::DbClient;

#[pyfunction]
fn ping() -> &'static str {
    "pong"
}

/// One Databricks SQL warehouse endpoint -- a persistent `reqwest::Client`
/// (connection pool) reused across every `execute_arrow` call, same reason
/// Python's own `DatabricksClient` reuses one `httpx.AsyncClient` rather than
/// building a fresh one per statement: repeated TCP+TLS handshakes are pure
/// waste against the same host.
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
    /// ARROW_STREAM statement. Returns a `pyo3_arrow` `Table` -- it
    /// implements the Arrow C Data Interface (`__arrow_c_stream__`), so
    /// DuckDB/pyarrow/arro3 can all import it directly, zero-copy.
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
}

#[pymodule]
fn arrowbricks_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    m.add_class::<PyDbClient>()?;
    Ok(())
}
