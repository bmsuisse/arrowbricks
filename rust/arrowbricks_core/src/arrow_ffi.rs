//! Arrow PyCapsule interface, both directions, on arrow-rs's own
//! `ffi_stream` -- replaces `pyo3-arrow`, whose `Table.__arrow_c_stream__`
//! honors `requested_schema` via `arrow_cast::cast` and so kept the whole cast
//! kernel (~1.8 MiB, a third of `.text`) linked in for a feature no caller of
//! this crate used.

use std::ffi::CStr;

use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods};

const STREAM_CAPSULE: &CStr = c"arrow_array_stream";
const SCHEMA_CAPSULE: &CStr = c"arrow_schema";

/// An in-memory Arrow table. Consume it through `__arrow_c_stream__`
/// (arro3, pyarrow, polars, DuckDB, ...); the attributes are just enough to
/// inspect a result without importing another Arrow library.
#[pyclass(name = "Table", module = "arrowbricks._core", frozen)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    pub fn try_new(batches: Vec<RecordBatch>, schema: SchemaRef) -> PyResult<Self> {
        // A stream advertises one schema for every batch. Exporting different
        // buffer layouts under it can make consumers misinterpret memory.
        // Like the previous bridge, ignore metadata and nullability differences.
        if batches.iter().any(|batch| {
            let fields = batch.schema_ref().fields();
            fields.len() != schema.fields().len()
                || fields.iter().zip(schema.fields()).any(|(actual, expected)| {
                    actual.name() != expected.name() || !actual.data_type().equals_datatype(expected.data_type())
                })
        }) {
            return Err(PyRuntimeError::new_err("All batches must have same schema"));
        }
        Ok(Self { batches, schema })
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn __len__(&self) -> usize {
        self.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "arrowbricks.Table(num_rows={}, num_columns={})",
            self.num_rows(),
            self.num_columns()
        )
    }

    /// `requested_schema` is ignored, as the PyCapsule interface allows: the
    /// consumer checks the schema it actually gets.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = RecordBatchIterator::new(self.batches.clone().into_iter().map(Ok), self.schema.clone());
        PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(Box::new(reader)), STREAM_CAPSULE)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema =
            FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        PyCapsule::new_with_value(py, schema, SCHEMA_CAPSULE)
    }
}

/// Imports any object implementing `__arrow_c_stream__`.
pub fn import_stream(obj: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    let capsule = obj.call_method0("__arrow_c_stream__")?.cast_into::<PyCapsule>()?;
    let ptr = capsule
        .pointer_checked(Some(STREAM_CAPSULE))?
        .cast::<FFI_ArrowArrayStream>();
    // SAFETY: the capsule name check guarantees an `ArrowArrayStream`;
    // `from_raw` moves it out and marks the capsule's copy released, so the
    // capsule destructor does not release it a second time.
    unsafe { ArrowArrayStreamReader::from_raw(ptr.as_ptr()) }.map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
