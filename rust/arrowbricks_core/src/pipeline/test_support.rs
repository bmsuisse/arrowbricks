//! Test-only fixtures shared across this module tree's own `#[cfg(test)]`
//! blocks -- unlike `tests/common/mod.rs` (which exists to share code
//! between separate *integration*-test binaries under `tests/`, each
//! compiled as its own crate), unit tests living inside `src/` are already
//! part of the same crate and can just share a plain module gated behind
//! `#[cfg(test)]`, no `tests/`-style workaround needed. Added because
//! `make_batch` was found duplicated verbatim in both `pipeline/reorder.rs`
//! and `pipeline/ndjson.rs`'s own test modules.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{Float64Array, Int64Array};
use arrow_schema::{DataType, Field, Schema};

/// A two-column (`id: Int64`, `value: Float64`) batch -- used by
/// `reorder`'s `decode_chunk_item` truncation tests and `ndjson`'s
/// non-finite-float encoding test.
pub(crate) fn make_batch(id: Vec<i64>, values: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(id)), Arc::new(Float64Array::from(values))],
    )
    .unwrap()
}
