//! Reorder buffer (port of `_ResultSet._pull_one_chunk_table`) + Arrow-IPC
//! decode of the reordered chunk stream via arrow-rs, split into submodules
//! along this crate's own natural seams:
//!
//! - `reorder` -- `ReorderBuffer` + `decode_chunk`/`decode_chunk_item`, the
//!   primitives every backend and consumer in this module shares.
//! - `stats` -- observability/stats-accumulation plumbing (`cancel_hook`,
//!   `StatsReporter`, and the `PoisonOnDrop`/`ReportOnDrop` drop-guards that
//!   make sure a `QueryStats` event still fires on an abandoned fetch).
//! - `sea` -- the REST Statement-Execution-API backed `ResultStream`/
//!   `execute_lazy`/`run_pipeline`/`execute_lazy_prefer_inline`.
//! - `thrift_exec` -- the Thrift/TCLIService backed `execute_lazy_thrift` and
//!   everything it depends on (session handling, the sequential
//!   `FetchResults` discovery loop, concurrent link-download workers).
//! - `ndjson` -- the chunk-granularity `NdjsonStream`/`execute_ndjson_stream`
//!   path backing `stream_query_json`.
//!
//! See `rust/arrowbricks_core/README.md` for the crate-level design (reorder
//! buffer, heartbeat primitives, etc).

mod ndjson;
mod reorder;
mod sea;
mod stats;
#[cfg(test)]
mod test_support;
mod thrift_exec;

pub(crate) use reorder::decode_ipc_stream;

pub use ndjson::{NdjsonStream, execute_ndjson_stream};
pub use sea::{ExecuteResult, ResultStream, execute_lazy, execute_lazy_prefer_inline, run_pipeline};
pub use stats::cancel_hook;
pub use thrift_exec::execute_lazy_thrift;
