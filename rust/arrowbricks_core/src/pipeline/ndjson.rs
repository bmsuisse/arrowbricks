//! The NDJSON streaming path backing `stream_query_json`: chunk-granularity
//! (not row-count-granularity) `NdjsonStream`, its `execute_ndjson_stream`
//! entry point (dispatching to either backend's own submit/poll/fetch-start,
//! matching `PyDbClient::execute`'s own `protocol` dispatch), and Arrow-batch-
//! to-NDJSON-lines encoding -- including the non-finite-float (`NaN`/
//! `Infinity`/`-Infinity`) string-patching arrow-json's own fixed encoding
//! needs help with.

use std::io::{self, Write};
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_schema::DataType;
use serde_json::Value;

use crate::client::{ApiError, ApiErrorKind, CancelHandle, DbClient, Protocol, QueryStatsAccumulator, join_error};

use super::reorder::{ReorderBuffer, decode_chunk_item};
use super::sea::submit_sea_and_report;
use super::stats::{ReportOnDrop, StatsReporter};
use super::thrift_exec::{ThriftSubmitResult, submit_thrift_and_start_fetch};

/// Collect rows as Arrow writes them, avoiding a second, chunk-sized buffer.
/// Writes may end within a row (or even a UTF-8 character).
struct LineCollector {
    lines: Vec<String>,
    pending: Vec<u8>,
}

impl Write for LineCollector {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut start = 0;
        for end in memchr::memchr_iter(b'\n', bytes) {
            let line = &bytes[start..end];
            let row = if self.pending.is_empty() {
                line.to_vec()
            } else {
                self.pending.extend_from_slice(line);
                std::mem::take(&mut self.pending)
            };
            self.lines
                .push(String::from_utf8(row).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?);
            start = end + 1;
        }
        self.pending.extend_from_slice(&bytes[start..]);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Converts one chunk's decoded batches into NDJSON lines, one per row, in
/// arro3-`write_ndjson(explicit_nulls=True)`-compatible format: null-valued
/// keys stay present as JSON `null` rather than being omitted, and a
/// UTC-aware timestamp column renders as full ISO-8601 with a trailing `Z`
/// (arrow-json's own default -- no custom format string needed, verified
/// against arrow-json's own `write_timestamps_with_tz` test producing
/// `"2018-11-13T17:11:10Z"`-shaped output, same as arro3).
///
/// `non_finite_as_string`: `arrow-json` (this crate's own JSON writer, not
/// something arrowbricks wrote) hardcodes NaN/+-Infinity to JSON `null` --
/// valid JSON, but indistinguishable from a real NULL once it's out (found
/// by testing a wide real query against a live warehouse: a NaN and a NULL
/// column came back identically as `null`). When this is `true`, those
/// specific cells are patched to the JSON strings `"NaN"`/`"Infinity"`/
/// `"-Infinity"` after encoding -- see `patch_non_finite_floats`. Only
/// top-level Float32/Float64 columns are covered; a non-finite float nested
/// inside a STRUCT/ARRAY/MAP still comes back as `null` either way.
fn encode_ndjson_lines(batches: &[RecordBatch], non_finite_as_string: bool) -> Result<Vec<String>, ApiError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = LineCollector {
        lines: Vec::with_capacity(batches.iter().map(RecordBatch::num_rows).sum()),
        pending: Vec::new(),
    };
    {
        let builder = arrow_json::WriterBuilder::new().with_explicit_nulls(true);
        let mut writer = builder.build::<_, arrow_json::writer::LineDelimited>(&mut output);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        writer.write_batches(&refs).map_err(|e| ApiError {
            message: format!("NDJSON encode error: {e}"),
            transient: false,
            kind: ApiErrorKind::Other,
        })?;
        writer.finish().map_err(|e| ApiError {
            message: format!("NDJSON encode error: {e}"),
            transient: false,
            kind: ApiErrorKind::Other,
        })?;
    }
    if !output.pending.is_empty() {
        return Err(ApiError::permanent("NDJSON encode produced an unterminated row"));
    }
    let mut lines = output.lines;

    if non_finite_as_string {
        patch_non_finite_floats(batches, &mut lines);
    }
    Ok(lines)
}

/// `Some(token)` (already-quoted JSON, e.g. `"\"NaN\""`) if `v` is NaN or
/// +-infinite, `None` for any finite value (including `-0.0`).
fn non_finite_token(v: f64) -> Option<&'static str> {
    if v.is_nan() {
        Some("\"NaN\"")
    } else if v == f64::INFINITY {
        Some("\"Infinity\"")
    } else if v == f64::NEG_INFINITY {
        Some("\"-Infinity\"")
    } else {
        None
    }
}

/// One top-level Float32/Float64 column.
enum FloatColumn<'a> {
    F64(usize, &'a arrow_array::Float64Array),
    F32(usize, &'a arrow_array::Float32Array),
}

/// Rewrites each affected line's `null` (arrow-json's fixed encoding for a
/// non-finite float, see `encode_ndjson_lines`) to a `"NaN"`/`"Infinity"`/
/// `"-Infinity"` JSON string, in place. Only scans top-level Float32/Float64
/// columns -- one row of `lines` per row of the batches, in the same order.
/// Precomputes each batch's float columns once (schema scan + downcast) up
/// front instead of redoing both per row -- for a wide, non-float-heavy
/// schema (this feature's own motivating case: a 120-column real table) that
/// was a dynamic downcast attempt on every column for every row, the large
/// majority immediately discarded, and a schema with no float columns at all
/// still paid for the full row x column scan for nothing.
fn patch_non_finite_floats(batches: &[RecordBatch], lines: &mut [String]) {
    let mut global_row = 0usize;
    for batch in batches {
        let schema = batch.schema();
        let float_columns: Vec<FloatColumn> = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(field_index, field)| match field.data_type() {
                DataType::Float64 => Some(FloatColumn::F64(
                    field_index,
                    batch.column(field_index).as_primitive::<Float64Type>(),
                )),
                DataType::Float32 => Some(FloatColumn::F32(
                    field_index,
                    batch.column(field_index).as_primitive::<Float32Type>(),
                )),
                _ => None,
            })
            .collect();

        if float_columns.is_empty() {
            global_row += batch.num_rows();
            continue;
        }

        for row_in_batch in 0..batch.num_rows() {
            for col in &float_columns {
                let (field_index, token) = match col {
                    FloatColumn::F64(field_index, arr) => (
                        *field_index,
                        arr.is_valid(row_in_batch)
                            .then(|| non_finite_token(arr.value(row_in_batch)))
                            .flatten(),
                    ),
                    FloatColumn::F32(field_index, arr) => (
                        *field_index,
                        arr.is_valid(row_in_batch)
                            .then(|| non_finite_token(arr.value(row_in_batch) as f64))
                            .flatten(),
                    ),
                };
                if let Some(token) = token {
                    lines[global_row] = replace_nth_top_level_null(&lines[global_row], field_index, token);
                }
            }
            global_row += 1;
        }
    }
}

/// Replaces the value at the `field_index`-th top-level key (0-based, in
/// schema field order -- matching arrow-json's own emission order, which
/// writes keys in field order rather than sorting them) of one NDJSON line
/// with `replacement` (already valid JSON). Only ever called where the
/// existing value is known -- from the source Arrow array, not by inspecting
/// the JSON text -- to be exactly the 4-byte `null` arrow-json writes for a
/// non-finite float, so this never touches a real NULL and, by counting
/// keys positionally rather than by name, is correct even when two top-level
/// columns share the same name (a real, supported case in this crate).
fn replace_nth_top_level_null(line: &str, field_index: usize, replacement: &str) -> String {
    let bytes = line.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut current_field = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth -= 1,
            b':' if depth == 1 => {
                if current_field == field_index {
                    let value_start = i + 1;
                    debug_assert_eq!(
                        bytes.get(value_start..value_start + 4),
                        Some(&b"null"[..]),
                        "replace_nth_top_level_null called on a field whose value wasn't null"
                    );
                    let mut out = String::with_capacity(line.len() + replacement.len());
                    out.push_str(&line[..value_start]);
                    out.push_str(replacement);
                    out.push_str(&line[value_start + 4..]);
                    return out;
                }
                current_field += 1;
            }
            _ => {}
        }
    }
    line.to_string()
}

/// Chunk-granularity (not row-count-granularity) counterpart to
/// `ResultStream` (`pipeline/sea.rs`): pulls, decodes, and NDJSON-encodes
/// exactly one reordered chunk per `next_chunk()` call rather than buffering
/// ahead to satisfy a row count. Backs `stream_query_json` end to end --
/// unlike the Arrow-Table pipelines in `pipeline/sea.rs`/`pipeline/thrift_exec.rs`,
/// there's no further Python-side conversion step.
pub struct NdjsonStream {
    pub statement_id: String,
    pub num_chunks: usize,
    reorder: ReorderBuffer,
    non_finite_as_string: bool,
    /// See `ResultStream`'s identically-named fields -- same cancellation/
    /// observability contract, just backing `stream_ndjson_lines` instead.
    pub cancel_handle: CancelHandle,
    pub stats: Arc<QueryStatsAccumulator>,
    reporter: StatsReporter,
}

impl NdjsonStream {
    /// Pulls the next chunk in logical (chunk_index) order, decodes it, and
    /// NDJSON-encodes it (all on a blocking thread, since both decode and
    /// JSON encoding are CPU work) -- `None` once the source is exhausted.
    /// One network chunk in, one line per row out, matching
    /// `fetch_arrow_chunks_for_statement`'s old per-chunk yield.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<String>>, ApiError> {
        let mut guard = ReportOnDrop::new(&mut self.reporter, self.stats.as_ref());
        match self.reorder.next().await {
            Ok(Some(item)) => {
                let non_finite_as_string = self.non_finite_as_string;
                let truncate_to = item.truncate_to;
                let decoded = tokio::task::spawn_blocking(move || {
                    let batches = decode_chunk_item(&item.blob, truncate_to)?;
                    encode_ndjson_lines(&batches, non_finite_as_string)
                })
                .await
                .map_err(join_error);
                match decoded {
                    Ok(Ok(lines)) => {
                        guard.defuse();
                        guard.reporter.end_fetch();
                        Ok(Some(lines))
                    }
                    Ok(Err(e)) | Err(e) => {
                        guard.defuse();
                        guard.fail(e)
                    }
                }
            }
            Ok(None) => {
                guard.defuse();
                guard.reporter.finish("success", guard.stats);
                Ok(None)
            }
            Err(e) => {
                guard.defuse();
                guard.fail(e)
            }
        }
    }
}

/// Submit -> poll -> start background chunk fetching for the chunk-at-a-time
/// stream above. Matches Python's `fetch_arrow_chunks_with_manifest`: this
/// await itself is never heartbeat-wrapped (only the per-chunk pulls that
/// follow are) -- `stream_query_json` only wraps its chunk iterator, not
/// this initial submit/poll wait, so this crate preserves that same gap
/// rather than "fixing" it during the port.
///
/// **Branches on `client.protocol`, same as `PyDbClient::execute`'s own
/// dispatch (`lib.rs`) -- found missing entirely during a later review
/// pass, not caught by any test.** This function unconditionally submitted
/// via SEA (`execute_arrow_statement`) regardless of `protocol=`, so every
/// caller on `protocol="thrift"` (the default) got NDJSON streaming over
/// SEA anyway -- silently forgoing the entire reason Thrift is the default
/// (see AGENTS.md's "Thrift is now the default protocol" entry) for this
/// one public API. `NdjsonStream` itself was always fully protocol-agnostic
/// (just a `ReorderBuffer` over a `ChunkItem` receiver, same as
/// `ResultStream`) -- the gap was purely in this function never routing to
/// `submit_thrift_and_start_fetch`, Thrift's own equivalent of
/// `execute_arrow_statement` + `fetch_chunks_with_backpressure`.
pub async fn execute_ndjson_stream(
    client: Arc<DbClient>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    parameters: Option<Value>,
    non_finite_as_string: bool,
) -> Result<NdjsonStream, ApiError> {
    let (statement_id, num_chunks, rx, cancel_handle, stats, warehouse_wait_s, submit_to_ready_s, protocol) =
        if client.protocol == Protocol::Thrift {
            // NDJSON output has no `description`/column-schema concept.
            let ThriftSubmitResult {
                statement_id,
                schema_bytes: _schema_bytes,
                rx,
                operation,
                stats,
                warehouse_wait_s,
                submit_to_ready_s,
            } = submit_thrift_and_start_fetch(client.clone(), statement, catalog, schema, parameters).await?;
            (
                statement_id,
                None, // Thrift: unknown upfront, see `StatsReporter::static_num_chunks`
                rx,
                CancelHandle::Thrift { operation },
                stats,
                warehouse_wait_s,
                submit_to_ready_s,
                "thrift",
            )
        } else {
            let stats = Arc::new(QueryStatsAccumulator::default());
            let (submitted, submit_to_ready_s) =
                submit_sea_and_report(&client, statement, catalog, schema, parameters, &stats).await?;
            let warehouse_wait_s = stats.warehouse_wait_s();
            let num_chunks = submitted.chunk_metas.len();
            let rx = client.clone().fetch_chunks_with_backpressure(
                submitted.statement_id.clone(),
                submitted.chunk_metas,
                submitted.compressed,
                stats.clone(),
            );
            (
                submitted.statement_id.clone(),
                Some(num_chunks),
                rx,
                CancelHandle::Sea {
                    statement_id: submitted.statement_id,
                },
                stats,
                warehouse_wait_s,
                submit_to_ready_s,
                "sea",
            )
        };
    Ok(NdjsonStream {
        statement_id: statement_id.clone(),
        num_chunks: num_chunks.unwrap_or(0),
        reorder: ReorderBuffer::new(rx),
        non_finite_as_string,
        cancel_handle,
        stats,
        reporter: StatsReporter {
            client,
            statement_id,
            protocol,
            warehouse_wait_s,
            submit_to_ready_s,
            fetch_s: 0.0,
            fetch_started_at: None,
            static_num_chunks: num_chunks,
            reported: false,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::test_support::make_batch;

    fn encode_ndjson_reference(batches: &[RecordBatch], non_finite_as_string: bool) -> Result<Vec<String>, ApiError> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let mut buf = Vec::new();
        {
            let builder = arrow_json::WriterBuilder::new().with_explicit_nulls(true);
            let mut writer = builder.build::<_, arrow_json::writer::LineDelimited>(&mut buf);
            let refs: Vec<&RecordBatch> = batches.iter().collect();
            writer.write_batches(&refs).map_err(|e| ApiError {
                message: format!("NDJSON encode error: {e}"),
                transient: false,
                kind: ApiErrorKind::Other,
            })?;
            writer.finish().map_err(|e| ApiError {
                message: format!("NDJSON encode error: {e}"),
                transient: false,
                kind: ApiErrorKind::Other,
            })?;
        }
        let mut lines: Vec<String> = String::from_utf8(buf)
            .map_err(|e| ApiError {
                message: format!("NDJSON encode produced invalid UTF-8: {e}"),
                transient: false,
                kind: ApiErrorKind::Other,
            })?
            .lines()
            .map(|line| line.to_string())
            .collect();

        if non_finite_as_string {
            patch_non_finite_floats(batches, &mut lines);
        }
        Ok(lines)
    }

    #[test]
    fn line_collector_handles_split_unicode_and_rejects_invalid_utf8() {
        let mut output = LineCollector {
            lines: Vec::new(),
            pending: Vec::new(),
        };
        for byte in "{\"label\":\"café 🦀\"}\n{}\n".as_bytes() {
            output.write_all(&[*byte]).unwrap();
        }
        assert_eq!(output.lines, ["{\"label\":\"café 🦀\"}", "{}"]);
        assert!(output.pending.is_empty());
        assert_eq!(
            output.write_all(&[0xff, b'\n']).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    fn text_batch(rows: usize, columns: usize, width: usize) -> RecordBatch {
        use arrow_array::StringArray;
        use arrow_schema::{Field, Schema};
        let value = format!("café 🦀 \"quoted\"\n{}", "x".repeat(width));
        let array = Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 7 != 0).then_some(value.as_str())),
        ));
        let fields = (0..columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            (0..columns).map(|_| array.clone() as _).collect(),
        )
        .unwrap()
    }

    #[test]
    fn collected_ndjson_matches_reference_for_large_unicode_rows_and_multiple_batches() {
        let batches = vec![
            text_batch(20, 3, 12000),
            text_batch(0, 3, 12000),
            text_batch(17, 3, 12000),
        ];
        let expected = encode_ndjson_reference(&batches, false).unwrap();
        let actual = encode_ndjson_lines(&batches, false).unwrap();
        assert_eq!(actual.len(), 37);
        assert_eq!(actual, expected);
    }

    #[test]
    #[ignore = "manual release-mode allocation/encoding benchmark"]
    fn benchmark_ndjson_collector() {
        use std::hint::black_box;
        use std::time::Instant;
        for (name, rows, columns, width) in [("narrow", 100000, 4, 16), ("wide", 20000, 32, 96)] {
            let batches = vec![text_batch(rows, columns, width)];
            assert_eq!(
                encode_ndjson_lines(&batches, false).unwrap(),
                encode_ndjson_reference(&batches, false).unwrap()
            );
            for round in 0..9 {
                for candidate in if round % 2 == 0 { [false, true] } else { [true, false] } {
                    let start = Instant::now();
                    let lines = if candidate {
                        encode_ndjson_lines(black_box(&batches), false)
                    } else {
                        encode_ndjson_reference(black_box(&batches), false)
                    }
                    .unwrap();
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    black_box(&lines);
                    println!(
                        "{}",
                        serde_json::json!({"workload":name,"round":round,"candidate":candidate,"ms":elapsed,"rows":lines.len(),"bytes":lines.iter().map(String::len).sum::<usize>()})
                    );
                }
            }
        }
    }

    #[test]
    fn non_finite_token_covers_nan_and_both_infinities_only() {
        assert_eq!(non_finite_token(f64::NAN), Some("\"NaN\""));
        assert_eq!(non_finite_token(f64::INFINITY), Some("\"Infinity\""));
        assert_eq!(non_finite_token(f64::NEG_INFINITY), Some("\"-Infinity\""));
        assert_eq!(non_finite_token(0.0), None);
        assert_eq!(non_finite_token(-0.0), None);
        assert_eq!(non_finite_token(123.456), None);
        assert_eq!(non_finite_token(-123.456), None);
    }

    #[test]
    fn replace_nth_top_level_null_targets_by_position_not_name() {
        // Two fields named "dup" -- the second is the one that's actually
        // null (arrow-json's non-finite-float encoding); the first, same-
        // named field is a real value and must be left alone. Proves the
        // positional approach (not name matching) is what makes this safe.
        let line = r#"{"dup":1,"dup":null}"#;
        let patched = replace_nth_top_level_null(line, 1, "\"NaN\"");
        assert_eq!(patched, r#"{"dup":1,"dup":"NaN"}"#);
    }

    #[test]
    fn replace_nth_top_level_null_ignores_nested_nulls_at_deeper_depth() {
        // A nested object's own "null" must not be mistaken for the
        // top-level field being targeted -- depth tracking (not a naive
        // first-`null`-wins scan) is what keeps this correct.
        let line = r#"{"a":{"inner":null},"b":null}"#;
        let patched = replace_nth_top_level_null(line, 1, "\"Infinity\"");
        assert_eq!(patched, r#"{"a":{"inner":null},"b":"Infinity"}"#);
    }

    #[test]
    fn replace_nth_top_level_null_skips_colons_inside_string_values() {
        // A colon inside a quoted string value (e.g. a timestamp or URL)
        // must not be mistaken for a field separator.
        let line = r#"{"label":"12:34:56","dup":1,"dup":null}"#;
        let patched = replace_nth_top_level_null(line, 2, "\"NaN\"");
        assert_eq!(patched, r#"{"label":"12:34:56","dup":1,"dup":"NaN"}"#);
    }

    /// Regression test for a real behavior found by testing edge-case data
    /// types against a live Databricks warehouse: NaN and Infinity floats
    /// came back from `stream_query_json` as JSON `null`, indistinguishable
    /// from a genuine SQL NULL in the same column -- arrow-json's own fixed
    /// encoding for non-finite floats, not something arrowbricks chose.
    /// `non_finite_as_string=true` must recover the distinction.
    #[test]
    fn encode_ndjson_lines_preserves_non_finite_floats_as_strings_when_requested() {
        let batch = make_batch(vec![1, 2, 3, 4], vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5]);

        let default_lines = encode_ndjson_lines(std::slice::from_ref(&batch), false).unwrap();
        assert_eq!(default_lines[0], r#"{"id":1,"value":null}"#);
        assert_eq!(default_lines[1], r#"{"id":2,"value":null}"#);
        assert_eq!(default_lines[2], r#"{"id":3,"value":null}"#);
        assert_eq!(default_lines[3], r#"{"id":4,"value":1.5}"#);

        let string_lines = encode_ndjson_lines(std::slice::from_ref(&batch), true).unwrap();
        assert_eq!(string_lines[0], r#"{"id":1,"value":"NaN"}"#);
        assert_eq!(string_lines[1], r#"{"id":2,"value":"Infinity"}"#);
        assert_eq!(string_lines[2], r#"{"id":3,"value":"-Infinity"}"#);
        assert_eq!(string_lines[3], r#"{"id":4,"value":1.5}"#); // finite values untouched
    }

    #[test]
    fn encode_ndjson_lines_leaves_a_real_null_alone_when_requested() {
        use arrow_array::{Float64Array, Int64Array};
        use arrow_schema::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![None])), // a genuine SQL NULL, not NaN
            ],
        )
        .unwrap();

        let lines = encode_ndjson_lines(std::slice::from_ref(&batch), true).unwrap();
        assert_eq!(
            lines[0], r#"{"id":1,"value":null}"#,
            "a real NULL must stay null, never become a string"
        );
    }
}
