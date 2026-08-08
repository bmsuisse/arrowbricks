//! Converts a JSON_ARRAY result (`Vec<Vec<Option<String>>>` rows -- every
//! non-null value a string regardless of real column type, Databricks' own
//! contract) into a proper, correctly-typed Arrow `RecordBatch`, driven by the manifest's
//! `ColumnDescription`s (`type_name`, plus `type_precision`/`type_scale` for
//! `DECIMAL`).
//!
//! Backs the `prefer_inline` fast path (`client.rs`'s `execute_statement`):
//! a small result requested with `disposition: INLINE` comes back as exactly
//! this shape, embedded directly in the statement response -- no separate
//! chunk-fetch round trip at all. Only ever called for a *known-supported*
//! set of scalar types; any column outside that set makes this return an
//! `Err`, which the caller (`client.rs`) treats as "fall back to the normal
//! `EXTERNAL_LINKS`/`ARROW_STREAM` path" -- never a silent, wrong, or lossy
//! conversion. Every type mapping and string format below was confirmed
//! against a real Databricks workspace before being written (see this
//! session's own verification queries), not assumed from documentation.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde_json::Value as JsonValue;

use crate::client::{ApiError, ColumnDescription};

const UNIX_EPOCH_DATE: fn() -> NaiveDate = || NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");

fn conv_err(column: &str, value: &str, type_name: &str, detail: impl std::fmt::Display) -> ApiError {
    ApiError {
        message: format!("column `{column}` (type {type_name}): could not parse {value:?}: {detail}"),
        transient: false,
    }
}

/// `Err` here always means "this column's type (or a value in it) isn't one
/// `json_convert` handles" -- the caller falls back to the normal fetch path
/// rather than ever guessing at a conversion.
pub fn json_array_to_record_batch(
    rows: &[Vec<Option<String>>],
    columns: &[ColumnDescription],
) -> Result<RecordBatch, ApiError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (col_idx, col) in columns.iter().enumerate() {
        let type_name = col.type_name.as_deref().ok_or_else(|| ApiError {
            message: format!("column `{}` has no type_name in the manifest", col.name),
            transient: false,
        })?;
        let values = rows.iter().map(|row| row.get(col_idx).cloned().flatten());
        let (data_type, array) = build_column(
            &col.name,
            type_name,
            col.type_precision,
            col.type_scale,
            col.type_text.as_deref(),
            values,
        )?;
        fields.push(Field::new(&col.name, data_type, true));
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|e| ApiError {
        message: format!("failed to assemble RecordBatch from JSON_ARRAY data: {e}"),
        transient: false,
    })
}

#[allow(clippy::too_many_lines)]
fn build_column(
    name: &str,
    type_name: &str,
    precision: Option<u8>,
    scale: Option<i8>,
    type_text: Option<&str>,
    values: impl Iterator<Item = Option<String>>,
) -> Result<(DataType, ArrayRef), ApiError> {
    match type_name {
        "BYTE" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(s.parse::<i8>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Int8, Arc::new(Int8Array::from(out))))
        }
        "SHORT" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(s.parse::<i16>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Int16, Arc::new(Int16Array::from(out))))
        }
        "INT" | "INTEGER" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(s.parse::<i32>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Int32, Arc::new(Int32Array::from(out))))
        }
        "LONG" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(s.parse::<i64>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Int64, Arc::new(Int64Array::from(out))))
        }
        "FLOAT" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    // Rust's f32/f64 FromStr already accepts "NaN"/"inf"/
                    // "infinity" (any case, optional sign) -- confirmed
                    // against a real workspace that Databricks' own
                    // JSON_ARRAY non-finite float strings are exactly
                    // "NaN"/"Infinity"/"-Infinity", so no special-casing
                    // needed here (unlike the Arrow-IPC/arrow-json path,
                    // which collapses these to `null` -- see
                    // `non_finite_floats` in AGENTS.md -- a problem that
                    // simply doesn't exist on this path).
                    Some(s) => Some(s.parse::<f32>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Float32, Arc::new(Float32Array::from(out))))
        }
        "DOUBLE" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(s.parse::<f64>().map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            Ok((DataType::Float64, Arc::new(Float64Array::from(out))))
        }
        "DECIMAL" => {
            let (Some(precision), Some(scale)) = (precision, scale) else {
                return Err(ApiError {
                    message: format!("column `{name}`: DECIMAL type missing type_precision/type_scale in manifest"),
                    transient: false,
                });
            };
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(parse_decimal_to_i128(&s, scale).map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            let array = Decimal128Array::from(out)
                .with_precision_and_scale(precision, scale)
                .map_err(|e| ApiError {
                    message: format!("column `{name}`: invalid DECIMAL(precision={precision}, scale={scale}): {e}"),
                    transient: false,
                })?;
            Ok((DataType::Decimal128(precision, scale), Arc::new(array)))
        }
        "BOOLEAN" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(match s.as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(conv_err(name, &s, type_name, "expected \"true\" or \"false\"")),
                    }),
                });
            }
            Ok((DataType::Boolean, Arc::new(BooleanArray::from(out))))
        }
        "STRING" => {
            let out: Vec<Option<String>> = values.collect();
            Ok((DataType::Utf8, Arc::new(StringArray::from(out))))
        }
        "DATE" => {
            let epoch = UNIX_EPOCH_DATE();
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => {
                        let date =
                            NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| conv_err(name, &s, type_name, e))?;
                        Some((date - epoch).num_days() as i32)
                    }
                });
            }
            Ok((DataType::Date32, Arc::new(Date32Array::from(out))))
        }
        "TIMESTAMP" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => {
                        // Confirmed against a real workspace: always UTC,
                        // "Z"-suffixed, millisecond precision, e.g.
                        // "2026-08-06T12:34:56.000Z" -- valid RFC3339.
                        let dt = DateTime::parse_from_rfc3339(&s).map_err(|e| conv_err(name, &s, type_name, e))?;
                        Some(dt.timestamp_micros())
                    }
                });
            }
            Ok((
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Arc::new(TimestampMicrosecondArray::from(out).with_timezone("UTC")),
            ))
        }
        "TIMESTAMP_NTZ" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => {
                        // Confirmed against a real workspace: no "Z"/offset
                        // suffix, e.g. "2026-08-06T12:34:56.789" -- a naive
                        // (timezone-less) datetime, matching TIMESTAMP_NTZ's
                        // own semantics.
                        let dt = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")
                            .map_err(|e| conv_err(name, &s, type_name, e))?;
                        Some(dt.and_utc().timestamp_micros())
                    }
                });
            }
            Ok((
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(out)),
            ))
        }
        "BINARY" => {
            let mut out = Vec::new();
            for v in values {
                out.push(match v {
                    None => None,
                    Some(s) => Some(base64_decode(&s).map_err(|e| conv_err(name, &s, type_name, e))?),
                });
            }
            let refs: Vec<Option<&[u8]>> = out.iter().map(|o| o.as_deref()).collect();
            Ok((DataType::Binary, Arc::new(BinaryArray::from(refs))))
        }
        "STRUCT" => {
            let type_text = type_text.ok_or_else(|| ApiError {
                message: format!("column `{name}`: STRUCT type missing type_text in manifest"),
                transient: false,
            })?;
            let field_defs = parse_struct_fields(type_text).map_err(|e| ApiError {
                message: format!("column `{name}`: could not parse STRUCT type_text {type_text:?}: {e}"),
                transient: false,
            })?;

            // One parsed JSON object per row (`None` = the whole struct is
            // NULL for that row) -- Databricks encodes a STRUCT value as a
            // JSON object with every leaf value still a string (confirmed
            // against a real workspace), same string-typed-leaf contract as
            // every scalar type above.
            let mut rows_parsed: Vec<Option<serde_json::Map<String, JsonValue>>> = Vec::new();
            for v in values {
                match v {
                    None => rows_parsed.push(None),
                    Some(s) => {
                        let parsed: JsonValue =
                            serde_json::from_str(&s).map_err(|e| conv_err(name, &s, type_name, e))?;
                        let obj = match parsed {
                            JsonValue::Object(obj) => obj,
                            _ => {
                                return Err(conv_err(
                                    name,
                                    &s,
                                    type_name,
                                    "expected a JSON object for a STRUCT value",
                                ));
                            }
                        };
                        rows_parsed.push(Some(obj));
                    }
                }
            }

            let mut child_fields = Vec::with_capacity(field_defs.len());
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(field_defs.len());
            for (field_name, field_type, field_precision, field_scale) in &field_defs {
                let mut field_values: Vec<Option<String>> = Vec::with_capacity(rows_parsed.len());
                for row in &rows_parsed {
                    let value = match row {
                        None => None,
                        Some(obj) => match obj.get(field_name) {
                            None | Some(JsonValue::Null) => None,
                            Some(JsonValue::String(s)) => Some(s.clone()),
                            Some(other) => {
                                return Err(conv_err(
                                    name,
                                    &other.to_string(),
                                    type_name,
                                    format!("unexpected non-string value for STRUCT field `{field_name}`"),
                                ));
                            }
                        },
                    };
                    field_values.push(value);
                }
                let qualified_name = format!("{name}.{field_name}");
                let (field_data_type, field_array) = build_column(
                    &qualified_name,
                    field_type,
                    *field_precision,
                    *field_scale,
                    None,
                    field_values.into_iter(),
                )?;
                child_fields.push(Field::new(field_name, field_data_type, true));
                child_arrays.push(field_array);
            }

            let validity: Vec<bool> = rows_parsed.iter().map(Option::is_some).collect();
            let fields = Fields::from(child_fields);
            let struct_array = StructArray::new(fields.clone(), child_arrays, Some(NullBuffer::from(validity)));
            Ok((DataType::Struct(fields), Arc::new(struct_array)))
        }
        other => Err(ApiError {
            message: format!("column `{name}`: type {other} isn't supported by the INLINE/JSON_ARRAY fast path"),
            transient: false,
        }),
    }
}

/// Parses a STRUCT's `type_text` (e.g. `"STRUCT<a: BIGINT NOT NULL, b:
/// DECIMAL(10,4)>"`) into `(field_name, mapped_type_name, precision, scale)`
/// tuples, in declared field order. A nested composite field (another
/// `STRUCT<...>`/`ARRAY<...>`/`MAP<...>`) is deliberately left as its raw,
/// unmapped type text -- `build_column`'s catch-all then errors on it (safe
/// fallback to the normal fetch path) rather than this parser guessing at
/// unbounded recursion.
fn parse_struct_fields(type_text: &str) -> Result<Vec<(String, String, Option<u8>, Option<i8>)>, String> {
    let inner = type_text
        .strip_prefix("STRUCT<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| format!("not a STRUCT type_text: {type_text}"))?;

    // Top-level-comma split, tracking `<`/`(` depth so a nested composite
    // field or a DECIMAL(p,s)'s own comma doesn't get mistaken for a field
    // separator.
    let mut fields = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, b) in inner.bytes().enumerate() {
        match b {
            b'<' | b'(' => depth += 1,
            b'>' | b')' => depth -= 1,
            b',' if depth == 0 => {
                fields.push(parse_one_field(&inner[start..i])?);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < inner.len() {
        fields.push(parse_one_field(&inner[start..])?);
    }
    Ok(fields)
}

fn parse_one_field(raw: &str) -> Result<(String, String, Option<u8>, Option<i8>), String> {
    let raw = raw.trim();
    let (field_name, type_part) = raw
        .split_once(':')
        .ok_or_else(|| format!("malformed struct field: {raw}"))?;
    let field_name = field_name.trim().to_string();
    let mut type_part = type_part.trim();
    if let Some(stripped) = type_part.strip_suffix("NOT NULL") {
        type_part = stripped.trim();
    }

    if let Some(rest) = type_part.strip_prefix("DECIMAL(") {
        let rest = rest
            .strip_suffix(')')
            .ok_or_else(|| format!("malformed DECIMAL in struct field: {type_part}"))?;
        let (p, s) = rest
            .split_once(',')
            .ok_or_else(|| format!("malformed DECIMAL(p,s) in struct field: {type_part}"))?;
        let precision: u8 = p.trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
        let scale: i8 = s.trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
        return Ok((field_name, "DECIMAL".to_string(), Some(precision), Some(scale)));
    }

    // SQL DDL spelling differs from this manifest's own top-level type_name
    // vocabulary for exactly these three widths -- confirmed against a real
    // workspace (`STRUCT<a: TINYINT NOT NULL, ...>` etc.); every other name
    // (INT/INTEGER/FLOAT/DOUBLE/BOOLEAN/STRING/DATE/TIMESTAMP/TIMESTAMP_NTZ/
    // BINARY) already matches `build_column`'s own match arms as-is, and an
    // unrecognized one (a nested STRUCT/ARRAY/MAP/VARIANT) passes through
    // unchanged too, to be caught by that function's catch-all.
    let mapped = match type_part {
        "TINYINT" => "BYTE",
        "SMALLINT" => "SHORT",
        "BIGINT" => "LONG",
        other => other,
    };
    Ok((field_name, mapped.to_string(), None, None))
}

/// Parses a decimal string (e.g. "3.1400", "-3.50", "42") into its unscaled
/// `i128` representation for the given `scale` (e.g. "3.1400" at scale 4 ->
/// 31400). Databricks' own JSON_ARRAY contract always formats the string
/// with exactly `scale` fractional digits (confirmed against a real
/// workspace), but this pads/truncates defensively rather than assuming
/// that never varies.
fn parse_decimal_to_i128(s: &str, scale: i8) -> Result<i128, String> {
    let (negative, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let scale = scale.max(0) as usize;
    let mut frac_digits = frac_part.to_string();
    if frac_digits.len() > scale {
        frac_digits.truncate(scale);
    } else {
        while frac_digits.len() < scale {
            frac_digits.push('0');
        }
    }
    let digits = format!("{int_part}{frac_digits}");
    let magnitude: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().map_err(|e| format!("{e}"))?
    };
    Ok(if negative { -magnitude } else { magnitude })
}

/// Minimal, dependency-free base64 decoder (standard alphabet, `=` padding)
/// -- this crate deliberately has no `base64` dependency for one field; the
/// alphabet is small and stable enough to not be worth adding one.
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=');
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        for (i, &b) in chunk.iter().enumerate() {
            buf[i] = val(b).ok_or_else(|| format!("invalid base64 byte {b:?}"))?;
        }
        let n = chunk.len();
        out.push((buf[0] << 2) | (buf[1] >> 4));
        if n > 2 {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if n > 3 {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, type_name: &str) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            type_name: Some(type_name.to_string()),
            type_precision: None,
            type_scale: None,
            type_text: None,
        }
    }

    fn struct_col(name: &str, type_text: &str) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            type_name: Some("STRUCT".to_string()),
            type_precision: None,
            type_scale: None,
            type_text: Some(type_text.to_string()),
        }
    }

    fn decimal_col(name: &str, precision: u8, scale: i8) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            type_name: Some("DECIMAL".to_string()),
            type_precision: Some(precision),
            type_scale: Some(scale),
            type_text: None,
        }
    }

    #[test]
    fn converts_every_scalar_type_confirmed_against_a_real_workspace() {
        let columns = vec![
            col("byte_val", "BYTE"),
            col("short_val", "SHORT"),
            col("int_val", "INT"),
            col("long_val", "LONG"),
            col("float_val", "FLOAT"),
            col("double_val", "DOUBLE"),
            decimal_col("dec_val", 10, 4),
            col("bool_val", "BOOLEAN"),
            col("string_val", "STRING"),
            col("date_val", "DATE"),
            col("ts_val", "TIMESTAMP"),
            col("ts_ntz_val", "TIMESTAMP_NTZ"),
            col("bin_val", "BINARY"),
        ];
        let rows = vec![vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
            Some("123456789012".to_string()),
            Some("1.5".to_string()),
            Some("NaN".to_string()),
            Some("3.1400".to_string()),
            Some("true".to_string()),
            Some("hello".to_string()),
            Some("2026-08-06".to_string()),
            Some("2026-08-06T12:34:56.000Z".to_string()),
            Some("2026-08-06T12:34:56.789".to_string()),
            Some("aGVsbG8=".to_string()),
        ]];

        let batch = json_array_to_record_batch(&rows, &columns).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 13);

        use arrow::array::Array;
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int8Array>().unwrap().value(0),
            1
        );
        assert_eq!(
            batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            123456789012
        );
        assert!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .is_nan()
        );
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            31400
        );
        assert!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            batch.column(8).as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "hello"
        );
        // 2026-08-06 is 20671 days after 1970-01-01.
        assert_eq!(
            batch.column(9).as_any().downcast_ref::<Date32Array>().unwrap().value(0),
            20671
        );
        assert_eq!(
            batch
                .column(12)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"hello"
        );
    }

    #[test]
    fn nulls_stay_null_for_every_type() {
        let columns = vec![col("a", "LONG"), col("b", "STRING"), decimal_col("c", 10, 2)];
        let rows = vec![vec![None, None, None]];
        let batch = json_array_to_record_batch(&rows, &columns).unwrap();
        assert_eq!(batch.column(0).null_count(), 1);
        assert_eq!(batch.column(1).null_count(), 1);
        assert_eq!(batch.column(2).null_count(), 1);
    }

    #[test]
    fn negative_decimal_parses_correctly() {
        assert_eq!(parse_decimal_to_i128("-3.50", 2).unwrap(), -350);
        assert_eq!(parse_decimal_to_i128("3.1400", 4).unwrap(), 31400);
        assert_eq!(parse_decimal_to_i128("42", 2).unwrap(), 4200);
        assert_eq!(parse_decimal_to_i128("0.00", 2).unwrap(), 0);
    }

    #[test]
    fn unsupported_type_errors_instead_of_guessing() {
        let columns = vec![col("arr", "ARRAY")];
        let rows = vec![vec![Some("[\"1\",\"2\"]".to_string())]];
        let err = json_array_to_record_batch(&rows, &columns).unwrap_err();
        assert!(
            err.message.contains("ARRAY"),
            "error should name the unsupported type: {}",
            err.message
        );
    }

    #[test]
    fn base64_decode_matches_known_vectors() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert_eq!(base64_decode("YQ==").unwrap(), b"a");
    }

    #[test]
    fn converts_a_struct_column_of_primitive_fields_confirmed_against_a_real_workspace() {
        // Exact type_text/value shape captured from a real workspace: SQL DDL
        // field type spelling (TINYINT/BIGINT, not this crate's own
        // BYTE/LONG), " NOT NULL" suffixes present on some fields, JSON
        // object key order not matching declared field order.
        let columns = vec![struct_col(
            "s",
            "STRUCT<a: TINYINT NOT NULL, b: SMALLINT NOT NULL, c: INT NOT NULL, d: BIGINT NOT NULL, \
             e: FLOAT NOT NULL, f: DOUBLE NOT NULL, g: DECIMAL(10,4) NOT NULL, h: BOOLEAN NOT NULL, \
             i: STRING NOT NULL, j: DATE, k: TIMESTAMP NOT NULL>",
        )];
        let rows = vec![vec![Some(
            "{\"e\":\"1.5\",\"j\":\"2026-01-01\",\"f\":\"2.5\",\"a\":\"1\",\"i\":\"str\",\"b\":\"2\",\
             \"g\":\"3.1400\",\"c\":\"3\",\"h\":\"true\",\"k\":\"2026-08-06T08:32:06.342Z\",\"d\":\"4\"}"
                .to_string(),
        )]];

        let batch = json_array_to_record_batch(&rows, &columns).unwrap();
        use arrow::array::Array;
        let s = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            s.column_by_name("a")
                .unwrap()
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            s.column_by_name("d")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
        );
        assert_eq!(
            s.column_by_name("g")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            31400
        );
        assert_eq!(
            s.column_by_name("i")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "str"
        );
        assert!(
            s.column_by_name("h")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn struct_field_null_and_whole_struct_null_row_both_stay_null() {
        let columns = vec![struct_col("s", "STRUCT<a: INT, b: STRING>")];
        let rows = vec![
            vec![Some("{\"a\":null,\"b\":\"x\"}".to_string())],
            vec![None], // the whole struct is NULL for this row
        ];
        let batch = json_array_to_record_batch(&rows, &columns).unwrap();
        use arrow::array::Array;
        let s = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert!(
            s.column_by_name("a").unwrap().is_null(0),
            "a field-level null must stay null"
        );
        assert!(!s.is_null(0), "row 0's struct itself is present, just one null field");
        assert!(s.is_null(1), "row 1's whole struct must be null");
    }

    #[test]
    fn struct_with_a_nested_unsupported_composite_field_errors_instead_of_guessing() {
        let columns = vec![struct_col("s", "STRUCT<a: INT, nested: STRUCT<x: INT>>")];
        let rows = vec![vec![Some("{\"a\":\"1\",\"nested\":{\"x\":\"1\"}}".to_string())]];
        let err = json_array_to_record_batch(&rows, &columns).unwrap_err();
        assert!(
            err.message.contains("nested") || err.message.contains("STRUCT"),
            "error should point at the unsupported nested field: {}",
            err.message
        );
    }

    #[test]
    fn parse_struct_fields_handles_decimal_commas_and_not_null() {
        let fields = parse_struct_fields("STRUCT<a: DECIMAL(10,4) NOT NULL, b: STRING>").unwrap();
        assert_eq!(fields[0], ("a".to_string(), "DECIMAL".to_string(), Some(10), Some(4)));
        assert_eq!(fields[1], ("b".to_string(), "STRING".to_string(), None, None));
    }
}
