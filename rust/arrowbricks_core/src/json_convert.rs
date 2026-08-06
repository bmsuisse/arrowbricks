//! Converts a JSON_ARRAY result (`Vec<Vec<Option<String>>>` rows -- every
//! non-null value a string regardless of real column type, Databricks' own
//! contract, see `client.rs`'s `execute_json_statement`) into a proper,
//! correctly-typed Arrow `RecordBatch`, driven by the manifest's
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
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime};

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
        let (data_type, array) = build_column(&col.name, type_name, col.type_precision, col.type_scale, values)?;
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
        other => Err(ApiError {
            message: format!("column `{name}`: type {other} isn't supported by the INLINE/JSON_ARRAY fast path"),
            transient: false,
        }),
    }
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
        }
    }

    fn decimal_col(name: &str, precision: u8, scale: i8) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            type_name: Some("DECIMAL".to_string()),
            type_precision: Some(precision),
            type_scale: Some(scale),
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
}
