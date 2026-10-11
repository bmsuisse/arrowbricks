use crate::client::{ApiError, ApiErrorKind};
use serde_json::{Value, json};

pub(super) fn error_json(error: &ApiError, max_bytes: usize, timeout: bool) -> String {
    let mut prefix_end = error.message.len().min(max_bytes);
    while !error.message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }
    let message = &error.message[..prefix_end];
    let shortened = prefix_end < error.message.len();
    let upper = message.to_ascii_uppercase();
    let (category, next_step) = if timeout {
        (
            "timeout",
            "Increase total_timeout_s or narrow the query before retrying.",
        )
    } else if error.kind == ApiErrorKind::Auth {
        ("authentication", "Refresh credentials and verify warehouse access.")
    } else if upper.contains("UNRESOLVED_COLUMN") || upper.contains("UNRESOLVED_FIELD") {
        (
            "missing_column",
            "Check column names against the table schema and database suggestions.",
        )
    } else if upper.contains("UNRESOLVED_ROUTINE") {
        (
            "missing_function",
            "Check the function name and namespace with SHOW FUNCTIONS or DESCRIBE FUNCTION.",
        )
    } else if upper.contains("SCHEMA_NOT_FOUND") || upper.contains("CATALOG_NOT_FOUND") {
        (
            "missing_namespace",
            "Verify catalog and schema names and access before submitting again.",
        )
    } else if upper.contains("TABLE_OR_VIEW_NOT_FOUND") {
        ("missing_table", "Verify the table name, catalog, schema and access.")
    } else if upper.contains("PARSE_SYNTAX_ERROR") || upper.contains("SYNTAX_ERROR") {
        (
            "syntax_error",
            "Correct the SQL using the database diagnostic before submitting again.",
        )
    } else if upper.contains("PERMISSION") || upper.contains("INSUFFICIENT_PRIVILEGES") {
        (
            "permission_denied",
            "Request access to the referenced objects or use an authorized object.",
        )
    } else if error.transient {
        ("transient", "Check warehouse and network availability before retrying.")
    } else {
        (
            "execution_error",
            "Inspect the database diagnostic and verify the statement and warehouse.",
        )
    };
    let error_classes = message.split('[').skip(1).filter_map(|part| {
        let candidate = part.split(']').next()?;
        (!candidate.is_empty()
            && candidate.len() <= 128
            && candidate
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || b"_.".contains(&c)))
        .then_some(candidate)
    });
    let error_class = error_classes
        .clone()
        .find(|class| {
            ["UNRESOLVED_", "TABLE_OR_VIEW_", "PARSE_", "SYNTAX_", "INSUFFICIENT_"]
                .iter()
                .any(|prefix| class.starts_with(prefix))
        })
        .or_else(|| error_classes.into_iter().next())
        .or_else(|| {
            message.split([' ', ':']).find(|token| {
                ["UNRESOLVED_", "TABLE_OR_VIEW_", "PARSE_", "SYNTAX_", "INSUFFICIENT_"]
                    .iter()
                    .any(|prefix| token.starts_with(prefix))
            })
        });
    let mut value = json!({
        "category":category, "message":message, "next_step":next_step,
        "error_class":error_class,
    });
    if shortened {
        value["truncated"] = Value::Bool(true);
    }
    let output = value.to_string();
    if output.len() <= max_bytes {
        return output;
    }
    value["truncated"] = Value::Bool(true);
    // Preserve the original diagnostic prefix within the caller's byte budget.
    value.as_object_mut().unwrap().remove("error_class");
    let fixed_bytes = {
        value["message"] = Value::String(String::new());
        value.to_string().len()
    };
    if fixed_bytes > max_bytes {
        value["next_step"] = Value::String("Inspect the database diagnostic before retrying.".into());
    }
    let mut low = 0;
    let mut high = message.len();
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        let mut end = middle;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        value["message"] = Value::String(message[..end].into());
        if value.to_string().len() <= max_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let mut end = low;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    value["message"] = Value::String(message[..end].into());
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest::proptest! {
        #[test]
        fn arbitrary_unicode_diagnostics_keep_original_prefix_and_output_budget(
            message in ".{0,4096}", budget in 256usize..10_001,
        ) {
            let error = ApiError::permanent(message);
            let output = error_json(&error, budget, false);
            proptest::prop_assert!(output.len() <= budget);
            let value: Value = serde_json::from_str(&output).unwrap();
            let prefix = value["message"].as_str().unwrap();
            proptest::prop_assert!(error.message.starts_with(prefix));
            if prefix.len() < error.message.len() {
                proptest::prop_assert_eq!(&value["truncated"], &Value::Bool(true));
            }
        }
    }

    #[test]
    fn huge_diagnostic_only_classifies_the_bounded_original_prefix() {
        let message = format!("{}[PARSE_SYNTAX_ERROR]", "é🦀".repeat(1_400_000));
        let error = ApiError::statement_failed(message);
        let output = error_json(&error, 10_000, false);
        let value: Value = serde_json::from_str(&output).unwrap();
        assert!(output.len() <= 10_000);
        assert_eq!(value["category"], "execution_error");
        assert_eq!(value["truncated"], true);
        let message = value["message"].as_str().unwrap();
        assert!(error.message.starts_with(message));
        assert!(!message.is_empty());
    }

    #[test]
    #[ignore = "manual diagnostic throughput measurement"]
    fn benchmark_eight_megabyte_diagnostic_with_ten_kilobyte_budget() {
        let error = ApiError::statement_failed(format!("[PARSE_SYNTAX_ERROR] {}", "x".repeat(8 * 1024 * 1024)));
        let start = std::time::Instant::now();
        let mut output_bytes = 0;
        for _ in 0..20 {
            let output = error_json(std::hint::black_box(&error), 10_000, false);
            output_bytes += std::hint::black_box(output.len());
            assert!(output.len() <= 10_000);
        }
        eprintln!(
            "8MiB error / 10kB budget: {:.1}us/call, {} output bytes/call",
            start.elapsed().as_secs_f64() * 1_000_000.0 / 20.0,
            output_bytes / 20
        );
    }

    #[test]
    fn diagnostic_is_valid_bounded_json_even_with_escaped_unicode() {
        let error = ApiError::statement_failed(format!("[PARSE_SYNTAX_ERROR] {}", "\"é\\\n".repeat(1000)));
        for budget in [256, 257, 500, 10_000] {
            let output = error_json(&error, budget, false);
            assert!(output.len() <= budget);
            let value: Value = serde_json::from_str(&output).unwrap();
            assert_eq!(value["category"], "syntax_error");
            if budget < 1000 {
                assert_eq!(value["truncated"], true);
            }
        }
    }
}
