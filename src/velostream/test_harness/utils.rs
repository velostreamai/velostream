//! Shared utilities for the test harness
//!
//! Contains common helper functions used across multiple test harness modules:
//! - FieldValue formatting and conversion
//! - Path resolution utilities
//! - Common constants

use crate::velostream::sql::execution::types::FieldValue;
use std::path::{Path, PathBuf};

// ==================== Constants ====================

/// Default numeric tolerance for floating-point comparisons
pub const DEFAULT_NUMERIC_TOLERANCE: f64 = 0.0001;

/// Maximum sample errors to keep in DLQ statistics
pub const MAX_SAMPLE_ERRORS: usize = 10;

// ==================== DLQ Header Constants ====================

/// Standard error message header
pub const HEADER_ERROR_MESSAGE: &str = "error_message";
/// Alternative error message header (x-prefixed)
pub const HEADER_ERROR_MESSAGE_ALT: &str = "x-error-message";

/// Standard error type header
pub const HEADER_ERROR_TYPE: &str = "error_type";
/// Alternative error type header (x-prefixed)
pub const HEADER_ERROR_TYPE_ALT: &str = "x-error-type";

/// Standard source topic header
pub const HEADER_SOURCE_TOPIC: &str = "source_topic";
/// Alternative source topic header (x-prefixed)
pub const HEADER_SOURCE_TOPIC_ALT: &str = "x-source-topic";

/// Standard error timestamp header
pub const HEADER_ERROR_TIMESTAMP: &str = "error_timestamp";
/// Alternative error timestamp header (x-prefixed)
pub const HEADER_ERROR_TIMESTAMP_ALT: &str = "x-error-timestamp";

// ==================== FieldValue Utilities ====================

/// Convert a FieldValue to its string representation
///
/// This is the canonical function for converting FieldValue to strings,
/// used for key generation, CSV output, comparison, etc.
pub fn field_value_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Null => "NULL".to_string(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::String(s) => s.clone(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Date(d) => d.to_string(),
        FieldValue::Timestamp(t) => t.to_string(),
        FieldValue::Decimal(d) => d.to_string(),
        FieldValue::ScaledInteger(v, s) => format_scaled_integer(*v, *s),
        FieldValue::Array(arr) => format!("[{}]", arr.len()),
        FieldValue::Map(m) => format!("{{map:{}}}", m.len()),
        FieldValue::Struct(m) => format!("{{struct:{}}}", m.len()),
        FieldValue::Interval { value, unit } => format!("{} {:?}", value, unit),
    }
}

/// Format a ScaledInteger as a decimal string
///
/// This is the canonical function for formatting ScaledInteger values,
/// ensuring consistent output across CSV, JSON, and display contexts.
///
/// # Examples
/// ```ignore
/// assert_eq!(format_scaled_integer(12345, 2), "123.45");
/// assert_eq!(format_scaled_integer(100, 0), "100");
/// assert_eq!(format_scaled_integer(-12345, 3), "-12.345");
/// ```
pub fn format_scaled_integer(value: i64, scale: u8) -> String {
    if scale == 0 {
        value.to_string()
    } else {
        let divisor = 10i64.pow(scale as u32);
        let whole = value / divisor;
        let frac = (value % divisor).abs();
        format!("{}.{:0>width$}", whole, frac, width = scale as usize)
    }
}

/// Convert a FieldValue to a JSON-compatible serde_json::Value
pub fn field_value_to_json(value: &FieldValue) -> serde_json::Value {
    match value {
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::String(s) => serde_json::Value::String(s.clone()),
        FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        FieldValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
        FieldValue::ScaledInteger(value, scale) => {
            // Convert to decimal string for JSON to preserve precision
            if *scale == 0 {
                serde_json::Value::Number(serde_json::Number::from(*value))
            } else {
                serde_json::Value::String(format_scaled_integer(*value, *scale))
            }
        }
        FieldValue::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
        FieldValue::Date(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Decimal(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(field_value_to_json).collect())
        }
        FieldValue::Map(map) | FieldValue::Struct(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), field_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        FieldValue::Interval { value, unit } => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "value".to_string(),
                serde_json::Value::Number(serde_json::Number::from(*value)),
            );
            obj.insert(
                "unit".to_string(),
                serde_json::Value::String(format!("{:?}", unit)),
            );
            serde_json::Value::Object(obj)
        }
    }
}

/// Convert a FieldValue to a CSV-safe string
///
/// Handles quoting for values containing commas, quotes, or newlines.
pub fn field_value_to_csv_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Null => String::new(),
        FieldValue::String(s) => {
            // Quote strings that contain commas, quotes, or newlines
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::ScaledInteger(value, scale) => format_scaled_integer(*value, *scale),
        FieldValue::Timestamp(ts) => ts.to_string(),
        FieldValue::Date(d) => d.to_string(),
        FieldValue::Decimal(d) => d.to_string(),
        FieldValue::Array(arr) => {
            let json_arr: Vec<serde_json::Value> = arr.iter().map(field_value_to_json).collect();
            serde_json::to_string(&json_arr).unwrap_or_default()
        }
        FieldValue::Map(map) | FieldValue::Struct(map) => {
            let json_obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), field_value_to_json(v)))
                .collect();
            serde_json::to_string(&json_obj).unwrap_or_default()
        }
        FieldValue::Interval { value, unit } => format!("{} {:?}", value, unit),
    }
}

// ==================== Path Utilities ====================

/// Resolve a path relative to a base directory
///
/// If the path is absolute, returns it as-is.
/// If the path is relative, joins it with the base directory.
pub fn resolve_path(path: &str, base_dir: &Path) -> PathBuf {
    let path_buf = PathBuf::from(path);
    if path_buf.is_absolute() {
        path_buf
    } else {
        base_dir.join(path_buf)
    }
}

/// Get the parent directory of a file path, or the current directory if none
pub fn get_base_dir(file_path: &Path) -> PathBuf {
    file_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Resolve a schema file path relative to the current working directory
///
/// This function resolves schema paths using the following rules:
/// 1. Absolute paths are returned as-is
/// 2. Relative paths are joined with the current working directory
/// 3. If `std::env::current_dir()` fails, logs a warning and returns the relative path
///
/// # Arguments
/// * `schema_path` - The schema file path (may be absolute or relative)
///
/// # Returns
/// The resolved absolute path, or the original relative path if cwd resolution fails
pub fn resolve_schema_path(schema_path: &str) -> PathBuf {
    let path = Path::new(schema_path);

    if path.is_absolute() {
        return path.to_path_buf();
    }

    match std::env::current_dir() {
        Ok(cwd) => cwd.join(schema_path),
        Err(e) => {
            log::warn!(
                "Failed to get current directory for schema resolution: {}. Using relative path.",
                e
            );
            path.to_path_buf()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_scaled_integer() {
        assert_eq!(format_scaled_integer(12345, 2), "123.45");
        assert_eq!(format_scaled_integer(100, 0), "100");
        assert_eq!(format_scaled_integer(-12345, 3), "-12.345");
        assert_eq!(format_scaled_integer(5, 2), "0.05");
        assert_eq!(format_scaled_integer(0, 4), "0.0000");
    }

    #[test]
    fn test_field_value_to_string() {
        assert_eq!(field_value_to_string(&FieldValue::Null), "NULL");
        assert_eq!(field_value_to_string(&FieldValue::Integer(42)), "42");
        assert_eq!(field_value_to_string(&FieldValue::Float(3.15)), "3.15");
        assert_eq!(
            field_value_to_string(&FieldValue::String("hello".to_string())),
            "hello"
        );
        assert_eq!(field_value_to_string(&FieldValue::Boolean(true)), "true");
        assert_eq!(
            field_value_to_string(&FieldValue::ScaledInteger(12345, 2)),
            "123.45"
        );
    }

    #[test]
    fn test_field_value_to_csv_string() {
        assert_eq!(field_value_to_csv_string(&FieldValue::Integer(123)), "123");
        assert_eq!(
            field_value_to_csv_string(&FieldValue::String("hello".to_string())),
            "hello"
        );
        assert_eq!(
            field_value_to_csv_string(&FieldValue::String("hello,world".to_string())),
            "\"hello,world\""
        );
        assert_eq!(
            field_value_to_csv_string(&FieldValue::String("say \"hi\"".to_string())),
            "\"say \"\"hi\"\"\""
        );
        assert_eq!(field_value_to_csv_string(&FieldValue::Null), "");
    }

    #[test]
    fn test_field_value_to_json() {
        assert_eq!(
            field_value_to_json(&FieldValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            field_value_to_json(&FieldValue::Integer(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            field_value_to_json(&FieldValue::Boolean(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            field_value_to_json(&FieldValue::ScaledInteger(12345, 2)),
            serde_json::json!("123.45")
        );
    }

    #[test]
    fn test_resolve_path() {
        let base = Path::new("/home/user/test");

        assert_eq!(
            resolve_path("data.csv", base),
            PathBuf::from("/home/user/test/data.csv")
        );
        assert_eq!(
            resolve_path("./data.csv", base),
            PathBuf::from("/home/user/test/./data.csv")
        );
        assert_eq!(
            resolve_path("/absolute/path.csv", base),
            PathBuf::from("/absolute/path.csv")
        );
    }

    #[test]
    fn test_get_base_dir() {
        assert_eq!(
            get_base_dir(Path::new("/home/user/test/spec.yaml")),
            PathBuf::from("/home/user/test")
        );
        assert_eq!(get_base_dir(Path::new("spec.yaml")), PathBuf::from("."));
    }

    #[test]
    fn test_resolve_schema_path_absolute() {
        // Absolute paths should be returned as-is
        let result = resolve_schema_path("/absolute/path/schema.yaml");
        assert_eq!(result, PathBuf::from("/absolute/path/schema.yaml"));
    }

    #[test]
    fn test_resolve_schema_path_relative() {
        // Relative paths should be joined with cwd
        let result = resolve_schema_path("schemas/market_data.yaml");

        // Should be an absolute path (joined with cwd)
        assert!(
            result.is_absolute(),
            "Expected absolute path, got: {:?}",
            result
        );

        // Should end with the relative path components
        assert!(
            result.ends_with("schemas/market_data.yaml"),
            "Expected path to end with 'schemas/market_data.yaml', got: {:?}",
            result
        );
    }

    #[test]
    fn test_resolve_schema_path_dot_relative() {
        // Paths starting with ./ should also be resolved
        let result = resolve_schema_path("./config/schema.yaml");

        assert!(
            result.is_absolute(),
            "Expected absolute path, got: {:?}",
            result
        );
    }
}
