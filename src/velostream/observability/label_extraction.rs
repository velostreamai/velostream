// FR-073 Phase 3: Enhanced Label Extraction
//
// This module provides advanced label extraction capabilities for SQL-native observability:
// - Nested field access (e.g., `metadata.region`)
// - Type conversion with Prometheus-compatible formatting
// - Default values for missing fields
// - Label value validation

use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Configuration for label extraction behavior
#[derive(Debug, Clone)]
pub struct LabelExtractionConfig {
    /// Default value to use when a field is missing
    pub default_value: String,

    /// Whether to validate label values against Prometheus rules
    pub validate_values: bool,

    /// Maximum length for label values (Prometheus recommended: 1024)
    pub max_value_length: usize,
}

impl Default for LabelExtractionConfig {
    fn default() -> Self {
        Self {
            default_value: "unknown".to_string(),
            validate_values: true,
            max_value_length: 1024,
        }
    }
}

/// Extract label values from a record with enhanced capabilities
pub fn extract_label_values(
    record: &StreamRecord,
    label_names: &[String],
    config: &LabelExtractionConfig,
) -> Vec<String> {
    label_names
        .iter()
        .map(|label_name| extract_single_label(record, label_name, config))
        .collect()
}

/// Extract a single label value with nested field support
fn extract_single_label(
    record: &StreamRecord,
    label_name: &str,
    config: &LabelExtractionConfig,
) -> String {
    // Check if this is a nested field access (contains '.')
    if label_name.contains('.') {
        extract_nested_field(record, label_name, config)
    } else {
        // Simple top-level field access
        match record.fields.get(label_name) {
            Some(value) => field_value_to_label_string(value, config),
            None => config.default_value.clone(),
        }
    }
}

/// Extract a nested field value using dot notation
fn extract_nested_field(
    record: &StreamRecord,
    field_path: &str,
    config: &LabelExtractionConfig,
) -> String {
    let parts: Vec<&str> = field_path.split('.').collect();

    // Start with the top-level field
    let first_part = parts[0];
    let mut current_value = match record.fields.get(first_part) {
        Some(v) => v,
        None => return config.default_value.clone(),
    };

    // Navigate through nested fields
    for part in &parts[1..] {
        match current_value {
            FieldValue::Map(map) => match map.get(*part) {
                Some(v) => current_value = v,
                None => return config.default_value.clone(),
            },
            _ => {
                // Can't navigate further - not a map
                return config.default_value.clone();
            }
        }
    }

    field_value_to_label_string(current_value, config)
}

/// Convert a FieldValue to a Prometheus-compatible label string
///
/// Uses the FieldValue::to_label_string method with additional validation if configured.
fn field_value_to_label_string(value: &FieldValue, config: &LabelExtractionConfig) -> String {
    let raw_value = value.to_label_string(&config.default_value, config.max_value_length);

    if config.validate_values {
        sanitize_label_value(&raw_value, config)
    } else {
        raw_value
    }
}

/// Sanitize a label value to meet Prometheus requirements
fn sanitize_label_value(value: &str, config: &LabelExtractionConfig) -> String {
    // Prometheus label values can contain any Unicode characters
    // but we'll apply some basic sanitization for safety
    let sanitized = value
        .chars()
        .map(|c| {
            // Replace control characters with space
            if c.is_control() { ' ' } else { c }
        })
        .collect::<String>();

    // Trim whitespace and truncate if needed
    let trimmed = sanitized.trim();
    truncate_if_needed(trimmed, config.max_value_length)
}

/// Truncate a string if it exceeds the maximum length
fn truncate_if_needed(value: &str, max_length: usize) -> String {
    if value.len() > max_length {
        let mut truncated = value.chars().take(max_length - 3).collect::<String>();
        truncated.push_str("...");
        truncated
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    fn create_test_record() -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert("volume".to_string(), FieldValue::Integer(1000));
        fields.insert("price".to_string(), FieldValue::Float(150.25));
        fields.insert("active".to_string(), FieldValue::Boolean(true));

        // Nested metadata
        let mut metadata = HashMap::new();
        metadata.insert("region".to_string(), FieldValue::String("US".to_string()));
        metadata.insert(
            "exchange".to_string(),
            FieldValue::String("NASDAQ".to_string()),
        );
        fields.insert("metadata".to_string(), FieldValue::Map(metadata));

        StreamRecord {
            fields,
            timestamp: 1000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn test_extract_simple_string_field() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "symbol", &config);
        assert_eq!(value, "AAPL");
    }

    #[test]
    fn test_extract_integer_field() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "volume", &config);
        assert_eq!(value, "1000");
    }

    #[test]
    fn test_extract_float_field() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "price", &config);
        assert_eq!(value, "150.25");
    }

    #[test]
    fn test_extract_boolean_field() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "active", &config);
        assert_eq!(value, "true");
    }

    #[test]
    fn test_extract_missing_field_returns_default() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "nonexistent", &config);
        assert_eq!(value, "unknown");
    }

    #[test]
    fn test_extract_nested_field() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "metadata.region", &config);
        assert_eq!(value, "US");

        let value = extract_single_label(&record, "metadata.exchange", &config);
        assert_eq!(value, "NASDAQ");
    }

    #[test]
    fn test_extract_missing_nested_field_returns_default() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let value = extract_single_label(&record, "metadata.nonexistent", &config);
        assert_eq!(value, "unknown");
    }

    #[test]
    fn test_extract_nested_field_from_non_map_returns_default() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        // Try to access nested field on a string value
        let value = extract_single_label(&record, "symbol.nested", &config);
        assert_eq!(value, "unknown");
    }

    #[test]
    fn test_extract_multiple_labels() {
        let record = create_test_record();
        let config = LabelExtractionConfig::default();

        let labels = vec![
            "symbol".to_string(),
            "metadata.region".to_string(),
            "volume".to_string(),
        ];

        let values = extract_label_values(&record, &labels, &config);
        assert_eq!(values, vec!["AAPL", "US", "1000"]);
    }

    #[test]
    fn test_custom_default_value() {
        let record = create_test_record();
        let config = LabelExtractionConfig {
            default_value: "N/A".to_string(),
            ..Default::default()
        };

        let value = extract_single_label(&record, "nonexistent", &config);
        assert_eq!(value, "N/A");
    }

    #[test]
    fn test_truncate_long_values() {
        let mut fields = HashMap::new();
        let long_string = "a".repeat(2000);
        fields.insert("long".to_string(), FieldValue::String(long_string));

        let record = StreamRecord {
            fields,
            timestamp: 1000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        let config = LabelExtractionConfig::default();
        let value = extract_single_label(&record, "long", &config);

        assert!(value.len() <= 1024);
        assert!(value.ends_with("..."));
    }

    #[test]
    fn test_sanitize_control_characters() {
        let mut fields = HashMap::new();
        fields.insert(
            "dirty".to_string(),
            FieldValue::String("hello\nworld\ttab".to_string()),
        );

        let record = StreamRecord {
            fields,
            timestamp: 1000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        let config = LabelExtractionConfig::default();
        let value = extract_single_label(&record, "dirty", &config);

        // Control characters should be replaced with spaces
        assert!(!value.contains('\n'));
        assert!(!value.contains('\t'));
    }

    #[test]
    fn test_scaled_integer_conversion() {
        let mut fields = HashMap::new();
        fields.insert("scaled".to_string(), FieldValue::ScaledInteger(123450, 3));

        let record = StreamRecord {
            fields,
            timestamp: 1000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        let config = LabelExtractionConfig::default();
        let value = extract_single_label(&record, "scaled", &config);

        assert_eq!(value, "123.45");
    }
}
