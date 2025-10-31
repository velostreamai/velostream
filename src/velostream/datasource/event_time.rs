//! Generic Event-Time Extraction Module
//!
//! This module provides generic event-time extraction functionality that works across
//! ALL data sources (Kafka, File, HTTP, SQL, S3, etc.).
//!
//! ## Purpose
//!
//! Extract event-time timestamps from record fields based on configuration, enabling
//! proper event-time semantics for watermarks and windowing operations.
//!
//! ## Usage
//!
//! ```rust
//! use velostream::velostream::datasource::event_time::{EventTimeConfig, extract_event_time};
//! use std::collections::HashMap;
//!
//! // Configure event-time extraction
//! let mut props = HashMap::new();
//! props.insert("event.time.field".to_string(), "timestamp".to_string());
//! props.insert("event.time.format".to_string(), "epoch_millis".to_string());
//!
//! let config = EventTimeConfig::from_properties(&props).unwrap();
//!
//! // Extract event-time from fields
//! // let event_time = extract_event_time(&record.fields, &config)?;
//! ```

use crate::velostream::sql::execution::types::FieldValue;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::collections::HashMap;
use thiserror::Error;

/// Timestamp format enumeration
///
/// Defines the supported timestamp formats for event-time extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimestampFormat {
    /// Unix epoch milliseconds (e.g., 1696723200000)
    EpochMillis,
    /// Unix epoch seconds (e.g., 1696723200)
    EpochSeconds,
    /// ISO 8601 format (e.g., "2023-10-08T00:00:00Z")
    ISO8601,
    /// Custom chrono format string (e.g., "%Y-%m-%d %H:%M:%S")
    Custom(String),
}

impl TimestampFormat {
    /// Parse a timestamp format string into a TimestampFormat variant
    ///
    /// # Arguments
    ///
    /// * `s` - Format string (e.g., "epoch_millis", "iso8601", custom format)
    ///
    /// # Examples
    ///
    /// ```
    /// use velostream::velostream::datasource::event_time::TimestampFormat;
    ///
    /// let format = TimestampFormat::parse("epoch_millis").unwrap();
    /// assert_eq!(format, TimestampFormat::EpochMillis);
    ///
    /// let custom = TimestampFormat::parse("%Y-%m-%d").unwrap();
    /// assert_eq!(custom, TimestampFormat::Custom("%Y-%m-%d".to_string()));
    /// ```
    pub fn parse(s: &str) -> Result<Self, EventTimeError> {
        match s {
            "epoch_millis" => Ok(TimestampFormat::EpochMillis),
            "epoch_seconds" | "epoch" => Ok(TimestampFormat::EpochSeconds),
            "iso8601" | "ISO8601" => Ok(TimestampFormat::ISO8601),
            custom => Ok(TimestampFormat::Custom(custom.to_string())),
        }
    }
}

/// Configuration for event-time extraction
///
/// Specifies which field to extract and what format it's in.
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Field name to extract timestamp from
    pub field_name: String,
    /// Format of the timestamp (None = auto-detect)
    pub format: Option<TimestampFormat>,
}

impl EventTimeConfig {
    /// Create EventTimeConfig from properties HashMap
    ///
    /// Looks for "event.time.field" and optional "event.time.format" keys.
    ///
    /// # Arguments
    ///
    /// * `properties` - Configuration properties
    ///
    /// # Returns
    ///
    /// `Some(EventTimeConfig)` if event.time.field is present, `None` otherwise
    ///
    /// # Examples
    ///
    /// ```
    /// use velostream::velostream::datasource::event_time::EventTimeConfig;
    /// use std::collections::HashMap;
    ///
    /// let mut props = HashMap::new();
    /// props.insert("event.time.field".to_string(), "timestamp".to_string());
    /// props.insert("event.time.format".to_string(), "epoch_millis".to_string());
    ///
    /// let config = EventTimeConfig::from_properties(&props).unwrap();
    /// assert_eq!(config.field_name, "timestamp");
    /// ```
    pub fn from_properties(properties: &HashMap<String, String>) -> Option<Self> {
        let field_name = properties.get("event.time.field")?.clone();
        let format = properties
            .get("event.time.format")
            .map(|s| TimestampFormat::parse(s))
            .transpose()
            .ok()?;

        Some(EventTimeConfig { field_name, format })
    }

    /// Create a new EventTimeConfig with explicit values
    ///
    /// # Arguments
    ///
    /// * `field_name` - Name of the field containing the timestamp
    /// * `format` - Optional format specification
    pub fn new(field_name: String, format: Option<TimestampFormat>) -> Self {
        EventTimeConfig { field_name, format }
    }
}

/// Extract event-time from StreamRecord fields
///
/// Generic extraction function that works for ANY data source.
///
/// # Arguments
///
/// * `fields` - HashMap of field names to values from StreamRecord
/// * `config` - Event-time extraction configuration
///
/// # Returns
///
/// `DateTime<Utc>` if extraction succeeds, `EventTimeError` otherwise
///
/// # Examples
///
/// ```
/// use velostream::velostream::datasource::event_time::{EventTimeConfig, TimestampFormat, extract_event_time};
/// use velostream::velostream::sql::execution::types::FieldValue;
/// use std::collections::HashMap;
///
/// let mut fields = HashMap::new();
/// fields.insert("timestamp".to_string(), FieldValue::Integer(1696723200000));
///
/// let config = EventTimeConfig::new(
///     "timestamp".to_string(),
///     Some(TimestampFormat::EpochMillis)
/// );
///
/// let event_time = extract_event_time(&fields, &config).unwrap();
/// ```
pub fn extract_event_time(
    fields: &HashMap<String, FieldValue>,
    config: &EventTimeConfig,
) -> Result<DateTime<Utc>, EventTimeError> {
    let field_value =
        fields
            .get(&config.field_name)
            .ok_or_else(|| EventTimeError::MissingField {
                field: config.field_name.clone(),
                available_fields: fields.keys().cloned().collect(),
            })?;

    let datetime = match &config.format {
        Some(TimestampFormat::EpochMillis) => {
            extract_epoch_millis(field_value, &config.field_name)?
        }
        Some(TimestampFormat::EpochSeconds) => {
            extract_epoch_seconds(field_value, &config.field_name)?
        }
        Some(TimestampFormat::ISO8601) => extract_iso8601(field_value, &config.field_name)?,
        Some(TimestampFormat::Custom(fmt)) => {
            extract_custom_format(field_value, fmt, &config.field_name)?
        }
        None => {
            // Auto-detect: try integer (epoch millis), then ISO 8601
            auto_detect_timestamp(field_value, &config.field_name)?
        }
    };

    Ok(datetime)
}

/// Extract timestamp from epoch milliseconds field
fn extract_epoch_millis(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    let millis = match field_value {
        FieldValue::Integer(i) => *i,
        _ => {
            return Err(EventTimeError::TypeMismatch {
                field: field_name.to_string(),
                expected: "Integer (epoch millis)".to_string(),
                actual: field_value.type_name(),
            });
        }
    };

    DateTime::from_timestamp_millis(millis).ok_or(EventTimeError::InvalidTimestamp {
        value: format!("{}", millis),
        format: "epoch_millis",
    })
}

/// Extract timestamp from epoch seconds field
fn extract_epoch_seconds(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    let secs = match field_value {
        FieldValue::Integer(i) => *i,
        _ => {
            return Err(EventTimeError::TypeMismatch {
                field: field_name.to_string(),
                expected: "Integer (epoch seconds)".to_string(),
                actual: field_value.type_name(),
            });
        }
    };

    DateTime::from_timestamp(secs, 0).ok_or(EventTimeError::InvalidTimestamp {
        value: format!("{}", secs),
        format: "epoch_seconds",
    })
}

/// Extract timestamp from ISO 8601 string field
fn extract_iso8601(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    let s = match field_value {
        FieldValue::String(s) => s,
        _ => {
            return Err(EventTimeError::TypeMismatch {
                field: field_name.to_string(),
                expected: "String (ISO 8601)".to_string(),
                actual: field_value.type_name(),
            });
        }
    };

    DateTime::parse_from_rfc3339(s)
        .map_err(|e| EventTimeError::ParseError {
            value: s.clone(),
            format: "ISO8601".to_string(),
            error: e.to_string(),
        })
        .map(|dt| dt.with_timezone(&Utc))
}

/// Extract timestamp using custom chrono format string
fn extract_custom_format(
    field_value: &FieldValue,
    format: &str,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    let s = match field_value {
        FieldValue::String(s) => s,
        _ => {
            return Err(EventTimeError::TypeMismatch {
                field: field_name.to_string(),
                expected: format!("String ({})", format),
                actual: field_value.type_name(),
            });
        }
    };

    let naive_dt =
        NaiveDateTime::parse_from_str(s, format).map_err(|e| EventTimeError::ParseError {
            value: s.clone(),
            format: format.to_string(),
            error: e.to_string(),
        })?;

    naive_dt
        .and_local_timezone(Utc)
        .single()
        .ok_or(EventTimeError::AmbiguousTimezone { value: s.clone() })
}

/// Auto-detect timestamp format from field value
///
/// Tries the following in order:
/// 1. Integer -> epoch milliseconds
/// 2. String -> ISO 8601
fn auto_detect_timestamp(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    // Try integer (epoch millis) first
    if let FieldValue::Integer(millis) = field_value {
        if let Some(dt) = DateTime::from_timestamp_millis(*millis) {
            return Ok(dt);
        }
    }

    // Try string (ISO 8601)
    if let FieldValue::String(s) = field_value {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    Err(EventTimeError::AutoDetectFailed {
        field: field_name.to_string(),
        value_type: field_value.type_name(),
    })
}

/// Event-time extraction errors
///
/// Provides detailed error information for debugging extraction failures.
#[derive(Debug, Error)]
pub enum EventTimeError {
    /// Field specified in config not found in record
    #[error("Field '{field}' not found in record. Available fields: {}", available_fields.join(", "))]
    MissingField {
        field: String,
        available_fields: Vec<String>,
    },

    /// Field value type doesn't match expected type for format
    #[error("Type mismatch for field '{field}': expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: &'static str,
    },

    /// Timestamp value is invalid for the specified format
    #[error("Invalid timestamp value '{value}' for format '{format}'")]
    InvalidTimestamp { value: String, format: &'static str },

    /// Failed to parse timestamp value
    #[error("Failed to parse '{value}' as {format}: {error}")]
    ParseError {
        value: String,
        format: String,
        error: String,
    },

    /// Timezone conversion resulted in ambiguous result
    #[error("Ambiguous timezone for value '{value}'")]
    AmbiguousTimezone { value: String },

    /// Auto-detection failed - couldn't determine format
    #[error("Auto-detect failed for field '{field}' with type {value_type}")]
    AutoDetectFailed {
        field: String,
        value_type: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_format_parse() {
        assert_eq!(
            TimestampFormat::parse("epoch_millis").unwrap(),
            TimestampFormat::EpochMillis
        );
        assert_eq!(
            TimestampFormat::parse("epoch_seconds").unwrap(),
            TimestampFormat::EpochSeconds
        );
        assert_eq!(
            TimestampFormat::parse("epoch").unwrap(),
            TimestampFormat::EpochSeconds
        );
        assert_eq!(
            TimestampFormat::parse("iso8601").unwrap(),
            TimestampFormat::ISO8601
        );
        assert_eq!(
            TimestampFormat::parse("ISO8601").unwrap(),
            TimestampFormat::ISO8601
        );
        assert_eq!(
            TimestampFormat::parse("%Y-%m-%d").unwrap(),
            TimestampFormat::Custom("%Y-%m-%d".to_string())
        );
    }

    #[test]
    fn test_event_time_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("event.time.field".to_string(), "timestamp".to_string());
        props.insert("event.time.format".to_string(), "epoch_millis".to_string());

        let config = EventTimeConfig::from_properties(&props).unwrap();
        assert_eq!(config.field_name, "timestamp");
        assert_eq!(config.format, Some(TimestampFormat::EpochMillis));
    }

    #[test]
    fn test_event_time_config_from_properties_no_format() {
        let mut props = HashMap::new();
        props.insert("event.time.field".to_string(), "ts".to_string());

        let config = EventTimeConfig::from_properties(&props).unwrap();
        assert_eq!(config.field_name, "ts");
        assert_eq!(config.format, None);
    }

    #[test]
    fn test_event_time_config_from_properties_missing() {
        let props = HashMap::new();
        assert!(EventTimeConfig::from_properties(&props).is_none());
    }

    #[test]
    fn test_extract_epoch_millis() {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(1696723200000));

        let config =
            EventTimeConfig::new("timestamp".to_string(), Some(TimestampFormat::EpochMillis));

        let dt = extract_event_time(&fields, &config).unwrap();
        assert_eq!(dt.timestamp_millis(), 1696723200000);
    }

    #[test]
    fn test_extract_epoch_seconds() {
        let mut fields = HashMap::new();
        fields.insert("ts".to_string(), FieldValue::Integer(1696723200));

        let config = EventTimeConfig::new("ts".to_string(), Some(TimestampFormat::EpochSeconds));

        let dt = extract_event_time(&fields, &config).unwrap();
        assert_eq!(dt.timestamp(), 1696723200);
    }

    #[test]
    fn test_extract_iso8601() {
        let mut fields = HashMap::new();
        fields.insert(
            "event_time".to_string(),
            FieldValue::String("2023-10-08T00:00:00Z".to_string()),
        );

        let config = EventTimeConfig::new("event_time".to_string(), Some(TimestampFormat::ISO8601));

        let dt = extract_event_time(&fields, &config).unwrap();
        assert_eq!(dt.timestamp(), 1696723200);
    }

    #[test]
    fn test_auto_detect_epoch_millis() {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(1696723200000));

        let config = EventTimeConfig::new("timestamp".to_string(), None);

        let dt = extract_event_time(&fields, &config).unwrap();
        assert_eq!(dt.timestamp_millis(), 1696723200000);
    }

    #[test]
    fn test_auto_detect_iso8601() {
        let mut fields = HashMap::new();
        fields.insert(
            "event_time".to_string(),
            FieldValue::String("2023-10-08T00:00:00Z".to_string()),
        );

        let config = EventTimeConfig::new("event_time".to_string(), None);

        let dt = extract_event_time(&fields, &config).unwrap();
        assert_eq!(dt.timestamp(), 1696723200);
    }

    #[test]
    fn test_missing_field_error() {
        let fields = HashMap::new();
        let config = EventTimeConfig::new("timestamp".to_string(), None);

        let result = extract_event_time(&fields, &config);
        assert!(result.is_err());

        if let Err(EventTimeError::MissingField { field, .. }) = result {
            assert_eq!(field, "timestamp");
        } else {
            panic!("Expected MissingField error");
        }
    }

    #[test]
    fn test_type_mismatch_error() {
        let mut fields = HashMap::new();
        fields.insert(
            "timestamp".to_string(),
            FieldValue::String("not a number".to_string()),
        );

        let config =
            EventTimeConfig::new("timestamp".to_string(), Some(TimestampFormat::EpochMillis));

        let result = extract_event_time(&fields, &config);
        assert!(result.is_err());

        if let Err(EventTimeError::TypeMismatch { field, .. }) = result {
            assert_eq!(field, "timestamp");
        } else {
            panic!("Expected TypeMismatch error");
        }
    }
}
