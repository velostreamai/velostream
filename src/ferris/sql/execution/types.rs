//! Core streaming SQL data types.
//!
//! This module contains the fundamental data types used throughout the streaming SQL engine:
//! - [`FieldValue`] - The value type system supporting SQL data types
//! - [`StreamRecord`] - The record format for streaming data processing

use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// A value in a SQL record field
///
/// This enum represents all supported SQL data types in the streaming execution engine.
/// It supports both simple types (integers, strings, booleans) and complex types
/// (arrays, maps, structured data).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Boolean value (true/false)
    Boolean(bool),
    /// SQL NULL value
    Null,
    /// Date type (YYYY-MM-DD)
    Date(NaiveDate),
    /// Timestamp type (YYYY-MM-DD HH:MM:SS[.nnn])
    Timestamp(NaiveDateTime),
    /// Decimal type for precise arithmetic
    Decimal(Decimal),
    /// Array of values - all elements must be the same type
    Array(Vec<FieldValue>),
    /// Map of key-value pairs - keys must be strings
    Map(HashMap<String, FieldValue>),
    /// Structured data with named fields
    Struct(HashMap<String, FieldValue>),
}

impl FieldValue {
    /// Get the type name for error messages and debugging
    ///
    /// Returns a static string representing the type name that can be used
    /// in error messages and debugging output.
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Integer(_) => "INTEGER",
            FieldValue::Float(_) => "FLOAT",
            FieldValue::String(_) => "STRING",
            FieldValue::Boolean(_) => "BOOLEAN",
            FieldValue::Null => "NULL",
            FieldValue::Date(_) => "DATE",
            FieldValue::Timestamp(_) => "TIMESTAMP",
            FieldValue::Decimal(_) => "DECIMAL",
            FieldValue::Array(_) => "ARRAY",
            FieldValue::Map(_) => "MAP",
            FieldValue::Struct(_) => "STRUCT",
        }
    }

    /// Check if this value represents a numeric type
    ///
    /// Returns true for integers, floats, and decimals that can be used
    /// in arithmetic operations.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            FieldValue::Integer(_) | FieldValue::Float(_) | FieldValue::Decimal(_)
        )
    }

    /// Convert this value to a string representation for display
    ///
    /// This method provides a human-readable string representation of the value
    /// that's suitable for output and debugging. Unlike Debug formatting,
    /// this provides clean, SQL-like formatting.
    pub fn to_display_string(&self) -> String {
        match self {
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::String(s) => s.clone(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Date(d) => d.format("%Y-%m-%d").to_string(),
            FieldValue::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            FieldValue::Decimal(dec) => dec.to_string(),
            FieldValue::Array(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_display_string()).collect();
                format!("[{}]", elements.join(", "))
            }
            FieldValue::Map(map) => {
                let pairs: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_display_string()))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
            FieldValue::Struct(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|(name, value)| format!("{}: {}", name, value.to_display_string()))
                    .collect();
                format!("{{{}}}", field_strs.join(", "))
            }
        }
    }
}

/// A record in a streaming data source
///
/// This structure represents a single record from a streaming data source like Kafka.
/// It contains the actual field data plus metadata about the record's position and
/// timing within the stream.
#[derive(Debug, Clone)]
pub struct StreamRecord {
    /// The actual field data for this record
    pub fields: HashMap<String, FieldValue>,
    /// Timestamp when this record was created (milliseconds since epoch)
    pub timestamp: i64,
    /// Offset of this record within its partition
    pub offset: i64,
    /// Partition number this record came from
    pub partition: i32,
    /// Message headers (key-value pairs) associated with this record
    pub headers: HashMap<String, String>,
}

impl StreamRecord {
    /// Create a new StreamRecord with the given fields
    ///
    /// This constructor creates a record with the specified field data and
    /// default values for metadata fields (timestamp=0, offset=0, partition=0, no headers).
    pub fn new(fields: HashMap<String, FieldValue>) -> Self {
        Self {
            fields,
            timestamp: 0,
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
        }
    }

    /// Create a new StreamRecord with fields and metadata
    ///
    /// This constructor allows setting all record metadata along with the field data.
    pub fn with_metadata(
        fields: HashMap<String, FieldValue>,
        timestamp: i64,
        offset: i64,
        partition: i32,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            fields,
            timestamp,
            offset,
            partition,
            headers,
        }
    }

    /// Get a field value by name
    ///
    /// Returns a reference to the field value if it exists, or None if the field
    /// is not present in this record.
    pub fn get_field(&self, name: &str) -> Option<&FieldValue> {
        self.fields.get(name)
    }

    /// Check if a field exists in this record
    ///
    /// Returns true if the field is present, regardless of its value (including NULL).
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Get the number of fields in this record
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}
