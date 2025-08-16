use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
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
    /// Get the type name for error messages
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

    /// Check if this value is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            FieldValue::Integer(_) | FieldValue::Float(_) | FieldValue::Decimal(_)
        )
    }

    /// Convert to string representation for display
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
