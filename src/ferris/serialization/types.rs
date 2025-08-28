//! Core types for serialization

use std::collections::HashMap;

/// Internal value type for SQL execution (replaces hardcoded serde_json::Value)
#[derive(Debug, Clone, PartialEq)]
pub enum InternalValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Null,
    /// Scaled integer for financial precision (value, scale)
    ScaledNumber(i64, u8),
    Array(Vec<InternalValue>),
    Object(HashMap<String, InternalValue>),
}
