//! Core streaming SQL data types.
//!
//! This module contains the fundamental data types used throughout the streaming SQL engine:
//! - [`FieldValue`] - The value type system supporting SQL data types
//! - [`StreamRecord`] - The record format for streaming data processing

use crate::velostream::sql::ast::TimeUnit;
use crate::velostream::sql::error::SqlError;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

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
    /// Scaled integer for financial precision (value, *scale) where value is scaled by 10^*scale
    ScaledInteger(i64, u8),
    /// Array of values - all elements must be the same type
    Array(Vec<FieldValue>),
    /// Map of key-value pairs - keys must be strings
    Map(HashMap<String, FieldValue>),
    /// Structured data with named fields
    Struct(HashMap<String, FieldValue>),
    /// Time interval (value, unit)
    Interval { value: i64, unit: TimeUnit },
}

/// Phase 4B: Hash implementation for FieldValue to support GroupKey optimization
///
/// This enables zero-allocation group keys using Arc<[FieldValue]> instead of Vec<String>.
/// Special handling for f64 (Float) using bit representation to make it hashable.
impl Hash for FieldValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash discriminant first to distinguish variants
        std::mem::discriminant(self).hash(state);

        match self {
            FieldValue::Integer(i) => i.hash(state),
            FieldValue::Float(f) => {
                // Use bit representation for f64 to make it hashable
                // This handles NaN, infinity, and -0.0 correctly
                f.to_bits().hash(state);
            }
            FieldValue::String(s) => s.hash(state),
            FieldValue::Boolean(b) => b.hash(state),
            FieldValue::Null => {}
            FieldValue::Date(d) => {
                // Hash date components
                d.year().hash(state);
                d.month().hash(state);
                d.day().hash(state);
            }
            FieldValue::Timestamp(ts) => {
                // Hash timestamp as i64 milliseconds
                ts.and_utc().timestamp_millis().hash(state);
            }
            FieldValue::Decimal(dec) => {
                // Hash decimal as string representation (deterministic)
                dec.to_string().hash(state);
            }
            FieldValue::ScaledInteger(value, scale) => {
                value.hash(state);
                scale.hash(state);
            }
            FieldValue::Array(arr) => {
                // Hash length and each element
                arr.len().hash(state);
                for elem in arr {
                    elem.hash(state);
                }
            }
            FieldValue::Map(map) => {
                // Sort keys for deterministic hashing
                let mut sorted_keys: Vec<&String> = map.keys().collect();
                sorted_keys.sort();
                sorted_keys.len().hash(state);
                for key in sorted_keys {
                    key.hash(state);
                    map.get(key).unwrap().hash(state);
                }
            }
            FieldValue::Struct(fields) => {
                // Sort keys for deterministic hashing
                let mut sorted_keys: Vec<&String> = fields.keys().collect();
                sorted_keys.sort();
                sorted_keys.len().hash(state);
                for key in sorted_keys {
                    key.hash(state);
                    fields.get(key).unwrap().hash(state);
                }
            }
            FieldValue::Interval { value, unit } => {
                value.hash(state);
                // Hash unit as discriminant
                std::mem::discriminant(unit).hash(state);
            }
        }
    }
}

/// System column names in Velostream
///
/// These are special columns that come from StreamRecord properties, not from field data.
/// OPTIMIZATION: Defined in UPPERCASE for internal use. User input is normalized once at parse time.
/// This eliminates repeated string allocations during query execution.
pub mod system_columns {
    use super::HashSet;
    use std::sync::OnceLock;

    /// Processing time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const TIMESTAMP: &str = "_TIMESTAMP";
    /// Kafka partition offset for the record (UPPERCASE internal form)
    pub const OFFSET: &str = "_OFFSET";
    /// Kafka partition number (UPPERCASE internal form)
    pub const PARTITION: &str = "_PARTITION";
    /// Event time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const EVENT_TIME: &str = "_EVENT_TIME";
    /// Window start time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const WINDOW_START: &str = "_WINDOW_START";
    /// Window end time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const WINDOW_END: &str = "_WINDOW_END";

    /// Array of all system column names (UPPERCASE) for validation
    pub const ALL: &[&str] = &[
        TIMESTAMP,
        OFFSET,
        PARTITION,
        EVENT_TIME,
        WINDOW_START,
        WINDOW_END,
    ];

    /// Lazy-initialized system columns set for O(1) lookups (uses UPPERCASE)
    fn get_system_columns_set() -> &'static HashSet<&'static str> {
        static SYSTEM_COLUMNS_SET: OnceLock<HashSet<&'static str>> = OnceLock::new();
        SYSTEM_COLUMNS_SET.get_or_init(|| {
            let mut set = HashSet::with_capacity(6);
            set.insert(TIMESTAMP);
            set.insert(OFFSET);
            set.insert(PARTITION);
            set.insert(EVENT_TIME);
            set.insert(WINDOW_START);
            set.insert(WINDOW_END);
            set
        })
    }

    /// Normalize column name to UPPERCASE if it's a system column
    ///
    /// This should be called ONCE at parse/validation time.
    /// Internally, all system column references use UPPERCASE to avoid repeated allocations.
    ///
    /// # Arguments
    /// * `name` - User-provided column name (any case)
    ///
    /// # Returns
    /// The UPPERCASE system column name if it matches, None otherwise
    #[inline]
    pub fn normalize_if_system_column(name: &str) -> Option<&'static str> {
        let upper = name.to_uppercase();
        get_system_columns_set().get(upper.as_str()).copied()
    }

    /// Check if a name (UPPERCASE) is a system column - O(1) lookup
    ///
    /// Use this for internal checks (after normalization).
    /// All system column names should be in UPPERCASE form.
    ///
    /// # Arguments
    /// * `name_upper` - Column name in UPPERCASE form
    ///
    /// # Returns
    /// True if it's a system column, false otherwise
    #[inline]
    pub fn is_system_column_upper(name_upper: &str) -> bool {
        get_system_columns_set().contains(name_upper)
    }
}

impl FieldValue {
    /// Convert this value to a Prometheus-compatible label string (Phase 3: ToLabelString trait)
    ///
    /// This method provides label string conversion with configurable formatting:
    /// - Float precision: 6 decimal places with trailing zero removal
    /// - ScaledInteger: Converted to decimal with proper precision
    /// - Special characters: Sanitized for Prometheus compatibility
    /// - Length: Truncated to max_length with "..." suffix if needed
    ///
    /// # Arguments
    /// * `default_value` - Value to use for NULL or missing fields
    /// * `max_length` - Maximum length for label values (Prometheus recommended: 1024)
    ///
    /// # Returns
    /// A Prometheus-compatible label string
    pub fn to_label_string(&self, default_value: &str, max_length: usize) -> String {
        let raw_value = match self {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => {
                // Format floats with reasonable precision (avoid scientific notation)
                if f.is_finite() {
                    format!("{:.6}", f)
                        .trim_end_matches('0')
                        .trim_end_matches('.')
                        .to_string()
                } else {
                    default_value.to_string()
                }
            }
            FieldValue::ScaledInteger(value, scale) => {
                // Convert scaled integer to decimal representation
                let divisor = 10_f64.powi(*scale as i32);
                let decimal = (*value as f64) / divisor;
                format!("{:.6}", decimal)
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
            }
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
            FieldValue::Date(d) => d.format("%Y-%m-%d").to_string(),
            FieldValue::Decimal(d) => d.to_string(),
            FieldValue::Interval { value, unit } => format!("{} {:?}", value, unit),
            FieldValue::Null => default_value.to_string(),
            FieldValue::Array(_) => "[array]".to_string(),
            FieldValue::Map(_) => "[map]".to_string(),
            FieldValue::Struct(_) => "[struct]".to_string(),
        };

        // Sanitize and truncate
        let sanitized = raw_value
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect::<String>();

        let trimmed = sanitized.trim();
        if trimmed.len() > max_length {
            let mut truncated = trimmed.chars().take(max_length - 3).collect::<String>();
            truncated.push_str("...");
            truncated
        } else {
            trimmed.to_string()
        }
    }

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
            FieldValue::ScaledInteger(_, _) => "SCALED_INTEGER",
            FieldValue::Array(_) => "ARRAY",
            FieldValue::Map(_) => "MAP",
            FieldValue::Struct(_) => "STRUCT",
            FieldValue::Interval { .. } => "INTERVAL",
        }
    }

    /// Check if this value represents a numeric type
    ///
    /// Returns true for integers, floats, and decimals that can be used
    /// in arithmetic operations.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            FieldValue::Integer(_)
                | FieldValue::Float(_)
                | FieldValue::Decimal(_)
                | FieldValue::ScaledInteger(_, _)
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
            FieldValue::ScaledInteger(value, scale) => {
                let divisor = 10_i64.pow(*scale as u32);
                let integer_part = value / divisor;
                let fractional_part = (value % divisor).abs();
                if fractional_part == 0 {
                    integer_part.to_string()
                } else {
                    format!(
                        "{}.{:0width$}",
                        integer_part,
                        fractional_part,
                        width = *scale as usize
                    )
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
                }
            }
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
            FieldValue::Interval { value, unit } => {
                format!("INTERVAL {} {:?}", value, unit)
            }
        }
    }

    /// Cast this value to the specified target type
    ///
    /// Performs type conversion from this FieldValue to another type following SQL casting rules.
    /// This includes support for widening conversions (e.g., INTEGER -> FLOAT) and string parsing.
    ///
    /// # Arguments
    /// * `target_type` - The target type name (e.g., "INTEGER", "FLOAT", "STRING")
    ///
    /// # Returns
    /// * `Ok(FieldValue)` - The converted value
    /// * `Err(SqlError)` - If the conversion is not supported or fails
    pub fn cast_to(self, target_type: &str) -> Result<FieldValue, SqlError> {
        match target_type {
            "INTEGER" | "INT" => match self {
                FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                FieldValue::Float(f) => Ok(FieldValue::Integer(f as i64)),
                FieldValue::String(s) => s.parse::<i64>().map(FieldValue::Integer).map_err(|_| {
                    SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to INTEGER", s),
                        query: None,
                    }
                }),
                FieldValue::Boolean(b) => Ok(FieldValue::Integer(if b { 1 } else { 0 })),
                FieldValue::Decimal(d) => {
                    // Convert decimal to integer, truncating fractional part
                    let int_part = d.trunc();
                    match int_part.to_string().parse::<i64>() {
                        Ok(i) => Ok(FieldValue::Integer(i)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to INTEGER", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::ScaledInteger(value, scale) => {
                    // Convert scaled integer to regular integer by dividing by scale
                    let divisor = 10_i64.pow(scale as u32);
                    Ok(FieldValue::Integer(value / divisor))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to INTEGER", self.type_name()),
                    query: None,
                }),
            },
            "FLOAT" | "DOUBLE" => match self {
                FieldValue::Integer(i) => Ok(FieldValue::Float(i as f64)),
                FieldValue::Float(f) => Ok(FieldValue::Float(f)),
                FieldValue::String(s) => {
                    s.parse::<f64>()
                        .map(FieldValue::Float)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to FLOAT", s),
                            query: None,
                        })
                }
                FieldValue::Boolean(b) => Ok(FieldValue::Float(if b { 1.0 } else { 0.0 })),
                FieldValue::Decimal(d) => {
                    // Convert decimal to float
                    match d.to_string().parse::<f64>() {
                        Ok(f) => Ok(FieldValue::Float(f)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to FLOAT", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::ScaledInteger(value, scale) => {
                    // Convert scaled integer to float
                    let divisor = 10_i64.pow(scale as u32);
                    Ok(FieldValue::Float(value as f64 / divisor as f64))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to FLOAT", self.type_name()),
                    query: None,
                }),
            },
            "STRING" | "VARCHAR" | "TEXT" => match self {
                FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                FieldValue::String(s) => Ok(FieldValue::String(s)),
                FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                FieldValue::Null => Ok(FieldValue::String("NULL".to_string())),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Decimal(_)
                | FieldValue::ScaledInteger(_, _)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Ok(FieldValue::String(self.to_display_string())),
            },
            "BOOLEAN" | "BOOL" => match self {
                FieldValue::Integer(i) => Ok(FieldValue::Boolean(i != 0)),
                FieldValue::Float(f) => Ok(FieldValue::Boolean(f != 0.0)),
                FieldValue::String(s) => match s.to_uppercase().as_str() {
                    "TRUE" | "T" | "1" => Ok(FieldValue::Boolean(true)),
                    "FALSE" | "F" | "0" => Ok(FieldValue::Boolean(false)),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to BOOLEAN", s),
                        query: None,
                    }),
                },
                FieldValue::Boolean(b) => Ok(FieldValue::Boolean(b)),
                FieldValue::ScaledInteger(value, _) => Ok(FieldValue::Boolean(value != 0)),
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Decimal(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to BOOLEAN", self.type_name()),
                    query: None,
                }),
            },
            "DATE" => match self {
                FieldValue::Date(d) => Ok(FieldValue::Date(d)),
                FieldValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%Y/%m/%d"))
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%m/%d/%Y"))
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%d-%m-%Y"))
                    .map(FieldValue::Date)
                    .map_err(|_| SqlError::ExecutionError {
                        message: format!(
                            "Cannot cast '{}' to DATE. Expected format: YYYY-MM-DD",
                            s
                        ),
                        query: None,
                    }),
                FieldValue::Timestamp(ts) => Ok(FieldValue::Date(ts.date())),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to DATE", self.type_name()),
                    query: None,
                }),
            },
            "TIMESTAMP" | "DATETIME" => match self {
                FieldValue::Timestamp(ts) => Ok(FieldValue::Timestamp(ts)),
                FieldValue::Date(d) => Ok(FieldValue::Timestamp(d.and_hms_opt(0, 0, 0).unwrap())),
                FieldValue::String(s) => {
                    // Try various timestamp formats
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.3f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y/%m/%d %H:%M:%S"))
                        .or_else(|_| {
                            // Try parsing as date only and add time
                            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .map(FieldValue::Timestamp)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to TIMESTAMP. Expected format: YYYY-MM-DD HH:MM:SS", s),
                            query: None,
                        })
                }
                FieldValue::Integer(i) => {
                    // Treat as Unix timestamp (seconds)
                    let dt =
                        DateTime::from_timestamp(i, 0).ok_or_else(|| SqlError::ExecutionError {
                            message: format!("Invalid Unix timestamp: {}", i),
                            query: None,
                        })?;
                    Ok(FieldValue::Timestamp(dt.naive_utc()))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to TIMESTAMP", self.type_name()),
                    query: None,
                }),
            },
            "DECIMAL" | "NUMERIC" => {
                match self {
                    FieldValue::Decimal(d) => Ok(FieldValue::Decimal(d)),
                    FieldValue::Integer(i) => Ok(FieldValue::Decimal(Decimal::from(i))),
                    FieldValue::Float(f) => Decimal::from_str(&f.to_string())
                        .map(FieldValue::Decimal)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast float {} to DECIMAL", f),
                            query: None,
                        }),
                    FieldValue::String(s) => Decimal::from_str(&s)
                        .map(FieldValue::Decimal)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to DECIMAL", s),
                            query: None,
                        }),
                    FieldValue::Boolean(b) => Ok(FieldValue::Decimal(if b {
                        Decimal::ONE
                    } else {
                        Decimal::ZERO
                    })),
                    FieldValue::ScaledInteger(value, scale) => {
                        // Convert scaled integer to decimal by dividing by the scale factor
                        let divisor = 10_i64.pow(scale as u32);
                        let decimal_value = Decimal::from(value) / Decimal::from(divisor);
                        Ok(FieldValue::Decimal(decimal_value))
                    }
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast {} to DECIMAL", self.type_name()),
                        query: None,
                    }),
                }
            }
            // Support casting to SCALED_INTEGER with default *scale of 4 (financial standard)
            "SCALED_INTEGER" => match self {
                FieldValue::ScaledInteger(value, scale) => {
                    Ok(FieldValue::ScaledInteger(value, scale))
                }
                FieldValue::Integer(i) => Ok(FieldValue::ScaledInteger(i * 10000, 4)), // Default to 4 decimal places
                FieldValue::Float(f) => {
                    let scaled_value = (f * 10000.0).round() as i64;
                    Ok(FieldValue::ScaledInteger(scaled_value, 4))
                }
                FieldValue::String(s) => {
                    // Parse as float first, then convert to scaled integer
                    match s.parse::<f64>() {
                        Ok(f) => {
                            let scaled_value = (f * 10000.0).round() as i64;
                            Ok(FieldValue::ScaledInteger(scaled_value, 4))
                        }
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to SCALED_INTEGER", s),
                            query: None,
                        }),
                    }
                }
                FieldValue::Decimal(d) => {
                    // Convert decimal to scaled integer with 4 decimal places
                    let scaled_decimal = d * Decimal::from(10000);
                    match scaled_decimal.to_string().parse::<i64>() {
                        Ok(scaled_value) => Ok(FieldValue::ScaledInteger(scaled_value, 4)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to SCALED_INTEGER", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::Boolean(b) => {
                    Ok(FieldValue::ScaledInteger(if b { 10000 } else { 0 }, 4))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to SCALED_INTEGER", self.type_name()),
                    query: None,
                }),
            },
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported cast target type: {}", target_type),
                query: None,
            }),
        }
    }

    /// Add two FieldValue instances with proper type coercion
    ///
    /// Supports addition between numeric types (Integer, Float) with automatic
    /// type promotion, and interval arithmetic with timestamps.
    /// Returns appropriate SQL error for incompatible types.
    pub fn add(&self, other: &FieldValue) -> Result<FieldValue, SqlError> {
        match (self, other) {
            // Standard numeric addition
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),

            // ScaledInteger arithmetic - exact precision
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                if *scale_a == *scale_b {
                    // Same *scale, can add directly
                    Ok(FieldValue::ScaledInteger(*a + *b, *scale_a))
                } else {
                    // Different *scales, normalize to higher precision
                    let max_scale = (*scale_a).max(*scale_b);
                    let factor_a = 10_i64.pow((max_scale - *scale_a) as u32);
                    let factor_b = 10_i64.pow((max_scale - *scale_b) as u32);
                    Ok(FieldValue::ScaledInteger(
                        *a * factor_a + *b * factor_b,
                        max_scale,
                    ))
                }
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                let scaled_b = *b * 10_i64.pow(*scale as u32);
                Ok(FieldValue::ScaledInteger(*a + scaled_b, *scale))
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                let scaled_a = *a * 10_i64.pow(*scale as u32);
                Ok(FieldValue::ScaledInteger(scaled_a + *b, *scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                let scaled_b = (b * 10_i64.pow(*scale as u32) as f64).round() as i64;
                Ok(FieldValue::ScaledInteger(*a + scaled_b, *scale))
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                let scaled_a = (a * 10_i64.pow(*scale as u32) as f64).round() as i64;
                Ok(FieldValue::ScaledInteger(scaled_a + *b, *scale))
            }

            // Interval + Timestamp arithmetic: timestamp + interval
            (FieldValue::Integer(timestamp), FieldValue::Interval { value, unit }) => {
                let interval_millis = Self::interval_to_millis(*value, unit);
                Ok(FieldValue::Integer(timestamp + interval_millis))
            }

            // Interval + Timestamp arithmetic: interval + timestamp
            (FieldValue::Interval { value, unit }, FieldValue::Integer(timestamp)) => {
                let interval_millis = Self::interval_to_millis(*value, unit);
                Ok(FieldValue::Integer(interval_millis + timestamp))
            }

            // Interval + Interval arithmetic
            (
                FieldValue::Interval {
                    value: v1,
                    unit: u1,
                },
                FieldValue::Interval {
                    value: v2,
                    unit: u2,
                },
            ) => {
                let millis1 = Self::interval_to_millis(*v1, u1);
                let millis2 = Self::interval_to_millis(*v2, u2);
                Ok(FieldValue::Integer(millis1 + millis2))
            }

            // Null handling
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),

            _ => Err(SqlError::TypeError {
                expected: "numeric or interval/timestamp".to_string(),
                actual: "incompatible types".to_string(),
                value: None,
            }),
        }
    }

    /// Subtract two FieldValue instances with proper type coercion
    ///
    /// Supports subtraction between numeric types (Integer, Float) with automatic
    /// type promotion, and interval arithmetic with timestamps.
    /// Returns appropriate SQL error for incompatible types.
    pub fn subtract(&self, other: &FieldValue) -> Result<FieldValue, SqlError> {
        match (self, other) {
            // Standard numeric subtraction
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),

            // ScaledInteger arithmetic - exact precision
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                if *scale_a == *scale_b {
                    // Same *scale, can subtract directly
                    Ok(FieldValue::ScaledInteger(a - b, *scale_a))
                } else {
                    // Different *scales, normalize to higher precision
                    let max_scale = (*scale_a).max(*scale_b);
                    let factor_a = 10_i64.pow((max_scale - *scale_a) as u32);
                    let factor_b = 10_i64.pow((max_scale - *scale_b) as u32);
                    Ok(FieldValue::ScaledInteger(
                        a * factor_a - b * factor_b,
                        max_scale,
                    ))
                }
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                let scaled_b = *b * 10_i64.pow(*scale as u32);
                Ok(FieldValue::ScaledInteger(a - scaled_b, *scale))
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                let scaled_a = *a * 10_i64.pow(*scale as u32);
                Ok(FieldValue::ScaledInteger(scaled_a - b, *scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                let scaled_b = (b * 10_i64.pow(*scale as u32) as f64).round() as i64;
                Ok(FieldValue::ScaledInteger(a - scaled_b, *scale))
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                let scaled_a = (a * 10_i64.pow(*scale as u32) as f64).round() as i64;
                Ok(FieldValue::ScaledInteger(scaled_a - b, *scale))
            }

            // Interval arithmetic: timestamp - interval
            (FieldValue::Integer(timestamp), FieldValue::Interval { value, unit }) => {
                let interval_millis = Self::interval_to_millis(*value, unit);
                Ok(FieldValue::Integer(timestamp - interval_millis))
            }

            // Interval arithmetic: interval - interval
            (
                FieldValue::Interval {
                    value: v1,
                    unit: u1,
                },
                FieldValue::Interval {
                    value: v2,
                    unit: u2,
                },
            ) => {
                let millis1 = Self::interval_to_millis(*v1, u1);
                let millis2 = Self::interval_to_millis(*v2, u2);
                Ok(FieldValue::Integer(millis1 - millis2))
            }

            // Null handling
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),

            _ => Err(SqlError::TypeError {
                expected: "numeric or interval/timestamp".to_string(),
                actual: "incompatible types".to_string(),
                value: None,
            }),
        }
    }

    /// Multiply two FieldValue instances with proper type coercion
    ///
    /// Supports multiplication between numeric types (Integer, Float) with automatic
    /// type promotion. Returns appropriate SQL error for incompatible types.
    pub fn multiply(&self, other: &FieldValue) -> Result<FieldValue, SqlError> {
        match (self, other) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a * b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a * b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 * b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a * *b as f64)),

            // ScaledInteger multiplication - result *scale is sum of *scales
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                let result_scale = *scale_a + *scale_b;
                Ok(FieldValue::ScaledInteger(a * b, result_scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                Ok(FieldValue::ScaledInteger(a * b, *scale))
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                Ok(FieldValue::ScaledInteger(a * b, *scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                let scaled_b = (b * 10_i64.pow(*scale as u32) as f64).round() as i64;
                let result_scale = *scale + *scale; // Double *scale since we scaled b
                Ok(FieldValue::ScaledInteger(a * scaled_b, result_scale))
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                let scaled_a = (a * 10_i64.pow(*scale as u32) as f64).round() as i64;
                let result_scale = *scale + *scale; // Double *scale since we scaled a
                Ok(FieldValue::ScaledInteger(scaled_a * b, result_scale))
            }
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            (FieldValue::Interval { .. }, _) | (_, FieldValue::Interval { .. }) => {
                Err(SqlError::TypeError {
                    expected: "numeric".to_string(),
                    actual: "interval (intervals cannot be multiplied)".to_string(),
                    value: None,
                })
            }
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    /// Divide two FieldValue instances with proper type coercion
    ///
    /// Supports division between numeric types (Integer, Float) with automatic
    /// type promotion. **Handles division by zero defensively**: when a division by
    /// zero is detected, returns `FieldValue::Integer(1)` as a safe default value
    /// instead of throwing an error. This allows record processing to continue
    /// with minimal impact.
    ///
    /// Returns appropriate SQL error for incompatible types.
    pub fn divide(&self, other: &FieldValue) -> Result<FieldValue, SqlError> {
        match (self, other) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    Ok(FieldValue::Float(*a as f64 / *b as f64))
                }
            }
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    Ok(FieldValue::Float(a / b))
                }
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    Ok(FieldValue::Float(*a as f64 / b))
                }
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    Ok(FieldValue::Float(a / *b as f64))
                }
            }

            // ScaledInteger division - preserve precision by scaling numerator
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    // Scale the numerator by the target precision to maintain precision
                    let target_scale = (*scale_a).max(*scale_b);
                    let extra_precision = 4; // Add extra precision for division
                    let scale_factor = 10_i64.pow((target_scale + extra_precision) as u32);
                    let scaled_numerator = a * scale_factor;
                    let result = scaled_numerator / b;
                    // Adjust *scale to account for the extra scaling
                    let result_scale = *scale_a + extra_precision - *scale_b;
                    Ok(FieldValue::ScaledInteger(result, result_scale))
                }
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    Ok(FieldValue::ScaledInteger(a / b, *scale))
                }
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    // Scale the integer numerator to match the denominator's *scale, then add extra precision
                    let extra_precision = 4;
                    let scale_factor = 10_i64.pow((*scale + extra_precision) as u32);
                    let scaled_numerator = a * scale_factor;
                    let result = scaled_numerator / b;
                    Ok(FieldValue::ScaledInteger(result, extra_precision))
                }
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    let scaled_b = (b * 10_i64.pow(*scale as u32) as f64).round() as i64;
                    if scaled_b == 0 {
                        // Defensive: return 1 instead of error to allow record processing to continue
                        Ok(FieldValue::Integer(1))
                    } else {
                        Ok(FieldValue::ScaledInteger(a / scaled_b, *scale))
                    }
                }
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                if *b == 0 {
                    // Defensive: return 1 instead of error to allow record processing to continue
                    Ok(FieldValue::Integer(1))
                } else {
                    let scaled_a = (a * 10_i64.pow(*scale as u32) as f64).round() as i64;
                    Ok(FieldValue::ScaledInteger(scaled_a / b, *scale))
                }
            }
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            (FieldValue::Interval { .. }, _) | (_, FieldValue::Interval { .. }) => {
                Err(SqlError::TypeError {
                    expected: "numeric".to_string(),
                    actual: "interval (intervals cannot be divided)".to_string(),
                    value: None,
                })
            }
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    /// Convert interval to milliseconds - helper for arithmetic operations
    fn interval_to_millis(value: i64, unit: &TimeUnit) -> i64 {
        match unit {
            TimeUnit::Nanosecond => value / 1_000_000,
            TimeUnit::Microsecond => value / 1000,
            TimeUnit::Millisecond => value,
            TimeUnit::Second => value * 1000,
            TimeUnit::Minute => value * 60 * 1000,
            TimeUnit::Hour => value * 60 * 60 * 1000,
            TimeUnit::Day => value * 24 * 60 * 60 * 1000,
            TimeUnit::Week => value * 7 * 24 * 60 * 60 * 1000,
            TimeUnit::Month => value * 30 * 24 * 60 * 60 * 1000, // Approximate: 30 days
            TimeUnit::Year => value * 365 * 24 * 60 * 60 * 1000, // Approximate: 365 days
        }
    }

    /// Create a ScaledInteger from an f64 with specified decimal places
    ///
    /// This is the preferred way to create financial values from floating point numbers.
    /// The *scale parameter specifies how many decimal places to preserve.
    ///
    /// # Arguments
    /// * `value` - The floating point value to convert
    /// * `*scale` - Number of decimal places (typically 4 for financial applications)
    ///
    /// # Returns
    /// A ScaledInteger FieldValue with the specified precision
    ///
    /// # Examples
    /// ```
    /// use velostream::velostream::sql::execution::types::FieldValue;
    ///
    /// // Create a financial value for $123.45 with 4 decimal places
    /// let price = FieldValue::from_financial_f64(123.45, 4);
    /// // This stores 1234500 internally with scale=4
    /// ```
    pub fn from_financial_f64(value: f64, scale: u8) -> FieldValue {
        let scale_factor = 10_i64.pow(scale as u32);
        let scaled_value = (value * scale_factor as f64).round() as i64;
        FieldValue::ScaledInteger(scaled_value, scale)
    }

    /// Convert a ScaledInteger back to f64
    ///
    /// This method converts a scaled integer back to a floating point representation
    /// for compatibility with existing systems that expect f64 values.
    ///
    /// # Returns
    /// * `Some(f64)` - The converted floating point value
    /// * `None` - If this FieldValue is not a ScaledInteger
    pub fn to_financial_f64(&self) -> Option<f64> {
        match self {
            FieldValue::ScaledInteger(value, scale) => {
                let divisor = 10_i64.pow(*scale as u32);
                Some(*value as f64 / divisor as f64)
            }
            _ => None,
        }
    }

    /// Check if this value is a financial type (ScaledInteger)
    ///
    /// Returns true if this is a ScaledInteger that provides exact financial arithmetic.
    pub fn is_financial(&self) -> bool {
        matches!(self, FieldValue::ScaledInteger(_, _))
    }
}

/// A record in a streaming data source
///
/// This structure represents a single record from a streaming data source like Kafka.
/// It contains the actual field data plus metadata about the record's position and
/// timing within the stream.
#[derive(Debug, Clone, Default)]
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
    /// Event-time timestamp for watermark-based processing (optional)
    /// When None, processing-time (timestamp field) is used
    /// When Some, this timestamp is used for event-time windowing and watermarks
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl StreamRecord {}

impl StreamRecord {
    /// Create a new StreamRecord with the given fields
    ///
    /// This constructor creates a record with the specified field data and
    /// default values for metadata fields (timestamp=0, offset=0, partition=0, no headers).
    /// Event-time is set to None (uses processing-time).
    pub fn new(fields: HashMap<String, FieldValue>) -> Self {
        Self {
            fields,
            timestamp: 0,
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
            event_time: None, // Default to processing-time
        }
    }

    /// Create a new StreamRecord with fields and metadata
    ///
    /// This constructor allows setting all record metadata along with the field data.
    /// Event-time is set to None (uses processing-time).
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
            event_time: None, // Default to processing-time
        }
    }

    /// Create a StreamRecord with event-time for watermark-based processing
    ///
    /// This constructor allows specifying an event-time timestamp that will be used
    /// for event-time windowing and watermark generation instead of processing-time.
    pub fn with_event_time(
        fields: HashMap<String, FieldValue>,
        timestamp: i64,
        offset: i64,
        partition: i32,
        headers: HashMap<String, String>,
        event_time: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            fields,
            timestamp,
            offset,
            partition,
            headers,
            event_time: Some(event_time),
        }
    }

    /// Set the event-time for this record (fluent API)
    ///
    /// This method allows setting the event-time after record creation.
    /// When event-time is set, it will be used for watermark-based processing.
    pub fn with_event_time_fluent(mut self, event_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.event_time = Some(event_time);
        self
    }

    /// Get the effective timestamp for time-based processing
    ///
    /// Returns the event-time if set, otherwise falls back to processing-time.
    /// This is the timestamp that should be used for windowing and temporal operations.
    pub fn get_event_time(&self) -> chrono::DateTime<chrono::Utc> {
        match self.event_time {
            Some(event_time) => event_time,
            None => {
                // Convert processing-time timestamp to DateTime
                // Assume timestamp is milliseconds since epoch
                chrono::DateTime::from_timestamp(
                    self.timestamp / 1000,
                    ((self.timestamp % 1000) * 1_000_000) as u32,
                )
                .unwrap_or_else(chrono::Utc::now)
            }
        }
    }

    /// Check if this record has explicit event-time set
    pub fn has_event_time(&self) -> bool {
        self.event_time.is_some()
    }

    /// Set partition based on a hash of the provided key (fluent API)
    ///
    /// This method assigns a partition number to the record based on a consistent hash
    /// of the provided key string. Records with the same key will always be assigned
    /// to the same partition, enabling proper grouping for sticky partition strategies.
    ///
    /// This is useful for test data generation to simulate realistic Kafka partition
    /// distribution based on message keys.
    ///
    /// # Arguments
    /// * `key` - The partition key (e.g., symbol, customer_id, trader_id)
    /// * `max_partitions` - Maximum partition number (typically 32 for testing)
    ///
    /// # Returns
    /// Self with partition field set based on hash(key) % max_partitions
    pub fn with_partition_from_key(mut self, key: &str, max_partitions: i32) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.partition = (hasher.finish() % max_partitions as u64) as i32;
        self
    }

    /// Extract event-time from a field if present
    ///
    /// This method attempts to extract event-time from a specific field in the record.
    /// Useful for parsing event-time from record data (e.g., "_timestamp" field).
    ///
    /// # Arguments
    /// * `field_name` - Name of the field containing the timestamp
    ///
    /// # Returns
    /// * `Some(DateTime)` if the field exists and can be parsed as a timestamp
    /// * `None` if the field doesn't exist or can't be parsed
    pub fn extract_event_time_from_field(
        &mut self,
        field_name: &str,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        match self.fields.get(field_name) {
            Some(FieldValue::Integer(timestamp_ms)) => {
                // Convert milliseconds to DateTime
                let datetime = chrono::DateTime::from_timestamp(
                    *timestamp_ms / 1000,
                    ((*timestamp_ms % 1000) * 1_000_000) as u32,
                );
                if let Some(dt) = datetime {
                    self.event_time = Some(dt);
                    Some(dt)
                } else {
                    None
                }
            }
            Some(FieldValue::Timestamp(naive_dt)) => {
                // Convert NaiveDateTime to UTC DateTime
                let dt = chrono::DateTime::from_naive_utc_and_offset(*naive_dt, chrono::Utc);
                self.event_time = Some(dt);
                Some(dt)
            }
            Some(FieldValue::String(timestamp_str)) => {
                // Try to parse string as ISO 8601 timestamp
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
                    let utc_dt = dt.with_timezone(&chrono::Utc);
                    self.event_time = Some(utc_dt);
                    Some(utc_dt)
                } else {
                    None
                }
            }
            _ => None,
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

    /// Check if the record contains a specific key
    ///
    /// Returns true if the specified key exists in the record's fields.
    pub fn contains_key(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Check if the debug representation of this record contains a substring
    ///
    /// This is useful for testing to verify if certain field values are present.
    /// The check is performed on the debug representation of all fields.
    pub fn contains(&self, pattern: &str) -> bool {
        format!("{:?}", self.fields).contains(pattern)
    }
}

impl std::fmt::Display for StreamRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.fields)
    }
}
