//! Core streaming SQL data types.
//!
//! This module contains the fundamental data types used throughout the streaming SQL engine:
//! - [`FieldValue`] - The value type system supporting SQL data types
//! - [`StreamRecord`] - The record format for streaming data processing

use crate::ferris::sql::ast::TimeUnit;
use crate::ferris::sql::error::SqlError;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::collections::HashMap;
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
    /// type promotion. Handles division by zero appropriately.
    /// Returns appropriate SQL error for incompatible types.
    pub fn divide(&self, other: &FieldValue) -> Result<FieldValue, SqlError> {
        match (self, other) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / *b as f64))
                }
            }
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(a / b))
                }
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / b))
                }
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(a / *b as f64))
                }
            }

            // ScaledInteger division - preserve precision by scaling numerator
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
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
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::ScaledInteger(a / b, *scale))
                }
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
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
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    let scaled_b = (b * 10_i64.pow(*scale as u32) as f64).round() as i64;
                    if scaled_b == 0 {
                        Err(SqlError::ExecutionError {
                            message: "Division by zero".to_string(),
                            query: None,
                        })
                    } else {
                        Ok(FieldValue::ScaledInteger(a / scaled_b, *scale))
                    }
                }
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
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
            TimeUnit::Millisecond => value,
            TimeUnit::Second => value * 1000,
            TimeUnit::Minute => value * 60 * 1000,
            TimeUnit::Hour => value * 60 * 60 * 1000,
            TimeUnit::Day => value * 24 * 60 * 60 * 1000,
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
    /// use ferrisstreams::ferris::sql::execution::types::FieldValue;
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
