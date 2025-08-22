//! Arithmetic operations for SQL expressions.
//!
//! This module implements arithmetic operations (add, subtract, multiply, divide)
//! with proper type coercion and SQL semantics.

use super::super::types::FieldValue;
use crate::ferris::sql::ast::TimeUnit;
use crate::ferris::sql::error::SqlError;

/// Provides arithmetic operations for SQL expressions
pub struct ArithmeticOperations;

impl ArithmeticOperations {
    /// Convert interval to milliseconds
    fn interval_to_millis(value: i64, unit: &TimeUnit) -> i64 {
        match unit {
            TimeUnit::Millisecond => value,
            TimeUnit::Second => value * 1000,
            TimeUnit::Minute => value * 60 * 1000,
            TimeUnit::Hour => value * 60 * 60 * 1000,
            TimeUnit::Day => value * 24 * 60 * 60 * 1000,
        }
    }
    /// Add two FieldValue instances with proper type coercion
    ///
    /// Supports addition between numeric types (Integer, Float) with automatic
    /// type promotion, and interval arithmetic with timestamps.
    /// Returns appropriate SQL error for incompatible types.
    pub fn add_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            // Standard numeric addition
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),

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
    pub fn subtract_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            // Standard numeric subtraction
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),

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
    pub fn multiply_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a * b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a * b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 * b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a * *b as f64)),
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
    pub fn divide_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
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
}
