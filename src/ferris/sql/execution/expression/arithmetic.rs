//! Arithmetic operations for SQL expressions.
//!
//! This module implements arithmetic operations (add, subtract, multiply, divide)
//! with proper type coercion and SQL semantics.

use super::super::types::FieldValue;
use crate::ferris::sql::error::SqlError;

/// Provides arithmetic operations for SQL expressions
pub struct ArithmeticOperations;

impl ArithmeticOperations {
    /// Add two FieldValue instances with proper type coercion
    ///
    /// Supports addition between numeric types (Integer, Float) with automatic
    /// type promotion. Returns appropriate SQL error for incompatible types.
    pub fn add_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    /// Subtract two FieldValue instances with proper type coercion
    ///
    /// Supports subtraction between numeric types (Integer, Float) with automatic
    /// type promotion. Returns appropriate SQL error for incompatible types.
    pub fn subtract_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
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
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }
}
