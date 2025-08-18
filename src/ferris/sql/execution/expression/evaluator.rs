//! Expression evaluator for SQL expressions.
//!
//! This module implements the core expression evaluation logic that processes
//! SQL expressions against streaming data records.

use crate::ferris::sql::ast::{BinaryOperator, Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;
use super::super::types::{FieldValue, StreamRecord};
use super::arithmetic::ArithmeticOperations;
use super::functions::BuiltinFunctions;

/// Main expression evaluator that handles all SQL expression types
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluates a boolean expression against a record
    ///
    /// Used primarily for WHERE clause evaluation. Returns true/false based on
    /// the expression result, with proper SQL NULL handling.
    pub fn evaluate_expression(expr: &Expr, record: &StreamRecord) -> Result<bool, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                let field_value = match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => FieldValue::Integer(record.timestamp),
                    "_OFFSET" => FieldValue::Integer(record.offset),
                    "_PARTITION" => FieldValue::Integer(record.partition as i64),
                    _ => {
                        // Handle qualified column names (table.column)
                        if name.contains('.') {
                            // Try to find the field with the qualified name first (for JOIN aliases)
                            if let Some(value) = record.fields.get(name) {
                                value.clone()
                            } else {
                                let column_name = name.split('.').last().unwrap_or(name);
                                // Try to find with the "right_" prefix (for non-aliased JOINs)
                                let prefixed_name = format!("right_{}", column_name);
                                if let Some(value) = record.fields.get(&prefixed_name) {
                                    value.clone()
                                } else {
                                    // Fall back to just the column name (for FROM clause aliases like l.name -> name)
                                    record.fields.get(column_name).cloned().ok_or_else(|| {
                                        SqlError::SchemaError {
                                            message: format!(
                                                "Schema error for column '{}': Column not found",
                                                name
                                            ),
                                            column: Some(name.clone()),
                                        }
                                    })?
                                }
                            }
                        } else {
                            // Regular field lookup
                            record.fields.get(name).cloned().ok_or_else(|| {
                                SqlError::SchemaError {
                                    message: format!(
                                        "Schema error for column '{}': Column not found",
                                        name
                                    ),
                                    column: Some(name.clone()),
                                }
                            })?
                        }
                    }
                };

                // Convert to boolean
                Self::field_value_to_bool(&field_value)
            }
            Expr::Literal(literal) => {
                let field_value = match literal {
                    LiteralValue::String(s) => FieldValue::String(s.clone()),
                    LiteralValue::Integer(i) => FieldValue::Integer(*i),
                    LiteralValue::Float(f) => FieldValue::Float(*f),
                    LiteralValue::Boolean(b) => FieldValue::Boolean(*b),
                    LiteralValue::Null => FieldValue::Null,
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: format!("Unsupported literal type: {:?}", literal),
                            query: None,
                        });
                    }
                };
                Self::field_value_to_bool(&field_value)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expression_value(left, record)?;
                let right_val = Self::evaluate_expression_value(right, record)?;

                match op {
                    BinaryOperator::Equal => Ok(Self::values_equal(&left_val, &right_val)),
                    BinaryOperator::NotEqual => Ok(!Self::values_equal(&left_val, &right_val)),
                    BinaryOperator::LessThan => Self::compare_values(&left_val, &right_val, |cmp| cmp < 0),
                    BinaryOperator::LessThanOrEqual => Self::compare_values(&left_val, &right_val, |cmp| cmp <= 0),
                    BinaryOperator::GreaterThan => Self::compare_values(&left_val, &right_val, |cmp| cmp > 0),
                    BinaryOperator::GreaterThanOrEqual => Self::compare_values(&left_val, &right_val, |cmp| cmp >= 0),
                    BinaryOperator::And => {
                        Ok(Self::field_value_to_bool(&left_val)? && Self::field_value_to_bool(&right_val)?)
                    }
                    BinaryOperator::Or => {
                        Ok(Self::field_value_to_bool(&left_val)? || Self::field_value_to_bool(&right_val)?)
                    }
                    BinaryOperator::Like => {
                        match (&left_val, &right_val) {
                            (FieldValue::String(text), FieldValue::String(pattern)) => {
                                Ok(Self::match_pattern(text, pattern))
                            }
                            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(false),
                            _ => Err(SqlError::TypeError {
                                expected: "string".to_string(),
                                actual: "non-string".to_string(),
                                value: None,
                            }),
                        }
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported binary operator: {:?}", op),
                        query: None,
                    }),
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported expression for boolean evaluation: {:?}", expr),
                query: None,
            }),
        }
    }

    /// Evaluates an expression to get its value (not just boolean)
    ///
    /// Used for SELECT clause evaluation, arithmetic operations, and function calls.
    /// Returns the actual value of the expression.
    pub fn evaluate_expression_value(expr: &Expr, record: &StreamRecord) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => Ok(FieldValue::Integer(record.timestamp)),
                    "_OFFSET" => Ok(FieldValue::Integer(record.offset)),
                    "_PARTITION" => Ok(FieldValue::Integer(record.partition as i64)),
                    _ => {
                        // Handle qualified column names (table.column)
                        if name.contains('.') {
                            // Try to find the field with the qualified name first (for JOIN aliases)
                            if let Some(value) = record.fields.get(name) {
                                Ok(value.clone())
                            } else {
                                let column_name = name.split('.').last().unwrap_or(name);
                                // Try to find with the "right_" prefix (for non-aliased JOINs)
                                let prefixed_name = format!("right_{}", column_name);
                                if let Some(value) = record.fields.get(&prefixed_name) {
                                    Ok(value.clone())
                                } else {
                                    // Fall back to just the column name (for FROM clause aliases like l.name -> name)
                                    record.fields.get(column_name).cloned().ok_or_else(|| {
                                        SqlError::SchemaError {
                                            message: format!(
                                                "Schema error for column '{}': Column not found",
                                                name
                                            ),
                                            column: Some(name.clone()),
                                        }
                                    })
                                }
                            }
                        } else {
                            // Regular field lookup
                            record.fields.get(name).cloned().ok_or_else(|| {
                                SqlError::SchemaError {
                                    message: format!(
                                        "Schema error for column '{}': Column not found",
                                        name
                                    ),
                                    column: Some(name.clone()),
                                }
                            })
                        }
                    }
                }
            }
            Expr::Literal(literal) => {
                match literal {
                    LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
                    LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
                    LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                    LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
                    LiteralValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported literal type: {:?}", literal),
                        query: None,
                    }),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expression_value(left, record)?;
                let right_val = Self::evaluate_expression_value(right, record)?;

                match op {
                    BinaryOperator::Add => ArithmeticOperations::add_values(&left_val, &right_val),
                    BinaryOperator::Subtract => ArithmeticOperations::subtract_values(&left_val, &right_val),
                    BinaryOperator::Multiply => ArithmeticOperations::multiply_values(&left_val, &right_val),
                    BinaryOperator::Divide => ArithmeticOperations::divide_values(&left_val, &right_val),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Binary operator {:?} not supported in value context", op),
                        query: None,
                    }),
                }
            }
            Expr::Function { .. } => {
                BuiltinFunctions::evaluate_function(expr, record)
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported expression type: {:?}", expr),
                query: None,
            }),
        }
    }

    // Helper methods for comparison and conversion
    
    fn field_value_to_bool(value: &FieldValue) -> Result<bool, SqlError> {
        match value {
            FieldValue::Boolean(b) => Ok(*b),
            FieldValue::Integer(i) => Ok(*i != 0),
            FieldValue::Float(f) => Ok(*f != 0.0),
            FieldValue::String(s) => Ok(!s.is_empty()),
            FieldValue::Null => Ok(false),
            _ => Err(SqlError::TypeError {
                expected: "boolean".to_string(),
                actual: "unsupported".to_string(),
                value: None,
            }),
        }
    }

    fn values_equal(left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Null, FieldValue::Null) => true,
            (FieldValue::Null, _) | (_, FieldValue::Null) => false,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (FieldValue::Float(a), FieldValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
            _ => false,
        }
    }

    fn compare_values<F>(left: &FieldValue, right: &FieldValue, op: F) -> Result<bool, SqlError>
    where
        F: Fn(i32) -> bool,
    {
        let cmp = match (left, right) {
            (FieldValue::Null, _) | (_, FieldValue::Null) => return Ok(false),
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a.cmp(b) as i32,
            (FieldValue::Float(a), FieldValue::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32,
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32,
            (FieldValue::Float(a), FieldValue::Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal) as i32,
            (FieldValue::String(a), FieldValue::String(b)) => a.cmp(b) as i32,
            _ => {
                return Err(SqlError::TypeError {
                    expected: "comparable types".to_string(),
                    actual: "incompatible types".to_string(),
                    value: None,
                });
            }
        };
        Ok(op(cmp))
    }

    fn match_pattern(text: &str, pattern: &str) -> bool {
        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        
        match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(re) => re.is_match(text),
            Err(_) => false,
        }
    }
}