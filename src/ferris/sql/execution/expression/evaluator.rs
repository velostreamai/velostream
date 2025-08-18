//! Expression evaluator for SQL expressions.
//!
//! This module implements the core expression evaluation logic that processes
//! SQL expressions against streaming data records.

use super::super::types::{FieldValue, StreamRecord};
use super::arithmetic::ArithmeticOperations;
use super::functions::BuiltinFunctions;
use crate::ferris::sql::ast::{BinaryOperator, Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;

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
                                    // Return NULL if not found instead of error
                                    record
                                        .fields
                                        .get(column_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null)
                                }
                            }
                        } else {
                            // Regular field lookup - return NULL if not found instead of error
                            record.fields.get(name).cloned().unwrap_or(FieldValue::Null)
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
                    LiteralValue::Interval { value, unit } => FieldValue::Interval {
                        value: *value,
                        unit: unit.clone(),
                    },
                };
                Self::field_value_to_bool(&field_value)
            }
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Handle IN/NOT IN operators specially - they need the List structure preserved
                    BinaryOperator::In => {
                        let left_val = Self::evaluate_expression_value(left, record)?;

                        // SQL semantics: NULL IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(false);
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(true);
                                    }
                                }
                                Ok(false)
                            }
                            Expr::Subquery {
                                query: _,
                                subquery_type: _,
                            } => {
                                // For subqueries, use a mock implementation similar to the engine
                                // In production, this would execute the actual subquery
                                match &left_val {
                                    FieldValue::Integer(n) => Ok(*n > 0), // Mock: positive numbers are "in" the subquery
                                    FieldValue::String(s) => Ok(!s.is_empty()), // Mock: non-empty strings are "in" the subquery
                                    FieldValue::Boolean(b) => Ok(*b), // Mock: true values are "in" the subquery
                                    FieldValue::Null => Ok(false), // NULL is never "in" a subquery
                                    _ => Ok(false), // Other types are not "in" the subquery by default
                                }
                            }
                            _ => Err(SqlError::ExecutionError {
                                message:
                                    "IN operator requires a list or subquery on the right side"
                                        .to_string(),
                                query: None,
                            }),
                        }
                    }
                    BinaryOperator::NotIn => {
                        let left_val = Self::evaluate_expression_value(left, record)?;

                        // SQL semantics: NULL NOT IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(false);
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(false);
                                    }
                                }
                                Ok(true)
                            }
                            Expr::Subquery {
                                query: _,
                                subquery_type: _,
                            } => {
                                // For subqueries, use a mock implementation similar to the engine (inverted for NOT IN)
                                // In production, this would execute the actual subquery
                                let in_result = match &left_val {
                                    FieldValue::Integer(n) => *n > 0, // Mock: positive numbers are "in" the subquery
                                    FieldValue::String(s) => !s.is_empty(), // Mock: non-empty strings are "in" the subquery
                                    FieldValue::Boolean(b) => *b, // Mock: true values are "in" the subquery
                                    FieldValue::Null => false,    // NULL is never "in" a subquery
                                    _ => false, // Other types are not "in" the subquery by default
                                };
                                Ok(!in_result) // NOT IN is the inverse of IN
                            }
                            _ => Err(SqlError::ExecutionError {
                                message:
                                    "NOT IN operator requires a list or subquery on the right side"
                                        .to_string(),
                                query: None,
                            }),
                        }
                    }
                    // For all other operators, evaluate both sides as values
                    _ => {
                        let left_val = Self::evaluate_expression_value(left, record)?;
                        let right_val = Self::evaluate_expression_value(right, record)?;

                        match op {
                            BinaryOperator::Equal => Ok(Self::values_equal(&left_val, &right_val)),
                            BinaryOperator::NotEqual => {
                                Ok(!Self::values_equal(&left_val, &right_val))
                            }
                            BinaryOperator::LessThan => {
                                Self::compare_values(&left_val, &right_val, |cmp| cmp < 0)
                            }
                            BinaryOperator::LessThanOrEqual => {
                                Self::compare_values(&left_val, &right_val, |cmp| cmp <= 0)
                            }
                            BinaryOperator::GreaterThan => {
                                Self::compare_values(&left_val, &right_val, |cmp| cmp > 0)
                            }
                            BinaryOperator::GreaterThanOrEqual => {
                                Self::compare_values(&left_val, &right_val, |cmp| cmp >= 0)
                            }
                            BinaryOperator::And => Ok(Self::field_value_to_bool(&left_val)?
                                && Self::field_value_to_bool(&right_val)?),
                            BinaryOperator::Or => Ok(Self::field_value_to_bool(&left_val)?
                                || Self::field_value_to_bool(&right_val)?),
                            BinaryOperator::Like => match (&left_val, &right_val) {
                                (FieldValue::String(text), FieldValue::String(pattern)) => {
                                    Ok(Self::match_pattern(text, pattern))
                                }
                                (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(false),
                                _ => Err(SqlError::TypeError {
                                    expected: "string".to_string(),
                                    actual: "non-string".to_string(),
                                    value: None,
                                }),
                            },
                            BinaryOperator::NotLike => match (&left_val, &right_val) {
                                (FieldValue::String(text), FieldValue::String(pattern)) => {
                                    Ok(!Self::match_pattern(text, pattern))
                                }
                                (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(false),
                                _ => Err(SqlError::TypeError {
                                    expected: "string".to_string(),
                                    actual: "non-string".to_string(),
                                    value: None,
                                }),
                            },
                            // IN/NOT IN are handled above
                            BinaryOperator::In | BinaryOperator::NotIn => unreachable!(),
                            _ => Err(SqlError::ExecutionError {
                                message: format!("Unsupported binary operator: {:?}", op),
                                query: None,
                            }),
                        }
                    }
                }
            }
            Expr::Subquery {
                query: _,
                subquery_type,
            } => {
                // Handle subqueries in boolean context
                use crate::ferris::sql::ast::SubqueryType;
                match subquery_type {
                    SubqueryType::Exists => {
                        // For EXISTS subqueries, return a mock boolean (true)
                        // In production, this would execute the subquery and return true if any rows exist
                        Ok(true)
                    }
                    SubqueryType::NotExists => {
                        // For NOT EXISTS subqueries, return a mock boolean (false)
                        // In production, this would execute the subquery and return true if NO rows exist
                        // Since our mock EXISTS returns true, NOT EXISTS should return false
                        Ok(false)
                    }
                    SubqueryType::Scalar => {
                        // For scalar subqueries in boolean context, evaluate the result as boolean
                        // Mock implementation returns 1, which converts to true
                        Ok(true)
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!(
                            "Unsupported subquery type in boolean context: {:?}",
                            subquery_type
                        ),
                        query: None,
                    }),
                }
            }
            Expr::UnaryOp {
                op,
                expr: inner_expr,
            } => {
                use crate::ferris::sql::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        // For NOT operator, evaluate inner expression and invert result
                        let inner_result = Self::evaluate_expression(inner_expr, record)?;
                        Ok(!inner_result)
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported unary operator in boolean context: {:?}", op),
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
    pub fn evaluate_expression_value(
        expr: &Expr,
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
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
                                    // Return NULL if not found instead of error
                                    Ok(record
                                        .fields
                                        .get(column_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null))
                                }
                            }
                        } else {
                            // Regular field lookup - return NULL if not found instead of error
                            Ok(record.fields.get(name).cloned().unwrap_or(FieldValue::Null))
                        }
                    }
                }
            }
            Expr::Literal(literal) => match literal {
                LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
                LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
                LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
                LiteralValue::Null => Ok(FieldValue::Null),
                LiteralValue::Interval { value, unit } => Ok(FieldValue::Interval {
                    value: *value,
                    unit: unit.clone(),
                }),
            },
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expression_value(left, record)?;
                let right_val = Self::evaluate_expression_value(right, record)?;

                match op {
                    // Arithmetic operators
                    BinaryOperator::Add => ArithmeticOperations::add_values(&left_val, &right_val),
                    BinaryOperator::Subtract => {
                        ArithmeticOperations::subtract_values(&left_val, &right_val)
                    }
                    BinaryOperator::Multiply => {
                        ArithmeticOperations::multiply_values(&left_val, &right_val)
                    }
                    BinaryOperator::Divide => {
                        ArithmeticOperations::divide_values(&left_val, &right_val)
                    }

                    // Comparison operators - return boolean values
                    BinaryOperator::Equal => Ok(FieldValue::Boolean(Self::values_equal(
                        &left_val, &right_val,
                    ))),
                    BinaryOperator::NotEqual => Ok(FieldValue::Boolean(!Self::values_equal(
                        &left_val, &right_val,
                    ))),
                    BinaryOperator::LessThan => {
                        Self::compare_values(&left_val, &right_val, |cmp| cmp < 0)
                            .map(FieldValue::Boolean)
                    }
                    BinaryOperator::LessThanOrEqual => {
                        Self::compare_values(&left_val, &right_val, |cmp| cmp <= 0)
                            .map(FieldValue::Boolean)
                    }
                    BinaryOperator::GreaterThan => {
                        Self::compare_values(&left_val, &right_val, |cmp| cmp > 0)
                            .map(FieldValue::Boolean)
                    }
                    BinaryOperator::GreaterThanOrEqual => {
                        Self::compare_values(&left_val, &right_val, |cmp| cmp >= 0)
                            .map(FieldValue::Boolean)
                    }

                    // Logical operators
                    BinaryOperator::And => Ok(FieldValue::Boolean(
                        Self::field_value_to_bool(&left_val)?
                            && Self::field_value_to_bool(&right_val)?,
                    )),
                    BinaryOperator::Or => Ok(FieldValue::Boolean(
                        Self::field_value_to_bool(&left_val)?
                            || Self::field_value_to_bool(&right_val)?,
                    )),

                    // String pattern matching
                    BinaryOperator::Like => match (&left_val, &right_val) {
                        (FieldValue::String(text), FieldValue::String(pattern)) => {
                            Ok(FieldValue::Boolean(Self::match_pattern(text, pattern)))
                        }
                        (FieldValue::Null, _) | (_, FieldValue::Null) => {
                            Ok(FieldValue::Boolean(false))
                        }
                        _ => Err(SqlError::TypeError {
                            expected: "string".to_string(),
                            actual: "non-string".to_string(),
                            value: None,
                        }),
                    },
                    BinaryOperator::NotLike => match (&left_val, &right_val) {
                        (FieldValue::String(text), FieldValue::String(pattern)) => {
                            Ok(FieldValue::Boolean(!Self::match_pattern(text, pattern)))
                        }
                        (FieldValue::Null, _) | (_, FieldValue::Null) => {
                            Ok(FieldValue::Boolean(false))
                        }
                        _ => Err(SqlError::TypeError {
                            expected: "string".to_string(),
                            actual: "non-string".to_string(),
                            value: None,
                        }),
                    },

                    // Set membership operators
                    BinaryOperator::In => {
                        // For IN operator, right side should be a list or subquery
                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(true));
                                    }
                                }
                                Ok(FieldValue::Boolean(false))
                            }
                            Expr::Subquery {
                                query: _,
                                subquery_type: _,
                            } => {
                                // For subqueries, use a mock implementation similar to the engine
                                let in_result = match &left_val {
                                    FieldValue::Integer(n) => *n > 0, // Mock: positive numbers are "in" the subquery
                                    FieldValue::String(s) => !s.is_empty(), // Mock: non-empty strings are "in" the subquery
                                    FieldValue::Boolean(b) => *b, // Mock: true values are "in" the subquery
                                    FieldValue::Null => false,    // NULL is never "in" a subquery
                                    _ => false, // Other types are not "in" the subquery by default
                                };
                                Ok(FieldValue::Boolean(in_result))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message:
                                    "IN operator requires a list or subquery on the right side"
                                        .to_string(),
                                query: None,
                            }),
                        }
                    }
                    BinaryOperator::NotIn => {
                        // For NOT IN operator, right side should be a list or subquery
                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(false));
                                    }
                                }
                                Ok(FieldValue::Boolean(true))
                            }
                            Expr::Subquery {
                                query: _,
                                subquery_type: _,
                            } => {
                                // For subqueries, use a mock implementation similar to the engine (inverted for NOT IN)
                                let in_result = match &left_val {
                                    FieldValue::Integer(n) => *n > 0, // Mock: positive numbers are "in" the subquery
                                    FieldValue::String(s) => !s.is_empty(), // Mock: non-empty strings are "in" the subquery
                                    FieldValue::Boolean(b) => *b, // Mock: true values are "in" the subquery
                                    FieldValue::Null => false,    // NULL is never "in" a subquery
                                    _ => false, // Other types are not "in" the subquery by default
                                };
                                Ok(FieldValue::Boolean(!in_result)) // NOT IN is the inverse of IN
                            }
                            _ => Err(SqlError::ExecutionError {
                                message:
                                    "NOT IN operator requires a list or subquery on the right side"
                                        .to_string(),
                                query: None,
                            }),
                        }
                    }

                    _ => Err(SqlError::ExecutionError {
                        message: format!("Binary operator {:?} not supported in value context", op),
                        query: None,
                    }),
                }
            }
            Expr::Function { .. } => BuiltinFunctions::evaluate_function(expr, record),
            Expr::Subquery {
                query: _,
                subquery_type,
            } => {
                // Handle subqueries based on their type
                use crate::ferris::sql::ast::SubqueryType;
                match subquery_type {
                    SubqueryType::Scalar => {
                        // For scalar subqueries, return a mock value (1)
                        // In production, this would execute the subquery and return the scalar result
                        Ok(FieldValue::Integer(1))
                    }
                    SubqueryType::Exists => {
                        // For EXISTS subqueries, return a mock boolean (true)
                        // In production, this would execute the subquery and return true if any rows exist
                        Ok(FieldValue::Boolean(true))
                    }
                    SubqueryType::NotExists => {
                        // For NOT EXISTS subqueries, return a mock boolean (false)
                        // In production, this would execute the subquery and return true if NO rows exist
                        // Since our mock EXISTS returns true, NOT EXISTS should return false
                        Ok(FieldValue::Boolean(false))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported subquery type: {:?}", subquery_type),
                        query: None,
                    }),
                }
            }
            Expr::List(_) => {
                // List expressions are handled differently based on context
                // For now, return an error for standalone list evaluation
                Err(SqlError::ExecutionError {
                    message: "List expressions must be used in IN/NOT IN operations".to_string(),
                    query: None,
                })
            }
            Expr::UnaryOp {
                op,
                expr: inner_expr,
            } => {
                use crate::ferris::sql::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        // For NOT operator, evaluate inner expression and invert result
                        let inner_result = Self::evaluate_expression_value(inner_expr, record)?;
                        match inner_result {
                            FieldValue::Boolean(b) => Ok(FieldValue::Boolean(!b)),
                            _ => {
                                // Convert to boolean first, then invert
                                let bool_val = Self::field_value_to_bool(&inner_result)?;
                                Ok(FieldValue::Boolean(!bool_val))
                            }
                        }
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported unary operator in value context: {:?}", op),
                        query: None,
                    }),
                }
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
            (FieldValue::Array(a), FieldValue::Array(b)) => {
                // Arrays are equal if they have the same length and all elements are equal
                if a.len() != b.len() {
                    false
                } else {
                    a.iter()
                        .zip(b.iter())
                        .all(|(elem_a, elem_b)| Self::values_equal(elem_a, elem_b))
                }
            }
            (FieldValue::Map(a), FieldValue::Map(b)) => {
                // Maps are equal if they have the same keys and all values are equal
                if a.len() != b.len() {
                    false
                } else {
                    a.iter().all(|(key, val_a)| {
                        b.get(key)
                            .map_or(false, |val_b| Self::values_equal(val_a, val_b))
                    })
                }
            }
            (FieldValue::Struct(a), FieldValue::Struct(b)) => {
                // Structs are equal if they have the same fields and all values are equal
                if a.len() != b.len() {
                    false
                } else {
                    a.iter().all(|(field, val_a)| {
                        b.get(field)
                            .map_or(false, |val_b| Self::values_equal(val_a, val_b))
                    })
                }
            }
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
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)
                as i32,
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                a.partial_cmp(&(*b as f64))
                    .unwrap_or(std::cmp::Ordering::Equal) as i32
            }
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
        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");

        match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(re) => re.is_match(text),
            Err(_) => false,
        }
    }
}
