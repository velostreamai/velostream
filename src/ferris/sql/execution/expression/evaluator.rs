//! Expression evaluator for SQL expressions.
//!
//! This module implements the core expression evaluation logic that processes
//! SQL expressions against streaming data records.

use super::super::processors::ProcessorContext;
use super::super::types::{FieldValue, StreamRecord};
use super::functions::BuiltinFunctions;
use super::subquery_executor::{evaluate_subquery_with_executor, SubqueryExecutor};
use crate::ferris::sql::ast::{BinaryOperator, Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;
use rust_decimal::Decimal;
use std::str::FromStr;

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
                                let column_name = name.split('.').next_back().unwrap_or(name);
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
                    LiteralValue::Decimal(s) => {
                        // Parse decimal string to Decimal type
                        match Decimal::from_str(s) {
                            Ok(d) => FieldValue::Decimal(d),
                            Err(_) => {
                                return Err(SqlError::ExecutionError {
                                    message: format!("Invalid DECIMAL literal: {}", s),
                                    query: None,
                                });
                            }
                        }
                    }
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
                            Expr::Subquery { .. } => {
                                Err(SqlError::ExecutionError {
                                    message: "IN subqueries are not yet implemented. Please use EXISTS instead.".to_string(),
                                    query: None,
                                })
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
                            Expr::Subquery { .. } => {
                                Err(SqlError::ExecutionError {
                                    message: "NOT IN subqueries are not yet implemented. Please use NOT EXISTS instead.".to_string(),
                                    query: None,
                                })
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
                    SubqueryType::Exists => Err(SqlError::ExecutionError {
                        message: "EXISTS subqueries are not yet implemented.".to_string(),
                        query: None,
                    }),
                    SubqueryType::NotExists => Err(SqlError::ExecutionError {
                        message: "NOT EXISTS subqueries are not yet implemented.".to_string(),
                        query: None,
                    }),
                    SubqueryType::Scalar => Err(SqlError::ExecutionError {
                        message: "Scalar subqueries are not yet implemented.".to_string(),
                        query: None,
                    }),
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
                    UnaryOperator::IsNull => {
                        // For IS NULL operator, evaluate inner expression and check if it's null
                        let inner_result = Self::evaluate_expression_value(inner_expr, record)?;
                        Ok(matches!(inner_result, FieldValue::Null))
                    }
                    UnaryOperator::IsNotNull => {
                        // For IS NOT NULL operator, evaluate inner expression and check if it's not null
                        let inner_result = Self::evaluate_expression_value(inner_expr, record)?;
                        Ok(!matches!(inner_result, FieldValue::Null))
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
                                let column_name = name.split('.').next_back().unwrap_or(name);
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
                LiteralValue::Decimal(s) => {
                    // Parse decimal string to Decimal type
                    Decimal::from_str(s).map(FieldValue::Decimal).map_err(|_| {
                        SqlError::ExecutionError {
                            message: format!("Invalid DECIMAL literal: {}", s),
                            query: None,
                        }
                    })
                }
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
                    BinaryOperator::Add => left_val.add(&right_val),
                    BinaryOperator::Subtract => left_val.subtract(&right_val),
                    BinaryOperator::Multiply => left_val.multiply(&right_val),
                    BinaryOperator::Divide => left_val.divide(&right_val),

                    // String concatenation operator
                    BinaryOperator::Concat => match (&left_val, &right_val) {
                        (FieldValue::String(s1), FieldValue::String(s2)) => {
                            Ok(FieldValue::String(format!("{}{}", s1, s2)))
                        }
                        (FieldValue::String(s), other) | (other, FieldValue::String(s)) => {
                            // Convert non-string to string representation and concatenate
                            let other_str = match other {
                                FieldValue::String(s) => s.clone(),
                                FieldValue::Integer(i) => i.to_string(),
                                FieldValue::Float(f) => f.to_string(),
                                FieldValue::Boolean(b) => b.to_string(),
                                FieldValue::ScaledInteger(value, scale) => {
                                    // Format scaled integer as decimal
                                    let divisor = 10_i64.pow(*scale as u32);
                                    if divisor == 1 {
                                        value.to_string()
                                    } else {
                                        format!(
                                            "{:.prec$}",
                                            *value as f64 / divisor as f64,
                                            prec = *scale as usize
                                        )
                                    }
                                }
                                FieldValue::Timestamp(ts) => ts.to_string(),
                                FieldValue::Date(d) => d.to_string(),
                                FieldValue::Decimal(d) => d.to_string(),
                                FieldValue::Array(arr) => format!("{:?}", arr),
                                FieldValue::Map(map) => format!("{:?}", map),
                                FieldValue::Struct(s) => format!("{:?}", s),
                                FieldValue::Interval { value, unit } => {
                                    format!("{} {:?}", value, unit)
                                }
                                FieldValue::Null => "".to_string(), // SQL standard: concat with NULL gives NULL, but we'll use empty string
                            };
                            if left_val == *other {
                                Ok(FieldValue::String(format!("{}{}", other_str, s)))
                            } else {
                                Ok(FieldValue::String(format!("{}{}", s, other_str)))
                            }
                        }
                        (FieldValue::Null, _) | (_, FieldValue::Null) => {
                            // SQL standard: concatenation with NULL returns NULL
                            Ok(FieldValue::Null)
                        }
                        (left, right) => {
                            // Convert both non-string values to strings and concatenate
                            let left_str = match left {
                                FieldValue::String(s) => s.clone(),
                                FieldValue::Integer(i) => i.to_string(),
                                FieldValue::Float(f) => f.to_string(),
                                FieldValue::Boolean(b) => b.to_string(),
                                FieldValue::ScaledInteger(value, scale) => {
                                    let divisor = 10_i64.pow(*scale as u32);
                                    if divisor == 1 {
                                        value.to_string()
                                    } else {
                                        format!(
                                            "{:.prec$}",
                                            *value as f64 / divisor as f64,
                                            prec = *scale as usize
                                        )
                                    }
                                }
                                FieldValue::Timestamp(ts) => ts.to_string(),
                                FieldValue::Date(d) => d.to_string(),
                                FieldValue::Decimal(d) => d.to_string(),
                                FieldValue::Array(arr) => format!("{:?}", arr),
                                FieldValue::Map(map) => format!("{:?}", map),
                                FieldValue::Struct(s) => format!("{:?}", s),
                                FieldValue::Interval { value, unit } => {
                                    format!("{} {:?}", value, unit)
                                }
                                FieldValue::Null => "".to_string(),
                            };
                            let right_str = match right {
                                FieldValue::String(s) => s.clone(),
                                FieldValue::Integer(i) => i.to_string(),
                                FieldValue::Float(f) => f.to_string(),
                                FieldValue::Boolean(b) => b.to_string(),
                                FieldValue::ScaledInteger(value, scale) => {
                                    let divisor = 10_i64.pow(*scale as u32);
                                    if divisor == 1 {
                                        value.to_string()
                                    } else {
                                        format!(
                                            "{:.prec$}",
                                            *value as f64 / divisor as f64,
                                            prec = *scale as usize
                                        )
                                    }
                                }
                                FieldValue::Timestamp(ts) => ts.to_string(),
                                FieldValue::Date(d) => d.to_string(),
                                FieldValue::Decimal(d) => d.to_string(),
                                FieldValue::Array(arr) => format!("{:?}", arr),
                                FieldValue::Map(map) => format!("{:?}", map),
                                FieldValue::Struct(s) => format!("{:?}", s),
                                FieldValue::Interval { value, unit } => {
                                    format!("{} {:?}", value, unit)
                                }
                                FieldValue::Null => "".to_string(),
                            };
                            Ok(FieldValue::String(format!("{}{}", left_str, right_str)))
                        }
                    },

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
                            Expr::Subquery { .. } => {
                                Err(SqlError::ExecutionError {
                                    message: "IN subqueries are not yet implemented. Please use EXISTS instead.".to_string(),
                                    query: None,
                                })
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
                            Expr::Subquery { .. } => {
                                Err(SqlError::ExecutionError {
                                    message: "NOT IN subqueries are not yet implemented. Please use NOT EXISTS instead.".to_string(),
                                    query: None,
                                })
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
                        Err(SqlError::ExecutionError {
                            message: "Scalar subqueries are not yet implemented.".to_string(),
                            query: None,
                        })
                    }
                    SubqueryType::Exists => {
                        Err(SqlError::ExecutionError {
                            message: "EXISTS subqueries are not yet implemented.".to_string(),
                            query: None,
                        })
                    }
                    SubqueryType::NotExists => {
                        Err(SqlError::ExecutionError {
                            message: "NOT EXISTS subqueries are not yet implemented.".to_string(),
                            query: None,
                        })
                    }
                    SubqueryType::In | SubqueryType::NotIn => {
                        Err(SqlError::ExecutionError {
                            message: "IN/NOT IN subqueries are not yet implemented. Please use EXISTS/NOT EXISTS instead.".to_string(),
                            query: None,
                        })
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
                    UnaryOperator::IsNull => {
                        // For IS NULL operator, evaluate inner expression and return boolean
                        let inner_result = Self::evaluate_expression_value(inner_expr, record)?;
                        Ok(FieldValue::Boolean(matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    UnaryOperator::IsNotNull => {
                        // For IS NOT NULL operator, evaluate inner expression and return boolean
                        let inner_result = Self::evaluate_expression_value(inner_expr, record)?;
                        Ok(FieldValue::Boolean(!matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Unsupported unary operator in value context: {:?}", op),
                        query: None,
                    }),
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                // Evaluate each WHEN condition until one is true
                for (condition, result) in when_clauses {
                    let condition_value = Self::evaluate_expression(condition, record)?;
                    if condition_value {
                        return Self::evaluate_expression_value(result, record);
                    }
                }

                // If no WHEN condition was true, evaluate ELSE clause or return NULL
                if let Some(else_expr) = else_clause {
                    Self::evaluate_expression_value(else_expr, record)
                } else {
                    Ok(FieldValue::Null)
                }
            }
            Expr::WindowFunction {
                function_name,
                args,
                over_clause,
            } => {
                // Delegate to window function evaluator
                // For now, use empty window buffer - this should be populated by WindowProcessor
                let empty_buffer: Vec<crate::ferris::sql::execution::StreamRecord> = Vec::new();
                super::WindowFunctions::evaluate_window_function(
                    function_name,
                    args,
                    over_clause,
                    record,
                    &empty_buffer,
                )
            }
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
                            .is_some_and(|val_b| Self::values_equal(val_a, val_b))
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
                            .is_some_and(|val_b| Self::values_equal(val_a, val_b))
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

    /// Evaluates a boolean expression with subquery support
    ///
    /// This is the enhanced version of evaluate_expression that can handle subqueries
    /// by delegating their execution to the provided SubqueryExecutor.
    pub fn evaluate_expression_with_subqueries<T: SubqueryExecutor>(
        expr: &Expr,
        record: &StreamRecord,
        subquery_executor: &T,
        context: &ProcessorContext,
    ) -> Result<bool, SqlError> {
        match expr {
            Expr::Subquery {
                query,
                subquery_type,
            } => {
                // Handle subqueries in boolean context
                let result = evaluate_subquery_with_executor(
                    subquery_executor,
                    query,
                    subquery_type,
                    record,
                    context,
                    None,
                )?;
                Self::field_value_to_bool(&result)
            }
            _ => {
                // For non-subquery expressions, delegate to enhanced value evaluator
                let value = Self::evaluate_expression_value_with_subqueries(
                    expr,
                    record,
                    subquery_executor,
                    context,
                )?;
                Self::field_value_to_bool(&value)
            }
        }
    }

    /// Evaluates an expression value with subquery support
    ///
    /// This is the enhanced version of evaluate_expression_value that can handle subqueries
    /// by delegating their execution to the provided SubqueryExecutor.
    pub fn evaluate_expression_value_with_subqueries<T: SubqueryExecutor>(
        expr: &Expr,
        record: &StreamRecord,
        subquery_executor: &T,
        context: &ProcessorContext,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Subquery {
                query,
                subquery_type,
            } => {
                // Handle subqueries based on their type
                evaluate_subquery_with_executor(
                    subquery_executor,
                    query,
                    subquery_type,
                    record,
                    context,
                    None,
                )
            }
            Expr::BinaryOp { left, op, right } => {
                use crate::ferris::sql::ast::BinaryOperator;

                // Handle IN/NOT IN operations that might involve subqueries
                match op {
                    BinaryOperator::In => {
                        let left_val = Self::evaluate_expression_value_with_subqueries(
                            left,
                            record,
                            subquery_executor,
                            context,
                        )?;

                        // SQL semantics: NULL IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(FieldValue::Boolean(false));
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value = Self::evaluate_expression_value_with_subqueries(
                                        value_expr,
                                        record,
                                        subquery_executor,
                                        context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(true));
                                    }
                                }
                                Ok(FieldValue::Boolean(false))
                            }
                            Expr::Subquery { query, .. } => {
                                let in_result = subquery_executor
                                    .execute_in_subquery(&left_val, query, record, context)?;
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
                        let left_val = Self::evaluate_expression_value_with_subqueries(
                            left,
                            record,
                            subquery_executor,
                            context,
                        )?;

                        // SQL semantics: NULL NOT IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(FieldValue::Boolean(false));
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value = Self::evaluate_expression_value_with_subqueries(
                                        value_expr,
                                        record,
                                        subquery_executor,
                                        context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(false));
                                    }
                                }
                                Ok(FieldValue::Boolean(true))
                            }
                            Expr::Subquery { query, .. } => {
                                let in_result = subquery_executor
                                    .execute_in_subquery(&left_val, query, record, context)?;
                                Ok(FieldValue::Boolean(!in_result))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message:
                                    "NOT IN operator requires a list or subquery on the right side"
                                        .to_string(),
                                query: None,
                            }),
                        }
                    }
                    _ => {
                        // For other binary operations, recursively evaluate with subquery support
                        let left_val = Self::evaluate_expression_value_with_subqueries(
                            left,
                            record,
                            subquery_executor,
                            context,
                        )?;
                        let right_val = Self::evaluate_expression_value_with_subqueries(
                            right,
                            record,
                            subquery_executor,
                            context,
                        )?;

                        // Use existing binary operation logic
                        match op {
                            BinaryOperator::Equal => Ok(FieldValue::Boolean(Self::values_equal(
                                &left_val, &right_val,
                            ))),
                            BinaryOperator::NotEqual => Ok(FieldValue::Boolean(
                                !Self::values_equal(&left_val, &right_val),
                            )),
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
                            BinaryOperator::And => Ok(FieldValue::Boolean(
                                Self::field_value_to_bool(&left_val)?
                                    && Self::field_value_to_bool(&right_val)?,
                            )),
                            BinaryOperator::Or => Ok(FieldValue::Boolean(
                                Self::field_value_to_bool(&left_val)?
                                    || Self::field_value_to_bool(&right_val)?,
                            )),
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
                            BinaryOperator::Add => left_val.add(&right_val),
                            BinaryOperator::Subtract => left_val.subtract(&right_val),
                            BinaryOperator::Multiply => left_val.multiply(&right_val),
                            BinaryOperator::Divide => left_val.divide(&right_val),
                            BinaryOperator::Concat => match (&left_val, &right_val) {
                                (FieldValue::String(s1), FieldValue::String(s2)) => {
                                    Ok(FieldValue::String(format!("{}{}", s1, s2)))
                                }
                                (FieldValue::String(s), other) | (other, FieldValue::String(s)) => {
                                    // Convert non-string to string and concatenate
                                    let other_str = match other {
                                        FieldValue::String(s) => s.clone(),
                                        FieldValue::Integer(i) => i.to_string(),
                                        FieldValue::Float(f) => f.to_string(),
                                        FieldValue::Boolean(b) => b.to_string(),
                                        FieldValue::ScaledInteger(value, scale) => {
                                            let divisor = 10_i64.pow(*scale as u32);
                                            if divisor == 1 {
                                                value.to_string()
                                            } else {
                                                format!(
                                                    "{:.prec$}",
                                                    *value as f64 / divisor as f64,
                                                    prec = *scale as usize
                                                )
                                            }
                                        }
                                        FieldValue::Timestamp(ts) => ts.to_string(),
                                        FieldValue::Date(d) => d.to_string(),
                                        FieldValue::Decimal(d) => d.to_string(),
                                        FieldValue::Array(arr) => format!("{:?}", arr),
                                        FieldValue::Map(map) => format!("{:?}", map),
                                        FieldValue::Struct(s) => format!("{:?}", s),
                                        FieldValue::Interval { value, unit } => {
                                            format!("{} {:?}", value, unit)
                                        }
                                        FieldValue::Null => return Ok(FieldValue::Null),
                                    };
                                    if left_val == *other {
                                        Ok(FieldValue::String(format!("{}{}", other_str, s)))
                                    } else {
                                        Ok(FieldValue::String(format!("{}{}", s, other_str)))
                                    }
                                }
                                (FieldValue::Null, _) | (_, FieldValue::Null) => {
                                    Ok(FieldValue::Null)
                                }
                                (left, right) => {
                                    // Convert both to strings and concatenate
                                    let left_str = match left {
                                        FieldValue::String(s) => s.clone(),
                                        FieldValue::Integer(i) => i.to_string(),
                                        FieldValue::Float(f) => f.to_string(),
                                        FieldValue::Boolean(b) => b.to_string(),
                                        FieldValue::ScaledInteger(value, scale) => {
                                            let divisor = 10_i64.pow(*scale as u32);
                                            if divisor == 1 {
                                                value.to_string()
                                            } else {
                                                format!(
                                                    "{:.prec$}",
                                                    *value as f64 / divisor as f64,
                                                    prec = *scale as usize
                                                )
                                            }
                                        }
                                        FieldValue::Timestamp(ts) => ts.to_string(),
                                        FieldValue::Date(d) => d.to_string(),
                                        FieldValue::Decimal(d) => d.to_string(),
                                        FieldValue::Array(arr) => format!("{:?}", arr),
                                        FieldValue::Map(map) => format!("{:?}", map),
                                        FieldValue::Struct(fields) => format!("{:?}", fields),
                                        FieldValue::Interval { value, unit } => {
                                            format!("{} {:?}", value, unit)
                                        }
                                        FieldValue::Null => "".to_string(),
                                    };
                                    let right_str = match right {
                                        FieldValue::String(s) => s.clone(),
                                        FieldValue::Integer(i) => i.to_string(),
                                        FieldValue::Float(f) => f.to_string(),
                                        FieldValue::Boolean(b) => b.to_string(),
                                        FieldValue::ScaledInteger(value, scale) => {
                                            let divisor = 10_i64.pow(*scale as u32);
                                            if divisor == 1 {
                                                value.to_string()
                                            } else {
                                                format!(
                                                    "{:.prec$}",
                                                    *value as f64 / divisor as f64,
                                                    prec = *scale as usize
                                                )
                                            }
                                        }
                                        FieldValue::Timestamp(ts) => ts.to_string(),
                                        FieldValue::Date(d) => d.to_string(),
                                        FieldValue::Decimal(d) => d.to_string(),
                                        FieldValue::Array(arr) => format!("{:?}", arr),
                                        FieldValue::Map(map) => format!("{:?}", map),
                                        FieldValue::Struct(fields) => format!("{:?}", fields),
                                        FieldValue::Interval { value, unit } => {
                                            format!("{} {:?}", value, unit)
                                        }
                                        FieldValue::Null => "".to_string(),
                                    };
                                    Ok(FieldValue::String(format!("{}{}", left_str, right_str)))
                                }
                            },
                            _ => Err(SqlError::ExecutionError {
                                message: format!(
                                    "Binary operator {:?} not supported in value context",
                                    op
                                ),
                                query: None,
                            }),
                        }
                    }
                }
            }
            _ => {
                // For all other expressions, delegate to the original method
                // since they don't involve subqueries
                Self::evaluate_expression_value(expr, record)
            }
        }
    }
}
