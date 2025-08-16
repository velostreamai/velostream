use super::{add_values, divide_values, multiply_values, subtract_values};
use crate::ferris::sql::ast::{BinaryOperator, Expr, LiteralValue, TimeUnit, UnaryOperator};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::{FieldValue, HeaderMutation, StreamRecord};

/// Core expression evaluation logic
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate expression as a boolean value for filtering
    pub fn evaluate_expression(expr: &Expr, record: &StreamRecord) -> Result<bool, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                let field_value = match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => FieldValue::Integer(record.timestamp),
                    "_OFFSET" => FieldValue::Integer(record.offset),
                    "_PARTITION" => FieldValue::Integer(record.partition as i64),
                    _ => {
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
                                    // Fall back to just the column name
                                    record
                                        .fields
                                        .get(column_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null)
                                }
                            }
                        } else {
                            // Regular field lookup
                            record.fields.get(name).cloned().unwrap_or(FieldValue::Null)
                        }
                    }
                };

                // Convert field value to boolean
                match field_value {
                    FieldValue::Boolean(b) => Ok(b),
                    FieldValue::Integer(i) => Ok(i != 0),
                    FieldValue::Float(f) => Ok(f != 0.0),
                    FieldValue::String(s) => Ok(!s.is_empty()),
                    FieldValue::Null => Ok(false),
                    _ => Ok(true), // Non-null values are generally truthy
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Boolean(b) => Ok(*b),
                LiteralValue::Integer(i) => Ok(*i != 0),
                LiteralValue::Float(f) => Ok(*f != 0.0),
                LiteralValue::String(s) => Ok(!s.is_empty()),
                LiteralValue::Null => Ok(false),
                LiteralValue::Interval { .. } => Ok(true), // Intervals are truthy
            },
            Expr::BinaryOp { left, op, right } => {
                Self::evaluate_binary_comparison(left, right, record, op)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let val = Self::evaluate_expression(expr, record)?;
                    Ok(!val)
                }
                UnaryOperator::IsNull => {
                    let val = Self::evaluate_expression_value(expr, record)?;
                    Ok(matches!(val, FieldValue::Null))
                }
                UnaryOperator::IsNotNull => {
                    let val = Self::evaluate_expression_value(expr, record)?;
                    Ok(!matches!(val, FieldValue::Null))
                }
                _ => Err(SqlError::ExecutionError {
                    message: format!("Unsupported unary operator: {:?}", op),
                    query: None,
                }),
            },
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    if Self::evaluate_expression(condition, record)? {
                        return Self::evaluate_expression(result, record);
                    }
                }
                if let Some(else_result) = else_clause {
                    Self::evaluate_expression(else_result, record)
                } else {
                    Ok(false)
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!(
                    "Expression type {} is not supported in boolean context",
                    Self::expression_type_name(expr)
                ),
                query: None,
            }),
        }
    }

    /// Evaluate expression as a field value
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
                                    // Fall back to just the column name
                                    Ok(record
                                        .fields
                                        .get(column_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null))
                                }
                            }
                        } else {
                            // Regular field lookup
                            Ok(record.fields.get(name).cloned().unwrap_or(FieldValue::Null))
                        }
                    }
                }
            }
            Expr::Literal(lit) => Self::evaluate_literal(lit),
            Expr::BinaryOp { left, op, right } => {
                Self::evaluate_binary_operation(left, op, right, record)
            }
            Expr::UnaryOp { op: _, expr: _ } => Err(SqlError::ExecutionError {
                message: "Unary operations not supported for value expressions".to_string(),
                query: None,
            }),
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    if Self::evaluate_expression(condition, record)? {
                        return Self::evaluate_expression_value(result, record);
                    }
                }
                if let Some(else_result) = else_clause {
                    Self::evaluate_expression_value(else_result, record)
                } else {
                    Ok(FieldValue::Null)
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!(
                    "Expression type {} is not supported in value context",
                    Self::expression_type_name(expr)
                ),
                query: None,
            }),
        }
    }

    fn evaluate_literal(lit: &LiteralValue) -> Result<FieldValue, SqlError> {
        match lit {
            LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
            LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
            LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
            LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
            LiteralValue::Null => Ok(FieldValue::Null),
            LiteralValue::Interval { value, unit } => {
                // Convert INTERVAL to milliseconds for internal representation
                let millis = match unit {
                    TimeUnit::Millisecond => *value,
                    TimeUnit::Second => *value * 1000,
                    TimeUnit::Minute => *value * 60 * 1000,
                    TimeUnit::Hour => *value * 60 * 60 * 1000,
                    TimeUnit::Day => *value * 24 * 60 * 60 * 1000,
                };
                Ok(FieldValue::Integer(millis))
            }
        }
    }

    fn evaluate_binary_operation(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        let left_val = Self::evaluate_expression_value(left, record)?;
        let right_val = Self::evaluate_expression_value(right, record)?;

        match op {
            BinaryOperator::Add => add_values(&left_val, &right_val),
            BinaryOperator::Subtract => subtract_values(&left_val, &right_val),
            BinaryOperator::Multiply => multiply_values(&left_val, &right_val),
            BinaryOperator::Divide => divide_values(&left_val, &right_val),
            BinaryOperator::Modulo => divide_values(&left_val, &right_val), // TODO: Implement proper modulo
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::LessThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::LessThanOrEqual => {
                let result = Self::evaluate_binary_comparison(left, right, record, op)?;
                Ok(FieldValue::Boolean(result))
            }
            BinaryOperator::Like | BinaryOperator::NotLike => {
                let result = Self::evaluate_binary_comparison(left, right, record, op)?;
                Ok(FieldValue::Boolean(result))
            }
            BinaryOperator::And | BinaryOperator::Or => Err(SqlError::ExecutionError {
                message: "Operator not supported in value context".to_string(),
                query: None,
            }),
            BinaryOperator::In | BinaryOperator::NotIn => {
                let result = Self::evaluate_binary_comparison(left, right, record, op)?;
                Ok(FieldValue::Boolean(result))
            }
        }
    }

    fn evaluate_binary_comparison(
        left: &Expr,
        right: &Expr,
        record: &StreamRecord,
        op: &BinaryOperator,
    ) -> Result<bool, SqlError> {
        // This is a simplified version - full implementation would be much more complex
        let left_val = Self::evaluate_expression_value(left, record)?;
        let right_val = Self::evaluate_expression_value(right, record)?;

        match op {
            BinaryOperator::Equal => Ok(Self::values_equal(&left_val, &right_val)),
            BinaryOperator::NotEqual => Ok(!Self::values_equal(&left_val, &right_val)),
            _ => {
                // For other comparison operators, we need numeric comparison logic
                // This is simplified - full implementation would handle all comparison types
                Ok(false)
            }
        }
    }

    fn values_equal(a: &FieldValue, b: &FieldValue) -> bool {
        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a == b,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Null, FieldValue::Null) => true,
            _ => false,
        }
    }

    fn expression_type_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(_) => "column",
            Expr::Literal(_) => "literal",
            Expr::BinaryOp { .. } => "binary_operation",
            Expr::UnaryOp { .. } => "unary_operation",
            Expr::Function { .. } => "function",
            Expr::Case { .. } => "case",
            _ => "expression",
        }
        .to_string()
    }
}
