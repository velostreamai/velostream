//! Expression evaluator for SQL expressions.
//!
//! This module implements the core expression evaluation logic that processes
//! SQL expressions against streaming data records.

use super::super::processors::ProcessorContext;
use std::sync::atomic::{AtomicBool, Ordering};

use super::super::types::{FieldValue, StreamRecord, system_columns};
use super::functions::BuiltinFunctions;
use super::subquery_executor::{SubqueryExecutor, evaluate_subquery_with_executor};
use crate::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue};
use crate::velostream::sql::error::SqlError;

/// Type alias for WHERE clause predicate function
type WhereClausePredicate = Box<dyn Fn(&String, &FieldValue) -> bool>;

/// Helper functions for converting decimal string literals to ScaledInteger
mod scaled_integer_helper {
    use super::*;

    /// Convert a decimal string literal like "123.45" to ScaledInteger(12345, 2)
    /// Provides 42x faster performance than f64 with exact precision
    pub fn parse_decimal_to_scaled_integer(s: &str) -> Result<FieldValue, SqlError> {
        if let Some(dot_pos) = s.find('.') {
            let integer_part = &s[..dot_pos];
            let decimal_part = &s[dot_pos + 1..];
            let scale = decimal_part.len() as u8;

            // Combine parts: "123.45" -> "12345"
            let combined = format!("{}{}", integer_part, decimal_part);
            match combined.parse::<i64>() {
                Ok(value) => Ok(FieldValue::ScaledInteger(value, scale)),
                Err(_) => Err(SqlError::ExecutionError {
                    message: format!("Invalid DECIMAL literal: {}", s),
                    query: None,
                }),
            }
        } else {
            // No decimal point - treat as integer with scale 0
            match s.parse::<i64>() {
                Ok(value) => Ok(FieldValue::ScaledInteger(value, 0)),
                Err(_) => Err(SqlError::ExecutionError {
                    message: format!("Invalid DECIMAL literal: {}", s),
                    query: None,
                }),
            }
        }
    }
}

/// Maintains intermediate alias values during SELECT clause processing.
///
/// This context is used to track aliases that have been computed during SELECT
/// field evaluation, allowing later fields in the same SELECT to reference them.
/// This enables queries like:
/// ```sql
/// SELECT
///     x + 1 AS computed_value,
///     computed_value * 2 AS result  -- Can reference the alias
/// ```
#[derive(Debug, Clone)]
pub struct SelectAliasContext {
    /// Map of alias name → computed FieldValue for current record
    pub aliases: std::collections::HashMap<String, FieldValue>,
}

impl SelectAliasContext {
    /// Creates a new empty alias context
    pub fn new() -> Self {
        Self {
            aliases: std::collections::HashMap::new(),
        }
    }

    /// Adds an alias with its computed value to the context
    pub fn add_alias(&mut self, name: String, value: FieldValue) {
        self.aliases.insert(name, value);
    }

    /// Retrieves an alias value if it exists
    pub fn get_alias(&self, name: &str) -> Option<&FieldValue> {
        self.aliases.get(name)
    }
}

impl Default for SelectAliasContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Main expression evaluator that handles all SQL expression types
pub struct ExpressionEvaluator;

impl Default for ExpressionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionEvaluator {
    /// Create a new expression evaluator (for backward compatibility)
    pub fn new() -> Self {
        ExpressionEvaluator
    }

    /// Get the _EVENT_TIME value for a record
    ///
    /// Returns the event time as milliseconds since epoch. When event_time is not set,
    /// behavior is controlled by the `VELOSTREAM_EVENT_TIME_FALLBACK` env var:
    /// - `processing_time` (default): silently fall back to _TIMESTAMP
    /// - `warn`: fall back to _TIMESTAMP but log a warning
    /// - `null`: return FieldValue::Null
    #[inline]
    pub fn get_event_time_value(record: &StreamRecord) -> FieldValue {
        static WARNED: AtomicBool = AtomicBool::new(false);

        match record.event_time {
            Some(event_time) => FieldValue::Integer(event_time.timestamp_millis()),
            None => match system_columns::event_time_fallback() {
                system_columns::EventTimeFallback::Null => FieldValue::Null,
                system_columns::EventTimeFallback::Warn => {
                    if !WARNED.swap(true, Ordering::Relaxed) {
                        log::warn!(
                            "_EVENT_TIME accessed but record.event_time is None; \
                             falling back to _TIMESTAMP. This warning is logged once."
                        );
                    }
                    FieldValue::Integer(record.timestamp)
                }
                system_columns::EventTimeFallback::ProcessingTime => {
                    FieldValue::Integer(record.timestamp)
                }
            },
        }
    }

    /// Parse a WHERE clause and return a predicate function
    ///
    /// This is a simplified version for testing that returns a closure
    /// that evaluates the WHERE clause against key-value pairs.
    pub fn parse_where_clause(&self, where_clause: &str) -> Result<WhereClausePredicate, SqlError> {
        // This is a simplified implementation for testing
        // In a real implementation, you'd parse the WHERE clause into an AST
        let _clause = where_clause.to_string();

        Ok(Box::new(
            move |_key: &String, _value: &FieldValue| -> bool {
                // Simple default implementation - always return true for now
                // This should be replaced with actual WHERE clause evaluation
                true
            },
        ))
    }

    /// Evaluates a boolean expression against a record
    ///
    /// Used primarily for WHERE clause evaluation. Returns true/false based on
    /// the expression result, with proper SQL NULL handling.
    pub fn evaluate_expression(expr: &Expr, record: &StreamRecord) -> Result<bool, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (normalized to UPPERCASE)
                let field_value = if let Some(sys_col) =
                    system_columns::normalize_if_system_column(name)
                {
                    match sys_col {
                        system_columns::TIMESTAMP => FieldValue::Integer(record.timestamp),
                        system_columns::OFFSET => FieldValue::Integer(record.offset),
                        system_columns::PARTITION => FieldValue::Integer(record.partition as i64),
                        system_columns::EVENT_TIME => Self::get_event_time_value(record),
                        system_columns::WINDOW_START | system_columns::WINDOW_END => {
                            // Window columns are injected into fields HashMap by WindowProcessor
                            // Look them up as regular fields
                            record
                                .fields
                                .get(sys_col)
                                .cloned()
                                .unwrap_or(FieldValue::Null)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    // Handle qualified column names (table.column)
                    if name.contains('.') {
                        // Try to find the field with the qualified name first (for JOIN aliases)
                        if let Some(value) = record.fields.get(name) {
                            value.clone()
                        } else {
                            let column_name = name.split('.').next_back().unwrap_or(name);
                            // Check if the unqualified column is a system column
                            if let Some(sys_col) =
                                system_columns::normalize_if_system_column(column_name)
                            {
                                match sys_col {
                                    system_columns::TIMESTAMP => {
                                        FieldValue::Integer(record.timestamp)
                                    }
                                    system_columns::OFFSET => FieldValue::Integer(record.offset),
                                    system_columns::PARTITION => {
                                        FieldValue::Integer(record.partition as i64)
                                    }
                                    system_columns::EVENT_TIME => {
                                        Self::get_event_time_value(record)
                                    }
                                    system_columns::WINDOW_START | system_columns::WINDOW_END => {
                                        record
                                            .fields
                                            .get(sys_col)
                                            .cloned()
                                            .unwrap_or(FieldValue::Null)
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
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
                        }
                    } else {
                        // Regular field lookup - return NULL if not found instead of error
                        record.fields.get(name).cloned().unwrap_or(FieldValue::Null)
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
                        // Convert decimal literals to ScaledInteger for 42x faster performance
                        scaled_integer_helper::parse_decimal_to_scaled_integer(s)?
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
                                // Subqueries MUST be evaluated using evaluate_expression_with_subqueries()
                                unreachable!(
                                    "IN subqueries cannot be evaluated with basic evaluator. \
                                    Use evaluate_expression_with_subqueries() with SubqueryExecutor instead."
                                )
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
                                // Subqueries MUST be evaluated using evaluate_expression_with_subqueries()
                                unreachable!(
                                    "NOT IN subqueries cannot be evaluated with basic evaluator. \
                                    Use evaluate_expression_with_subqueries() with SubqueryExecutor instead."
                                )
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
            Expr::Subquery { .. } => {
                // Subqueries MUST be evaluated using evaluate_expression_with_subqueries()
                // which routes them to the appropriate SubqueryExecutor implementation.
                // This basic evaluator cannot handle subqueries.
                unreachable!(
                    "Subqueries cannot be evaluated with basic evaluator. \
                    Use evaluate_expression_with_subqueries() with SubqueryExecutor instead."
                )
            }
            Expr::UnaryOp {
                op,
                expr: inner_expr,
            } => {
                use crate::velostream::sql::ast::UnaryOperator;
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
            Expr::Between { .. } => {
                // BETWEEN expressions always evaluate to boolean
                let result = Self::evaluate_expression_value(expr, record)?;
                match result {
                    FieldValue::Boolean(b) => Ok(b),
                    _ => Err(SqlError::ExecutionError {
                        message: "BETWEEN expression should return boolean".to_string(),
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
                // Check for system columns first (normalized to UPPERCASE)
                if let Some(sys_col) = system_columns::normalize_if_system_column(name) {
                    Ok(match sys_col {
                        system_columns::TIMESTAMP => FieldValue::Integer(record.timestamp),
                        system_columns::OFFSET => FieldValue::Integer(record.offset),
                        system_columns::PARTITION => FieldValue::Integer(record.partition as i64),
                        system_columns::EVENT_TIME => Self::get_event_time_value(record),
                        system_columns::WINDOW_START | system_columns::WINDOW_END => {
                            // Window columns are injected into fields HashMap by WindowProcessor
                            // Look them up as regular fields
                            record
                                .fields
                                .get(sys_col)
                                .cloned()
                                .unwrap_or(FieldValue::Null)
                        }
                        _ => unreachable!(),
                    })
                } else {
                    // Handle qualified column names (table.column)
                    if name.contains('.') {
                        // Try to find the field with the qualified name first (for JOIN aliases)
                        if let Some(value) = record.fields.get(name) {
                            Ok(value.clone())
                        } else {
                            let column_name = name.split('.').next_back().unwrap_or(name);
                            // Check if the unqualified column is a system column (e.g. m._event_time → _event_time)
                            if let Some(sys_col) =
                                system_columns::normalize_if_system_column(column_name)
                            {
                                Ok(match sys_col {
                                    system_columns::TIMESTAMP => {
                                        FieldValue::Integer(record.timestamp)
                                    }
                                    system_columns::OFFSET => FieldValue::Integer(record.offset),
                                    system_columns::PARTITION => {
                                        FieldValue::Integer(record.partition as i64)
                                    }
                                    system_columns::EVENT_TIME => {
                                        Self::get_event_time_value(record)
                                    }
                                    system_columns::WINDOW_START | system_columns::WINDOW_END => {
                                        record
                                            .fields
                                            .get(sys_col)
                                            .cloned()
                                            .unwrap_or(FieldValue::Null)
                                    }
                                    _ => unreachable!(),
                                })
                            } else {
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
                        }
                    } else {
                        // Regular field lookup - return NULL if not found instead of error
                        Ok(record.fields.get(name).cloned().unwrap_or(FieldValue::Null))
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
                    // Convert decimal literals to ScaledInteger for 42x faster performance
                    scaled_integer_helper::parse_decimal_to_scaled_integer(s)
                }
                LiteralValue::Interval { value, unit } => Ok(FieldValue::Interval {
                    value: *value,
                    unit: unit.clone(),
                }),
            },
            Expr::BinaryOp { left, op, right } => {
                // Handle IN/NOT IN BEFORE evaluating right side, since List
                // nodes cannot be evaluated as standalone expressions.
                match op {
                    BinaryOperator::In => {
                        let left_val = Self::evaluate_expression_value(left, record)?;
                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(true));
                                    }
                                }
                                return Ok(FieldValue::Boolean(false));
                            }
                            _ => {
                                let right_val = Self::evaluate_expression_value(right, record)?;
                                return Ok(FieldValue::Boolean(Self::values_equal(
                                    &left_val, &right_val,
                                )));
                            }
                        }
                    }
                    BinaryOperator::NotIn => {
                        let left_val = Self::evaluate_expression_value(left, record)?;
                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value =
                                        Self::evaluate_expression_value(value_expr, record)?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(false));
                                    }
                                }
                                return Ok(FieldValue::Boolean(true));
                            }
                            _ => {
                                let right_val = Self::evaluate_expression_value(right, record)?;
                                return Ok(FieldValue::Boolean(!Self::values_equal(
                                    &left_val, &right_val,
                                )));
                            }
                        }
                    }
                    _ => {}
                }

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
                        // Handle NULL values first (SQL standard: concatenation with NULL returns NULL)
                        (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
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
                                FieldValue::Null => unreachable!("NULL values handled above"),
                            };
                            if left_val == *other {
                                Ok(FieldValue::String(format!("{}{}", other_str, s)))
                            } else {
                                Ok(FieldValue::String(format!("{}{}", s, other_str)))
                            }
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

                    // Set membership operators - handled early above (before eager evaluation)
                    BinaryOperator::In | BinaryOperator::NotIn => {
                        unreachable!("IN/NOT IN handled above before right-side evaluation")
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
                use crate::velostream::sql::ast::SubqueryType;
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
                use crate::velostream::sql::ast::UnaryOperator;
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
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr_val = Self::evaluate_expression_value(expr, record)?;
                let low_val = Self::evaluate_expression_value(low, record)?;
                let high_val = Self::evaluate_expression_value(high, record)?;

                // Handle NULL values according to SQL standards
                if matches!(expr_val, FieldValue::Null)
                    || matches!(low_val, FieldValue::Null)
                    || matches!(high_val, FieldValue::Null)
                {
                    return Ok(FieldValue::Boolean(false));
                }

                let result = match (&expr_val, &low_val, &high_val) {
                    // Integer comparisons
                    (FieldValue::Integer(e), FieldValue::Integer(l), FieldValue::Integer(h)) => {
                        e >= l && e <= h
                    }
                    // Float comparisons
                    (FieldValue::Float(e), FieldValue::Float(l), FieldValue::Float(h)) => {
                        e >= l && e <= h
                    }
                    // ScaledInteger comparisons (financial precision)
                    (
                        FieldValue::ScaledInteger(e_val, e_scale),
                        FieldValue::ScaledInteger(l_val, l_scale),
                        FieldValue::ScaledInteger(h_val, h_scale),
                    ) => {
                        // Normalize scales for proper comparison
                        let max_scale = (*e_scale).max(*l_scale).max(*h_scale);
                        let e_normalized = *e_val * 10_i64.pow((max_scale - e_scale) as u32);
                        let l_normalized = *l_val * 10_i64.pow((max_scale - l_scale) as u32);
                        let h_normalized = *h_val * 10_i64.pow((max_scale - h_scale) as u32);

                        e_normalized >= l_normalized && e_normalized <= h_normalized
                    }
                    // String comparisons (lexicographic)
                    (FieldValue::String(e), FieldValue::String(l), FieldValue::String(h)) => {
                        e >= l && e <= h
                    }
                    // Mixed type comparisons - convert to common type
                    _ => {
                        // Try to convert all to Float for comparison
                        let e_float = Self::to_comparable_float(&expr_val)?;
                        let l_float = Self::to_comparable_float(&low_val)?;
                        let h_float = Self::to_comparable_float(&high_val)?;

                        e_float >= l_float && e_float <= h_float
                    }
                };

                Ok(FieldValue::Boolean(if *negated { !result } else { result }))
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
                let empty_buffer: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();
                super::WindowFunctions::evaluate_window_function(
                    function_name,
                    args,
                    over_clause,
                    record,
                    &empty_buffer,
                    false,
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
            // Temporal equality
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a == b,
            (FieldValue::Date(a), FieldValue::Date(b)) => a == b,
            (FieldValue::Date(a), FieldValue::Timestamp(b)) => {
                a.and_hms_opt(0, 0, 0).unwrap() == *b
            }
            (FieldValue::Timestamp(a), FieldValue::Date(b)) => {
                *a == b.and_hms_opt(0, 0, 0).unwrap()
            }
            // Temporal vs Integer(epoch-millis) equality
            (FieldValue::Date(a), FieldValue::Integer(ms))
            | (FieldValue::Integer(ms), FieldValue::Date(a)) => {
                chrono::DateTime::from_timestamp_millis(*ms)
                    .map(|dt| a.and_hms_opt(0, 0, 0).unwrap() == dt.naive_utc())
                    .unwrap_or(false)
            }
            (FieldValue::Timestamp(a), FieldValue::Integer(ms))
            | (FieldValue::Integer(ms), FieldValue::Timestamp(a)) => {
                chrono::DateTime::from_timestamp_millis(*ms)
                    .map(|dt| *a == dt.naive_utc())
                    .unwrap_or(false)
            }
            // Temporal vs String equality (for substituted correlation literals)
            (FieldValue::Date(a), FieldValue::String(s))
            | (FieldValue::String(s), FieldValue::Date(a)) => {
                chrono::NaiveDate::parse_from_str(&s[..s.len().min(10)], "%Y-%m-%d")
                    .map(|d| a == &d)
                    .unwrap_or(false)
            }
            (FieldValue::Timestamp(a), FieldValue::String(s))
            | (FieldValue::String(s), FieldValue::Timestamp(a)) => {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map(|dt| a == &dt)
                    .unwrap_or(false)
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

            // ScaledInteger comparisons
            (
                FieldValue::ScaledInteger(value_a, scale_a),
                FieldValue::ScaledInteger(value_b, scale_b),
            ) => {
                // Compare ScaledInteger with ScaledInteger - align scales for exact comparison
                let max_scale = (*scale_a).max(*scale_b);
                let adjusted_a = value_a * 10i64.pow((max_scale - scale_a) as u32);
                let adjusted_b = value_b * 10i64.pow((max_scale - scale_b) as u32);
                adjusted_a.cmp(&adjusted_b) as i32
            }
            (FieldValue::ScaledInteger(value, scale), FieldValue::Integer(b)) => {
                // Compare ScaledInteger with Integer - convert to same scale
                let scaled_b = b * 10i64.pow(*scale as u32);
                value.cmp(&scaled_b) as i32
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(value, scale)) => {
                // Compare Integer with ScaledInteger - convert to same scale
                let scaled_a = a * 10i64.pow(*scale as u32);
                scaled_a.cmp(value) as i32
            }
            (FieldValue::ScaledInteger(value, scale), FieldValue::Float(b)) => {
                // Compare ScaledInteger with Float - convert ScaledInteger to float
                let float_a = (*value as f64) / 10f64.powi(*scale as i32);
                float_a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(value, scale)) => {
                // Compare Float with ScaledInteger - convert ScaledInteger to float
                let float_b = (*value as f64) / 10f64.powi(*scale as i32);
                a.partial_cmp(&float_b).unwrap_or(std::cmp::Ordering::Equal) as i32
            }

            // Temporal comparisons
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a.cmp(b) as i32,
            (FieldValue::Date(a), FieldValue::Date(b)) => a.cmp(b) as i32,
            (FieldValue::Date(a), FieldValue::Timestamp(b)) => {
                a.and_hms_opt(0, 0, 0).unwrap().cmp(b) as i32
            }
            (FieldValue::Timestamp(a), FieldValue::Date(b)) => {
                a.cmp(&b.and_hms_opt(0, 0, 0).unwrap()) as i32
            }

            // Temporal vs Integer(epoch-millis) comparisons — _event_time is epoch millis
            (FieldValue::Date(a), FieldValue::Integer(ms))
            | (FieldValue::Integer(ms), FieldValue::Date(a)) => {
                let dt = chrono::DateTime::from_timestamp_millis(*ms)
                    .ok_or_else(|| SqlError::TypeError {
                        expected: "valid epoch millis".to_string(),
                        actual: ms.to_string(),
                        value: None,
                    })?
                    .naive_utc();
                let a_dt = a.and_hms_opt(0, 0, 0).unwrap();
                if matches!(left, FieldValue::Date(_)) {
                    a_dt.cmp(&dt) as i32
                } else {
                    dt.cmp(&a_dt) as i32
                }
            }
            (FieldValue::Timestamp(a), FieldValue::Integer(ms))
            | (FieldValue::Integer(ms), FieldValue::Timestamp(a)) => {
                let dt = chrono::DateTime::from_timestamp_millis(*ms)
                    .ok_or_else(|| SqlError::TypeError {
                        expected: "valid epoch millis".to_string(),
                        actual: ms.to_string(),
                        value: None,
                    })?
                    .naive_utc();
                if matches!(left, FieldValue::Timestamp(_)) {
                    a.cmp(&dt) as i32
                } else {
                    dt.cmp(a) as i32
                }
            }

            // Temporal vs String comparisons (for substituted correlation literals)
            (FieldValue::Date(a), FieldValue::String(s))
            | (FieldValue::String(s), FieldValue::Date(a))
                if s.len() >= 10 =>
            {
                let other = chrono::NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d")
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .map(|dt| dt.date())
                    })
                    .map_err(|_| SqlError::TypeError {
                        expected: "date-compatible string".to_string(),
                        actual: s.clone(),
                        value: None,
                    })?;
                if matches!(left, FieldValue::Date(_)) {
                    a.cmp(&other) as i32
                } else {
                    other.cmp(a) as i32
                }
            }
            (FieldValue::Timestamp(a), FieldValue::String(s))
            | (FieldValue::String(s), FieldValue::Timestamp(a))
                if s.len() >= 19 =>
            {
                let other = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map_err(|_| SqlError::TypeError {
                        expected: "timestamp-compatible string".to_string(),
                        actual: s.clone(),
                        value: None,
                    })?;
                if matches!(left, FieldValue::Timestamp(_)) {
                    a.cmp(&other) as i32
                } else {
                    other.cmp(a) as i32
                }
            }

            _ => {
                return Err(SqlError::TypeError {
                    expected: "comparable types".to_string(),
                    actual: format!(
                        "incompatible types: left={} ({:?}), right={} ({:?})",
                        left.type_name(),
                        left,
                        right.type_name(),
                        right
                    ),
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
                use crate::velostream::sql::ast::BinaryOperator;

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
            Expr::Column(name) => {
                // Handle column references - normalized to UPPERCASE
                if let Some(sys_col) = system_columns::normalize_if_system_column(name) {
                    Ok(match sys_col {
                        system_columns::TIMESTAMP => FieldValue::Integer(record.timestamp),
                        system_columns::OFFSET => FieldValue::Integer(record.offset),
                        system_columns::PARTITION => FieldValue::Integer(record.partition as i64),
                        system_columns::EVENT_TIME => Self::get_event_time_value(record),
                        system_columns::WINDOW_START | system_columns::WINDOW_END => {
                            // Window columns are injected into fields HashMap by WindowProcessor
                            // Look them up as regular fields
                            record
                                .fields
                                .get(sys_col)
                                .cloned()
                                .unwrap_or(FieldValue::Null)
                        }
                        _ => unreachable!(),
                    })
                } else {
                    // Handle qualified column names (table.column)
                    if name.contains('.') {
                        // Try to find the field with the qualified name first (for JOIN aliases)
                        if let Some(value) = record.fields.get(name) {
                            Ok(value.clone())
                        } else {
                            let column_name = name.split('.').next_back().unwrap_or(name);
                            // Check if the unqualified column is a system column
                            if let Some(sys_col) =
                                system_columns::normalize_if_system_column(column_name)
                            {
                                Ok(match sys_col {
                                    system_columns::TIMESTAMP => {
                                        FieldValue::Integer(record.timestamp)
                                    }
                                    system_columns::OFFSET => FieldValue::Integer(record.offset),
                                    system_columns::PARTITION => {
                                        FieldValue::Integer(record.partition as i64)
                                    }
                                    system_columns::EVENT_TIME => {
                                        Self::get_event_time_value(record)
                                    }
                                    system_columns::WINDOW_START | system_columns::WINDOW_END => {
                                        record
                                            .fields
                                            .get(sys_col)
                                            .cloned()
                                            .unwrap_or(FieldValue::Null)
                                    }
                                    _ => unreachable!(),
                                })
                            } else {
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
                        }
                    } else {
                        // Regular field lookup - return NULL if not found
                        Ok(record.fields.get(name).cloned().unwrap_or(FieldValue::Null))
                    }
                }
            }
            Expr::Literal(literal) => {
                // Handle literal values - no subquery support needed
                match literal {
                    LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
                    LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
                    LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                    LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
                    LiteralValue::Null => Ok(FieldValue::Null),
                    LiteralValue::Decimal(s) => {
                        scaled_integer_helper::parse_decimal_to_scaled_integer(s)
                    }
                    LiteralValue::Interval { value, unit } => Ok(FieldValue::Interval {
                        value: *value,
                        unit: unit.clone(),
                    }),
                }
            }
            Expr::Function { name, args } => {
                // Handle function calls - evaluate args recursively with subquery support
                // For now, we need to create a temporary expression to evaluate the function
                // We'll evaluate the args first to ensure subqueries in args work
                let mut evaluated_args = Vec::new();
                for arg in args {
                    let _arg_value = Self::evaluate_expression_value_with_subqueries(
                        arg,
                        record,
                        subquery_executor,
                        context,
                    )?;
                    // Convert back to expression for function evaluation
                    // This is a bit of a workaround - ideally functions should work with FieldValues directly
                    evaluated_args.push(arg.clone());
                }

                // Use the original function evaluation logic
                let func_expr = Expr::Function {
                    name: name.clone(),
                    args: evaluated_args,
                };
                BuiltinFunctions::evaluate_function(&func_expr, record)
            }
            Expr::UnaryOp {
                op,
                expr: inner_expr,
            } => {
                // Handle unary operators recursively with subquery support
                use crate::velostream::sql::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        let inner_result = Self::evaluate_expression_value_with_subqueries(
                            inner_expr,
                            record,
                            subquery_executor,
                            context,
                        )?;
                        match inner_result {
                            FieldValue::Boolean(b) => Ok(FieldValue::Boolean(!b)),
                            _ => {
                                let bool_val = Self::field_value_to_bool(&inner_result)?;
                                Ok(FieldValue::Boolean(!bool_val))
                            }
                        }
                    }
                    UnaryOperator::IsNull => {
                        let inner_result = Self::evaluate_expression_value_with_subqueries(
                            inner_expr,
                            record,
                            subquery_executor,
                            context,
                        )?;
                        Ok(FieldValue::Boolean(matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    UnaryOperator::IsNotNull => {
                        let inner_result = Self::evaluate_expression_value_with_subqueries(
                            inner_expr,
                            record,
                            subquery_executor,
                            context,
                        )?;
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
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // Handle BETWEEN expressions recursively with subquery support
                let expr_val = Self::evaluate_expression_value_with_subqueries(
                    expr,
                    record,
                    subquery_executor,
                    context,
                )?;
                let low_val = Self::evaluate_expression_value_with_subqueries(
                    low,
                    record,
                    subquery_executor,
                    context,
                )?;
                let high_val = Self::evaluate_expression_value_with_subqueries(
                    high,
                    record,
                    subquery_executor,
                    context,
                )?;

                // Handle NULL values according to SQL standards
                if matches!(expr_val, FieldValue::Null)
                    || matches!(low_val, FieldValue::Null)
                    || matches!(high_val, FieldValue::Null)
                {
                    return Ok(FieldValue::Boolean(false));
                }

                let result = match (&expr_val, &low_val, &high_val) {
                    (FieldValue::Integer(e), FieldValue::Integer(l), FieldValue::Integer(h)) => {
                        e >= l && e <= h
                    }
                    (FieldValue::Float(e), FieldValue::Float(l), FieldValue::Float(h)) => {
                        e >= l && e <= h
                    }
                    (
                        FieldValue::ScaledInteger(e_val, e_scale),
                        FieldValue::ScaledInteger(l_val, l_scale),
                        FieldValue::ScaledInteger(h_val, h_scale),
                    ) => {
                        let max_scale = (*e_scale).max(*l_scale).max(*h_scale);
                        let e_normalized = *e_val * 10_i64.pow((max_scale - e_scale) as u32);
                        let l_normalized = *l_val * 10_i64.pow((max_scale - l_scale) as u32);
                        let h_normalized = *h_val * 10_i64.pow((max_scale - h_scale) as u32);
                        e_normalized >= l_normalized && e_normalized <= h_normalized
                    }
                    (FieldValue::String(e), FieldValue::String(l), FieldValue::String(h)) => {
                        e >= l && e <= h
                    }
                    _ => {
                        let e_float = Self::to_comparable_float(&expr_val)?;
                        let l_float = Self::to_comparable_float(&low_val)?;
                        let h_float = Self::to_comparable_float(&high_val)?;
                        e_float >= l_float && e_float <= h_float
                    }
                };

                Ok(FieldValue::Boolean(if *negated { !result } else { result }))
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                // Handle CASE expressions recursively with subquery support
                for (condition, result) in when_clauses {
                    let condition_value = Self::evaluate_expression_with_subqueries(
                        condition,
                        record,
                        subquery_executor,
                        context,
                    )?;
                    if condition_value {
                        return Self::evaluate_expression_value_with_subqueries(
                            result,
                            record,
                            subquery_executor,
                            context,
                        );
                    }
                }

                // If no WHEN condition was true, evaluate ELSE clause or return NULL
                if let Some(else_expr) = else_clause {
                    Self::evaluate_expression_value_with_subqueries(
                        else_expr,
                        record,
                        subquery_executor,
                        context,
                    )
                } else {
                    Ok(FieldValue::Null)
                }
            }
            Expr::WindowFunction {
                function_name,
                args,
                over_clause,
            } => {
                // Window functions need window buffer - use the buffer from ProcessorContext
                // This allows window functions to access related rows and apply frame bounds
                let empty_buffer = Vec::new();
                let window_buffer = if let Some(window_ctx) = &context.window_context {
                    &window_ctx.buffer
                } else {
                    &empty_buffer
                };
                let buffer_includes_current = context
                    .window_context
                    .as_ref()
                    .map(|wc| wc.buffer_includes_current)
                    .unwrap_or(false);
                super::WindowFunctions::evaluate_window_function(
                    function_name,
                    args,
                    over_clause,
                    record,
                    window_buffer,
                    buffer_includes_current,
                )
            }
            Expr::List(_) => {
                // List expressions should only appear in IN/NOT IN context, already handled above
                Err(SqlError::ExecutionError {
                    message: "List expressions must be used in IN/NOT IN operations".to_string(),
                    query: None,
                })
            }
        }
    }

    /// Evaluates an expression with support for SELECT alias references.
    ///
    /// This method is used during SELECT clause processing to allow columns
    /// to reference earlier-computed alias values from the same SELECT clause.
    /// Aliases take priority over record fields when resolving column references.
    ///
    /// # Arguments
    ///
    /// * `expr` - The expression to evaluate
    /// * `record` - The current stream record with field values
    /// * `alias_context` - The context containing previously-computed aliases
    ///
    /// # Example
    ///
    /// ```text
    /// SELECT
    ///     x + 1 AS computed,           -- First: evaluate and store as alias
    ///     computed * 2 AS result       -- Second: can reference alias "computed"
    /// ```
    pub fn evaluate_expression_value_with_alias_context(
        expr: &Expr,
        record: &StreamRecord,
        alias_context: &SelectAliasContext,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                // NEW: Check alias context FIRST (before record fields)
                // This gives aliases priority over original columns (alias shadowing)
                if let Some(value) = alias_context.get_alias(name) {
                    return Ok(value.clone());
                }

                // Fall back to original column lookup (record fields and system columns)
                Self::evaluate_expression_value(expr, record)
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle IN/NOT IN operators with special alias context support
                match op {
                    BinaryOperator::In => {
                        let left_val = Self::evaluate_expression_value_with_alias_context(
                            left,
                            record,
                            alias_context,
                        )?;

                        // SQL semantics: NULL IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(FieldValue::Boolean(false));
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value = Self::evaluate_expression_value_with_alias_context(
                                        value_expr,
                                        record,
                                        alias_context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(true));
                                    }
                                }
                                Ok(FieldValue::Boolean(false))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message: "IN operator requires a list on the right side"
                                    .to_string(),
                                query: None,
                            }),
                        }
                    }
                    BinaryOperator::NotIn => {
                        let left_val = Self::evaluate_expression_value_with_alias_context(
                            left,
                            record,
                            alias_context,
                        )?;

                        // SQL semantics: NULL NOT IN (...) always returns NULL (false in boolean context)
                        if matches!(left_val, FieldValue::Null) {
                            return Ok(FieldValue::Boolean(false));
                        }

                        match &**right {
                            Expr::List(values) => {
                                for value_expr in values {
                                    let value = Self::evaluate_expression_value_with_alias_context(
                                        value_expr,
                                        record,
                                        alias_context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(false));
                                    }
                                }
                                Ok(FieldValue::Boolean(true))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message: "NOT IN operator requires a list on the right side"
                                    .to_string(),
                                query: None,
                            }),
                        }
                    }
                    _ => {
                        // For all other operators, recursively evaluate both sides with alias context
                        let left_val = Self::evaluate_expression_value_with_alias_context(
                            left,
                            record,
                            alias_context,
                        )?;
                        let right_val = Self::evaluate_expression_value_with_alias_context(
                            right,
                            record,
                            alias_context,
                        )?;

                        // Use existing binary operation logic
                        match op {
                            BinaryOperator::Add => left_val.add(&right_val),
                            BinaryOperator::Subtract => left_val.subtract(&right_val),
                            BinaryOperator::Multiply => left_val.multiply(&right_val),
                            BinaryOperator::Divide => left_val.divide(&right_val),
                            BinaryOperator::Concat => match (&left_val, &right_val) {
                                (FieldValue::Null, _) | (_, FieldValue::Null) => {
                                    Ok(FieldValue::Null)
                                }
                                (FieldValue::String(s1), FieldValue::String(s2)) => {
                                    Ok(FieldValue::String(format!("{}{}", s1, s2)))
                                }
                                _ => Self::evaluate_expression_value(expr, record),
                            },
                            // For comparison operators, construct the result
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
                            _ => Err(SqlError::ExecutionError {
                                message: format!("Unsupported binary operator: {:?}", op),
                                query: None,
                            }),
                        }
                    }
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                // Recursively evaluate inner expression with alias context
                match op {
                    crate::velostream::sql::ast::UnaryOperator::Not => {
                        let inner_result = Self::evaluate_expression_value_with_alias_context(
                            inner,
                            record,
                            alias_context,
                        )?;
                        match inner_result {
                            FieldValue::Boolean(b) => Ok(FieldValue::Boolean(!b)),
                            _ => {
                                let bool_val = Self::field_value_to_bool(&inner_result)?;
                                Ok(FieldValue::Boolean(!bool_val))
                            }
                        }
                    }
                    crate::velostream::sql::ast::UnaryOperator::IsNull => {
                        let inner_result = Self::evaluate_expression_value_with_alias_context(
                            inner,
                            record,
                            alias_context,
                        )?;
                        Ok(FieldValue::Boolean(matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    crate::velostream::sql::ast::UnaryOperator::IsNotNull => {
                        let inner_result = Self::evaluate_expression_value_with_alias_context(
                            inner,
                            record,
                            alias_context,
                        )?;
                        Ok(FieldValue::Boolean(!matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    _ => Self::evaluate_expression_value(expr, record),
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                // Evaluate CASE conditions and results with alias context support
                for (condition, result_expr) in when_clauses {
                    let cond_val = Self::evaluate_expression_value_with_alias_context(
                        condition,
                        record,
                        alias_context,
                    )?;
                    let cond_bool = Self::field_value_to_bool(&cond_val)?;
                    if cond_bool {
                        return Self::evaluate_expression_value_with_alias_context(
                            result_expr,
                            record,
                            alias_context,
                        );
                    }
                }

                // If no condition matched, evaluate ELSE clause
                if let Some(else_expr) = else_clause {
                    Self::evaluate_expression_value_with_alias_context(
                        else_expr,
                        record,
                        alias_context,
                    )
                } else {
                    Ok(FieldValue::Null)
                }
            }
            // For all other expression types (literals, functions, etc.),
            // delegate to the original evaluator since they don't reference aliases
            _ => Self::evaluate_expression_value(expr, record),
        }
    }

    /// Evaluates an expression with support for BOTH SELECT alias references AND subqueries.
    ///
    /// This method combines the capabilities of alias context and subquery execution,
    /// allowing SELECT fields to reference both previous aliases and execute subqueries.
    /// This is used in Phase 5 to wire scalar subqueries into SELECT clause processing.
    ///
    /// # Arguments
    ///
    /// * `expr` - The expression to evaluate
    /// * `record` - The current stream record with field values
    /// * `alias_context` - The context containing previously-computed aliases
    /// * `subquery_executor` - Implementation of SubqueryExecutor for executing subqueries
    /// * `context` - The processor context for subquery state management
    pub fn evaluate_expression_value_with_alias_and_subquery_context<T: SubqueryExecutor>(
        expr: &Expr,
        record: &StreamRecord,
        alias_context: &SelectAliasContext,
        subquery_executor: &T,
        context: &ProcessorContext,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check alias context FIRST (priority to aliases), then fall back to fields
                if let Some(value) = alias_context.get_alias(name) {
                    return Ok(value.clone());
                }
                // Fall back to column evaluation (with subquery support)
                Self::evaluate_expression_value_with_subqueries(
                    expr,
                    record,
                    subquery_executor,
                    context,
                )
            }
            Expr::Subquery { .. } => {
                // Delegate subquery execution to the enhanced evaluator with subquery support
                Self::evaluate_expression_value_with_subqueries(
                    expr,
                    record,
                    subquery_executor,
                    context,
                )
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle IN/NOT IN operators with special alias AND subquery context support
                match op {
                    BinaryOperator::In => {
                        let left_val =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                left,
                                record,
                                alias_context,
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
                                    let value = Self::evaluate_expression_value_with_alias_and_subquery_context(
                                        value_expr,
                                        record,
                                        alias_context,
                                        subquery_executor,
                                        context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(true));
                                    }
                                }
                                Ok(FieldValue::Boolean(false))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message: "IN operator requires a list on the right side"
                                    .to_string(),
                                query: None,
                            }),
                        }
                    }
                    BinaryOperator::NotIn => {
                        let left_val =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                left,
                                record,
                                alias_context,
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
                                    let value = Self::evaluate_expression_value_with_alias_and_subquery_context(
                                        value_expr,
                                        record,
                                        alias_context,
                                        subquery_executor,
                                        context,
                                    )?;
                                    if Self::values_equal(&left_val, &value) {
                                        return Ok(FieldValue::Boolean(false));
                                    }
                                }
                                Ok(FieldValue::Boolean(true))
                            }
                            _ => Err(SqlError::ExecutionError {
                                message: "NOT IN operator requires a list on the right side"
                                    .to_string(),
                                query: None,
                            }),
                        }
                    }
                    _ => {
                        // For all other operators, recursively evaluate both sides with BOTH alias and subquery context
                        let left_val =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                left,
                                record,
                                alias_context,
                                subquery_executor,
                                context,
                            )?;
                        let right_val =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                right,
                                record,
                                alias_context,
                                subquery_executor,
                                context,
                            )?;

                        // Use existing binary operation logic
                        match op {
                            BinaryOperator::Add => left_val.add(&right_val),
                            BinaryOperator::Subtract => left_val.subtract(&right_val),
                            BinaryOperator::Multiply => left_val.multiply(&right_val),
                            BinaryOperator::Divide => left_val.divide(&right_val),
                            BinaryOperator::Concat => match (&left_val, &right_val) {
                                (FieldValue::Null, _) | (_, FieldValue::Null) => {
                                    Ok(FieldValue::Null)
                                }
                                (FieldValue::String(s1), FieldValue::String(s2)) => {
                                    Ok(FieldValue::String(format!("{}{}", s1, s2)))
                                }
                                _ => Self::evaluate_expression_value(expr, record),
                            },
                            // For comparison and other operators, delegate to subquery-aware evaluator
                            _ => Self::evaluate_expression_value_with_subqueries(
                                expr,
                                record,
                                subquery_executor,
                                context,
                            ),
                        }
                    }
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                // Recursively evaluate inner expression with alias and subquery context
                match op {
                    crate::velostream::sql::ast::UnaryOperator::Not => {
                        let inner_result =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                inner,
                                record,
                                alias_context,
                                subquery_executor,
                                context,
                            )?;
                        match inner_result {
                            FieldValue::Boolean(b) => Ok(FieldValue::Boolean(!b)),
                            _ => {
                                let bool_val = Self::field_value_to_bool(&inner_result)?;
                                Ok(FieldValue::Boolean(!bool_val))
                            }
                        }
                    }
                    crate::velostream::sql::ast::UnaryOperator::IsNull => {
                        let inner_result =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                inner,
                                record,
                                alias_context,
                                subquery_executor,
                                context,
                            )?;
                        Ok(FieldValue::Boolean(matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    crate::velostream::sql::ast::UnaryOperator::IsNotNull => {
                        let inner_result =
                            Self::evaluate_expression_value_with_alias_and_subquery_context(
                                inner,
                                record,
                                alias_context,
                                subquery_executor,
                                context,
                            )?;
                        Ok(FieldValue::Boolean(!matches!(
                            inner_result,
                            FieldValue::Null
                        )))
                    }
                    _ => Self::evaluate_expression_value(expr, record),
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                // Evaluate CASE conditions and results with alias and subquery context support
                for (condition, result_expr) in when_clauses {
                    let cond_val = Self::evaluate_expression_value_with_alias_and_subquery_context(
                        condition,
                        record,
                        alias_context,
                        subquery_executor,
                        context,
                    )?;
                    let cond_bool = Self::field_value_to_bool(&cond_val)?;
                    if cond_bool {
                        return Self::evaluate_expression_value_with_alias_and_subquery_context(
                            result_expr,
                            record,
                            alias_context,
                            subquery_executor,
                            context,
                        );
                    }
                }

                // If no condition matched, evaluate ELSE clause
                if let Some(else_expr) = else_clause {
                    Self::evaluate_expression_value_with_alias_and_subquery_context(
                        else_expr,
                        record,
                        alias_context,
                        subquery_executor,
                        context,
                    )
                } else {
                    Ok(FieldValue::Null)
                }
            }
            // For all other expression types, delegate to subquery-aware evaluator
            _ => Self::evaluate_expression_value_with_subqueries(
                expr,
                record,
                subquery_executor,
                context,
            ),
        }
    }

    /// Helper method to convert FieldValue to f64 for comparison purposes
    fn to_comparable_float(value: &FieldValue) -> Result<f64, SqlError> {
        match value {
            FieldValue::Integer(i) => Ok(*i as f64),
            FieldValue::Float(f) => Ok(*f),
            FieldValue::ScaledInteger(val, scale) => {
                let divisor = 10_f64.powi(*scale as i32);
                Ok(*val as f64 / divisor)
            }
            FieldValue::String(s) => s.parse::<f64>().map_err(|_| SqlError::ExecutionError {
                message: format!(
                    "Cannot convert string '{}' to number for BETWEEN comparison",
                    s
                ),
                query: None,
            }),
            _ => Err(SqlError::ExecutionError {
                message: format!("Cannot compare type {:?} in BETWEEN expression", value),
                query: None,
            }),
        }
    }
}
