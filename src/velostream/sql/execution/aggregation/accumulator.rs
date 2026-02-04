//! Accumulator utilities for streaming SQL aggregations.
//!
//! This module provides utilities for managing GroupAccumulator instances,
//! including record processing and aggregate value computation.

use super::super::internal::GroupAccumulator;
use super::super::types::{FieldValue, StreamRecord};
use super::super::validation::{FieldValidator, ValidationContext};
use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::expression::{ExpressionEvaluator, is_aggregate_function};
use std::collections::HashSet;

/// Utilities for GroupAccumulator management
pub struct AccumulatorManager;

impl AccumulatorManager {
    /// Process a record into an accumulator based on aggregate expressions
    ///
    /// # Arguments
    /// * `accumulator` - The group accumulator to update
    /// * `record` - The input record to process
    /// * `aggregate_expressions` - (field_name, expression) pairs for aggregates
    /// * `select_aliases` - Set of SELECT alias names that should not be validated
    ///   as input fields (they are computed from other expressions, not from input)
    #[doc(hidden)]
    pub fn process_record_into_accumulator(
        accumulator: &mut GroupAccumulator,
        record: &StreamRecord,
        aggregate_expressions: &[(String, Expr)],
        select_aliases: &HashSet<String>,
    ) -> Result<(), SqlError> {
        // Phase 2: Validate all aggregate fields exist in record before processing
        // Extract all fields referenced in the expressions
        let mut all_fields = Vec::new();
        for (_, expr) in aggregate_expressions {
            let expr_fields = FieldValidator::extract_field_names(expr);
            all_fields.extend(expr_fields);
        }

        if !all_fields.is_empty() {
            // Filter out SELECT aliases - these are computed fields, not input fields
            // For example, if SELECT has "CASE ... END AS spike_classification"
            // and another expression references "spike_classification IN (...)",
            // we should not validate spike_classification as an input field
            let fields_to_validate: Vec<&str> = all_fields
                .iter()
                .filter(|f| !select_aliases.contains(*f))
                .map(|s| s.as_str())
                .collect();

            if !fields_to_validate.is_empty() {
                FieldValidator::validate_fields_exist(
                    record,
                    &fields_to_validate,
                    ValidationContext::Aggregation,
                )
                .map_err(|e| e.to_sql_error())?;
            }
        }

        // Always increment the count for this group
        accumulator.increment_count();

        // Store the first record as a sample for non-aggregate fields
        if accumulator.sample_record.is_none() {
            accumulator.sample_record = Some(record.clone());
        }

        // Process each aggregate expression
        for (field_name, expr) in aggregate_expressions {
            Self::process_aggregate_expression(accumulator, record, field_name, expr)?;
        }

        Ok(())
    }

    /// Process a single aggregate expression for a record
    fn process_aggregate_expression(
        accumulator: &mut GroupAccumulator,
        record: &StreamRecord,
        field_name: &str,
        expr: &Expr,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        // For COUNT(column), only count non-NULL values
                        if !args.is_empty() {
                            if let Some(arg) = args.first() {
                                let value =
                                    ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                                if !matches!(value, FieldValue::Null) {
                                    accumulator.add_non_null_count(field_name);
                                }
                            }
                        }
                        // For COUNT(*), we rely on the global count which is incremented for every record
                    }
                    "SUM" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match value {
                                FieldValue::Integer(i) => {
                                    accumulator.add_sum(field_name, i as f64, true);
                                }
                                FieldValue::Float(f) => {
                                    accumulator.add_sum(field_name, f, false);
                                }
                                FieldValue::ScaledInteger(value, scale) => {
                                    let divisor = 10_i64.pow(scale as u32) as f64;
                                    let float_value = value as f64 / divisor;
                                    accumulator.add_sum(field_name, float_value, false);
                                }
                                FieldValue::Null => {
                                    // NULL values are ignored in SUM
                                }
                                _ => {
                                    return Err(SqlError::ExecutionError {
                                        message: format!(
                                            "Cannot sum non-numeric value: {:?}",
                                            value
                                        ),
                                        query: None,
                                    });
                                }
                            }
                        }
                    }
                    "MIN" | "MAX" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                if name.to_uppercase() == "MIN" {
                                    accumulator.update_min(field_name, value);
                                } else {
                                    accumulator.update_max(field_name, value);
                                }
                            }
                        }
                    }
                    "AVG" | "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" | "VARIANCE" | "VAR_SAMP"
                    | "VAR_POP" => {
                        // Use Welford's online algorithm — O(1) memory instead of O(n)
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match super::compute::field_value_to_f64(&value) {
                                Some(f) => accumulator.update_welford(field_name, f),
                                None if matches!(value, FieldValue::Null) => {
                                    // NULL values are ignored
                                }
                                None => {
                                    return Err(SqlError::ExecutionError {
                                        message: format!(
                                            "Cannot compute {} on non-numeric value: {:?}",
                                            name.to_uppercase(),
                                            value
                                        ),
                                        query: None,
                                    });
                                }
                            }
                        }
                    }
                    "MEDIAN" | "PERCENTILE_CONT" => {
                        // Need all values for sort-based computation
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match super::compute::field_value_to_f64(&value) {
                                Some(f) => {
                                    accumulator
                                        .numeric_values
                                        .entry(field_name.to_string())
                                        .or_default()
                                        .push(f);
                                }
                                None if matches!(value, FieldValue::Null) => {
                                    // NULL values are ignored
                                }
                                None => {
                                    return Err(SqlError::ExecutionError {
                                        message: format!(
                                            "Cannot compute {} on non-numeric value: {:?}",
                                            name.to_uppercase(),
                                            value
                                        ),
                                        query: None,
                                    });
                                }
                            }
                        }
                    }
                    "COUNT_DISTINCT" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                let string_value = Self::field_value_to_string(&value);
                                accumulator
                                    .distinct_values
                                    .entry(field_name.to_string())
                                    .or_default()
                                    .insert(string_value);
                            }
                        }
                    }
                    "APPROX_COUNT_DISTINCT" => {
                        // Use HyperLogLog for probabilistic distinct count estimation
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                accumulator.add_to_approx_set(field_name, value);
                            }
                        }
                    }
                    "FIRST" | "FIRST_VALUE" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            // Only set if not already set (first value wins)
                            if !accumulator.first_values.contains_key(field_name) {
                                accumulator
                                    .first_values
                                    .insert(field_name.to_string(), value);
                            }
                        }
                    }
                    "LAST" | "LAST_VALUE" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            // Always update (last value wins)
                            accumulator
                                .last_values
                                .insert(field_name.to_string(), value);
                        }
                    }
                    "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" | "COLLECT" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                let string_value = Self::field_value_to_string(&value);
                                accumulator
                                    .string_values
                                    .entry(field_name.to_string())
                                    .or_default()
                                    .push(string_value);
                            }
                        }
                    }
                    _ => {
                        // For non-recognized aggregates, store as first/last
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !accumulator.first_values.contains_key(field_name) {
                                accumulator
                                    .first_values
                                    .insert(field_name.to_string(), value.clone());
                            }
                            accumulator
                                .last_values
                                .insert(field_name.to_string(), value);
                        }
                    }
                }
            }
            _ => {
                // For non-function expressions, evaluate and store as first/last
                let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                if !accumulator.first_values.contains_key(field_name) {
                    accumulator
                        .first_values
                        .insert(field_name.to_string(), value.clone());
                }
                accumulator
                    .last_values
                    .insert(field_name.to_string(), value);
            }
        }

        Ok(())
    }

    /// Convert a FieldValue to string for distinct counting and string aggregation
    #[doc(hidden)]
    pub fn field_value_to_string(value: &FieldValue) -> String {
        match value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Timestamp(ts) => ts.to_string(),
            FieldValue::Array(arr) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(Self::field_value_to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            FieldValue::Map(obj) | FieldValue::Struct(obj) => {
                let mut pairs: Vec<_> = obj
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, Self::field_value_to_string(v)))
                    .collect();
                pairs.sort(); // Ensure consistent ordering
                format!("{{{}}}", pairs.join(","))
            }
            FieldValue::Date(date) => date.to_string(),
            FieldValue::Decimal(decimal) => decimal.to_string(),
            FieldValue::ScaledInteger(value, scale) => {
                // Format as decimal with appropriate precision
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
            FieldValue::Interval { value, unit } => format!("{}_{:?}", value, unit),
        }
    }

    /// Extract all aggregate expressions from a list of select fields
    #[doc(hidden)]
    pub fn extract_aggregate_expressions(
        select_fields: &[crate::velostream::sql::ast::SelectField],
    ) -> Vec<(String, Expr)> {
        let mut aggregate_expressions = Vec::new();

        for field in select_fields {
            match field {
                crate::velostream::sql::ast::SelectField::Expression { expr, alias } => {
                    if Self::is_aggregate_expression(expr) {
                        let field_name = alias
                            .clone()
                            .unwrap_or_else(|| Self::generate_field_name(expr));
                        aggregate_expressions.push((field_name, expr.clone()));
                    }
                }
                crate::velostream::sql::ast::SelectField::Column(_) => {
                    // Simple column reference is not an aggregate
                }
                crate::velostream::sql::ast::SelectField::AliasedColumn { .. } => {
                    // Aliased column reference is not an aggregate
                }
                crate::velostream::sql::ast::SelectField::Wildcard => {
                    // Wildcard doesn't contain aggregates
                }
            }
        }

        aggregate_expressions
    }

    /// Check if an expression is an aggregate function.
    ///
    /// Uses the centralized function catalog instead of a hardcoded list.
    #[doc(hidden)]
    pub fn is_aggregate_expression(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => is_aggregate_function(name),
            _ => false,
        }
    }

    /// Generate a field name for an expression
    #[doc(hidden)]
    pub fn generate_field_name(expr: &Expr) -> String {
        match expr {
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}(*)", name)
                } else {
                    format!("{}({})", name, Self::expr_to_string(&args[0]))
                }
            }
            _ => Self::expr_to_string(expr),
        }
    }

    /// Convert an expression to a string representation
    fn expr_to_string(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Literal(val) => format!("{:?}", val),
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}()", name)
                } else {
                    let arg_strings: Vec<_> = args.iter().map(Self::expr_to_string).collect();
                    format!("{}({})", name, arg_strings.join(", "))
                }
            }
            _ => "expr".to_string(),
        }
    }
}
