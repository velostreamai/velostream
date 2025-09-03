//! Aggregate function computation utilities for streaming SQL aggregations.
//!
//! This module provides utilities for computing final aggregate values from
//! accumulated state in GroupAccumulator instances.

use super::super::internal::GroupAccumulator;
use super::super::types::FieldValue;
use crate::ferris::sql::ast::{Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;
use hyperloglogplus::HyperLogLog;

/// Utilities for aggregate function computation
pub struct AggregateFunctions;

impl AggregateFunctions {
    /// Compute the final aggregate value for a field from an accumulator
    ///
    /// This method takes a field name, its corresponding aggregate expression,
    /// and a GroupAccumulator, and returns the final computed aggregate value.
    pub fn compute_field_aggregate_value(
        field_name: &str,
        expr: &Expr,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Function { name, .. } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => Self::compute_count_aggregate(field_name, expr, accumulator),
                    "SUM" => Self::compute_sum_aggregate(field_name, accumulator),
                    "AVG" => Self::compute_avg_aggregate(field_name, accumulator),
                    "MIN" => Self::compute_min_aggregate(field_name, accumulator),
                    "MAX" => Self::compute_max_aggregate(field_name, accumulator),
                    "STDDEV" => Self::compute_stddev_aggregate(field_name, accumulator),
                    "VARIANCE" => Self::compute_variance_aggregate(field_name, accumulator),
                    "FIRST" => Self::compute_first_aggregate(field_name, accumulator),
                    "LAST" => Self::compute_last_aggregate(field_name, accumulator),
                    "STRING_AGG" | "GROUP_CONCAT" => {
                        Self::compute_string_agg_aggregate(field_name, expr, accumulator)
                    }
                    "COUNT_DISTINCT" => {
                        Self::compute_count_distinct_aggregate(field_name, accumulator)
                    }
                    "APPROX_COUNT_DISTINCT" => {
                        Self::compute_approx_count_distinct_aggregate(field_name, accumulator)
                    }
                    _ => {
                        // Non-aggregate function - use first value
                        Ok(accumulator
                            .first_values
                            .get(field_name)
                            .cloned()
                            .unwrap_or(FieldValue::Null))
                    }
                }
            }
            _ => {
                // Non-function expression - use first value
                Ok(accumulator
                    .first_values
                    .get(field_name)
                    .cloned()
                    .unwrap_or(FieldValue::Null))
            }
        }
    }

    /// Compute COUNT aggregate value
    fn compute_count_aggregate(
        field_name: &str,
        expr: &Expr,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        // Check if this is COUNT(*) or COUNT(column)
        if let Expr::Function { args, .. } = expr {
            if args.is_empty() {
                // COUNT(*) - count all records
                Ok(FieldValue::Integer(accumulator.count as i64))
            } else {
                // COUNT(column) - count non-NULL values for this field
                Ok(FieldValue::Integer(
                    accumulator
                        .non_null_counts
                        .get(field_name)
                        .copied()
                        .unwrap_or(0) as i64,
                ))
            }
        } else {
            Ok(FieldValue::Integer(accumulator.count as i64))
        }
    }

    /// Compute SUM aggregate value
    fn compute_sum_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        Ok(FieldValue::Float(
            accumulator.sums.get(field_name).copied().unwrap_or(0.0),
        ))
    }

    /// Compute AVG aggregate value
    fn compute_avg_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(values) = accumulator.numeric_values.get(field_name) {
            if values.is_empty() {
                Ok(FieldValue::Null)
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                Ok(FieldValue::Float(avg))
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute MIN aggregate value
    fn compute_min_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        Ok(accumulator
            .mins
            .get(field_name)
            .cloned()
            .unwrap_or(FieldValue::Null))
    }

    /// Compute MAX aggregate value
    fn compute_max_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        Ok(accumulator
            .maxs
            .get(field_name)
            .cloned()
            .unwrap_or(FieldValue::Null))
    }

    /// Compute STDDEV aggregate value (sample standard deviation)
    fn compute_stddev_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(values) = accumulator.numeric_values.get(field_name) {
            if values.len() < 2 {
                Ok(FieldValue::Float(0.0))
            } else {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64; // Sample standard deviation
                Ok(FieldValue::Float(variance.sqrt()))
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute VARIANCE aggregate value (sample variance)
    fn compute_variance_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(values) = accumulator.numeric_values.get(field_name) {
            if values.len() < 2 {
                Ok(FieldValue::Float(0.0))
            } else {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64; // Sample variance
                Ok(FieldValue::Float(variance))
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute FIRST aggregate value
    fn compute_first_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        Ok(accumulator
            .first_values
            .get(field_name)
            .cloned()
            .unwrap_or(FieldValue::Null))
    }

    /// Compute LAST aggregate value
    fn compute_last_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        Ok(accumulator
            .last_values
            .get(field_name)
            .cloned()
            .unwrap_or(FieldValue::Null))
    }

    /// Compute STRING_AGG/GROUP_CONCAT aggregate value
    fn compute_string_agg_aggregate(
        field_name: &str,
        expr: &Expr,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(values) = accumulator.string_values.get(field_name) {
            // Extract separator from second argument of the original expression
            let separator = if let Expr::Function { args, .. } = expr {
                if args.len() > 1 {
                    // Try to extract separator from second argument
                    if let Expr::Literal(LiteralValue::String(sep)) = &args[1] {
                        sep.as_str()
                    } else {
                        "," // Default if not a string literal
                    }
                } else {
                    "," // Default if no second argument
                }
            } else {
                "," // Default if not a function
            };
            Ok(FieldValue::String(values.join(separator)))
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute COUNT_DISTINCT aggregate value
    fn compute_count_distinct_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(distinct_set) = accumulator.distinct_values.get(field_name) {
            Ok(FieldValue::Integer(distinct_set.len() as i64))
        } else {
            Ok(FieldValue::Integer(0))
        }
    }

    /// Compute APPROX_COUNT_DISTINCT aggregate value using HyperLogLog
    fn compute_approx_count_distinct_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(hll) = accumulator.approx_distinct_values.get(field_name) {
            // Create a mutable copy to get the count
            let mut hll_copy = hll.clone();
            let count = hll_copy.count() as i64;
            Ok(FieldValue::Integer(count))
        } else {
            Ok(FieldValue::Integer(0))
        }
    }

    /// Check if an expression is a valid aggregate function
    #[doc(hidden)]
    pub fn is_aggregate_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "COUNT_DISTINCT"
                        | "APPROX_COUNT_DISTINCT"
                        | "FIRST"
                        | "LAST"
                        | "STRING_AGG"
                        | "GROUP_CONCAT"
                )
            }
            _ => false,
        }
    }

    /// Get the list of supported aggregate function names
    #[doc(hidden)]
    pub fn supported_functions() -> &'static [&'static str] {
        &[
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "STDDEV",
            "VARIANCE",
            "COUNT_DISTINCT",
            "APPROX_COUNT_DISTINCT",
            "FIRST",
            "LAST",
            "STRING_AGG",
            "GROUP_CONCAT",
        ]
    }
}
