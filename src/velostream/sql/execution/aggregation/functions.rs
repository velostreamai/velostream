//! Aggregate function computation utilities for streaming SQL aggregations.
//!
//! This module provides utilities for computing final aggregate values from
//! accumulated state in GroupAccumulator instances.

use super::super::internal::GroupAccumulator;
use super::super::types::FieldValue;
use crate::velostream::sql::ast::{Expr, LiteralValue};
use crate::velostream::sql::error::SqlError;
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
                    "STDDEV" | "STDDEV_SAMP" => {
                        Self::compute_stddev_aggregate(field_name, accumulator)
                    }
                    "STDDEV_POP" => Self::compute_stddev_pop_aggregate(field_name, accumulator),
                    "VARIANCE" | "VAR_SAMP" => {
                        Self::compute_variance_aggregate(field_name, accumulator)
                    }
                    "VAR_POP" => Self::compute_var_pop_aggregate(field_name, accumulator),
                    "MEDIAN" => Self::compute_median_aggregate(field_name, accumulator),
                    "FIRST" | "FIRST_VALUE" => {
                        Self::compute_first_aggregate(field_name, accumulator)
                    }
                    "LAST" | "LAST_VALUE" => Self::compute_last_aggregate(field_name, accumulator),
                    "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" | "COLLECT" => {
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
        let has_values = accumulator
            .sum_has_values
            .get(field_name)
            .copied()
            .unwrap_or(false);
        let sum = accumulator.sums.get(field_name).copied().unwrap_or(0.0);
        let all_integer = accumulator
            .sum_all_integer
            .get(field_name)
            .copied()
            .unwrap_or(false);
        Ok(super::compute::compute_sum_result(
            sum,
            all_integer,
            has_values,
        ))
    }

    /// Compute AVG aggregate value using Welford state
    fn compute_avg_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(state) = accumulator.welford_states.get(field_name) {
            match super::compute::compute_avg_from_welford(state) {
                Some(avg) => Ok(FieldValue::Float(avg)),
                None => Ok(FieldValue::Null),
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

    /// Compute STDDEV aggregate value (sample standard deviation, N-1) using Welford
    fn compute_stddev_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(state) = accumulator.welford_states.get(field_name) {
            match super::compute::compute_stddev_from_welford(state, true) {
                Some(stddev) => Ok(FieldValue::Float(stddev)),
                None => Ok(FieldValue::Null),
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute STDDEV_POP aggregate value (population standard deviation, N) using Welford
    fn compute_stddev_pop_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(state) = accumulator.welford_states.get(field_name) {
            match super::compute::compute_stddev_from_welford(state, false) {
                Some(stddev) => Ok(FieldValue::Float(stddev)),
                None => Ok(FieldValue::Null),
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute VARIANCE aggregate value (sample variance, N-1) using Welford
    fn compute_variance_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(state) = accumulator.welford_states.get(field_name) {
            match super::compute::compute_variance_from_welford(state, true) {
                Some(var) => Ok(FieldValue::Float(var)),
                None => Ok(FieldValue::Null),
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute VAR_POP aggregate value (population variance, N) using Welford
    fn compute_var_pop_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(state) = accumulator.welford_states.get(field_name) {
            match super::compute::compute_variance_from_welford(state, false) {
                Some(var) => Ok(FieldValue::Float(var)),
                None => Ok(FieldValue::Null),
            }
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Compute MEDIAN aggregate value
    fn compute_median_aggregate(
        field_name: &str,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        if let Some(values) = accumulator.numeric_values.get(field_name) {
            match super::compute::compute_median_from_values(values) {
                Some(median) => Ok(FieldValue::Float(median)),
                None => Ok(FieldValue::Null),
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

    /// Check if an expression is a valid aggregate function.
    ///
    /// Delegates to the centralized function catalog.
    #[doc(hidden)]
    pub fn is_aggregate_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                crate::velostream::sql::execution::expression::is_aggregate_function(name)
            }
            _ => false,
        }
    }

    /// Get the list of supported aggregate function names.
    ///
    /// Delegates to the centralized function catalog.
    #[doc(hidden)]
    pub fn supported_functions() -> Vec<String> {
        crate::velostream::sql::execution::expression::function_metadata::all_aggregate_functions()
            .into_iter()
            .map(|f| f.name.to_uppercase())
            .collect()
    }
}
