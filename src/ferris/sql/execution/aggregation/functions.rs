//! Aggregate function computation utilities for streaming SQL aggregations.
//!
//! This module provides utilities for computing final aggregate values from
//! accumulated state in GroupAccumulator instances.

use super::super::internal::GroupAccumulator;
use super::super::types::FieldValue;
use crate::ferris::sql::ast::{Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;

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

    /// Check if an expression is a valid aggregate function
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
            "FIRST",
            "LAST",
            "STRING_AGG",
            "GROUP_CONCAT",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::ast::Expr;
    use std::collections::HashSet;

    fn create_test_accumulator() -> GroupAccumulator {
        let mut accumulator = GroupAccumulator::new();
        accumulator.count = 10;

        // Add some test data
        accumulator.non_null_counts.insert("amount".to_string(), 8);
        accumulator.sums.insert("amount".to_string(), 150.0);
        accumulator
            .mins
            .insert("amount".to_string(), FieldValue::Float(5.0));
        accumulator
            .maxs
            .insert("amount".to_string(), FieldValue::Float(50.0));

        // Add numeric values for AVG, STDDEV, VARIANCE
        accumulator
            .numeric_values
            .insert("amount".to_string(), vec![10.0, 20.0, 30.0, 40.0, 50.0]);

        // Add string values for STRING_AGG
        accumulator.string_values.insert(
            "names".to_string(),
            vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
            ],
        );

        // Add distinct values for COUNT_DISTINCT
        let mut distinct_set = HashSet::new();
        distinct_set.insert("A".to_string());
        distinct_set.insert("B".to_string());
        distinct_set.insert("C".to_string());
        accumulator
            .distinct_values
            .insert("category".to_string(), distinct_set);

        // Add first/last values
        accumulator
            .first_values
            .insert("name".to_string(), FieldValue::String("First".to_string()));
        accumulator
            .last_values
            .insert("name".to_string(), FieldValue::String("Last".to_string()));

        accumulator
    }

    #[test]
    fn test_compute_count_star() {
        let accumulator = create_test_accumulator();
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![], // COUNT(*)
        };

        let result = AggregateFunctions::compute_field_aggregate_value(
            "count_star",
            &count_expr,
            &accumulator,
        )
        .unwrap();

        assert_eq!(result, FieldValue::Integer(10));
    }

    #[test]
    fn test_compute_count_column() {
        let accumulator = create_test_accumulator();
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };

        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &count_expr, &accumulator)
                .unwrap();

        assert_eq!(result, FieldValue::Integer(8));
    }

    #[test]
    fn test_compute_sum() {
        let accumulator = create_test_accumulator();
        let sum_expr = Expr::Function {
            name: "SUM".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };

        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &sum_expr, &accumulator)
                .unwrap();

        assert_eq!(result, FieldValue::Float(150.0));
    }

    #[test]
    fn test_compute_avg() {
        let accumulator = create_test_accumulator();
        let avg_expr = Expr::Function {
            name: "AVG".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };

        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &avg_expr, &accumulator)
                .unwrap();

        // Average of [10, 20, 30, 40, 50] = 30.0
        assert_eq!(result, FieldValue::Float(30.0));
    }

    #[test]
    fn test_compute_min_max() {
        let accumulator = create_test_accumulator();

        let min_expr = Expr::Function {
            name: "MIN".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &min_expr, &accumulator)
                .unwrap();
        assert_eq!(result, FieldValue::Float(5.0));

        let max_expr = Expr::Function {
            name: "MAX".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &max_expr, &accumulator)
                .unwrap();
        assert_eq!(result, FieldValue::Float(50.0));
    }

    #[test]
    fn test_compute_stddev_variance() {
        let accumulator = create_test_accumulator();

        let stddev_expr = Expr::Function {
            name: "STDDEV".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        let result =
            AggregateFunctions::compute_field_aggregate_value("amount", &stddev_expr, &accumulator)
                .unwrap();

        // For values [10, 20, 30, 40, 50], sample stddev should be calculated
        if let FieldValue::Float(stddev) = result {
            assert!(stddev > 15.0 && stddev < 17.0); // Approximate check
        } else {
            panic!("Expected Float result for STDDEV");
        }
    }

    #[test]
    fn test_compute_first_last() {
        let accumulator = create_test_accumulator();

        let first_expr = Expr::Function {
            name: "FIRST".to_string(),
            args: vec![Expr::Column("name".to_string())],
        };
        let result =
            AggregateFunctions::compute_field_aggregate_value("name", &first_expr, &accumulator)
                .unwrap();
        assert_eq!(result, FieldValue::String("First".to_string()));

        let last_expr = Expr::Function {
            name: "LAST".to_string(),
            args: vec![Expr::Column("name".to_string())],
        };
        let result =
            AggregateFunctions::compute_field_aggregate_value("name", &last_expr, &accumulator)
                .unwrap();
        assert_eq!(result, FieldValue::String("Last".to_string()));
    }

    #[test]
    fn test_compute_string_agg() {
        let accumulator = create_test_accumulator();
        let string_agg_expr = Expr::Function {
            name: "STRING_AGG".to_string(),
            args: vec![
                Expr::Column("names".to_string()),
                Expr::Literal(LiteralValue::String(";".to_string())),
            ],
        };

        let result = AggregateFunctions::compute_field_aggregate_value(
            "names",
            &string_agg_expr,
            &accumulator,
        )
        .unwrap();

        assert_eq!(result, FieldValue::String("Alice;Bob;Charlie".to_string()));
    }

    #[test]
    fn test_compute_count_distinct() {
        let accumulator = create_test_accumulator();
        let count_distinct_expr = Expr::Function {
            name: "COUNT_DISTINCT".to_string(),
            args: vec![Expr::Column("category".to_string())],
        };

        let result = AggregateFunctions::compute_field_aggregate_value(
            "category",
            &count_distinct_expr,
            &accumulator,
        )
        .unwrap();

        assert_eq!(result, FieldValue::Integer(3));
    }

    #[test]
    fn test_is_aggregate_function() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![],
        };
        assert!(AggregateFunctions::is_aggregate_function(&count_expr));

        let upper_expr = Expr::Function {
            name: "UPPER".to_string(),
            args: vec![Expr::Column("name".to_string())],
        };
        assert!(!AggregateFunctions::is_aggregate_function(&upper_expr));

        let identifier_expr = Expr::Column("name".to_string());
        assert!(!AggregateFunctions::is_aggregate_function(&identifier_expr));
    }

    #[test]
    fn test_supported_functions() {
        let functions = AggregateFunctions::supported_functions();
        assert!(functions.contains(&"COUNT"));
        assert!(functions.contains(&"SUM"));
        assert!(functions.contains(&"AVG"));
        assert!(functions.contains(&"MIN"));
        assert!(functions.contains(&"MAX"));
        assert!(functions.contains(&"STDDEV"));
        assert!(functions.contains(&"VARIANCE"));
        assert!(functions.contains(&"COUNT_DISTINCT"));
        assert!(functions.contains(&"FIRST"));
        assert!(functions.contains(&"LAST"));
        assert!(functions.contains(&"STRING_AGG"));
        assert!(functions.contains(&"GROUP_CONCAT"));
    }
}
