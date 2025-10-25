//! Aggregation Function Validation
//!
//! Validates that aggregate functions are used correctly in SQL queries:
//! - Aggregate functions only in SELECT and HAVING clauses
//! - GROUP BY completeness (non-aggregated columns must be in GROUP BY)
//! - No mixed aggregates and non-aggregates in the same context
//! This is Phase 7 of the correctness work.

use crate::velostream::sql::ast::{BinaryOperator, Expr};
use crate::velostream::sql::SqlError;
use std::collections::HashSet;

/// Information about whether expression contains aggregates
#[derive(Debug, Clone)]
struct AggregateInfo {
    contains_aggregate: bool,
}

/// Validator for SQL aggregation functions and GROUP BY rules
pub struct AggregationValidator;

impl AggregationValidator {
    /// List of known aggregate function names
    fn aggregate_functions() -> HashSet<&'static str> {
        let mut functions = HashSet::new();
        functions.insert("COUNT");
        functions.insert("SUM");
        functions.insert("AVG");
        functions.insert("MIN");
        functions.insert("MAX");
        functions.insert("STDDEV");
        functions.insert("STDDEV_POP");
        functions.insert("STDDEV_SAMP");
        functions.insert("VAR_POP");
        functions.insert("VAR_SAMP");
        functions.insert("VARIANCE");
        functions.insert("MEDIAN");
        functions.insert("PERCENTILE");
        functions.insert("GROUP_CONCAT");
        functions.insert("ARRAY_AGG");
        functions.insert("STRING_AGG");
        functions
    }

    /// Check if an expression contains aggregate functions
    pub fn contains_aggregate(expr: &Expr) -> bool {
        Self::check_for_aggregate(expr).contains_aggregate
    }

    /// Extract field names from an expression that are NOT inside aggregate functions
    pub fn extract_non_aggregated_fields(expr: &Expr) -> HashSet<String> {
        let mut fields = HashSet::new();
        Self::collect_non_aggregated_fields(expr, &mut fields, false);
        fields
    }

    /// Check if GROUP BY is complete for a SELECT expression
    ///
    /// If there's a GROUP BY clause, all non-aggregated fields in SELECT must be in GROUP BY
    pub fn validate_group_by_completeness(
        select_exprs: &[Expr],
        group_by: Option<&[String]>,
        context_name: &str,
    ) -> Result<(), SqlError> {
        // If no GROUP BY, all columns must use aggregates
        let Some(group_by_fields) = group_by else {
            // Check that all expressions use aggregates (if any are present)
            for expr in select_exprs {
                if !Self::contains_aggregate(expr) && !Self::is_constant_expression(expr) {
                    let fields = Self::extract_non_aggregated_fields(expr);
                    if !fields.is_empty() {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "Non-aggregated field '{}' in {} without GROUP BY clause",
                                fields.iter().next().unwrap_or(&"unknown".to_string()),
                                context_name
                            ),
                            query: None,
                        });
                    }
                }
            }
            return Ok(());
        };

        let group_by_set: HashSet<String> = group_by_fields.iter().cloned().collect();

        // Check each SELECT expression
        for expr in select_exprs {
            // Extract non-aggregated field references
            let non_agg_fields = Self::extract_non_aggregated_fields(expr);

            // All non-aggregated fields must be in GROUP BY
            for field in non_agg_fields {
                if !group_by_set.contains(&field) {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "Column '{}' in {} must be in GROUP BY clause or used in an aggregate function",
                            field, context_name
                        ),
                        query: None,
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate that aggregate functions don't appear in WHERE or ORDER BY
    pub fn validate_aggregate_placement(
        select_exprs: &[Expr],
        where_expr: Option<&Expr>,
        order_by_exprs: Option<&[Expr]>,
    ) -> Result<(), SqlError> {
        // Check WHERE clause
        if let Some(where_clause) = where_expr {
            if Self::contains_aggregate(where_clause) {
                return Err(SqlError::ExecutionError {
                    message: "Aggregate functions are not allowed in WHERE clause".to_string(),
                    query: None,
                });
            }
        }

        // Check ORDER BY clause (if provided separately)
        if let Some(order_by) = order_by_exprs {
            for expr in order_by {
                if Self::contains_aggregate(expr) {
                    return Err(SqlError::ExecutionError {
                        message: "Aggregate functions are not allowed in ORDER BY clause"
                            .to_string(),
                        query: None,
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate aggregation function usage in SELECT and HAVING
    pub fn validate_aggregation_usage(
        select_exprs: &[Expr],
        group_by: Option<&[String]>,
        having: Option<&Expr>,
        where_clause: Option<&Expr>,
        order_by_exprs: Option<&[Expr]>,
    ) -> Result<(), SqlError> {
        // First check placement rules
        Self::validate_aggregate_placement(select_exprs, where_clause, order_by_exprs)?;

        // Then check GROUP BY completeness
        Self::validate_group_by_completeness(select_exprs, group_by, "SELECT clause")?;

        // If there's a HAVING clause, validate it
        if let Some(having_expr) = having {
            // HAVING can only reference fields in GROUP BY or aggregates
            if let Some(group_by_fields) = group_by {
                let group_by_set: HashSet<String> = group_by_fields.iter().cloned().collect();
                let non_agg_fields = Self::extract_non_aggregated_fields(having_expr);

                for field in non_agg_fields {
                    if !group_by_set.contains(&field) {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "Column '{}' in HAVING clause must be in GROUP BY or used in aggregate",
                                field
                            ),
                            query: None,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    // ===== Private Helper Methods =====

    /// Recursively check if expression contains aggregate functions
    fn check_for_aggregate(expr: &Expr) -> AggregateInfo {
        match expr {
            Expr::Function { name, .. } => {
                let agg_funcs = Self::aggregate_functions();
                AggregateInfo {
                    contains_aggregate: agg_funcs.contains(name.to_uppercase().as_str()),
                }
            }
            Expr::WindowFunction { .. } => AggregateInfo {
                contains_aggregate: false, // Window functions are not aggregates in GROUP BY sense
            },
            Expr::BinaryOp { left, right, .. } => {
                let left_has = Self::check_for_aggregate(left).contains_aggregate;
                let right_has = Self::check_for_aggregate(right).contains_aggregate;
                AggregateInfo {
                    contains_aggregate: left_has || right_has,
                }
            }
            Expr::UnaryOp { expr, .. } => Self::check_for_aggregate(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                let expr_has = Self::check_for_aggregate(expr).contains_aggregate;
                let low_has = Self::check_for_aggregate(low).contains_aggregate;
                let high_has = Self::check_for_aggregate(high).contains_aggregate;
                AggregateInfo {
                    contains_aggregate: expr_has || low_has || high_has,
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                let has_agg = when_clauses.iter().any(|(cond, result)| {
                    Self::check_for_aggregate(cond).contains_aggregate
                        || Self::check_for_aggregate(result).contains_aggregate
                });
                let else_has = else_clause
                    .as_ref()
                    .map(|e| Self::check_for_aggregate(e).contains_aggregate)
                    .unwrap_or(false);
                AggregateInfo {
                    contains_aggregate: has_agg || else_has,
                }
            }
            Expr::List(exprs) => {
                let has_agg = exprs
                    .iter()
                    .any(|e| Self::check_for_aggregate(e).contains_aggregate);
                AggregateInfo {
                    contains_aggregate: has_agg,
                }
            }
            _ => AggregateInfo {
                contains_aggregate: false,
            },
        }
    }

    /// Collect non-aggregated field names (not inside aggregate functions)
    fn collect_non_aggregated_fields(
        expr: &Expr,
        fields: &mut HashSet<String>,
        inside_aggregate: bool,
    ) {
        match expr {
            Expr::Column(name) => {
                if !inside_aggregate {
                    fields.insert(name.clone());
                }
            }
            Expr::Function { name, args } => {
                let agg_funcs = Self::aggregate_functions();
                let is_aggregate = agg_funcs.contains(name.to_uppercase().as_str());

                for arg in args {
                    Self::collect_non_aggregated_fields(
                        arg,
                        fields,
                        inside_aggregate || is_aggregate,
                    );
                }
            }
            Expr::WindowFunction { args, .. } => {
                // Window function arguments are not affected by aggregation
                for arg in args {
                    Self::collect_non_aggregated_fields(arg, fields, inside_aggregate);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_non_aggregated_fields(left, fields, inside_aggregate);
                Self::collect_non_aggregated_fields(right, fields, inside_aggregate);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_non_aggregated_fields(expr, fields, inside_aggregate);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_non_aggregated_fields(expr, fields, inside_aggregate);
                Self::collect_non_aggregated_fields(low, fields, inside_aggregate);
                Self::collect_non_aggregated_fields(high, fields, inside_aggregate);
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (cond, result) in when_clauses {
                    Self::collect_non_aggregated_fields(cond, fields, inside_aggregate);
                    Self::collect_non_aggregated_fields(result, fields, inside_aggregate);
                }
                if let Some(else_expr) = else_clause {
                    Self::collect_non_aggregated_fields(else_expr, fields, inside_aggregate);
                }
            }
            Expr::List(exprs) => {
                for e in exprs {
                    Self::collect_non_aggregated_fields(e, fields, inside_aggregate);
                }
            }
            _ => {
                // Literals, subqueries, etc. don't contribute field names
            }
        }
    }

    /// Check if an expression is a constant (no field references)
    fn is_constant_expression(expr: &Expr) -> bool {
        Self::extract_non_aggregated_fields(expr).is_empty()
            && !matches!(expr, Expr::Subquery { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_function_detection() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![Expr::Column("id".to_string())],
        };
        assert!(AggregationValidator::contains_aggregate(&count_expr));
    }

    #[test]
    fn test_non_aggregate_function_detection() {
        let upper_expr = Expr::Function {
            name: "UPPER".to_string(),
            args: vec![Expr::Column("name".to_string())],
        };
        assert!(!AggregationValidator::contains_aggregate(&upper_expr));
    }

    #[test]
    fn test_extract_non_aggregated_fields() {
        let sum_expr = Expr::Function {
            name: "SUM".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        let fields = AggregationValidator::extract_non_aggregated_fields(&sum_expr);
        assert!(
            fields.is_empty(),
            "Fields inside aggregate should not be extracted"
        );
    }

    #[test]
    fn test_extract_fields_outside_aggregate() {
        let binary_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            right: Box::new(Expr::Function {
                name: "SUM".to_string(),
                args: vec![Expr::Column("amount".to_string())],
            }),
            op: BinaryOperator::Add,
        };
        let fields = AggregationValidator::extract_non_aggregated_fields(&binary_expr);
        assert!(
            fields.contains("id"),
            "Non-aggregated field should be extracted"
        );
        assert!(
            !fields.contains("amount"),
            "Aggregated field should not be extracted"
        );
    }

    #[test]
    fn test_aggregate_in_where_fails() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![Expr::Column("id".to_string())],
        };
        let result =
            AggregationValidator::validate_aggregate_placement(&[], Some(&count_expr), None);
        assert!(result.is_err(), "Aggregate in WHERE should fail");
    }

    #[test]
    fn test_simple_count_star() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![Expr::Column("id".to_string())],
        };
        let result =
            AggregationValidator::validate_group_by_completeness(&[count_expr], None, "SELECT");
        assert!(result.is_ok(), "COUNT(*) without GROUP BY should be valid");
    }

    #[test]
    fn test_missing_group_by_field() {
        let select_expr = Expr::Column("name".to_string());
        let group_by = vec!["id".to_string()];
        let result = AggregationValidator::validate_group_by_completeness(
            &[select_expr],
            Some(&group_by),
            "SELECT",
        );
        assert!(
            result.is_err(),
            "Non-aggregated field not in GROUP BY should fail"
        );
    }

    #[test]
    fn test_valid_group_by() {
        let select_exprs = vec![
            Expr::Column("id".to_string()),
            Expr::Function {
                name: "COUNT".to_string(),
                args: vec![Expr::Column("id".to_string())],
            },
        ];
        let group_by = vec!["id".to_string()];
        let result = AggregationValidator::validate_group_by_completeness(
            &select_exprs,
            Some(&group_by),
            "SELECT",
        );
        assert!(result.is_ok(), "Valid GROUP BY should pass");
    }
}
