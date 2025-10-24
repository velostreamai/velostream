//! SQL Semantic Validation
//!
//! This module provides semantic validation for SQL queries, checking constraints that go
//! beyond syntax (which is handled by the parser). It ensures queries follow SQL semantics
//! and constraints necessary for proper execution.
//!
//! ## Validators
//!
//! - **AggregateValidator**: Validates aggregate function usage contexts
//!   - Ensures aggregate functions (AVG, SUM, COUNT, STDDEV, MAX, MIN) are only used
//!     with GROUP BY or WINDOW clauses
//!   - Prevents undefined behavior from aggregates without grouping context

use crate::velostream::sql::ast::{BinaryOperator, Expr, SelectField, StreamingQuery};
use crate::velostream::sql::error::SqlError;

/// Validates aggregate function usage in SQL queries.
///
/// Aggregate functions require state accumulation across multiple records and therefore
/// cannot be evaluated on individual records. They are only valid in these contexts:
/// 1. SELECT with GROUP BY clause (implicit grouping by key)
/// 2. SELECT with WINDOW clause (implicit grouping by window)
/// 3. As part of scalar subqueries or other valid contexts
///
/// Invalid: `SELECT AVG(price) FROM orders` (no grouping)
/// Valid:   `SELECT symbol, AVG(price) FROM orders GROUP BY symbol` (with grouping)
/// Valid:   `SELECT symbol, AVG(price) FROM orders WINDOW TUMBLING(1m)` (with window)
pub struct AggregateValidator;

impl AggregateValidator {
    /// Validate that aggregate functions only appear in valid contexts.
    ///
    /// Returns `Err(SqlError::AggregateWithoutGrouping)` if aggregate functions
    /// are found without GROUP BY or WINDOW clause context.
    pub fn validate(query: &StreamingQuery) -> Result<(), SqlError> {
        match query {
            StreamingQuery::Select {
                fields,
                group_by,
                window,
                ..
            } => {
                // Extract all function calls from SELECT fields
                let aggregates = Self::extract_aggregate_functions(fields);

                // Aggregate functions ONLY valid if:
                // 1. GROUP BY is present (explicit grouping), OR
                // 2. WINDOW is present (implicit grouping)
                let has_grouping = group_by.is_some() || window.is_some();

                if !aggregates.is_empty() && !has_grouping {
                    return Err(SqlError::AggregateWithoutGrouping {
                        functions: aggregates,
                        suggestion:
                            "Add GROUP BY clause or WINDOW clause to provide grouping context for aggregate functions"
                                .to_string(),
                    });
                }

                Ok(())
            }
            // Other query types don't have aggregates in the same way
            _ => Ok(()),
        }
    }

    /// Extract all aggregate function names from SELECT fields.
    ///
    /// Returns a sorted, deduplicated list of aggregate function names found.
    fn extract_aggregate_functions(fields: &[SelectField]) -> Vec<String> {
        let mut aggregates = std::collections::HashSet::new();

        for field in fields {
            match field {
                SelectField::Expression { expr, .. } => {
                    Self::collect_aggregates_from_expr(expr, &mut aggregates);
                }
                SelectField::Column(_) | SelectField::AliasedColumn { .. } => {
                    // Columns don't have aggregates
                }
                SelectField::Wildcard => {
                    // Wildcard doesn't have aggregates
                }
            }
        }

        let mut result: Vec<String> = aggregates.into_iter().collect();
        result.sort();
        result
    }

    /// Recursively collect aggregate function names from an expression.
    fn collect_aggregates_from_expr(
        expr: &Expr,
        aggregates: &mut std::collections::HashSet<String>,
    ) {
        match expr {
            Expr::Function { name, args } => {
                // Check if this is an aggregate function
                if Self::is_aggregate_function(name) {
                    aggregates.insert(name.to_uppercase());
                }

                // Recursively check arguments
                for arg in args {
                    Self::collect_aggregates_from_expr(arg, aggregates);
                }
            }
            Expr::WindowFunction {
                function_name,
                args,
                ..
            } => {
                // Window functions are already grouped, so no need to check
                // But still recursively check arguments for nested aggregates
                for arg in args {
                    Self::collect_aggregates_from_expr(arg, aggregates);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates_from_expr(left, aggregates);
                Self::collect_aggregates_from_expr(right, aggregates);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_aggregates_from_expr(expr, aggregates);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_aggregates_from_expr(expr, aggregates);
                Self::collect_aggregates_from_expr(low, aggregates);
                Self::collect_aggregates_from_expr(high, aggregates);
            }
            Expr::List(exprs) => {
                for e in exprs {
                    Self::collect_aggregates_from_expr(e, aggregates);
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (cond, result) in when_clauses {
                    Self::collect_aggregates_from_expr(cond, aggregates);
                    Self::collect_aggregates_from_expr(result, aggregates);
                }
                if let Some(else_expr) = else_clause {
                    Self::collect_aggregates_from_expr(else_expr, aggregates);
                }
            }
            Expr::Subquery { .. } => {
                // Subqueries have their own validation context
            }
            // Leaf expressions (literals, columns) don't contain aggregates
            Expr::Column(_) | Expr::Literal(_) => {}
        }
    }

    /// Check if a function name is an aggregate function.
    fn is_aggregate_function(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "STDDEV"
                | "VARIANCE"
                | "COLLECT_LIST"
                | "COLLECT_SET"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::ast::{
        Expr, LiteralValue, SelectField, StreamSource, StreamingQuery, WindowSpec,
    };
    use std::time::Duration;

    fn create_select_with_aggregates(
        fields: Vec<SelectField>,
        group_by: Option<Vec<Expr>>,
        window: Option<WindowSpec>,
    ) -> StreamingQuery {
        StreamingQuery::Select {
            fields,
            from: StreamSource::Stream("test_stream".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by,
            having: None,
            window,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
        }
    }

    #[test]
    fn test_avg_without_group_by_fails() {
        let fields = vec![SelectField::Expression {
            expr: Expr::Function {
                name: "AVG".to_string(),
                args: vec![Expr::Column("price".to_string())],
            },
            alias: None,
        }];

        let query = create_select_with_aggregates(fields, None, None);
        let result = AggregateValidator::validate(&query);

        assert!(matches!(
            result,
            Err(SqlError::AggregateWithoutGrouping { functions, .. }) if functions.contains(&"AVG".to_string())
        ));
    }

    #[test]
    fn test_avg_with_group_by_succeeds() {
        let fields = vec![SelectField::Expression {
            expr: Expr::Function {
                name: "AVG".to_string(),
                args: vec![Expr::Column("price".to_string())],
            },
            alias: None,
        }];

        let group_by = Some(vec![Expr::Column("symbol".to_string())]);
        let query = create_select_with_aggregates(fields, group_by, None);
        let result = AggregateValidator::validate(&query);

        assert!(result.is_ok());
    }

    #[test]
    fn test_avg_with_window_succeeds() {
        let fields = vec![SelectField::Expression {
            expr: Expr::Function {
                name: "AVG".to_string(),
                args: vec![Expr::Column("price".to_string())],
            },
            alias: None,
        }];

        let window = Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: None,
        });
        let query = create_select_with_aggregates(fields, None, window);
        let result = AggregateValidator::validate(&query);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_aggregates_without_grouping_fails() {
        let fields = vec![
            SelectField::Expression {
                expr: Expr::Function {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("price".to_string())],
                },
                alias: None,
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("volume".to_string())],
                },
                alias: None,
            },
        ];

        let query = create_select_with_aggregates(fields, None, None);
        let result = AggregateValidator::validate(&query);

        assert!(matches!(
            result,
            Err(SqlError::AggregateWithoutGrouping { functions, .. }) if {
                functions.contains(&"AVG".to_string()) && functions.contains(&"SUM".to_string())
            }
        ));
    }

    #[test]
    fn test_non_aggregate_functions_succeed() {
        let fields = vec![SelectField::Expression {
            expr: Expr::Function {
                name: "UPPER".to_string(),
                args: vec![Expr::Column("name".to_string())],
            },
            alias: None,
        }];

        let query = create_select_with_aggregates(fields, None, None);
        let result = AggregateValidator::validate(&query);

        assert!(result.is_ok());
    }

    #[test]
    fn test_count_without_grouping_fails() {
        let fields = vec![SelectField::Expression {
            expr: Expr::Function {
                name: "COUNT".to_string(),
                args: vec![Expr::Literal(LiteralValue::String("*".to_string()))],
            },
            alias: None,
        }];

        let query = create_select_with_aggregates(fields, None, None);
        let result = AggregateValidator::validate(&query);

        assert!(matches!(
            result,
            Err(SqlError::AggregateWithoutGrouping { functions, .. }) if functions.contains(&"COUNT".to_string())
        ));
    }

    #[test]
    fn test_nested_aggregates_detected() {
        // Test that aggregates inside nested expressions are detected
        let fields = vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Function {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("price".to_string())],
                }),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(10))),
            },
            alias: None,
        }];

        let query = create_select_with_aggregates(fields, None, None);
        let result = AggregateValidator::validate(&query);

        assert!(matches!(
            result,
            Err(SqlError::AggregateWithoutGrouping { functions, .. }) if functions.contains(&"AVG".to_string())
        ));
    }
}
