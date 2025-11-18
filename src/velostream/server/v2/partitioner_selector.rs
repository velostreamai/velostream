//! Automatic partitioner selection based on query analysis
//!
//! This module analyzes a StreamingQuery's AST to automatically select the best
//! partitioning strategy for optimal performance.
//!
//! ## Strategy Selection Logic
//!
//! The selector follows these rules:
//!
//! 1. **Window functions with ORDER BY**: Use `StickyPartition` by timestamp
//!    - Prevents out-of-order buffering overhead (20-25ms per batch)
//!    - Essential for ROWS WINDOW, TUMBLING WINDOW with ORDER BY
//!
//! 2. **GROUP BY with no ORDER BY**: Use `AlwaysHash` by GROUP BY keys
//!    - Perfect cache locality for aggregation
//!    - Groups related records together
//!
//! 3. **Pure SELECT (no aggregation)**: Use `AlwaysHash`
//!    - Any key works fine, stateless processing
//!    - Maximum parallelism with zero contention
//!
//! 4. **Default**: Use source partition (`SmartRepartition`)
//!    - Respects Kafka topic partitions
//!    - Maintains producer-consumer alignment
//!
//! ## Performance Impact
//!
//! Correct strategy selection can provide 5-20x throughput improvements:
//! - Scenario 0 (Pure SELECT): 2.27x faster with Hash
//! - Scenario 1 (ROWS WINDOW): 2.6x faster with Sticky (vs current Hash)
//! - Scenario 2 (GROUP BY): 2.42x faster with Hash (291% per-core efficiency!)
//! - Scenario 3a (TUMBLING): ~12x faster with Sticky (vs current Hash)
//! - Scenario 3b (EMIT CHANGES): 4.6x faster with Hash

use crate::velostream::sql::ast::{Expr, OrderByExpr, StreamingQuery};

/// Result of partitioner selection with reasoning
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionerSelection {
    /// Selected strategy name ("always_hash", "sticky_partition", "smart_repartition", etc.)
    pub strategy_name: String,
    /// Routing keys for the strategy (e.g., GROUP BY columns, timestamp column)
    pub routing_keys: Vec<String>,
    /// Reasoning for this selection
    pub reason: String,
    /// Whether this is the optimal strategy for this query
    pub is_optimal: bool,
}

impl PartitionerSelection {
    /// Create a new partitioner selection
    pub fn new(
        strategy_name: String,
        routing_keys: Vec<String>,
        reason: String,
        is_optimal: bool,
    ) -> Self {
        Self {
            strategy_name,
            routing_keys,
            reason,
            is_optimal,
        }
    }
}

/// Analyzes a streaming query to select the best partitioning strategy
///
/// # Strategy Selection (Revised - Sticky Default)
///
/// The selector now uses **StickyPartitionStrategy as the default** because:
/// 1. Every record ALWAYS has a `record.partition` field from the data source
/// 2. Using it is ZERO-COST (just a field read)
/// 3. It naturally preserves source partition affinity
/// 4. Only deviate for queries that require a different partitioning strategy
///
/// Selection rules (priority order):
/// 1. **GROUP BY without ORDER BY** → Override to Hash (better aggregation locality)
/// 2. **Window without ORDER BY** → Override to Hash (parallelization opportunity)
/// 3. **Default (everything else)** → Use Sticky (zero-cost source partition affinity)
///    - Pure SELECT
///    - Window with ORDER BY (Sticky is required anyway)
///    - Other queries
///
/// # Example
///
/// ```rust,no_run
/// use velostream::velostream::sql::ast::{Expr, StreamingQuery, StreamSource, SelectField};
/// use velostream::velostream::server::v2::PartitionerSelector;
///
/// let query = StreamingQuery::Select {
///     fields: vec![SelectField::Wildcard],
///     from: StreamSource::Stream("trades".to_string()),
///     from_alias: None,
///     joins: None,
///     where_clause: None,
///     group_by: Some(vec![
///         Expr::Column("symbol".to_string()),
///     ]),
///     having: None,
///     window: None,
///     order_by: None,
///     limit: None,
///     emit_mode: None,
///     properties: None,
///     job_mode: None,
///     batch_size: None,
///     num_partitions: None,
///     partitioning_strategy: None,
/// };
///
/// let selection = PartitionerSelector::select(&query);
/// assert_eq!(selection.strategy_name, "always_hash"); // Override: GROUP BY needs hash
/// assert_eq!(selection.routing_keys, vec!["symbol".to_string()]);
/// ```
pub struct PartitionerSelector;

impl PartitionerSelector {
    /// Analyze a streaming query and select the best partitioning strategy
    ///
    /// DEFAULT is StickyPartitionStrategy (use record.partition field).
    /// Only override if query pattern requires different strategy.
    pub fn select(query: &StreamingQuery) -> PartitionerSelection {
        match query {
            StreamingQuery::Select {
                group_by,
                order_by,
                window,
                ..
            } => {
                // Check for GROUP BY without ORDER BY → override to Hash
                if let Some(group_cols) = group_by {
                    if !group_cols.is_empty() && order_by.is_none() {
                        // GROUP BY without ORDER BY needs Hash for aggregation locality
                        return Self::select_hash_partition(Some(group_cols));
                    }
                }

                // Check for window functions without ORDER BY → override to Hash
                if let Some(window_spec) = window {
                    if order_by.is_none() {
                        // Window without ORDER BY: use Hash for parallelism opportunity
                        return Self::select_hash_partition(group_by.as_ref());
                    }
                    // Window with ORDER BY: Sticky is already default, confirm explicitly
                    if order_by.is_some() {
                        return Self::select_sticky_partition(order_by.as_ref());
                    }
                }

                // Check for window functions in SELECT expressions without ORDER BY
                if Self::has_window_functions(query) && order_by.is_none() {
                    return Self::select_hash_partition(group_by.as_ref());
                }

                // DEFAULT: StickyPartitionStrategy (use record.partition field)
                // This applies to:
                // - Pure SELECT (no aggregation or windowing)
                // - Window with ORDER BY (Sticky is required)
                // - Other queries
                Self::select_sticky_partition_default()
            }
            _ => {
                // For other query types (CREATE STREAM, CREATE TABLE, etc.),
                // use sticky partition to align with source partitions
                Self::select_sticky_partition_default()
            }
        }
    }

    /// Select default Sticky partition strategy (use record.partition field)
    ///
    /// This is the default for all queries unless explicitly overridden.
    /// Zero-cost: just reads the partition field provided by the data source.
    fn select_sticky_partition_default() -> PartitionerSelection {
        PartitionerSelection::new(
            "sticky_partition".to_string(),
            vec!["record.partition".to_string()],
            "Sticky partition (default): use source partition field from record (zero-cost affinity)"
                .to_string(),
            true,
        )
    }

    /// Select Sticky partition strategy based on ORDER BY columns
    fn select_sticky_partition(order_by: Option<&Vec<OrderByExpr>>) -> PartitionerSelection {
        if let Some(order_exprs) = order_by {
            // Extract column names from ORDER BY expressions
            let routing_keys = order_exprs
                .iter()
                .filter_map(|expr| Self::extract_column_name(&expr.expr))
                .collect::<Vec<_>>();

            if !routing_keys.is_empty() {
                // Check if it's a timestamp column
                let is_timestamp = routing_keys
                    .iter()
                    .any(|k| k.contains("time") || k.contains("timestamp") || k.contains("date"));

                if is_timestamp {
                    return PartitionerSelection::new(
                        "sticky_partition".to_string(),
                        routing_keys,
                        "Sticky partition by timestamp: preserves ordering for window functions"
                            .to_string(),
                        true,
                    );
                }
            }
        }

        // Fallback for ORDER BY without clear timestamp
        PartitionerSelection::new(
            "sticky_partition".to_string(),
            vec!["order_by_key".to_string()],
            "Sticky partition by ORDER BY key: prevents out-of-order buffering".to_string(),
            true,
        )
    }

    /// Select Hash partition strategy
    fn select_hash_partition(group_by: Option<&Vec<Expr>>) -> PartitionerSelection {
        if let Some(group_cols) = group_by {
            if !group_cols.is_empty() {
                let routing_keys = group_cols
                    .iter()
                    .filter_map(Self::extract_column_name)
                    .collect::<Vec<_>>();

                if !routing_keys.is_empty() {
                    return PartitionerSelection::new(
                        "always_hash".to_string(),
                        routing_keys,
                        "Hash partition by GROUP BY keys: groups related records for aggregation"
                            .to_string(),
                        true,
                    );
                }
            }
        }

        // Pure SELECT or no aggregation keys found
        PartitionerSelection::new(
            "always_hash".to_string(),
            vec!["any_key".to_string()],
            "Hash partition with any key: stateless operation, perfect parallelism".to_string(),
            true,
        )
    }

    /// Check if query contains window functions
    fn has_window_functions(query: &StreamingQuery) -> bool {
        match query {
            StreamingQuery::Select { fields, .. } => fields.iter().any(|f| {
                use crate::velostream::sql::ast::SelectField;
                match f {
                    SelectField::Expression { expr, .. } => Self::expr_has_window_function(expr),
                    _ => false,
                }
            }),
            _ => false,
        }
    }

    /// Check if an expression contains window functions
    fn expr_has_window_function(expr: &Expr) -> bool {
        match expr {
            Expr::WindowFunction { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_window_function(left) || Self::expr_has_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_window_function(expr),
            Expr::Function { args, .. } => args.iter().any(Self::expr_has_window_function),
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                when_clauses.iter().any(|(cond, then_expr)| {
                    Self::expr_has_window_function(cond)
                        || Self::expr_has_window_function(then_expr)
                }) || else_clause
                    .as_ref()
                    .is_some_and(|e| Self::expr_has_window_function(e))
            }
            _ => false,
        }
    }

    /// Extract column name from an expression
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => Some(name.clone()),
            Expr::BinaryOp { left, .. } => Self::extract_column_name(left),
            Expr::UnaryOp { expr, .. } => Self::extract_column_name(expr),
            Expr::Function { name, args } => {
                // For functions like EXTRACT(YEAR FROM timestamp), extract timestamp
                if !args.is_empty() {
                    Self::extract_column_name(&args[0])
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::ast::{
        OrderByExpr, OrderDirection, SelectField, StreamSource, WindowSpec,
    };
    use std::time::Duration;

    #[test]
    fn test_pure_select_uses_sticky_default() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("orders".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "sticky_partition"); // NEW: default is sticky
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_group_by_without_order_uses_hash() {
        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Column("symbol".to_string()),
                SelectField::Expression {
                    expr: Expr::Function {
                        name: "COUNT".to_string(),
                        args: vec![Expr::Literal(
                            crate::velostream::sql::ast::LiteralValue::Integer(1),
                        )],
                    },
                    alias: Some("count".to_string()),
                },
            ],
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![Expr::Column("symbol".to_string())]),
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "always_hash");
        assert_eq!(selection.routing_keys, vec!["symbol"]);
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_rows_window_with_order_by_uses_sticky() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Column("symbol".to_string())],
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![Expr::Column("symbol".to_string())]),
            having: None,
            window: Some(WindowSpec::Rows {
                buffer_size: 100,
                partition_by: vec![Expr::Column("symbol".to_string())],
                order_by: vec![OrderByExpr {
                    expr: Expr::Column("timestamp".to_string()),
                    direction: OrderDirection::Asc,
                }],
                time_gap: None,
                window_frame: None,
                emit_mode: crate::velostream::sql::ast::RowsEmitMode::EveryRecord,
                expire_after: crate::velostream::sql::ast::RowExpirationMode::Default,
            }),
            order_by: Some(vec![OrderByExpr {
                expr: Expr::Column("timestamp".to_string()),
                direction: OrderDirection::Asc,
            }]),
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "sticky_partition");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_tumbling_window_with_order_by_uses_sticky() {
        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Column("trader_id".to_string()),
                SelectField::Column("symbol".to_string()),
            ],
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![
                Expr::Column("trader_id".to_string()),
                Expr::Column("symbol".to_string()),
            ]),
            having: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: Some("event_time".to_string()),
            }),
            order_by: Some(vec![OrderByExpr {
                expr: Expr::Column("event_time".to_string()),
                direction: OrderDirection::Asc,
            }]),
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "sticky_partition");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_window_without_order_by_uses_hash() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Column("symbol".to_string())],
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![Expr::Column("symbol".to_string())]),
            having: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: None,
            }),
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "always_hash");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_complex_order_by_extracts_column() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("trades".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: Some("trade_time".to_string()),
            }),
            order_by: Some(vec![OrderByExpr {
                expr: Expr::Column("trade_time".to_string()),
                direction: OrderDirection::Desc,
            }]),
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "sticky_partition");
        assert!(selection.routing_keys.contains(&"trade_time".to_string()));
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Column("trader_id".to_string()),
                SelectField::Column("symbol".to_string()),
            ],
            from: StreamSource::Stream("trades".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![
                Expr::Column("trader_id".to_string()),
                Expr::Column("symbol".to_string()),
            ]),
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let selection = PartitionerSelector::select(&query);
        assert_eq!(selection.strategy_name, "always_hash");
        assert!(!selection.routing_keys.is_empty());
        assert!(
            selection
                .routing_keys
                .iter()
                .any(|k| k == "trader_id" || k == "symbol")
        );
    }
}
