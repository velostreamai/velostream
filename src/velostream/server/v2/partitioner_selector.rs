//! Automatic partitioner selection based on query analysis
//!
//! This module analyzes a StreamingQuery's AST to automatically select the best
//! partitioning strategy for optimal performance.
//!
//! ## Strategy Selection Logic
//!
//! The selector follows these rules (priority order):
//!
//! 1. **Tumbling/Sliding/Session Windows**: Use `AlwaysHash` (data rekeying required)
//!    - Window boundaries require rekeying data by the window's implicit GROUP BY
//!    - Applies regardless of ORDER BY, GROUP BY, or any other clause
//!    - Essential for correct window boundary evaluation across partitions
//!
//! 2. **GROUP BY (without Tumbling/Sliding/Session windows)**: Use `AlwaysHash` by GROUP BY keys
//!    - Data rekeying required to group related records
//!    - Perfect cache locality for aggregation
//!    - **Note**: If source data is already partitioned by GROUP BY keys, `StickyPartition`
//!      is equally efficient (zero-cost, no repartitioning needed)
//!
//! 3. **ROWS Window or Pure SELECT**: Use `StickyPartition` (default)
//!    - ROWS window is analytic (no data rekeying needed) - operates within partitions
//!    - Pure SELECT has no aggregation or windowing - no rekeying needed
//!    - Zero-cost: just reads existing `record.partition` field
//!    - Preserves Kafka topic partition alignment
//!    - Works optimally when source data is already properly partitioned
//!
//! ## Performance Impact
//!
//! Strategy selection impact depends on source data partitioning:
//! - **Pre-partitioned data** (by GROUP BY keys): StickyPartition is zero-cost and optimal
//! - **Random/unpartitioned data**: AlwaysHash provides better locality and parallelism
//!
//! Baseline measurements with properly partitioned test data:
//! - Scenario 0 (Pure SELECT): sticky_partition optimal (zero-cost, no rekeying)
//! - Scenario 1 (ROWS WINDOW): sticky_partition optimal (analytic, no rekeying)
//! - Scenario 2 (GROUP BY): always_hash optimal (data rekeying by GROUP BY keys)
//! - Scenario 3a (TUMBLING + GROUP BY): always_hash required (window boundaries require rekeying)
//! - Scenario 3b (TUMBLING + EMIT CHANGES): always_hash required (window boundaries require rekeying)

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
/// # Strategy Selection Rules (Priority Order)
///
/// The selector defaults to **StickyPartitionStrategy** because:
/// 1. Every record ALWAYS has a `record.partition` field from the data source
/// 2. Using it is ZERO-COST (just a field read, no hash computation)
/// 3. It naturally preserves source partition affinity
/// 4. It works optimally when source data is already properly partitioned
///
/// Selection rules (checked in priority order):
///
/// ## PRIORITY 1: Tumbling/Sliding/Session Windows → **ALWAYS Hash**
/// Window boundaries require rekeying data because:
/// - Window semantics require correct grouping of records within time/session boundaries
/// - Different partitions may have records that belong in the same window
/// - Sticky partition would prevent correct window evaluation
/// - Applies regardless of ORDER BY, GROUP BY, or any other clause
///
/// ## PRIORITY 2: GROUP BY → **Hash** (unless it's with a Tumbling/Sliding/Session window, handled above)
/// Aggregation requires rekeying by GROUP BY keys because:
/// - GROUP BY semantics require correct grouping of records
/// - Different partitions may have records with the same GROUP BY key values
/// - Sticky partition would produce incorrect aggregation results
/// - Exception: If source data is already partitioned by GROUP BY keys, StickyPartition
///   is equally efficient (zero-cost, no repartitioning needed)
///
/// ## DEFAULT: Everything Else → **Sticky** (zero-cost source partition affinity)
/// Applies to:
/// - **ROWS window** (analytic function, not aggregation - operates within existing partitions)
/// - **Pure SELECT** (no aggregation or windowing - no rekeying needed)
/// - Any query where source partitioning is already optimal
///
/// **Performance Note**: If source data is already properly partitioned (e.g., Kafka topic
/// with keyed messages), StickyPartition is the best choice even for GROUP BY queries.
/// Example: Data partitioned by `(trader_id, symbol)` + Query groups by same columns
/// = StickyPartition is optimal (zero-cost, perfect cache locality)
///
/// # Example
///
/// ```rust,no_run
/// use velostream::velostream::sql::ast::{Expr, StreamingQuery, StreamSource, SelectField};
/// use velostream::velostream::server::v2::PartitionerSelector;
///
/// let query = StreamingQuery::Select {
///     fields: vec![SelectField::Wildcard],
///     key_fields: None,
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
                order_by: _,
                window,
                ..
            } => {
                // PRIORITY 1: Tumbling/Sliding/Session windows ALWAYS require rekeying
                if let Some(window_spec) = window {
                    match window_spec {
                        crate::velostream::sql::ast::WindowSpec::Rows { .. } => {
                            // ROWS window is analytic (no rekeying) - proceed to check GROUP BY
                        }
                        crate::velostream::sql::ast::WindowSpec::Tumbling { .. }
                        | crate::velostream::sql::ast::WindowSpec::Sliding { .. }
                        | crate::velostream::sql::ast::WindowSpec::Session { .. } => {
                            // Tumbling/Sliding/Session windows ALWAYS require rekeying
                            // Use GROUP BY keys if present, otherwise use hash with any key
                            return Self::select_hash_partition(group_by.as_ref());
                        }
                    }
                }

                // PRIORITY 2: GROUP BY requires rekeying by GROUP BY keys
                if let Some(group_cols) = group_by {
                    if !group_cols.is_empty() {
                        // GROUP BY present → use Hash (rekeying required)
                        return Self::select_hash_partition(Some(group_cols));
                    }
                }

                // DEFAULT: StickyPartitionStrategy (use record.partition field)
                // This applies to:
                // - Pure SELECT (no aggregation or windowing)
                // - ROWS window (analytic, no rekeying)
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
            key_fields: None,
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
            key_fields: None,
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
    fn test_rows_window_without_group_by_uses_sticky() {
        // ROWS window without GROUP BY: pure analytic function, no aggregation
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Column("symbol".to_string())],
            key_fields: None,
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None, // NO GROUP BY - pure analytic
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
        // ROWS window is analytic (no rekeying), no GROUP BY → sticky_partition
        assert_eq!(selection.strategy_name, "sticky_partition");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_rows_window_with_group_by_uses_hash() {
        // ROWS window WITH GROUP BY: GROUP BY requires rekeying
        // (Note: This is a semantically unusual query, but we handle it correctly)
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Column("symbol".to_string())],
            key_fields: None,
            from: StreamSource::Stream("market_data".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: Some(vec![Expr::Column("symbol".to_string())]), // GROUP BY present
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
        // GROUP BY requires rekeying, so always_hash (ROWS window doesn't prevent GROUP BY)
        assert_eq!(selection.strategy_name, "always_hash");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_tumbling_window_always_uses_hash_regardless_of_order_by() {
        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Column("trader_id".to_string()),
                SelectField::Column("symbol".to_string()),
            ],
            key_fields: None,
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
        // CRITICAL: Tumbling windows ALWAYS require hash, even with ORDER BY
        // Window boundaries require rekeying data regardless of ORDER BY clause
        assert_eq!(selection.strategy_name, "always_hash");
        assert!(selection.is_optimal);
    }

    #[test]
    fn test_window_without_order_by_uses_hash() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Column("symbol".to_string())],
            key_fields: None,
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
    fn test_tumbling_window_without_group_by_uses_hash() {
        // Tumbling window always requires hash, even without explicit GROUP BY
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            key_fields: None,
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
        // Tumbling window requires hash (window boundaries require rekeying)
        assert_eq!(selection.strategy_name, "always_hash");
    }

    #[test]
    fn test_rows_window_with_order_by_extracts_column() {
        // ROWS window (analytic) with ORDER BY - pure analytic operation
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            key_fields: None,
            from: StreamSource::Stream("trades".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: Some(WindowSpec::Rows {
                buffer_size: 100,
                partition_by: vec![],
                order_by: vec![OrderByExpr {
                    expr: Expr::Column("trade_time".to_string()),
                    direction: OrderDirection::Desc,
                }],
                time_gap: None,
                window_frame: None,
                emit_mode: crate::velostream::sql::ast::RowsEmitMode::EveryRecord,
                expire_after: crate::velostream::sql::ast::RowExpirationMode::Default,
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
        // ROWS window is analytic (no rekeying) → sticky_partition
        assert_eq!(selection.strategy_name, "sticky_partition");
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Column("trader_id".to_string()),
                SelectField::Column("symbol".to_string()),
            ],
            key_fields: None,
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
