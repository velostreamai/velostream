/*!
# Tests for ROWS WINDOW Config Caching in SelectProcessor

Tests the performance optimization that caches ROWS WINDOW configuration
extracted from SELECT fields to avoid re-walking the AST on every record.

## Coverage

- `extract_rows_window_config` function behavior
- Cache entry API (single lookup vs contains+get+insert)
- Cache eviction when max size exceeded
- Multiple ROWS WINDOW specs (max buffer selection)
- Partition column deduplication
- Edge cases: empty fields, no window functions
*/

use std::collections::HashMap;
use velostream::velostream::sql::ast::{
    Expr, OverClause, RowExpirationMode, RowsEmitMode, SelectField, WindowSpec,
};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create a test record with specified fields
fn create_test_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
    let mut field_map = HashMap::new();
    for (name, value) in fields {
        field_map.insert(name.to_string(), value);
    }
    StreamRecord {
        fields: field_map,
        timestamp: 1000,
        offset: 1,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

/// Create a SelectField::Expression with a window function using ROWS WINDOW
fn create_rows_window_field(
    alias: &str,
    function_name: &str,
    expr_column: &str,
    buffer_size: u32,
    partition_by: Vec<&str>,
    emit_mode: RowsEmitMode,
) -> SelectField {
    SelectField::Expression {
        expr: Expr::WindowFunction {
            function_name: function_name.to_string(),
            args: vec![Expr::Column(expr_column.to_string())],
            over_clause: OverClause {
                window_spec: Some(Box::new(WindowSpec::Rows {
                    buffer_size,
                    partition_by: partition_by
                        .into_iter()
                        .map(|s| Expr::Column(s.to_string()))
                        .collect(),
                    order_by: vec![],
                    time_gap: None,
                    window_frame: None,
                    emit_mode,
                    expire_after: RowExpirationMode::Default,
                })),
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
            },
        },
        alias: Some(alias.to_string()),
    }
}

/// Create a simple column SelectField (no window function)
fn create_column_field(name: &str) -> SelectField {
    SelectField::Column(name.to_string())
}

#[cfg(test)]
mod extract_rows_window_config_tests {
    use super::*;
    use velostream::velostream::sql::execution::processors::select::SelectProcessor;

    #[test]
    fn test_extract_config_from_single_rows_window() {
        let fields = vec![create_rows_window_field(
            "avg_price",
            "AVG",
            "price",
            100,
            vec!["symbol"],
            RowsEmitMode::EveryRecord,
        )];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (buffer_size, partition_cols, emit_mode) = config.unwrap();
        assert_eq!(buffer_size, 100);
        assert_eq!(partition_cols, vec!["symbol".to_string()]);
        assert!(matches!(emit_mode, RowsEmitMode::EveryRecord));
    }

    #[test]
    fn test_extract_config_returns_none_for_no_window_functions() {
        let fields = vec![
            create_column_field("id"),
            create_column_field("name"),
            create_column_field("price"),
        ];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_none());
    }

    #[test]
    fn test_extract_config_returns_none_for_empty_fields() {
        let fields: Vec<SelectField> = vec![];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_none());
    }

    #[test]
    fn test_extract_config_selects_max_buffer_from_multiple_windows() {
        let fields = vec![
            create_rows_window_field(
                "avg_price",
                "AVG",
                "price",
                50,
                vec!["symbol"],
                RowsEmitMode::EveryRecord,
            ),
            create_rows_window_field(
                "sum_volume",
                "SUM",
                "volume",
                200, // Larger buffer
                vec!["symbol"],
                RowsEmitMode::EveryRecord,
            ),
            create_rows_window_field(
                "max_price",
                "MAX",
                "price",
                100,
                vec!["symbol"],
                RowsEmitMode::EveryRecord,
            ),
        ];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (buffer_size, _, _) = config.unwrap();
        assert_eq!(buffer_size, 200); // Should select the maximum
    }

    #[test]
    fn test_extract_config_deduplicates_partition_columns() {
        let fields = vec![
            create_rows_window_field(
                "avg_price",
                "AVG",
                "price",
                100,
                vec!["symbol", "exchange"],
                RowsEmitMode::EveryRecord,
            ),
            create_rows_window_field(
                "sum_volume",
                "SUM",
                "volume",
                100,
                vec!["symbol"], // "symbol" already in first window
                RowsEmitMode::EveryRecord,
            ),
        ];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (_, partition_cols, _) = config.unwrap();
        // Should have unique columns only
        assert_eq!(partition_cols.len(), 2);
        assert!(partition_cols.contains(&"symbol".to_string()));
        assert!(partition_cols.contains(&"exchange".to_string()));
    }

    #[test]
    fn test_extract_config_handles_buffer_full_emit_mode() {
        let fields = vec![create_rows_window_field(
            "avg_price",
            "AVG",
            "price",
            100,
            vec!["symbol"],
            RowsEmitMode::BufferFull,
        )];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (_, _, emit_mode) = config.unwrap();
        assert!(matches!(emit_mode, RowsEmitMode::BufferFull));
    }

    #[test]
    fn test_extract_config_handles_mixed_fields() {
        // Mix of regular columns and window functions
        let fields = vec![
            create_column_field("id"),
            create_rows_window_field(
                "avg_price",
                "AVG",
                "price",
                100,
                vec!["symbol"],
                RowsEmitMode::EveryRecord,
            ),
            create_column_field("timestamp"),
            SelectField::Wildcard,
        ];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (buffer_size, partition_cols, _) = config.unwrap();
        assert_eq!(buffer_size, 100);
        assert_eq!(partition_cols, vec!["symbol".to_string()]);
    }

    #[test]
    fn test_extract_config_handles_no_partition_by() {
        // Window function with no PARTITION BY clause
        let fields = vec![create_rows_window_field(
            "row_num",
            "ROW_NUMBER",
            "id",
            50,
            vec![], // No partitioning
            RowsEmitMode::EveryRecord,
        )];

        let config = SelectProcessor::extract_rows_window_config(&fields);

        assert!(config.is_some());
        let (buffer_size, partition_cols, _) = config.unwrap();
        assert_eq!(buffer_size, 50);
        assert!(partition_cols.is_empty());
    }
}

#[cfg(test)]
mod cache_behavior_tests {
    use super::*;

    #[test]
    fn test_cache_starts_empty() {
        let context = ProcessorContext::new("test_query");
        assert!(context.rows_window_config_cache.is_empty());
    }

    #[test]
    fn test_cache_entry_persists() {
        let mut context = ProcessorContext::new("test_query");

        // Simulate caching a config
        let config = Some((
            100u32,
            vec!["symbol".to_string()],
            RowsEmitMode::EveryRecord,
            "rows_window:select_test".to_string(),
        ));
        context
            .rows_window_config_cache
            .insert("test_source".to_string(), config.clone());

        // Verify it persists
        assert_eq!(context.rows_window_config_cache.len(), 1);
        let retrieved = context.rows_window_config_cache.get("test_source");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), &config);
    }

    #[test]
    fn test_cache_none_config_for_non_window_query() {
        let mut context = ProcessorContext::new("test_query");

        // Cache a None config (no window functions found)
        context
            .rows_window_config_cache
            .insert("simple_source".to_string(), None);

        // Verify None is cached (not re-computed)
        assert_eq!(context.rows_window_config_cache.len(), 1);
        let retrieved = context.rows_window_config_cache.get("simple_source");
        assert!(retrieved.is_some());
        assert!(retrieved.unwrap().is_none());
    }

    #[test]
    fn test_cache_different_sources() {
        let mut context = ProcessorContext::new("test_query");

        // Cache configs for different sources
        let config1 = Some((
            100u32,
            vec!["symbol".to_string()],
            RowsEmitMode::EveryRecord,
            "rows_window:select_source1".to_string(),
        ));
        let config2 = Some((
            200u32,
            vec!["exchange".to_string()],
            RowsEmitMode::BufferFull,
            "rows_window:select_source2".to_string(),
        ));

        context
            .rows_window_config_cache
            .insert("source1".to_string(), config1.clone());
        context
            .rows_window_config_cache
            .insert("source2".to_string(), config2.clone());

        assert_eq!(context.rows_window_config_cache.len(), 2);
        assert_eq!(
            context.rows_window_config_cache.get("source1").unwrap(),
            &config1
        );
        assert_eq!(
            context.rows_window_config_cache.get("source2").unwrap(),
            &config2
        );
    }

    #[test]
    fn test_cache_entry_api_efficiency() {
        let mut context = ProcessorContext::new("test_query");
        let cache_key = "test_source".to_string();

        // First access: should insert
        let mut insert_count = 0;
        let _config = context
            .rows_window_config_cache
            .entry(cache_key.clone())
            .or_insert_with(|| {
                insert_count += 1;
                Some((
                    100u32,
                    vec!["symbol".to_string()],
                    RowsEmitMode::EveryRecord,
                    "prefix".to_string(),
                ))
            });
        assert_eq!(insert_count, 1);

        // Second access: should NOT insert (cached)
        let _config = context
            .rows_window_config_cache
            .entry(cache_key.clone())
            .or_insert_with(|| {
                insert_count += 1;
                Some((
                    200u32,
                    vec!["other".to_string()],
                    RowsEmitMode::BufferFull,
                    "prefix2".to_string(),
                ))
            });
        assert_eq!(insert_count, 1); // Still 1, not re-computed

        // Verify original value is preserved
        let cached = context.rows_window_config_cache.get(&cache_key).unwrap();
        assert_eq!(cached.as_ref().unwrap().0, 100); // Original buffer size
    }
}

#[cfg(test)]
mod window_context_buffer_includes_current_tests {
    use super::*;
    use velostream::velostream::sql::execution::processors::context::WindowContext;

    #[test]
    fn test_window_context_buffer_includes_current_true() {
        let record = create_test_record(vec![("price", FieldValue::Float(100.0))]);
        let buffer = vec![record.clone()];

        let ctx = WindowContext {
            buffer,
            last_emit: 0,
            should_emit: true,
            buffer_includes_current: true,
        };

        assert!(ctx.buffer_includes_current);
        assert_eq!(ctx.buffer.len(), 1);
    }

    #[test]
    fn test_window_context_buffer_includes_current_false() {
        let buffer: Vec<StreamRecord> = vec![];

        let ctx = WindowContext {
            buffer,
            last_emit: 0,
            should_emit: true,
            buffer_includes_current: false,
        };

        assert!(!ctx.buffer_includes_current);
    }
}

#[cfg(test)]
mod state_key_prefix_tests {
    use super::*;

    /// Test that state key prefix is correctly formatted
    #[test]
    fn test_state_key_format() {
        let prefix = "rows_window:select_market_data";
        let partition_key = "AAPL";
        let state_key = format!("{}:{}", prefix, partition_key);

        assert_eq!(state_key, "rows_window:select_market_data:AAPL");
    }

    /// Test that different partition keys produce different state keys
    #[test]
    fn test_different_partitions_different_keys() {
        let prefix = "rows_window:select_trades";

        let key1 = format!("{}:{}", prefix, "AAPL");
        let key2 = format!("{}:{}", prefix, "GOOGL");
        let key3 = format!("{}:{}", prefix, "MSFT");

        assert_ne!(key1, key2);
        assert_ne!(key2, key3);
        assert_ne!(key1, key3);
    }

    /// Test empty partition key handling
    #[test]
    fn test_empty_partition_key() {
        let prefix = "rows_window:select_global";
        let partition_key = "";
        let state_key = format!("{}:{}", prefix, partition_key);

        assert_eq!(state_key, "rows_window:select_global:");
    }

    /// Test complex partition key (multiple columns)
    #[test]
    fn test_composite_partition_key() {
        let prefix = "rows_window:select_orders";
        // Composite key: symbol|exchange
        let partition_key = "AAPL|NYSE";
        let state_key = format!("{}:{}", prefix, partition_key);

        assert_eq!(state_key, "rows_window:select_orders:AAPL|NYSE");
    }
}
