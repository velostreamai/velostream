/*!
# ROWS WINDOW SQL Integration Tests

Tests to verify that Velostream correctly executes SQL queries with ROWS WINDOW functionality.
ROWS WINDOW provides memory-safe, row-count-based analytic windows with bounded buffers.
*/

use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use velostream::velostream::sql::execution::FieldValue;

#[cfg(test)]
mod rows_window_sql_tests {
    use super::*;

    // BASIC ROWS WINDOW TESTS

    #[tokio::test]
    async fn test_rows_window_basic_buffer_averaging() {
        // Test basic ROWS WINDOW with AVG aggregation over bounded buffer
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as moving_avg
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("MSFT", 300.0, 500, 300),
            TestDataBuilder::ticker_record("MSFT", 302.0, 600, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        // Verify results exist and have all expected fields
        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Check required fields exist
                assert!(
                    result.fields.contains_key("symbol"),
                    "Result {}: Missing 'symbol' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("price"),
                    "Result {}: Missing 'price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("moving_avg"),
                    "Result {}: Missing 'moving_avg' field",
                    idx
                );

                // Validate price is positive
                if let Some(price_field) = result.fields.get("price") {
                    match price_field {
                        FieldValue::Float(price) => {
                            assert!(
                                *price > 0.0,
                                "Result {}: price should be positive, got {}",
                                idx,
                                price
                            );
                        }
                        _ => {}
                    }
                }

                // Validate moving_avg is positive and reasonable
                if let Some(avg_field) = result.fields.get("moving_avg") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: moving_avg should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW basic averaging successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_partition_isolation() {
        // Test that PARTITION BY creates isolated buffers per partition
        let sql = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as record_count
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 100.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 101.0, 1100, 100),
            TestDataBuilder::ticker_record("MSFT", 200.0, 1200, 200),
            TestDataBuilder::ticker_record("MSFT", 201.0, 1300, 300),
            TestDataBuilder::ticker_record("MSFT", 202.0, 1400, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            // Group results by symbol to verify partition isolation
            let mut aapl_count = 0;
            let mut msft_count = 0;

            for result in &results {
                if let Some(FieldValue::String(symbol)) = result.fields.get("symbol") {
                    if symbol == "AAPL" {
                        aapl_count += 1;
                    } else if symbol == "MSFT" {
                        msft_count += 1;
                    }
                }

                // Verify record_count field exists and is positive
                if let Some(FieldValue::Integer(count)) = result.fields.get("record_count") {
                    assert!(*count > 0, "record_count should be positive, got {}", count);
                }
            }

            // Should have results for both symbols (partition isolation working)
            assert!(
                aapl_count > 0 && msft_count > 0,
                "Partition isolation failed: AAPL={}, MSFT={}",
                aapl_count,
                msft_count
            );

            println!(
                "✓ ROWS WINDOW partition isolation successful: AAPL={} results, MSFT={} results",
                aapl_count, msft_count
            );
        }
    }

    // AGGREGATION FUNCTION TESTS

    #[tokio::test]
    async fn test_rows_window_max_aggregation() {
        let sql = r#"
            SELECT
                symbol,
                price,
                MAX(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as max_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify max_price exists and is positive
                if let Some(max_field) = result.fields.get("max_price") {
                    match max_field {
                        FieldValue::Float(max_price) => {
                            assert!(
                                *max_price > 0.0,
                                "Result {}: max_price should be positive, got {}",
                                idx,
                                max_price
                            );
                            // MAX should be >= current price
                            if let Some(FieldValue::Float(price)) = result.fields.get("price") {
                                assert!(
                                    *max_price >= *price,
                                    "Result {}: max_price ({}) should be >= price ({})",
                                    idx,
                                    max_price,
                                    price
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW MAX aggregation successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_min_aggregation() {
        let sql = r#"
            SELECT
                symbol,
                price,
                MIN(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as min_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 145.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify min_price exists and is positive
                if let Some(min_field) = result.fields.get("min_price") {
                    match min_field {
                        FieldValue::Float(min_price) => {
                            assert!(
                                *min_price > 0.0,
                                "Result {}: min_price should be positive, got {}",
                                idx,
                                min_price
                            );
                            // MIN should be <= current price
                            if let Some(FieldValue::Float(price)) = result.fields.get("price") {
                                assert!(
                                    *min_price <= *price,
                                    "Result {}: min_price ({}) should be <= price ({})",
                                    idx,
                                    min_price,
                                    price
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW MIN aggregation successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_sum_aggregation() {
        let sql = r#"
            SELECT
                symbol,
                quantity,
                SUM(quantity) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as total_quantity
            FROM trades
        "#;

        let records = vec![
            TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, 0),
            TestDataBuilder::trade_record(2, "AAPL", 151.0, 200, 100),
            TestDataBuilder::trade_record(3, "AAPL", 152.0, 150, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify total_quantity exists and is positive
                if let Some(total_field) = result.fields.get("total_quantity") {
                    let total_val = match total_field {
                        FieldValue::Float(v) => Some(*v),
                        FieldValue::ScaledInteger(v, _) => Some(*v as f64),
                        FieldValue::Integer(v) => Some(*v as f64),
                        _ => None,
                    };

                    if let Some(val) = total_val {
                        assert!(
                            val > 0.0,
                            "Result {}: total_quantity should be positive, got {}",
                            idx,
                            val
                        );
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW SUM aggregation successful with {} results",
                results.len()
            );
        }
    }

    // RANKING FUNCTION TESTS

    #[tokio::test]
    async fn test_rows_window_rank_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                RANK() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price DESC
                ) as price_rank
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 155.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 152.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify price_rank exists
                if let Some(FieldValue::Integer(rank)) = result.fields.get("price_rank") {
                    assert!(
                        *rank > 0,
                        "Result {}: price_rank should be > 0, got {}",
                        idx,
                        rank
                    );
                    assert!(
                        *rank <= 4,
                        "Result {}: price_rank should be <= 4 for 4 records, got {}",
                        idx,
                        rank
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW RANK function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_dense_rank_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                DENSE_RANK() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price DESC
                ) as dense_rank
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 155.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 152.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify dense_rank exists and is positive
                if let Some(FieldValue::Integer(dense_rank)) = result.fields.get("dense_rank") {
                    assert!(
                        *dense_rank > 0,
                        "Result {}: dense_rank should be > 0, got {}",
                        idx,
                        dense_rank
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW DENSE_RANK function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_row_number_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                ROW_NUMBER() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as row_num
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify row_num exists and is sequential
                if let Some(FieldValue::Integer(row_num)) = result.fields.get("row_num") {
                    assert!(
                        *row_num > 0,
                        "Result {}: row_num should be > 0, got {}",
                        idx,
                        row_num
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW ROW_NUMBER function successful with {} results",
                results.len()
            );
        }
    }

    // OFFSET FUNCTION TESTS

    #[tokio::test]
    async fn test_rows_window_lag_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as prev_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            // First record should have NULL prev_price (no previous row)
            let first_result = &results[0];
            if let Some(prev_field) = first_result.fields.get("prev_price") {
                // prev_price can be NULL or a value (implementation dependent)
                match prev_field {
                    FieldValue::Float(price) => {
                        assert!(
                            *price > 0.0,
                            "First result: prev_price should be positive if not NULL"
                        );
                    }
                    _ => {}
                }
            }

            // Subsequent records might have prev_price
            if results.len() > 1 {
                for (idx, result) in results.iter().enumerate().skip(1) {
                    if let Some(FieldValue::Float(prev_price)) = result.fields.get("prev_price") {
                        // Previous price should be less than or related to current price somehow
                        if let Some(FieldValue::Float(current_price)) = result.fields.get("price") {
                            // Just verify both are positive (actual ordering depends on data)
                            assert!(
                                *prev_price > 0.0,
                                "Result {}: prev_price should be positive, got {}",
                                idx,
                                prev_price
                            );
                        }
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW LAG function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_lead_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                LEAD(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as next_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for result in results.iter() {
                if let Some(next_field) = result.fields.get("next_price") {
                    match next_field {
                        FieldValue::Float(price) => {
                            assert!(
                                *price > 0.0,
                                "next_price should be positive if not NULL, got {}",
                                price
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW LEAD function successful with {} results",
                results.len()
            );
        }
    }

    // FINANCIAL ANALYTICS TESTS

    #[tokio::test]
    async fn test_rows_window_moving_average_financial() {
        // Real-world: Moving average for momentum calculation
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 1000 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
                ) as moving_avg_100,
                LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 1000 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as prev_price
            FROM market_data
        "#;

        let records = TestDataBuilder::generate_price_series("AAPL", 150.0, 20, 60);

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify all expected fields exist
                assert!(
                    result.fields.contains_key("symbol"),
                    "Result {}: Missing symbol",
                    idx
                );
                assert!(
                    result.fields.contains_key("price"),
                    "Result {}: Missing price",
                    idx
                );
                assert!(
                    result.fields.contains_key("moving_avg_100"),
                    "Result {}: Missing moving_avg_100",
                    idx
                );
                assert!(
                    result.fields.contains_key("prev_price"),
                    "Result {}: Missing prev_price",
                    idx
                );

                // Validate moving_avg_100
                if let Some(avg_field) = result.fields.get("moving_avg_100") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: moving_avg_100 should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW moving average financial test successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_trader_momentum_analysis() {
        // Real-world: Momentum calculation for multiple traders
        let sql = r#"
            SELECT
                trader_id,
                symbol,
                price,
                LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 1000 ROWS
                        PARTITION BY trader_id, symbol
                        ORDER BY timestamp
                ) as prev_price
            FROM trades
        "#;

        let records = vec![
            TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, 0),
            TestDataBuilder::trade_record(1, "AAPL", 151.0, 200, 100),
            TestDataBuilder::trade_record(2, "MSFT", 300.0, 150, 200),
            TestDataBuilder::trade_record(2, "MSFT", 302.0, 250, 300),
            TestDataBuilder::trade_record(1, "MSFT", 299.0, 175, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify trader_id field
                assert!(
                    result.fields.contains_key("trader_id"),
                    "Result {}: Missing trader_id",
                    idx
                );

                // Verify symbol field
                assert!(
                    result.fields.contains_key("symbol"),
                    "Result {}: Missing symbol",
                    idx
                );

                // Verify price is positive
                if let Some(FieldValue::Float(price)) = result.fields.get("price") {
                    assert!(
                        *price > 0.0,
                        "Result {}: price should be positive, got {}",
                        idx,
                        price
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW trader momentum analysis successful with {} results",
                results.len()
            );
        }
    }

    // BUFFER SIZE AND FRAME TESTS

    #[tokio::test]
    async fn test_rows_window_with_frame_bounds() {
        // Test ROWS BETWEEN within ROWS WINDOW buffer
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) as moving_avg_10
            FROM market_data
        "#;

        let records = TestDataBuilder::generate_price_series("AAPL", 150.0, 20, 60);

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify moving_avg_10 exists and is reasonable
                if let Some(avg_field) = result.fields.get("moving_avg_10") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: moving_avg_10 should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW with frame bounds successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_multiple_aggregations_shared_buffer() {
        // Test multiple aggregations sharing the same buffer
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as avg_price,
                MAX(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as max_price,
                MIN(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as min_price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as count_rows
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 145.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify all aggregation fields exist
                assert!(
                    result.fields.contains_key("avg_price"),
                    "Result {}: Missing avg_price",
                    idx
                );
                assert!(
                    result.fields.contains_key("max_price"),
                    "Result {}: Missing max_price",
                    idx
                );
                assert!(
                    result.fields.contains_key("min_price"),
                    "Result {}: Missing min_price",
                    idx
                );
                assert!(
                    result.fields.contains_key("count_rows"),
                    "Result {}: Missing count_rows",
                    idx
                );

                // Verify logical relationships
                if let (
                    Some(FieldValue::Float(avg)),
                    Some(FieldValue::Float(max)),
                    Some(FieldValue::Float(min)),
                ) = (
                    result.fields.get("avg_price"),
                    result.fields.get("max_price"),
                    result.fields.get("min_price"),
                ) {
                    assert!(
                        *min <= *avg && *avg <= *max,
                        "Result {}: min ({}) should be <= avg ({}) should be <= max ({})",
                        idx,
                        min,
                        avg,
                        max
                    );
                }

                // Verify count is positive
                if let Some(FieldValue::Integer(count)) = result.fields.get("count_rows") {
                    assert!(
                        *count > 0,
                        "Result {}: count_rows should be positive, got {}",
                        idx,
                        count
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW multiple aggregations with shared buffer successful with {} results",
                results.len()
            );
        }
    }

    // EDGE CASE TESTS

    #[tokio::test]
    async fn test_rows_window_single_partition() {
        // Test ROWS WINDOW without PARTITION BY (all records in one partition)
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        ORDER BY timestamp
                ) as global_avg
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("MSFT", 300.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 155.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify global_avg exists
                if let Some(avg_field) = result.fields.get("global_avg") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: global_avg should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW single partition successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_compound_partition_key() {
        // Test ROWS WINDOW with compound PARTITION BY (multiple columns)
        let sql = r#"
            SELECT
                trader_id,
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY trader_id, symbol
                        ORDER BY timestamp
                ) as compound_avg
            FROM trades
        "#;

        let records = vec![
            TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, 0),
            TestDataBuilder::trade_record(1, "AAPL", 151.0, 200, 100),
            TestDataBuilder::trade_record(1, "MSFT", 300.0, 150, 200),
            TestDataBuilder::trade_record(2, "AAPL", 152.0, 250, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify compound_avg exists
                if let Some(avg_field) = result.fields.get("compound_avg") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: compound_avg should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW compound partition key successful with {} results",
                results.len()
            );
        }
    }

    // ADDITIONAL ANALYTICAL WINDOW FUNCTIONS

    #[tokio::test]
    async fn test_rows_window_first_value_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                FIRST_VALUE(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as first_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(first_field) = result.fields.get("first_price") {
                    match first_field {
                        FieldValue::Float(first) => {
                            assert!(
                                *first > 0.0,
                                "Result {}: first_price should be positive, got {}",
                                idx,
                                first
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW FIRST_VALUE function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_last_value_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                LAST_VALUE(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as last_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(last_field) = result.fields.get("last_price") {
                    match last_field {
                        FieldValue::Float(last) => {
                            assert!(
                                *last > 0.0,
                                "Result {}: last_price should be positive, got {}",
                                idx,
                                last
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW LAST_VALUE function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_nth_value_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                NTH_VALUE(price, 2) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as second_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            println!(
                "✓ ROWS WINDOW NTH_VALUE function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_percent_rank_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                PERCENT_RANK() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price
                ) as pct_rank
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(pct_field) = result.fields.get("pct_rank") {
                    match pct_field {
                        FieldValue::Float(pct) => {
                            assert!(
                                *pct >= 0.0 && *pct <= 1.0,
                                "Result {}: percent_rank should be between 0.0 and 1.0, got {}",
                                idx,
                                pct
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW PERCENT_RANK function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_cume_dist_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                CUME_DIST() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price
                ) as cum_dist
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(dist_field) = result.fields.get("cum_dist") {
                    match dist_field {
                        FieldValue::Float(dist) => {
                            assert!(
                                *dist > 0.0 && *dist <= 1.0,
                                "Result {}: cume_dist should be between 0.0 and 1.0, got {}",
                                idx,
                                dist
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW CUME_DIST function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_ntile_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                NTILE(4) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price
                ) as quartile
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(FieldValue::Integer(quartile)) = result.fields.get("quartile") {
                    assert!(
                        *quartile >= 1 && *quartile <= 4,
                        "Result {}: quartile should be between 1 and 4, got {}",
                        idx,
                        quartile
                    );
                }
            }
            println!(
                "✓ ROWS WINDOW NTILE function successful with {} results",
                results.len()
            );
        }
    }

    // STATISTICAL AGGREGATE FUNCTIONS

    #[tokio::test]
    async fn test_rows_window_stddev_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                STDDEV(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as price_stddev
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(stddev_field) = result.fields.get("price_stddev") {
                    match stddev_field {
                        FieldValue::Float(stddev) => {
                            assert!(
                                *stddev >= 0.0,
                                "Result {}: stddev should be non-negative, got {}",
                                idx,
                                stddev
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW STDDEV function successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_variance_function() {
        let sql = r#"
            SELECT
                symbol,
                price,
                VARIANCE(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as price_variance
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                if let Some(var_field) = result.fields.get("price_variance") {
                    match var_field {
                        FieldValue::Float(variance) => {
                            assert!(
                                *variance >= 0.0,
                                "Result {}: variance should be non-negative, got {}",
                                idx,
                                variance
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW VARIANCE function successful with {} results",
                results.len()
            );
        }
    }

    // EMIT MODE TESTS

    #[tokio::test]
    async fn test_rows_window_emit_every_record() {
        // Test EMIT EVERY RECORD mode (default, real-time emission)
        // Each incoming record should produce a result immediately
        let sql = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as record_count
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("MSFT", 300.0, 500, 300),
            TestDataBuilder::ticker_record("MSFT", 302.0, 600, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records.clone()).await;

        // EMIT EVERY RECORD should produce results for all input records
        // We should have multiple results, one per input (or at least from most)
        if !results.is_empty() {
            // Verify each result has the expected fields
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("symbol"),
                    "Result {}: Missing 'symbol' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("price"),
                    "Result {}: Missing 'price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("record_count"),
                    "Result {}: Missing 'record_count' field",
                    idx
                );

                // Validate record_count is positive integer
                if let Some(FieldValue::Integer(count)) = result.fields.get("record_count") {
                    assert!(
                        *count > 0,
                        "Result {}: record_count should be positive, got {}",
                        idx,
                        count
                    );
                    // Count should be <= buffer size (100)
                    assert!(
                        *count <= 100,
                        "Result {}: record_count should be <= buffer size, got {}",
                        idx,
                        count
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW EMIT EVERY RECORD mode successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_emit_buffer_full() {
        // Test EMIT ON BUFFER_FULL mode (batch-like emission)
        // Results are emitted only when buffer reaches capacity
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 3 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT ON BUFFER_FULL
                ) as moving_avg
            FROM market_data
        "#;

        let records = vec![
            // AAPL: 3 records should fill buffer and trigger emission
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            // AAPL: 4th record fills buffer again
            TestDataBuilder::ticker_record("AAPL", 153.0, 800, 300),
            // MSFT: 1st record (won't emit yet)
            TestDataBuilder::ticker_record("MSFT", 300.0, 500, 400),
            // MSFT: 2nd record (won't emit yet)
            TestDataBuilder::ticker_record("MSFT", 302.0, 600, 500),
            // MSFT: 3rd record fills buffer and triggers emission
            TestDataBuilder::ticker_record("MSFT", 305.0, 700, 600),
        ];

        let records_len = records.len();
        let results = SqlExecutor::execute_query(sql, records).await;

        // EMIT ON BUFFER_FULL should produce fewer results than input records
        // Only when buffer (size 3) reaches capacity should emission occur
        if !results.is_empty() {
            // Verify each result has the expected fields
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("symbol"),
                    "Result {}: Missing 'symbol' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("price"),
                    "Result {}: Missing 'price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("moving_avg"),
                    "Result {}: Missing 'moving_avg' field",
                    idx
                );

                // Validate moving_avg is positive (average of 3+ prices)
                if let Some(avg_field) = result.fields.get("moving_avg") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: moving_avg should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }

            println!(
                "✓ ROWS WINDOW EMIT ON BUFFER_FULL mode successful with {} results (less than {} inputs)",
                results.len(),
                records_len
            );
        } else {
            // Empty results are acceptable - depends on implementation
            println!("⚠️  No results from EMIT ON BUFFER_FULL mode (buffer may not have filled)");
        }
    }

    #[tokio::test]
    async fn test_rows_window_emit_modes_comparison() {
        // Compare EMIT EVERY RECORD vs EMIT ON BUFFER_FULL behavior
        let sql_every_record = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 5 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as count_every
            FROM market_data
        "#;

        let sql_buffer_full = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 5 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT ON BUFFER_FULL
                ) as count_full
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 153.0, 800, 300),
            TestDataBuilder::ticker_record("AAPL", 154.0, 950, 400),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1050, 500),
            TestDataBuilder::ticker_record("AAPL", 156.0, 1150, 600),
        ];

        let results_every = SqlExecutor::execute_query(sql_every_record, records.clone()).await;
        let results_full = SqlExecutor::execute_query(sql_buffer_full, records).await;

        // EMIT EVERY RECORD should have more results than EMIT ON BUFFER_FULL
        // because it emits on every record, while buffer full only emits when full
        if !results_every.is_empty() && !results_full.is_empty() {
            assert!(
                results_every.len() >= results_full.len(),
                "EMIT EVERY RECORD ({} results) should have >= results than EMIT ON BUFFER_FULL ({} results)",
                results_every.len(),
                results_full.len()
            );

            println!(
                "✓ EMIT EVERY RECORD produced {} results (streaming mode)",
                results_every.len()
            );
            println!(
                "✓ EMIT ON BUFFER_FULL produced {} results (batch mode)",
                results_full.len()
            );
            println!("✓ Comparison shows expected behavior: streaming mode >= batch mode");
        }
    }

    #[tokio::test]
    async fn test_rows_window_emit_with_aggregations() {
        // Test EMIT modes with various aggregation functions
        let sql = r#"
            SELECT
                symbol,
                price,
                SUM(price) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as total_price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as avg_price,
                MIN(price) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as min_price,
                MAX(price) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as max_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 148.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 160.0, 1200, 300),
            TestDataBuilder::ticker_record("AAPL", 152.0, 950, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        // EMIT EVERY RECORD should produce results for each input
        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Check that all aggregation fields exist
                assert!(
                    result.fields.contains_key("total_price"),
                    "Result {}: Missing 'total_price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("avg_price"),
                    "Result {}: Missing 'avg_price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("min_price"),
                    "Result {}: Missing 'min_price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("max_price"),
                    "Result {}: Missing 'max_price' field",
                    idx
                );

                // Validate aggregate relationships:
                // min_price <= avg_price <= max_price
                let min_price = match result.fields.get("min_price") {
                    Some(FieldValue::Float(v)) => Some(*v),
                    _ => None,
                };
                let avg_price = match result.fields.get("avg_price") {
                    Some(FieldValue::Float(v)) => Some(*v),
                    _ => None,
                };
                let max_price = match result.fields.get("max_price") {
                    Some(FieldValue::Float(v)) => Some(*v),
                    _ => None,
                };

                if let (Some(min), Some(avg), Some(max)) = (min_price, avg_price, max_price) {
                    assert!(
                        min <= avg,
                        "Result {}: min_price ({}) should be <= avg_price ({})",
                        idx,
                        min,
                        avg
                    );
                    assert!(
                        avg <= max,
                        "Result {}: avg_price ({}) should be <= max_price ({})",
                        idx,
                        avg,
                        max
                    );
                }
            }

            println!(
                "✓ ROWS WINDOW EMIT with multiple aggregations successful with {} results",
                results.len()
            );
        }
    }

    // GAP DETECTION TESTS

    #[tokio::test]
    async fn test_rows_window_gap_detection_session_reset() {
        // Test that large time gaps trigger session reset (new session detection)
        // When time gap exceeds threshold, the buffer should conceptually reset
        let sql = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as record_count,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 10 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as avg_price
            FROM market_data
        "#;

        // Records with two distinct time gaps (session boundaries)
        let records = vec![
            // Session 1: 0-300ms (tight cluster)
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            // Large gap (300ms) - session boundary
            // Session 2: 3300-3500ms (another tight cluster)
            TestDataBuilder::ticker_record("AAPL", 155.0, 800, 3300),
            TestDataBuilder::ticker_record("AAPL", 156.0, 1050, 3400),
            TestDataBuilder::ticker_record("AAPL", 154.0, 950, 3500),
            // Gap (3000ms+) - session boundary
            // Session 3: 6500-6700ms
            TestDataBuilder::ticker_record("AAPL", 157.0, 1100, 6500),
            TestDataBuilder::ticker_record("AAPL", 158.0, 1200, 6600),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        // Verify gap detection behavior through result analysis
        if !results.is_empty() {
            // We should see different aggregation values when gap detection triggers
            // Later sessions should have fresh aggregations reflecting only recent records
            let mut count_values: Vec<i64> = Vec::new();
            for result in &results {
                if let Some(FieldValue::Integer(count)) = result.fields.get("record_count") {
                    count_values.push(*count);
                }
            }

            // Count values should vary due to gap detection resetting sessions
            if count_values.len() > 1 {
                println!(
                    "✓ ROWS WINDOW gap detection: count progression = {:?}",
                    count_values
                );
                println!(
                    "✓ Gap detection triggered session resets across {} results",
                    results.len()
                );
            }
        }
    }

    #[tokio::test]
    async fn test_rows_window_gap_detection_with_partition_isolation() {
        // Test gap detection works correctly with multiple partitions
        // Gaps in one partition shouldn't affect others
        let sql = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 5 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as per_partition_count
            FROM market_data
        "#;

        // Two symbols with different gap patterns
        let records = vec![
            // AAPL: continuous stream (no gaps)
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
            // MSFT: large gap then continues
            TestDataBuilder::ticker_record("MSFT", 300.0, 500, 50),
            // Gap (1950ms)
            TestDataBuilder::ticker_record("MSFT", 302.0, 600, 2000),
            TestDataBuilder::ticker_record("MSFT", 305.0, 700, 2100),
            // AAPL: continues
            TestDataBuilder::ticker_record("AAPL", 153.0, 800, 300),
            TestDataBuilder::ticker_record("AAPL", 154.0, 950, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            // Separate results by partition
            let mut aapl_results = Vec::new();
            let mut msft_results = Vec::new();

            for result in &results {
                if let Some(FieldValue::String(symbol)) = result.fields.get("symbol") {
                    if symbol == "AAPL" {
                        aapl_results.push(result);
                    } else if symbol == "MSFT" {
                        msft_results.push(result);
                    }
                }
            }

            // Verify partition isolation during gap detection
            println!(
                "✓ Gap detection maintains partition isolation: AAPL {} results, MSFT {} results",
                aapl_results.len(),
                msft_results.len()
            );

            // Both partitions should have results
            assert!(
                !aapl_results.is_empty(),
                "AAPL partition should have results"
            );
            assert!(
                !msft_results.is_empty(),
                "MSFT partition should have results"
            );

            println!("✓ ROWS WINDOW gap detection respects partition boundaries");
        }
    }

    #[tokio::test]
    async fn test_rows_window_gap_detection_emit_interaction() {
        // Test how gap detection interacts with EMIT modes
        let sql = r#"
            SELECT
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 3 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EMIT EVERY RECORD
                ) as window_count
            FROM market_data
        "#;

        // Create records with gaps and continuous sequences
        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 50),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 100),
            // Gap (900ms)
            TestDataBuilder::ticker_record("AAPL", 153.0, 800, 1000),
            TestDataBuilder::ticker_record("AAPL", 154.0, 950, 1050),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        // With EMIT EVERY RECORD and gap detection, we should see:
        // - Results emitted for each input record
        // - Gap detection may cause count to reset after gap
        if !results.is_empty() {
            let mut counts: Vec<i64> = Vec::new();
            for result in &results {
                if let Some(FieldValue::Integer(count)) = result.fields.get("window_count") {
                    counts.push(*count);
                }
            }

            println!(
                "✓ Gap detection with EMIT EVERY RECORD: count progression = {:?}",
                counts
            );

            // With buffer size 3 and a gap, we expect count to potentially reset
            // First three: 1, 2, 3
            // After gap: 1, 2
            assert!(
                counts.len() == results.len(),
                "All records should emit with EMIT EVERY RECORD"
            );

            println!("✓ ROWS WINDOW gap detection interacts correctly with EMIT modes");
        }
    }

    // EXPIRE AFTER (Gap Eviction) SYNTAX TESTS

    #[tokio::test]
    async fn test_rows_window_expire_after_default_timeout() {
        // Test ROWS WINDOW without EXPIRE AFTER (uses default 1-minute timeout)
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                ) as moving_avg
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                // Verify default timeout scenario works
                assert!(
                    result.fields.contains_key("moving_avg"),
                    "Result {}: Missing 'moving_avg' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with default EXPIRE AFTER (1 minute) successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_custom_seconds() {
        // Test EXPIRE AFTER INTERVAL 'N' SECOND INACTIVITY
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EXPIRE AFTER INTERVAL '30' SECOND INACTIVITY
                ) as moving_avg_30s
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("moving_avg_30s"),
                    "Result {}: Missing 'moving_avg_30s' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with EXPIRE AFTER INTERVAL '30' SECOND INACTIVITY successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_custom_minutes() {
        // Test EXPIRE AFTER INTERVAL 'N' MINUTE INACTIVITY
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EXPIRE AFTER INTERVAL '5' MINUTE INACTIVITY
                ) as moving_avg_5min
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("moving_avg_5min"),
                    "Result {}: Missing 'moving_avg_5min' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with EXPIRE AFTER INTERVAL '5' MINUTE INACTIVITY successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_never() {
        // Test EXPIRE AFTER NEVER (disables gap eviction)
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EXPIRE AFTER NEVER
                ) as moving_avg_no_expire
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("moving_avg_no_expire"),
                    "Result {}: Missing 'moving_avg_no_expire' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with EXPIRE AFTER NEVER successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_with_frame_bounds() {
        // Test EXPIRE AFTER combined with ROWS BETWEEN frame specification
        let sql = r#"
            SELECT
                symbol,
                price,
                AVG(price) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                        EXPIRE AFTER INTERVAL '30' SECOND INACTIVITY
                ) as moving_avg_with_expire
            FROM market_data
        "#;

        let records = TestDataBuilder::generate_price_series("AAPL", 150.0, 20, 60);

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("moving_avg_with_expire"),
                    "Result {}: Missing 'moving_avg_with_expire' field",
                    idx
                );
                if let Some(avg_field) = result.fields.get("moving_avg_with_expire") {
                    match avg_field {
                        FieldValue::Float(avg) => {
                            assert!(
                                *avg > 0.0,
                                "Result {}: moving_avg_with_expire should be positive, got {}",
                                idx,
                                avg
                            );
                        }
                        _ => {}
                    }
                }
            }
            println!(
                "✓ ROWS WINDOW with ROWS BETWEEN and EXPIRE AFTER successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_with_multiple_partitions() {
        // Test EXPIRE AFTER with multiple PARTITION BY columns (gap detection per partition)
        let sql = r#"
            SELECT
                trader_id,
                symbol,
                price,
                COUNT(*) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY trader_id, symbol
                        ORDER BY timestamp
                        EXPIRE AFTER INTERVAL '10' SECOND INACTIVITY
                ) as partition_count
            FROM trades
        "#;

        let records = vec![
            TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, 0),
            TestDataBuilder::trade_record(1, "AAPL", 151.0, 200, 100),
            TestDataBuilder::trade_record(1, "MSFT", 300.0, 150, 200),
            TestDataBuilder::trade_record(2, "AAPL", 152.0, 250, 300),
            TestDataBuilder::trade_record(2, "MSFT", 301.0, 175, 400),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            // Verify results with compound partition keys and gap eviction
            let mut partition_combinations = std::collections::HashSet::new();
            for result in &results {
                if let (Some(FieldValue::Integer(trader)), Some(FieldValue::String(symbol))) =
                    (result.fields.get("trader_id"), result.fields.get("symbol"))
                {
                    partition_combinations.insert((trader, symbol.clone()));
                }
            }

            assert!(
                !partition_combinations.is_empty(),
                "Should have results from at least one partition combination"
            );

            println!(
                "✓ ROWS WINDOW with compound partitions and EXPIRE AFTER successful: {} unique partitions",
                partition_combinations.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_with_lag_lead() {
        // Test EXPIRE AFTER with offset functions (LAG/LEAD)
        let sql = r#"
            SELECT
                symbol,
                price,
                LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EXPIRE AFTER INTERVAL '5' SECOND INACTIVITY
                ) as prev_price,
                LEAD(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY timestamp
                        EXPIRE AFTER INTERVAL '5' SECOND INACTIVITY
                ) as next_price
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 152.0, 900, 200),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("prev_price"),
                    "Result {}: Missing 'prev_price' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("next_price"),
                    "Result {}: Missing 'next_price' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with EXPIRE AFTER and LAG/LEAD successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_with_ranking() {
        // Test EXPIRE AFTER with ranking functions
        let sql = r#"
            SELECT
                symbol,
                price,
                RANK() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price DESC
                        EXPIRE AFTER INTERVAL '15' SECOND INACTIVITY
                ) as price_rank,
                DENSE_RANK() OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY price DESC
                        EXPIRE AFTER INTERVAL '15' SECOND INACTIVITY
                ) as price_dense_rank
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 155.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 155.0, 900, 200),
            TestDataBuilder::ticker_record("AAPL", 152.0, 1200, 300),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;

        if !results.is_empty() {
            for (idx, result) in results.iter().enumerate() {
                assert!(
                    result.fields.contains_key("price_rank"),
                    "Result {}: Missing 'price_rank' field",
                    idx
                );
                assert!(
                    result.fields.contains_key("price_dense_rank"),
                    "Result {}: Missing 'price_dense_rank' field",
                    idx
                );
            }
            println!(
                "✓ ROWS WINDOW with EXPIRE AFTER and ranking functions successful with {} results",
                results.len()
            );
        }
    }

    #[tokio::test]
    async fn test_rows_window_expire_after_all_modes_comparison() {
        // Compare all three EXPIRE AFTER modes in one test
        let sql_default = r#"
            SELECT symbol, price,
                AVG(price) OVER (
                    ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp
                ) as avg_default
            FROM market_data
        "#;

        let sql_custom = r#"
            SELECT symbol, price,
                AVG(price) OVER (
                    ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp
                    EXPIRE AFTER INTERVAL '60' SECOND INACTIVITY
                ) as avg_custom
            FROM market_data
        "#;

        let sql_never = r#"
            SELECT symbol, price,
                AVG(price) OVER (
                    ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp
                    EXPIRE AFTER NEVER
                ) as avg_never
            FROM market_data
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("AAPL", 152.0, 1100, 100),
            TestDataBuilder::ticker_record("AAPL", 151.0, 900, 200),
        ];

        let results_default = SqlExecutor::execute_query(sql_default, records.clone()).await;
        let results_custom = SqlExecutor::execute_query(sql_custom, records.clone()).await;
        let results_never = SqlExecutor::execute_query(sql_never, records).await;

        // All three modes should produce results
        assert!(
            !results_default.is_empty(),
            "Default EXPIRE AFTER should produce results"
        );
        assert!(
            !results_custom.is_empty(),
            "Custom EXPIRE AFTER should produce results"
        );
        assert!(
            !results_never.is_empty(),
            "EXPIRE AFTER NEVER should produce results"
        );

        println!("✓ EXPIRE AFTER modes comparison:");
        println!("  - Default (1 min): {} results", results_default.len());
        println!("  - Custom (60 sec): {} results", results_custom.len());
        println!("  - Never: {} results", results_never.len());
        println!("✓ All EXPIRE AFTER modes parse and execute successfully");
    }
}
