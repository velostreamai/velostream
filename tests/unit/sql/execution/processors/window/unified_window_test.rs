use super::shared_test_utils::{SqlExecutor, SqlQueries, TestDataBuilder, WindowTestAssertions};

#[cfg(test)]
mod unified_window_tests {
    use super::*;

    // TUMBLING WINDOW TESTS
    #[tokio::test]
    async fn test_tumbling_windows_all_aggregations() {
        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 50.0, "completed", 30),
            TestDataBuilder::order_record(3, 102, 75.0, "pending", 45),
            TestDataBuilder::order_record(4, 103, 100.0, "completed", 120), // Next window
        ];

        // Test multiple aggregation functions
        let test_cases = vec![
            ("COUNT", "*", "Tumbling COUNT"),
            ("SUM", "amount", "Tumbling SUM"),
            ("AVG", "amount", "Tumbling AVG"),
            ("MIN", "amount", "Tumbling MIN"),
            ("MAX", "amount", "Tumbling MAX"),
        ];

        for (agg_func, field, test_name) in test_cases {
            let sql = SqlQueries::tumbling_window("30s", agg_func, field);
            let results = SqlExecutor::execute_query(&sql, records.clone()).await;
            WindowTestAssertions::assert_has_results(&results, test_name);
            WindowTestAssertions::print_results(&results, test_name);
        }
    }

    #[tokio::test]
    async fn test_tumbling_business_scenarios() {
        let sql = r#"
            SELECT 
                status,
                COUNT(*) as order_count,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value
            FROM orders 
            WINDOW TUMBLING(1h)
            GROUP BY status
            HAVING COUNT(*) > 1
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 100.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 200.0, "completed", 1800), // 30 min
            TestDataBuilder::order_record(3, 102, 50.0, "pending", 2400),    // 40 min
            TestDataBuilder::order_record(4, 103, 300.0, "completed", 3600), // Next hour
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::assert_has_results(&results, "Tumbling Business Analysis");
        WindowTestAssertions::print_results(&results, "Tumbling Business Analysis");
    }

    // SLIDING WINDOW TESTS
    #[tokio::test]
    async fn test_sliding_windows_all_intervals() {
        let records = TestDataBuilder::generate_price_series("AAPL", 150.0, 20, 60); // 20 minutes

        let test_cases = vec![
            ("10m", "5m", "Short-term sliding"),
            ("15m", "5m", "Medium-term sliding"),
            ("20m", "10m", "Long-term sliding"),
        ];

        for (window_size, advance, test_name) in test_cases {
            let sql = SqlQueries::sliding_window(window_size, advance, "AVG", "price");
            let results = SqlExecutor::execute_query(&sql, records.clone()).await;
            WindowTestAssertions::assert_has_results(&results, test_name);
            WindowTestAssertions::print_results(&results, test_name);
        }
    }

    #[tokio::test]
    async fn test_moving_averages_multiple_timeframes() {
        let records = TestDataBuilder::generate_price_series("TSLA", 800.0, 60, 60); // 1 hour data

        let test_cases = vec![
            ("15m", "1m", "MA-15m"),
            ("30m", "5m", "MA-30m"),
            ("1h", "15m", "MA-1h"),
        ];

        for (window, advance, test_name) in test_cases {
            let sql = SqlQueries::moving_average(window, advance);
            let results = SqlExecutor::execute_query(&sql, records.clone()).await;
            WindowTestAssertions::print_results(&results, test_name);
        }
    }

    // SESSION WINDOW TESTS
    #[tokio::test]
    async fn test_session_windows_all_scenarios() {
        let test_cases = vec![
            (
                vec![
                    TestDataBuilder::order_record(1, 100, 25.0, "view", 0),
                    TestDataBuilder::order_record(2, 100, 50.0, "add_to_cart", 120), // 2 min
                    TestDataBuilder::order_record(3, 100, 75.0, "purchase", 240),    // 4 min
                    TestDataBuilder::order_record(4, 100, 30.0, "view", 600), // 10 min - new session
                ],
                "5m",
                "User shopping session",
            ),
            (
                vec![
                    TestDataBuilder::order_record(1, 100, 100.0, "game_start", 0),
                    TestDataBuilder::order_record(2, 100, 200.0, "game_action", 60), // 1 min
                    TestDataBuilder::order_record(3, 100, 350.0, "game_action", 90), // 1.5 min
                    TestDataBuilder::order_record(4, 100, 100.0, "game_start", 270), // 4.5 min - new session
                ],
                "2m",
                "Gaming session",
            ),
        ];

        for (records, gap, test_name) in test_cases {
            let sql = SqlQueries::session_window(gap, "customer_id", "COUNT", "*");
            let results = SqlExecutor::execute_query(&sql, records).await;
            WindowTestAssertions::assert_has_results(&results, test_name);
            WindowTestAssertions::print_results(&results, test_name);
        }
    }

    // FINANCIAL ANALYTICS TESTS
    #[tokio::test]
    async fn test_financial_outlier_detection() {
        let records = TestDataBuilder::generate_with_outliers("NVDA", 500.0, 15);

        let test_cases = vec![
            (10.0, "Major outlier detection"),
            (25.0, "Extreme outlier detection"),
            (50.0, "Critical outlier detection"),
        ];

        for (threshold, test_name) in test_cases {
            let sql = SqlQueries::outlier_detection(threshold);
            let results = SqlExecutor::execute_query(&sql, records.clone()).await;
            WindowTestAssertions::print_results(&results, test_name);
        }
    }

    #[tokio::test]
    async fn test_comprehensive_trading_analytics() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as ma_15m,
                COUNT(*) as tick_count,
                SUM(volume) as total_volume,
                (MAX(price) - MIN(price)) / AVG(price) * 100 as volatility_pct,
                CASE 
                    WHEN AVG(volume) > 2000 AND (MAX(price) - MIN(price)) / AVG(price) > 0.02 
                    THEN 'HIGH_ACTIVITY'
                    ELSE 'NORMAL'
                END as market_status
            FROM ticker_feed 
            WINDOW SLIDING(15m, 3m)
            GROUP BY symbol
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("BTC", 45000.0, 1000, 0),
            TestDataBuilder::ticker_record("BTC", 45200.0, 2500, 180), // High volume
            TestDataBuilder::ticker_record("BTC", 44800.0, 3000, 360), // Price drop + high volume
            TestDataBuilder::ticker_record("BTC", 45100.0, 1500, 540),
            TestDataBuilder::ticker_record("BTC", 45300.0, 2000, 720),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::assert_has_results(&results, "Comprehensive Trading Analytics");
        WindowTestAssertions::print_results(&results, "Comprehensive Trading Analytics");
    }

    // PERFORMANCE AND EDGE CASES
    #[tokio::test]
    async fn test_window_edge_cases() {
        // Empty windows
        let sql = SqlQueries::tumbling_window("1m", "COUNT", "*");
        let sparse_records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0), // First window
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 180), // Third window (gap)
        ];
        let results = SqlExecutor::execute_query(&sql, sparse_records).await;
        WindowTestAssertions::print_results(&results, "Empty windows handling");

        // Boundary conditions
        let boundary_records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0), // Exactly at start
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 59), // Just before boundary
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 60), // Exactly at boundary
        ];
        let results = SqlExecutor::execute_query(&sql, boundary_records).await;
        WindowTestAssertions::print_results(&results, "Window boundaries");
    }

    #[tokio::test]
    async fn test_multi_symbol_concurrent_processing() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as avg_price,
                COUNT(*) as tick_count
            FROM ticker_feed 
            WINDOW SLIDING(10m, 2m)
            GROUP BY symbol
            ORDER BY symbol
        "#;

        let records = vec![
            TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 0),
            TestDataBuilder::ticker_record("MSFT", 300.0, 800, 0),
            TestDataBuilder::ticker_record("GOOGL", 2500.0, 200, 0),
            TestDataBuilder::ticker_record("AAPL", 151.0, 1200, 120),
            TestDataBuilder::ticker_record("MSFT", 302.0, 900, 120),
            TestDataBuilder::ticker_record("GOOGL", 2520.0, 250, 120),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::assert_has_results(&results, "Multi-symbol processing");
        WindowTestAssertions::print_results(&results, "Multi-symbol processing");
    }

    #[tokio::test]
    async fn test_complex_analytical_queries() {
        let sql = "SELECT
                customer_id,
                COUNT(*) as session_actions,
                SUM(amount) as session_value,
                AVG(amount) as avg_action_value,
                CASE
                    WHEN SUM(amount) > 500 THEN 'HIGH_VALUE'
                    WHEN SUM(amount) > 200 THEN 'MEDIUM_VALUE'
                    ELSE 'LOW_VALUE'
                END as customer_segment
            FROM orders 
            WINDOW SESSION(10m)
            GROUP BY customer_id
            HAVING COUNT(*) >= 2
            ORDER BY SUM(amount) DESC
        ";

        let records = vec![
            // High-value customer session
            TestDataBuilder::order_record(1, 100, 200.0, "browse", 0),
            TestDataBuilder::order_record(2, 100, 300.0, "purchase", 300),
            TestDataBuilder::order_record(3, 100, 150.0, "purchase", 450),
            // Medium-value customer session
            TestDataBuilder::order_record(4, 200, 100.0, "browse", 600),
            TestDataBuilder::order_record(5, 200, 150.0, "purchase", 750),
            // Low-value session (single action - filtered by HAVING)
            TestDataBuilder::order_record(6, 300, 50.0, "browse", 900),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::assert_has_results(&results, "Complex analytical query");
        WindowTestAssertions::print_results(&results, "Complex analytical query");
        WindowTestAssertions::assert_result_count_min(&results, 2, "Complex analytical query");
    }
}
