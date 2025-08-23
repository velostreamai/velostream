use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};

#[cfg(test)]
mod financial_ticker_analytics_tests {
    use super::*;

    #[tokio::test]
    async fn test_15_minute_moving_average() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as ma_15m,
                COUNT(*) as tick_count,
                MIN(price) as low_15m,
                MAX(price) as high_15m
            FROM ticker_feed 
            WINDOW SLIDING(15m, 1m)
            GROUP BY symbol
        "#;

        let records = TestDataBuilder::generate_price_series("AAPL", 150.0, 20, 60); // 20 minutes
        let results = SqlExecutor::execute_query(sql, records).await;
        println!("15-minute moving average results: {:?}", results);
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_1_hour_moving_average() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as ma_1h,
                AVG(volume) as avg_volume_1h,
                AVG(spread) as avg_spread_1h
            FROM ticker_feed 
            WINDOW SLIDING(1h, 5m)
            GROUP BY symbol
        "#;

        let records = TestDataBuilder::generate_price_series("TSLA", 800.0, 70, 60); // 70 minutes of data
        let results = SqlExecutor::execute_query(sql, records).await;
        println!("1-hour moving average results count: {}", results.len());
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_4_hour_moving_average() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as ma_4h,
                SUM(volume) as total_volume_4h,
                (MAX(price) - MIN(price)) as trading_range_4h
            FROM ticker_feed 
            WINDOW SLIDING(4h, 15m)
            GROUP BY symbol
        "#;

        let records = TestDataBuilder::generate_price_series("NVDA", 500.0, 250, 60); // 4+ hours of data
        let results = SqlExecutor::execute_query(sql, records).await;
        println!("4-hour moving average results count: {}", results.len());
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_outlier_detection_price_spikes() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(*) as price_count
            FROM ticker_feed 
            WINDOW SLIDING(10m, 1m)
            GROUP BY symbol
        "#;

        let records = TestDataBuilder::generate_with_outliers("AAPL", 150.0, 10);
        let results = SqlExecutor::execute_query(sql, records).await;
        println!("Price spike outlier detection results: {:?}", results);
        WindowTestAssertions::print_results(&results, "Price outlier detection");
    }

    #[tokio::test]
    async fn test_volume_outlier_detection() {
        let sql = r#"
            SELECT 
                symbol,
                volume,
                AVG(volume) as avg_volume_window
            FROM ticker_feed 
            WINDOW SLIDING(30m, 5m)
            GROUP BY symbol
            HAVING volume > AVG(volume) * 2  -- Volume 2x above average
        "#;

        let records = TestDataBuilder::generate_with_outliers("MSFT", 300.0, 8);
        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Volume outlier detection");
    }

    #[tokio::test]
    async fn test_comprehensive_trading_dashboard() {
        let sql = r#"
            SELECT 
                symbol,
                AVG(price) as ma_15m,
                COUNT(*) as tick_count_15m,
                (MAX(price) - MIN(price)) / AVG(price) * 100 as volatility_pct,
                CASE 
                    WHEN AVG(volume) > 2000 THEN 'HIGH_VOLUME'
                    ELSE 'NORMAL'
                END as volume_status
            FROM ticker_feed 
            WINDOW SLIDING(15m, 3m)
            GROUP BY symbol
        "#;

        let records = TestDataBuilder::generate_price_series("DASHBOARD", 425.0, 20, 180); // 1 hour of 3-min intervals
        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::assert_has_results(&results, "Trading dashboard");
        WindowTestAssertions::print_results(&results, "Comprehensive trading dashboard");
    }
}
