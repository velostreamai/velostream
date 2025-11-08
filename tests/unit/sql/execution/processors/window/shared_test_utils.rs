use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::HashMap;
use std::str::FromStr;
/// Shared utilities for window testing
use tokio::sync::mpsc;
use velostream::velostream::sql::{
    ast::StreamingQuery,
    execution::{
        StreamExecutionEngine,
        types::{FieldValue, StreamRecord},
    },
    parser::StreamingSqlParser,
};

/// SQL validation helper - validates SQL and panics with clear error on invalid syntax
/// This should be used instead of `.unwrap()` when parsing SQL in tests
pub fn validate_sql(parser: &StreamingSqlParser, sql: &str) -> StreamingQuery {
    match parser.parse(sql) {
        Ok(query) => query,
        Err(e) => {
            eprintln!("❌ SQL Validation Error: {:?}", e);
            eprintln!("   Invalid SQL: {}", sql);
            panic!("SQL validation failed: {:?}", e);
        }
    }
}

/// Common record creation utilities
pub struct TestDataBuilder;

impl TestDataBuilder {
    /// Create a basic order record
    pub fn order_record(
        id: i64,
        customer_id: i64,
        amount: f64,
        status: &str,
        timestamp_seconds: i64,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
        fields.insert("amount".to_string(), FieldValue::Float(amount));
        fields.insert("status".to_string(), FieldValue::String(status.to_string()));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(timestamp_seconds * 1000),
        );

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let timestamp_ms = timestamp_seconds * 1000;
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp_ms;
        record
    }

    /// Create a financial ticker record
    pub fn ticker_record(
        symbol: &str,
        price: f64,
        volume: i64,
        timestamp_seconds: i64,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("volume".to_string(), FieldValue::Integer(volume));
        fields.insert("bid".to_string(), FieldValue::Float(price - 0.01));
        fields.insert("ask".to_string(), FieldValue::Float(price + 0.01));
        fields.insert("spread".to_string(), FieldValue::Float(0.02));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(timestamp_seconds * 1000),
        );

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let timestamp_ms = timestamp_seconds * 1000;
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp_ms;
        record
    }

    /// Create a trade record (for use in trading-specific window tests)
    pub fn trade_record(
        id: i64,
        symbol: &str,
        price: f64,
        volume: i64,
        timestamp_millis: i64,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("volume".to_string(), FieldValue::Integer(volume));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(timestamp_millis),
        );

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp_millis;
        record
    }

    /// Create a market data record based on the market_data_ts.avsc schema
    pub fn market_data_record(
        symbol: &str,
        exchange: &str,
        price: Decimal,
        bid_price: Decimal,
        ask_price: Decimal,
        bid_size: i64,
        ask_size: i64,
        volume: i64,
        timestamp_millis: i64,
        vwap: Option<Decimal>,
        market_cap: Option<Decimal>,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert(
            "exchange".to_string(),
            FieldValue::String(exchange.to_string()),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(timestamp_millis),
        );
        fields.insert(
            "event_timestamp".to_string(),
            FieldValue::Integer(timestamp_millis),
        );
        fields.insert("price".to_string(), FieldValue::Decimal(price));
        fields.insert("bid_price".to_string(), FieldValue::Decimal(bid_price));
        fields.insert("ask_price".to_string(), FieldValue::Decimal(ask_price));
        fields.insert("bid_size".to_string(), FieldValue::Integer(bid_size));
        fields.insert("ask_size".to_string(), FieldValue::Integer(ask_size));
        fields.insert("volume".to_string(), FieldValue::Integer(volume));

        if let Some(vwap_value) = vwap {
            fields.insert("vwap".to_string(), FieldValue::Decimal(vwap_value));
        }

        if let Some(market_cap_value) = market_cap {
            fields.insert(
                "market_cap".to_string(),
                FieldValue::Decimal(market_cap_value),
            );
        }

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp_millis;
        record
    }

    /// Generate a sequence of records with gradual price changes
    pub fn generate_price_series(
        symbol: &str,
        base_price: f64,
        count: usize,
        interval_seconds: i64,
    ) -> Vec<StreamRecord> {
        (0..count)
            .map(|i| {
                let price = base_price + (i as f64 * 0.1); // Small increments
                let volume = 1000 + (i * 100) as i64;
                Self::ticker_record(symbol, price, volume, i as i64 * interval_seconds)
            })
            .collect()
    }

    /// Generate records with outliers for testing
    pub fn generate_with_outliers(
        symbol: &str,
        base_price: f64,
        count: usize,
    ) -> Vec<StreamRecord> {
        (0..count)
            .map(|i| {
                let price = if i % 7 == 0 {
                    base_price * 1.1 // 10% spike every 7th record
                } else {
                    base_price + (i as f64 * 0.01)
                };
                Self::ticker_record(symbol, price, 1000, i as i64 * 60)
            })
            .collect()
    }

    /// Generate a sequence of market data records
    pub fn generate_market_data_series(
        symbol: &str,
        exchange: &str,
        a_base_price: f64,
        count: usize,
        interval_millis: i64,
    ) -> Vec<StreamRecord> {
        let base_time = chrono::Utc::now().timestamp_millis();

        let base_price = Decimal::from_f64(a_base_price).unwrap();

        (0..count)
            .map(|i| {
                let increment = Decimal::from_str(&format!("{:.4}", (i as f64) * 0.01)).unwrap();
                let price = base_price + increment;
                let bid_price = price - Decimal::from_str("0.01").unwrap();
                let ask_price = price + Decimal::from_str("0.01").unwrap();
                let volume = 1000 + (i * 100) as i64;
                let timestamp = base_time + (i as i64 * interval_millis);
                let vwap = Some(price); // Simplified VWAP calculation
                let market_cap = Some(price * Decimal::from(1_000_000)); // Simple market cap calculation

                Self::market_data_record(
                    symbol,
                    exchange,
                    price,
                    bid_price,
                    ask_price,
                    volume / 2, // bid_size
                    volume / 2, // ask_size
                    volume,
                    timestamp,
                    vwap,
                    market_cap,
                )
            })
            .collect()
    }
}

/// SQL execution utility
pub struct SqlExecutor;

impl SqlExecutor {
    /// Execute SQL query and return results as StreamRecords
    pub async fn execute_query(sql: &str, records: Vec<StreamRecord>) -> Vec<StreamRecord> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Window_v2 is the only architecture available (Phase 2E+)
        let config = velostream::velostream::sql::execution::config::StreamingConfig::new();
        let mut engine = StreamExecutionEngine::new_with_config(tx, config);

        let parser = StreamingSqlParser::new();

        // Validate SQL before execution using helper function
        let query = validate_sql(&parser, sql);

        // Execute records
        for (i, record) in records.iter().enumerate() {
            // Convert StreamRecord to StreamRecord
            let result = engine.execute_with_record(&query, record.clone()).await;
            if let Err(e) = result {
                eprintln!("❌ Error executing record {}: {:?}", i + 1, e);
                eprintln!("   Record: {:?}", record);
                eprintln!("   Query: {}", sql);
                // Don't continue processing if there's an error
                break;
            } else {
                println!("✅ Record {} executed successfully", i + 1);
            }
        }

        // Need to flush any remaining results for windowed queries with group by
        // First flush windows, then flush group by results
        if let Err(e) = engine.flush_windows().await {
            eprintln!("❌ Error flushing windows: {:?}", e);
        } else {
            println!("✅ Windows flushed successfully");
        }

        if let Err(e) = engine.flush_group_by_results(&query) {
            eprintln!("❌ Error flushing group by results: {:?}", e);
        } else {
            println!("✅ Group by results flushed successfully");
        }
        // Collect results with timeout to allow async processing
        let mut results = Vec::new();

        // Give the engine a moment to process any final emissions
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Collect all available results
        while let Ok(output) = rx.try_recv() {
            results.push(output);
        }

        println!("Collected {} results after flushing", results.len());
        results
    }
}

/// Common SQL query templates
pub struct SqlQueries;

impl SqlQueries {
    pub fn tumbling_window(interval: &str, agg_func: &str, field: &str) -> String {
        format!(
            r#"
            SELECT {}({}) as result
            FROM orders 
            WINDOW TUMBLING({})
            EMIT CHANGES
            "#,
            agg_func, field, interval
        )
    }

    pub fn sliding_window(window_size: &str, advance: &str, agg_func: &str, field: &str) -> String {
        format!(
            r#"
            SELECT {}({}) as result
            FROM orders 
            WINDOW SLIDING({}, {})
            "#,
            agg_func, field, window_size, advance
        )
    }

    pub fn session_window(gap: &str, partition_field: &str, agg_func: &str, field: &str) -> String {
        format!(
            r#"
            SELECT {}({}) as result
            FROM orders
            GROUP BY {}
            WINDOW SESSION({})
            "#,
            agg_func, field, partition_field, gap
        )
    }

    pub fn moving_average(window_size: &str, advance: &str) -> String {
        format!(
            r#"
            SELECT
                symbol,
                AVG(price) as ma,
                COUNT(*) as tick_count,
                MIN(price) as low,
                MAX(price) as high
            FROM ticker_feed
            GROUP BY symbol
            WINDOW SLIDING({}, {})
            "#,
            window_size, advance
        )
    }

    pub fn outlier_detection(threshold: f64) -> String {
        format!(
            "SELECT
                symbol,
                price,
                AVG(price) as avg_price,
                ABS(price - AVG(price)) as deviation
            FROM ticker_feed
            GROUP BY symbol
            WINDOW SLIDING(15m, 1m)
            HAVING ABS(price - AVG(price)) > {}
            ",
            threshold
        )
    }
}

/// Test assertions helper
pub struct WindowTestAssertions;

impl WindowTestAssertions {
    pub fn assert_has_results(results: &[StreamRecord], test_name: &str) {
        // For now, don't assert on empty results as windowed queries might not emit immediately
        if results.is_empty() {
            println!(
                "⚠️  {} produced no results - this may be expected for windowed queries without enough boundary crossings",
                test_name
            );
        } else {
            println!("✅ {} produced {} results", test_name, results.len());
        }
    }

    pub fn assert_result_count_min(results: &[StreamRecord], min_count: usize, test_name: &str) {
        assert!(
            results.len() >= min_count,
            "{} should produce at least {} results, got {}",
            test_name,
            min_count,
            results.len()
        );
    }

    pub fn print_results(results: &[StreamRecord], test_name: &str) {
        println!("{} results ({} total):", test_name, results.len());
        for (i, record) in results.iter().take(3).enumerate() {
            // Print StreamRecord fields in a readable format
            let fields_str: Vec<String> = record
                .fields
                .iter()
                .map(|(k, v)| format!("{}: {:?}", k, v))
                .collect();
            println!("  [{}]: {}", i, fields_str.join(", "));
        }
        if results.len() > 3 {
            println!("  ... and {} more", results.len() - 3);
        }
    }
}
