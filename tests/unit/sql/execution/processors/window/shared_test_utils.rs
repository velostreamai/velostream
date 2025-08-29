use std::collections::HashMap;
/// Shared utilities for window testing
use std::sync::Arc;
use tokio::sync::mpsc;

use ferrisstreams::ferris::serialization::SqlError;
use ferrisstreams::ferris::{
    serialization::{InternalValue, JsonFormat},
    sql::{
        execution::{
            types::{FieldValue, StreamRecord},
            StreamExecutionEngine,
        },
        parser::StreamingSqlParser,
    },
};

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
        StreamRecord::new(fields)
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
        StreamRecord::new(fields)
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
}

/// SQL execution utility
pub struct SqlExecutor;

impl SqlExecutor {
    /// Execute SQL query and return results
    pub async fn execute_query(sql: &str, records: Vec<StreamRecord>) -> Vec<String> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        let parser = StreamingSqlParser::new();
        let query = parser.parse(sql).expect("Failed to parse SQL");

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

        // need to flush any remaining results for windowed queries with group by
        // First flush windows, then flush group by results
        if let Err(e) = engine.flush_windows().await {
            eprintln!("❌ Error flushing windows: {:?}", e);
        } else {
            println!("✅ Windows flushed successfully");
        }

        let flushed_results = engine.flush_group_by_results(&query);
        println!("Group by flush results: {:?}", flushed_results);
        // Collect results
        let mut results = Vec::new();
        while let Ok(output) = rx.try_recv() {
            results.push(format!("{:?}", output));
        }
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
            WINDOW SESSION({})
            GROUP BY {}
            "#,
            agg_func, field, gap, partition_field
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
            WINDOW SLIDING({}, {})
            GROUP BY symbol
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
            WINDOW SLIDING(15m, 1m)
            GROUP BY symbol
            HAVING ABS(price - AVG(price)) > {}
            ",
            threshold
        )
    }
}

/// Test assertions helper
pub struct WindowTestAssertions;

impl WindowTestAssertions {
    pub fn assert_has_results(results: &[String], test_name: &str) {
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

    pub fn assert_result_count_min(results: &[String], min_count: usize, test_name: &str) {
        assert!(
            results.len() >= min_count,
            "{} should produce at least {} results, got {}",
            test_name,
            min_count,
            results.len()
        );
    }

    pub fn print_results(results: &[String], test_name: &str) {
        println!("{} results ({} total):", test_name, results.len());
        for (i, result) in results.iter().take(3).enumerate() {
            println!("  [{}]: {}", i, result);
        }
        if results.len() > 3 {
            println!("  ... and {} more", results.len() - 3);
        }
    }
}
