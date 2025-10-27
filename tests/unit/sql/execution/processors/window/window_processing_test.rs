/*!
# Window Processing Tests

Comprehensive tests for the newly implemented window processing functionality including:
- Tumbling windows with COUNT, SUM, AVG, MIN, MAX aggregations
- Sliding windows with proper buffer management
- Session windows with gap timeout handling
- Event-time vs processing-time semantics
- Window boundary detection and emission logic
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record(id: i64, amount: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("customer_id".to_string(), FieldValue::Integer(id));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp,
        offset: id,
        partition: 0,
    }
}

async fn execute_windowed_test(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query).map_err(|e| {
        println!("PARSE ERROR for query '{}': {}", query, e);
        e
    })?;

    // Register the query
    engine
        .start_query_execution("test_query".to_string(), parsed_query)
        .await
        .map_err(|e| {
            println!("QUERY EXECUTION ERROR for '{}': {}", query, e);
            e
        })?;

    // Process records one by one through stream processing
    for record in &records {
        // Handle the "No records after filtering" error by ignoring it
        // This allows empty windows to be properly handled in tests
        match engine.process_stream_record("orders", record.clone()).await {
            Ok(_) => {}
            Err(e) => {
                // Check if it's the specific error we want to handle
                if let Some(_err_str) = e
                    .to_string()
                    .to_lowercase()
                    .find("no records after filtering")
                {
                    // Ignore this specific error
                } else {
                    // Propagate other errors with detailed info
                    println!("STREAM PROCESSING ERROR for '{}': {}", query, e);
                    return Err(e.into());
                }
            }
        }
    }

    // Flush any pending windows by adding a trigger record past all expected windows
    // Find the maximum timestamp and add a large buffer
    let max_timestamp = records.iter().map(|r| r.timestamp).max().unwrap_or(0);
    let flush_timestamp = max_timestamp + 30000; // 30 seconds past the last record
    let flush_record = create_test_record(999, 0.0, flush_timestamp);
    match engine.process_stream_record("orders", flush_record).await {
        Ok(_) => {}
        Err(e) => {
            // Check if it's the specific error we want to handle
            if let Some(_err_str) = e
                .to_string()
                .to_lowercase()
                .find("no records after filtering")
            {
                // Ignore this specific error
            } else {
                // Propagate other errors
                return Err(e.into());
            }
        }
    };

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_tumbling_window_count() {
    // Test tumbling window with COUNT aggregation
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WINDOW TUMBLING(5s) GROUP BY customer_id";

    // Create records spanning 10 seconds (2 windows of 5 seconds each)
    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1: 0-5000ms
        create_test_record(2, 200.0, 2000), // Window 1
        create_test_record(3, 300.0, 3000), // Window 1
        create_test_record(4, 400.0, 6000), // Window 2: 5000-10000ms
        create_test_record(5, 500.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Should have 2 results (one per window)
    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window should have count of 3
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("order_count"),
            Some(&FieldValue::Integer(3))
        );
    }

    // Second window should have count of 2
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("order_count"),
            Some(&FieldValue::Integer(2))
        );
    }
}

#[tokio::test]
async fn test_tumbling_window_sum() {
    // Test tumbling window with SUM aggregation
    let query = "SELECT customer_id, SUM(amount) as total_amount FROM orders WINDOW TUMBLING(5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1
        create_test_record(2, 200.0, 2000), // Window 1
        create_test_record(3, 300.0, 6000), // Window 2
        create_test_record(4, 400.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: 100 + 200 = 300
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("total_amount"),
            Some(&FieldValue::Float(300.0))
        );
    }

    // Second window: 300 + 400 = 700
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("total_amount"),
            Some(&FieldValue::Float(700.0))
        );
    }
}

#[tokio::test]
async fn test_tumbling_window_avg() {
    // Test tumbling window with AVG aggregation
    let query = "SELECT customer_id, AVG(amount) as avg_amount FROM orders WINDOW TUMBLING(5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1
        create_test_record(2, 200.0, 2000), // Window 1
        create_test_record(3, 400.0, 6000), // Window 2
        create_test_record(4, 600.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: (100 + 200) / 2 = 150
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("avg_amount"),
            Some(&FieldValue::Float(150.0))
        );
    }

    // Second window: (400 + 600) / 2 = 500
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("avg_amount"),
            Some(&FieldValue::Float(500.0))
        );
    }
}

#[tokio::test]
async fn test_tumbling_window_min_max() {
    // Test tumbling window with MIN and MAX aggregations
    let query = "SELECT customer_id, MIN(amount) as min_amount, MAX(amount) as max_amount FROM orders WINDOW TUMBLING(5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1
        create_test_record(2, 300.0, 2000), // Window 1
        create_test_record(3, 200.0, 3000), // Window 1
        create_test_record(4, 500.0, 6000), // Window 2
        create_test_record(5, 400.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: MIN=100, MAX=300
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("min_amount"),
            Some(&FieldValue::Float(100.0))
        );
        assert_eq!(
            first_result.fields.get("max_amount"),
            Some(&FieldValue::Float(300.0))
        );
    }

    // Second window: MIN=400, MAX=500
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("min_amount"),
            Some(&FieldValue::Float(400.0))
        );
        assert_eq!(
            second_result.fields.get("max_amount"),
            Some(&FieldValue::Float(500.0))
        );
    }
}

#[tokio::test]
async fn test_sliding_window() {
    // Test sliding window (10s window, 5s advance)
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WINDOW SLIDING(10s, 5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000),  // Will be in multiple windows
        create_test_record(2, 200.0, 3000),  // Will be in multiple windows
        create_test_record(3, 300.0, 6000),  // Triggers first slide
        create_test_record(4, 400.0, 8000),  // Will be in multiple windows
        create_test_record(5, 500.0, 11000), // Triggers second slide
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Sliding windows should emit more frequently than tumbling
    assert!(
        results.len() >= 2,
        "Should emit multiple sliding window results"
    );

    // Each result should contain a count
    for result in &results {
        assert!(result.fields.contains_key("order_count"));
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "Count should be positive");
        }
    }
}

#[tokio::test]
async fn test_session_window() {
    // Test session window with 3-second gap
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WINDOW SESSION(3s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000),  // Start session 1
        create_test_record(2, 200.0, 2000),  // Continue session 1 (within gap)
        create_test_record(3, 300.0, 6000),  // Start session 2 (gap exceeded)
        create_test_record(4, 400.0, 7000),  // Continue session 2
        create_test_record(5, 500.0, 11000), // Start session 3 (gap exceeded)
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Should emit when gaps are exceeded
    assert!(results.len() >= 2, "Should emit session window results");

    for result in &results {
        assert!(result.fields.contains_key("order_count"));
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "Session count should be positive");
        }
    }
}

#[tokio::test]
async fn test_window_with_where_clause() {
    // Test windowed query with WHERE filtering
    let query = "SELECT customer_id, COUNT(*) as high_value_orders FROM orders WHERE amount > 250.0 WINDOW TUMBLING(5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Filtered out (amount <= 250)
        create_test_record(2, 300.0, 2000), // Included
        create_test_record(3, 400.0, 3000), // Included
        create_test_record(4, 200.0, 6000), // Filtered out
        create_test_record(5, 500.0, 7000), // Included
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: 2 records with amount > 250
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("high_value_orders"),
            Some(&FieldValue::Integer(2))
        );
    }

    // Second window: 1 record with amount > 250
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("high_value_orders"),
            Some(&FieldValue::Integer(1))
        );
    }
}

#[tokio::test]
async fn test_window_with_having_clause() {
    // Test windowed query with HAVING filtering
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WINDOW TUMBLING(5s) GROUP BY customer_id HAVING COUNT(*) >= 2";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1: 3 records (passes HAVING)
        create_test_record(2, 200.0, 2000),
        create_test_record(3, 300.0, 3000),
        create_test_record(4, 400.0, 6000), // Window 2: 1 record (fails HAVING)
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Should only emit 1 result (first window that passes HAVING)
    assert_eq!(results.len(), 1, "Should emit only 1 result");

    if let Some(result) = results.first() {
        assert_eq!(
            result.fields.get("order_count"),
            Some(&FieldValue::Integer(3))
        );
    }
}

#[tokio::test]
async fn test_empty_window() {
    // Test window behavior with no matching records
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WHERE amount > 1000.0 WINDOW TUMBLING(5s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // All amounts < 1000, so no matches
        create_test_record(2, 200.0, 2000),
        create_test_record(3, 300.0, 6000),
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // No results should be emitted for empty windows
    assert_eq!(results.len(), 0, "Empty windows should not emit results");
}

#[tokio::test]
async fn test_window_boundary_alignment() {
    // Test that tumbling windows align properly to boundaries
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WINDOW TUMBLING(10s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000),  // Window 1: 0-10000ms
        create_test_record(2, 200.0, 9999),  // Still Window 1 (just before boundary)
        create_test_record(3, 300.0, 10000), // Window 2: 10000-20000ms (exactly on boundary)
        create_test_record(4, 400.0, 15000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(
        results.len(),
        2,
        "Should emit 2 properly aligned window results"
    );

    // First window should have 2 records (including the one at 9999ms)
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("order_count"),
            Some(&FieldValue::Integer(2))
        );
    }

    // Second window should have 2 records (starting exactly at 10000ms)
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("order_count"),
            Some(&FieldValue::Integer(2))
        );
    }
}

#[tokio::test]
async fn test_window_with_multiple_aggregations() {
    // Test window query with multiple aggregation functions
    // This demonstrates advanced window processing patterns similar to subquery behavior
    let query = "SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total FROM orders WINDOW TUMBLING(10s) GROUP BY customer_id";

    // Create test records with varying amounts
    let records = vec![
        create_test_record(1, 100.0, 1000),  // Window 1
        create_test_record(2, 300.0, 2000),  // Window 1
        create_test_record(3, 250.0, 3000),  // Window 1
        create_test_record(4, 50.0, 11000),  // Window 2
        create_test_record(5, 100.0, 12000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Should have 2 results (one for each window)
    assert_eq!(results.len(), 2, "Should have 2 window results");

    // Verify all aggregation fields exist in results
    for result in &results {
        assert!(result.fields.contains_key("customer_id"));
        assert!(result.fields.contains_key("order_count"));
        assert!(result.fields.contains_key("total"));

        // Verify counts are positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "Order count should be positive");
        }

        // Verify totals are reasonable
        if let Some(FieldValue::Float(total)) = result.fields.get("total") {
            assert!(*total > 0.0, "Total should be positive");
        }
    }
}

#[tokio::test]
async fn test_window_with_calculated_fields() {
    // Test window with simple arithmetic that simulates subquery-like behavior
    // This demonstrates advanced window processing patterns within current parser capabilities
    let query = "SELECT customer_id, amount, COUNT(*) as window_count FROM orders WHERE amount > 150 WINDOW TUMBLING(8s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Filtered out (amount <= 150)
        create_test_record(2, 200.0, 2000), // Included - Window 1
        create_test_record(3, 300.0, 3000), // Included - Window 1
        create_test_record(4, 120.0, 4000), // Filtered out (amount <= 150)
        create_test_record(5, 250.0, 9000), // Included - Window 2
        create_test_record(6, 350.0, 10000), // Included - Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(
        results.len(),
        2,
        "Should have 2 windows with filtered results"
    );

    // Verify fields and aggregations
    for result in &results {
        assert!(result.fields.contains_key("customer_id"));
        assert!(result.fields.contains_key("amount"));
        assert!(result.fields.contains_key("window_count"));

        // Verify that amount is above filter threshold
        if let Some(FieldValue::Float(amount)) = result.fields.get("amount") {
            assert!(
                *amount > 150.0,
                "Amount should be above 150 due to WHERE clause"
            );
        }

        // Verify window count is positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("window_count") {
            assert!(*count > 0, "Window count should be positive");
        }
    }
}

#[tokio::test]
async fn test_window_with_subquery_simulation() {
    // Test window with patterns that simulate subquery behavior using basic SQL features
    // This tests advanced conditional logic within current parser capabilities
    let query = "SELECT customer_id, COUNT(*) as total_orders, SUM(amount) as total_amount, AVG(amount) as avg_amount FROM orders WHERE amount > 100 WINDOW TUMBLING(10s) GROUP BY customer_id";

    let records = vec![
        create_test_record(1, 50.0, 1000),   // Window 1 - filtered out
        create_test_record(2, 300.0, 2000),  // Window 1 - included
        create_test_record(3, 200.0, 3000),  // Window 1 - included
        create_test_record(4, 400.0, 4000),  // Window 1 - included
        create_test_record(5, 80.0, 5000),   // Window 1 - filtered out
        create_test_record(6, 350.0, 11000), // Window 2 - included
        create_test_record(7, 90.0, 12000),  // Window 2 - filtered out
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should have 2 window results");

    // Check first window: 3 orders (300, 200, 400), total = 900, avg = 300
    if let Some(first_result) = results.first() {
        assert_eq!(
            first_result.fields.get("total_orders"),
            Some(&FieldValue::Integer(3)),
            "First window should have 3 orders over 100"
        );

        assert_eq!(
            first_result.fields.get("total_amount"),
            Some(&FieldValue::Float(900.0)),
            "First window total should be 900"
        );

        assert_eq!(
            first_result.fields.get("avg_amount"),
            Some(&FieldValue::Float(300.0)),
            "First window average should be 300"
        );
    }

    // Check second window: 1 order (350), total = 350, avg = 350
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result.fields.get("total_orders"),
            Some(&FieldValue::Integer(1)),
            "Second window should have 1 order over 100"
        );

        assert_eq!(
            second_result.fields.get("total_amount"),
            Some(&FieldValue::Float(350.0)),
            "Second window total should be 350"
        );

        assert_eq!(
            second_result.fields.get("avg_amount"),
            Some(&FieldValue::Float(350.0)),
            "Second window average should be 350"
        );
    }
}
