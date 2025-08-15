/*!
# Window Processing Tests

Comprehensive tests for the newly implemented window processing functionality including:
- Tumbling windows with COUNT, SUM, AVG, MIN, MAX aggregations
- Sliding windows with proper buffer management
- Session windows with gap timeout handling
- Event-time vs processing-time semantics
- Window boundary detection and emission logic
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::StreamingQuery;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record(id: i64, amount: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("customer_id".to_string(), FieldValue::Integer(1));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp,
        offset: id,
        partition: 0,
    }
}

fn convert_stream_record_to_internal(record: &StreamRecord) -> HashMap<String, InternalValue> {
    record
        .fields
        .iter()
        .map(|(k, v)| {
            let internal_val = match v {
                FieldValue::Integer(i) => InternalValue::Integer(*i),
                FieldValue::Float(f) => InternalValue::Number(*f),
                FieldValue::String(s) => InternalValue::String(s.clone()),
                FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                FieldValue::Null => InternalValue::Null,
                _ => InternalValue::String(format!("{:?}", v)),
            };
            (k.clone(), internal_val)
        })
        .collect()
}

async fn execute_windowed_test(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;

    // Register the query
    engine
        .start_query_execution("test_query".to_string(), parsed_query)
        .await?;

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
                    // Propagate other errors
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
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id WINDOW TUMBLING(5s)";

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
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["order_count"], InternalValue::Integer(3));
    }

    // Second window should have count of 2
    if let Some(second_result) = results.get(1) {
        assert_eq!(second_result["order_count"], InternalValue::Integer(2));
    }
}

#[tokio::test]
async fn test_tumbling_window_sum() {
    // Test tumbling window with SUM aggregation
    let query = "SELECT customer_id, SUM(amount) as total_amount FROM orders GROUP BY customer_id WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1
        create_test_record(2, 200.0, 2000), // Window 1
        create_test_record(3, 300.0, 6000), // Window 2
        create_test_record(4, 400.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: 100 + 200 = 300
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["total_amount"], InternalValue::Number(300.0));
    }

    // Second window: 300 + 400 = 700
    if let Some(second_result) = results.get(1) {
        assert_eq!(second_result["total_amount"], InternalValue::Number(700.0));
    }
}

#[tokio::test]
async fn test_tumbling_window_avg() {
    // Test tumbling window with AVG aggregation
    let query = "SELECT customer_id, AVG(amount) as avg_amount FROM orders GROUP BY customer_id WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1
        create_test_record(2, 200.0, 2000), // Window 1
        create_test_record(3, 400.0, 6000), // Window 2
        create_test_record(4, 600.0, 7000), // Window 2
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    assert_eq!(results.len(), 2, "Should emit 2 window results");

    // First window: (100 + 200) / 2 = 150
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["avg_amount"], InternalValue::Number(150.0));
    }

    // Second window: (400 + 600) / 2 = 500
    if let Some(second_result) = results.get(1) {
        assert_eq!(second_result["avg_amount"], InternalValue::Number(500.0));
    }
}

#[tokio::test]
async fn test_tumbling_window_min_max() {
    // Test tumbling window with MIN and MAX aggregations
    let query = "SELECT customer_id, MIN(amount) as min_amount, MAX(amount) as max_amount FROM orders GROUP BY customer_id WINDOW TUMBLING(5s)";

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
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["min_amount"], InternalValue::Number(100.0));
        assert_eq!(first_result["max_amount"], InternalValue::Number(300.0));
    }

    // Second window: MIN=400, MAX=500
    if let Some(second_result) = results.get(1) {
        assert_eq!(second_result["min_amount"], InternalValue::Number(400.0));
        assert_eq!(second_result["max_amount"], InternalValue::Number(500.0));
    }
}

#[tokio::test]
async fn test_sliding_window() {
    // Test sliding window (10s window, 5s advance)
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id WINDOW SLIDING(10s, 5s)";

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
        assert!(result.contains_key("order_count"));
        if let InternalValue::Integer(count) = &result["order_count"] {
            assert!(*count > 0, "Count should be positive");
        }
    }
}

#[tokio::test]
async fn test_session_window() {
    // Test session window with 3-second gap
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id WINDOW SESSION(3s)";

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
        assert!(result.contains_key("order_count"));
        if let InternalValue::Integer(count) = &result["order_count"] {
            assert!(*count > 0, "Session count should be positive");
        }
    }
}

#[tokio::test]
async fn test_window_with_where_clause() {
    // Test windowed query with WHERE filtering
    let query = "SELECT customer_id, COUNT(*) as high_value_orders FROM orders WHERE amount > 250.0 GROUP BY customer_id WINDOW TUMBLING(5s)";

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
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["high_value_orders"], InternalValue::Integer(2));
    }

    // Second window: 1 record with amount > 250
    if let Some(second_result) = results.get(1) {
        assert_eq!(
            second_result["high_value_orders"],
            InternalValue::Integer(1)
        );
    }
}

#[tokio::test]
async fn test_window_with_having_clause() {
    // Test windowed query with HAVING filtering
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id HAVING COUNT(*) >= 2 WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 100.0, 1000), // Window 1: 3 records (passes HAVING)
        create_test_record(2, 200.0, 2000),
        create_test_record(3, 300.0, 3000),
        create_test_record(4, 400.0, 6000), // Window 2: 1 record (fails HAVING)
    ];

    let results = execute_windowed_test(query, records).await.unwrap();

    // Should only emit 1 result (first window that passes HAVING)
    assert_eq!(
        results.len(),
        1,
        "Should emit only windows passing HAVING clause"
    );

    if let Some(result) = results.get(0) {
        assert_eq!(result["order_count"], InternalValue::Integer(3));
    }
}

#[tokio::test]
async fn test_empty_window() {
    // Test window behavior with no matching records
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders WHERE amount > 1000.0 GROUP BY customer_id WINDOW TUMBLING(5s)";

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
    let query = "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id WINDOW TUMBLING(10s)";

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
    if let Some(first_result) = results.get(0) {
        assert_eq!(first_result["order_count"], InternalValue::Integer(2));
    }

    // Second window should have 2 records (starting exactly at 10000ms)
    if let Some(second_result) = results.get(1) {
        assert_eq!(second_result["order_count"], InternalValue::Integer(2));
    }
}
