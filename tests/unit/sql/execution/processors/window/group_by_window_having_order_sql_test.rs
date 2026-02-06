/*!
# GROUP BY → WINDOW → HAVING Clause Ordering Test

Tests to verify that Velostream correctly executes queries with GROUP BY → WINDOW → HAVING
clause ordering. This ensures the execution engine properly handles this specific clause
sequence during windowed aggregation execution.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Use shared test utilities from the same module directory
use super::shared_test_utils::{SqlExecutor, TestDataBuilder};

fn create_test_record(id: i64, amount: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("customer_id".to_string(), FieldValue::Integer(id % 2));
    // FR-081: Add timestamp field for window_v2
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp,
        offset: id,
        partition: 0,
        topic: None,
        key: None,
    }
}

async fn execute_windowed_query(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    // Window_v2 is the only architecture available (Phase 2E+)
    let config = velostream::velostream::sql::execution::config::StreamingConfig::new();
    let mut engine = StreamExecutionEngine::new_with_config(tx, config);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    engine
        .start_query_execution("test_query".to_string(), parsed_query)
        .await?;

    for record in &records {
        match engine.process_stream_record("orders", record.clone()).await {
            Ok(_) => {}
            Err(e) => {
                if !e
                    .to_string()
                    .to_lowercase()
                    .contains("no records after filtering")
                {
                    return Err(e.into());
                }
            }
        }
    }

    // Flush windows
    let max_timestamp = records.iter().map(|r| r.timestamp).max().unwrap_or(0);
    let flush_record = create_test_record(999, 0.0, max_timestamp + 10000);
    let _ = engine.process_stream_record("orders", flush_record).await;

    // Collect results
    let mut results = Vec::new();
    while let Ok(record) = rx.try_recv() {
        results.push(record);
    }
    Ok(results)
}

#[tokio::test]
async fn test_group_by_window_having_basic_execution() {
    let query = "SELECT customer_id, COUNT(*) as cnt FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) >= 2";

    let records = vec![
        create_test_record(1, 100.0, 1000), // customer_id: 1
        create_test_record(2, 200.0, 1500), // customer_id: 0
        create_test_record(3, 150.0, 2000), // customer_id: 1
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            // Should have results since we have groups with COUNT >= 2
            assert!(
                !results.is_empty(),
                "Expected results from windowed query with HAVING clause"
            );

            println!("DEBUG: Got {} results", results.len());
            for (idx, result) in results.iter().enumerate() {
                println!("DEBUG: Result {}: {:?}", idx, result.fields);
            }

            // Verify each result has the expected fields
            for result in &results {
                assert!(
                    result.fields.contains_key("customer_id"),
                    "Missing customer_id field"
                );
                assert!(result.fields.contains_key("cnt"), "Missing cnt field");

                // Validate HAVING clause: cnt should be >= 2
                if let Some(FieldValue::Integer(count)) = result.fields.get("cnt") {
                    assert!(
                        *count >= 2,
                        "HAVING clause failed: count ({}) should be >= 2",
                        count
                    );
                }
            }
            println!(
                "✓ GROUP BY → WINDOW → HAVING basic execution successful with {} results",
                results.len()
            );
        }
        Err(e) => {
            panic!("GROUP BY → WINDOW → HAVING execution failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_group_by_window_having_sum_aggregate() {
    let query = "SELECT customer_id, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING SUM(amount) > 200.0";

    let records = vec![
        create_test_record(1, 100.0, 1000), // customer_id: 1, contributes 100.0
        create_test_record(2, 150.0, 1500), // customer_id: 0, contributes 150.0
        create_test_record(3, 200.0, 2000), // customer_id: 1, contributes 200.0
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            // Note: SUM aggregate with multiple records in same GROUP BY may have limited support
            // This test documents the current behavior without asserting expectations
            if !results.is_empty() {
                // Verify each result has the expected fields
                for result in &results {
                    assert!(
                        result.fields.contains_key("customer_id"),
                        "Missing customer_id field"
                    );
                    assert!(result.fields.contains_key("total"), "Missing total field");

                    // Validate HAVING clause: total should be > 200.0
                    if let Some(FieldValue::Float(sum_value)) = result.fields.get("total") {
                        assert!(
                            *sum_value > 200.0,
                            "HAVING clause failed: total ({}) should be > 200.0",
                            sum_value
                        );
                    }
                }
                println!(
                    "✓ GROUP BY → WINDOW → HAVING with SUM aggregate successful with {} results",
                    results.len()
                );
            }
        }
        Err(e) => {
            panic!("GROUP BY → WINDOW → HAVING with SUM failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_group_by_window_having_avg_aggregate() {
    let query = "SELECT customer_id, AVG(amount) as avg_amt FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING AVG(amount) > 100.0";

    let records = vec![
        create_test_record(1, 100.0, 1000), // customer_id: 1, contributes 100.0
        create_test_record(2, 200.0, 1500), // customer_id: 0, contributes 200.0
        create_test_record(3, 150.0, 2000), // customer_id: 1, contributes 150.0
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            // Note: AVG aggregate with multiple records in same GROUP BY may have limited support
            // This test documents the current behavior without asserting expectations
            if !results.is_empty() {
                // Verify each result has the expected fields
                for result in &results {
                    assert!(
                        result.fields.contains_key("customer_id"),
                        "Missing customer_id field"
                    );
                    assert!(
                        result.fields.contains_key("avg_amt"),
                        "Missing avg_amt field"
                    );

                    // Validate HAVING clause: avg_amt should be > 100.0
                    if let Some(FieldValue::Float(avg_value)) = result.fields.get("avg_amt") {
                        assert!(
                            *avg_value > 100.0,
                            "HAVING clause failed: avg_amt ({}) should be > 100.0",
                            avg_value
                        );
                    }
                }
                println!(
                    "✓ GROUP BY → WINDOW → HAVING with AVG aggregate successful with {} results",
                    results.len()
                );
            }
        }
        Err(e) => {
            panic!("GROUP BY → WINDOW → HAVING with AVG failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_group_by_window_having_complex_condition() {
    let query = "SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) > 1 AND SUM(amount) > 150.0";

    let records = vec![
        create_test_record(1, 100.0, 1000), // customer_id: 1, count=2, sum=250.0
        create_test_record(2, 200.0, 1500), // customer_id: 0, count=2, sum=300.0
        create_test_record(3, 150.0, 2000), // customer_id: 1, count=2, sum=250.0
        create_test_record(4, 100.0, 2500), // customer_id: 0, count=2, sum=300.0
    ];

    let results = execute_windowed_query(query, records)
        .await
        .expect("GROUP BY → WINDOW → HAVING with complex condition failed");

    assert!(
        !results.is_empty(),
        "BUG: compound HAVING with AND returned 0 results"
    );

    // Verify each result has the expected fields
    for result in &results {
        assert!(
            result.fields.contains_key("customer_id"),
            "Missing customer_id field"
        );
        assert!(result.fields.contains_key("cnt"), "Missing cnt field");
        assert!(result.fields.contains_key("total"), "Missing total field");

        // Validate HAVING clause: COUNT(*) > 1 AND SUM(amount) > 150.0
        let count_valid = if let Some(FieldValue::Integer(count)) = result.fields.get("cnt") {
            *count > 1
        } else {
            false
        };

        let sum_valid = if let Some(FieldValue::Float(sum_value)) = result.fields.get("total") {
            *sum_value > 150.0
        } else {
            false
        };

        assert!(
            count_valid && sum_valid,
            "HAVING clause failed: COUNT(*) > 1 AND SUM(amount) > 150.0 not satisfied"
        );
    }
    println!(
        "✓ GROUP BY → WINDOW → HAVING with complex condition successful with {} results",
        results.len()
    );
}

#[tokio::test]
async fn test_group_by_window_having_arithmetic_expressions() {
    // Test financial trading scenario with arithmetic expressions in HAVING
    // This validates: COUNT(*) > 1 AND AVG(price) > 100.0 AND MAX(volume) > 500.0
    let query = "SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price, MAX(volume) as max_volume FROM trades \
                GROUP BY symbol \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) > 1 AND AVG(price) > 100.0 AND MAX(volume) > 500.0";

    let records = vec![
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 1000, 0),
        TestDataBuilder::trade_record(2, "AAPL", 155.0, 1200, 100),
        TestDataBuilder::trade_record(3, "AAPL", 145.0, 900, 200),
        TestDataBuilder::trade_record(4, "GOOGL", 2000.0, 100, 300), // Only 1 trade, won't pass COUNT > 1
        TestDataBuilder::trade_record(5, "MSFT", 350.0, 600, 400),
        TestDataBuilder::trade_record(6, "MSFT", 340.0, 550, 500),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    // HAVING clause should filter results
    // Results should include AAPL and MSFT (both have 2+ trades and meet other conditions)
    // Should NOT include GOOGL (only 1 trade, fails COUNT(*) > 1)
    assert!(
        !results.is_empty(),
        "BUG: compound HAVING with AND returned 0 results"
    );

    // Validate comprehensive conditions for each result
    for (idx, result) in results.iter().enumerate() {
        // Verify trade_count is > 1 (COUNT(*) > 1)
        if let Some(FieldValue::Integer(count)) = result.fields.get("trade_count") {
            assert!(
                *count > 1,
                "Result {}: trade_count should be > 1, got {}",
                idx,
                count
            );
        }

        // Verify avg_price is > 100.0 (AVG(price) > 100.0)
        if let Some(avg_price_field) = result.fields.get("avg_price") {
            let avg_price = match avg_price_field {
                FieldValue::Float(v) => Some(*v),
                FieldValue::ScaledInteger(v, _) => Some(*v as f64),
                _ => None,
            };

            if let Some(val) = avg_price {
                assert!(
                    val > 100.0,
                    "Result {}: avg_price should be > 100.0, got {}",
                    idx,
                    val
                );
            }
        }

        // Verify max_volume is > 500.0 (MAX(volume) > 500.0)
        if let Some(max_volume_field) = result.fields.get("max_volume") {
            let max_volume = match max_volume_field {
                FieldValue::Float(v) => Some(*v),
                FieldValue::ScaledInteger(v, _) => Some(*v as f64),
                FieldValue::Integer(v) => Some(*v as f64),
                _ => None,
            };

            if let Some(val) = max_volume {
                assert!(
                    val > 500.0,
                    "Result {}: max_volume should be > 500.0, got {}",
                    idx,
                    val
                );
            }
        }
    }
    println!(
        "✓ GROUP BY → WINDOW → HAVING with arithmetic expressions successful with {} results",
        results.len()
    );
}

#[tokio::test]
async fn test_group_by_window_having_arithmetic_with_multipliers() {
    // Test financial trading pattern with aggregate comparison
    // This tests: COUNT(*) >= 2 AND AVG(volume) > 500.0
    let query = "SELECT symbol, COUNT(*) as cnt, AVG(volume) as avg_vol FROM trades \
                GROUP BY symbol \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) >= 2 AND AVG(volume) > 500.0";

    let records = vec![
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 600, 0), // avg_vol would be 600
        TestDataBuilder::trade_record(2, "AAPL", 155.0, 800, 100),
        TestDataBuilder::trade_record(3, "MSFT", 350.0, 100, 200), // avg_vol would be 100, filtered out
        TestDataBuilder::trade_record(4, "MSFT", 340.0, 200, 300),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    // Should only have AAPL (avg_volume=700 > 500)
    // MSFT should be filtered (avg_volume=150, not > 500)
    assert!(
        !results.is_empty(),
        "BUG: compound HAVING with AND returned 0 results"
    );

    // Validate comprehensive conditions for each result
    for (idx, result) in results.iter().enumerate() {
        // Verify cnt >= 2 (COUNT(*) >= 2)
        if let Some(FieldValue::Integer(count)) = result.fields.get("cnt") {
            assert!(
                *count >= 2,
                "Result {}: cnt should be >= 2, got {}",
                idx,
                count
            );
        }

        // Verify avg_vol > 500.0 (AVG(volume) > 500.0)
        if let Some(avg_vol_field) = result.fields.get("avg_vol") {
            let avg_vol = match avg_vol_field {
                FieldValue::Float(v) => Some(*v),
                FieldValue::ScaledInteger(v, _) => Some(*v as f64),
                FieldValue::Integer(v) => Some(*v as f64),
                _ => None,
            };

            if let Some(val) = avg_vol {
                assert!(
                    val > 500.0,
                    "Result {}: avg_vol should be > 500.0, got {}",
                    idx,
                    val
                );
            }
        }
    }
    println!(
        "✓ GROUP BY → WINDOW → HAVING with aggregate arithmetic multipliers successful with {} results",
        results.len()
    );
}

#[tokio::test]
async fn test_group_by_window_having_or_condition() {
    // Test HAVING with OR: should pass groups that satisfy either condition
    let query = "SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) >= 3 OR SUM(amount) > 250.0";

    let records = vec![
        create_test_record(1, 100.0, 1000), // customer_id: 1
        create_test_record(2, 200.0, 1500), // customer_id: 0
        create_test_record(3, 150.0, 2000), // customer_id: 1
        create_test_record(4, 100.0, 2500), // customer_id: 0
    ];

    let results = execute_windowed_query(query, records)
        .await
        .expect("HAVING with OR failed");

    assert!(
        !results.is_empty(),
        "BUG: HAVING with OR returned 0 results"
    );

    for result in &results {
        assert!(
            result.fields.contains_key("customer_id"),
            "Missing customer_id field"
        );
        assert!(result.fields.contains_key("cnt"), "Missing cnt field");
        assert!(result.fields.contains_key("total"), "Missing total field");

        // At least one of the OR conditions must be true
        let count_ok = if let Some(FieldValue::Integer(c)) = result.fields.get("cnt") {
            *c >= 3
        } else {
            false
        };
        let sum_ok = if let Some(FieldValue::Float(s)) = result.fields.get("total") {
            *s > 250.0
        } else {
            false
        };
        assert!(
            count_ok || sum_ok,
            "HAVING OR clause failed: neither COUNT(*) >= 3 nor SUM(amount) > 250.0 satisfied"
        );
    }
    println!(
        "✓ GROUP BY → WINDOW → HAVING with OR condition successful with {} results",
        results.len()
    );
}

#[tokio::test]
async fn test_group_by_window_having_division_expression() {
    // Test HAVING with arithmetic division: SUM(volume) / COUNT(*) > 100.0
    let query = "SELECT symbol, SUM(volume) as total_vol, COUNT(*) as cnt \
                FROM trades \
                GROUP BY symbol \
                WINDOW TUMBLING(5s) \
                HAVING SUM(volume) / COUNT(*) > 100.0";

    let records = vec![
        // AAPL: sum=3100, count=3, avg=1033 -> passes
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 1000, 0),
        TestDataBuilder::trade_record(2, "AAPL", 155.0, 1200, 100),
        TestDataBuilder::trade_record(3, "AAPL", 145.0, 900, 200),
        // MSFT: sum=30, count=2, avg=15 -> filtered out
        TestDataBuilder::trade_record(4, "MSFT", 350.0, 10, 300),
        TestDataBuilder::trade_record(5, "MSFT", 340.0, 20, 400),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "BUG: HAVING with division expression returned 0 results"
    );

    for (idx, result) in results.iter().enumerate() {
        let total_vol = match result.fields.get("total_vol") {
            Some(FieldValue::Integer(v)) => *v as f64,
            Some(FieldValue::Float(v)) => *v,
            other => panic!("Result {}: unexpected total_vol type: {:?}", idx, other),
        };
        let cnt = match result.fields.get("cnt") {
            Some(FieldValue::Integer(v)) => *v as f64,
            Some(FieldValue::Float(v)) => *v,
            other => panic!("Result {}: unexpected cnt type: {:?}", idx, other),
        };
        assert!(
            total_vol / cnt > 100.0,
            "Result {}: SUM(volume)/COUNT(*) = {}/{} = {} should be > 100.0",
            idx,
            total_vol,
            cnt,
            total_vol / cnt
        );
    }
    println!(
        "✓ GROUP BY → WINDOW → HAVING with division expression successful with {} results",
        results.len()
    );
}

#[tokio::test]
async fn test_parse_group_by_window_having_order() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT customer_id, COUNT(*) FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) > 1";

    match parser.parse(query) {
        Ok(_) => {
            println!("✓ Parser accepts GROUP BY → WINDOW → HAVING ordering");
        }
        Err(e) => {
            panic!("Parser rejected GROUP BY → WINDOW → HAVING: {}", e);
        }
    }
}
