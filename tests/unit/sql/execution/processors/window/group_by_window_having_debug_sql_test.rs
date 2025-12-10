/*!
# GROUP BY → WINDOW → HAVING Debug Tests

Targeted debugging tests to understand why SUM/AVG aggregates in HAVING clauses
are not returning results while COUNT works correctly.

Key hypothesis: The HAVING clause may be using scalar function versions instead of
aggregate function versions for SUM/AVG evaluation.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
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
        topic: None,
        key: None,
    }
}

async fn execute_windowed_query(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
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

/// Debug Test 1: Verify SUM works without HAVING
/// Expected: Should return results with SUM aggregate value
#[tokio::test]
async fn test_sum_without_having() {
    let query = "SELECT customer_id, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 1500),
        create_test_record(3, 150.0, 2000),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!("✓ SUM without HAVING returned {} results", results.len());
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
        }
        Err(e) => {
            eprintln!("✗ SUM without HAVING failed: {}", e);
        }
    }
}

/// Debug Test 2: Verify COUNT with HAVING works (known working case)
/// Expected: Should return results matching the HAVING condition
#[tokio::test]
async fn test_count_with_having() {
    let query = "SELECT customer_id, COUNT(*) as cnt FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING COUNT(*) >= 1";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 1500),
        create_test_record(3, 150.0, 2000),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!("✓ COUNT with HAVING returned {} results", results.len());
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
        }
        Err(e) => {
            eprintln!("✗ COUNT with HAVING failed: {}", e);
        }
    }
}

/// Debug Test 3: Test SUM with simple HAVING condition (no multiple aggregates)
/// Expected: Should return results if aggregate path is correct
#[tokio::test]
async fn test_sum_simple_having() {
    let query = "SELECT customer_id, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING SUM(amount) > 100.0";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 1500),
        create_test_record(3, 150.0, 2000),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!("✓ SUM with HAVING returned {} results", results.len());
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
            if results.is_empty() {
                println!(
                    "  ⚠️  No results - likely using scalar function path instead of aggregate"
                );
            }
        }
        Err(e) => {
            eprintln!("✗ SUM with HAVING failed: {}", e);
        }
    }
}

/// Debug Test 4: Test SUM with comparison to literal value
/// This tests if the aggregate is being evaluated at all
#[tokio::test]
async fn test_sum_literal_comparison() {
    let query = "SELECT customer_id, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING 500.0 > 100.0";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 1500),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!(
                "✓ Literal comparison HAVING returned {} results",
                results.len()
            );
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
        }
        Err(e) => {
            eprintln!("✗ Literal comparison HAVING failed: {}", e);
        }
    }
}

/// Debug Test 5: Mix COUNT (working) with SUM (not working) in WHERE clause
/// This tests if the issue is specific to HAVING or broader
#[tokio::test]
async fn test_count_and_sum_without_having() {
    let query = "SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 1500),
        create_test_record(3, 150.0, 2000),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!(
                "✓ COUNT + SUM without HAVING returned {} results",
                results.len()
            );
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
        }
        Err(e) => {
            eprintln!("✗ COUNT + SUM without HAVING failed: {}", e);
        }
    }
}

/// Debug Test 6: Test if field names in HAVING matter
/// Some systems fail if aggregate alias is used instead of function
#[tokio::test]
async fn test_sum_having_with_alias_vs_function() {
    let query = "SELECT customer_id, SUM(amount) as total FROM orders \
                GROUP BY customer_id \
                WINDOW TUMBLING(5s) \
                HAVING total > 100.0";

    let records = vec![
        create_test_record(1, 150.0, 1000),
        create_test_record(2, 200.0, 1500),
    ];

    match execute_windowed_query(query, records).await {
        Ok(results) => {
            println!("✓ SUM HAVING with alias returned {} results", results.len());
            for (idx, result) in results.iter().enumerate() {
                println!("  Result {}: {:?}", idx, result.fields);
            }
        }
        Err(e) => {
            println!("✗ SUM HAVING with alias error (expected): {}", e);
        }
    }
}
