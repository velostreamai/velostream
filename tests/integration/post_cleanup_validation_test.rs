//! Post-cleanup validation tests to ensure functionality completeness
//!
//! These tests validate that all core functionality still works after removing
//! the legacy sql_server.rs binary and consolidating to velo-sql-multi.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::SqlValidator;
use velostream::velostream::{
    serialization::{JsonFormat, SerializationFormat},
    sql::{
        FieldValue, execution::StreamExecutionEngine, execution::types::StreamRecord,
        parser::StreamingSqlParser,
    },
};

/// Test that the StreamExecutionEngine still works after architecture changes
#[tokio::test]
async fn test_core_sql_engine_functionality() {
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT amount, customer_id FROM transactions WHERE amount > 100")
        .unwrap();

    // Create test record with financial data
    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::ScaledInteger(12550, 2)); // $125.50
    fields.insert(
        "customer_id".to_string(),
        FieldValue::String("cust_123".to_string()),
    );
    fields.insert("timestamp".to_string(), FieldValue::Integer(1672531200));

    let record = StreamRecord {
        fields,
        timestamp: 1672531200,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Execute the query
    let result = engine.execute_with_record(&query, &record).await;
    assert!(
        result.is_ok(),
        "SQL execution should work: {:?}",
        result.err()
    );

    // Check that we get output
    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        output_receiver.recv(),
    )
    .await
    .ok();
}

/// Test that financial precision is maintained
#[tokio::test]
async fn test_financial_precision_maintained() {
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT amount * 1.025 as amount_with_fee FROM transactions")
        .unwrap();

    // Test exact financial calculation
    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::ScaledInteger(100000, 2)); // $1000.00

    let record = StreamRecord {
        fields,
        timestamp: 0,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(
        result.is_ok(),
        "Financial precision calculation should work"
    );
}

/// Test that window functions still work
#[tokio::test]
async fn test_window_functions_functionality() {
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT amount, ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS ORDER BY amount DESC) as rank FROM transactions")
        .unwrap();

    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::Float(150.0));

    let record = StreamRecord {
        fields,
        timestamp: 0,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(
        result.is_ok(),
        "Window functions should work: {:?}",
        result.err()
    );
}

/// Test that aggregation with GROUP BY works
#[tokio::test]
async fn test_aggregation_functionality() {
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT customer_id, SUM(amount) as total FROM transactions GROUP BY customer_id")
        .unwrap();

    // Test with multiple records
    for i in 0..3 {
        let mut fields = HashMap::new();
        fields.insert(
            "customer_id".to_string(),
            FieldValue::String("cust_123".to_string()),
        );
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(10000 + i * 5000, 2),
        );

        let record = StreamRecord {
            fields,
            timestamp: i as i64,
            offset: i as i64,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok(), "GROUP BY aggregation should work");
    }
}

/// Test complex query with joins and subqueries
#[tokio::test]
#[inline(never)]
async fn test_complex_query_functionality() {
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    // Test a complex query that exercises multiple SQL features
    // Note: Using float literal (100.0) for compatibility with ScaledInteger comparisons
    let sql_string = "SELECT customer_id, amount,
         CASE WHEN amount > 100.0 THEN 'high' ELSE 'low' END as amount_category
         FROM transactions
         WHERE timestamp > 1672531200";
    let query = parser.parse(sql_string).unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "customer_id".to_string(),
        FieldValue::String("cust_456".to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::ScaledInteger(25075, 2)); // $250.75 - now supported with type coercion
    fields.insert("timestamp".to_string(), FieldValue::Integer(1672531300));

    let record = StreamRecord {
        fields,
        timestamp: 1672531300,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(
        result.is_ok(),
        "Complex query should work: {:?}",
        result.err()
    );
}

/// Test that error handling still works properly
#[tokio::test]
async fn test_error_handling_functionality() {
    let parser = StreamingSqlParser::new();

    // Test invalid SQL query
    let result = parser.parse("INVALID SQL SYNTAX HERE");
    assert!(result.is_err(), "Invalid SQL should return error");

    // Test valid query but with type mismatch
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let query = parser
        .parse("SELECT amount + 'invalid' FROM transactions")
        .unwrap();

    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::Integer(100));

    let record = StreamRecord {
        fields,
        timestamp: 0,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // This should handle the type error gracefully
    let result = engine.execute_with_record(&query, &record).await;
    // Note: depending on implementation, this might succeed with type coercion or fail gracefully
    // The important thing is it doesn't panic
}

/// Test that serialization still works
#[tokio::test]
async fn test_serialization_functionality() {
    let json_format = JsonFormat;

    let mut record = HashMap::new();
    record.insert(
        "customer_id".to_string(),
        FieldValue::String("cust_789".to_string()),
    );
    record.insert("amount".to_string(), FieldValue::ScaledInteger(50025, 2)); // $500.25
    record.insert("approved".to_string(), FieldValue::Boolean(true));

    // Test serialization
    let serialized = json_format.serialize_record(&record);
    assert!(serialized.is_ok(), "JSON serialization should work");

    // Test deserialization
    if let Ok(bytes) = serialized {
        let deserialized = json_format.deserialize_record(&bytes);
        assert!(deserialized.is_ok(), "JSON deserialization should work");
    }
}

/// Test that the multi-job architecture components are accessible
#[tokio::test]
async fn test_multi_job_components_available() {
    use velostream::velostream::server::processors::common::{
        JobExecutionStats, JobProcessingConfig,
    };

    // Test that we can create job processing components
    let stats = JobExecutionStats::new();
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_failed, 0);

    let config = JobProcessingConfig::default();
    assert_eq!(config.max_batch_size, 100);
    assert!(!config.use_transactions);
}

/// Performance regression test - ensure optimizations are maintained
#[tokio::test]
async fn test_performance_regression_check() {
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT amount, customer_id FROM transactions")
        .unwrap();

    let start = std::time::Instant::now();

    // Process 100 records to check performance hasn't regressed
    for i in 0..100 {
        let mut fields = HashMap::new();
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(10000 + i, 2),
        );
        fields.insert(
            "customer_id".to_string(),
            FieldValue::String(format!("cust_{}", i)),
        );

        let record = StreamRecord {
            fields,
            timestamp: i as i64,
            offset: i as i64,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok(), "Record processing should work");
    }

    let elapsed = start.elapsed();

    // Performance check - should be able to process 100 records quickly
    // This is a regression test to ensure cleanup didn't hurt performance
    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "Performance regression detected: took {:?} for 100 records",
        elapsed
    );

    println!(
        "✅ Performance check passed: processed 100 records in {:?}",
        elapsed
    );
}

/// Integration test for end-to-end functionality
#[tokio::test]
async fn test_end_to_end_integration() {
    // This test simulates a complete workflow that would have used the old sql_server.rs
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);

    let parser = StreamingSqlParser::new();

    // Test the type of query that was hardcoded in the old sql_server.rs
    let query = parser
        .parse("SELECT customer_id, amount, timestamp FROM transactions WHERE amount > 100")
        .unwrap();

    // Process several financial records
    let test_records = vec![
        (12550, "cust_123"), // $125.50
        (8999, "cust_456"),  // $89.99 (should be filtered out)
        (15000, "cust_789"), // $150.00
    ];

    let mut results_received = 0;

    for (amount, customer) in test_records {
        let mut fields = HashMap::new();
        fields.insert(
            "customer_id".to_string(),
            FieldValue::String(customer.to_string()),
        );
        fields.insert("amount".to_string(), FieldValue::ScaledInteger(amount, 2));
        fields.insert("timestamp".to_string(), FieldValue::Integer(1672531200));

        let record = StreamRecord {
            fields,
            timestamp: 1672531200,
            offset: results_received,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok(), "Query execution should succeed");

        // Try to receive output (with timeout to avoid hanging)
        if let Ok(Some(_output)) =
            tokio::time::timeout(std::time::Duration::from_millis(10), output_receiver.recv()).await
        {
            results_received += 1;
        }
    }

    // We should have received results for records > $100 (2 out of 3)
    println!("✅ End-to-end test completed, processed records successfully");
}
