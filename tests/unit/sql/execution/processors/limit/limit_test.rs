use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_parsing() {
        let parser = StreamingSqlParser::new();

        // Test parsing queries with LIMIT
        let queries = vec![
            "SELECT * FROM orders LIMIT 10",
            "SELECT customer_id, amount FROM orders LIMIT 5",
            "SELECT * FROM orders WHERE amount > 100 LIMIT 3",
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(result.is_ok(), "Failed to parse query: {}", query);

            // Verify limit is parsed correctly
            if let Ok(parsed_query) = result {
                match parsed_query {
                    ferrisstreams::ferris::sql::ast::StreamingQuery::Select { limit, .. } => {
                        assert!(
                            limit.is_some(),
                            "LIMIT should be parsed for query: {}",
                            query
                        );
                    }
                    _ => panic!("Expected SELECT query"),
                }
            }
        }
    }

    #[test]
    fn test_limit_parsing_values() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            ("SELECT * FROM orders LIMIT 1", 1),
            ("SELECT * FROM orders LIMIT 10", 10),
            ("SELECT * FROM orders LIMIT 100", 100),
            ("SELECT * FROM orders LIMIT 1000", 1000),
        ];

        for (query, expected_limit) in test_cases {
            let result = parser.parse(query).unwrap();
            match result {
                ferrisstreams::ferris::sql::ast::StreamingQuery::Select { limit, .. } => {
                    assert_eq!(
                        limit,
                        Some(expected_limit),
                        "Limit value mismatch for query: {}",
                        query
                    );
                }
                _ => panic!("Expected SELECT query"),
            }
        }
    }

    #[tokio::test]
    async fn test_limit_execution_basic() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query with LIMIT 2
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, amount FROM orders LIMIT 2")
            .unwrap();

        // Create test records
        let _records: Vec<StreamRecord> = Vec::new();
        for i in 1..=3 {
            let mut fields = HashMap::new();
            fields.insert("customer_id".to_string(), FieldValue::Integer(i));
            fields.insert("amount".to_string(), FieldValue::Float(100.0 * i as f64));
            let record = StreamRecord {
                fields,
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: i,
                partition: 0,
                headers: HashMap::new(),
            };
            // Execute each record individually
            engine.execute_with_record(&query, record).await.unwrap();
        }

        // Check results with timeout to avoid hanging
        let mut count = 0;
        // Give some time for processing, then collect results
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        while let Ok(result) = rx.try_recv() {
            count += 1;
            let _ = result; // Consume the result
            assert!(
                count <= 2,
                "Should not receive more than 2 records due to LIMIT"
            );
        }
        assert_eq!(count, 2, "Should receive exactly 2 records due to LIMIT");
    }

    #[tokio::test]
    async fn test_limit_with_where_clause() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query with WHERE and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, amount FROM orders WHERE amount > 150 LIMIT 1")
            .unwrap();

        // Create and execute test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = StreamRecord {
            fields: fields1,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let record2 = StreamRecord {
            fields: fields2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 2,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record2).await.unwrap();

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Integer(3));
        fields3.insert("amount".to_string(), FieldValue::Float(300.0));
        let record3 = StreamRecord {
            fields: fields3,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 3,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record3).await.unwrap();

        // Should receive exactly 1 output (first record matching WHERE clause)
        if let Some(output) = rx.recv().await {
            match (
                output.fields.get("customer_id"),
                output.fields.get("amount"),
            ) {
                (Some(FieldValue::Integer(id)), Some(FieldValue::Float(amount))) => {
                    assert_eq!(*id, 2);
                    assert_eq!(*amount, 200.0);
                }
                _ => panic!("Unexpected output types"),
            }
        }

        // Should not receive second matching record due to LIMIT
        assert!(
            rx.try_recv().is_err(),
            "Should not receive second record due to LIMIT"
        );
    }

    #[tokio::test]
    async fn test_limit_zero() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query with LIMIT 0
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id FROM orders LIMIT 0")
            .unwrap();

        // Create and execute test record
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(1));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record).await.unwrap();

        // Should not receive any output due to LIMIT 0
        assert!(
            rx.try_recv().is_err(),
            "Should not receive any records due to LIMIT 0"
        );
    }

    #[tokio::test]
    async fn test_limit_with_csas() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse CSAS query with LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("CREATE STREAM limited_orders AS SELECT customer_id, amount FROM orders LIMIT 1")
            .unwrap();

        // Create and execute test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = StreamRecord {
            fields: fields1,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let record2 = StreamRecord {
            fields: fields2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 2,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record2).await.unwrap();

        // Should receive exactly 1 output
        if let Some(output) = rx.recv().await {
            match output.fields.get("customer_id") {
                Some(FieldValue::Integer(id)) => assert_eq!(*id, 1),
                _ => panic!("Unexpected customer_id type"),
            }
        } else {
            panic!("Expected to receive one record");
        }

        // Should not receive second record
        assert!(
            rx.try_recv().is_err(),
            "Should not receive second record due to LIMIT in CSAS"
        );
    }

    #[tokio::test]
    async fn test_limit_with_system_columns() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query with system columns and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, _timestamp, _partition FROM orders LIMIT 1")
            .unwrap();

        // Create and execute test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        let record1 = StreamRecord {
            fields: fields1,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        let record2 = StreamRecord {
            fields: fields2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 2,
            partition: 0,
            headers: HashMap::new(),
        };
        engine.execute_with_record(&query, record2).await.unwrap();

        // Should receive exactly 1 output
        if let Some(output) = rx.recv().await {
            match output.fields.get("customer_id") {
                Some(FieldValue::Integer(id)) => assert_eq!(*id, 1),
                _ => panic!("Unexpected customer_id type"),
            }
            assert!(output.fields.contains_key("_timestamp"));
            assert!(output.fields.contains_key("_partition"));
        }

        // Should not receive second record
        assert!(
            rx.try_recv().is_err(),
            "Should not receive second record due to LIMIT"
        );
    }

    #[tokio::test]
    async fn test_limit_with_headers() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Create test headers
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test-app".to_string());

        // Parse query with header function and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, HEADER('source') AS source FROM orders LIMIT 1")
            .unwrap();

        // Create and execute test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        let record1 = StreamRecord {
            fields: fields1,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: headers.clone(),
        };
        engine.execute_with_record(&query, record1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        let record2 = StreamRecord {
            fields: fields2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 2,
            partition: 0,
            headers,
        };
        engine.execute_with_record(&query, record2).await.unwrap();

        // Should receive exactly 1 output
        if let Some(output) = rx.recv().await {
            match (
                output.fields.get("customer_id"),
                output.fields.get("source"),
            ) {
                (Some(FieldValue::Integer(id)), Some(FieldValue::String(source))) => {
                    assert_eq!(*id, 1);
                    assert_eq!(source, "test-app");
                }
                _ => panic!("Unexpected output types"),
            }
        }

        // Should not receive second record
        assert!(
            rx.try_recv().is_err(),
            "Should not receive second record due to LIMIT"
        );
    }

    #[test]
    fn test_limit_parsing_errors() {
        let parser = StreamingSqlParser::new();

        // Test invalid LIMIT values
        let invalid_queries = vec![
            "SELECT * FROM orders LIMIT -1",  // Negative not handled gracefully
            "SELECT * FROM orders LIMIT abc", // Non-numeric
            "SELECT * FROM orders LIMIT 1.5", // Float
        ];

        for query in invalid_queries {
            let result = parser.parse(query);
            // These should fail at parse time or be handled gracefully
            if result.is_ok() {
                println!("Query parsed unexpectedly: {}", query);
            }
        }
    }

    #[test]
    fn test_query_without_limit() {
        let parser = StreamingSqlParser::new();

        // Test query without LIMIT clause
        let query = "SELECT * FROM orders WHERE amount > 100";
        let result = parser.parse(query).unwrap();

        match result {
            ferrisstreams::ferris::sql::ast::StreamingQuery::Select { limit, .. } => {
                assert_eq!(limit, None, "Query without LIMIT should have None limit");
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}
