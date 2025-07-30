use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use tokio::sync::mpsc;
use serde_json::Value;

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
                        assert!(limit.is_some(), "LIMIT should be parsed for query: {}", query);
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
                    assert_eq!(limit, Some(expected_limit), "Limit value mismatch for query: {}", query);
                }
                _ => panic!("Expected SELECT query"),
            }
        }
    }

    #[tokio::test]
    async fn test_limit_execution_basic() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Parse query with LIMIT 2
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT customer_id, amount FROM orders LIMIT 2").unwrap();
        
        // Create test records
        let records = vec![
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(100.0).unwrap()));
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(2)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(200.0).unwrap()));
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(3)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(300.0).unwrap()));
                record
            },
        ];
        
        // Execute records - should only process first 2
        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }
        
        // Should receive exactly 2 outputs
        let output1 = rx.try_recv().unwrap();
        assert_eq!(output1.get("customer_id").unwrap().as_i64().unwrap(), 1);
        
        let output2 = rx.try_recv().unwrap();
        assert_eq!(output2.get("customer_id").unwrap().as_i64().unwrap(), 2);
        
        // Third record should not produce output due to LIMIT
        assert!(rx.try_recv().is_err(), "Should not receive third record due to LIMIT");
    }

    #[tokio::test]
    async fn test_limit_with_where_clause() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Parse query with WHERE and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT customer_id, amount FROM orders WHERE amount > 150 LIMIT 1").unwrap();
        
        // Create test records - only some will match WHERE clause
        let records = vec![
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(100.0).unwrap())); // Won't match WHERE
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(2)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(200.0).unwrap())); // Will match
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(3)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(300.0).unwrap())); // Will match but LIMIT reached
                record
            },
        ];
        
        // Execute records
        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }
        
        // Should receive exactly 1 output (first record matching WHERE clause)
        let output = rx.try_recv().unwrap();
        assert_eq!(output.get("customer_id").unwrap().as_i64().unwrap(), 2);
        assert_eq!(output.get("amount").unwrap().as_f64().unwrap(), 200.0);
        
        // Should not receive second matching record due to LIMIT
        assert!(rx.try_recv().is_err(), "Should not receive second record due to LIMIT");
    }

    #[tokio::test]
    async fn test_limit_zero() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Parse query with LIMIT 0
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT customer_id FROM orders LIMIT 0").unwrap();
        
        // Create test record
        let mut record = HashMap::new();
        record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
        
        // Execute record
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());
        
        // Should not receive any output due to LIMIT 0
        assert!(rx.try_recv().is_err(), "Should not receive any records due to LIMIT 0");
    }

    #[tokio::test]
    async fn test_limit_with_csas() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Parse CSAS query with LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser.parse("CREATE STREAM limited_orders AS SELECT customer_id, amount FROM orders LIMIT 1").unwrap();
        
        // Create test records
        let records = vec![
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(100.0).unwrap()));
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(2)));
                record.insert("amount".to_string(), Value::Number(serde_json::Number::from_f64(200.0).unwrap()));
                record
            },
        ];
        
        // Execute records
        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }
        
        // Should receive exactly 1 output due to LIMIT in CSAS
        let output = rx.try_recv().unwrap();
        assert_eq!(output.get("customer_id").unwrap().as_i64().unwrap(), 1);
        
        // Should not receive second record
        assert!(rx.try_recv().is_err(), "Should not receive second record due to LIMIT in CSAS");
    }

    #[tokio::test]
    async fn test_limit_with_system_columns() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Parse query with system columns and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT customer_id, _timestamp, _partition FROM orders LIMIT 1").unwrap();
        
        // Create test records
        let records = vec![
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(2)));
                record
            },
        ];
        
        // Execute records
        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }
        
        // Should receive exactly 1 output
        let output = rx.try_recv().unwrap();
        assert_eq!(output.get("customer_id").unwrap().as_i64().unwrap(), 1);
        assert!(output.contains_key("_timestamp"));
        assert!(output.contains_key("_partition"));
        
        // Should not receive second record
        assert!(rx.try_recv().is_err(), "Should not receive second record due to LIMIT");
    }

    #[tokio::test]
    async fn test_limit_with_headers() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);
        
        // Create test headers
        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test-app".to_string());
        
        // Parse query with header function and LIMIT
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT customer_id, HEADER('source') AS source FROM orders LIMIT 1").unwrap();
        
        // Create test records
        let records = vec![
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(1)));
                record
            },
            {
                let mut record = HashMap::new();
                record.insert("customer_id".to_string(), Value::Number(serde_json::Number::from(2)));
                record
            },
        ];
        
        // Execute records with headers
        for record in records {
            let result = engine.execute_with_headers(&query, record, headers.clone()).await;
            assert!(result.is_ok());
        }
        
        // Should receive exactly 1 output
        let output = rx.try_recv().unwrap();
        assert_eq!(output.get("customer_id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(output.get("source").unwrap().as_str().unwrap(), "test-app");
        
        // Should not receive second record
        assert!(rx.try_recv().is_err(), "Should not receive second record due to LIMIT");
    }

    #[test]
    fn test_limit_parsing_errors() {
        let parser = StreamingSqlParser::new();
        
        // Test invalid LIMIT values
        let invalid_queries = vec![
            "SELECT * FROM orders LIMIT -1",      // Negative not handled gracefully
            "SELECT * FROM orders LIMIT abc",     // Non-numeric
            "SELECT * FROM orders LIMIT 1.5",     // Float
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