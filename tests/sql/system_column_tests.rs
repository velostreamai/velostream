use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_column_parsing() {
        let parser = StreamingSqlParser::new();

        // Test parsing queries with system columns
        let queries = vec![
            "SELECT _timestamp FROM orders",
            "SELECT _offset FROM orders",
            "SELECT _partition FROM orders",
            "SELECT customer_id, _timestamp, _offset, _partition FROM orders",
            "SELECT _TIMESTAMP, _OFFSET, _PARTITION FROM orders", // Test case insensitive
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(result.is_ok(), "Failed to parse query: {}", query);
        }
    }

    #[test]
    fn test_system_column_aliasing() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "SELECT _timestamp AS ts, _offset AS msg_offset, _partition AS part FROM orders",
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 3);

                // Check that system columns are parsed as regular expressions with aliases
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        match expr {
                            Expr::Column(name) => assert_eq!(name, "_timestamp"),
                            _ => panic!("Expected column expression"),
                        }
                        assert_eq!(alias.as_ref().unwrap(), "ts");
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[tokio::test]
    async fn test_system_column_execution() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse query with system columns
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, _timestamp, _offset, _partition FROM orders")
            .unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(123)),
        );
        record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(299.99).unwrap()),
        );

        // Execute query
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check output contains system columns
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("customer_id"));
        assert!(output.contains_key("_timestamp"));
        assert!(output.contains_key("_offset"));
        assert!(output.contains_key("_partition"));

        // Verify system column values are integers
        assert!(output.get("_timestamp").unwrap().is_number());
        assert!(output.get("_offset").unwrap().is_number());
        assert!(output.get("_partition").unwrap().is_number());
    }

    #[tokio::test]
    async fn test_system_column_with_aliases() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse query with aliased system columns
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT _timestamp AS event_time, _partition AS kafka_partition FROM orders")
            .unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(456)),
        );

        // Execute query
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check output has aliased names, not original system column names
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("event_time"));
        assert!(output.contains_key("kafka_partition"));
        assert!(!output.contains_key("_timestamp"));
        assert!(!output.contains_key("_partition"));
    }

    #[tokio::test]
    async fn test_system_column_in_where_clause() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse query with system column in WHERE clause
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id FROM orders WHERE _partition = 0")
            .unwrap();

        // Create test record (partition will be 0 by default in test)
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(789)),
        );

        // Execute query
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Should produce output since _partition = 0 matches
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("customer_id"));
    }

    #[tokio::test]
    async fn test_mixed_regular_and_system_columns() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse query mixing regular and system columns
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT customer_id, amount, _timestamp, _offset FROM orders")
            .unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(101)),
        );
        record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(59.99).unwrap()),
        );
        record.insert("status".to_string(), Value::String("active".to_string()));

        // Execute query
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check output has both regular and system columns
        let output = rx.try_recv().unwrap();
        assert_eq!(output.len(), 4);
        assert!(output.contains_key("customer_id"));
        assert!(output.contains_key("amount"));
        assert!(output.contains_key("_timestamp"));
        assert!(output.contains_key("_offset"));
        // status should not be included since not selected
        assert!(!output.contains_key("status"));
    }

    #[tokio::test]
    async fn test_wildcard_does_not_include_system_columns() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse wildcard query
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT * FROM orders").unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(202)),
        );
        record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(149.99).unwrap()),
        );

        // Execute query
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check that wildcard does NOT include system columns
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("customer_id"));
        assert!(output.contains_key("amount"));
        assert!(!output.contains_key("_timestamp"));
        assert!(!output.contains_key("_offset"));
        assert!(!output.contains_key("_partition"));
    }

    #[tokio::test]
    async fn test_system_columns_case_insensitive() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Test both lowercase and uppercase system column names
        let queries = vec![
            "SELECT _timestamp FROM orders",
            "SELECT _TIMESTAMP FROM orders",
            "SELECT _Timestamp FROM orders", // Mixed case should also work due to case-insensitive matching
        ];

        for (i, query_str) in queries.iter().enumerate() {
            let parser = StreamingSqlParser::new();
            let query = parser.parse(query_str).unwrap();

            // Create test record
            let mut record = HashMap::new();
            record.insert(
                "test_id".to_string(),
                Value::Number(serde_json::Number::from(i as i64)),
            );

            // Execute query
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok(), "Failed to execute query: {}", query_str);

            // Should produce timestamp output
            let output = rx.try_recv().unwrap();
            // The output key will match the case used in the query
            let expected_key = if query_str.contains("_TIMESTAMP") {
                "_TIMESTAMP"
            } else if query_str.contains("_Timestamp") {
                "_Timestamp"
            } else {
                "_timestamp"
            };
            assert!(output.contains_key(expected_key));
        }
    }

    #[tokio::test]
    async fn test_csas_with_system_columns() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse CSAS query with system columns
        let parser = StreamingSqlParser::new();
        let query = parser.parse("CREATE STREAM enriched_orders AS SELECT customer_id, amount, _timestamp, _partition FROM orders").unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(303)),
        );
        record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(75.50).unwrap()),
        );

        // Execute CREATE STREAM
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check that system columns are included in the output
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("customer_id"));
        assert!(output.contains_key("amount"));
        assert!(output.contains_key("_timestamp"));
        assert!(output.contains_key("_partition"));
    }

    #[test]
    fn test_system_column_in_expression() {
        let parser = StreamingSqlParser::new();

        // Test system columns in expressions (should parse successfully)
        let queries = vec![
            "SELECT _timestamp + 1000 AS future_time FROM orders",
            "SELECT _partition * 2 AS double_partition FROM orders",
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(
                result.is_ok(),
                "Failed to parse query with system column in expression: {}",
                query
            );
        }
    }

    #[test]
    fn test_system_column_reserved_names() {
        // Ensure our system column names are properly reserved and recognized
        let parser = StreamingSqlParser::new();

        // These should parse as system columns, not regular identifiers
        let system_columns = vec![
            "_timestamp",
            "_offset",
            "_partition",
            "_TIMESTAMP",
            "_OFFSET",
            "_PARTITION",
        ];

        for col in system_columns {
            let query = format!("SELECT {} FROM orders", col);
            let result = parser.parse(&query);
            assert!(
                result.is_ok(),
                "System column {} should parse correctly",
                col
            );

            // Verify it's parsed as a column reference
            match result.unwrap() {
                StreamingQuery::Select { fields, .. } => match &fields[0] {
                    SelectField::Expression { expr, .. } => match expr {
                        Expr::Column(name) => assert_eq!(name, col),
                        _ => panic!("System column should be parsed as column expression"),
                    },
                    _ => panic!("Expected expression field for {}", col),
                },
                _ => panic!("Expected Select query"),
            }
        }
    }
}
