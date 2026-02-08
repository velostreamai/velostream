use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(123));
        fields.insert("amount".to_string(), FieldValue::Float(299.99));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output contains system columns
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        assert!(output.fields.contains_key("_timestamp"));
        assert!(output.fields.contains_key("_offset"));
        assert!(output.fields.contains_key("_partition"));

        // Verify system column values are integers
        assert!(matches!(
            output.fields.get("_timestamp").unwrap(),
            FieldValue::Integer(_)
        ));
        assert!(matches!(
            output.fields.get("_offset").unwrap(),
            FieldValue::Integer(_)
        ));
        assert!(matches!(
            output.fields.get("_partition").unwrap(),
            FieldValue::Integer(_)
        ));
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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(456));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output has aliased names, not original system column names
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("event_time"));
        assert!(output.fields.contains_key("kafka_partition"));
        assert!(!output.fields.contains_key("_timestamp"));
        assert!(!output.fields.contains_key("_partition"));
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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(789));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Should produce output since _partition = 0 matches
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(101));
        fields.insert("amount".to_string(), FieldValue::Float(59.99));
        fields.insert(
            "status".to_string(),
            FieldValue::String("active".to_string()),
        );
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output has both regular and system columns
        let output = rx.try_recv().unwrap();
        assert_eq!(output.fields.len(), 4);
        assert!(output.fields.contains_key("customer_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(output.fields.contains_key("_timestamp"));
        assert!(output.fields.contains_key("_offset"));
        // status should not be included since not selected
        assert!(!output.fields.contains_key("status"));
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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(202));
        fields.insert("amount".to_string(), FieldValue::Float(149.99));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check that wildcard does NOT include system columns
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(!output.fields.contains_key("_timestamp"));
        assert!(!output.fields.contains_key("_offset"));
        assert!(!output.fields.contains_key("_partition"));
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
            let mut fields = HashMap::new();
            fields.insert("test_id".to_string(), FieldValue::Integer(i as i64));
            let record = StreamRecord {
                fields,
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: 0,
                partition: 0,
                event_time: None,
                headers: HashMap::new(),
                topic: None,
                key: None,
            };

            // Execute query
            let result = engine.execute_with_record(&query, &record).await;
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
            assert!(output.fields.contains_key(expected_key));
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
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(303));
        fields.insert("amount".to_string(), FieldValue::Float(75.50));
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute CREATE STREAM
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check that system columns are included in the output
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(output.fields.contains_key("_timestamp"));
        assert!(output.fields.contains_key("_partition"));
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

    // FR-081: Tests for SQL-based event-time assignment
    #[tokio::test]
    async fn test_event_time_assignment_from_integer_execution() {
        // FR-081: Test that _event_time assigned from integer milliseconds is converted to DateTime
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, timestamp_ms as _event_time FROM orders")
            .unwrap();

        // Create test record with timestamp_ms field
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        fields.insert(
            "timestamp_ms".to_string(),
            FieldValue::Integer(1609459200000), // 2021-01-01 00:00:00 UTC
        );

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("id"));
        // _event_time is a system column — stripped from output fields, stored in metadata only
        assert!(
            !output.fields.contains_key("_event_time"),
            "_event_time is a system column and should be stripped from output fields"
        );

        // Verify event_time metadata is set (not None) - proves conversion happened
        assert!(
            output.event_time.is_some(),
            "event_time should be set from _event_time assignment"
        );

        // Verify it's a valid DateTime (should be around 2021-01-01)
        let event_time = output.event_time.unwrap();
        assert_eq!(event_time.timestamp(), 1609459200);
    }

    #[tokio::test]
    async fn test_event_time_default_now_when_not_assigned() {
        // FR-081: Test that event_time defaults to NOW() when _event_time is not assigned
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        // Query WITHOUT _event_time assignment
        let query = parser.parse("SELECT id, amount FROM orders").unwrap();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(42));
        fields.insert("amount".to_string(), FieldValue::Float(99.99));

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;

        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        // When _event_time isn't assigned and input has no event_time,
        // output preserves None (correct event-time semantics - don't inject processing-time)
        assert!(
            output.event_time.is_none(),
            "event_time should be None when not assigned and input has no event_time"
        );
    }

    #[tokio::test]
    async fn test_event_time_preserved_from_input_when_not_assigned() {
        // FR-081: Test that event_time is preserved from input when not explicitly assigned
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT id, name FROM orders").unwrap();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(42));
        fields.insert("name".to_string(), FieldValue::String("Test".to_string()));

        // Input record HAS event_time set
        let input_event_time = chrono::DateTime::from_timestamp_millis(1640998800000).unwrap();

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: Some(input_event_time),
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;

        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        // Event time should be preserved from input
        assert!(
            output.event_time.is_some(),
            "event_time should be preserved from input"
        );
        assert_eq!(
            output.event_time.unwrap().timestamp_millis(),
            input_event_time.timestamp_millis(),
            "event_time should match input record's event_time"
        );
    }

    #[tokio::test]
    async fn test_event_time_fallback_for_non_convertible_type() {
        // FR-081: Test that event_time falls back to input when _event_time value is non-convertible
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        // Assign a STRING value to _event_time (non-convertible)
        let query = parser
            .parse("SELECT id, description as _event_time FROM orders")
            .unwrap();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(99));
        fields.insert(
            "description".to_string(),
            FieldValue::String("invalid timestamp".to_string()),
        );

        // Input record has NO event_time
        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;

        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        // Non-convertible type falls back to input's event_time (None)
        assert!(
            output.event_time.is_none(),
            "event_time should be None when alias is non-convertible and input has no event_time"
        );
    }

    #[tokio::test]
    async fn test_event_time_assignment_with_timestamp_type() {
        // FR-081: Test that _event_time assigned from NaiveDateTime Timestamp is converted correctly
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, event_ts as _event_time FROM events")
            .unwrap();

        // Create NaiveDateTime for 2021-06-15 12:30:45
        let naive_dt = chrono::NaiveDateTime::from_timestamp_opt(1623769845, 0).unwrap();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(5));
        fields.insert("event_ts".to_string(), FieldValue::Timestamp(naive_dt));

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        assert!(output.event_time.is_some());

        // Verify the timestamp was preserved
        let event_time = output.event_time.unwrap();
        assert_eq!(event_time.timestamp(), 1623769845);
    }

    #[test]
    fn test_event_time_assignment_from_integer_parsing() {
        let parser = StreamingSqlParser::new();
        let query = "SELECT id, timestamp_ms as _event_time FROM orders";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse event-time assignment query"
        );

        // Verify the query parses correctly with the alias
        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                // Should have two fields: id and timestamp_ms as _event_time
                assert_eq!(fields.len(), 2);

                // Check second field is aliased as _event_time
                match &fields[1] {
                    SelectField::Expression {
                        alias: Some(alias), ..
                    } => {
                        assert_eq!(alias.to_uppercase(), "_EVENT_TIME");
                    }
                    _ => panic!("Expected aliased expression for _event_time"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_event_time_assignment_basic_parsing() {
        let parser = StreamingSqlParser::new();
        // Test basic parsing without complex window syntax
        let query = "SELECT price, timestamp_ms as _event_time FROM trades";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse basic event-time assignment"
        );

        // Verify query structure
        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 2);
                // Verify second field is aliased as _event_time
                match &fields[1] {
                    SelectField::Expression {
                        alias: Some(alias), ..
                    } => {
                        assert_eq!(alias.to_uppercase(), "_EVENT_TIME");
                    }
                    _ => panic!("Expected aliased expression for _event_time"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_event_time_assignment_case_insensitive_parsing() {
        let parser = StreamingSqlParser::new();

        // Test case-insensitive _event_time alias
        let queries = vec![
            "SELECT id, ts as _event_time FROM stream",
            "SELECT id, ts as _EVENT_TIME FROM stream", // uppercase
            "SELECT id, ts as _Event_Time FROM stream", // mixed case
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(
                result.is_ok(),
                "Failed to parse query with _event_time alias: {}",
                query
            );

            match result.unwrap() {
                StreamingQuery::Select { fields, .. } => {
                    assert_eq!(fields.len(), 2);

                    // Verify second field is the aliased column
                    match &fields[1] {
                        SelectField::Expression {
                            alias: Some(alias), ..
                        } => {
                            assert_eq!(alias.to_uppercase(), "_EVENT_TIME");
                        }
                        _ => panic!("Expected aliased expression for {}", query),
                    }
                }
                _ => panic!("Expected Select query"),
            }
        }
    }

    // Test for direct _event_time column selection (not aliased)
    // This tests the fix for selecting m._event_time in joins where
    // the field is selected directly without using AS alias
    #[tokio::test]
    async fn test_event_time_direct_column_selection() {
        // FR-081 extension: Test that selecting _event_time directly (not via alias)
        // still sets the output record's event_time metadata
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        // Select _event_time directly (not aliased) - simulates: SELECT m._event_time FROM stream m
        let query = parser.parse("SELECT id, _event_time FROM orders").unwrap();

        // Create test record with event_time metadata already set
        // (simulates upstream data source with 'event.time.field' config)
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));

        // Set event_time metadata (not as a field - _event_time is a system column
        // that reads from record.event_time metadata)
        let input_event_time = chrono::DateTime::from_timestamp_millis(1609459200000).unwrap(); // 2021-01-01 00:00:00 UTC

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: Some(input_event_time), // Input HAS event_time metadata set
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("id"));
        // _event_time is a system column — stripped from output fields, stored in metadata only
        assert!(
            !output.fields.contains_key("_event_time"),
            "_event_time is a system column and should be stripped from output fields"
        );

        // Verify event_time metadata is propagated from input and preserved in output
        assert!(
            output.event_time.is_some(),
            "event_time should be set from direct _event_time column selection"
        );

        // Verify it's a valid DateTime (should be 2021-01-01)
        let event_time = output.event_time.unwrap();
        assert_eq!(event_time.timestamp(), 1609459200);
    }

    #[tokio::test]
    async fn test_event_time_qualified_column_selection() {
        // FR-081 extension: Test that selecting qualified _event_time (e.g., m._event_time)
        // still sets the output record's event_time metadata
        // This simulates JOIN scenarios like: SELECT m._event_time FROM market_data m
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        // Select with qualified column name (simulates table alias prefix)
        let query = parser
            .parse("SELECT id, m._event_time FROM orders m")
            .unwrap();

        // Create test record with event_time metadata set
        // (simulates upstream data source with 'event.time.field' config)
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(2));

        // Set event_time metadata (not as a field - _event_time is a system column
        // that reads from record.event_time metadata)
        let input_event_time = chrono::DateTime::from_timestamp_millis(1640998800000).unwrap(); // 2022-01-01 00:00:00 UTC

        let record = StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: Some(input_event_time), // Input HAS event_time metadata set
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Execute query
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        // Check output
        let output = rx.try_recv().unwrap();

        // Verify event_time metadata is propagated from input and preserved in output
        assert!(
            output.event_time.is_some(),
            "event_time should be set from qualified m._event_time column selection"
        );

        // Verify it's the correct DateTime (2022-01-01)
        let event_time = output.event_time.unwrap();
        assert_eq!(event_time.timestamp(), 1640998800);
    }
}
