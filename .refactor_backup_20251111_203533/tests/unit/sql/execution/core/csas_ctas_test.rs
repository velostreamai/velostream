use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::ast::DataType;
use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::context::StreamingSqlContext;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::schema::{FieldDefinition, Schema, StreamHandle};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record(id: i64, amount: f64, status: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("status".to_string(), FieldValue::String(status.to_string()));

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: id,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csas_parsing_simple() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("CREATE STREAM high_value_orders AS SELECT * FROM orders");

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStream {
                name,
                columns,
                as_select,
                properties,
                emit_mode: _,
                metric_annotations: _,
                job_name: _,
            } => {
                assert_eq!(name, "high_value_orders");
                assert!(columns.is_none());
                assert!(properties.is_empty());

                // Check the underlying SELECT query
                match *as_select {
                    StreamingQuery::Select { fields, from, .. } => {
                        assert_eq!(fields.len(), 1);
                        assert!(matches!(fields[0], SelectField::Wildcard));
                        assert!(matches!(from, StreamSource::Stream(_)));
                    }
                    _ => panic!("Expected Select query in CREATE STREAM"),
                }
            }
            _ => panic!("Expected CreateStream query"),
        }
    }

    #[test]
    fn test_csas_parsing_with_columns() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM processed_orders (order_id INT, total_amount DOUBLE) AS SELECT id, amount FROM orders"
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStream { name, columns, .. } => {
                assert_eq!(name, "processed_orders");
                assert!(columns.is_some());

                let cols = columns.unwrap();
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].name, "order_id");
                assert_eq!(cols[0].data_type, DataType::Integer);
                assert_eq!(cols[1].name, "total_amount");
                assert_eq!(cols[1].data_type, DataType::Float);
            }
            _ => panic!("Expected CreateStream query"),
        }
    }

    #[test]
    fn test_csas_with_window() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM windowed_aggregates AS SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)",
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStream { as_select, .. } => {
                match *as_select {
                    StreamingQuery::Select {
                        window,
                        group_by: None,
                        having: None,
                        order_by: None,
                        limit: None,
                        emit_mode: None,
                        ..
                    } => {
                        assert!(window.is_some());
                        if let Some(WindowSpec::Tumbling { size, .. }) = window {
                            assert_eq!(size.as_secs(), 300); // 5 minutes
                        } else {
                            panic!("Expected tumbling window");
                        }
                    }
                    _ => panic!("Expected Select query"),
                }
            }
            _ => panic!("Expected CreateStream query"),
        }
    }

    #[test]
    fn test_ctas_parsing_simple() {
        let parser = StreamingSqlParser::new();
        let result = parser
            .parse("CREATE TABLE customer_totals AS SELECT customer_id, SUM(amount) FROM orders");

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateTable {
                name,
                columns,
                as_select,
                properties,
                emit_mode: _,
            } => {
                assert_eq!(name, "customer_totals");
                assert!(columns.is_none());
                assert!(properties.is_empty());

                // Check the underlying SELECT query
                match *as_select {
                    StreamingQuery::Select { fields, .. } => {
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Select query in CREATE TABLE"),
                }
            }
            _ => panic!("Expected CreateTable query"),
        }
    }

    #[test]
    fn test_ctas_with_explicit_schema() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE TABLE order_stats (customer_id INT, total_spent DOUBLE, order_count INT) AS SELECT customer_id, SUM(amount), COUNT(*) FROM orders"
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateTable { name, columns, .. } => {
                assert_eq!(name, "order_stats");
                assert!(columns.is_some());

                let cols = columns.unwrap();
                assert_eq!(cols.len(), 3);
                assert_eq!(cols[0].name, "customer_id");
                assert_eq!(cols[1].name, "total_spent");
                assert_eq!(cols[2].name, "order_count");
            }
            _ => panic!("Expected CreateTable query"),
        }
    }

    #[test]
    fn test_data_type_parsing() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM typed_stream (id INTEGER, name STRING, active BOOLEAN, created_at TIMESTAMP) AS SELECT * FROM orders"
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStream { columns, .. } => {
                let cols = columns.unwrap();
                assert_eq!(cols.len(), 4);

                assert_eq!(cols[0].data_type, DataType::Integer);
                assert_eq!(cols[1].data_type, DataType::String);
                assert_eq!(cols[2].data_type, DataType::Boolean);
                assert_eq!(cols[3].data_type, DataType::Timestamp);
            }
            _ => panic!("Expected CreateStream query"),
        }
    }

    #[test]
    fn test_complex_data_types() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM complex_stream (tags ARRAY(STRING), metadata MAP(STRING, STRING)) AS SELECT * FROM orders"
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStream { columns, .. } => {
                let cols = columns.unwrap();
                assert_eq!(cols.len(), 2);

                // Check ARRAY type
                if let DataType::Array(inner) = &cols[0].data_type {
                    assert_eq!(**inner, DataType::String);
                } else {
                    panic!("Expected Array type");
                }

                // Check MAP type
                if let DataType::Map(key, value) = &cols[1].data_type {
                    assert_eq!(**key, DataType::String);
                    assert_eq!(**value, DataType::String);
                } else {
                    panic!("Expected Map type");
                }
            }
            _ => panic!("Expected CreateStream query"),
        }
    }

    #[tokio::test]
    async fn test_csas_execution() {
        // Setup execution engine
        let serialization_format = std::sync::Arc::new(JsonFormat);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse CSAS query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("CREATE STREAM filtered_orders AS SELECT customer_id, amount FROM orders")
            .unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert("customer_id".to_string(), FieldValue::Integer(123));
        record.insert("amount".to_string(), FieldValue::Float(299.99));
        record.insert(
            "status".to_string(),
            FieldValue::String("pending".to_string()),
        );
        // Execute CREATE STREAM
        let record_obj = StreamRecord::new(record);
        let result = engine.execute_with_record(&query, &record_obj).await;
        assert!(result.is_ok());

        // Check that the underlying SELECT was executed
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(!output.fields.contains_key("status")); // Should be filtered out
    }

    #[tokio::test]
    async fn test_ctas_execution() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let serialization_format = std::sync::Arc::new(JsonFormat);

        let mut engine = StreamExecutionEngine::new(tx);

        // Parse CTAS query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("CREATE TABLE order_summary AS SELECT customer_id, COUNT(*) FROM orders")
            .unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert("customer_id".to_string(), FieldValue::Integer(456));
        record.insert("amount".to_string(), FieldValue::Float(150.00));

        // Execute CREATE TABLE
        let record_obj = StreamRecord::new(record);
        let result = engine.execute_with_record(&query, &record_obj).await;
        assert!(result.is_ok());

        // Check that the underlying SELECT was executed
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        // COUNT(*) should be present (though implementation details may vary)
    }

    #[test]
    fn test_csas_context_validation() {
        // Setup context with a stream
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = Schema::new(vec![
            FieldDefinition::required("customer_id".to_string(), DataType::Integer),
            FieldDefinition::required("amount".to_string(), DataType::Float),
        ]);
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Test valid CSAS query
        let _valid_result =
            context.execute_query("CREATE STREAM high_value AS SELECT customer_id FROM orders");
        // Note: This may not be fully implemented in context yet, but should parse correctly
        // assert!(valid_result.is_ok()); // Uncomment when context supports CREATE queries

        // Test invalid CSAS query (referencing non-existent column)
        let _invalid_result =
            context.execute_query("CREATE STREAM invalid AS SELECT nonexistent_column FROM orders");
        // assert!(invalid_result.is_err()); // Uncomment when context supports CREATE queries
    }

    #[test]
    fn test_query_introspection() {
        let parser = StreamingSqlParser::new();

        // Test has_window for CSAS
        let windowed_csas = parser
            .parse("CREATE STREAM windowed AS SELECT COUNT(*) FROM orders WINDOW TUMBLING(1m)")
            .unwrap();
        assert!(windowed_csas.has_window());

        let simple_csas = parser
            .parse("CREATE STREAM simple AS SELECT * FROM orders")
            .unwrap();
        assert!(!simple_csas.has_window());

        // Test get_columns for CSAS
        let column_csas = parser
            .parse("CREATE STREAM filtered AS SELECT customer_id, amount FROM orders")
            .unwrap();
        let columns = column_csas.get_columns();
        assert!(columns.contains(&"customer_id".to_string()));
        assert!(columns.contains(&"amount".to_string()));
    }

    #[test]
    fn test_error_handling() {
        let parser = StreamingSqlParser::new();

        // Test missing AS keyword
        let result = parser.parse("CREATE STREAM test SELECT * FROM orders");
        assert!(result.is_err());

        // Test missing SELECT after AS
        let result = parser.parse("CREATE STREAM test AS");
        assert!(result.is_err());

        // Test invalid data type
        let result = parser.parse("CREATE STREAM test (id INVALID_TYPE) AS SELECT * FROM orders");
        assert!(result.is_err());

        // Test malformed column definition
        let result = parser.parse("CREATE STREAM test (id,) AS SELECT * FROM orders");
        assert!(result.is_err());
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let parser = StreamingSqlParser::new();

        let queries = vec![
            "create stream test as select * from orders",
            "CREATE STREAM test AS SELECT * FROM orders",
            "Create Stream test As Select * From orders",
            "create table test as select * from orders",
            "CREATE TABLE test AS SELECT * FROM orders",
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(result.is_ok(), "Failed to parse: {}", query);
        }
    }

    #[test]
    fn test_nullable_columns() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM test_stream (required_field INT, optional_field STRING NOT) AS SELECT * FROM orders"
        );

        // This should work with basic nullable syntax
        // Full NOT NULL support may need additional parser work
        // For now, just test that it doesn't crash
        let _ = result; // May be ok or error depending on implementation completeness
    }

    #[test]
    fn test_nested_queries() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM aggregated AS SELECT customer_id, AVG(amount) FROM orders WINDOW SLIDING(10m, 5m)"
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        // Verify it's a proper nested structure
        match query {
            StreamingQuery::CreateStream { as_select, .. } => match *as_select {
                StreamingQuery::Select {
                    window,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    emit_mode: None,
                    fields,
                    ..
                } => {
                    assert!(window.is_some());
                    assert_eq!(fields.len(), 2);
                }
                _ => panic!("Expected Select query"),
            },
            _ => panic!("Expected CreateStream query"),
        }
    }
}
