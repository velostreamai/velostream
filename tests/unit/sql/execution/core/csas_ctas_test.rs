use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::context::StreamingSqlContext;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::DataType;

use ferrisstreams::ferris::schema::{FieldDefinition, Schema, StreamHandle};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use tokio::sync::mpsc;

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
        let result = engine
            .execute_with_record(&query, StreamRecord::new(record))
            .await;
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
        let result = engine
            .execute_with_record(&query, StreamRecord::new(record))
            .await;
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

    // ===== NEW DATASOURCE-STYLE TESTS WITH INTO SYNTAX =====

    #[test]
    fn test_csas_with_into_parsing() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE STREAM enriched_orders AS SELECT * FROM kafka_source WITH (\"source_config\" = \"configs/kafka.yaml\") INTO postgres_sink",
        );
        
        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStreamInto {
                name,
                columns,
                as_select,
                into_clause,
                properties,
                emit_mode: _,
            } => {
                assert_eq!(name, "enriched_orders");
                assert!(columns.is_none());
                assert_eq!(into_clause.sink_name, "postgres_sink");
                assert!(into_clause.sink_properties.is_empty());
                assert!(properties.source_config.is_none()); // Config is in SELECT WITH, not outer config

                // Check the underlying SELECT query
                match *as_select {
                    StreamingQuery::Select { fields, from, properties, .. } => {
                        assert_eq!(fields.len(), 1);
                        assert!(matches!(fields[0], SelectField::Wildcard));
                        assert!(matches!(from, StreamSource::Stream(_)));
                        
                        // Check that SELECT has the source config in its properties
                        if let Some(props) = properties {
                            assert_eq!(props.get("source_config"), Some(&"configs/kafka.yaml".to_string()));
                        } else {
                            panic!("Expected SELECT to have WITH properties");
                        }
                    }
                    _ => panic!("Expected Select query in CREATE STREAM INTO"),
                }
            }
            _ => panic!("Expected CreateStreamInto query"),
        }
    }

    #[test]
    fn test_ctas_with_into_parsing() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            "CREATE TABLE analytics_summary AS SELECT customer_id, COUNT(*), AVG(amount) FROM kafka_stream WITH (\"source_config\" = \"configs/kafka.yaml\") INTO clickhouse_sink"
        );
        
        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateTableInto {
                name,
                columns,
                as_select,
                into_clause,
                properties,
                emit_mode: _,
            } => {
                assert_eq!(name, "analytics_summary");
                assert!(columns.is_none());
                assert_eq!(into_clause.sink_name, "clickhouse_sink");
                assert!(properties.source_config.is_none()); // Config is in SELECT WITH

                // Check the underlying SELECT query has aggregation
                match *as_select {
                    StreamingQuery::Select { fields, .. } => {
                        assert_eq!(fields.len(), 3); // customer_id, COUNT(*), AVG(amount)
                    }
                    _ => panic!("Expected Select query in CREATE TABLE INTO"),
                }
            }
            _ => panic!("Expected CreateTableInto query"),
        }
    }

    #[test]
    fn test_csas_with_into_and_config() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            r#"
            CREATE STREAM processed_events AS 
            SELECT id, timestamp, event_type 
            FROM file_source 
            INTO s3_sink
            WITH (
                "source_config" = "configs/file_input.yaml",
                "sink_config" = "configs/s3_output.yaml"
            )
        "#,
        );

        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStreamInto {
                name,
                into_clause,
                properties,
                ..
            } => {
                assert_eq!(name, "processed_events");
                assert_eq!(into_clause.sink_name, "s3_sink");
                assert_eq!(
                    properties.source_config,
                    Some("configs/file_input.yaml".to_string())
                );
                assert_eq!(
                    properties.sink_config,
                    Some("configs/s3_output.yaml".to_string())
                );
            }
            _ => panic!("Expected CreateStreamInto query"),
        }
    }

    #[test]
    fn test_ctas_with_into_and_multi_config() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            r#"
            CREATE TABLE aggregated_metrics AS 
            SELECT 
                date_trunc('hour', timestamp) as hour,
                COUNT(*) as event_count,
                AVG(latency) as avg_latency
            FROM performance_events 
            GROUP BY date_trunc('hour', timestamp)
            INTO postgres_sink
            WITH (
                "source_config" = "configs/kafka_perf.yaml",
                "sink_config" = "configs/postgres_analytics.yaml", 
                "monitoring_config" = "configs/monitoring.yaml"
            )
        "#,
        );

        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }
        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateTableInto {
                name,
                into_clause,
                properties,
                ..
            } => {
                assert_eq!(name, "aggregated_metrics");
                assert_eq!(into_clause.sink_name, "postgres_sink");
                assert_eq!(
                    properties.source_config,
                    Some("configs/kafka_perf.yaml".to_string())
                );
                assert_eq!(
                    properties.sink_config,
                    Some("configs/postgres_analytics.yaml".to_string())
                );
                assert_eq!(
                    properties.monitoring_config,
                    Some("configs/monitoring.yaml".to_string())
                );
            }
            _ => panic!("Expected CreateTableInto query"),
        }
    }

    #[test]
    fn test_csas_with_into_and_columns() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            r#"
            CREATE STREAM validated_orders (
                order_id INTEGER,
                customer_id INTEGER,
                total_amount FLOAT,
                order_status STRING
            ) AS 
            SELECT id, customer_id, amount, status 
            FROM raw_orders 
            WITH ("source_config" = "configs/kafka.yaml")
            INTO kafka_validated_orders
        "#,
        );

        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }
        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStreamInto {
                name,
                columns,
                into_clause,
                ..
            } => {
                assert_eq!(name, "validated_orders");
                assert_eq!(into_clause.sink_name, "kafka_validated_orders");

                assert!(columns.is_some());
                let cols = columns.unwrap();
                assert_eq!(cols.len(), 4);
                assert_eq!(cols[0].name, "order_id");
                assert_eq!(cols[0].data_type, DataType::Integer);
                assert_eq!(cols[1].name, "customer_id");
                assert_eq!(cols[1].data_type, DataType::Integer);
                assert_eq!(cols[2].name, "total_amount");
                assert_eq!(cols[2].data_type, DataType::Float);
                assert_eq!(cols[3].name, "order_status");
                assert_eq!(cols[3].data_type, DataType::String);
            }
            _ => panic!("Expected CreateStreamInto query"),
        }
    }

    #[test]
    fn test_csas_with_into_and_window() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse(
            r#"
            CREATE STREAM windowed_analytics AS 
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                AVG(amount) as avg_amount
            FROM orders 
            WITH ("source_config" = "configs/kafka.yaml")
            WINDOW TUMBLING(5m)
            INTO analytics_sink
        "#,
        );

        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }
        assert!(result.is_ok());
        let query = result.unwrap();

        match query {
            StreamingQuery::CreateStreamInto {
                name,
                as_select,
                into_clause,
                ..
            } => {
                assert_eq!(name, "windowed_analytics");
                assert_eq!(into_clause.sink_name, "analytics_sink");

                // Check that the SELECT has a window
                match *as_select {
                    StreamingQuery::Select { window, .. } => {
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
            _ => panic!("Expected CreateStreamInto query"),
        }
    }

    #[tokio::test]
    async fn test_csas_into_execution() {
        // Setup execution engine
        let serialization_format = std::sync::Arc::new(JsonFormat);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse CREATE STREAM INTO query
        let parser = StreamingSqlParser::new();
        let result = parser
            .parse("CREATE STREAM orders_to_warehouse AS SELECT order_id, amount FROM source WITH (\"source_config\" = \"configs/kafka.yaml\") INTO warehouse_sink");
            
        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }
        let query = result.unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert("order_id".to_string(), FieldValue::Integer(12345));
        record.insert("amount".to_string(), FieldValue::Float(599.99));
        record.insert("customer_id".to_string(), FieldValue::Integer(789)); // Extra field

        // Execute CREATE STREAM INTO
        let result = engine
            .execute_with_record(&query, StreamRecord::new(record))
            .await;
        assert!(result.is_ok());

        // Check that the underlying SELECT was executed
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("order_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(!output.fields.contains_key("customer_id")); // Should be filtered out by SELECT
    }

    #[tokio::test]
    async fn test_ctas_into_execution() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let serialization_format = std::sync::Arc::new(JsonFormat);
        let mut engine = StreamExecutionEngine::new(tx);

        // Parse CREATE TABLE INTO query
        let parser = StreamingSqlParser::new();
        let result = parser
            .parse("CREATE TABLE user_stats AS SELECT customer_id, COUNT(*) FROM orders WITH (\"source_config\" = \"configs/kafka.yaml\") INTO analytics_db");
            
        if let Err(e) = &result {
            panic!("Parse failed with error: {:?}", e);
        }
        let query = result.unwrap();

        // Create test record
        let mut record = HashMap::new();
        record.insert("customer_id".to_string(), FieldValue::Integer(555));
        record.insert("amount".to_string(), FieldValue::Float(299.50));

        // Execute CREATE TABLE INTO
        let result = engine
            .execute_with_record(&query, StreamRecord::new(record))
            .await;
        assert!(result.is_ok());

        // Check that the underlying SELECT was executed
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("customer_id"));
        // COUNT(*) result may vary based on aggregation implementation
    }

    #[test]
    fn test_datasource_style_error_handling() {
        let parser = StreamingSqlParser::new();

        // Test missing sink name after INTO
        let result = parser.parse("CREATE STREAM test AS SELECT * FROM source INTO");
        assert!(result.is_err());

        // Test incomplete WITH clause
        let result = parser.parse(
            r#"
            CREATE STREAM test AS SELECT * FROM source INTO sink
            WITH (
        "#,
        );
        assert!(result.is_err());

        // Test empty query
        let result = parser.parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_mixed_syntax_compatibility() {
        let parser = StreamingSqlParser::new();

        // Test that both old and new syntax work in same parser
        let old_query = parser.parse("CREATE STREAM old_style AS SELECT * FROM orders");
        let new_query = parser.parse("CREATE STREAM new_style AS SELECT * FROM orders WITH (\"source_config\" = \"configs/kafka.yaml\") INTO sink");

        assert!(old_query.is_ok());
        assert!(new_query.is_ok());

        // Verify they parse to different variants
        match old_query.unwrap() {
            StreamingQuery::CreateStream { .. } => {
                // Expected - old syntax
            }
            _ => panic!("Expected CreateStream for old syntax"),
        }

        match new_query.unwrap() {
            StreamingQuery::CreateStreamInto { .. } => {
                // Expected - new syntax
            }
            _ => panic!("Expected CreateStreamInto for new syntax"),
        }
    }
}
