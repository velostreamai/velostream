use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::DataType;
use ferrisstreams::ferris::sql::context::StreamingSqlContext;
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::sql::schema::{FieldDefinition, Schema, StreamHandle};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_orders_schema() -> Schema {
        Schema::new(vec![
            FieldDefinition::required("order_id".to_string(), DataType::Integer),
            FieldDefinition::required("customer_id".to_string(), DataType::Integer),
            FieldDefinition::required("amount".to_string(), DataType::Float),
            FieldDefinition::optional("status".to_string(), DataType::String),
            FieldDefinition::required("timestamp".to_string(), DataType::Timestamp),
        ])
    }

    fn create_users_schema() -> Schema {
        Schema::new(vec![
            FieldDefinition::required("user_id".to_string(), DataType::Integer),
            FieldDefinition::required("name".to_string(), DataType::String),
            FieldDefinition::required("email".to_string(), DataType::String),
            FieldDefinition::optional("age".to_string(), DataType::Integer),
        ])
    }

    fn create_order_record(
        order_id: i64,
        customer_id: i64,
        amount: f64,
        status: Option<&str>,
    ) -> HashMap<String, InternalValue> {
        let mut record = HashMap::new();
        record.insert("order_id".to_string(), InternalValue::Integer(order_id));
        record.insert(
            "customer_id".to_string(),
            InternalValue::Integer(customer_id),
        );
        record.insert("amount".to_string(), InternalValue::Number(amount));
        if let Some(s) = status {
            record.insert("status".to_string(), InternalValue::String(s.to_string()));
        }
        record.insert(
            "timestamp".to_string(),
            InternalValue::Integer(chrono::Utc::now().timestamp()),
        );
        record
    }

    fn create_user_record(
        user_id: i64,
        name: &str,
        email: &str,
        age: Option<i64>,
    ) -> HashMap<String, InternalValue> {
        let mut record = HashMap::new();
        record.insert("user_id".to_string(), InternalValue::Integer(user_id));
        record.insert("name".to_string(), InternalValue::String(name.to_string()));
        record.insert(
            "email".to_string(),
            InternalValue::String(email.to_string()),
        );
        if let Some(a) = age {
            record.insert("age".to_string(), InternalValue::Integer(a));
        }
        record
    }

    #[tokio::test]
    async fn test_end_to_end_simple_query() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = create_orders_schema();
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query
        let parser = StreamingSqlParser::new();
        let query = parser.parse("SELECT order_id, amount FROM orders").unwrap();

        // Execute query
        let record = create_order_record(1, 100, 299.99, Some("pending"));
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Verify output
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("order_id"));
        assert!(output.contains_key("amount"));
        assert!(!output.contains_key("customer_id")); // Should not be included
    }

    #[tokio::test]
    async fn test_end_to_end_windowed_aggregation() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = create_orders_schema();
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse windowed query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT SUM(amount) AS total_amount FROM orders WINDOW TUMBLING(5m)")
            .unwrap();

        // Execute with multiple records
        let records = vec![
            create_order_record(1, 100, 100.0, Some("pending")),
            create_order_record(2, 101, 200.0, Some("confirmed")),
            create_order_record(3, 102, 150.0, Some("pending")),
        ];

        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }

        // Should have aggregated outputs
        for _ in 0..3 {
            let output = rx.try_recv();
            assert!(output.is_ok());
        }
    }

    #[tokio::test]
    async fn test_multiple_stream_registration_and_queries() {
        // Setup context with multiple streams
        let mut context = StreamingSqlContext::new();

        // Register orders stream
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        context
            .register_stream("orders".to_string(), orders_handle, create_orders_schema())
            .unwrap();

        // Register users stream
        let users_handle = StreamHandle::new(
            "users_stream".to_string(),
            "users_topic".to_string(),
            "users_schema".to_string(),
        );
        context
            .register_stream("users".to_string(), users_handle, create_users_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
        let parser = StreamingSqlParser::new();

        // Test orders query
        let orders_query = parser
            .parse("SELECT order_id, customer_id FROM orders")
            .unwrap();
        let order_record = create_order_record(1, 100, 299.99, Some("pending"));
        let result = engine.execute(&orders_query, order_record).await;
        assert!(result.is_ok());

        // Test users query
        let users_query = parser.parse("SELECT user_id, name FROM users").unwrap();
        let user_record = create_user_record(100, "John Doe", "john@example.com", Some(30));
        let result = engine.execute(&users_query, user_record).await;
        assert!(result.is_ok());

        // Verify outputs
        let orders_output = rx.try_recv().unwrap();
        assert!(orders_output.contains_key("order_id"));
        assert!(orders_output.contains_key("customer_id"));

        let users_output = rx.try_recv().unwrap();
        assert!(users_output.contains_key("user_id"));
        assert!(users_output.contains_key("name"));
    }

    #[tokio::test]
    async fn test_complex_expression_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        context
            .register_stream("orders".to_string(), orders_handle, create_orders_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse complex query with arithmetic and aliases
        let parser = StreamingSqlParser::new();
        let query = parser.parse(
            "SELECT order_id, amount * 1.08 AS amount_with_tax, 'processed' AS new_status FROM orders"
        ).unwrap();

        // Execute query
        let record = create_order_record(1, 100, 100.0, Some("pending"));
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Verify complex output
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("order_id"));
        assert!(output.contains_key("amount_with_tax"));
        assert!(output.contains_key("new_status"));

        // Check calculated value
        if let Some(InternalValue::Number(tax_value)) = output.get("amount_with_tax") {
            assert!((tax_value - 108.0).abs() < 0.001); // 100.0 * 1.08 = 108.0
        }

        // Check literal value
        assert_eq!(
            output.get("new_status"),
            Some(&InternalValue::String("processed".to_string()))
        );
    }

    #[tokio::test]
    async fn test_schema_validation_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = create_orders_schema();
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Test valid query
        let valid_result = context.execute_query("SELECT order_id, amount FROM orders");
        assert!(valid_result.is_ok());

        // Test invalid column query
        let invalid_result = context.execute_query("SELECT nonexistent_column FROM orders");
        assert!(invalid_result.is_err());

        // Test query on nonexistent stream
        let no_stream_result = context.execute_query("SELECT * FROM nonexistent_stream");
        assert!(no_stream_result.is_err());
    }

    #[tokio::test]
    async fn test_sliding_window_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        context
            .register_stream("orders".to_string(), orders_handle, create_orders_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse sliding window query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT AVG(amount) AS avg_amount FROM orders WINDOW SLIDING(10m, 5m)")
            .unwrap();

        // Execute with multiple records
        for i in 1..=5 {
            let record = create_order_record(i, 100 + i, 100.0 * i as f64, Some("pending"));
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }

        // Should have outputs for sliding window
        for _ in 0..5 {
            let output = rx.try_recv();
            assert!(output.is_ok());
        }
    }

    #[tokio::test]
    async fn test_session_window_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        context
            .register_stream("orders".to_string(), orders_handle, create_orders_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse session window query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT COUNT(*) AS session_count FROM orders WINDOW SESSION(30s)")
            .unwrap();

        // Execute with records
        let record = create_order_record(1, 100, 299.99, Some("pending"));
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        // Check output
        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("session_count"));
    }

    #[tokio::test]
    async fn test_error_propagation_integration() {
        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse valid query
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT nonexistent_column FROM orders")
            .unwrap();

        // Execute with valid record - nonexistent columns return NULL
        let record = create_order_record(1, 100, 299.99, Some("pending"));
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        assert_eq!(output.get("nonexistent_column"), Some(&InternalValue::Null));
    }

    #[tokio::test]
    async fn test_data_type_validation_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = create_orders_schema();
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Get schema and validate different record types
        let schema = context.get_stream_schema("orders").unwrap();

        // Valid record
        let valid_record = create_order_record(1, 100, 299.99, Some("pending"));
        // Convert InternalValue to serde_json::Value for validation
        let valid_json_record: HashMap<String, serde_json::Value> = valid_record
            .iter()
            .map(|(k, v)| {
                let json_val = match v {
                    InternalValue::Integer(i) => {
                        serde_json::Value::Number(serde_json::Number::from(*i))
                    }
                    InternalValue::Number(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                    ),
                    InternalValue::String(s) => serde_json::Value::String(s.clone()),
                    InternalValue::Boolean(b) => serde_json::Value::Bool(*b),
                    InternalValue::Null => serde_json::Value::Null,
                    _ => serde_json::Value::String(format!("{:?}", v)),
                };
                (k.clone(), json_val)
            })
            .collect();
        assert!(schema.validate_record(&valid_json_record));

        // Invalid record - wrong type for amount
        let mut invalid_record = HashMap::new();
        invalid_record.insert(
            "order_id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        );
        invalid_record.insert(
            "customer_id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(100)),
        );
        invalid_record.insert(
            "amount".to_string(),
            serde_json::Value::String("not_a_number".to_string()),
        ); // Wrong type
        invalid_record.insert(
            "timestamp".to_string(),
            serde_json::Value::Number(serde_json::Number::from(chrono::Utc::now().timestamp())),
        );

        assert!(!schema.validate_record(&invalid_record));

        // Missing required field
        let mut missing_field_record = HashMap::new();
        missing_field_record.insert(
            "order_id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        );
        // Missing customer_id and other required fields

        assert!(!schema.validate_record(&missing_field_record));
    }

    #[tokio::test]
    async fn test_multiple_aggregations_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        context
            .register_stream("orders".to_string(), orders_handle, create_orders_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        // Parse query with multiple aggregations
        let parser = StreamingSqlParser::new();
        let query = parser.parse(
            "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount, AVG(amount) AS avg_amount FROM orders WINDOW TUMBLING(1m)"
        ).unwrap();

        // Execute with multiple records
        let records = vec![
            create_order_record(1, 100, 100.0, Some("pending")),
            create_order_record(2, 101, 200.0, Some("confirmed")),
            create_order_record(3, 102, 300.0, Some("pending")),
        ];

        for record in records {
            let result = engine.execute(&query, record).await;
            assert!(result.is_ok());
        }

        // Check outputs
        for _ in 0..3 {
            let output = rx.try_recv().unwrap();
            assert!(output.contains_key("order_count"));
            assert!(output.contains_key("total_amount"));
            assert!(output.contains_key("avg_amount"));
        }
    }

    #[tokio::test]
    async fn test_stream_lifecycle_integration() {
        // Setup context
        let mut context = StreamingSqlContext::new();

        // Register stream
        let orders_handle = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let orders_schema = create_orders_schema();
        context
            .register_stream("orders".to_string(), orders_handle, orders_schema)
            .unwrap();

        // Verify stream is registered
        assert_eq!(context.list_streams().len(), 1);
        assert!(context.get_stream_schema("orders").is_some());

        // Test query execution
        let result = context.execute_query("SELECT * FROM orders");
        assert!(result.is_ok());

        // Unregister stream
        let unregister_result = context.unregister_stream("orders");
        assert!(unregister_result.is_ok());

        // Verify stream is unregistered
        assert_eq!(context.list_streams().len(), 0);
        assert!(context.get_stream_schema("orders").is_none());

        // Query should now fail
        let result = context.execute_query("SELECT * FROM orders");
        assert!(result.is_err());
    }
}
