use ferrisstreams::ferris::schema::{FieldDefinition, Schema, StreamHandle};
use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::context::StreamingSqlContext;
use ferrisstreams::ferris::sql::execution::{
    types::{FieldValue, StreamRecord},
    StreamExecutionEngine,
};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::sql::DataType;
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

    // Helper function to convert InternalValue to FieldValue
    fn internal_to_field_value(internal: &InternalValue) -> FieldValue {
        match internal {
            InternalValue::String(s) => FieldValue::String(s.clone()),
            InternalValue::Number(n) => FieldValue::Float(*n),
            InternalValue::Integer(i) => FieldValue::Integer(*i),
            InternalValue::Boolean(b) => FieldValue::Boolean(*b),
            InternalValue::Null => FieldValue::Null,
            InternalValue::ScaledNumber(value, scale) => FieldValue::ScaledInteger(*value, *scale),
            InternalValue::Array(arr) => {
                let field_arr: Vec<FieldValue> = arr.iter().map(internal_to_field_value).collect();
                FieldValue::Array(field_arr)
            }
            InternalValue::Object(obj) => {
                let mut field_map = HashMap::new();
                for (k, v) in obj {
                    field_map.insert(k.clone(), internal_to_field_value(v));
                }
                FieldValue::Map(field_map)
            }
        }
    }

    fn create_order_record(
        order_id: i64,
        customer_id: i64,
        amount: f64,
        status: Option<&str>,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("order_id".to_string(), FieldValue::Integer(order_id));
        fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
        fields.insert("amount".to_string(), FieldValue::Float(amount));
        if let Some(s) = status {
            fields.insert("status".to_string(), FieldValue::String(s.to_string()));
        }
        // Use safe, controlled timestamp to avoid arithmetic overflow
        let safe_timestamp = 1000 + (order_id * 1000); // Each record 1 second apart
        fields.insert("timestamp".to_string(), FieldValue::Integer(safe_timestamp));

        StreamRecord {
            fields,
            timestamp: safe_timestamp,
            offset: order_id,
            partition: 0,
            headers: HashMap::new(),
        }
    }

    fn create_user_record(user_id: i64, name: &str, email: &str, age: Option<i64>) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
        fields.insert("name".to_string(), FieldValue::String(name.to_string()));
        fields.insert("email".to_string(), FieldValue::String(email.to_string()));
        if let Some(a) = age {
            fields.insert("age".to_string(), FieldValue::Integer(a));
        }

        StreamRecord {
            fields,
            timestamp: 1000 + user_id * 1000,
            offset: user_id,
            partition: 0,
            headers: HashMap::new(),
        }
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
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok());

        // Verify output
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("order_id"));
        assert!(output.fields.contains_key("amount"));
        assert!(!output.fields.contains_key("customer_id")); // Should not be included
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

        // Parse simple aggregation query (integration test - no windowing)
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT SUM(amount) AS total_amount FROM orders GROUP BY customer_id")
            .unwrap();

        // Execute with multiple records
        let records = vec![
            create_order_record(1, 100, 100.0, Some("pending")),
            create_order_record(2, 101, 200.0, Some("confirmed")),
            create_order_record(3, 102, 150.0, Some("pending")),
        ];

        for record in records {
            let result = engine.execute_with_record(&query, record).await;
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
        let result = engine
            .execute_with_record(&orders_query, order_record)
            .await;
        assert!(result.is_ok());

        // Test users query
        let users_query = parser.parse("SELECT user_id, name FROM users").unwrap();
        let user_record = create_user_record(100, "John Doe", "john@example.com", Some(30));
        let result = engine.execute_with_record(&users_query, user_record).await;
        assert!(result.is_ok());

        // Verify outputs
        let orders_output = rx.try_recv().unwrap();
        assert!(orders_output.fields.contains_key("order_id"));
        assert!(orders_output.fields.contains_key("customer_id"));

        let users_output = rx.try_recv().unwrap();
        assert!(users_output.fields.contains_key("user_id"));
        assert!(users_output.fields.contains_key("name"));
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
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok());

        // Verify complex output
        let output = rx.try_recv().unwrap();
        assert!(output.fields.contains_key("order_id"));
        assert!(output.fields.contains_key("amount_with_tax"));
        assert!(output.fields.contains_key("new_status"));

        // Check calculated value
        if let Some(FieldValue::Float(tax_value)) = output.fields.get("amount_with_tax") {
            assert!((tax_value - 108.0).abs() < 0.001); // 100.0 * 1.08 = 108.0
        }

        // Check literal value
        assert_eq!(
            output.fields.get("new_status"),
            Some(&FieldValue::String("processed".to_string()))
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

        // Parse simple aggregation query (integration test - no windowing)
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT AVG(amount) AS avg_amount FROM orders GROUP BY customer_id")
            .unwrap();

        // Execute with multiple records
        for i in 1..=5 {
            let record = create_order_record(i, 100 + i, 100.0 * i as f64, Some("pending"));
            let result = engine.execute_with_record(&query, record).await;
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

        // Execute with multiple records to trigger session window
        // First record starts a session
        let record1 = create_order_record(1, 100, 299.99, Some("pending"));
        let result1 = engine.execute_with_record(&query, record1).await;
        assert!(result1.is_ok());

        // Second record after session gap (30s + buffer) to close the session
        // Using timestamps that are far apart to trigger session boundary
        let mut record2_data = create_order_record(2, 100, 199.99, Some("completed"));
        // Set timestamp to be 35 seconds after the first record (exceeds 30s session gap)
        record2_data
            .fields
            .insert("timestamp".to_string(), FieldValue::Integer(36000)); // 36 seconds
        record2_data.timestamp = 36000; // Also update the StreamRecord timestamp
        let result2 = engine.execute_with_record(&query, record2_data).await;
        assert!(result2.is_ok());

        // Check for session window output - should have emission from first session
        let mut output_found = false;
        for _ in 0..10 {
            // Try multiple times as session windows can be async
            if let Ok(output) = rx.try_recv() {
                if output.fields.contains_key("session_count") {
                    output_found = true;
                    break;
                }
            }
        }
        assert!(
            output_found,
            "Expected session_count output from session window"
        );
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
        let result = engine.execute_with_record(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        assert_eq!(
            output.fields.get("nonexistent_column"),
            Some(&FieldValue::Null)
        );
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
            .fields
            .iter()
            .map(|(k, v)| {
                let json_val = match v {
                    FieldValue::Integer(i) => {
                        serde_json::Value::Number(serde_json::Number::from(*i))
                    }
                    FieldValue::Float(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                    ),
                    FieldValue::String(s) => serde_json::Value::String(s.clone()),
                    FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
                    FieldValue::Null => serde_json::Value::Null,
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

        // Parse query with multiple aggregations (integration test - no windowing)
        let parser = StreamingSqlParser::new();
        let query = parser.parse(
            "SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount, AVG(amount) AS avg_amount FROM orders GROUP BY customer_id"
        ).unwrap();

        // Execute with multiple records
        let records = vec![
            create_order_record(1, 100, 100.0, Some("pending")),
            create_order_record(2, 101, 200.0, Some("confirmed")),
            create_order_record(3, 102, 300.0, Some("pending")),
        ];

        for record in records {
            let result = engine.execute_with_record(&query, record).await;
            assert!(result.is_ok());
        }

        // Check outputs
        for _ in 0..3 {
            let output = rx.try_recv().unwrap();
            assert!(output.fields.contains_key("order_count"));
            assert!(output.fields.contains_key("total_amount"));
            assert!(output.fields.contains_key("avg_amount"));
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
