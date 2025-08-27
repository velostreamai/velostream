use ferrisstreams::ferris::schema::{FieldDefinition, Schema, StreamHandle};
use ferrisstreams::ferris::sql::ast::DataType;
use ferrisstreams::ferris::sql::context::StreamingSqlContext;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("customer_id".to_string(), DataType::Integer),
            FieldDefinition::required("amount".to_string(), DataType::Float),
            FieldDefinition::optional("status".to_string(), DataType::String),
        ])
    }

    fn create_test_handle() -> StreamHandle {
        StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        )
    }

    #[test]
    fn test_context_creation() {
        let context = StreamingSqlContext::new();
        assert_eq!(context.list_streams().len(), 0);
    }

    #[test]
    fn test_stream_registration() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        let result = context.register_stream("orders".to_string(), handle, schema);
        assert!(result.is_ok());

        let streams = context.list_streams();
        assert_eq!(streams.len(), 1);
        assert!(streams.contains(&"orders".to_string()));
    }

    #[test]
    fn test_duplicate_stream_registration() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle.clone(), schema.clone())
            .unwrap();
        let result = context.register_stream("orders".to_string(), handle, schema);

        assert!(result.is_err());
        // Check that it's a StreamError
        if let Err(err) = result {
            assert!(matches!(
                err,
                ferrisstreams::ferris::sql::error::SqlError::StreamError { .. }
            ));
        }
    }

    #[test]
    fn test_unregister_stream() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();
        assert_eq!(context.list_streams().len(), 1);

        let result = context.unregister_stream("orders");
        assert!(result.is_ok());
        assert_eq!(context.list_streams().len(), 0);
    }

    #[test]
    fn test_unregister_nonexistent_stream() {
        let mut context = StreamingSqlContext::new();
        let result = context.unregister_stream("nonexistent");

        assert!(result.is_err());
        if let Err(err) = result {
            assert!(matches!(
                err,
                ferrisstreams::ferris::sql::error::SqlError::StreamError { .. }
            ));
        }
    }

    #[test]
    fn test_valid_query_execution() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let result = context.execute_query("SELECT * FROM orders");
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_with_nonexistent_stream() {
        let context = StreamingSqlContext::new();
        let result = context.execute_query("SELECT * FROM nonexistent");

        assert!(result.is_err());
        if let Err(err) = result {
            assert!(matches!(
                err,
                ferrisstreams::ferris::sql::error::SqlError::StreamError { .. }
            ));
        }
    }

    #[test]
    fn test_query_with_valid_columns() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let result = context.execute_query("SELECT id, customer_id, amount FROM orders");
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_with_invalid_column() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let result = context.execute_query("SELECT nonexistent_column FROM orders");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(matches!(
                err,
                ferrisstreams::ferris::sql::error::SqlError::SchemaError { .. }
            ));
        }
    }

    #[test]
    fn test_schema_retrieval() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let retrieved_schema = context.get_stream_schema("orders");
        assert!(retrieved_schema.is_some());

        let schema = retrieved_schema.unwrap();
        assert_eq!(schema.fields.len(), 4);

        // Check specific fields
        assert!(schema.has_field("id"));
        assert!(schema.has_field("customer_id"));
        assert!(schema.has_field("amount"));
        assert!(schema.has_field("status"));
        assert!(!schema.has_field("nonexistent"));
    }

    #[test]
    fn test_schema_field_types() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let retrieved_schema = context.get_stream_schema("orders").unwrap();

        // Check field types
        assert_eq!(
            retrieved_schema.get_field_type("id"),
            Some(&DataType::Integer)
        );
        assert_eq!(
            retrieved_schema.get_field_type("customer_id"),
            Some(&DataType::Integer)
        );
        assert_eq!(
            retrieved_schema.get_field_type("amount"),
            Some(&DataType::Float)
        );
        assert_eq!(
            retrieved_schema.get_field_type("status"),
            Some(&DataType::String)
        );
        assert_eq!(retrieved_schema.get_field_type("nonexistent"), None);
    }

    #[test]
    fn test_windowed_query() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let result = context.execute_query("SELECT SUM(amount) FROM orders WINDOW TUMBLING(5m)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_aliased_columns() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let result = context.execute_query("SELECT id AS order_id, amount AS total FROM orders");
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_stream_registration() {
        let mut context = StreamingSqlContext::new();

        // Register first stream
        let handle1 = StreamHandle::new(
            "orders_stream".to_string(),
            "orders_topic".to_string(),
            "orders_schema".to_string(),
        );
        let schema1 = Schema::new(vec![
            FieldDefinition::required("order_id".to_string(), DataType::Integer),
            FieldDefinition::required("amount".to_string(), DataType::Float),
        ]);
        context
            .register_stream("orders".to_string(), handle1, schema1)
            .unwrap();

        // Register second stream
        let handle2 = StreamHandle::new(
            "users_stream".to_string(),
            "users_topic".to_string(),
            "users_schema".to_string(),
        );
        let schema2 = Schema::new(vec![
            FieldDefinition::required("user_id".to_string(), DataType::Integer),
            FieldDefinition::required("name".to_string(), DataType::String),
        ]);
        context
            .register_stream("users".to_string(), handle2, schema2)
            .unwrap();

        assert_eq!(context.list_streams().len(), 2);

        // Test queries on both streams
        let result1 = context.execute_query("SELECT order_id FROM orders");
        assert!(result1.is_ok());

        let result2 = context.execute_query("SELECT user_id FROM users");
        assert!(result2.is_ok());

        // Test cross-stream validation
        let result3 = context.execute_query("SELECT order_id FROM users");
        assert!(result3.is_err()); // order_id doesn't exist in users stream
    }

    #[test]
    fn test_stream_field_names() {
        let mut context = StreamingSqlContext::new();
        let handle = create_test_handle();
        let schema = create_test_schema();

        context
            .register_stream("orders".to_string(), handle, schema)
            .unwrap();

        let retrieved_schema = context.get_stream_schema("orders").unwrap();
        let field_names = retrieved_schema.field_names();

        assert_eq!(field_names.len(), 4);
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"customer_id"));
        assert!(field_names.contains(&"amount"));
        assert!(field_names.contains(&"status"));
    }

    #[test]
    fn test_nullable_vs_required_fields() {
        let schema = Schema::new(vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::optional("description".to_string(), DataType::String),
        ]);

        // Check field nullability
        let id_field = schema.get_field("id").unwrap();
        assert!(!id_field.nullable);

        let desc_field = schema.get_field("description").unwrap();
        assert!(desc_field.nullable);
    }

    #[test]
    fn test_schema_validation_with_json() {
        use serde_json::Value;
        use std::collections::HashMap;

        let schema = create_test_schema();

        // Valid record
        let mut valid_record = HashMap::new();
        valid_record.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        valid_record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(100)),
        );
        valid_record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(299.99).unwrap()),
        );
        valid_record.insert("status".to_string(), Value::String("pending".to_string()));

        assert!(schema.validate_record(&valid_record));

        // Invalid record (missing required field)
        let mut invalid_record = HashMap::new();
        invalid_record.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        // Missing customer_id (required field)
        invalid_record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(299.99).unwrap()),
        );

        assert!(!schema.validate_record(&invalid_record));

        // Invalid record (wrong type)
        let mut wrong_type_record = HashMap::new();
        wrong_type_record.insert("id".to_string(), Value::String("not_a_number".to_string()));
        wrong_type_record.insert(
            "customer_id".to_string(),
            Value::Number(serde_json::Number::from(100)),
        );
        wrong_type_record.insert(
            "amount".to_string(),
            Value::Number(serde_json::Number::from_f64(299.99).unwrap()),
        );

        assert!(!schema.validate_record(&wrong_type_record));
    }
}
