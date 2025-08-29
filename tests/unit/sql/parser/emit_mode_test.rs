/*!
Test EMIT clause parsing and functionality
*/

use ferrisstreams::ferris::sql::{
    ast::{EmitMode, StreamingQuery},
    parser::StreamingSqlParser,
};

#[tokio::test]
async fn test_emit_changes_parsing() {
    let parser = StreamingSqlParser::new();

    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(5m) EMIT CHANGES";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_some(), "Should have WINDOW clause");
            assert_eq!(emit_mode, Some(EmitMode::Changes));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_emit_final_parsing() {
    let parser = StreamingSqlParser::new();

    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT FINAL";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_none(), "Should have no WINDOW clause");
            assert_eq!(emit_mode, Some(EmitMode::Final));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_no_emit_clause() {
    let parser = StreamingSqlParser::new();

    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let StreamingQuery::Select { emit_mode, .. } = query {
            assert_eq!(emit_mode, None);
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_emit_overrides_windowed_mode() {
    let parser = StreamingSqlParser::new();

    // EMIT CHANGES should override windowed behavior and emit immediately
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(5m) EMIT CHANGES";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let StreamingQuery::Select {
            emit_mode, window, ..
        } = query
        {
            // Should have window clause (windowed aggregation)
            assert!(window.is_some(), "Should have WINDOW clause");

            // But EMIT CHANGES should override to continuous emission
            assert_eq!(emit_mode, Some(EmitMode::Changes));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[test]
fn test_emit_final_parsing_without_window() {
    let parser = StreamingSqlParser::new();

    // EMIT FINAL should parse successfully but fail at execution without window
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT FINAL";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let StreamingQuery::Select {
            emit_mode, window, ..
        } = query
        {
            // Should have no window clause
            assert!(window.is_none(), "Should have no WINDOW clause");

            // Should parse EMIT FINAL correctly (execution will validate)
            assert_eq!(emit_mode, Some(EmitMode::Final));
        } else {
            panic!("Expected Select query");
        }
    }

    #[tokio::test]
    async fn test_emit_final_validation_error() {
        use ferrisstreams::ferris::serialization::{
            InternalValue, JsonFormat, SerializationFormat,
        };
        use ferrisstreams::ferris::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::mpsc;

        let parser = StreamingSqlParser::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let format: Arc<dyn SerializationFormat> = Arc::new(JsonFormat);
        let mut engine = StreamExecutionEngine::new(tx, format);

        // Parse query with EMIT FINAL but no WINDOW clause - should parse fine
        let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT FINAL";
        let query = parser.parse(query_str).expect("Should parse successfully");

        // Create test record
        let mut fields = HashMap::new();
        fields.insert("customer_id".to_string(), FieldValue::Integer(1));

        let record1 = StreamRecord::new(fields);


        // Execution should fail with validation error
        let result = engine.execute_with_record(&query, record1).await;
        assert!(result.is_err(), "Expected execution to fail");

        if let Err(error) = result {
            let error_msg = format!("{:?}", error);
            assert!(
                error_msg.contains("EMIT FINAL can only be used with windowed aggregations"),
                "Expected validation error message, got: {}",
                error_msg
            );
        }
    }
}

#[tokio::test]
async fn test_invalid_emit_mode_parsing() {
    let parser = StreamingSqlParser::new();

    // Test invalid EMIT mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT INVALID";
    let result = parser.parse(query_str);

    assert!(
        result.is_err(),
        "Query parsing should fail for invalid EMIT mode"
    );

    if let Err(err) = result {
        let error_msg = err.to_string();
        assert!(
            error_msg.contains("Expected CHANGES or FINAL after EMIT")
                || error_msg.contains("Invalid EMIT mode")
                || error_msg.contains("INVALID"),
            "Expected EMIT validation error, got: {}",
            error_msg
        );
    }
}
