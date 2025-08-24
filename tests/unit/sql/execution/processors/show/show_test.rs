/*!
# SHOW Processor Tests

Comprehensive test suite for schema introspection operations.
*/

use ferrisstreams::ferris::sql::ast::DataType;
use ferrisstreams::ferris::sql::ast::{ShowResourceType, StreamingQuery};
use ferrisstreams::ferris::sql::execution::processors::{
    JoinContext, ProcessorContext, QueryProcessor,
};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamRecord};
use ferrisstreams::ferris::sql::schema::{FieldDefinition, Schema, StreamHandle};
use std::collections::HashMap;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: 1234567890000,
        offset: 0,
        partition: 0,
    }
}

fn create_test_context_with_schemas() -> ProcessorContext {
    let mut schemas = HashMap::new();
    let mut stream_handles = HashMap::new();

    // Create test schemas
    let users_schema = Schema::new(vec![
        FieldDefinition::new("user_id".to_string(), DataType::Integer, false),
        FieldDefinition::new("username".to_string(), DataType::String, false),
        FieldDefinition::new("email".to_string(), DataType::String, true),
        FieldDefinition::new("created_at".to_string(), DataType::Timestamp, false),
    ]);

    let orders_schema = Schema::new(vec![
        FieldDefinition::new("order_id".to_string(), DataType::Integer, false),
        FieldDefinition::new("user_id".to_string(), DataType::Integer, false),
        FieldDefinition::new("amount".to_string(), DataType::Float, false),
        FieldDefinition::new("status".to_string(), DataType::String, false),
    ]);

    schemas.insert("users".to_string(), users_schema);
    schemas.insert("orders".to_string(), orders_schema);

    // Create test stream handles
    stream_handles.insert(
        "users".to_string(),
        StreamHandle {
            id: "users_stream_1".to_string(),
            topic: "user_events".to_string(),
            schema_id: "users_schema_v1".to_string(),
        },
    );

    stream_handles.insert(
        "orders".to_string(),
        StreamHandle {
            id: "orders_stream_1".to_string(),
            topic: "order_events".to_string(),
            schema_id: "orders_schema_v1".to_string(),
        },
    );

    ProcessorContext {
        record_count: 0,
        max_records: None,
        window_context: None,
        join_context: JoinContext::new(),
        group_by_states: HashMap::new(),
        schemas,
        stream_handles,
        data_sources: HashMap::new(),
        persistent_window_states: Vec::new(),
        dirty_window_states: 0,
        metadata: HashMap::new(),
    }
}

// Test 1: SHOW STREAMS
#[tokio::test]
async fn test_show_streams() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("stream_name"));
    assert!(output_record.fields.contains_key("topic"));
    assert!(output_record.fields.contains_key("type"));

    if let Some(FieldValue::String(stream_type)) = output_record.fields.get("type") {
        assert_eq!(stream_type, "STREAM");
    } else {
        panic!("Expected type field to be STREAM");
    }
}

// Test 2: SHOW STREAMS with pattern
#[tokio::test]
async fn test_show_streams_with_pattern() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: Some("user%".to_string()),
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    if let Some(FieldValue::String(stream_name)) = output_record.fields.get("stream_name") {
        assert!(stream_name.starts_with("user"));
    } else {
        panic!("Expected stream_name to match pattern");
    }
}

// Test 3: SHOW TABLES
#[tokio::test]
async fn test_show_tables() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Tables,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("table_name"));
    assert!(output_record.fields.contains_key("topic"));
    assert!(output_record.fields.contains_key("type"));

    if let Some(FieldValue::String(table_type)) = output_record.fields.get("type") {
        assert_eq!(table_type, "TABLE");
    } else {
        panic!("Expected type field to be TABLE");
    }
}

// Test 4: SHOW TOPICS
#[tokio::test]
async fn test_show_topics() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Topics,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("topic_name"));
    assert!(output_record.fields.contains_key("registered"));

    if let Some(FieldValue::Boolean(registered)) = output_record.fields.get("registered") {
        assert!(*registered);
    } else {
        panic!("Expected registered field to be true");
    }
}

// Test 5: SHOW FUNCTIONS
#[tokio::test]
async fn test_show_functions() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Functions,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("function_name"));
    assert!(output_record.fields.contains_key("category"));
    assert!(output_record.fields.contains_key("description"));
}

// Test 6: SHOW SCHEMA
#[tokio::test]
async fn test_show_schema() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Schema {
            name: "users".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("column_name"));
    assert!(output_record.fields.contains_key("data_type"));
    assert!(output_record.fields.contains_key("nullable"));
}

// Test 7: SHOW SCHEMA for non-existent schema
#[tokio::test]
async fn test_show_schema_not_found() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Schema {
            name: "nonexistent".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Schema not found"));
}

// Test 8: DESCRIBE resource
#[tokio::test]
async fn test_describe_resource() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Describe {
            name: "users".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("column_name"));
    assert!(output_record.fields.contains_key("data_type"));
    assert!(output_record.fields.contains_key("nullable"));
    assert!(output_record.fields.contains_key("topic"));
    assert!(output_record.fields.contains_key("schema_id"));
}

// Test 9: SHOW PROPERTIES for STREAM
#[tokio::test]
async fn test_show_properties_stream() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Properties {
            resource_type: "STREAM".to_string(),
            name: "users".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("property"));
    assert!(output_record.fields.contains_key("value"));
}

// Test 10: SHOW PROPERTIES for non-existent resource
#[tokio::test]
async fn test_show_properties_not_found() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Properties {
            resource_type: "STREAM".to_string(),
            name: "nonexistent".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("STREAM not found"));
}

// Test 11: SHOW JOBS (placeholder)
#[tokio::test]
async fn test_show_jobs() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Jobs,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("message"));

    if let Some(FieldValue::String(message)) = output_record.fields.get("message") {
        assert!(message.contains("not yet implemented"));
    } else {
        panic!("Expected message field");
    }
}

// Test 12: SHOW JOB STATUS (placeholder)
#[tokio::test]
async fn test_show_job_status() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::JobStatus {
            name: Some("test_job".to_string()),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("message"));
}

// Test 13: SHOW PARTITIONS (placeholder)
#[tokio::test]
async fn test_show_partitions() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Partitions {
            name: "users".to_string(),
        },
        pattern: None,
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    assert!(output_record.fields.contains_key("message"));
}

// Test 14: Empty context (no schemas/streams)
#[tokio::test]
async fn test_show_streams_empty_context() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: None,
    };
    let record = create_test_record();
    let mut context = ProcessorContext {
        record_count: 0,
        max_records: None,
        window_context: None,
        join_context: JoinContext::new(),
        group_by_states: HashMap::new(),
        schemas: HashMap::new(),
        stream_handles: HashMap::new(),
        data_sources: HashMap::new(),
        persistent_window_states: Vec::new(),
        dirty_window_states: 0,
        metadata: HashMap::new(),
    };

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    // Should return empty result when no streams are registered
    assert!(processor_result.record.is_none());
    assert!(!processor_result.should_count);
}

// Test 15: Pattern matching edge cases
#[tokio::test]
async fn test_pattern_matching_edge_cases() {
    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: Some("%order%".to_string()),
    };
    let record = create_test_record();
    let mut context = create_test_context_with_schemas();

    let result = QueryProcessor::process_query(&query, &record, &mut context);

    assert!(result.is_ok());
    let processor_result = result.unwrap();
    assert!(processor_result.record.is_some());

    let output_record = processor_result.record.unwrap();
    if let Some(FieldValue::String(stream_name)) = output_record.fields.get("stream_name") {
        assert!(stream_name.contains("order"));
    } else {
        panic!("Expected stream_name to match pattern");
    }
}
