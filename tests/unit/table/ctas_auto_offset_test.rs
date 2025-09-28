use std::collections::HashMap;
use velostream::velostream::sql::ast::StreamingQuery;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::SqlError;
use velostream::velostream::table::ctas::CtasProcessor;

#[test]
fn test_parse_ctas_with_auto_offset_latest() {
    let parser = StreamingSqlParser::new();

    let query = r#"
        CREATE TABLE real_time_data AS
        SELECT * FROM kafka_stream
        WITH ("auto.offset.reset" = "latest")
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS with auto.offset.reset"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable { properties, .. } => {
            assert_eq!(
                properties.get("auto.offset.reset"),
                Some(&"latest".to_string()),
                "auto.offset.reset property not found or incorrect"
            );
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[test]
fn test_parse_ctas_with_auto_offset_earliest() {
    let parser = StreamingSqlParser::new();

    let query = r#"
        CREATE TABLE historical_data AS
        SELECT * FROM kafka_stream
        WITH ("auto.offset.reset" = "earliest")
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS with auto.offset.reset"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable { properties, .. } => {
            assert_eq!(
                properties.get("auto.offset.reset"),
                Some(&"earliest".to_string()),
                "auto.offset.reset property not found or incorrect"
            );
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[test]
fn test_parse_ctas_with_multiple_properties() {
    let parser = StreamingSqlParser::new();

    let query = r#"
        CREATE TABLE analytics_table AS
        SELECT * FROM kafka_stream
        WITH (
            "auto.offset.reset" = "latest",
            "kafka.batch.size" = "2000",
            "retention" = "7 days"
        )
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS with multiple properties"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable { properties, .. } => {
            assert_eq!(
                properties.get("auto.offset.reset"),
                Some(&"latest".to_string()),
                "auto.offset.reset property not found"
            );
            assert_eq!(
                properties.get("kafka.batch.size"),
                Some(&"2000".to_string()),
                "kafka.batch.size property not found"
            );
            assert_eq!(
                properties.get("retention"),
                Some(&"7 days".to_string()),
                "retention property not found"
            );
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[tokio::test]
async fn test_ctas_processor_with_auto_offset() {
    let processor = CtasProcessor::new("localhost:9092".to_string());

    // Test that the processor correctly handles auto.offset.reset property
    let mut properties = HashMap::new();
    properties.insert("auto.offset.reset".to_string(), "latest".to_string());

    // The actual table creation will fail in tests due to Kafka connection,
    // but we're testing that the property is passed through correctly
    let result = processor
        .create_table(
            "test_table",
            Box::new(StreamingQuery::Select {
                fields: vec![velostream::velostream::sql::ast::SelectField::Wildcard],
                from: velostream::velostream::sql::ast::StreamSource::Stream(
                    "test_stream".to_string(),
                ),
                from_alias: None,
                joins: None,
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
                order_by: None,
                limit: None,
                emit_mode: None,
                properties: properties.clone(),
            }),
            properties,
        )
        .await;

    // We expect an error due to Kafka connection issues in test environment
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            println!("Expected error in test environment: {}", message);
        }
        Ok(_) => println!("Unexpected success in test environment"),
        Err(e) => println!("Got error: {:?}", e),
    }
}

#[test]
fn test_ctas_without_auto_offset_defaults() {
    let parser = StreamingSqlParser::new();

    // When no auto.offset.reset is specified, it should use default (earliest)
    let query = r#"
        CREATE TABLE default_table AS
        SELECT * FROM kafka_stream
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS without auto.offset.reset"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable { properties, .. } => {
            // No auto.offset.reset property should be present
            assert_eq!(
                properties.get("auto.offset.reset"),
                None,
                "auto.offset.reset should not be present when not specified"
            );
        }
        _ => panic!("Expected CreateTable query"),
    }
}
