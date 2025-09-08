//! Tests for simplified compound type determination in QueryAnalyzer
//!
//! This module tests that QueryAnalyzer correctly handles:
//! 1. Simple compound type declarations (.type with values like 'kafka_source', 'kafka_sink')
//! 2. Error handling for missing or invalid type declarations
//! 3. Source vs Sink role determination from SQL structure
//! 4. Consistent type+role specification in single compound value

use ferrisstreams::ferris::sql::{
    ast::{IntoClause, SelectField, StreamSource, StreamingQuery},
    parser::StreamingSqlParser,
    query_analyzer::{DataSinkType, DataSourceType, QueryAnalyzer},
    SqlError,
};
use std::collections::HashMap;

/// Test that compound source type declaration works (kafka_source)
#[test]
fn test_kafka_source_compound_type() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("kafka_orders.type".to_string(), "kafka_source".to_string());
    config.insert(
        "kafka_orders.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    config.insert("kafka_orders.topic".to_string(), "orders".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "kafka_orders",
        &config,
        &serialization_config,
        &mut analysis,
    );

    assert!(result.is_ok());
    assert_eq!(analysis.required_sources.len(), 1);
    assert_eq!(
        analysis.required_sources[0].source_type,
        DataSourceType::Kafka
    );
    assert_eq!(analysis.required_sources[0].name, "kafka_orders");
}

/// Test that compound source type declaration works (s3_source)
#[test]
fn test_s3_source_compound_type() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("s3_data.type".to_string(), "s3_source".to_string());
    config.insert("s3_data.bucket".to_string(), "data-lake".to_string());
    config.insert("s3_data.region".to_string(), "us-west-2".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source("s3_data", &config, &serialization_config, &mut analysis);

    assert!(result.is_ok());
    assert_eq!(analysis.required_sources.len(), 1);
    assert_eq!(analysis.required_sources[0].source_type, DataSourceType::S3);
    assert_eq!(analysis.required_sources[0].name, "s3_data");
}

/// Test that compound source type declaration works (file_source)
#[test]
fn test_file_source_compound_type() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("file_data.type".to_string(), "file_source".to_string());
    config.insert("file_data.path".to_string(), "/data/input.csv".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result =
        analyzer.analyze_source("file_data", &config, &serialization_config, &mut analysis);

    assert!(result.is_ok());
    assert_eq!(analysis.required_sources.len(), 1);
    assert_eq!(
        analysis.required_sources[0].source_type,
        DataSourceType::File
    );
    assert_eq!(analysis.required_sources[0].name, "file_data");
}

/// Test that compound sink type declaration works (kafka_sink)
#[test]
fn test_kafka_sink_compound_type() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("kafka_sink.type".to_string(), "kafka_sink".to_string());
    config.insert(
        "kafka_sink.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    config.insert("kafka_sink.topic".to_string(), "processed-data".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_sink("kafka_sink", &config, &serialization_config, &mut analysis);

    assert!(result.is_ok());
    assert_eq!(analysis.required_sinks.len(), 1);
    assert_eq!(analysis.required_sinks[0].sink_type, DataSinkType::Kafka);
    assert_eq!(analysis.required_sinks[0].name, "kafka_sink");
}

/// Test that missing type declaration results in an error for sources
#[test]
fn test_missing_source_type_declaration_error() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Config with properties but no explicit type
    let mut config = HashMap::new();
    config.insert(
        "orders.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    config.insert("orders.topic".to_string(), "orders".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source("orders", &config, &serialization_config, &mut analysis);

    // Should fail with configuration error
    assert!(result.is_err());
    if let Err(SqlError::ConfigurationError { message }) = result {
        assert!(message.contains("Source type must be explicitly specified"));
        assert!(message.contains("orders.type"));
        assert!(message.contains("kafka_source"));
        assert!(message.contains("file_source"));
        assert!(message.contains("s3_source"));
    } else {
        panic!("Expected ConfigurationError, got: {:?}", result);
    }
}

/// Test that missing type declaration results in an error for sinks
#[test]
fn test_missing_sink_type_declaration_error() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Config with properties but no explicit type
    let mut config = HashMap::new();
    config.insert("output.path".to_string(), "/data/output.json".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_sink("output", &config, &serialization_config, &mut analysis);

    // Should fail with configuration error
    assert!(result.is_err());
    if let Err(SqlError::ConfigurationError { message }) = result {
        assert!(message.contains("Sink type must be explicitly specified"));
        assert!(message.contains("output.type"));
        assert!(message.contains("kafka_sink"));
        assert!(message.contains("file_sink"));
        assert!(message.contains("s3_sink"));
    } else {
        panic!("Expected ConfigurationError, got: {:?}", result);
    }
}

/// Test invalid compound type results in error
#[test]
fn test_invalid_compound_source_type_error() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("invalid.type".to_string(), "invalid_type".to_string());
    config.insert(
        "invalid.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source("invalid", &config, &serialization_config, &mut analysis);

    assert!(result.is_err());
    if let Err(SqlError::ConfigurationError { message }) = result {
        assert!(message.contains("Invalid source type 'invalid_type'"));
        assert!(message.contains("kafka_source"));
        assert!(message.contains("file_source"));
        assert!(message.contains("s3_source"));
        assert!(message.contains("database_source"));
    } else {
        panic!(
            "Expected ConfigurationError for invalid type, got: {:?}",
            result
        );
    }
}

/// Test invalid compound sink type results in error  
#[test]
fn test_invalid_compound_sink_type_error() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("invalid.type".to_string(), "wrong_sink".to_string());
    config.insert("invalid.path".to_string(), "/data/output.json".to_string());

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_sink("invalid", &config, &serialization_config, &mut analysis);

    assert!(result.is_err());
    if let Err(SqlError::ConfigurationError { message }) = result {
        assert!(message.contains("Invalid sink type 'wrong_sink'"));
        assert!(message.contains("kafka_sink"));
        assert!(message.contains("file_sink"));
        assert!(message.contains("s3_sink"));
        assert!(message.contains("database_sink"));
        assert!(message.contains("iceberg_sink"));
    } else {
        panic!(
            "Expected ConfigurationError for invalid type, got: {:?}",
            result
        );
    }
}

/// Test all supported compound source types
#[test]
fn test_all_supported_compound_source_types() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();

    let test_cases = vec![
        ("kafka_source", DataSourceType::Kafka),
        ("file_source", DataSourceType::File),
        ("s3_source", DataSourceType::S3),
        ("database_source", DataSourceType::Database),
    ];

    for (type_str, expected_type) in test_cases {
        let mut config = HashMap::new();
        config.insert(format!("test.type"), type_str.to_string());

        let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
            required_sources: Vec::new(),
            required_sinks: Vec::new(),
            configuration: config.clone(),
        };

        let result = analyzer.analyze_source("test", &config, &serialization_config, &mut analysis);

        assert!(result.is_ok(), "Failed for compound type: {}", type_str);
        assert_eq!(
            analysis.required_sources[0].source_type, expected_type,
            "Wrong type for: {}",
            type_str
        );

        // Clear analysis for next iteration
        analysis.required_sources.clear();
    }
}

/// Test all supported compound sink types
#[test]
fn test_all_supported_compound_sink_types() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();

    let test_cases = vec![
        ("kafka_sink", DataSinkType::Kafka),
        ("file_sink", DataSinkType::File),
        ("s3_sink", DataSinkType::S3),
        ("database_sink", DataSinkType::Database),
        ("iceberg_sink", DataSinkType::Iceberg),
    ];

    for (type_str, expected_type) in test_cases {
        let mut config = HashMap::new();
        config.insert(format!("test.type"), type_str.to_string());

        let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
            required_sources: Vec::new(),
            required_sinks: Vec::new(),
            configuration: config.clone(),
        };

        let result = analyzer.analyze_sink("test", &config, &serialization_config, &mut analysis);

        assert!(result.is_ok(), "Failed for compound type: {}", type_str);
        assert_eq!(
            analysis.required_sinks[0].sink_type, expected_type,
            "Wrong type for: {}",
            type_str
        );

        // Clear analysis for next iteration
        analysis.required_sinks.clear();
    }
}

/// Test that properties are correctly extracted with compound types
#[test]
fn test_properties_extraction_with_compound_types() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut config = HashMap::new();
    config.insert("kafka_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "kafka_source.bootstrap.servers".to_string(),
        "broker1:9092,broker2:9092".to_string(),
    );
    config.insert("kafka_source.topic".to_string(), "events".to_string());
    config.insert(
        "kafka_source.group.id".to_string(),
        "my-consumer-group".to_string(),
    );
    config.insert(
        "kafka_source.auto.offset.reset".to_string(),
        "earliest".to_string(),
    );

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "kafka_source",
        &config,
        &serialization_config,
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // Check that all properties are correctly extracted
    assert_eq!(
        source.properties.get("bootstrap.servers").unwrap(),
        "broker1:9092,broker2:9092"
    );
    assert_eq!(source.properties.get("topic").unwrap(), "events");
    assert_eq!(
        source.properties.get("group.id").unwrap(),
        "my-consumer-group"
    );
    assert_eq!(
        source.properties.get("auto.offset.reset").unwrap(),
        "earliest"
    );

    // Check that original keys are preserved for compatibility
    assert!(source
        .properties
        .contains_key("kafka_source.bootstrap.servers"));
    assert!(source.properties.contains_key("kafka_source.topic"));
}

/// Test error messages contain helpful guidance
#[test]
fn test_error_message_contains_helpful_guidance() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let config = HashMap::new(); // Empty config - no type specified

    let serialization_config =
        ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default();
    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: config.clone(),
    };

    let result =
        analyzer.analyze_source("my_source", &config, &serialization_config, &mut analysis);

    assert!(result.is_err());
    if let Err(SqlError::ConfigurationError { message }) = result {
        // Check that error message is helpful and contains specific source name
        assert!(message.contains("my_source"));
        assert!(message.contains("my_source.type"));
        assert!(message.contains("kafka_source"));
        assert!(message.contains("file_source"));
        assert!(message.contains("explicitly specified"));
    } else {
        panic!("Expected ConfigurationError with helpful message");
    }
}

/// Integration test: Test complete SQL parsing with explicit types
#[test]
fn test_integration_sql_parsing_with_explicit_types() {
    let parser = StreamingSqlParser::new();
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // SQL with explicit type configuration
    let sql = r#"
        CREATE STREAM processed_orders AS
        SELECT 
            o.order_id,
            c.customer_name,
            o.amount * 1.1 as total_with_tax
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        INTO processed_sink
        WITH (
            'orders.type' = 'kafka_source',
            'orders.bootstrap.servers' = 'localhost:9092',
            'orders.topic' = 'raw-orders',
            
            'customers.type' = 'file_source',
            'customers.path' = '/data/customers.csv',
            
            'processed_sink.type' = 'kafka_sink',
            'processed_sink.bootstrap.servers' = 'localhost:9092',
            'processed_sink.topic' = 'processed-orders'
        )
    "#;

    let query = parser.parse(sql).expect("SQL should parse successfully");
    let analysis = analyzer.analyze(&query).expect("Analysis should succeed");

    // Verify sources were correctly identified
    assert_eq!(analysis.required_sources.len(), 2);

    let orders_source = analysis
        .required_sources
        .iter()
        .find(|s| s.name == "orders")
        .expect("orders source should exist");
    assert_eq!(orders_source.source_type, DataSourceType::Kafka);

    let customers_source = analysis
        .required_sources
        .iter()
        .find(|s| s.name == "customers")
        .expect("customers source should exist");
    assert_eq!(customers_source.source_type, DataSourceType::File);

    // Verify sink was correctly identified
    assert_eq!(analysis.required_sinks.len(), 1);
    assert_eq!(analysis.required_sinks[0].name, "processed_sink");
    assert_eq!(analysis.required_sinks[0].sink_type, DataSinkType::Kafka);
}

/// Test that SQL structure determines source vs sink roles, not configuration
#[test]
fn test_source_sink_role_determination_from_sql_structure() {
    let parser = StreamingSqlParser::new();
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Same configuration can be source or sink depending on SQL structure
    let sql_with_source = r#"
        SELECT * FROM data_table
        WITH (
            'data_table.type' = 'kafka_source',
            'data_table.bootstrap.servers' = 'localhost:9092',
            'data_table.topic' = 'data'
        )
    "#;

    let sql_with_sink = r#"
        SELECT * FROM raw_data
        INTO data_table
        WITH (
            'raw_data.type' = 'kafka_source',
            'raw_data.bootstrap.servers' = 'localhost:9092',
            'raw_data.topic' = 'raw',
            
            'data_table.type' = 'kafka_sink',
            'data_table.bootstrap.servers' = 'localhost:9092',
            'data_table.topic' = 'data'
        )
    "#;

    // Test data_table as SOURCE (in FROM clause)
    let query1 = parser.parse(sql_with_source).expect("SQL should parse");
    let analysis1 = analyzer.analyze(&query1).expect("Analysis should succeed");

    assert_eq!(analysis1.required_sources.len(), 1);
    assert_eq!(analysis1.required_sources[0].name, "data_table");
    assert_eq!(analysis1.required_sinks.len(), 0);

    // Test data_table as SINK (in INTO clause)
    let query2 = parser.parse(sql_with_sink).expect("SQL should parse");
    let analysis2 = analyzer.analyze(&query2).expect("Analysis should succeed");

    assert_eq!(analysis2.required_sources.len(), 1);
    assert_eq!(analysis2.required_sources[0].name, "raw_data");
    assert_eq!(analysis2.required_sinks.len(), 1);
    assert_eq!(analysis2.required_sinks[0].name, "data_table");
}
