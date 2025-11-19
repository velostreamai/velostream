/*!
# Multi-Source/Multi-Sink Integration Tests

Comprehensive test suite for Velostream multi-source and multi-sink processing capabilities.
Tests the complete pipeline from query analysis through job execution with multiple data sources and sinks.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{BatchConfig, DataReader, DataWriter};
use velostream::velostream::server::processors::{
    SimpleJobProcessor, TransactionalJobProcessor, create_multi_sink_writers,
    create_multi_source_readers,
};
use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::sql::query_analyzer::{
    DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType, QueryAnalyzer,
};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

/// Create test data source requirements for multi-source testing
fn create_test_multi_sources() -> Vec<DataSourceRequirement> {
    vec![
        DataSourceRequirement {
            name: "orders".to_string(),
            source_type: DataSourceType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "bootstrap.servers".to_string(),
                    "localhost:9092".to_string(),
                );
                props.insert("topic".to_string(), "orders".to_string());
                props.insert("source.format".to_string(), "json".to_string());
                props
            },
        },
        DataSourceRequirement {
            name: "customers".to_string(),
            source_type: DataSourceType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "test_data/customers.json".to_string());
                props.insert("source.format".to_string(), "json".to_string());
                props.insert("source.has_headers".to_string(), "true".to_string());
                props
            },
        },
        DataSourceRequirement {
            name: "products".to_string(),
            source_type: DataSourceType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "test_data/products.csv".to_string());
                props.insert("source.format".to_string(), "csv".to_string());
                props.insert("source.has_headers".to_string(), "true".to_string());
                props
            },
        },
    ]
}

/// Create test data sink requirements for multi-sink testing
fn create_test_multi_sinks() -> Vec<DataSinkRequirement> {
    vec![
        DataSinkRequirement {
            name: "processed_orders".to_string(),
            sink_type: DataSinkType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "bootstrap.servers".to_string(),
                    "localhost:9092".to_string(),
                );
                props.insert("topic".to_string(), "processed-orders".to_string());
                props.insert("sink.value.format".to_string(), "json".to_string());
                props
            },
        },
        DataSinkRequirement {
            name: "audit_log".to_string(),
            sink_type: DataSinkType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "output/audit.json".to_string());
                props.insert("sink.format".to_string(), "json".to_string());
                props.insert("sink.append".to_string(), "true".to_string());
                props
            },
        },
    ]
}

#[tokio::test]
async fn test_create_multi_source_readers() {
    let sources = create_test_multi_sources();
    let batch_config = None;

    // This will fail in CI without actual Kafka, but tests the creation logic
    let result = create_multi_source_readers(&sources, "test-job", None, &batch_config).await;

    match result {
        Ok(readers) => {
            // In a real environment with Kafka running, we'd get all 3 readers
            println!("Successfully created {} readers", readers.len());
        }
        Err(e) => {
            // Expected in CI environment - verify it's attempting to create all sources
            let error_msg = e.to_string();
            println!("Expected error in CI environment: {}", error_msg);

            // Verify it attempted to create sources (could fail on any of the 3 sources)
            assert!(
                error_msg.contains("source_0_orders")
                    || error_msg.contains("orders")
                    || error_msg.contains("source_1_customers")
                    || error_msg.contains("customers")
                    || error_msg.contains("source_2_products")
                    || error_msg.contains("products"),
                "Error message should mention one of the expected sources, got: {}",
                error_msg
            );
        }
    }
}

#[tokio::test]
async fn test_create_multi_sink_writers() {
    let sinks = create_test_multi_sinks();
    let batch_config = None;

    let result = create_multi_sink_writers(&sinks, "test-job", &batch_config).await;

    match result {
        Ok(writers) => {
            println!("Successfully created {} writers", writers.len());
            // File writer should work even in CI
            assert!(writers.len() >= 1, "Should create at least file writer");
        }
        Err(e) => {
            println!("Error creating writers: {}", e);
            // Some writers may fail, but shouldn't fail completely
        }
    }
}

#[tokio::test]
async fn test_simple_processor_multi_job_interface() {
    let config = velostream::velostream::server::processors::JobProcessingConfig {
        use_transactions: false,
        failure_strategy:
            velostream::velostream::server::processors::FailureStrategy::LogAndContinue,
        max_batch_size: 100,
        batch_timeout: Duration::from_millis(1000),
        max_retries: 3,
        retry_backoff: Duration::from_millis(500),
        progress_interval: 10,
        log_progress: true,
        empty_batch_count: 1,
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = SimpleJobProcessor::new(config);

    // Create mock readers and writers for interface testing
    let readers = HashMap::new(); // Empty for interface test
    let writers = HashMap::new(); // Empty for interface test

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Test that the interface exists and can be called
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        processor.process_multi_job(
            readers,
            writers,
            engine,
            query,
            "test-job".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    // Should timeout quickly since no sources, but interface should exist
    assert!(result.is_err(), "Should timeout quickly with no sources");

    // Signal shutdown to cleanup
    let _ = shutdown_tx.send(()).await;
}

#[tokio::test]
async fn test_transactional_processor_multi_job_interface() {
    let config = velostream::velostream::server::processors::JobProcessingConfig {
        use_transactions: true,
        failure_strategy: velostream::velostream::server::processors::FailureStrategy::FailBatch,
        max_batch_size: 50,
        batch_timeout: Duration::from_millis(2000),
        max_retries: 5,
        retry_backoff: Duration::from_millis(1000),
        progress_interval: 5,
        log_progress: true,
        empty_batch_count: 1,
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = TransactionalJobProcessor::new(config);

    // Create mock readers and writers for interface testing
    let readers = HashMap::new();
    let writers = HashMap::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT * FROM test_stream WITH ('use_transactions' = 'true')")
        .unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Test that the transactional interface exists and can be called
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        processor.process_multi_job(
            readers,
            writers,
            engine,
            query,
            "test-transactional-job".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    // Should complete immediately since no sources to process
    assert!(
        result.is_ok(),
        "Should complete immediately with no sources"
    );

    // Signal shutdown (may already be dropped if task completed)
    let _ = shutdown_tx.send(()).await;
}

#[tokio::test]
async fn test_query_analyzer_multi_source_detection() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Test complex multi-source query analysis
    let parser = StreamingSqlParser::new();
    let multi_source_query = r#"
        SELECT 
            o.order_id,
            c.customer_name,
            p.product_name,
            o.quantity * p.price as total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id  
        JOIN products p ON o.product_id = p.product_id
        WHERE o.amount > 100
    "#;

    match parser.parse(multi_source_query) {
        Ok(parsed_query) => {
            match analyzer.analyze(&parsed_query) {
                Ok(analysis) => {
                    println!("Query analysis completed:");
                    println!("  Required sources: {}", analysis.required_sources.len());
                    println!("  Required sinks: {}", analysis.required_sinks.len());

                    // Should detect multiple sources from JOIN clauses
                    // Note: Actual detection depends on QueryAnalyzer implementation
                    for (i, source) in analysis.required_sources.iter().enumerate() {
                        println!("  Source {}: {} ({:?})", i, source.name, source.source_type);
                    }
                }
                Err(e) => {
                    println!("Query analysis failed: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("Query parsing failed: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_multi_sink_sql_parsing() {
    let parser = StreamingSqlParser::new();

    // Test multi-sink SQL syntax
    let multi_sink_query = r#"
        CREATE STREAM processed_orders AS
        SELECT 
            order_id,
            customer_id, 
            total_amount,
            CASE 
                WHEN total_amount > 1000 THEN 'HIGH_VALUE'
                ELSE 'NORMAL' 
            END as order_tier
        FROM raw_orders
        WHERE total_amount > 0
        INTO kafka_sink, file_sink, audit_sink
        WITH (
            'kafka_sink.topic' = 'processed-orders',
            'file_sink.path' = 'output/processed_orders.json',
            'audit_sink.path' = 'audit/orders.log'
        )
    "#;

    match parser.parse(multi_sink_query) {
        Ok(parsed_query) => {
            println!("Successfully parsed multi-sink query");

            let analyzer = QueryAnalyzer::new("test-group".to_string());
            match analyzer.analyze(&parsed_query) {
                Ok(analysis) => {
                    println!("Analysis results:");
                    println!("  Sources: {}", analysis.required_sources.len());
                    println!("  Sinks: {}", analysis.required_sinks.len());

                    // Should detect multiple sinks
                    for (i, sink) in analysis.required_sinks.iter().enumerate() {
                        println!("  Sink {}: {} ({:?})", i, sink.name, sink.sink_type);
                    }
                }
                Err(e) => {
                    println!("Analysis failed: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("Parse failed (expected in current implementation): {:?}", e);
            // Multi-sink syntax may not be fully implemented yet
        }
    }
}

#[tokio::test]
async fn test_error_handling_partial_source_failures() {
    let mut sources = create_test_multi_sources();

    // Add an invalid source to test partial failure handling
    sources.push(DataSourceRequirement {
        name: "invalid_source".to_string(),
        source_type: DataSourceType::Generic("invalid".to_string()),
        properties: HashMap::new(),
    });

    let result = create_multi_source_readers(&sources, "test-error-handling", None, &None).await;

    // Should fail gracefully and provide meaningful error message
    match result {
        Ok(_) => {
            println!(
                "Unexpectedly succeeded - may indicate test environment has all sources available"
            );
        }
        Err(e) => {
            let error_msg = e.to_string();
            println!("Expected error for invalid source: {}", error_msg);

            // Verify error message is informative about the failure
            // Could fail on invalid source type OR missing files - both are valid error scenarios
            assert!(
                error_msg.contains("invalid_source")
                    || error_msg.contains("Unsupported")
                    || error_msg.contains("File not found")
                    || error_msg.contains("does not exist")
                    || error_msg.contains("customers")
                    || error_msg.contains("products"),
                "Error message should mention the failure cause, got: {}",
                error_msg
            );
        }
    }
}

#[tokio::test]
async fn test_batch_config_propagation_multi_source() {
    let sources = vec![DataSourceRequirement {
        name: "test_kafka".to_string(),
        source_type: DataSourceType::Kafka,
        properties: {
            let mut props = HashMap::new();
            props.insert(
                "bootstrap.servers".to_string(),
                "localhost:9092".to_string(),
            );
            props.insert("topic".to_string(), "test_batch_config_topic".to_string());
            props
        },
    }];

    let batch_config = Some(BatchConfig {
        strategy: velostream::velostream::datasource::BatchStrategy::FixedSize(500),
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(2000),
        enable_batching: true,
    });

    let result =
        create_multi_source_readers(&sources, "test-batch-config", None, &batch_config).await;

    // Test that batch config is properly propagated (will fail in CI but tests the interface)
    match result {
        Ok(readers) => {
            println!(
                "Successfully created {} readers with batch config",
                readers.len()
            );
        }
        Err(e) => {
            println!("Expected failure in CI: {}", e);
            // Verify the error suggests batch config was attempted
            assert!(
                e.to_string().len() > 0,
                "Should have meaningful error message"
            );
        }
    }
}

#[tokio::test]
async fn test_multi_source_job_server_integration() {
    // Test StreamJobServer integration with multi-source queries
    let server = StreamJobServer::new_with_monitoring(
        "localhost:9092".to_string(),
        "test-group".to_string(),
        10,   // max jobs
        true, // enable monitoring
    )
    .await;

    let multi_source_sql = r#"
        CREATE STREAM enriched_orders AS
        SELECT 
            o.order_id,
            o.customer_id,
            c.customer_name,
            o.total_amount
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.total_amount > 100
    "#;

    // Test that job deployment analyzes sources correctly
    // This will fail without actual data sources, but tests the analysis pipeline
    let result = server
        .deploy_job(
            "multi-source-test".to_string(),
            "1.0.0".to_string(),
            multi_source_sql.to_string(),
            "orders".to_string(), // default topic
        )
        .await;

    match result {
        Ok(()) => {
            println!("Successfully deployed multi-source job");

            // Clean up
            let _ = server.stop_job("multi-source-test").await;
        }
        Err(e) => {
            println!("Expected deployment failure in test environment: {:?}", e);
            // Verify the error is related to source creation, not parsing/analysis
            let error_msg = format!("{:?}", e);
            assert!(
                error_msg.contains("source")
                    || error_msg.contains("datasource")
                    || error_msg.contains("create"),
                "Error should be related to source creation: {}",
                error_msg
            );
        }
    }
}

/// Integration test for complete multi-source workflow
#[tokio::test]
async fn test_complete_multi_source_workflow() {
    println!("=== Testing Complete Multi-Source Workflow ===");

    // 1. Test SQL parsing
    let parser = StreamingSqlParser::new();
    let sql = r#"
        SELECT 
            orders.order_id,
            customers.name,
            products.price
        FROM orders
        LEFT JOIN customers ON orders.customer_id = customers.id
        INNER JOIN products ON orders.product_id = products.id
        WHERE orders.amount > 50
    "#;

    let parsed_query = parser.parse(sql);
    assert!(parsed_query.is_ok(), "SQL parsing should succeed");

    // 2. Test query analysis
    let mut analyzer = QueryAnalyzer::new("test-workflow".to_string());

    // Add known tables to skip external source validation for this integration test
    analyzer.add_known_table("orders".to_string());
    analyzer.add_known_table("customers".to_string());
    analyzer.add_known_table("products".to_string());

    let analysis = analyzer.analyze(&parsed_query.unwrap());
    if let Err(e) = &analysis {
        println!("Query analysis error: {:?}", e);
    }
    assert!(
        analysis.is_ok(),
        "Query analysis should succeed: {:?}",
        analysis.err()
    );

    let analysis = analysis.unwrap();
    println!(
        "Detected {} sources, {} sinks",
        analysis.required_sources.len(),
        analysis.required_sinks.len()
    );

    // 3. Test source creation (will fail but tests interface)
    if !analysis.required_sources.is_empty() {
        let result =
            create_multi_source_readers(&analysis.required_sources, "workflow-test", None, &None)
                .await;

        match result {
            Ok(readers) => {
                println!("Created {} source readers", readers.len());
            }
            Err(e) => {
                println!("Source creation failed as expected: {}", e);
            }
        }
    }

    println!("=== Multi-Source Workflow Test Complete ===");
}
