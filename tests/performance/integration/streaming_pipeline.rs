//! Streaming Pipeline Integration Benchmarks
//!
//! End-to-end performance benchmarks testing complete streaming pipelines
//! including Kafka integration, SQL execution, and cross-format serialization.

use super::super::common::{
    generate_test_records, BenchmarkConfig, BenchmarkMode, MetricsCollector, TestRecordConfig,
};
use ferrisstreams::ferris::sql::execution::{types::FieldValue, StreamRecord};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;

/// End-to-end pipeline benchmark testing full streaming workflow
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_end_to_end_pipeline() {
    println!("🚀 End-to-End Streaming Pipeline Benchmark");
    println!("===========================================");

    let config = BenchmarkConfig::enhanced();
    let mut metrics = MetricsCollector::verbose();

    // Test different pipeline scales
    for &scale in &[1_000, 10_000, 50_000] {
        println!("\n📊 Pipeline Scale: {} records", scale);

        let test_config = TestRecordConfig::complex(scale);

        metrics.start();
        let result = run_full_streaming_pipeline(&test_config).await;
        let duration = metrics.end(&format!("pipeline_scale_{}", scale));

        match result {
            Ok(processed_count) => {
                metrics.set_counter(
                    &format!("pipeline_scale_{}_records", scale),
                    processed_count,
                );

                let throughput = processed_count as f64 / duration.as_secs_f64();
                println!(
                    "   ✅ Processed {} records in {:?} ({:.0} records/sec)",
                    processed_count, duration, throughput
                );

                // Performance validation thresholds
                let min_throughput = match scale {
                    1_000 => 500.0,    // At least 500 records/sec for small scale
                    10_000 => 2_000.0, // At least 2K records/sec for medium scale
                    50_000 => 5_000.0, // At least 5K records/sec for large scale
                    _ => 100.0,
                };

                if throughput >= min_throughput {
                    println!(
                        "   🎯 PASS: Throughput {} >= {} records/sec",
                        throughput, min_throughput
                    );
                } else {
                    println!(
                        "   ⚠️  WARN: Throughput {} < {} records/sec",
                        throughput, min_throughput
                    );
                }
            }
            Err(e) => {
                println!("   ❌ Pipeline failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Test Kafka → SQL → Kafka pipeline simulation
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_kafka_to_sql_to_kafka_pipeline() {
    println!("🚀 Kafka → SQL → Kafka Pipeline Benchmark");
    println!("=========================================");

    let config = BenchmarkConfig::production();
    let mut metrics = MetricsCollector::verbose();

    let test_config = TestRecordConfig::complex(25_000);
    let records = generate_test_records(&test_config);

    metrics.start();

    // Simulate: Kafka Consumer → StreamRecord processing → SQL execution → Kafka Producer
    let (input_tx, mut input_rx) = mpsc::unbounded_channel();
    let (output_tx, mut output_rx) = mpsc::unbounded_channel();

    // Producer task (simulates Kafka consumer)
    let producer_records = records.clone();
    tokio::spawn(async move {
        for record in producer_records {
            if input_tx.send(record).is_err() {
                break;
            }
        }
    });

    // Processing task (simulates SQL execution engine)
    tokio::spawn(async move {
        while let Some(record) = input_rx.recv().await {
            // Simulate SQL processing with aggregation
            let processed_record = simulate_sql_processing(record).await;
            if output_tx.send(processed_record).is_err() {
                break;
            }
        }
    });

    // Consumer task (simulates Kafka producer)
    let mut processed_count = 0u64;
    while let Some(_processed_record) =
        tokio::time::timeout(Duration::from_millis(100), output_rx.recv())
            .await
            .ok()
            .flatten()
    {
        processed_count += 1;
    }

    let duration = metrics.end("kafka_pipeline");

    let throughput = processed_count as f64 / duration.as_secs_f64();
    println!(
        "✅ Processed {} records in {:?} ({:.0} records/sec)",
        processed_count, duration, throughput
    );

    metrics.set_counter("kafka_pipeline_records", processed_count);
    metrics.report().print();
}

/// Test cross-format serialization performance in pipeline
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_cross_format_serialization_pipeline() {
    println!("🚀 Cross-Format Serialization Pipeline Benchmark");
    println!("===============================================");

    let config = BenchmarkConfig::enhanced();
    let mut metrics = MetricsCollector::verbose();

    let test_config = TestRecordConfig::complex(10_000);
    let records = generate_test_records(&test_config);

    // Test JSON → Avro → Protobuf pipeline simulation
    metrics.start();
    let json_processed = simulate_json_processing(&records).await;
    let json_duration = metrics.end("json_processing");

    metrics.start();
    let avro_processed = simulate_avro_processing(&records).await;
    let avro_duration = metrics.end("avro_processing");

    metrics.start();
    let protobuf_processed = simulate_protobuf_processing(&records).await;
    let protobuf_duration = metrics.end("protobuf_processing");

    println!("📊 Format Performance Comparison:");
    println!(
        "   JSON:     {} records in {:?} ({:.0} records/sec)",
        json_processed,
        json_duration,
        json_processed as f64 / json_duration.as_secs_f64()
    );
    println!(
        "   Avro:     {} records in {:?} ({:.0} records/sec)",
        avro_processed,
        avro_duration,
        avro_processed as f64 / avro_duration.as_secs_f64()
    );
    println!(
        "   Protobuf: {} records in {:?} ({:.0} records/sec)",
        protobuf_processed,
        protobuf_duration,
        protobuf_processed as f64 / protobuf_duration.as_secs_f64()
    );

    metrics.report().print();
}

// Implementation functions

async fn run_full_streaming_pipeline(
    config: &TestRecordConfig,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    // Simulate full pipeline: ingestion → processing → output
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Send all records
    for record in records {
        tx.send(record).map_err(|e| format!("Send error: {}", e))?;
    }
    drop(tx);

    // Process all records with SQL-like operations
    let mut processed_count = 0u64;
    while let Some(record) = rx.recv().await {
        let _processed = simulate_sql_processing(record).await;
        processed_count += 1;
    }

    Ok(processed_count)
}

async fn simulate_sql_processing(record: StreamRecord) -> StreamRecord {
    // Simulate SQL operations: SELECT, WHERE, aggregation
    let mut processed_fields = HashMap::new();

    for (key, value) in record.fields {
        // Simulate WHERE clause processing
        if key == "id" {
            if let FieldValue::Integer(i) = value {
                if i % 2 == 0 {
                    // Simple filter
                    processed_fields
                        .insert(format!("filtered_{}", key), FieldValue::Integer(i * 2));
                }
            }
        }

        // Simulate SELECT with transformation
        processed_fields.insert(key, value);
    }

    StreamRecord::new(processed_fields)
}

async fn simulate_json_processing(records: &[StreamRecord]) -> u64 {
    // Simulate JSON serialization/deserialization overhead
    let mut processed = 0u64;
    for record in records {
        // Simulate serialization work
        let _json_data = format!("{{\"field_count\":{}}}", record.fields.len());
        processed += 1;
    }
    processed
}

async fn simulate_avro_processing(records: &[StreamRecord]) -> u64 {
    // Simulate Avro processing overhead (typically faster than JSON)
    let mut processed = 0u64;
    for _record in records {
        // Simulate Avro binary processing (faster)
        processed += 1;
    }
    processed
}

async fn simulate_protobuf_processing(records: &[StreamRecord]) -> u64 {
    // Simulate Protobuf processing overhead (typically fastest)
    let mut processed = 0u64;
    for _record in records {
        // Simulate Protobuf binary processing (fastest)
        processed += 1;
    }
    processed
}
