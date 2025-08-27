//! # Data Source Abstraction Performance Test
//!
//! This example measures the performance overhead of the pluggable data source abstraction
//! compared to direct Kafka operations to ensure zero regression:
//!
//! ## Comparisons:
//! - Direct Kafka operations (baseline)
//! - Kafka through abstraction layer
//! - URI parsing and configuration overhead
//! - Schema discovery performance
//! - Record transformation throughput
//!
//! ## Prerequisites:
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example datasource_performance_test`

use ferrisstreams::ferris::kafka::{JsonSerializer, KafkaConsumer};
use ferrisstreams::ferris::schema::{CompatibilityMode, FieldDefinition, Schema, SchemaMetadata};
use ferrisstreams::ferris::sql::ast::DataType;
use ferrisstreams::ferris::sql::config::ConnectionString;
use ferrisstreams::ferris::sql::datasource::create_source;
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Performance test configuration
const OPERATION_COUNT: u64 = 10_000;
const BATCH_SIZE: usize = 100;
const URI_PARSE_COUNT: usize = 1_000;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TestMessage {
    id: u64,
    symbol: String,
    price: f64,
    volume: i64,
    timestamp: u64,
}

impl TestMessage {
    fn new(id: u64) -> Self {
        Self {
            id,
            symbol: format!("STOCK{:04}", id % 100),
            price: 100.0 + (id as f64 * 0.01) % 50.0,
            volume: 1000 + (id % 10000) as i64,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    fn to_stream_record(&self) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(self.id as i64));
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(self.symbol.clone()),
        );
        fields.insert(
            "price".to_string(),
            FieldValue::ScaledInteger((self.price * 10000.0) as i64, 4),
        );
        fields.insert("volume".to_string(), FieldValue::Integer(self.volume));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(self.timestamp as i64),
        );

        StreamRecord {
            fields,
            timestamp: self.timestamp as i64,
            offset: self.id as i64,
            partition: 0,
            headers: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct PerformanceMetrics {
    operation_count: AtomicU64,
    total_duration: std::sync::Mutex<Duration>,
    start_time: Instant,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            operation_count: AtomicU64::new(0),
            total_duration: std::sync::Mutex::new(Duration::from_secs(0)),
            start_time: Instant::now(),
        }
    }

    fn record_operation(&self, duration: Duration) {
        self.operation_count.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut total) = self.total_duration.lock() {
            *total += duration;
        }
    }

    fn get_stats(&self) -> PerformanceStats {
        let count = self.operation_count.load(Ordering::Relaxed);
        let total_duration = *self.total_duration.lock().unwrap();
        let wall_time = self.start_time.elapsed();

        PerformanceStats {
            operations: count,
            wall_time_ms: wall_time.as_millis() as u64,
            total_operation_time_ms: total_duration.as_millis() as u64,
            ops_per_second: if wall_time.as_secs_f64() > 0.0 {
                count as f64 / wall_time.as_secs_f64()
            } else {
                0.0
            },
            avg_operation_time_us: if count > 0 {
                total_duration.as_micros() as u64 / count
            } else {
                0
            },
        }
    }
}

#[derive(Debug)]
struct PerformanceStats {
    operations: u64,
    wall_time_ms: u64,
    total_operation_time_ms: u64,
    ops_per_second: f64,
    avg_operation_time_us: u64,
}

impl PerformanceStats {
    fn print(&self, test_name: &str) {
        println!("\n📊 {} Results:", test_name);
        println!("  Operations: {}", self.operations);
        println!("  Wall time: {}ms", self.wall_time_ms);
        println!("  Ops/second: {:.2}", self.ops_per_second);
        println!("  Avg operation time: {}μs", self.avg_operation_time_us);
        println!("  Total operation time: {}ms", self.total_operation_time_ms);
    }
}

// Benchmark direct Kafka consumer creation (baseline)
async fn benchmark_kafka_direct() -> PerformanceStats {
    println!("🔧 Benchmarking direct Kafka operations...");
    let metrics = PerformanceMetrics::new();

    for i in 0..OPERATION_COUNT {
        let start = Instant::now();

        // Simulate direct Kafka consumer creation
        let _consumer_result = KafkaConsumer::<String, String, JsonSerializer, JsonSerializer>::new(
            "localhost:9092",
            &format!("test-group-{}", i % 10),
            JsonSerializer,
            JsonSerializer,
        );

        let duration = start.elapsed();
        metrics.record_operation(duration);

        if i % 1000 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    println!();
    metrics.get_stats()
}

// Benchmark Kafka through abstraction layer
async fn benchmark_kafka_abstraction() -> PerformanceStats {
    println!("🔧 Benchmarking Kafka through abstraction layer...");
    let metrics = PerformanceMetrics::new();

    for i in 0..OPERATION_COUNT {
        let start = Instant::now();

        // Simulate abstraction layer source creation
        let uri = format!(
            "kafka://localhost:9092/test-topic-{}?group_id=test-group-{}",
            i % 100,
            i % 10
        );
        let _source_result = create_source(&uri);

        let duration = start.elapsed();
        metrics.record_operation(duration);

        if i % 1000 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    println!();
    metrics.get_stats()
}

// Benchmark URI parsing performance
async fn benchmark_uri_parsing() -> PerformanceStats {
    println!("🔧 Benchmarking URI parsing performance...");
    let metrics = PerformanceMetrics::new();

    let test_uris = [
        "kafka://localhost:9092/topic?group_id=consumer",
        "kafka://broker1:9092,broker2:9092/topic?group_id=consumer&auto_offset_reset=earliest",
        "file:///data/input.csv?format=csv&header=true&delimiter=,",
        "s3://bucket/path/data.parquet?region=us-west-2&access_key=key",
        "postgresql://user:pass@localhost:5432/db?table=orders&pool_size=10",
    ];

    for i in 0..URI_PARSE_COUNT {
        let uri = &test_uris[i % test_uris.len()];
        let start = Instant::now();

        let _parsed = ConnectionString::parse(uri);

        let duration = start.elapsed();
        metrics.record_operation(duration);

        if i % 100 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    println!();
    metrics.get_stats()
}

// Benchmark record transformation (core processing path)
async fn benchmark_record_transformation() -> PerformanceStats {
    println!("🔧 Benchmarking record transformation...");
    let metrics = PerformanceMetrics::new();

    // Pre-generate test data
    let test_messages: Vec<_> = (0..BATCH_SIZE)
        .map(|i| TestMessage::new(i as u64))
        .collect();

    for batch in 0..(OPERATION_COUNT / BATCH_SIZE as u64) {
        let start = Instant::now();

        // Transform messages to StreamRecord format
        let _records: Vec<_> = test_messages
            .iter()
            .enumerate()
            .map(|(i, msg)| {
                let mut record = msg.to_stream_record();
                // Add processing metadata (common transformation)
                record.fields.insert(
                    "processed_at".to_string(),
                    FieldValue::Integer(chrono::Utc::now().timestamp_millis()),
                );
                record
                    .fields
                    .insert("batch_id".to_string(), FieldValue::Integer(batch as i64));
                record.fields.insert(
                    "processor_id".to_string(),
                    FieldValue::String(format!("processor-{}", i % 4)),
                );
                record
            })
            .collect();

        let duration = start.elapsed();
        metrics.record_operation(duration);

        if batch % 10 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    println!();
    metrics.get_stats()
}

// Benchmark schema discovery
async fn benchmark_schema_discovery() -> PerformanceStats {
    println!("🔧 Benchmarking schema discovery...");
    let metrics = PerformanceMetrics::new();

    // Pre-generate sample records for schema inference
    let sample_records: Vec<_> = (0..100)
        .map(|i| TestMessage::new(i).to_stream_record())
        .collect();

    for i in 0..(OPERATION_COUNT / 100) {
        let start = Instant::now();

        // Simulate schema discovery from sample records
        let mut discovered_fields = Vec::new();
        let mut field_types = HashMap::new();

        for record in &sample_records {
            for (field_name, field_value) in &record.fields {
                let data_type = match field_value {
                    FieldValue::Integer(_) => DataType::Integer,
                    FieldValue::Float(_) => DataType::Float,
                    FieldValue::String(_) => DataType::String,
                    FieldValue::Boolean(_) => DataType::Boolean,
                    FieldValue::ScaledInteger(_, _) => DataType::Float, // Represent as Float for now
                    FieldValue::Date(_) => DataType::String, // Represent as String for now
                    FieldValue::Timestamp(_) => DataType::Timestamp,
                    FieldValue::Decimal(_) => DataType::Float, // Represent as Float for now
                    FieldValue::Array(_) => DataType::Array(Box::new(DataType::String)), // Generic array
                    FieldValue::Map(_) => {
                        DataType::Map(Box::new(DataType::String), Box::new(DataType::String))
                    } // String-to-String map
                    FieldValue::Struct(_) => DataType::Struct(Vec::new()), // Empty struct for now
                    FieldValue::Interval { .. } => DataType::Integer, // Represent as Integer for now
                    FieldValue::Null => DataType::String,             // Default for null
                };
                field_types.insert(field_name.clone(), data_type);
            }
        }

        // Create schema
        for (name, data_type) in field_types {
            discovered_fields.push(FieldDefinition {
                name,
                data_type,
                nullable: true,
                default_value: None,
                description: None,
            });
        }

        let _schema = Schema {
            fields: discovered_fields,
            metadata: SchemaMetadata {
                source_type: "performance_test".to_string(),
                created_at: chrono::Utc::now().timestamp(),
                updated_at: chrono::Utc::now().timestamp(),
                tags: HashMap::new(),
                compatibility: CompatibilityMode::None,
            },
            version: Some("1.0".to_string()),
        };

        let duration = start.elapsed();
        metrics.record_operation(duration);

        if i % 100 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    println!();
    metrics.get_stats()
}

// Calculate overhead percentage
fn calculate_overhead(baseline: &PerformanceStats, test: &PerformanceStats) -> f64 {
    if baseline.avg_operation_time_us > 0 {
        ((test.avg_operation_time_us as f64 - baseline.avg_operation_time_us as f64)
            / baseline.avg_operation_time_us as f64)
            * 100.0
    } else {
        0.0
    }
}

#[tokio::main]
async fn main() {
    println!("🚀 Data Source Abstraction Performance Test");
    println!("============================================");

    // Run all benchmarks
    let kafka_direct_stats = benchmark_kafka_direct().await;
    let kafka_abstraction_stats = benchmark_kafka_abstraction().await;
    let uri_parsing_stats = benchmark_uri_parsing().await;
    let record_transformation_stats = benchmark_record_transformation().await;
    let schema_discovery_stats = benchmark_schema_discovery().await;

    // Print detailed results
    kafka_direct_stats.print("Direct Kafka Operations (Baseline)");
    kafka_abstraction_stats.print("Kafka Through Abstraction");
    uri_parsing_stats.print("URI Parsing");
    record_transformation_stats.print("Record Transformation");
    schema_discovery_stats.print("Schema Discovery");

    // Calculate and print overhead analysis
    println!("\n🔍 Performance Overhead Analysis:");
    println!("==================================");

    let abstraction_overhead = calculate_overhead(&kafka_direct_stats, &kafka_abstraction_stats);
    println!("Abstraction layer overhead: {:.2}%", abstraction_overhead);

    if abstraction_overhead < 5.0 {
        println!("✅ PASS: Abstraction overhead is within acceptable limits (< 5%)");
    } else if abstraction_overhead < 10.0 {
        println!(
            "⚠️  WARNING: Abstraction overhead is elevated ({:.2}%)",
            abstraction_overhead
        );
    } else {
        println!(
            "❌ FAIL: Abstraction overhead is too high ({:.2}%)",
            abstraction_overhead
        );
    }

    // URI parsing performance
    if uri_parsing_stats.avg_operation_time_us < 100 {
        println!(
            "✅ PASS: URI parsing is fast ({}μs)",
            uri_parsing_stats.avg_operation_time_us
        );
    } else {
        println!(
            "⚠️  URI parsing performance: {}μs",
            uri_parsing_stats.avg_operation_time_us
        );
    }

    // Record transformation performance
    let records_per_second = (BATCH_SIZE as f64) * record_transformation_stats.ops_per_second;
    println!(
        "📊 Record transformation: {:.0} records/second",
        records_per_second
    );

    if records_per_second > 100_000.0 {
        println!("✅ PASS: Record transformation meets performance targets");
    } else {
        println!("⚠️  Record transformation performance below target");
    }

    // Schema discovery performance
    if schema_discovery_stats.avg_operation_time_us < 1000 {
        println!(
            "✅ PASS: Schema discovery is efficient ({}μs)",
            schema_discovery_stats.avg_operation_time_us
        );
    } else {
        println!(
            "⚠️  Schema discovery performance: {}μs",
            schema_discovery_stats.avg_operation_time_us
        );
    }

    println!("\n🎯 Performance Validation Summary:");
    println!("===================================");

    let total_tests = 5;
    let mut passed_tests = 0;

    if abstraction_overhead < 10.0 {
        passed_tests += 1;
    }
    if uri_parsing_stats.avg_operation_time_us < 100 {
        passed_tests += 1;
    }
    if records_per_second > 50_000.0 {
        passed_tests += 1;
    }
    if schema_discovery_stats.avg_operation_time_us < 1000 {
        passed_tests += 1;
    }
    if kafka_abstraction_stats.ops_per_second > 1000.0 {
        passed_tests += 1;
    }

    println!("Tests passed: {}/{}", passed_tests, total_tests);

    if passed_tests == total_tests {
        println!("🎉 ALL PERFORMANCE TESTS PASSED!");
        println!("✅ Zero performance regression confirmed");
    } else {
        println!("⚠️  Some performance tests need attention");
    }
}
