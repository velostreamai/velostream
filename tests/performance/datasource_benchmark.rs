/*!
# Data Source Performance Benchmarks

This benchmark compares performance between:
- Direct Kafka operations (baseline)
- Kafka through pluggable abstraction layer
- Schema discovery operations
- URI parsing and configuration

Goal: Ensure zero performance regression with new abstraction layer.
*/

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ferrisstreams::ferris::kafka::{JsonSerializer, KafkaConsumer, KafkaProducer};
use ferrisstreams::ferris::sql::config::ConnectionString;
use ferrisstreams::ferris::sql::datasource::{create_source, create_sink};
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;
use std::time::Duration;

// Helper function to create test records
fn create_test_record(id: u64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id as i64));
    fields.insert("price".to_string(), FieldValue::ScaledInteger(123456, 4)); // $12.3456
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("volume".to_string(), FieldValue::Integer(1000));
    
    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: id,
        partition: 0,
        headers: HashMap::new(),
    }
}

// Benchmark direct Kafka operations (baseline)
fn bench_kafka_direct_creation(c: &mut Criterion) {
    c.bench_function("kafka_direct_consumer_creation", |b| {
        b.iter(|| {
            // Simulate consumer creation cost
            let _consumer = KafkaConsumer::new(
                black_box("localhost:9092"),
                black_box("test-group"),
                JsonSerializer,
                JsonSerializer,
            );
        });
    });
}

// Benchmark Kafka through abstraction layer
fn bench_kafka_abstraction_creation(c: &mut Criterion) {
    c.bench_function("kafka_abstraction_source_creation", |b| {
        b.iter(|| {
            // Simulate abstraction layer creation cost
            let uri = black_box("kafka://localhost:9092/test-topic?group_id=test-group");
            let _source = create_source(uri);
        });
    });
}

// Benchmark URI parsing performance
fn bench_uri_parsing(c: &mut Criterion) {
    let uris = vec![
        "kafka://localhost:9092/topic?group_id=consumer",
        "kafka://broker1:9092,broker2:9092/topic?group_id=consumer&auto_offset_reset=earliest",
        "file:///data/input.csv?format=csv&header=true&delimiter=,",
        "s3://bucket/path/data.parquet?region=us-west-2&access_key=key",
        "postgresql://user:pass@localhost:5432/db?table=orders&pool_size=10",
    ];

    c.bench_function("uri_parsing_various_schemes", |b| {
        b.iter(|| {
            for uri in &uris {
                let _parsed = ConnectionString::parse(black_box(uri));
            }
        });
    });
}

// Benchmark configuration validation
fn bench_config_validation(c: &mut Criterion) {
    c.bench_function("config_validation", |b| {
        b.iter(|| {
            let uri = black_box("kafka://localhost:9092/topic?group_id=test&batch_size=1000&timeout_ms=5000");
            if let Ok(connection) = ConnectionString::parse(uri) {
                // Simulate validation cost
                let _is_valid = connection.scheme() == "kafka" && 
                               connection.hosts().len() > 0 &&
                               connection.path().is_some();
            }
        });
    });
}

// Benchmark record transformation (core hot path)
fn bench_record_transformation(c: &mut Criterion) {
    let records: Vec<_> = (0..1000).map(create_test_record).collect();
    
    c.bench_function("record_transformation_batch", |b| {
        b.iter(|| {
            let transformed: Vec<_> = records
                .iter()
                .map(|record| {
                    let mut new_record = record.clone();
                    // Simulate typical transformation: add processing metadata
                    new_record.fields.insert(
                        "processed_at".to_string(),
                        FieldValue::Integer(chrono::Utc::now().timestamp_millis())
                    );
                    new_record.fields.insert(
                        "processor_id".to_string(),
                        FieldValue::String("benchmark-processor".to_string())
                    );
                    black_box(new_record)
                })
                .collect();
            black_box(transformed);
        });
    });
}

// Benchmark schema discovery simulation
fn bench_schema_discovery(c: &mut Criterion) {
    let sample_records: Vec<_> = (0..100).map(create_test_record).collect();
    
    c.bench_function("schema_discovery_simulation", |b| {
        b.iter(|| {
            let mut field_types = HashMap::new();
            
            for record in &sample_records {
                for (field_name, field_value) in &record.fields {
                    let field_type = match field_value {
                        FieldValue::Integer(_) => "integer",
                        FieldValue::Float(_) => "float", 
                        FieldValue::String(_) => "string",
                        FieldValue::Boolean(_) => "boolean",
                        FieldValue::ScaledInteger(_, _) => "decimal",
                        FieldValue::Null => "null",
                    };
                    field_types.insert(field_name.clone(), field_type);
                }
            }
            
            black_box(field_types);
        });
    });
}

// Benchmark memory allocation patterns
fn bench_memory_allocation(c: &mut Criterion) {
    c.bench_function("record_creation_allocation", |b| {
        b.iter(|| {
            // Simulate typical record creation pattern
            let records: Vec<_> = (0..100)
                .map(|i| {
                    black_box(create_test_record(i))
                })
                .collect();
            black_box(records);
        });
    });
}

// Benchmark trait dispatch overhead
fn bench_trait_dispatch(c: &mut Criterion) {
    // This would require actual DataReader implementations
    // For now, simulate the overhead with function pointers
    
    let direct_fn = |value: i64| -> i64 { value * 2 };
    let trait_fn: Box<dyn Fn(i64) -> i64> = Box::new(|value: i64| -> i64 { value * 2 });
    
    c.bench_function("direct_function_call", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for i in 0..1000 {
                sum += direct_fn(black_box(i));
            }
            black_box(sum);
        });
    });
    
    c.bench_function("trait_object_dispatch", |b| {
        b.iter(|| {
            let mut sum = 0i64;
            for i in 0..1000 {
                sum += trait_fn(black_box(i));
            }
            black_box(sum);
        });
    });
}

// Comprehensive benchmark suite
criterion_group!(
    datasource_benchmarks,
    bench_kafka_direct_creation,
    bench_kafka_abstraction_creation,
    bench_uri_parsing,
    bench_config_validation,
    bench_record_transformation,
    bench_schema_discovery,
    bench_memory_allocation,
    bench_trait_dispatch
);

criterion_main!(datasource_benchmarks);

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_benchmark_setup() {
        // Verify benchmark helper functions work correctly
        let record = create_test_record(123);
        assert_eq!(record.offset, 123);
        assert_eq!(record.fields.len(), 4);
    }
    
    #[test]
    fn test_uri_parsing_performance() {
        // Verify URI parsing doesn't fail
        let uri = "kafka://localhost:9092/topic?group_id=test";
        let parsed = ConnectionString::parse(uri).unwrap();
        assert_eq!(parsed.scheme(), "kafka");
    }
    
    #[test]
    fn test_record_transformation_correctness() {
        let record = create_test_record(1);
        let original_field_count = record.fields.len();
        
        // Simulate the transformation from benchmark
        let mut transformed = record.clone();
        transformed.fields.insert(
            "processed_at".to_string(),
            FieldValue::Integer(chrono::Utc::now().timestamp_millis())
        );
        transformed.fields.insert(
            "processor_id".to_string(),
            FieldValue::String("benchmark-processor".to_string())
        );
        
        assert_eq!(transformed.fields.len(), original_field_count + 2);
    }
}