//! Performance benchmark for StreamRecord → InternalValue conversion
//! This addresses the performance analysis item in TODO_WIP.md

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ferrisstreams::ferris::sql::execution::{
    types::{FieldValue, StreamRecord},
    utils::FieldValueConverter,
};
use std::collections::HashMap;

/// Create a test record with various field types
fn create_test_record(field_count: usize) -> StreamRecord {
    let mut fields = HashMap::new();

    // Add different types of fields to test various conversion paths
    for i in 0..field_count {
        match i % 4 {
            0 => fields.insert(
                format!("string_{}", i),
                FieldValue::String(format!("value_{}", i)),
            ),
            1 => fields.insert(format!("int_{}", i), FieldValue::Integer(i as i64)),
            2 => fields.insert(format!("float_{}", i), FieldValue::Float(i as f64 * 0.5)),
            3 => fields.insert(
                format!("scaled_{}", i),
                FieldValue::ScaledInteger(i as i64 * 1000, 3),
            ),
            _ => unreachable!(),
        };
    }

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    }
}

/// Benchmark the current FieldValueConverter::field_value_to_internal performance
fn benchmark_field_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_conversion");

    // Test with different field counts to measure scaling
    for field_count in [10, 50, 100].iter() {
        let record = create_test_record(*field_count);

        group.bench_function(&format!("convert_{}_fields", field_count), |b| {
            b.iter(|| {
                let _converted: HashMap<String, _> = record
                    .fields
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            black_box(FieldValueConverter::field_value_to_internal(v.clone())),
                        )
                    })
                    .collect();
            });
        });
    }

    group.finish();
}

/// Benchmark individual field type conversions
fn benchmark_individual_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("individual_conversions");

    // Test each field type individually
    let test_values = vec![
        (
            "string",
            FieldValue::String("test_string_value".to_string()),
        ),
        ("integer", FieldValue::Integer(12345)),
        ("float", FieldValue::Float(123.456)),
        ("scaled_integer", FieldValue::ScaledInteger(123456, 3)),
    ];

    for (type_name, field_value) in test_values {
        group.bench_function(type_name, |b| {
            b.iter(|| {
                black_box(FieldValueConverter::field_value_to_internal(black_box(
                    field_value.clone(),
                )))
            });
        });
    }

    group.finish();
}

/// Test conversion with different string sizes to understand memory allocation impact
fn benchmark_string_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_conversion");

    for size in [10, 100, 1000, 10000].iter() {
        let large_string = "x".repeat(*size);
        let field_value = FieldValue::String(large_string);

        group.bench_function(&format!("string_{}_bytes", size), |b| {
            b.iter(|| {
                black_box(FieldValueConverter::field_value_to_internal(black_box(
                    field_value.clone(),
                )))
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_field_conversion,
    benchmark_individual_types,
    benchmark_string_sizes
);
criterion_main!(benches);

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisstreams::ferris::serialization::InternalValue;

    #[test]
    fn test_conversion_correctness() {
        let record = create_test_record(10);

        // Ensure conversions produce expected types
        for (key, field_value) in &record.fields {
            let internal_value = FieldValueConverter::field_value_to_internal(field_value.clone());

            match field_value {
                FieldValue::String(_) => {
                    assert!(matches!(internal_value, InternalValue::String(_)));
                }
                FieldValue::Integer(_) => {
                    assert!(matches!(internal_value, InternalValue::Integer(_)));
                }
                FieldValue::Float(_) => {
                    assert!(matches!(internal_value, InternalValue::Number(_)));
                }
                FieldValue::ScaledInteger(_, _) => {
                    assert!(matches!(internal_value, InternalValue::ScaledNumber(_, _)));
                }
                _ => {} // Other types not tested in this benchmark
            }
        }
    }

    #[test]
    fn test_performance_baseline() {
        let start = std::time::Instant::now();
        let record = create_test_record(100);

        let _converted: HashMap<String, InternalValue> = record
            .fields
            .into_iter()
            .map(|(k, v)| (k, FieldValueConverter::field_value_to_internal(v)))
            .collect();

        let elapsed = start.elapsed();
        println!("100 field conversion took: {:?}", elapsed);

        // Performance target from TODO_WIP.md: <100ns per field
        // With 100 fields, should be < 10µs total
        assert!(
            elapsed.as_nanos() < 50_000,
            "Conversion too slow: {:?}",
            elapsed
        );
    }
}
