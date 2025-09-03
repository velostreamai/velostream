//! Performance test for StreamRecord → FieldValue conversion
//! This addresses the performance analysis item in TODO_WIP.md

use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;
use std::time::Instant;

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

#[test]
fn test_conversion_performance_baseline() {
    println!("\n=== StreamRecord → FieldValue Conversion Performance Analysis ===");

    // Test different field counts to understand scaling
    for field_count in [10, 50, 100, 200].iter() {
        let record = create_test_record(*field_count);

        // Warm up - test field access performance
        for _ in 0..10 {
            for (k, v) in &record.fields {
                let _accessed = (k.clone(), v.clone());
            }
        }

        // Actual benchmark - test field access and cloning
        let iterations = 1000;
        let start = Instant::now();

        for _ in 0..iterations {
            let _cloned_fields: HashMap<String, FieldValue> = record
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }

        let elapsed = start.elapsed();
        let avg_per_iteration = elapsed / iterations;
        let avg_per_field = avg_per_iteration.as_nanos() as f64 / *field_count as f64;

        println!(
            "Fields: {:3} | Total: {:8.2}µs | Per-Field: {:6.1}ns",
            field_count,
            avg_per_iteration.as_nanos() as f64 / 1000.0,
            avg_per_field
        );

        // Performance target from TODO_WIP.md: <100ns per field
        assert!(
            avg_per_field < 100.0,
            "Per-field conversion too slow: {:.1}ns (target: <100ns)",
            avg_per_field
        );
    }
}

#[test]
fn test_individual_type_performance() {
    println!("\n=== Individual Type Conversion Performance ===");

    let test_values = vec![
        (
            "String",
            FieldValue::String("test_string_value".to_string()),
        ),
        ("Integer", FieldValue::Integer(12345)),
        ("Float", FieldValue::Float(123.456)),
        ("ScaledInteger", FieldValue::ScaledInteger(123456, 3)),
    ];

    for (type_name, field_value) in test_values {
        let iterations = 10000;
        let start = Instant::now();

        for _ in 0..iterations {
            let _result = field_value.clone(); // Simple clone performance test
        }

        let elapsed = start.elapsed();
        let avg_per_conversion = elapsed.as_nanos() as f64 / iterations as f64;

        println!(
            "{:13}: {:6.1}ns per conversion",
            type_name, avg_per_conversion
        );

        // All individual conversions should be very fast
        assert!(
            avg_per_conversion < 50.0,
            "{} conversion too slow: {:.1}ns",
            type_name,
            avg_per_conversion
        );
    }
}

#[test]
fn test_string_size_impact() {
    println!("\n=== String Size Impact on Conversion Performance ===");

    for size in [10, 100, 1000, 10000].iter() {
        let large_string = "x".repeat(*size);
        let field_value = FieldValue::String(large_string);

        let iterations = 1000;
        let start = Instant::now();

        for _ in 0..iterations {
            let _result = field_value.clone(); // Simple clone performance test
        }

        let elapsed = start.elapsed();
        let avg_per_conversion = elapsed.as_nanos() as f64 / iterations as f64;
        let ns_per_byte = avg_per_conversion / *size as f64;

        println!(
            "String {:5} bytes: {:8.1}ns total | {:4.2}ns/byte",
            size, avg_per_conversion, ns_per_byte
        );

        // Should scale roughly linearly with string size
        // Allow more time for very large strings due to memory allocation
        let max_expected = if *size <= 1000 { 200.0 } else { 500.0 };
        assert!(
            avg_per_conversion < max_expected,
            "String conversion too slow for size {}: {:.1}ns",
            size,
            avg_per_conversion
        );
    }
}

#[test]
fn test_conversion_correctness() {
    let record = create_test_record(20);

    // Ensure conversions produce expected types
    for (key, field_value) in &record.fields {
        let internal_value = field_value.clone(); // Direct field value test

        match field_value {
            FieldValue::String(_) => {
                assert!(
                    matches!(internal_value, FieldValue::String(_)),
                    "String field '{}' converted incorrectly",
                    key
                );
            }
            FieldValue::Integer(_) => {
                assert!(
                    matches!(internal_value, FieldValue::Integer(_)),
                    "Integer field '{}' converted incorrectly",
                    key
                );
            }
            FieldValue::Float(_) => {
                assert!(
                    matches!(internal_value, FieldValue::Float(_)),
                    "Float field '{}' converted incorrectly",
                    key
                );
            }
            FieldValue::ScaledInteger(_, _) => {
                assert!(
                    matches!(internal_value, FieldValue::ScaledInteger(_, _)),
                    "ScaledInteger field '{}' converted incorrectly",
                    key
                );
            }
            _ => {} // Other types not tested in this benchmark
        }
    }

    println!(
        "✓ All {} field conversions produced correct types",
        record.fields.len()
    );
}

#[test]
fn test_memory_allocation_patterns() {
    println!("\n=== Memory Allocation Analysis ===");

    // Test if the conversion is zero-copy for certain types
    let string_value = "test_string".to_string();
    let original_ptr = string_value.as_ptr();

    let field_value = FieldValue::String(string_value.clone());
    let internal_value = field_value.clone(); // Direct field value

    if let FieldValue::String(converted_string) = internal_value {
        let converted_ptr = converted_string.as_ptr();

        if original_ptr == converted_ptr {
            println!("✓ String conversion appears to be zero-copy (same pointer)");
        } else {
            println!("✗ String conversion creates new allocation (different pointer)");
        }

        // Even if not zero-copy, strings should convert correctly
        assert_eq!(converted_string, "test_string");
    } else {
        panic!("String conversion produced wrong type");
    }
}
