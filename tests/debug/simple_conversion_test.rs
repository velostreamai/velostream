//! Simple test to verify StreamRecord → InternalValue conversion performance
//! This addresses the performance analysis item in TODO_WIP.md

use ferrisstreams::ferris::serialization::InternalValue;
use ferrisstreams::ferris::sql::execution::{
    types::{FieldValue, StreamRecord},
    utils::FieldValueConverter,
};
use std::collections::HashMap;
use std::time::Instant;

fn create_test_record(field_count: usize) -> StreamRecord {
    let mut fields = HashMap::new();

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

fn main() {
    println!("=== StreamRecord → InternalValue Conversion Performance Test ===\n");

    // Test different field counts to understand scaling
    for field_count in [10, 50, 100, 200].iter() {
        let record = create_test_record(*field_count);

        // Warm up
        for _ in 0..10 {
            let _: HashMap<String, InternalValue> = record
                .fields
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        FieldValueConverter::field_value_to_internal(v.clone()),
                    )
                })
                .collect();
        }

        // Actual benchmark
        let iterations = 1000;
        let start = Instant::now();

        for _ in 0..iterations {
            let _converted: HashMap<String, InternalValue> = record
                .fields
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        FieldValueConverter::field_value_to_internal(v.clone()),
                    )
                })
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

        // Check if it meets performance target from TODO_WIP.md: <100ns per field
        let target_met = if avg_per_field < 100.0 {
            "✓ PASS"
        } else {
            "✗ SLOW"
        };
        println!("  Target: <100ns per field | Result: {}\n", target_met);
    }

    println!("=== Individual Type Conversion Performance ===\n");

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
            let _result = FieldValueConverter::field_value_to_internal(field_value.clone());
        }

        let elapsed = start.elapsed();
        let avg_per_conversion = elapsed.as_nanos() as f64 / iterations as f64;

        println!(
            "{:13}: {:6.1}ns per conversion",
            type_name, avg_per_conversion
        );
    }

    // Test correctness
    println!("\n=== Conversion Correctness Verification ===");
    let record = create_test_record(20);
    let mut correct_conversions = 0;

    for (key, field_value) in &record.fields {
        let internal_value = FieldValueConverter::field_value_to_internal(field_value.clone());

        let correct = match field_value {
            FieldValue::String(_) => matches!(internal_value, InternalValue::String(_)),
            FieldValue::Integer(_) => matches!(internal_value, InternalValue::Integer(_)),
            FieldValue::Float(_) => matches!(internal_value, InternalValue::Number(_)),
            FieldValue::ScaledInteger(_, _) => {
                matches!(internal_value, InternalValue::ScaledNumber(_, _))
            }
            _ => false,
        };

        if correct {
            correct_conversions += 1;
        }
    }

    println!(
        "✓ {}/{} field conversions produced correct types",
        correct_conversions,
        record.fields.len()
    );
    println!("\n=== Performance Analysis Complete ===");
}
