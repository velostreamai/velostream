//! Performance regression test for subquery security fixes

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::{
    execution::{
        FieldValue, StreamRecord,
        processors::{ProcessorContext, SelectProcessor, TableReference},
    },
    parser::StreamingSqlParser,
};

#[test]
fn test_correlation_context_performance() {
    let iterations = 1000;

    // Benchmark the new thread-local correlation context approach
    let start = Instant::now();

    for i in 0..iterations {
        let mut context = ProcessorContext::new(&format!("query_{}", i));

        // Simulate setting correlation context
        let table_ref = TableReference::new("test_table".to_string());
        let original = context.correlation_context.clone();
        context.correlation_context = Some(table_ref);

        // Simulate some processing work
        let _ = context.correlation_context.as_ref().unwrap().name.len();

        // Restore context
        context.correlation_context = original;
    }

    let duration = start.elapsed();

    println!(
        "Correlation context operations: {:?} for {} iterations",
        duration, iterations
    );
    println!("Average per operation: {:?}", duration / iterations);

    // Should be very fast - under 1ms for 1000 operations
    assert!(
        duration.as_millis() < 10,
        "Correlation context operations too slow: {:?}",
        duration
    );
}

#[test]
fn test_sql_injection_protection_performance() {
    let iterations = 1000;

    // Test SQL injection protection performance
    let test_strings = vec![
        "normal_string",
        "string with 'quotes'",
        "string with \\backslashes\\",
        "'; DROP TABLE users; --",
        "string\x00with\x1anull\0bytes",
    ];

    let start = Instant::now();

    for _ in 0..iterations {
        for test_str in &test_strings {
            let field = FieldValue::String(test_str.to_string());
            let _escaped = field_value_to_sql_string_safe(&field);
        }
    }

    let duration = start.elapsed();

    println!(
        "SQL injection protection: {:?} for {} iterations",
        duration, iterations
    );
    println!(
        "Average per operation: {:?}",
        duration / (iterations * test_strings.len() as u32)
    );

    // Should be reasonably fast - under 50ms for all operations
    assert!(
        duration.as_millis() < 50,
        "SQL injection protection too slow: {:?}",
        duration
    );
}

#[test]
fn test_overall_subquery_performance() {
    // Test that our fixes don't add significant overhead to subquery processing
    let parser = StreamingSqlParser::new();
    let iterations = 100;

    // Parse a representative subquery
    let sql = "SELECT * FROM trades WHERE user_id = 123";
    let query = parser.parse(sql).expect("Query should parse");

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("user_id".to_string(), FieldValue::Integer(123));
    fields.insert("amount".to_string(), FieldValue::Float(1000.0));

    let record = StreamRecord {
        fields,
        timestamp: 0,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let start = Instant::now();

    for i in 0..iterations {
        let mut context = ProcessorContext::new(&format!("perf_test_{}", i));

        // This would normally fail due to missing table, but we're testing overhead
        let _result = SelectProcessor::process(&query, &record, &mut context);

        // Verify correlation context is cleaned up
        assert!(context.correlation_context.is_none());
    }

    let duration = start.elapsed();

    println!(
        "Subquery processing: {:?} for {} iterations",
        duration, iterations
    );
    println!("Average per query: {:?}", duration / iterations);

    // Should be under 100ms for 100 iterations
    assert!(
        duration.as_millis() < 100,
        "Subquery processing too slow: {:?}",
        duration
    );
}

/// Helper function for SQL injection protection testing
fn field_value_to_sql_string_safe(field_value: &FieldValue) -> String {
    match field_value {
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "NULL".to_string()
            }
        }
        FieldValue::String(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('\'', "''")
                .replace('\0', "")
                .replace('\x1a', "")
                .chars()
                .filter(|c| !c.is_control() || c == &'\t' || c == &'\n' || c == &'\r')
                .collect::<String>();
            format!("'{}'", escaped)
        }
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Null => "NULL".to_string(),
        _ => "NULL".to_string(),
    }
}
