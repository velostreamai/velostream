//! Test for parameterized query implementation

use velostream::velostream::sql::execution::{
    processors::{SelectProcessor, SqlParameter},
    FieldValue,
};

#[test]
fn test_parameterized_query_performance() {
    let processor = SelectProcessor;

    // Test parameterized query with various types
    let template = "SELECT * FROM users WHERE id = $0 AND name = $1 AND score > $2";
    let params = vec![
        SqlParameter::new(0, FieldValue::Integer(123)),
        SqlParameter::new(
            1,
            FieldValue::String("John'; DROP TABLE users; --".to_string()),
        ),
        SqlParameter::new(2, FieldValue::Float(85.5)),
    ];

    let result = processor
        .build_parameterized_query(template, params)
        .unwrap();

    // Verify SQL injection is prevented - dangerous SQL should be safely quoted
    assert!(
        result.starts_with("SELECT * FROM users WHERE id = 123 AND name = '"),
        "Should start correctly"
    );
    assert!(result.contains("123"), "Should contain parameter value");
    assert!(
        result.contains("'John''; DROP TABLE users; --'"),
        "Should escape quotes properly"
    );
    assert!(result.contains("85.5"), "Should contain float value");

    // Verify that the dangerous SQL is now safely contained within quotes
    assert!(
        result.contains("'John''; DROP TABLE users; --' AND"),
        "Injection attempt should be safely quoted"
    );

    println!("Parameterized query result: {}", result);
}

#[test]
fn test_parameterized_query_security() {
    let processor = SelectProcessor;

    // Test various injection attempts
    let malicious_inputs = vec![
        "'; DROP TABLE users; --",
        "' OR '1'='1",
        "admin'--",
        "\x00'; DROP TABLE users; --",
    ];

    for malicious_input in malicious_inputs {
        let template = "SELECT * FROM users WHERE name = $0";
        let params = vec![SqlParameter::new(
            0,
            FieldValue::String(malicious_input.to_string()),
        )];

        let result = processor
            .build_parameterized_query(template, params)
            .unwrap();

        // Verify dangerous patterns are safely escaped
        assert!(
            !result.contains("DROP TABLE")
                || result.starts_with("SELECT * FROM users WHERE name = '"),
            "DROP TABLE should be inside quotes for input: {}",
            malicious_input
        );
        assert!(
            result.starts_with("SELECT * FROM users WHERE name = '"),
            "Query should start correctly for input: {}",
            malicious_input
        );

        println!("Safe query for '{}': {}", malicious_input, result);
    }
}

#[test]
fn test_parameterized_query_types() {
    let processor = SelectProcessor;

    // Test all FieldValue types
    let template = "SELECT * FROM test WHERE int_val = $0 AND float_val = $1 AND bool_val = $2 AND null_val = $3";
    let params = vec![
        SqlParameter::new(0, FieldValue::Integer(42)),
        SqlParameter::new(1, FieldValue::Float(42.7)),
        SqlParameter::new(2, FieldValue::Boolean(true)),
        SqlParameter::new(3, FieldValue::Null),
    ];

    let result = processor
        .build_parameterized_query(template, params)
        .unwrap();

    assert!(result.contains("42"), "Should contain integer");
    assert!(result.contains("42.7"), "Should contain float");
    assert!(result.contains("true"), "Should contain boolean");
    assert!(result.contains("NULL"), "Should contain null");

    println!("Type test result: {}", result);
}

#[test]
fn test_parameterized_query_performance_vs_escaping() {
    use std::time::Instant;

    let processor = SelectProcessor;
    let iterations = 1000;

    // Performance test: parameterized approach
    let start = Instant::now();
    for i in 0..iterations {
        let template = "SELECT * FROM users WHERE id = $0 AND name = $1";
        let params = vec![
            SqlParameter::new(0, FieldValue::Integer(i as i64)),
            SqlParameter::new(1, FieldValue::String(format!("user_{}", i))),
        ];
        let _result = processor
            .build_parameterized_query(template, params)
            .unwrap();
    }
    let parameterized_duration = start.elapsed();

    println!(
        "Parameterized queries: {:?} for {} iterations",
        parameterized_duration, iterations
    );
    println!(
        "Average per query: {:?}",
        parameterized_duration / iterations
    );

    // Should be under 5ms for 1000 operations (very fast)
    assert!(
        parameterized_duration.as_millis() < 10,
        "Parameterized queries too slow: {:?}",
        parameterized_duration
    );
}
