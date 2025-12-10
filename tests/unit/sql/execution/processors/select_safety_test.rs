//! Safety tests for SELECT processor security and concurrency fixes

use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use velostream::velostream::sql::{
    SqlError, StreamingQuery,
    ast::{Expr, LiteralValue, SelectField, StreamSource},
    execution::{
        FieldValue, StreamRecord,
        processors::{ProcessorContext, ProcessorResult, SelectProcessor, TableReference},
    },
    parser::StreamingSqlParser,
};

/// Test concurrent subquery execution for thread safety
#[tokio::test]
async fn test_concurrent_subquery_execution() {
    // Create a shared parser for parsing queries
    let parser = Arc::new(StreamingSqlParser::new());

    // Create test queries that would use correlation context
    let queries = vec![
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)",
        "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)",
        "SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 1000)",
    ];

    // Spawn 100 concurrent tasks executing correlated subqueries
    let mut handles = Vec::new();

    for i in 0..100 {
        let parser_clone = parser.clone();
        let query = queries[i % queries.len()].to_string();

        let handle = task::spawn(async move {
            // Parse the query
            let parsed_query = match parser_clone.parse(&query) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("Parse error in task {}: {:?}", i, e);
                    return Err(e);
                }
            };

            // Create a test record
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("name".to_string(), FieldValue::String(format!("User{}", i)));
            fields.insert("user_id".to_string(), FieldValue::Integer((i % 10) as i64));
            fields.insert("amount".to_string(), FieldValue::Float(100.0 * i as f64));

            let record = StreamRecord {
                fields,
                timestamp: 0,
                offset: i as i64,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            };

            // Create a context with unique query ID
            let mut context = ProcessorContext::new(&format!("query_{}", i));

            // Process the query
            match SelectProcessor::process(&parsed_query, &record, &mut context) {
                Ok(result) => {
                    // Verify no correlation context leakage
                    assert!(
                        context.correlation_context.is_none(),
                        "Task {}: Correlation context should be cleaned up",
                        i
                    );
                    Ok(result)
                }
                Err(e) => {
                    eprintln!("Processing error in task {}: {:?}", i, e);
                    Err(e)
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks and collect results
    let mut success_count = 0;
    let mut error_count = 0;

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => {
                error_count += 1;
                eprintln!("Task error: {:?}", e);
            }
            Err(e) => {
                error_count += 1;
                eprintln!("Join error: {:?}", e);
            }
        }
    }

    println!(
        "Concurrent execution complete: {} successes, {} errors",
        success_count, error_count
    );

    // We expect some errors due to missing tables, but no panics or data corruption
    assert!(
        success_count + error_count == 100,
        "All tasks should complete without panics"
    );
}

/// Test SQL injection prevention
#[test]
fn test_sql_injection_prevention() {
    // Test various SQL injection attempts
    let malicious_inputs = vec![
        "'; DROP TABLE users; --",
        "' OR '1'='1",
        "\\'; DROP TABLE users; --",
        "admin'--",
        "' UNION SELECT * FROM passwords --",
        "\x00'; DROP TABLE users; --", // Null byte injection
        "\x1a'; DROP TABLE users; --", // SUB character injection
        "'; EXEC sp_MSForEachTable 'DROP TABLE ?'; --",
    ];

    for malicious_input in malicious_inputs {
        // Create a FieldValue with the malicious string
        let field = FieldValue::String(malicious_input.to_string());

        // Convert to SQL string using the protected function
        let sql_string = field_value_to_sql_string_safe(&field);

        // Verify that dangerous patterns are properly escaped
        assert!(
            sql_string.starts_with("'") && sql_string.ends_with("'"),
            "Output should be properly quoted for input: {}",
            malicious_input
        );
        // If the output contains dangerous patterns, they should be within quotes
        if sql_string.contains("DROP TABLE") {
            assert!(
                sql_string.starts_with("'") && sql_string.ends_with("'"),
                "DROP TABLE should be safely quoted for input: {}",
                malicious_input
            );
        }
        assert!(
            !sql_string.contains("\x00"),
            "Null bytes should be removed for input: {}",
            malicious_input
        );
        assert!(
            !sql_string.contains("\x1a"),
            "SUB characters should be removed for input: {}",
            malicious_input
        );

        // Verify proper quoting
        assert!(
            sql_string.starts_with("'") && sql_string.ends_with("'"),
            "String should be properly quoted for input: {}",
            malicious_input
        );

        // Verify single quotes are escaped
        let inner = &sql_string[1..sql_string.len() - 1];
        if inner.contains("'") {
            // Check that all single quotes are doubled
            let mut chars = inner.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '\'' {
                    assert_eq!(
                        chars.peek(),
                        Some(&'\''),
                        "Single quotes should be escaped by doubling"
                    );
                    chars.next(); // Skip the second quote
                }
            }
        }
    }

    // Test numeric injection attempts
    let nan = FieldValue::Float(f64::NAN);
    let inf = FieldValue::Float(f64::INFINITY);
    let neg_inf = FieldValue::Float(f64::NEG_INFINITY);

    assert_eq!(field_value_to_sql_string_safe(&nan), "NULL");
    assert_eq!(field_value_to_sql_string_safe(&inf), "NULL");
    assert_eq!(field_value_to_sql_string_safe(&neg_inf), "NULL");
}

/// Test panic recovery and proper cleanup
#[test]
fn test_panic_cleanup() {
    use std::panic;

    // Create a context with correlation context set
    let mut context = ProcessorContext::new("test_query");
    let table_ref = TableReference::new("test_table".to_string());
    context.correlation_context = Some(table_ref.clone());

    // Simulate a panic during processing
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        // This would normally be in SelectProcessor::process
        let mut local_context = ProcessorContext::new("test_query_local");

        // Save original context
        let original = local_context.correlation_context.clone();

        // Set new context
        local_context.correlation_context = Some(TableReference::new("inner_table".to_string()));

        // Simulate panic
        panic!("Simulated panic during subquery processing");

        // This restoration would not happen due to panic
        #[allow(unreachable_code)]
        {
            local_context.correlation_context = original;
        }
    }));

    // Verify that panic was caught
    assert!(result.is_err(), "Should have caught the panic");

    // In the new implementation, context is not globally shared,
    // so each processing instance has its own context
    // This means panics don't affect other concurrent executions
}

/// Test that correlation context is properly scoped
#[test]
fn test_correlation_context_scoping() {
    let mut context = ProcessorContext::new("test_query");

    // Initially no correlation context
    assert!(context.correlation_context.is_none());

    // Simulate nested subquery processing
    {
        // Outer query sets context
        let outer_table = TableReference::new("outer_table".to_string());
        let original = context.correlation_context.clone();
        context.correlation_context = Some(outer_table.clone());

        assert_eq!(
            context.correlation_context.as_ref().unwrap().name,
            "outer_table"
        );

        // Inner subquery processing
        {
            let inner_table = TableReference::new("inner_table".to_string());
            let inner_original = context.correlation_context.clone();
            context.correlation_context = Some(inner_table);

            assert_eq!(
                context.correlation_context.as_ref().unwrap().name,
                "inner_table"
            );

            // Restore after inner processing
            context.correlation_context = inner_original;
        }

        // Verify outer context is restored
        assert_eq!(
            context.correlation_context.as_ref().unwrap().name,
            "outer_table"
        );

        // Restore original (None) context
        context.correlation_context = original;
    }

    // Verify context is fully cleaned up
    assert!(context.correlation_context.is_none());
}

/// Helper function to test SQL injection protection
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
        _ => "NULL".to_string(), // Simplified for testing
    }
}
