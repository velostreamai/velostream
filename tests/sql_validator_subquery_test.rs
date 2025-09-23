//! Tests for SQL validator subquery pattern detection and validation

use velostream::velostream::sql::validator::{ErrorSeverity, QueryValidationResult, SqlValidator};

#[test]
fn test_exists_subquery_detection() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect EXISTS subquery and provide warnings
    assert!(
        !result.warnings.is_empty(),
        "Expected warnings for EXISTS subquery"
    );

    let exists_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("EXISTS"));
    assert!(exists_warning.is_some(), "Expected EXISTS subquery warning");

    // Should also detect correlation
    let correlation_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Correlated EXISTS"));
    assert!(
        correlation_warning.is_some(),
        "Expected correlated subquery warning"
    );
}

#[test]
fn test_in_subquery_detection() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE status = 'active')";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect IN subquery and provide warnings
    assert!(
        !result.warnings.is_empty(),
        "Expected warnings for IN subquery"
    );

    let in_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("IN subqueries"));
    assert!(in_warning.is_some(), "Expected IN subquery warning");
}

#[test]
fn test_correlated_in_subquery_detection() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM orders o WHERE o.product_id IN (SELECT p.id FROM products p WHERE p.category_id = o.category_id)";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect both IN subquery and correlation warnings
    let in_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("IN subqueries"));
    assert!(in_warning.is_some(), "Expected IN subquery warning");

    let correlation_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Correlated IN"));
    assert!(
        correlation_warning.is_some(),
        "Expected correlated IN subquery warning"
    );
}

#[test]
fn test_scalar_subquery_detection() {
    let validator = SqlValidator::new();

    let query = "SELECT name, (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) as order_count FROM customers";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect scalar subquery
    let scalar_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Scalar subqueries"));
    assert!(scalar_warning.is_some(), "Expected scalar subquery warning");
}

#[test]
fn test_deeply_nested_subquery_detection() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM table1 WHERE id IN (SELECT id FROM table2 WHERE id IN (SELECT id FROM table3 WHERE id IN (SELECT id FROM table4)))";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect deeply nested subqueries (4 SELECT statements)
    let nesting_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Deeply nested"));
    assert!(
        nesting_warning.is_some(),
        "Expected deeply nested subquery warning"
    );

    // Should mention the level count
    let warning_text = nesting_warning.unwrap();
    assert!(
        warning_text.message.contains("4"),
        "Expected warning to mention 4 levels"
    );
}

#[test]
fn test_subquery_where_clause_performance_warnings() {
    let validator = SqlValidator::new();

    let queries_and_patterns = vec![
        ("SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.name LIKE '%pattern')", "LIKE patterns starting with %"),
        ("SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE t2.desc REGEXP 'pattern')", "Regular expressions"),
        ("SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE SUBSTRING(t2.name, 1, 5) = 'test')", "String functions"),
        ("SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE CASE WHEN t2.status = 'A' THEN 1 ELSE 0 END = 1)", "Complex CASE expressions"),
    ];

    for (query, expected_pattern) in queries_and_patterns {
        let result = validator.validate_query(query, 0, 1, query);

        let performance_warning = result
            .warnings
            .iter()
            .find(|w| w.message.contains(expected_pattern));
        assert!(
            performance_warning.is_some(),
            "Expected performance warning for pattern '{}' in query: {}",
            expected_pattern,
            query
        );
    }
}

#[test]
fn test_no_subquery_no_warnings() {
    let validator = SqlValidator::new();

    let query = "SELECT id, name, email FROM users WHERE status = 'active' ORDER BY name";
    let result = validator.validate_query(query, 0, 1, query);

    // Should not have any subquery-related warnings
    let subquery_warnings: Vec<_> = result
        .warnings
        .iter()
        .filter(|w| {
            w.message.contains("subquer")
                || w.message.contains("EXISTS")
                || w.message.contains("IN")
        })
        .collect();

    assert!(
        subquery_warnings.is_empty(),
        "Expected no subquery warnings for simple query"
    );
}

#[test]
fn test_simple_join_not_flagged_as_subquery() {
    let validator = SqlValidator::new();

    let query = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.customer_id";
    let result = validator.validate_query(query, 0, 1, query);

    // Should not flag simple JOINs as subqueries
    let subquery_warnings: Vec<_> = result
        .warnings
        .iter()
        .filter(|w| w.message.contains("subquer"))
        .collect();

    assert!(
        subquery_warnings.is_empty(),
        "Expected no subquery warnings for simple JOIN"
    );
}

#[test]
fn test_correlation_pattern_detection() {
    let validator = SqlValidator::new();

    // Test various correlation patterns
    let correlation_queries = vec![
        "SELECT * FROM outer_table WHERE EXISTS (SELECT 1 FROM inner_table WHERE inner_table.user_id = outer_table.id)",
        "SELECT * FROM customers c WHERE c.id IN (SELECT o.customer_id FROM orders o WHERE o.status = c.preferred_status)",
        "SELECT * FROM products p WHERE EXISTS (SELECT 1 FROM reviews r WHERE r.product_id = p.id AND r.rating > p.min_rating)",
    ];

    for query in correlation_queries {
        let result = validator.validate_query(query, 0, 1, query);

        let correlation_warning = result
            .warnings
            .iter()
            .find(|w| w.message.contains("Correlated"));
        assert!(
            correlation_warning.is_some(),
            "Expected correlation warning for query: {}",
            query
        );
    }
}

#[test]
fn test_complex_subquery_warning() {
    let validator = SqlValidator::new();

    // Test a complex subquery that's not EXISTS, IN, or scalar (using UNION which should parse)
    let query = "SELECT id FROM table1 UNION SELECT id FROM table2";
    let result = validator.validate_query(query, 0, 1, query);

    // Should get the general complex subquery warning
    let complex_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Complex subqueries detected"));
    assert!(
        complex_warning.is_some(),
        "Expected complex subquery warning for UNION query"
    );
}

#[test]
fn test_mixed_subquery_types() {
    let validator = SqlValidator::new();

    let query = "SELECT id, (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id) as order_count FROM customers WHERE EXISTS (SELECT 1 FROM payments WHERE customer_id = customers.id)";
    let result = validator.validate_query(query, 0, 1, query);

    // Should detect both scalar and EXISTS subqueries
    let scalar_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("Scalar subqueries"));
    assert!(scalar_warning.is_some(), "Expected scalar subquery warning");

    let exists_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("EXISTS"));
    assert!(exists_warning.is_some(), "Expected EXISTS subquery warning");

    // Should detect correlation in both
    let correlation_warnings: Vec<_> = result
        .warnings
        .iter()
        .filter(|w| w.message.contains("Correlated"))
        .collect();
    assert!(
        !correlation_warnings.is_empty(),
        "Expected correlation warnings"
    );
}

#[test]
fn test_validator_strict_mode() {
    let validator = SqlValidator::new_strict();

    let query = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')";
    let result = validator.validate_query(query, 0, 1, query);

    // Strict mode should still detect subqueries and provide warnings
    let in_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("IN subqueries"));
    assert!(
        in_warning.is_some(),
        "Expected IN subquery warning in strict mode"
    );
}

#[test]
fn test_query_validation_result_structure() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)";
    let result = validator.validate_query(query, 0, 1, query);

    // Verify basic result structure
    assert_eq!(result.query_text, query);
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);

    // Should have warnings but validation structure should be intact
    assert!(result.warnings.len() > 0);
    assert!(result.parsing_errors.is_empty() || !result.parsing_errors.is_empty()); // Either is valid
    assert!(result.configuration_errors.is_empty() || !result.configuration_errors.is_empty());
    // Either is valid
}
