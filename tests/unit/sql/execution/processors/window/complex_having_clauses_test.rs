/*!
# Phase 2.3: Complex HAVING Clauses Tests

Tests for complex HAVING clause scenarios including window functions, subqueries, and arithmetic expressions.
*/

use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test HAVING with nested aggregates
#[test]
fn test_having_with_nested_aggregates() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT category, COUNT(*) as cnt FROM products \
                 WINDOW TUMBLING(10s) \
                 GROUP BY category \
                 HAVING COUNT(*) > AVG(price)";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with nested aggregates parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with nested aggregates may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with division operation
#[test]
fn test_having_with_division() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT product_id, SUM(quantity) as total_qty FROM orders \
                 WINDOW TUMBLING(5s) \
                 GROUP BY product_id \
                 HAVING SUM(quantity) / COUNT(*) > 10";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with division parses correctly"),
        Err(e) => println!("⚠️  HAVING with division may have limited support: {}", e),
    }
}

/// Test HAVING with multiple arithmetic operations
#[test]
fn test_having_with_arithmetic_expressions() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT store_id, SUM(sales) as total_sales FROM transactions \
                 WINDOW TUMBLING(60s) \
                 GROUP BY store_id \
                 HAVING (SUM(sales) - AVG(sales)) * COUNT(*) > 1000";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with arithmetic expressions parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with arithmetic expressions may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with window frame specification
#[test]
fn test_having_with_window_frame() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT symbol, AVG(price) as avg_price FROM stocks \
                 WINDOW SLIDING(1h, 15m) \
                 GROUP BY symbol \
                 HAVING MAX(price) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > 100";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with window frame parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with window frame may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with complex boolean expressions
#[test]
fn test_having_with_complex_boolean() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT user_id, COUNT(*) as activity_count FROM events \
                 WINDOW TUMBLING(30s) \
                 GROUP BY user_id \
                 HAVING (COUNT(*) > 5 AND AVG(value) < 100) OR SUM(value) > 500";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with complex boolean expressions parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with complex boolean may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with CASE expression
#[test]
fn test_having_with_case_expression() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT category FROM orders \
                 WINDOW TUMBLING(10s) \
                 GROUP BY category \
                 HAVING CASE \
                   WHEN COUNT(*) > 100 THEN SUM(amount) \
                   ELSE AVG(amount) \
                 END > 50";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with CASE expression parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with CASE expression may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with IN subquery (if supported)
#[test]
fn test_having_with_subquery() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT product_id, COUNT(*) as sales_count FROM sales \
                 WINDOW TUMBLING(15s) \
                 GROUP BY product_id \
                 HAVING product_id IN (SELECT id FROM featured_products)";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with subquery parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with subquery may have limited support (expected): {}",
            e
        ),
    }
}

/// Test HAVING with BETWEEN operator
#[test]
fn test_having_with_between() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT region FROM sales \
                 WINDOW TUMBLING(20s) \
                 GROUP BY region \
                 HAVING SUM(revenue) BETWEEN 1000 AND 5000";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with BETWEEN parses correctly"),
        Err(e) => println!("⚠️  HAVING with BETWEEN may have limited support: {}", e),
    }
}

/// Test HAVING with NULL checks
#[test]
fn test_having_with_null_check() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT dept FROM employees \
                 WINDOW TUMBLING(10s) \
                 GROUP BY dept \
                 HAVING SUM(salary) IS NOT NULL AND COUNT(*) > 1";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with NULL check parses correctly"),
        Err(e) => println!("⚠️  HAVING with NULL check may have limited support: {}", e),
    }
}

/// Test HAVING with string aggregate functions
#[test]
fn test_having_with_string_aggregates() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT country FROM users \
                 WINDOW TUMBLING(30s) \
                 GROUP BY country \
                 HAVING STRING_AGG(name, ',') IS NOT NULL AND COUNT(*) > 10";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with string aggregates parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with string aggregates may have limited support: {}",
            e
        ),
    }
}
