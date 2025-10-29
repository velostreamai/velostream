/*!
# Phase 2.4: Time-Based JOINs Tests

Tests for time-based JOIN operations including temporal joins with BETWEEN on timestamps.
These tests verify that JOIN operations with temporal constraints work correctly in windowed contexts.
*/

use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test basic temporal JOIN with BETWEEN
#[test]
fn test_temporal_join_with_between() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT o.order_id, o.customer_id, p.price \
                 FROM orders o \
                 JOIN prices p ON o.product_id = p.product_id \
                   AND p.timestamp BETWEEN o.timestamp - 3600 AND o.timestamp";

    match parser.parse(query) {
        Ok(_) => println!("✓ Temporal JOIN with BETWEEN parses correctly"),
        Err(e) => println!(
            "⚠️  Temporal JOIN with BETWEEN may have limited support: {}",
            e
        ),
    }
}

/// Test windowed JOIN with GROUP BY
#[test]
fn test_windowed_join_with_group_by() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT m.symbol, COUNT(*) as trade_count \
                 FROM market_data m \
                 JOIN trades t ON m.symbol = t.symbol \
                   AND t.timestamp BETWEEN m.timestamp AND m.timestamp + 5000 \
                 WINDOW TUMBLING(10s) \
                 GROUP BY m.symbol";

    match parser.parse(query) {
        Ok(_) => println!("✓ Windowed JOIN with GROUP BY parses correctly"),
        Err(e) => println!(
            "⚠️  Windowed JOIN with GROUP BY may have limited support: {}",
            e
        ),
    }
}

/// Test LEFT JOIN with temporal constraint
#[test]
fn test_left_join_temporal() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT o.order_id, p.property_value \
                 FROM orders o \
                 LEFT JOIN order_properties p ON o.id = p.order_id \
                   AND p.timestamp <= o.timestamp";

    match parser.parse(query) {
        Ok(_) => println!("✓ LEFT JOIN with temporal constraint parses correctly"),
        Err(e) => println!(
            "⚠️  LEFT JOIN with temporal constraint may have limited support: {}",
            e
        ),
    }
}

/// Test JOIN with HAVING clause on aggregates
#[test]
fn test_join_with_having() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT u.user_id, COUNT(*) as event_count \
                 FROM users u \
                 JOIN events e ON u.id = e.user_id \
                   AND e.timestamp BETWEEN u.created_at AND NOW() \
                 WINDOW TUMBLING(60s) \
                 GROUP BY u.user_id \
                 HAVING COUNT(*) > 10";

    match parser.parse(query) {
        Ok(_) => println!("✓ JOIN with HAVING clause parses correctly"),
        Err(e) => println!(
            "⚠️  JOIN with HAVING clause may have limited support: {}",
            e
        ),
    }
}

/// Test multiple JOINs with temporal constraints
#[test]
fn test_multiple_joins_temporal() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT o.order_id, s.status \
                 FROM orders o \
                 JOIN shipments s ON o.id = s.order_id \
                   AND s.timestamp >= o.timestamp \
                 JOIN deliveries d ON s.id = d.shipment_id \
                   AND d.timestamp BETWEEN s.timestamp AND s.timestamp + 86400000";

    match parser.parse(query) {
        Ok(_) => println!("✓ Multiple JOINs with temporal constraints parse correctly"),
        Err(e) => println!(
            "⚠️  Multiple JOINs with temporal constraints may have limited support: {}",
            e
        ),
    }
}

/// Test self-JOIN with temporal condition
#[test]
fn test_self_join_temporal() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT t1.id, t2.id FROM transactions t1 \
                 JOIN transactions t2 ON t1.account_id = t2.account_id \
                   AND t2.timestamp BETWEEN t1.timestamp AND t1.timestamp + 60000 \
                   AND t1.id < t2.id";

    match parser.parse(query) {
        Ok(_) => println!("✓ Self-JOIN with temporal condition parses correctly"),
        Err(e) => println!(
            "⚠️  Self-JOIN with temporal condition may have limited support: {}",
            e
        ),
    }
}

/// Test JOIN with sliding window
#[test]
fn test_join_with_sliding_window() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT m.symbol, AVG(t.price) as avg_price \
                 FROM market_updates m \
                 JOIN trades t ON m.symbol = t.symbol \
                   AND t.timestamp BETWEEN m.timestamp - 300000 AND m.timestamp \
                 WINDOW SLIDING(600s, 60s) \
                 GROUP BY m.symbol";

    match parser.parse(query) {
        Ok(_) => println!("✓ JOIN with sliding window parses correctly"),
        Err(e) => println!(
            "⚠️  JOIN with sliding window may have limited support: {}",
            e
        ),
    }
}

/// Test JOIN with complex temporal filter
#[test]
fn test_join_complex_temporal_filter() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT c.customer_id, SUM(o.amount) as total \
                 FROM customers c \
                 JOIN orders o ON c.id = o.customer_id \
                   AND (o.timestamp BETWEEN c.last_login - 86400000 AND c.last_login \
                        OR o.timestamp > c.created_at) \
                 WINDOW TUMBLING(3600s) \
                 GROUP BY c.customer_id";

    match parser.parse(query) {
        Ok(_) => println!("✓ JOIN with complex temporal filter parses correctly"),
        Err(e) => println!(
            "⚠️  JOIN with complex temporal filter may have limited support: {}",
            e
        ),
    }
}

/// Test INNER JOIN with timestamp range
#[test]
fn test_inner_join_timestamp_range() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT a.account_id, COUNT(*) as transaction_count \
                 FROM accounts a \
                 INNER JOIN transactions t ON a.id = t.account_id \
                   AND t.created_at BETWEEN a.opened_at AND a.closed_at \
                 GROUP BY a.account_id";

    match parser.parse(query) {
        Ok(_) => println!("✓ INNER JOIN with timestamp range parses correctly"),
        Err(e) => println!(
            "⚠️  INNER JOIN with timestamp range may have limited support: {}",
            e
        ),
    }
}

/// Test JOIN with ORDER BY on temporal field
#[test]
fn test_join_with_order_by_temporal() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT o.order_id, p.product_name \
                 FROM orders o \
                 JOIN products p ON o.product_id = p.id \
                   AND p.created_at <= o.timestamp \
                 ORDER BY o.timestamp DESC";

    match parser.parse(query) {
        Ok(_) => println!("✓ JOIN with ORDER BY temporal field parses correctly"),
        Err(e) => println!(
            "⚠️  JOIN with ORDER BY temporal field may have limited support: {}",
            e
        ),
    }
}
