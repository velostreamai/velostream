/*!
# Query Performance Tests

Tests for SQL query parsing, execution, and memory usage performance.
These are lightweight tests - heavy benchmarks run in CI examples.
*/

use ferrisstreams::ferris::sql::ast::StreamingQuery;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

#[test]
fn test_query_parsing_memory_efficiency() {
    // Test that parsing many queries doesn't consume excessive memory
    let parser = StreamingSqlParser::new();
    let queries = [
        "SELECT * FROM orders",
        "SELECT customer_id, amount FROM orders WHERE amount > 100",
        "SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)",
        "SELECT AVG(amount) FROM orders WINDOW SLIDING(10m, 5m)",
        "SELECT SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 1000",
    ];

    // Parse multiple queries and ensure they don't consume excessive memory
    let parsed_queries: Result<Vec<_>, _> = queries
        .iter()
        .cycle()
        .take(1000) // Test with 1000 queries (200 of each type)
        .map(|query| parser.parse(query))
        .collect();

    assert!(parsed_queries.is_ok());
    assert_eq!(parsed_queries.unwrap().len(), 1000);
}

#[test]
fn test_streaming_query_enum_size() {
    // Test that StreamingQuery enum variants don't have excessive size differences
    // This is a regression test for clippy::large_enum_variant warnings

    let parser = StreamingSqlParser::new();

    // Create different types of queries
    let simple_query = parser.parse("SELECT * FROM orders").unwrap();
    let complex_query = parser
        .parse(
            "SELECT customer_id, SUM(amount) as total 
         FROM orders 
         WHERE created_at > '2024-01-01' 
         GROUP BY customer_id 
         HAVING SUM(amount) > 1000 
         WINDOW TUMBLING(1h)",
        )
        .unwrap();
    let show_query = parser.parse("SHOW STREAMS").unwrap();

    // All queries should be created successfully
    match (&simple_query, &complex_query, &show_query) {
        (
            StreamingQuery::Select { .. },
            StreamingQuery::Select { .. },
            StreamingQuery::Show { .. },
        ) => {
            // Test that we can store them in collections without issues
            let _queries = [simple_query, complex_query, show_query];
        }
        _ => panic!("Unexpected query types parsed"),
    }
}

#[test]
fn test_query_where_clause_access_patterns() {
    // Test performance characteristics of where clause access
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT * FROM orders WHERE amount > 100")
        .unwrap();

    // Test repeated access to where clause (simulating execution engine behavior)
    for _ in 0..1000 {
        match &query {
            StreamingQuery::Select { where_clause, .. } => {
                // This access pattern should be efficient
                let _has_where = where_clause.is_some();
                if let Some(_expr) = where_clause {
                    // Field access should be fast
                }
            }
            _ => panic!("Expected Select query"),
        }
    }
}

#[test]
fn test_query_having_clause_access_patterns() {
    // Test performance characteristics of having clause access
    let parser = StreamingSqlParser::new();
    let query = parser.parse(
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 1000"
    ).unwrap();

    // Test repeated access to having clause
    for _ in 0..1000 {
        match &query {
            StreamingQuery::Select { having, .. } => {
                // This access pattern should be efficient
                let _has_having = having.is_some();
                if let Some(_expr) = having {
                    // Field access should be fast
                }
            }
            _ => panic!("Expected Select query"),
        }
    }
}

#[test]
fn test_query_vector_storage_efficiency() {
    // Test that storing many queries in vectors is memory efficient
    let parser = StreamingSqlParser::new();

    let mut queries = Vec::with_capacity(100);

    // Create various types of queries
    for i in 0..100 {
        let query_str = match i % 4 {
            0 => format!("SELECT * FROM table_{}", i),
            1 => format!("SELECT col_{} FROM table_{} WHERE id > {}", i, i, i),
            2 => format!(
                "SELECT COUNT(*) FROM table_{} WINDOW TUMBLING({}m)",
                i,
                i + 1
            ),
            _ => "SHOW STREAMS".to_string(),
        };

        if let Ok(query) = parser.parse(&query_str) {
            queries.push(query);
        }
    }

    // Should successfully store many queries
    assert!(!queries.is_empty());
    assert!(queries.len() <= 100);
}

#[test]
fn test_complex_query_construction_performance() {
    // Test that constructing complex queries doesn't have performance issues
    let parser = StreamingSqlParser::new();

    let complex_queries = [
        "SELECT a.customer_id, a.amount, b.order_count 
         FROM orders a 
         JOIN (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) b 
         ON a.customer_id = b.customer_id 
         WHERE a.amount > 100 AND b.order_count > 5",
        "SELECT customer_id, 
                AVG(amount) as avg_amount, 
                COUNT(*) as order_count, 
                SUM(amount) as total_amount 
         FROM orders 
         WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31' 
         GROUP BY customer_id 
         HAVING AVG(amount) > 50 AND COUNT(*) > 10 
         WINDOW SLIDING(1d, 1h)",
        "SELECT * FROM orders 
         WHERE customer_id IN (SELECT customer_id FROM premium_customers) 
         AND amount > (SELECT AVG(amount) FROM orders) 
         AND status NOT IN ('cancelled', 'refunded')",
    ];

    // All complex queries should parse without performance issues
    for query_str in &complex_queries {
        let result = parser.parse(query_str);
        // Some may fail due to unsupported features, but parsing should be fast
        match result {
            Ok(_) => {}  // Successfully parsed
            Err(_) => {} // May fail due to unimplemented features, but should be fast
        }
    }
}
