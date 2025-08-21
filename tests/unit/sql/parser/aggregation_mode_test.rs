/*!
Test SQL parsing for aggregation mode support
*/

use ferrisstreams::ferris::sql::{ast::AggregationMode, parser::StreamingSqlParser};

#[tokio::test]
async fn test_parse_windowed_aggregation_mode() {
    let parser = StreamingSqlParser::new();

    // Test explicit windowed mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH AGGREGATION_MODE = WINDOWED";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode, ..
        } = query
        {
            assert_eq!(aggregation_mode, Some(AggregationMode::Windowed));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_parse_continuous_aggregation_mode() {
    let parser = StreamingSqlParser::new();

    // Test explicit continuous mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH AGGREGATION_MODE = CONTINUOUS";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode, ..
        } = query
        {
            assert_eq!(aggregation_mode, Some(AggregationMode::Continuous));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_parse_string_literal_aggregation_mode() {
    let parser = StreamingSqlParser::new();

    // Test string literal mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH AGGREGATION_MODE = 'CONTINUOUS'";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode, ..
        } = query
        {
            assert_eq!(aggregation_mode, Some(AggregationMode::Continuous));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_parse_default_aggregation_mode() {
    let parser = StreamingSqlParser::new();

    // Test without explicit aggregation mode (should be None/default)
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode, ..
        } = query
        {
            assert_eq!(aggregation_mode, None); // Default behavior
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_parse_invalid_aggregation_mode() {
    let parser = StreamingSqlParser::new();

    // Test invalid aggregation mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH AGGREGATION_MODE = 'INVALID'";
    let result = parser.parse(query_str);

    assert!(
        result.is_err(),
        "Query parsing should fail for invalid mode"
    );

    if let Err(err) = result {
        assert!(err.to_string().contains("Invalid aggregation mode"));
    }
}
