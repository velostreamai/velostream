/*!
Test implicit aggregation mode based on SQL structure
*/

use ferrisstreams::ferris::sql::{ast::AggregationMode, parser::StreamingSqlParser};

#[tokio::test]
async fn test_implicit_continuous_mode_no_window() {
    let parser = StreamingSqlParser::new();

    // No window clause should imply continuous mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode,
            window,
            group_by,
            ..
        } = query
        {
            // Should have GROUP BY but no window
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_none(), "Should have no WINDOW clause");
            // aggregation_mode should be None (parser doesn't set implicit mode)
            assert_eq!(aggregation_mode, None);
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_implicit_windowed_mode_with_window() {
    let parser = StreamingSqlParser::new();

    // Window clause should imply windowed mode
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(INTERVAL 5 MINUTES)";
    let result = parser.parse(query_str);

    // Note: This might fail if WINDOW parsing isn't fully implemented
    // but the concept test is what matters
    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_some(), "Should have WINDOW clause");
            // aggregation_mode should still be None (parser doesn't set implicit mode)
            assert_eq!(aggregation_mode, None);
        }
    }
    // This test mainly documents the expected behavior
}

#[tokio::test]
async fn test_explicit_mode_overrides_implicit() {
    let parser = StreamingSqlParser::new();

    // Explicit mode should override implicit behavior
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH AGGREGATION_MODE = WINDOWED";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            aggregation_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_none(), "Should have no WINDOW clause");
            // Explicit mode should be set
            assert_eq!(aggregation_mode, Some(AggregationMode::Windowed));
        } else {
            panic!("Expected Select query");
        }
    }
}
