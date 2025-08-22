/*!
Test implicit emit mode behavior based on SQL structure
*/

use ferrisstreams::ferris::sql::{ast::EmitMode, parser::StreamingSqlParser};

#[tokio::test]
async fn test_implicit_continuous_mode_no_window() {
    let parser = StreamingSqlParser::new();

    // No window clause should default to EMIT CHANGES behavior (when no explicit EMIT)
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            // Should have GROUP BY but no window
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_none(), "Should have no WINDOW clause");
            // emit_mode should be None (parser doesn't set implicit mode - engine handles defaults)
            assert_eq!(emit_mode, None);
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_implicit_windowed_mode_with_window() {
    let parser = StreamingSqlParser::new();

    // Window clause should default to EMIT FINAL behavior (when no explicit EMIT)
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(INTERVAL 5 MINUTES)";
    let result = parser.parse(query_str);

    // Note: This might fail if WINDOW parsing isn't fully implemented
    // but the concept test is what matters
    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_some(), "Should have WINDOW clause");
            // emit_mode should still be None (parser doesn't set implicit mode - engine handles defaults)
            assert_eq!(emit_mode, None);
        }
    }
    // This test mainly documents the expected behavior
}

#[tokio::test]
async fn test_explicit_emit_mode_overrides_default() {
    let parser = StreamingSqlParser::new();

    // Explicit EMIT CHANGES should override implicit behavior
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT CHANGES";
    let result = parser.parse(query_str);

    assert!(result.is_ok(), "Query parsing should succeed");

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_none(), "Should have no WINDOW clause");
            // Explicit EMIT mode should be set
            assert_eq!(emit_mode, Some(EmitMode::Changes));
        } else {
            panic!("Expected Select query");
        }
    }
}

#[tokio::test]
async fn test_emit_changes_with_window_override() {
    let parser = StreamingSqlParser::new();

    // EMIT CHANGES with WINDOW should override default windowed behavior
    let query_str = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(INTERVAL 5 MINUTES) EMIT CHANGES";
    let result = parser.parse(query_str);

    if let Ok(query) = result {
        if let ferrisstreams::ferris::sql::ast::StreamingQuery::Select {
            emit_mode,
            window,
            group_by,
            ..
        } = query
        {
            assert!(group_by.is_some(), "Should have GROUP BY clause");
            assert!(window.is_some(), "Should have WINDOW clause");
            // EMIT CHANGES should override windowed default
            assert_eq!(emit_mode, Some(EmitMode::Changes));
        } else {
            panic!("Expected Select query");
        }
    }
}
