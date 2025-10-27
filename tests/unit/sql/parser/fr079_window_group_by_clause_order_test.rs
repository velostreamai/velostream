/// Test to verify parser handling of WINDOW and GROUP BY clause ordering
/// for FR-079: STDDEV aggregate functions in GROUP BY queries with windowing
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_parse_group_by_then_window() {
    // This test verifies that GROUP BY BEFORE WINDOW (invalid ordering) is rejected
    let sql = r#"
        SELECT status, SUM(amount) as total
        FROM orders
        GROUP BY status
        WINDOW TUMBLING(1m)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    match parser.parse(sql) {
        Ok(_query) => {
            panic!("Parser should reject GROUP BY before WINDOW clause");
        }
        Err(e) => {
            // Expected to fail with validation error about clause ordering
            let error_msg = format!("{:?}", e);
            assert!(
                error_msg.contains("WINDOW") && error_msg.contains("GROUP BY"),
                "Should have validation error about clause ordering. Got: {}",
                error_msg
            );
            println!(
                "✓ Correctly rejected invalid ordering (GROUP BY before WINDOW): {:?}",
                e
            );
        }
    }
}

#[test]
fn test_parse_window_then_group_by() {
    let sql = r#"
        SELECT status, SUM(amount) as total
        FROM orders
        WINDOW TUMBLING(1m)
        GROUP BY status
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    match parser.parse(sql) {
        Ok(query) => {
            if let velostream::velostream::sql::StreamingQuery::Select {
                window, group_by, ..
            } = query
            {
                println!("WINDOW then GROUP BY:");
                println!("  window: {}", window.is_some());
                println!("  group_by: {}", group_by.is_some());
                assert!(
                    window.is_some(),
                    "WINDOW should be parsed when it comes before GROUP BY"
                );
                assert!(
                    group_by.is_some(),
                    "GROUP BY should be parsed even after WINDOW"
                );
            }
        }
        Err(e) => panic!("Failed to parse: {:?}", e),
    }
}

#[test]
fn test_parse_window_only() {
    let sql = r#"
        SELECT status, SUM(amount) as total
        FROM orders
        WINDOW TUMBLING(1m)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    match parser.parse(sql) {
        Ok(query) => {
            if let velostream::velostream::sql::StreamingQuery::Select {
                window, group_by, ..
            } = query
            {
                println!("WINDOW only:");
                println!("  window: {}", window.is_some());
                println!("  group_by: {}", group_by.is_some());
                assert!(window.is_some(), "WINDOW should always be parsed");
            }
        }
        Err(e) => panic!("Failed to parse: {:?}", e),
    }
}

#[test]
fn test_parse_group_by_only() {
    let sql = r#"
        SELECT status, SUM(amount) as total
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    match parser.parse(sql) {
        Ok(query) => {
            if let velostream::velostream::sql::StreamingQuery::Select {
                window, group_by, ..
            } = query
            {
                println!("GROUP BY only:");
                println!("  window: {}", window.is_some());
                println!("  group_by: {}", group_by.is_some());
                assert!(group_by.is_some(), "GROUP BY should always be parsed");
            }
        }
        Err(e) => panic!("Failed to parse: {:?}", e),
    }
}
