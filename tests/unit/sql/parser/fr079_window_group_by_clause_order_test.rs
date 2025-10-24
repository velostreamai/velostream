/// Test to verify parser handling of WINDOW and GROUP BY clause ordering
/// for FR-079: STDDEV aggregate functions in GROUP BY queries with windowing
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_parse_group_by_then_window() {
    let sql = r#"
        SELECT status, SUM(amount) as total
        FROM orders
        GROUP BY status
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
                println!("GROUP BY then WINDOW:");
                println!("  window: {}", window.is_some());
                println!("  group_by: {}", group_by.is_some());
                assert!(
                    group_by.is_some(),
                    "GROUP BY should be parsed when it comes before WINDOW"
                );
                assert!(
                    window.is_some(),
                    "WINDOW should be parsed even after GROUP BY"
                );
            }
        }
        Err(e) => panic!("Failed to parse: {:?}", e),
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
