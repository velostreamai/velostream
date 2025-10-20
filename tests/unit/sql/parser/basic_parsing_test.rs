use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::annotations::MetricType;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_simple_select_all() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT * FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields,
            from,
            where_clause,
            window,
            limit: _,
            ..
        } => {
            assert_eq!(fields.len(), 1);
            assert!(matches!(fields[0], SelectField::Wildcard));
            assert!(matches!(from, StreamSource::Stream(_)));
            assert!(where_clause.is_none());
            assert!(window.is_none());
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_specific_columns() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT customer_id, amount, timestamp FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, from, .. } => {
            assert_eq!(fields.len(), 3);
            assert!(matches!(from, StreamSource::Stream(_)));

            for field in &fields {
                assert!(matches!(field, SelectField::Expression { .. }));
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_with_alias() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT customer_id AS cid, amount AS total FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 2);

            if let SelectField::Expression { alias, .. } = &fields[0] {
                assert_eq!(alias.as_ref().unwrap(), "cid");
            }

            if let SelectField::Expression { alias, .. } = &fields[1] {
                assert_eq!(alias.as_ref().unwrap(), "total");
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_tumbling_window() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            window,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            ..
        } => {
            assert!(window.is_some());
            let window = window.unwrap();

            assert!(matches!(window, WindowSpec::Tumbling { .. }));
            if let WindowSpec::Tumbling { size, .. } = window {
                assert_eq!(size.as_secs(), 300); // 5 minutes
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_sliding_window() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT AVG(amount) FROM orders WINDOW SLIDING(10m, 5m)");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            window,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            ..
        } => {
            assert!(window.is_some());
            let window = window.unwrap();

            assert!(matches!(window, WindowSpec::Sliding { .. }));
            if let WindowSpec::Sliding { size, advance, .. } = window {
                assert_eq!(size.as_secs(), 600); // 10 minutes
                assert_eq!(advance.as_secs(), 300); // 5 minutes
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_session_window() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT SUM(amount) FROM orders WINDOW SESSION(30s)");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            window,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            ..
        } => {
            assert!(window.is_some());
            let window = window.unwrap();

            assert!(matches!(window, WindowSpec::Session { .. }));
            if let WindowSpec::Session { gap, .. } = window {
                assert_eq!(gap.as_secs(), 30);
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_invalid_sql() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("INVALID SQL QUERY");

    assert!(result.is_err());
}

#[test]
fn test_missing_from_clause() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT *");

    // Parser now allows SELECT without FROM clause (e.g., SELECT 1, SELECT NOW())
    assert!(result.is_ok());
}

#[test]
fn test_string_literals() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT 'hello world', \"another string\" FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 2);

            for field in &fields {
                if let SelectField::Expression { expr, .. } = field {
                    assert!(matches!(expr, Expr::Literal(LiteralValue::String(_))));
                }
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_numeric_literals() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT 42, 2.718, 0 FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 3);

            // Check first field (integer)
            if let SelectField::Expression { expr, .. } = &fields[0] {
                assert!(matches!(expr, Expr::Literal(LiteralValue::Integer(42))));
            }

            // Check second field (decimal - decimal numbers are now parsed as Decimal for exact precision)
            if let SelectField::Expression { expr, .. } = &fields[1] {
                assert!(matches!(expr, Expr::Literal(LiteralValue::Decimal(_))));
            }

            // Check third field (integer zero)
            if let SelectField::Expression { expr, .. } = &fields[2] {
                assert!(matches!(expr, Expr::Literal(LiteralValue::Integer(0))));
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_column_references() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT customer_id FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 1);

            if let SelectField::Expression { expr, .. } = &fields[0] {
                assert!(matches!(expr, Expr::Column(_)));
                if let Expr::Column(name) = expr {
                    assert_eq!(name, "customer_id");
                }
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_qualified_column_references() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT orders.customer_id FROM orders");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 1);

            if let SelectField::Expression { expr, .. } = &fields[0] {
                assert!(matches!(expr, Expr::Column(_)));
                if let Expr::Column(name) = expr {
                    assert_eq!(name, "orders.customer_id");
                }
            }
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_duration_parsing() {
    let parser = StreamingSqlParser::new();

    // Test seconds
    let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(30s)");
    assert!(result.is_ok());

    // Test minutes
    let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(5m)");
    assert!(result.is_ok());

    // Test hours
    let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(2h)");
    assert!(result.is_ok());

    // Test plain number (defaults to seconds)
    let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(60)");
    assert!(result.is_ok());
}

#[test]
fn test_case_insensitive_keywords() {
    let parser = StreamingSqlParser::new();

    let queries = vec![
        "select * from orders",
        "SELECT * FROM orders",
        "Select * From orders",
        "sElEcT * fRoM orders",
    ];

    for query in queries {
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
    }
}

#[test]
fn test_whitespace_handling() {
    let parser = StreamingSqlParser::new();

    let queries = vec![
        "SELECT * FROM orders",
        "SELECT  *  FROM  orders",
        "  SELECT * FROM orders  ",
        "\tSELECT\t*\tFROM\torders\t",
        "\nSELECT\n*\nFROM\norders\n",
    ];

    for query in queries {
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
    }
}

#[test]
fn test_create_stream_with_metric_annotations() {
    let parser = StreamingSqlParser::new();
    let sql = r#"
            -- FR-073 SQL-Native Observability: Market Data Counter
            -- @metric: velo_trading_market_data_total
            -- @metric_type: counter
            -- @metric_help: "Total market data records processed"
            -- @metric_labels: symbol, exchange
            CREATE STREAM market_data_ts AS
            SELECT symbol, exchange, price
            FROM market_data_stream
            EMIT CHANGES
        "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse SQL with metric annotations"
    );

    let query = result.unwrap();
    match query {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "market_data_ts");
            assert_eq!(
                metric_annotations.len(),
                1,
                "Expected 1 metric annotation, found {}",
                metric_annotations.len()
            );

            let annotation = &metric_annotations[0];
            assert_eq!(annotation.name, "velo_trading_market_data_total");
            assert_eq!(
                annotation.help,
                Some("Total market data records processed".to_string())
            );
            assert_eq!(annotation.labels, vec!["symbol", "exchange"]);
        }
        _ => panic!("Expected CreateStream query"),
    }
}

#[test]
fn test_create_stream_with_multiple_metric_annotations() {
    let parser = StreamingSqlParser::new();
    let sql = r#"
            -- @metric: velo_trading_tick_buckets_total
            -- @metric_type: counter
            -- @metric_help: "Tick buckets created per symbol"
            -- @metric_labels: symbol
            -- @metric: velo_trading_trades_per_bucket
            -- @metric_type: gauge
            -- @metric_help: "Number of trades in each 1-second bucket"
            -- @metric_field: trade_count
            -- @metric_labels: symbol
            -- @metric: velo_trading_tick_volume_distribution
            -- @metric_type: histogram
            -- @metric_help: "Distribution of trading volume per tick"
            -- @metric_field: total_volume
            -- @metric_labels: symbol
            -- @metric_buckets: 100, 500, 1000, 5000, 10000, 50000, 100000
            CREATE STREAM tick_buckets AS
            SELECT symbol, SUM(volume) as total_volume, COUNT(*) as trade_count
            FROM market_data_ts
            GROUP BY symbol
            WINDOW TUMBLING(event_time, INTERVAL '1' SECOND)
            EMIT CHANGES
        "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse SQL with multiple metric annotations"
    );

    let query = result.unwrap();
    match query {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "tick_buckets");
            assert_eq!(
                metric_annotations.len(),
                3,
                "Expected 3 metric annotations, found {}",
                metric_annotations.len()
            );

            // Check counter metric
            assert_eq!(
                metric_annotations[0].name,
                "velo_trading_tick_buckets_total"
            );
            assert!(matches!(
                metric_annotations[0].metric_type,
                MetricType::Counter
            ));

            // Check gauge metric
            assert_eq!(metric_annotations[1].name, "velo_trading_trades_per_bucket");
            assert!(matches!(
                metric_annotations[1].metric_type,
                MetricType::Gauge
            ));
            assert_eq!(metric_annotations[1].field, Some("trade_count".to_string()));

            // Check histogram metric
            assert_eq!(
                metric_annotations[2].name,
                "velo_trading_tick_volume_distribution"
            );
            assert!(matches!(
                metric_annotations[2].metric_type,
                MetricType::Histogram
            ));
            assert_eq!(
                metric_annotations[2].field,
                Some("total_volume".to_string())
            );
            assert_eq!(
                metric_annotations[2].buckets,
                Some(vec![
                    100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0
                ])
            );
        }
        _ => panic!("Expected CreateStream query"),
    }
}
