use velostream::velostream::sql::app_parser::SqlApplicationParser;
use velostream::velostream::sql::ast::StreamingQuery;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::parser::annotations::MetricType;

/// Tests that @metric annotations are correctly preserved and parsed
/// across multiple CREATE STREAM statements in a single SQL file.
///
/// This is a critical test gap: the server deploys each statement independently,
/// re-parsing the SQL for each. If annotations aren't preserved through the
/// split-and-reparse cycle, metrics won't be registered for 2nd/3rd+ queries.

#[test]
fn test_multi_statement_each_create_stream_has_own_annotations() {
    let sql = r#"
-- @metric: velo_first_records_total
-- @metric_type: counter
-- @metric_help: "First stream records"
CREATE STREAM first_stream AS
SELECT symbol, price FROM input_topic
EMIT CHANGES
WITH (
    'input_topic.type' = 'kafka_source',
    'input_topic.topic.name' = 'input',
    'first_stream.type' = 'kafka_sink',
    'first_stream.topic.name' = 'first'
);

-- @metric: velo_second_records_total
-- @metric_type: counter
-- @metric_help: "Second stream records"
--
-- @metric: velo_second_avg_price
-- @metric_type: gauge
-- @metric_help: "Average price"
-- @metric_field: avg_price
-- @metric_labels: symbol
CREATE STREAM second_stream AS
SELECT symbol, AVG(price) as avg_price FROM first_stream
GROUP BY symbol
EMIT CHANGES
WITH (
    'first_stream.type' = 'kafka_source',
    'first_stream.topic.name' = 'first',
    'second_stream.type' = 'kafka_sink',
    'second_stream.topic.name' = 'second'
);

-- @metric: velo_third_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Processing latency"
-- @metric_field: latency_seconds
-- @metric_buckets: [0.01, 0.05, 0.1, 0.5, 1.0]
CREATE STREAM third_stream AS
SELECT symbol, price, 0.05 as latency_seconds FROM first_stream
EMIT CHANGES
WITH (
    'first_stream.type' = 'kafka_source',
    'first_stream.topic.name' = 'first',
    'third_stream.type' = 'kafka_sink',
    'third_stream.topic.name' = 'third'
)"#;

    let parser = StreamingSqlParser::new();

    // Split by semicolons (same way app_parser does)
    let statements: Vec<&str> = sql.split(';').collect();

    let mut parsed_queries = Vec::new();
    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(query) = parser.parse(trimmed) {
            parsed_queries.push(query);
        }
    }

    assert_eq!(
        parsed_queries.len(),
        3,
        "Should parse 3 CREATE STREAM statements"
    );

    // Verify first_stream annotations
    match &parsed_queries[0] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "first_stream");
            assert_eq!(
                metric_annotations.len(),
                1,
                "first_stream should have 1 metric annotation"
            );
            assert_eq!(metric_annotations[0].name, "velo_first_records_total");
            assert_eq!(metric_annotations[0].metric_type, MetricType::Counter);
            assert_eq!(
                metric_annotations[0].help,
                Some("First stream records".to_string())
            );
        }
        other => panic!(
            "Expected CreateStream for first_stream, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Verify second_stream annotations (2 metrics: counter + gauge)
    match &parsed_queries[1] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "second_stream");
            assert_eq!(
                metric_annotations.len(),
                2,
                "second_stream should have 2 metric annotations"
            );

            assert_eq!(metric_annotations[0].name, "velo_second_records_total");
            assert_eq!(metric_annotations[0].metric_type, MetricType::Counter);

            assert_eq!(metric_annotations[1].name, "velo_second_avg_price");
            assert_eq!(metric_annotations[1].metric_type, MetricType::Gauge);
            assert_eq!(metric_annotations[1].field, Some("avg_price".to_string()));
            assert_eq!(metric_annotations[1].labels, vec!["symbol"]);
        }
        other => panic!(
            "Expected CreateStream for second_stream, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Verify third_stream annotations (histogram)
    match &parsed_queries[2] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "third_stream");
            assert_eq!(
                metric_annotations.len(),
                1,
                "third_stream should have 1 metric annotation"
            );
            assert_eq!(metric_annotations[0].name, "velo_third_latency_seconds");
            assert_eq!(metric_annotations[0].metric_type, MetricType::Histogram);
            assert_eq!(
                metric_annotations[0].field,
                Some("latency_seconds".to_string())
            );
            assert_eq!(
                metric_annotations[0].buckets,
                Some(vec![0.01, 0.05, 0.1, 0.5, 1.0])
            );
        }
        other => panic!(
            "Expected CreateStream for third_stream, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}

#[test]
fn test_app_parser_preserves_metric_annotations_in_statements() {
    let sql = r#"-- SQL Application: test_app
-- Version: 1.0.0
-- Description: Test app for metric annotation preservation

-- @metric: velo_stream_a_total
-- @metric_type: counter
-- @metric_help: "Stream A records"
CREATE STREAM stream_a AS
SELECT id, value FROM input
EMIT CHANGES
WITH (
    'input.type' = 'kafka_source',
    'input.topic.name' = 'input',
    'stream_a.type' = 'kafka_sink',
    'stream_a.topic.name' = 'stream_a'
);

-- @metric: velo_stream_b_total
-- @metric_type: counter
-- @metric_help: "Stream B records"
--
-- @metric: velo_stream_b_gauge
-- @metric_type: gauge
-- @metric_help: "Stream B value"
-- @metric_field: avg_value
CREATE STREAM stream_b AS
SELECT id, AVG(value) as avg_value FROM stream_a
GROUP BY id
EMIT CHANGES
WITH (
    'stream_a.type' = 'kafka_source',
    'stream_a.topic.name' = 'stream_a',
    'stream_b.type' = 'kafka_sink',
    'stream_b.topic.name' = 'stream_b'
);"#;

    let app_parser = SqlApplicationParser::new();
    let app = app_parser.parse_application(sql).unwrap();

    // Should have 2 statements
    assert_eq!(app.statements.len(), 2, "Should parse 2 statements");

    // Re-parse each statement (as the server does in deploy_job)
    let parser = StreamingSqlParser::new();

    // Statement 0: stream_a — should have @metric annotations in its SQL
    let query_a = parser.parse(&app.statements[0].sql).unwrap();
    match &query_a {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "stream_a");
            assert_eq!(
                metric_annotations.len(),
                1,
                "stream_a should have 1 metric annotation after re-parse. SQL:\n{}",
                &app.statements[0].sql
            );
            assert_eq!(metric_annotations[0].name, "velo_stream_a_total");
            assert_eq!(metric_annotations[0].metric_type, MetricType::Counter);
        }
        other => panic!(
            "Expected CreateStream, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Statement 1: stream_b — should have 2 @metric annotations
    let query_b = parser.parse(&app.statements[1].sql).unwrap();
    match &query_b {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "stream_b");
            assert_eq!(
                metric_annotations.len(),
                2,
                "stream_b should have 2 metric annotations after re-parse. SQL:\n{}",
                &app.statements[1].sql
            );
            assert_eq!(metric_annotations[0].name, "velo_stream_b_total");
            assert_eq!(metric_annotations[0].metric_type, MetricType::Counter);
            assert_eq!(metric_annotations[1].name, "velo_stream_b_gauge");
            assert_eq!(metric_annotations[1].metric_type, MetricType::Gauge);
            assert_eq!(metric_annotations[1].field, Some("avg_value".to_string()));
        }
        other => panic!(
            "Expected CreateStream, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}

#[test]
fn test_annotations_not_leaked_between_statements() {
    // Verifies that annotations from the first CREATE STREAM
    // do NOT appear on the second CREATE STREAM
    let sql = r#"
-- @metric: velo_only_on_first
-- @metric_type: counter
-- @metric_help: "Only on first"
CREATE STREAM first AS
SELECT id FROM input
EMIT CHANGES
WITH (
    'input.type' = 'kafka_source',
    'input.topic.name' = 'input',
    'first.type' = 'kafka_sink',
    'first.topic.name' = 'first'
);

-- No annotations on this one
CREATE STREAM second AS
SELECT id FROM first
EMIT CHANGES
WITH (
    'first.type' = 'kafka_source',
    'first.topic.name' = 'first',
    'second.type' = 'kafka_sink',
    'second.topic.name' = 'second'
)"#;

    let parser = StreamingSqlParser::new();

    let statements: Vec<&str> = sql.split(';').collect();
    let mut parsed = Vec::new();
    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(query) = parser.parse(trimmed) {
            parsed.push(query);
        }
    }

    assert_eq!(parsed.len(), 2);

    // First should have the annotation
    match &parsed[0] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "first");
            assert_eq!(metric_annotations.len(), 1);
            assert_eq!(metric_annotations[0].name, "velo_only_on_first");
        }
        _ => panic!("Expected CreateStream"),
    }

    // Second should have NO annotations
    match &parsed[1] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "second");
            assert_eq!(
                metric_annotations.len(),
                0,
                "second stream should have 0 annotations but got: {:?}",
                metric_annotations
            );
        }
        _ => panic!("Expected CreateStream"),
    }
}

#[test]
fn test_trading_demo_market_data_pattern() {
    // Mirrors the real app_market_data.sql structure to verify
    // all 3 queries get their respective annotations
    let sql = r#"
-- @metric: velo_market_data_ts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by market_data_ts"
--
-- @metric: velo_market_data_current_price
-- @metric_type: gauge
-- @metric_help: "Current price value"
-- @metric_field: price
CREATE STREAM market_data_ts AS
SELECT symbol PRIMARY KEY, price FROM in_market_data_stream
EMIT CHANGES
WITH (
    'in_market_data_stream.type' = 'kafka_source',
    'in_market_data_stream.topic.name' = 'in_market_data_stream',
    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.topic.name' = 'market_data_ts'
);

-- @metric: velo_tick_buckets_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by tick_buckets"
-- @metric_labels: symbol
--
-- @metric: velo_tick_buckets_avg_price
-- @metric_type: gauge
-- @metric_help: "Average price in bucket"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
-- @metric: velo_tick_buckets_trade_count
-- @metric_type: gauge
-- @metric_help: "Trade count in bucket"
-- @metric_labels: symbol
-- @metric_field: trade_count
CREATE STREAM tick_buckets AS
SELECT
    symbol PRIMARY KEY,
    AVG(price) as avg_price,
    COUNT(*) as trade_count
FROM market_data_ts
WINDOW TUMBLING(timestamp, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.topic.name' = 'tick_buckets'
);

-- @metric: velo_enriched_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by enriched_market_data"
--
-- @metric: velo_enriched_market_data_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Enrichment latency in seconds"
-- @metric_field: enrichment_latency_seconds
CREATE STREAM enriched_market_data AS
SELECT m.symbol PRIMARY KEY, m.price, r.instrument_name
FROM market_data_ts m
LEFT JOIN instrument_reference r ON m.symbol = r.symbol
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'instrument_reference.type' = 'file_source',
    'instrument_reference.config_file' = 'ref.yaml',
    'enriched_market_data.type' = 'kafka_sink',
    'enriched_market_data.topic.name' = 'enriched_market_data'
)"#;

    let parser = StreamingSqlParser::new();

    let statements: Vec<&str> = sql.split(';').collect();
    let mut parsed = Vec::new();
    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(query) = parser.parse(trimmed) {
            parsed.push(query);
        }
    }

    assert_eq!(parsed.len(), 3, "Should parse 3 CREATE STREAM statements");

    // market_data_ts: 2 annotations (counter + gauge)
    match &parsed[0] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "market_data_ts");
            assert_eq!(
                metric_annotations.len(),
                2,
                "market_data_ts: expected 2 annotations, got {}",
                metric_annotations.len()
            );
            assert_eq!(
                metric_annotations[0].name,
                "velo_market_data_ts_records_total"
            );
            assert_eq!(metric_annotations[1].name, "velo_market_data_current_price");
            assert_eq!(metric_annotations[1].metric_type, MetricType::Gauge);
        }
        _ => panic!("Expected CreateStream"),
    }

    // tick_buckets: 3 annotations (counter + 2 gauges)
    match &parsed[1] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "tick_buckets");
            assert_eq!(
                metric_annotations.len(),
                3,
                "tick_buckets: expected 3 annotations, got {}",
                metric_annotations.len()
            );
            assert_eq!(
                metric_annotations[0].name,
                "velo_tick_buckets_records_total"
            );
            assert_eq!(metric_annotations[1].name, "velo_tick_buckets_avg_price");
            assert_eq!(metric_annotations[2].name, "velo_tick_buckets_trade_count");
        }
        _ => panic!("Expected CreateStream"),
    }

    // enriched_market_data: 2 annotations (counter + histogram)
    match &parsed[2] {
        StreamingQuery::CreateStream {
            name,
            metric_annotations,
            ..
        } => {
            assert_eq!(name, "enriched_market_data");
            assert_eq!(
                metric_annotations.len(),
                2,
                "enriched_market_data: expected 2 annotations, got {}",
                metric_annotations.len()
            );
            assert_eq!(
                metric_annotations[0].name,
                "velo_enriched_market_data_records_total"
            );
            assert_eq!(
                metric_annotations[1].name,
                "velo_enriched_market_data_latency_seconds"
            );
            assert_eq!(metric_annotations[1].metric_type, MetricType::Histogram);
        }
        _ => panic!("Expected CreateStream"),
    }
}
