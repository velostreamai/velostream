//! Tests for job name extraction from SQL application files.
//!
//! Verifies the end-to-end job name resolution priority chain:
//!   1. `-- @name:` annotation (app parser comment)
//!   2. `CREATE STREAM <name>` / `CREATE TABLE <name>` (AST extraction)
//!   3. `extract_sql_snippet` (fallback)
//!
//! These tests exercise the same code path as `deploy_sql_application_with_filename`.

use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::sql::app_parser::SqlApplicationParser;

async fn create_test_server() -> StreamJobServer {
    StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 20)
}

/// Test that `-- @name:` annotations are used as the job name for each statement.
/// This is the primary naming mechanism used by the trading demo deploy files.
#[tokio::test]
async fn test_name_annotation_sets_job_name() {
    let sql = r#"-- SQL Application: name_annotation_test
-- Version: 1.0.0
-- Description: Test @name annotation job naming

-- @name: market_data_ts
CREATE STREAM market_data_output AS
SELECT symbol PRIMARY KEY, price FROM raw_market_data
EMIT CHANGES
WITH (
    'raw_market_data.type' = 'kafka_source',
    'raw_market_data.topic.name' = 'raw_market_data',
    'raw_market_data.bootstrap.servers' = 'localhost:9092',
    'market_data_output.type' = 'kafka_sink',
    'market_data_output.topic.name' = 'market_data_output',
    'market_data_output.bootstrap.servers' = 'localhost:9092'
);

-- @name: tick_buckets
CREATE STREAM tick_bucket_output AS
SELECT symbol PRIMARY KEY, AVG(price) as avg_price, COUNT(*) as cnt
FROM market_data_output
GROUP BY symbol
EMIT CHANGES
WITH (
    'market_data_output.type' = 'kafka_source',
    'market_data_output.topic.name' = 'market_data_output',
    'market_data_output.bootstrap.servers' = 'localhost:9092',
    'tick_bucket_output.type' = 'kafka_sink',
    'tick_bucket_output.topic.name' = 'tick_bucket_output',
    'tick_bucket_output.bootstrap.servers' = 'localhost:9092'
);

-- @name: enriched_market_data
CREATE STREAM enriched_output AS
SELECT m.symbol PRIMARY KEY, m.price
FROM market_data_output m
EMIT CHANGES
WITH (
    'market_data_output.type' = 'kafka_source',
    'market_data_output.topic.name' = 'market_data_output',
    'market_data_output.bootstrap.servers' = 'localhost:9092',
    'enriched_output.type' = 'kafka_sink',
    'enriched_output.topic.name' = 'enriched_output',
    'enriched_output.bootstrap.servers' = 'localhost:9092'
);
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 3);

    let server = create_test_server().await;
    let deployed = server
        .deploy_sql_application(app, None)
        .await
        .expect("Should deploy application");

    // Job names should come from @name annotations, NOT from CREATE STREAM names
    assert_eq!(deployed.len(), 3, "Should deploy 3 jobs");
    assert_eq!(
        deployed[0], "market_data_ts",
        "First job should use @name annotation"
    );
    assert_eq!(
        deployed[1], "tick_buckets",
        "Second job should use @name annotation"
    );
    assert_eq!(
        deployed[2], "enriched_market_data",
        "Third job should use @name annotation"
    );
}

/// Test that when `-- @name:` is absent, the job name falls back to the
/// `CREATE STREAM <name>` from the AST.
#[tokio::test]
async fn test_ast_name_fallback_without_annotation() {
    let sql = r#"-- SQL Application: ast_fallback_test
-- Version: 1.0.0
-- Description: Test AST name fallback

CREATE STREAM sensor_readings AS
SELECT sensor_id PRIMARY KEY, temperature FROM raw_sensors
EMIT CHANGES
WITH (
    'raw_sensors.type' = 'kafka_source',
    'raw_sensors.topic.name' = 'raw_sensors',
    'raw_sensors.bootstrap.servers' = 'localhost:9092',
    'sensor_readings.type' = 'kafka_sink',
    'sensor_readings.topic.name' = 'sensor_readings',
    'sensor_readings.bootstrap.servers' = 'localhost:9092'
);

CREATE STREAM temperature_alerts AS
SELECT sensor_id PRIMARY KEY, temperature FROM sensor_readings
WHERE temperature > 100
EMIT CHANGES
WITH (
    'sensor_readings.type' = 'kafka_source',
    'sensor_readings.topic.name' = 'sensor_readings',
    'sensor_readings.bootstrap.servers' = 'localhost:9092',
    'temperature_alerts.type' = 'kafka_sink',
    'temperature_alerts.topic.name' = 'temperature_alerts',
    'temperature_alerts.bootstrap.servers' = 'localhost:9092'
);
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 2);

    let server = create_test_server().await;
    let deployed = server
        .deploy_sql_application(app, None)
        .await
        .expect("Should deploy application");

    // Without @name, job names should come from CREATE STREAM <name>
    assert_eq!(deployed.len(), 2, "Should deploy 2 jobs");
    assert_eq!(
        deployed[0], "sensor_readings",
        "First job should use CREATE STREAM name"
    );
    assert_eq!(
        deployed[1], "temperature_alerts",
        "Second job should use CREATE STREAM name"
    );
}

/// Test that `-- @name:` takes priority over the `CREATE STREAM <name>`.
/// This verifies the priority chain when both are present and differ.
#[tokio::test]
async fn test_name_annotation_overrides_ast_name() {
    let sql = r#"-- SQL Application: priority_test
-- Version: 1.0.0
-- Description: Test @name priority over AST name

-- @name: custom_pipeline_name
CREATE STREAM stream_output_name AS
SELECT id PRIMARY KEY, value FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'input_stream',
    'input_stream.bootstrap.servers' = 'localhost:9092',
    'stream_output_name.type' = 'kafka_sink',
    'stream_output_name.topic.name' = 'stream_output_name',
    'stream_output_name.bootstrap.servers' = 'localhost:9092'
);
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 1);

    // Verify the app parser captured the @name annotation
    assert_eq!(
        app.statements[0].name.as_deref(),
        Some("custom_pipeline_name"),
        "App parser should capture @name annotation"
    );

    let server = create_test_server().await;
    let deployed = server
        .deploy_sql_application(app, None)
        .await
        .expect("Should deploy application");

    // @name should win over CREATE STREAM name
    assert_eq!(deployed.len(), 1);
    assert_eq!(
        deployed[0], "custom_pipeline_name",
        "Job name should be from @name, not from CREATE STREAM"
    );
}

/// Test mixed naming: some statements have `-- @name:`, others don't.
/// Each should resolve independently.
#[tokio::test]
async fn test_mixed_annotation_and_ast_naming() {
    let sql = r#"-- SQL Application: mixed_naming_test
-- Version: 1.0.0
-- Description: Test mixed naming strategies

-- @name: annotated_job
CREATE STREAM first_stream AS
SELECT id PRIMARY KEY, value FROM source_a
EMIT CHANGES
WITH (
    'source_a.type' = 'kafka_source',
    'source_a.topic.name' = 'source_a',
    'source_a.bootstrap.servers' = 'localhost:9092',
    'first_stream.type' = 'kafka_sink',
    'first_stream.topic.name' = 'first_stream',
    'first_stream.bootstrap.servers' = 'localhost:9092'
);

CREATE STREAM unannotated_stream AS
SELECT id PRIMARY KEY, value FROM source_b
EMIT CHANGES
WITH (
    'source_b.type' = 'kafka_source',
    'source_b.topic.name' = 'source_b',
    'source_b.bootstrap.servers' = 'localhost:9092',
    'unannotated_stream.type' = 'kafka_sink',
    'unannotated_stream.topic.name' = 'unannotated_stream',
    'unannotated_stream.bootstrap.servers' = 'localhost:9092'
);

-- @name: another_annotated_job
CREATE STREAM third_stream AS
SELECT id PRIMARY KEY, value FROM source_c
EMIT CHANGES
WITH (
    'source_c.type' = 'kafka_source',
    'source_c.topic.name' = 'source_c',
    'source_c.bootstrap.servers' = 'localhost:9092',
    'third_stream.type' = 'kafka_sink',
    'third_stream.topic.name' = 'third_stream',
    'third_stream.bootstrap.servers' = 'localhost:9092'
);
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 3);

    let server = create_test_server().await;
    let deployed = server
        .deploy_sql_application(app, None)
        .await
        .expect("Should deploy application");

    assert_eq!(deployed.len(), 3, "Should deploy 3 jobs");
    assert_eq!(deployed[0], "annotated_job", "First job: @name annotation");
    assert_eq!(
        deployed[1], "unannotated_stream",
        "Second job: CREATE STREAM name (no @name)"
    );
    assert_eq!(
        deployed[2], "another_annotated_job",
        "Third job: @name annotation"
    );
}

/// Test that `-- Name:` (legacy format) also works for job naming.
#[tokio::test]
async fn test_legacy_name_format_also_works() {
    let sql = r#"-- SQL Application: legacy_format_test
-- Version: 1.0.0
-- Description: Test legacy -- Name: format

-- Name: legacy_job_name
CREATE STREAM legacy_output AS
SELECT id PRIMARY KEY, value FROM legacy_source
EMIT CHANGES
WITH (
    'legacy_source.type' = 'kafka_source',
    'legacy_source.topic.name' = 'legacy_source',
    'legacy_source.bootstrap.servers' = 'localhost:9092',
    'legacy_output.type' = 'kafka_sink',
    'legacy_output.topic.name' = 'legacy_output',
    'legacy_output.bootstrap.servers' = 'localhost:9092'
);
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 1);

    assert_eq!(
        app.statements[0].name.as_deref(),
        Some("legacy_job_name"),
        "App parser should capture -- Name: annotation"
    );

    let server = create_test_server().await;
    let deployed = server
        .deploy_sql_application(app, None)
        .await
        .expect("Should deploy application");

    assert_eq!(deployed.len(), 1);
    assert_eq!(
        deployed[0], "legacy_job_name",
        "Job name should come from -- Name: annotation"
    );
}
