-- SQL Application: metric_demo
-- Version: 1.0.0
-- Description: Demonstrates @metric SQL annotations for observability
-- =============================================================================
-- Tier 1: Metric Annotations
-- =============================================================================
--
-- Tests: @metric annotations for counter, gauge, and histogram metrics
-- Expected: Metrics are emitted to Prometheus and can be validated via assertions
--
-- =============================================================================

-- @app: metric_demo
-- @description: Demonstrates SQL-annotated metrics for observability

-- -----------------------------------------------------------------------------
-- @name: metric_output_stream
-- @description: Stream with counter and gauge metrics
-- -----------------------------------------------------------------------------
-- @metric: velo_metric_demo_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by metric demo"
--
-- @metric: velo_metric_demo_amount
-- @metric_type: gauge
-- @metric_help: "Current amount value"
-- @metric_field: amount
-- @metric_labels: id
--
-- @metric: velo_metric_demo_value_distribution
-- @metric_type: histogram
-- @metric_help: "Distribution of values processed"
-- @metric_field: value

CREATE STREAM metric_output_stream AS
SELECT
    id PRIMARY KEY,
    value,
    amount,
    count,
    active,
    event_time
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_metric_input',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'metric_output_stream.type' = 'kafka_sink',
    'metric_output_stream.topic.name' = 'test_metric_output',
    'metric_output_stream.config_file' = '../configs/output_stream_sink.yaml'
);
