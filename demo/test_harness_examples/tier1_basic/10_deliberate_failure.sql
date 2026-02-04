-- SQL Application: deliberate_failure_demo
-- Version: 1.0.0
-- Description: Deliberately failing test to demonstrate error reporting
-- =============================================================================
-- Tier 1: Deliberate Failure
-- =============================================================================
--
-- This test is intentionally designed to fail, demonstrating the test harness
-- error reporting capabilities. DO NOT "fix" this test.
--
-- =============================================================================

-- @app: deliberate_failure_demo
-- @description: Deliberately failing test to demonstrate error reporting

CREATE STREAM output_stream AS
SELECT
    id,
    value,
    amount,
    count,
    active,
    event_time
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'output_stream.type' = 'kafka_sink',
    'output_stream.topic.name' = 'test_deliberate_failure_output',
    'output_stream.config_file' = '../configs/output_stream_sink.yaml'
);
