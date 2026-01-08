-- SQL Application: filter_demo
-- Version: 1.0.0
-- Description: WHERE clause filtering
-- =============================================================================
-- Tier 1: Filter (WHERE clause)
-- =============================================================================
--
-- Tests: Row filtering with predicates
-- Expected: Only matching records pass through
--
-- =============================================================================

-- @app: filter_demo
-- @description: WHERE clause filtering

CREATE STREAM filtered_output AS
SELECT
    id,
    value,
    amount,
    active,
    event_time
FROM input_stream
WHERE amount > 100
  AND active = true
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'filtered_output.type' = 'kafka_sink',
    'filtered_output.topic.name' = 'test_filter_output',
    'filtered_output.config_file' = '../configs/output_stream_sink.yaml'
);
