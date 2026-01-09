-- SQL Application: count_demo
-- Version: 1.0.0
-- Description: COUNT aggregation patterns
-- =============================================================================
-- Tier 2: COUNT Aggregation
-- =============================================================================
--
-- Tests: COUNT(*) and COUNT(column)
-- Expected: Correct row counting with GROUP BY
--
-- =============================================================================

-- @app: count_demo
-- @description: COUNT aggregation patterns

CREATE STREAM count_output AS
SELECT
    active PRIMARY KEY,
    COUNT(*) AS total_count,
    COUNT(value) AS value_count
FROM input_stream
GROUP BY active
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'count_output.type' = 'kafka_sink',
    'count_output.topic.name' = 'test_count_output',
    'count_output.config_file' = '../configs/output_stream_sink.yaml'
);
