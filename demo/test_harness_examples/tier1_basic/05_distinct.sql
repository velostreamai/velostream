-- SQL Application: distinct_demo
-- Version: 1.0.0
-- Description: Remove duplicate values from stream
-- =============================================================================
-- Tier 1: DISTINCT (Remove Duplicates)
-- =============================================================================
--
-- WHAT IS DISTINCT?
-- -----------------
-- DISTINCT removes duplicate rows from your result set. In streaming SQL,
-- this is useful for:
--   - Deduplicating events that may arrive multiple times
--   - Getting unique values from a stream
--   - Reducing data volume by eliminating redundant records
--
-- SYNTAX:
--   SELECT DISTINCT column1, column2, ...
--   FROM stream
--
-- NOTE: DISTINCT considers NULL values as equal for deduplication purposes.
--
-- =============================================================================

-- @app: distinct_demo
-- @description: Remove duplicate values from stream

CREATE STREAM distinct_output AS
SELECT DISTINCT
    value,                    -- Only unique combinations of value + active
    active
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'distinct_output.type' = 'kafka_sink',
    'distinct_output.topic.name' = 'test_distinct_output',
    'distinct_output.config_file' = '../configs/output_stream_sink.yaml'
);
