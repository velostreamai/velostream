-- SQL Application: limit_demo
-- Version: 1.0.0
-- Description: Get top 10 records by amount
-- =============================================================================
-- Tier 1: LIMIT (Restrict Row Count)
-- =============================================================================
--
-- WHAT IS LIMIT?
-- --------------
-- LIMIT restricts the number of rows returned. Use cases:
--   - TOP N queries (combined with ORDER BY)
--   - Sampling data for testing
--   - Preventing unbounded result sets
--
-- STREAMING CONSIDERATIONS:
-- In streaming SQL, LIMIT affects how many records are emitted:
--   - With ORDER BY: Get the top/bottom N values
--   - Without ORDER BY: Get the first N records to arrive
--   - With windows: Limit applies within each window
--
-- SYNTAX:
--   SELECT ... FROM stream
--   [ORDER BY ...]
--   LIMIT N
--
-- COMMON PATTERNS:
--   SELECT * ORDER BY amount DESC LIMIT 10  -- Top 10 by amount
--   SELECT * ORDER BY event_time ASC LIMIT 1 -- First record
--
-- =============================================================================

-- @app: limit_demo
-- @description: Get top 10 records by amount

CREATE STREAM top_10_output AS
SELECT
    id,
    value,
    amount,
    event_time
FROM input_stream
WHERE amount > 0
ORDER BY amount DESC
LIMIT 10                      -- Only return top 10 records
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'top_10_output.type' = 'kafka_sink',
    'top_10_output.topic.name' = 'test_top_10_output',
    'top_10_output.config_file' = '../configs/output_stream_sink.yaml'
);
