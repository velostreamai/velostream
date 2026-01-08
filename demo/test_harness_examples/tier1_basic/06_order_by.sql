-- SQL Application: order_by_demo
-- Version: 1.0.0
-- Description: Sort stream output by amount descending
-- =============================================================================
-- Tier 1: ORDER BY (Sort Results)
-- =============================================================================
--
-- WHAT IS ORDER BY?
-- -----------------
-- ORDER BY sorts the output of your query. In streaming SQL:
--   - ASC (ascending) is the default - smallest values first
--   - DESC (descending) - largest values first
--   - Multiple columns: sorts by first column, then second, etc.
--
-- STREAMING CONSIDERATIONS:
-- In continuous streaming, ORDER BY is typically used with:
--   - Windowed aggregations (sort within each window)
--   - TOP N queries (combined with LIMIT)
--   - Final output streams where order matters
--
-- SYNTAX:
--   SELECT ... FROM stream
--   ORDER BY column1 [ASC|DESC], column2 [ASC|DESC], ...
--
-- =============================================================================

-- @app: order_by_demo
-- @description: Sort stream output by amount descending

CREATE STREAM ordered_output AS
SELECT
    id,
    value,
    amount,
    event_time
FROM input_stream
WHERE amount > 0
ORDER BY amount DESC, event_time ASC    -- Highest amounts first, then oldest
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'ordered_output.type' = 'kafka_sink',
    'ordered_output.topic.name' = 'test_ordered_output',
    'ordered_output.config_file' = '../configs/output_stream_sink.yaml'
);
