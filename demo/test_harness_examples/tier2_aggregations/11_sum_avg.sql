-- Tier 2: SUM and AVG Aggregation
-- Tests: SUM() and AVG() functions
-- Expected: Correct sum and average calculations

-- Application metadata
-- @name sum_avg_demo
-- @description SUM and AVG aggregation patterns

CREATE TABLE sum_avg_output AS
SELECT
    category PRIMARY KEY,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT(*) AS record_count
FROM input_stream
GROUP BY category
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = 'configs/input_stream_source.yaml',

    'sum_avg_output.type' = 'kafka_sink',
    'sum_avg_output.topic.name' = 'test_sum_avg_output',
    'sum_avg_output.config_file' = 'configs/aggregates_sink.yaml'
);
