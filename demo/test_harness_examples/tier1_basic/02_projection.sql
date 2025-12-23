-- Tier 1: Projection (SELECT specific columns)
-- Tests: Column selection and aliasing
-- Expected: Only selected columns in output with correct aliases

-- Application metadata
-- @name projection_demo
-- @description Column projection and aliasing

CREATE STREAM projected_output AS
SELECT
    id AS record_id,
    value AS record_value,
    amount * 1.1 AS adjusted_amount,
    event_time
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'projected_output.type' = 'kafka_sink',
    'projected_output.topic.name' = 'test_projection_output',
    'projected_output.config_file' = '../configs/output_stream_sink.yaml'
);
