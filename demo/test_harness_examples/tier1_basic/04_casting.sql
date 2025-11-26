-- Tier 1: Casting (CAST expressions)
-- Tests: Type conversion functions
-- Expected: Correct type conversions

-- Application metadata
-- @name casting_demo
-- @description Type casting and conversion

CREATE STREAM cast_output AS
SELECT
    id,
    CAST(amount AS INTEGER) AS amount_int,
    CAST(count AS DECIMAL) AS count_decimal,
    CAST(active AS STRING) AS active_string,
    CAST(id AS STRING) AS id_string,
    event_time
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = 'configs/input_stream_source.yaml',

    'cast_output.type' = 'kafka_sink',
    'cast_output.topic.name' = 'test_casting_output',
    'cast_output.config_file' = 'configs/output_stream_sink.yaml'
);
