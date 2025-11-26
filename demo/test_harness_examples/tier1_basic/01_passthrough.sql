-- Tier 1: Passthrough (SELECT *)
-- Tests: Basic data flow without transformation
-- Expected: All input records pass through unchanged

-- Application metadata
-- @name passthrough_demo
-- @description Simple passthrough stream - all records flow unchanged

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
    'input_stream.config_file' = 'configs/input_stream_source.yaml',

    'output_stream.type' = 'kafka_sink',
    'output_stream.topic.name' = 'test_passthrough_output',
    'output_stream.config_file' = 'configs/output_stream_sink.yaml'
);
