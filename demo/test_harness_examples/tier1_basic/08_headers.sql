-- SQL Application: headers_demo
-- Version: 1.0.0
-- Description: Kafka message header manipulation
-- =============================================================================
-- Tier 1: Headers (Read, Write, Check message headers)
-- =============================================================================
--
-- Tests: HEADER(), SET_HEADER(), REMOVE_HEADER(), HAS_HEADER(), HEADER_KEYS()
-- Expected: Header functions execute correctly and return expected values
--
-- =============================================================================

-- @app: headers_demo
-- @description: Kafka message header manipulation functions

CREATE STREAM headers_output AS
SELECT
    id,
    value,
    -- Read existing headers (will be NULL if not present in input)
    HEADER('trace-id') AS input_trace_id,
    HEADER('correlation-id') AS input_correlation_id,

    -- Check if headers exist (boolean result)
    HAS_HEADER('trace-id') AS has_trace_header,
    HAS_HEADER('nonexistent') AS has_nonexistent_header,

    -- Set new headers on output record (returns the value that was set)
    SET_HEADER('output-trace-id', CONCAT('trace-', CAST(id AS STRING))) AS set_trace_result,
    SET_HEADER('processed-by', 'velostream') AS set_processor_result,
    SET_HEADER('priority', 'high') AS set_priority_result,

    -- Set header from field value
    SET_HEADER('record-value', value) AS set_value_result,

    -- Get all header keys (comma-separated string)
    HEADER_KEYS() AS all_header_keys,

    event_time
FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_headers_input',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'headers_output.type' = 'kafka_sink',
    'headers_output.topic.name' = 'test_headers_output',
    'headers_output.config_file' = '../configs/output_stream_sink.yaml'
);
