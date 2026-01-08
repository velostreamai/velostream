-- SQL Application: dlq_basic_demo
-- Version: 1.0.0
-- Description: Demonstrate basic DLQ functionality
-- =============================================================================
-- Tier 8: Dead Letter Queue (DLQ) Basics
-- =============================================================================
--
-- WHAT IS A DEAD LETTER QUEUE?
-- ----------------------------
-- A DLQ is a separate queue that captures records that fail processing:
--   - Failed records preserved for investigation
--   - Processing continues without crashing
--   - Error messages attached to failed records
--   - Enables post-incident analysis
--
-- DLQ ENTRY CONTENTS:
--   - Original record with all fields
--   - Error message explaining the failure
--   - Record index in the batch
--   - Whether error is recoverable
--   - Timestamp of failure
--
-- USE CASES:
--   1. Debug data quality issues
--   2. Monitor failure rates
--   3. Retry failed records after fixing issues
--   4. Comply with data processing regulations
--
-- =============================================================================

-- @app: dlq_basic_demo
-- @description: Demonstrate basic DLQ functionality
-- @job_mode: simple
-- @enable_dlq: true
-- @dlq_max_size: 100

CREATE STREAM processed_events AS
SELECT
    event_id,
    user_id,
    event_type,
    -- This calculation may fail on bad data
    CAST(amount AS DECIMAL(10,2)) AS amount,
    CAST(timestamp AS TIMESTAMP) AS event_time
FROM raw_events
WHERE event_type IS NOT NULL
EMIT CHANGES
WITH (
    'raw_events.type' = 'kafka_source',
    'raw_events.topic.name' = 'test_raw_events',
    'raw_events.config_file' = '../configs/events_source.yaml',

    'processed_events.type' = 'kafka_sink',
    'processed_events.topic.name' = 'test_processed_events',
    'processed_events.config_file' = '../configs/output_stream_sink.yaml',

    -- DLQ configuration
    'job.enable_dlq' = 'true',
    'job.dlq_max_size' = '100'
);
