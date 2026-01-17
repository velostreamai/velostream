-- SQL Application: stress_test_demo
-- Version: 1.0.0
-- Description: High-volume stress test with fault tolerance
-- =============================================================================
-- Tier 8: Stress Testing
-- =============================================================================
--
-- WHAT IS STRESS TESTING?
-- -----------------------
-- Stress testing validates system behavior under extreme conditions:
--   - High message volume
--   - Combined with fault injection
--   - Verifies no memory leaks
--   - Confirms DLQ limits work
--   - Tests backpressure handling
--
-- STRESS TEST MODES:
--   1. Volume: High record count
--   2. Rate: High records per second
--   3. Duration: Extended runtime
--   4. Chaos: Combined volume + faults
--
-- RUNNING STRESS TESTS:
--   velo-test stress query.sql --records 100000 --rate 5000
--   velo-test stress query.sql --duration 5m --faults
--
-- =============================================================================

-- @app: stress_test_demo
-- @description: High-volume stress test with fault tolerance
-- @job_mode: simple
-- @enable_dlq: true
-- @dlq_max_size: 1000

CREATE STREAM high_volume_output AS
SELECT
    trade_id,
    symbol,
    quantity,
    price,
    quantity * price AS notional_value,
    side,
    CASE
        WHEN quantity * price > 100000 THEN 'LARGE'
        WHEN quantity * price > 10000 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS trade_size,
    event_time
FROM trades
WHERE quantity > 0
  AND price > 0
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic.name' = 'test_trades',
    'trades.config_file' = '../configs/trades_source.yaml',

    'high_volume_output.type' = 'kafka_sink',
    'high_volume_output.topic.name' = 'test_high_volume_output',
    'high_volume_output.config_file' = '../configs/output_stream_sink.yaml',

    -- DLQ for capturing failures during stress
    'job.enable_dlq' = 'true',
    'job.dlq_max_size' = '1000',

    -- Performance tuning for high volume
    'producer.batch.size' = '32768',
    'producer.linger.ms' = '5',
    'producer.compression.type' = 'lz4'
);
