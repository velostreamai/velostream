-- SQL Application: fault_injection_demo
-- Version: 1.0.0
-- Description: Demonstrate fault injection for resilience testing
-- =============================================================================
-- Tier 8: Fault Injection (Chaos Testing)
-- =============================================================================
--
-- WHAT IS FAULT INJECTION?
-- ------------------------
-- Fault injection intentionally introduces failures to test resilience:
--   - Malformed records (bad data)
--   - Duplicate messages
--   - Out-of-order events
--   - Slow processing
--   - Field corruption
--
-- WHY USE FAULT INJECTION?
--   1. Verify error handling works correctly
--   2. Test DLQ and recovery mechanisms
--   3. Find edge cases before production
--   4. Build confidence in system reliability
--
-- FAULT TYPES:
--   - missing_field: Remove required fields
--   - wrong_type: Use incorrect data types
--   - invalid_json: Produce malformed JSON
--   - overflow: Exceed field length limits
--   - null_value: Inject unexpected nulls
--
-- =============================================================================

-- @app: fault_injection_demo
-- @description: Demonstrate fault injection for resilience testing
-- @job_mode: simple
-- @enable_dlq: true

CREATE STREAM resilient_output AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    -- Calculations that might fail on bad data
    quantity * unit_price AS total_amount,
    CASE
        WHEN quantity > 100 THEN 'BULK'
        WHEN quantity > 10 THEN 'STANDARD'
        ELSE 'SMALL'
    END AS order_size,
    event_time
FROM orders
WHERE quantity > 0
  AND unit_price > 0
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'resilient_output.type' = 'kafka_sink',
    'resilient_output.topic.name' = 'test_resilient_output',
    'resilient_output.config_file' = '../configs/output_stream_sink.yaml',

    -- Enable DLQ to capture failures
    'job.enable_dlq' = 'true',
    'job.dlq_max_size' = '200'
);
