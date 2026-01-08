-- SQL Application: debug_mode_demo
-- Version: 1.0.0
-- Description: Multi-stage pipeline for step-through debugging
-- =============================================================================
-- Tier 8: Debug Mode (Step-by-Step Execution)
-- =============================================================================
--
-- WHAT IS DEBUG MODE?
-- -------------------
-- Debug mode enables interactive step-by-step execution:
--   - Pause after each statement
--   - Inspect intermediate results
--   - View state at each pipeline stage
--   - Set breakpoints on specific statements
--
-- RUNNING IN DEBUG MODE:
--   velo-test debug 73_debug_mode.sql
--
-- DEBUG COMMANDS (during execution):
--   s, step       - Execute next statement
--   c, continue   - Run until next breakpoint
--   r, run        - Run all remaining statements
--   b <name>      - Set breakpoint on statement
--   u <name>      - Remove breakpoint
--   l, list       - List all statements
--   i <name>      - Inspect output from statement
--   st, status    - Show current state
--   head <name>   - Show first N records
--   tail <name>   - Show last N records
--   q, quit       - Exit debugger
--
-- USE CASES:
--   1. Understanding pipeline behavior
--   2. Debugging complex transformations
--   3. Validating business logic
--   4. Learning streaming SQL concepts
--
-- =============================================================================

-- @app: debug_mode_demo
-- @description: Multi-stage pipeline for step-through debugging
-- @job_mode: simple

-- Stage 1: Clean and validate input
CREATE STREAM validated_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    -- Calculate derived fields (good to step through)
    quantity * unit_price AS line_total,
    event_time
FROM raw_orders
WHERE quantity > 0
  AND unit_price > 0
EMIT CHANGES
WITH (
    'raw_orders.type' = 'kafka_source',
    'raw_orders.topic.name' = 'test_raw_orders',
    'raw_orders.config_file' = '../configs/orders_source.yaml',

    'validated_orders.type' = 'kafka_sink',
    'validated_orders.topic.name' = 'test_validated_orders',
    'validated_orders.config_file' = '../configs/output_stream_sink.yaml'
);

-- Stage 2: Enrich with category
CREATE STREAM enriched_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    -- Categorization logic (step through to verify)
    CASE
        WHEN line_total > 1000 THEN 'PREMIUM'
        WHEN line_total > 100 THEN 'STANDARD'
        ELSE 'BASIC'
    END AS order_tier,
    CASE
        WHEN quantity >= 100 THEN 'BULK_ORDER'
        WHEN quantity >= 10 THEN 'MULTI_UNIT'
        ELSE 'SINGLE_UNIT'
    END AS order_type,
    event_time
FROM validated_orders
EMIT CHANGES
WITH (
    'validated_orders.type' = 'kafka_source',
    'validated_orders.topic.name' = 'test_validated_orders',
    'validated_orders.config_file' = '../configs/orders_source.yaml',

    'enriched_orders.type' = 'kafka_sink',
    'enriched_orders.topic.name' = 'test_enriched_orders',
    'enriched_orders.config_file' = '../configs/output_stream_sink.yaml'
);

-- Stage 3: Final output with all enrichments
CREATE STREAM final_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    order_tier,
    order_type,
    -- Combine tier and type for routing
    order_tier || '_' || order_type AS routing_key,
    event_time
FROM enriched_orders
EMIT CHANGES
WITH (
    'enriched_orders.type' = 'kafka_source',
    'enriched_orders.topic.name' = 'test_enriched_orders',
    'enriched_orders.config_file' = '../configs/orders_source.yaml',

    'final_orders.type' = 'kafka_sink',
    'final_orders.topic.name' = 'test_final_orders',
    'final_orders.config_file' = '../configs/output_stream_sink.yaml'
);
