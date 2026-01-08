-- SQL Application: complex_filter_demo
-- Version: 1.0.0
-- Description: Complex filter combinations and patterns
-- =============================================================================
-- Tier 5: Complex Filtering
-- =============================================================================
--
-- Tests: Multiple predicates, nested conditions, BETWEEN
-- Expected: Correct compound filter evaluation
--
-- =============================================================================

-- @app: complex_filter_demo
-- @description: Complex filter combinations and patterns

-- Complex filter with multiple conditions
CREATE STREAM filtered_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    category,
    quantity,
    unit_price,
    discount_pct,
    quantity * unit_price AS subtotal,
    quantity * unit_price * (1 - discount_pct / 100) AS net_total,
    status,
    region,
    priority,
    event_time
FROM order_stream
WHERE
    -- Quantity range check
    quantity BETWEEN 1 AND 100
    -- Price threshold
    AND unit_price > 10.00
    -- Status filter with IN
    AND status IN ('confirmed', 'processing', 'shipped')
    -- Region filter with OR
    AND (region = 'US' OR region = 'EU' OR region = 'APAC')
    -- Priority filter
    AND priority IN ('high', 'medium')
    -- Exclude test orders
    AND customer_id > 0
    -- Valid product category
    AND category IS NOT NULL
EMIT CHANGES
WITH (
    'order_stream.type' = 'kafka_source',
    'order_stream.topic.name' = 'test_order_stream',
    'order_stream.config_file' = '../configs/orders_source.yaml',

    'filtered_orders.type' = 'kafka_sink',
    'filtered_orders.topic.name' = 'test_filtered_orders',
    'filtered_orders.config_file' = '../configs/orders_sink.yaml'
);
