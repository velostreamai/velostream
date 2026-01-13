-- SQL Application: stream_table_join_demo
-- Version: 1.0.0
-- Description: Stream enrichment via table lookup
-- =============================================================================
-- Tier 3: Stream-Table Join
-- =============================================================================
--
-- Tests: JOIN stream with reference table
-- Expected: Enriched stream with lookup data
--
-- =============================================================================

-- @app: stream_table_join_demo
-- @description: Stream enrichment via table lookup

CREATE STREAM enriched_orders AS
SELECT
    o.order_id AS order_id,
    o.customer_id AS customer_id,
    o.product_id AS product_id,
    o.quantity AS quantity,
    o.unit_price AS unit_price,
    o.quantity * o.unit_price AS order_total,
    p.product_name AS product_name,
    p.category AS category,
    p.supplier_id AS supplier_id,
    o.event_time AS event_time
FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'products.type' = 'file_source',
    'products.config_file' = '../configs/products_table.yaml',

    'enriched_orders.type' = 'kafka_sink',
    'enriched_orders.topic.name' = 'test_enriched_orders',
    'enriched_orders.config_file' = '../configs/orders_sink.yaml'
);
