-- SQL Application: multi_join_demo
-- Version: 1.0.0
-- Description: Multi-table join pattern
-- =============================================================================
-- Tier 3: Multi-Table Join (Stream + Multiple Tables)
-- =============================================================================
--
-- Tests: JOIN stream with multiple reference tables
-- Expected: Fully enriched records with customer and product data
--
-- Pattern: Use file_source directly in JOIN (no separate CREATE TABLE needed)
--
-- =============================================================================

-- @app: multi_join_demo
-- @description: Multi-table join pattern

-- Join query: Enrich orders with customer and product data
-- References file sources directly for table lookups
CREATE STREAM fully_enriched AS
SELECT
    o.order_id AS order_id,
    o.customer_id AS customer_id,
    o.product_id AS product_id,
    o.quantity AS quantity,
    o.quantity * o.unit_price AS order_total,
    c.customer_name AS customer_name,
    c.tier AS customer_tier,
    c.region AS customer_region,
    p.product_name AS product_name,
    p.category AS product_category,
    o.event_time AS event_time
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'customers.type' = 'file_source',
    'customers.config_file' = '../configs/customers_table.yaml',

    'products.type' = 'file_source',
    'products.config_file' = '../configs/products_table.yaml',

    'fully_enriched.type' = 'kafka_sink',
    'fully_enriched.topic.name' = 'test_fully_enriched',
    'fully_enriched.config_file' = '../configs/orders_sink.yaml',

    'join.timeout' = '30s'
);
