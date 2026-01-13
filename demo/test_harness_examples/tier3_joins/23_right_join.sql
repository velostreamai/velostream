-- SQL Application: right_join_demo
-- Version: 1.0.0
-- Description: Show all products with their orders (if any)
-- =============================================================================
-- Tier 3: RIGHT JOIN (Keep All Records from Right Table)
-- =============================================================================
--
-- WHAT IS RIGHT JOIN?
-- -------------------
-- RIGHT JOIN returns all records from the right table plus matching records
-- from the left table. Non-matching left records appear as NULL.
--
-- RIGHT JOIN vs LEFT JOIN:
--   LEFT JOIN:  All from LEFT,  matching from RIGHT
--   RIGHT JOIN: All from RIGHT, matching from LEFT
--
-- Equivalent: SELECT * FROM B RIGHT JOIN A ... = SELECT * FROM A LEFT JOIN B ...
--
-- STREAMING USE CASE:
--   - Show all products, even those without orders
--   - Complete inventory views with optional transaction data
--   - Master data completeness checks
--
-- SYNTAX:
--   SELECT ... FROM left_stream
--   RIGHT JOIN right_table ON condition
--
-- =============================================================================

-- @app: right_join_demo
-- @description: Show all products with their orders (if any)

CREATE STREAM product_order_status AS
SELECT
    p.product_id AS product_id,
    p.product_name AS product_name,
    p.category AS category,
    p.unit_price AS list_price,
    p.in_stock AS in_stock,
    o.order_id AS order_id,            -- NULL if no orders
    o.customer_id AS customer_id,      -- NULL if no orders
    o.quantity AS quantity,            -- NULL if no orders
    o.order_total AS order_total,      -- NULL if no orders
    CASE
        WHEN o.order_id IS NULL THEN 'NO_ORDERS'
        WHEN o.quantity > 100 THEN 'HIGH_VOLUME'
        ELSE 'NORMAL'
    END AS order_status,
    o.event_time AS event_time
FROM orders o
RIGHT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'products.type' = 'file_source',
    'products.config_file' = '../configs/products_table.yaml',

    'product_order_status.type' = 'kafka_sink',
    'product_order_status.topic.name' = 'test_product_order_status',
    'product_order_status.config_file' = '../configs/orders_sink.yaml'
);
