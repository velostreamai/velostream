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
-- STREAMING USE CASE:
--   - Show all products, even those without orders
--   - Complete inventory views with optional transaction data
--   - Master data completeness checks
--
-- =============================================================================

-- @app: right_join_demo
-- @description: Show all products with their orders (if any)

-- Reference table: Products (all products must appear in output)
CREATE TABLE products AS
SELECT
    product_id PRIMARY KEY,
    product_name,
    category,
    unit_price,
    in_stock
FROM products_file
WITH (
    'products_file.type' = 'file_source',
    'products_file.config_file' = '../configs/products_table.yaml',

    'products.type' = 'memory_sink'
);

-- Source stream: Orders
CREATE STREAM orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    quantity * unit_price AS order_total,
    event_time
FROM orders_raw
EMIT CHANGES
WITH (
    'orders_raw.type' = 'kafka_source',
    'orders_raw.topic.name' = 'test_orders',
    'orders_raw.config_file' = '../configs/orders_source.yaml',

    'orders.type' = 'kafka_sink',
    'orders.topic.name' = 'test_orders_stream',
    'orders.config_file' = '../configs/orders_sink.yaml'
);

-- RIGHT JOIN: All products with optional order data
CREATE STREAM product_order_status AS
SELECT
    p.product_id AS product_id,
    p.product_name AS product_name,
    p.category AS category,
    p.unit_price AS list_price,
    o.order_id AS order_id,
    o.customer_id AS customer_id,
    o.quantity AS quantity,
    o.order_total AS order_total,
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
    'orders.topic.name' = 'test_orders_stream',
    'orders.config_file' = '../configs/orders_source.yaml',

    'products.type' = 'table_source',

    'product_order_status.type' = 'kafka_sink',
    'product_order_status.topic.name' = 'test_product_order_status',
    'product_order_status.config_file' = '../configs/orders_sink.yaml'
);
