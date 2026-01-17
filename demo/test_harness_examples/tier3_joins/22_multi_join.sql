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
-- =============================================================================

-- @app: multi_join_demo
-- @description: Multi-table join pattern

-- Reference table 1: Customers
CREATE TABLE customers AS
SELECT
    customer_id PRIMARY KEY,
    customer_name,
    tier,
    region
FROM customers_file
WITH (
    'customers_file.type' = 'file_source',
    'customers_file.config_file' = '../configs/customers_table.yaml',

    'customers.type' = 'memory_sink'
);

-- Reference table 2: Products
CREATE TABLE products AS
SELECT
    product_id PRIMARY KEY,
    product_name,
    category,
    unit_price,
    supplier_id
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
    unit_price,
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

-- Join query: Enrich orders with customer and product data
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
    'orders.topic.name' = 'test_orders_stream',
    'orders.config_file' = '../configs/orders_source.yaml',

    'customers.type' = 'table_source',
    'products.type' = 'table_source',

    'fully_enriched.type' = 'kafka_sink',
    'fully_enriched.topic.name' = 'test_fully_enriched',
    'fully_enriched.config_file' = '../configs/orders_sink.yaml',

    'join.timeout' = '30s'
);
