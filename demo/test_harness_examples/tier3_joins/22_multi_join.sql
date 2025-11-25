-- Tier 3: Multi-Table JOIN
-- Tests: Multiple JOINs in single query
-- Expected: Data enriched from multiple sources

-- Application metadata
-- @name multi_join_demo
-- @description Join orders with both customers and products

-- Stream source: orders
CREATE SOURCE order_events (
    order_id STRING,
    customer_id INTEGER,
    product_id STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    status STRING,
    region STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_events_multi',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Table source: customers
CREATE TABLE customer_lookup (
    customer_id INTEGER,
    name STRING,
    tier STRING,
    region STRING,
    signup_date DATE
) WITH (
    'connector' = 'file',
    'path' = 'data/customers.csv',
    'format' = 'csv'
);

-- Table source: products
CREATE TABLE product_lookup (
    product_id STRING,
    name STRING,
    category STRING,
    base_price DECIMAL(10,2)
) WITH (
    'connector' = 'file',
    'path' = 'data/products.csv',
    'format' = 'csv'
);

-- Fully enriched orders
CREATE STREAM fully_enriched_orders AS
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.tier AS customer_tier,
    o.product_id,
    p.name AS product_name,
    p.category AS product_category,
    o.quantity,
    o.unit_price,
    p.base_price,
    o.quantity * o.unit_price AS order_total,
    o.status,
    o.event_time
FROM order_events o
LEFT JOIN customer_lookup c ON o.customer_id = c.customer_id
LEFT JOIN product_lookup p ON o.product_id = p.product_id;

-- Sink definition
CREATE SINK fully_enriched_sink FOR fully_enriched_orders WITH (
    'connector' = 'kafka',
    'topic' = 'fully_enriched_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
