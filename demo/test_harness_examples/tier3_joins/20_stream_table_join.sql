-- Tier 3: Stream-Table JOIN
-- Tests: LEFT JOIN with file/table source
-- Expected: Stream enriched with reference data

-- Application metadata
-- @name stream_table_join_demo
-- @description Enrich order stream with customer data

-- Stream source: orders
CREATE SOURCE order_stream (
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
    'topic' = 'order_stream',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Table source: customer reference data
CREATE TABLE customers (
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

-- Enrich orders with customer info
CREATE STREAM enriched_orders AS
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.tier AS customer_tier,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS total_amount,
    o.status,
    o.event_time
FROM order_stream o
LEFT JOIN customers c ON o.customer_id = c.customer_id;

-- Sink definition
CREATE SINK enriched_orders_sink FOR enriched_orders WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
