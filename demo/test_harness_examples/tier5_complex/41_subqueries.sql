-- Tier 5: Subqueries
-- Tests: IN (SELECT ...), EXISTS patterns
-- Expected: Correct subquery evaluation

-- Application metadata
-- @name subqueries_demo
-- @description Subquery patterns for filtering

-- Source definition
CREATE SOURCE all_orders (
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
    'topic' = 'all_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- High-value customer reference
CREATE TABLE vip_customers (
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

-- Filter orders from VIP customers using subquery pattern
CREATE STREAM vip_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_total,
    o.status,
    o.event_time
FROM all_orders o
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
);

-- Sink definition
CREATE SINK vip_orders_sink FOR vip_orders WITH (
    'connector' = 'kafka',
    'topic' = 'vip_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
