-- Tier 5: Complex Filtering
-- Tests: Multiple predicates, nested conditions, BETWEEN, LIKE
-- Expected: Correct compound filter evaluation

-- Application metadata
-- @name complex_filter_demo
-- @description Complex filter combinations and patterns

-- Source definition
CREATE SOURCE order_stream (
    order_id STRING,
    customer_id INTEGER,
    product_id STRING,
    category STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_pct DECIMAL(5,2),
    status STRING,
    region STRING,
    priority STRING,
    order_date DATE,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_stream_complex',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

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
    AND category IS NOT NULL;

-- Sink definition
CREATE SINK filtered_orders_sink FOR filtered_orders WITH (
    'connector' = 'kafka',
    'topic' = 'filtered_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
