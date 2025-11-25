-- Tier 4: ROW_NUMBER/RANK Functions
-- Tests: ROW_NUMBER and RANK window functions
-- Expected: Sequential numbering within partitions

-- Application metadata
-- @name row_number_demo
-- @description ROW_NUMBER and RANK for ordering within groups

-- Source definition
CREATE SOURCE sales_events (
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
    'topic' = 'sales_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Rank orders by value within each region
CREATE STREAM ranked_orders AS
SELECT
    order_id,
    region,
    customer_id,
    quantity * unit_price AS order_value,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY quantity * unit_price DESC) AS value_rank,
    RANK() OVER (PARTITION BY region ORDER BY quantity * unit_price DESC) AS dense_rank,
    event_time
FROM sales_events;

-- Sink definition
CREATE SINK ranked_orders_sink FOR ranked_orders WITH (
    'connector' = 'kafka',
    'topic' = 'ranked_orders',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
