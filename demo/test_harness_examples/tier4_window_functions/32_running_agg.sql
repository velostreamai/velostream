-- Tier 4: Running Aggregates
-- Tests: Running SUM, AVG over ordered window
-- Expected: Cumulative aggregations

-- Application metadata
-- @name running_agg_demo
-- @description Running SUM and AVG for cumulative metrics

-- Source definition
CREATE SOURCE transaction_stream (
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
    'topic' = 'transaction_stream',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Calculate running totals per customer
CREATE STREAM running_totals AS
SELECT
    customer_id,
    order_id,
    quantity * unit_price AS order_value,
    SUM(quantity * unit_price) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    AVG(quantity * unit_price) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_avg,
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS order_count,
    event_time
FROM transaction_stream;

-- Sink definition
CREATE SINK running_totals_sink FOR running_totals WITH (
    'connector' = 'kafka',
    'topic' = 'running_totals',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
