-- Tier 2: Sum/Avg Aggregation
-- Tests: SUM, AVG, MIN, MAX with GROUP BY
-- Expected: Correct aggregate values per group

-- Application metadata
-- @name sum_avg_demo
-- @description SUM, AVG, MIN, MAX aggregations

-- Source definition
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
    'topic' = 'order_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Calculate order statistics per region
CREATE STREAM region_stats AS
SELECT
    region,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_quantity,
    AVG(unit_price) AS avg_price,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    SUM(quantity * unit_price) AS total_revenue
FROM order_events
GROUP BY region
WINDOW TUMBLING (INTERVAL '5' MINUTE);

-- Sink definition
CREATE SINK region_stats_sink FOR region_stats WITH (
    'connector' = 'kafka',
    'topic' = 'region_stats',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
