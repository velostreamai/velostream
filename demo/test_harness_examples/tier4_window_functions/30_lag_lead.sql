-- Tier 4: LAG/LEAD Functions
-- Tests: LAG and LEAD window functions
-- Expected: Access to previous/next row values

-- Application metadata
-- @name lag_lead_demo
-- @description LAG and LEAD for price change detection

-- Source definition
CREATE SOURCE price_stream (
    symbol STRING,
    price DECIMAL(10,4),
    volume INTEGER,
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    exchange STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'price_stream',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Calculate price changes using LAG
CREATE STREAM price_changes AS
SELECT
    symbol,
    price AS current_price,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY event_time) AS prev_price,
    LEAD(price, 1) OVER (PARTITION BY symbol ORDER BY event_time) AS next_price,
    price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY event_time) AS price_change,
    volume,
    event_time
FROM price_stream;

-- Sink definition
CREATE SINK price_changes_sink FOR price_changes WITH (
    'connector' = 'kafka',
    'topic' = 'price_changes',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
