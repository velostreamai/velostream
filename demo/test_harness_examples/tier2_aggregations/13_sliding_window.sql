-- Tier 2: Sliding Window
-- Tests: WINDOW SLIDING with size and slide
-- Expected: Overlapping time windows

-- Application metadata
-- @name sliding_window_demo
-- @description Sliding window aggregation for moving averages

-- Source definition
CREATE SOURCE price_feed (
    symbol STRING,
    price DECIMAL(10,4),
    volume INTEGER,
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    exchange STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'price_feed',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- 5-minute moving average, updated every minute
CREATE STREAM moving_avg_5m AS
SELECT
    symbol,
    AVG(price) AS avg_price,
    AVG(volume) AS avg_volume,
    COUNT(*) AS sample_count,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM price_feed
GROUP BY symbol
WINDOW SLIDING (SIZE INTERVAL '5' MINUTE, SLIDE INTERVAL '1' MINUTE)
EMIT CHANGES;

-- Sink definition
CREATE SINK moving_avg_sink FOR moving_avg_5m WITH (
    'connector' = 'kafka',
    'topic' = 'moving_avg_5m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
