-- Tier 2: Tumbling Window
-- Tests: WINDOW TUMBLING with various intervals
-- Expected: Non-overlapping time buckets

-- Application metadata
-- @name tumbling_window_demo
-- @description Tumbling window aggregation

-- Source definition
CREATE SOURCE market_ticks (
    symbol STRING,
    price DECIMAL(10,4),
    volume INTEGER,
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    exchange STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'market_ticks',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- 1-minute OHLCV bars
CREATE STREAM ohlcv_1m AS
SELECT
    symbol,
    MIN(price) AS low,
    MAX(price) AS high,
    SUM(volume) AS total_volume,
    COUNT(*) AS tick_count,
    AVG(ask - bid) AS avg_spread
FROM market_ticks
GROUP BY symbol
WINDOW TUMBLING (INTERVAL '1' MINUTE)
EMIT CHANGES;

-- Sink definition
CREATE SINK ohlcv_1m_sink FOR ohlcv_1m WITH (
    'connector' = 'kafka',
    'topic' = 'ohlcv_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
