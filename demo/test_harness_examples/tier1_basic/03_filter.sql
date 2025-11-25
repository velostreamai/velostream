-- Tier 1: Simple Filter
-- Tests: WHERE clause with comparison operators
-- Expected: Only matching records pass through

-- Application metadata
-- @name filter_demo
-- @description Filter records using WHERE clause

-- Source definition
CREATE SOURCE market_input (
    symbol STRING,
    price DECIMAL(10,4),
    volume INTEGER,
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    exchange STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'market_input',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Filter for high-volume trades on major exchanges
CREATE STREAM high_volume_trades AS
SELECT
    symbol,
    price,
    volume,
    exchange,
    event_time
FROM market_input
WHERE volume > 10000
  AND exchange IN ('NYSE', 'NASDAQ');

-- Sink definition
CREATE SINK high_volume_sink FOR high_volume_trades WITH (
    'connector' = 'kafka',
    'topic' = 'high_volume_trades',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
