-- Tier 4: LAG and LEAD Window Functions
-- Tests: LAG(column, offset) and LEAD(column, offset)
-- Expected: Previous and next row values

-- Application metadata
-- @name lag_lead_demo
-- @description LAG and LEAD window functions

CREATE STREAM price_changes AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS prev_price,
    LEAD(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS next_price,
    price - LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS price_change
FROM market_data
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = '../configs/market_data_source.yaml',

    'price_changes.type' = 'kafka_sink',
    'price_changes.topic.name' = 'test_price_changes',
    'price_changes.config_file' = '../configs/market_data_sink.yaml'
);
