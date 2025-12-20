-- Tier 2: Sliding Window Aggregation
-- Tests: WINDOW SLIDING with size and advance
-- Expected: Overlapping window aggregation

-- Application metadata
-- @name sliding_window_demo
-- @description Sliding window aggregation

CREATE TABLE sliding_output AS
SELECT
    symbol PRIMARY KEY,
    COUNT(*) AS trade_count,
    AVG(price) AS moving_avg,
    STDDEV(price) AS price_stddev,
    _window_start AS window_start,
    _window_end AS window_end
FROM market_data
GROUP BY symbol
WINDOW SLIDING(5m, 1m)
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = 'configs/market_data_source.yaml',

    'sliding_output.type' = 'kafka_sink',
    'sliding_output.topic.name' = 'test_sliding_output',
    'sliding_output.config_file' = 'configs/aggregates_sink.yaml'
);
