-- Tier 2: Tumbling Window Aggregation
-- Tests: WINDOW TUMBLING with time intervals
-- Expected: Correct time-bucketed aggregation

-- Application metadata
-- @name tumbling_window_demo
-- @description Tumbling window aggregation

CREATE STREAM tumbling_output AS
SELECT
    symbol,
    COUNT(*) AS trade_count,
    SUM(volume) AS total_volume,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    _window_start AS window_start,
    _window_end AS window_end
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = 'configs/market_data_source.yaml',

    'tumbling_output.type' = 'kafka_sink',
    'tumbling_output.topic.name' = 'test_tumbling_output',
    'tumbling_output.config_file' = 'configs/aggregates_sink.yaml'
);
