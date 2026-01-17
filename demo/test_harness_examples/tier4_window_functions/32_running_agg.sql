-- SQL Application: running_agg_demo
-- Version: 1.0.0
-- Description: Running aggregate window functions
-- =============================================================================
-- Tier 4: Running Aggregates
-- =============================================================================
--
-- Tests: Running SUM, AVG, COUNT with OVER
-- Expected: Cumulative calculations
--
-- =============================================================================

-- @app: running_agg_demo
-- @description: Running aggregate window functions

CREATE STREAM running_stats AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    SUM(volume) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS cumulative_volume,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS moving_avg_100,
    COUNT(*) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS trade_count
FROM market_data
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = '../configs/market_data_source.yaml',

    'running_stats.type' = 'kafka_sink',
    'running_stats.topic.name' = 'test_running_stats',
    'running_stats.config_file' = '../configs/market_data_sink.yaml'
);
