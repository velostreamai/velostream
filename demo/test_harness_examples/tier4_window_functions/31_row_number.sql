-- SQL Application: row_number_demo
-- Version: 1.0.0
-- Description: Ranking window functions
-- =============================================================================
-- Tier 4: ROW_NUMBER and RANK Functions
-- =============================================================================
--
-- Tests: ROW_NUMBER(), RANK(), DENSE_RANK()
-- Expected: Correct ranking values
--
-- =============================================================================

-- @app: row_number_demo
-- @description: Ranking window functions

CREATE STREAM ranked_trades AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    ROW_NUMBER() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS row_num,
    RANK() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY price DESC
    ) AS price_rank,
    DENSE_RANK() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY volume DESC
    ) AS volume_rank
FROM market_data
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = '../configs/market_data_source.yaml',

    'ranked_trades.type' = 'kafka_sink',
    'ranked_trades.topic.name' = 'test_ranked_trades',
    'ranked_trades.config_file' = '../configs/market_data_sink.yaml'
);
