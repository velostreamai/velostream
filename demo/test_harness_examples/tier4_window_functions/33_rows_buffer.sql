-- Tier 4: ROWS WINDOW BUFFER
-- Tests: ROWS WINDOW BUFFER N ROWS
-- Expected: Correct row-count windowing

-- Application metadata
-- @name rows_buffer_demo
-- @description ROWS WINDOW BUFFER patterns

CREATE STREAM buffered_analysis AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 50 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS avg_50,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 200 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS avg_200,
    STDDEV(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS stddev_100,
    MIN(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS min_100,
    MAX(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS max_100
FROM market_data
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic.name' = 'test_market_data',
    'market_data.config_file' = 'configs/market_data_source.yaml',

    'buffered_analysis.type' = 'kafka_sink',
    'buffered_analysis.topic.name' = 'test_buffered_analysis',
    'buffered_analysis.config_file' = 'configs/market_data_sink.yaml'
);
