-- Tier 4: ROWS WINDOW BUFFER
-- Tests: ROWS WINDOW BUFFER N ROWS
-- Expected: Row-count based windowing

-- Application metadata
-- @name rows_buffer_demo
-- @description ROWS WINDOW BUFFER for fixed-size moving calculations

-- Source definition
CREATE SOURCE tick_stream (
    symbol STRING,
    price DECIMAL(10,4),
    volume INTEGER,
    bid DECIMAL(10,4),
    ask DECIMAL(10,4),
    exchange STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'tick_stream',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- 10-tick moving average
CREATE STREAM moving_avg_10tick AS
SELECT
    symbol,
    price,
    AVG(price) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol) AS ma_10,
    MIN(price) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol) AS min_10,
    MAX(price) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol) AS max_10,
    volume,
    event_time
FROM tick_stream;

-- Sink definition
CREATE SINK moving_avg_10tick_sink FOR moving_avg_10tick WITH (
    'connector' = 'kafka',
    'topic' = 'moving_avg_10tick',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
