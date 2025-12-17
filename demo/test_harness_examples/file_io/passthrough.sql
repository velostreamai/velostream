-- File-based trade analysis demo
-- Reads trade data from CSV, enriches it, and outputs to CSV
-- No Kafka required - pure file I/O!
--
-- @name file_io_demo
-- @description Demonstrates file-based SQL processing without Kafka

CREATE STREAM enriched_trades AS
SELECT
    symbol,
    price,
    volume,
    price * volume AS trade_value,
    CASE
        WHEN price * volume > 100000 THEN 'LARGE'
        WHEN price * volume > 50000 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS trade_size,
    timestamp
FROM trades
WHERE volume >= 50
EMIT CHANGES
WITH (
    -- File-based source configuration
    'trades.type' = 'file_source',
    'trades.path' = './input_trades.csv',
    'trades.format' = 'csv',

    -- File-based sink configuration
    'enriched_trades.type' = 'file_sink',
    'enriched_trades.path' = './output_trades.csv',
    'enriched_trades.format' = 'csv'
);
