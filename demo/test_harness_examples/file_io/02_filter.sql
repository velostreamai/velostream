-- SQL Application: file_filter_demo
-- Version: 1.0.0
-- Description: Filter trades by volume using file I/O
-- =============================================================================
-- FILE I/O: Filtering with WHERE Clause
-- =============================================================================
--
-- This example demonstrates filtering data from a CSV file.
-- No Kafka required - just file input and output!
--
-- WHAT YOU'LL LEARN:
--   1. File-based source configuration
--   2. WHERE clause filtering
--   3. File-based sink configuration
--
-- RUN IT:
--   velo-test run 02_filter.sql -y
--
-- =============================================================================

CREATE STREAM high_volume_trades AS
SELECT
    symbol,
    price,
    volume,
    timestamp
FROM trades
-- Filter to only include trades with volume >= 100
WHERE volume >= 100
EMIT CHANGES
WITH (
    'trades.type' = 'file_source',
    'trades.path' = './input_trades.csv',
    'trades.format' = 'csv',

    'high_volume_trades.type' = 'file_sink',
    'high_volume_trades.path' = './02_filter_output.csv',
    'high_volume_trades.format' = 'csv'
);
