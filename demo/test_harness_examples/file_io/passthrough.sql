-- SQL Application: file_io_demo
-- Version: 1.0.0
-- Description: Demonstrates file-based SQL processing without Kafka
-- =============================================================================
-- FILE I/O: Complete Trade Processing Example
-- =============================================================================
--
-- This example demonstrates a complete file-based SQL processing pipeline.
-- Reads trade data from CSV, enriches it, and outputs to CSV.
-- No Kafka required - pure file I/O!
--
-- WHAT YOU'LL LEARN:
--   1. File-based source and sink configuration
--   2. Calculated fields (price * volume)
--   3. CASE expressions for categorization
--   4. WHERE clause filtering
--
-- RUN IT:
--   velo-test run passthrough.sql -y
--
-- =============================================================================

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
