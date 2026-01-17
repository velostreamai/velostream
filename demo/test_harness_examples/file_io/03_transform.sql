-- SQL Application: file_transform_demo
-- Version: 1.0.0
-- Description: Transform trades with calculations using file I/O
-- =============================================================================
-- FILE I/O: Transformations and Calculations
-- =============================================================================
--
-- This example demonstrates transforming data with calculations and CASE.
-- No Kafka required - just file input and output!
--
-- WHAT YOU'LL LEARN:
--   1. Arithmetic expressions (price * volume)
--   2. CASE expressions for conditional logic
--   3. Column aliasing with AS
--
-- RUN IT:
--   velo-test run 03_transform.sql -y
--
-- =============================================================================

CREATE STREAM enriched_trades AS
SELECT
    symbol,
    price,
    volume,
    -- Calculate total trade value
    price * volume AS trade_value,
    -- Add symbol category based on first letter
    CASE
        WHEN symbol LIKE 'A%' THEN 'A-Group'
        WHEN symbol LIKE 'G%' THEN 'G-Group'
        WHEN symbol LIKE 'M%' THEN 'M-Group'
        ELSE 'Other'
    END AS symbol_group,
    timestamp
FROM trades
EMIT CHANGES
WITH (
    'trades.type' = 'file_source',
    'trades.path' = './input_trades.csv',
    'trades.format' = 'csv',

    'enriched_trades.type' = 'file_sink',
    'enriched_trades.path' = './03_transform_output.csv',
    'enriched_trades.format' = 'csv'
);
