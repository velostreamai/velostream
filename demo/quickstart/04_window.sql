-- SQL Application: window_demo
-- Version: 1.0.0
-- Description: Window functions with LAG to compare to previous values
-- =============================================================================
-- LESSON 4: Window Functions (LAG, Running Totals)
-- =============================================================================
--
-- This is the most advanced quickstart example. Window functions let you
-- access data from other rows without using GROUP BY.
--
-- WHAT YOU'LL LEARN:
--   1. LAG(column, offset) - access the previous row's value
--   2. OVER clause - defines the "window" of rows to consider
--   3. ROWS WINDOW BUFFER - how many rows to keep in memory
--   4. PARTITION BY - group rows within the window
--   5. ORDER BY - order rows within each partition
--
-- STREAMING CONTEXT:
--   In streaming, we don't have "all the data" - data arrives continuously.
--   ROWS WINDOW BUFFER tells Velostream how many previous rows to remember.
--
-- THIS QUERY:
--   For each price update, shows the previous price and calculates the change.
--
-- RUN IT:
--   velo-test run 04_window.sql -y
--
-- =============================================================================

CREATE STREAM price_with_change AS

SELECT
    symbol,
    price,
    timestamp,

    -- LAG(price, 1) gets the price from 1 row ago (within same symbol)
    -- Returns NULL if there is no previous row
    LAG(price, 1) OVER (
        -- ROWS WINDOW BUFFER: how many rows to keep in memory
        -- For LAG(1), we only need 1 previous row, but buffer more for safety
        ROWS WINDOW BUFFER 10 ROWS

        -- PARTITION BY: calculate LAG separately for each symbol
        -- Without this, LAG would mix prices from different symbols
        PARTITION BY symbol

        -- ORDER BY: defines which row is "previous"
        ORDER BY timestamp
    ) AS previous_price,

    -- Calculate the price change (current - previous)
    price - LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 10 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    ) AS price_change

FROM stock_prices

EMIT CHANGES

WITH (
    'stock_prices.type' = 'file_source',
    'stock_prices.path' = './04_window_input.csv',
    'stock_prices.format' = 'csv',

    'price_with_change.type' = 'file_sink',
    'price_with_change.path' = './output/04_window_output.csv',
    'price_with_change.format' = 'csv'
);
