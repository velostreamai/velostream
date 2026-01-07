-- SQL Application: aggregate_demo
-- Version: 1.0.0
-- Description: Aggregate data with COUNT, SUM, AVG
-- =============================================================================
-- LESSON 3: Aggregations (COUNT, SUM, AVG)
-- =============================================================================
--
-- This example shows how to aggregate data using GROUP BY and aggregate
-- functions like COUNT, SUM, and AVG.
--
-- WHAT YOU'LL LEARN:
--   1. GROUP BY - groups rows by a column value
--   2. COUNT(*) - counts rows in each group
--   3. SUM(column) - adds up values in a column
--   4. AVG(column) - calculates the average
--   5. CREATE TABLE vs CREATE STREAM - tables are for aggregated results
--   6. PRIMARY KEY - required for tables, identifies unique rows
--
-- THIS QUERY:
--   Groups transactions by category and calculates totals
--
-- RUN IT:
--   velo-test run 03_aggregate.sql -y
--
-- =============================================================================

-- Note: We use CREATE TABLE (not STREAM) for aggregations
-- because aggregated results are "materialized" and updated over time
CREATE TABLE category_stats AS

SELECT
    -- PRIMARY KEY is required for tables - it identifies each unique row
    category PRIMARY KEY,

    -- COUNT(*) counts the number of rows in each category
    COUNT(*) AS transaction_count,

    -- SUM adds up all values in the group
    SUM(amount) AS total_amount,

    -- AVG calculates the average value
    AVG(amount) AS avg_amount

FROM transactions

-- GROUP BY specifies how to group the data
-- All rows with the same category are grouped together
GROUP BY category

EMIT CHANGES

WITH (
    'transactions.type' = 'file_source',
    'transactions.path' = './03_aggregate_input.csv',
    'transactions.format' = 'csv',

    'category_stats.type' = 'file_sink',
    'category_stats.path' = './output/03_aggregate_output.csv',
    'category_stats.format' = 'csv'
);
