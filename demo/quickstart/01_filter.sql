-- SQL Application: filter_demo
-- Version: 1.0.0
-- Description: Filter rows using WHERE clause
-- =============================================================================
-- LESSON 1: Filtering Data with WHERE
-- =============================================================================
--
-- Building on hello_world.sql, this example adds a WHERE clause to filter
-- which rows pass through the stream.
--
-- WHAT YOU'LL LEARN:
--   1. WHERE clause - filters rows based on a condition
--   2. Comparison operators: >, <, =, >=, <=, <>
--   3. Only matching rows appear in the output
--
-- THIS QUERY:
--   Filters to show only rows where value > 150
--   (Alice=100, Carol=150 are excluded; Bob=200, Dave=300, Eve=250 pass)
--
-- RUN IT:
--   velo-test run 01_filter.sql -y
--
-- EXPECTED OUTPUT:
--   3 rows (Bob, Dave, Eve)
--
-- =============================================================================

CREATE STREAM filtered_data AS

SELECT *

FROM input_data

-- WHERE clause filters rows: only rows where value > 150 pass through
-- Try changing 150 to different values and observe the output!
WHERE value > 150

EMIT CHANGES

WITH (
    'input_data.type' = 'file_source',
    'input_data.path' = './hello_world_input.csv',
    'input_data.format' = 'csv',

    'filtered_data.type' = 'file_sink',
    'filtered_data.path' = './output/01_filter_output.csv',
    'filtered_data.format' = 'csv'
);
