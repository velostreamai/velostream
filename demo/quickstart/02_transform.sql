-- SQL Application: transform_demo
-- Version: 1.0.0
-- Description: Transform columns with calculations and functions
-- =============================================================================
-- LESSON 2: Transforming Columns
-- =============================================================================
--
-- This example shows how to transform data by selecting specific columns,
-- renaming them, and creating calculated columns.
--
-- WHAT YOU'LL LEARN:
--   1. SELECT specific columns instead of *
--   2. Column aliasing with AS
--   3. Arithmetic expressions (value * 2)
--   4. String functions (UPPER)
--
-- THIS QUERY:
--   - Selects only id and name columns
--   - Creates a new column 'doubled_value' (value * 2)
--   - Creates 'name_upper' using UPPER() function
--
-- RUN IT:
--   velo-test run 02_transform.sql -y
--
-- =============================================================================

CREATE STREAM transformed_data AS

SELECT
    -- Keep the original id
    id,

    -- Keep the original name
    name,

    -- Create a new calculated column: multiply value by 2
    value * 2 AS doubled_value,

    -- Create uppercase version of name using UPPER() function
    UPPER(name) AS name_upper

FROM input_data

EMIT CHANGES

WITH (
    'input_data.type' = 'file_source',
    'input_data.path' = './hello_world_input.csv',
    'input_data.format' = 'csv',

    'transformed_data.type' = 'file_sink',
    'transformed_data.path' = './output/02_transform_output.csv',
    'transformed_data.format' = 'csv'
);
