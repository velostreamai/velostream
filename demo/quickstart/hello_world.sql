-- SQL Application: hello_world
-- Version: 1.0.0
-- Description: Your First Streaming SQL Query - Passthrough all data
-- =============================================================================
--
-- This is the simplest possible Velostream query. It reads data from a CSV
-- file and writes it unchanged to another CSV file (a "passthrough").
--
-- WHAT YOU'LL LEARN:
--   1. CREATE STREAM syntax - creates a continuous streaming query
--   2. SELECT * - selects all columns from the source
--   3. FROM - specifies where data comes from
--   4. EMIT CHANGES - outputs each row as it arrives
--   5. WITH clause - configures sources and sinks
--
-- NO KAFKA REQUIRED! This demo uses file-based I/O.
--
-- RUN IT:
--   velo-test run hello_world.sql -y
--
-- CHECK OUTPUT:
--   cat output/hello_world_output.csv
--
-- =============================================================================

CREATE STREAM hello_world AS

-- SELECT * means "pass through all columns unchanged"
SELECT *

-- FROM specifies the data source (configured in WITH clause below)
FROM input_data

-- EMIT CHANGES outputs each record as soon as it's processed
EMIT CHANGES

-- WITH clause configures the data source and sink
WITH (
    -- =========================================================================
    -- SOURCE CONFIGURATION
    -- =========================================================================
    -- 'input_data.type' tells Velostream to read from a file (not Kafka)
    'input_data.type' = 'file_source',

    -- 'input_data.path' is the path to the input file
    'input_data.path' = './hello_world_input.csv',

    -- 'input_data.format' specifies the file format (csv, json, etc.)
    'input_data.format' = 'csv',

    -- =========================================================================
    -- SINK CONFIGURATION (uses stream name 'hello_world')
    -- =========================================================================
    -- 'hello_world.type' tells Velostream to write to a file (not Kafka)
    'hello_world.type' = 'file_sink',

    -- 'hello_world.path' is where the output will be written
    'hello_world.path' = './output/hello_world_output.csv',

    -- 'hello_world.format' specifies the output format
    'hello_world.format' = 'csv'
);
