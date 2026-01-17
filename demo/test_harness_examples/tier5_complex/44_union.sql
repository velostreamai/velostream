-- SQL Application: union_demo
-- Version: 1.0.0
-- Description: Combine multiple transaction streams
-- =============================================================================
-- Tier 5: UNION (Combine Multiple Streams)
-- =============================================================================
--
-- WHAT IS UNION?
-- --------------
-- UNION combines results from multiple SELECT statements:
--   - UNION: Removes duplicate rows (slower, deduplicates)
--   - UNION ALL: Keeps all rows including duplicates (faster)
--
-- REQUIREMENTS:
--   - Same number of columns in each SELECT
--   - Compatible column types (same or castable)
--   - Column names come from the first SELECT
--
-- STREAMING USE CASES:
--   - Merging events from multiple sources
--   - Combining regional data streams
--   - Creating unified views of similar data types
--
-- SYNTAX:
--   SELECT ... FROM stream1
--   UNION [ALL]
--   SELECT ... FROM stream2
--
-- =============================================================================

-- @app: union_demo
-- @description: Combine multiple transaction streams

-- Combine US, EU, and APAC transactions into a unified stream
CREATE STREAM all_transactions AS
-- US Transactions
SELECT
    order_id,
    customer_id,
    'US' AS region,
    amount,
    currency,
    event_time
FROM us_transactions

UNION ALL         -- Use UNION ALL for better performance (no dedup needed)

-- EU Transactions
SELECT
    order_id,
    customer_id,
    'EU' AS region,
    amount,
    currency,
    event_time
FROM eu_transactions

UNION ALL

-- APAC Transactions
SELECT
    order_id,
    customer_id,
    'APAC' AS region,
    amount,
    currency,
    event_time
FROM apac_transactions

EMIT CHANGES
WITH (
    'us_transactions.type' = 'kafka_source',
    'us_transactions.topic.name' = 'test_us_transactions',
    'us_transactions.config_file' = '../configs/transactions_source.yaml',

    'eu_transactions.type' = 'kafka_source',
    'eu_transactions.topic.name' = 'test_eu_transactions',
    'eu_transactions.config_file' = '../configs/transactions_source.yaml',

    'apac_transactions.type' = 'kafka_source',
    'apac_transactions.topic.name' = 'test_apac_transactions',
    'apac_transactions.config_file' = '../configs/transactions_source.yaml',

    'all_transactions.type' = 'kafka_sink',
    'all_transactions.topic.name' = 'test_all_transactions',
    'all_transactions.config_file' = '../configs/output_stream_sink.yaml'
);
