-- Tier 6: Empty/Zero Records Handling
-- Tests: Zero input records, empty aggregation groups
-- Expected: Graceful handling of empty datasets

-- Application metadata
-- @name empty_demo
-- @description Empty dataset and zero record handling

-- Simple passthrough - should handle zero records gracefully
CREATE STREAM all_transactions AS
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_type,
    category,
    event_time
FROM transactions
EMIT CHANGES
WITH (
    'transactions.type' = 'kafka_source',
    'transactions.topic.name' = 'test_transactions',
    'transactions.config_file' = 'configs/transactions_source.yaml',

    'all_transactions.type' = 'kafka_sink',
    'all_transactions.topic.name' = 'test_all_transactions',
    'all_transactions.config_file' = 'configs/output_stream_sink.yaml'
);

-- Aggregation that might have empty groups (uses CREATE TABLE for GROUP BY)
CREATE TABLE category_totals AS
SELECT
    category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    _window_start AS window_start,
    _window_end AS window_end
FROM transactions
GROUP BY category
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'transactions.type' = 'kafka_source',
    'transactions.topic.name' = 'test_transactions',
    'transactions.config_file' = 'configs/transactions_source.yaml',

    'category_totals.type' = 'kafka_sink',
    'category_totals.topic.name' = 'test_category_totals',
    'category_totals.config_file' = 'configs/aggregates_sink.yaml'
);
