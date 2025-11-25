-- Tier 6: Empty/Zero Records Handling
-- Tests: Zero input records, empty aggregation groups
-- Expected: Graceful handling of empty datasets

-- Application metadata
-- @name empty_demo
-- @description Empty dataset and zero record handling

-- Source definition
CREATE SOURCE transactions (
    transaction_id STRING,
    account_id STRING,
    amount DECIMAL(10,2),
    transaction_type STRING,
    category STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions_empty',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Simple passthrough - should handle zero records gracefully
CREATE STREAM all_transactions AS
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_type,
    category,
    event_time
FROM transactions;

-- Aggregation that might have empty groups
CREATE STREAM category_totals AS
SELECT
    category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY category
WINDOW TUMBLING(INTERVAL '1' MINUTE);

-- Sink definitions
CREATE SINK all_transactions_sink FOR all_transactions WITH (
    'connector' = 'kafka',
    'topic' = 'all_transactions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

CREATE SINK category_totals_sink FOR category_totals WITH (
    'connector' = 'kafka',
    'topic' = 'category_totals',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
