-- Tier 5: Multi-Stage Pipeline
-- Tests: Multiple dependent streams
-- Expected: Correct data flow through stages

-- Application metadata
-- @name pipeline_demo
-- @description Multi-stage transformation pipeline

-- Stage 1: Clean and enrich transactions
CREATE STREAM cleaned_transactions AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price AS total_amount,
    status,
    region,
    event_time
FROM raw_transactions
WHERE quantity > 0
  AND unit_price > 0
EMIT CHANGES
WITH (
    'raw_transactions.type' = 'kafka_source',
    'raw_transactions.topic.name' = 'test_raw_transactions',
    'raw_transactions.config_file' = 'configs/transactions_source.yaml',

    'cleaned_transactions.type' = 'kafka_sink',
    'cleaned_transactions.topic.name' = 'test_cleaned_transactions',
    'cleaned_transactions.config_file' = 'configs/output_stream_sink.yaml'
);

-- Stage 2: Aggregate by region (uses CREATE TABLE for GROUP BY aggregation)
CREATE TABLE regional_summary AS
SELECT
    region,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction,
    _window_start AS window_start,
    _window_end AS window_end
FROM cleaned_transactions
GROUP BY region
WINDOW TUMBLING(5m)
EMIT CHANGES
WITH (
    'cleaned_transactions.type' = 'kafka_source',
    'cleaned_transactions.topic.name' = 'test_cleaned_transactions',
    'cleaned_transactions.config_file' = 'configs/transactions_source.yaml',

    'regional_summary.type' = 'kafka_sink',
    'regional_summary.topic.name' = 'test_regional_summary',
    'regional_summary.config_file' = 'configs/aggregates_sink.yaml'
);

-- Stage 3: Flag high-value regions
CREATE STREAM flagged_regions AS
SELECT
    region,
    transaction_count,
    total_revenue,
    avg_transaction,
    window_start,
    window_end,
    CASE
        WHEN total_revenue > 100000 THEN 'HIGH'
        WHEN total_revenue > 50000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS value_tier
FROM regional_summary
EMIT CHANGES
WITH (
    'regional_summary.type' = 'kafka_source',
    'regional_summary.topic.name' = 'test_regional_summary',
    'regional_summary.config_file' = 'configs/transactions_source.yaml',

    'flagged_regions.type' = 'kafka_sink',
    'flagged_regions.topic.name' = 'test_flagged_regions',
    'flagged_regions.config_file' = 'configs/output_stream_sink.yaml'
);
