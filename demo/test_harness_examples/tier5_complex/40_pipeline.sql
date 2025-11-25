-- Tier 5: Multi-Stage Pipeline
-- Tests: Multiple CREATE STREAM chained together
-- Expected: Data flows through multiple stages

-- Application metadata
-- @name pipeline_demo
-- @description Multi-stage data processing pipeline

-- Source definition
CREATE SOURCE raw_transactions (
    order_id STRING,
    customer_id INTEGER,
    product_id STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    status STRING,
    region STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_transactions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Stage 1: Clean and enrich raw data
CREATE STREAM cleaned_transactions AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price AS total_amount,
    UPPER(status) AS status,
    LOWER(region) AS region,
    event_time
FROM raw_transactions
WHERE quantity > 0 AND unit_price > 0;

-- Stage 2: Aggregate by region
CREATE STREAM region_summary AS
SELECT
    region,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction
FROM cleaned_transactions
GROUP BY region
WINDOW TUMBLING (INTERVAL '5' MINUTE);

-- Stage 3: Flag high-value regions
CREATE STREAM flagged_regions AS
SELECT
    region,
    transaction_count,
    total_revenue,
    avg_transaction,
    CASE
        WHEN total_revenue > 10000 THEN 'HIGH'
        WHEN total_revenue > 1000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS value_tier
FROM region_summary;

-- Sink definitions
CREATE SINK cleaned_sink FOR cleaned_transactions WITH (
    'connector' = 'kafka',
    'topic' = 'cleaned_transactions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

CREATE SINK flagged_regions_sink FOR flagged_regions WITH (
    'connector' = 'kafka',
    'topic' = 'flagged_regions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
