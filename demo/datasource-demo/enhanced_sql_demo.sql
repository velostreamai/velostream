-- SQL Application: enhanced file processing financial_transaction_processing_demo
-- Description: Enhanced Financial Transaction Processing Demo  
-- Description: Demonstrates Velostream advanced SQL capabilities with file data sources
-- Version: 4.1.0
-- Author: Quantitative Trading Team
-- Data Sources: Named sources and sinks with individual config files
-- Config Directory: demo/datasource-demo/configs/
-- Schema: JSON Schema validation available at velostream-config.schema.json
-- Tag: latency:ultra-low
-- Tag: compliance:regulatory

-- =====================================================
-- Part 1: Basic File Processing with Decimal Precision  
-- =====================================================

-- Create a stream from the CSV file with schema inference
CREATE STREAM raw_transactions AS
SELECT 
    transaction_id,
    customer_id,
    amount,
    currency,
    timestamp,
    merchant_category,
    description
FROM transactions_source
WITH (
    'transactions_source.type' = 'file_source',
    'transactions_source.config_file' = 'demo/datasource-demo/configs/transactions_source.yaml'
);

-- Basic transaction enrichment with financial metadata
CREATE STREAM enriched_transactions AS 
SELECT 
    transaction_id,
    customer_id,
    amount,
    amount * 100 AS amount_cents,  -- Convert to cents for integer arithmetic
    currency,
    FROM_UNIXTIME(timestamp) AS transaction_time,
    merchant_category,
    description,
    CASE 
        WHEN amount > 100.0 THEN 'HIGH_VALUE'
        WHEN amount > 50.0 THEN 'MEDIUM_VALUE' 
        ELSE 'LOW_VALUE'
    END AS value_tier,
    CASE 
        WHEN amount > 100.0 THEN TRUE 
        ELSE FALSE 
    END AS is_high_value,
    CURRENT_TIMESTAMP AS processed_at,
    'enhanced_sql_demo_v1.0' AS pipeline_version
FROM raw_transactions
WITH (
    'raw_transactions.type' = 'kafka_source',
    'raw_transactions.config_file' = 'demo/datasource-demo/configs/raw_transactions.yaml'
)
WHERE amount > 0  -- Filter out invalid amounts
INTO enriched_kafka_sink
WITH (
    'enriched_kafka_sink.type' = 'kafka_sink',
    'enriched_kafka_sink.config_file' = 'demo/datasource-demo/configs/enriched_kafka_sink.yaml'
)
EMIT CHANGES;

-- =====================================================
-- Part 2: Real-Time Analytics with Windowed Aggregation
-- =====================================================

-- Merchant category analytics (1-minute tumbling windows)
CREATE TABLE merchant_analytics AS
SELECT
    merchant_category PRIMARY KEY,
    'tumbling_1m_window' AS window_info,  -- Fixed: Simplified window info
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount, 
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT_DISTINCT(customer_id) AS unique_customers,
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_count,
    SUM(CASE WHEN is_high_value THEN amount ELSE 0 END) AS high_value_total
FROM enriched_transactions
WITH (
    'enriched_transactions.type' = 'kafka_source',
    'enriched_transactions.config_file' = 'demo/datasource-demo/configs/enriched_transactions.yaml'
)
GROUP BY merchant_category
-- WINDOW TUMBLING(1m)  -- Fixed: Commented out unsupported syntax
INTO merchant_kafka_sink
WITH (
    'merchant_kafka_sink.type' = 'kafka_sink',
    'merchant_kafka_sink.config_file' = 'demo/datasource-demo/configs/merchant_kafka_sink.yaml'
)
EMIT CHANGES;

-- Customer spending patterns (5-minute sliding windows)  
CREATE TABLE customer_spending_patterns AS
SELECT
    customer_id PRIMARY KEY,
    '5_minute_sliding_window' AS window_info,
    COUNT(*) AS transaction_frequency,
    SUM(amount) AS total_spent,
    AVG(amount) AS avg_transaction_size,
    MAX(amount) AS largest_transaction,
    COUNT_DISTINCT(merchant_category) AS merchant_diversity,
    STDDEV(amount) AS spending_volatility
FROM enriched_transactions  
WITH (
    'enriched_transactions.type' = 'kafka_source',
    'enriched_transactions.config_file' = 'demo/datasource-demo/configs/enriched_transactions.yaml'
)
GROUP BY customer_id
WINDOW SLIDING(5m, 1m)
INTO customer_kafka_sink
WITH (
    'customer_kafka_sink.type' = 'kafka_sink',
    'customer_kafka_sink.config_file' = 'demo/datasource-demo/configs/customer_kafka_sink.yaml'
)
EMIT CHANGES;

-- =====================================================
-- Part 3: Risk Detection and Fraud Analytics
-- =====================================================

-- Real-time fraud detection alerts
CREATE STREAM fraud_alerts AS
SELECT 
    e.transaction_id,
    e.customer_id,
    e.amount,
    e.merchant_category,
    e.transaction_time,
    -- Risk scoring based on spending patterns with subquery comparisons
    CASE 
        WHEN e.amount > (c.avg_transaction_size * 5) THEN 'AMOUNT_ANOMALY'
        WHEN e.amount > 500.0 AND e.merchant_category = 'gas' THEN 'SUSPICIOUS_GAS'
        WHEN c.transaction_frequency > 20 THEN 'HIGH_FREQUENCY'
        WHEN e.amount > (SELECT AVG(amount) * 3 FROM enriched_transactions WHERE merchant_category = e.merchant_category) THEN 'CATEGORY_ANOMALY'
        ELSE 'NORMAL'
    END AS risk_flag,
    -- Calculate risk score (0-100)
    LEAST(100, GREATEST(0, 
        (e.amount / NULLIF(c.avg_transaction_size, 0) * 10) +
        (c.transaction_frequency * 2) +
        (CASE WHEN e.amount > 200 THEN 20 ELSE 0 END)
    )) AS risk_score,
    c.total_spent AS customer_total_spent,
    c.transaction_frequency AS customer_frequency
FROM enriched_transactions e
INNER JOIN customer_spending_patterns c 
    ON e.customer_id = c.customer_id
    AND e.processed_at >= CURRENT_TIMESTAMP
WHERE e.amount > 50.0  -- Focus on medium+ value transactions
INTO fraud_kafka_sink
WITH (
    'fraud_kafka_sink.type' = 'kafka_sink',
    'fraud_kafka_sink.bootstrap.servers' = 'localhost:9092',
    'fraud_kafka_sink.topic' = 'fraud-alerts',
    'fraud_kafka_sink.value.format' = 'json',
    'fraud_kafka_sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- =====================================================
-- Part 4: Performance Monitoring and Data Quality
-- =====================================================

-- Processing performance metrics
CREATE TABLE processing_metrics AS  
SELECT
    '30_second_window' AS window_info,
    COUNT(*) AS records_processed,
    SUM(amount) AS total_amount_processed,
    AVG(CURRENT_TIMESTAMP - timestamp) AS avg_processing_delay_ms,
    COUNT_DISTINCT(customer_id) AS unique_customers,
    COUNT_DISTINCT(merchant_category) AS active_categories,
    -- Data quality metrics
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts,
    SUM(CASE WHEN LENGTH(transaction_id) != 8 THEN 1 ELSE 0 END) AS invalid_ids,
    SUM(CASE WHEN currency != 'USD' THEN 1 ELSE 0 END) AS non_usd_transactions
FROM raw_transactions
WITH (
    'raw_transactions.type' = 'kafka_source',
    'raw_transactions.config_file' = 'demo/datasource-demo/configs/raw_transactions.yaml'
)
GROUP BY 1
-- WINDOW TUMBLING(30s)  -- Fixed: Commented out unsupported syntax
INTO metrics_kafka_sink
WITH (
    'metrics_kafka_sink.type' = 'kafka_sink',
    'metrics_kafka_sink.bootstrap.servers' = 'localhost:9092',
    'metrics_kafka_sink.topic' = 'processing-metrics',
    'metrics_kafka_sink.value.format' = 'json',
    'metrics_kafka_sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- =====================================================
-- Part 5: Data Export and Persistence
-- =====================================================

-- Export high-value transactions to file
CREATE STREAM high_value_export AS
SELECT 
    transaction_id,
    customer_id,
    amount,
    merchant_category,
    transaction_time,
    value_tier,
    processed_at,
    'exported_' || UNIX_TIMESTAMP() AS export_id
FROM enriched_transactions
WHERE is_high_value = TRUE
INTO file_sink
WITH (
    'file_sink.type' = 'file_sink',
    'file_sink.config_file' = 'demo/datasource-demo/configs/file_sink.yaml'
)
EMIT CHANGES;

-- Export merchant analytics to CSV
CREATE STREAM merchant_analytics_csv AS
SELECT
    merchant_category,
    CURRENT_TIMESTAMP AS window_start,
    CURRENT_TIMESTAMP AS window_end,
    transaction_count,
    ROUND(total_amount, 2) AS total_amount,
    ROUND(average_amount, 2) AS average_amount,
    unique_customers,
    high_value_count
FROM merchant_analytics
INTO csv_sink
WITH (
    'csv_sink.type' = 'file_sink',
    'csv_sink.config_file' = 'demo/datasource-demo/configs/csv_sink.yaml'
)
EMIT CHANGES;

-- Export high-value transactions to Kafka for testing Kafka writer logging
CREATE STREAM high_value_kafka AS
SELECT 
    transaction_id,
    customer_id,
    amount,
    merchant_category,
    transaction_time,
    value_tier,
    processed_at
FROM enriched_transactions
WHERE is_high_value = TRUE
INTO kafka_sink
WITH (
    'kafka_sink.type' = 'kafka_sink',
    'kafka_sink.bootstrap.servers' = 'localhost:9092',
    'kafka_sink.topic' = 'high-value-transactions',
    'kafka_sink.value.format' = 'json',
    'kafka_sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- =====================================================  
-- Part 6: Advanced Analytics with Complex Joins
-- =====================================================

-- Customer lifetime value calculation
CREATE TABLE customer_ltv AS
SELECT
    customer_id PRIMARY KEY,
    '10_minute_session_window' AS session_window,  -- Fixed: SESSION_WINDOW not supported
    COUNT(*) AS session_transactions,
    SUM(amount) AS session_total,
    AVG(amount) AS session_avg,
    -- Calculate derived metrics
    FIRST_VALUE(transaction_time) AS session_start,
    LAST_VALUE(transaction_time) AS session_end,
    COUNT_DISTINCT(merchant_category) AS categories_in_session
    -- Subqueries are supported - this was incorrectly simplified
    (SELECT SUM(amount) FROM enriched_transactions e2
       WHERE e2.customer_id = e.customer_id
       AND e2.processed_at <= e.processed_at) AS running_total_spent
FROM enriched_transactions
-- GROUP BY customer_id -- Fixed: Simplified grouping
GROUP BY customer_id, WINDOW SESSION(10m)
INTO ltv_kafka_sink
WITH (
    'ltv_kafka_sink.type' = 'kafka_sink',
    'ltv_kafka_sink.bootstrap.servers' = 'localhost:9092',
    'ltv_kafka_sink.topic' = 'customer-ltv',
    'ltv_kafka_sink.value.format' = 'json',
    'ltv_kafka_sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- Cross-category spending analysis
CREATE TABLE cross_category_analysis AS
SELECT
    '2_minute_window' AS window_info,
    merchant_category PRIMARY KEY,
    COUNT(*) AS transaction_count,
    SUM(amount) AS category_total,
    -- Percentage calculation using subquery (subqueries ARE supported)
    ROUND((SUM(amount) / (SELECT SUM(amount) FROM enriched_transactions) * 100), 2) AS category_percentage,
    ROUND(SUM(amount) / COUNT(*), 2) AS avg_transaction_amount,
    -- Rankings (using simple ordering)
    SUM(amount) AS amount_rank,
    COUNT(*) AS frequency_rank
FROM enriched_transactions  
GROUP BY merchant_category
-- GROUP BY merchant_category, WINDOW TUMBLING(2m)  -- Fixed: Simplified grouping
INTO category_kafka_sink
WITH (
    'category_kafka_sink.type' = 'kafka_sink',
    'category_kafka_sink.bootstrap.servers' = 'localhost:9092',
    'category_kafka_sink.topic' = 'cross-category-analysis',
    'category_kafka_sink.value.format' = 'json',
    'category_kafka_sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- =====================================================
-- Demo Monitoring Queries  
-- =====================================================

-- Show current processing status
SELECT 
    'Processing Status' AS metric_type,
    COUNT(*) AS total_records_processed,
    ROUND(SUM(amount), 2) AS total_amount_processed,  
    COUNT_DISTINCT(customer_id) AS unique_customers,
    COUNT_DISTINCT(merchant_category) AS active_categories,
    ROUND(AVG(amount), 2) AS average_transaction_size
FROM enriched_transactions;

-- Show real-time fraud alerts
SELECT 
    transaction_id,
    customer_id, 
    amount,
    risk_flag,
    ROUND(risk_score, 1) AS risk_score,
    merchant_category,
    transaction_time
FROM fraud_alerts  
WHERE risk_score > 30
ORDER BY risk_score DESC
LIMIT 10;

-- Show top spending customers with subquery comparisons
SELECT 
    customer_id,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_spent,
    ROUND(AVG(amount), 2) AS avg_transaction,
    COUNT_DISTINCT(merchant_category) AS categories_used,
    -- Use subquery to compare against overall average
    CASE 
        WHEN SUM(amount) > (SELECT AVG(total_spent_per_customer) FROM (SELECT SUM(amount) AS total_spent_per_customer FROM enriched_transactions GROUP BY customer_id) AS customer_totals) 
        THEN 'ABOVE_AVERAGE' 
        ELSE 'BELOW_AVERAGE' 
    END AS spending_tier
FROM enriched_transactions
GROUP BY customer_id
HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM enriched_transactions)  -- Only show customers spending more than 2x average
ORDER BY total_spent DESC  
LIMIT 5;

-- Show merchant performance summary  
SELECT 
    merchant_category,
    transaction_count,
    ROUND(total_amount, 2) AS total_revenue,
    ROUND(average_amount, 2) AS avg_transaction,
    unique_customers,
    high_value_count,
    CURRENT_TIMESTAMP AS last_updated
FROM merchant_analytics  
ORDER BY total_amount DESC;

-- =====================================================
-- Cleanup Commands (run at end of demo)
-- =====================================================

-- Drop all created objects
-- DROP SINK IF EXISTS high_value_export;
-- DROP SINK IF EXISTS merchant_analytics_csv;  
-- DROP TABLE IF EXISTS processing_metrics;
-- DROP TABLE IF EXISTS cross_category_analysis;
-- DROP TABLE IF EXISTS customer_ltv;
-- DROP STREAM IF EXISTS fraud_alerts;
-- DROP TABLE IF EXISTS customer_spending_patterns;
-- DROP TABLE IF EXISTS merchant_analytics;
-- DROP STREAM IF EXISTS enriched_transactions;
-- DROP STREAM IF EXISTS raw_transactions;