-- SQL Application: enhanced financial_transaction_processing_demo
-- Enhanced Financial Transaction Processing Demo
-- Demonstrates FerrisStreams advanced SQL capabilities with file data sources

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
FROM 'file://demo/datasource-demo/demo_data/financial_transactions.csv'
WITH (
    'source.format'='csv',
    'source.has_headers'='true',
    'source.watching'='true',
    'source.use_transactions'='false',
    'source.failure_strategy'='RetryWithBackoff',
    'source.retry_backoff'='1000',
    'source.max_retries'='3'
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
WHERE amount > 0  -- Filter out invalid amounts
EMIT CHANGES;

-- =====================================================
-- Part 2: Real-Time Analytics with Windowed Aggregation
-- =====================================================

-- Merchant category analytics (1-minute tumbling windows)
CREATE TABLE merchant_analytics AS
SELECT 
    merchant_category,
    WINDOW TUMBLING(1m) AS window_info,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount, 
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT_DISTINCT(customer_id) AS unique_customers,
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_count,
    SUM(CASE WHEN is_high_value THEN amount ELSE 0 END) AS high_value_total
FROM enriched_transactions
GROUP BY merchant_category
WINDOW TUMBLING(1m)
EMIT CHANGES;

-- Customer spending patterns (5-minute sliding windows)  
CREATE TABLE customer_spending_patterns AS
SELECT
    customer_id,
    '5_minute_hop_window' AS window_info,
    COUNT(*) AS transaction_frequency,
    SUM(amount) AS total_spent,
    AVG(amount) AS avg_transaction_size,
    MAX(amount) AS largest_transaction,
    COUNT_DISTINCT(merchant_category) AS merchant_diversity,
    STDDEV(amount) AS spending_volatility
FROM enriched_transactions  
GROUP BY customer_id
WINDOW SLIDING(5m, 1m)
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
    -- Risk scoring based on spending patterns
    CASE 
        WHEN e.amount > (c.avg_transaction_size * 5) THEN 'AMOUNT_ANOMALY'
        WHEN e.amount > 500.0 AND e.merchant_category = 'gas' THEN 'SUSPICIOUS_GAS'
        WHEN c.transaction_frequency > 20 THEN 'HIGH_FREQUENCY'  
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
GROUP BY 1
WINDOW TUMBLING(30s) 
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
    'sink.path' = 'demo/datasource-demo/output/high_value_transactions.json',
    'sink.format' = 'json',
    'sink.append' = 'true'
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
    'sink.path' = 'demo/datasource-demo/output/merchant_analytics.csv',
    'sink.format' = 'csv',
    'sink.has_headers' = 'true',
    'sink.append' = 'false'
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
    'sink.bootstrap.servers' = 'localhost:9092',
    'sink.topic' = 'high-value-transactions',
    'sink.value.format' = 'json',
    'sink.failure_strategy' = 'LogAndContinue'
)
EMIT CHANGES;

-- =====================================================  
-- Part 6: Advanced Analytics with Complex Joins
-- =====================================================

-- Customer lifetime value calculation
CREATE TABLE customer_ltv AS
SELECT 
    e.customer_id,
    SESSION_WINDOW('10 MINUTES') AS session_window,
    COUNT(*) AS session_transactions,
    SUM(e.amount) AS session_total,
    AVG(e.amount) AS session_avg,
    -- Calculate derived metrics
    FIRST_VALUE(e.transaction_time) AS session_start,
    LAST_VALUE(e.transaction_time) AS session_end,
    COUNT_DISTINCT(e.merchant_category) AS categories_in_session,
    -- Cumulative metrics via self-join
    (SELECT SUM(amount) FROM enriched_transactions e2 
     WHERE e2.customer_id = e.customer_id 
     AND e2.processed_at <= e.processed_at) AS running_total_spent
FROM enriched_transactions e
GROUP BY e.customer_id, WINDOW SESSION(10m)
EMIT CHANGES;

-- Cross-category spending analysis
CREATE TABLE cross_category_analysis AS
SELECT 
    '2_minute_window' AS window_info,
    merchant_category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS category_total,
    -- Compare with other categories
    SUM(amount) / (SELECT SUM(amount) 
                   FROM enriched_transactions e2
                   WHERE e2.processed_at >= CURRENT_TIMESTAMP 
                   AND e2.processed_at < CURRENT_TIMESTAMP) * 100 AS percentage_of_total,
    -- Rankings (using simple ordering)
    SUM(amount) AS amount_rank,
    COUNT(*) AS frequency_rank
FROM enriched_transactions  
GROUP BY merchant_category, WINDOW TUMBLING(2m)
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

-- Show top spending customers
SELECT 
    customer_id,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_spent,
    ROUND(AVG(amount), 2) AS avg_transaction,
    COUNT_DISTINCT(merchant_category) AS categories_used
FROM enriched_transactions
GROUP BY customer_id
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