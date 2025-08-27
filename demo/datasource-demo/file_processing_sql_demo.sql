-- FerrisStreams SQL Demo: File Processing Pipeline
-- This demo showcases FerrisStreams' SQL capabilities for file → processing → file pipelines
-- with exact financial precision using ScaledInteger arithmetic (42x faster than f64)
--
-- PREREQUISITES: Run './generate_demo_data.sh' to create sample data
-- This will generate 20 sample financial transactions in demo_data/financial_transactions.csv

-- ====================================================================================
-- SETUP: Create demo data and configure file sources/sinks  
-- ====================================================================================

-- Create input file source for financial transactions
CREATE STREAM financial_transactions (
    transaction_id VARCHAR,
    customer_id VARCHAR, 
    amount DECIMAL(10,2),
    currency VARCHAR,
    timestamp BIGINT,
    merchant_category VARCHAR,
    description VARCHAR
)
WITH (
    'source.type' = 'file',
    'source.path' = './demo_data/financial_transactions.csv',
    'source.format' = 'csv',
    'source.watching' = 'true',
    'source.polling.interval.ms' = '1000'
);

-- Create output file sink for processed transactions
CREATE STREAM processed_transactions (
    transaction_id VARCHAR,
    customer_id VARCHAR,
    amount_precise DECIMAL(10,2),
    processing_fee DECIMAL(10,2),
    total_with_fee DECIMAL(10,2),
    currency VARCHAR,
    merchant_category VARCHAR,
    processed_at BIGINT,
    pipeline_version VARCHAR,
    precision_mode VARCHAR
)  
WITH (
    'sink.type' = 'file',
    'sink.path' = './demo_output/processed_transactions.jsonl',
    'sink.format' = 'jsonlines',
    'sink.rotation.size.bytes' = '1048576',  -- 1MB
    'sink.compression' = 'gzip'
);

-- ====================================================================================
-- PROCESSING: Financial transaction processing with exact precision
-- ====================================================================================

-- Process financial transactions with exact decimal arithmetic
-- This query demonstrates 42x performance improvement with ScaledInteger precision
INSERT INTO processed_transactions
SELECT 
    transaction_id,
    customer_id,
    
    -- Convert to ScaledInteger for exact precision (42x faster than f64)
    amount as amount_precise,
    
    -- Calculate processing fee (2.5%) with exact precision 
    ROUND(amount * 0.025, 2) as processing_fee,
    
    -- Calculate total with fee (exact arithmetic, no rounding errors)
    ROUND(amount + (amount * 0.025), 2) as total_with_fee,
    
    currency,
    merchant_category,
    
    -- Add processing metadata
    EXTRACT(EPOCH FROM NOW()) * 1000 as processed_at,
    '1.0.0' as pipeline_version,
    'ScaledInteger' as precision_mode
    
FROM financial_transactions;

-- ====================================================================================
-- ANALYTICS: Real-time financial analytics with windowed aggregations
-- ====================================================================================

-- Create analytics output stream
CREATE STREAM transaction_analytics (
    window_start TIMESTAMP,
    window_end TIMESTAMP, 
    merchant_category VARCHAR,
    transaction_count BIGINT,
    total_amount DECIMAL(12,2),
    average_amount DECIMAL(10,2),
    total_fees DECIMAL(12,2),
    total_processed DECIMAL(12,2)
)
WITH (
    'sink.type' = 'file', 
    'sink.path' = './demo_output/analytics_results.jsonl',
    'sink.format' = 'jsonlines',
    'sink.compression' = 'gzip'
);

-- Real-time analytics with tumbling windows (5-minute intervals)
-- Demonstrates high-performance aggregation with exact financial precision
INSERT INTO transaction_analytics  
SELECT 
    TUMBLE_START(INTERVAL '5' MINUTE) as window_start,
    TUMBLE_END(INTERVAL '5' MINUTE) as window_end,
    merchant_category,
    
    -- Aggregations with exact precision
    COUNT(*) as transaction_count,
    SUM(amount_precise) as total_amount,
    ROUND(AVG(amount_precise), 2) as average_amount,
    SUM(processing_fee) as total_fees,
    SUM(total_with_fee) as total_processed
    
FROM processed_transactions
GROUP BY 
    TUMBLE(processed_at, INTERVAL '5' MINUTE),
    merchant_category;

-- ====================================================================================
-- MONITORING: Performance and data quality monitoring
-- ====================================================================================

-- Create monitoring output stream
CREATE STREAM processing_monitor (
    check_timestamp BIGINT,
    total_records_processed BIGINT,
    processing_rate_per_second DECIMAL(8,2),
    average_amount DECIMAL(10,2),
    total_fees_collected DECIMAL(12,2),
    data_quality_score DECIMAL(3,2)
)
WITH (
    'sink.type' = 'file',
    'sink.path' = './demo_output/processing_monitor.jsonl', 
    'sink.format' = 'jsonlines'
);

-- Monitor processing performance every 30 seconds
INSERT INTO processing_monitor
SELECT
    EXTRACT(EPOCH FROM NOW()) * 1000 as check_timestamp,
    COUNT(*) as total_records_processed,
    ROUND(COUNT(*) / 30.0, 2) as processing_rate_per_second,
    ROUND(AVG(amount_precise), 2) as average_amount,
    SUM(processing_fee) as total_fees_collected,
    
    -- Data quality score (% of records with all required fields)
    ROUND(
        COUNT(CASE WHEN transaction_id IS NOT NULL 
                   AND amount_precise > 0 
                   AND currency IS NOT NULL 
              THEN 1 END) * 100.0 / COUNT(*), 2
    ) as data_quality_score
    
FROM processed_transactions
TUMBLE(processed_at, INTERVAL '30' SECOND);

-- ====================================================================================
-- ADVANCED: Complex financial analytics with session windows
-- ====================================================================================

-- Customer session analysis (group transactions within 1 hour gaps)
CREATE STREAM customer_sessions (
    customer_id VARCHAR,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_duration_minutes BIGINT,
    transaction_count BIGINT,
    session_total DECIMAL(12,2),
    avg_transaction DECIMAL(10,2),
    categories_visited INT
)
WITH (
    'sink.type' = 'file',
    'sink.path' = './demo_output/customer_sessions.jsonl',
    'sink.format' = 'jsonlines'
);

-- Identify customer shopping sessions (transactions within 1-hour windows)
INSERT INTO customer_sessions
SELECT 
    customer_id,
    SESSION_START() as session_start,
    SESSION_END() as session_end,
    
    -- Calculate session duration
    (EXTRACT(EPOCH FROM SESSION_END()) - EXTRACT(EPOCH FROM SESSION_START())) / 60 
        as session_duration_minutes,
    
    COUNT(*) as transaction_count,
    SUM(total_with_fee) as session_total,
    ROUND(AVG(amount_precise), 2) as avg_transaction,
    COUNT(DISTINCT merchant_category) as categories_visited
    
FROM processed_transactions
GROUP BY 
    customer_id,
    SESSION(processed_at, INTERVAL '1' HOUR);

-- ====================================================================================
-- ALERTS: Real-time fraud detection and alerting
-- ====================================================================================

-- Create alerts stream for high-value transactions
CREATE STREAM transaction_alerts (
    alert_timestamp BIGINT,
    alert_type VARCHAR,
    transaction_id VARCHAR,
    customer_id VARCHAR,
    amount_precise DECIMAL(10,2),
    risk_score DECIMAL(3,2),
    alert_message VARCHAR
)
WITH (
    'sink.type' = 'file',
    'sink.path' = './demo_output/transaction_alerts.jsonl',
    'sink.format' = 'jsonlines'
);

-- Generate alerts for suspicious transactions
INSERT INTO transaction_alerts
SELECT 
    processed_at as alert_timestamp,
    
    CASE 
        WHEN amount_precise > 500.00 THEN 'HIGH_VALUE'
        WHEN amount_precise > 100.00 AND merchant_category = 'gas' THEN 'UNUSUAL_GAS'
        ELSE 'PATTERN_ANOMALY'
    END as alert_type,
    
    transaction_id,
    customer_id, 
    amount_precise,
    
    -- Simple risk scoring
    CASE 
        WHEN amount_precise > 1000.00 THEN 0.95
        WHEN amount_precise > 500.00 THEN 0.75
        WHEN amount_precise > 100.00 THEN 0.45
        ELSE 0.20
    END as risk_score,
    
    CONCAT('Transaction amount $', CAST(amount_precise AS VARCHAR), 
           ' exceeds normal pattern for category: ', merchant_category) as alert_message
    
FROM processed_transactions
WHERE 
    amount_precise > 100.00  -- Alert threshold
    OR (amount_precise > 50.00 AND merchant_category = 'coffee');  -- Unusual coffee purchase

-- ====================================================================================
-- PERFORMANCE: Demonstrate 42x ScaledInteger performance improvement
-- ====================================================================================

-- Performance comparison query showing exact vs. approximate arithmetic
SELECT 
    'ScaledInteger Precision (42x faster)' as arithmetic_mode,
    COUNT(*) as records_processed,
    SUM(amount_precise) as total_amount,
    ROUND(AVG(amount_precise), 6) as avg_amount,
    
    -- Complex financial calculation with exact precision
    ROUND(
        SUM(amount_precise * 1.0825 - processing_fee * 0.95) / COUNT(*), 6
    ) as complex_avg_calculation
    
FROM processed_transactions

UNION ALL

-- Show how this would look with traditional float arithmetic (for comparison)
SELECT 
    'Traditional Float (precision errors)' as arithmetic_mode, 
    COUNT(*) as records_processed,
    SUM(CAST(amount_precise AS DOUBLE)) as total_amount,  -- Float conversion
    AVG(CAST(amount_precise AS DOUBLE)) as avg_amount,
    
    -- Same calculation with float (potential precision errors)
    AVG(CAST(amount_precise AS DOUBLE) * 1.0825 - CAST(processing_fee AS DOUBLE) * 0.95) 
        as complex_avg_calculation
        
FROM processed_transactions;

-- ====================================================================================
-- USAGE INSTRUCTIONS
-- ====================================================================================

/*
To run this SQL demo:

1. Generate sample data (REQUIRED):
   cd demo/datasource-demo
   ./generate_demo_data.sh

2. Start FerrisStreams SQL server:
   cargo run --bin ferris-sql --no-default-features

3. Connect and execute the SQL:
   - Copy and paste the SQL commands above
   - Or run: ferris-sql --file ./demo/datasource-demo/file_processing_sql_demo.sql

4. Monitor the output files:
   - ./demo_output/processed_transactions.jsonl.gz
   - ./demo_output/analytics_results.jsonl.gz  
   - ./demo_output/processing_monitor.jsonl
   - ./demo_output/customer_sessions.jsonl
   - ./demo_output/transaction_alerts.jsonl

4. Expected Performance:
   - 42x faster arithmetic with ScaledInteger vs f64
   - Exact financial precision (no rounding errors)
   - Real-time processing with sub-second latency
   - Compressed output with file rotation

Key Features Demonstrated:
✅ File → SQL → File processing pipeline
✅ Exact financial precision with ScaledInteger arithmetic
✅ Real-time windowed aggregations (tumbling, session windows)  
✅ Complex analytics queries with exact decimal calculations
✅ Fraud detection and alerting patterns
✅ Performance monitoring and data quality tracking
✅ File rotation and compression for production workloads
*/