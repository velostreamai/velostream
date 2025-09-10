-- FerrisStreams SQL Demo: File Processing Pipeline (FR-047 Compliant)
-- This demo showcases FerrisStreams' pluggable data sources with unified URI syntax
-- File → processing → file pipelines with exact financial precision
--
-- PREREQUISITES: Run './generate_demo_data.sh' to create sample data
-- This will generate 5000 sample financial transactions in demo_data/financial_transactions.csv

-- ====================================================================================
-- PROCESSING: Financial transaction processing with exact precision (FR-047 Syntax)
-- ====================================================================================

-- Process financial transactions with exact decimal arithmetic
-- This query demonstrates unified URI syntax from FR-047 specification
SELECT 
    transaction_id,
    customer_id,
    
    -- Convert to ScaledInteger for exact precision
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
    
FROM 'file://./demo_data/financial_transactions.csv'
WHERE amount > 0.01  -- Filter out invalid transactions  
INTO 'file://./demo_output/processed_transactions.jsonl'
WITH (
    format = 'jsonlines',
    compression = 'gzip',
    rotation_size = '1MB',
    watch = true,
    polling_interval = '1s'
);

-- ====================================================================================
-- ANALYTICS: Real-time financial analytics with windowed aggregations (FR-047)
-- ====================================================================================

-- Real-time analytics with tumbling windows (5-minute intervals)
-- Demonstrates high-performance aggregation with exact financial precision
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
    
FROM 'file://./demo_output/processed_transactions.jsonl'
WHERE amount_precise > 0  -- Valid transactions only
GROUP BY 
    TUMBLE(processed_at, INTERVAL '5' MINUTE),
    merchant_category
INTO 'file://./demo_output/analytics_results.jsonl'
WITH (
    format = 'jsonlines',
    compression = 'gzip'
);

-- ====================================================================================
-- ALTERNATIVE: Config-based approach (FR-047 also supports this)
-- ====================================================================================

-- Example showing config-based syntax instead of URIs
-- This demonstrates FR-047 flexibility supporting both approaches
CREATE TABLE dashboard_metrics AS
SELECT 
    DATE_TRUNC('hour', processed_at) as hour,
    COUNT(*) as hourly_transactions,
    SUM(total_with_fee) as hourly_revenue,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(AVG(amount_precise), 2) as avg_transaction_amount
FROM processed_transactions_stream  -- Named stream instead of URI
GROUP BY DATE_TRUNC('hour', processed_at)
INTO dashboard_sink  -- Named sink instead of URI  
WITH (
    "source_config" = "configs/jsonl_processed_source.yaml",
    "sink_config" = "configs/dashboard_api_sink.yaml",
    "update_frequency" = "1m"
);

-- ====================================================================================
-- MONITORING: Performance and data quality monitoring (FR-047)
-- ====================================================================================

-- Monitor processing performance every 30 seconds
-- Uses FR-047 unified syntax with configuration files
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
    
FROM 'file://./demo_output/processed_transactions.jsonl'
GROUP BY TUMBLE(processed_at, INTERVAL '30' SECOND)
INTO 'file://./demo_output/processing_monitor.jsonl'
WITH (
    "source_config" = "configs/jsonl_source.yaml",
    "sink_config" = "configs/monitor_sink.yaml"
);

-- ====================================================================================
-- ADVANCED: Complex financial analytics with session windows (FR-047)  
-- ====================================================================================

-- Identify customer shopping sessions (transactions within 1-hour windows)
-- Demonstrates FR-047 advanced analytics with unified syntax
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
    
FROM 'file://./demo_output/processed_transactions.jsonl'
WHERE customer_id IS NOT NULL
GROUP BY 
    customer_id,
    SESSION(processed_at, INTERVAL '1' HOUR)
INTO 'file://./demo_output/customer_sessions.jsonl'
WITH (
    "source_config" = "configs/jsonl_source.yaml",
    "sink_config" = "configs/sessions_sink.yaml"
);

-- ====================================================================================
-- ALERTS: Real-time fraud detection and alerting
-- ====================================================================================

-- Generate alerts for suspicious transactions (FR-047 unified URI syntax)
-- Using direct SELECT INTO with URI destination
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
    
FROM 'file://./demo_output/processed_transactions.jsonl'
WHERE 
    amount_precise > 100.00  -- Alert threshold
    OR (amount_precise > 50.00 AND merchant_category = 'coffee')  -- Unusual coffee purchase
INTO 'file://./demo_output/transaction_alerts.jsonl'
WITH (
    format = 'jsonlines',
    compression = 'none'  -- Alerts should be immediately readable
);

-- ====================================================================================
-- PERFORMANCE: Demonstrate ScaledInteger exact precision
-- ====================================================================================

-- Performance comparison query showing exact vs. approximate arithmetic
SELECT 
    'ScaledInteger Precision (exact)' as arithmetic_mode,
    COUNT(*) as records_processed,
    SUM(amount_precise) as total_amount,
    ROUND(AVG(amount_precise), 6) as avg_amount,
    
    -- Complex financial calculation with exact precision
    ROUND(
        SUM(amount_precise * 1.0825 - processing_fee * 0.95) / COUNT(*), 6
    ) as complex_avg_calculation
    
FROM 'file://./demo_output/processed_transactions.jsonl'

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
        
FROM 'file://./demo_output/processed_transactions.jsonl';

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
   - Exact financial precision with ScaledInteger arithmetic
   - No rounding errors in calculations
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