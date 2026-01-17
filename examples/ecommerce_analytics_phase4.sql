-- SQL Application: E-commerce Analytics Platform (Phase 1B-4 Features)
-- Version: 2.0.0
-- Description: Advanced e-commerce data processing showcasing Velostream Phase 1B-4 capabilities
-- Author: Analytics Team
-- Features: Event-time processing, Circuit breakers, Advanced analytics, Observability
-- Dependencies: Configuration files in configs/ directory using extends pattern
-- Tag: fraud:enabled
-- Tag: personalization:ml-powered
-- Tag: features:watermarks,circuit-breakers,advanced-sql,observability

-- ====================================================================================
-- PHASE 1B: EVENT-TIME PROCESSING - Order Stream with Late Data Handling
-- ====================================================================================
-- Process orders with proper event-time handling for accurate fraud detection

CREATE STREAM orders_with_event_time AS
SELECT 
    order_id,
    customer_id,
    amount,
    payment_method,
    order_timestamp,
    customer_location,
    payment_location,
    -- Extract event-time from when order was actually placed
    TIMESTAMP(order_timestamp) as event_time,
    shipping_address,
    billing_address,
    product_items,
    discount_applied,
    currency_code
FROM orders_raw
WITH (
    -- Phase 1B: Event-time configuration for e-commerce order processing
    'event.time.field' = 'order_timestamp',
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '60s',  -- Payment processing delays
    'late.data.strategy' = 'include_in_next_window', -- Include delayed orders
    
    'config_file' = 'examples/configs/orders_topic.yaml'
);

-- ====================================================================================
-- PHASE 3: ADVANCED FRAUD DETECTION WITH STATISTICAL ANALYSIS
-- ====================================================================================
-- Sophisticated fraud detection using advanced SQL features and machine learning patterns

CREATE STREAM advanced_fraud_detection AS
SELECT 
    order_id,
    customer_id,
    amount,
    payment_method,
    event_time,
    customer_location,
    payment_location,
    
    -- Phase 3: Advanced window functions for fraud pattern detection
    RANK() OVER (
        PARTITION BY customer_id 
        ORDER BY amount DESC
    ) as amount_rank_for_customer,
    
    DENSE_RANK() OVER (
        PARTITION BY payment_method, DATE_TRUNC('hour', event_time)
        ORDER BY amount DESC  
    ) as hourly_amount_rank,
    
    PERCENT_RANK() OVER (
        PARTITION BY payment_method
        ORDER BY amount
    ) as amount_percentile,
    
    -- Velocity analysis for rapid-fire transactions
    LAG(event_time, 1) OVER (
        PARTITION BY customer_id 
        ORDER BY event_time
    ) as prev_order_time,
    
    LEAD(event_time, 1) OVER (
        PARTITION BY customer_id
        ORDER BY event_time  
    ) as next_order_time,
    
    -- Statistical analysis over sliding window for anomaly detection
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as avg_amount_20_orders,
    
    STDDEV(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as amount_volatility,
    
    -- Z-score for statistical anomaly detection
    (amount - AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )) / NULLIF(STDDEV(amount) OVER (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 0) as amount_z_score,
    
    -- Geographic risk analysis
    CASE
        WHEN customer_location != payment_location THEN 'GEO_MISMATCH'
        WHEN customer_location IS NULL THEN 'UNKNOWN_LOCATION'
        ELSE 'GEO_NORMAL'
    END as geographic_risk,
    
    -- Velocity risk calculation
    EXTRACT(EPOCH FROM (event_time - LAG(event_time, 1) OVER (
        PARTITION BY customer_id 
        ORDER BY event_time
    ))) / 60 as minutes_since_last_order,
    
    -- Complex fraud classification using advanced logic
    CASE
        WHEN amount > AVG(amount) OVER (
            PARTITION BY customer_id
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 10 THEN 'MEGA_FRAUD_ALERT'
        WHEN ABS((amount - AVG(amount) OVER (
            PARTITION BY customer_id
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(amount) OVER (
            PARTITION BY customer_id
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0)) > 3.0 THEN 'STATISTICAL_ANOMALY'
        WHEN customer_location != payment_location 
         AND amount > 1000 THEN 'HIGH_RISK_GEO'
        WHEN EXTRACT(EPOCH FROM (event_time - LAG(event_time, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY event_time
        ))) / 60 < 2 -- Less than 2 minutes between orders
         AND amount > 500 THEN 'VELOCITY_FRAUD'
        ELSE 'LOW_RISK'
    END as fraud_classification,
    
    NOW() as detection_time
FROM orders_with_event_time
-- Phase 1B: Event-time tumbling windows (10-minute fraud analysis windows)
WINDOW TUMBLING (event_time, INTERVAL '10' MINUTE)
-- Phase 3: Complex HAVING with subqueries for high-value fraud detection
HAVING amount > (
    SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount)
    FROM orders_with_event_time o2
    WHERE o2.customer_id = orders_with_event_time.customer_id
    AND o2.event_time >= orders_with_event_time.event_time - INTERVAL '30' DAY
)
OR ABS((amount - AVG(amount) OVER (
    PARTITION BY customer_id
    ORDER BY event_time
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
)) / NULLIF(STDDEV(amount) OVER (
    PARTITION BY customer_id
    ORDER BY event_time
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
), 0)) > 2.0  -- Statistical threshold
INTO fraud_alerts_advanced
WITH (
    -- Phase 2: Resource management for high-volume e-commerce data
    'max.memory.mb' = '8192',
    'max.groups' = '1000000',  -- Many customers
    'spill.to.disk' = 'true',
    'memory.pressure.threshold' = '0.85',
    
    -- Phase 2: Circuit breaker for payment verification API calls
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '3',
    'circuit.breaker.timeout' = '120s',
    'circuit.breaker.slow.call.threshold' = '10s',
    
    -- Phase 2: Retry configuration for payment APIs
    'retry.max.attempts' = '5',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '500ms',
    'retry.max.delay' = '60s',
    
    -- Phase 4: Comprehensive observability
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'advanced_fraud_detection',
    'prometheus.histogram.buckets' = '0.5,1.0,5.0,10.0,30.0,60.0,300.0',
    
    'config_file' = 'examples/configs/fraud_alerts_advanced_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B+3: CUSTOMER ANALYTICS WITH SESSION WINDOWS
-- ====================================================================================
-- Advanced customer behavior analysis using session-based windows and complex joins

CREATE STREAM customer_behavior_analytics AS
SELECT
    customer_id PRIMARY KEY,
    COUNT(DISTINCT order_id) as orders_in_session,
    COUNT(DISTINCT payment_method) as payment_methods_used,
    SUM(amount) as total_session_value,
    AVG(amount) as avg_order_value,
    
    -- Phase 3: Advanced statistical measures
    STDDEV(amount) as order_amount_stddev,
    VARIANCE(amount) as order_amount_variance,
    MIN(amount) as min_order_value,
    MAX(amount) as max_order_value,
    
    -- Purchase velocity calculations
    COUNT(*) / EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) * 3600 as orders_per_hour,
    
    -- Session time analysis
    SESSION_START() as session_start,
    SESSION_END() as session_end,
    EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 60 as session_duration_minutes,
    
    -- Product diversity analysis
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(product_items, '$[*].category')) as unique_categories,
    SUM(CAST(JSON_EXTRACT_SCALAR(product_items, '$[*].quantity') AS INTEGER)) as total_items,
    
    -- Geographic consistency
    COUNT(DISTINCT customer_location) as location_changes,
    FIRST_VALUE(customer_location) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            ORDER BY event_time
    ) as session_start_location,
    LAST_VALUE(customer_location) OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            ORDER BY event_time
    ) as session_end_location,
    
    -- Advanced customer tier classification with ML-like logic
    CASE
        WHEN SUM(amount) > 5000 
         AND COUNT(DISTINCT order_id) > 10
         AND EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 60 < 120 
         THEN 'VIP_HIGH_VELOCITY'
        WHEN SUM(amount) > 2000 
         AND AVG(amount) > 200 
         THEN 'PREMIUM_HIGH_VALUE'
        WHEN COUNT(DISTINCT JSON_EXTRACT_SCALAR(product_items, '$[*].category')) >= 5
         AND COUNT(DISTINCT order_id) > 5 
         THEN 'DIVERSE_SHOPPER'
        WHEN EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 3600 > 4
         AND COUNT(DISTINCT order_id) > 3
         THEN 'DELIBERATE_SHOPPER' 
        WHEN COUNT(DISTINCT payment_method) > 1
         AND SUM(amount) > 1000
         THEN 'PAYMENT_EXPERIMENTER'
        ELSE 'STANDARD_CUSTOMER'
    END as customer_behavior_type,
    
    NOW() as analysis_time
FROM orders_with_event_time
-- Phase 1B: Session windows based on customer activity (45-minute inactivity gap)
GROUP BY customer_id, SESSION(event_time, INTERVAL '45' MINUTE)
-- Phase 3: Complex HAVING with multiple aggregation conditions
HAVING COUNT(DISTINCT order_id) >= 2
   AND SUM(amount) > 100
   AND EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) >= 180  -- At least 3 minutes
INTO customer_sessions_analytics
WITH (
    -- Phase 2: Full resource and fault tolerance configuration
    'max.memory.mb' = '12288',  -- Large memory for customer analytics
    'max.groups' = '2000000',  -- Many customers
    'max.session.windows' = '500000',
    'spill.to.disk' = 'true',
    'session.timeout' = '7200s',  -- 2 hour max session
    
    -- Circuit breaker for customer data enrichment
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '5',
    'circuit.breaker.success.threshold' = '10',
    'circuit.breaker.timeout' = '90s',
    
    -- Retry for critical customer analytics
    'retry.max.attempts' = '3',
    'retry.backoff.strategy' = 'linear',
    'retry.initial.delay' = '200ms',
    'retry.max.delay' = '30s',
    
    -- Dead letter queue for complex customer analysis failures
    'dead.letter.queue.enabled' = 'true',
    'dead.letter.queue.topic' = 'customer-analytics-failures',
    
    -- Phase 4: Advanced observability for customer analytics
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'customer_behavior_analytics',
    'observability.alerts.enabled' = 'true',
    'prometheus.histogram.buckets' = '5,15,45,120,300,1800,7200',
    
    'config_file' = 'examples/configs/customer_sessions_analytics_sink.yaml'
);

-- ====================================================================================
-- PHASE 2+4: PRODUCT RECOMMENDATION ENGINE WITH FAULT TOLERANCE
-- ====================================================================================
-- Real-time product recommendation system with comprehensive error handling

CREATE STREAM product_recommendation_engine AS
SELECT 
    customer_id,
    current_product_id,
    current_category,
    
    -- Collaborative filtering simulation using window functions
    ARRAY_AGG(DISTINCT other_customers.preferred_product) OVER (
        PARTITION BY current_category
        ORDER BY similarity_score DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING
    ) as recommended_products,
    
    -- Purchase history analysis
    COUNT(DISTINCT prev_orders.order_id) OVER (
        PARTITION BY customer_id, current_category
    ) as category_purchase_count,
    
    AVG(prev_orders.amount) OVER (
        PARTITION BY customer_id, current_category
    ) as avg_category_spend,
    
    -- Seasonal trend analysis
    EXTRACT(HOUR FROM event_time) as purchase_hour,
    EXTRACT(DOW FROM event_time) as purchase_day_of_week,
    
    -- Recommendation confidence scoring
    CASE
        WHEN COUNT(DISTINCT prev_orders.order_id) OVER (
            PARTITION BY customer_id, current_category
        ) >= 5 THEN 'HIGH_CONFIDENCE'
        WHEN COUNT(DISTINCT prev_orders.order_id) OVER (
            PARTITION BY customer_id, current_category
        ) >= 2 THEN 'MEDIUM_CONFIDENCE'
        ELSE 'LOW_CONFIDENCE'
    END as recommendation_confidence,
    
    NOW() as recommendation_time
FROM orders_with_event_time current_order
-- Phase 3: Complex JOIN with previous customer orders
LEFT JOIN orders_with_event_time prev_orders 
    ON current_order.customer_id = prev_orders.customer_id
    AND prev_orders.event_time >= current_order.event_time - INTERVAL '90' DAY
    AND prev_orders.event_time < current_order.event_time
-- Phase 3: JOIN with similar customer preferences (collaborative filtering simulation)
LEFT JOIN (
    SELECT 
        customer_id as other_customer_id,
        JSON_EXTRACT_SCALAR(product_items, '$[0].id') as preferred_product,
        JSON_EXTRACT_SCALAR(product_items, '$[0].category') as product_category,
        AVG(amount) as similarity_score
    FROM orders_with_event_time
    GROUP BY customer_id, JSON_EXTRACT_SCALAR(product_items, '$[0].category')
) other_customers 
    ON JSON_EXTRACT_SCALAR(current_order.product_items, '$[0].category') = other_customers.product_category
    AND current_order.customer_id != other_customers.other_customer_id
-- Phase 1B: Event-time sliding windows (1-hour recommendation windows)
WINDOW SLIDING (event_time, INTERVAL '1' HOUR, INTERVAL '15' MINUTE)
INTO product_recommendations
WITH (
    -- Phase 2: Resource management for ML-style processing
    'max.memory.mb' = '16384',  -- Large memory for recommendation processing
    'max.groups' = '5000000',  -- Many product combinations
    'max.joins' = '1000000',   -- Many customer similarity calculations
    'spill.to.disk' = 'true',
    'join.timeout' = '300s',
    
    -- Phase 2: Circuit breaker for external ML API calls
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '7',
    'circuit.breaker.success.threshold' = '5',
    'circuit.breaker.timeout' = '180s',
    'circuit.breaker.slow.call.threshold' = '20s',
    
    -- Retry configuration for ML service calls
    'retry.max.attempts' = '4',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '1s',
    'retry.max.delay' = '120s',
    
    -- Dead letter queue for failed recommendations
    'dead.letter.queue.enabled' = 'true',
    'dead.letter.queue.topic' = 'recommendation-failures',
    'dead.letter.queue.max.retries' = '3',
    
    -- Phase 4: Advanced observability for recommendation engine
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'product_recommendation_engine',
    'observability.alerts.enabled' = 'true',
    'observability.custom.metrics' = 'recommendation_accuracy,customer_engagement_rate',
    'prometheus.histogram.buckets' = '1,5,15,45,120,300,900,3600',
    
    'config_file' = 'examples/configs/product_recommendations_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B-4 E-COMMERCE FEATURE SHOWCASE SUMMARY
-- ====================================================================================
-- This e-commerce analytics demo demonstrates:

-- PHASE 1B: Watermarks & Time Semantics
-- ✓ Event-time extraction from order_timestamp for fraud detection
-- ✓ BoundedOutOfOrderness strategy with payment processing delay tolerance
-- ✓ Session windows for customer behavior analysis
-- ✓ Sliding windows for recommendation engine
-- ✓ Late data inclusion for delayed payment confirmations

-- PHASE 2: Resource Management & Circuit Breakers
-- ✓ High-memory configuration for customer analytics and ML processing
-- ✓ Circuit breakers for payment verification and ML API calls
-- ✓ Retry logic for payment processing and recommendation services
-- ✓ Dead letter queue for complex analytics failures
-- ✓ Join timeouts and resource limits for recommendation engine

-- PHASE 3: Advanced Query Features  
-- ✓ Window functions: RANK, DENSE_RANK, PERCENT_RANK for fraud analysis
-- ✓ Statistical functions: STDDEV, VARIANCE for anomaly detection
-- ✓ Session windows with SESSION_START/SESSION_END for customer behavior
-- ✓ Complex joins for collaborative filtering simulation
-- ✓ JSON functions: JSON_EXTRACT_SCALAR, ARRAY_AGG for product analysis
-- ✓ Advanced CASE expressions for customer segmentation

-- PHASE 4: Observability Integration
-- ✓ Distributed tracing for fraud detection and recommendation pipeline
-- ✓ Prometheus metrics with custom buckets for e-commerce patterns
-- ✓ Performance profiling for customer analytics bottlenecks
-- ✓ Alert integration for fraud detection and system health
-- ✓ Custom metrics for recommendation accuracy tracking

-- Use cases demonstrated:
-- - Real-time fraud detection with statistical anomaly analysis
-- - Customer behavior analytics with session-based windows
-- - Product recommendation engine with collaborative filtering
-- - Advanced customer segmentation with ML-style classification
-- - Robust handling of high-volume, mission-critical e-commerce data