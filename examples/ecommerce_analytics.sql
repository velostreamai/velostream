-- SQL Application: E-commerce Analytics Platform
-- Version: 1.2.0
-- Description: Complete e-commerce data processing pipeline for real-time analytics
-- Author: Analytics Team
-- Dependencies: kafka-orders, kafka-users, kafka-products

-- Name: High Value Orders Processor
-- Property: priority=high
-- Property: replicas=3
START JOB high_value_orders AS
SELECT 
    customer_id,
    order_id,
    amount,
    timestamp() as processed_at
FROM orders 
WHERE amount > 1000
WITH ('output.topic' = 'high_value_orders_stream');

-- Name: User Activity Tracker  
-- Property: priority=medium
-- Property: replicas=2
START JOB user_activity_analytics AS
SELECT 
    JSON_VALUE(payload, '$.user_id') as user_id,
    JSON_VALUE(payload, '$.action') as action,
    JSON_VALUE(payload, '$.page') as page_visited,
    CAST(JSON_VALUE(payload, '$.session_duration') AS INTEGER) as session_duration
FROM user_events
WHERE JSON_VALUE(payload, '$.action') IN ('purchase', 'add_to_cart', 'view_product')
WITH ('output.topic' = 'user_analytics_stream');

-- Name: Fraud Detection System
-- Property: priority=critical
-- Property: replicas=5
-- Property: memory_limit=2gb
START JOB fraud_detection AS
SELECT 
    customer_id,
    order_id,
    amount,
    payment_method,
    CASE 
        WHEN amount > 5000 AND payment_method = 'credit_card' THEN 'HIGH_RISK'
        WHEN amount > 2000 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level,
    timestamp() as flagged_at
FROM orders
WHERE amount > 500
WITH ('output.topic' = 'fraud_alerts');

-- Name: Customer Segmentation
-- Property: priority=low
-- Property: window=1h
START JOB customer_segmentation AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    CASE 
        WHEN SUM(amount) > 10000 THEN 'VIP'
        WHEN SUM(amount) > 1000 THEN 'PREMIUM'
        ELSE 'STANDARD'
    END as customer_tier
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(1h)
WITH ('output.topic' = 'customer_segments');

-- Name: Product Performance Analytics
-- Property: priority=medium
-- Property: window=30m
START JOB product_analytics AS
SELECT 
    JSON_VALUE(payload, '$.product_id') as product_id,
    JSON_VALUE(payload, '$.category') as category,
    COUNT(*) as view_count,
    COUNT(CASE WHEN JSON_VALUE(payload, '$.action') = 'purchase' THEN 1 END) as purchase_count,
    AVG(CAST(JSON_VALUE(payload, '$.price') AS FLOAT)) as avg_price
FROM product_events
GROUP BY 
    JSON_VALUE(payload, '$.product_id'),
    JSON_VALUE(payload, '$.category')
WINDOW TUMBLING(30m)
WITH ('output.topic' = 'product_analytics_stream');