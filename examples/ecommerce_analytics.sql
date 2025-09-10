-- SQL Application: E-commerce Analytics Platform
-- Version: 1.3.0
-- Description: Complete e-commerce data processing pipeline for real-time analytics
-- Author: Analytics Team
-- Dependencies: Configuration files in configs/ directory using extends pattern

-- High Value Orders Processing
-- Uses extends-based configuration for maintainability
CREATE STREAM orders WITH (
    config_file = 'examples/configs/orders_topic.yaml'
);

CREATE STREAM high_value_orders_stream WITH (
    config_file = 'examples/configs/high_value_orders_sink.yaml'
);

INSERT INTO high_value_orders_stream
SELECT 
    customer_id,
    order_id,
    amount,
    timestamp() as processed_at
FROM orders 
WHERE amount > 1000;

-- User Activity Analytics
-- Tracks user interactions and behavior patterns
CREATE STREAM user_events WITH (
    config_file = 'examples/configs/user_events_topic.yaml'
);

CREATE STREAM user_analytics_stream WITH (
    config_file = 'examples/configs/user_analytics_sink.yaml'
);

INSERT INTO user_analytics_stream
SELECT 
    JSON_VALUE(payload, '$.user_id') as user_id,
    JSON_VALUE(payload, '$.action') as action,
    JSON_VALUE(payload, '$.page') as page_visited,
    CAST(JSON_VALUE(payload, '$.session_duration') AS INTEGER) as session_duration
FROM user_events
WHERE JSON_VALUE(payload, '$.action') IN ('purchase', 'add_to_cart', 'view_product');

-- Fraud Detection System
-- Critical fraud monitoring with risk categorization
CREATE STREAM fraud_alerts WITH (
    config_file = 'examples/configs/fraud_alerts_sink.yaml'
);

INSERT INTO fraud_alerts
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
WHERE amount > 500;

-- Customer Segmentation
-- Windowed aggregation for customer tier analysis
CREATE STREAM customer_segments WITH (
    config_file = 'examples/configs/customer_segments_sink.yaml'
);

INSERT INTO customer_segments
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
WINDOW TUMBLING(1h);

-- Product Performance Analytics
-- Windowed analytics for product performance metrics
CREATE STREAM product_events WITH (
    config_file = 'examples/configs/product_events_topic.yaml'
);

CREATE STREAM product_analytics_stream WITH (
    config_file = 'examples/configs/product_analytics_sink.yaml'
);

INSERT INTO product_analytics_stream
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
WINDOW TUMBLING(30m);