-- E-Commerce Analytics with SQL-Native Observability
-- Demonstrates FR-073 SQL-native metric annotations for e-commerce monitoring
--
-- Metrics Defined:
-- 1. Order processing counters by status and payment method
-- 2. Revenue gauges (current order values)
-- 3. Processing latency histograms
-- 4. Cart abandonment detection
-- 5. High-value customer tracking
--
-- Run with: velo-sql deploy-app --file examples/ecommerce_with_metrics.sql

-- ===========================================================================
-- Stream 1: Order Processing Metrics
-- ===========================================================================

-- Counter: Total orders by status
-- @metric: velo_orders_total
-- @metric_type: counter
-- @metric_help: "Total orders processed by status and payment method"
-- @metric_labels: status, payment_method, region

-- Gauge: Current order value
-- @metric: velo_order_value_dollars
-- @metric_type: gauge
-- @metric_help: "Individual order value in dollars"
-- @metric_field: order_total
-- @metric_labels: status, payment_method, region

-- Histogram: Order processing latency
-- @metric: velo_order_processing_seconds
-- @metric_type: histogram
-- @metric_help: "Order processing latency from cart to checkout"
-- @metric_field: processing_time_seconds
-- @metric_labels: status, payment_method
-- @metric_buckets: 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0
CREATE STREAM order_metrics AS
SELECT
    order_id,
    status,
    payment_method,
    region,
    order_total,
    EXTRACT(EPOCH FROM (checkout_time - cart_time)) as processing_time_seconds,
    event_time
FROM orders
WHERE checkout_time IS NOT NULL
  AND cart_time IS NOT NULL;

-- ===========================================================================
-- Stream 2: High-Value Order Detection
-- ===========================================================================

-- @metric: velo_high_value_orders_total
-- @metric_type: counter
-- @metric_help: "Orders exceeding $500 in value"
-- @metric_labels: region, payment_method, customer_tier
-- @metric_condition: order_total > 500
CREATE STREAM high_value_orders AS
SELECT
    order_id,
    customer_id,
    CASE
        WHEN lifetime_value > 10000 THEN 'vip'
        WHEN lifetime_value > 1000 THEN 'gold'
        WHEN lifetime_value > 100 THEN 'silver'
        ELSE 'bronze'
    END as customer_tier,
    region,
    payment_method,
    order_total,
    event_time
FROM enriched_orders
WHERE order_total > 500;

-- ===========================================================================
-- Stream 3: Cart Abandonment Tracking
-- ===========================================================================

-- @metric: velo_cart_abandonments_total
-- @metric_type: counter
-- @metric_help: "Shopping carts abandoned before checkout"
-- @metric_labels: abandonment_stage, region, cart_value_tier
-- @metric_condition: abandoned = true
CREATE STREAM cart_abandonment AS
SELECT
    cart_id,
    CASE
        WHEN checkout_started THEN 'checkout'
        WHEN items_added > 0 THEN 'shopping'
        ELSE 'browse'
    END as abandonment_stage,
    region,
    CASE
        WHEN cart_total > 200 THEN 'high'
        WHEN cart_total > 50 THEN 'medium'
        ELSE 'low'
    END as cart_value_tier,
    abandoned,
    cart_total,
    event_time
FROM cart_events
WHERE abandoned = true;

-- ===========================================================================
-- Stream 4: Payment Processing Metrics
-- ===========================================================================

-- Counter: Payment attempts by result
-- @metric: velo_payment_attempts_total
-- @metric_type: counter
-- @metric_help: "Payment processing attempts by result"
-- @metric_labels: payment_method, result, failure_reason

-- Histogram: Payment processing latency
-- @metric: velo_payment_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Payment gateway processing latency"
-- @metric_field: latency_seconds
-- @metric_labels: payment_method, result
-- @metric_buckets: 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0
CREATE STREAM payment_processing AS
SELECT
    payment_id,
    order_id,
    payment_method,
    CASE
        WHEN success = true THEN 'success'
        WHEN retry_count > 0 THEN 'retried'
        ELSE 'failed'
    END as result,
    COALESCE(failure_reason, 'none') as failure_reason,
    EXTRACT(EPOCH FROM (completed_time - started_time)) as latency_seconds,
    event_time
FROM payment_transactions
WHERE completed_time IS NOT NULL;

-- ===========================================================================
-- Stream 5: Inventory Alert Monitoring
-- ===========================================================================

-- @metric: velo_low_inventory_items_total
-- @metric_type: counter
-- @metric_help: "Products with low inventory levels"
-- @metric_labels: product_category, warehouse, urgency
-- @metric_condition: inventory_level < reorder_point
CREATE STREAM inventory_alerts AS
SELECT
    product_id,
    product_category,
    warehouse,
    inventory_level,
    reorder_point,
    CASE
        WHEN inventory_level < reorder_point * 0.25 THEN 'critical'
        WHEN inventory_level < reorder_point * 0.5 THEN 'high'
        ELSE 'medium'
    END as urgency,
    event_time
FROM inventory_snapshots
WHERE inventory_level < reorder_point;

-- ===========================================================================
-- Stream 6: Customer Experience Metrics
-- ===========================================================================

-- Gauge: Current page load time
-- @metric: velo_page_load_seconds
-- @metric_type: gauge
-- @metric_help: "Page load time in seconds"
-- @metric_field: load_time_seconds
-- @metric_labels: page_type, device_type, region

-- Counter: Slow page loads (>2 seconds)
-- @metric: velo_slow_page_loads_total
-- @metric_type: counter
-- @metric_help: "Page loads exceeding 2 seconds"
-- @metric_labels: page_type, device_type, region
-- @metric_condition: load_time_seconds > 2.0
CREATE STREAM page_performance AS
SELECT
    session_id,
    page_type,
    device_type,
    region,
    load_time_seconds,
    event_time
FROM page_load_events;

-- ===========================================================================
-- Stream 7: Product Search Analytics
-- ===========================================================================

-- Counter: Search queries
-- @metric: velo_product_searches_total
-- @metric_type: counter
-- @metric_help: "Product search queries performed"
-- @metric_labels: result_status, device_type, region

-- Histogram: Search result count distribution
-- @metric: velo_search_results_count
-- @metric_type: histogram
-- @metric_help: "Distribution of search result counts"
-- @metric_field: result_count
-- @metric_labels: result_status, device_type
-- @metric_buckets: 0, 1, 5, 10, 25, 50, 100, 250, 500
CREATE STREAM product_search AS
SELECT
    search_id,
    query_text,
    result_count,
    CASE
        WHEN result_count = 0 THEN 'no_results'
        WHEN result_count < 10 THEN 'low_results'
        ELSE 'good_results'
    END as result_status,
    device_type,
    region,
    event_time
FROM search_events;

-- ===========================================================================
-- Stream 8: Return and Refund Monitoring
-- ===========================================================================

-- Counter: Return requests
-- @metric: velo_return_requests_total
-- @metric_type: counter
-- @metric_help: "Product return requests by reason"
-- @metric_labels: return_reason, product_category, region

-- Gauge: Return value
-- @metric: velo_return_value_dollars
-- @metric_type: gauge
-- @metric_help: "Value of returned items in dollars"
-- @metric_field: return_value
-- @metric_labels: return_reason, product_category
CREATE STREAM returns_and_refunds AS
SELECT
    return_id,
    order_id,
    product_category,
    return_reason,
    region,
    return_value,
    event_time
FROM return_requests;

-- ===========================================================================
-- Stream 9: Shipping Performance Tracking
-- ===========================================================================

-- Histogram: Shipping latency (order to delivery)
-- @metric: velo_shipping_latency_hours
-- @metric_type: histogram
-- @metric_help: "Shipping latency in hours"
-- @metric_field: shipping_hours
-- @metric_labels: shipping_method, region, carrier
-- @metric_buckets: 12, 24, 48, 72, 96, 120, 168, 240, 336

-- Counter: Late deliveries
-- @metric: velo_late_deliveries_total
-- @metric_type: counter
-- @metric_help: "Deliveries exceeding promised delivery time"
-- @metric_labels: shipping_method, region, carrier, delay_severity
-- @metric_condition: shipping_hours > promised_hours
CREATE STREAM shipping_performance AS
SELECT
    shipment_id,
    order_id,
    shipping_method,
    region,
    carrier,
    EXTRACT(EPOCH FROM (delivered_time - shipped_time)) / 3600 as shipping_hours,
    promised_hours,
    CASE
        WHEN (shipping_hours - promised_hours) > 48 THEN 'severe'
        WHEN (shipping_hours - promised_hours) > 24 THEN 'moderate'
        ELSE 'minor'
    END as delay_severity,
    event_time
FROM shipment_tracking
WHERE delivered_time IS NOT NULL
  AND shipped_time IS NOT NULL;

-- ===========================================================================
-- Expected Prometheus Metrics Output
-- ===========================================================================

-- # HELP velo_orders_total Total orders processed by status and payment method
-- # TYPE velo_orders_total counter
-- velo_orders_total{status="completed",payment_method="credit_card",region="us-east"} 15234
-- velo_orders_total{status="failed",payment_method="paypal",region="eu-west"} 45

-- # HELP velo_order_value_dollars Individual order value in dollars
-- # TYPE velo_order_value_dollars gauge
-- velo_order_value_dollars{status="completed",payment_method="credit_card",region="us-east"} 127.50

-- # HELP velo_order_processing_seconds Order processing latency from cart to checkout
-- # TYPE velo_order_processing_seconds histogram
-- velo_order_processing_seconds_bucket{status="completed",payment_method="credit_card",le="1.0"} 8234
-- velo_order_processing_seconds_bucket{status="completed",payment_method="credit_card",le="5.0"} 12450

-- # HELP velo_high_value_orders_total Orders exceeding $500 in value
-- # TYPE velo_high_value_orders_total counter
-- velo_high_value_orders_total{region="us-east",payment_method="credit_card",customer_tier="vip"} 342

-- # HELP velo_cart_abandonments_total Shopping carts abandoned before checkout
-- # TYPE velo_cart_abandonments_total counter
-- velo_cart_abandonments_total{abandonment_stage="checkout",region="us-east",cart_value_tier="high"} 89

-- ===========================================================================
-- Prometheus Alert Examples
-- ===========================================================================

-- Alert: High payment failure rate
-- rate(velo_payment_attempts_total{result="failed"}[5m]) / rate(velo_payment_attempts_total[5m]) > 0.05

-- Alert: High cart abandonment at checkout
-- rate(velo_cart_abandonments_total{abandonment_stage="checkout"}[10m]) > 10

-- Alert: Critical inventory level
-- velo_low_inventory_items_total{urgency="critical"} > 50

-- Alert: Slow page load times (p95)
-- histogram_quantile(0.95, rate(velo_page_load_seconds_bucket[5m])) > 3.0

-- Alert: High late delivery rate
-- rate(velo_late_deliveries_total[1h]) / rate(velo_shipping_latency_hours_count[1h]) > 0.1

-- ===========================================================================
-- Business Dashboards (Prometheus + Grafana)
-- ===========================================================================

-- Revenue Dashboard:
-- - sum(rate(velo_order_value_dollars[5m])) # Revenue per second
-- - sum by (region) (velo_orders_total) # Orders by region
-- - histogram_quantile(0.5, rate(velo_order_value_dollars_bucket[5m])) # Median order value

-- Customer Experience Dashboard:
-- - rate(velo_slow_page_loads_total[5m]) # Slow page rate
-- - rate(velo_cart_abandonments_total[5m]) # Abandonment rate
-- - histogram_quantile(0.99, rate(velo_order_processing_seconds_bucket[5m])) # p99 checkout latency

-- Operational Dashboard:
-- - rate(velo_payment_attempts_total{result="failed"}[5m]) # Payment failures
-- - velo_low_inventory_items_total{urgency="critical"} # Critical inventory
-- - rate(velo_late_deliveries_total[1h]) # Late delivery rate
