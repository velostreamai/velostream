# Real-Time Dashboard Queries

Copy-paste SQL queries for building real-time dashboards and monitoring systems.

## KPI Dashboard Queries

### Real-Time Sales Metrics

```sql
-- Current day sales summary
SELECT
    COUNT(*) as total_orders_today,
    SUM(amount) as total_revenue_today,
    AVG(amount) as avg_order_value_today,
    COUNT(DISTINCT customer_id) as unique_customers_today,
    SUM(amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
FROM orders
WHERE DATE(order_timestamp) = CURRENT_DATE;

-- Hourly sales trends (last 24 hours)
SELECT
    DATE_FORMAT(order_timestamp, '%Y-%m-%d %H:00:00') as hour,
    COUNT(*) as orders,
    SUM(amount) as revenue,
    AVG(amount) as avg_order_value
FROM orders
WHERE order_timestamp > NOW() - INTERVAL '24' HOURS
GROUP BY DATE_FORMAT(order_timestamp, '%Y-%m-%d %H:00:00')
ORDER BY hour;
```

### Live User Activity

```sql
-- Active users in last 5 minutes
SELECT
    COUNT(DISTINCT user_id) as active_users_5min,
    COUNT(*) as total_events_5min,
    COUNT(DISTINCT session_id) as active_sessions_5min
FROM user_events
WHERE event_timestamp > NOW() - INTERVAL '5' MINUTES;

-- User activity by page (last hour)
SELECT
    page_url,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as page_views,
    AVG(session_duration) as avg_session_duration
FROM user_events
WHERE event_timestamp > NOW() - INTERVAL '1' HOUR
  AND event_type = 'page_view'
GROUP BY page_url
ORDER BY unique_users DESC
LIMIT 10;
```

## System Performance Monitoring

### Server Health Dashboard

```sql
-- Current server status
SELECT
    server_id,
    MAX(cpu_usage) as current_cpu,
    MAX(memory_usage) as current_memory,
    MAX(disk_usage) as current_disk,
    AVG(response_time_ms) as avg_response_time,
    MAX(metric_timestamp) as last_update,
    CASE
        WHEN MAX(cpu_usage) > 90 OR MAX(memory_usage) > 95 THEN 'CRITICAL'
        WHEN MAX(cpu_usage) > 80 OR MAX(memory_usage) > 85 THEN 'WARNING'
        ELSE 'HEALTHY'
    END as status
FROM server_metrics
WHERE metric_timestamp > NOW() - INTERVAL '5' MINUTES
GROUP BY server_id;

-- System alerts (last hour)
SELECT
    alert_timestamp,
    server_id,
    alert_type,
    alert_message,
    severity,
    DATEDIFF('minutes', alert_timestamp, NOW()) as minutes_ago
FROM system_alerts
WHERE alert_timestamp > NOW() - INTERVAL '1' HOUR
ORDER BY alert_timestamp DESC;
```

### API Performance Metrics

```sql
-- API endpoint performance (last hour)
SELECT
    endpoint,
    COUNT(*) as request_count,
    AVG(response_time_ms) as avg_response_time,
    MIN(response_time_ms) as min_response_time,
    MAX(response_time_ms) as max_response_time,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) * 100.0 / COUNT(*) as error_rate_pct
FROM api_requests
WHERE request_timestamp > NOW() - INTERVAL '1' HOUR
GROUP BY endpoint
ORDER BY request_count DESC;
```

## Business Intelligence Dashboard

### Revenue Analytics

```sql
-- Revenue by product category (today vs yesterday)
SELECT
    p.category,
    SUM(CASE WHEN DATE(o.order_timestamp) = CURRENT_DATE THEN o.amount ELSE 0 END) as revenue_today,
    SUM(CASE WHEN DATE(o.order_timestamp) = CURRENT_DATE - 1 THEN o.amount ELSE 0 END) as revenue_yesterday,
    (SUM(CASE WHEN DATE(o.order_timestamp) = CURRENT_DATE THEN o.amount ELSE 0 END) - 
     SUM(CASE WHEN DATE(o.order_timestamp) = CURRENT_DATE - 1 THEN o.amount ELSE 0 END)) * 100.0 / 
    NULLIF(SUM(CASE WHEN DATE(o.order_timestamp) = CURRENT_DATE - 1 THEN o.amount ELSE 0 END), 0) as growth_pct
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE DATE(o.order_timestamp) IN (CURRENT_DATE, CURRENT_DATE - 1)
GROUP BY p.category
ORDER BY revenue_today DESC;

-- Top performing products (last 7 days)
SELECT
    p.product_name,
    COUNT(o.order_id) as units_sold,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_selling_price
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.order_timestamp > NOW() - INTERVAL '7' DAYS
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### Customer Metrics

```sql
-- Customer acquisition metrics
SELECT
    DATE(signup_timestamp) as signup_date,
    COUNT(*) as new_signups,
    COUNT(CASE WHEN first_purchase_timestamp IS NOT NULL THEN 1 END) as converted_customers,
    COUNT(CASE WHEN first_purchase_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_pct,
    AVG(DATEDIFF('hours', signup_timestamp, first_purchase_timestamp)) as avg_hours_to_first_purchase
FROM customers
WHERE signup_timestamp > NOW() - INTERVAL '30' DAYS
GROUP BY DATE(signup_timestamp)
ORDER BY signup_date;

-- Customer retention cohort (simplified)
SELECT
    DATE_FORMAT(signup_timestamp, '%Y-%m') as signup_month,
    COUNT(*) as cohort_size,
    COUNT(CASE WHEN last_purchase_timestamp > signup_timestamp + INTERVAL '30' DAYS THEN 1 END) as retained_30_days,
    COUNT(CASE WHEN last_purchase_timestamp > signup_timestamp + INTERVAL '90' DAYS THEN 1 END) as retained_90_days,
    COUNT(CASE WHEN last_purchase_timestamp > signup_timestamp + INTERVAL '30' DAYS THEN 1 END) * 100.0 / COUNT(*) as retention_30d_pct
FROM customers
WHERE signup_timestamp > NOW() - INTERVAL '12' MONTHS
GROUP BY DATE_FORMAT(signup_timestamp, '%Y-%m')
ORDER BY signup_month;
```

## Operational Dashboards

### Inventory Status

```sql
-- Low stock alerts
SELECT
    p.product_id,
    p.product_name,
    p.category,
    i.current_stock,
    i.reorder_point,
    i.reorder_quantity,
    CASE
        WHEN i.current_stock = 0 THEN 'OUT_OF_STOCK'
        WHEN i.current_stock <= i.reorder_point THEN 'REORDER_NEEDED'
        WHEN i.current_stock <= i.reorder_point * 1.5 THEN 'LOW_STOCK'
        ELSE 'ADEQUATE'
    END as stock_status,
    -- Days of inventory remaining (estimated)
    CASE
        WHEN s.avg_daily_sales > 0 THEN i.current_stock / s.avg_daily_sales
        ELSE NULL
    END as days_remaining
FROM products p
JOIN inventory i ON p.product_id = i.product_id
LEFT JOIN (
    SELECT
        product_id,
        AVG(daily_quantity) as avg_daily_sales
    FROM (
        SELECT
            product_id,
            DATE(order_timestamp) as order_date,
            SUM(quantity) as daily_quantity
        FROM orders
        WHERE order_timestamp > NOW() - INTERVAL '30' DAYS
        GROUP BY product_id, DATE(order_timestamp)
    ) daily_sales
    GROUP BY product_id
) s ON p.product_id = s.product_id
WHERE i.current_stock <= i.reorder_point * 2
ORDER BY stock_status, days_remaining;
```

### Order Processing Status

```sql
-- Order pipeline status
SELECT
    status,
    COUNT(*) as order_count,
    SUM(amount) as total_value,
    AVG(DATEDIFF('hours', order_timestamp, NOW())) as avg_age_hours,
    MIN(order_timestamp) as oldest_order,
    MAX(order_timestamp) as newest_order
FROM orders
WHERE order_timestamp > NOW() - INTERVAL '7' DAYS
GROUP BY status
ORDER BY FIELD(status, 'pending', 'processing', 'shipped', 'delivered', 'cancelled');

-- Processing bottlenecks
SELECT
    processing_stage,
    COUNT(*) as orders_in_stage,
    AVG(DATEDIFF('minutes', stage_start_timestamp, COALESCE(stage_end_timestamp, NOW()))) as avg_processing_time_min,
    COUNT(CASE WHEN DATEDIFF('hours', stage_start_timestamp, NOW()) > 24 THEN 1 END) as overdue_orders
FROM order_processing_stages
WHERE stage_start_timestamp > NOW() - INTERVAL '7' DAYS
  AND (stage_end_timestamp IS NULL OR stage_end_timestamp > NOW() - INTERVAL '1' DAY)
GROUP BY processing_stage
ORDER BY avg_processing_time_min DESC;
```

## Marketing Dashboard

### Campaign Performance

```sql
-- Email campaign performance (last 30 days)
SELECT
    campaign_name,
    send_date,
    emails_sent,
    emails_delivered,
    emails_opened,
    emails_clicked,
    unsubscribes,
    emails_delivered * 100.0 / emails_sent as delivery_rate_pct,
    emails_opened * 100.0 / emails_delivered as open_rate_pct,
    emails_clicked * 100.0 / emails_opened as click_rate_pct,
    unsubscribes * 100.0 / emails_delivered as unsubscribe_rate_pct
FROM email_campaigns
WHERE send_date > NOW() - INTERVAL '30' DAYS
ORDER BY send_date DESC;

-- Social media engagement
SELECT
    platform,
    post_type,
    COUNT(*) as posts_count,
    SUM(likes) as total_likes,
    SUM(shares) as total_shares,
    SUM(comments) as total_comments,
    AVG(likes) as avg_likes_per_post,
    AVG(engagement_rate) as avg_engagement_rate
FROM social_media_posts
WHERE post_date > NOW() - INTERVAL '7' DAYS
GROUP BY platform, post_type
ORDER BY avg_engagement_rate DESC;
```

### Traffic and Conversion

```sql
-- Website traffic sources (today)
SELECT
    traffic_source,
    COUNT(DISTINCT session_id) as sessions,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as page_views,
    COUNT(*) / COUNT(DISTINCT session_id) as pages_per_session,
    AVG(session_duration_seconds) / 60 as avg_session_duration_min,
    COUNT(CASE WHEN conversion_event IS NOT NULL THEN 1 END) as conversions,
    COUNT(CASE WHEN conversion_event IS NOT NULL THEN 1 END) * 100.0 / COUNT(DISTINCT session_id) as conversion_rate_pct
FROM web_analytics
WHERE DATE(event_timestamp) = CURRENT_DATE
GROUP BY traffic_source
ORDER BY sessions DESC;
```

## Financial Dashboard

### Daily Financial Summary

```sql
-- Today's financial snapshot
SELECT
    -- Revenue
    SUM(CASE WHEN transaction_type = 'sale' THEN amount ELSE 0 END) as total_revenue,
    COUNT(CASE WHEN transaction_type = 'sale' THEN 1 END) as total_sales,
    -- Refunds
    SUM(CASE WHEN transaction_type = 'refund' THEN amount ELSE 0 END) as total_refunds,
    COUNT(CASE WHEN transaction_type = 'refund' THEN 1 END) as refund_count,
    -- Net revenue
    SUM(CASE WHEN transaction_type = 'sale' THEN amount 
             WHEN transaction_type = 'refund' THEN -amount 
             ELSE 0 END) as net_revenue,
    -- Fees and costs
    SUM(processing_fee) as total_processing_fees,
    SUM(CASE WHEN transaction_type = 'sale' THEN cost ELSE 0 END) as total_costs,
    -- Profit
    SUM(CASE WHEN transaction_type = 'sale' THEN amount - cost - processing_fee
             WHEN transaction_type = 'refund' THEN -amount 
             ELSE 0 END) as gross_profit
FROM transactions
WHERE DATE(transaction_timestamp) = CURRENT_DATE;

-- Cash flow by hour
SELECT
    EXTRACT('HOUR', transaction_timestamp) as hour,
    SUM(CASE WHEN transaction_type = 'sale' THEN amount 
             WHEN transaction_type = 'refund' THEN -amount 
             ELSE 0 END) as net_cash_flow,
    COUNT(*) as transaction_count
FROM transactions
WHERE DATE(transaction_timestamp) = CURRENT_DATE
GROUP BY EXTRACT('HOUR', transaction_timestamp)
ORDER BY hour;
```

## Alert Queries for Dashboards

### Critical Business Alerts

```sql
-- Revenue drop alerts
SELECT
    'REVENUE_DROP' as alert_type,
    'Revenue down ' || 
    ROUND(ABS((today_revenue - yesterday_revenue) * 100.0 / yesterday_revenue), 1) || 
    '% vs yesterday' as alert_message,
    'HIGH' as severity,
    NOW() as alert_timestamp
FROM (
    SELECT
        SUM(CASE WHEN DATE(order_timestamp) = CURRENT_DATE THEN amount ELSE 0 END) as today_revenue,
        SUM(CASE WHEN DATE(order_timestamp) = CURRENT_DATE - 1 THEN amount ELSE 0 END) as yesterday_revenue
    FROM orders
    WHERE DATE(order_timestamp) IN (CURRENT_DATE, CURRENT_DATE - 1)
) revenue_comparison
WHERE today_revenue < yesterday_revenue * 0.8  -- Alert if down 20% or more

UNION ALL

-- High error rate alert
SELECT
    'HIGH_ERROR_RATE' as alert_type,
    'API error rate at ' || ROUND(error_rate, 1) || '% in last hour' as alert_message,
    CASE
        WHEN error_rate > 10 THEN 'CRITICAL'
        WHEN error_rate > 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,
    NOW() as alert_timestamp
FROM (
    SELECT
        COUNT(CASE WHEN status_code >= 400 THEN 1 END) * 100.0 / COUNT(*) as error_rate
    FROM api_requests
    WHERE request_timestamp > NOW() - INTERVAL '1' HOUR
) api_health
WHERE error_rate > 3;
```

## Dashboard Refresh Intervals

**Recommended refresh rates:**
- **Real-time metrics**: 30 seconds - 1 minute
- **Business KPIs**: 5-15 minutes  
- **Financial data**: 15-30 minutes
- **System health**: 1-5 minutes
- **Historical trends**: 1-6 hours

## Performance Tips

1. **Use time-based filtering** to limit data scanned
2. **Create materialized views** for complex calculations
3. **Index timestamp columns** used in WHERE clauses
4. **Consider caching** for expensive aggregations
5. **Use LIMIT** for top-N queries

## Common Dashboard Patterns

```sql
-- Pattern: Current vs Previous Period
SELECT
    metric_name,
    current_period_value,
    previous_period_value,
    (current_period_value - previous_period_value) * 100.0 / previous_period_value as change_pct
FROM metrics_comparison;

-- Pattern: Top N with Others
SELECT
    CASE
        WHEN rank <= 5 THEN category_name
        ELSE 'Others'
    END as display_category,
    SUM(revenue) as total_revenue
FROM (
    SELECT
        category_name,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) as rank
    FROM category_revenue
) ranked
GROUP BY CASE WHEN rank <= 5 THEN category_name ELSE 'Others' END;

-- Pattern: Health Status Summary
SELECT
    system_component,
    CASE
        WHEN error_count = 0 AND avg_response_time < 200 THEN 'Healthy'
        WHEN error_count > 0 OR avg_response_time > 1000 THEN 'Unhealthy'
        ELSE 'Warning'
    END as health_status,
    last_check_timestamp
FROM system_health_summary;
```