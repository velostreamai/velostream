# Detect Patterns and Alerts

Learn how to detect patterns, anomalies, and trigger alerts in streaming data using SQL pattern detection techniques.

## Basic Pattern Detection

### Value Threshold Patterns

```sql
-- Simple threshold alerts
SELECT
    sensor_id,
    reading_value,
    reading_timestamp,
    CASE
        WHEN reading_value > 80 THEN 'HIGH_TEMP_ALERT'
        WHEN reading_value < 10 THEN 'LOW_TEMP_ALERT'
        ELSE 'NORMAL'
    END as alert_status
FROM temperature_sensors
WHERE reading_timestamp > NOW() - INTERVAL '1' HOUR;

-- Multiple condition patterns
SELECT
    order_id,
    amount,
    customer_location,
    payment_method,
    CASE
        WHEN amount > 5000 AND customer_location != 'US' THEN 'LARGE_FOREIGN_TRANSACTION'
        WHEN amount > 1000 AND payment_method = 'new_card' THEN 'LARGE_NEW_CARD'
        WHEN amount > 10000 THEN 'VERY_LARGE_TRANSACTION'
        ELSE 'NORMAL'
    END as fraud_risk_pattern
FROM transactions;
```

### Sequential Pattern Detection

```sql
-- Detect increasing/decreasing trends
SELECT
    device_id,
    reading_timestamp,
    current_value,
    LAG(current_value, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as prev_value_1,
    LAG(current_value, 2) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as prev_value_2,
    LAG(current_value, 3) OVER (PARTITION BY device_id ORDER BY reading_timestamp) as prev_value_3,
    CASE
        WHEN current_value > LAG(current_value, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
         AND LAG(current_value, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp) > 
             LAG(current_value, 2) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
         AND LAG(current_value, 2) OVER (PARTITION BY device_id ORDER BY reading_timestamp) >
             LAG(current_value, 3) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
        THEN 'INCREASING_TREND'
        WHEN current_value < LAG(current_value, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
         AND LAG(current_value, 1) OVER (PARTITION BY device_id ORDER BY reading_timestamp) < 
             LAG(current_value, 2) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
         AND LAG(current_value, 2) OVER (PARTITION BY device_id ORDER BY reading_timestamp) <
             LAG(current_value, 3) OVER (PARTITION BY device_id ORDER BY reading_timestamp)
        THEN 'DECREASING_TREND'
        ELSE 'STABLE'
    END as trend_pattern
FROM sensor_readings;
```

## Statistical Anomaly Detection

### Moving Average Deviation

```sql
-- Detect values outside normal range
SELECT
    timestamp,
    metric_value,
    AVG(metric_value) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) as moving_avg_20,
    STDDEV(metric_value) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    ) as moving_stddev_20,
    ABS(metric_value - AVG(metric_value) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
    )) as deviation_from_avg,
    CASE
        WHEN ABS(metric_value - AVG(metric_value) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        )) > 2 * STDDEV(metric_value) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ) THEN 'ANOMALY'
        WHEN ABS(metric_value - AVG(metric_value) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        )) > STDDEV(metric_value) OVER (
            ORDER BY timestamp
            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING
        ) THEN 'WARNING'
        ELSE 'NORMAL'
    END as anomaly_status
FROM system_metrics
WHERE timestamp > NOW() - INTERVAL '24' HOURS;
```

### Percentile-Based Detection

```sql
-- Detect outliers using percentiles
SELECT
    user_id,
    session_duration,
    NTILE(100) OVER (ORDER BY session_duration) as duration_percentile,
    CASE
        WHEN NTILE(100) OVER (ORDER BY session_duration) >= 99 THEN 'VERY_LONG_SESSION'
        WHEN NTILE(100) OVER (ORDER BY session_duration) <= 1 THEN 'VERY_SHORT_SESSION'
        WHEN NTILE(100) OVER (ORDER BY session_duration) >= 95 THEN 'LONG_SESSION'
        ELSE 'NORMAL_SESSION'
    END as session_pattern
FROM user_sessions
WHERE session_start > NOW() - INTERVAL '7' DAYS;
```

## Frequency and Rate Patterns

### High-Frequency Event Detection

```sql
-- Detect burst patterns
SELECT
    user_id,
    event_timestamp,
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY event_timestamp
        RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
    ) as events_last_minute,
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY event_timestamp
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) as events_last_5_minutes,
    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
        ) > 100 THEN 'BURST_PATTERN'
        WHEN COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
        ) > 200 THEN 'HIGH_ACTIVITY'
        ELSE 'NORMAL'
    END as activity_pattern
FROM user_events;
```

### Rate Change Detection

```sql
-- Detect sudden rate changes
SELECT
    hour_bucket,
    hourly_count,
    LAG(hourly_count, 1) OVER (ORDER BY hour_bucket) as prev_hour_count,
    LAG(hourly_count, 24) OVER (ORDER BY hour_bucket) as same_hour_yesterday,
    hourly_count - LAG(hourly_count, 1) OVER (ORDER BY hour_bucket) as hour_over_hour_change,
    hourly_count - LAG(hourly_count, 24) OVER (ORDER BY hour_bucket) as day_over_day_change,
    CASE
        WHEN hourly_count > 2 * LAG(hourly_count, 1) OVER (ORDER BY hour_bucket) THEN 'SUDDEN_SPIKE'
        WHEN hourly_count < 0.5 * LAG(hourly_count, 1) OVER (ORDER BY hour_bucket) THEN 'SUDDEN_DROP'
        WHEN ABS(hourly_count - LAG(hourly_count, 24) OVER (ORDER BY hour_bucket)) > 
             0.5 * LAG(hourly_count, 24) OVER (ORDER BY hour_bucket) THEN 'UNUSUAL_DAILY_CHANGE'
        ELSE 'NORMAL'
    END as rate_pattern
FROM (
    SELECT
        DATE_FORMAT(event_timestamp, '%Y-%m-%d %H:00:00') as hour_bucket,
        COUNT(*) as hourly_count
    FROM events
    GROUP BY DATE_FORMAT(event_timestamp, '%Y-%m-%d %H:00:00')
) hourly_stats;
```

## User Behavior Patterns

### Unusual User Patterns

```sql
-- Detect unusual user behavior
SELECT
    user_id,
    session_date,
    pages_visited,
    session_duration_minutes,
    actions_taken,
    -- User's typical behavior
    AVG(pages_visited) OVER (
        PARTITION BY user_id
        ORDER BY session_date
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) as avg_pages_30_days,
    AVG(session_duration_minutes) OVER (
        PARTITION BY user_id
        ORDER BY session_date
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) as avg_duration_30_days,
    -- Pattern detection
    CASE
        WHEN pages_visited > 3 * AVG(pages_visited) OVER (
            PARTITION BY user_id
            ORDER BY session_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) THEN 'UNUSUAL_HIGH_ACTIVITY'
        WHEN session_duration_minutes > 2 * AVG(session_duration_minutes) OVER (
            PARTITION BY user_id
            ORDER BY session_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) THEN 'UNUSUAL_LONG_SESSION'
        WHEN pages_visited < 0.3 * AVG(pages_visited) OVER (
            PARTITION BY user_id
            ORDER BY session_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) THEN 'UNUSUAL_LOW_ACTIVITY'
        ELSE 'NORMAL_BEHAVIOR'
    END as behavior_pattern
FROM user_daily_sessions
WHERE session_date > NOW() - INTERVAL '90' DAYS;
```

### Purchase Pattern Detection

```sql
-- Detect unusual purchase patterns
SELECT
    customer_id,
    purchase_date,
    amount,
    product_category,
    -- Customer history
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY purchase_date
        ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING
    ) as avg_purchase_amount,
    COUNT(*) OVER (
        PARTITION BY customer_id, product_category
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) as previous_purchases_in_category,
    -- Pattern classification
    CASE
        WHEN amount > 5 * AVG(amount) OVER (
            PARTITION BY customer_id
            ORDER BY purchase_date
            ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING
        ) THEN 'UNUSUALLY_LARGE_PURCHASE'
        WHEN COUNT(*) OVER (
            PARTITION BY customer_id, product_category
            ORDER BY purchase_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) = 0 THEN 'NEW_CATEGORY_PURCHASE'
        WHEN DATEDIFF('days', 
            LAG(purchase_date, 1) OVER (PARTITION BY customer_id ORDER BY purchase_date),
            purchase_date
        ) < 1 THEN 'RAPID_REPEAT_PURCHASE'
        ELSE 'NORMAL_PURCHASE'
    END as purchase_pattern
FROM purchases
WHERE purchase_date > NOW() - INTERVAL '30' DAYS;
```

## Real-Time Alert Systems

### Fraud Detection Patterns

```sql
-- Comprehensive fraud detection
SELECT
    transaction_id,
    customer_id,
    amount,
    merchant_location,
    transaction_timestamp,
    -- Velocity checks
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as transactions_last_hour,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_timestamp
        RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
    ) as amount_last_24h,
    -- Location pattern
    LAG(merchant_location, 1) OVER (
        PARTITION BY customer_id ORDER BY transaction_timestamp
    ) as prev_location,
    -- Time pattern
    EXTRACT('HOUR', transaction_timestamp) as transaction_hour,
    -- Risk assessment
    CASE
        WHEN amount > 5000 AND merchant_location != 
             LAG(merchant_location, 1) OVER (
                 PARTITION BY customer_id ORDER BY transaction_timestamp
             )
        THEN 'HIGH_RISK_LOCATION_AMOUNT'
        WHEN COUNT(*) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ) > 10 THEN 'HIGH_VELOCITY'
        WHEN SUM(amount) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
        ) > 10000 THEN 'HIGH_DAILY_VOLUME'
        WHEN EXTRACT('HOUR', transaction_timestamp) BETWEEN 2 AND 5
         AND amount > 1000 THEN 'UNUSUAL_TIME_LARGE_AMOUNT'
        ELSE 'LOW_RISK'
    END as fraud_risk_level
FROM transactions
ORDER BY transaction_timestamp DESC;
```

### System Health Monitoring

```sql
-- Detect system health issues
SELECT
    server_id,
    metric_timestamp,
    cpu_usage,
    memory_usage,
    disk_usage,
    response_time_ms,
    error_count,
    -- Health score calculation
    CASE
        WHEN cpu_usage > 90 THEN 20
        WHEN cpu_usage > 80 THEN 10
        ELSE 0
    END +
    CASE
        WHEN memory_usage > 95 THEN 25
        WHEN memory_usage > 85 THEN 15
        ELSE 0
    END +
    CASE
        WHEN disk_usage > 90 THEN 20
        WHEN disk_usage > 80 THEN 10
        ELSE 0
    END +
    CASE
        WHEN response_time_ms > 5000 THEN 15
        WHEN response_time_ms > 2000 THEN 8
        ELSE 0
    END +
    CASE
        WHEN error_count > 100 THEN 20
        WHEN error_count > 50 THEN 10
        ELSE 0
    END as health_risk_score,
    -- Alert classification
    CASE
        WHEN (cpu_usage > 90 OR memory_usage > 95 OR disk_usage > 90) THEN 'CRITICAL'
        WHEN (cpu_usage > 80 OR memory_usage > 85 OR response_time_ms > 5000) THEN 'WARNING'
        WHEN (cpu_usage > 70 OR memory_usage > 75 OR response_time_ms > 2000) THEN 'WATCH'
        ELSE 'HEALTHY'
    END as health_status
FROM server_metrics
WHERE metric_timestamp > NOW() - INTERVAL '1' HOUR;
```

## Pattern-Based Alerting

### Email Campaign Anomalies

```sql
-- Detect email campaign issues
SELECT
    campaign_id,
    send_hour,
    emails_sent,
    emails_delivered,
    emails_opened,
    emails_clicked,
    bounces,
    unsubscribes,
    -- Calculate rates
    emails_delivered * 100.0 / NULLIF(emails_sent, 0) as delivery_rate,
    emails_opened * 100.0 / NULLIF(emails_delivered, 0) as open_rate,
    emails_clicked * 100.0 / NULLIF(emails_opened, 0) as click_rate,
    bounces * 100.0 / NULLIF(emails_sent, 0) as bounce_rate,
    -- Historical averages
    AVG(emails_delivered * 100.0 / NULLIF(emails_sent, 0)) OVER (
        PARTITION BY campaign_id
        ORDER BY send_hour
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) as avg_delivery_rate_30d,
    AVG(emails_opened * 100.0 / NULLIF(emails_delivered, 0)) OVER (
        PARTITION BY campaign_id
        ORDER BY send_hour
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) as avg_open_rate_30d,
    -- Anomaly detection
    CASE
        WHEN bounces * 100.0 / NULLIF(emails_sent, 0) > 20 THEN 'HIGH_BOUNCE_RATE'
        WHEN emails_delivered * 100.0 / NULLIF(emails_sent, 0) < 80 THEN 'LOW_DELIVERY_RATE'
        WHEN emails_opened * 100.0 / NULLIF(emails_delivered, 0) < 
             0.5 * AVG(emails_opened * 100.0 / NULLIF(emails_delivered, 0)) OVER (
                 PARTITION BY campaign_id
                 ORDER BY send_hour
                 ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
             ) THEN 'UNUSUALLY_LOW_OPEN_RATE'
        ELSE 'NORMAL_PERFORMANCE'
    END as campaign_alert
FROM email_campaign_hourly_stats;
```

## Performance Tips

### Efficient Pattern Detection

```sql
-- ✅ Good: Use window functions for pattern detection
SELECT
    timestamp,
    value,
    LAG(value, 1) OVER (ORDER BY timestamp) as prev_value,
    value - LAG(value, 1) OVER (ORDER BY timestamp) as change
FROM metrics;

-- ⚠️ Less efficient: Self-joins for pattern detection
SELECT
    m1.timestamp,
    m1.value,
    m2.value as prev_value,
    m1.value - m2.value as change
FROM metrics m1
LEFT JOIN metrics m2 ON m2.timestamp = (
    SELECT MAX(timestamp) FROM metrics WHERE timestamp < m1.timestamp
);
```

### Alerting Best Practices

1. **Use time-based windows** to avoid alert storms
2. **Implement alert suppression** for known patterns
3. **Use statistical methods** rather than fixed thresholds when possible
4. **Test alert sensitivity** with historical data
5. **Include context** in alert messages

## Quick Reference

### Pattern Types
| Pattern | Description | Use Case |
|---------|-------------|----------|
| **Threshold** | Value exceeds limits | Temperature alerts, fraud amounts |
| **Trend** | Sequential changes | Stock prices, system metrics |
| **Frequency** | Event rate changes | Login attempts, API calls |
| **Anomaly** | Statistical outliers | Unusual behavior, system issues |
| **Sequence** | Event order patterns | User journeys, process flows |

### Detection Techniques
| Technique | SQL Pattern | Best For |
|-----------|-------------|----------|
| **Moving Average** | `AVG() OVER (ROWS BETWEEN...)` | Smoothing, trend detection |
| **Standard Deviation** | `STDDEV() OVER (...)` | Anomaly detection |
| **Percentiles** | `NTILE() OVER (...)` | Outlier identification |
| **LAG/LEAD** | `LAG(value, n) OVER (...)` | Sequential patterns |
| **Rate Calculation** | `COUNT() OVER (RANGE BETWEEN...)` | Frequency analysis |

## Next Steps

- [Window analysis](window-analysis.md) - Advanced time-based pattern detection
- [Aggregate data](aggregate-data.md) - Statistical functions for pattern detection
- [Essential functions](../functions/essential.md) - Functions commonly used in pattern detection