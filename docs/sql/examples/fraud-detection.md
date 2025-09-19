# Fraud Detection Queries

Copy-paste SQL queries for detecting fraudulent activities and suspicious patterns in real-time.

> **⚠️ Important**: All examples include required CREATE STREAM statements. These queries are ready to run in Velostream.

## Transaction-Based Fraud Detection

### High-Risk Transaction Patterns

```sql
-- Data source: incoming transactions
CREATE STREAM transactions WITH (
    topic = 'transactions-topic',
    bootstrap.servers = 'localhost:9092',
    value.deserializer = 'json'
);

-- Data sink: flagged transactions
CREATE STREAM fraud_alerts WITH (
    topic = 'fraud-alerts-topic',
    bootstrap.servers = 'localhost:9092',
    value.serializer = 'json'
);

-- Detect suspicious transaction patterns
INSERT INTO fraud_alerts
SELECT
    transaction_id,
    customer_id,
    amount,
    merchant_location,
    transaction_timestamp,
    -- Risk factors
    CASE WHEN amount > 5000 THEN 'LARGE_AMOUNT' END as large_amount_flag,
    CASE WHEN merchant_location != customer_home_country THEN 'FOREIGN_TRANSACTION' END as foreign_flag,
    CASE WHEN EXTRACT('HOUR', transaction_timestamp) BETWEEN 2 AND 5 THEN 'UNUSUAL_TIME' END as time_flag,
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
    -- Risk score calculation
    (CASE WHEN amount > 5000 THEN 30 ELSE 0 END +
     CASE WHEN merchant_location != customer_home_country THEN 20 ELSE 0 END +
     CASE WHEN EXTRACT('HOUR', transaction_timestamp) BETWEEN 2 AND 5 THEN 15 ELSE 0 END) as risk_score
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
WHERE transaction_timestamp > NOW() - INTERVAL '1' HOUR;
```

### Unusual Spending Patterns

```sql
-- Detect spending anomalies compared to customer history
SELECT
    customer_id,
    transaction_id,
    amount,
    merchant_category,
    -- Customer's historical patterns
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_timestamp
        ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
    ) as avg_amount_30_days,
    COUNT(*) OVER (
        PARTITION BY customer_id, merchant_category
        ORDER BY transaction_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) as previous_transactions_in_category,
    -- Anomaly detection
    CASE
        WHEN amount > 5 * AVG(amount) OVER (
            PARTITION BY customer_id
            ORDER BY transaction_timestamp
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) THEN 'UNUSUALLY_LARGE'
        WHEN COUNT(*) OVER (
            PARTITION BY customer_id, merchant_category
            ORDER BY transaction_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) = 0 THEN 'NEW_MERCHANT_CATEGORY'
        ELSE 'NORMAL'
    END as spending_anomaly
FROM transactions
WHERE transaction_timestamp > NOW() - INTERVAL '7' DAYS;
```

## Account Takeover Detection

### Suspicious Login Patterns

```sql
-- Detect account takeover attempts
SELECT
    user_id,
    login_timestamp,
    ip_address,
    user_agent,
    location_country,
    -- Previous login comparison
    LAG(ip_address, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp) as prev_ip,
    LAG(location_country, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp) as prev_country,
    LAG(login_timestamp, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp) as prev_login,
    -- Geographic velocity (impossible travel)
    CASE
        WHEN LAG(location_country, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp) != location_country
         AND DATEDIFF('minutes',
             LAG(login_timestamp, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp),
             login_timestamp
         ) < 120  -- Less than 2 hours between different countries
        THEN 'IMPOSSIBLE_TRAVEL'
        ELSE 'NORMAL'
    END as travel_anomaly,
    -- Login frequency
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY login_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as logins_last_hour,
    -- Device consistency
    CASE
        WHEN user_agent != LAG(user_agent, 1) OVER (PARTITION BY user_id ORDER BY login_timestamp)
        THEN 'NEW_DEVICE'
        ELSE 'KNOWN_DEVICE'
    END as device_consistency
FROM user_logins
WHERE login_timestamp > NOW() - INTERVAL '24' HOURS;
```

### Failed Authentication Analysis

```sql
-- Brute force and credential stuffing detection
SELECT
    ip_address,
    user_id,
    login_timestamp,
    success_flag,
    -- Count failed attempts
    COUNT(CASE WHEN success_flag = false THEN 1 END) OVER (
        PARTITION BY ip_address
        ORDER BY login_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as failed_attempts_from_ip_1h,
    COUNT(CASE WHEN success_flag = false THEN 1 END) OVER (
        PARTITION BY user_id
        ORDER BY login_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as failed_attempts_for_user_1h,
    -- Distinct users from same IP
    COUNT(DISTINCT user_id) OVER (
        PARTITION BY ip_address
        ORDER BY login_timestamp
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as unique_users_from_ip,
    -- Attack classification
    CASE
        WHEN COUNT(CASE WHEN success_flag = false THEN 1 END) OVER (
            PARTITION BY ip_address
            ORDER BY login_timestamp
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ) > 50 THEN 'BRUTE_FORCE_ATTACK'
        WHEN COUNT(DISTINCT user_id) OVER (
            PARTITION BY ip_address
            ORDER BY login_timestamp
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ) > 20 THEN 'CREDENTIAL_STUFFING'
        ELSE 'NORMAL'
    END as attack_type
FROM login_attempts
WHERE login_timestamp > NOW() - INTERVAL '6' HOURS;
```

## Payment Fraud Detection

### Card Testing Detection

```sql
-- Detect card testing (small transactions to verify stolen cards)
SELECT
    customer_id,
    payment_method_id,
    COUNT(*) as small_transactions_count,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    AVG(amount) as avg_amount,
    MIN(transaction_timestamp) as first_transaction,
    MAX(transaction_timestamp) as last_transaction,
    DATEDIFF('minutes', MIN(transaction_timestamp), MAX(transaction_timestamp)) as time_span_minutes,
    -- Card testing indicators
    CASE
        WHEN COUNT(*) >= 5
         AND MAX(amount) < 10
         AND DATEDIFF('minutes', MIN(transaction_timestamp), MAX(transaction_timestamp)) < 60
        THEN 'CARD_TESTING_SUSPECTED'
        ELSE 'NORMAL'
    END as fraud_indicator
FROM transactions
WHERE transaction_timestamp > NOW() - INTERVAL '2' HOURS
  AND amount < 50  -- Focus on small amounts
GROUP BY customer_id, payment_method_id
HAVING COUNT(*) >= 3
ORDER BY small_transactions_count DESC;
```

### Chargeback Risk Assessment

```sql
-- Identify high chargeback risk transactions
SELECT
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.merchant_category,
    c.account_age_days,
    c.previous_chargebacks,
    -- Risk factors
    CASE WHEN c.account_age_days < 30 THEN 'NEW_ACCOUNT' END as new_account_flag,
    CASE WHEN c.previous_chargebacks > 0 THEN 'CHARGEBACK_HISTORY' END as chargeback_history_flag,
    CASE WHEN t.amount > c.avg_transaction_amount * 3 THEN 'UNUSUAL_AMOUNT' END as amount_flag,
    -- Merchant risk
    m.chargeback_rate as merchant_chargeback_rate,
    CASE WHEN m.chargeback_rate > 0.05 THEN 'HIGH_RISK_MERCHANT' END as merchant_risk_flag,
    -- Overall risk score
    (CASE WHEN c.account_age_days < 30 THEN 25 ELSE 0 END +
     CASE WHEN c.previous_chargebacks > 0 THEN 30 ELSE 0 END +
     CASE WHEN t.amount > c.avg_transaction_amount * 3 THEN 20 ELSE 0 END +
     CASE WHEN m.chargeback_rate > 0.05 THEN 20 ELSE 0 END) as chargeback_risk_score
FROM transactions t
JOIN customer_risk_profile c ON t.customer_id = c.customer_id
JOIN merchant_risk_profile m ON t.merchant_id = m.merchant_id
WHERE t.transaction_timestamp > NOW() - INTERVAL '1' HOUR;
```

## E-commerce Fraud Patterns

### Return Fraud Detection

```sql
-- Detect return fraud patterns
SELECT
    customer_id,
    product_id,
    COUNT(*) as return_count,
    SUM(return_amount) as total_return_amount,
    AVG(DATEDIFF('days', purchase_date, return_date)) as avg_days_to_return,
    -- Fraud indicators
    CASE
        WHEN COUNT(*) > 10 AND AVG(DATEDIFF('days', purchase_date, return_date)) < 2
        THEN 'SUSPICIOUS_RETURN_PATTERN'
        WHEN SUM(return_amount) > 5000 AND COUNT(*) > 5
        THEN 'HIGH_VALUE_RETURNS'
        ELSE 'NORMAL'
    END as return_fraud_flag,
    -- Return reasons analysis
    STRING_AGG(DISTINCT return_reason, ', ') as return_reasons
FROM returns
WHERE return_date > NOW() - INTERVAL '30' DAYS
GROUP BY customer_id, product_id
HAVING COUNT(*) >= 3
ORDER BY return_count DESC, total_return_amount DESC;
```

### Promo Code Abuse

```sql
-- Detect promo code abuse
SELECT
    promo_code,
    customer_id,
    COUNT(*) as usage_count,
    SUM(discount_amount) as total_discount,
    MIN(transaction_timestamp) as first_use,
    MAX(transaction_timestamp) as last_use,
    -- Abuse patterns
    CASE
        WHEN COUNT(*) > (SELECT max_uses_per_customer FROM promo_codes WHERE code = p.promo_code)
        THEN 'EXCEEDED_USAGE_LIMIT'
        WHEN COUNT(DISTINCT ip_address) = 1 AND COUNT(DISTINCT customer_id) > 10
        THEN 'MULTIPLE_ACCOUNTS_SAME_IP'
        ELSE 'NORMAL'
    END as abuse_pattern
FROM promo_usage p
WHERE transaction_timestamp > NOW() - INTERVAL '7' DAYS
GROUP BY promo_code, customer_id
HAVING COUNT(*) > 1
ORDER BY usage_count DESC;
```

## Real-Time Fraud Scoring

### Comprehensive Fraud Score

```sql
-- Real-time fraud scoring system
SELECT
    transaction_id,
    customer_id,
    amount,
    merchant_id,
    transaction_timestamp,
    -- Individual risk components
    CASE WHEN amount > customer_avg_amount * 5 THEN 20 ELSE 0 END as amount_risk,
    CASE WHEN merchant_risk_level = 'HIGH' THEN 15 ELSE 0 END as merchant_risk,
    CASE WHEN customer_risk_tier = 'HIGH' THEN 25 ELSE 0 END as customer_risk,
    CASE WHEN transaction_country != customer_country THEN 10 ELSE 0 END as location_risk,
    CASE WHEN EXTRACT('HOUR', transaction_timestamp) BETWEEN 2 AND 6 THEN 5 ELSE 0 END as time_risk,
    -- Velocity risk
    CASE
        WHEN transactions_last_hour > 10 THEN 20
        WHEN transactions_last_hour > 5 THEN 10
        ELSE 0
    END as velocity_risk,
    -- Total fraud score
    (CASE WHEN amount > customer_avg_amount * 5 THEN 20 ELSE 0 END +
     CASE WHEN merchant_risk_level = 'HIGH' THEN 15 ELSE 0 END +
     CASE WHEN customer_risk_tier = 'HIGH' THEN 25 ELSE 0 END +
     CASE WHEN transaction_country != customer_country THEN 10 ELSE 0 END +
     CASE WHEN EXTRACT('HOUR', transaction_timestamp) BETWEEN 2 AND 6 THEN 5 ELSE 0 END +
     CASE WHEN transactions_last_hour > 10 THEN 20 WHEN transactions_last_hour > 5 THEN 10 ELSE 0 END) as total_fraud_score,
    -- Risk classification
    CASE
        WHEN (CASE WHEN amount > customer_avg_amount * 5 THEN 20 ELSE 0 END +
              CASE WHEN merchant_risk_level = 'HIGH' THEN 15 ELSE 0 END +
              CASE WHEN customer_risk_tier = 'HIGH' THEN 25 ELSE 0 END) >= 70 THEN 'BLOCK'
        WHEN (CASE WHEN amount > customer_avg_amount * 5 THEN 20 ELSE 0 END +
              CASE WHEN merchant_risk_level = 'HIGH' THEN 15 ELSE 0 END) >= 40 THEN 'REVIEW'
        WHEN (CASE WHEN amount > customer_avg_amount * 5 THEN 20 ELSE 0 END) >= 20 THEN 'MONITOR'
        ELSE 'APPROVE'
    END as recommendation
FROM transaction_risk_view
WHERE transaction_timestamp > NOW() - INTERVAL '5' MINUTES;
```

## Fraud Alert System

### Real-Time Alerts

```sql
-- Generate fraud alerts for immediate action
SELECT
    'FRAUD_ALERT' as alert_type,
    transaction_id,
    customer_id,
    amount,
    fraud_score,
    CASE
        WHEN fraud_score >= 80 THEN 'CRITICAL'
        WHEN fraud_score >= 60 THEN 'HIGH'
        WHEN fraud_score >= 40 THEN 'MEDIUM'
        ELSE 'LOW'
    END as alert_severity,
    CONCAT(
        'Fraud Score: ', fraud_score,
        ' | Amount: $', amount,
        ' | Customer: ', customer_id,
        CASE WHEN amount_risk > 0 THEN ' | LARGE_AMOUNT' ELSE '' END,
        CASE WHEN velocity_risk > 0 THEN ' | HIGH_VELOCITY' ELSE '' END,
        CASE WHEN location_risk > 0 THEN ' | FOREIGN_TXN' ELSE '' END
    ) as alert_message,
    NOW() as alert_timestamp
FROM fraud_scoring_results
WHERE fraud_score >= 40
  AND transaction_timestamp > NOW() - INTERVAL '1' MINUTE
ORDER BY fraud_score DESC;
```

### Fraud Investigation Queue

```sql
-- Prioritized queue for fraud investigation
SELECT
    ROW_NUMBER() OVER (ORDER BY fraud_score DESC, amount DESC) as queue_position,
    transaction_id,
    customer_id,
    amount,
    fraud_score,
    transaction_timestamp,
    DATEDIFF('minutes', transaction_timestamp, NOW()) as age_minutes,
    investigation_status,
    assigned_investigator,
    -- Investigation priority
    CASE
        WHEN fraud_score >= 80 AND amount > 1000 THEN 'URGENT'
        WHEN fraud_score >= 60 THEN 'HIGH'
        WHEN fraud_score >= 40 THEN 'MEDIUM'
        ELSE 'LOW'
    END as investigation_priority,
    -- SLA deadline
    CASE
        WHEN fraud_score >= 80 THEN transaction_timestamp + INTERVAL '15' MINUTES
        WHEN fraud_score >= 60 THEN transaction_timestamp + INTERVAL '1' HOUR
        ELSE transaction_timestamp + INTERVAL '4' HOURS
    END as investigation_deadline
FROM fraud_cases
WHERE investigation_status IN ('PENDING', 'IN_PROGRESS')
ORDER BY queue_position;
```

## Performance Monitoring

### Fraud Detection Metrics

```sql
-- Fraud detection system performance
SELECT
    DATE(transaction_timestamp) as date,
    COUNT(*) as total_transactions,
    COUNT(CASE WHEN fraud_score >= 40 THEN 1 END) as flagged_transactions,
    COUNT(CASE WHEN fraud_score >= 40 THEN 1 END) * 100.0 / COUNT(*) as flag_rate_pct,
    COUNT(CASE WHEN confirmed_fraud = true THEN 1 END) as confirmed_fraud,
    COUNT(CASE WHEN confirmed_fraud = true AND fraud_score < 40 THEN 1 END) as missed_fraud,
    COUNT(CASE WHEN confirmed_fraud = false AND fraud_score >= 40 THEN 1 END) as false_positives,
    -- Precision and Recall
    COUNT(CASE WHEN confirmed_fraud = true AND fraud_score >= 40 THEN 1 END) * 100.0 /
    NULLIF(COUNT(CASE WHEN fraud_score >= 40 THEN 1 END), 0) as precision_pct,
    COUNT(CASE WHEN confirmed_fraud = true AND fraud_score >= 40 THEN 1 END) * 100.0 /
    NULLIF(COUNT(CASE WHEN confirmed_fraud = true THEN 1 END), 0) as recall_pct
FROM fraud_analysis_history
WHERE transaction_timestamp > NOW() - INTERVAL '30' DAYS
GROUP BY DATE(transaction_timestamp)
ORDER BY date DESC;
```