# Graceful Degradation Guide

**Version**: 1.0
**Last Updated**: September 28, 2025
**Status**: Production Ready

## Overview

Graceful degradation in Velostream ensures that stream processing continues even when reference tables are unavailable, slow to load, or experiencing issues. This guide covers the 5 degradation strategies and practical implementation examples.

## Table of Contents

- [Understanding Graceful Degradation](#understanding-graceful-degradation)
- [The 5 Degradation Strategies](#the-5-degradation-strategies)
- [Strategy Implementation Examples](#strategy-implementation-examples)
- [Real-World Use Cases](#real-world-use-cases)
- [Performance Considerations](#performance-considerations)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Production Best Practices](#production-best-practices)
- [Configuration Reference](#configuration-reference)

## Understanding Graceful Degradation

### The Problem

In stream-table joins, reference tables may be unavailable due to:
- **Loading delays**: Large tables taking time to populate
- **Network issues**: Connectivity problems with data sources
- **Source failures**: Kafka topics down, files missing
- **System overload**: High load causing table access delays

### The Solution

Graceful degradation provides configurable fallback behaviors that keep streams processing while maintaining data quality expectations.

### Strategy Selection Matrix

| Scenario | Recommended Strategy | Use Case |
|----------|---------------------|----------|
| **Critical Data Missing** | `FailFast` | Financial transactions requiring complete data |
| **Optional Enrichment** | `EmitWithNulls` | Adding display names to events |
| **Known Defaults Available** | `UseDefaults` | User preferences with sensible defaults |
| **Temporary Issues** | `WaitAndRetry` | Brief network or loading delays |
| **Non-Essential Records** | `SkipRecord` | Filtering out incomplete data |

## The 5 Degradation Strategies

### 1. UseDefaults
**Purpose**: Provide fallback values when table data is missing
**Best For**: Optional enrichment with known good defaults

### 2. SkipRecord
**Purpose**: Filter out records that cannot be enriched
**Best For**: Quality-first scenarios where incomplete data is unacceptable

### 3. EmitWithNulls
**Purpose**: Continue processing with NULL values for missing data
**Best For**: Downstream systems that can handle incomplete data

### 4. WaitAndRetry
**Purpose**: Retry table lookups with exponential backoff
**Best For**: Temporary issues that resolve quickly

### 5. FailFast
**Purpose**: Stop processing and raise errors immediately
**Best For**: Critical systems requiring complete data integrity

## Strategy Implementation Examples

### 1. UseDefaults Strategy

#### Basic Default Values

```sql
-- Stream with user preference defaults
CREATE STREAM personalized_content AS
SELECT
    e.event_id,
    e.content_id,
    e.user_id,
    COALESCE(p.language, 'en') as language,
    COALESCE(p.theme, 'light') as theme,
    COALESCE(p.timezone, 'UTC') as timezone
FROM content_events e
LEFT JOIN user_preferences p ON e.user_id = p.user_id
WITH (
    "fallback.strategy" = "UseDefaults",
    "fallback.defaults" = '{
        "language": "en",
        "theme": "light",
        "timezone": "UTC",
        "notifications": true
    }'
);
```

#### Context-Aware Defaults

```rust
use velostream::velostream::server::graceful_degradation::*;
use std::collections::HashMap;

// Create context-aware defaults based on record content
let mut defaults = HashMap::new();

// Geographic defaults
defaults.insert("currency".to_string(), FieldValue::String("USD".to_string()));
defaults.insert("locale".to_string(), FieldValue::String("en_US".to_string()));

// Business defaults
defaults.insert("tier".to_string(), FieldValue::String("standard".to_string()));
defaults.insert("risk_score".to_string(), FieldValue::ScaledInteger(500, 2)); // 5.00

// Time-based defaults
defaults.insert("business_hours".to_string(), FieldValue::Boolean(true));

let strategy = TableMissingDataStrategy::UseDefaults(defaults);
```

#### Advanced Default Logic

```rust
// Dynamic defaults based on record content
impl DynamicDefaultProvider {
    fn get_defaults_for_record(&self, record: &StreamRecord) -> HashMap<String, FieldValue> {
        let mut defaults = HashMap::new();

        // Region-based defaults
        if let Some(FieldValue::String(country)) = record.fields.get("country") {
            match country.as_str() {
                "US" => {
                    defaults.insert("currency".to_string(), FieldValue::String("USD".to_string()));
                    defaults.insert("tax_rate".to_string(), FieldValue::ScaledInteger(875, 3)); // 8.75%
                }
                "GB" => {
                    defaults.insert("currency".to_string(), FieldValue::String("GBP".to_string()));
                    defaults.insert("tax_rate".to_string(), FieldValue::ScaledInteger(200, 2)); // 20%
                }
                "DE" => {
                    defaults.insert("currency".to_string(), FieldValue::String("EUR".to_string()));
                    defaults.insert("tax_rate".to_string(), FieldValue::ScaledInteger(190, 2)); // 19%
                }
                _ => {
                    defaults.insert("currency".to_string(), FieldValue::String("USD".to_string()));
                    defaults.insert("tax_rate".to_string(), FieldValue::ScaledInteger(0, 2)); // 0%
                }
            }
        }

        // Time-based defaults
        let now = chrono::Utc::now();
        let business_hours = now.hour() >= 9 && now.hour() < 17;
        defaults.insert("business_hours".to_string(), FieldValue::Boolean(business_hours));

        defaults
    }
}
```

### 2. SkipRecord Strategy

#### Quality Filtering

```sql
-- Skip incomplete financial records
CREATE STREAM verified_trades AS
SELECT
    t.trade_id,
    t.symbol,
    t.quantity,
    t.price,
    r.risk_limit,
    r.position_limit
FROM trade_events t
INNER JOIN risk_limits r ON t.trader_id = r.trader_id
WITH (
    "fallback.strategy" = "SkipRecord",
    "skip.reason.logging" = true,  -- Log why records were skipped
    "skip.metrics.enabled" = true  -- Track skip rates
);
```

#### Conditional Skipping

```rust
// Skip records based on business logic
let skip_strategy = ConditionalSkipStrategy {
    skip_conditions: vec![
        // Skip if critical fields are missing
        SkipCondition::MissingField("account_balance".to_string()),
        SkipCondition::MissingField("credit_score".to_string()),

        // Skip if risk data is unavailable for high-value trades
        SkipCondition::Custom(Box::new(|record| {
            if let Some(FieldValue::ScaledInteger(amount, _)) = record.fields.get("trade_amount") {
                *amount > 1_000_000 // Skip if >$10,000 and missing risk data
            } else {
                false
            }
        })),
    ],
    fallback_for_low_value: Some(TableMissingDataStrategy::UseDefaults(default_risk_values)),
};
```

#### Skip Rate Monitoring

```rust
// Monitor skip rates for quality alerts
impl SkipRateMonitor {
    async fn check_skip_rates(&self) -> Result<(), QualityAlert> {
        let stats = self.get_skip_statistics(Duration::from_minutes(5)).await;

        if stats.skip_rate > 0.10 { // 10% skip rate threshold
            return Err(QualityAlert {
                severity: AlertSeverity::Critical,
                message: format!(
                    "High skip rate detected: {:.1}% of records skipped in last 5 minutes",
                    stats.skip_rate * 100.0
                ),
                suggested_action: "Check table loading status and data source health",
            });
        }

        if stats.skip_rate > 0.05 { // 5% warning threshold
            log::warn!(
                "Elevated skip rate: {:.1}% of records skipped",
                stats.skip_rate * 100.0
            );
        }

        Ok(())
    }
}
```

### 3. EmitWithNulls Strategy

#### Basic Null Emission

```sql
-- Continue processing with NULLs for missing enrichment
CREATE STREAM events_with_optional_enrichment AS
SELECT
    e.event_id,
    e.timestamp,
    e.user_id,
    e.action,
    u.display_name,  -- May be NULL if user_profiles unavailable
    u.avatar_url,    -- May be NULL
    u.preferences    -- May be NULL
FROM user_events e
LEFT JOIN user_profiles u ON e.user_id = u.user_id
WITH (
    "fallback.strategy" = "EmitWithNulls",
    "null.field.tracking" = true,    -- Track which fields are NULL
    "null.rate.alerting" = true      -- Alert on high NULL rates
);
```

#### Null Value Documentation

```rust
// Document expected NULL behavior for downstream consumers
let null_strategy_config = EmitWithNullsConfig {
    nullable_fields: vec![
        NullableField {
            field_name: "display_name".to_string(),
            description: "User's display name, NULL if profile loading".to_string(),
            default_display: Some("Unknown User".to_string()),
        },
        NullableField {
            field_name: "avatar_url".to_string(),
            description: "User's avatar URL, NULL if profile unavailable".to_string(),
            default_display: Some("/images/default-avatar.png".to_string()),
        },
        NullableField {
            field_name: "preferences".to_string(),
            description: "User preferences JSON, NULL if profile loading".to_string(),
            default_display: Some("{}".to_string()),
        },
    ],
    null_rate_threshold: 0.20, // Alert if >20% of records have NULLs
    null_tracking_enabled: true,
};
```

#### Downstream Null Handling

```rust
// Help downstream consumers handle NULLs gracefully
impl NullAwareProcessor {
    fn process_record_with_nulls(&self, record: &StreamRecord) -> ProcessedRecord {
        let mut processed = ProcessedRecord::new();

        // Handle nullable display name
        let display_name = record.fields.get("display_name")
            .and_then(|v| match v {
                FieldValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "Unknown User".to_string());

        processed.insert("display_name".to_string(), FieldValue::String(display_name));

        // Handle nullable preferences with defaults
        let preferences = record.fields.get("preferences")
            .and_then(|v| match v {
                FieldValue::String(json) => serde_json::from_str(json).ok(),
                _ => None,
            })
            .unwrap_or_else(|| serde_json::json!({"theme": "light", "language": "en"}));

        processed.insert("preferences".to_string(),
            FieldValue::String(preferences.to_string()));

        processed
    }
}
```

### 4. WaitAndRetry Strategy

#### Basic Retry Configuration

```sql
-- Retry table lookups for temporary issues
CREATE STREAM resilient_enrichment AS
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.credit_limit,
    c.payment_method
FROM order_events o
JOIN customer_data c ON o.customer_id = c.customer_id
WITH (
    "fallback.strategy" = "WaitAndRetry",
    "retry.max_attempts" = "3",
    "retry.initial_delay" = "1s",
    "retry.max_delay" = "10s",
    "retry.exponential_base" = "2.0"
);
```

#### Advanced Retry Logic

```rust
use velostream::velostream::server::graceful_degradation::*;
use std::time::Duration;

// Configure sophisticated retry behavior
let retry_strategy = TableMissingDataStrategy::WaitAndRetry {
    max_retries: 5,
    initial_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(30),
    exponential_base: 1.5,
    jitter: true, // Add randomness to prevent thundering herd
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 10,
        recovery_timeout: Duration::from_secs(300),
        failure_rate_window: Duration::from_secs(60),
        min_calls_in_window: 5,
        failure_rate_threshold: 75.0,
    }),
};
```

#### Retry with Fallback Chain

```rust
// Chain multiple strategies for robust handling
let fallback_chain = FallbackChain {
    strategies: vec![
        // First: Try quick retry
        TableMissingDataStrategy::WaitAndRetry {
            max_retries: 2,
            delay: Duration::from_millis(500),
        },
        // Second: Use defaults if retries fail
        TableMissingDataStrategy::UseDefaults(emergency_defaults),
        // Last resort: Emit with nulls
        TableMissingDataStrategy::EmitWithNulls,
    ],
};
```

#### Intelligent Retry Decisions

```rust
// Retry logic based on error type and system state
impl IntelligentRetryStrategy {
    async fn should_retry(&self, error: &JoinError, attempt: u32) -> bool {
        match error {
            JoinError::TableLoading => {
                // Always retry if table is just loading
                attempt < self.max_retries
            }
            JoinError::NetworkTimeout => {
                // Retry network issues with backoff
                attempt < 3 && self.system_health.network_ok()
            }
            JoinError::TableCorrupted => {
                // Don't retry corruption errors
                false
            }
            JoinError::PermissionDenied => {
                // Don't retry auth errors
                false
            }
            JoinError::ResourceExhausted => {
                // Retry resource issues with longer delay
                attempt < 2 && self.get_retry_delay(attempt) > Duration::from_secs(5)
            }
        }
    }

    fn get_retry_delay(&self, attempt: u32) -> Duration {
        let base_delay = Duration::from_millis(500);
        let exponential = self.exponential_base.powi(attempt as i32);
        let delay = base_delay.mul_f64(exponential);

        // Add jitter to prevent thundering herd
        let jitter = if self.jitter_enabled {
            Duration::from_millis(rand::random::<u64>() % 100)
        } else {
            Duration::ZERO
        };

        std::cmp::min(delay + jitter, self.max_delay)
    }
}
```

### 5. FailFast Strategy

#### Critical Data Validation

```sql
-- Fail immediately for critical financial operations
CREATE STREAM validated_transactions AS
SELECT
    t.transaction_id,
    t.account_id,
    t.amount,
    a.balance,
    a.account_status,
    r.daily_limit
FROM transaction_events t
INNER JOIN account_balances a ON t.account_id = a.account_id
INNER JOIN risk_controls r ON t.account_id = r.account_id
WITH (
    "fallback.strategy" = "FailFast",
    "error.message" = "Critical account data unavailable - transaction cannot be processed",
    "alert.on.failure" = true,
    "failure.circuit.breaker" = true
);
```

#### Business-Critical Validation

```rust
// Fail fast for business-critical scenarios
let critical_strategy = FailFastConfig {
    error_message: "Regulatory compliance data unavailable".to_string(),
    required_fields: vec![
        "kyc_status".to_string(),
        "sanctions_check".to_string(),
        "risk_rating".to_string(),
    ],
    alert_config: AlertConfig {
        enabled: true,
        severity: AlertSeverity::Critical,
        escalation_delay: Duration::from_minutes(2),
        notification_channels: vec!["ops-team", "compliance-team"],
    },
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 3, // Very low threshold for critical data
        recovery_timeout: Duration::from_minutes(15),
        failure_rate_window: Duration::from_minutes(5),
        min_calls_in_window: 1,
        failure_rate_threshold: 100.0, // Any failure triggers circuit
    }),
};
```

#### Graceful Shutdown on Critical Failures

```rust
// Implement graceful shutdown for critical system failures
impl CriticalFailureHandler {
    async fn handle_critical_failure(&self, failure: &CriticalFailure) -> Result<(), SystemError> {
        // Log critical failure
        log::error!("Critical failure detected: {}", failure.message);

        // Send immediate alerts
        self.alert_manager.send_critical_alert(failure).await?;

        // Stop accepting new work
        self.gracefully_shutdown_processing().await?;

        // Wait for current work to complete
        self.wait_for_active_streams(Duration::from_secs(30)).await?;

        // Update health status
        self.health_monitor.mark_critical_failure(failure).await?;

        Ok(())
    }

    async fn gracefully_shutdown_processing(&self) -> Result<(), SystemError> {
        // Stop accepting new streams
        self.stream_processor.pause_new_work().await?;

        // Allow current batches to complete
        let active_count = self.stream_processor.get_active_count().await;
        log::info!("Waiting for {} active streams to complete", active_count);

        // Set shutdown timeout
        let shutdown_timeout = Duration::from_secs(60);
        let start = Instant::now();

        while self.stream_processor.has_active_work().await {
            if start.elapsed() > shutdown_timeout {
                log::warn!("Shutdown timeout reached, forcing termination");
                self.stream_processor.force_shutdown().await?;
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Ok(())
    }
}
```

## Real-World Use Cases

### Use Case 1: E-commerce Personalization

**Scenario**: Personalizing product recommendations when user profile data is slow to load

```sql
-- E-commerce recommendation stream with graceful degradation
CREATE STREAM personalized_recommendations AS
SELECT
    v.session_id,
    v.product_id,
    v.user_id,
    v.timestamp,
    COALESCE(p.preferred_categories, '["general"]') as categories,
    COALESCE(p.price_range, 'medium') as price_preference,
    COALESCE(p.brand_preferences, '[]') as brands,
    COALESCE(h.recent_views, '[]') as recent_products
FROM product_views v
LEFT JOIN user_profiles p ON v.user_id = p.user_id
LEFT JOIN user_history h ON v.user_id = h.user_id
WITH (
    "fallback.strategy" = "UseDefaults",
    "fallback.defaults" = '{
        "preferred_categories": ["general", "popular"],
        "price_range": "medium",
        "brand_preferences": [],
        "recent_views": []
    }',
    "table.wait.timeout" = "2s"  -- Quick timeout for real-time UX
);
```

### Use Case 2: Financial Risk Management

**Scenario**: Real-time fraud detection requiring complete risk data

```sql
-- Fraud detection with strict data requirements
CREATE STREAM fraud_alerts AS
SELECT
    t.transaction_id,
    t.amount,
    t.merchant_id,
    t.card_id,
    r.risk_score,
    r.fraud_indicators,
    h.recent_transaction_count,
    h.spending_pattern_anomaly
FROM transaction_stream t
INNER JOIN risk_profiles r ON t.card_id = r.card_id
INNER JOIN transaction_history h ON t.card_id = h.card_id
WHERE r.risk_score > 0.7 OR h.spending_pattern_anomaly = true
WITH (
    "fallback.strategy" = "FailFast",
    "error.message" = "Risk data unavailable - cannot process transaction",
    "alert.compliance.team" = true,
    "circuit.breaker.enabled" = true
);
```

### Use Case 3: IoT Sensor Enrichment

**Scenario**: Enriching sensor data with device metadata, handling intermittent connectivity

```sql
-- IoT data enrichment with retry for connectivity issues
CREATE STREAM enriched_sensor_data AS
SELECT
    s.sensor_id,
    s.timestamp,
    s.value,
    s.unit,
    d.device_type,
    d.location,
    d.calibration_date,
    d.accuracy_rating
FROM sensor_readings s
LEFT JOIN device_metadata d ON s.sensor_id = d.sensor_id
WITH (
    "fallback.strategy" = "WaitAndRetry",
    "retry.max_attempts" = "5",
    "retry.initial_delay" = "2s",
    "retry.exponential_base" = "1.5",
    "fallback.after.retry" = "EmitWithNulls"
);
```

### Use Case 4: Content Moderation

**Scenario**: Content filtering where moderation rules must be complete

```sql
-- Content moderation requiring complete rule data
CREATE STREAM moderated_content AS
SELECT
    c.content_id,
    c.user_id,
    c.content_text,
    c.content_type,
    m.severity_level,
    m.auto_action,
    m.human_review_required
FROM content_submissions c
INNER JOIN moderation_rules m ON c.content_type = m.content_type
WHERE m.severity_level != 'blocked'
WITH (
    "fallback.strategy" = "SkipRecord",
    "skip.reason" = "Moderation rules unavailable",
    "skip.alert.threshold" = "0.05",  -- Alert if >5% skipped
    "alternative.action" = "queue_for_manual_review"
);
```

### Use Case 5: Gaming Leaderboards

**Scenario**: Real-time leaderboard updates with player profile enrichment

```sql
-- Gaming leaderboard with graceful profile degradation
CREATE STREAM live_leaderboard AS
SELECT
    g.game_id,
    g.player_id,
    g.score,
    g.timestamp,
    COALESCE(p.display_name, CONCAT('Player_', g.player_id)) as name,
    COALESCE(p.avatar_url, '/images/default-avatar.png') as avatar,
    COALESCE(p.country, 'Unknown') as country,
    COALESCE(p.level, 1) as player_level
FROM game_scores g
LEFT JOIN player_profiles p ON g.player_id = p.player_id
WITH (
    "fallback.strategy" = "UseDefaults",
    "fallback.defaults" = '{
        "display_name": "Anonymous Player",
        "avatar_url": "/images/default-avatar.png",
        "country": "Unknown",
        "level": 1,
        "achievements": []
    }',
    "table.wait.timeout" = "1s"  -- Very fast for gaming UX
);
```

## Performance Considerations

### Strategy Performance Comparison

| Strategy | Latency Impact | Throughput Impact | Memory Usage | CPU Usage |
|----------|----------------|-------------------|--------------|-----------|
| **UseDefaults** | Low | Minimal | Low | Low |
| **SkipRecord** | Very Low | Negative (reduces output) | Very Low | Very Low |
| **EmitWithNulls** | Very Low | Minimal | Low | Low |
| **WaitAndRetry** | High | High (during retries) | Medium | Medium |
| **FailFast** | Low | High (stops processing) | Low | Low |

### Performance Optimization Tips

#### 1. Default Value Caching

```rust
// Cache expensive default calculations
use std::sync::Arc;
use tokio::sync::RwLock;

struct CachedDefaultProvider {
    cache: Arc<RwLock<HashMap<String, HashMap<String, FieldValue>>>>,
    cache_ttl: Duration,
}

impl CachedDefaultProvider {
    async fn get_defaults(&self, context: &str) -> HashMap<String, FieldValue> {
        let cache = self.cache.read().await;
        if let Some(cached) = cache.get(context) {
            return cached.clone();
        }
        drop(cache);

        // Calculate expensive defaults
        let defaults = self.calculate_context_defaults(context).await;

        // Cache for future use
        let mut cache = self.cache.write().await;
        cache.insert(context.to_string(), defaults.clone());

        defaults
    }
}
```

#### 2. Retry Optimization

```rust
// Optimize retry performance with intelligent batching
impl BatchRetryStrategy {
    async fn retry_batch(&self, failed_records: Vec<StreamRecord>) -> Vec<StreamRecord> {
        // Group retries by table for efficient batch lookups
        let mut by_table: HashMap<String, Vec<StreamRecord>> = HashMap::new();
        for record in failed_records {
            let table_key = self.extract_table_key(&record);
            by_table.entry(table_key).or_default().push(record);
        }

        let mut successful = Vec::new();

        // Retry each table's records as a batch
        for (table, records) in by_table {
            match self.retry_table_batch(&table, records).await {
                Ok(mut batch_results) => successful.append(&mut batch_results),
                Err(_) => {
                    // Handle batch failure - could retry individually or apply fallback
                    for record in records {
                        if let Some(fallback) = self.apply_fallback_strategy(&record) {
                            successful.push(fallback);
                        }
                    }
                }
            }
        }

        successful
    }
}
```

#### 3. Memory-Efficient Null Handling

```rust
// Optimize memory usage for NULL values
struct CompactNullRecord {
    base_fields: HashMap<String, FieldValue>,
    null_field_mask: BitSet, // Track which fields are NULL
}

impl CompactNullRecord {
    fn get_field(&self, field_name: &str) -> Option<&FieldValue> {
        let field_index = self.get_field_index(field_name);
        if self.null_field_mask.contains(field_index) {
            None // Field is NULL
        } else {
            self.base_fields.get(field_name)
        }
    }

    fn set_field_null(&mut self, field_name: &str) {
        let field_index = self.get_field_index(field_name);
        self.null_field_mask.insert(field_index);
        self.base_fields.remove(field_name); // Free memory
    }
}
```

## Monitoring and Alerting

### Strategy-Specific Metrics

#### UseDefaults Monitoring

```rust
// Monitor default value usage rates
struct DefaultUsageMetrics {
    pub default_usage_rate: f64,      // % of records using defaults
    pub field_default_rates: HashMap<String, f64>, // Per-field default usage
    pub context_default_rates: HashMap<String, f64>, // Per-context usage
}

impl DefaultUsageMonitor {
    async fn check_default_usage_alerts(&self) -> Vec<Alert> {
        let mut alerts = Vec::new();
        let metrics = self.get_current_metrics().await;

        // Alert on high overall default usage
        if metrics.default_usage_rate > 0.50 {
            alerts.push(Alert::new(
                AlertSeverity::Warning,
                format!("High default usage rate: {:.1}%", metrics.default_usage_rate * 100.0),
                "Consider checking table loading performance"
            ));
        }

        // Alert on specific field default usage
        for (field, rate) in metrics.field_default_rates {
            if rate > 0.80 {
                alerts.push(Alert::new(
                    AlertSeverity::Critical,
                    format!("Field '{}' using defaults {:.1}% of time", field, rate * 100.0),
                    format!("Check data source for field '{}'", field)
                ));
            }
        }

        alerts
    }
}
```

#### Retry Strategy Monitoring

```rust
// Monitor retry performance and success rates
struct RetryMetrics {
    pub total_retries: u64,
    pub retry_success_rate: f64,
    pub avg_retry_count: f64,
    pub retry_latency_p95: Duration,
    pub circuit_breaker_trips: u64,
}

impl RetryMonitor {
    async fn analyze_retry_patterns(&self) -> RetryAnalysis {
        let metrics = self.get_metrics_window(Duration::from_minutes(15)).await;

        RetryAnalysis {
            efficiency: metrics.retry_success_rate,
            latency_impact: metrics.retry_latency_p95,
            recommendations: self.generate_recommendations(&metrics),
        }
    }

    fn generate_recommendations(&self, metrics: &RetryMetrics) -> Vec<String> {
        let mut recommendations = Vec::new();

        if metrics.retry_success_rate < 0.70 {
            recommendations.push(
                "Low retry success rate - consider reducing max_retries or improving table loading".to_string()
            );
        }

        if metrics.avg_retry_count > 2.5 {
            recommendations.push(
                "High average retry count - investigate root causes of table unavailability".to_string()
            );
        }

        if metrics.circuit_breaker_trips > 5 {
            recommendations.push(
                "Frequent circuit breaker trips - check system health and table loading stability".to_string()
            );
        }

        recommendations
    }
}
```

### Alerting Configuration

```yaml
# config/degradation-alerts.yaml
degradation_monitoring:
  strategy_thresholds:
    use_defaults:
      warning_rate: 0.30      # 30% default usage
      critical_rate: 0.60     # 60% default usage

    skip_record:
      warning_rate: 0.05      # 5% skip rate
      critical_rate: 0.15     # 15% skip rate

    emit_nulls:
      warning_rate: 0.25      # 25% null rate
      critical_rate: 0.50     # 50% null rate

    wait_retry:
      max_retry_latency: "30s"
      warning_retry_rate: 0.20
      critical_circuit_trips: 10

    fail_fast:
      immediate_alert: true
      escalation_delay: "1m"

  alert_channels:
    - type: "slack"
      channel: "#ops-alerts"
      severity: ["warning", "critical"]

    - type: "email"
      recipients: ["ops-team@company.com"]
      severity: ["critical"]

    - type: "pagerduty"
      service_key: "velostream-degradation"
      severity: ["critical"]
```

## Production Best Practices

### 1. Strategy Selection Guidelines

```yaml
# Decision matrix for strategy selection
strategy_selection_guide:
  use_cases:
    real_time_ui:
      primary: "UseDefaults"
      fallback: "EmitWithNulls"
      timeout: "2s"

    financial_transactions:
      primary: "FailFast"
      fallback: null
      timeout: "5s"

    analytics_enrichment:
      primary: "WaitAndRetry"
      fallback: "EmitWithNulls"
      timeout: "30s"

    content_moderation:
      primary: "SkipRecord"
      fallback: "FailFast"
      timeout: "10s"

    user_personalization:
      primary: "UseDefaults"
      fallback: "EmitWithNulls"
      timeout: "3s"
```

### 2. Testing Degradation Strategies

```rust
// Comprehensive testing framework for degradation strategies
#[cfg(test)]
mod degradation_tests {
    use super::*;

    #[tokio::test]
    async fn test_use_defaults_strategy() {
        let strategy = TableMissingDataStrategy::UseDefaults(test_defaults());
        let result = simulate_table_unavailable(&strategy).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().fields.get("country").unwrap(), &FieldValue::String("US".to_string()));
    }

    #[tokio::test]
    async fn test_retry_strategy_exhaustion() {
        let strategy = TableMissingDataStrategy::WaitAndRetry {
            max_retries: 3,
            delay: Duration::from_millis(10), // Fast for testing
        };

        let start = Instant::now();
        let result = simulate_persistent_table_failure(&strategy).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed >= Duration::from_millis(30)); // 3 retries * 10ms
        assert!(elapsed <= Duration::from_millis(100)); // Should fail quickly
    }

    #[tokio::test]
    async fn test_circuit_breaker_integration() {
        let strategy = create_retry_with_circuit_breaker();

        // Trigger circuit breaker
        for _ in 0..10 {
            let _ = simulate_table_failure(&strategy).await;
        }

        // Verify circuit is open
        let start = Instant::now();
        let result = simulate_table_failure(&strategy).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed < Duration::from_millis(10)); // Should fail immediately
    }
}
```

### 3. Deployment Checklist

#### Pre-Deployment Validation

- [ ] **Strategy Configuration**: All strategies properly configured for use cases
- [ ] **Default Values**: Default values tested and validated for business logic
- [ ] **Timeout Settings**: Timeouts appropriate for expected table loading times
- [ ] **Circuit Breaker**: Circuit breaker thresholds tuned for system characteristics
- [ ] **Monitoring**: Alerts configured for all degradation scenarios
- [ ] **Fallback Chain**: Multiple strategies configured for complex scenarios

#### Post-Deployment Monitoring

- [ ] **Degradation Rates**: Monitor usage of each strategy
- [ ] **Performance Impact**: Measure latency and throughput effects
- [ ] **Quality Metrics**: Track data quality with degradation active
- [ ] **Alert Validation**: Verify alerts trigger appropriately
- [ ] **Business Impact**: Measure impact on business metrics

### 4. Operational Procedures

#### Incident Response

```bash
#!/bin/bash
# Degradation incident response script

echo "=== Graceful Degradation Incident Response ==="

# Check current degradation rates
echo "Current degradation statistics:"
curl -s http://localhost:8080/health/degradation | jq '.'

# Check which tables are causing issues
echo -e "\nProblem tables:"
curl -s http://localhost:8080/health/tables | jq '.tables[] | select(.status != "Healthy")'

# Check circuit breaker status
echo -e "\nCircuit breaker status:"
curl -s http://localhost:8080/health/circuit-breakers | jq '.'

# Recommend actions based on degradation type
echo -e "\nRecommended actions:"
if [ "$(curl -s http://localhost:8080/health/degradation | jq '.use_defaults_rate')" != "null" ]; then
    echo "- High default usage detected - check table loading performance"
fi

if [ "$(curl -s http://localhost:8080/health/degradation | jq '.skip_rate')" != "null" ]; then
    echo "- Records being skipped - investigate data source availability"
fi

if [ "$(curl -s http://localhost:8080/health/degradation | jq '.retry_rate')" != "null" ]; then
    echo "- High retry rate - check network connectivity and table loading"
fi
```

## Configuration Reference

### Strategy Configuration Syntax

```sql
-- General syntax for all strategies
WITH (
    "fallback.strategy" = "StrategyName",
    "fallback.config" = '{strategy-specific-config}',
    "table.wait.timeout" = "duration",
    "fallback.monitoring" = "true|false"
)
```

### UseDefaults Configuration

```json
{
    "fallback.strategy": "UseDefaults",
    "fallback.defaults": {
        "field1": "default_value",
        "field2": 42,
        "field3": true
    },
    "defaults.cache.enabled": true,
    "defaults.cache.ttl": "300s"
}
```

### WaitAndRetry Configuration

```json
{
    "fallback.strategy": "WaitAndRetry",
    "retry.max_attempts": 5,
    "retry.initial_delay": "1s",
    "retry.max_delay": "30s",
    "retry.exponential_base": 2.0,
    "retry.jitter": true,
    "circuit.breaker.enabled": true,
    "circuit.breaker.failure_threshold": 10,
    "circuit.breaker.recovery_timeout": "300s"
}
```

### SkipRecord Configuration

```json
{
    "fallback.strategy": "SkipRecord",
    "skip.logging.enabled": true,
    "skip.metrics.enabled": true,
    "skip.reason.field": "skip_reason",
    "skip.alert.threshold": 0.10
}
```

### EmitWithNulls Configuration

```json
{
    "fallback.strategy": "EmitWithNulls",
    "null.tracking.enabled": true,
    "null.rate.alerting": true,
    "null.rate.threshold": 0.25,
    "null.documentation.enabled": true
}
```

### FailFast Configuration

```json
{
    "fallback.strategy": "FailFast",
    "error.message": "Custom error message",
    "alert.enabled": true,
    "alert.severity": "critical",
    "circuit.breaker.enabled": true,
    "graceful.shutdown": true
}
```

## Conclusion

Graceful degradation strategies provide robust handling of table unavailability scenarios:

### Key Benefits

- **System Resilience**: Continues processing despite table issues
- **Configurable Quality**: Choose appropriate trade-offs for each use case
- **Operational Visibility**: Comprehensive monitoring and alerting
- **Performance Optimization**: Minimal impact on throughput and latency

### Strategy Selection Summary

- **UseDefaults**: Best for optional enrichment with sensible fallbacks
- **SkipRecord**: Ideal for quality-first scenarios
- **EmitWithNulls**: Good for fault-tolerant downstream systems
- **WaitAndRetry**: Excellent for transient issues
- **FailFast**: Required for critical data integrity scenarios

For complete production deployment guidance, see:
- [Progress Monitoring Guide](progress-monitoring-guide.md)
- [Timeout Configuration Guide](timeout-configuration-guide.md)
- [Production Deployment Guide](production-deployment-guide.md)