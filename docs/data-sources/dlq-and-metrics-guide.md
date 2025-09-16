# Dead Letter Queue & Advanced Metrics Configuration Guide

## Table of Contents
1. [Dead Letter Queue Configuration](#dead-letter-queue-configuration)
2. [Throughput Metrics with Statistics](#throughput-metrics-with-statistics)
3. [24-Hour Rolling Metrics](#24-hour-rolling-metrics)
4. [Configuration Examples](#configuration-examples)
5. [Monitoring and Alerting](#monitoring-and-alerting)

## Dead Letter Queue Configuration

### Core DLQ Settings

```yaml
# velo-config.yaml
dead_letter_queue:
  # Storage configuration
  storage:
    type: kafka  # Options: kafka, file, s3, postgresql
    location: "kafka://localhost:9092/dlq-topic"
    
  # Size limits
  max_queue_size: 1000000      # Maximum messages in DLQ
  max_message_size_bytes: 10485760  # 10MB per message
  max_retention_days: 30        # Auto-delete after 30 days
  
  # Retry configuration
  retry:
    max_attempts: 5             # Maximum retry attempts per message
    initial_delay_ms: 1000       # First retry after 1 second
    backoff_multiplier: 2.0      # Exponential backoff multiplier
    max_delay_ms: 3600000        # Maximum 1 hour between retries
    jitter_factor: 0.1           # Add 10% jitter to prevent thundering herd
    
  # Processing configuration  
  batch_size: 100               # Process DLQ in batches
  processing_parallelism: 4      # Parallel DLQ processors
  reprocess_interval: "5m"       # Check for reprocessing every 5 minutes
  
  # Overflow handling
  overflow_policy: reject        # Options: reject, drop_oldest, circular_buffer
  alert_threshold_percent: 80    # Alert when 80% full
```

### Programmatic DLQ Configuration

```rust
use velostream::velo::sql::error::recovery::{
    DeadLetterQueue, DLQConfig, RetryStrategy, OverflowPolicy
};

// Configure DLQ with builder pattern
let dlq = DeadLetterQueue::builder()
    // Storage configuration
    .storage_type(StorageType::Kafka)
    .storage_location("kafka://localhost:9092/dlq-topic")
    
    // Size configuration
    .max_queue_size(1_000_000)
    .max_message_size_bytes(10 * 1024 * 1024) // 10MB
    .max_retention(Duration::from_days(30))
    
    // Retry configuration
    .retry_strategy(RetryStrategy::ExponentialBackoff {
        max_attempts: 5,
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_hours(1),
        multiplier: 2.0,
        jitter: 0.1,
    })
    
    // Processing configuration
    .batch_size(100)
    .processing_parallelism(4)
    .reprocess_interval(Duration::from_minutes(5))
    
    // Overflow configuration
    .overflow_policy(OverflowPolicy::RejectNew)
    .alert_threshold_percent(80)
    
    // Error categorization
    .categorize_errors(true)
    .error_categories(vec![
        ErrorCategory::Transient,     // Network issues, timeouts
        ErrorCategory::Parsing,        // Data format issues
        ErrorCategory::Validation,     // Business rule violations
        ErrorCategory::Permanent,      // Unrecoverable errors
    ])
    
    .build()?;
```

### SQL DLQ Configuration

```sql
-- Configure DLQ for a stream
CREATE STREAM orders_stream AS
SELECT * FROM 'kafka://localhost:9092/orders'
WITH (
    -- DLQ Storage
    dlq_enabled = true,
    dlq_topic = 'orders-dlq',
    dlq_storage_type = 'kafka',  -- or 'file', 's3', 'postgresql'
    
    -- DLQ Size Limits
    dlq_max_size = 1000000,
    dlq_max_message_bytes = 10485760,
    dlq_retention_days = 30,
    
    -- Retry Configuration
    dlq_max_retries = 5,
    dlq_retry_initial_delay_ms = 1000,
    dlq_retry_backoff_multiplier = 2.0,
    dlq_retry_max_delay_ms = 3600000,
    dlq_retry_jitter = 0.1,
    
    -- Processing Configuration
    dlq_batch_size = 100,
    dlq_reprocess_interval_minutes = 5,
    dlq_processing_parallelism = 4,
    
    -- Overflow Handling
    dlq_overflow_policy = 'reject',  -- 'reject', 'drop_oldest', 'circular'
    dlq_alert_threshold = 0.8,
    
    -- Error Categorization
    dlq_categorize_errors = true,
    dlq_transient_error_codes = '408,429,503',  -- HTTP codes for transient
    dlq_permanent_error_codes = '400,404'        -- HTTP codes for permanent
);
```

### DLQ Message Structure

```json
{
  "id": "dlq_msg_123456",
  "original_message": {
    "key": "order-789",
    "value": {"order_id": 789, "amount": 99.99},
    "headers": {"source": "web", "version": "1.0"},
    "timestamp": 1705325400000,
    "partition": 2,
    "offset": 12345
  },
  "error_details": {
    "error_type": "ParseError",
    "error_message": "Invalid JSON at position 42",
    "error_category": "parsing",
    "stack_trace": "...",
    "processor": "enrichment-pipeline",
    "stage": "json-deserialize"
  },
  "retry_info": {
    "attempt_count": 3,
    "first_failure": "2024-01-15T10:30:00Z",
    "last_attempt": "2024-01-15T11:45:00Z",
    "next_retry": "2024-01-15T13:45:00Z",
    "retry_history": [
      {"attempt": 1, "timestamp": "2024-01-15T10:31:00Z", "error": "timeout"},
      {"attempt": 2, "timestamp": "2024-01-15T10:35:00Z", "error": "timeout"},
      {"attempt": 3, "timestamp": "2024-01-15T11:45:00Z", "error": "parse_error"}
    ]
  },
  "metadata": {
    "dlq_timestamp": "2024-01-15T11:45:30Z",
    "source_stream": "orders_stream",
    "correlation_id": "req-abc-123",
    "environment": "production",
    "version": "1.2.3"
  }
}
```

## Throughput Metrics with Statistics

### Enhanced Throughput Metrics Configuration

```rust
use velostream::velo::sql::execution::performance::{
    ThroughputMetrics, MetricsWindow, StatisticalMetrics
};

// Configure comprehensive throughput metrics
let metrics = ThroughputMetrics::builder()
    // Basic configuration
    .window_size(Duration::from_hours(24))
    .sample_interval(Duration::from_seconds(1))
    
    // Statistical metrics to track
    .track_statistics(vec![
        StatisticalMetrics::Average,
        StatisticalMetrics::Minimum,
        StatisticalMetrics::Maximum,
        StatisticalMetrics::Median,
        StatisticalMetrics::Percentile(95),
        StatisticalMetrics::Percentile(99),
        StatisticalMetrics::StandardDeviation,
        StatisticalMetrics::Variance,
    ])
    
    // Reset configuration
    .auto_reset(true)
    .reset_interval(Duration::from_hours(24))
    .reset_time("00:00:00")  // Reset at midnight
    .timezone("UTC")
    
    // Persistence
    .persist_to_disk(true)
    .persistence_path("/var/velo/metrics")
    .persistence_interval(Duration::from_minutes(5))
    
    .build()?;
```

### Metrics Collection Implementation

```rust
pub struct EnhancedThroughputMetrics {
    // Current window metrics
    current: Arc<RwLock<WindowMetrics>>,
    
    // Historical windows for comparison
    history: Arc<RwLock<VecDeque<WindowMetrics>>>,
    
    // Configuration
    config: MetricsConfig,
}

#[derive(Debug, Clone)]
pub struct WindowMetrics {
    // Time window
    start_time: Instant,
    end_time: Option<Instant>,
    
    // Message counts
    total_messages: u64,
    total_bytes: u64,
    
    // Throughput samples (messages/sec)
    samples: Vec<f64>,
    
    // Computed statistics
    stats: MessageStatistics,
}

#[derive(Debug, Clone)]
pub struct MessageStatistics {
    // Messages per second statistics
    avg_msg_per_sec: f64,
    min_msg_per_sec: f64,
    max_msg_per_sec: f64,
    median_msg_per_sec: f64,
    p95_msg_per_sec: f64,
    p99_msg_per_sec: f64,
    stddev_msg_per_sec: f64,
    
    // Bytes per second statistics
    avg_bytes_per_sec: f64,
    min_bytes_per_sec: f64,
    max_bytes_per_sec: f64,
    
    // Message size statistics
    avg_message_size: f64,
    min_message_size: u64,
    max_message_size: u64,
    
    // Time-based metrics
    peak_time: DateTime<Utc>,
    valley_time: DateTime<Utc>,
}

impl EnhancedThroughputMetrics {
    pub fn record_message(&self, size_bytes: u64) {
        let mut current = self.current.write().unwrap();
        
        // Update counts
        current.total_messages += 1;
        current.total_bytes += size_bytes;
        
        // Calculate current throughput
        let elapsed = current.start_time.elapsed().as_secs_f64();
        let current_throughput = current.total_messages as f64 / elapsed;
        
        // Add sample
        current.samples.push(current_throughput);
        
        // Update statistics incrementally
        self.update_statistics(&mut current);
        
        // Check for reset
        if self.should_reset() {
            self.reset_window();
        }
    }
    
    pub fn get_current_stats(&self) -> MessageStatistics {
        self.current.read().unwrap().stats.clone()
    }
    
    fn update_statistics(&self, window: &mut WindowMetrics) {
        let samples = &window.samples;
        if samples.is_empty() {
            return;
        }
        
        // Sort samples for percentile calculations
        let mut sorted_samples = samples.clone();
        sorted_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        // Calculate statistics
        window.stats = MessageStatistics {
            avg_msg_per_sec: samples.iter().sum::<f64>() / samples.len() as f64,
            min_msg_per_sec: *sorted_samples.first().unwrap(),
            max_msg_per_sec: *sorted_samples.last().unwrap(),
            median_msg_per_sec: sorted_samples[sorted_samples.len() / 2],
            p95_msg_per_sec: sorted_samples[(sorted_samples.len() as f64 * 0.95) as usize],
            p99_msg_per_sec: sorted_samples[(sorted_samples.len() as f64 * 0.99) as usize],
            stddev_msg_per_sec: self.calculate_stddev(samples),
            
            avg_bytes_per_sec: window.total_bytes as f64 / window.start_time.elapsed().as_secs_f64(),
            min_bytes_per_sec: 0.0, // Track separately
            max_bytes_per_sec: 0.0, // Track separately
            
            avg_message_size: window.total_bytes as f64 / window.total_messages as f64,
            min_message_size: 0, // Track separately
            max_message_size: 0, // Track separately
            
            peak_time: Utc::now(), // Track when max occurred
            valley_time: Utc::now(), // Track when min occurred
        };
    }
    
    fn should_reset(&self) -> bool {
        // Reset every 24 hours at configured time
        let now = Utc::now();
        let reset_time = self.config.reset_time;
        
        // Check if we've passed the reset time and haven't reset today
        now.hour() == reset_time.hour() 
            && now.minute() == reset_time.minute() 
            && self.current.read().unwrap().start_time.elapsed() > Duration::from_hours(23)
    }
    
    fn reset_window(&self) {
        let mut current = self.current.write().unwrap();
        let mut history = self.history.write().unwrap();
        
        // Archive current window
        let archived = current.clone();
        history.push_back(archived);
        
        // Keep only last N windows
        while history.len() > self.config.max_history_windows {
            history.pop_front();
        }
        
        // Reset current window
        *current = WindowMetrics {
            start_time: Instant::now(),
            end_time: None,
            total_messages: 0,
            total_bytes: 0,
            samples: Vec::new(),
            stats: MessageStatistics::default(),
        };
        
        log::info!("Metrics window reset at {}", Utc::now());
    }
}
```

## 24-Hour Rolling Metrics

### Rolling Window Implementation

```rust
pub struct RollingMetrics {
    // Circular buffer for 24-hour window
    hourly_buckets: [HourlyMetrics; 24],
    current_hour_index: usize,
    
    // Fine-grained metrics (per minute for last hour)
    minute_buckets: [MinuteMetrics; 60],
    current_minute_index: usize,
    
    // Aggregated statistics
    daily_stats: Arc<RwLock<DailyStatistics>>,
}

#[derive(Default, Clone)]
struct HourlyMetrics {
    timestamp: DateTime<Utc>,
    message_count: u64,
    byte_count: u64,
    error_count: u64,
    avg_latency_ms: f64,
    max_throughput_msg_sec: f64,
    min_throughput_msg_sec: f64,
}

#[derive(Default, Clone)]
struct MinuteMetrics {
    timestamp: DateTime<Utc>,
    message_count: u64,
    throughput_msg_sec: f64,
}

impl RollingMetrics {
    pub fn new() -> Self {
        let mut metrics = Self {
            hourly_buckets: Default::default(),
            current_hour_index: 0,
            minute_buckets: Default::default(),
            current_minute_index: 0,
            daily_stats: Arc::new(RwLock::new(DailyStatistics::default())),
        };
        
        // Start background task for bucket rotation
        metrics.start_rotation_task();
        metrics
    }
    
    fn start_rotation_task(&self) {
        let buckets = self.hourly_buckets.clone();
        let stats = self.daily_stats.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                
                let now = Utc::now();
                
                // Rotate minute buckets every minute
                if now.second() == 0 {
                    Self::rotate_minute_buckets(&mut minute_buckets);
                }
                
                // Rotate hour buckets every hour
                if now.minute() == 0 && now.second() == 0 {
                    Self::rotate_hour_buckets(&mut hourly_buckets);
                    Self::recalculate_daily_stats(&hourly_buckets, &stats);
                }
            }
        });
    }
    
    pub fn get_24h_statistics(&self) -> DailyStatistics {
        let stats = self.daily_stats.read().unwrap();
        
        DailyStatistics {
            total_messages: self.sum_hourly_messages(),
            avg_msg_per_sec: self.calculate_24h_avg_throughput(),
            min_msg_per_sec: self.find_24h_min_throughput(),
            max_msg_per_sec: self.find_24h_max_throughput(),
            peak_hour: self.find_peak_hour(),
            valley_hour: self.find_valley_hour(),
            current_rate: self.get_current_minute_rate(),
            trend: self.calculate_trend(), // increasing/decreasing/stable
        }
    }
}
```

### Metrics API Endpoints

```rust
// REST API for metrics
#[get("/metrics/throughput/current")]
async fn get_current_throughput(metrics: &State<MetricsCollector>) -> Json<ThroughputStats> {
    Json(ThroughputStats {
        current_msg_per_sec: metrics.get_current_rate(),
        avg_msg_per_sec_1h: metrics.get_hourly_average(),
        avg_msg_per_sec_24h: metrics.get_daily_average(),
    })
}

#[get("/metrics/throughput/24h")]
async fn get_24h_throughput(metrics: &State<MetricsCollector>) -> Json<DailyMetrics> {
    let stats = metrics.get_24h_statistics();
    
    Json(DailyMetrics {
        period: "24h",
        start_time: Utc::now() - Duration::from_hours(24),
        end_time: Utc::now(),
        total_messages: stats.total_messages,
        total_bytes: stats.total_bytes,
        avg_msg_per_sec: stats.avg_msg_per_sec,
        min_msg_per_sec: stats.min_msg_per_sec,
        max_msg_per_sec: stats.max_msg_per_sec,
        median_msg_per_sec: stats.median_msg_per_sec,
        p95_msg_per_sec: stats.p95_msg_per_sec,
        p99_msg_per_sec: stats.p99_msg_per_sec,
        peak_time: stats.peak_time,
        valley_time: stats.valley_time,
        hourly_breakdown: metrics.get_hourly_breakdown(),
    })
}

#[post("/metrics/reset")]
async fn reset_metrics(metrics: &State<MetricsCollector>) -> StatusCode {
    metrics.reset_all();
    StatusCode::OK
}
```

## Configuration Examples

### Environment Variables

```bash
# DLQ Configuration
export VELO_DLQ_ENABLED=true
export VELO_DLQ_MAX_SIZE=1000000
export VELO_DLQ_MAX_RETRIES=5
export VELO_DLQ_RETRY_DELAY_MS=1000
export VELO_DLQ_STORAGE_TYPE=kafka
export VELO_DLQ_STORAGE_LOCATION="kafka://localhost:9092/dlq"

# Metrics Configuration  
export VELO_METRICS_WINDOW_HOURS=24
export VELO_METRICS_RESET_TIME="00:00:00"
export VELO_METRICS_TIMEZONE="UTC"
export VELO_METRICS_PERSIST_PATH="/var/velo/metrics"
export VELO_METRICS_SAMPLE_INTERVAL_MS=1000
```

### Configuration File

```yaml
# config.yaml
velostream:
  dlq:
    enabled: true
    storage:
      type: kafka
      location: "kafka://localhost:9092/dlq"
    limits:
      max_size: 1000000
      max_message_bytes: 10485760
      retention_days: 30
    retry:
      max_attempts: 5
      initial_delay_ms: 1000
      backoff_multiplier: 2.0
      max_delay_ms: 3600000
    processing:
      batch_size: 100
      parallelism: 4
      reprocess_interval: "5m"
      
  metrics:
    throughput:
      window_size: "24h"
      sample_interval: "1s"
      reset_time: "00:00:00"
      timezone: "UTC"
      statistics:
        - average
        - minimum
        - maximum
        - median
        - p95
        - p99
        - stddev
    persistence:
      enabled: true
      path: "/var/velo/metrics"
      interval: "5m"
    retention:
      detailed: "7d"
      aggregated: "30d"
```

## Monitoring and Alerting

### Grafana Dashboard Queries

```sql
-- Current throughput
SELECT 
    avg_msg_per_sec,
    min_msg_per_sec,
    max_msg_per_sec
FROM metrics_throughput
WHERE time > now() - interval '5 minutes'

-- 24-hour throughput trend
SELECT 
    time_bucket('1 hour', timestamp) as hour,
    avg(msg_per_sec) as avg_throughput,
    min(msg_per_sec) as min_throughput,
    max(msg_per_sec) as max_throughput
FROM metrics_throughput
WHERE timestamp > now() - interval '24 hours'
GROUP BY hour
ORDER BY hour;

-- DLQ size over time
SELECT 
    timestamp,
    queue_size,
    retry_pending,
    permanent_failures
FROM metrics_dlq
WHERE timestamp > now() - interval '24 hours';
```

### Prometheus Alert Rules

```yaml
groups:
  - name: dlq_alerts
    rules:
      - alert: DLQSizeHigh
        expr: velo_dlq_size > 800000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DLQ size exceeding threshold ({{ $value }})"
          
      - alert: DLQFull
        expr: velo_dlq_size >= velo_dlq_max_size * 0.95
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "DLQ is almost full ({{ $value }})"
          
      - alert: HighRetryRate
        expr: rate(velo_dlq_retries_total[5m]) > 100
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High DLQ retry rate"
          
  - name: throughput_alerts
    rules:
      - alert: LowThroughput
        expr: velo_throughput_msg_per_sec < 100
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Throughput below threshold"
          
      - alert: ThroughputDrop
        expr: |
          (velo_throughput_avg_24h - velo_throughput_avg_1h) 
          / velo_throughput_avg_24h > 0.5
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Throughput dropped by 50%"
```

### CLI Commands for Monitoring

```bash
# View current DLQ status
velo-cli dlq status

# Output:
# DLQ Status Report
# ================
# Total Messages: 12,345
# Size: 12.3 MB / 1 GB (1.23%)
# Oldest Message: 2024-01-15T08:00:00Z
# Retry Pending: 234
# Permanent Failures: 56
# Processing Rate: 10 msg/sec

# View DLQ messages
velo-cli dlq list --limit 10 --category parsing

# Reprocess DLQ messages
velo-cli dlq reprocess --category transient --batch-size 100

# Clear old messages
velo-cli dlq purge --older-than 7d --category permanent

# View throughput metrics
velo-cli metrics throughput --period 24h

# Output:
# 24-Hour Throughput Statistics
# =============================
# Period: 2024-01-14 00:00:00 - 2024-01-15 00:00:00
# Total Messages: 86,400,000
# Average: 1,000 msg/sec
# Minimum: 500 msg/sec (at 2024-01-14 03:15:00)
# Maximum: 2,500 msg/sec (at 2024-01-14 14:30:00)
# P95: 1,800 msg/sec
# P99: 2,200 msg/sec
# Current: 1,150 msg/sec

# Reset metrics
velo-cli metrics reset --confirm

# Export metrics
velo-cli metrics export --format json --output metrics.json
```

## Best Practices

1. **DLQ Size**: Set to 10-20% of expected daily volume
2. **Retry Strategy**: Use exponential backoff with jitter
3. **Metrics Window**: 24-hour rolling window with hourly granularity
4. **Alert Thresholds**: DLQ at 80%, critical at 95%
5. **Retention**: Keep DLQ messages for 7-30 days
6. **Monitoring**: Check DLQ size, retry rate, and age distribution
7. **Categorization**: Separate transient vs permanent failures
8. **Batch Processing**: Process DLQ in batches for efficiency
9. **Circuit Breaking**: Prevent cascading failures
10. **Metrics Reset**: Daily at low-traffic time (e.g., midnight UTC)

## References

- [Health Monitoring Guide](./HEALTH_MONITORING_GUIDE.md)
- [SQL Integration Guide](./SQL_INTEGRATION_GUIDE.md)
- [Performance Tuning Guide](./PERFORMANCE_GUIDE.md)
- [Error Recovery Patterns](./ERROR_RECOVERY_PATTERNS.md)