# Health Monitoring & Observability Guide

## Overview

FerrisStreams provides comprehensive health monitoring for all pluggable data sources, including circuit breakers, health checks, metrics collection, and automatic recovery mechanisms.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Health Monitoring System                │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │Health Monitor│  │Circuit Breaker│  │Dead Letter Q │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
├─────────────────────────────────────────────────────────┤
│                    Metrics Collector                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │Throughput│  │  Latency │  │  Errors  │            │
│  └──────────┘  └──────────┘  └──────────┘            │
├─────────────────────────────────────────────────────────┤
│                  Data Source Monitors                    │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌──────────┐   │
│  │ Kafka  │  │  File  │  │   S3   │  │PostgreSQL│   │
│  └────────┘  └────────┘  └────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Health Monitor

The central component that tracks health status across all data sources:

```rust
use ferrisstreams::ferris::sql::error::recovery::{HealthMonitor, HealthStatus};

// Create health monitor
let monitor = HealthMonitor::new()
    .check_interval(Duration::from_secs(30))
    .unhealthy_threshold(3)
    .healthy_threshold(2)
    .build();

// Register data source for monitoring
monitor.register_source("kafka://localhost:9092/orders", kafka_checker);

// Get current health status
let status = monitor.get_status("kafka://localhost:9092/orders").await;
match status {
    HealthStatus::Healthy => println!("✅ Source is healthy"),
    HealthStatus::Degraded(issues) => println!("⚠️ Source degraded: {:?}", issues),
    HealthStatus::Unhealthy(error) => println!("❌ Source unhealthy: {}", error),
}
```

### 2. Circuit Breaker

Prevents cascading failures by temporarily stopping operations to failing sources:

```rust
use ferrisstreams::ferris::sql::error::recovery::{CircuitBreaker, CircuitState};

// Configure circuit breaker
let breaker = CircuitBreaker::builder()
    .failure_threshold(5)           // Open after 5 failures
    .success_threshold(2)            // Close after 2 successes
    .timeout(Duration::from_secs(30)) // Half-open timeout
    .build();

// Use circuit breaker in operations
match breaker.call(async_operation).await {
    Ok(result) => process_result(result),
    Err(CircuitOpen) => {
        // Circuit is open, use fallback
        use_fallback_source()
    }
    Err(other) => handle_error(other),
}

// Monitor circuit state
let state = breaker.current_state();
match state {
    CircuitState::Closed => metrics.record("circuit.closed", 1),
    CircuitState::Open(until) => {
        metrics.record("circuit.open", 1);
        log::warn!("Circuit open until: {:?}", until);
    }
    CircuitState::HalfOpen => metrics.record("circuit.half_open", 1),
}
```

### 3. Dead Letter Queue

Handles failed records that cannot be processed:

```rust
use ferrisstreams::ferris::sql::error::recovery::DeadLetterQueue;

// Configure DLQ
let dlq = DeadLetterQueue::builder()
    .max_retries(3)
    .retry_delay(Duration::from_secs(60))
    .storage("file:///var/ferris/dlq")
    .build();

// Send failed record to DLQ
match process_record(record).await {
    Ok(result) => Ok(result),
    Err(error) => {
        dlq.send(record, error).await?;
        metrics.increment("dlq.records");
        Err(error)
    }
}

// Process DLQ records (retry or manual review)
let failed_records = dlq.retrieve_batch(100).await?;
for record in failed_records {
    match reprocess_record(record).await {
        Ok(_) => dlq.acknowledge(record.id).await?,
        Err(_) => dlq.move_to_permanent_failure(record.id).await?,
    }
}
```

## Health Check Endpoints

### REST API Endpoints

```yaml
# Main health endpoint
GET /health
Response:
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "1.0.0",
  "uptime_seconds": 3600
}

# Detailed health check
GET /health/detailed
Response:
{
  "status": "degraded",
  "components": {
    "kafka": {
      "status": "healthy",
      "latency_ms": 15,
      "last_check": "2024-01-15T10:29:45Z"
    },
    "postgresql": {
      "status": "degraded",
      "error": "High connection pool usage (90%)",
      "latency_ms": 250
    },
    "schema_registry": {
      "status": "healthy",
      "cache_hit_rate": 0.95
    }
  }
}

# Readiness probe (Kubernetes)
GET /health/ready
Response: 200 OK or 503 Service Unavailable

# Liveness probe (Kubernetes)
GET /health/live
Response: 200 OK or 503 Service Unavailable
```

### Data Source Health Checks

```rust
// Kafka health check
impl HealthCheck for KafkaHealthCheck {
    async fn check(&self) -> HealthResult {
        // 1. Check broker connectivity
        let metadata = self.client.fetch_metadata(Duration::from_secs(5)).await?;
        
        // 2. Check consumer group lag
        let lag = self.get_consumer_lag().await?;
        if lag > self.max_lag_threshold {
            return HealthResult::Degraded(format!("High lag: {}", lag));
        }
        
        // 3. Check producer buffer
        if self.producer.queue_size() > self.max_queue_size {
            return HealthResult::Degraded("Producer queue full");
        }
        
        HealthResult::Healthy
    }
}

// PostgreSQL health check
impl HealthCheck for PostgreSQLHealthCheck {
    async fn check(&self) -> HealthResult {
        // 1. Check connection pool
        let pool_status = self.pool.status();
        if pool_status.available == 0 {
            return HealthResult::Unhealthy("No connections available");
        }
        
        // 2. Execute health query
        let start = Instant::now();
        sqlx::query("SELECT 1").fetch_one(&self.pool).await?;
        let latency = start.elapsed();
        
        if latency > Duration::from_secs(1) {
            return HealthResult::Degraded(format!("High latency: {:?}", latency));
        }
        
        HealthResult::Healthy
    }
}

// File system health check
impl HealthCheck for FileSystemHealthCheck {
    async fn check(&self) -> HealthResult {
        // 1. Check disk space
        let stats = fs2::statvfs(&self.path)?;
        let free_percent = (stats.available_space() as f64 / stats.total_space() as f64) * 100.0;
        
        if free_percent < 10.0 {
            return HealthResult::Degraded(format!("Low disk space: {:.1}%", free_percent));
        }
        
        // 2. Check write permissions
        let test_file = self.path.join(".health_check");
        fs::write(&test_file, b"test").await?;
        fs::remove_file(&test_file).await?;
        
        HealthResult::Healthy
    }
}
```

## Metrics Collection

### Available Metrics

```rust
// Throughput metrics
metrics.record("source.records.processed", count);
metrics.record("source.bytes.processed", bytes);
metrics.record("sink.records.written", count);
metrics.record("sink.bytes.written", bytes);

// Latency metrics
metrics.record("source.read.latency", duration);
metrics.record("sink.write.latency", duration);
metrics.record("processing.latency", duration);
metrics.record("e2e.latency", duration);

// Error metrics
metrics.increment("source.errors.total");
metrics.increment("source.errors.timeout");
metrics.increment("source.errors.parse");
metrics.increment("sink.errors.total");

// Circuit breaker metrics
metrics.record("circuit_breaker.open", is_open as u64);
metrics.record("circuit_breaker.failures", failure_count);
metrics.record("circuit_breaker.success", success_count);

// DLQ metrics
metrics.record("dlq.size", queue_size);
metrics.record("dlq.age.max", max_age);
metrics.record("dlq.retries", retry_count);
```

### Prometheus Integration

```yaml
# Prometheus scrape configuration
scrape_configs:
  - job_name: 'ferrisstreams'
    static_configs:
      - targets: ['localhost:9090']
    metrics_path: '/metrics'
```

```rust
// Expose metrics endpoint
use prometheus::{Encoder, TextEncoder};

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

// Example metrics output
# HELP ferris_source_records_total Total records processed by source
# TYPE ferris_source_records_total counter
ferris_source_records_total{source="kafka",topic="orders"} 1234567
ferris_source_records_total{source="file",path="/data/input.csv"} 98765

# HELP ferris_processing_latency_seconds Processing latency histogram
# TYPE ferris_processing_latency_seconds histogram
ferris_processing_latency_seconds_bucket{le="0.01"} 45678
ferris_processing_latency_seconds_bucket{le="0.1"} 56789
ferris_processing_latency_seconds_bucket{le="1"} 57890
```

## Alerting Configuration

### Alert Rules

```yaml
# Prometheus alert rules
groups:
  - name: ferrisstreams
    rules:
      - alert: HighErrorRate
        expr: rate(ferris_source_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate on {{ $labels.source }}"
          description: "Error rate is {{ $value }} errors/sec"
      
      - alert: CircuitBreakerOpen
        expr: ferris_circuit_breaker_open == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Circuit breaker open for {{ $labels.source }}"
      
      - alert: HighConsumerLag
        expr: ferris_kafka_consumer_lag > 100000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High Kafka consumer lag"
          description: "Lag is {{ $value }} messages"
      
      - alert: DLQGrowing
        expr: rate(ferris_dlq_size[5m]) > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Dead letter queue is growing"
```

## Monitoring Dashboard

### Grafana Dashboard JSON

```json
{
  "dashboard": {
    "title": "FerrisStreams Health Monitor",
    "panels": [
      {
        "title": "Data Source Health",
        "type": "stat",
        "targets": [
          {
            "expr": "up{job='ferrisstreams'}"
          }
        ]
      },
      {
        "title": "Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(ferris_source_records_total[5m])"
          }
        ]
      },
      {
        "title": "Error Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(ferris_source_errors_total[5m])"
          }
        ]
      },
      {
        "title": "Circuit Breaker Status",
        "type": "table",
        "targets": [
          {
            "expr": "ferris_circuit_breaker_open"
          }
        ]
      },
      {
        "title": "Processing Latency",
        "type": "heatmap",
        "targets": [
          {
            "expr": "ferris_processing_latency_seconds"
          }
        ]
      }
    ]
  }
}
```

## Auto-Recovery Strategies

### 1. Retry with Exponential Backoff

```rust
use ferrisstreams::ferris::sql::error::recovery::RetryPolicy;

let retry_policy = RetryPolicy::exponential()
    .initial_delay(Duration::from_millis(100))
    .max_delay(Duration::from_secs(30))
    .max_retries(5)
    .jitter(0.1);

retry_policy.execute(|| async {
    source.read_batch(100).await
}).await?;
```

### 2. Fallback Sources

```rust
// Define fallback chain
let fallback_chain = FallbackChain::new()
    .primary("kafka://primary:9092/orders")
    .fallback("kafka://secondary:9092/orders")
    .fallback("file:///backup/orders.jsonl");

// Use with automatic fallback
let records = fallback_chain.read_with_fallback().await?;
```

### 3. Self-Healing Connections

```rust
// Automatic reconnection with health monitoring
let connection = SelfHealingConnection::new(source)
    .health_check_interval(Duration::from_secs(30))
    .reconnect_delay(Duration::from_secs(5))
    .max_reconnect_attempts(10);

// Connection automatically recovers from failures
let records = connection.read().await?; // Handles reconnection transparently
```

## Kubernetes Integration

### Health Check Configuration

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: ferrisstreams
    image: ferrisstreams:latest
    livenessProbe:
      httpGet:
        path: /health/live
        port: 8080
      initialDelaySeconds: 30
      periodSeconds: 10
      timeoutSeconds: 5
      failureThreshold: 3
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8080
      initialDelaySeconds: 10
      periodSeconds: 5
      timeoutSeconds: 3
      failureThreshold: 2
```

### Horizontal Pod Autoscaling

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ferrisstreams-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ferrisstreams
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Pods
    pods:
      metric:
        name: kafka_consumer_lag
      target:
        type: AverageValue
        averageValue: "10000"
```

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. Circuit Breaker Keeps Opening
```bash
# Check failure rate
curl http://localhost:8080/metrics | grep circuit_breaker_failures

# Review recent errors
kubectl logs -l app=ferrisstreams --since=10m | grep ERROR

# Increase failure threshold if needed
FERRIS_CIRCUIT_BREAKER_THRESHOLD=10
```

#### 2. High Consumer Lag
```bash
# Check current lag
curl http://localhost:8080/health/detailed | jq '.components.kafka.lag'

# Scale up consumers
kubectl scale deployment ferrisstreams --replicas=5

# Check processing rate
curl http://localhost:8080/metrics | grep source_records_total
```

#### 3. DLQ Growing
```bash
# Check DLQ size
curl http://localhost:8080/metrics | grep dlq_size

# Review failed records
ferris-cli dlq list --limit 10

# Reprocess DLQ
ferris-cli dlq reprocess --batch-size 100
```

## Best Practices

1. **Configure appropriate health check intervals** - Not too frequent to avoid overhead
2. **Set realistic circuit breaker thresholds** - Based on expected failure rates
3. **Monitor DLQ size and age** - Process or purge old records
4. **Use graduated health states** - Healthy → Degraded → Unhealthy
5. **Implement proper retry strategies** - With backoff and jitter
6. **Set up alerting for critical metrics** - Before issues become critical
7. **Regular health check endpoint testing** - Ensure they work when needed
8. **Document recovery procedures** - For each failure scenario

## References

- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Health Check API Pattern](https://microservices.io/patterns/observability/health-check-api.html)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [Kubernetes Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)