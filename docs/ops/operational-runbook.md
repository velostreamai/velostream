# Velostream Operational Runbook

## Overview

This runbook provides operational procedures for managing Velostream in production, covering monitoring, troubleshooting, maintenance, and emergency response procedures for both legacy and enhanced streaming modes.

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Monitoring and Alerting](#monitoring-and-alerting)
3. [Health Checks](#health-checks)
4. [Troubleshooting Guide](#troubleshooting-guide)
5. [Performance Optimization](#performance-optimization)
6. [Emergency Procedures](#emergency-procedures)
7. [Maintenance Procedures](#maintenance-procedures)
8. [Configuration Management](#configuration-management)
9. [Scaling Procedures](#scaling-procedures)
10. [Disaster Recovery](#disaster-recovery)

## System Architecture

### Production Deployment Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│ Velostream   │───▶│   Data Sinks    │
│                 │    │  Engine         │    │                 │
│ • Kafka Topics  │    │                 │    │ • Kafka Topics  │
│ • REST APIs     │    │ ┌─────────────┐ │    │ • Databases     │
│ • File Streams  │    │ │Circuit      │ │    │ • APIs          │
└─────────────────┘    │ │Breakers     │ │    └─────────────────┘
                       │ └─────────────┘ │
┌─────────────────┐    │ ┌─────────────┐ │    ┌─────────────────┐
│   Monitoring    │◀───│ │Resource     │ │───▶│   Alerting      │
│                 │    │ │Manager      │ │    │                 │
│ • Metrics       │    │ └─────────────┘ │    │ • PagerDuty     │
│ • Logs          │    │ ┌─────────────┐ │    │ • Slack         │
│ • Traces        │    │ │Watermark    │ │    │ • Email         │
└─────────────────┘    │ │Manager      │ │    └─────────────────┘
                       │ └─────────────┘ │
                       └─────────────────┘
```

### Key Components

1. **Streaming Engine Core**: Main SQL processing engine
2. **Circuit Breakers**: Fault tolerance and failure isolation
3. **Resource Manager**: Memory and processing limits enforcement
4. **Watermark Manager**: Time-based event ordering and late data handling
5. **Monitoring Stack**: Metrics collection, logging, and alerting

## Monitoring and Alerting

### Critical Metrics to Monitor

#### System Health Metrics
```bash
# Memory Usage
velo_streams_memory_usage_bytes{component="total"}
velo_streams_memory_usage_bytes{component="operator"}
velo_streams_memory_peak_bytes{component="total"}

# Processing Performance
velo_streams_processing_latency_ms{operation="record_processing"}
velo_streams_throughput_records_per_second
velo_streams_processing_errors_total{error_type="retryable"}
velo_streams_processing_errors_total{error_type="fatal"}

# Circuit Breaker Status
velo_streams_circuit_breaker_state{name="main"} # 0=Closed, 1=Open, 2=HalfOpen
velo_streams_circuit_breaker_failures_total{name="main"}
velo_streams_circuit_breaker_successes_total{name="main"}

# Resource Utilization
velo_streams_resource_utilization_percent{resource="memory"}
velo_streams_resource_utilization_percent{resource="cpu"}
velo_streams_resource_limit_violations_total{resource="memory"}

# Watermark Metrics
velo_streams_watermark_lag_seconds
velo_streams_late_data_dropped_total
velo_streams_late_data_processed_total
```

#### Business Metrics
```bash
# Records Processing
velo_streams_records_processed_total{source="kafka"}
velo_streams_records_failed_total{source="kafka"}
velo_streams_backlog_size{topic="input_topic"}

# Query Performance
velo_streams_query_execution_time_ms{query_type="aggregation"}
velo_streams_query_execution_time_ms{query_type="windowing"}
velo_streams_query_results_produced_total
```

### Alert Rules

#### Critical Alerts (Page Immediately)

```yaml
# Circuit Breaker Open
- alert: CircuitBreakerOpen
  expr: velo_streams_circuit_breaker_state > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Velostream circuit breaker is open"
    description: "Circuit breaker {{ $labels.name }} has been open for {{ $value }} seconds"
    runbook: "https://docs.company.com/velostream/runbook#circuit-breaker-open"

# Memory Exhaustion
- alert: MemoryExhausted
  expr: velo_streams_resource_utilization_percent{resource="memory"} > 95
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Velostream memory usage critical"
    description: "Memory utilization is {{ $value }}% (>95%)"
    runbook: "https://docs.company.com/velostream/runbook#memory-exhausted"

# Processing Stopped
- alert: ProcessingStopped
  expr: rate(velo_streams_records_processed_total[5m]) == 0
  for: 3m
  labels:
    severity: critical
  annotations:
    summary: "Velostream processing has stopped"
    description: "No records processed in the last 5 minutes"
    runbook: "https://docs.company.com/velostream/runbook#processing-stopped"
```

#### Warning Alerts (Investigate Soon)

```yaml
# High Error Rate
- alert: HighErrorRate
  expr: rate(velo_streams_processing_errors_total[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High error rate in Velostream"
    description: "Error rate is {{ $value }} errors/second (>10/sec)"

# High Latency
- alert: HighProcessingLatency
  expr: velo_streams_processing_latency_ms > 1000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High processing latency"
    description: "Processing latency is {{ $value }}ms (>1000ms)"

# Resource Warning
- alert: ResourceWarning
  expr: velo_streams_resource_utilization_percent > 80
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Resource utilization high"
    description: "{{ $labels.resource }} utilization is {{ $value }}% (>80%)"
```

## Health Checks

### Application Health Check Endpoint

```rust
// Health check implementation
use warp::Filter;
use serde_json::json;

pub fn health_routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("health")
        .and(warp::get())
        .and_then(health_check)
        .or(warp::path("ready")
            .and(warp::get())
            .and_then(readiness_check))
}

async fn health_check() -> Result<impl warp::Reply, warp::Rejection> {
    // Basic health check - is the service running?
    Ok(warp::reply::json(&json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

async fn readiness_check() -> Result<impl warp::Reply, warp::Rejection> {
    // Comprehensive readiness check
    let mut checks = Vec::new();
    
    // Check circuit breaker status
    let circuit_breaker_ok = check_circuit_breakers().await;
    checks.push(("circuit_breakers", circuit_breaker_ok));
    
    // Check resource availability
    let resources_ok = check_resource_availability().await;
    checks.push(("resources", resources_ok));
    
    // Check data source connectivity
    let kafka_ok = check_kafka_connectivity().await;
    checks.push(("kafka", kafka_ok));
    
    let all_healthy = checks.iter().all(|(_, healthy)| *healthy);
    let status = if all_healthy { "ready" } else { "not_ready" };
    
    Ok(warp::reply::json(&json!({
        "status": status,
        "checks": checks.into_iter().collect::<std::collections::HashMap<_, _>>(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}
```

### Manual Health Verification

```bash
#!/bin/bash
# health_check.sh - Manual health verification script

echo "=== Velostream Health Check ==="

# 1. Check service is running
echo "1. Checking service status..."
curl -s http://localhost:8080/health || echo "❌ Service not responding"

# 2. Check readiness
echo "2. Checking readiness..."
curl -s http://localhost:8080/ready | jq .

# 3. Check metrics endpoint
echo "3. Checking metrics..."
curl -s http://localhost:8080/metrics | grep -E "velo_streams_(memory|processing|circuit)"

# 4. Check recent logs
echo "4. Checking recent logs..."
journalctl -u velostream --since "5 minutes ago" --no-pager | tail -10

# 5. Check resource usage
echo "5. Checking resource usage..."
ps -p $(pgrep velostream) -o pid,ppid,cmd,%mem,%cpu --no-headers

echo "=== Health Check Complete ==="
```

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue: Circuit Breaker Open

**Symptoms:**
- Alert: `CircuitBreakerOpen`
- Processing appears stopped
- Error logs show "Circuit breaker open"

**Investigation Steps:**
1. Check circuit breaker metrics:
   ```bash
   curl http://localhost:8080/metrics | grep circuit_breaker
   ```

2. Review recent error logs:
   ```bash
   journalctl -u velostream --since "15 minutes ago" | grep ERROR
   ```

3. Check upstream dependencies:
   ```bash
   # Check Kafka connectivity
   kafka-console-consumer --bootstrap-server kafka:9092 --topic input_topic --max-messages 1
   
   # Check database connectivity  
   pg_isready -h database -p 5432
   ```

**Resolution Steps:**
1. If upstream issues resolved, circuit breaker will auto-recover
2. Force circuit breaker reset (emergency only):
   ```bash
   curl -X POST http://localhost:8080/admin/circuit-breaker/reset
   ```

3. Monitor recovery:
   ```bash
   watch -n 5 'curl -s http://localhost:8080/metrics | grep circuit_breaker_state'
   ```

#### Issue: Memory Exhausted

**Symptoms:**
- Alert: `MemoryExhausted`
- Out of memory errors in logs
- Processing slowdown or failures

**Investigation Steps:**
1. Check memory metrics:
   ```bash
   curl http://localhost:8080/metrics | grep memory_usage
   ```

2. Check resource manager status:
   ```bash
   curl http://localhost:8080/admin/resources
   ```

3. Analyze memory usage by component:
   ```bash
   # Check JVM heap (if applicable)
   jstat -gc $(pgrep velostream)
   
   # Check system memory
   free -h
   ```

**Resolution Steps:**
1. Trigger resource cleanup:
   ```bash
   curl -X POST http://localhost:8080/admin/resources/cleanup
   ```

2. Reduce resource limits temporarily:
   ```bash
   # Update configuration
   curl -X PUT http://localhost:8080/admin/config \
        -H "Content-Type: application/json" \
        -d '{"resource_limits": {"max_total_memory": 1073741824}}'
   ```

3. Restart service if memory leak suspected:
   ```bash
   systemctl restart velostream
   ```

#### Issue: Processing Stopped

**Symptoms:**
- Alert: `ProcessingStopped`
- No records being processed
- Input backlog growing

**Investigation Steps:**
1. Check input source status:
   ```bash
   # Check Kafka consumer lag
   kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group velostream
   ```

2. Check processing threads:
   ```bash
   # Check thread status
   curl http://localhost:8080/admin/threads
   
   # Check for deadlocks
   jstack $(pgrep velostream) | grep -A 5 "Found Java-level deadlock"
   ```

3. Check configuration:
   ```bash
   curl http://localhost:8080/admin/config
   ```

**Resolution Steps:**
1. Restart processing:
   ```bash
   curl -X POST http://localhost:8080/admin/processing/restart
   ```

2. Check and fix configuration:
   ```bash
   # Validate configuration
   velostream --config-check /etc/velostream/config.yaml
   ```

3. Full service restart:
   ```bash
   systemctl restart velostream
   ```

#### Issue: High Latency

**Symptoms:**
- Alert: `HighProcessingLatency`
- Slow query responses
- Growing input backlog

**Investigation Steps:**
1. Check processing metrics:
   ```bash
   curl http://localhost:8080/metrics | grep latency
   ```

2. Identify bottlenecks:
   ```bash
   # Check query performance
   curl http://localhost:8080/admin/queries/performance
   
   # Check resource contention
   curl http://localhost:8080/admin/resources/utilization
   ```

3. Profile application (if profiling enabled):
   ```bash
   curl http://localhost:8080/admin/profile?duration=30s > profile.svg
   ```

**Resolution Steps:**
1. Optimize queries:
   ```sql
   -- Review slow queries
   SELECT * FROM velo_query_stats WHERE avg_latency_ms > 1000;
   ```

2. Scale up resources:
   ```bash
   # Increase memory limits
   curl -X PUT http://localhost:8080/admin/config \
        -d '{"resource_limits": {"max_total_memory": 2147483648}}'
   
   # Increase parallelism
   curl -X PUT http://localhost:8080/admin/config \
        -d '{"processing": {"parallelism": 8}}'
   ```

3. Enable performance optimizations:
   ```bash
   curl -X PUT http://localhost:8080/admin/config \
        -d '{"performance": {"batch_size": 1000, "enable_caching": true}}'
   ```

### Log Analysis

#### Important Log Patterns

```bash
# Circuit breaker state changes
grep "Circuit breaker" /var/log/velostream/application.log

# Resource limit violations
grep "Resource exhausted" /var/log/velostream/application.log

# Watermark violations (late data)
grep "WatermarkViolation" /var/log/velostream/application.log

# Processing errors
grep "ProcessingError" /var/log/velostream/application.log

# Performance warnings
grep "processing_time.*exceeded" /var/log/velostream/application.log
```

#### Structured Log Queries

```bash
# Using jq for JSON logs
jq '.level == "ERROR" and .timestamp > "'$(date -d '1 hour ago' -Iseconds)'"' \
   /var/log/velostream/application.json

# Count error types
jq -r '.error_type // "unknown"' /var/log/velostream/application.json | sort | uniq -c

# Find slow operations
jq 'select(.processing_time_ms > 1000)' /var/log/velostream/application.json
```

## Performance Optimization

### Tuning Parameters

#### Memory Configuration
```yaml
# config/velostream.yaml
resource_limits:
  max_total_memory: 4294967296  # 4GB - adjust based on available system memory
  max_operator_memory: 1073741824  # 1GB per operator
  max_windows_per_key: 10000    # Limit window proliferation
  max_aggregation_groups: 50000  # Limit aggregation memory usage

# Memory management settings
memory_management:
  gc_threshold: 0.8  # Trigger cleanup at 80% usage
  cleanup_interval: 300  # Cleanup every 5 minutes
  history_retention: 3600  # Keep 1 hour of history
```

#### Processing Configuration
```yaml
processing:
  batch_size: 1000        # Records per batch
  max_parallelism: 8      # Number of parallel processors
  queue_size: 10000       # Internal queue size
  timeout_ms: 30000       # Processing timeout
  
# Circuit breaker tuning
circuit_breaker:
  failure_threshold: 10    # Failures before opening
  timeout_seconds: 60      # How long breaker stays open
  recovery_timeout: 30     # Time for recovery test
  success_threshold: 3     # Successes needed to close
```

#### Kafka Configuration
```yaml
kafka:
  consumer:
    fetch.min.bytes: 1048576      # 1MB minimum fetch
    fetch.max.wait.ms: 100        # Max wait for data
    max.poll.records: 1000        # Records per poll
    session.timeout.ms: 30000     # Session timeout
    auto.offset.reset: "latest"   # Start from latest
    
  producer:
    batch.size: 65536             # 64KB batch size
    linger.ms: 5                  # Wait 5ms for batching
    compression.type: "snappy"    # Enable compression
    acks: "all"                   # Wait for all replicas
```

### Performance Monitoring

```bash
#!/bin/bash
# performance_monitor.sh - Continuous performance monitoring

echo "Starting Velostream performance monitoring..."

while true; do
    echo "=== $(date) ==="
    
    # Processing metrics
    echo "Processing Rate:"
    curl -s http://localhost:8080/metrics | grep throughput_records_per_second
    
    echo "Latency:"
    curl -s http://localhost:8080/metrics | grep processing_latency_ms
    
    echo "Memory Usage:"
    curl -s http://localhost:8080/metrics | grep memory_usage_bytes
    
    echo "Error Rate:"
    curl -s http://localhost:8080/metrics | grep processing_errors_total
    
    echo "---"
    sleep 30
done
```

## Emergency Procedures

### Service Recovery Procedures

#### Emergency Stop
```bash
#!/bin/bash
# emergency_stop.sh - Emergency service shutdown

echo "EMERGENCY STOP - Velostream"
echo "Timestamp: $(date)"

# 1. Stop accepting new requests
echo "1. Disabling new requests..."
curl -X POST http://localhost:8080/admin/maintenance/enable

# 2. Drain processing queue
echo "2. Draining processing queue..."
curl -X POST http://localhost:8080/admin/processing/drain

# 3. Stop service
echo "3. Stopping service..."
systemctl stop velostream

echo "Emergency stop complete"
```

#### Emergency Recovery
```bash
#!/bin/bash
# emergency_recovery.sh - Emergency service recovery

echo "EMERGENCY RECOVERY - Velostream"
echo "Timestamp: $(date)"

# 1. Verify system resources
echo "1. Checking system resources..."
free -h
df -h

# 2. Start service with safe configuration
echo "2. Starting with safe configuration..."
cp /etc/velostream/config.safe.yaml /etc/velostream/config.yaml
systemctl start velostream

# 3. Wait for service to be ready
echo "3. Waiting for service readiness..."
until curl -sf http://localhost:8080/ready; do
    echo "Service not ready, waiting..."
    sleep 10
done

# 4. Enable request processing
echo "4. Enabling request processing..."
curl -X POST http://localhost:8080/admin/maintenance/disable

echo "Emergency recovery complete"
```

#### Circuit Breaker Emergency Reset
```bash
#!/bin/bash
# cb_emergency_reset.sh - Force reset all circuit breakers

echo "EMERGENCY CIRCUIT BREAKER RESET"
echo "WARNING: This will bypass safety mechanisms!"
read -p "Are you sure? (yes/no): " confirm

if [ "$confirm" = "yes" ]; then
    echo "Resetting all circuit breakers..."
    curl -X POST http://localhost:8080/admin/circuit-breaker/reset-all
    echo "Circuit breakers reset"
    
    # Monitor for immediate failures
    echo "Monitoring for 60 seconds..."
    for i in {1..12}; do
        sleep 5
        failures=$(curl -s http://localhost:8080/metrics | grep circuit_breaker_failures_total | awk '{print $2}')
        echo "Failures: $failures"
    done
else
    echo "Reset cancelled"
fi
```

### Disaster Recovery Scenarios

#### Data Loss Recovery
```bash
#!/bin/bash
# data_recovery.sh - Recover from data loss

echo "DATA LOSS RECOVERY PROCEDURE"
echo "Timestamp: $(date)"

# 1. Stop processing
systemctl stop velostream

# 2. Reset Kafka consumer to earliest offset
echo "Resetting Kafka consumer to earliest..."
kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group velostream \
    --reset-offsets --to-earliest \
    --topic input_topic \
    --execute

# 3. Clear local state (if applicable)
echo "Clearing local state..."
rm -rf /var/lib/velostream/state/*

# 4. Restart with reprocessing mode
echo "Starting reprocessing mode..."
export VELO_REPROCESSING_MODE=true
systemctl start velostream

echo "Data recovery initiated - monitor progress"
```

#### Cluster Failover
```bash
#!/bin/bash
# cluster_failover.sh - Failover to backup cluster

echo "CLUSTER FAILOVER PROCEDURE"

# 1. Update DNS to point to backup cluster
echo "1. Updating DNS routing..."
# Implementation depends on your DNS provider

# 2. Stop primary cluster
echo "2. Stopping primary cluster..."
for host in primary-node-{1..3}; do
    ssh $host "systemctl stop velostream"
done

# 3. Start backup cluster
echo "3. Starting backup cluster..."
for host in backup-node-{1..3}; do
    ssh $host "systemctl start velostream"
done

# 4. Verify backup cluster health
echo "4. Verifying backup cluster..."
for host in backup-node-{1..3}; do
    curl -sf http://$host:8080/health || echo "❌ $host not healthy"
done

echo "Failover complete"
```

## Maintenance Procedures

### Regular Maintenance Tasks

#### Weekly Maintenance
```bash
#!/bin/bash
# weekly_maintenance.sh - Weekly maintenance tasks

echo "=== Weekly Velostream Maintenance ==="
echo "Date: $(date)"

# 1. Log rotation
echo "1. Rotating logs..."
logrotate -f /etc/logrotate.d/velostream

# 2. Metrics cleanup
echo "2. Cleaning up old metrics..."
curl -X POST http://localhost:8080/admin/metrics/cleanup?older_than=7d

# 3. Resource usage analysis
echo "3. Analyzing resource usage..."
curl -s http://localhost:8080/admin/resources/analysis > /tmp/resource_analysis_$(date +%Y%m%d).json

# 4. Performance report
echo "4. Generating performance report..."
curl -s http://localhost:8080/admin/performance/weekly-report > /tmp/performance_report_$(date +%Y%m%d).json

# 5. Health check
echo "5. Running comprehensive health check..."
./health_check.sh > /tmp/health_check_$(date +%Y%m%d).log

echo "Weekly maintenance complete"
```

#### Monthly Maintenance
```bash
#!/bin/bash
# monthly_maintenance.sh - Monthly maintenance tasks

echo "=== Monthly Velostream Maintenance ==="

# 1. Configuration backup
echo "1. Backing up configuration..."
tar -czf /backup/velostream_config_$(date +%Y%m).tar.gz /etc/velostream/

# 2. Performance baseline update
echo "2. Updating performance baselines..."
curl -X POST http://localhost:8080/admin/performance/update-baselines

# 3. Resource limit review
echo "3. Reviewing resource limits..."
curl -s http://localhost:8080/admin/resources/recommendations > /tmp/resource_recommendations_$(date +%Y%m).json

# 4. Circuit breaker statistics
echo "4. Circuit breaker statistics..."
curl -s http://localhost:8080/admin/circuit-breaker/monthly-stats > /tmp/cb_stats_$(date +%Y%m).json

# 5. Upgrade check
echo "5. Checking for updates..."
curl -s https://releases.velostream.io/latest.json > /tmp/latest_version.json

echo "Monthly maintenance complete"
```

### Configuration Updates

#### Safe Configuration Deployment
```bash
#!/bin/bash
# deploy_config.sh - Safely deploy configuration changes

CONFIG_FILE=$1
if [ -z "$CONFIG_FILE" ]; then
    echo "Usage: $0 <config_file>"
    exit 1
fi

echo "Deploying configuration: $CONFIG_FILE"

# 1. Validate configuration
echo "1. Validating configuration..."
velostream --config-check "$CONFIG_FILE" || {
    echo "❌ Configuration validation failed"
    exit 1
}

# 2. Backup current configuration
echo "2. Backing up current configuration..."
cp /etc/velostream/config.yaml /etc/velostream/config.backup.$(date +%s)

# 3. Enable maintenance mode
echo "3. Enabling maintenance mode..."
curl -X POST http://localhost:8080/admin/maintenance/enable

# 4. Apply configuration
echo "4. Applying new configuration..."
cp "$CONFIG_FILE" /etc/velostream/config.yaml

# 5. Reload configuration
echo "5. Reloading configuration..."
curl -X POST http://localhost:8080/admin/config/reload

# 6. Verify health
echo "6. Verifying service health..."
sleep 10
curl -sf http://localhost:8080/ready || {
    echo "❌ Service not ready after config change"
    echo "Rolling back..."
    cp /etc/velostream/config.backup.* /etc/velostream/config.yaml
    curl -X POST http://localhost:8080/admin/config/reload
    exit 1
}

# 7. Disable maintenance mode
echo "7. Disabling maintenance mode..."
curl -X POST http://localhost:8080/admin/maintenance/disable

echo "✅ Configuration deployed successfully"
```

## Scaling Procedures

### Horizontal Scaling

#### Scale Out Procedure
```bash
#!/bin/bash
# scale_out.sh - Add new Velostream instances

NEW_INSTANCES=$1
if [ -z "$NEW_INSTANCES" ]; then
    echo "Usage: $0 <number_of_new_instances>"
    exit 1
fi

echo "Scaling out by $NEW_INSTANCES instances"

# 1. Provision new instances
echo "1. Provisioning new instances..."
for i in $(seq 1 $NEW_INSTANCES); do
    # This would integrate with your cloud provider
    echo "Provisioning instance velostream-$(date +%s)-$i"
done

# 2. Deploy application to new instances
echo "2. Deploying application..."
# Deployment logic here

# 3. Update load balancer
echo "3. Updating load balancer configuration..."
# Load balancer configuration update

# 4. Verify new instances
echo "4. Verifying new instances..."
# Health check logic

echo "Scale out complete"
```

#### Scale In Procedure
```bash
#!/bin/bash
# scale_in.sh - Remove Velostream instances

INSTANCES_TO_REMOVE=$1
if [ -z "$INSTANCES_TO_REMOVE" ]; then
    echo "Usage: $0 <instance_list>"
    exit 1
fi

echo "Scaling in instances: $INSTANCES_TO_REMOVE"

for instance in $INSTANCES_TO_REMOVE; do
    echo "Removing instance: $instance"
    
    # 1. Remove from load balancer
    echo "  1. Removing from load balancer..."
    
    # 2. Drain connections
    echo "  2. Draining connections..."
    ssh $instance "curl -X POST http://localhost:8080/admin/maintenance/enable"
    
    # 3. Wait for processing to complete
    echo "  3. Waiting for processing to complete..."
    ssh $instance "curl -X POST http://localhost:8080/admin/processing/drain"
    
    # 4. Stop service
    echo "  4. Stopping service..."
    ssh $instance "systemctl stop velostream"
    
    # 5. Terminate instance
    echo "  5. Terminating instance..."
    # Cloud provider termination logic
done

echo "Scale in complete"
```

### Vertical Scaling

#### Resource Adjustment
```bash
#!/bin/bash
# adjust_resources.sh - Adjust resource limits

ACTION=$1  # increase|decrease
PERCENTAGE=${2:-20}  # Default 20%

case $ACTION in
    increase)
        MULTIPLIER="1.$(printf %02d $PERCENTAGE)"
        ;;
    decrease)
        MULTIPLIER="0.$(printf %02d $((100-PERCENTAGE)))"
        ;;
    *)
        echo "Usage: $0 <increase|decrease> [percentage]"
        exit 1
        ;;
esac

echo "Adjusting resources by ${PERCENTAGE}% ($ACTION)"

# Get current limits
CURRENT_MEMORY=$(curl -s http://localhost:8080/admin/config | jq '.resource_limits.max_total_memory')
CURRENT_OPERATORS=$(curl -s http://localhost:8080/admin/config | jq '.resource_limits.max_operator_memory')

# Calculate new limits
NEW_MEMORY=$(echo "$CURRENT_MEMORY * $MULTIPLIER" | bc | cut -d. -f1)
NEW_OPERATORS=$(echo "$CURRENT_OPERATORS * $MULTIPLIER" | bc | cut -d. -f1)

echo "Current memory limit: $CURRENT_MEMORY bytes"
echo "New memory limit: $NEW_MEMORY bytes"

# Apply new limits
curl -X PUT http://localhost:8080/admin/config \
    -H "Content-Type: application/json" \
    -d "{
        \"resource_limits\": {
            \"max_total_memory\": $NEW_MEMORY,
            \"max_operator_memory\": $NEW_OPERATORS
        }
    }"

echo "Resource limits adjusted"
```

## Disaster Recovery

### Backup Procedures

#### Configuration Backup
```bash
#!/bin/bash
# backup_config.sh - Backup Velostream configuration

BACKUP_DIR="/backup/velostream/$(date +%Y/%m/%d)"
mkdir -p "$BACKUP_DIR"

echo "Backing up Velostream configuration to $BACKUP_DIR"

# 1. Application configuration
echo "1. Backing up application configuration..."
cp -r /etc/velostream/ "$BACKUP_DIR/config/"

# 2. Runtime state (if applicable)
echo "2. Backing up runtime state..."
curl -s http://localhost:8080/admin/state/export > "$BACKUP_DIR/runtime_state.json"

# 3. Metrics history
echo "3. Backing up metrics history..."
curl -s http://localhost:8080/admin/metrics/export?days=30 > "$BACKUP_DIR/metrics_30days.json"

# 4. Create manifest
echo "4. Creating backup manifest..."
cat > "$BACKUP_DIR/manifest.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "version": "$(curl -s http://localhost:8080/admin/version)",
    "files": [
        "config/",
        "runtime_state.json",
        "metrics_30days.json"
    ]
}
EOF

echo "Backup complete: $BACKUP_DIR"
```

#### State Recovery
```bash
#!/bin/bash
# restore_state.sh - Restore Velostream from backup

BACKUP_DIR=$1
if [ -z "$BACKUP_DIR" ]; then
    echo "Usage: $0 <backup_directory>"
    exit 1
fi

echo "Restoring Velostream from backup: $BACKUP_DIR"

# 1. Stop service
echo "1. Stopping service..."
systemctl stop velostream

# 2. Restore configuration
echo "2. Restoring configuration..."
cp -r "$BACKUP_DIR/config/"* /etc/velostream/

# 3. Restore runtime state
echo "3. Restoring runtime state..."
if [ -f "$BACKUP_DIR/runtime_state.json" ]; then
    curl -X POST http://localhost:8080/admin/state/import \
         -H "Content-Type: application/json" \
         -d @"$BACKUP_DIR/runtime_state.json"
fi

# 4. Start service
echo "4. Starting service..."
systemctl start velostream

# 5. Verify restoration
echo "5. Verifying restoration..."
until curl -sf http://localhost:8080/ready; do
    echo "Service not ready, waiting..."
    sleep 10
done

echo "State restoration complete"
```

This operational runbook provides comprehensive procedures for managing Velostream in production environments, covering all aspects from routine monitoring to emergency recovery procedures.