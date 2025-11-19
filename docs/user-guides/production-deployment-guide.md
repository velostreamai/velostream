# Production Deployment Guide

**Version**: 1.0
**Last Updated**: September 28, 2025
**Status**: Production Ready

## Overview

This guide provides comprehensive instructions for deploying Velostream in production environments, covering configuration, monitoring, scaling, and operational best practices for high-availability stream processing.

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Pre-Production Checklist](#pre-production-checklist)
3. [Configuration Management](#configuration-management)
4. [Deployment Strategies](#deployment-strategies)
5. [Performance Tuning](#performance-tuning)
6. [Monitoring & Observability](#monitoring--observability)
7. [High Availability Setup](#high-availability-setup)
8. [Disaster Recovery](#disaster-recovery)
9. [Security Hardening](#security-hardening)
10. [Troubleshooting Guide](#troubleshooting-guide)

## System Requirements

### Hardware Requirements

```yaml
# Minimum Production Requirements
CPU: 8 cores (16 recommended)
Memory: 16 GB RAM (32 GB recommended)
Storage:
  - System: 50 GB SSD
  - Data: 500 GB+ SSD (based on retention)
Network: 1 Gbps (10 Gbps for high-throughput)

# Kafka Cluster Requirements
Brokers: Minimum 3 nodes
Zookeeper: 3 nodes (odd number)
Replication Factor: Minimum 2 (3 recommended)
```

### Software Requirements

```yaml
Operating System: Linux (Ubuntu 20.04+, RHEL 8+)
Container Runtime: Docker 20.10+ / Containerd 1.5+
Orchestration: Kubernetes 1.24+ (optional)
Java: OpenJDK 11+ (for Kafka)
Monitoring: Prometheus 2.30+, Grafana 8.0+
```

## Pre-Production Checklist

### Essential Configurations

```bash
#!/bin/bash
# pre-production-check.sh

echo "🔍 Running Pre-Production Checks..."

# 1. Verify Kafka connectivity
kafka-topics --bootstrap-server=$KAFKA_BROKERS --list > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Kafka connectivity verified"
else
    echo "❌ Kafka connection failed"
    exit 1
fi

# 2. Check database connectivity
psql -h $DB_HOST -U $DB_USER -c "SELECT 1" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Database connectivity verified"
else
    echo "❌ Database connection failed"
    exit 1
fi

# 3. Validate configuration files
for config in config/*.yaml; do
    if [ -f "$config" ]; then
        yaml-lint "$config" || exit 1
    fi
done
echo "✅ Configuration files validated"

# 4. Check resource limits
ulimit -n 65536  # File descriptors
ulimit -u 32768  # Max processes
echo "✅ Resource limits configured"

# 5. Verify logging directory
mkdir -p /var/log/velostream
chmod 755 /var/log/velostream
echo "✅ Logging directory ready"

echo "✅ All pre-production checks passed!"
```

### Performance Baseline

```sql
-- Create performance baseline tables
CREATE TABLE IF NOT EXISTS perf_baseline (
    timestamp TIMESTAMP,
    metric_name VARCHAR(100),
    value DOUBLE,
    unit VARCHAR(20)
);

-- Record baseline metrics
INSERT INTO perf_baseline VALUES
    (NOW(), 'throughput_records_sec', 50000, 'records/sec'),
    (NOW(), 'latency_p99', 100, 'ms'),
    (NOW(), 'memory_usage', 8192, 'MB'),
    (NOW(), 'cpu_usage', 60, 'percent');
```

## Configuration Management

### Environment-Based Configuration

```yaml
# config/production.yaml
velostream:
  environment: production

  # Kafka Configuration
  kafka:
    bootstrap_servers: "kafka1:9092,kafka2:9092,kafka3:9092"
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    batch_size: 65536
    linger_ms: 100
    compression_type: snappy
    acks: all
    retries: 5

  # Processing Configuration
  processing:
    parallelism: 16
    checkpoint_interval: 60000
    state_backend: rocksdb

  # Timeout Configuration
  timeouts:
    operation_timeout: 30s
    connection_timeout: 10s
    query_timeout: 5m
    table_load_timeout: 2m

  # Circuit Breaker
  circuit_breaker:
    failure_threshold: 5
    recovery_timeout: 60s
    failure_rate_threshold: 50.0
    min_calls_in_window: 10

  # Monitoring
  monitoring:
    metrics_port: 9090
    health_check_port: 8080
    progress_tracking: true
    log_level: INFO
```

### Secrets Management

```bash
#!/bin/bash
# setup-secrets.sh

# Use environment variables for sensitive data
export KAFKA_USERNAME=$(vault kv get -field=username secret/kafka)
export KAFKA_PASSWORD=$(vault kv get -field=password secret/kafka)
export DB_PASSWORD=$(vault kv get -field=password secret/database)

# Or use Kubernetes secrets
kubectl create secret generic velostream-secrets \
    --from-literal=kafka-username=$KAFKA_USERNAME \
    --from-literal=kafka-password=$KAFKA_PASSWORD \
    --from-literal=db-password=$DB_PASSWORD
```

## Deployment Strategies

### Docker Deployment

```dockerfile
# Dockerfile.production
FROM rust:1.70 as builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build with optimizations
RUN cargo build --release

# Runtime image
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
    libssl1.1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/velostream /usr/local/bin/

# Non-root user
RUN useradd -m -u 1000 velostream
USER velostream

EXPOSE 8080 9090

ENTRYPOINT ["velostream"]
CMD ["--config", "/etc/velostream/config.yaml"]
```

### Kubernetes Deployment

```yaml
# kubernetes/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velostream
  namespace: stream-processing
spec:
  replicas: 3
  selector:
    matchLabels:
      app: velostream
  template:
    metadata:
      labels:
        app: velostream
    spec:
      containers:
      - name: velostream
        image: velostream:latest
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "16Gi"
            cpu: "8"
        env:
        - name: KAFKA_BROKERS
          value: "kafka-0:9092,kafka-1:9092,kafka-2:9092"
        - name: KAFKA_USERNAME
          valueFrom:
            secretKeyRef:
              name: velostream-secrets
              key: kafka-username
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /etc/velostream
        - name: data
          mountPath: /var/lib/velostream
      volumes:
      - name: config
        configMap:
          name: velostream-config
      - name: data
        persistentVolumeClaim:
          claimName: velostream-data
```

### Horizontal Scaling with @application Annotation

Velostream supports automatic multi-server coordination through the `@application` annotation. Multiple JobServer instances share Kafka consumer groups and automatically balance the partition workload.

**Enable automatic coordination:**

```sql
-- my-application.sql
@application trading_platform
@phase production

CREATE STREAM market_data AS
SELECT symbol, price, volume, trade_time
FROM kafka_market_data
EMIT CHANGES;

CREATE STREAM trade_aggregates AS
SELECT symbol, SUM(volume) as total_volume
FROM market_data
GROUP BY symbol
EMIT CHANGES;
```

**Deploy on multiple servers:**

```bash
# Each server runs the same SQL application
# Kafka automatically coordinates through consumer groups

# Server 1
velostream-server --app trading-platform.sql --port 8081

# Server 2 (automatic partition sharing)
velostream-server --app trading-platform.sql --port 8081

# Server 3 (automatic partition sharing)
velostream-server --app trading-platform.sql --port 8081
```

**Automatic behavior:**

- Consumer groups: `velo-{app_name}-{job_name}` (e.g., `velo-trading_platform-market_data`)
- Kafka distributes partitions across all 3 servers
- Each server processes ~1/3 of data automatically
- When servers join/leave, partitions are rebalanced transparently

**Kubernetes HPA (Horizontal Pod Autoscaler):**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: velostream-hpa
  namespace: stream-processing
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: velostream
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 15
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
      - type: Pods
        value: 2
        periodSeconds: 15
      selectPolicy: Max
```

**Monitoring partition distribution:**

```bash
# Check consumer group status
kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group velo-trading_platform-market_data \
    --describe

# Expected output (3 servers, 12 partitions):
# TOPIC           PARTITION  CURRENT-OFFSET  CONSUMER-ID
# market_data     0          1000            server1-consumer
# market_data     1          1000            server2-consumer
# market_data     2          1000            server3-consumer
# market_data     3          1000            server1-consumer
# ... (evenly distributed)
```

For detailed information on multi-server coordination, see [Multi-Server Coordination Guide](../sql/ops/multi-server-coordination-guide.md).

### Blue-Green Deployment

```bash
#!/bin/bash
# blue-green-deploy.sh

CURRENT_ENV=$(kubectl get service velostream-active -o jsonpath='{.spec.selector.environment}')
NEW_ENV="green"
if [ "$CURRENT_ENV" = "green" ]; then
    NEW_ENV="blue"
fi

echo "Deploying to $NEW_ENV environment..."

# Deploy new version
kubectl set image deployment/velostream-$NEW_ENV \
    velostream=velostream:$VERSION \
    --record

# Wait for rollout
kubectl rollout status deployment/velostream-$NEW_ENV

# Run smoke tests
./run-smoke-tests.sh $NEW_ENV
if [ $? -ne 0 ]; then
    echo "❌ Smoke tests failed, rolling back..."
    kubectl rollout undo deployment/velostream-$NEW_ENV
    exit 1
fi

# Switch traffic
kubectl patch service velostream-active \
    -p '{"spec":{"selector":{"environment":"'$NEW_ENV'"}}}'

echo "✅ Deployment successful! Traffic switched to $NEW_ENV"

# Keep old version for quick rollback
echo "Previous version ($CURRENT_ENV) kept for rollback"
```

## Performance Tuning

### JVM Tuning (for Kafka clients)

```bash
# jvm-options.sh
export JVM_OPTS="-Xms4g -Xmx4g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=50 \
    -XX:+ParallelRefProcEnabled \
    -XX:+AlwaysPreTouch \
    -XX:+DisableExplicitGC \
    -Djava.awt.headless=true \
    -Djava.net.preferIPv4Stack=true"
```

### System Tuning

```bash
#!/bin/bash
# system-tuning.sh

# Network tuning
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
sysctl -w net.core.netdev_max_backlog=5000

# File system tuning
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf

# Disk I/O tuning
echo noop > /sys/block/sda/queue/scheduler
echo 256 > /sys/block/sda/queue/nr_requests

# Memory tuning
echo 'vm.swappiness=1' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio=5' >> /etc/sysctl.conf
echo 'vm.dirty_ratio=10' >> /etc/sysctl.conf

sysctl -p
```

### Velostream Optimization

```yaml
# optimized-config.yaml
performance:
  # Batch processing
  batch_size: 100000
  batch_timeout: 200ms

  # Memory management
  buffer_size: 1048576  # 1MB
  max_in_flight_requests: 5

  # Compression
  compression:
    type: snappy
    level: 6

  # Parallelism
  worker_threads: 16
  io_threads: 8

  # Caching
  cache:
    table_cache_size: 10000
    query_cache_size: 1000
    ttl: 300s
```

## Monitoring & Observability

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'velostream'
    static_configs:
      - targets: ['velostream:9090']
    metrics_path: /metrics

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-exporter:9308']

  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
```

### Key Metrics to Monitor

```yaml
# Critical Metrics
throughput:
  - velostream_records_processed_total
  - velostream_bytes_processed_total
  - velostream_events_per_second

latency:
  - velostream_processing_latency_ms
  - velostream_e2e_latency_ms
  - velostream_checkpoint_duration_ms

errors:
  - velostream_errors_total
  - velostream_retries_total
  - velostream_circuit_breaker_trips

resources:
  - process_cpu_seconds_total
  - process_resident_memory_bytes
  - go_memstats_heap_inuse_bytes

kafka:
  - kafka_consumer_lag
  - kafka_producer_record_send_rate
  - kafka_broker_availability
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Velostream Production Monitoring",
    "panels": [
      {
        "title": "Throughput",
        "targets": [
          {
            "expr": "rate(velostream_records_processed_total[5m])"
          }
        ]
      },
      {
        "title": "P99 Latency",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, velostream_processing_latency_ms)"
          }
        ]
      },
      {
        "title": "Error Rate",
        "targets": [
          {
            "expr": "rate(velostream_errors_total[5m])"
          }
        ]
      },
      {
        "title": "Consumer Lag",
        "targets": [
          {
            "expr": "kafka_consumer_lag"
          }
        ]
      }
    ]
  }
}
```

### Logging Configuration

```yaml
# logging-config.yaml
logging:
  level: INFO
  format: json
  outputs:
    - type: console
      level: INFO
    - type: file
      path: /var/log/velostream/app.log
      rotation:
        max_size: 100MB
        max_age: 7d
        max_backups: 10
    - type: syslog
      host: syslog.internal
      port: 514

  structured_fields:
    - timestamp
    - level
    - message
    - trace_id
    - span_id
    - component
    - error_type
```

## High Availability Setup

### Multi-Region Deployment

```yaml
# multi-region-config.yaml
regions:
  primary:
    name: us-east-1
    endpoints:
      kafka: "kafka-east.internal:9092"
      database: "db-east.internal:5432"

  secondary:
    name: us-west-2
    endpoints:
      kafka: "kafka-west.internal:9092"
      database: "db-west.internal:5432"

  failover:
    strategy: active-passive
    health_check_interval: 10s
    failover_threshold: 3
```

### Load Balancing

```nginx
# nginx.conf
upstream velostream {
    least_conn;
    server velostream-1:8080 max_fails=3 fail_timeout=30s;
    server velostream-2:8080 max_fails=3 fail_timeout=30s;
    server velostream-3:8080 max_fails=3 fail_timeout=30s;

    keepalive 32;
}

server {
    listen 80;

    location / {
        proxy_pass http://velostream;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_connect_timeout 5s;
        proxy_read_timeout 30s;

        # Circuit breaking
        proxy_next_upstream error timeout http_502 http_503;
        proxy_next_upstream_timeout 5s;
        proxy_next_upstream_tries 3;
    }

    location /health {
        access_log off;
        proxy_pass http://velostream/health;
    }
}
```

### State Replication

```bash
#!/bin/bash
# state-replication.sh

# RocksDB state backup
backup_state() {
    BACKUP_DIR="/backup/velostream/$(date +%Y%m%d_%H%M%S)"
    mkdir -p $BACKUP_DIR

    # Stop processing temporarily
    curl -X POST http://localhost:8080/checkpoint

    # Backup state
    rsync -av /var/lib/velostream/state/ $BACKUP_DIR/

    # Resume processing
    curl -X POST http://localhost:8080/resume

    # Upload to S3
    aws s3 sync $BACKUP_DIR s3://velostream-state-backup/
}

# Restore state
restore_state() {
    RESTORE_FROM=$1

    # Download from S3
    aws s3 sync s3://velostream-state-backup/$RESTORE_FROM /tmp/restore/

    # Stop application
    systemctl stop velostream

    # Restore state
    rsync -av /tmp/restore/ /var/lib/velostream/state/

    # Start application
    systemctl start velostream
}
```

## Disaster Recovery

### Backup Strategy

```bash
#!/bin/bash
# backup-strategy.sh

# Configuration backup
backup_configs() {
    tar -czf /backup/configs-$(date +%Y%m%d).tar.gz /etc/velostream/
    aws s3 cp /backup/configs-*.tar.gz s3://velostream-backup/configs/
}

# Kafka offset backup
backup_offsets() {
    kafka-consumer-groups --bootstrap-server $KAFKA_BROKERS \
        --all-groups --describe > /backup/offsets-$(date +%Y%m%d).txt
    aws s3 cp /backup/offsets-*.txt s3://velostream-backup/offsets/
}

# Database backup
backup_database() {
    pg_dump -h $DB_HOST -U $DB_USER -d velostream \
        > /backup/db-$(date +%Y%m%d).sql
    gzip /backup/db-*.sql
    aws s3 cp /backup/db-*.sql.gz s3://velostream-backup/database/
}

# Run all backups
backup_configs
backup_offsets
backup_database
backup_state
```

### Recovery Procedures

```bash
#!/bin/bash
# recovery-procedures.sh

# Point-in-time recovery
recover_to_timestamp() {
    TIMESTAMP=$1

    echo "Starting recovery to $TIMESTAMP..."

    # 1. Stop current processing
    kubectl scale deployment velostream --replicas=0

    # 2. Restore database
    aws s3 cp s3://velostream-backup/database/db-$TIMESTAMP.sql.gz /tmp/
    gunzip /tmp/db-$TIMESTAMP.sql.gz
    psql -h $DB_HOST -U $DB_USER -d velostream < /tmp/db-$TIMESTAMP.sql

    # 3. Restore Kafka offsets
    aws s3 cp s3://velostream-backup/offsets/offsets-$TIMESTAMP.txt /tmp/
    # Parse and reset consumer group offsets

    # 4. Restore state
    restore_state $TIMESTAMP

    # 5. Start processing
    kubectl scale deployment velostream --replicas=3

    echo "✅ Recovery complete!"
}
```

### RTO/RPO Targets

```yaml
# Recovery targets
disaster_recovery:
  rpo: 15 minutes  # Recovery Point Objective
  rto: 1 hour      # Recovery Time Objective

  backup_schedule:
    state: every 15 minutes
    config: daily
    database: hourly

  retention:
    state: 7 days
    config: 30 days
    database: 30 days
```

## Security Hardening

### Network Security

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: velostream-network-policy
spec:
  podSelector:
    matchLabels:
      app: velostream
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: gateway
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: kafka
    ports:
    - protocol: TCP
      port: 9092
```

### TLS Configuration

```yaml
# tls-config.yaml
security:
  tls:
    enabled: true
    cert_file: /etc/velostream/certs/server.crt
    key_file: /etc/velostream/certs/server.key
    ca_file: /etc/velostream/certs/ca.crt

    client_auth: required
    min_version: TLS1.2
    ciphers:
      - TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
      - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
```

### RBAC Configuration

```yaml
# rbac.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: velostream-role
rules:
- apiGroups: [""]
  resources: ["pods", "services"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["configmaps", "secrets"]
  verbs: ["get"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: velostream-rolebinding
subjects:
- kind: ServiceAccount
  name: velostream
roleRef:
  kind: Role
  name: velostream-role
  apiGroup: rbac.authorization.k8s.io
```

## Troubleshooting Guide

### Common Issues

#### High Memory Usage

```bash
# Diagnose memory issues
#!/bin/bash

# Check memory usage
ps aux | grep velostream | awk '{print $6/1024 " MB"}'

# Analyze heap dump
jmap -dump:format=b,file=heap.bin $(pgrep velostream)
jhat heap.bin

# Solutions:
# 1. Increase heap size
# 2. Reduce batch size
# 3. Enable compression
# 4. Check for memory leaks
```

#### Kafka Lag

```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server $KAFKA_BROKERS \
    --group velostream-consumer --describe

# Reset offsets if needed
kafka-consumer-groups --bootstrap-server $KAFKA_BROKERS \
    --group velostream-consumer \
    --reset-offsets --to-earliest \
    --topic input-topic --execute
```

#### Processing Delays

```sql
-- Check processing metrics
SELECT
    AVG(processing_time_ms) as avg_time,
    MAX(processing_time_ms) as max_time,
    MIN(processing_time_ms) as min_time,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_time_ms) as p99
FROM processing_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Identify slow queries
SELECT
    query_text,
    execution_time_ms,
    rows_processed
FROM query_logs
WHERE execution_time_ms > 1000
ORDER BY execution_time_ms DESC
LIMIT 10;
```

### Health Check Script

```bash
#!/bin/bash
# health-check.sh

ERRORS=0

# Check service health
check_service() {
    SERVICE=$1
    URL=$2

    response=$(curl -s -o /dev/null -w "%{http_code}" $URL)
    if [ "$response" = "200" ]; then
        echo "✅ $SERVICE is healthy"
    else
        echo "❌ $SERVICE is unhealthy (HTTP $response)"
        ERRORS=$((ERRORS + 1))
    fi
}

# Run checks
check_service "Velostream" "http://localhost:8080/health"
check_service "Metrics" "http://localhost:9090/metrics"
check_service "Kafka" "http://localhost:9092/health"

# Check resources
MEM_USAGE=$(free -m | awk 'NR==2{printf "%.1f", $3*100/$2}')
CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)

echo "📊 Resource Usage:"
echo "   Memory: ${MEM_USAGE}%"
echo "   CPU: ${CPU_USAGE}%"

if [ $ERRORS -gt 0 ]; then
    echo "⚠️  Health check failed with $ERRORS errors"
    exit 1
else
    echo "✅ All health checks passed!"
fi
```

## Production Readiness Checklist

```markdown
### Pre-Production
- [ ] All unit tests passing
- [ ] Integration tests completed
- [ ] Load testing performed
- [ ] Security scanning completed
- [ ] Documentation updated

### Configuration
- [ ] Environment variables configured
- [ ] Secrets management setup
- [ ] Logging configured
- [ ] Monitoring enabled
- [ ] Alerts configured

### Infrastructure
- [ ] High availability setup
- [ ] Load balancing configured
- [ ] Auto-scaling enabled
- [ ] Backup strategy implemented
- [ ] Disaster recovery plan tested

### Operational
- [ ] Runbooks created
- [ ] On-call rotation setup
- [ ] Incident response plan
- [ ] Performance baselines established
- [ ] SLA/SLO defined

### Security
- [ ] TLS enabled
- [ ] Authentication configured
- [ ] Authorization (RBAC) setup
- [ ] Network policies applied
- [ ] Security audit completed
```

## Support & Resources

### Documentation
- [Architecture Guide](../architecture/)
- [API Reference](../api/)
- [Configuration Reference](../config/)
- [Troubleshooting Guide](../troubleshooting/)

### Monitoring Dashboards
- Grafana: `https://grafana.internal/dashboard/velostream`
- Prometheus: `https://prometheus.internal/targets`
- Logs: `https://kibana.internal/app/logs`

### Support Channels
- Slack: `#velostream-support`
- Email: `velostream-team@company.com`
- On-Call: `+1-xxx-xxx-xxxx`

---

*This guide is maintained by the Velostream team. For updates or corrections, please submit a pull request.*