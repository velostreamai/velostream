# FerrisStreams Native SQL Deployment Guide

> **⚠️ For Development Only**: This guide covers native binary deployment. For production deployment with containers, financial precision, and multi-format serialization, see **[DEPLOYMENT_SUMMARY.md](../DEPLOYMENT_SUMMARY.md)**

## Overview

This guide covers how to deploy and run FerrisStreams SQL functionality for processing Kafka streams with SQL queries. The system provides both server mode for long-running SQL services and client mode for one-off query execution.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Building the SQL Server](#building-the-sql-server)
3. [Deployment Modes](#deployment-modes)
4. [Configuration](#configuration)
5. [Production Deployment](#production-deployment)
6. [Examples](#examples)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)

## Quick Start

### Prerequisites

- Rust 1.70+ installed
- Kafka cluster running (local or remote)
- At least one Kafka topic with data

### 1. Build the SQL Server

```bash
# Clone and build
git clone <your-repo>
cd ferrisstreams

# Build the SQL server binary
cargo build --release --bin ferris-sql

# The binary will be available at: target/release/ferris-sql
```

### 2. Execute a Simple Query

```bash
# Execute a SQL query against a Kafka topic
./target/release/ferris-sql execute \
  --query "SELECT customer_id, amount FROM orders WHERE amount > 100 LIMIT 10" \
  --topic orders \
  --brokers localhost:9092
```

### 3. Start the SQL Server

```bash
# Start the SQL server for interactive use
./target/release/ferris-sql server \
  --brokers localhost:9092 \
  --port 8080
```

## Building the SQL Server

### Development Build

```bash
# Build for development (with debug symbols)
cargo build --bin ferris-sql

# Run directly with cargo
cargo run --bin ferris-sql -- --help
```

### Production Build

```bash
# Build optimized release version
cargo build --release --bin ferris-sql

# The optimized binary will be at target/release/ferris-sql
# Copy to your deployment location
cp target/release/ferris-sql /usr/local/bin/
```

### Cross-Compilation

```bash
# For Linux from macOS
cargo build --release --target x86_64-unknown-linux-gnu --bin ferris-sql

# For musl (Alpine Linux, Docker)
cargo build --release --target x86_64-unknown-linux-musl --bin ferris-sql
```

## Deployment Modes

### 1. Client Mode (Execute)

Execute a single SQL query and exit:

```bash
ferris-sql execute \
  --query "SELECT * FROM events WHERE user_id = 'user123'" \
  --topic user_events \
  --brokers kafka-cluster:9092 \
  --group_id analytics_team \
  --limit 100
```

**Use Cases:**
- Ad-hoc data analysis
- Batch processing scripts
- Development and testing
- CI/CD pipeline data validation

### 2. Server Mode (Long-Running)

Start a persistent SQL service:

```bash
ferris-sql server \
  --brokers kafka-cluster:9092 \
  --port 8080 \
  --group_id ferris_multi_prod
```

**Use Cases:**
- Production SQL streaming services
- Multi-user SQL environments
- Real-time dashboard backends
- Microservice SQL APIs

## Configuration

### Command Line Options

#### Execute Mode

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--query` | SQL query to execute | Required | `"SELECT * FROM orders"` |
| `--topic` | Kafka topic name | Required | `orders` |
| `--brokers` | Kafka broker addresses | `localhost:9092` | `kafka1:9092,kafka2:9092` |
| `--group-id` | Consumer group ID | `ferris-sql-client` | `analytics_team` |
| `--limit` | Max records to process | Unlimited | `1000` |

#### Server Mode

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--brokers` | Kafka broker addresses | `localhost:9092` | `kafka1:9092,kafka2:9092` |
| `--port` | Server HTTP port | `8080` | `9999` |
| `--group-id` | Consumer group ID | `ferris-sql-server` | `sql_service_prod` |

### Environment Variables

Create a `.env` file for configuration:

```bash
# Kafka Configuration
KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_GROUP_ID=ferris_sql_prod
KAFKA_CLIENT_ID=ferris-sql-server-01

# Server Configuration  
SQL_SERVER_PORT=8080
SQL_SERVER_LOG_LEVEL=info

# Security (if using SASL/SSL)
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISMS=PLAIN
KAFKA_SASL_USERNAME=sql_service
KAFKA_SASL_PASSWORD=secret_password

# Performance
SQL_MAX_MEMORY_MB=2048
SQL_WORKER_THREADS=4
```

## Production Deployment

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM rust:1.70 as builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin ferris-sql

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ferris-sql /usr/local/bin/
EXPOSE 8080

CMD ["ferris-sql", "server", "--port", "8080"]
```

Build and run:

```bash
# Build the Docker image
docker build -t ferris-sql:latest .

# Run the container
docker run -d \
  --name ferris-sql-prod \
  -p 8080:8080 \
  -e KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
  ferris-sql:latest
```

### Kubernetes Deployment

Create `k8s-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ferris-sql-server
  labels:
    app: ferris-sql
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ferris-sql
  template:
    metadata:
      labels:
        app: ferris-sql
    spec:
      containers:
      - name: ferris-sql
        image: ferris-sql:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BROKERS
          value: "kafka-cluster:9092"
        - name: SQL_SERVER_PORT
          value: "8080"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
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
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ferris-sql-service
spec:
  selector:
    app: ferris-sql
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8080
  type: LoadBalancer
```

Deploy to Kubernetes:

```bash
kubectl apply -f k8s-deployment.yaml
```

### Systemd Service (Linux)

Create `/etc/systemd/system/ferris-sql.service`:

```ini
[Unit]
Description=FerrisStreams SQL Server
After=network.target
Wants=network.target

[Service]
Type=simple
User=ferris-sql
Group=ferris-sql
ExecStart=/usr/local/bin/ferris-sql server --brokers kafka1:9092,kafka2:9092 --port 8080
Restart=always
RestartSec=5
Environment=RUST_LOG=info
Environment=KAFKA_GROUP_ID=ferris_sql_prod

# Security settings
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable ferris-sql
sudo systemctl start ferris-sql
sudo systemctl status ferris-sql
```

## Examples

### 1. Real-Time Order Processing

```bash
# Process high-value orders in real-time
ferris-sql execute \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.order_id') as order_id,
      JSON_VALUE(payload, '$.customer_id') as customer_id,
      CAST(JSON_VALUE(payload, '$.total'), 'FLOAT') as total,
      TIMESTAMP() as processed_at
    FROM orders 
    WHERE CAST(JSON_VALUE(payload, '$.total'), 'FLOAT') > 1000.0
  " \
  --topic order_events \
  --brokers kafka-prod:9092 \
  --group-id high_value_orders
```

### 2. User Activity Analytics

```bash
# Analyze user behavior patterns
ferris-sql execute \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.user_id') as user_id,
      JSON_VALUE(payload, '$.action') as action,
      SUBSTRING(JSON_VALUE(payload, '$.page_url'), 1, 50) as page,
      _timestamp as event_time
    FROM user_activity 
    WHERE JSON_VALUE(payload, '$.action') IN ('login', 'purchase', 'signup')
  " \
  --topic user_events \
  --brokers kafka-prod:9092 \
  --limit 1000
```

### 3. IoT Sensor Monitoring

```bash
# Monitor temperature sensors for alerts
ferris-sql execute \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.device_id') as device_id,
      JSON_VALUE(payload, '$.location') as location,
      CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') as temp,
      CASE 
        WHEN CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') > 75.0 THEN 'HIGH'
        WHEN CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') < 32.0 THEN 'LOW'
        ELSE 'NORMAL'
      END as alert_level
    FROM iot_sensors 
    WHERE JSON_VALUE(payload, '$.sensor_type') = 'temperature'
  " \
  --topic sensor_data \
  --brokers iot-kafka:9092
```

## Monitoring

### Logging

The SQL server provides structured logging:

```bash
# Set log level
export RUST_LOG=debug
ferris-sql server --brokers localhost:9092

# Log to file
ferris-sql server --brokers localhost:9092 2>&1 | tee sql-server.log
```

### Health Checks

```bash
# Check if server is running (when HTTP endpoints are implemented)
curl http://localhost:8080/health

# Check job status
curl http://localhost:8080/api/jobs

# Get metrics
curl http://localhost:8080/metrics
```

### Performance Monitoring

Key metrics to monitor:

- **Throughput**: Records processed per second
- **Latency**: Query execution time
- **Memory Usage**: SQL engine memory consumption
- **Error Rate**: Failed queries and processing errors
- **Kafka Lag**: Consumer lag on input topics

## Advanced Configuration

### Memory Management

```bash
# Limit memory usage for large queries
export SQL_MAX_MEMORY_MB=1024
ferris-sql server --brokers localhost:9092
```

### Parallelism

```bash
# Configure worker threads
export SQL_WORKER_THREADS=8
ferris-sql server --brokers localhost:9092
```

### Kafka Security

For secure Kafka clusters:

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISMS=PLAIN
export KAFKA_SASL_USERNAME=ferris_sql
export KAFKA_SASL_PASSWORD=secure_password

ferris-sql server --brokers secure-kafka:9093
```

## Troubleshooting

### Common Issues

#### Connection Problems

```bash
# Test Kafka connectivity
kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 1

# Check DNS resolution
nslookup kafka-cluster
```

#### Query Parsing Errors

```bash
# Validate SQL syntax
ferris-sql execute --query "SELECT * FROM orders LIMIT 1" --topic orders --brokers localhost:9092
```

#### Performance Issues

```bash
# Monitor resource usage
top -p $(pgrep ferris-sql)

# Check Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --group ferris-sql-client --describe
```

### Debug Mode

```bash
# Enable debug logging
export RUST_LOG=debug
ferris-sql execute --query "SELECT * FROM orders" --topic orders --brokers localhost:9092
```

### Log Analysis

```bash
# Filter SQL execution logs
grep "SQL" sql-server.log | tail -20

# Monitor error patterns
grep "ERROR" sql-server.log | cut -d' ' -f4- | sort | uniq -c
```

## Next Steps

1. **HTTP API**: The current implementation is a foundation. Next steps include:
   - REST API for job management
   - WebSocket support for real-time results
   - Web UI for SQL query development

2. **Clustering**: Deploy multiple instances for high availability

3. **State Persistence**: Add checkpointing for stateful queries

4. **Query Optimization**: Implement cost-based query optimization

5. **Integration**: Connect with existing data tools and dashboards

For questions and support, refer to the [SQL Reference Guide](SQL_REFERENCE_GUIDE.md) and project documentation.