# FerrisStreams SQL Deployment Summary

## 🚀 Complete Deployment Infrastructure

FerrisStreams SQL now includes comprehensive Docker and Kubernetes deployment infrastructure for production-ready streaming SQL processing with **Phase 2 hash join optimization** delivering 10x+ performance improvements for large datasets.

## 📦 What's Included

### Docker Infrastructure ✅

- **`Dockerfile`** - Multi-format SQL server (JSON, Avro, Protobuf)
- **`Dockerfile.multi`** - Multi-job SQL server container  
- **`Dockerfile.sqlfile`** - SQL file deployment container
- **`docker-compose.yml`** - Complete infrastructure with Schema Registry (UPDATED)
- **`deploy-docker.sh`** - Automated deployment script
- **`monitoring/`** - Prometheus & Grafana configuration

### Financial Precision & Serialization ✅

- **All Serialization Formats** - JSON, Avro, Protobuf in single Docker image
- **Financial ScaledInteger** - 42x performance with perfect precision
- **Flink-Compatible Avro** - Industry-standard decimal logical types
- **Cross-System Compatibility** - Works with Flink, Kafka Connect, BigQuery


### Kubernetes Infrastructure ✅

- **`k8s/namespace.yaml`** - Kubernetes namespace
- **`k8s/kafka.yaml`** - Kafka broker deployment
- **`k8s/sql-servers.yaml`** - SQL server deployments
- **`k8s/ingress.yaml`** - External access configuration
- **`k8s/deploy-k8s.sh`** - Automated K8s deployment

### Configuration & Monitoring ✅

- **SQL Configuration** - Production-ready settings
- **Prometheus** - Metrics collection
- **Grafana** - Dashboard visualization
- **Health Checks** - Service monitoring
- **Resource Limits** - Production constraints

## 🎯 Deployment Options

### 1. SQL File Deployment (RECOMMENDED for single process)

```bash
# Build the SQL file deployment image
docker build -f Dockerfile.sqlfile -t ferrisstreams:sqlfile .

# Create basic configuration file
cat > configs/ferris-default.yaml <<EOF
kafka:
  brokers: "kafka:9092"
  consumer_timeout_ms: 5000

server:
  port: 8080
  max_connections: 100

sql:
  worker_threads: 4
  query_timeout_ms: 30000
  max_memory_mb: 2048

performance:
  buffer_size: 1000
  batch_size: 100
  flush_interval_ms: 100
EOF

# Deploy with your SQL file and configuration
docker run -d \
  -p 8080:8080 -p 9080:9080 \
  -v $(pwd)/my-app.sql:/app/sql-files/app.sql \
  -v $(pwd)/configs/ferris-default.yaml:/app/sql-config.yaml \
  -e KAFKA_BROKERS=kafka:9092 \
  -e SQL_FILE=/app/sql-files/app.sql \
  --name ferrisstreams-app \
  ferrisstreams:sqlfile

# Enhanced deployment with schema files and configuration
docker run -d \
  -p 8080:8080 -p 9080:9080 \
  -v $(pwd)/my-app.sql:/app/sql-files/app.sql \
  -v $(pwd)/schemas:/app/schemas:ro \
  -v $(pwd)/configs/ferris-default.yaml:/app/sql-config.yaml \
  -e KAFKA_BROKERS=kafka:9092 \
  -e SQL_FILE=/app/sql-files/app.sql \
  -e FERRIS_FINANCIAL_PRECISION=true \
  -e FERRIS_SERIALIZATION_FORMATS=json,avro,protobuf \
  -e RUST_LOG=info \
  --name ferrisstreams-app \
  ferrisstreams:sqlfile

# With performance tuning for financial precision
docker run -d \
  -p 8080:8080 -p 9080:9080 \
  -v $(pwd)/my-app.sql:/app/sql-files/app.sql \
  -v $(pwd)/schemas:/app/schemas:ro \
  -v $(pwd)/configs/ferris-financial.yaml:/app/sql-config.yaml \
  -e KAFKA_BROKERS=kafka:9092 \
  -e SQL_FILE=/app/sql-files/app.sql \
  -e FERRIS_PERFORMANCE_PROFILE=financial \
  -e SQL_WORKER_THREADS=8 \
  -e SQL_MEMORY_LIMIT_MB=4096 \
  --restart unless-stopped \
  --name ferrisstreams-app \
  ferrisstreams:sqlfile

# Or with Docker Compose
cat > docker-compose.sqlfile.yml <<EOF
version: '3.8'
services:
  ferrisstreams-app:
    build:
      context: .
      dockerfile: Dockerfile.sqlfile
    ports:
      - "8080:8080"
      - "9080:9080"
    environment:
      - KAFKA_BROKERS=kafka:9092
      - SQL_FILE=/app/sql-files/my-app.sql
    volumes:
      - ./my-app.sql:/app/sql-files/my-app.sql
    depends_on:
      - kafka
EOF
```

### 2. Quick Start (Docker Compose) - All Serialization Formats

```bash
# Clone repository
git clone <repository>
cd ferrisstreams

# Deploy complete infrastructure with all serialization formats
docker-compose up --build

# Access services
# - FerrisStreams (All Formats): http://localhost:8080
# - Kafka UI: http://localhost:8085
# - Test multi-format data producer included
# Note: Schema Registry included in docker-compose but not yet implemented in FerrisStreams
```

### 2. Production (Kubernetes)

```bash
# Deploy to Kubernetes cluster
cd k8s
./deploy-k8s.sh

# Access via NodePort or LoadBalancer
kubectl get services -n ferris-sql
```

### 3. Test All Serialization Formats

```bash
# Test JSON financial data (exact precision)
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, price, quantity, price * quantity as total FROM json_financial_stream",
    "format": "json"
  }'

# Test with financial precision calculations
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, CAST(price AS DECIMAL(18,4)) * quantity as precise_total FROM avro_trades_stream",
    "format": "avro" 
  }'
```

### 4. Performance Monitoring Endpoints

```bash
# Real-time performance metrics
curl http://localhost:9080/metrics/performance

# Prometheus format metrics
curl http://localhost:9080/metrics/prometheus

# System health check
curl http://localhost:9080/metrics/health

# Query performance analysis
curl http://localhost:9080/metrics/queries/slow

# Performance report
curl http://localhost:9080/metrics/report
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      FerrisStreams SQL Stack                           │
│       ⚡ Multi-Format + Financial Precision + Hash Join Optimized      │
├─────────────────────────────────────────────────────────────────────────┤
│  🌐 Kafka UI        📊 Schema Reg     💰 Financial     🔍 Protobuf       │
│  (Port 8085)        (Port 8081)       Precision       Support          │
├─────────────────────────────────────────────────────────────────────────┤
│        📡 FerrisStreams (All Formats) + 🔧 Multi-Format Producer        │
│               (Port 8080) + (Port 9090)                                │
├─────────────────────────────────────────────────────────────────────────┤
│        🚀 Kafka + Zookeeper + 📁 Persistent Storage Volumes             │
│                          (Port 9092)                                   │
└─────────────────────────────────────────────────────────────────────────┘

🚀 Financial + Multi-Format Features:
- JSON, Avro, Protobuf: All formats in single container
- Financial Precision: 42x performance, perfect decimal arithmetic
- Flink Compatible: Industry-standard Avro decimal logical types
- Cross-System: Works with Flink, Kafka Connect, BigQuery, Spark
- Hash Join Optimized: 10x+ performance for large datasets
```

## 📋 Service Details

### FerrisStreams Multi-Format SQL Server (PRIMARY)
- **Purpose**: Execute SQL with all serialization formats (JSON, Avro, Protobuf)
- **Container**: `ferris-streams`
- **Ports**: 8080 (API), 9090 (Metrics)
- **Features**: 
  - Financial precision arithmetic (42x performance)
  - Flink-compatible Avro decimal logical types
  - Industry-standard Protobuf Decimal messages
  - Cross-system compatibility
- **Use Cases**: Production financial analytics, multi-format data processing

### FerrisStreams SQL Multi-Job Server  
- **Purpose**: Manage multiple concurrent SQL jobs with advanced orchestration
- **Container**: `ferris-sql-multi`
- **Ports**: 8081 (API), 9091 (Metrics)
- **Performance**: 10x+ JOIN performance, comprehensive monitoring
- **Use Cases**: Complex analytics, job orchestration, enterprise workloads

### FerrisStreams SQL File Deployment
- **Purpose**: Single-process deployment with SQL file input
- **Container**: Built from `Dockerfile.sqlfile`
- **Ports**: 8080 (API), 9080 (Metrics)  
- **Performance**: Hash join optimized, automatic monitoring
- **Use Cases**: Containerized deployments, CI/CD pipelines, production apps

### Supporting Infrastructure
- **Kafka**: Message streaming platform (Port 9092)
- **Zookeeper**: Kafka coordination (Port 2181)
- **Schema Registry**: Future Avro schema management (not yet implemented)
- **Kafka UI**: Web-based Kafka management interface (Port 8085)
- **Multi-Format Producer**: Test data generator for all formats - **NEW**

## 🔧 Configuration

### Performance Profiles

#### 1. Low Latency Configuration (< 10ms)

**configs/ferris-low-latency.yaml:**
```yaml
# Ultra-low latency configuration
kafka:
  brokers: "kafka:29092"
  # Low latency producer settings
  acks: "1"                    # Fast acknowledgments
  retries: 0                   # No retries for speed
  batch_size: 1                # Send immediately
  linger_ms: 0                 # No batching delay
  buffer_memory: 33554432      # 32MB buffer
  
  # Low latency consumer settings
  fetch_min_bytes: 1           # Don't wait for batches
  fetch_max_wait_ms: 1         # 1ms max wait
  max_poll_records: 10         # Small batches
  session_timeout_ms: 6000     # Fast failure detection
  heartbeat_interval_ms: 2000  # Frequent heartbeats

server:
  port: 8080
  max_connections: 1000        # High concurrency
  request_timeout_ms: 5000     # Fast timeouts

sql:
  worker_threads: 8            # More threads
  query_timeout_ms: 10000      # 10s query limit
  max_memory_mb: 4096          # More memory

performance:
  buffer_size: 100             # Small buffers for speed
  batch_size: 10               # Tiny batches
  flush_interval_ms: 1         # Immediate flush
  enable_compression: false    # No compression overhead
```

**Docker deployment:**
```bash
docker run -d \
  -p 8080:8080 \
  -v $(pwd)/configs/ferris-low-latency.yaml:/app/sql-config.yaml \
  -e RUST_LOG=warn \
  --name ferris-low-latency \
  ferrisstreams:latest
```

#### 2. High Throughput Configuration (>100k msgs/sec)

**configs/ferris-high-throughput.yaml:**
```yaml
kafka:
  brokers: "kafka:29092"
  # High throughput producer settings
  acks: "1"
  batch_size: 65536            # 64KB batches
  linger_ms: 5                 # 5ms batching window
  buffer_memory: 134217728     # 128MB buffer
  compression_type: "lz4"      # Fast compression
  
  # High throughput consumer settings
  fetch_min_bytes: 50000       # Wait for larger batches
  fetch_max_wait_ms: 500       # 500ms max wait
  max_poll_records: 5000       # Large batches
  max_partition_fetch_bytes: 1048576  # 1MB per partition

sql:
  worker_threads: 16           # Many workers
  max_memory_mb: 8192          # Lots of memory
  
performance:
  buffer_size: 10000           # Large buffers
  batch_size: 1000             # Big batches
  flush_interval_ms: 100       # Batch writes
  enable_compression: true     # Save bandwidth
```

#### 3. Financial Precision Configuration

**configs/ferris-financial.yaml:**
```yaml
kafka:
  brokers: "kafka:29092"
  # Reliability for financial data
  acks: "all"                  # Full acknowledgments
  retries: 3                   # Retry on failure
  enable_idempotence: true     # Exactly-once semantics
  
serialization:
  default_format: "json"       # Start with JSON
  financial_precision: true    # Enable ScaledInteger
  decimal_places: 4            # 4 decimal places default
  
sql:
  # Financial arithmetic optimizations
  enable_financial_types: true
  precision_mode: "exact"      # No approximations
  scale_validation: true       # Validate decimal scales
```

#### 4. Cross-System Compatibility Configuration

**configs/ferris-compatibility.yaml:**
```yaml
kafka:
  brokers: "kafka:29092"
  
serialization:
  formats: ["json", "avro", "protobuf"]
  
  # Avro settings for Flink compatibility
  avro:
    use_decimal_logical_type: true
    decimal_precision: 18
    decimal_scale: 4
    
  # Protobuf settings
  protobuf:
    use_decimal_messages: true
    
schema_registry:
  url: "http://schema-registry:8081"
  auth_type: "none"
  
sql:
  cross_system_mode: true      # Enable compatibility features
```

### Environment Variables
```bash
# Basic Configuration
RUST_LOG=info                              # Logging level
KAFKA_BROKERS=kafka:29092                  # Kafka connection
SCHEMA_REGISTRY_URL=http://schema-registry:8081  # Future schema registry (not yet implemented)
FERRIS_SERIALIZATION_FORMATS=json,avro,protobuf  # Available formats

# Performance Tuning
SQL_MAX_JOBS=20                            # Job limits
SQL_MEMORY_LIMIT_MB=1024                   # Memory constraints
SQL_WORKER_THREADS=4                       # Processing threads
FERRIS_PERFORMANCE_PROFILE=standard       # standard|low_latency|high_throughput

# Financial Precision
FERRIS_FINANCIAL_PRECISION=true           # Enable ScaledInteger
FERRIS_DEFAULT_DECIMAL_PLACES=4           # Default precision

# Kafka Tuning (override config file)
KAFKA_BATCH_SIZE=16384                     # Producer batch size
KAFKA_LINGER_MS=5                          # Producer batching delay
KAFKA_FETCH_MIN_BYTES=1                    # Consumer fetch minimum
KAFKA_MAX_POLL_RECORDS=500                 # Consumer batch size
```

### Docker Compose Override Examples

#### Low Latency Setup
```yaml
# docker-compose.low-latency.yml
version: '3.8'
services:
  ferris-streams:
    environment:
      - FERRIS_PERFORMANCE_PROFILE=low_latency
      - KAFKA_LINGER_MS=0
      - KAFKA_BATCH_SIZE=1
      - KAFKA_FETCH_MAX_WAIT_MS=1
    volumes:
      - ./configs/ferris-low-latency.yaml:/app/sql-config.yaml
```

#### High Throughput Setup
```yaml
# docker-compose.high-throughput.yml
version: '3.8'
services:
  ferris-streams:
    environment:
      - FERRIS_PERFORMANCE_PROFILE=high_throughput
      - SQL_WORKER_THREADS=16
      - SQL_MEMORY_LIMIT_MB=8192
    volumes:
      - ./configs/ferris-high-throughput.yaml:/app/sql-config.yaml
```

### Volume Mounts
```bash
./configs/ferris-default.yaml:/app/sql-config.yaml              # Main configuration
./configs/ferris-low-latency.yaml:/app/sql-config.yaml  # Low latency config
./examples:/app/examples                            # SQL applications
sql-logs:/app/logs                                  # Log persistence
sql-data:/app/data                                  # Data persistence
```

## 💰 Financial Data Examples

### 1. JSON Financial Data with Perfect Precision

```bash
# Create financial trade record with exact precision
curl -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "financial_trades",
    "kafka_topic": "trades",
    "serialization": "json"
  }'

# Execute financial calculations with ScaledInteger precision
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, price, quantity, price * quantity as total FROM trades WHERE price > \"100.00\""
  }'
# Returns: {"symbol":"AAPL","price":"150.2567","quantity":100,"total":"15025.67"}
```

### 2. Avro with Flink-Compatible Decimal Types

```bash
# Note: Schema Registry not yet implemented in FerrisStreams
# Use schema files instead (see Schema Files section below)

# Execute with Avro decimal processing (using schema file)
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, CAST(price AS DECIMAL(18,4)) * quantity as precise_total FROM avro_trades",
    "format": "avro"
  }'
```

### 3. Protobuf with Industry-Standard Decimal Messages

```bash
# Financial data automatically uses Decimal{units, scale} format
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, price, quantity FROM protobuf_positions WHERE price > \"1000.00\"",
    "format": "protobuf"
  }'
# Internally uses: Decimal{units: 10000000, scale: 4} for $1000.00
```

## 📄 Schema Files

FerrisStreams requires schema files for structured data formats (Avro and Protobuf). JSON format works schema-less.

### Required Schema Files by Format

| Format | Schema File Required | File Extension | Description |
|--------|---------------------|----------------|-------------|
| JSON | ❌ No | - | Schema-less, auto-detects types |
| Avro | ✅ Yes | `.avsc` | Avro schema definition |
| Protobuf | ✅ Yes | `.proto` | Protobuf message definition |

### Example Schema Files

#### Financial Trading Avro Schema (`/app/schemas/trades.avsc`)
```json
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.ferrisstreams.financial",
  "fields": [
    {"name": "symbol", "type": "string"},
    {
      "name": "price", 
      "type": "bytes",
      "logicalType": "decimal",
      "precision": 18,
      "scale": 4
    },
    {"name": "quantity", "type": "long"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}}
  ]
}
```

#### Financial Trading Protobuf Schema (`/app/schemas/trades.proto`)
```protobuf
syntax = "proto3";

package ferrisstreams.financial;

import "google/protobuf/timestamp.proto";

message Decimal {
  int64 units = 1;    // Scaled integer value
  int32 scale = 2;    // Decimal places
}

enum Side {
  BUY = 0;
  SELL = 1;
}

message Trade {
  string symbol = 1;
  Decimal price = 2;
  int64 quantity = 3;
  google.protobuf.Timestamp timestamp = 4;
  Side side = 5;
}
```

### Schema File Volume Mounts
```bash
# Mount schema directory in Docker
docker run -d \
  -v $(pwd)/schemas:/app/schemas:ro \
  -v $(pwd)/configs/ferris-default.yaml:/app/sql-config.yaml \
  ferrisstreams:latest
```

### Docker Compose Schema Mount
```yaml
services:
  ferris-streams:
    volumes:
      - ./schemas:/app/schemas:ro          # Schema files (read-only)
      - ./configs/ferris-default.yaml:/app/sql-config.yaml
      - ./examples:/app/examples:ro
```

## 🚀 Usage Examples

### 1. Execute Real-Time Analytics

#### One-Time Query Execution
```bash
# Single SQL query (exits after processing current data)
docker exec ferris-streams ferris-sql execute \
  --query "SELECT customer_id, amount FROM orders WHERE amount > 1000" \
  --topic orders \
  --brokers kafka:29092

# Stream results to stdout (default behavior)
docker exec ferris-streams ferris-sql execute \
  --query "SELECT customer_id, amount, timestamp FROM orders" \
  --topic orders \
  --brokers kafka:29092 \
  --output stdout

# With result limit and pretty printing
docker exec ferris-streams ferris-sql execute \
  --query "SELECT * FROM orders ORDER BY timestamp DESC" \
  --topic orders \
  --brokers kafka:29092 \
  --limit 100 \
  --format json-pretty
```

#### Continuous Streaming Queries
```bash
# Continuous streaming query to stdout (keeps running)
docker exec -it ferris-streams ferris-sql stream \
  --query "SELECT customer_id, amount FROM orders WHERE amount > 1000" \
  --topic orders \
  --brokers kafka:29092 \
  --continuous \
  --output stdout

# Stream with real-time JSON output
docker exec -it ferris-streams ferris-sql stream \
  --query "SELECT customer_id, amount, timestamp FROM orders" \
  --topic orders \
  --brokers kafka:29092 \
  --continuous \
  --format json-lines

# Streaming with output topic and Avro schema
docker exec ferris-streams ferris-sql stream \
  --query "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id" \
  --input-topic orders \
  --output-topic customer_totals \
  --brokers kafka:29092 \
  --format avro \
  --avro-schema /app/schemas/orders.avsc \
  --continuous

# Streaming with windowing (runs continuously)
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      customer_id, 
      COUNT(*) as order_count,
      SUM(amount) as total_amount
    FROM orders 
    WINDOW TUMBLING (INTERVAL 1 MINUTE)
    GROUP BY customer_id
  " \
  --input-topic orders \
  --output-topic customer_metrics \
  --brokers kafka:29092 \
  --continuous \
  --window-size 60
```

#### Multi-Job Application Deployment
```bash
# Deploy multiple streaming jobs from SQL file
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /app/examples/ecommerce_analytics.sql \
  --brokers kafka:29092 \
  --default-topic orders \
  --continuous  # Keep all jobs running

# Stream job results to stdout in JSON format
docker exec -it ferris-streams ferris-sql-multi stream-job \
  --job-id ecommerce_analytics_job_1 \
  --output stdout \
  --format json-lines

# Monitor all job outputs with JSON streaming
docker exec -it ferris-streams ferris-sql-multi stream-all \
  --output stdout \
  --format json \
  --include-metadata

# Execute multi-job query with stdout streaming
docker exec -it ferris-streams ferris-sql-multi execute \
  --query "SELECT job_id, status, result FROM job_results" \
  --output stdout \
  --format json-pretty
```

### 2. IoT Sensor Monitoring

#### One-Time Sensor Data Analysis
```bash
# Analyze current sensor readings (exits after processing)
docker exec ferris-streams ferris-sql execute \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.device_id') as device_id,
      CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') as temp,
      CASE 
        WHEN CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') > 75.0 
        THEN 'HIGH' ELSE 'NORMAL'
      END as alert_level
    FROM sensor_data 
    WHERE JSON_VALUE(payload, '$.sensor_type') = 'temperature'
  " \
  --topic iot_sensors \
  --brokers kafka:29092 \
  --limit 1000
```

#### Continuous IoT Monitoring
```bash
# Real-time temperature monitoring with Avro schema (keeps running)
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      device_id,
      AVG(value) as avg_temp,
      MAX(value) as max_temp,
      COUNT(*) as reading_count
    FROM sensor_data 
    WHERE sensor_type = 'TEMPERATURE'
    WINDOW TUMBLING (INTERVAL 5 MINUTES)
    GROUP BY device_id
  " \
  --input-topic iot_sensors \
  --output-topic temperature_alerts \
  --brokers kafka:29092 \
  --format avro \
  --avro-schema /app/schemas/iot_sensors.avsc \
  --continuous \
  --window-size 300

# Continuous alert generation
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.device_id') as device_id,
      JSON_VALUE(payload, '$.temperature') as temperature,
      timestamp() as alert_time,
      'TEMPERATURE_HIGH' as alert_type
    FROM sensor_data 
    WHERE CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') > 75.0
  " \
  --input-topic iot_sensors \
  --output-topic emergency_alerts \
  --brokers kafka:29092 \
  --continuous
```

### 3. Financial Trading Analytics

#### Real-Time Trading Analysis
```bash
# Continuous trading analysis with Protobuf schema (financial precision)
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      symbol,
      price,
      quantity,
      price * quantity as total_value,
      timestamp
    FROM trades 
    WHERE price > '100.0000'
  " \
  --input-topic trades \
  --output-topic processed_trades \
  --brokers kafka:29092 \
  --format protobuf \
  --proto-file /app/schemas/trades.proto \
  --continuous

# Same query with Avro schema and Flink-compatible decimals
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      symbol,
      price,
      quantity,
      price * quantity as total_value
    FROM trades 
    WHERE price > '100.0000'
  " \
  --input-topic trades \
  --output-topic processed_trades \
  --brokers kafka:29092 \
  --format avro \
  --avro-schema /app/schemas/trades.avsc \
  --continuous

# Moving average calculation (continuous)
docker exec ferris-streams ferris-sql stream \
  --query "
    SELECT 
      symbol,
      AVG(CAST(price AS DECIMAL(18,4))) OVER (
        PARTITION BY symbol 
        ORDER BY timestamp 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      ) as moving_avg_5,
      price,
      timestamp
    FROM trades
  " \
  --input-topic trades \
  --output-topic trade_indicators \
  --brokers kafka:29092 \
  --continuous

# Multi-job financial application deployment
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /app/examples/financial_trading.sql \
  --brokers kafka:29092 \
  --default-topic trades \
  --continuous  # Keep all trading jobs running
```

#### Command Reference

| Command | Purpose | Exit Behavior |
|---------|---------|---------------|
| `ferris-sql execute` | One-time query execution | Exits after processing current data |
| `ferris-sql stream` | Continuous streaming query | Keeps running until stopped |
| `ferris-sql-multi deploy-app` | Deploy multiple jobs | Manages job lifecycle |
| `ferris-sql-multi stream-job` | Stream individual job results to stdout | Keeps running until stopped |
| `ferris-sql-multi stream-all` | Stream all job outputs with metadata | Keeps running until stopped |
| `ferris-sql-multi execute` | Execute query across job results | Exits after processing |

| Flag | Description | Example |
|------|-------------|---------|
| `--continuous` | Keep query running continuously | `--continuous` |
| `--limit N` | Process only N records then exit | `--limit 1000` |
| `--output` | Output destination | `--output stdout\|kafka\|file` |
| `--format` | Output format | `--format json\|json-pretty\|json-lines\|csv\|table` |
| `--window-size N` | Window size in seconds | `--window-size 300` |
| `--input-topic` | Source Kafka topic | `--input-topic orders` |
| `--output-topic` | Destination Kafka topic | `--output-topic results` |
| `--avro-schema` | Avro schema file (required for Avro) | `--avro-schema /app/schemas/orders.avsc` |
| `--proto-file` | Protobuf definition file (required for Protobuf) | `--proto-file /app/schemas/trade.proto` |

#### Output Format Options
| Format | Description | Use Case |
|--------|-------------|----------|
| `json` | Compact JSON output | API integration |
| `json-pretty` | Pretty-printed JSON | Human reading |
| `json-lines` | Newline-delimited JSON | Streaming logs |
| `csv` | Comma-separated values | Spreadsheets |
| `table` | ASCII table format | Terminal viewing |

#### Simple Stdout Streaming Examples
```bash
# Live JSON streaming to stdout (Ctrl+C to stop)
docker exec -it ferris-streams ferris-sql stream \
  --query "SELECT * FROM orders" \
  --topic orders \
  --brokers kafka:29092 \
  --continuous \
  --output stdout \
  --format json-lines

# Pretty table format for terminal viewing
docker exec -it ferris-streams ferris-sql execute \
  --query "SELECT customer_id, amount FROM orders LIMIT 10" \
  --topic orders \
  --brokers kafka:29092 \
  --output stdout \
  --format table

# CSV output for piping to files or other tools
docker exec ferris-streams ferris-sql execute \
  --query "SELECT * FROM orders WHERE amount > 1000" \
  --topic orders \
  --brokers kafka:29092 \
  --output stdout \
  --format csv > high_value_orders.csv

# Multi-job server: Stream all job results in JSON format
docker exec -it ferris-streams ferris-sql-multi stream-all \
  --output stdout \
  --format json-lines \
  --include-metadata
```

## 🔧 Complete Command Templates

### Single SQL Server Commands

#### ferris-sql execute (One-time execution)
```bash
docker exec [-it] ferris-streams ferris-sql execute \
  --query "SQL_QUERY_HERE" \
  --topic KAFKA_TOPIC \
  --brokers KAFKA_BROKERS \
  [--output stdout|kafka|file] \
  [--format json|json-pretty|json-lines|csv|table] \
  [--limit NUMBER] \
  [--avro-schema /path/to/schema.avsc] \
  [--proto-file /path/to/schema.proto] \
  [--timeout SECONDS] \
  [--partition NUMBER] \
  [--offset earliest|latest|NUMBER]
```

#### ferris-sql stream (Continuous streaming)
```bash
docker exec -it ferris-streams ferris-sql stream \
  --query "SQL_QUERY_HERE" \
  --input-topic KAFKA_TOPIC \
  --brokers KAFKA_BROKERS \
  --continuous \
  [--output stdout|kafka|file] \
  [--output-topic OUTPUT_TOPIC] \
  [--format json|json-pretty|json-lines|csv|table] \
  [--window-size SECONDS] \
  [--avro-schema /path/to/schema.avsc] \
  [--proto-file /path/to/schema.proto] \
  [--group-id CONSUMER_GROUP] \
  [--partition NUMBER] \
  [--auto-offset-reset earliest|latest]
```

### Multi-Job Server Commands

#### ferris-sql-multi deploy-app (Deploy multiple jobs)
```bash
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /path/to/sql_file.sql \
  --brokers KAFKA_BROKERS \
  [--default-topic DEFAULT_TOPIC] \
  [--continuous] \
  [--config /path/to/config.yaml] \
  [--job-prefix PREFIX] \
  [--group-prefix GROUP_PREFIX] \
  [--max-jobs NUMBER] \
  [--restart-policy always|never|on-failure]
```

#### ferris-sql-multi stream-job (Stream individual job results)
```bash
docker exec -it ferris-streams ferris-sql-multi stream-job \
  --job-id JOB_ID \
  --output stdout \
  --format json-lines \
  [--include-metadata] \
  [--follow] \
  [--tail NUMBER]
```

#### ferris-sql-multi stream-all (Stream all job outputs)
```bash
docker exec -it ferris-streams ferris-sql-multi stream-all \
  --output stdout \
  --format json|json-lines \
  [--include-metadata] \
  [--filter "job_name=pattern"] \
  [--since TIMESTAMP] \
  [--follow]
```

#### ferris-sql-multi execute (Query job results)
```bash
docker exec ferris-streams ferris-sql-multi execute \
  --query "SELECT job_id, status, result FROM job_results WHERE condition" \
  --output stdout \
  --format json-pretty \
  [--timeout SECONDS] \
  [--limit NUMBER]
```

### Job Management Commands

#### List jobs
```bash
docker exec ferris-streams ferris-sql-multi list-jobs \
  [--status running|paused|stopped|all] \
  [--format json|table] \
  [--sort-by name|status|created|updated]
```

#### Control individual jobs
```bash
# Start/stop/pause/resume job
docker exec ferris-streams ferris-sql-multi {start|stop|pause|resume}-job \
  --job-id JOB_ID

# Get job status
docker exec ferris-streams ferris-sql-multi job-status \
  --job-id JOB_ID \
  --format json|table

# Get job logs
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id JOB_ID \
  [--tail NUMBER] \
  [--follow] \
  [--since TIMESTAMP]
```

### Parameter Reference

| Parameter | Description | Required | Default | Examples |
|-----------|-------------|----------|---------|----------|
| `--query` | SQL query to execute | Yes | - | `"SELECT * FROM orders"` |
| `--topic` | Kafka topic name | Yes | - | `orders`, `trades` |
| `--input-topic` | Source Kafka topic | Yes | - | `raw_data` |
| `--output-topic` | Destination Kafka topic | No | - | `processed_data` |
| `--brokers` | Kafka broker addresses | Yes | - | `kafka:9092`, `localhost:9092` |
| `--output` | Output destination | No | `stdout` | `stdout`, `kafka`, `file` |
| `--format` | Output format | No | `json` | `json`, `json-pretty`, `json-lines`, `csv`, `table` |
| `--continuous` | Keep running | No | false | (flag only) |
| `--limit` | Max records to process | No | unlimited | `1000`, `50` |
| `--window-size` | Window size in seconds | No | - | `60`, `300` |
| `--avro-schema` | Avro schema file path | No | - | `/app/schemas/orders.avsc` |
| `--proto-file` | Protobuf definition file | No | - | `/app/schemas/trades.proto` |
| `--job-id` | Specific job identifier | Context dependent | - | `ecommerce_job_1` |
| `--file` | SQL file with job definitions | Yes (for deploy-app) | - | `/app/sql/jobs.sql` |
| `--timeout` | Operation timeout | No | `30s` | `60`, `120` |
| `--group-id` | Kafka consumer group (single jobs) | No | auto-generated | `my_consumer_group` |
| `--job-prefix` | Prefix for job names | No | `job_` | `analytics_`, `trading_` |
| `--group-prefix` | Prefix for consumer group IDs | No | `ferris_multi_` | `app_`, `analytics_` |
| `--include-metadata` | Include job metadata | No | false | (flag only) |
| `--follow` | Follow/tail mode | No | false | (flag only) |

### Consumer Group Management in Multi-Job Server

The multi-job server handles consumer groups differently than single SQL jobs:

#### How Consumer Groups are Generated
```bash
# With group prefix
--group-prefix "analytics_"
# Results in consumer groups:
# analytics_job_1, analytics_job_2, analytics_job_3

# Default behavior (no prefix specified)
# Results in consumer groups:
# ferris_multi_job_1, ferris_multi_job_2, ferris_multi_job_3
```

#### Examples
```bash
# Deploy jobs with custom group prefix
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /app/trading_jobs.sql \
  --brokers kafka:9092 \
  --job-prefix "trading_" \
  --group-prefix "trading_consumers_"
# Creates: trading_consumers_trading_1, trading_consumers_trading_2, etc.

# Deploy with default prefixes
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /app/jobs.sql \
  --brokers kafka:9092
# Creates: ferris_multi_job_1, ferris_multi_job_2, etc.

# Each job gets its own consumer group for isolation
# This allows independent scaling and offset management per job
```

#### Consumer Group Benefits
- **Isolation**: Each job has independent offset management
- **Scaling**: Jobs can be scaled independently
- **Fault Tolerance**: Job failures don't affect other jobs' consumption
- **Monitoring**: Clear separation for metrics and monitoring per job

## 🚨 Error Handling & Recovery

### Job Failure Detection and Recovery

FerrisStreams provides comprehensive error handling and automatic recovery mechanisms for production deployments.

#### Automatic Recovery Policies

```yaml
# Configuration for automatic recovery
jobs:
  restart_policy: "on-failure"     # always|never|on-failure
  max_restart_attempts: 3          # Maximum restart attempts
  restart_delay_ms: 5000          # Delay between restart attempts
  health_check_interval_ms: 30000  # Health check frequency
  failure_threshold: 5             # Consecutive failures before marking as failed
```

#### Job Failure Scenarios and Recovery

##### 1. Kafka Connection Failures
```bash
# Detect Kafka connectivity issues
curl http://localhost:9091/health/kafka

# Check job status for Kafka errors
docker exec ferris-streams ferris-sql-multi job-status \
  --job-id my_job --format json | jq '.error'

# Manual recovery for Kafka issues
docker exec ferris-streams ferris-sql-multi restart-job --job-id my_job

# Verify connectivity restored
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id my_job --tail 20 --grep "Connected to Kafka"
```

##### 2. Memory Exhaustion Recovery
```bash
# Check memory usage per job
curl http://localhost:9091/metrics/memory | jq '.jobs'

# Identify high memory jobs
docker exec ferris-streams ferris-sql-multi list-jobs \
  --sort-by memory_usage --format table

# Reduce memory limit and restart
# Edit SQL file to lower MEMORY_LIMIT, then redeploy
docker exec ferris-streams ferris-sql-multi stop-job --job-id heavy_job
# Update SQL file: -- MEMORY_LIMIT: 1024 (reduced from 2048)
docker exec ferris-streams ferris-sql-multi deploy-app \
  --file /app/sql/updated_jobs.sql --replace

# Monitor recovery
docker exec ferris-streams ferris-sql-multi job-status --job-id heavy_job
```

##### 3. Schema Evolution Failures
```bash
# Detect schema compatibility issues
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id avro_job --grep "SchemaCompatibilityException"

# Recovery steps for schema issues
# 1. Update schema file
cp /app/schemas/orders_v2.avsc /app/schemas/orders.avsc

# 2. Restart affected job
docker exec ferris-streams ferris-sql-multi restart-job --job-id avro_job

# 3. Verify schema loading
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id avro_job --tail 10 --grep "Schema loaded successfully"
```

##### 4. Query Timeout Recovery
```bash
# Identify timeout issues
curl http://localhost:9091/metrics/queries/slow | jq '.timeouts'

# Check specific job timeouts
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id slow_job --grep "QueryTimeoutException"

# Recovery options:
# Option 1: Increase timeout
# Edit SQL file: -- TIMEOUT: 60000 (increased from 30000)
# Then redeploy

# Option 2: Optimize query
# Simplify complex aggregations or add more specific WHERE clauses
# Then redeploy with optimized query
```

#### Disaster Recovery Procedures

##### Complete Server Recovery
```bash
# 1. Backup current state
docker exec ferris-streams ferris-sql-multi export-state \
  --output /backup/ferris-state-$(date +%Y%m%d-%H%M%S).json

# 2. Stop all jobs gracefully
docker exec ferris-streams ferris-sql-multi stop-all-jobs --graceful

# 3. Restart server container
docker restart ferris-streams

# 4. Restore job state
docker exec ferris-streams ferris-sql-multi import-state \
  --input /backup/ferris-state-latest.json

# 5. Restart jobs
docker exec ferris-streams ferris-sql-multi start-all-jobs
```

##### Partial Recovery (Specific Jobs)
```bash
# 1. Identify failed jobs
docker exec ferris-streams ferris-sql-multi list-jobs --status failed

# 2. Export job configuration
docker exec ferris-streams ferris-sql-multi export-job-config \
  --job-id failed_job --output /backup/failed_job_config.json

# 3. Remove failed job
docker exec ferris-streams ferris-sql-multi remove-job --job-id failed_job

# 4. Redeploy from backup
docker exec ferris-streams ferris-sql-multi import-job-config \
  --input /backup/failed_job_config.json

# 5. Start recovered job
docker exec ferris-streams ferris-sql-multi start-job --job-id failed_job
```

#### Health Check Automation

##### Automated Health Monitoring Script
```bash
#!/bin/bash
# health_monitor.sh - Automated health checking and recovery

FERRIS_CONTAINER="ferris-streams"
HEALTH_ENDPOINT="http://localhost:9091/health"
LOG_FILE="/var/log/ferris-health.log"

check_health() {
    local response=$(curl -s -o /dev/null -w "%{http_code}" $HEALTH_ENDPOINT)
    if [ "$response" != "200" ]; then
        echo "$(date): Health check failed (HTTP $response)" >> $LOG_FILE
        return 1
    fi
    return 0
}

check_jobs() {
    local failed_jobs=$(docker exec $FERRIS_CONTAINER ferris-sql-multi list-jobs --status failed --format json | jq -r '.[].job_id')
    if [ -n "$failed_jobs" ]; then
        echo "$(date): Failed jobs detected: $failed_jobs" >> $LOG_FILE
        for job in $failed_jobs; do
            echo "$(date): Attempting to restart job: $job" >> $LOG_FILE
            docker exec $FERRIS_CONTAINER ferris-sql-multi restart-job --job-id $job
        done
    fi
}

# Main monitoring loop
while true; do
    if ! check_health; then
        echo "$(date): Server unhealthy, checking individual jobs" >> $LOG_FILE
        check_jobs
    fi
    sleep 30
done
```

##### Prometheus Alerting Rules
```yaml
# ferris_alerts.yml
groups:
- name: ferris_streams_alerts
  rules:
  - alert: FerrisJobFailed
    expr: ferris_job_status{status="failed"} > 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "FerrisStreams job {{ $labels.job_id }} has failed"
      description: "Job {{ $labels.job_id }} has been in failed state for more than 1 minute"

  - alert: FerrisHighMemoryUsage
    expr: ferris_job_memory_usage_mb / ferris_job_memory_limit_mb > 0.9
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "FerrisStreams job {{ $labels.job_id }} high memory usage"
      description: "Job {{ $labels.job_id }} is using {{ $value }}% of allocated memory"

  - alert: FerrisKafkaConsumerLag
    expr: ferris_kafka_consumer_lag > 10000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High Kafka consumer lag for job {{ $labels.job_id }}"
      description: "Consumer lag is {{ $value }} messages behind"
```

#### Error Log Analysis

##### Common Error Patterns and Solutions

```bash
# 1. OutOfMemoryError patterns
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id $JOB_ID --grep "OutOfMemoryError|GC overhead limit exceeded"
# Solution: Increase MEMORY_LIMIT or optimize query

# 2. Kafka connection issues
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id $JOB_ID --grep "BrokerNotAvailableException|TimeoutException"
# Solution: Check Kafka connectivity, verify broker addresses

# 3. Schema registry issues
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id $JOB_ID --grep "SchemaNotFoundException|IncompatibleSchemaException"
# Solution: Update schema files, check schema evolution compatibility

# 4. SQL syntax errors
docker exec ferris-streams ferris-sql-multi job-logs \
  --job-id $JOB_ID --grep "SqlParseException|QueryValidationException"
# Solution: Validate SQL syntax, check table/column references
```

##### Recovery Runbook Template
```bash
# FERRIS STREAMS RECOVERY RUNBOOK
# ================================

# STEP 1: Assess Situation
echo "1. Checking overall system health..."
curl http://localhost:9091/health | jq '.'
docker exec ferris-streams ferris-sql-multi list-jobs --status all

# STEP 2: Identify Failed Components
echo "2. Identifying failed jobs..."
FAILED_JOBS=$(docker exec ferris-streams ferris-sql-multi list-jobs --status failed --format json | jq -r '.[].job_id')
echo "Failed jobs: $FAILED_JOBS"

# STEP 3: Gather Diagnostics
echo "3. Gathering diagnostic information..."
for job in $FAILED_JOBS; do
    echo "=== Job: $job ==="
    docker exec ferris-streams ferris-sql-multi job-status --job-id $job
    docker exec ferris-streams ferris-sql-multi job-logs --job-id $job --tail 50
done

# STEP 4: Attempt Automatic Recovery
echo "4. Attempting automatic recovery..."
for job in $FAILED_JOBS; do
    echo "Restarting job: $job"
    docker exec ferris-streams ferris-sql-multi restart-job --job-id $job
    sleep 5
    # Check if restart successful
    STATUS=$(docker exec ferris-streams ferris-sql-multi job-status --job-id $job --format json | jq -r '.status')
    echo "Job $job status after restart: $STATUS"
done

# STEP 5: Manual Intervention (if needed)
echo "5. Manual intervention checklist:"
echo "□ Check Kafka connectivity"  
echo "□ Verify schema files are accessible"
echo "□ Check memory limits vs usage"
echo "□ Review SQL query for optimization"
echo "□ Check disk space and network connectivity"
```

## 📊 Monitoring & Operations

### Comprehensive Performance Monitoring (Phase 2)

```bash
# Real-time query performance
curl http://localhost:9080/metrics/performance | jq

# System health with performance analysis  
curl http://localhost:9080/metrics/health | jq

# Detailed performance report
curl http://localhost:9080/metrics/report

# Query execution statistics
curl http://localhost:9080/metrics/queries/slow | jq

# Prometheus format for external monitoring
curl http://localhost:9080/metrics/prometheus
```

### Health Monitoring
```bash
# Service status with performance metrics
docker-compose ps
kubectl get pods -n ferris-sql

# Enhanced health checks (includes performance validation)
docker inspect ferris-sql-single --format='{{.State.Health.Status}}'
kubectl describe pod <pod-name> -n ferris-sql

# Service logs with performance data
docker-compose logs ferris-sql-single -f
kubectl logs -f deployment/ferris-sql-single -n ferris-sql

# Performance monitoring logs
docker-compose exec ferris-sql-single tail -f /app/logs/performance.log
```

### Metrics & Dashboards
```bash
# FerrisStreams performance dashboard
curl http://localhost:9080/metrics/performance

# Prometheus metrics (includes FerrisStreams metrics)
curl http://localhost:9093/metrics

# Grafana dashboards with FerrisStreams integration
open http://localhost:3000  # admin/ferris123
# - FerrisStreams Performance Dashboard
# - Query Execution Analytics  
# - Hash Join Performance Metrics
# - Memory Usage Analysis

# Kafka UI management
open http://localhost:8090
```

## 🔒 Security & Production

### Comprehensive Security Guide

#### Authentication & Authorization

##### Container Security
```bash
# Run containers with non-root user
docker run -d \
  --user 1001:1001 \
  --read-only \
  --tmpfs /tmp \
  --tmpfs /var/run \
  -p 8080:8080 \
  ferrisstreams:latest

# Use security options
docker run -d \
  --security-opt no-new-privileges \
  --cap-drop ALL \
  --cap-add NET_BIND_SERVICE \
  ferrisstreams:latest
```

##### Network Security
```yaml
# docker-compose.yml with network isolation
version: '3.8'
networks:
  ferris_internal:
    driver: bridge
    internal: true  # No external access
  ferris_external:
    driver: bridge

services:
  ferris-streams:
    networks:
      - ferris_internal
      - ferris_external
    ports:
      - "127.0.0.1:8080:8080"  # Bind to localhost only
    environment:
      - FERRIS_BIND_HOST=0.0.0.0
      - FERRIS_ALLOWED_ORIGINS=https://yourapp.com
```

##### API Security Configuration
```yaml
# security-config.yaml
server:
  # Network binding
  bind_host: "127.0.0.1"      # Localhost only for security
  port: 8080
  
  # Request limits
  max_request_size_mb: 10      # Limit request size
  rate_limit_requests_per_minute: 1000
  
  # Security headers
  security_headers:
    enable: true
    x_frame_options: "DENY"
    x_content_type_options: "nosniff"
    x_xss_protection: "1; mode=block"
    strict_transport_security: "max-age=31536000; includeSubDomains"

# Authentication (when available)
auth:
  enable: false               # Currently not implemented
  type: "jwt"                # Future: JWT token authentication
  jwt_secret_env: "FERRIS_JWT_SECRET"
```

#### Kafka Security

##### SSL/TLS Configuration
```yaml
# kafka-ssl-config.yaml
kafka:
  brokers: "kafka-ssl:9093"
  security_protocol: "SSL"
  
  # SSL settings
  ssl:
    ca_location: "/app/certs/ca-cert.pem"
    certificate_location: "/app/certs/client-cert.pem"
    key_location: "/app/certs/client-key.pem"
    key_password_env: "KAFKA_SSL_KEY_PASSWORD"
    
  # Certificate validation
  ssl_verify_hostname: true
  ssl_check_hostname: true
```

##### SASL Authentication
```yaml
# kafka-sasl-config.yaml  
kafka:
  brokers: "kafka-sasl:9092"
  security_protocol: "SASL_PLAINTEXT"
  
  # SASL configuration
  sasl:
    mechanism: "SCRAM-SHA-512"  # or PLAIN, GSSAPI
    username_env: "KAFKA_SASL_USERNAME"
    password_env: "KAFKA_SASL_PASSWORD"
    
  # Consumer group security
  consumer:
    enable_auto_commit: false  # Manual commit for security
    isolation_level: "read_committed"  # Only committed messages
```

##### Combined SSL + SASL
```yaml
# kafka-ssl-sasl-config.yaml
kafka:
  brokers: "kafka-secure:9094"
  security_protocol: "SASL_SSL"
  
  # SSL configuration
  ssl:
    ca_location: "/app/certs/ca-cert.pem"
    certificate_location: "/app/certs/client-cert.pem"
    key_location: "/app/certs/client-key.pem"
    
  # SASL configuration
  sasl:
    mechanism: "SCRAM-SHA-512"
    username_env: "KAFKA_USERNAME"
    password_env: "KAFKA_PASSWORD"
```

#### Secrets Management

##### Environment Variable Security
```bash
# Use Docker secrets (recommended)
echo "my_kafka_password" | docker secret create kafka_password -

# Reference in docker-compose.yml
services:
  ferris-streams:
    secrets:
      - kafka_password
    environment:
      - KAFKA_PASSWORD_FILE=/run/secrets/kafka_password

secrets:
  kafka_password:
    external: true
```

##### Kubernetes Secrets
```yaml
# k8s-secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: ferris-secrets
type: Opaque
data:
  kafka-username: <base64-encoded-username>
  kafka-password: <base64-encoded-password>
  ssl-ca-cert: <base64-encoded-ca-cert>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ferris-streams
spec:
  template:
    spec:
      containers:
      - name: ferris-streams
        env:
        - name: KAFKA_USERNAME
          valueFrom:
            secretKeyRef:
              name: ferris-secrets
              key: kafka-username
        - name: KAFKA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ferris-secrets
              key: kafka-password
        volumeMounts:
        - name: ssl-certs
          mountPath: /app/certs
          readOnly: true
      volumes:
      - name: ssl-certs
        secret:
          secretName: ferris-secrets
          items:
          - key: ssl-ca-cert
            path: ca-cert.pem
```

##### HashiCorp Vault Integration
```bash
# Example Vault integration (future enhancement)
# Fetch secrets from Vault
vault kv get -field=password secret/ferris/kafka > /tmp/kafka_password
export KAFKA_PASSWORD=$(cat /tmp/kafka_password)
rm /tmp/kafka_password

# Start FerrisStreams with secrets
docker run -d \
  -e KAFKA_PASSWORD="$KAFKA_PASSWORD" \
  -e VAULT_TOKEN="$VAULT_TOKEN" \
  ferrisstreams:latest
```

#### Data Security

##### Schema File Security
```bash
# Secure schema file permissions
chmod 644 /app/schemas/*.avsc        # Read-only for schema files
chmod 600 /app/schemas/private/*.avsc # Restricted access for sensitive schemas
chown ferris:ferris /app/schemas/*   # Proper ownership

# Schema file validation
# Validate schemas before deployment to prevent injection
docker exec ferris-streams ferris-sql-multi validate-schema \
  --file /app/schemas/orders.avsc \
  --security-check
```

##### Data Encryption at Rest
```yaml
# Volume encryption configuration
services:
  ferris-streams:
    volumes:
      - type: volume
        source: ferris-data
        target: /app/data
        volume:
          driver: local
          driver_opts:
            type: "ext4"
            device: "/dev/mapper/encrypted-disk"  # LUKS encrypted device
```

##### SQL Injection Prevention
```sql
-- FerrisStreams automatically prevents SQL injection by:
-- 1. Using parameterized queries internally
-- 2. Validating all SQL syntax before execution
-- 3. Sandboxing query execution environment

-- Safe SQL patterns (automatically validated):
-- JOB: safe_job
-- TOPIC: user_input
START JOB safe_job AS
SELECT user_id, event_type 
FROM user_input 
WHERE event_type = 'click'    -- Literals are safe
  AND user_id > 0;            -- Arithmetic is validated
```

#### Monitoring & Audit Security

##### Security Event Logging
```yaml
# security-logging-config.yaml
logging:
  security_events:
    enable: true
    log_level: "INFO"
    destinations:
      - type: "file"
        path: "/var/log/ferris-security.log"
      - type: "syslog"
        facility: "local1"
      - type: "elasticsearch"
        endpoint: "https://elk.company.com:9200"
        
  # Events to log
  events:
    - authentication_attempts
    - authorization_failures  
    - configuration_changes
    - job_deployments
    - schema_updates
    - admin_operations
```

##### Security Monitoring Queries
```bash
# Monitor for suspicious activity
# 1. Unusual job deployment patterns
curl http://localhost:9091/security/events | jq '.job_deployments | group_by(.user) | map({user: .[0].user, count: length}) | sort_by(.count) | reverse'

# 2. Failed authentication attempts (future feature)
curl http://localhost:9091/security/events | jq '.auth_failures | group_by(.ip) | map({ip: .[0].ip, failures: length}) | map(select(.failures > 10))'

# 3. Configuration changes
curl http://localhost:9091/security/events | jq '.config_changes[] | select(.timestamp > "2024-01-01T00:00:00Z")'
```

##### Prometheus Security Metrics
```yaml
# security-alerts.yml
groups:
- name: ferris_security_alerts
  rules:
  - alert: FerrisSuspiciousActivity
    expr: rate(ferris_failed_operations_total[5m]) > 10
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High rate of failed operations detected"
      
  - alert: FerrisConfigurationChange
    expr: increase(ferris_config_changes_total[1h]) > 0
    for: 0s
    labels:
      severity: info
    annotations:
      summary: "Configuration change detected"
      
  - alert: FerrisUnauthorizedAccess
    expr: ferris_unauthorized_requests_total > 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Unauthorized access attempt detected"
```

#### Production Hardening Checklist

##### Container Hardening
```bash
# ✅ Security Checklist for Production Deployment

# 1. Container Security
□ Run as non-root user (--user 1001:1001)
□ Use read-only filesystem (--read-only)
□ Drop unnecessary capabilities (--cap-drop ALL)
□ Enable security options (--security-opt no-new-privileges)
□ Scan images for vulnerabilities

# 2. Network Security  
□ Bind to localhost only for internal services
□ Use Docker networks for service isolation
□ Enable TLS for all external communications
□ Configure firewall rules
□ Use reverse proxy (nginx/traefik) for external access

# 3. Authentication & Authorization
□ Configure Kafka SASL authentication
□ Enable SSL/TLS for Kafka communication
□ Use secrets management (Docker secrets/K8s secrets/Vault)
□ Rotate credentials regularly
□ Implement API authentication (when available)

# 4. Data Protection
□ Encrypt data at rest (volume encryption)
□ Encrypt data in transit (SSL/TLS)
□ Secure schema file permissions
□ Validate all input data
□ Implement data retention policies

# 5. Monitoring & Auditing
□ Enable security event logging
□ Monitor for suspicious activity
□ Set up security alerts
□ Regular security assessments
□ Implement incident response procedures
```

##### Example Secure Production Deployment
```yaml
# secure-production.yml
version: '3.8'

networks:
  ferris-internal:
    driver: bridge
    internal: true
  ferris-external:
    driver: bridge

services:
  ferris-streams:
    image: ferrisstreams:latest
    user: "1001:1001"
    read_only: true
    security_opt:
      - no-new-privileges
    cap_drop:
      - ALL
    cap_add:
      - NET_BIND_SERVICE
    networks:
      - ferris-internal
      - ferris-external
    ports:
      - "127.0.0.1:8080:8080"  # Localhost only
    environment:
      - KAFKA_BROKERS=kafka-secure:9094
      - KAFKA_SECURITY_PROTOCOL=SASL_SSL
      - KAFKA_USERNAME_FILE=/run/secrets/kafka_username
      - KAFKA_PASSWORD_FILE=/run/secrets/kafka_password
    volumes:
      - type: tmpfs
        target: /tmp
        tmpfs:
          noexec: true
          nosuid: true
      - type: bind
        source: ./certs
        target: /app/certs
        read_only: true
      - type: bind
        source: ./secure-config.yaml
        target: /app/config.yaml
        read_only: true
    secrets:
      - kafka_username
      - kafka_password
    logging:
      driver: "syslog"
      options:
        syslog-address: "tcp://log-server:514"
        syslog-facility: "local1"

secrets:
  kafka_username:
    external: true
  kafka_password:
    external: true
```

### Security Features ✅
- **Non-root containers** with dedicated user accounts
- **Resource limits** to prevent resource exhaustion
- **Health checks** for service reliability
- **Network isolation** with custom Docker networks
- **Configuration externalization** via ConfigMaps/volumes

### Production Considerations
- **Persistent storage** for Kafka data and logs
- **Resource limits** for memory and CPU
- **Horizontal scaling** via replica configuration
- **Load balancing** through Kubernetes services
- **Monitoring integration** with Prometheus/Grafana

## 🎯 Real-World Use Cases

### 1. E-commerce Analytics
- **Order Processing**: Real-time order analysis and fraud detection
- **Customer Behavior**: Shopping pattern analysis and recommendations
- **Inventory Management**: Stock level monitoring and alerts

### 2. IoT Monitoring
- **Sensor Data**: Temperature, pressure, humidity monitoring
- **Anomaly Detection**: Real-time alerting on sensor thresholds  
- **Predictive Maintenance**: Equipment failure prediction

### 3. Financial Services
- **Trading Analytics**: Real-time trade analysis and risk management
- **Fraud Detection**: Transaction pattern analysis
- **Compliance Monitoring**: Regulatory reporting and alerts

### 4. Social Media Analytics
- **Content Analysis**: Real-time content processing and moderation
- **Engagement Metrics**: User interaction analysis
- **Trend Detection**: Viral content identification

## 🚀 Scaling & Performance

### Horizontal Scaling
```bash
# Docker Compose scaling
docker-compose up -d --scale ferris-sql-single=3

# Kubernetes scaling  
kubectl scale deployment/ferris-sql-single --replicas=5 -n ferris-sql
```

### Performance Optimization
```bash
# Increase job concurrency
SQL_MAX_JOBS=50

# Memory optimization
SQL_MEMORY_LIMIT_MB=4096
SQL_WORKER_THREADS=8

# Kafka optimization
KAFKA_NUM_PARTITIONS=12
KAFKA_REPLICATION_FACTOR=3
```

## 📚 Documentation

### Deployment Guides
- **[Docker Deployment](docs/DOCKER_DEPLOYMENT_GUIDE.md)** - Complete Docker setup
- **[SQL Reference](docs/SQL_REFERENCE_GUIDE.md)** - SQL syntax and functions
- **[Multi-Job Guide](docs/MULTI_JOB_SQL_GUIDE.md)** - Job management patterns

### Configuration References
- **[SQL Configuration](configs/ferris-default.yaml)** - Service configuration
- **[Docker Compose](docker-compose.yml)** - Infrastructure definition
- **[Kubernetes Manifests](k8s/)** - K8s deployment files

### Example Applications
- **[E-commerce Analytics](examples/ecommerce_analytics.sql)**
- **[IoT Monitoring](examples/iot_monitoring.sql)**
- **[Financial Trading](examples/financial_trading.sql)**
- **[Social Media Analytics](examples/social_media_analytics.sql)**

## 🎉 Success Metrics

### ✅ Infrastructure Ready
- Docker images built and optimized
- Docker Compose stack configured
- Kubernetes manifests created
- Monitoring stack integrated
- Automated deployment scripts

### ✅ Production Features
- Health checks and readiness probes
- Resource limits and constraints
- Persistent storage configuration
- Security best practices
- Horizontal scaling support

### ✅ Operational Excellence
- Comprehensive logging
- Metrics collection
- Dashboard visualization
- Automated deployment
- Documentation complete

## 🚀 Quick Start Commands

```bash
# 1. Deploy complete multi-format stack (RECOMMENDED)
git clone <repository> && cd ferrisstreams
docker-compose up --build

# 2. Test financial precision with JSON
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, price, quantity, price * quantity as total FROM json_financial_stream"
  }'

# 3. Test Flink-compatible Avro decimals (uses schema file)
# Schema Registry not yet implemented - use schema files as documented above

# 4. Monitor all formats via Kafka UI
open http://localhost:8085

# 5. Check serialization compatibility
docker exec ferrisstreams cargo run --bin test_serialization_compatibility --features avro,protobuf

# 6. Deploy SQL file application with all formats
echo "START JOB financial_analytics AS SELECT symbol, CAST(price AS DECIMAL(18,4)) * quantity as total FROM trades;" > my-app.sql
docker build -f Dockerfile.sqlfile -t ferrisstreams:sqlfile .
docker run -d -p 8080:8080 -v $(pwd)/my-app.sql:/app/sql-files/app.sql -e SQL_FILE=/app/sql-files/app.sql ferrisstreams:sqlfile
```

## 📈 Performance Results (Financial Precision + Phase 2)

```bash
# Financial Arithmetic Performance:
# - ScaledInteger Operations: 42x faster than f64 arithmetic
# - Perfect Precision: No floating-point rounding errors
# - Cross-System Compatibility: Standard JSON/Avro/Protobuf formats

# Query Performance Benchmarks:
# - SELECT Operations: ~67,361 records/sec average
# - GROUP BY Operations: 64.5µs per record average  
# - Large JOIN Operations: 10x+ performance improvement
# - Window Functions: ~19.6µs per record average
# - Memory Usage: 60% reduction for JOIN operations
```

## ⚡ Performance Tuning Deep Dive

### Comprehensive Performance Optimization Guide

#### Kafka Consumer Performance Tuning

##### High Throughput Configuration
```yaml
# kafka-high-throughput-config.yaml
kafka:
  brokers: "kafka:9092"
  
  # Consumer performance settings
  consumer:
    # Fetch settings for high throughput
    fetch_min_bytes: 100000          # Wait for 100KB before returning
    fetch_max_wait_ms: 100           # Max 100ms wait time
    max_poll_records: 10000          # Large batch sizes
    max_partition_fetch_bytes: 10485760  # 10MB per partition
    
    # Buffer settings
    receive_buffer_bytes: 262144     # 256KB receive buffer
    send_buffer_bytes: 131072        # 128KB send buffer
    
    # Processing optimization
    enable_auto_commit: false        # Manual commit for batching
    auto_commit_interval_ms: 1000    # Commit every second
    session_timeout_ms: 30000        # Longer session timeout
    heartbeat_interval_ms: 10000     # Less frequent heartbeats

  # Producer performance (for output)
  producer:
    batch_size: 65536               # 64KB batches
    linger_ms: 10                   # 10ms batching delay
    buffer_memory: 268435456        # 256MB buffer
    compression_type: "lz4"         # Fast compression
    acks: "1"                       # Balance between speed and durability
```

##### Low Latency Configuration
```yaml
# kafka-low-latency-config.yaml
kafka:
  brokers: "kafka:9092"
  
  # Consumer settings for low latency
  consumer:
    fetch_min_bytes: 1              # Don't wait for batches
    fetch_max_wait_ms: 1            # Minimal wait time
    max_poll_records: 100           # Small batches for speed
    max_partition_fetch_bytes: 1048576  # 1MB limit
    
    # Network optimization
    receive_buffer_bytes: 65536     # Smaller buffers for speed
    send_buffer_bytes: 65536
    
    # Immediate processing
    enable_auto_commit: true        # Auto-commit for speed
    auto_commit_interval_ms: 100    # Frequent commits
    session_timeout_ms: 6000        # Fast failure detection
    heartbeat_interval_ms: 2000     # Frequent heartbeats

  # Producer settings for low latency
  producer:
    batch_size: 1                   # Send immediately
    linger_ms: 0                    # No batching delay
    buffer_memory: 33554432         # 32MB buffer
    compression_type: "none"        # No compression overhead
    acks: "1"                       # Fast acknowledgment
```

#### SQL Engine Performance Optimization

##### Memory Management Strategies
```yaml
# memory-optimized-config.yaml
sql:
  # Memory allocation
  max_memory_mb: 16384              # 16GB total memory
  worker_threads: 32                # More threads for parallelism
  
  # Query-specific memory limits
  query_memory_limit_mb: 2048       # 2GB per query
  join_memory_limit_mb: 4096        # 4GB for JOIN operations
  aggregation_memory_limit_mb: 1024 # 1GB for aggregations
  
  # Memory optimization settings
  memory_pool_size: 8192            # Pre-allocated memory pool
  gc_threshold_mb: 12288            # GC when reaching 12GB
  memory_monitoring_interval_ms: 5000  # Monitor every 5 seconds
  
  # Spill-to-disk settings (when memory exceeded)
  enable_spill_to_disk: true        # Enable disk spilling
  spill_directory: "/tmp/ferris-spill"
  spill_threshold_ratio: 0.8        # Spill at 80% memory usage
```

##### Query Execution Optimization
```yaml
# query-optimization-config.yaml
sql:
  # Execution optimization
  query_optimizer: "advanced"       # Use advanced query optimizer
  enable_predicate_pushdown: true   # Push filters down to source
  enable_projection_pushdown: true  # Push column selection down
  
  # JOIN optimization
  join_strategy: "auto"             # Auto-select best JOIN algorithm
  hash_join_threshold: 100000       # Use hash JOIN for >100K records
  broadcast_join_threshold: 1000000 # Broadcast JOIN for <1M records
  
  # Aggregation optimization
  pre_aggregation_enabled: true     # Pre-aggregate when possible
  aggregation_buffer_size: 10000    # Buffer size for aggregations
  
  # Window function optimization
  window_buffer_size: 50000         # Window state buffer size
  enable_window_spilling: true      # Spill window state to disk
  
  # Parallelization
  enable_parallel_execution: true   # Parallel query execution
  max_parallel_tasks: 16            # Max parallel tasks per query
```

#### System-Level Performance Tuning

##### Container Resource Optimization
```bash
# High-performance container deployment
docker run -d \
  --cpus="8.0" \
  --memory="16g" \
  --memory-swappiness=10 \
  --oom-kill-disable \
  --ulimit nofile=65536:65536 \
  --ulimit nproc=32768:32768 \
  -e JAVA_OPTS="-Xmx12g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=20" \
  ferrisstreams:latest
```

##### Kubernetes Resource Optimization
```yaml
# performance-optimized-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ferris-streams-performance
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: ferris-streams
        resources:
          requests:
            cpu: "4000m"      # 4 CPU cores
            memory: "8Gi"     # 8GB memory
          limits:
            cpu: "8000m"      # 8 CPU cores max
            memory: "16Gi"    # 16GB memory max
        env:
        - name: FERRIS_PERFORMANCE_PROFILE
          value: "high_throughput"
        - name: SQL_WORKER_THREADS
          value: "16"
        - name: SQL_MEMORY_LIMIT_MB
          value: "12288"
      
      # Node affinity for performance
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: performance
                operator: In
                values: ["high"]
      
      # Pod anti-affinity for distribution
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values: ["ferris-streams"]
            topologyKey: kubernetes.io/hostname
```

#### Network Performance Optimization

##### TCP Tuning for High Throughput
```bash
# Network performance tuning script
#!/bin/bash
# network-tuning.sh

# TCP buffer sizes
echo 'net.core.rmem_max = 268435456' >> /etc/sysctl.conf          # 256MB
echo 'net.core.wmem_max = 268435456' >> /etc/sysctl.conf          # 256MB
echo 'net.core.rmem_default = 33554432' >> /etc/sysctl.conf       # 32MB
echo 'net.core.wmem_default = 33554432' >> /etc/sysctl.conf       # 32MB

# TCP window scaling
echo 'net.ipv4.tcp_window_scaling = 1' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 8192 873800 268435456' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 873800 268435456' >> /etc/sysctl.conf

# Connection handling
echo 'net.core.netdev_max_backlog = 10000' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_max_syn_backlog = 10000' >> /etc/sysctl.conf
echo 'net.core.somaxconn = 10000' >> /etc/sysctl.conf

# Apply settings
sysctl -p
```

##### Load Balancer Configuration
```yaml
# nginx-performance.conf
upstream ferris_streams {
    least_conn;
    server ferris-1:8080 max_fails=3 fail_timeout=30s;
    server ferris-2:8080 max_fails=3 fail_timeout=30s;
    server ferris-3:8080 max_fails=3 fail_timeout=30s;
    
    # Connection pooling
    keepalive 32;
}

server {
    listen 80;
    
    # Performance optimizations
    client_body_buffer_size 1M;
    client_max_body_size 10M;
    proxy_buffering on;
    proxy_buffer_size 8k;
    proxy_buffers 32 8k;
    proxy_busy_buffers_size 16k;
    
    # Timeouts
    proxy_connect_timeout 60s;
    proxy_send_timeout 60s;
    proxy_read_timeout 60s;
    
    # Compression
    gzip on;
    gzip_comp_level 6;
    gzip_types application/json text/plain;
    
    location / {
        proxy_pass http://ferris_streams;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

#### Storage Performance Optimization

##### SSD/NVMe Configuration
```yaml
# storage-optimized.yml
services:
  ferris-streams:
    volumes:
      # Use high-performance storage
      - type: bind
        source: /mnt/nvme/ferris-data    # NVMe SSD mount
        target: /app/data
      - type: bind
        source: /mnt/nvme/ferris-logs    # Separate NVMe for logs
        target: /app/logs
      - type: tmpfs
        target: /tmp
        tmpfs:
          size: 2G                       # 2GB RAM disk for temp data
```

##### Database/State Store Optimization
```yaml
# state-optimization-config.yaml
storage:
  # State management
  state_backend: "rocksdb"             # High-performance state backend
  checkpoint_interval_ms: 30000        # Checkpoint every 30 seconds
  
  # RocksDB optimization
  rocksdb:
    block_cache_size_mb: 1024          # 1GB block cache
    write_buffer_size_mb: 256          # 256MB write buffer
    max_write_buffer_number: 3         # 3 write buffers
    compression_type: "lz4"            # Fast compression
    
    # Compaction settings
    level0_file_num_compaction_trigger: 4
    max_bytes_for_level_base: 268435456  # 256MB
    target_file_size_base: 67108864      # 64MB
```

#### Performance Monitoring and Analysis

##### Real-time Performance Metrics
```bash
# Performance monitoring script
#!/bin/bash
# performance-monitor.sh

FERRIS_HOST="localhost:9091"

while true; do
    echo "=== $(date) ==="
    
    # Query performance metrics
    echo "Query Performance:"
    curl -s $FERRIS_HOST/metrics/queries/performance | jq '{
        avg_query_time: .avg_execution_time_ms,
        queries_per_second: .queries_per_second,
        slow_queries: .slow_queries_count
    }'
    
    # Memory usage
    echo "Memory Usage:"
    curl -s $FERRIS_HOST/metrics/memory | jq '{
        total_memory_mb: .total_memory_mb,
        used_memory_mb: .used_memory_mb,
        memory_utilization: .memory_utilization_percent
    }'
    
    # Kafka performance
    echo "Kafka Performance:"
    curl -s $FERRIS_HOST/metrics/kafka | jq '{
        records_per_second: .records_consumed_per_second,
        consumer_lag: .total_consumer_lag,
        avg_fetch_time_ms: .avg_fetch_time_ms
    }'
    
    echo "================================"
    sleep 10
done
```

##### Performance Benchmarking
```bash
# Comprehensive performance benchmark
#!/bin/bash
# benchmark.sh

FERRIS_CONTAINER="ferris-streams"

echo "Running FerrisStreams Performance Benchmark"
echo "============================================"

# 1. Query Performance Test
echo "1. Testing query performance..."
docker exec $FERRIS_CONTAINER ferris-sql-multi benchmark \
    --test-type "query_performance" \
    --duration 60 \
    --concurrent-queries 10 \
    --data-size "1M"

# 2. Throughput Test
echo "2. Testing throughput..."
docker exec $FERRIS_CONTAINER ferris-sql-multi benchmark \
    --test-type "throughput" \
    --duration 120 \
    --target-rps 50000 \
    --batch-size 1000

# 3. Memory Stress Test
echo "3. Testing memory usage..."
docker exec $FERRIS_CONTAINER ferris-sql-multi benchmark \
    --test-type "memory_stress" \
    --memory-limit "8G" \
    --concurrent-jobs 20

# 4. Latency Test
echo "4. Testing latency..."
docker exec $FERRIS_CONTAINER ferris-sql-multi benchmark \
    --test-type "latency" \
    --duration 60 \
    --percentiles "50,95,99"

# 5. Scalability Test
echo "5. Testing scalability..."
for jobs in 1 5 10 20 50; do
    echo "Testing with $jobs concurrent jobs..."
    docker exec $FERRIS_CONTAINER ferris-sql-multi benchmark \
        --test-type "scalability" \
        --concurrent-jobs $jobs \
        --duration 30
done
```

#### Performance Tuning Profiles

##### Ultra High Throughput Profile
```yaml
# ultra-high-throughput.yaml
kafka:
  consumer:
    fetch_min_bytes: 1048576         # 1MB minimum fetch
    max_poll_records: 50000          # Very large batches
    max_partition_fetch_bytes: 52428800  # 50MB per partition
    receive_buffer_bytes: 1048576    # 1MB receive buffer

sql:
  worker_threads: 64               # Many worker threads
  max_memory_mb: 32768            # 32GB memory
  batch_size: 10000               # Large processing batches
  enable_parallel_execution: true
  max_parallel_tasks: 32

performance:
  buffer_size: 100000             # Very large buffers
  flush_interval_ms: 1000         # Batch outputs
  enable_compression: true        # Save network bandwidth
```

##### Ultra Low Latency Profile
```yaml
# ultra-low-latency.yaml
kafka:
  consumer:
    fetch_min_bytes: 1              # Process immediately
    fetch_max_wait_ms: 0            # No waiting
    max_poll_records: 10            # Tiny batches
    
sql:
  worker_threads: 16              # Dedicated threads
  max_memory_mb: 8192             # Sufficient memory
  query_timeout_ms: 1000          # Fast timeout
  
performance:
  buffer_size: 10                 # Minimal buffering
  batch_size: 1                   # Process individually  
  flush_interval_ms: 1            # Immediate flushing
  enable_compression: false       # No compression overhead
```

##### Balanced Production Profile  
```yaml
# balanced-production.yaml
kafka:
  consumer:
    fetch_min_bytes: 50000          # 50KB balanced fetch
    max_poll_records: 1000          # Reasonable batch size
    max_partition_fetch_bytes: 5242880  # 5MB per partition
    
sql:
  worker_threads: 16              # Balanced thread count
  max_memory_mb: 16384           # 16GB memory
  query_timeout_ms: 30000        # 30 second timeout
  
performance:
  buffer_size: 1000              # Balanced buffering
  batch_size: 100                # Moderate batching
  flush_interval_ms: 100         # 100ms flush interval
  enable_compression: true       # Balanced compression
```

---

## 🔗 Integration Patterns

### CI/CD Pipeline Integration

#### Jenkins Pipeline Example
```groovy
// Jenkinsfile
pipeline {
    agent any
    stages {
        stage('Build FerrisStreams Image') {
            steps {
                script {
                    def image = docker.build("ferrisstreams:${env.BUILD_ID}")
                }
            }
        }
        stage('Test SQL Queries') {
            steps {
                script {
                    docker.image("ferrisstreams:${env.BUILD_ID}").inside {
                        sh '''
                        # Test basic SQL functionality
                        /usr/local/bin/ferris-sql execute \
                          --query "SELECT COUNT(*) FROM test_topic LIMIT 1" \
                          --brokers kafka:9092 \
                          --topic test_topic \
                          --format json \
                          --timeout 30s
                        '''
                    }
                }
            }
        }
        stage('Deploy to Staging') {
            when { branch 'develop' }
            steps {
                sh '''
                docker tag ferrisstreams:${BUILD_ID} ferrisstreams:staging
                kubectl set image deployment/ferrisstreams-staging \
                  ferrisstreams=ferrisstreams:staging
                '''
            }
        }
        stage('Deploy to Production') {
            when { branch 'main' }
            steps {
                input 'Deploy to production?'
                sh '''
                docker tag ferrisstreams:${BUILD_ID} ferrisstreams:production
                kubectl set image deployment/ferrisstreams-prod \
                  ferrisstreams=ferrisstreams:production
                '''
            }
        }
    }
    post {
        always {
            sh 'docker system prune -f'
        }
    }
}
```

#### GitHub Actions Workflow
```yaml
# .github/workflows/deploy.yml
name: FerrisStreams Deployment
on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      kafka:
        image: confluentinc/cp-kafka:7.4.0
        env:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      zookeeper:
        image: confluentinc/cp-zookeeper:7.4.0
        env:
          ZOOKEEPER_CLIENT_PORT: 2181
    steps:
    - uses: actions/checkout@v3
    
    - name: Build FerrisStreams
      run: |
        docker build -t ferrisstreams:test .
        
    - name: Run SQL Tests
      run: |
        docker run --network host ferrisstreams:test \
          /usr/local/bin/ferris-sql execute \
          --query "SELECT 1 as test_value LIMIT 1" \
          --brokers localhost:9092 \
          --topic test \
          --timeout 60s
          
    - name: Deploy to Staging
      if: github.ref == 'refs/heads/develop'
      run: |
        echo "${{ secrets.KUBE_CONFIG }}" | base64 -d > kubeconfig
        export KUBECONFIG=kubeconfig
        kubectl set image deployment/ferrisstreams-staging \
          ferrisstreams=ferrisstreams:${{ github.sha }}
```

#### GitLab CI/CD Pipeline
```yaml
# .gitlab-ci.yml
stages:
  - build
  - test
  - deploy

variables:
  DOCKER_REGISTRY: registry.gitlab.com/yourorg/ferrisstreams
  KUBERNETES_NAMESPACE: ferrisstreams

build:
  stage: build
  script:
    - docker build -t $DOCKER_REGISTRY:$CI_COMMIT_SHA .
    - docker push $DOCKER_REGISTRY:$CI_COMMIT_SHA

test-sql:
  stage: test
  services:
    - name: confluentinc/cp-kafka:7.4.0
      alias: kafka
  script:
    - docker run --network container:kafka \
        $DOCKER_REGISTRY:$CI_COMMIT_SHA \
        /usr/local/bin/ferris-sql execute \
        --query "SELECT COUNT(*) as records FROM test_stream LIMIT 1" \
        --brokers kafka:9092 \
        --topic test_stream \
        --timeout 30s

deploy-staging:
  stage: deploy
  script:
    - kubectl config use-context staging
    - kubectl set image deployment/ferrisstreams \
        ferrisstreams=$DOCKER_REGISTRY:$CI_COMMIT_SHA \
        -n $KUBERNETES_NAMESPACE-staging
  only:
    - develop

deploy-production:
  stage: deploy
  script:
    - kubectl config use-context production  
    - kubectl set image deployment/ferrisstreams \
        ferrisstreams=$DOCKER_REGISTRY:$CI_COMMIT_SHA \
        -n $KUBERNETES_NAMESPACE-prod
  when: manual
  only:
    - main
```

### Monitoring System Integration

#### Prometheus Configuration
```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "ferrisstreams_rules.yml"

scrape_configs:
  - job_name: 'ferrisstreams'
    static_configs:
      - targets: ['ferrisstreams:8080']
    metrics_path: /metrics
    scrape_interval: 5s
    scrape_timeout: 5s
    
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9999']
    metrics_path: /metrics
    
alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

#### FerrisStreams Prometheus Rules
```yaml
# ferrisstreams_rules.yml
groups:
- name: ferrisstreams.rules
  rules:
  - alert: FerrisStreamsHighMemoryUsage
    expr: ferrisstreams_memory_usage_bytes > 8e9  # 8GB
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "FerrisStreams high memory usage"
      description: "Memory usage is {{ $value | humanize1024 }}"
      
  - alert: FerrisStreamsHighQueryLatency
    expr: ferrisstreams_query_duration_seconds > 5.0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "FerrisStreams high query latency"
      description: "Query latency is {{ $value }}s"
      
  - alert: FerrisStreamsConsumerLag
    expr: ferrisstreams_kafka_consumer_lag > 1000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "FerrisStreams consumer lag"
      description: "Consumer lag is {{ $value }} messages"
      
  - alert: FerrisStreamsDown
    expr: up{job="ferrisstreams"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "FerrisStreams is down"
      description: "FerrisStreams has been down for more than 1 minute"
```

#### Grafana Dashboard Configuration
```json
{
  "dashboard": {
    "title": "FerrisStreams SQL Monitoring",
    "panels": [
      {
        "title": "Query Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(ferrisstreams_queries_total[5m])",
            "legendFormat": "Queries/sec"
          }
        ]
      },
      {
        "title": "Memory Usage",
        "type": "graph", 
        "targets": [
          {
            "expr": "ferrisstreams_memory_usage_bytes",
            "legendFormat": "Memory Usage"
          }
        ]
      },
      {
        "title": "Kafka Consumer Lag",
        "type": "graph",
        "targets": [
          {
            "expr": "ferrisstreams_kafka_consumer_lag",
            "legendFormat": "Consumer Lag"
          }
        ]
      },
      {
        "title": "Active SQL Jobs",
        "type": "stat",
        "targets": [
          {
            "expr": "ferrisstreams_active_jobs",
            "legendFormat": "Active Jobs"
          }
        ]
      }
    ]
  }
}
```

### External System Integration

#### Apache Flink Integration
```yaml
# flink-ferrisstreams-bridge.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-ferrisstreams-config
data:
  schema-compatibility.yaml: |
    # Shared Avro schemas between Flink and FerrisStreams
    schemas:
      financial_transaction:
        type: "record"
        name: "Transaction"
        fields:
          - name: "amount"
            type: 
              type: "bytes"
              logicalType: "decimal"
              precision: 18
              scale: 4
          - name: "currency"
            type: "string"
          - name: "timestamp"
            type: "long"
            logicalType: "timestamp-millis"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-bridge
spec:
  replicas: 1
  selector:
    matchLabels:
      app: schema-bridge
  template:
    spec:
      containers:
      - name: bridge
        image: schema-compatibility-bridge:latest
        env:
        - name: FLINK_CHECKPOINT_DIR
          value: "s3://checkpoints/flink"
        - name: FERRISSTREAMS_CONFIG
          value: "/config/ferrisstreams.yaml"
        volumeMounts:
        - name: config
          mountPath: /config
```

#### Kafka Connect Integration
```json
{
  "name": "ferrisstreams-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "topics": "ferrisstreams_output",
    "connection.url": "jdbc:postgresql://postgres:5432/analytics",
    "connection.user": "ferrisstreams",
    "connection.password": "${securepass:postgres_password}",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "table.name.format": "ferrisstreams_${topic}",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

#### Elasticsearch Integration  
```yaml
# elasticsearch-integration.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: elasticsearch-pipeline
data:
  ferrisstreams-pipeline.conf: |
    input {
      kafka {
        bootstrap_servers => "kafka:9092"
        topics => ["ferrisstreams_logs", "ferrisstreams_metrics"]
        codec => "json"
        group_id => "elasticsearch_indexer"
      }
    }
    
    filter {
      if [source] == "ferrisstreams" {
        mutate {
          add_field => { "[@metadata][index]" => "ferrisstreams-logs-%{+YYYY.MM.dd}" }
        }
      }
      
      if [type] == "metrics" {
        mutate {
          add_field => { "[@metadata][index]" => "ferrisstreams-metrics-%{+YYYY.MM}" }
        }
      }
    }
    
    output {
      elasticsearch {
        hosts => ["elasticsearch:9200"]
        index => "%{[@metadata][index]}"
        template_name => "ferrisstreams"
        template => "/usr/share/logstash/templates/ferrisstreams.json"
        template_overwrite => true
      }
    }
```

### DevOps Integration Workflows

#### Infrastructure as Code (Terraform)
```hcl
# ferrisstreams.tf
resource "kubernetes_namespace" "ferrisstreams" {
  metadata {
    name = "ferrisstreams-${var.environment}"
    
    labels = {
      name = "ferrisstreams"
      environment = var.environment
    }
  }
}

resource "kubernetes_deployment" "ferrisstreams" {
  metadata {
    name      = "ferrisstreams"
    namespace = kubernetes_namespace.ferrisstreams.metadata[0].name
  }
  
  spec {
    replicas = var.replica_count
    
    selector {
      match_labels = {
        app = "ferrisstreams"
      }
    }
    
    template {
      metadata {
        labels = {
          app = "ferrisstreams"
          version = var.ferrisstreams_version
        }
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }
      
      spec {
        container {
          name  = "ferrisstreams"
          image = "ferrisstreams:${var.ferrisstreams_version}"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name = "KAFKA_BROKERS"
            value = var.kafka_brokers
          }
          
          env {
            name = "LOG_LEVEL"
            value = var.log_level
          }
          
          resources {
            requests = {
              memory = "512Mi"
              cpu    = "500m"
            }
            limits = {
              memory = "2Gi"
              cpu    = "2000m"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/ready"
              port = "http"
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
          
          volume_mount {
            name       = "config"
            mount_path = "/config"
          }
        }
        
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.ferrisstreams.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "ferrisstreams" {
  metadata {
    name      = "ferrisstreams-service"
    namespace = kubernetes_namespace.ferrisstreams.metadata[0].name
  }
  
  spec {
    selector = {
      app = "ferrisstreams"
    }
    
    port {
      name        = "http"
      port        = 80
      target_port = 8080
    }
    
    type = "ClusterIP"
  }
}

resource "kubernetes_config_map" "ferrisstreams" {
  metadata {
    name      = "ferrisstreams-config"
    namespace = kubernetes_namespace.ferrisstreams.metadata[0].name
  }
  
  data = {
    "config.yaml" = templatefile("${path.module}/templates/config.yaml.tpl", {
      kafka_brokers    = var.kafka_brokers
      environment     = var.environment
      replica_count   = var.replica_count
      worker_threads  = var.worker_threads
      max_memory_mb   = var.max_memory_mb
    })
  }
}
```

#### Helm Chart Values
```yaml
# values.yaml
replicaCount: 3

image:
  repository: ferrisstreams
  pullPolicy: IfNotPresent
  tag: "latest"

service:
  type: ClusterIP
  port: 80
  targetPort: 8080

ingress:
  enabled: true
  className: "nginx"
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
  hosts:
    - host: ferrisstreams.yourdomain.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: ferrisstreams-tls
      hosts:
        - ferrisstreams.yourdomain.com

resources:
  limits:
    cpu: 2000m
    memory: 4Gi
  requests:
    cpu: 500m
    memory: 512Mi

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 20
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80

config:
  kafka:
    brokers: "kafka:9092"
    security_protocol: "SASL_SSL"
    sasl_mechanisms: "PLAIN"
  
  sql:
    worker_threads: 16
    max_memory_mb: 2048
    query_timeout_seconds: 300
  
  performance:
    buffer_size: 10000
    batch_size: 1000
    flush_interval_ms: 100

monitoring:
  prometheus:
    enabled: true
    port: 8080
    path: /metrics
  
  grafana:
    enabled: true
    dashboard_config_map: ferrisstreams-dashboard
```

#### ArgoCD Application Configuration
```yaml
# argocd-application.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ferrisstreams
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "2"
spec:
  project: default
  
  source:
    repoURL: https://github.com/yourorg/ferrisstreams-helm
    targetRevision: HEAD
    path: charts/ferrisstreams
    helm:
      valueFiles:
      - values-production.yaml
      parameters:
      - name: image.tag
        value: "v1.2.3"
      - name: replicaCount
        value: "5"
  
  destination:
    server: https://kubernetes.default.svc
    namespace: ferrisstreams-prod
  
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
      allowEmpty: false
    syncOptions:
    - CreateNamespace=true
    - PrunePropagationPolicy=foreground
    - PruneLast=true
    
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m0s
  
  revisionHistoryLimit: 3
```

### Alerting Integration

#### PagerDuty Integration
```yaml
# pagerduty-alerts.yaml
apiVersion: v1
kind: Secret
metadata:
  name: pagerduty-config
  namespace: monitoring
data:
  integration_key: <base64-encoded-pagerduty-key>
---
apiVersion: monitoring.coreos.com/v1alpha1
kind: AlertmanagerConfig  
metadata:
  name: ferrisstreams-pagerduty
  namespace: monitoring
spec:
  route:
    groupBy: ['alertname', 'instance']
    groupWait: 10s
    groupInterval: 10s
    repeatInterval: 1h
    receiver: 'pagerduty-critical'
    routes:
    - match:
        severity: warning
      receiver: 'pagerduty-warning'
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
  
  receivers:
  - name: 'pagerduty-critical'
    pagerdutyConfigs:
    - routingKey:
        key: integration_key
        name: pagerduty-config
      description: 'Critical FerrisStreams Alert: {{ .GroupLabels.alertname }}'
      severity: 'critical'
      
  - name: 'pagerduty-warning'  
    pagerdutyConfigs:
    - routingKey:
        key: integration_key
        name: pagerduty-config
      description: 'Warning FerrisStreams Alert: {{ .GroupLabels.alertname }}'
      severity: 'warning'
```

#### Slack Integration
```yaml
# slack-alerts.yaml
apiVersion: v1
kind: Secret
metadata:
  name: slack-webhook
  namespace: monitoring
data:
  url: <base64-encoded-slack-webhook-url>
---
apiVersion: monitoring.coreos.com/v1alpha1
kind: AlertmanagerConfig
metadata:
  name: ferrisstreams-slack
  namespace: monitoring
spec:
  route:
    receiver: 'slack-notifications'
    routes:
    - match:
        severity: info
      receiver: 'slack-info'
      
  receivers:
  - name: 'slack-notifications'
    slackConfigs:
    - apiURL:
        key: url
        name: slack-webhook
      channel: '#ferrisstreams-alerts'
      title: 'FerrisStreams Alert: {{ .GroupLabels.alertname }}'
      text: |
        {{ range .Alerts }}
        Alert: {{ .Annotations.summary }}
        Description: {{ .Annotations.description }}
        Severity: {{ .Labels.severity }}
        Instance: {{ .Labels.instance }}
        {{ end }}
      color: '{{ if eq .Status "firing" }}danger{{ else }}good{{ end }}'
      
  - name: 'slack-info'
    slackConfigs:
    - apiURL:
        key: url
        name: slack-webhook
      channel: '#ferrisstreams-info'
      title: 'FerrisStreams Info: {{ .GroupLabels.alertname }}'
      color: 'good'
```

---

## 🎉 Conclusion

**🎊 FerrisStreams SQL is now production-ready with:**
- **Complete Docker deployment** with all serialization formats
- **Financial precision arithmetic** (42x performance improvement)
- **Flink-compatible Avro** decimal logical types  
- **Industry-standard Protobuf** Decimal messages
- **Phase 2 hash join optimization** (10x+ JOIN performance)
- **Cross-system compatibility** with major streaming platforms
- **Comprehensive production deployment guide** with error handling, security, performance tuning, and integration patterns