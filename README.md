# Velostream

![Rust CI](https://github.com/bluemonk3y/velostream/workflows/Rust%20CI/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/velostream.svg)](https://crates.io/crates/velostream)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

A high-performance **streaming SQL engine** written in Rust that provides real-time data processing with **pluggable data sources** (Kafka, File), **financial-grade precision arithmetic**, and **production-ready observability**. Process streaming data with SQL, achieve 42x performance improvements over floating-point arithmetic, and deploy with confidence as a single binary. Additional data sources (PostgreSQL, ClickHouse, S3, Iceberg) are planned.


🌀 Velostream: Precision Streaming Analytics in a Single Binary

Velostream is a single-binary streaming SQL engine, purpose-built in Rust for finance, IoT, and edge analytics.
Unlike cluster-based systems like Flink or Materialize, Velostream runs anywhere — from the cloud to embedded devices — with zero orchestration.

Its core innovation is deterministic arithmetic via the ScaledDecimal engine, enabling exact numeric computation across high-velocity streams.
This makes Velostream uniquely suited to financial tick data, sensor feeds, and real-time outlier detection, where float rounding or clock skew cannot be tolerated.

Velostream fuses SQL expressiveness, windowed computation, and AI-ready introspection — giving developers the power of a data warehouse, the speed of Rust, and the precision of a spreadsheet.


| Feature                | Velostream              | Flink        | Materialize      | Arroyo         |
| ---------------------- | ----------------------- | ------------ | ---------------- | -------------- |
| Deployment             | Single binary           | Java cluster | Stateful cluster | Rust + cluster |
| Arithmetic             | Exact (`ScaledDecimal`) | Float-based  | Float-based      | Float-based    |
| Latency                | µs–ms                   | ms–s         | ms–s             | ms–s           |
| Nested value access    | ✅                       | ⚠️ Limited   | ⚠️ Limited       | ✅              |
| AI / outlier detection | ✅ Planned               | ❌            | ❌                | ❌              |

✅ Summary

**IP** = deterministic arithmetic + single binary simplicity + introspection hooks.
**Differentiation** = developer-first, zero-cluster precision analytics for finance/IoT.

## 🚀 Quick Start

### Prerequisites
- **Rust 1.91+** (`rustup update stable`)
- **Docker** (for Kafka via `docker-compose`)

### 1. Build

```bash
cargo build --release --bin velo-sql
```

### 2. Start Kafka

```bash
docker-compose up -d
```

### 3. Write a SQL Application

Create `app.sql`:

```sql
-- @app: my_first_app
-- @job_mode: simple

CREATE STREAM filtered_orders AS
SELECT order_id, customer_id, amount
FROM orders_input
WHERE amount > 100.0
INTO high_value_orders;
```

### 4. Deploy

```bash
./target/release/velo-sql deploy-app --file app.sql --brokers localhost:9092
```

### 5. Monitor

Enable Prometheus metrics with `--enable-metrics`:

```bash
./target/release/velo-sql deploy-app --file app.sql --brokers localhost:9092 \
  --enable-metrics --metrics-port 9091
```

Metrics are available at `http://localhost:9091/metrics`.

For a complete working example with data generation, Grafana dashboards, and multiple SQL apps, see the **[Trading Demo](demo/trading/)** — run it with `demo/trading/start-demo.sh`.

---

## 📦 Deploying a SQL Application

Velostream deploys SQL applications from `.sql` files using the `velo-sql deploy-app` command.

### SQL File Structure

A SQL file can contain application-level annotations and one or more `CREATE STREAM`/`CREATE TABLE` statements:

```sql
-- Application metadata
-- @app: trading_signals
-- @version: 1.0.0
-- @description: Real-time trading signal generation

-- Job configuration
-- @job_mode: simple
-- @batch_size: 500

-- Observability
-- @observability.metrics.enabled: true

-- Prometheus metric (optional)
-- @metric: velo_signals_total
-- @metric_type: counter
-- @metric_labels: symbol, signal_type

CREATE STREAM trading_signals AS
SELECT symbol, price, volume,
       CASE WHEN price > avg_price THEN 'BUY' ELSE 'SELL' END as signal_type
FROM market_data
INTO signals_output;
```

See the [SQL Annotations Reference](docs/user-guides/sql-annotations.md) for the full list of supported annotations.

### `velo-sql deploy-app` CLI Reference

```
velo-sql deploy-app --file <PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--file <PATH>` | **(required)** | Path to the `.sql` application file |
| `--brokers <ADDRS>` | `localhost:9092` | Kafka broker addresses |
| `--group-id <ID>` | `velo-sql-app` | Base consumer group ID |
| `--server <URL>` | — | Deploy to a running `velo-sql server` (e.g. `http://localhost:8080`) |
| `--no-monitor` | `false` | Exit immediately after deployment |
| `--enable-metrics` | `false` | Enable Prometheus metrics export |
| `--metrics-port <PORT>` | `9091` | Prometheus metrics port |
| `--enable-tracing` | `false` | Enable OpenTelemetry distributed tracing |
| `--enable-profiling` | `false` | Enable performance profiling |

### Server Mode

For hosting multiple applications, run `velo-sql server` and deploy apps to it:

```bash
# Start the server
./target/release/velo-sql server --brokers localhost:9092 --port 8080

# Deploy apps to the server
./target/release/velo-sql deploy-app --file app1.sql --server http://localhost:8080
./target/release/velo-sql deploy-app --file app2.sql --server http://localhost:8080
```

### Docker Deployment

Use the provided [`Dockerfile.sqlfile`](Dockerfile.sqlfile) to containerise a SQL application:

```bash
docker build -f Dockerfile.sqlfile -t my-velo-app .
docker run -e SQL_FILE=/app/sql-files/app.sql \
           -e KAFKA_BROKERS=kafka:9092 \
           my-velo-app
```

### Trading Demo

The [`demo/trading/`](demo/trading/) directory contains a complete, ready-to-run example with:
- Multiple SQL applications (compliance, risk, trading signals)
- Realistic data generation
- Docker Compose stack (Kafka, Prometheus, Grafana)
- Pre-built Grafana dashboards

```bash
demo/trading/start-demo.sh
```

---

## 🌟 Key Features

### 🔌 Pluggable Data Sources
* **Production-Ready Sources:** Kafka, File (CSV, JSON, Parquet)
* **Planned Sources:** PostgreSQL (with CDC), ClickHouse, S3, Iceberg
* **Heterogeneous Pipelines:** Read from one source, write to another (e.g., Kafka → File)
* **Unified SQL Interface:** `CREATE STREAM AS SELECT * FROM 'kafka://localhost:9092/topic' INTO 'file:///output/data.json'`
* **Single Binary, Scale Out:** K8s-native horizontal pod autoscaling with stateless architecture
* **URI-Based Configuration:** Simple connection strings for all data sources

### 💰 Financial Precision Arithmetic
* **ScaledInteger Type:** Exact decimal arithmetic with zero precision loss
* **42x Performance:** Faster than f64 floating-point for financial calculations
* **Cross-Format Compatibility:** Serializes correctly to JSON/Avro/Protobuf
* **Regulatory Compliance:** Meets financial industry precision requirements

### 🔍 Advanced SQL Streaming
* **Enterprise SQL Parser:** Table aliases in window functions (`PARTITION BY table.column`), INTERVAL frames (`RANGE BETWEEN INTERVAL '1' HOUR PRECEDING`), SQL standard EXTRACT syntax
* **Complete Window Functions:** LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE
* **Statistical Analytics:** STDDEV, VARIANCE, MEDIAN with windowing support
* **Full JOIN Support:** INNER, LEFT, RIGHT, FULL OUTER with temporal windowing and stream-table joins
* **Subqueries:** EXISTS, IN, scalar subqueries with correlated and nested support
* **Table/KTable:** Materialized views with automatic updates and stream-table enrichment

### 🛡️ Production-Ready Operations
* **SQL-Native Observability:** Define Prometheus metrics directly in SQL with `@metric` annotations—no external code needed (FR-073) ✨ NEW
* **Configuration Schema System:** Self-validating schemas with JSON Schema generation and IDE integration
* **Health Monitoring:** Circuit breakers, health checks, degraded state detection
* **Observability:** Dead letter queues, 24-hour rolling metrics, Prometheus/Grafana integration
* **SQL Validation:** Pre-deployment validation gates in CLI and deployment pipelines
* **Management CLI:** Real-time monitoring, health checks, job management, SQL validation

### ⚡ High Performance
* **Type-Safe Operations:** Full support for typed keys, values, and headers with automatic serialization/deserialization
* **Flexible Serialization:** JSON (always available), Avro (schema registry + evolution), Protocol Buffers (high-performance)
* **Asynchronous Processing:** Built on `rdkafka` & `tokio` for efficient, non-blocking I/O
* **Zero-Copy Optimizations:** Minimal allocations in hot paths
* **Batch Processing:** Configurable batching with compression independence

## 🔌 Data Sources

Velostream supports pluggable data sources with unified URI-based configuration.

### Production-Ready Data Sources ✅

| Source | URI Format | Example | Capabilities |
|--------|-----------|---------|--------------|
| **Kafka** | `kafka://brokers/topic` | `kafka://localhost:9092/orders` | Streaming, exactly-once, schema registry |
| **File** | `file:///path` | `file:///data/input/*.csv?header=true` | CSV, JSON, Parquet, watch mode |

### Planned Data Sources 🔄

| Source | URI Format | Example | Capabilities |
|--------|-----------|---------|--------------|
| **PostgreSQL** | `postgresql://host/db` | `postgresql://localhost/shop?table=orders&cdc=true` | Queries, CDC, transactions, ACID |
| **ClickHouse** | `clickhouse://host/db` | `clickhouse://localhost:8123/warehouse?table=events` | Analytics, columnar, compression |
| **S3** | `s3://bucket/prefix` | `s3://data-lake/events/*.parquet?region=us-west-2` | Batch files, partitioning, formats |
| **Iceberg** | `iceberg://catalog/table` | `iceberg://catalog/analytics/events` | ACID, time travel, schema evolution |

### Multi-Source SQL Examples (Production-Ready)

```sql
-- Kafka → File (JSON Lines) streaming pipeline
CREATE STREAM kafka_to_json AS
SELECT * FROM kafka_orders_source
INTO file_json_sink
WITH (
    'kafka_orders_source.config_file' = 'config/kafka_source.yaml',
    'kafka_orders_source.group_id' = 'file-export',
    'file_json_sink.config_file' = 'config/file_sink.yaml',
    'file_json_sink.path' = '/output/orders.jsonl'
);

-- Kafka → File (CSV) with transformation
CREATE STREAM kafka_to_csv AS
SELECT
    customer_id,
    order_id,
    SUM(amount) as total_spent,
    COUNT(*) as order_count
FROM kafka_orders_source
GROUP BY customer_id, order_id
INTO file_csv_sink
WITH (
    'kafka_orders_source.config_file' = 'config/kafka_source.yaml',
    'kafka_orders_source.group_id' = 'analytics',
    'file_csv_sink.config_file' = 'config/file_sink.yaml',
    'file_csv_sink.path' = '/output/customer_stats.csv',
    'file_csv_sink.format' = 'csv'
);

-- File (CSV) → Kafka streaming pipeline
CREATE STREAM csv_to_kafka AS
SELECT * FROM file_csv_source
WHERE amount > 100.0
INTO kafka_high_value_sink
WITH (
    'file_csv_source.config_file' = 'config/file_source.yaml',
    'file_csv_source.path' = '/data/input/*.csv',
    'file_csv_source.watch' = 'true',
    'kafka_high_value_sink.config_file' = 'config/kafka_sink.yaml',
    'kafka_high_value_sink.topic' = 'high-value-orders'
);

-- Kafka → File (Parquet) with windowing
CREATE STREAM kafka_to_parquet AS
SELECT
    customer_id,
    AVG(amount) as avg_order_value,
    COUNT(*) as order_count,
    window_start
FROM kafka_orders_source
WINDOW TUMBLING(1h)
GROUP BY customer_id, window_start
INTO file_parquet_sink
WITH (
    'kafka_orders_source.config_file' = 'config/kafka_source.yaml',
    'kafka_orders_source.group_id' = 'analytics',
    'file_parquet_sink.config_file' = 'config/file_sink.yaml',
    'file_parquet_sink.path' = '/output/hourly_stats.parquet',
    'file_parquet_sink.format' = 'parquet'
);
```

**Example Configuration Files:**

```yaml
# config/kafka_source.yaml
type: kafka_source
brokers: localhost:9092
topic: orders
format: json
schema_registry: http://localhost:8081
```

```yaml
# config/file_sink.yaml
type: file_sink
format: jsonl
compression: gzip
```

### Future Multi-Source Examples (Planned)

```sql
-- PostgreSQL CDC → Kafka (Coming Soon)
CREATE STREAM order_events AS
SELECT * FROM postgres_orders_source
INTO kafka_events_sink
WITH (
    'postgres_orders_source.config_file' = 'config/postgres_source.yaml',
    'postgres_orders_source.table' = 'orders',
    'postgres_orders_source.cdc' = 'true',
    'kafka_events_sink.config_file' = 'config/kafka_sink.yaml',
    'kafka_events_sink.topic' = 'order-stream'
);

-- Cross-source enrichment: Kafka + PostgreSQL → ClickHouse (Coming Soon)
CREATE STREAM enriched_orders AS
SELECT
    o.*,
    c.customer_name,
    c.tier
FROM kafka_orders_source o
INNER JOIN postgres_customers_table c
    ON o.customer_id = c.customer_id
INTO clickhouse_analytics_sink
WITH (
    'kafka_orders_source.config_file' = 'config/kafka_source.yaml',
    'kafka_orders_source.group_id' = 'enrichment',
    'postgres_customers_table.config_file' = 'config/postgres_table.yaml',
    'postgres_customers_table.table' = 'customers',
    'clickhouse_analytics_sink.config_file' = 'config/clickhouse_sink.yaml',
    'clickhouse_analytics_sink.table' = 'enriched_orders'
);
```

**Learn More**: See [Data Sources Documentation](docs/data-sources/) for complete URI reference and configuration options.

## 💰 Financial Precision Arithmetic

Velostream provides **ScaledInteger** for exact decimal arithmetic in financial applications, achieving **42x performance improvement** over f64 floating-point with zero precision loss.

### Why ScaledInteger?

```rust
// ❌ WRONG: Floating-point precision errors in financial calculations
let price_f64: f64 = 123.45;
let quantity_f64: f64 = 1000.0;
let total_f64 = price_f64 * quantity_f64; // 123449.99999999999 (precision loss!)

// ✅ CORRECT: ScaledInteger for exact decimal arithmetic
use velostream::velo::sql::execution::types::FieldValue;

let price = FieldValue::ScaledInteger(12345, 2);      // 123.45
let quantity = FieldValue::ScaledInteger(1000, 0);    // 1000
let total = price * quantity;                          // ScaledInteger(12345000, 2) = 123450.00 (exact!)
```

### Performance Comparison

| Operation | f64 (Float) | ScaledInteger | Performance Gain |
|-----------|-------------|---------------|------------------|
| Financial calculations | 83.458µs | 1.958µs | **42x FASTER** ✨ |
| Precision | ❌ Rounding errors | ✅ Exact | Perfect accuracy |
| Compliance | ❌ Risk | ✅ Regulatory safe | Production-ready |

### Automatic Serialization

ScaledInteger automatically serializes to compatible formats:

```rust
// JSON: Decimal string for universal parsing
{"amount": "123.45"}

// Avro: String field with decimal logical type
{"amount": {"string": "123.45"}}

// Protobuf: Structured Decimal message (industry standard)
message Decimal {
    int64 units = 1;    // 12345
    uint32 scale = 2;   // 2 decimal places
}
```

### SQL Integration

```sql
-- Automatic ScaledInteger arithmetic in SQL
CREATE STREAM order_totals AS
SELECT
    order_id,
    price * quantity as notional_value,          -- Exact precision
    SUM(price * quantity) as total_value,        -- Exact aggregation
    AVG(price) as average_price,                 -- Exact average
    price * quantity * commission_rate as fee    -- Complex calculations
FROM 'kafka://localhost:9092/orders'
GROUP BY order_id;
```

**Use Cases:**
- Financial trading systems (prices, quantities, P&L)
- Banking applications (account balances, interest calculations)
- E-commerce platforms (order totals, tax calculations)
- Analytics requiring exact decimal precision

**Learn More**: See [Productionisation Guide](docs/ops/productionisation.md) for complete deployment documentation.

## 🛡️ Production-Ready Operations

Velostream includes comprehensive production features for reliable, observable, and maintainable deployments.

### Configuration Schema System

Self-validating configuration with IDE integration:

```yaml
# config.yaml with JSON Schema validation
# yaml-language-server: $schema=./config.schema.json

kafka_source:
  brokers: localhost:9092           # IDE autocomplete!
  topic: orders                     # IDE validates property names
  group_id: analytics               # IDE shows valid values
  format: avro                      # IDE catches typos
  schema_registry: http://localhost:8081
```

**Features:**
- Self-registering schema system for all components
- JSON Schema generation for IDE integration
- Comprehensive validation with detailed error messages
- Environment-aware defaults (development vs production)
- Configuration inheritance with YAML `extends:`

### Health Monitoring & Circuit Breakers

```rust
use velostream::velo::datasource::health::{HealthCheck, CircuitBreaker};

// Automatic circuit breaker protection
let circuit_breaker = CircuitBreaker::new(
    5,                          // failure_threshold
    Duration::from_secs(60)     // recovery_timeout
);

// Health checks with degraded state detection
let health = source.health_check().await?;
match health.status {
    HealthStatus::Healthy => println!("✅ All systems operational"),
    HealthStatus::Degraded => println!("⚠️ Performance degraded"),
    HealthStatus::Unhealthy => println!("❌ System failure"),
}
```

### Dead Letter Queues (DLQ)

```rust
// Automatic retry with DLQ for permanent failures
let dlq_config = DlqConfig {
    topic: "failed-records".to_string(),
    max_retries: 3,
    retry_backoff: Duration::from_secs(5),
};

// Failed records automatically routed to DLQ after exhausting retries
```

### Observability & Metrics

```rust
// 24-hour rolling metrics with min/max/avg statistics
let metrics = source.get_metrics().await?;
println!("Throughput: {:.2} records/sec (avg over 24h)", metrics.throughput_avg);
println!("Latency: {:.2}ms (p99)", metrics.latency_p99);
println!("Error rate: {:.2}%", metrics.error_rate);

// Prometheus integration
// /metrics endpoint exposes:
// - velostream_records_processed_total
// - velostream_processing_latency_seconds
// - velostream_errors_total
// - velostream_circuit_breaker_state
```

### SQL Validation (CI/CD Integration)

```bash
# Pre-deployment validation prevents invalid SQL from reaching production
./velo-cli validate financial_pipeline.sql --strict

# CI/CD pipeline integration
- name: Validate SQL
  run: |
    ./velo-cli validate sql/ --strict --format json > validation.json
    if [ $? -ne 0 ]; then
      echo "❌ SQL validation failed"
      cat validation.json
      exit 1
    fi
```

**Validation Coverage:**
- SQL syntax and parser errors
- Data source URI validation
- Configuration schema compliance
- JOIN compatibility across heterogeneous sources
- Window function correctness
- Performance warnings (missing indexes, full table scans)

### K8s Native Deployment

```yaml
# Horizontal Pod Autoscaler for automatic scaling
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: velostream-pipeline
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: velostream-pipeline
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

**Features:**
- Single binary with all data sources included
- Stateless architecture for horizontal scaling
- Zero-downtime deployments
- Automatic pod recovery
- Resource-efficient (minimal memory footprint)

**Learn More**: See [Productionisation Guide](docs/ops/productionisation.md) and [Observability Guide](docs/ops/observability.md).

---

## 📊 SQL-Native Observability ✨ NEW

Define Prometheus metrics **directly in your SQL** using `@metric` annotations—no external code, no metric registration boilerplate. VeloStream automatically registers metrics, extracts label values, and emits to Prometheus as data flows through your streams.

### Quick Example

```sql
-- Define Prometheus counter metric with SQL comments
-- @metric: velo_high_volume_trades_total
-- @metric_type: counter
-- @metric_help: "High-volume trades detected (>1000 shares)"
-- @metric_labels: symbol, exchange
-- @metric_condition: volume > 1000
CREATE STREAM high_volume_trades AS
SELECT
    symbol,
    exchange,
    volume,
    price,
    event_time
FROM market_data;
```

**Result**: Automatic Prometheus metric with zero external code!

```prometheus
# HELP velo_high_volume_trades_total High-volume trades detected (>1000 shares)
# TYPE velo_high_volume_trades_total counter
velo_high_volume_trades_total{symbol="AAPL",exchange="NASDAQ"} 142
velo_high_volume_trades_total{symbol="GOOGL",exchange="NASDAQ"} 89
```

### Supported Metric Types

#### Counter Metrics
Count events and track totals:
```sql
-- @metric: velo_orders_processed_total
-- @metric_type: counter
-- @metric_labels: status, payment_method
CREATE STREAM order_processing AS SELECT * FROM orders;
```

#### Gauge Metrics
Track current values and states:
```sql
-- @metric: velo_queue_depth_messages
-- @metric_type: gauge
-- @metric_field: queue_size
-- @metric_labels: exchange, order_type
CREATE STREAM queue_monitoring AS SELECT * FROM order_queues;
```

#### Histogram Metrics
Measure distributions (latency, sizes):
```sql
-- @metric: velo_order_latency_seconds
-- @metric_type: histogram
-- @metric_field: latency_seconds
-- @metric_labels: exchange
-- @metric_buckets: 0.01, 0.05, 0.1, 0.5, 1.0, 5.0
CREATE STREAM latency_tracking AS SELECT * FROM processing_events;
```

### Advanced Features

**Nested Field Labels** (dot notation):
```sql
-- @metric_labels: metadata.region, metadata.datacenter, device_id
```

**Conditional Emission**:
```sql
-- @metric_condition: volume > avg_volume * 2 AND price > 100
```

**Multiple Metrics Per Stream**:
```sql
-- Counter: Count events
-- @metric: velo_trades_total
-- @metric_type: counter
-- @metric_labels: symbol

-- Gauge: Current price
-- @metric: velo_current_price_dollars
-- @metric_type: gauge
-- @metric_field: price
-- @metric_labels: symbol

-- Histogram: Volume distribution
-- @metric: velo_trade_volume_shares
-- @metric_type: histogram
-- @metric_field: volume
-- @metric_labels: symbol
-- @metric_buckets: 100, 500, 1000, 5000, 10000
CREATE STREAM trade_metrics AS SELECT * FROM market_data;
```

### Performance

- **Throughput**: >100K records/sec with conditional metrics
- **Overhead**: <1% CPU for typical workloads
- **Cached Parsing**: Conditions parsed once at deployment (~1000x faster than per-record parsing)
- **Concurrent Access**: RwLock pattern allows massively parallel processing

### Real-World Examples

See complete annotated examples:
- **[Financial Trading](examples/financial_trading_with_metrics.sql)** - Volume spikes, latency monitoring, high-value trade detection
- **[E-Commerce](examples/ecommerce_with_metrics.sql)** - Order processing, cart abandonment, payment failures
- **[IoT Monitoring](examples/iot_monitoring_with_metrics.sql)** - Device health, battery levels, connectivity tracking

### Documentation

- **[User Guide](docs/user-guides/sql-native-observability.md)** - Complete annotation reference and best practices
- **[Architecture](docs/architecture/observability-architecture.md)** - System design and performance characteristics
- **[FR-073 Implementation](docs/feature/FR-073-IMPLEMENTATION.md)** - Implementation tracking and technical details

### Key Benefits

✅ **Declarative**: Metrics defined where data is defined
✅ **Zero Boilerplate**: No manual registration or emission code
✅ **Type-Safe**: Validation at SQL parse time
✅ **Performance**: Optimized for high-throughput (>100K rec/sec)
✅ **Discoverable**: Metrics documentation lives with SQL
✅ **Production-Ready**: Battle-tested with comprehensive test coverage

---

## 🔧 Current API

### Producer API
```rust
// Create producer with key and value serializers
let producer = KafkaProducer::<String, MyMessage, _, _>::new(
    "localhost:9092", 
    "my-topic", 
    JsonSerializer,    // Key serializer
    JsonSerializer     // Value serializer
)?;

// Send with headers
let headers = Headers::new()
    .insert("source", "web-api")
    .insert("version", "1.0.0");

producer.send(Some(&key), &message, headers, None).await?;
```

### Consumer API
```rust
// Create consumer with key and value serializers
let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
    "localhost:9092", 
    "my-group", 
    JsonSerializer,    // Key deserializer
    JsonSerializer     // Value deserializer
)?;

// Poll for messages - returns Message<K, V> with headers
let message = consumer.poll(Duration::from_secs(5)).await?;
println!("Key: {:?}", message.key());
println!("Value: {:?}", message.value());
println!("Headers: {:?}", message.headers());

// Or use streaming
consumer.stream()
    .for_each(|result| async move {
        if let Ok(message) = result {
            // Access key, value, and headers
            let headers = message.headers();
            if let Some(source) = headers.get("source") {
                println!("Message from: {}", source);
            }
        }
    })
    .await;
```

### Consumer API with Message Metadata
```rust
// Create consumer with key and value serializers
let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
    "localhost:9092", 
    "my-group", 
    JsonSerializer,
    JsonSerializer
)?;

// Poll for messages with full metadata access
let message = consumer.poll(Duration::from_secs(5)).await?;

// Access all metadata at once
println!("{}", message.metadata_string());

// Or access individual fields
println!("Topic: {}", message.topic());
println!("Partition: {}", message.partition());
println!("Offset: {}", message.offset());
if let Some(ts) = message.timestamp_string() {
    println!("Timestamp: {}", ts);
}

// Get topic-partition as a single entity
let tp = message.topic_partition();
println!("Processing {}", tp.to_string()); // prints like "my-topic-0"

// Check if it's the first message in partition
if message.is_first() {
    println!("First message in partition!");
}

// Stream processing with metadata
consumer.stream()
    .for_each(|result| async move {
        if let Ok(message) = result {
            // Group messages by topic-partition
            let tp = message.topic_partition();
            println!("Processing message from {}", tp.to_string());
            
            // Show progression within partition
            println!("Offset {} in partition {}", 
                message.offset(), 
                message.partition());
        }
    })
    .await;
```

### Headers API
```rust
// Create headers
let headers = Headers::new()
    .insert("source", "inventory-service")
    .insert("event-type", "product-created")
    .insert("timestamp", "2024-01-15T10:30:00Z");

// Query headers
if let Some(source) = headers.get("source") {
    println!("Source: {}", source);
}

// Iterate over all headers
for (key, value) in headers.iter() {
    match value {
        Some(v) => println!("{}: {}", key, v),
        None => println!("{}: <null>", key),
    }
}
```

## 🚀 Deployment

- **[Productionisation Guide](docs/ops/productionisation.md)** - **Complete production deployment guide**
  - Docker + Kubernetes deployment
  - Financial precision arithmetic (42x performance)
  - Multi-format serialization (JSON/Avro/Protobuf)
  - Performance configuration profiles
  - Schema file management

- **[Native SQL Deployment](docs/sql/deployment/native-deployment.md)** - Native binary deployment (development only)

## 📚 Examples

### Basic Usage
- **[Producer Example](examples/typed_kafka_example.rs)** - Basic producer/consumer with JSON serialization
- **[Headers Example](examples/headers_example.rs)** - Simple headers usage demonstration
- **[Consumer with Headers](examples/consumer_with_headers.rs)** - Comprehensive headers, keys, and values demo

### Advanced Usage
- **[Builder Configuration](examples/builder_configuration.rs)** - Advanced builder pattern and performance presets
- **[Fluent API Example](examples/fluent_api_example.rs)** - Stream processing with fluent API patterns
- **[Message Metadata Example](examples/message_metadata_example.rs)** - Complete demonstration of message metadata features

### Test Suite Examples

#### Unit Tests
- **[Error Handling Tests](tests/unit/kafka/error_handling_test.rs)** - Error scenarios and edge cases
- **[Message Metadata Tests](tests/unit/kafka/message_metadata_test.rs)** - Message metadata functionality
- **[Headers Edge Cases](tests/unit/kafka/headers_edge_cases_test.rs)** - Advanced headers testing

#### Integration Tests
- **[Builder Pattern Tests](tests/integration/builder_pattern_test.rs)** - Comprehensive builder pattern test suite
- **[Kafka Advanced Tests](tests/integration/kafka_advanced_test.rs)** - Advanced patterns and edge cases
- **[Transaction Tests](tests/integration/transactions_test.rs)** - Transactional producer/consumer patterns
- **[Failure Recovery Tests](tests/integration/failure_recovery_test.rs)** - Network partition and retry logic

### Shared Test Infrastructure
- **[Test Messages](tests/unit/test_messages.rs)** - Unified message types for testing
- **[Test Utils](tests/unit/test_utils.rs)** - Shared utilities and helper functions
- **[Common Imports](tests/unit/common.rs)** - Consolidated imports for all tests

## 🔧 SQL Streaming API

#### CREATE STREAM Syntax (Recommended)

```sql
-- Deploy a complete streaming pipeline with SQL
CREATE STREAM financial_analytics AS
SELECT
    t.trader_id,
    t.symbol,
    t.price,
    t.quantity,
    -- Financial precision arithmetic (ScaledInteger - 42x faster than f64)
    t.price * t.quantity as notional_value,
    -- Window functions with table aliases ✨ NEW
    LAG(t.price, 1) OVER (
        PARTITION BY t.symbol
        ORDER BY t.event_time
    ) as prev_price,
    -- Time-based rolling windows with INTERVAL ✨ NEW
    AVG(t.price) OVER (
        PARTITION BY t.symbol
        ORDER BY t.event_time
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_moving_avg,
    -- Statistical analytics
    STDDEV(t.price) OVER (PARTITION BY t.symbol) as price_volatility,
    -- SQL standard EXTRACT syntax ✨ NEW
    EXTRACT(HOUR FROM t.event_time) as trade_hour,
    EXTRACT(EPOCH FROM (NOW() - t.event_time)) as age_seconds,
    -- Stream-table JOIN for enrichment
    p.position_size,
    p.avg_cost
FROM kafka_trades_source t
LEFT JOIN positions_table p ON t.trader_id = p.trader_id AND t.symbol = p.symbol
WHERE t.price > 0
INTO file_trades_sink
WITH (
    'kafka_trades_source.config_file' = 'config/kafka_source.yaml',
    'kafka_trades_source.topic' = 'trades',
    'kafka_trades_source.group_id' = 'analytics',
    'file_trades_sink.config_file' = 'config/file_sink.yaml',
    'file_trades_sink.path' = '/output/trades_analytics.parquet',
    'file_trades_sink.format' = 'parquet'
);
```

#### Subquery Support ✨ NEW

```sql
-- Complex risk analysis with subqueries
CREATE STREAM high_risk_trades AS
SELECT
    trader_id,
    symbol,
    notional_value,
    -- EXISTS subquery for risk classification
    CASE
        WHEN EXISTS (
            SELECT 1 FROM kafka_trades_source t2
            WHERE t2.trader_id = trades.trader_id
            AND t2.event_time >= trades.event_time - INTERVAL '1' HOUR
            AND ABS(t2.pnl) > 50000
        ) THEN 'HIGH_RISK'
        ELSE 'NORMAL'
    END as risk_category,
    -- IN subquery for filtering
    symbol IN (SELECT symbol FROM high_volume_stocks) as is_high_volume,
    -- Scalar subquery for comparison
    (SELECT AVG(notional_value) FROM kafka_trades_source) as market_avg
FROM kafka_trades_source trades
WHERE trader_id IN (
    SELECT trader_id FROM active_traders WHERE status = 'ACTIVE'
)
INTO kafka_risk_alerts_sink
WITH (
    'kafka_trades_source.config_file' = 'config/kafka_source.yaml',
    'kafka_trades_source.topic' = 'trades',
    'kafka_trades_source.group_id' = 'risk-analysis',
    'kafka_risk_alerts_sink.config_file' = 'config/kafka_sink.yaml',
    'kafka_risk_alerts_sink.topic' = 'risk-alerts'
);
```

#### Programmatic API

```rust
use velostream::velo::sql::{StreamExecutionEngine, StreamingSqlParser};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Parse and execute CREATE STREAM with config files
    let query = "
        CREATE STREAM enriched_orders AS
        SELECT o.*, c.customer_name
        FROM kafka_orders_source o
        INNER JOIN customers_table c
            ON o.customer_id = c.customer_id
        INTO file_enriched_sink
        WITH (
            'kafka_orders_source.config_file' = 'config/kafka_source.yaml',
            'kafka_orders_source.group_id' = 'enrichment',
            'file_enriched_sink.config_file' = 'config/file_sink.yaml',
            'file_enriched_sink.path' = '/output/enriched_orders.json'
        );
    ";

    let parsed_query = parser.parse(query)?;
    engine.execute(&parsed_query).await?;

    Ok(())
}
```

## 🛠️ Velostream CLI

Velostream includes a powerful CLI tool for monitoring, validation, and managing deployments in both local development and production environments.

### Features
- **SQL Validation**: Pre-deployment validation gates with detailed error reporting
- **Health Monitoring**: Real-time health checks of all Velostream components
- **Job Management**: Monitor SQL jobs, data generators, and streaming tasks
- **Kafka Monitoring**: Topic inspection, consumer group monitoring, and cluster health
- **Remote Support**: Connect to production servers via HTTP APIs
- **Real-time Dashboards**: Live monitoring with auto-refresh capabilities

### Quick Start
```bash
# Build the CLI (creates convenient symlink)
./demo/trading/build_cli.sh

# SQL validation (prevents invalid deployments)
./velo-cli validate sql/my_query.sql --verbose
./velo-cli validate sql/ --strict --format json
./velo-cli validate sql/financial_analytics.sql --config production.yaml

# Local monitoring
./velo-cli health
./velo-cli status --verbose
./velo-cli jobs --sql --topics

# Remote production monitoring
./velo-cli --remote --sql-host prod-server.com health
./velo-cli --remote --sql-host prod-server.com --sql-port 8080 status --refresh 10
```

### SQL Validation (Pre-Deployment Safety)
```bash
# Validate single file with detailed errors
./velo-cli validate my_stream.sql --verbose

# Validate entire directory (CI/CD integration)
./velo-cli validate sql/ --strict --format json > validation-report.json

# Validate with configuration context
./velo-cli validate financial_pipeline.sql --config prod.yaml

# Exit codes: 0 (valid), 1 (invalid) - perfect for CI/CD gates
```

**Validation Checks:**
- SQL syntax and parser errors
- Data source URI validation
- Configuration schema validation
- JOIN compatibility across sources
- Window function correctness
- Subquery support validation
- Performance warnings

### Available Commands
- `validate` - SQL validation for files or directories (pre-deployment gates)
- `health` - Quick health check of all components
- `status` - Comprehensive system status with optional real-time monitoring
- `jobs` - Detailed job and task information (SQL, generators, topics)
- `kafka` - Kafka cluster and topic monitoring
- `sql` - SQL server information and job details
- `docker` - Docker container status
- `processes` - Process information

### Connection Options
```bash
--sql-host <HOST>          # SQL server host (default: localhost)
--sql-port <PORT>          # SQL server port (default: 8080)
--kafka-brokers <BROKERS>  # Kafka brokers (default: localhost:9092)
--remote                   # Remote mode - skip local Docker/process checks
--strict                   # Strict validation mode (warnings become errors)
--format <FORMAT>          # Output format: text, json (default: text)
```

See [cli-usage.md](demo/trading/cli-usage.md) for comprehensive documentation.

## 🔄 Message Processing Patterns

### 1. Polling Pattern
```rust
// Traditional polling approach
while let Ok(message) = consumer.poll(timeout).await {
    let (key, value, headers) = message.into_parts();
    // Process message...
}
```

### 2. Streaming Pattern
```rust
// Reactive streaming approach - recommended!
consumer.stream()
    .filter_map(|result| async move { result.ok() })
    .for_each(|message| async move {
        // Message is automatically deserialized!
        println!("Processing: {:?}", message.value());
    })
    .await;
```

### 3. Fluent Processing
```rust
// Functional processing pipeline - see examples/fluent_api_example.rs
let high_priority: Vec<_> = consumer.stream()
    .take(100)
    .filter_map(|result| async move { result.ok() })
    .filter(|message| {
        // Filter by headers
        futures::future::ready(
            message.headers().get("priority") == Some("high")
        )
    })
    .map(|message| message.value().clone())
    .collect()
    .await;
```

## 🏗️ Architecture

### Core Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                  Velostream Engine                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         SQL Streaming Engine                        │   │
│  │  • Parser  • Executor  • Optimizer                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│  ┌──────────────────────┴────────────────────┐             │
│  │                                            │             │
│  ▼                                            ▼             │
│  Pluggable Data Sources              Type System           │
│  ┌────────────────────┐              ┌──────────────┐      │
│  │ • Kafka            │              │ ScaledInteger│      │
│  │ • PostgreSQL (CDC) │              │ (Financial)  │      │
│  │ • ClickHouse       │              │ FieldValue   │      │
│  │ • S3 / Iceberg     │              │ Types        │      │
│  │ • File             │              └──────────────┘      │
│  └────────────────────┘                                    │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │        Serialization Layer                          │   │
│  │  • JSON  • Avro  • Protobuf                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Production Operations Layer                    │
├─────────────────────────────────────────────────────────────┤
│  • Circuit Breakers  • Health Monitoring  • DLQ            │
│  • SQL Validation    • Configuration Schema                │
│  • Metrics & Observability (Prometheus/Grafana)            │
└─────────────────────────────────────────────────────────────┘
```

### Pluggable Data Sources
- **`DataSource` Trait** - Unified interface for all data sources
- **`DataSink` Trait** - Unified interface for all data sinks
- **`DataReader`** - Async streaming reader abstraction
- **`DataWriter`** - Async batched writer abstraction
- **URI-Based Config** - Simple connection strings for all sources

### Type System
- **`FieldValue::ScaledInteger(i64, u8)`** - Financial precision arithmetic (42x faster)
- **`FieldValue::Float(f64)`** - Standard floating-point
- **`FieldValue::Integer(i64)`** - Standard integer
- **`FieldValue::String(String)`** - Text data
- **`FieldValue::Timestamp(NaiveDateTime)`** - Date/time values
- **`Message<K, V>`** - Typed message container with key, value, and headers
- **`Headers`** - Custom headers type with HashMap backing

### Serialization Layer
- **`JsonFormat`** - Built-in JSON serialization (always available)
- **`AvroFormat`** - Schema-based Avro serialization with evolution support
- **`ProtobufFormat`** - High-performance Protocol Buffers serialization
- **`SerializationFormat`** - Trait for implementing custom formats
- **Cross-Format Compatibility** - ScaledInteger serializes correctly to all formats

## 🚀 Roadmap

> **Note**: Detailed roadmap and feature requests are located in [docs/feature/](docs/feature/)

### Current Features ✅

#### Core Architecture
- ✅ **Pluggable Data Sources** - Production-ready sources (Kafka, File) with planned additions (PostgreSQL, ClickHouse, S3, Iceberg)
- ✅ **Heterogeneous Pipelines** - Read from one source, write to another with URI-based config
- ✅ **Single Binary, Scale Out** - K8s-native horizontal pod autoscaling
- ✅ **Financial Precision** - ScaledInteger arithmetic (42x faster than f64, zero precision loss)
- ✅ **Configuration Schema System** - Self-validating schemas with JSON Schema generation

#### SQL Streaming Engine
- ✅ **Enterprise SQL Parser** - Table aliases, INTERVAL frames, SQL standard EXTRACT syntax
- ✅ **CREATE STREAM/TABLE** - Modern SQL deployment syntax with multi-source support
- ✅ **Window Functions** - Complete set of 11 functions (LAG, LEAD, ROW_NUMBER, RANK, etc.)
- ✅ **Statistical Functions** - STDDEV, VARIANCE, MEDIAN with windowing support
- ✅ **JOIN Operations** - All JOIN types (INNER, LEFT, RIGHT, FULL OUTER) with temporal windowing
- ✅ **Stream-Table JOINs** - Materialized views for reference data enrichment
- ✅ **Subqueries** - EXISTS, IN, scalar subqueries with correlated and nested support
- ✅ **Table/KTable** - Materialized views with automatic updates

#### Production Operations
- ✅ **SQL-Native Observability (FR-073)** - Define Prometheus metrics in SQL with @metric annotations ✨ NEW
- ✅ **SQL Validation** - Pre-deployment validation gates in CLI and pipelines
- ✅ **Health Monitoring** - Circuit breakers, health checks, degraded state detection
- ✅ **Observability** - Dead letter queues, 24-hour rolling metrics, Prometheus/Grafana
- ✅ **Management CLI** - Real-time monitoring, health checks, job management, SQL validation

#### Serialization & Performance
- ✅ **Multiple Formats** - JSON (always available), Avro (schema registry + evolution), Protobuf (high-performance)
- ✅ **Type-Safe Operations** - Full support for typed keys, values, and headers
- ✅ **Stream Processing** - Polling and streaming patterns with implicit deserialization
- ✅ **Builder Patterns** - Ergonomic APIs for configuration
- ✅ **Zero-Copy Optimizations** - Minimal allocations in hot paths
- ✅ **Batch Processing** - Configurable batching with compression independence

### Planned Features 🔄

#### Advanced Stream Processing
- **Fan-in Processing** - Multiple topics → single topic with smart merging
- **Fan-out Processing** - Single topic → multiple topics with conditional routing
- **Stream Transformations** - Advanced filtering, mapping, and reducing pipelines
- **Header Propagation** - Automatic header transformation and enrichment

#### State Management
- **Distributed State** - Multi-node state sharing with consistency guarantees
- **State Snapshots** - Point-in-time state recovery and replay
- **External State Stores** - Redis, RocksDB backends for large state

#### Performance Optimizations
- **Query Optimization** - Cost-based query planning and predicate pushdown
- **Columnar Processing** - Apache Arrow integration for analytics workloads
- **Time Travel Queries** - Consume from specific timestamps with offset management

#### Data Source Extensions
- **Delta Lake** - Support for Databricks Delta Lake table format
- **BigQuery** - Google BigQuery source/sink integration
- **Snowflake** - Snowflake data warehouse integration

## 🧪 Testing

The project includes a comprehensive test suite with shared infrastructure to eliminate duplication:

### Running Tests

```bash
# All tests (unit + integration)
cargo test

# Unit tests only
cargo test --lib

# Integration tests (requires Kafka running on localhost:9092)
cargo test --test builder_pattern_test
cargo test --test error_handling_test
cargo test --test serialization_unit_test

# Specific test categories
cargo test test_headers_functionality  # Headers functionality
cargo test test_builder_pattern        # Builder patterns
cargo test test_error_handling         # Error scenarios
cargo test test_performance            # Performance benchmarks
```

### Test Structure

The test suite has been consolidated to eliminate duplication:
- **Shared Messages**: `tests/unit/test_messages.rs` - Unified message types
- **Shared Utilities**: `tests/unit/test_utils.rs` - Common test helpers
- **Common Imports**: `tests/unit/common.rs` - Single import module
- **35+ Tests**: Covering builder patterns, error handling, serialization, and integration scenarios

### Baseline Performance Tests (FR-082)

Comprehensive baseline measurements for SQL query performance patterns:

```bash
# Run all baseline scenarios
cargo test --tests --no-default-features analysis:: -- --nocapture

# Run individual scenarios
cargo test --tests --no-default-features scenario_0_pure_select_baseline -- --nocapture
cargo test --tests --no-default-features scenario_1_rows_window_baseline -- --nocapture
cargo test --tests --no-default-features scenario_2_pure_group_by_baseline -- --nocapture
cargo test --tests --no-default-features scenario_3a_tumbling_standard_baseline -- --nocapture
cargo test --tests --no-default-features scenario_3b_tumbling_emit_changes_baseline -- --nocapture
```

**Scenario Coverage**:
- **Scenario 0**: Pure SELECT (passthrough, 17% of workload) - Reference baseline
- **Scenario 1**: ROWS WINDOW (memory-bounded buffers) - Sliding window analytics
- **Scenario 2**: Pure GROUP BY (hash aggregation, 44% of workload) - Primary optimization target
- **Scenario 3a**: TUMBLING + GROUP BY (standard emission, 28% of workload) - Batch windowing
- **Scenario 3b**: TUMBLING + EMIT CHANGES (continuous emission) - Real-time dashboards

Each scenario measures both SQL Engine (direct execution) and Job Server (full pipeline) performance for accurate overhead calculation. See [FR-082 Performance Documentation](docs/feature/FR-082-perf-part-2/) for detailed results.

### Testing SQL Applications with `velo-test`

The `velo-test` binary runs declarative test specs (`.test.yaml`) against SQL applications with real Kafka infrastructure:

```bash
cargo build --release --bin velo-test

# Run a test spec against a SQL app (uses testcontainers for Kafka)
./target/release/velo-test run demo/test_harness_examples/tier1_basic/ \
  --use-testcontainers --reuse-containers
```

See [`demo/test_harness_examples/`](demo/test_harness_examples/) for test specs across 8 tiers of complexity.

### Current Test Status ✅
- Builder Pattern: 16/16 tests passing
- Error Handling: 12/12 tests passing
- Serialization: 7/7 tests passing
- Integration: All tests passing
- Performance: Benchmarks available

## 🤝 Contributing

Contributions are welcome! Please see our documentation for details.

## 📄 License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## 📚 Documentation

### Available Documentation
- **[SQL-Native Observability](docs/user-guides/sql-native-observability.md)** - Define Prometheus metrics in SQL ✨ NEW
- **[Observability Architecture](docs/architecture/observability-architecture.md)** - System design and performance ✨ NEW
- **[Data Sources](docs/data-sources/)** - Complete guide to pluggable data sources
- **[SQL Reference](docs/sql/)** - Comprehensive SQL syntax and functions reference
- **[Configuration](docs/developer/configuration-quick-start.md)** - Configuration schema system and validation
- **[Deployment Guide](docs/ops/productionisation.md)** - Production deployment with Docker/K8s
- **[Performance Guide](docs/ops/performance-guide.md)** - Performance tuning and optimization
- **[Observability](docs/ops/observability.md)** - Monitoring, metrics, and health checks
- **[CLI Usage](demo/trading/cli-usage.md)** - Complete CLI tool documentation

### Quick Links
- [Examples Directory](examples/) - Working code examples
- [Test Suite](tests/) - Comprehensive test coverage
- [API Reference](docs/api/) - Detailed API documentation

## 🙏 Acknowledgments

Built on top of the excellent [rdkafka](https://github.com/fede1024/rust-rdkafka) library.
