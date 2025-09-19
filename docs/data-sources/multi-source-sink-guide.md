# Velostream Multi-Source/Multi-Sink Processing Guide

## Overview

Velostream provides comprehensive support for processing data from **multiple sources simultaneously** and routing output to **multiple sinks concurrently**. This enables complex analytical pipelines, data enrichment workflows, and enterprise-scale streaming applications.

## Table of Contents

1. [Multi-Source Processing](#multi-source-processing)
2. [Multi-Sink Output](#multi-sink-output)
3. [SQL Syntax](#sql-syntax)
4. [Configuration](#configuration)
5. [Job Processing Modes](#job-processing-modes)
6. [Performance Considerations](#performance-considerations)
7. [Error Handling](#error-handling)
8. [Monitoring](#monitoring)
9. [Real-World Examples](#real-world-examples)
10. [Troubleshooting](#troubleshooting)

---

## Multi-Source Processing

### Architecture

Velostream processes multiple data sources through coordinated readers managed by the `StreamJobServer`. Each source is created independently and processed in a round-robin fashion within each batch cycle.

```rust
// Internal architecture (for reference)
pub async fn process_multi_job(
    readers: HashMap<String, Box<dyn DataReader>>,  // Multiple sources
    writers: HashMap<String, Box<dyn DataWriter>>,  // Multiple sinks
    // ... other parameters
)
```

### Supported Source Combinations

#### 1. **Kafka + File Sources**
```sql
SELECT 
    k.event_id,
    k.user_id,
    f.user_profile
FROM kafka_events k
JOIN user_profiles f ON k.user_id = f.user_id
```

**Sources Created:**
- `kafka_events` → KafkaDataSource 
- `user_profiles` → FileDataSource

#### 2. **Multiple Kafka Topics**
```sql
SELECT 
    orders.order_id,
    payments.payment_id,
    orders.amount,
    payments.status
FROM orders
INNER JOIN payments ON orders.order_id = payments.order_id
WITHIN INTERVAL '5' MINUTES
```

**Sources Created:**
- `orders` → KafkaDataSource (orders topic)
- `payments` → KafkaDataSource (payments topic)

#### 3. **File + Database + Kafka**
```sql
SELECT 
    transactions.txn_id,
    customer_data.risk_score,
    fraud_patterns.pattern_match
FROM kafka_transactions transactions
JOIN customer_db customer_data ON transactions.customer_id = customer_data.id
JOIN fraud_files fraud_patterns ON transactions.merchant_id = fraud_patterns.merchant_id
WHERE transactions.amount > 1000
```

**Sources Created:**
- `kafka_transactions` → KafkaDataSource
- `customer_db` → DatabaseDataSource  
- `fraud_files` → FileDataSource

### Source Processing Flow

1. **Source Creation**: All sources are created during job deployment
2. **Round-Robin Reading**: Each processing cycle reads from all sources
3. **SQL Execution**: Records are processed through the SQL engine
4. **Result Correlation**: JOINs and aggregations correlate data across sources
5. **Output Routing**: Results are sent to all configured sinks

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Source 1  │    │   Source 2  │    │   Source 3  │
│   (Kafka)   │    │   (File)    │    │ (Database)  │
└─────┬───────┘    └─────┬───────┘    └─────┬───────┘
      │                  │                  │
      └──────────────────┼──────────────────┘
                         │
                    ┌────▼─────┐
                    │   SQL    │
                    │  Engine  │
                    └────┬─────┘
                         │
      ┌──────────────────┼──────────────────┐
      │                  │                  │
┌─────▼───────┐    ┌─────▼───────┐    ┌─────▼───────┐
│   Sink 1    │    │   Sink 2    │    │   Sink 3    │
│  (Kafka)    │    │   (File)    │    │    (S3)     │
└─────────────┘    └─────────────┘    └─────────────┘
```

---

## Multi-Sink Output

### Sink Types Supported

#### 1. **Kafka Sinks**
```sql
CREATE STREAM processed_data AS
SELECT * FROM raw_data WHERE amount > 100
INTO kafka_alerts
WITH (
    'kafka_alerts.bootstrap.servers' = 'localhost:9092',
    'kafka_alerts.topic' = 'high-value-alerts',
    'kafka_alerts.value.format' = 'json'
);
```

#### 2. **File Sinks**  
```sql
CREATE STREAM audit_log AS
SELECT order_id, customer_id, amount, processing_time
FROM processed_orders
INTO audit_file
WITH (
    'audit_file.path' = '/var/log/audit/orders.json',
    'audit_file.format' = 'json',
    'audit_file.append' = 'true'
);
```

#### 3. **Multiple Concurrent Sinks**
```sql
CREATE STREAM comprehensive_output AS
SELECT 
    order_id,
    customer_tier,
    total_amount,
    risk_score,
    CURRENT_TIMESTAMP as processed_at
FROM enriched_orders
INTO kafka_realtime, file_archive, s3_backup
WITH (
    'kafka_realtime.topic' = 'realtime-orders',
    'file_archive.path' = 'archive/orders_{date}.json',
    's3_backup.bucket' = 'data-lake',
    's3_backup.prefix' = 'orders/year={year}/month={month}/'
);
```

### Sink Processing Guarantees

- **Parallel Writing**: All sinks receive data simultaneously
- **Independent Failure**: One sink failure doesn't affect others
- **Transaction Support**: Transactional sinks participate in ACID guarantees
- **Fallback Handling**: Automatic stdout fallback if all sinks fail

---

## SQL Syntax

### Multi-Source JOIN Syntax

#### **INNER JOIN**
```sql
SELECT 
    o.order_id,
    c.customer_name,
    p.product_name,
    o.quantity * p.price as total_value
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= '2024-01-01';
```

#### **OUTER JOINs**
```sql
-- LEFT JOIN: All orders, optional customer data
SELECT 
    o.order_id,
    o.amount,
    COALESCE(c.customer_name, 'Unknown') as customer_name,
    COALESCE(c.customer_tier, 'Standard') as tier
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id;

-- FULL OUTER JOIN: Complete data view
SELECT 
    COALESCE(o.customer_id, c.customer_id) as customer_id,
    o.order_id,
    o.amount,
    c.customer_name
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.customer_id;
```

#### **Temporal JOINs**
```sql
-- Time-based correlation
SELECT 
    click.user_id,
    click.page_url,
    purchase.order_id,
    purchase.amount
FROM user_clicks click
INNER JOIN user_purchases purchase ON click.user_id = purchase.user_id
WITHIN INTERVAL '30' MINUTES
WHERE click.utm_campaign = 'summer_sale';
```

### Multi-Sink Output Syntax

#### **Named Sinks**
```sql
CREATE STREAM multi_output AS
SELECT * FROM processed_data
INTO primary_kafka, backup_file, audit_s3;
```

#### **Sink-Specific Configuration**
```sql
CREATE STREAM configured_output AS  
SELECT 
    event_id,
    user_id,
    event_type,
    properties
FROM events
INTO kafka_sink, file_sink
WITH (
    -- Kafka sink configuration
    'kafka_sink.type' = 'kafka_sink',
    'kafka_sink.bootstrap.servers' = 'kafka1:9092,kafka2:9092',
    'kafka_sink.topic' = 'processed-events',
    'kafka_sink.value.format' = 'json',
    'kafka_sink.batch.size' = '1000',
    
    -- File sink configuration  
    'file_sink.type' = 'file_sink',
    'file_sink.path' = '/data/events/processed_{timestamp}.json',
    'file_sink.format' = 'json',
    'file_sink.compression' = 'gzip',
    'file_sink.rotation' = 'hourly'
);
```

---

## Configuration

### Job-Level Configuration

#### **Source Configuration**
```yaml
# In SQL WITH clause
WITH (
    # Global settings
    'use_transactions' = 'true',
    'failure_strategy' = 'RetryWithBackoff',
    'batch.strategy' = 'fixed_size',
    'batch.size' = '500',
    'batch.timeout' = '2000ms',
    'batch.enable' = 'true',
    
    # Source-specific settings
    'orders.type' = 'kafka_source',
    'orders.bootstrap.servers' = 'kafka1:9092',
    'orders.topic' = 'raw-orders',
    'orders.value.format' = 'avro',
    
    'customers.type' = 's3_source',
    'customers.path' = 's3://data-lake/customers/',
    'customers.format' = 'parquet',
    'customers.compression' = 'snappy'
)
```

#### **Batch Processing Configuration**
```yaml
WITH (
    # Batch strategy
    'batch.strategy' = 'AdaptiveSize',
    'batch.max_size' = '1000',
    'batch.timeout' = '5000',
    'batch.memory_limit_mb' = '256',
    
    # Per-source batch configuration
    'kafka_source.batch.size' = '500',
    'file_source.batch.timeout' = '10000'
)
```

### Server-Level Configuration

#### **StreamJobServer Settings**
```rust
let server = StreamJobServer::new_with_monitoring(
    "kafka1:9092,kafka2:9092",  // Kafka brokers
    "velo-streams",           // Consumer group base
    50,                         // Max concurrent jobs
    true,                       // Enable monitoring
);
```

#### **Resource Limits**
```yaml
# Environment configuration
VELO_MAX_JOBS: 50
VELO_MAX_MEMORY_MB: 2048
VELO_MAX_SOURCES_PER_JOB: 10
VELO_MAX_SINKS_PER_JOB: 5
VELO_BATCH_TIMEOUT_MS: 5000
```

### Configuration File-Based Examples

#### **Named Source/Sink Configuration Pattern**

Velostream uses **named source/sink configuration** for clean separation and precise type detection. Each source and sink is configured using its specific name prefix.

**Named Configuration Approach**:
```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'kafka_source.failure_strategy' = 'RetryWithBackoff',
    'kafka_source.retry_backoff' = '1000ms',
    'kafka_source.max_retries' = '3',
    'kafka_source.bootstrap.servers' = 'localhost:9092',
    'kafka_source.topic' = 'raw-events'
);
```

#### **Explicit Type Declaration Options**

Velostream supports multiple ways to explicitly declare source and sink types, making configuration clear and obvious:

**Method 1: Using `.type` (Recommended)**
```sql
SELECT * FROM orders_source
WITH (
    'orders_source.type' = 'kafka',                    -- ✅ Explicit and clear
    'orders_source.bootstrap.servers' = 'localhost:9092',
    'orders_source.topic' = 'orders'
);
```



**Supported Explicit Types:**

| Type | Source | Sink | Description |
|------|--------|------|-------------|
| **`kafka`** | ✅ | ✅ | Apache Kafka streams |
| **`file`** | ✅ | ✅ | Local/NFS file systems |  
| **`s3`** | ✅ | ✅ | Amazon S3 object storage |
| **`database`** | ✅ | ✅ | JDBC-compatible databases |
| **`iceberg`** | ❌ | ✅ | Apache Iceberg data lake |
| **`elasticsearch`** | ✅ | ✅ | Elasticsearch search engine |
| **`redis`** | ✅ | ✅ | Redis key-value store |
| **`mongodb`** | ✅ | ✅ | MongoDB document database |



### Source/Sink Type Detection

Velostream uses **explicit type declaration only** (no autodetection). Types must be explicitly specified using one of these configuration keys:

#### **Type Declaration Methods**

**Using `.type` with Compound Values**
```sql
-- Source configuration (FROM clause)
SELECT * FROM orders_source
WITH (
    'orders_source.type' = 'kafka_source',             -- ✅ REQUIRED: Compound type (type + role)
    'orders_source.bootstrap.servers' = 'localhost:9092',
    'orders_source.topic' = 'orders'
);
```

```sql
-- Sink configuration (INTO clause) 
SELECT * FROM stream_data
INTO processed_sink
WITH (
    'stream_data.type' = 'file_source',                -- ✅ REQUIRED: Source compound type
    'stream_data.path' = '/data/input.csv',
    
    'processed_sink.type' = 'kafka_sink',              -- ✅ REQUIRED: Sink compound type
    'processed_sink.bootstrap.servers' = 'localhost:9092'
);
```

**Supported Compound Types:**
- **Sources**: `kafka_source`, `file_source`, `s3_source`, `database_source`
- **Sinks**: `kafka_sink`, `file_sink`, `s3_sink`, `database_sink`, `iceberg_sink`

**Error Handling for Missing Types:**
If no explicit type is provided, the system will fail with a clear error message:

```
Error: Source type must be explicitly specified for 'orders_source'. 
Use: 'orders_source.type' with values like 'kafka_source', 'file_source', 's3_source', 'database_source'
```

#### **YAML Configuration File Examples**

Velostream supports comprehensive YAML configuration with advanced features including `extends`, environment variable substitution, and profile-based configurations.

**SQL Usage with YAML Config Files:**

You can reference YAML configuration files for named sources and sinks in your SQL:

```sql
-- Using YAML config files for sources and sinks
SELECT 
    orders.order_id,
    customer_data.risk_score,
    orders.amount
FROM orders
JOIN customer_data ON orders.customer_id = customer_data.id
INTO processed_orders, audit_log
WITH (
    -- Named sources with type and config file
    'orders.type' = 'kafka_source',
    'orders.config_file' = './configs/kafka-orders-source.yaml',
    'customer_data.type' = 'database_source',
    'customer_data.config_file' = './configs/database-customers-source.yaml',
    
    -- Named sinks with type and config file  
    'processed_orders.type' = 'kafka_sink',
    'processed_orders.config_file' = './configs/kafka-processed-sink.yaml',
    'audit_log.type' = 'file_sink',
    'audit_log.config_file' = './configs/file-audit-sink.yaml'
);
```

**Alternative: Mixed inline + YAML config:**
```sql
SELECT * FROM events_stream
INTO analytics_warehouse
WITH (
    -- Inline source configuration
    'events_stream.type' = 'kafka_source',
    'events_stream.bootstrap.servers' = 'localhost:9092',
    'events_stream.topic' = 'raw-events',
    
    -- YAML config file for complex sink
    'analytics_warehouse.type' = 's3_sink',
    'analytics_warehouse.config_file' = './configs/s3-warehouse-sink.yaml'
);
```

**Referenced YAML Config Files:**

```yaml
# configs/base_kafka_config.yml
bootstrap.servers: "${KAFKA_BROKERS:-kafka1:9092,kafka2:9092}"
security.protocol: "${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
compression.type: "snappy"
failure_strategy: "RetryWithBackoff"
max_retries: 3
```

```yaml
# configs/kafka-orders-source.yaml
extends: "base_kafka_config.yml"
topic: "orders"
group.id: "order-processor-${ENVIRONMENT:-dev}"
auto.offset.reset: "earliest"
value.format: "avro"
schema.registry.url: "${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
```

```yaml
# configs/database-customers-source.yaml  
connection.url: "jdbc:postgresql://db:5432/customers"
connection.username: "${DB_USER}"
connection.password: "${DB_PASSWORD}"
table: "customer_profiles"
query: "SELECT id, risk_score, tier FROM customer_profiles WHERE active = true"
```

```yaml
# configs/kafka-processed-sink.yaml
extends: "base_kafka_config.yml"
topic: "processed-orders-${ENVIRONMENT:-dev}"  
key.format: "string"
value.format: "json"
batch.size: 1000
acks: "all"
enable.idempotence: true
```

```yaml
# configs/file-audit-sink.yaml
path: "/var/log/audit/orders-{date}.log"
format: "jsonlines"
append: true
rotation: "daily"
max_files: 30
```

```yaml
# configs/s3-warehouse-sink.yaml
bucket: "analytics-warehouse-${ENVIRONMENT}"
prefix: "orders/year={year}/month={month}/day={day}/"
format: "parquet"
compression: "snappy"
partition_columns: ["order_date", "region"]
```

### Key Features

**Named Configuration with External Files:**
- Each named source/sink requires both `.type` and `.config_file` properties
- The `.type` specifies the compound type (e.g., `kafka_source`, `file_sink`)
- The `.config_file` references a YAML file with detailed configuration
- YAML files contain only configuration properties (no type field needed)
- Inline properties can override YAML values for environment-specific settings

**Example 1: E-commerce Pipeline with Individual Config Files**

**Base Configuration (configs/base_kafka_config.yml):**
```yaml
# Common Kafka configuration - can be extended by all sources/sinks
bootstrap.servers: "${KAFKA_BROKERS:-localhost:9092}"
schema.registry.url: "${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
security.protocol: "${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
compression.type: "snappy"
failure_strategy: "RetryWithBackoff"
max_retries: 3
```

**Source Configurations:**
```yaml
# configs/orders_source.yaml
extends: "base_kafka_config.yml"
topic: "raw-orders"
group.id: "ecommerce-processor-${ENVIRONMENT:-dev}"
value.format: "avro"
auto.offset.reset: "earliest"
```

```yaml
# configs/customers_source.yaml  
# Database sources don't extend Kafka base config since they're different type
connection.url: "jdbc:postgresql://${DB_HOST:-localhost}:5432/${DB_NAME:-customers}"
connection.username: "${DB_USER}"
connection.password: "${DB_PASSWORD}"
table: "customer_profiles"
query: "SELECT id, name, email, segment FROM customer_profiles WHERE active = true"
connection.pool.max_size: 20
```

```yaml
# configs/products_source.yaml
# File sources don't extend base_config since they don't share Kafka properties
path: "/data/products/products_${ENVIRONMENT:-dev}.csv"
format: "csv"
has_headers: true
delimiter: ","
encoding: "UTF-8"
```

**Sink Configurations:**
```yaml
# configs/processed_orders_sink.yaml
extends: "base_kafka_config.yml"
topic: "processed-orders-${ENVIRONMENT:-dev}"
key.format: "string"  
value.format: "json"
batch.size: 1000
acks: "all"
enable.idempotence: true
```

```yaml
# configs/analytics_warehouse_sink.yaml
bucket: "analytics-warehouse-${ENVIRONMENT:-dev}"
region: "${AWS_REGION:-us-west-2}"
prefix: "processed-orders/year={year}/month={month}/day={day}/"
format: "parquet"
compression: "snappy"
partition_columns: ["customer_segment", "order_date"]
access_key_id: "${AWS_ACCESS_KEY_ID}"
secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

```yaml
# configs/audit_log_sink.yaml
path: "/var/log/${ENVIRONMENT:-dev}/audit-orders.jsonl"
format: "jsonlines"
append: true
rotation: "daily"
max_files: 30
compression: "gzip"
```

**SQL Pipeline:**
```sql
CREATE STREAM enriched_orders AS
SELECT 
    o.order_id,
    o.amount,
    c.customer_name,
    c.customer_segment,
    p.product_name,
    o.amount * p.price as total_value,
    CURRENT_TIMESTAMP as processed_at
FROM orders_source o
JOIN customers_source c ON o.customer_id = c.id  
JOIN products_source p ON o.product_id = p.id
WHERE o.amount > 100
INTO processed_orders_sink, analytics_warehouse_sink, audit_log_sink
WITH (
    -- Source configurations
    'orders_source.type' = 'kafka_source',
    'orders_source.config_file' = './configs/orders_source.yaml',
    'customers_source.type' = 'database_source', 
    'customers_source.config_file' = './configs/customers_source.yaml',
    'products_source.type' = 'file_source',
    'products_source.config_file' = './configs/products_source.yaml',
    
    -- Sink configurations  
    'processed_orders_sink.type' = 'kafka_sink',
    'processed_orders_sink.config_file' = './configs/processed_orders_sink.yaml',
    'analytics_warehouse_sink.type' = 's3_sink',
    'analytics_warehouse_sink.config_file' = './configs/analytics_warehouse_sink.yaml',
    'audit_log_sink.type' = 'file_sink',
    'audit_log_sink.config_file' = './configs/audit_log_sink.yaml'
);
```

**Example 2: Advanced Configuration with `extends` and Environment Variables**

**Base Kafka Configuration (configs/kafka_base.yaml):**
```yaml
# Base Kafka configuration - can be extended by sources/sinks
bootstrap.servers: "${KAFKA_BROKERS:-localhost:9092}"
security.protocol: "${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
sasl.mechanism: "${KAFKA_SASL_MECHANISM:-PLAIN}"
sasl.username: "${KAFKA_USERNAME}"
sasl.password: "${KAFKA_PASSWORD}"
compression.type: "snappy"
failure_strategy: "RetryWithBackoff"
max_retries: 3
```

**Production Kafka Source (configs/prod_market_data_source.yaml):**
```yaml
extends: "kafka_base.yaml"  # Inherit common Kafka settings
topic: "market-data-${ENVIRONMENT}"
group.id: "trading-analytics-${ENVIRONMENT}"
value.format: "avro"
schema.registry.url: "${SCHEMA_REGISTRY_URL}"
auto.offset.reset: "earliest"
max.poll.records: 500
session.timeout.ms: 30000
```

**Production Kafka Sink (configs/prod_alerts_sink.yaml):**
```yaml
extends: "kafka_base.yaml"  # Inherit common Kafka settings
topic: "risk-alerts-${ENVIRONMENT}"
key.format: "string"
value.format: "json"
batch.size: 1000
linger.ms: 10
acks: "all"
enable.idempotence: true
```

**Database Source (configs/reference_data_source.yaml):**
```yaml
# Database sources don't extend kafka_base since they're different type
connection.url: "jdbc:postgresql://${DB_HOST:-localhost}:5432/${DB_NAME:-trading}"
connection.username: "${DB_USER}"
connection.password: "${DB_PASSWORD}"
connection.pool.max_size: 20
table: "reference_data.instruments"
query: "SELECT * FROM reference_data.instruments WHERE active = true"
refresh_interval: "300000"  # 5 minutes
```

**File Sink (configs/audit_file_sink.yaml):**
```yaml
path: "/var/log/trading/${ENVIRONMENT}/audit-${DATE}.jsonl"
format: "jsonlines"
append: true
rotation: "daily"
max_files: 90  # 90 days retention
compression: "gzip"
buffer_size_bytes: 65536
```

**SQL Pipeline with Advanced Configuration:**
```sql
CREATE STREAM trading_alerts AS
SELECT 
    m.symbol,
    m.price,
    m.volume,
    r.instrument_name,
    r.risk_category,
    CASE 
        WHEN m.price > r.high_threshold THEN 'HIGH_RISK'
        WHEN m.price < r.low_threshold THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END as risk_level,
    CURRENT_TIMESTAMP as alert_time
FROM market_data_source m
JOIN reference_data_source r ON m.symbol = r.symbol
WHERE m.volume > 10000
INTO alerts_sink, audit_sink
WITH (
    -- Source configurations with environment-specific files
    'market_data_source.type' = 'kafka_source',
    'market_data_source.config_file' = './configs/prod_market_data_source.yaml',
    'reference_data_source.type' = 'database_source',
    'reference_data_source.config_file' = './configs/reference_data_source.yaml',
    
    -- Sink configurations with inheritance
    'alerts_sink.type' = 'kafka_sink',
    'alerts_sink.config_file' = './configs/prod_alerts_sink.yaml',
    'audit_sink.type' = 'file_sink', 
    'audit_sink.config_file' = './configs/audit_file_sink.yaml',
    
    -- Inline overrides for environment-specific values
    'alerts_sink.topic' = 'risk-alerts-prod',  # Override YAML value
    'audit_sink.path' = '/logs/prod/trading-audit.log'  # Override YAML value
);
```

**Example 3: Multi-Environment Configuration with Override**
**Development Environment Config (configs/dev_kafka_source.yaml):**
```yaml
extends: "kafka_base.yaml"
topic: "events-dev"
group.id: "dev-processor"
auto.offset.reset: "latest"  # Different from prod
max.poll.records: 100        # Smaller batches for dev
```

**Production Environment Config (configs/prod_kafka_source.yaml):**
```yaml  
extends: "kafka_base.yaml"
topic: "events-prod"
group.id: "prod-processor"
auto.offset.reset: "earliest"
max.poll.records: 1000       # Larger batches for prod
```

**SQL with Environment-Specific Configuration:**
```sql
CREATE STREAM multi_example AS
SELECT 
    k.event_id,
    k.event_type,
    f.metadata
FROM kafka_source k, file_source f  
INTO processed_sink
WITH (
    -- Source configurations - environment-specific files
    'kafka_source.type' = 'kafka_source',
    'kafka_source.config_file' = './configs/${ENVIRONMENT:-dev}_kafka_source.yaml',
    'file_source.type' = 'file_source',
    'file_source.config_file' = './configs/file_source.yaml',
    
    -- Sink configuration
    'processed_sink.type' = 'file_sink',
    'processed_sink.config_file' = './configs/output_sink.yaml',
    
    -- Global job settings
    'use_transactions' = 'true',
    'batch.timeout' = '5000'
);
```

#### **Environment Variables and Profile Support**

Velostream provides comprehensive environment variable substitution and profile-based configuration management.

**Environment Variable Patterns:**
```yaml
# Basic substitution
bootstrap.servers: "${KAFKA_BROKERS}"

# With default fallback
schema.registry.url: "${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

# Nested substitution  
topic: "orders-${ENVIRONMENT:-dev}-${VERSION}"

# Complex expressions
connection.url: "jdbc:postgresql://${DB_HOST}:${DB_PORT:-5432}/${DB_NAME}"
```

**Profile Activation via Environment Variables:**
```bash
# Activate production profile
export VELO_PROFILE=production
export KAFKA_BROKERS=kafka-prod-cluster:9092
export AWS_REGION=us-east-1

# Activate development profile  
export VELO_PROFILE=development
export KAFKA_BROKERS=localhost:9092
export AWS_REGION=us-west-2

# Activate staging with overrides
export VELO_PROFILE=staging
export KAFKA_BROKERS=kafka-staging:9092
export SCHEMA_REGISTRY_URL=http://schema-registry-staging:8081
```

**Runtime Configuration Loading:**
```bash
# Load config with profile
./velo-sql --config configs/financial-trading.yaml --profile production

# Override specific environment variables
KAFKA_BROKERS=custom-kafka:9092 ./velo-sql --config configs/app.yaml

# Use environment-specific config file
./velo-sql --config configs/environments/${ENVIRONMENT}.yaml
```

**Profile Inheritance and Merging:**
```yaml
# configs/app.yaml
active_profile: "${VELO_PROFILE:-development}"

profiles:
  base: &base_profile
    log_level: "INFO"
    metrics_enabled: true
    batch_size: 1000
    
  development:
    <<: *base_profile                    # Inherit base settings
    kafka_brokers: "localhost:9092"
    log_level: "DEBUG"                   # Override specific settings
    
  staging:
    <<: *base_profile
    kafka_brokers: "kafka-staging:9092"
    monitoring_enabled: true
    
  production:
    <<: *base_profile  
    kafka_brokers: "${KAFKA_BROKERS}"    # Environment variable required
    log_level: "WARN"                    # Override for production
    monitoring_enabled: true
    alerting_enabled: true
```

**Supported Environment Variable Features:**

| Feature | Example | Description |
|---------|---------|-------------|
| **Basic Substitution** | `${VAR}` | Replace with environment variable value |
| **Default Values** | `${VAR:-default}` | Use default if environment variable is not set |
| **Required Variables** | `${VAR}` (no default) | Fail startup if variable is missing |
| **Nested Substitution** | `${PREFIX}-${SUFFIX}` | Multiple variables in one value |
| **Profile Variables** | `${kafka_brokers}` | Variables defined in active profile |
| **System Variables** | `${USER}`, `${HOME}` | Standard system environment variables |
| **Dynamic Variables** | `${DATE}`, `${TIME}` | Runtime-generated values |

**Configuration File Discovery:**

Velostream searches for configuration files in the following order:
1. `--config` command line argument
2. `VELO_CONFIG_FILE` environment variable
3. `./velo.yaml` in current directory
4. `./configs/velo.yaml`
5. `~/.velo/config.yaml`
6. `/etc/velo/config.yaml`

**Example Usage Scenarios:**

```bash
# Development with local services
export VELO_PROFILE=development
./velo-sql --sql "SELECT * FROM orders"

# Staging with shared infrastructure  
export VELO_PROFILE=staging
export KAFKA_BROKERS=kafka-staging.company.com:9092
export DB_HOST=postgres-staging.company.com
./velo-sql --config configs/ecommerce.yaml

# Production with full environment variables
export VELO_PROFILE=production
export KAFKA_BROKERS=$PRODUCTION_KAFKA_BROKERS
export SCHEMA_REGISTRY_URL=$PRODUCTION_SCHEMA_REGISTRY
export AWS_REGION=$AWS_DEFAULT_REGION
export DB_HOST=$RDS_ENDPOINT
export DB_PASSWORD=$RDS_PASSWORD
./velo-sql --config configs/production/ecommerce.yaml

# CI/CD deployment with overrides
VELO_PROFILE=production \
KAFKA_BROKERS=${CI_KAFKA_BROKERS} \
DB_PASSWORD=${CI_DB_SECRET} \
./velo-sql --config configs/ci-deployment.yaml
```

---

## Job Processing Modes

### Simple Processing Mode

**Characteristics:**
- ✅ High throughput
- ✅ Low latency
- ✅ Best-effort delivery
- ❌ No transaction guarantees

**Use Cases:**
- Real-time dashboards
- Monitoring and alerting
- High-volume analytics
- Non-critical data pipelines

**Configuration:**
```sql
WITH (
    'use_transactions' = 'false',
    'failure_strategy' = 'LogAndContinue',
    'max_batch_size' = '1000',
    'batch_timeout' = '100'
)
```

### Transactional Processing Mode

**Characteristics:**
- ✅ ACID guarantees
- ✅ Exactly-once processing
- ✅ Cross-source consistency
- ❌ Higher latency
- ❌ Lower throughput

**Use Cases:**
- Financial processing
- Critical business data
- Regulatory compliance
- Data integrity requirements

**Configuration:**
```sql
WITH (
    'use_transactions' = 'true',
    'failure_strategy' = 'FailBatch',
    'max_batch_size' = '100',
    'batch_timeout' = '2000'
)
```

### Processing Mode Selection

Velostream automatically selects the appropriate processor based on:

1. **`use_transactions` setting**
2. **Source transaction support**
3. **Sink transaction support**
4. **Failure strategy configuration**

```rust
// Automatic selection logic
let use_transactions = config.use_transactions && 
    all_sources_support_transactions && 
    all_sinks_support_transactions;

if use_transactions {
    // Use TransactionalJobProcessor
} else {
    // Use SimpleJobProcessor  
}
```

---

## Performance Considerations

### Throughput Optimization

#### **Batch Size Tuning**
```sql
-- High throughput configuration
WITH (
    'max_batch_size' = '2000',
    'batch_timeout' = '50',
    'batch.strategy' = 'FixedSize'
)

-- Low latency configuration  
WITH (
    'max_batch_size' = '10',
    'batch_timeout' = '10', 
    'batch.strategy' = 'LowLatency'
)
```

#### **Source Balancing**
- **Round-robin processing**: Fair allocation across sources
- **Source prioritization**: Configure processing order
- **Backpressure handling**: Automatic slowdown on sink pressure

#### **Memory Management**
```sql
WITH (
    'batch.memory_limit_mb' = '512',
    'batch.strategy' = 'MemoryBased',
    'source.buffer_size' = '1000'
)
```

### Performance Benchmarks

| Configuration | Throughput | Latency P95 | Memory Usage |
|---------------|------------|-------------|--------------|
| **Simple Mode - High Throughput** | >100K records/sec | <200ms | <100MB |
| **Simple Mode - Low Latency** | >50K records/sec | <50ms | <50MB |
| **Transactional Mode** | >25K records/sec | <500ms | <200MB |
| **Multi-Source (3) Simple** | >75K records/sec | <300ms | <150MB |
| **Multi-Source (3) Transactional** | >15K records/sec | <1000ms | <300MB |

### Scaling Guidelines

#### **Horizontal Scaling**
- Deploy multiple StreamJobServer instances
- Use Kafka consumer groups for load balancing
- Partition data sources appropriately

#### **Vertical Scaling**
- Increase batch sizes for higher throughput
- Allocate more memory for larger batches
- Use faster storage for file sources

---

## Error Handling

### Failure Strategies

#### **LogAndContinue**
```sql
WITH ('failure_strategy' = 'LogAndContinue')
```
- ✅ **High availability**: Processing continues despite errors
- ✅ **Maximum throughput**: Minimal impact on performance
- ❌ **Data loss possible**: Failed records are lost
- **Use case**: Non-critical analytics, monitoring

#### **FailBatch** 
```sql
WITH ('failure_strategy' = 'FailBatch')
```
- ✅ **Data consistency**: No partial processing
- ✅ **Error isolation**: Batch boundaries prevent error propagation  
- ❌ **Lower throughput**: Failed batches stop processing
- **Use case**: Critical business data, financial processing

#### **RetryWithBackoff**
```sql
WITH (
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '5',
    'retry_backoff' = '2000'
)
```
- ✅ **Transient error recovery**: Automatic retry with exponential backoff
- ✅ **High success rate**: Handles temporary issues
- ❌ **Increased latency**: Retry delays impact performance
- **Use case**: Network-sensitive sources, temporary service outages

### Multi-Source Error Scenarios

#### **Partial Source Failure**
```
Source 1 (Kafka): ✅ Operating normally
Source 2 (File):  ❌ File not found
Source 3 (S3):    ✅ Operating normally
```

**Behavior:**
- **Simple Mode**: Continues processing with available sources
- **Transactional Mode**: Fails entire job if transaction sources fail

#### **Partial Sink Failure**
```  
Sink 1 (Kafka): ✅ Writing successfully
Sink 2 (File):  ❌ Disk full
Sink 3 (S3):    ✅ Writing successfully  
```

**Behavior:**
- **Graceful degradation**: Continues writing to available sinks
- **Error logging**: Failed sink errors are logged
- **Monitoring alerts**: Metrics indicate partial sink failures

### Error Recovery

#### **Automatic Recovery**
- **Source reconnection**: Automatic retry for temporary connection issues
- **Sink failover**: Falls back to stdout if all configured sinks fail
- **Batch retry**: RetryWithBackoff strategy handles transient errors

#### **Manual Recovery**
- **Job restart**: Stop and restart failed jobs
- **Configuration update**: Fix configuration issues and redeploy
- **Data replay**: Use Kafka offsets to replay failed data

---

## Monitoring

### Job Metrics

#### **Processing Metrics**
```rust
pub struct JobMetrics {
    pub records_processed: u64,
    pub records_per_second: f64, 
    pub batches_processed: u64,
    pub batches_failed: u64,
    pub last_record_time: Option<DateTime<Utc>>,
    pub errors: u64,
    pub memory_usage_mb: f64,
}
```

#### **Source-Specific Metrics**
- **Records read per source**
- **Source processing latency**
- **Source connection status**
- **Source error rates**

#### **Sink-Specific Metrics**
- **Records written per sink**
- **Sink write latency**
- **Sink connection status**
- **Sink error rates**

### Health Monitoring

#### **Job Health Checks**
```bash
# Check job status
curl http://localhost:8080/api/jobs/my-multi-source-job/status

# Response
{
  "name": "my-multi-source-job",
  "status": "Running", 
  "sources": {
    "source_0_orders": "Connected",
    "source_1_customers": "Connected", 
    "source_2_products": "Reconnecting"
  },
  "sinks": {
    "sink_0_kafka": "Connected",
    "sink_1_file": "Connected"
  },
  "metrics": {
    "records_processed": 150000,
    "records_per_second": 2500.5,
    "batches_processed": 3000,
    "memory_usage_mb": 125.7
  }
}
```

#### **Prometheus Metrics**
```prometheus
# Multi-source job metrics
velo_job_sources_total{job="order-processing"} 3
velo_job_sinks_total{job="order-processing"} 2
velo_job_source_records_total{job="order-processing", source="orders"} 50000
velo_job_source_records_total{job="order-processing", source="customers"} 25000
velo_job_sink_records_total{job="order-processing", sink="kafka"} 45000
velo_job_sink_records_total{job="order-processing", sink="file"} 45000
```

### Alerting

#### **Critical Alerts**
- Job failure or unexpected termination
- All sources disconnected
- All sinks failed
- Memory usage above threshold
- Processing lag above threshold

#### **Warning Alerts**
- Partial source failure
- Partial sink failure  
- High error rate
- Batch processing delays

---

## Real-World Examples

### 1. E-commerce Order Processing

#### **Scenario**
Process orders from Kafka, enrich with customer data from files, validate against product catalog, output to multiple systems.

#### **SQL Implementation**
```sql
CREATE STREAM enriched_orders AS
SELECT 
    o.order_id,
    o.order_date,
    o.quantity,
    c.customer_name,
    c.customer_tier,
    c.email,
    p.product_name,
    p.category,
    p.unit_price,
    o.quantity * p.unit_price as line_total,
    CASE 
        WHEN c.customer_tier = 'GOLD' THEN 0.1
        WHEN c.customer_tier = 'SILVER' THEN 0.05
        ELSE 0.0
    END as discount_rate
FROM kafka_orders o
INNER JOIN customer_files c ON o.customer_id = c.customer_id
INNER JOIN product_catalog p ON o.product_id = p.product_id
WHERE o.quantity > 0 AND p.active = true
INTO kafka_fulfillment, file_audit, s3_analytics
WITH (
    -- Source configuration
    'kafka_orders.type' = 'kafka_source',
    'kafka_orders.bootstrap.servers' = 'kafka1:9092',
    'kafka_orders.topic' = 'raw-orders',
    'kafka_orders.value.format' = 'avro',
    
    'customer_files.type' = 's3_source',
    'customer_files.path' = 's3://customer-data/profiles/',
    'customer_files.format' = 'parquet',
    
    'product_catalog.type' = 'file_source',
    'product_catalog.path' = '/data/products/catalog.json',
    'product_catalog.format' = 'json',
    'product_catalog.source.has_headers' = 'true',
    
    -- Sink configuration
    'kafka_fulfillment.type' = 'kafka_sink',
    'kafka_fulfillment.topic' = 'order-fulfillment',
    'kafka_fulfillment.value.format' = 'json',
    
    'file_audit.type' = 'file_sink',
    'file_audit.path' = '/var/log/audit/orders_{date}.log',
    'file_audit.format' = 'json',
    'file_audit.append' = 'true',
    
    's3_analytics.type' = 's3_sink',
    's3_analytics.bucket' = 'analytics-lake',
    's3_analytics.prefix' = 'orders/year={year}/month={month}/',
    
    -- Processing configuration
    'use_transactions' = 'false',
    'failure_strategy' = 'LogAndContinue',
    'batch.strategy' = 'fixed_size',
    'batch.size' = '500',
    'batch.timeout' = '2000ms',
    'batch.enable' = 'true'
);
```

#### **Expected Results**
- **Sources**: 3 (Kafka + 2 Files)  
- **Sinks**: 3 (Kafka + File + S3)
- **Processing**: ~10K orders/sec with customer/product enrichment
- **Latency**: <500ms end-to-end

### 2. Financial Risk Analysis

#### **Scenario**
Real-time fraud detection combining transaction streams, customer profiles, and merchant risk data with regulatory audit logging.

#### **SQL Implementation**
```sql
CREATE STREAM fraud_analysis AS
SELECT 
    t.transaction_id,
    t.amount,
    t.merchant_id,
    t.timestamp as transaction_time,
    c.risk_score,
    c.country as customer_country,
    m.risk_level as merchant_risk,
    l.current_location,
    CASE 
        WHEN t.amount > 10000 THEN 'HIGH_VALUE'
        WHEN l.current_location != c.country THEN 'LOCATION_RISK'  
        WHEN m.risk_level = 'HIGH' THEN 'MERCHANT_RISK'
        WHEN c.risk_score > 0.8 THEN 'CUSTOMER_RISK'
        ELSE 'LOW_RISK'
    END as risk_assessment,
    CASE
        WHEN (t.amount > 10000 AND l.current_location != c.country) 
        THEN 'BLOCK_TRANSACTION'
        WHEN (c.risk_score > 0.9 OR m.risk_level = 'HIGH')
        THEN 'MANUAL_REVIEW' 
        ELSE 'APPROVE'
    END as recommendation
FROM transaction_stream t
INNER JOIN customer_profiles c ON t.customer_id = c.customer_id
INNER JOIN merchant_data m ON t.merchant_id = m.merchant_id  
LEFT JOIN location_service l ON t.customer_id = l.user_id
WITHIN INTERVAL '5' MINUTES
WHERE t.amount > 0
INTO fraud_alerts, audit_compliance, risk_warehouse
WITH (
    -- Transactional processing for financial data
    'use_transactions' = 'true',
    'failure_strategy' = 'FailBatch',
    'max_batch_size' = '100',
    'batch_timeout' = '1000',
    
    -- Source configuration
    'transaction_stream.bootstrap.servers' = 'kafka-secure:9093',
    'transaction_stream.topic' = 'transactions',
    'transaction_stream.security.protocol' = 'SSL',
    'transaction_stream.source.format' = 'avro',
    
    'customer_profiles.connection' = 'postgresql://secure-db:5432/customers',
    'customer_profiles.table' = 'customer_risk_profiles',
    
    'merchant_data.path' = 's3://risk-data/merchants/',
    'merchant_data.source.format' = 'parquet',
    
    'location_service.endpoint' = 'https://location-api/lookup',
    'location_service.auth_token' = '${LOCATION_API_TOKEN}',
    
    -- Sink configuration
    'fraud_alerts.topic' = 'fraud-alerts',
    'fraud_alerts.bootstrap.servers' = 'kafka-secure:9093',
    'fraud_alerts.security.protocol' = 'SSL',
    
    'audit_compliance.path' = '/secure/audit/fraud_analysis_{timestamp}.log',
    'audit_compliance.format' = 'json',
    'audit_compliance.encryption' = 'AES256',
    
    'risk_warehouse.connection' = 'postgresql://warehouse:5432/risk',
    'risk_warehouse.table' = 'fraud_analysis_results'
);
```

#### **Expected Results**
- **Sources**: 4 (Kafka + Database + S3 + API)
- **Sinks**: 3 (Kafka + Encrypted File + Database)  
- **Processing**: ~5K transactions/sec with full risk analysis
- **Latency**: <2s end-to-end with transaction guarantees

### 3. IoT Sensor Data Processing

#### **Scenario**
Process multiple sensor streams, correlate readings, detect anomalies, output to monitoring systems and data lake.

#### **SQL Implementation**
```sql  
CREATE STREAM sensor_analysis AS
SELECT 
    t.device_id,
    t.location,
    t.timestamp,
    t.temperature,
    h.humidity,
    p.pressure,
    v.vibration_level,
    -- Calculate composite metrics
    (t.temperature - 20) / 10.0 as temp_deviation,
    (h.humidity - 50) / 20.0 as humidity_deviation,
    (p.pressure - 1013.25) / 50.0 as pressure_deviation,
    -- Anomaly detection
    CASE
        WHEN ABS(t.temperature - LAG(t.temperature, 1) OVER (
            PARTITION BY t.device_id ORDER BY t.timestamp
        )) > 10 THEN 'TEMP_SPIKE'
        WHEN h.humidity > 90 AND t.temperature > 35 THEN 'HIGH_HEAT_HUMIDITY'
        WHEN v.vibration_level > 8.0 THEN 'EXCESSIVE_VIBRATION'  
        WHEN p.pressure < 950 THEN 'LOW_PRESSURE_ALARM'
        ELSE 'NORMAL'
    END as anomaly_type
FROM temperature_sensors t
INNER JOIN humidity_sensors h 
    ON t.device_id = h.device_id
INNER JOIN pressure_sensors p
    ON t.device_id = p.device_id  
LEFT JOIN vibration_sensors v
    ON t.device_id = v.device_id
WITHIN INTERVAL '2' MINUTES
WHERE t.temperature IS NOT NULL 
  AND h.humidity IS NOT NULL
  AND p.pressure IS NOT NULL
INTO monitoring_alerts, data_lake, device_dashboard
WITH (
    -- High-frequency sensor data processing
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '10',
    'batch.low_latency_wait' = '5ms',
    'batch.eager_processing' = 'true',
    'batch.enable' = 'true',
    'use_transactions' = 'false',
    'failure_strategy' = 'LogAndContinue',
    
    -- Sensor source configuration
    'temperature_sensors.bootstrap.servers' = 'iot-kafka:9092',
    'temperature_sensors.topic' = 'temperature-readings',
    'temperature_sensors.source.format' = 'json',
    
    'humidity_sensors.bootstrap.servers' = 'iot-kafka:9092', 
    'humidity_sensors.topic' = 'humidity-readings',
    'humidity_sensors.source.format' = 'json',
    
    'pressure_sensors.bootstrap.servers' = 'iot-kafka:9092',
    'pressure_sensors.topic' = 'pressure-readings', 
    'pressure_sensors.source.format' = 'json',
    
    'vibration_sensors.bootstrap.servers' = 'iot-kafka:9092',
    'vibration_sensors.topic' = 'vibration-readings',
    'vibration_sensors.source.format' = 'json',
    
    -- Output sink configuration
    'monitoring_alerts.bootstrap.servers' = 'alert-kafka:9092',
    'monitoring_alerts.topic' = 'sensor-alerts',
    'monitoring_alerts.value.format' = 'json',
    
    'data_lake.bucket' = 'sensor-data-lake',
    'data_lake.prefix' = 'sensors/year={year}/month={month}/day={day}/',
    'data_lake.format' = 'parquet',
    'data_lake.compression' = 'snappy',
    
    'device_dashboard.endpoint' = 'https://dashboard-api/sensors',
    'device_dashboard.auth_header' = 'Bearer ${DASHBOARD_TOKEN}',
    'device_dashboard.batch_size' = '50'
);
```

#### **Expected Results**
- **Sources**: 4 (All Kafka sensor topics)
- **Sinks**: 3 (Kafka + S3 + HTTP API)
- **Processing**: ~50K sensor readings/sec with correlation and anomaly detection  
- **Latency**: <100ms for real-time monitoring

### 4. High-Throughput Batch Processing

#### **Scenario**
Process large volumes of financial transactions using different batch strategies optimized for throughput, memory efficiency, and latency requirements across multiple sources and sinks.

#### **SQL Implementation**
```sql
-- SQL Application: HighThroughputBatchProcessing
CREATE STREAM batch_optimized_transactions AS
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.merchant_category,
    c.risk_score,
    m.fraud_indicators,
    t.amount * CASE 
        WHEN c.risk_score > 0.8 THEN 1.05  -- High risk fee
        WHEN t.amount > 10000 THEN 1.02    -- Large transaction fee
        ELSE 1.0
    END as adjusted_amount,
    CURRENT_TIMESTAMP as processed_at
FROM transaction_stream t
LEFT JOIN customer_profiles c ON t.customer_id = c.customer_id  
LEFT JOIN merchant_data m ON t.merchant_id = m.merchant_id
WHERE t.amount > 0 AND t.status = 'PENDING'
INTO high_throughput_sink, audit_log_sink, analytics_sink
WITH (
    -- Global Batch Configuration: Adaptive Size for Variable Load
    'batch.strategy' = 'adaptive_size',
    'batch.enable' = 'true',
    'batch.min_size' = '100',
    'batch.adaptive_max_size' = '5000',
    'batch.target_latency' = '200ms',
    'batch.timeout' = '10000ms',
    
    -- Source Configuration: High-throughput Kafka stream
    'transaction_stream.type' = 'kafka_source',
    'transaction_stream.bootstrap.servers' = 'kafka-cluster:9092',
    'transaction_stream.topic' = 'raw-transactions',
    'transaction_stream.value.format' = 'avro',
    'transaction_stream.group.id' = 'batch-processor',
    'transaction_stream.max.poll.records' = '2000',
    'transaction_stream.fetch.min.bytes' = '32768',
    
    -- File-based Reference Data
    'customer_profiles.type' = 'file_source',
    'customer_profiles.path' = '/data/customers/profiles.parquet',
    'customer_profiles.format' = 'parquet',
    
    'merchant_data.type' = 'file_source', 
    'merchant_data.path' = '/data/merchants/fraud_indicators.json',
    'merchant_data.format' = 'json',
    
    -- High-Throughput Kafka Sink with Compression
    'high_throughput_sink.type' = 'kafka_sink',
    'high_throughput_sink.bootstrap.servers' = 'kafka-cluster:9092',
    'high_throughput_sink.topic' = 'processed-transactions',
    'high_throughput_sink.value.format' = 'json',
    'high_throughput_sink.batch.size' = '65536',      -- Kafka producer batch size
    'high_throughput_sink.linger.ms' = '50',          -- Kafka batching delay
    'high_throughput_sink.compression.type' = 'snappy',
    
    -- File-based Audit Log
    'audit_log_sink.type' = 'file_sink',
    'audit_log_sink.path' = '/var/audit/transactions_{date}.log',
    'audit_log_sink.format' = 'json',
    'audit_log_sink.append' = 'true',
    
    -- Analytics Data Lake  
    'analytics_sink.type' = 's3_sink',
    'analytics_sink.bucket' = 'financial-analytics',
    'analytics_sink.prefix' = 'transactions/year={year}/month={month}/day={day}/',
    'analytics_sink.format' = 'parquet',
    'analytics_sink.compression' = 'gzip',
    
    -- Job-Level Failure Handling
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '3',
    'retry_backoff' = '2000ms',
    
    -- Performance Tuning
    'use_transactions' = 'false'  -- Simple mode for maximum throughput
);

-- Alternative: Memory-Based Batching for Large Records
CREATE STREAM memory_optimized_large_records AS
SELECT 
    large_payload.record_id,
    large_payload.data_blob,
    large_payload.metadata,
    BYTE_LENGTH(large_payload.data_blob) as payload_size
FROM large_data_stream large_payload  
WHERE BYTE_LENGTH(large_payload.data_blob) < 1048576  -- Filter records < 1MB
INTO memory_aware_sink
WITH (
    -- Memory-Based Batching for Large Payloads
    'batch.strategy' = 'memory_based',
    'batch.enable' = 'true', 
    'batch.memory_size' = '16777216',  -- 16MB batches
    'batch.timeout' = '30000ms',       -- 30 second timeout
    
    -- Source: High-volume data stream
    'large_data_stream.type' = 'kafka_source',
    'large_data_stream.bootstrap.servers' = 'kafka-cluster:9092',
    'large_data_stream.topic' = 'large-payloads',
    'large_data_stream.value.format' = 'avro',
    'large_data_stream.max.poll.records' = '100',  -- Smaller poll for large records
    
    -- Memory-aware sink
    'memory_aware_sink.type' = 'file_sink',
    'memory_aware_sink.path' = '/data/large_records/{hour}/batch_{batch_id}.json',
    'memory_aware_sink.format' = 'json'
);

-- Low-Latency Processing for Real-Time Alerts  
CREATE STREAM low_latency_alerts AS
SELECT 
    alert.alert_id,
    alert.severity, 
    alert.message,
    alert.timestamp,
    'URGENT' as priority
FROM critical_alerts alert
WHERE alert.severity IN ('CRITICAL', 'HIGH')
INTO immediate_notification_sink, alert_dashboard_sink
WITH (
    -- Low-Latency Batching: Process immediately
    'batch.strategy' = 'low_latency',
    'batch.enable' = 'true',
    'batch.low_latency_max_size' = '5',    -- Very small batches
    'batch.low_latency_wait' = '10ms',     -- Minimal wait time
    'batch.eager_processing' = 'true',     -- Process immediately when available
    
    -- Real-time alert source
    'critical_alerts.type' = 'kafka_source', 
    'critical_alerts.bootstrap.servers' = 'kafka-cluster:9092',
    'critical_alerts.topic' = 'system-alerts',
    'critical_alerts.value.format' = 'json',
    'critical_alerts.fetch.max.wait.ms' = '1',  -- Minimal fetch delay
    
    -- Immediate notification (HTTP/webhook)
    'immediate_notification_sink.type' = 'http_sink',
    'immediate_notification_sink.url' = 'https://alerts.company.com/webhook',
    'immediate_notification_sink.method' = 'POST',
    'immediate_notification_sink.timeout' = '5000ms',
    
    -- Dashboard updates  
    'alert_dashboard_sink.type' = 'kafka_sink',
    'alert_dashboard_sink.bootstrap.servers' = 'kafka-cluster:9092', 
    'alert_dashboard_sink.topic' = 'dashboard-alerts',
    'alert_dashboard_sink.value.format' = 'json',
    'alert_dashboard_sink.batch.size' = '1024',     -- Small Kafka batches
    'alert_dashboard_sink.linger.ms' = '1'          -- Immediate send
);
```

#### **Expected Results**
- **Adaptive Size Stream**: 
  - **Sources**: 3 (Kafka + 2 Files)
  - **Sinks**: 3 (Kafka + File + S3)
  - **Processing**: ~25K transactions/sec with automatic batch size optimization
  - **Latency**: 200ms average, scales with load
  
- **Memory-Based Stream**:
  - **Sources**: 1 (Kafka large payloads)
  - **Sinks**: 1 (File)
  - **Processing**: ~500 large records/sec with 16MB memory batches  
  - **Memory**: Consistent 16MB batch memory usage

- **Low-Latency Stream**:
  - **Sources**: 1 (Kafka alerts)
  - **Sinks**: 2 (HTTP + Kafka)
  - **Processing**: ~10K alerts/sec with <50ms end-to-end latency
  - **Latency**: <50ms P95 for critical alerts

#### **Batch Strategy Benefits Demonstrated**
- **Adaptive Size**: Automatically optimizes batch size based on system load and processing time
- **Memory Based**: Prevents memory overflow when processing variable-size large records  
- **Low Latency**: Minimizes processing delay for time-critical data
- **Global Configuration**: Single batch strategy applied to all sources and sinks in the job
- **Sink-Specific Overrides**: Kafka producer settings (batch.size, linger.ms) work alongside global batch configuration

---

## Troubleshooting

### Common Issues

#### **1. "No sources found" Error**
```
Error: No supported datasource found in query analysis for job 'my-job'
```

**Causes:**
- SQL query doesn't reference any tables/streams
- QueryAnalyzer failed to detect sources in FROM/JOIN clauses
- Unsupported source type specified

**Solutions:**
```sql
-- ✅ Correct: Specify table in FROM clause
SELECT * FROM orders WHERE amount > 100

-- ❌ Wrong: No table specified
SELECT 100 as constant_value
```

#### **2. "Failed to create source" Error**
```
Error: Job 'multi-source-job' failed to create data sources: Failed to create source 'source_0_orders': Connection refused
```

**Causes:**
- Source service unavailable (Kafka broker down, file not found, etc.)
- Configuration errors (wrong connection string, missing credentials)
- Network connectivity issues

**Solutions:**
- Verify source availability: `telnet kafka-broker 9092`
- Check configuration parameters
- Review authentication and network access

#### **3. Partial Sink Failure**
```
Warning: Job 'my-job' failed to create sinks: Failed to create sink 'sink_1_file': Permission denied
```

**Causes:**
- Insufficient file system permissions
- Missing directories
- Disk space issues
- Network access problems for remote sinks

**Solutions:**
- Check directory permissions: `ls -la /output/path/`
- Create missing directories: `mkdir -p /output/path/`
- Verify disk space: `df -h`

#### **4. High Memory Usage**
```
Warning: Job memory usage 450MB exceeds recommended limit of 400MB
```  

**Causes:**
- Batch sizes too large
- Window sizes too large for temporal joins
- Memory leaks in processing logic
- Too many concurrent sources

**Solutions:**
```sql
-- Reduce batch size
WITH ('max_batch_size' = '100')

-- Use memory-based batching
WITH ('batch.strategy' = 'MemoryBased', 'batch.memory_limit_mb' = '200')

-- Reduce window size for temporal joins  
WITHIN INTERVAL '1' MINUTE  -- instead of '1' HOUR
```

#### **5. Performance Issues**

**Symptoms:**
- Low throughput (<1K records/sec)
- High latency (>5s)
- Processing lag growing

**Diagnostic Steps:**
```bash
# Check job metrics
curl http://localhost:8080/api/jobs/my-job/metrics

# Monitor resource usage
top -p $(pgrep velo-sql-multi)

# Check Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group velo-streams-my-job
```

**Solutions:**
- Increase batch sizes for higher throughput
- Use simple processing mode instead of transactional
- Add more parallel consumer instances  
- Optimize SQL queries (add WHERE clauses, efficient JOINs)

#### **6. Transaction Failures**
```
Error: Job failed (transactional): Failed to commit transactions: Sink 'kafka_sink' transaction failed
```

**Causes:**
- Not all sources/sinks support transactions
- Network issues during commit phase
- Configuration mismatch between transactional settings

**Solutions:**
- Verify transaction support: Check source/sink capabilities
- Use appropriate failure strategy for transactional processing
- Consider using simple mode for non-critical data

### Debugging Tools

#### **Log Analysis**
```bash
# Filter multi-source logs
grep "multi-source\|multi.*job" /var/log/velo-streams.log

# Monitor source creation
grep "Creating.*source\|Successfully created.*source" /var/log/velo-streams.log

# Check sink status
grep "sink.*created\|sink.*failed" /var/log/velo-streams.log
```

#### **Metrics Monitoring**
```bash  
# Job status
curl -s http://localhost:8080/api/jobs | jq '.[] | {name, status, sources, sinks}'

# Detailed metrics
curl -s http://localhost:8080/api/jobs/my-job/metrics | jq .

# Prometheus metrics
curl -s http://localhost:8080/metrics | grep velo_job
```

#### **SQL Query Analysis**
```rust
// Test query parsing
let parser = StreamingSqlParser::new();
let query = parser.parse("YOUR SQL HERE")?;

let analyzer = QueryAnalyzer::new("test".to_string());
let analysis = analyzer.analyze(&query)?;

println!("Sources: {}", analysis.required_sources.len());
println!("Sinks: {}", analysis.required_sinks.len());
```

### Getting Help

#### **Community Support**
- GitHub Issues: Report bugs and request features
- Documentation: Check latest docs for configuration examples
- Performance Tuning: Benchmark your specific use case

#### **Enterprise Support**
- Professional Services: Architecture consulting and optimization
- Custom Development: Specialized source/sink implementations
- 24/7 Support: Production deployment assistance

---

## Conclusion

Velostream' multi-source/multi-sink processing enables sophisticated streaming analytics pipelines that can:

- **Process data from unlimited sources simultaneously**
- **Route output to multiple destinations concurrently**  
- **Provide ACID transaction guarantees across heterogeneous systems**
- **Scale to handle enterprise-level data volumes**
- **Maintain high availability with graceful error handling**

This comprehensive guide covers the essential aspects of building production-ready multi-source streaming applications. For specific use cases or advanced configurations, refer to the individual component documentation and example applications.