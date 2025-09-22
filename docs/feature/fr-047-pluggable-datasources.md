# Feature Request: Pluggable Input/Output Data Sources Architecture

## 🎯 Summary

Enable Velostream SQL engine to work with multiple data sources beyond Kafka, including files (JSON/Parquet/CSV), cloud storage (S3/GCS/Azure), table formats (Iceberg/Delta Lake), and database CDC streams.

## Problem Statement

Currently, Velostream is tightly coupled to Kafka as its only data source. Modern data architectures require:
- Processing data from data lakes (S3, GCS, Azure Blob)
- Working with table formats (Apache Iceberg, Delta Lake, Hudi)
- File-based batch processing (JSON, Parquet, CSV)
- Database change data capture (CDC) processing
- Hybrid real-time + batch analytics

## Proposed Solution

### Core Architecture

Implement a pluggable data source architecture using Rust traits:

```rust
pub trait DataSource: Send + Sync {
    type Record: StreamRecord;
    
    async fn connect(&mut self, config: &DataSourceConfig) -> Result<SchemaInfo>;
    async fn stream(&mut self) -> Pin<Box<dyn Stream<Item = Result<Self::Record>>>>;
    async fn read_batch(&mut self, query: Option<&str>) -> Result<Vec<Self::Record>>;
    async fn poll_updates(&mut self, watermark: Option<&str>) -> Result<Vec<Self::Record>>;
}

pub trait DataSink: Send + Sync {
    type Record: StreamRecord;
    
    async fn connect(&mut self, config: &DataSinkConfig) -> Result<()>;
    async fn write_batch(&mut self, records: Vec<Self::Record>) -> Result<()>;
    async fn update(&mut self, key: &str, record: Self::Record) -> Result<()>;
    async fn delete(&mut self, key: &str) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
}
```

### Supported Data Sources

**Phase 1 - File Systems**
- Local files: `file://path/to/data.parquet`
- JSON Lines, Parquet, CSV, Avro formats
- Compression support (gzip, snappy, zstd)

**Phase 2 - Cloud Storage**
- S3: `s3://bucket/prefix/*.parquet`
- Google Cloud Storage: `gs://bucket/prefix/`
- Azure Blob: `azure://container/path/`

**Phase 3 - Table Formats**
- Apache Iceberg: `iceberg://catalog.database.table`
- Delta Lake: `delta://path/to/table`
- Apache Hudi: `hudi://path/to/table`

**Phase 4 - Database CDC**
- PostgreSQL: `postgres-cdc://host:5432/database`
- MySQL: `mysql-cdc://host:3306/database`
- MongoDB: `mongodb-cdc://connection-string`

## Usage Examples - Heterogeneous Data Flow

### Example 1: Kafka to Data Lake (Stream to Batch)
```sql
-- Read from Kafka, write to S3 Parquet files
SELECT 
  customer_id,
  transaction_id,
  amount,
  timestamp
FROM 'kafka://localhost:9092/transactions'
WHERE amount > 100.00
INTO 's3://data-lake/high-value-transactions/date={YYYY-MM-DD}/*.parquet'
WITH (
  format = 'parquet',
  compression = 'snappy',
  partition_by = 'date',
  batch_size = 10000,
  batch_timeout = '5 minutes'
);
```

### Example 2: S3 to Kafka (Batch to Stream)
```sql
-- Read historical data from S3, stream to Kafka for real-time processing
SELECT 
  user_id,
  event_type,
  properties,
  CURRENT_TIMESTAMP as replay_timestamp
FROM 's3://warehouse/user-events/2024-01/*.parquet'
WHERE event_type IN ('purchase', 'signup')
INTO 'kafka://localhost:9092/replayed-events'
WITH (
  rate_limit = 1000,  -- messages per second
  preserve_timestamps = false
);
```

### Example 3: Database CDC to Iceberg (Change Capture to Table Format)
```sql
-- Capture PostgreSQL changes and maintain Iceberg table
SELECT 
  CASE 
    WHEN operation_type = 'DELETE' THEN 'D'
    ELSE 'U'
  END as _action,
  after->>'id' as id,
  after->>'customer_id' as customer_id,
  after->>'status' as status,
  after->>'updated_at' as updated_at
FROM 'postgres-cdc://localhost:5432/production'
WHERE table_name = 'orders'
INTO 'iceberg://catalog.warehouse.orders_mirror'
WITH (
  merge_on = 'id',
  delete_on = '_action = "D"'
);
```

### Example 4: File Watch to Multiple Sinks
```sql
-- Watch directory for new CSV files, write to both Kafka and S3
SELECT 
  JSON_OBJECT(
    'filename', _metadata.filename,
    'row_number', _metadata.row_number,
    'customer_id', customer_id,
    'amount', CAST(amount as DECIMAL(10,2))
  ) as enriched_record
FROM 'file:///data/incoming/*.csv'
WITH (watch = true, delete_after_read = true)
INTO 
  'kafka://localhost:9092/csv-ingestion',
  's3://archive/csv-backup/{YYYY}/{MM}/{DD}/*.json';
```

### Example 5: Multi-Source Join to Delta Lake
```sql
-- Join real-time Kafka with S3 historical data, write to Delta Lake
SELECT 
  k.order_id,
  k.customer_id,
  k.amount,
  h.customer_segment,
  h.lifetime_value,
  i.current_inventory
FROM 'kafka://localhost:9092/orders' k
LEFT JOIN 's3://warehouse/customers/*.parquet' h
  ON k.customer_id = h.customer_id
LEFT JOIN 'iceberg://catalog.inventory.current_stock' i  
  ON k.product_id = i.product_id
WHERE k.amount > 500.00
INTO 'delta://lakehouse/enriched_orders/'
WITH (
  mode = 'append',
  optimize_write = true
);
```

### Example 6: Redis to PostgreSQL (Cache to Database)
```sql
-- Read from Redis streams, persist to PostgreSQL
SELECT 
  JSON_EXTRACT(data, '$.user_id') as user_id,
  JSON_EXTRACT(data, '$.action') as action,
  JSON_EXTRACT(data, '$.timestamp') as event_time,
  CURRENT_TIMESTAMP as processed_at
FROM 'redis://localhost:6379/user-activity-stream'
INTO 'postgresql://localhost:5432/analytics.user_events'
WITH (
  batch_size = 1000,
  on_conflict = 'IGNORE'
);
```

## Configuration

### Basic Configuration Structure

```yaml
# velostream-config.yaml
data_sources:
  orders_kafka:
    type: kafka
    brokers: "localhost:9092"
    topic: "orders"
    
  orders_historical:
    type: s3
    bucket: "data-lake"
    prefix: "orders/"
    format: parquet
    region: "us-west-2"
    
  customer_analytics:
    type: iceberg
    catalog_uri: "http://localhost:8181"
    warehouse: "s3://warehouse/"
    table: "customer_metrics"
```

### Configuration Inheritance with `extends`

Velostream supports DRY configuration management through the `extends` keyword, enabling configuration inheritance and reducing duplication.

#### Base Configuration Files

```yaml
# configs/common_kafka_source.yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap_servers: "broker:9092"
    auto_offset_reset: "latest"
    enable_auto_commit: true
    auto_commit_interval_ms: 100
    session_timeout_ms: 6000
    heartbeat_interval_ms: 2000
  schema:
    key_format: string
    value_format: avro
    schema_registry_url: "http://schema-registry:8081"
  stream.config:
    replication_factor: 3
    cleanup_policy: "delete"

# Performance profiles for different use cases
performance_profiles:
  ultra_low_latency:
    fetch_max_wait_ms: 10
    max_poll_records: 1000
    buffer_size: 65536
  high_throughput:
    fetch_max_wait_ms: 50
    max_poll_records: 2000
    buffer_size: 131072
  balanced:
    fetch_max_wait_ms: 20
    max_poll_records: 1000
    buffer_size: 65536
```

```yaml
# configs/common_kafka_sink.yaml
datasink:
  type: kafka
  producer_config:
    bootstrap_servers: "broker:9092"
    retries: 3
    batch_size: 16384
    buffer_memory: 33554432
  schema:
    key_format: string
    value_format: avro
    schema_registry_url: "http://schema-registry:8081"
  topic_config:
    replication_factor: 3
    cleanup_policy: "delete"
    segment_ms: 3600000

# Delivery profiles for different alert priorities
delivery_profiles:
  critical:
    acks: "all"
    linger_ms: 0
    delivery_guarantee: "exactly_once"
  high_priority:
    acks: "1"
    linger_ms: 1
    delivery_guarantee: "at_least_once"
  medium_priority:
    acks: "1"
    linger_ms: 5
    delivery_guarantee: "at_least_once"
```

#### Topic-Specific Configurations

```yaml
# configs/market_data_topic.yaml
extends: common_kafka_source.yaml

topic:
  name: "market_data"
performance_profile: ultra_low_latency
schema:
  key.field: symbol
  schema_file: "schemas/market_data.avsc"
topic_config:
  partitions: 12
  retention_ms: 86400000
metadata:
  description: "High-frequency trading market data feed"
  update_frequency: "microseconds"
```

```yaml
# configs/risk_alerts_topic.yaml
extends: common_kafka_sink.yaml

topic:
  name: "risk_alerts"
delivery_profile: critical
schema:
  key.field: trader_id
  schema_file: "schemas/risk_alerts.avsc"
topic_config:
  partitions: 8
  retention_ms: 2592000000  # 30 days for compliance
headers:
  alert_type: "risk_management"
  regulatory_requirement: "true"
integrations:
  email_alerts:
    enabled: true
    recipients: ["risk-team@trading-firm.com"]
  slack_notifications:
    enabled: true
    channel: "#risk-management"
```

### Configuration Inheritance Features

#### 1. **Hierarchical Inheritance**
- **Single Level**: `derived.yaml extends base.yaml`
- **Multi-Level**: `specialized.yaml extends derived.yaml extends base.yaml`
- **Circular Detection**: Prevents infinite inheritance loops

#### 2. **Merge Strategy**
- **Objects**: Recursive merge with derived values overriding base
- **Arrays**: Derived arrays completely replace base arrays
- **Primitives**: Derived values override base values

#### 3. **Path Resolution**
- **Relative Paths**: Resolved relative to the extending config file
- **Absolute Paths**: Used as-is
- **Cache Management**: Loaded configs are cached to prevent re-loading

#### 4. **Validation**
- **Schema Validation**: Final merged config is validated against schema
- **Circular Dependency Detection**: Prevents inheritance loops
- **Path Validation**: Ensures all extended files exist and are readable

### Usage in SQL

```sql
INSERT INTO risk_alerts
SELECT trader_id, position_value, risk_status
FROM trading_positions 
WHERE position_value > 1000000
WITH (
    source_config = 'configs/trading_positions_topic.yaml',
    sink_config = 'configs/risk_alerts_topic.yaml'
);
```

### Benefits of Configuration Inheritance

1. **DRY Principle**: Common settings defined once, referenced everywhere
2. **Maintainability**: Change bootstrap servers in one place, affects all configs  
3. **Consistency**: Ensures uniform settings across all topics
4. **Flexibility**: Override specific settings while inheriting common ones
5. **Scalability**: Easy to add new topics without duplicating infrastructure settings

## Benefits

### Technical Benefits
- **Unified SQL Interface**: One query language for all data sources
- **Performance**: Predicate pushdown, column pruning, partition elimination
- **Schema Evolution**: Automatic schema discovery and evolution
- **Transactional**: ACID properties where supported

### Business Impact
- **Data Lake Analytics**: Compete with Spark/Flink for analytics workloads
- **Hybrid Processing**: Bridge batch and streaming analytics
- **Cost Optimization**: Choose optimal storage for each use case
- **Vendor Independence**: Avoid lock-in to specific platforms

## Implementation Plan

| Phase | Timeline | Deliverables |
|-------|----------|--------------|
| 1. Core Architecture | 4-6 weeks | Trait definitions, refactored Kafka implementation |
| 2. File Systems | 3-4 weeks | JSON/Parquet/CSV support, file watching |
| 3. Cloud Storage | 4-5 weeks | S3/GCS/Azure support, IAM integration |
| 4. Table Formats | 6-8 weeks | Iceberg/Delta Lake, schema evolution |
| 5. Database CDC | 5-6 weeks | PostgreSQL/MySQL CDC, offset management |

## Backward Compatibility

Existing Kafka queries continue to work:
```sql
-- Current syntax (still works)
SELECT * FROM orders WHERE amount > 100;

-- New explicit syntax (also works)
SELECT * FROM 'kafka://localhost:9092/orders' WHERE amount > 100;
```

## Success Criteria

- ✅ Multi-source SQL queries work seamlessly
- ✅ Performance comparable to native implementations
- ✅ Schema discovery and evolution support
- ✅ Transactional writes where supported
- ✅ Comprehensive error handling and retry logic
- ✅ Production-ready monitoring and metrics

## Questions for Discussion

1. **Priority**: Which data sources should be implemented first?
2. **API Design**: Feedback on the trait-based architecture?
3. **Performance**: What are acceptable latency/throughput targets?
4. **Integration**: Specific requirements for your use cases?

## References

- Apache Arrow for columnar processing
- DataFusion for query engine integration
- Object Store crate for cloud storage
- Apache Iceberg Rust bindings

---

This feature would position Velostream as a universal SQL streaming engine, enabling it to process data from any source to any destination. Looking forward to community feedback and contributions!