# Core Data Sources Specification

## Overview

VeloStream will support six core data sources as first-class citizens:

1. **PostgreSQL** - Relational database with CDC capabilities
2. **S3** - Object storage for data lake architectures  
3. **File** - Local and network file systems
4. **Iceberg** - Modern table format for analytics
5. **ClickHouse** - Columnar OLAP database for analytics
6. **Kafka** - Distributed streaming platform (existing)

## Data Source Specifications

### 1. PostgreSQL

**URI Format**: `postgresql://[user[:password]@]host[:port]/database[?params]`

**Parameters**:
- `table` - Target table name
- `cdc` - Enable Change Data Capture (true/false)
- `schema` - Database schema (default: public)
- `batch_size` - Records per batch
- `ssl_mode` - SSL connection mode

**Capabilities**:
- ✅ Source: Query results, CDC events
- ✅ Sink: Insert, Update, Delete, Upsert
- ✅ Transactions: Full ACID support
- ✅ Schema Evolution: ALTER TABLE support

**Example URIs**:
```
postgresql://localhost/mydb?table=orders
postgresql://user:pass@db.example.com:5432/prod?cdc=true&schema=sales
```

### 2. S3

**URI Format**: `s3://bucket/prefix[/*.{format}][?params]`

**Parameters**:
- `region` - AWS region
- `format` - File format (parquet, json, csv, avro)
- `compression` - Compression type (gzip, snappy, zstd)
- `partition_by` - Partition columns
- `access_key_id` - AWS access key (or use env/IAM)
- `secret_access_key` - AWS secret key (or use env/IAM)

**Capabilities**:
- ✅ Source: Batch file reading
- ✅ Sink: Batch file writing
- ✅ Formats: Parquet, JSON, CSV, Avro
- ✅ Partitioning: Hive-style partitions

**Example URIs**:
```
s3://data-lake/events/*.parquet?region=us-west-2
s3://backup/logs/2024-01-*/*.json.gz?compression=gzip
```

### 3. File

**URI Format**: `file://[host]/path[/*.{format}][?params]`

**Parameters**:
- `format` - File format (json, csv, parquet, etc.)
- `watch` - Watch for new files (true/false)
- `recursive` - Scan subdirectories
- `encoding` - Character encoding (utf-8, etc.)
- `header` - CSV has header row

**Capabilities**:
- ✅ Source: Single/multiple file reading
- ✅ Sink: File writing with rotation
- ✅ Watch Mode: Monitor for new files
- ✅ Formats: JSON, CSV, Parquet, Avro, Text

**Example URIs**:
```
file:///data/input/*.csv?header=true
file:///logs/app.log?watch=true&format=json
```

### 4. Iceberg

**URI Format**: `iceberg://catalog/namespace/table[?params]`

**Parameters**:
- `catalog_uri` - Catalog location (REST, Hive, Glue)
- `warehouse` - Warehouse location
- `snapshot_id` - Read specific snapshot
- `branch` - Table branch
- `format` - Data file format (parquet, avro, orc)

**Capabilities**:
- ✅ Source: Table scans, incremental reads
- ✅ Sink: Append, Overwrite, Upsert
- ✅ ACID: Full transaction support
- ✅ Time Travel: Historical snapshots
- ✅ Schema Evolution: Full support

**Example URIs**:
```
iceberg://catalog/sales/orders
iceberg://rest-catalog/analytics/events?branch=staging
```

### 5. ClickHouse

**URI Format**: `clickhouse://[user[:password]@]host[:port]/database[?params]`

**Parameters**:
- `table` - Target table name
- `format` - Data format (Native, TabSeparated, JSON, Parquet)
- `compression` - Compression algorithm (lz4, gzip, zstd)
- `batch_size` - Records per batch
- `optimize` - Run OPTIMIZE after insert
- `engine` - Table engine (MergeTree, ReplacingMergeTree, etc.)

**Capabilities**:
- ✅ Source: High-speed analytical queries
- ✅ Sink: Bulk inserts, streaming inserts
- ✅ Columnar: Optimized for analytics
- ✅ Compression: Excellent compression ratios
- ✅ Aggregations: Built-in aggregation functions

**Example URIs**:
```
clickhouse://localhost:8123/analytics?table=events
clickhouse://user:pass@ch.example.com:9000/warehouse?table=facts&compression=zstd
```

### 6. Kafka

**URI Format**: `kafka://broker1[:port][,broker2[:port]]/topic[?params]`

**Parameters**:
- `group_id` - Consumer group ID
- `offset` - Starting offset (earliest, latest, timestamp)
- `format` - Message format (json, avro, protobuf)
- `security_protocol` - SASL_SSL, SSL, PLAINTEXT
- `schema_registry` - Schema registry URL

**Capabilities**:
- ✅ Source: Real-time streaming
- ✅ Sink: Real-time publishing
- ✅ Formats: JSON, Avro, Protobuf
- ✅ Exactly-once: Transaction support
- ✅ Schema Registry: Integration

**Example URIs**:
```
kafka://localhost:9092/events?group_id=processor-1
kafka://broker1:9092,broker2:9092/orders?format=avro
```

## Implementation Priority

### Phase 1 (Days 3-5)
1. **Kafka** - Adapt existing implementation
2. **File** - Simple local file support

### Phase 2 (Days 6-7)  
3. **ClickHouse** - OLAP database integration
4. **PostgreSQL** - Basic query/insert support

### Phase 3 (Days 8-10)
5. **S3** - Object storage integration
6. **Iceberg** - Table format support
7. **PostgreSQL CDC** - Change data capture

## Common Interfaces

All data sources implement these core traits:

```rust
// Source capabilities
trait DataSource {
    async fn fetch_schema(&self) -> Result<Schema>;
    async fn create_reader(&self) -> Result<Box<dyn DataReader>>;
    fn supports_streaming(&self) -> bool;
    fn supports_batch(&self) -> bool;
}

// Sink capabilities
trait DataSink {
    async fn validate_schema(&self, schema: &Schema) -> Result<()>;
    async fn create_writer(&self) -> Result<Box<dyn DataWriter>>;
    fn supports_transactions(&self) -> bool;
    fn supports_upsert(&self) -> bool;
}
```

## Configuration Examples

### SQL Usage

```sql
-- PostgreSQL CDC to Kafka
CREATE STREAM orders_stream AS
SELECT * FROM postgresql('localhost/shop?table=orders&cdc=true')
INSERT INTO kafka('localhost:9092/orders-events');

-- Kafka to ClickHouse Analytics
CREATE TABLE analytics AS  
SELECT * FROM kafka('localhost:9092/events?group_id=analytics')
INSERT INTO clickhouse('localhost:8123/warehouse?table=events');

-- S3 to Iceberg
CREATE TABLE data_lake AS
SELECT * FROM s3('data-lake/raw/*.parquet')
INSERT INTO iceberg('catalog/analytics/processed_events');

-- File monitoring to ClickHouse
CREATE STREAM file_monitor AS
SELECT * FROM file('/data/uploads/*.json?watch=true')
INSERT INTO clickhouse('localhost:8123/warehouse?table=uploads');
```

### Programmatic Usage

```rust
// Create heterogeneous pipeline
let kafka = create_source("kafka://localhost:9092/events?group_id=analytics")?;
let clickhouse = create_sink("clickhouse://localhost:8123/warehouse?table=events")?;

// Process streaming events to ClickHouse
let reader = kafka.create_reader().await?;
let writer = clickhouse.create_writer().await?;

while let Some(record) = reader.read().await? {
    writer.write(record).await?;
}
```

## Testing Strategy

Each data source requires:
1. Unit tests for URI parsing
2. Integration tests with test containers
3. Performance benchmarks
4. Error handling validation
5. Schema evolution tests

## Dependencies

### Required Crates
- `tokio-postgres` - PostgreSQL async driver
- `clickhouse` - ClickHouse async client
- `aws-sdk-s3` - S3 client
- `iceberg-rust` - Iceberg table format
- `rdkafka` - Kafka client (existing)
- `notify` - File system monitoring

### Optional Crates
- `arrow-rs` - Columnar data processing
- `parquet` - Parquet file support
- `csv` - CSV parsing
- `serde_json` - JSON support