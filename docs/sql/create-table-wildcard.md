# CREATE TABLE Wildcard Support

## Overview

Velostream fully supports SQL-standard wildcard (`*`) in CREATE TABLE AS SELECT statements, enabling automatic field discovery from source streams and tables.

## ⚠️ Important Limitations

### Currently Supported Data Sources
Velostream currently supports only:
- **Kafka** streams and topics
- **File** sources (JSON, CSV, Parquet)

### Planned Future Support
The following sources shown in examples are **planned but not yet implemented**:
- PostgreSQL/MySQL databases
- S3/Cloud storage
- ClickHouse
- Iceberg tables

These SQL/database sources would require specific query syntax when implemented.

## ✅ Supported Syntax

### Basic Wildcard Selection

#### From Kafka Sources
```sql
-- Select all fields from a Kafka stream
CREATE TABLE user_profiles AS
SELECT * FROM kafka_users;

-- With Kafka configuration
CREATE TABLE events AS
SELECT * FROM kafka://localhost:9092/events-topic
WITH ("auto.offset.reset" = "latest");
```

#### From File Sources
```sql
-- Select all fields from JSON files
CREATE TABLE customer_data AS
SELECT * FROM file:///data/customers.json
WITH ("file.format" = "json");

-- Select all fields from CSV files
CREATE TABLE sales_data AS
SELECT * FROM file:///data/sales.csv
WITH (
    "file.format" = "csv",
    "header" = "true"
);

-- Select all fields from multiple files with pattern
CREATE TABLE log_data AS
SELECT * FROM file:///logs/app-*.json
WITH (
    "file.format" = "json",
    "watch" = "true"  -- Monitor for new files
);
```

#### From SQL/Database Sources
```sql
-- Select all fields from PostgreSQL
CREATE TABLE user_profiles AS
SELECT * FROM postgresql://localhost/mydb?table=users
WITH ("cdc" = "true");  -- Enable Change Data Capture

-- Select all fields from MySQL
CREATE TABLE orders_snapshot AS
SELECT * FROM mysql://db.example.com/shop?table=orders;

-- Select all fields from ClickHouse
CREATE TABLE analytics_data AS
SELECT * FROM clickhouse://analytics:8123/metrics?table=events;
```

#### From S3/Cloud Storage
```sql
-- Select all fields from S3 Parquet files
CREATE TABLE historical_data AS
SELECT * FROM s3://data-lake/events/*.parquet
WITH (
    "region" = "us-west-2",
    "format" = "parquet"
);

-- Select all fields from S3 JSON files
CREATE TABLE user_events AS
SELECT * FROM s3://backup/logs/2024-01-*/*.json.gz
WITH (
    "compression" = "gzip",
    "format" = "json"
);
```

#### From Iceberg Tables
```sql
-- Select all fields from Iceberg table
CREATE TABLE warehouse_data AS
SELECT * FROM iceberg://catalog/namespace/products
WITH (
    "warehouse" = "s3://warehouse/",
    "format" = "parquet"
);
```

### Wildcard with Filtering
```sql
-- Select all fields but filter rows
CREATE TABLE high_value_orders AS
SELECT * FROM orders
WHERE amount > 1000;

-- Select all fields with complex conditions
CREATE TABLE active_users AS
SELECT * FROM users
WHERE last_login > NOW() - INTERVAL '30' DAY
  AND status = 'active';
```

### Wildcard with Joins
```sql
-- Select all fields from joined tables
CREATE TABLE enriched_orders AS
SELECT * FROM orders o
JOIN users u ON o.user_id = u.id;

-- Note: This includes all fields from both tables
```

### Wildcard with Aggregations
```sql
-- Wildcard in subqueries
CREATE TABLE user_summaries AS
SELECT user_id, order_count, total_amount
FROM (
    SELECT *, COUNT(*) as order_count, SUM(amount) as total_amount
    FROM orders
    GROUP BY user_id
);
```

## 🎯 Field Discovery

When using `SELECT *`, Velostream automatically:
1. **Discovers all fields** from the source stream/table
2. **Preserves field names** exactly as they appear in the source
3. **Maintains field types** from the source schema
4. **Handles nested structures** (if present in source)

## 📋 WITH Clause Configuration

### Combining Wildcard with Properties
```sql
-- Wildcard with Kafka configuration
CREATE TABLE real_time_data AS
SELECT * FROM kafka_stream
WITH (
    "bootstrap.servers" = "localhost:9092",
    "auto.offset.reset" = "latest",
    "group.id" = "table-consumer-group"
);

-- Wildcard with retention settings
CREATE TABLE analytics_table AS
SELECT * FROM source_stream
WITH (
    "retention" = "7 days",
    "kafka.batch.size" = "2000"
);
```

## 🔧 Implementation Details

### Parser Support
The SQL parser recognizes `SelectField::Wildcard` when it encounters `*` in the SELECT clause:

```rust
// Parser implementation (simplified)
fn parse_select_fields(&mut self) -> Result<Vec<SelectField>, SqlError> {
    if self.current_token().token_type == TokenType::Asterisk {
        fields.push(SelectField::Wildcard);
    }
    // ... handle other field types
}
```

### Field Resolution
At execution time, the wildcard is expanded to include all fields from the source:
1. Source schema is examined
2. All field definitions are extracted
3. Fields are materialized in the resulting table

## ⚡ Performance Considerations

### Advantages
- **Simplicity**: No need to manually list all fields
- **Maintenance**: Automatically adapts to schema changes
- **Development Speed**: Faster prototyping and development

### Trade-offs
- **Memory**: All fields are stored, even if not all are used
- **Network**: All fields are transmitted over the network
- **Storage**: Table stores all fields from source

### Best Practices
```sql
-- For production, consider explicit field selection if you only need specific fields
CREATE TABLE user_summary AS
SELECT user_id, name, email, last_login  -- Only needed fields
FROM users;

-- Use wildcard for development/prototyping or when all fields are needed
CREATE TABLE user_backup AS
SELECT * FROM users;  -- Full backup scenario
```

## 🧪 Testing Examples

### Unit Test Example
```rust
#[test]
fn test_create_table_with_wildcard() {
    let parser = StreamingSqlParser::new();
    let query = parser.parse("CREATE TABLE test AS SELECT * FROM source").unwrap();

    match query {
        StreamingQuery::CreateTable { fields, .. } => {
            assert!(fields.contains(&SelectField::Wildcard));
        }
        _ => panic!("Expected CreateTable query"),
    }
}
```

### Integration Test Example
```sql
-- Test data ingestion with wildcard
CREATE TABLE test_table AS SELECT * FROM test_stream;

-- Verify all fields are present
SELECT COUNT(*) FROM test_table;  -- Should match source record count
```

## 📊 Production Usage

### Real-world Examples

#### Financial Data Integration
```sql
-- Kafka: Real-time market data
CREATE TABLE market_data_backup AS
SELECT * FROM kafka://broker:9092/market-data
WITH ("auto.offset.reset" = "earliest");

-- PostgreSQL: Customer accounts with CDC
CREATE TABLE customer_accounts AS
SELECT * FROM postgresql://db/finance?table=accounts
WITH ("cdc" = "true");

-- S3: Historical trade data
CREATE TABLE historical_trades AS
SELECT * FROM s3://finance-data/trades/2024/*.parquet
WITH ("region" = "us-east-1");
```

#### Log Processing
```sql
-- File: Application logs with monitoring
CREATE TABLE application_logs AS
SELECT * FROM file:///var/log/app/*.json
WITH (
    "watch" = "true",
    "format" = "json"
);

-- S3: Archived logs
CREATE TABLE archived_logs AS
SELECT * FROM s3://logs-archive/2024-*/*.json.gz
WHERE log_level IN ('ERROR', 'WARN')
WITH ("compression" = "gzip");
```

#### Analytics Pipelines
```sql
-- ClickHouse: Analytics events
CREATE TABLE analytics_events AS
SELECT * FROM clickhouse://analytics:8123/events?table=user_events;

-- Iceberg: Data warehouse facts
CREATE TABLE fact_sales AS
SELECT * FROM iceberg://warehouse/sales/fact_sales
WITH ("snapshot_id" = "123456789");

-- CSV: Batch data ingestion
CREATE TABLE batch_upload AS
SELECT * FROM file:///uploads/data-*.csv
WITH (
    "format" = "csv",
    "header" = "true",
    "recursive" = "true"
);
```

#### IoT and Streaming
```sql
-- Kafka: Sensor data
CREATE TABLE sensor_readings AS
SELECT * FROM kafka://iot-cluster/sensors
WITH ("auto.offset.reset" = "latest");

-- PostgreSQL: Device registry
CREATE TABLE device_registry AS
SELECT * FROM postgresql://iot-db/devices?table=registry;

-- File: Local sensor cache
CREATE TABLE sensor_cache AS
SELECT * FROM file:///tmp/sensors/*.json
WITH ("watch" = "true");
```

## 🚀 Future Enhancements

### Planned Features
1. **Selective Wildcard**: `SELECT * EXCEPT (field1, field2)`
2. **Renamed Wildcard**: `SELECT *, field AS new_name`
3. **Qualified Wildcard**: `SELECT t1.*, t2.specific_field FROM t1 JOIN t2`

## 📋 Table Update Behavior

### Kafka Table Updates
Tables created from Kafka sources continuously consume from the topic:
- **Continuous Updates**: Tables automatically update as new messages arrive
- **Offset Tracking**: Uses Kafka consumer groups to track position
- **Resumption**: Can resume from last position after restart (uses consumer group offset)
- **Incremental Loading**: Yes, via Kafka's built-in offset management

### File Table Updates
File-based tables have different update patterns:
- **Static by Default**: File tables load once at creation
- **Watch Mode**: With `"watch" = "true"`, monitors for new files
- **No Incremental Updates**: Files are processed completely each time
- **Pattern Matching**: Can process new files matching patterns (e.g., `*.json`)

### SQL/Database Sources (Future)
When implemented, SQL sources would likely:
- **CDC (Change Data Capture)**: For incremental updates from databases
- **Polling**: Periodic queries to fetch new/changed records
- **Two-Query Pattern**: Initial full load + incremental change queries

## 🔄 Error Handling

### Non-existent Kafka Topics
When a Kafka topic doesn't exist:
- **Current Behavior**: Connection error is returned immediately
- **No Auto-Retry**: Does not wait for topic creation
- **Manual Recovery**: User must create topic and recreate table

### File Source Errors
- **Missing Files**: Error returned if file doesn't exist
- **Watch Mode**: Continues monitoring even if initial files missing
- **Pattern Matching**: Empty result set if no files match pattern

## 🎯 AUTO_OFFSET Configuration

### For Kafka Tables (NEW!)
```sql
-- Start from latest messages (real-time mode)
CREATE TABLE real_time_data AS
SELECT * FROM kafka_topic
WITH ("auto.offset.reset" = "latest");

-- Start from beginning (historical + real-time)
CREATE TABLE historical_data AS
SELECT * FROM kafka_topic
WITH ("auto.offset.reset" = "earliest");  -- Default

-- Default behavior (earliest)
CREATE TABLE my_table AS
SELECT * FROM kafka_topic;  -- Uses earliest
```

## Summary

The wildcard (`*`) operator in CREATE TABLE AS SELECT statements is fully supported in Velostream for **Kafka and File sources**, providing:
- ✅ Automatic field discovery
- ✅ Schema preservation
- ✅ Type safety
- ✅ Configurable offset reset (Kafka)
- ✅ Continuous updates (Kafka) or watch mode (Files)
- ⚠️ Currently limited to Kafka and File sources

This feature simplifies table creation and ensures complete data capture from supported source streams and files.