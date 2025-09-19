# Complete SQL Syntax Reference

Comprehensive reference for all Velostream SQL features, organized by category.

> **⚠️ Important**: All queries require CREATE STREAM statements with WITH clauses. See [Quick Start](../quickstart/hello-world.md) for complete examples.

## Core Query Structure

### Basic Pattern
```sql
-- 1. Define input stream
CREATE STREAM input_stream WITH (
    topic = 'input-topic',
    bootstrap.servers = 'localhost:9092',
    value.deserializer = 'json'
);

-- 2. Define output stream
CREATE STREAM output_stream WITH (
    topic = 'output-topic',
    bootstrap.servers = 'localhost:9092',
    value.serializer = 'json'
);

-- 3. Process data
INSERT INTO output_stream
SELECT columns...
FROM input_stream
WHERE conditions...;
```

### Alternative: Configuration Files (Recommended)
```sql
CREATE STREAM input_stream WITH (config_file = 'configs/input.yaml');
CREATE STREAM output_stream WITH (config_file = 'configs/output.yaml');
INSERT INTO output_stream SELECT * FROM input_stream WHERE condition;
```

## Query Types

### SELECT Queries
- **Basic Selection**: [Quick Start Guide](../quickstart/hello-world.md)
- **Filtering Data**: [Filter Data](../by-task/filter-data.md)
- **Transformations**: [Transform Data](../by-task/transform-data.md)

### Aggregation Queries
- **GROUP BY**: [Aggregate Data](../by-task/aggregate-data.md) | [GROUP BY Reference](./group-by.md)
- **Window Functions**: [Window Analysis](../by-task/window-analysis.md)
- **EMIT Modes**: [EMIT Reference](./emit-modes.md)

### JOIN Queries
- **Stream Joins**: [Join Streams](../by-task/join-streams.md)
- **Advanced Patterns**: [Pattern Detection](../by-task/detect-patterns.md)

## Operators and Expressions

### Comparison Operators
| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `status = 'active'` |
| `!=`, `<>` | Not equal | `status != 'cancelled'` |
| `>` | Greater than | `amount > 100.0` |
| `>=` | Greater than or equal | `price >= 50.0` |
| `<` | Less than | `quantity < 10` |
| `<=` | Less than or equal | `discount <= 0.2` |

### Logical Operators
| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Both conditions true | `price > 10 AND category = 'books'` |
| `OR` | At least one condition true | `status = 'urgent' OR priority = 'high'` |
| `NOT` | Negates condition | `NOT (status = 'cancelled')` |

### Special Operators
| Operator | Description | Example |
|----------|-------------|---------|
| `IN` | Value in list | `status IN ('pending', 'active')` |
| `BETWEEN` | Value in range | `price BETWEEN 10 AND 50` |
| `LIKE` | Pattern matching | `name LIKE 'John%'` |
| `IS NULL` | Null check | `email IS NOT NULL` |

## Functions

### Built-in Functions
- **Essential Functions**: [Essential Functions](../functions/essential.md)
- **Math Functions**: [Math Functions](../functions/math.md)
- **String Functions**: [String Functions](../functions/string.md)
- **Date/Time Functions**: [Date/Time Functions](../functions/date-time.md)
- **JSON Functions**: [JSON Functions](../functions/json.md)

### Aggregation Functions
- **Standard Aggregations**: [Aggregation Functions](../functions/aggregation.md)
- **Window Functions**: [Window Functions](../functions/window.md)
- **Advanced Functions**: [Advanced Functions](../functions/advanced.md)

## Data Types

### Supported Types
- **Numbers**: `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **Text**: `STRING`, `VARCHAR`
- **Date/Time**: `TIMESTAMP`, `DATE`, `TIME`
- **Boolean**: `BOOLEAN`
- **JSON**: Native JSON support with functions
- **Binary**: `BYTES`

### Type Conversion
```sql
-- Explicit casting
SELECT
    CAST(amount AS STRING) as amount_str,
    CAST('123.45' AS DECIMAL) as parsed_decimal,
    CAST(timestamp_col AS DATE) as date_only
FROM transactions;
```

## Advanced Features

### System Columns
All streams automatically include:
- `_timestamp` - Processing timestamp
- `_partition` - Kafka partition number
- `_offset` - Kafka offset
- `_key` - Message key

### Headers and Metadata
```sql
-- Access message headers
SELECT
    customer_id,
    HEADER('source-system') as source,
    HEADER('correlation-id') as correlation
FROM orders;
```

### Performance Configuration
```sql
-- Batch processing configuration
CREATE STREAM high_throughput_stream WITH (
    topic = 'high-volume-topic',
    bootstrap.servers = 'localhost:9092',
    'batch.size' = '65536',
    'compression.type' = 'gzip',
    'buffer.memory' = '67108864'
);
```

## Examples by Use Case

### Real-World Applications
- **Fraud Detection**: [Fraud Detection Queries](../examples/fraud-detection.md)
- **Financial Trading**: [Financial Trading Queries](../examples/financial-trading.md)
- **IoT Analytics**: [IoT Analytics Queries](../examples/iot-analytics.md)
- **User Behavior**: [User Behavior Queries](../examples/user-behavior.md)
- **Real-time Dashboard**: [Dashboard Queries](../examples/real-time-dashboard.md)
- **Operational Monitoring**: [Monitoring Queries](../examples/operational-monitoring.md)

## Tools and Deployment

### Development Tools
- **SQL Validator**: [Validator Guide](../tools/validator.md)
- **Query Testing**: [Development Workflow](../quickstart/hello-world.md)

### Production Deployment
- **Native Deployment**: [Native SQL Deployment](../deployment/native-deployment.md)
- **Data Source Integration**: [Data Sources](../integration/data-sources.md)

## Quick Navigation

- **Getting Started**: [2-Minute Quick Start](../README.md)
- **Learn by Task**: [Task-Oriented Guides](../by-task/)
- **Function Reference**: [All Functions](../functions/)
- **Real Examples**: [Use Case Examples](../examples/)

---

> 💡 **Tip**: Start with the [Quick Start Guide](../README.md) for a 2-minute introduction, then explore specific tasks in the [by-task guides](../by-task/).