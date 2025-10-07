# Kafka Schema Configuration Guide

Velostream supports multiple serialization formats for Kafka data sources and sinks. This guide explains how to configure schemas for Avro and Protobuf formats.

## Schema Configuration Keys

The following configuration keys are supported for schema specification:

### Avro Schema Configuration

#### Inline Schema
```yaml
# Primary configuration key
avro.schema: |
  {
    "type": "record",
    "name": "ExampleRecord",
    "fields": [
      {"name": "id", "type": "long"},
      {"name": "name", "type": "string"}
    ]
  }

# Alternative keys (for compatibility)
value.avro.schema: "..."
schema.avro: "..."
avro_schema: "..."
```

#### Schema File Path
```yaml
# Primary configuration key
avro.schema.file: "./schemas/example.avsc"

# Alternative keys (for compatibility)
schema.file: "./schemas/example.avsc"
avro_schema_file: "./schemas/example.avsc"
```

### Protobuf Schema Configuration

#### Inline Schema
```yaml
# Primary configuration key
protobuf.schema: |
  syntax = "proto3";
  message ExampleRecord {
    int64 id = 1;
    string name = 2;
  }

# Alternative keys (for compatibility)
value.protobuf.schema: "..."
schema.protobuf: "..."
protobuf_schema: "..."
proto.schema: "..."
```

#### Schema File Path
```yaml
# Primary configuration key
protobuf.schema.file: "./schemas/example.proto"

# Alternative keys (for compatibility)
proto.schema.file: "./schemas/example.proto"
schema.file: "./schemas/example.proto"
protobuf_schema_file: "./schemas/example.proto"
```

### JSON Schema (Optional)

JSON format doesn't require schemas but supports optional validation schemas:

```yaml
# Optional JSON schema for validation
json.schema: |
  {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
      "id": {"type": "integer"},
      "name": {"type": "string"}
    }
  }

# Alternative key
schema.json: "..."
```

## Format Specification

Set the serialization format using the `value.serializer` configuration key:

```yaml
# Specify the format
value.serializer: "avro"      # Requires avro.schema or avro.schema.file
value.serializer: "protobuf"  # Requires protobuf.schema or protobuf.schema.file  
value.serializer: "json"      # Schema optional
value.serializer: "auto"      # Auto-detect format
```

## Complete Configuration Examples

### Avro with Inline Schema
```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "financial_data"

  # Consumer group configuration
  group.id: "analytics_group"

  # Serialization configuration
  schema:
    value.format: avro

  avro.schema: |
    {
      "type": "record",
      "name": "Transaction",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 4}},
        {"name": "timestamp", "type": "long"}
      ]
    }
```

### Protobuf with File Schema
```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "user_events"

  # Consumer group configuration
  group.id: "events_processor"

  # Serialization configuration
  schema:
    value.format: protobuf

  protobuf.schema.file: "./schemas/user_event.proto"
```

### JSON without Schema
```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "logs"

  # Consumer group configuration
  group.id: "log_processor"

  # Serialization configuration
  schema:
    value.format: json
```

## Schema Registry Integration

For centralized schema management, configure Schema Registry:

```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "managed_topic"

  # Consumer group configuration
  group.id: "managed_consumer"

  # Schema Registry configuration
  schema:
    value.format: avro
    schema.registry.url: "http://schema-registry:8081"
    # Optional authentication
    registry.auth.username: "registry_user"
    registry.auth.password: "registry_pass"
```

## Financial Precision Best Practices

For financial data, use decimal logical types in Avro:

```json
{
  "name": "amount",
  "type": {
    "type": "bytes",
    "logicalType": "decimal", 
    "precision": 19,
    "scale": 4
  }
}
```

Or string fields in Protobuf for exact precision:

```protobuf
message FinancialRecord {
  string amount = 1;  // Store as string to preserve precision
  string price = 2;   // e.g., "123.4567"
}
```

## Error Handling

If schema configuration is missing or invalid:

- **Avro/Protobuf**: KafkaDataReader creation will fail with a descriptive error
- **JSON**: Will work without schema (schema-free operation)
- **Auto**: Will default to JSON if no schema is provided

## Migration from Old Configuration

Old configurations using generic `schema` keys are still supported for backward compatibility, but the specific format keys are preferred:

```yaml
# Old style (still works)
schema: "..."
schema_file: "./example.avsc"

# New style (preferred)
avro.schema: "..."
avro.schema.file: "./example.avsc"
```

## Troubleshooting

Common issues and solutions:

1. **"Avro format requires a schema"**: Add `avro.schema` or `avro.schema.file`
2. **"Protobuf format REQUIRES a schema"**: Add `protobuf.schema` or `protobuf.schema.file`
3. **"Failed to load schema from file"**: Check file path and permissions
4. **Schema parsing errors**: Validate JSON/Protobuf syntax

For debugging, enable detailed logging:
```yaml
logging:
  level: "debug"
```

## Data Sink Schema Configuration

When writing data TO Kafka topics, the same schema configuration patterns apply to ensure proper serialization.

### Sink Configuration Examples

#### JSON Sink (No Schema Required)
```yaml
datasink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "processed_data"

  # Serialization configuration
  schema:
    key.format: string
    key.field: "id"  # Use 'id' field as message key
    value.format: json
```

#### Avro Sink with Schema
```yaml
datasink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "financial_results"

  # Serialization configuration
  schema:
    key.format: string
    key.field: "symbol"
    value.format: avro

  avro.schema: |
    {
      "type": "record",
      "name": "FinancialResult",
      "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 4}},
        {"name": "volume", "type": "long"},
        {"name": "calculated_at", "type": "long"}
      ]
    }
```

#### Protobuf Sink with Schema
```yaml
datasink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "analytics_output"

  # Serialization configuration
  schema:
    key.format: string
    key.field: "metric_name"
    value.format: protobuf

  protobuf.schema: |
    syntax = "proto3";
    message AnalyticsResult {
      string metric_name = 1;
      double value = 2;
      int64 timestamp = 3;
      map<string, string> tags = 4;
    }
```

### Sink Schema File Configuration

#### Avro Schema File
```yaml
datasink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "user_profiles"

  schema:
    value.format: avro

  avro.schema.file: "./schemas/user_profile.avsc"
```

#### Protobuf Schema File
```yaml
datasink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "events"

  schema:
    value.format: protobuf

  protobuf.schema.file: "./schemas/event.proto"
```

### Complete Source-to-Sink Pipeline

```yaml
# Complete configuration showing source with Avro, processing, and sink with Protobuf
source:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "raw_transactions"

  group.id: "processor_group"

  schema:
    value.format: avro

  avro.schema.file: "./schemas/transaction.avsc"

sink:
  type: kafka
  producer_config:
    bootstrap.servers: "localhost:9092"

  topic:
    name: "processed_analytics"

  schema:
    value.format: protobuf
    protobuf.schema: |
      syntax = "proto3";
      message ProcessedAnalytics {
        string transaction_id = 1;
        string account_id = 2;
        DecimalValue amount = 3;
        int64 processed_at = 4;
      }
      message DecimalValue {
        int64 units = 1;
        uint32 scale = 2;
      }
    key.field: "transaction_id"
```

## Schema Configuration Keys Reference

### Source (Consumer) Keys
- `value.serializer` - Format: "json", "avro", "protobuf", "auto"
- `avro.schema` - Inline Avro schema (JSON)
- `avro.schema.file` - Path to .avsc file
- `protobuf.schema` - Inline Protobuf schema (.proto content)
- `protobuf.schema.file` - Path to .proto file
- `json.schema` - Optional JSON validation schema

### Sink (Producer) Keys
- `value.serializer` - Format: "json", "avro", "protobuf"
- **Key Field Extraction** (checked in priority order):
  1. `key.field` - Field name to use as Kafka message key (highest priority)
  2. `message.key.field` - Alternative key field specification
  3. `schema.key.field` - Nested schema configuration (for YAML `schema:` sections)
- Schema keys same as source (avro.schema, protobuf.schema, etc.)

### Schema Registry Keys (Both Source & Sink)
- `schema_registry.url` - Schema registry URL
- `schema_registry.auth_username` - Registry authentication
- `schema_registry.auth_password` - Registry password