# Kafka Schema Configuration Guide

FerrisStreams supports multiple serialization formats for Kafka data sources and sinks. This guide explains how to configure schemas for Avro and Protobuf formats.

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
    bootstrap_servers: "localhost:9092"
    topic: "financial_data"
    group_id: "analytics_group"
    
    # Serialization configuration
    value.serializer: "avro"
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
    bootstrap_servers: "localhost:9092"
    topic: "user_events"
    group_id: "events_processor"
    
    # Serialization configuration
    value.serializer: "protobuf"
    protobuf.schema.file: "./schemas/user_event.proto"
```

### JSON without Schema
```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap_servers: "localhost:9092"
    topic: "logs"
    group_id: "log_processor"
    
    # JSON doesn't require schema
    value.serializer: "json"
```

## Schema Registry Integration

For centralized schema management, configure Schema Registry:

```yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap_servers: "localhost:9092"
    topic: "managed_topic"
    group_id: "managed_consumer"
    
    # Schema Registry configuration
    schema_registry:
      url: "http://schema-registry:8081"
      # Optional authentication
      auth_username: "registry_user"
      auth_password: "registry_pass"
    
    # Format still needs to be specified
    value.serializer: "avro"
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