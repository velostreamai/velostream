# Tier 7: Serialization Formats

Examples demonstrating different serialization formats for Kafka messages.

## Overview

Velostream supports multiple serialization formats:

| Format | Use Case | Schema Required | Performance |
|--------|----------|-----------------|-------------|
| **JSON** | Development, debugging, interoperability | No | Moderate |
| **Avro** | Production, schema evolution, compact data | Yes | High |
| **Protobuf** | High-performance, cross-language | Yes | Highest |

## Examples

| File | Description |
|------|-------------|
| `60_json_format.sql` | Default JSON serialization |
| `61_avro_format.sql` | Avro with embedded schema |
| `62_protobuf_format.sql` | Protobuf with schema file |
| `63_format_conversion.sql` | Convert between formats |

## Key Concepts

### JSON (Default)
```yaml
schema:
  value.serializer: "json"    # Human-readable, no schema needed
```

### Avro
```yaml
schema:
  value.serializer: "avro"
  avro.schema.file: "./schemas/record.avsc"
```

Benefits:
- Compact binary format (50-80% smaller than JSON)
- Schema evolution support
- Type safety
- Schema Registry integration

### Protobuf
```yaml
schema:
  value.serializer: "protobuf"
  protobuf.schema.file: "./schemas/record.proto"
```

Benefits:
- Fastest serialization/deserialization
- Cross-language support (Java, Python, Go, C++, etc.)
- Backward/forward compatibility
- Smallest message size

## Running Examples

```bash
# JSON (default)
../velo-test.sh 60_json_format.sql

# Avro
../velo-test.sh 61_avro_format.sql

# Protobuf
../velo-test.sh 62_protobuf_format.sql

# Format conversion
../velo-test.sh 63_format_conversion.sql
```

## Best Practices

1. **Development**: Use JSON for readability
2. **Testing**: Use JSON for easy debugging
3. **Production**: Use Avro or Protobuf for performance
4. **Schema Registry**: Use Avro with Confluent Schema Registry
5. **Cross-Language**: Use Protobuf for polyglot environments
