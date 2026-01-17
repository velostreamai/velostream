# YAML Configuration Guide with Inheritance

Velostream supports powerful YAML configuration management with file inheritance through the `extends:` keyword. This enables DRY (Don't Repeat Yourself) configuration patterns for complex multi-environment deployments.

## Features

- **File Inheritance**: Use `extends: base_config.yaml` to inherit from base configurations
- **Deep Merging**: Child configurations override parent settings while preserving unmodified values
- **Circular Dependency Detection**: Prevents infinite inheritance loops
- **Path Resolution**: Supports both relative and absolute paths
- **Multi-level Inheritance**: Chain multiple inheritance levels (Base → Environment → Specific)
- **Environment Variables**: Full support for `${VAR}`, `${VAR:-default}`, and `${VAR:?error}` patterns
- **Inline Parameters**: Direct parameter specification using `source.*` and `sink.*` prefixes
- **Mixed Configuration**: Combine YAML files with inline parameter overrides

## Basic Usage

### Simple Inheritance

**Base Configuration** (`configs/base_kafka.yaml`):
```yaml
type: kafka
format: json
schema_registry:
  url: "http://localhost:8081"
options:
  compression: "gzip"
  acks: "all"
  batch_size: 16384
  linger_ms: 5
  retries: 3
```

**Environment-Specific Configuration** (`configs/kafka_prod.yaml`):
```yaml
extends: configs/base_kafka.yaml
topic: "orders_production"
brokers: ["prod-kafka-1:9092", "prod-kafka-2:9092", "prod-kafka-3:9092"]
schema_registry:
  url: "https://schema-registry-prod:8081"
options:
  # Inherits compression, acks, batch_size, linger_ms, retries from base
  # Override specific settings for production
  batch_size: 32768  # Larger batches for production
  request_timeout_ms: 30000
```

### SQL Integration

**Configuration File Approach:**
```sql
CREATE STREAM orders_processing AS 
SELECT id, customer_id, amount, status 
FROM kafka_source 
INTO kafka_sink
WITH (
    "source_config" = "configs/kafka_orders_${ENVIRONMENT}.yaml",
    "sink_config" = "configs/kafka_processed_${ENVIRONMENT}.yaml"
);
```

**Inline Configuration Approach:**
```sql
CREATE STREAM orders_inline AS
SELECT id, customer_id, amount, status 
FROM 'kafka://broker:9092/orders'
INTO 'file://output/processed.json'
WITH (
    "source.group_id" = "orders_processor_${ENVIRONMENT}",
    "source.value.format" = "json",
    "sink.format" = "json",
    "sink.append" = "true"
);
```

**Mixed Approach (Recommended for complex configurations):**
```sql
CREATE STREAM orders_mixed AS
SELECT id, customer_id, amount, status 
FROM kafka_source INTO file_sink
WITH (
    "source_config" = "configs/kafka_base_${ENVIRONMENT}.yaml",
    "sink_config" = "configs/file_base.yaml",
    -- Override specific parameters inline
    "source.group_id" = "custom_processor_${JOB_ID}",
    "sink.path" = "output/${DATE}/processed_orders.json"
);
```

## Advanced Patterns

### Multi-Level Inheritance

**Base Configuration** (`configs/base_kafka.yaml`):
```yaml
type: kafka
format: json
options:
  compression: "gzip"
  acks: "all"
  retries: 3
```

**Environment Base** (`configs/base_prod.yaml`):
```yaml
extends: configs/base_kafka.yaml
schema_registry:
  url: "https://schema-registry-prod:8081"
  auth:
    username: "${SCHEMA_REGISTRY_USER}"
    password: "${SCHEMA_REGISTRY_PASS}"
options:
  batch_size: 32768
  linger_ms: 10
```

**Service-Specific Configuration** (`configs/orders_prod.yaml`):
```yaml
extends: configs/base_prod.yaml
topic: "orders_v2"
brokers: ["${KAFKA_BROKER_1}", "${KAFKA_BROKER_2}", "${KAFKA_BROKER_3}"]
options:
  # Inherits all base settings, overrides specific ones
  max_request_size: 1048576  # 1MB for large orders
```

### Complex Inheritance with Environment Variables

**Development Environment** (`configs/kafka_dev.yaml`):
```yaml
extends: configs/base_kafka.yaml
topic: "orders_dev"
brokers: ["localhost:9092"]
schema_registry:
  url: "http://localhost:8081"
options:
  batch_size: 1000    # Smaller batches for development
  linger_ms: 0        # No batching delay for immediate processing
```

**Production Environment** (`configs/kafka_prod.yaml`):
```yaml
extends: configs/base_kafka.yaml
topic: "orders_production"
brokers: ["${KAFKA_BROKER_1}", "${KAFKA_BROKER_2}", "${KAFKA_BROKER_3}"]
schema_registry:
  url: "${SCHEMA_REGISTRY_URL}"
  auth:
    username: "${SCHEMA_REGISTRY_USER:?Schema registry user required}"
    password: "${SCHEMA_REGISTRY_PASS:?Schema registry password required}"
options:
  batch_size: 65536   # Large batches for throughput
  linger_ms: 100      # Balance latency vs throughput
  request_timeout_ms: 30000
  security:
    protocol: "SASL_SSL"
    sasl_mechanism: "PLAIN"
    sasl_username: "${KAFKA_USER:?Kafka user required}"
    sasl_password: "${KAFKA_PASS:?Kafka password required}"
```

## Inline Configuration Parameters

Velostream supports direct parameter specification in SQL WITH clauses using prefixed naming:

### Parameter Naming Convention

| Prefix | Purpose | Examples |
|---------|---------|----------|
| `source.*` | Source configuration | `source.format`, `source.topic`, `source.group_id` |
| `sink.*` | Sink configuration | `sink.path`, `sink.format`, `sink.bootstrap.servers` |
| No prefix | Job/processing config | `batch_size`, `failure_strategy`, `use_transactions` |

### Common Source Parameters

**File Sources:**
- `source.format` - File format (`csv`, `json`, `jsonl`)
- `source.has_headers` - CSV has header row (`true`, `false`)
- `source.watching` - Watch file for changes (`true`, `false`)
- `source.use_transactions` - Enable transactional processing (`true`, `false`)

**Kafka Sources:**
- `source.bootstrap.servers` - Kafka broker list
- `source.topic` - Kafka topic name
- `source.group_id` - Consumer group ID
- `source.value.format` - Value serialization format (`json`, `avro`, `protobuf`)
- `source.key.format` - Key serialization format

### Common Sink Parameters

**File Sinks:**
- `sink.path` - Output file path
- `sink.format` - Output format (`csv`, `json`, `jsonl`)
- `sink.append` - Append to existing file (`true`, `false`)
- `sink.has_headers` - Include headers in CSV (`true`, `false`)

**Kafka Sinks:**
- `sink.bootstrap.servers` - Kafka broker list
- `sink.topic` - Kafka topic name
- `sink.value.format` - Value serialization format
- `sink.key.format` - Key serialization format
- `sink.compression.type` - Compression (`gzip`, `snappy`, `lz4`)

### Usage Examples

**File Processing with Inline Parameters:**
```sql
CREATE STREAM csv_processing AS
SELECT * FROM 'file://data/transactions.csv'
INTO 'file://output/processed.json'
WITH (
    "source.format" = "csv",
    "source.has_headers" = "true",
    "source.watching" = "false",
    "sink.format" = "json",
    "sink.append" = "true",
    "failure_strategy" = "LogAndContinue"
);
```

**Kafka Processing with Inline Parameters:**
```sql
CREATE STREAM kafka_inline AS
SELECT * FROM 'kafka://localhost:9092/input'
INTO 'kafka://localhost:9092/output'
WITH (
    "source.group_id" = "processor_${ENVIRONMENT}",
    "source.value.format" = "avro",
    "sink.value.format" = "json",
    "sink.compression.type" = "gzip",
    "batch_size" = "1000"
);
```

## Prefix Priority and Fallback Behavior

The inline parameter system provides intelligent property resolution with clear priority rules:

### Priority Resolution Rules

1. **Prefixed properties take priority over unprefixed**
2. **Property isolation between source and sink**
3. **Fallback to unprefixed when prefixed version doesn't exist**
4. **Support for property aliases**

### Example: Priority Resolution
```sql
CREATE STREAM priority_demo AS
SELECT * FROM kafka_source INTO kafka_sink
WITH (
    -- Both prefixed and unprefixed specified
    "brokers" = "fallback-broker:9092",          -- Fallback value
    "source.brokers" = "source-broker:9092",     -- Source uses this (priority)
    -- No sink.brokers specified, so sink uses "brokers" (fallback)
    
    -- Property isolation
    "source.group_id" = "reader_group",          -- Only in source config
    "sink.topic" = "output_topic",               -- Only in sink config
    
    -- Property aliases (both work the same way)
    "source.bootstrap.servers" = "alias-broker:9092",  -- Same as source.brokers
    "source.watching" = "true",                  -- Same as source.watch (file sources)
    
    -- Job-level properties (shared by both)
    "failure_strategy" = "RetryWithBackoff",     -- Appears in both configs
    "batch_size" = "1000"                        -- Appears in both configs
);
```

### Property Aliases by Type

**Kafka Sources/Sinks:**
- `brokers` ↔ `bootstrap.servers`
- Both resolve to the same internal configuration

**File Sources/Sinks:**
- `watching` ↔ `watch` (watch files for changes)
- `has_headers` ↔ `header` (CSV header handling)

### Configuration Isolation

Source and sink configurations are kept separate:
- Source config excludes all `sink.*` properties
- Sink config excludes all `source.*` properties  
- Unprefixed properties appear in both (unless job-level)
- This prevents configuration conflicts in mixed scenarios

## Migration from Legacy Base Configs

### ❌ Old Pattern (Deprecated)
```sql
-- DEPRECATED: base_source_config and base_sink_config
CREATE STREAM orders AS 
SELECT * FROM kafka_source 
INTO kafka_sink
WITH (
    "base_source_config" = "configs/base_kafka.yaml",
    "source_config" = "configs/kafka_orders.yaml",
    "base_sink_config" = "configs/base_kafka_sink.yaml", 
    "sink_config" = "configs/kafka_processed.yaml"
);
```

### ✅ New Pattern (Recommended)
```sql
-- RECOMMENDED: Use extends in YAML files
CREATE STREAM orders AS 
SELECT * FROM kafka_source 
INTO kafka_sink
WITH (
    "source_config" = "configs/kafka_orders.yaml",
    "sink_config" = "configs/kafka_processed.yaml"
);
```

**YAML Configuration** (`configs/kafka_orders.yaml`):
```yaml
extends: configs/base_kafka.yaml
topic: "orders"
consumer_group: "orders_processor"
```

## Environment Variable Patterns

| Pattern | Description | Example |
|---------|-------------|---------|
| `${VAR}` | Simple substitution | `"${CONFIG_PATH}/app.yaml"` |
| `${VAR:-default}` | Use default if unset | `"kafka_${ENV:-dev}.yaml"` |
| `${VAR:?message}` | Error if unset | `"${REQUIRED_CONFIG:?Config required}"` |

### Velostream-Specific Environment Variables

These environment variables are read directly by Velostream at runtime:

| Variable | Description | Values | Default |
|----------|-------------|--------|---------|
| `VELOSTREAM_KAFKA_BROKERS` | Kafka broker endpoints | comma-separated list | `localhost:9092` |
| `VELOSTREAM_BROKER_ADDRESS_FAMILY` | Broker address resolution | `v4`, `v6`, `any` | `v4` |

**Note on `VELOSTREAM_BROKER_ADDRESS_FAMILY`**: Defaults to `v4` (IPv4 only) because containers (Docker, testcontainers) often advertise `localhost` in Kafka broker metadata, which can resolve to IPv6 `::1` on some systems while the broker only listens on IPv4. Set to `any` for production environments with proper DNS or IPv6 support.

## Best Practices

### 1. Organize Configuration Hierarchy
```
configs/
├── base/
│   ├── kafka.yaml          # Common Kafka settings
│   ├── postgres.yaml       # Common PostgreSQL settings
│   └── s3.yaml             # Common S3 settings
├── environments/
│   ├── dev.yaml             # Development overrides
│   ├── staging.yaml         # Staging overrides
│   └── prod.yaml            # Production overrides
└── services/
    ├── orders_dev.yaml      # Service-specific dev config
    ├── orders_prod.yaml     # Service-specific prod config
    └── analytics_prod.yaml  # Analytics service config
```

### 2. Use Meaningful File Names
- Include environment: `kafka_prod.yaml`, `postgres_dev.yaml`
- Include service: `orders_source.yaml`, `analytics_sink.yaml`
- Include purpose: `monitoring_config.yaml`, `security_config.yaml`

### 3. Document Inheritance Chains
```yaml
# orders_prod.yaml
# Inheritance: base_kafka.yaml -> base_prod.yaml -> orders_prod.yaml
extends: base_prod.yaml
topic: "orders_v2"
# ... configuration
```

### 4. Validate Required Variables
```yaml
# Use ${VAR:?message} for required environment variables
brokers: ["${KAFKA_BROKER:?Kafka broker must be specified}"]
```

## Error Handling

The YAML loader provides comprehensive error reporting:

- **File Not Found**: Clear path resolution errors
- **Parse Errors**: Detailed YAML syntax error messages
- **Circular Dependencies**: Inheritance loop detection with full chain
- **Missing Variables**: Environment variable resolution errors

## Performance Considerations

- Configuration files are cached after first load
- Inheritance resolution is performed once at startup
- No runtime performance impact from inheritance
- Circular dependency detection uses efficient set-based checking

## Migration Guide

1. **Identify Base Configurations**: Extract common settings into base YAML files
2. **Add Extends Keywords**: Update environment-specific configs to use `extends:`
3. **Update SQL Statements**: Remove `base_*_config` parameters from WITH clauses
4. **Test Inheritance**: Verify merged configurations work as expected
5. **Update Documentation**: Document inheritance chains for team members

## Examples Repository

Complete examples are available in the `/examples/configs/` directory:

- Multi-environment Kafka configurations
- PostgreSQL to S3 data pipeline configs
- Monitoring and security configurations
- Complex multi-service inheritance patterns

## Troubleshooting

### Common Issues

1. **Circular Dependencies**: Ensure inheritance chains don't loop back
2. **Path Resolution**: Use absolute paths or ensure relative paths are correct
3. **Variable Substitution**: Check environment variables are set correctly
4. **YAML Syntax**: Validate YAML syntax in all configuration files

### Debug Tips

- Use the configuration validation tool: `velo-sql validate-config`
- Enable debug logging: `RUST_LOG=debug`
- Check inheritance chains: `velo-sql show-config-inheritance`

## Configuration Approach Comparison

| Approach | Best For | Pros | Cons |
|----------|----------|------|------|
| **YAML Files Only** | Production environments with complex configs | Reusable, version controlled, inheritance | Requires separate file management |
| **Inline Only** | Simple jobs, prototyping, single-use scripts | Quick setup, self-contained | No reusability, can become verbose |
| **Mixed (Recommended)** | Enterprise deployments | Base configs + job-specific overrides | Requires understanding both approaches |
| **URI + Inline** | Ad-hoc processing, data exploration | Flexible, minimal setup | Limited to simple configurations |

## Quick Reference

### Configuration Methods
```sql
-- Method 1: YAML files with inheritance
WITH ("source_config" = "configs/kafka_prod.yaml")

-- Method 2: Inline parameters
WITH ("source.topic" = "orders", "source.group_id" = "processor")

-- Method 3: Mixed approach  
WITH ("source_config" = "configs/base.yaml", "source.group_id" = "custom_id")

-- Method 4: URI with inline overrides
FROM 'kafka://broker:9092/orders' WITH ("source.group_id" = "processor")
```

### Parameter Prefixes
- `source.*` - Source datasource configuration
- `sink.*` - Sink datasource configuration  
- No prefix - Job processing configuration (`batch_size`, `failure_strategy`, etc.)

This guide provides the foundation for powerful, maintainable configuration management in Velostream using modern configuration patterns including YAML inheritance and inline parameter specification.