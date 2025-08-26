# CREATE STREAM/TABLE ... INTO Syntax Enhancement

## Overview

This document outlines the enhancement to support both CREATE STREAM and CREATE TABLE with INTO clause for direct source-to-sink streaming transformations and materialized view creation.

## Current Limitation

The current CREATE STREAM syntax only supports creating streams from SELECT queries:

```sql
-- Current syntax (supported)
CREATE STREAM target_stream AS 
SELECT column1, column2 
FROM existing_stream
WITH (property='value')
```

## Implemented Enhancement ✅

### New Syntax Patterns

#### CREATE STREAM ... INTO
```sql
-- Stream continuous data transformations
CREATE STREAM transformation_name AS 
SELECT * FROM source_stream 
INTO target_sink
WITH (
    source_config='source_config.yaml',
    sink_config='sink_config.yaml'
)
```

#### CREATE TABLE ... INTO
```sql
-- Create materialized views with state management
CREATE TABLE materialized_view AS 
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue
FROM orders_stream
GROUP BY customer_id
INTO analytics_sink
WITH (
    source_config='kafka_source.yaml',
    sink_config='postgres_sink.yaml'
)
```

### Key Features

1. **Direct Source-to-Sink Streaming**: Stream data directly from any data source to any sink
2. **Multi-Config File Support**: Layered configuration with base + environment-specific files
3. **Environment Variable Resolution**: Dynamic config paths and values using ${VAR} syntax
4. **Configuration Inheritance**: Base configs extended by environment-specific overrides
5. **Transformation Pipeline**: Apply SQL transformations during the streaming process
6. **Multi-Format Support**: Support various source and sink formats with appropriate configurations

### Example Use Cases

#### File to Kafka Streaming with Multi-Config Support
```sql
CREATE STREAM file_to_kafka_job AS 
SELECT 
    id,
    name,
    CAST(timestamp_str AS TIMESTAMP) as event_time,
    UPPER(status) as normalized_status
FROM csv_file_source
INTO kafka_topic_sink
WITH (
    -- Multi-config support: base + environment-specific
    base_source_config='configs/base_file_source.yaml',
    source_config='${SOURCE_CONFIG_FILE}',  -- Environment variable
    base_sink_config='configs/base_kafka_sink.yaml',
    sink_config='configs/kafka_${ENVIRONMENT}.yaml',  -- Variable interpolation
    transformation_mode='streaming',
    batch_size=1000
)
```

**base_source_config='configs/base_file_source.yaml':**
```yaml
# Base configuration with common settings
type: file
format: csv
schema:
  - name: id
    type: integer
  - name: name
    type: string
  - name: timestamp_str
    type: string
  - name: status
    type: string
options:
  header: true
  delimiter: ","
  quote_char: "\""
  encoding: "utf-8"
  null_value: "NULL"
  # Environment-specific values will override
```

**source_config (resolved from ${SOURCE_CONFIG_FILE})='configs/prod_file_source.yaml':**
```yaml
# Environment-specific overrides
extends: "configs/base_file_source.yaml"  # Inherit from base
location: "${DATA_INPUT_PATH}/orders.csv"  # Environment variable
options:
  # Override base settings
  batch_size: 5000
  buffer_size: "64MB"
  compression: "gzip"
  # Add environment-specific settings
  monitoring:
    enabled: true
    interval: "30s"
    metrics_topic: "file_source_metrics"
```

**base_sink_config='configs/base_kafka_sink.yaml':**
```yaml
# Base Kafka configuration
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

**sink_config='configs/kafka_${ENVIRONMENT}.yaml' (resolved to kafka_production.yaml):**
```yaml
# Production-specific Kafka settings
extends: "configs/base_kafka_sink.yaml"
topic: "processed_orders_v2"
brokers: ["${KAFKA_BROKER_1}", "${KAFKA_BROKER_2}", "${KAFKA_BROKER_3}"]
schema_registry:
  url: "${SCHEMA_REGISTRY_URL}"
  subject: "processed_orders_v2-value"
  auth:
    username: "${SCHEMA_REGISTRY_USER}"
    password: "${SCHEMA_REGISTRY_PASSWORD}"
options:
  # Production-specific overrides
  batch_size: 32768
  max_in_flight_requests: 1
  enable_idempotence: true
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "${KAFKA_USERNAME}"
  sasl_password: "${KAFKA_PASSWORD}"
```

#### CREATE STREAM vs CREATE TABLE Differences

**CREATE STREAM INTO** - Continuous streaming pipeline:
```sql
-- Streams ALL records continuously without state
CREATE STREAM order_pipeline AS 
SELECT * FROM orders_source 
INTO orders_sink
WITH (
    source_config='configs/kafka_orders.yaml',
    sink_config='configs/s3_archive.yaml'
)
-- Result: Every order flows through unchanged, no aggregation state
```

**CREATE TABLE INTO** - Materialized view with state:
```sql
-- Maintains aggregated state, emits updates when state changes
CREATE TABLE customer_summary AS 
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    MAX(order_date) as last_order_date
FROM orders_source
GROUP BY customer_id
INTO summary_sink
WITH (
    source_config='configs/kafka_orders.yaml',
    sink_config='configs/postgres_analytics.yaml'
)
-- Result: Maintains state per customer, updates when new orders arrive
```

#### Database to S3 Streaming
```sql
CREATE STREAM db_to_s3_backup AS 
SELECT 
    customer_id,
    order_date,
    total_amount,
    DATE_FORMAT(order_date, '%Y-%m-%d') as partition_date
FROM postgres_source
INTO s3_sink
WITH (
    source_config='configs/postgres_source.yaml',
    sink_config='configs/s3_sink.yaml',
    window_size='1 HOUR',
    watermark_delay='5 MINUTES'
)
```

#### Multi-Source Enrichment
```sql
CREATE STREAM enriched_orders AS 
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_tier,
    o.order_amount,
    o.order_date
FROM kafka_orders_source o
JOIN postgres_customers_source c ON o.customer_id = c.id
INTO elasticsearch_sink
WITH (
    orders_source_config='configs/kafka_orders.yaml',
    customers_source_config='configs/postgres_customers.yaml',
    sink_config='configs/elasticsearch_sink.yaml',
    join_strategy='stream_table',
    cache_ttl='1 HOUR'
)
```

## Configuration Management Architecture

### Multi-Config File Pattern

The enhanced syntax supports multiple configuration files for each source and sink:

```sql
WITH (
    -- Source configuration hierarchy
    base_source_config='configs/base_postgres.yaml',     -- Base template
    source_config='configs/postgres_${ENV}.yaml',        -- Environment override
    
    -- Sink configuration hierarchy  
    base_sink_config='configs/base_kafka.yaml',          -- Base template
    sink_config='${KAFKA_CONFIG_FILE}',                  -- Runtime config
    
    -- Additional specialized configs
    monitoring_config='configs/monitoring_${ENV}.yaml',   -- Monitoring setup
    security_config='${SECURITY_CONFIG_PATH}',           -- Security settings
)
```

### Environment Variable Resolution

**Supported Variable Patterns:**
- `${VAR_NAME}` - Environment variable substitution
- `${VAR_NAME:-default}` - Environment variable with default value
- `${VAR_NAME:?error}` - Environment variable with error if missing

**Example Environment Setup:**
```bash
# Development environment
export ENVIRONMENT=dev
export SOURCE_CONFIG_FILE=configs/postgres_dev.yaml
export KAFKA_CONFIG_FILE=configs/kafka_local.yaml
export DATA_INPUT_PATH=/data/dev/input
export KAFKA_BROKER_1=localhost:9092

# Production environment  
export ENVIRONMENT=production
export SOURCE_CONFIG_FILE=configs/postgres_prod.yaml
export KAFKA_CONFIG_FILE=configs/kafka_prod.yaml
export DATA_INPUT_PATH=/data/prod/input
export KAFKA_BROKER_1=kafka-1.prod.company.com:9092
export KAFKA_BROKER_2=kafka-2.prod.company.com:9092
export KAFKA_BROKER_3=kafka-3.prod.company.com:9092
```

### Configuration Inheritance Model

**1. Base Configuration** - Common settings shared across environments
**2. Environment Configuration** - Environment-specific overrides
**3. Runtime Configuration** - Dynamic values resolved at execution time

```yaml
# Inheritance chain example:
base_postgres.yaml 
  ↓ (extends)
postgres_production.yaml
  ↓ (variables resolved)
Final Runtime Configuration
```

### Configuration Validation Strategy

**Load-Time Validation:**
- ✅ File existence and readability
- ✅ YAML/JSON syntax validation  
- ✅ Environment variable resolution
- ✅ Schema compatibility checks
- ✅ Required field validation

**Runtime Validation:**
- ✅ Connection testing (databases, Kafka brokers)
- ✅ Authentication verification
- ✅ Permission checks
- ✅ Resource availability

### Advanced Configuration Patterns

#### 1. Multi-Environment Support
```sql
-- Development
WITH (
    base_source_config='configs/base_postgres.yaml',
    source_config='configs/postgres_dev.yaml',
    base_sink_config='configs/base_kafka.yaml', 
    sink_config='configs/kafka_dev.yaml'
)

-- Staging
WITH (
    base_source_config='configs/base_postgres.yaml',      -- Same base
    source_config='configs/postgres_staging.yaml',        -- Different env
    base_sink_config='configs/base_kafka.yaml',          -- Same base
    sink_config='configs/kafka_staging.yaml'             -- Different env
)
```

#### 2. Multi-Tenant Configuration
```sql
-- Tenant-specific configs with shared infrastructure
WITH (
    base_source_config='configs/base_postgres.yaml',
    source_config='configs/postgres_tenant_${TENANT_ID}.yaml',
    base_sink_config='configs/base_kafka.yaml',
    sink_config='configs/kafka_tenant_${TENANT_ID}.yaml'
)
```

#### 3. Blue-Green Deployment Support
```sql
-- Blue deployment
WITH (
    source_config='configs/postgres_${ENVIRONMENT}_blue.yaml',
    sink_config='configs/kafka_${ENVIRONMENT}_blue.yaml'
)

-- Green deployment  
WITH (
    source_config='configs/postgres_${ENVIRONMENT}_green.yaml',
    sink_config='configs/kafka_${ENVIRONMENT}_green.yaml'
)
```

#### 4. Disaster Recovery Configuration
```sql
-- Primary region
WITH (
    base_source_config='configs/base_postgres.yaml',
    source_config='configs/postgres_${REGION}_primary.yaml',
    sink_config='configs/kafka_${REGION}_primary.yaml',
    backup_sink_config='configs/kafka_${DR_REGION}_backup.yaml'  -- Future: multi-sink
)
```

## Implementation Architecture

### 1. AST Enhancement

Add `IntoClause` to the `StreamingQuery` enum:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingQuery {
    // ... existing variants ...
    
    /// CREATE STREAM AS SELECT ... INTO statement for source-to-sink streaming
    CreateStreamInto {
        /// Name of the streaming transformation job
        name: String,
        /// Optional column definitions with types
        columns: Option<Vec<ColumnDef>>,
        /// SELECT query that defines the transformation
        as_select: Box<StreamingQuery>,
        /// Target sink specification
        into_clause: IntoClause,
        /// Configuration properties including source/sink configs
        properties: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntoClause {
    /// Target sink identifier
    pub sink_name: String,
    /// Optional sink-specific properties
    pub sink_properties: HashMap<String, String>,
}
```

### 2. Enhanced Parser with Multi-Config Support

Extend `parse_create_stream()` to handle multiple configuration files:

```rust
fn parse_create_stream(&mut self) -> Result<StreamingQuery, SqlError> {
    self.expect(TokenType::Stream)?;
    let name = self.expect(TokenType::Identifier)?.value;

    // Optional column definitions
    let columns = if self.current_token().token_type == TokenType::LeftParen {
        Some(self.parse_column_definitions()?)
    } else {
        None
    };

    self.expect(TokenType::As)?;
    let as_select = Box::new(self.parse_select()?);

    // Check for INTO clause
    if self.current_token().token_type == TokenType::Into {
        let into_clause = self.parse_into_clause()?;
        
        // Optional WITH properties with multi-config support
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_enhanced_with_properties()?
        } else {
            ConfigProperties::default()
        };

        return Ok(StreamingQuery::CreateStreamInto {
            name,
            columns,
            as_select,
            into_clause,
            properties,
        });
    }

    // Fallback to regular CREATE STREAM
    let properties = if self.current_token().token_type == TokenType::With {
        self.parse_enhanced_with_properties()?
    } else {
        ConfigProperties::default()
    };

    Ok(StreamingQuery::CreateStream {
        name,
        columns,
        as_select,
        properties: properties.into_legacy_format(), // Backward compatibility
    })
}

fn parse_into_clause(&mut self) -> Result<IntoClause, SqlError> {
    self.expect(TokenType::Into)?;
    let sink_name = self.expect(TokenType::Identifier)?.value;
    
    Ok(IntoClause {
        sink_name,
        sink_properties: HashMap::new(),
    })
}
```

### 3. Configuration File Support

Enhance the WITH clause parsing to support config file references:

```rust
/// Enhanced properties parsing with config file resolution
fn parse_with_properties(&mut self) -> Result<HashMap<String, String>, SqlError> {
    self.expect(TokenType::With)?;
    self.expect(TokenType::LeftParen)?;
    
    let mut properties = HashMap::new();
    
    loop {
        let key = self.expect(TokenType::Identifier)?.value;
        self.expect(TokenType::Equals)?;
        let value = self.expect(TokenType::String)?.value;
        
        // Special handling for config file properties
        if key.ends_with("_config") {
            // Resolve and validate config file
            let resolved_config = self.resolve_config_file(&value)?;
            properties.insert(key, resolved_config);
        } else {
            properties.insert(key, value);
        }
        
        if self.current_token().token_type == TokenType::Comma {
            self.advance();
        } else {
            break;
        }
    }
    
    self.expect(TokenType::RightParen)?;
    Ok(properties)
}

fn resolve_config_file(&self, config_path: &str) -> Result<String, SqlError> {
    // Load and validate YAML/JSON config files
    // Return serialized configuration for execution engine
    todo!("Implement config file loading and validation")
}
```

### 4. Execution Engine Integration

Extend the execution engine to handle source-to-sink streaming:

```rust
impl ExecutionEngine {
    pub async fn execute_create_stream_into(
        &mut self,
        name: &str,
        columns: &Option<Vec<ColumnDef>>,
        as_select: &StreamingQuery,
        into_clause: &IntoClause,
        properties: &HashMap<String, String>,
    ) -> Result<QueryResult, SqlError> {
        // 1. Parse and create source from source_config
        let source = self.create_configured_source(properties.get("source_config"))?;
        
        // 2. Parse and create sink from sink_config  
        let sink = self.create_configured_sink(&into_clause.sink_name, properties.get("sink_config"))?;
        
        // 3. Create transformation pipeline from SELECT query
        let transformation = self.compile_transformation(as_select)?;
        
        // 4. Create streaming job that connects source -> transformation -> sink
        let streaming_job = StreamingJob::new(name, source, transformation, sink);
        
        // 5. Register and start the streaming job
        self.job_manager.register_streaming_job(streaming_job).await?;
        
        Ok(QueryResult::Success {
            message: format!("Streaming job '{}' created and started", name),
            affected_rows: None,
        })
    }
    
    fn create_configured_source(&self, config_path: Option<&String>) -> Result<Box<dyn DataSource>, SqlError> {
        let config_path = config_path.ok_or_else(|| 
            SqlError::Configuration("Missing source_config in WITH clause".to_string())
        )?;
        
        // Load YAML config and create appropriate DataSource
        let config = self.load_yaml_config(config_path)?;
        self.data_source_factory.create_from_config(config)
    }
    
    fn create_configured_sink(&self, sink_name: &str, config_path: Option<&String>) -> Result<Box<dyn DataSink>, SqlError> {
        let config_path = config_path.ok_or_else(|| 
            SqlError::Configuration("Missing sink_config in WITH clause".to_string())
        )?;
        
        // Load YAML config and create appropriate DataSink
        let config = self.load_yaml_config(config_path)?;
        self.data_sink_factory.create_from_config(sink_name, config)
    }
}
```

## Benefits

1. **Unified Syntax**: Single SQL statement for complex source-to-sink pipelines
2. **Configuration Management**: External config files enable environment-specific configurations
3. **Type Safety**: Schema validation at CREATE time prevents runtime errors
4. **Monitoring**: Built-in job management and monitoring for streaming transformations
5. **Composability**: Can combine with existing window functions, aggregations, and joins

## Migration Path

This enhancement is fully backward compatible:
- Existing `CREATE STREAM AS SELECT` syntax continues to work
- New `INTO` clause is optional
- Configuration file support is optional (inline properties still supported)

## Implementation Priority

**Priority**: High
**Effort**: Medium (2-3 weeks)
**Impact**: High (enables major new use cases)

### Phase 1: Parser Enhancement (Week 1)
- Implement INTO clause parsing
- Add configuration file loading
- Update AST structures

### Phase 2: Execution Engine (Week 2)
- Implement source-to-sink streaming jobs
- Add job lifecycle management
- Configuration validation

### Phase 3: Testing & Documentation (Week 3)
- Comprehensive test suite
- Update SQL documentation
- Integration examples

## Success Criteria

- [ ] Parser correctly handles CREATE STREAM ... INTO syntax
- [ ] Configuration files can be loaded and validated
- [ ] Streaming jobs execute source-to-sink transformations
- [ ] Performance meets benchmarks (>10K records/sec)
- [ ] Comprehensive error handling and validation
- [ ] Documentation and examples complete

---

**Status**: Proposed Enhancement  
**Created**: 2025-01-08  
**Next Review**: 2025-01-15