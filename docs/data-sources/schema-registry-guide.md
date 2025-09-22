# Schema Registry Implementation Guide

## Overview

The Schema Registry provides centralized schema management for all pluggable data sources in Velostream, enabling schema discovery, evolution, versioning, and compatibility checking.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                Schema Registry                   │
├───────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐            │
│  │Schema Cache  │  │Schema Store  │            │
│  └──────────────┘  └──────────────┘            │
├───────────────────────────────────────────────────┤
│           Schema Providers (Pluggable)           │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌──────────┐│
│  │ Kafka  │ │  File  │ │  S3    │ │PostgreSQL││
│  └────────┘ └────────┘ └────────┘ └──────────┘│
└─────────────────────────────────────────────────┘
```

## Core Components

### 1. Schema Registry (`SchemaRegistry`)

The central registry that manages all schema providers and coordinates schema operations.

```rust
use velostream::velo::sql::schema::{SchemaRegistry, SchemaProvider};

// Create registry with default providers
let registry = create_default_registry();

// Register custom provider
registry.register_provider("custom", Box::new(CustomSchemaProvider::new()));

// Discover schema from any source
let schema = registry.discover_schema("kafka://localhost:9092/orders").await?;
```

### 2. Schema Providers

Each data source type has a dedicated schema provider that knows how to discover schemas:

#### Kafka Schema Provider
```rust
pub struct KafkaSchemaProvider {
    sample_size: usize,  // Number of messages to sample
    timeout: Duration,   // Discovery timeout
}

impl SchemaProvider for KafkaSchemaProvider {
    async fn discover_schema(&self, uri: &str) -> SchemaResult<Schema> {
        // 1. Connect to Kafka
        // 2. Sample N messages
        // 3. Infer schema from message structure
        // 4. Return discovered schema
    }
    
    async fn validate_schema(&self, uri: &str, schema: &Schema) -> SchemaResult<bool> {
        // Validate that current data matches schema
    }
}
```

#### File Schema Provider
```rust
pub struct FileSchemaProvider {
    format_detectors: HashMap<String, Box<dyn FormatDetector>>,
}

impl SchemaProvider for FileSchemaProvider {
    async fn discover_schema(&self, uri: &str) -> SchemaResult<Schema> {
        // 1. Detect file format (CSV, JSON, Parquet)
        // 2. Read header/metadata
        // 3. Infer types from sample data
        // 4. Build and return schema
    }
}
```

#### PostgreSQL Schema Provider
```rust
pub struct PostgreSQLSchemaProvider {
    connection_pool: PgPool,
}

impl SchemaProvider for PostgreSQLSchemaProvider {
    async fn discover_schema(&self, uri: &str) -> SchemaResult<Schema> {
        // 1. Query information_schema
        // 2. Map PostgreSQL types to FieldValue types
        // 3. Include constraints and indexes
        // 4. Return complete schema
    }
}
```

### 3. Schema Cache

High-performance caching layer with TTL and invalidation:

```rust
pub struct SchemaCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    config: CacheConfig,
}

pub struct CacheConfig {
    pub max_entries: usize,        // Maximum cached schemas
    pub ttl: Duration,              // Time to live
    pub refresh_interval: Duration, // Background refresh
}

impl SchemaCache {
    pub async fn get_or_fetch<F>(&self, key: &str, fetcher: F) -> SchemaResult<Schema>
    where
        F: Future<Output = SchemaResult<Schema>>,
    {
        // 1. Check cache for valid entry
        // 2. If expired or missing, fetch new schema
        // 3. Update cache with new schema
        // 4. Return schema
    }
}
```

### 4. Schema Evolution

Manages schema changes over time with compatibility checking:

```rust
pub struct SchemaEvolution {
    registry: Arc<SchemaRegistry>,
    compatibility_mode: CompatibilityMode,
}

pub enum CompatibilityMode {
    None,               // No compatibility checking
    Forward,            // New schema can read old data
    Backward,           // Old schema can read new data  
    Full,               // Both forward and backward
    ForwardTransitive,  // Forward compatible across all versions
    BackwardTransitive, // Backward compatible across all versions
    FullTransitive,     // Full compatibility across all versions
}

impl SchemaEvolution {
    pub async fn evolve_schema(
        &self,
        current: &Schema,
        proposed: &Schema,
    ) -> Result<MigrationPlan, EvolutionError> {
        // 1. Check compatibility based on mode
        // 2. Generate migration plan
        // 3. Validate migration safety
        // 4. Return executable plan
    }
    
    pub fn check_compatibility(
        &self,
        reader_schema: &Schema,
        writer_schema: &Schema,
    ) -> bool {
        match self.compatibility_mode {
            CompatibilityMode::Forward => self.can_read_with(writer_schema, reader_schema),
            CompatibilityMode::Backward => self.can_read_with(reader_schema, writer_schema),
            CompatibilityMode::Full => {
                self.can_read_with(writer_schema, reader_schema) &&
                self.can_read_with(reader_schema, writer_schema)
            }
            // ... other modes
        }
    }
}
```

## SQL Integration

### Using Schema Registry in SQL Queries

#### 1. Automatic Schema Discovery

```sql
-- Schema is automatically discovered from the source
CREATE STREAM orders AS
SELECT * FROM 'kafka://localhost:9092/raw-orders';

-- Velostream automatically:
-- 1. Uses KafkaSchemaProvider to discover schema
-- 2. Caches the schema for performance
-- 3. Creates stream with discovered fields
```

#### 2. Explicit Schema Definition

```sql
-- Override discovered schema with explicit definition
CREATE STREAM orders (
    order_id BIGINT,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    order_time TIMESTAMP
) WITH (
    datasource = 'kafka://localhost:9092/raw-orders',
    format = 'json',
    schema_registry = true  -- Enable schema registry validation
);
```

#### 3. Schema Evolution in SQL

```sql
-- Evolve schema with new field (forward compatible)
ALTER STREAM orders ADD COLUMN discount DECIMAL(5,2) DEFAULT 0.0;

-- Velostream automatically:
-- 1. Checks compatibility mode
-- 2. Updates schema in registry
-- 3. Handles missing values for existing data

-- Drop column (requires backward compatibility)
ALTER STREAM orders DROP COLUMN IF EXISTS temp_field;
```

#### 4. Schema Registry Metadata Queries

```sql
-- Show registered schemas
SHOW SCHEMAS;

-- Show schema details
DESCRIBE STREAM orders;

-- Show schema version history
SHOW SCHEMA VERSIONS FOR orders;

-- Show schema compatibility
SHOW SCHEMA COMPATIBILITY FOR orders;
```

## Configuration

### Environment Variables

```bash
# Schema Registry Configuration
export VELO_SCHEMA_REGISTRY_URL="http://localhost:8081"
export VELO_SCHEMA_CACHE_TTL="300"  # 5 minutes
export VELO_SCHEMA_CACHE_SIZE="1000"
export VELO_SCHEMA_COMPATIBILITY="BACKWARD"

# Provider-specific settings
export VELO_KAFKA_SCHEMA_SAMPLE_SIZE="100"
export VELO_FILE_SCHEMA_INFER_LINES="1000"
export VELO_POSTGRES_SCHEMA_INCLUDE_VIEWS="false"
```

### Programmatic Configuration

```rust
use velostream::velo::sql::schema::{SchemaRegistryBuilder, CacheConfig};

let registry = SchemaRegistryBuilder::new()
    .cache_config(CacheConfig {
        max_entries: 1000,
        ttl: Duration::from_secs(300),
        refresh_interval: Duration::from_secs(60),
    })
    .compatibility_mode(CompatibilityMode::Backward)
    .add_provider("kafka", KafkaSchemaProvider::new())
    .add_provider("file", FileSchemaProvider::new())
    .add_provider("postgresql", PostgreSQLSchemaProvider::new())
    .build()?;
```

## Usage Examples

### 1. Cross-Source Schema Mapping

```rust
// Discover schemas from different sources
let kafka_schema = registry.discover_schema("kafka://localhost:9092/orders").await?;
let pg_schema = registry.discover_schema("postgresql://localhost/db?table=orders").await?;

// Check compatibility for data pipeline
let compatible = evolution.check_compatibility(&kafka_schema, &pg_schema);
if !compatible {
    let plan = evolution.generate_migration_plan(&kafka_schema, &pg_schema)?;
    println!("Migration required: {:?}", plan);
}
```

### 2. Schema-Driven Data Pipeline

```sql
-- Create pipeline with automatic schema propagation
CREATE STREAM enriched_orders AS
SELECT 
    o.*,
    c.name as customer_name,
    c.segment as customer_segment
FROM 'kafka://localhost:9092/orders' o
JOIN 'postgresql://localhost/customers' c
ON o.customer_id = c.id;

-- Schema registry automatically:
-- 1. Discovers schema from both sources
-- 2. Validates join compatibility
-- 3. Creates output schema
-- 4. Registers new schema for enriched_orders
```

### 3. Schema Versioning

```rust
// Get schema with version
let schema = registry.get_schema("orders", Some(3)).await?;

// Register new version
let new_version = registry.register_schema("orders", new_schema).await?;
println!("Registered schema version: {}", new_version);

// Get latest version
let latest = registry.get_latest_schema("orders").await?;
```

### 4. Schema Validation Middleware

```rust
// Create validation middleware for data pipeline
let validator = SchemaValidator::new(registry.clone());

// Validate records before processing
let pipeline = Pipeline::new()
    .source(kafka_source)
    .middleware(validator) // Validates against registered schema
    .transform(enrichment)
    .sink(postgres_sink);
```

## Performance Considerations

### 1. Caching Strategy
- **TTL-based expiration**: Schemas expire after configurable TTL
- **LRU eviction**: Least recently used schemas evicted when cache full
- **Background refresh**: Proactive refresh before expiration
- **Write-through cache**: Updates immediately reflected

### 2. Discovery Optimization
- **Sampling**: Only sample necessary records for inference
- **Parallel discovery**: Discover from multiple sources concurrently
- **Incremental discovery**: Update schema as new fields appear
- **Lazy discovery**: Only discover when actually needed

### 3. Registry Clustering
```yaml
# Distributed schema registry configuration
velo:
  schema_registry:
    cluster:
      enabled: true
      nodes:
        - host: registry1.example.com
        - host: registry2.example.com
        - host: registry3.example.com
      replication_factor: 3
      consistency_level: QUORUM
```

## Monitoring

### Metrics
- `schema_registry_discoveries_total`: Total schema discoveries
- `schema_registry_cache_hits`: Cache hit rate
- `schema_registry_cache_misses`: Cache miss rate
- `schema_evolution_compatibility_checks`: Compatibility check count
- `schema_evolution_migrations`: Migration count
- `schema_registry_errors`: Error count by type

### Health Checks
```rust
// Schema registry health check endpoint
GET /health/schema-registry

Response:
{
  "status": "healthy",
  "cache": {
    "entries": 42,
    "hit_rate": 0.95,
    "memory_mb": 12.5
  },
  "providers": {
    "kafka": "healthy",
    "postgresql": "healthy",
    "file": "healthy"
  },
  "last_discovery": "2024-01-15T10:30:00Z"
}
```

## Error Handling

### Common Errors and Solutions

1. **Schema Discovery Timeout**
```rust
// Increase timeout for slow sources
let provider = KafkaSchemaProvider::builder()
    .timeout(Duration::from_secs(30))
    .sample_size(1000)
    .build();
```

2. **Incompatible Schema Evolution**
```rust
// Handle incompatibility with migration
match evolution.evolve_schema(&current, &proposed).await {
    Ok(plan) => plan.execute().await?,
    Err(IncompatibleChange(diff)) => {
        // Generate compatibility adapter
        let adapter = create_compatibility_adapter(diff)?;
        pipeline.add_transformer(adapter);
    }
}
```

3. **Cache Invalidation**
```rust
// Force cache refresh
registry.invalidate_schema("orders").await?;

// Clear entire cache
registry.clear_cache().await?;
```

## Best Practices

1. **Always use schema registry for production pipelines**
2. **Set appropriate compatibility mode for your use case**
3. **Monitor cache hit rates and adjust TTL accordingly**
4. **Use schema evolution for backward-compatible changes**
5. **Implement schema validation in data pipelines**
6. **Version schemas with semantic versioning**
7. **Document schema changes in migration guides**
8. **Test schema compatibility before deployment**

## References

- [Schema Evolution Best Practices](./SCHEMA_EVOLUTION.md)
- [Data Source Configuration](./DEVELOPER_GUIDE.md)
- [SQL Integration Guide](./SQL_INTEGRATION_GUIDE.md)
- [Performance Tuning](./PERFORMANCE_GUIDE.md)