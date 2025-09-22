# Phase 2.5: Schema Registry Integration with References Support

## Overview

Implementing comprehensive Schema Registry integration for Velostream with full support for schema references, dependency resolution, and enterprise-scale schema evolution patterns.

## Architecture Design

### Core Components

#### 1. Schema Registry Client
```rust
pub struct SchemaRegistryClient {
    base_url: String,
    auth: Option<AuthConfig>,
    cache: Arc<RwLock<SchemaCache>>,
    reference_resolver: SchemaReferenceResolver,
    http_client: reqwest::Client,
}

impl SchemaRegistryClient {
    // Basic schema operations
    async fn get_schema(&self, id: u32) -> Result<Schema, SchemaError>;
    async fn get_latest_schema(&self, subject: &str) -> Result<Schema, SchemaError>;
    async fn register_schema(&self, subject: &str, schema: &str) -> Result<u32, SchemaError>;
    
    // Reference-aware operations  
    async fn resolve_schema_with_references(&self, id: u32) -> Result<ResolvedSchema, SchemaError>;
    async fn validate_schema_compatibility(&self, subject: &str, schema: &str) -> Result<bool, SchemaError>;
    
    // Dependency management
    async fn get_schema_dependencies(&self, id: u32) -> Result<Vec<SchemaDependency>, SchemaError>;
    async fn get_schema_references(&self, subject: &str) -> Result<Vec<SchemaReference>, SchemaError>;
}
```

#### 2. Schema Reference Resolution
```rust
#[derive(Debug, Clone)]
pub struct SchemaReference {
    pub name: String,           // Reference name in the schema
    pub subject: String,        // Referenced subject
    pub version: Option<i32>,   // Specific version (None = latest)
    pub schema_id: Option<u32>, // Resolved schema ID
}

#[derive(Debug)]
pub struct SchemaDependency {
    pub schema_id: u32,
    pub subject: String,
    pub version: i32,
    pub dependencies: Vec<u32>, // Recursive dependencies
}

pub struct SchemaReferenceResolver {
    registry: Arc<SchemaRegistryClient>,
    dependency_cache: Arc<RwLock<HashMap<u32, Vec<SchemaDependency>>>>,
}

impl SchemaReferenceResolver {
    // Reference resolution with circular dependency detection
    async fn resolve_references(&self, schema_id: u32) -> Result<ResolvedSchema, SchemaError>;
    
    // Dependency graph operations
    async fn build_dependency_graph(&self, root_schema_id: u32) -> Result<DependencyGraph, SchemaError>;
    async fn detect_circular_references(&self, graph: &DependencyGraph) -> Result<(), SchemaError>;
    
    // Compatibility checking with references
    async fn validate_reference_compatibility(&self, old_schema: &ResolvedSchema, new_schema: &ResolvedSchema) -> Result<CompatibilityResult, SchemaError>;
}
```

#### 3. Enhanced Schema Cache
```rust
#[derive(Debug)]
pub struct SchemaCache {
    // Direct schema storage
    schemas: HashMap<u32, CachedSchema>,
    subject_versions: HashMap<String, HashMap<i32, u32>>, // subject -> version -> schema_id
    
    // Reference-aware storage
    resolved_schemas: HashMap<u32, ResolvedSchema>,
    dependency_graphs: HashMap<u32, DependencyGraph>,
    reference_index: HashMap<String, HashSet<u32>>, // subject -> schema_ids that reference it
    
    // Performance optimization
    resolution_cache: HashMap<u32, Instant>, // Last resolution time
    hot_schemas: LruCache<u32, ResolvedSchema>, // Frequently accessed schemas
}

#[derive(Debug)]
pub struct CachedSchema {
    pub id: u32,
    pub subject: String,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub cached_at: Instant,
    pub access_count: u64,
}

#[derive(Debug)]
pub struct ResolvedSchema {
    pub root_schema: CachedSchema,
    pub dependencies: HashMap<u32, CachedSchema>, // All resolved dependencies
    pub flattened_schema: String, // Schema with all references resolved
    pub resolution_path: Vec<u32>, // Resolution order for debugging
}
```

#### 4. Dependency Graph Management
```rust
#[derive(Debug)]
pub struct DependencyGraph {
    pub root: u32,
    pub nodes: HashMap<u32, GraphNode>,
    pub edges: HashMap<u32, Vec<u32>>, // schema_id -> dependencies
    pub resolution_order: Vec<u32>,    // Topological sort order
}

#[derive(Debug)]
pub struct GraphNode {
    pub schema_id: u32,
    pub subject: String,
    pub version: i32,
    pub in_degree: usize,  // For topological sorting
    pub depth: usize,      // Distance from root
}

impl DependencyGraph {
    fn topological_sort(&self) -> Result<Vec<u32>, SchemaError>;
    fn detect_cycles(&self) -> Option<Vec<u32>>;
    fn calculate_max_depth(&self) -> usize;
    
    // Performance optimization
    fn is_acyclic(&self) -> bool;
    fn get_leaves(&self) -> Vec<u32>; // Schemas with no dependencies
}
```

### Integration with Configurable Serialization

#### Enhanced SerializationFormat for References
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum SerializationFormat {
    Json,
    Bytes,
    String,
    
    Avro {
        schema_registry_url: String,
        subject: String,
        version: Option<i32>,           // NEW: specific version support
        resolve_references: bool,       // NEW: enable reference resolution
        cache_dependencies: bool,       // NEW: cache resolved dependencies
    },
    
    Protobuf {
        message_type: String,
        // Protobuf references handled at compile-time
    },
}
```

#### Reference-Aware Avro Serializer
```rust
pub struct AvroSchemaRegistrySerializer {
    registry_client: Arc<SchemaRegistryClient>,
    writer_schema_cache: Arc<RwLock<HashMap<String, ResolvedSchema>>>,
    reader_schema_cache: Arc<RwLock<HashMap<u32, ResolvedSchema>>>,
}

impl AvroSchemaRegistrySerializer {
    // Enhanced serialization with reference support
    pub async fn serialize_with_references<T>(&self, subject: &str, data: &T) -> Result<Vec<u8>, SerializationError>
    where T: Serialize;
    
    // Enhanced deserialization with reference resolution
    pub async fn deserialize_with_references<T>(&self, data: &[u8]) -> Result<T, SerializationError>
    where T: for<'de> Deserialize<'de>;
    
    // Schema evolution support
    pub async fn evolve_schema(&self, old_data: &[u8], target_subject: &str) -> Result<Vec<u8>, SerializationError>;
}
```

## Implementation Plan

### Phase 2.5a: Core Registry Client (Week 1)
- [ ] Implement basic SchemaRegistryClient with HTTP operations
- [ ] Add authentication support (Basic Auth, Bearer Token)
- [ ] Implement basic schema caching
- [ ] Add comprehensive error handling and retry logic

### Phase 2.5b: Reference Resolution (Week 2)  
- [ ] Implement SchemaReferenceResolver
- [ ] Add dependency graph construction
- [ ] Implement circular reference detection
- [ ] Add reference-aware schema validation

### Phase 2.5c: Enhanced Caching (Week 3)
- [ ] Implement multi-level caching (memory + optional disk)
- [ ] Add cache invalidation strategies
- [ ] Implement hot schema optimization
- [ ] Add cache metrics and monitoring

### Phase 2.5d: Integration & Performance (Week 4)
- [ ] Integrate with existing ConfigurableKafkaConsumer/Producer
- [ ] Add async serialization optimizations
- [ ] Implement connection pooling
- [ ] Add comprehensive benchmarks

## Advanced Features

### Schema Evolution with References
```rust
pub struct SchemaEvolutionManager {
    registry: Arc<SchemaRegistryClient>,
    compatibility_checker: CompatibilityChecker,
}

impl SchemaEvolutionManager {
    // Check evolution compatibility across reference boundaries
    async fn validate_evolution(&self, 
        subject: &str, 
        new_schema: &str, 
        compatibility_level: CompatibilityLevel) -> Result<EvolutionResult, SchemaError>;
    
    // Generate migration strategies for breaking changes
    async fn suggest_migration_path(&self, 
        from_schema: &ResolvedSchema, 
        to_schema: &ResolvedSchema) -> Result<MigrationPlan, SchemaError>;
    
    // Gradual schema rollout support
    async fn create_rollout_plan(&self, 
        subject: &str, 
        target_schema: &str, 
        rollout_strategy: RolloutStrategy) -> Result<RolloutPlan, SchemaError>;
}
```

### Performance Optimizations

#### 1. Intelligent Caching Strategy
- **Hot Schema Cache**: LRU cache for frequently accessed schemas
- **Dependency Prefetching**: Proactively load likely-needed dependencies
- **Batch Resolution**: Resolve multiple schemas in single Registry calls
- **Background Refresh**: Keep cache warm with background updates

#### 2. Connection Management
```rust
pub struct RegistryConnectionPool {
    pool: deadpool::managed::Pool<RegistryConnection>,
    health_checker: HealthChecker,
    load_balancer: LoadBalancer,
}

impl RegistryConnectionPool {
    // Multiple Registry endpoint support
    async fn get_connection(&self, preference: ConnectionPreference) -> Result<RegistryConnection, PoolError>;
    
    // Health monitoring
    async fn check_endpoint_health(&self, endpoint: &str) -> HealthStatus;
    
    // Load balancing strategies
    fn select_endpoint(&self, strategy: LoadBalancingStrategy) -> String;
}
```

### Enterprise Features

#### 1. Multi-Region Support
- Schema Registry cluster awareness
- Cross-region schema synchronization
- Failover and disaster recovery
- Geo-distributed caching

#### 2. Security & Compliance
- SSL/TLS configuration
- mTLS authentication
- Schema access control integration
- Audit logging for schema operations

#### 3. Monitoring & Observability
```rust
pub struct SchemaRegistryMetrics {
    // Performance metrics
    pub cache_hit_rate: f64,
    pub avg_resolution_time_ms: f64,
    pub registry_request_count: u64,
    
    // Dependency metrics  
    pub avg_dependency_count: f64,
    pub max_dependency_depth: usize,
    pub circular_reference_detections: u64,
    
    // Schema evolution metrics
    pub compatibility_check_count: u64,
    pub evolution_failures: u64,
    pub migration_operations: u64,
}
```

## SQL Integration

### Enhanced Stream Configuration with Schema References
```sql
-- Stream with schema references support
CREATE STREAM orders_transformation AS 
SELECT 
    customer_id,
    order_details,  -- References order-event-v2 schema
    shipping_info   -- References address-v1 schema
FROM kafka_orders_source
INTO kafka_processed_sink
WITH (
    source_config='configs/kafka_orders_source.yaml',
    sink_config='configs/kafka_processed_sink.yaml'
);
```

**kafka_orders_source.yaml:**
```yaml
type: kafka
format: avro
brokers: ["localhost:9092"]
topic: "orders-raw"
group_id: "orders-processor"
schema_registry:
  url: "http://localhost:8081"
  subject: "orders-composite-v1"
  references:
    enabled: true
    cache_ttl_seconds: 300
    resolution_strategy: "eager"
```

**kafka_processed_sink.yaml:**
```yaml
type: kafka
format: avro
brokers: ["localhost:9092"]
topic: "orders-processed"
schema_registry:
  url: "http://localhost:8081"
  subject: "orders-processed-v1"
  references:
    enabled: true
    dependency_validation: true
    compatibility_check: "BACKWARD"
```
```

### Schema Reference Management & Validation
```sql
-- Validate schema compatibility before deployment
VALIDATE SCHEMA COMPATIBILITY 
FOR SUBJECT 'orders-composite-v2' 
WITH LEVEL 'BACKWARD'
RESOLVE REFERENCES;

-- Check reference dependency health  
DESCRIBE SCHEMA DEPENDENCIES 
FOR SUBJECT 'orders-composite-v1';

-- Create stream with advanced schema evolution support
CREATE STREAM schema_evolution_pipeline AS
SELECT * FROM legacy_orders_source
INTO modernized_orders_sink
WITH (
    source_config='configs/legacy_kafka_source.yaml',
    sink_config='configs/modern_kafka_sink.yaml',
    schema_evolution='configs/evolution_strategy.yaml'
);
```

**evolution_strategy.yaml:**
```yaml
schema_evolution:
  compatibility_mode: "BACKWARD_TRANSITIVE"
  reference_validation: true
  migration_strategy:
    - field_mapping:
        old_field: "customer_info"
        new_field: "customer_details"
    - transformation:
        field: "order_date"
        function: "parse_iso8601"
  dependency_resolution:
    circular_detection: true
    max_depth: 10
    cache_strategy: "aggressive"
```

## Success Metrics

### Performance Targets
- **Schema Resolution**: < 50ms for cached, < 200ms for uncached
- **Cache Hit Rate**: > 95% for production workloads  
- **Dependency Resolution**: Support up to 10 levels deep
- **Reference Count**: Handle 100+ references per schema

### Reliability Targets
- **Circular Reference Detection**: 100% accuracy
- **Schema Compatibility**: Zero false positives/negatives
- **Cache Consistency**: Strong consistency guarantees
- **Failover Time**: < 5s for Registry endpoint failures

This design provides enterprise-grade Schema Registry integration with comprehensive schema reference support, enabling complex data architectures while maintaining high performance and reliability.