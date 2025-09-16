# Schema Registry Architecture Documentation

## Overview

The VeloStream Schema Registry is an enterprise-grade, high-performance system for managing schema definitions, references, evolution, and compatibility across heterogeneous data sources. It provides comprehensive schema lifecycle management with advanced caching, reference resolution, and performance optimization.

## Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Schema Registry System                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────┐    ┌─────────────────────────┐                   │
│  │    Registry Client      │    │  Reference Resolver     │                   │
│  │                         │    │                         │                   │
│  │ • HTTP API Client       │    │ • Circular Detection    │                   │
│  │ • Authentication        │    │ • Schema Evolution      │                   │
│  │ • Request/Response      │    │ • Migration Planning    │                   │
│  │ • Retry Logic           │    │ • Rollout Strategies    │                   │
│  └─────────────────────────┘    └─────────────────────────┘                   │
│              │                              │                                  │
│              └──────────────┬───────────────┘                                  │
│                             │                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Enhanced Caching System                              │   │
│  │                                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │ L1 (Hot)    │  │ L2 (Main)   │  │ L3(Resolved)│  │ Dependency  │   │   │
│  │  │             │  │             │  │             │  │ Index       │   │   │
│  │  │ • LRU Cache │  │ • TTL Cache │  │ • Resolved  │  │             │   │   │
│  │  │ • 100 items │  │ • 10K items │  │   Schemas   │  │ • Forward   │   │   │
│  │  │ • < 1ms     │  │ • Access    │  │ • Graphs    │  │ • Reverse   │   │   │
│  │  │   Access    │  │   Tracking  │  │             │  │ • Patterns  │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  │                                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                    │   │
│  │  │ Prefetch    │  │ Refresh     │  │ Metrics     │                    │   │
│  │  │ Engine      │  │ Manager     │  │ Collector   │                    │   │
│  │  │             │  │             │  │             │                    │   │
│  │  │ • Smart     │  │ • Priority  │  │ • Hit Rates │                    │   │
│  │  │   Loading   │  │   Queue     │  │ • Timing    │                    │   │
│  │  │ • Pattern   │  │ • Background│  │ • Eviction  │                    │   │
│  │  │   Analysis  │  │   Refresh   │  │   Stats     │                    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │      Confluent Schema Registry      │
                    │                                     │
                    │        External Service             │
                    │     (HTTP REST API)                 │
                    └─────────────────────────────────────┘
```

## Core Components

### 1. SchemaRegistryClient

**Purpose**: HTTP client for interacting with Confluent Schema Registry API

**Key Features**:
- RESTful API communication with Schema Registry
- Multiple authentication methods (Basic Auth, Bearer Token)
- Configurable retry logic with exponential backoff
- Request/response handling with proper error management
- Schema registration and retrieval operations
- Compatibility checking

**Configuration**:
```rust
pub struct RegistryClientConfig {
    pub timeout_seconds: u64,        // Request timeout
    pub max_retries: u32,           // Maximum retry attempts
    pub retry_delay_ms: u64,        // Base retry delay
    pub cache_ttl_seconds: u64,     // Cache TTL
    pub resolve_references: bool,    // Enable reference resolution
}
```

**Usage**:
```rust
let client = SchemaRegistryClient::new("http://localhost:8081")
    .with_auth(AuthConfig::Basic { 
        username: "user".to_string(), 
        password: "pass".to_string() 
    });

let schema = client.get_schema(123).await?;
let latest = client.get_latest_schema("orders-v1").await?;
```

### 2. SchemaReferenceResolver

**Purpose**: Advanced reference resolution with circular dependency detection

**Key Features**:
- Dependency graph construction and analysis
- Circular reference detection using DFS algorithms
- Schema evolution tracking and compatibility validation
- Migration plan generation with risk assessment
- Rollout strategy support (Canary, Blue-Green, Rolling)
- Schema flattening for complex compositions

**Algorithms**:
- **Topological Sort**: Determines schema resolution order
- **DFS Cycle Detection**: Identifies circular dependencies  
- **Compatibility Analysis**: Breaking change detection
- **Migration Planning**: Automated upgrade strategies

**Usage**:
```rust
let resolver = SchemaReferenceResolver::new(registry_client);
let resolved = resolver.resolve_references(schema_id).await?;
let migration_plan = resolver.generate_migration_plan(&old_schema, &new_schema).await?;
```

### 3. EnhancedSchemaCache

**Purpose**: High-performance, multi-level caching system

#### 3-Tier Cache Architecture

**L1 Cache (Hot)**:
- **Type**: LRU Cache
- **Size**: 100 most frequently accessed schemas
- **Access Time**: < 1ms
- **Purpose**: Ultra-fast access to hot schemas

**L2 Cache (Main)**:
- **Type**: TTL-based HashMap
- **Size**: Up to 10,000 schemas
- **Access Time**: < 5ms
- **Features**: Access tracking, automatic promotion to L1

**L3 Cache (Resolved)**:
- **Type**: Complex resolved schemas
- **Content**: Fully resolved schemas with dependency graphs
- **Access Time**: < 10ms
- **Purpose**: Avoid expensive re-resolution

#### Smart Optimization Features

**Dependency Prefetching**:
- Automatically loads related schemas when one is accessed
- Uses correlation analysis to predict likely access patterns
- Configurable prefetch depth (default: 3 levels)

**Access Pattern Learning**:
- Tracks which schemas are commonly accessed together
- Builds correlation scores for intelligent prefetching
- Adapts to usage patterns over time

**Background Refresh**:
- Priority-based refresh queue
- Proactive cache warming for critical schemas
- Configurable refresh intervals

#### Performance Metrics

```rust
pub struct CacheMetrics {
    pub total_hits: u64,                    // Total cache hits
    pub total_misses: u64,                  // Total cache misses
    pub l1_hits: u64,                       // L1 cache hits
    pub l2_hits: u64,                       // L2 cache hits  
    pub l3_hits: u64,                       // L3 cache hits
    pub avg_lookup_time_us: f64,            // Average lookup time
    pub hit_rate: f64,                      // Overall hit rate
    pub prefetch_success_rate: f64,         // Prefetch accuracy
}
```

## Schema Reference Support

### Reference Architecture

VeloStream provides comprehensive support for Confluent Schema Registry schema references, enabling:

- **Schema Composition**: Build complex schemas from reusable components
- **Dependency Management**: Track and resolve schema dependencies  
- **Circular Detection**: Prevent circular reference issues with DFS algorithms
- **Evolution Control**: Manage changes across dependent schemas
- **Performance Optimization**: Smart caching and prefetching of referenced schemas

### Reference Resolution Process

```
Schema Reference Resolution Flow:

1. Schema Request (with references)
   ↓
2. Parse Schema References
   ├─ Extract reference definitions
   ├─ Validate reference format
   └─ Check for required fields
   ↓  
3. Build Dependency Graph
   ├─ Create nodes for each referenced schema
   ├─ Establish edges for dependencies
   ├─ Calculate dependency depth
   └─ Detect circular references (DFS)
   ↓
4. Topological Sort
   ├─ Determine resolution order
   ├─ Dependencies resolved first
   └─ Handle complex hierarchies
   ↓
5. Resolve Dependencies
   ├─ Fetch from cache (L1/L2/L3)
   ├─ Fetch from Registry if not cached
   ├─ Recursively resolve sub-references
   └─ Build complete dependency tree
   ↓
6. Schema Flattening
   ├─ Merge all referenced schemas
   ├─ Resolve type references
   ├─ Create unified schema definition
   └─ Validate final schema
   ↓
7. Cache Resolution Results
   ├─ Store in L3 (Resolved Cache)
   ├─ Update dependency index
   ├─ Record performance metrics
   └─ Enable future fast lookups
```

### Reference Types Supported

**Direct References**:
```rust
SchemaReference {
    name: "CustomerInfo",                    // Type name in schema
    subject: "customer-info-value",          // Registry subject
    version: Some(2),                        // Specific version
    schema_id: Some(12345),                 // Resolved schema ID
}
```

**Latest Version References**:
```rust
SchemaReference {
    name: "CustomerInfo",
    subject: "customer-info-value", 
    version: None,                          // Use latest version
    schema_id: None,                        // Auto-resolve
}
```

**Complex Hierarchies**:
- Support for nested references (A → B → C → D)
- Maximum depth configurable (default: 10 levels)
- Automatic dependency ordering
- Performance optimized resolution

### Circular Reference Prevention

The system uses **Depth-First Search (DFS)** with cycle detection:

1. **Graph Coloring**: White → Gray → Black states
2. **Back Edge Detection**: Gray → Gray indicates cycle
3. **Path Tracking**: Maintain resolution path for debugging
4. **Early Termination**: Stop at first circular reference detected

**Algorithm Implementation**:
```rust
fn detect_cycles(&self, graph: &DependencyGraph) -> Option<Vec<u32>> {
    let mut color = HashMap::new();
    let mut rec_stack = HashSet::new();
    
    // DFS with cycle detection
    for node in graph.nodes.keys() {
        if self.has_cycle_dfs(*node, &graph, &mut color, &mut rec_stack) {
            return Some(self.build_cycle_path(&rec_stack));
        }
    }
    None
}
```

### Performance Characteristics

| Operation | Target Time | Typical Time |
|-----------|-------------|--------------|
| Simple Reference (cached) | < 1ms | 0.2-0.8ms |
| Complex Hierarchy (cached) | < 5ms | 2-4ms |
| Full Resolution (uncached) | < 100ms | 30-80ms |
| Circular Detection | < 10ms | 2-8ms |
| Dependency Prefetch | < 20ms | 5-15ms |

### Enterprise Features

**Smart Prefetching**:
- Analyzes access patterns to predict needed references
- Preloads dependencies before they're requested
- Maintains correlation scores for reference pairs
- Configurable prefetch depth and strategy

**Reference Evolution**:
- Tracks changes across dependent schemas
- Generates migration plans for breaking changes
- Supports gradual rollout strategies
- Provides impact analysis for schema updates

**Monitoring & Alerting**:
- Resolution time tracking per reference depth
- Circular reference detection alerts
- Cache hit rate monitoring for references
- Dependency graph size and complexity metrics

## Data Flow Architecture

### Schema Resolution Flow

```
1. Client Request
   └─> Check L1 (Hot Cache) ─┐
                              │
2. L1 Miss                   │  L1 Hit
   └─> Check L2 (Main Cache) ─┼─> Return Schema
                              │
3. L2 Miss                   │  L2 Hit
   └─> Check L3 (Resolved) ──┼─> Promote to L1 if hot
                              │   └─> Return Schema
4. L3 Miss                   │
   └─> Fetch from Registry ──┘
       └─> Resolve References
           └─> Cache at all levels
               └─> Return Schema
```

### Reference Resolution Flow

```
1. Schema Request with References
   └─> Build Dependency Graph
       └─> Detect Circular Dependencies
           └─> Topological Sort
               └─> Resolve in Dependency Order
                   └─> Flatten Schema
                       └─> Cache Resolved Result
```

## Performance Characteristics

### Latency Targets

| Operation | Target Latency | Typical Latency |
|-----------|---------------|-----------------|
| L1 Cache Hit | < 1ms | 0.1-0.5ms |
| L2 Cache Hit | < 5ms | 1-3ms |
| L3 Cache Hit | < 10ms | 3-8ms |
| Registry Fetch | < 200ms | 50-150ms |
| Reference Resolution | < 500ms | 100-400ms |

### Throughput Targets

- **Cache Operations**: 10,000+ ops/sec
- **Schema Resolution**: 1,000+ ops/sec
- **Registry Requests**: 100+ ops/sec

### Memory Usage

- **L1 Cache**: ~10MB (100 schemas)
- **L2 Cache**: ~100MB (10,000 schemas)
- **L3 Cache**: ~50MB (resolved schemas)
- **Total System**: ~200MB typical usage

## Configuration

### Production Configuration

```rust
let config = CacheConfig {
    hot_cache_size: 100,
    max_cache_size: 10000,
    schema_ttl_seconds: 300,           // 5 minutes
    resolved_ttl_seconds: 600,         // 10 minutes
    enable_prefetching: true,
    refresh_interval_seconds: 60,      // 1 minute
    max_prefetch_depth: 3,
    enable_persistence: true,
    persistence_path: Some("/var/cache/velo/schemas.json".to_string()),
};
```

### Monitoring Configuration

```rust
// Enable comprehensive metrics collection
let metrics = cache.metrics().await;
println!("Cache Hit Rate: {:.2}%", metrics.hit_rate * 100.0);
println!("Average Lookup: {:.2}μs", metrics.avg_lookup_time_us);
```

## Integration Patterns

### Kafka Integration

```rust
// Schema Registry with Kafka consumer
let registry_client = SchemaRegistryClient::new("http://schema-registry:8081")
    .with_auth(AuthConfig::Basic { username, password });

let cache = EnhancedSchemaCache::with_config(cache_config);
let resolver = SchemaReferenceResolver::new(registry_client.clone());

// Use in Kafka consumer/producer
let consumer = ConfigurableKafkaConsumer::builder()
    .with_schema_registry(registry_client)
    .with_cache(cache)
    .build()?;
```

### SQL Integration

```sql
-- Create stream with Schema Registry integration
CREATE STREAM orders_with_schema AS
SELECT * FROM kafka_orders_source
INTO processed_orders_sink
WITH (
    source_config='configs/kafka_orders_with_registry.yaml',
    sink_config='configs/processed_orders_sink.yaml'
);
```

## Failure Handling

### Circuit Breaker Pattern
- Automatic failover when Registry is unavailable
- Degraded mode operation with cached schemas only
- Health check integration with monitoring systems

### Error Recovery
- Automatic retry with exponential backoff
- Fallback to cached versions during outages
- Dead letter queue for failed operations

### Monitoring & Alerting
- Schema Registry connectivity monitoring
- Cache performance metrics
- Schema evolution failure alerts
- Dependency resolution timeout warnings

## Security Considerations

### Authentication
- Support for Basic Authentication
- Bearer Token authentication
- Integration with enterprise identity systems

### Data Protection
- Schema content encryption at rest (when persistence enabled)
- Secure transmission over HTTPS
- Access control integration

### Compliance
- Schema access auditing
- Version tracking for regulatory compliance
- Data lineage through schema evolution

This architecture provides enterprise-grade schema management with high performance, reliability, and scalability for production streaming applications.