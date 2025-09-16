# Schema Registry Quick Start Guide

## Overview

This guide provides practical examples for using the VeloStream Schema Registry system with its advanced caching and reference resolution capabilities.

## Basic Setup

### 1. Create Schema Registry Client

```rust
use velostream::velo::sql::schema::{
    SchemaRegistryClient, AuthConfig, RegistryClientConfig
};

// Basic setup
let client = SchemaRegistryClient::new("http://localhost:8081");

// With authentication
let client = SchemaRegistryClient::new("http://localhost:8081")
    .with_auth(AuthConfig::Basic {
        username: "registry_user".to_string(),
        password: "registry_pass".to_string(),
    });

// With custom configuration
let config = RegistryClientConfig {
    timeout_seconds: 30,
    max_retries: 3,
    retry_delay_ms: 1000,
    cache_ttl_seconds: 600,
    resolve_references: true,
};

let client = SchemaRegistryClient::with_config(
    "http://localhost:8081".to_string(),
    config
);
```

### 2. Setup Enhanced Caching

```rust
use velostream::velo::sql::schema::{
    EnhancedSchemaCache, EnhancedCacheConfig
};

// Default cache
let cache = EnhancedSchemaCache::new();

// Production cache configuration
let cache_config = EnhancedCacheConfig {
    hot_cache_size: 200,              // L1 cache size
    max_cache_size: 50000,            // L2 cache size  
    schema_ttl_seconds: 600,          // 10 minutes
    resolved_ttl_seconds: 1200,       // 20 minutes
    enable_prefetching: true,         // Smart prefetching
    refresh_interval_seconds: 300,    // 5 minutes
    max_prefetch_depth: 5,           // Prefetch depth
    enable_persistence: true,         // Disk persistence
    persistence_path: Some("/var/cache/velo/schemas.json".to_string()),
};

let cache = EnhancedSchemaCache::with_config(cache_config);
```

### 3. Initialize Reference Resolver

```rust
use velostream::velo::sql::schema::{
    SchemaReferenceResolver, ResolverConfig
};
use std::sync::Arc;

let resolver_config = ResolverConfig {
    max_depth: 15,                   // Maximum reference depth
    detect_cycles: true,             // Circular dependency detection
    cache_dependencies: true,        // Cache resolved dependencies
    validate_compatibility: true,    // Schema compatibility checks
    max_references: 200,            // Maximum references per schema
};

let resolver = SchemaReferenceResolver::with_config(
    Arc::new(client),
    resolver_config
);
```

## Common Operations

### Schema Retrieval

```rust
// Get schema by ID
let schema = client.get_schema(12345).await?;
println!("Schema: {}", schema.schema);

// Get latest schema for subject
let latest = client.get_latest_schema("orders-value").await?;
println!("Latest version: {}", latest.version);

// Get with reference resolution
let resolved = resolver.resolve_references(12345).await?;
println!("Dependencies: {}", resolved.dependencies.len());
```

### Schema Registration

```rust
use velostream::velo::sql::schema::SchemaReference;

// Simple schema registration
let schema_json = r#"{
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"}
    ]
}"#;

let schema_id = client.register_schema("orders-value", schema_json, vec![]).await?;
println!("Registered schema ID: {}", schema_id);

// Schema with references
let references = vec![
    SchemaReference {
        name: "CustomerInfo".to_string(),
        subject: "customer-info-value".to_string(),
        version: Some(2),
        schema_id: Some(98765),
    }
];

let schema_with_refs = r#"{
    "type": "record",
    "name": "OrderWithCustomer",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "customer", "type": "CustomerInfo"}
    ]
}"#;

let schema_id = client.register_schema(
    "orders-with-customer-value", 
    schema_with_refs, 
    references
).await?;
```

### Compatibility Checking

```rust
// Check if new schema is compatible
let is_compatible = client.validate_schema_compatibility(
    "orders-value",
    new_schema_json
).await?;

if is_compatible {
    println!("Schema is compatible - safe to deploy");
} else {
    println!("Breaking changes detected!");
}

// Advanced compatibility with reference analysis
let compatibility_result = resolver.validate_reference_compatibility(
    &old_resolved_schema,
    &new_resolved_schema
).await?;

if !compatibility_result.is_compatible {
    for change in &compatibility_result.breaking_changes {
        println!("Breaking change: {}", change.description);
    }
}
```

## Advanced Features

### Schema Evolution & Migration

```rust
// Generate migration plan
let migration_plan = resolver.generate_migration_plan(
    &old_schema,
    &new_schema
).await?;

println!("Migration steps: {}", migration_plan.migration_steps.len());
println!("Risk level: {:?}", migration_plan.risk_level);

for step in &migration_plan.migration_steps {
    println!("Step {}: {}", step.step_number, step.description);
    println!("  Reversible: {}", step.is_reversible);
}
```

### Rollout Strategies

```rust
use velostream::velo::sql::schema::RolloutStrategy;

// Canary deployment
let canary_plan = resolver.create_rollout_plan(
    "orders-value",
    new_schema_json,
    RolloutStrategy::Canary { percentage: 10 }
).await?;

// Blue-Green deployment  
let blue_green_plan = resolver.create_rollout_plan(
    "orders-value",
    new_schema_json,
    RolloutStrategy::BlueGreen
).await?;

// Rolling deployment
let rolling_plan = resolver.create_rollout_plan(
    "orders-value", 
    new_schema_json,
    RolloutStrategy::Rolling { batch_size: 5 }
).await?;

println!("Estimated deployment time: {} minutes", 
         rolling_plan.estimated_total_time_minutes);
```

### Cache Operations

```rust
// Manual cache operations
cache.put(schema.clone()).await;
let cached_schema = cache.get(schema_id).await;

// Cache metrics monitoring
let metrics = cache.metrics().await;
println!("Cache hit rate: {:.2}%", metrics.hit_rate * 100.0);
println!("L1 hits: {}, L2 hits: {}, L3 hits: {}", 
         metrics.l1_hits, metrics.l2_hits, metrics.l3_hits);
println!("Average lookup time: {:.2}μs", metrics.avg_lookup_time_us);

// Cache warming
let important_schemas = vec![1001, 1002, 1003, 1004];
cache.warm_up(important_schemas).await;

// Cache persistence
cache.persist().await?;  // Save to disk
cache.load().await?;     // Load from disk
```

## Integration Examples

### Kafka Consumer with Schema Registry

```rust
// This would be the integration pattern (implementation in Phase 2.5d)
use velostream::velo::kafka::ConfigurableKafkaConsumer;

let consumer = ConfigurableKafkaConsumer::builder()
    .brokers("localhost:9092")
    .group_id("order-processor")
    .with_schema_registry(client)
    .with_cache(cache)
    .with_resolver(resolver)
    .build()?;

// Consumer will automatically:
// 1. Fetch schemas from registry
// 2. Cache for performance  
// 3. Resolve references
// 4. Handle evolution
```

### SQL Stream Processing

```sql
-- Schema Registry configuration in external YAML files
CREATE STREAM enriched_orders AS
SELECT 
    o.order_id,
    o.customer_id,
    o.amount,
    c.customer_name,    -- From referenced schema
    c.customer_tier     -- From referenced schema
FROM kafka_orders o
JOIN kafka_customers c ON o.customer_id = c.customer_id
INTO processed_orders
WITH (
    source_config='configs/orders_with_registry.yaml',
    sink_config='configs/processed_orders.yaml'
);
```

**orders_with_registry.yaml:**
```yaml
type: kafka
brokers: ["localhost:9092"]
topic: "raw-orders"
group_id: "order-processor"
format: avro
schema_registry:
  url: "http://localhost:8081"
  subject: "orders-value"
  references:
    enabled: true
    cache_ttl_seconds: 600
    prefetch_depth: 3
    resolve_strategy: "eager"
```

## Performance Tuning

### Cache Tuning

```rust
// High-throughput configuration
let config = EnhancedCacheConfig {
    hot_cache_size: 500,              // More L1 entries
    max_cache_size: 100000,           // Larger L2 cache
    schema_ttl_seconds: 1800,         // Longer TTL (30 min)
    enable_prefetching: true,         // Aggressive prefetching
    max_prefetch_depth: 8,           // Deeper prefetching
    refresh_interval_seconds: 120,    // Frequent refresh
    ..Default::default()
};

// Memory-constrained configuration  
let config = EnhancedCacheConfig {
    hot_cache_size: 50,               // Smaller L1
    max_cache_size: 1000,             // Smaller L2
    schema_ttl_seconds: 180,          // Shorter TTL (3 min)
    enable_prefetching: false,        // Disable prefetching
    enable_persistence: false,        // No disk persistence
    ..Default::default()
};
```

### Monitoring Setup

```rust
use std::time::Duration;
use tokio::time::interval;

// Metrics collection loop
tokio::spawn(async move {
    let mut interval = interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        
        let metrics = cache.metrics().await;
        
        // Log metrics (integrate with your monitoring system)
        log::info!("Schema Cache Metrics:");
        log::info!("  Hit Rate: {:.2}%", metrics.hit_rate * 100.0);
        log::info!("  Lookup Time: {:.2}μs", metrics.avg_lookup_time_us);
        log::info!("  Cache Size: {} bytes", metrics.current_size_bytes);
        
        // Alert on low hit rate
        if metrics.hit_rate < 0.85 {
            log::warn!("Schema cache hit rate below 85%: {:.2}%", 
                      metrics.hit_rate * 100.0);
        }
    }
});
```

## Error Handling

### Comprehensive Error Handling

```rust
use velostream::velo::sql::schema::SchemaError;

match client.get_schema(schema_id).await {
    Ok(schema) => {
        // Success - use schema
        println!("Retrieved schema: {}", schema.subject);
    }
    Err(SchemaError::NotFound { source }) => {
        log::warn!("Schema not found: {}", source);
        // Fallback to default or cached version
    }
    Err(SchemaError::Provider { source, message }) => {
        log::error!("Registry error for {}: {}", source, message);
        // Maybe retry or use cache
    }
    Err(SchemaError::Evolution { from, to, reason }) => {
        log::error!("Evolution error from {} to {}: {}", from, to, reason);
        // Handle breaking changes
    }
    Err(e) => {
        log::error!("Unexpected schema error: {}", e);
        return Err(e.into());
    }
}
```

### Circuit Breaker Pattern

```rust
// This would be added in Phase 2.5d - Integration
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

struct SchemaRegistryCircuitBreaker {
    failure_count: AtomicU32,
    last_failure: Option<Instant>,
    threshold: u32,
    timeout: Duration,
}

impl SchemaRegistryCircuitBreaker {
    async fn call_registry<F, R>(&self, f: F) -> Result<R, SchemaError>
    where
        F: Future<Output = Result<R, SchemaError>>,
    {
        // Check if circuit is open
        if self.is_open() {
            log::warn!("Schema Registry circuit breaker is OPEN");
            return Err(SchemaError::Provider {
                source: "circuit_breaker".to_string(),
                message: "Circuit breaker is open".to_string(),
            });
        }

        // Try the operation
        match f.await {
            Ok(result) => {
                self.failure_count.store(0, Ordering::Relaxed);
                Ok(result)
            }
            Err(e) => {
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }
    
    fn is_open(&self) -> bool {
        self.failure_count.load(Ordering::Relaxed) >= self.threshold
    }
}
```

This comprehensive documentation covers the complete Schema Registry architecture and provides practical examples for all major use cases. The system is now fully documented for enterprise deployment!