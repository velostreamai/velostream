# Schema References Support Guide

## Overview

VeloStream provides comprehensive support for Schema Registry schema references, enabling complex schema compositions, dependency management, and evolution strategies. This feature allows schemas to reference other schemas, creating reusable schema components and maintaining consistency across related data structures.

## What Are Schema References?

Schema references allow one Avro schema to reference types defined in other schemas. This enables:
- **Schema Composition**: Building complex schemas from smaller, reusable components
- **Type Reuse**: Sharing common types across multiple schemas
- **Dependency Management**: Tracking relationships between schemas
- **Evolution Control**: Managing changes across dependent schemas

## Basic Schema Reference Structure

### Referenced Schema (Customer Info)
```json
{
  "type": "record",
  "name": "CustomerInfo",
  "namespace": "com.company.customer",
  "fields": [
    {"name": "customer_id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "tier", "type": {"type": "enum", "name": "CustomerTier", "symbols": ["BRONZE", "SILVER", "GOLD", "PLATINUM"]}}
  ]
}
```

### Referencing Schema (Order)
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.company.orders",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer", "type": "com.company.customer.CustomerInfo"},
    {"name": "amount", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

### Schema Reference Definition
```rust
use velostream::velo::sql::schema::SchemaReference;

let reference = SchemaReference {
    name: "CustomerInfo".to_string(),           // Type name in the schema
    subject: "customer-info-value".to_string(), // Registry subject name
    version: Some(3),                           // Specific version (optional)
    schema_id: Some(12345),                     // Resolved schema ID
};
```

## Working with Schema References

### 1. Registering Schemas with References

```rust
use velostream::velo::sql::schema::{SchemaRegistryClient, SchemaReference};

// First, register the referenced schema
let customer_schema = r#"{
    "type": "record",
    "name": "CustomerInfo",
    "namespace": "com.company.customer",
    "fields": [
        {"name": "customer_id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "tier", "type": {
            "type": "enum", 
            "name": "CustomerTier", 
            "symbols": ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
        }}
    ]
}"#;

let customer_schema_id = client.register_schema(
    "customer-info-value",
    customer_schema,
    vec![] // No references
).await?;

// Then register the referencing schema
let order_schema = r#"{
    "type": "record",
    "name": "Order",
    "namespace": "com.company.orders",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer", "type": "com.company.customer.CustomerInfo"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

let references = vec![
    SchemaReference {
        name: "com.company.customer.CustomerInfo".to_string(),
        subject: "customer-info-value".to_string(),
        version: Some(1), // Reference specific version
        schema_id: Some(customer_schema_id),
    }
];

let order_schema_id = client.register_schema(
    "orders-value",
    order_schema,
    references
).await?;
```

### 2. Resolving Schema References

```rust
use velostream::velo::sql::schema::SchemaReferenceResolver;

// Create resolver
let resolver = SchemaReferenceResolver::new(Arc::new(client));

// Resolve schema with all its references
let resolved_schema = resolver.resolve_references(order_schema_id).await?;

println!("Root schema: {}", resolved_schema.root_schema.subject);
println!("Dependencies: {}", resolved_schema.dependencies.len());

// Access resolved dependencies
for (id, dependency) in &resolved_schema.dependencies {
    println!("Dependency {}: {} v{}", id, dependency.subject, dependency.version);
}

// Get flattened schema (all references resolved inline)
println!("Flattened schema: {}", resolved_schema.flattened_schema);
```

### 3. Complex Reference Hierarchies

```rust
// Address schema (base component)
let address_schema = r#"{
    "type": "record",
    "name": "Address",
    "namespace": "com.company.common",
    "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "zip", "type": "string"},
        {"name": "country", "type": "string"}
    ]
}"#;

// Customer schema (references Address)
let customer_with_address_schema = r#"{
    "type": "record",
    "name": "CustomerWithAddress",
    "namespace": "com.company.customer",
    "fields": [
        {"name": "customer_id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "billing_address", "type": "com.company.common.Address"},
        {"name": "shipping_address", "type": "com.company.common.Address"}
    ]
}"#;

// Order schema (references Customer, which references Address)
let complex_order_schema = r#"{
    "type": "record",
    "name": "ComplexOrder",
    "namespace": "com.company.orders",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer", "type": "com.company.customer.CustomerWithAddress"},
        {"name": "items", "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "OrderItem",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "quantity", "type": "int"},
                    {"name": "price", "type": "double"}
                ]
            }
        }},
        {"name": "total_amount", "type": "double"}
    ]
}"#;

// Register in dependency order
let address_id = client.register_schema("address-value", address_schema, vec![]).await?;

let customer_refs = vec![
    SchemaReference {
        name: "com.company.common.Address".to_string(),
        subject: "address-value".to_string(),
        version: Some(1),
        schema_id: Some(address_id),
    }
];
let customer_id = client.register_schema("customer-with-address-value", customer_with_address_schema, customer_refs).await?;

let order_refs = vec![
    SchemaReference {
        name: "com.company.customer.CustomerWithAddress".to_string(),
        subject: "customer-with-address-value".to_string(),
        version: Some(1),
        schema_id: Some(customer_id),
    }
];
let complex_order_id = client.register_schema("complex-orders-value", complex_order_schema, order_refs).await?;

// Resolve the complete hierarchy
let resolved = resolver.resolve_references(complex_order_id).await?;
println!("Total dependencies resolved: {}", resolved.dependencies.len());
println!("Resolution path: {:?}", resolved.resolution_path);
```

## Dependency Management

### 1. Circular Reference Detection

```rust
// This would create a circular reference (not allowed)
// Customer -> Order -> Customer

let config = ResolverConfig {
    max_depth: 10,
    detect_cycles: true,        // Enable circular dependency detection
    cache_dependencies: true,
    validate_compatibility: true,
    max_references: 50,
};

let resolver = SchemaReferenceResolver::with_config(Arc::new(client), config);

// This will detect and prevent circular references
match resolver.resolve_references(schema_id).await {
    Ok(resolved) => {
        println!("Schema resolved successfully");
    }
    Err(SchemaError::Evolution { reason, .. }) if reason.contains("Circular reference") => {
        println!("Circular reference detected and prevented!");
    }
    Err(e) => {
        println!("Other error: {}", e);
    }
}
```

### 2. Dependency Graph Analysis

```rust
// Build dependency graph
let graph = resolver.build_dependency_graph(schema_id).await?;

println!("Dependency graph for schema {}:", schema_id);
println!("Total nodes: {}", graph.nodes.len());
println!("Resolution order: {:?}", graph.resolution_order);

// Analyze dependencies
for (node_id, node) in &graph.nodes {
    println!("Node {}: {} v{} (depth: {})", 
             node_id, node.subject, node.version, node.depth);
}

// Check edges (dependencies)
for (schema_id, deps) in &graph.edges {
    if !deps.is_empty() {
        println!("Schema {} depends on: {:?}", schema_id, deps);
    }
}
```

### 3. Schema Evolution with References

```rust
// When evolving schemas with references, check impact
let old_resolved = resolver.resolve_references(old_schema_id).await?;
let new_resolved = resolver.resolve_references(new_schema_id).await?;

let compatibility = resolver.validate_reference_compatibility(
    &old_resolved,
    &new_resolved
).await?;

if !compatibility.is_compatible {
    println!("Schema evolution has breaking changes:");
    for change in &compatibility.breaking_changes {
        match change.change_type {
            EvolutionType::ReferenceRemoved => {
                println!("  - Reference removed: {}", change.description);
            }
            EvolutionType::ReferenceAdded => {
                println!("  - New reference added: {}", change.description);
            }
            _ => {
                println!("  - Other change: {}", change.description);
            }
        }
    }
}

// Generate migration plan for referenced schemas
let migration = resolver.generate_migration_plan(&old_resolved, &new_resolved).await?;
for step in &migration.migration_steps {
    match &step.operation {
        MigrationOperation::AddReference { reference } => {
            println!("Step {}: Add reference to {}", step.step_number, reference.subject);
        }
        MigrationOperation::RemoveReference { reference } => {
            println!("Step {}: Remove reference to {} (BREAKING)", step.step_number, reference.subject);
        }
        _ => {
            println!("Step {}: {}", step.step_number, step.description);
        }
    }
}
```

## Advanced Reference Features

### 1. Reference Versioning Strategies

```rust
// Latest version reference (evolves automatically)
let latest_ref = SchemaReference {
    name: "CustomerInfo".to_string(),
    subject: "customer-info-value".to_string(),
    version: None, // Use latest version
    schema_id: None, // Will be resolved automatically
};

// Pinned version reference (stable, no auto-evolution)
let pinned_ref = SchemaReference {
    name: "CustomerInfo".to_string(),
    subject: "customer-info-value".to_string(),
    version: Some(2), // Pinned to version 2
    schema_id: Some(12345),
};

// Range-based version reference (future enhancement)
// This would allow "compatible with versions 2-4"
```

### 2. Conditional References

```rust
// Schema with optional/conditional references
let flexible_schema = r#"{
    "type": "record",
    "name": "FlexibleOrder",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_info", "type": [
            "null",
            "com.company.customer.CustomerInfo"
        ]}, 
        {"name": "amount", "type": "double"}
    ]
}"#;

// This allows the schema to work with or without the reference
```

### 3. Reference Caching and Performance

```rust
// Enable aggressive caching for references
let cache_config = EnhancedCacheConfig {
    enable_prefetching: true,
    max_prefetch_depth: 5, // Prefetch 5 levels of dependencies
    refresh_interval_seconds: 300, // Refresh every 5 minutes
    ..Default::default()
};

let cache = EnhancedSchemaCache::with_config(cache_config);

// The cache will automatically:
// 1. Prefetch referenced schemas when a schema is accessed
// 2. Track access patterns to predict which references are needed
// 3. Keep frequently used reference chains hot in memory
```

## Configuration Examples

### Kafka Integration with Schema References

**kafka_source_with_references.yaml:**
```yaml
type: kafka
brokers: ["localhost:9092"]
topic: "complex-orders"
group_id: "order-processor"
format: avro
schema_registry:
  url: "http://localhost:8081"
  subject: "complex-orders-value"
  references:
    enabled: true
    resolution_strategy: "eager"     # Resolve all references immediately
    cache_ttl_seconds: 600          # Cache resolved schemas for 10 minutes
    max_depth: 10                   # Maximum reference depth
    detect_cycles: true             # Prevent circular references
    compatibility_check: "BACKWARD" # Ensure backward compatibility
```

### SQL Stream with Referenced Schemas

```sql
-- Stream processing with complex referenced schemas
CREATE STREAM order_enrichment AS
SELECT 
    o.order_id,
    o.customer.customer_id,           -- Access nested customer info
    o.customer.name,                  -- From referenced CustomerInfo schema
    o.customer.billing_address.city,  -- From nested Address reference
    o.total_amount,
    CURRENT_TIMESTAMP() as processed_at
FROM kafka_complex_orders o
WHERE o.customer.tier IN ('GOLD', 'PLATINUM')  -- Filter by customer tier
INTO enriched_orders_sink
WITH (
    source_config='configs/kafka_complex_orders.yaml',
    sink_config='configs/enriched_orders_sink.yaml'
);
```

## Best Practices

### 1. Reference Design Patterns

```rust
// ✅ Good: Bottom-up dependency design
// Base types first, then composed types
Address -> Customer -> Order

// ✅ Good: Shared common types
CommonTypes -> (Customer, Product, Order)

// ❌ Bad: Circular references
Customer -> Order -> Customer

// ❌ Bad: Deep nesting without purpose
A -> B -> C -> D -> E -> F (6+ levels)
```

### 2. Versioning Strategy

```rust
// ✅ Good: Pin critical references to specific versions
let critical_ref = SchemaReference {
    name: "CriticalType".to_string(),
    subject: "critical-type-value".to_string(), 
    version: Some(2), // Pin to tested version
    schema_id: Some(schema_id),
};

// ✅ Good: Use latest for non-breaking references
let safe_ref = SchemaReference {
    name: "SafeType".to_string(),
    subject: "safe-type-value".to_string(),
    version: None, // Auto-evolve with compatible changes
    schema_id: None,
};
```

### 3. Performance Optimization

```rust
// Configure resolver for your use case
let high_performance_config = ResolverConfig {
    max_depth: 5,                    // Limit depth for performance
    detect_cycles: true,             // Always enable for safety
    cache_dependencies: true,        // Cache for performance
    validate_compatibility: false,   // Skip for high-throughput
    max_references: 20,             // Reasonable limit
};

let thorough_config = ResolverConfig {
    max_depth: 15,                   // Allow deep hierarchies
    detect_cycles: true,             // Safety first
    cache_dependencies: true,        // Cache everything
    validate_compatibility: true,    // Full validation
    max_references: 100,            // Allow complex schemas
};
```

### 4. Error Handling

```rust
// Comprehensive error handling for references
async fn resolve_with_fallback(
    resolver: &SchemaReferenceResolver,
    schema_id: u32
) -> Result<ResolvedSchema, Box<dyn std::error::Error>> {
    match resolver.resolve_references(schema_id).await {
        Ok(resolved) => Ok(resolved),
        Err(SchemaError::Evolution { reason, .. }) if reason.contains("Circular") => {
            log::error!("Circular reference detected in schema {}", schema_id);
            // Could return a simplified version without problematic references
            Err("Circular reference detected".into())
        }
        Err(SchemaError::NotFound { source }) => {
            log::warn!("Referenced schema not found: {}", source);
            // Could proceed with partial resolution or cached fallback
            Err(format!("Missing reference: {}", source).into())
        }
        Err(e) => {
            log::error!("Schema resolution failed: {}", e);
            Err(e.into())
        }
    }
}
```

## Monitoring and Debugging

### Reference Resolution Metrics

```rust
// Track reference-specific metrics
let metrics = cache.metrics().await;
println!("Reference resolution stats:");
println!("  Average resolution time: {:.2}ms", metrics.avg_lookup_time_us / 1000.0);
println!("  Cache hit rate: {:.2}%", metrics.hit_rate * 100.0);
println!("  Prefetch success rate: {:.2}%", metrics.prefetch_success_rate * 100.0);

// Log complex resolutions
if resolved_schema.dependencies.len() > 5 {
    log::info!("Complex schema resolved: {} dependencies, {} levels deep",
               resolved_schema.dependencies.len(),
               resolved_schema.resolution_path.len());
}
```

### Debugging Reference Issues

```rust
// Debug complex reference resolution
async fn debug_schema_references(
    resolver: &SchemaReferenceResolver,
    schema_id: u32
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging schema references for ID: {}", schema_id);
    
    // Get the base schema first
    let base_schema = client.get_schema(schema_id).await?;
    println!("Base schema: {} v{}", base_schema.subject, base_schema.version);
    println!("References: {}", base_schema.references.len());
    
    for (i, reference) in base_schema.references.iter().enumerate() {
        println!("  Reference {}: {} -> {}", i + 1, reference.name, reference.subject);
        if let Some(version) = reference.version {
            println!("    Pinned to version: {}", version);
        }
        if let Some(schema_id) = reference.schema_id {
            println!("    Resolved to schema ID: {}", schema_id);
        }
    }
    
    // Build and analyze dependency graph
    let graph = resolver.build_dependency_graph(schema_id).await?;
    println!("\nDependency graph analysis:");
    println!("  Total nodes: {}", graph.nodes.len());
    println!("  Max depth: {}", graph.nodes.values().map(|n| n.depth).max().unwrap_or(0));
    println!("  Resolution order: {:?}", graph.resolution_order);
    
    // Check for potential issues
    if graph.nodes.len() > 20 {
        println!("⚠️  Warning: Large dependency graph ({} nodes)", graph.nodes.len());
    }
    
    let max_depth = graph.nodes.values().map(|n| n.depth).max().unwrap_or(0);
    if max_depth > 5 {
        println!("⚠️  Warning: Deep reference hierarchy (depth: {})", max_depth);
    }
    
    Ok(())
}
```

This comprehensive guide covers all aspects of schema references in VeloStream, from basic usage to advanced enterprise patterns. The reference resolution system provides powerful capabilities for managing complex schema hierarchies while maintaining performance and reliability.