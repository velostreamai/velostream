# Feature Request: Self-Registering Configuration Schema System (Compact)

**Feature ID**: FSS-2024-001-COMPACT  
**Priority**: High  
**Status**: RFC  
**Created**: 2025-09-09  

## Executive Summary

Implement a self-registering configuration schema system to prevent configuration drift and automate validation for VeloStream' multi-source/multi-sink architecture. Each config-consuming class owns its validation schema, ensuring consistency and preventing deployment failures.

## Problem & Solution

**Current Issues:**
- Configuration drift: Schema rules scattered across docs/tests
- Runtime failures: Config errors discovered at deployment time  
- No schema ownership: Classes don't validate their own configuration
- Complex inheritance: Global + named + file inheritance lacks validation

**Solution:**
Self-registering schema system where `KafkaDataSource`, `BatchConfig`, `FileSink`, etc. own and register their validation schemas automatically.

## Integration with Existing Architecture

### Supporting Documentation
- **[MULTI_SOURCE_SINK_GUIDE.md](../data-sources/MULTI_SOURCE_SINK_GUIDE.md)** - Validates named source/sink patterns, global inheritance, production examples
- **[BATCH_CONFIGURATION_GUIDE.md](../BATCH_CONFIGURATION_GUIDE.md)** - Validates batch strategy and performance configurations
- **Existing implementations** - Zero breaking changes, adds validation layer before existing `from_properties()` methods

### Current Implementation Compatibility
```rust
// Existing (UNCHANGED)
impl KafkaDataSource {
    pub fn from_properties(props: &HashMap<String, String>) -> Self { /* existing */ }
}

// NEW (Schema validation layer)  
impl ConfigSchemaProvider for KafkaDataSource {
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "bootstrap.servers" => validate_kafka_servers(value),
            "topic" => validate_topic_name(value),
            "batch.strategy" => validate_batch_strategy(value), // FixedSize, AdaptiveSize, etc.
            "failure_strategy" => validate_failure_strategy(value), // RetryWithBackoff, etc.
            "batch.size" => validate_positive_integer(value),
            "batch.timeout" => validate_duration(value), // "5000ms", "30s"
            _ => Ok(()) // Allow other Kafka properties
        }
    }
}
```

## Core Architecture

### ConfigSchemaProvider Trait
```rust
pub trait ConfigSchemaProvider {
    fn config_type_id() -> &'static str;
    fn inheritable_properties() -> Vec<&'static str>;
    fn required_named_properties() -> Vec<&'static str>;
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>>;
    fn json_schema() -> Value;
    
    // Extensibility features
    fn supports_custom_properties() -> bool { true }
    fn global_schema_dependencies() -> Vec<&'static str> { vec![] }
    fn resolve_property_with_inheritance(
        &self, key: &str, local_value: Option<&str>, 
        global_context: &GlobalSchemaContext
    ) -> Result<Option<String>, String>;
    fn schema_version() -> &'static str { "1.0.0" }
}
```

### HierarchicalSchemaRegistry
```rust
pub struct HierarchicalSchemaRegistry {
    global_schemas: HashMap<String, Box<dyn ConfigSchemaProvider>>,
    source_schemas: HashMap<String, Box<dyn ConfigSchemaProvider>>,
    sink_schemas: HashMap<String, Box<dyn ConfigSchemaProvider>>,
    global_context: GlobalSchemaContext,
}

impl HierarchicalSchemaRegistry {
    pub fn validate_config_with_inheritance(
        &self,
        global_config: &HashMap<String, String>,
        named_configs: &HashMap<String, HashMap<String, String>>,
        config_files: &HashMap<String, String>,
    ) -> Result<(), Vec<String>> {
        // Validate global → config file → named → inline hierarchy
    }
}
```

## Validation Touchpoints

### Complete Validation Integration
| **Validation Point** | **When** | **Implementation** | **Performance** |
|----------------------|----------|-------------------|-----------------|
| **IDE Integration** | Real-time typing | Language Server Protocol | < 100ms |
| **SQL Validator** | File validation | Enhanced SqlValidator + schema registry | < 100ms |
| **CLI Tools** | `velo-sql-multi validate --file` | StreamJobServer + SqlValidator | < 100ms |
| **CI/CD Pipeline** | Every commit/PR | GitHub Actions/GitLab CI | < 5s |
| **REST API** | `/api/jobs/validate` | HTTP validation endpoint | < 100ms |
| **Pre-deployment** | Before job deployment | StreamJobServer integration | < 100ms |
| **DataSource Creation** | Source/sink instantiation | Factory pattern integration | < 10ms |

### SQL Validator Integration
```rust
impl SqlValidator {
    pub fn validate_with_config_schema(&self, sql: &str) -> Result<ValidationResult, ValidationError> {
        // PHASE 1: Existing SQL syntax validation (UNCHANGED)
        let mut result = self.validate_sql_syntax(sql)?;
        
        // PHASE 2: NEW - Configuration schema validation
        let schema_registry = HierarchicalSchemaRegistry::global();
        let sql_config = self.extract_configuration_from_sql(sql)?;
        schema_registry.validate_sql_configuration(&sql_config)?;
        
        Ok(result)
    }
}
```

### CLI Integration
```bash
# Combined SQL + Config validation
velo-sql-multi validate --file ./demo.sql
velo-sql-multi deploy-app --file ./demo.sql  # Auto-validates before deploy

# Standalone config validation
velo-config validate ./configs/ --check-inheritance
```

## Enhanced Features

### Future Extensibility
```rust
// 2024: Original configuration
'kafka_source.bootstrap.servers' = 'kafka1:9092',
'kafka_source.topic' = 'orders'

// 2025: Extended with custom properties (backward compatible)  
'kafka_source.bootstrap.servers' = 'kafka1:9092',
'kafka_source.topic' = 'orders',
'kafka_source.custom.message.headers' = 'trace-id,correlation-id',  // NEW
'kafka_source.consumer.max.partition.fetch.bytes' = '2097152'        // NEW
```

### Batch Strategy & Failure Strategy Integration
```sql
WITH (
    -- Global performance configuration inherited by ALL sources/sinks
    'batch.strategy' = 'AdaptiveSize',           -- Performance optimization
    'failure_strategy' = 'RetryWithBackoff',     -- Error handling strategy
    'batch.size' = '1000',                      -- Default batch size
    'batch.timeout' = '5000ms',                 -- Default timeout
    'bootstrap.servers' = 'kafka-cluster:9092',  -- Infrastructure config
    
    -- Named source with performance mode
    'kafka_orders.type' = 'kafka_source',
    'kafka_orders.topic' = 'raw-orders',
    'kafka_orders.batch.strategy' = 'MemoryBased',  -- Override for high-volume source
    -- Inherits failure_strategy and bootstrap.servers from global
    
    -- Sink with specific failure handling
    'file_sink.type' = 'file_sink',
    'file_sink.failure_strategy' = 'LogAndContinue',  -- Override for non-critical sink
    'file_sink.batch.size' = '500'                   -- Override batch size
);
```

### Performance Modes with Batch Strategy Implementation
```rust
// High-throughput mode: Large batches with robust error handling
batch.strategy = "MemoryBased(16777216)"  // 16MB batches
→ Kafka: batch.size=8388608, compression.type=gzip, linger.ms=100
→ File: buffer_size=16MB, compression=Gzip
+ failure_strategy = "RetryWithBackoff" // Exponential backoff for resilience

// Low-latency mode: Minimal buffering for speed  
batch.strategy = "LowLatency{max_wait_time=10ms}"
→ Kafka: batch.size=1024, compression.type=none, linger.ms=10, acks=1
→ File: buffer_size=4KB, compression=none
+ failure_strategy = "LogAndContinue"  // Non-blocking failure handling

// Adaptive mode: Dynamic optimization based on load
batch.strategy = "AdaptiveSize{target_latency=100ms}"
→ Kafka: batch.size=32KB, compression.type=snappy, linger.ms=100
→ File: buffer_size=512KB, adaptive compression
+ failure_strategy = "RetryWithBackoff" // Balanced error handling

// Fixed size: Predictable batch processing
batch.strategy = "FixedSize(1000)"
→ Kafka: batch.size=1MB, compression.type=snappy, linger.ms=10
→ File: buffer_size=4MB estimated (1000 * 4KB per record)

// Time-based: Consistent time windows
batch.strategy = "TimeWindow(5s)"
→ Kafka: linger.ms=5000, batch.size=64KB, compression.type=lz4
→ File: buffer_size=1MB for time-based batching
```

### Schema Versioning & Migration
```rust
impl ConfigSchemaProvider for KafkaDataSource {
    fn migration_rules() -> Vec<MigrationRule> {
        vec![
            MigrationRule {
                from_version: "1.0.0".to_string(),
                to_version: "2.0.0".to_string(), 
                property_migrations: vec![
                    PropertyMigration::Rename {
                        from: "brokers".to_string(),
                        to: "bootstrap.servers".to_string(),
                    }
                ]
            }
        ]
    }
}
```

## Implementation Plan

### Phase 1: Core Infrastructure (4-6 weeks)
- Create `ConfigSchemaProvider` trait and `HierarchicalSchemaRegistry`
- Implement schema for `BatchConfig`, `KafkaDataSource`, `KafkaDataSink`
- **Batch Strategy Validation**: Support for all 5 strategy types with performance presets
- **Failure Strategy Integration**: Validate retry policies and error handling modes
- Integrate with `SqlValidator` and `StreamJobServer`
- Add comprehensive error reporting with line numbers and performance recommendations

### Phase 2: Complete Coverage (3-4 weeks)  
- Implement schemas for `FileDataSource`, `FileSink`, additional sources/sinks
- Add config file inheritance validation (`extends` chains)
- Implement environment variable validation patterns
- Add schema versioning and migration support

### Phase 3: Tooling & Integration (2-3 weeks)
- Generate JSON Schema for IDE integration  
- Create `velo-config` CLI validation tool
- Add CI/CD pipeline integration examples
- Performance optimization (< 50ms validation time)

**Total Timeline:** 13 weeks, 96 dev-hours

## Key Benefits

### Development Impact
- **90% reduction** in config-related deployment failures
- **< 50ms validation time** for typical configurations  
- **100% schema coverage** for config-consuming classes
- **Auto-generated documentation** always in sync

### Critical Configuration Aspects Validated

**Batch Strategy Patterns:**
✅ `FixedSize(1000)` - Predictable batch sizes for consistent throughput
✅ `TimeWindow(Duration::from_secs(5))` - Time-based batching for real-time analytics  
✅ `AdaptiveSize{target_latency: 100ms}` - Dynamic sizing for variable loads
✅ `MemoryBased(16*1024*1024)` - Memory-optimized for high-volume processing
✅ `LowLatency{max_wait_time: 10ms}` - Ultra-fast processing for critical paths

**Failure Strategy Integration:**
✅ `RetryWithBackoff` - Exponential backoff for transient failures
✅ `RetryWithFixedDelay` - Fixed delay retry for predictable error handling
✅ `LogAndContinue` - Non-blocking failure handling for non-critical sinks
✅ `FailFast` - Immediate failure for strict data integrity requirements

**Performance Mode Combinations:**
✅ High-throughput: `MemoryBased` + `RetryWithBackoff` + compression
✅ Low-latency: `LowLatency` + `LogAndContinue` + minimal buffering
✅ Balanced: `AdaptiveSize` + `RetryWithBackoff` + dynamic optimization
✅ Financial: `FixedSize` + `FailFast` + exact precision requirements

### Error Prevention Strategy
```
Development (IDE) → CI/CD (Automated) → API (External) → Deployment (Mandatory) → Runtime (Safety Net)
```

## Technical References

### VeloStream Documentation  
- **MULTI_SOURCE_SINK_GUIDE.md** - Primary configuration patterns  
- **BATCH_CONFIGURATION_GUIDE.md** - Batch processing configuration  
- **Existing source files** - Current `from_properties()` implementations  
- **Test files** - Configuration validation patterns  

### Implementation Evidence  
- **`src/velo/datasource/kafka/data_source.rs`** - KafkaDataSource implementation  
- **`src/velo/sql/query_analyzer.rs`** - Configuration extraction from SQL  
- **`tests/unit/sql/config_file_test.rs`** - Config file loading patterns  

### External References  
- **[JSON Schema Specification](https://json-schema.org/specification.html)**  
- **[Serde YAML Documentation](https://docs.rs/serde_yaml/)**  

## Risk Mitigation

**Technical Risks:**  
- **Performance**: < 50ms validation requirement, deployment-time only  
- **Compatibility**: Comprehensive testing against existing configurations  
- **Complexity**: Phased approach, core functionality first  

**Business Risks:**  
- **Timeline**: 13-week phased delivery allows early value  
- **Adoption**: Gradual rollout, opt-in initially then mandatory  

## Success Metrics

- **Configuration Error Reduction**: 90% fewer deployment failures  
- **Validation Performance**: < 50ms for typical configurations  
- **Schema Coverage**: 100% of config-consuming classes  
- **Documentation Accuracy**: Auto-generated, 0% drift  
- **Developer Productivity**: 25% reduction in config debugging time  

## Approval Required

**Technical Review:** Architecture Team, Senior Backend Engineer, DevOps Lead  
**Product Review:** Product Manager, Engineering Manager  
**Final Approval:** CTO

---

This self-registering schema system provides comprehensive configuration validation while maintaining full compatibility with VeloStream' existing sophisticated multi-source/multi-sink architecture documented in MULTI_SOURCE_SINK_GUIDE.md.