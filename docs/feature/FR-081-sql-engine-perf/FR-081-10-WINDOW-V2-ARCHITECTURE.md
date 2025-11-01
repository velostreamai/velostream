# FR-081-10: Window V2 Architecture Guide

**Feature Request**: FR-081 SQL Window Processing Performance Optimization
**Version**: 1.0
**Status**: Production Ready (Feature-Flagged)
**Last Updated**: 2025-11-01

---

## Overview

Window V2 is a high-performance, trait-based window processing architecture that delivers **27-79x performance improvements** over the legacy implementation. It uses zero-copy semantics, Arc-based record sharing, and pluggable strategies to achieve throughput rates of **428K-1.23M records/second**.

### Key Metrics

| Metric | Legacy | Window V2 | Improvement |
|--------|--------|-----------|-------------|
| **TUMBLING throughput** | 15.7K rec/sec | 428K rec/sec | **27.3x** |
| **SLIDING throughput** | 15.7K rec/sec | 491K rec/sec | **31.3x** |
| **SESSION throughput** | 917 rec/sec | 1.01M rec/sec | **1,101x** |
| **ROWS throughput** | 46.5K rec/sec | 1.23M rec/sec | **26.5x** |
| **Per-record processing** | 60µs | 0.81-2.33µs | **25.8-74x faster** |
| **Memory overhead** | 1.0x (baseline) | 0.00-1.00x | Constant |

---

## Architecture Overview

### Design Principles

1. **Strategy Pattern**: Pluggable window strategies for different window types
2. **Zero-Copy Semantics**: Arc<StreamRecord> eliminates deep cloning
3. **Trait-Based**: Clear separation of concerns via WindowStrategy and EmissionStrategy traits
4. **Backward Compatible**: Adapter pattern enables gradual migration
5. **Feature-Flagged**: Production-safe rollout with instant rollback capability

### Component Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│              StreamExecutionEngine                          │
│                                                              │
│  ┌────────────────────────────────────────────────────┐   │
│  │        WindowProcessor                               │   │
│  │                                                       │   │
│  │  process_windowed_query_enhanced()                  │   │
│  │    │                                                  │   │
│  │    ├─> if window_v2_enabled:                        │   │
│  │    │     WindowAdapter::process_with_v2()           │   │
│  │    │                                                  │   │
│  │    ├─> else if watermarks_enabled:                  │   │
│  │    │     process_windowed_query_with_watermarks()   │   │
│  │    │                                                  │   │
│  │    └─> else:                                         │   │
│  │          process_windowed_query() [LEGACY]          │   │
│  └────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              WindowAdapter (Phase 2A.3)                     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐   │
│  │  process_with_v2(query_id, query, record, context)│   │
│  │    │                                                  │   │
│  │    ├─> create_strategy(WindowSpec)                 │   │
│  │    │     ├─> TumblingWindowStrategy                │   │
│  │    │     ├─> SlidingWindowStrategy                 │   │
│  │    │     ├─> SessionWindowStrategy                 │   │
│  │    │     └─> RowsWindowStrategy                    │   │
│  │    │                                                  │   │
│  │    ├─> create_emission_strategy(EmitMode)          │   │
│  │    │     ├─> EmitFinalStrategy                     │   │
│  │    │     └─> EmitChangesStrategy                   │   │
│  │    │                                                  │   │
│  │    ├─> get_group_by_columns(query)                 │   │
│  │    │                                                  │   │
│  │    └─> process_record_with_strategy()              │   │
│  │          ├─> emission_strategy.process_record()    │   │
│  │          ├─> strategy.get_window_records()         │   │
│  │          └─> convert_window_results()              │   │
│  └────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Window V2 Core (Phase 2A.1-2A.5)              │
│                                                              │
│  ┌───────────────────┐      ┌──────────────────────┐      │
│  │ WindowStrategy    │      │ EmissionStrategy     │      │
│  │ (Trait)           │      │ (Trait)              │      │
│  │                   │      │                      │      │
│  │ - add_record()    │      │ - process_record()   │      │
│  │ - get_window_     │      │ - should_emit_for_   │      │
│  │   records()       │      │   record()           │      │
│  │ - should_emit()   │      │                      │      │
│  │ - clear()         │      │ Returns EmitDecision │      │
│  │ - get_stats()     │      │ - Emit               │      │
│  └───────────────────┘      │ - Skip               │      │
│           │                  │ - EmitAndClear       │      │
│           │                  └──────────────────────┘      │
│           ▼                                                │
│  ┌─────────────────────────────────────────────────┐     │
│  │  SharedRecord (Arc<StreamRecord>)               │     │
│  │                                                    │     │
│  │  - Zero-copy record sharing                      │     │
│  │  - 42.9M clones/sec (23.29ns/clone)             │     │
│  │  - Constant memory overhead                      │     │
│  └─────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. WindowStrategy Trait

Defines how window boundaries are detected and managed.

```rust
pub trait WindowStrategy: Send + Sync {
    /// Add a record to the window buffer
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError>;

    /// Get all records in the current window
    fn get_window_records(&self) -> Vec<SharedRecord>;

    /// Check if window should emit based on time/size criteria
    fn should_emit(&self, current_time: i64) -> bool;

    /// Clear the window buffer (after emission)
    fn clear(&mut self);

    /// Get window statistics for monitoring
    fn get_stats(&self) -> WindowStats;
}
```

**Implementations**:

| Strategy | Window Type | Complexity | Memory Growth | Performance |
|----------|-------------|------------|---------------|-------------|
| `TumblingWindowStrategy` | Non-overlapping, time-based | O(1) add, O(N) emit | Linear | 428K rec/sec |
| `SlidingWindowStrategy` | Overlapping, advance interval | O(1) add, O(N) emit | Linear | 491K rec/sec |
| `SessionWindowStrategy` | Gap-based event grouping | O(1) add, O(1) gap check | Unbounded | 1.01M rec/sec |
| `RowsWindowStrategy` | Row-count-based, bounded | O(1) add, O(1) evict | **Constant** | 1.23M rec/sec |

### 2. EmissionStrategy Trait

Controls when and how window results are emitted.

```rust
pub trait EmissionStrategy: Send + Sync {
    /// Determine if results should be emitted for this record
    fn should_emit_for_record(&self, record: &SharedRecord, window_complete: bool) -> bool;

    /// Process a record and return emission decision
    fn process_record(
        &mut self,
        record: SharedRecord,
        window_strategy: &dyn WindowStrategy,
    ) -> Result<EmitDecision, SqlError>;
}

pub enum EmitDecision {
    Emit,           // Emit results now
    Skip,           // Do not emit yet
    EmitAndClear,   // Emit and clear window
}
```

**Implementations**:

| Strategy | Emission Behavior | Use Case |
|----------|-------------------|----------|
| `EmitFinalStrategy` | Emit once per window at boundary | Batch analytics, aggregations |
| `EmitChangesStrategy` | Emit on every record (or every Nth) | Real-time dashboards, streaming updates |

### 3. SharedRecord (Zero-Copy)

High-performance record sharing via Arc.

```rust
pub struct SharedRecord(Arc<StreamRecord>);

impl Clone for SharedRecord {
    fn clone(&self) -> Self {
        SharedRecord(Arc::clone(&self.0))  // 23.29ns/clone (42.9M/sec)
    }
}
```

**Benefits**:
- **42.9M clones/second** (vs deep clone overhead)
- **Constant memory** (shared ownership)
- **Thread-safe** (Arc provides Send + Sync)

### 4. WindowAdapter (Integration Layer)

Bridges legacy engine with window_v2 architecture.

```rust
pub struct WindowAdapter;

impl WindowAdapter {
    /// Main entry point for window_v2 processing
    pub fn process_with_v2(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError>;

    /// Create window strategy from WindowSpec
    pub fn create_strategy(window_spec: &WindowSpec)
        -> Result<Box<dyn WindowStrategy>, SqlError>;

    /// Create emission strategy from query
    pub fn create_emission_strategy(query: &StreamingQuery)
        -> Result<Box<dyn EmissionStrategy>, SqlError>;

    /// Convert SharedRecords back to StreamRecords
    pub fn convert_window_results(
        records: Vec<SharedRecord>,
        select_fields: &[SelectField],
    ) -> Result<Vec<StreamRecord>, SqlError>;
}
```

---

## Feature Flag System

### StreamingConfig

Centralized configuration for all streaming enhancements.

```rust
pub struct StreamingConfig {
    /// Enable window_v2 trait-based architecture (FR-081 Phase 2A)
    pub enable_window_v2: bool,

    // ... other feature flags
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            enable_window_v2: false,  // SAFE: defaults to legacy
            // ...
        }
    }
}

impl StreamingConfig {
    /// Enhanced mode with all optimizations enabled
    pub fn enhanced() -> Self {
        Self {
            enable_window_v2: true,  // Enable high-performance processing
            // ...
        }
    }

    /// Builder pattern for selective enablement
    pub fn with_window_v2(mut self) -> Self {
        self.enable_window_v2 = true;
        self
    }
}
```

### ProcessorContext Integration

```rust
pub struct ProcessorContext {
    // ... existing fields

    /// Optional streaming configuration
    pub streaming_config: Option<StreamingConfig>,

    /// Window V2 state storage (type-erased)
    pub window_v2_states: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl ProcessorContext {
    /// Check if window_v2 is enabled
    pub fn is_window_v2_enabled(&self) -> bool {
        self.streaming_config
            .as_ref()
            .map(|config| config.enable_window_v2)
            .unwrap_or(false)  // Default: disabled
    }

    /// Set streaming configuration
    pub fn set_streaming_config(&mut self, config: StreamingConfig) {
        self.streaming_config = Some(config);
    }
}
```

---

## Usage Examples

### Example 1: Enable window_v2 via Configuration

```rust
use velostream::velostream::sql::execution::config::StreamingConfig;
use velostream::velostream::sql::execution::engine::StreamExecutionEngine;

// Option 1: Enhanced mode (all optimizations)
let config = StreamingConfig::enhanced();

// Option 2: Selective enablement
let config = StreamingConfig::new()
    .with_window_v2();

// Create engine with configuration
let mut engine = StreamExecutionEngine::new(rx, tx);
let mut context = engine.create_processor_context("query_1");
context.set_streaming_config(config);
```

### Example 2: Tumbling Window with window_v2

```sql
-- This query will automatically use window_v2 if enabled
SELECT
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price
FROM trades
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY symbol;
```

```rust
// Engine automatically routes to window_v2 when enabled
let result = WindowProcessor::process_windowed_query_enhanced(
    query_id,
    &query,
    &record,
    &mut context,
    None,  // source_id
)?;
```

### Example 3: SESSION Window with EMIT CHANGES

```sql
-- Real-time session tracking with continuous updates
SELECT
    user_id,
    COUNT(*) as event_count,
    MAX(timestamp) as last_event
FROM user_activity
WINDOW SESSION (GAP 5 MINUTES)
GROUP BY user_id
EMIT CHANGES;
```

**Performance**: 1.01M records/sec with session gap detection

### Example 4: ROWS Window (Memory-Bounded)

```sql
-- Keep last 1000 records per symbol (constant memory)
SELECT
    symbol,
    AVG(price) as moving_avg_price
FROM trades
WINDOW ROWS (BUFFER 1000)
PARTITION BY symbol
ORDER BY timestamp;
```

**Memory**: Constant (no growth regardless of input size)

---

## Migration Guide

### Gradual Rollout Strategy

#### Phase 1: Testing (Recommended)

```rust
// Enable window_v2 only in test environments
#[cfg(test)]
fn create_test_config() -> StreamingConfig {
    StreamingConfig::new().with_window_v2()
}

#[cfg(not(test))]
fn create_test_config() -> StreamingConfig {
    StreamingConfig::default()  // Legacy
}
```

#### Phase 2: Canary Deployment

```rust
// Enable for 10% of traffic
fn create_config(canary_enabled: bool) -> StreamingConfig {
    if canary_enabled {
        StreamingConfig::new().with_window_v2()
    } else {
        StreamingConfig::default()
    }
}
```

#### Phase 3: Full Rollout

```rust
// Enable for all traffic
let config = StreamingConfig::enhanced();
```

### Rollback Procedure

```rust
// Instant rollback to legacy processor
let config = StreamingConfig::default();  // window_v2: false
context.set_streaming_config(config);

// No code changes required - feature flag controls behavior
```

---

## Performance Characteristics

### Throughput Benchmarks

| Window Type | Records/sec | vs Legacy | vs Target |
|-------------|-------------|-----------|-----------|
| TUMBLING | 428,000 | 27.3x | 5.7-8.6x over |
| SLIDING | 491,000 | 31.3x | 8.2-12.3x over |
| SESSION | 1,010,000 | 1,101x | 101x over |
| ROWS | 1,234,000 | 26.5x | 20.6x over |

### Memory Characteristics

| Window Type | Memory Growth | Behavior |
|-------------|---------------|----------|
| TUMBLING | 0.00-1.00x | Bounded by window size |
| SLIDING | 0.00-1.00x | Bounded by window size + overlap |
| SESSION | Unbounded | Grows with session duration |
| ROWS | **0.00x** | **Constant (strict limit)** |

### Latency Characteristics

| Operation | Time | Notes |
|-----------|------|-------|
| Record addition | 0.81-2.33µs | Per record |
| SharedRecord clone | 23.29ns | Arc increment |
| Window emission | 1.41-1.49µs | Per emission |
| Strategy creation | ~1µs | One-time cost |

---

## Production Readiness

### Test Coverage

- ✅ 428 lib tests (100% passing)
- ✅ 2331 unit tests (100% passing)
- ✅ 58 window_v2-specific tests (100% passing)
- ✅ 9 performance benchmarks (all exceeding targets)
- ✅ Zero regressions detected

### Stability

- ✅ Backward compatible (legacy processor preserved)
- ✅ Feature-flagged (instant rollback capability)
- ✅ Production-validated (comprehensive test suite)
- ✅ Type-safe (compile-time guarantees)

### Monitoring

```rust
// Get window statistics for monitoring
let stats = window_strategy.get_stats();
println!("Window size: {}", stats.window_size);
println!("Records buffered: {}", stats.records_in_window);
println!("Memory usage: {} bytes", stats.memory_bytes);
```

---

## Design Decisions

### Why Arc<StreamRecord>?

**Problem**: Deep cloning StreamRecords was causing O(N²) behavior
**Solution**: Arc provides zero-copy sharing with 23.29ns clone time
**Result**: 42.9M clones/sec vs deep copy overhead

### Why Trait-Based Architecture?

**Problem**: Monolithic window processor was difficult to extend
**Solution**: WindowStrategy and EmissionStrategy traits enable pluggability
**Result**: Easy to add new window types without modifying core engine

### Why Feature Flags?

**Problem**: Need production-safe rollout with instant rollback
**Solution**: StreamingConfig with enable_window_v2 boolean flag
**Result**: Zero downtime deployment, instant rollback capability

### Why Keep Legacy Processor?

**Problem**: New architecture needs production validation
**Solution**: Maintain backward compatibility via adapter pattern
**Result**: Safe migration path with proven fallback

---

## Future Enhancements

### Planned for Phase 2B

- Multi-source watermark coordination
- Advanced late data handling
- Cross-window correlations

### Planned for Phase 3

- Distributed window processing
- State persistence and recovery
- Advanced aggregation pushdown

---

## References

- [FR-081-01: Feature Specification](FR-081-01-FEATURE-SPEC.md)
- [FR-081-08: Implementation Schedule](FR-081-08-IMPLEMENTATION-SCHEDULE.md)
- [FR-081-09: Performance Results](FR-081-09-PHASE-2A-PERFORMANCE-RESULTS.md)
- [Adapter Pattern Documentation](adapter.rs)
- [Window Strategies](strategies/)
- [Emission Strategies](emission/)

---

**Document Version**: 1.0
**Last Review**: 2025-11-01
**Next Review**: 2025-12-01
**Owner**: FR-081 Implementation Team
