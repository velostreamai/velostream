# Performance Optimization Plan

**Date**: 2025-10-02
**Current Performance**: Good (see benchmarks below)
**Goal**: Achieve 10M+ records/sec throughput for in-memory channels

## Current Performance Benchmarks

### 1. Data Source Performance (PASSED ✅)
```
URI parsing:               2,918,049 ops/sec (1μs per operation)
Source creation:           1,563,619 ops/sec (0μs per operation)
Record transformation:     2,765,997 records/sec (3μs per operation)
```

### 2. Table Performance (PASSED ✅)
```
Data loading:              744,228 records/sec (1.34μs per record)
Key lookups (O(1)):        7,629,220 lookups/sec (131ns per lookup)
Streaming:                 249,426 records/sec
Query caching speedup:     1.9x - 3.0x faster
```

### 3. Performance Summary
- **Strengths**: Excellent key lookup (7.6M/sec), good URI parsing (2.9M/sec)
- **Weaknesses**: Streaming could be faster (249K vs 2.7M record transformation)
- **Overall**: Production-ready, but has room for 5-10x improvement

## Performance Bottlenecks Identified

### 1. FieldValue Cloning (HIGH IMPACT)

**Current**: Uses `Clone` trait, copies entire structures
```rust
pub enum FieldValue {
    String(String),      // Heap allocation + copy
    Array(Vec<FieldValue>),   // Recursive clones
    Map(HashMap<String, FieldValue>),  // Deep clone
    Struct(HashMap<String, FieldValue>),
    // ...
}
```

**Impact**: Every record operation clones all fields
- Record transformation: 3μs (could be ~50ns with Arc)
- **60x slower** than zero-copy approach

**Solution**: Use `Arc<FieldValue>` for shared ownership

```rust
pub enum FieldValue {
    String(Arc<str>),           // Shared string
    Array(Arc<[FieldValue]>),   // Shared array
    Map(Arc<HashMap<String, FieldValue>>),
    Struct(Arc<HashMap<String, FieldValue>>),
    // Simple types stay as-is (Copy)
    Integer(i64),
    Float(f64),
    Boolean(bool),
}
```

**Expected Improvement**: 5-10x faster record transformation (300ns vs 3μs)

### 2. String Allocations (MEDIUM IMPACT)

**Current**: `to_display_string()` allocates for every call
```rust
pub fn to_display_string(&self) -> String {
    match self {
        FieldValue::Integer(i) => i.to_string(),  // Allocates
        FieldValue::ScaledInteger(value, scale) => {
            format!("{}.{:0width$}", ...)  // Allocates + formats
        }
        // ...
    }
}
```

**Impact**:
- Called frequently in output/serialization
- ScaledInteger formatting is complex (10+ operations)

**Solution 1**: Lazy formatting with `Display` trait
```rust
impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Integer(i) => write!(f, "{}", i),  // No allocation
            FieldValue::ScaledInteger(value, scale) => {
                // Cache common scale values
                write_scaled_integer(f, *value, *scale)
            }
            // ...
        }
    }
}
```

**Solution 2**: Cache formatted strings for hot values
```rust
pub struct CachedFieldValue {
    value: FieldValue,
    display_cache: OnceCell<String>,  // Lazy init
}
```

**Expected Improvement**: 2-5x faster display operations

### 3. HashMap Overhead (MEDIUM IMPACT)

**Current**: Uses `HashMap<String, FieldValue>` for structs/maps
- Hash computation overhead
- Poor cache locality
- Entry API allocations

**Solution**: Use `Vec<(String, FieldValue)>` for small maps (<10 entries)
```rust
pub enum FieldValue {
    // ... existing variants
    SmallMap(smallvec::SmallVec<[(Arc<str>, FieldValue); 4]>),
    LargeMap(Arc<HashMap<Arc<str>, FieldValue>>),
}

impl FieldValue {
    pub fn get_field(&self, key: &str) -> Option<&FieldValue> {
        match self {
            SmallMap(vec) => vec.iter().find(|(k, _)| k.as_ref() == key).map(|(_, v)| v),
            LargeMap(map) => map.get(key),
            // ...
        }
    }
}
```

**Expected Improvement**: 2-3x faster for small structs (most common case)

### 4. Type Conversion Allocations (LOW IMPACT)

**Current**: `cast_to()` allocates for every conversion
```rust
pub fn cast_to(self, target_type: &str) -> Result<FieldValue, SqlError> {
    match target_type {
        "INTEGER" | "INT" => match self {
            FieldValue::String(s) => s.parse::<i64>()...  // String parse
            // ...
        }
    }
}
```

**Solution**: Pre-parse common types, use const comparisons
```rust
#[derive(Copy, Clone)]
pub enum TargetType {
    Integer,
    Float,
    String,
    // ...
}

impl TargetType {
    pub const fn from_str(s: &str) -> Option<Self> {
        match s {
            "INTEGER" | "INT" => Some(Self::Integer),
            "FLOAT" | "DOUBLE" => Some(Self::Float),
            // ...
        }
    }
}

pub fn cast_to(self, target: TargetType) -> Result<FieldValue, SqlError> {
    match (self, target) {
        (FieldValue::Integer(i), TargetType::Float) => Ok(FieldValue::Float(i as f64)),
        // Flat match - faster
    }
}
```

**Expected Improvement**: 20-30% faster type conversions

### 5. Streaming Throughput (HIGH IMPACT)

**Current**: 249K records/sec (4μs per record)
**Target**: 5M records/sec (200ns per record)

**Gap Analysis**:
- Record transformation: 3μs (good)
- Streaming overhead: ~1μs (needs optimization)

**Solution**: Batch processing with SIMD potential

```rust
pub async fn stream_batch(&self, batch_size: usize) -> StreamResult<Vec<Arc<StreamRecord>>> {
    // Pre-allocate
    let mut batch = Vec::with_capacity(batch_size);

    // Tight loop - no allocations
    for _ in 0..batch_size {
        if let Some(record) = self.read_one()? {
            batch.push(record);  // Arc clone only
        } else {
            break;
        }
    }

    Ok(batch)
}
```

**Expected Improvement**: 10-20x faster (5M records/sec)

## Optimization Roadmap

### Phase 1: Zero-Copy Foundation (Week 1) - **HIGHEST IMPACT**

**Goal**: Eliminate cloning overhead with Arc-based sharing

**Tasks**:
- [ ] Add `Arc<FieldValue>` wrapper type
- [ ] Update `StreamRecord` to use `Arc<FieldValue>`
- [ ] Benchmark: Target 10x improvement in record transformation
- [ ] Ensure backward compatibility

**Expected Results**:
- Record transformation: 3μs → 300ns (10x faster)
- Streaming: 249K → 2.5M records/sec (10x faster)

### Phase 2: String Optimization (Week 2) - **MEDIUM IMPACT**

**Goal**: Reduce string allocation overhead

**Tasks**:
- [ ] Implement `Display` trait (lazy formatting)
- [ ] Add `OnceCell` for cached display strings
- [ ] Use `Arc<str>` instead of `String`
- [ ] Benchmark: Target 2-3x improvement

**Expected Results**:
- Display operations: 50-70% faster
- Memory usage: 20-30% reduction

### Phase 3: Data Structure Optimization (Week 3) - **MEDIUM IMPACT**

**Goal**: Optimize HashMap/Vec usage for small collections

**Tasks**:
- [ ] Add `SmallVec` for small arrays
- [ ] Use linear search for small maps (<10 entries)
- [ ] Benchmark: Compare HashMap vs Vec for different sizes
- [ ] Auto-transition at threshold

**Expected Results**:
- Small struct operations: 2-3x faster
- Cache locality: Significantly improved

### Phase 4: Type System Enhancement (Week 4) - **LOW IMPACT**

**Goal**: Optimize type conversions and comparisons

**Tasks**:
- [ ] Add `TargetType` enum for cast operations
- [ ] Use const comparisons instead of string matching
- [ ] Pre-compute common conversions
- [ ] Benchmark: Target 20-30% improvement

**Expected Results**:
- Type conversions: 20-30% faster
- Reduced allocations in hot paths

### Phase 5: SIMD & Batching (Week 5-6) - **FUTURE**

**Goal**: Vectorize hot paths for maximum throughput

**Tasks**:
- [ ] Identify SIMD-friendly operations (arithmetic, comparisons)
- [ ] Use `packed_simd` or `std::simd` for batched operations
- [ ] Benchmark: Target 2-4x on numeric operations
- [ ] Document SIMD usage patterns

**Expected Results**:
- Numeric operations: 2-4x faster
- Batch processing: Near-linear scaling

## Specific Code Changes

### Change 1: Arc-Based FieldValue

**File**: `src/velostream/sql/execution/types.rs`

**Before**:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    String(String),
    Array(Vec<FieldValue>),
    // ...
}
```

**After**:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    String(Arc<str>),
    Array(Arc<[FieldValue]>),
    // Simple types remain Copy
    Integer(i64),
    Float(f64),
    Boolean(bool),
    // ...
}

// Helper for ergonomic construction
impl From<String> for FieldValue {
    fn from(s: String) -> Self {
        FieldValue::String(Arc::from(s))
    }
}

impl From<&str> for FieldValue {
    fn from(s: &str) -> Self {
        FieldValue::String(Arc::from(s))
    }
}
```

### Change 2: Display Trait Implementation

**File**: `src/velostream/sql/execution/types.rs`

**Before**:
```rust
pub fn to_display_string(&self) -> String {
    match self {
        FieldValue::Integer(i) => i.to_string(),
        // ...
    }
}
```

**After**:
```rust
impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Integer(i) => write!(f, "{}", i),
            FieldValue::Float(fl) => write!(f, "{}", fl),
            FieldValue::String(s) => write!(f, "{}", s),
            FieldValue::ScaledInteger(value, scale) => {
                write_scaled_integer(f, *value, *scale)
            }
            // ...
        }
    }
}

// Keep legacy method for compatibility
pub fn to_display_string(&self) -> String {
    format!("{}", self)
}
```

### Change 3: Batched Streaming

**File**: `src/velostream/table/streaming.rs`

**New Method**:
```rust
pub trait StreamableTable {
    /// Stream records in batches for higher throughput
    async fn stream_batch(&self, batch_size: usize) -> StreamResult<Vec<Arc<StreamRecord>>>;

    /// Stream all records with automatic batching
    async fn stream_all_batched(&self, batch_size: usize) -> StreamResult<RecordStream>;
}

impl StreamableTable for OptimizedTableImpl {
    async fn stream_batch(&self, batch_size: usize) -> StreamResult<Vec<Arc<StreamRecord>>> {
        let mut batch = Vec::with_capacity(batch_size);
        let records = self.records.read().await;

        // Tight loop - minimal overhead
        for (_, record) in records.iter().take(batch_size) {
            batch.push(record.clone());  // Arc clone only
        }

        Ok(batch)
    }
}
```

## Performance Testing Plan

### Micro-Benchmarks (Criterion)

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_field_value_clone(c: &mut Criterion) {
    let value = FieldValue::String("test".to_string());
    c.bench_function("field_value_clone", |b| {
        b.iter(|| black_box(value.clone()))
    });
}

fn bench_field_value_arc_clone(c: &mut Criterion) {
    let value = FieldValue::String(Arc::from("test"));
    c.bench_function("field_value_arc_clone", |b| {
        b.iter(|| black_box(value.clone()))
    });
}

criterion_group!(benches, bench_field_value_clone, bench_field_value_arc_clone);
criterion_main!(benches);
```

### Integration Benchmarks

```bash
# Before optimization
cargo test --release performance_baseline -- --nocapture

# After Phase 1
cargo test --release performance_phase1 -- --nocapture

# Regression detection
cargo test --release performance_regression -- --nocapture
```

## Success Metrics

| Metric | Current | Phase 1 Target | Ultimate Goal |
|--------|---------|----------------|---------------|
| **Record Transformation** | 2.7M/sec (3μs) | 27M/sec (300ns) | 50M/sec (200ns) |
| **Streaming Throughput** | 249K/sec | 2.5M/sec | 5M/sec |
| **Key Lookups** | 7.6M/sec (131ns) | 10M/sec (100ns) | 20M/sec (50ns) |
| **Display Operations** | N/A | 2-3x faster | 5x faster |
| **Memory per Record** | ~1KB | ~200 bytes | ~100 bytes |

## Monitoring & Validation

### Performance Regression Tests

```rust
#[test]
fn test_no_performance_regression() {
    let baseline = 2_700_000; // 2.7M records/sec
    let current = measure_record_transformation();

    assert!(
        current >= baseline * 0.95,
        "Performance regression detected: {} < {}",
        current,
        baseline * 0.95
    );
}
```

### Continuous Benchmarking

```bash
# Add to CI/CD pipeline
cargo bench --no-fail-fast > bench_results.txt
./scripts/compare_benchmarks.sh baseline.txt bench_results.txt
```

## Risk Assessment

### Low Risk
- Display trait implementation (backward compatible)
- Batched streaming (additive API)
- Type system improvements (internal)

### Medium Risk
- SmallVec optimization (behavior changes for edge cases)
- Cached formatting (memory usage increase)

### High Risk
- Arc-based FieldValue (breaking change to Clone semantics)
  - **Mitigation**: Feature flag + gradual rollout
  - **Testing**: Extensive integration tests
  - **Rollback**: Keep old implementation behind feature flag

## Conclusion

**Current Status**: ✅ Production-ready performance
**Optimization Potential**: 10-20x improvement possible
**Recommended Approach**: Incremental optimization, measure at each step
**Priority**: Phase 1 (Zero-Copy) has highest impact with manageable risk

**Next Steps**:
1. Implement Arc-based FieldValue (Phase 1)
2. Benchmark and validate
3. Roll out gradually with feature flags
4. Monitor production metrics
5. Proceed to Phase 2 if Phase 1 successful
