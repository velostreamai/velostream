# Execution.rs Refactoring Strategy & Plan

Based on analysis of the 7,077-line execution.rs file, here's a comprehensive refactoring strategy:

## Current Structure Analysis

**File Statistics:**
- **Size:** 7,077 lines (~311KB) - exceeding maintainability thresholds
- **Main Implementation:** Single massive `StreamExecutionEngine` impl block (4,000+ lines)
- **Key Concerns:** Monolithic design, mixed responsibilities, potential memory inefficiencies

## 1. Logical Module Groupings

### Core Engine (`engine/mod.rs`, `engine/core.rs`)
```rust
pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::Sender<ExecutionMessage>,
    output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
    record_count: u64,
    group_states: HashMap<String, GroupByState>, // Move to dedicated module
}
```

### Expression Evaluation (`expressions/mod.rs`)
- `expressions/evaluator.rs` - Core expression evaluation logic
- `expressions/operators.rs` - Binary/unary operators, comparisons
- `expressions/functions.rs` - Built-in functions (HEADER, aggregates)
- `expressions/casting.rs` - Type casting and coercion

### Window Processing (`windows/mod.rs`)
- `windows/processor.rs` - Window function execution
- `windows/state.rs` - Window buffer management
- `windows/timing.rs` - Window boundary calculations

### Group By Operations (`groupby/mod.rs`)
- `groupby/state.rs` - `GroupByState`, `GroupAccumulator`
- `groupby/processor.rs` - Group-by logic
- `groupby/aggregates.rs` - Aggregate functions

### Data Types (`types/mod.rs`)
- `types/field_value.rs` - `FieldValue` enum + methods
- `types/record.rs` - `StreamRecord`, conversion utilities
- `types/messages.rs` - `ExecutionMessage`, `HeaderMutation`

### Join Processing (`joins/mod.rs`)
- `joins/processor.rs` - Join execution logic
- `joins/types.rs` - Join-specific data structures

## 2. State Management Optimization

### Current Issues:
- `HashMap<String, GroupByState>` stores all group states in engine
- Large `GroupAccumulator` structures with multiple `HashMap`s
- No memory bounds or cleanup strategies

### Optimization Strategy:

#### A. State Isolation
```rust
// Move from engine to dedicated state manager
pub struct GroupByStateManager {
    states: LruCache<String, GroupByState>,
    memory_limit: usize,
    cleanup_threshold: Duration,
}
```

#### B. Memory-Efficient Accumulators
```rust
// Replace multiple HashMaps with a unified structure
pub struct CompactAccumulator {
    data: SmallVec<[u8; 64]>, // Stack-allocated for small groups
    schema: Arc<AccumulatorSchema>, // Shared schema reference
}
```

#### C. State Cleanup
- Implement TTL-based cleanup for inactive groups
- Memory pressure-triggered eviction
- Configurable retention policies

## 3. Memory Footprint Reduction

### A. Use `SmallVec` for Hot Paths
```rust
// Replace Vec<StreamRecord> with bounded collections
buffer: SmallVec<[StreamRecord; 8]>, // Stack allocation for small windows
```

### B. String Interning
```rust
// For repeated field names and group keys
type FieldName = Arc<str>;
static STRING_INTERNER: Lazy<DashMap<String, Arc<str>>> = Lazy::new(DashMap::new);
```

### C. Compact Data Structures
```rust
// Replace HashMap with more efficient alternatives
use ahash::AHashMap; // Faster hashing
use indexmap::IndexMap; // Better memory layout for iteration
```

### D. Object Pooling
```rust
// Reuse expensive allocations
pub struct ObjectPool<T> {
    available: VecDeque<T>,
    max_size: usize,
}
```

## 4. Performance Optimizations

### A. Static Function Extraction
Move pure functions to static implementations:
```rust
impl FieldValue {
    // Already some static functions exist, expand this pattern
    pub fn compare_static(a: &Self, b: &Self) -> Ordering { ... }
    pub fn to_group_key_static(value: &Self) -> GroupKey { ... }
}
```

### B. Compilation Units
Split large functions into smaller, more optimizable units:
- Current `apply_query()` is 400+ lines → split into 8-10 focused functions
- `evaluate_expression()` → modular expression handlers

### C. Branch Prediction Optimization
```rust
// Use likely/unlikely hints for hot paths
#[inline]
fn is_aggregate_function(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") // Most common first
}
```

### D. SIMD Opportunities
For aggregate computations (SUM, AVG) on large datasets:
```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
```

## 5. Refactoring Implementation Strategy

### Phase 1: Core Extraction (Week 1)
1. Create module structure (`src/ferris/sql/execution/`)
2. Extract data types to `types/` modules
3. Move `FieldValue` with all methods to dedicated module
4. Ensure all tests pass

### Phase 2: Expression System (Week 2)
1. Extract expression evaluation to `expressions/`
2. Split by operator types and function categories
3. Add static variants where possible
4. Benchmark performance impact

### Phase 3: Specialized Processors (Week 3)
1. Extract GROUP BY logic to `groupby/`
2. Extract window processing to `windows/`
3. Extract join logic to `joins/`
4. Implement memory-efficient state management

### Phase 4: Memory Optimization (Week 4)
1. Implement `SmallVec` replacements
2. Add string interning
3. Implement object pooling
4. Add memory monitoring and cleanup

### Phase 5: Performance Tuning (Week 5)
1. Profile hot paths
2. Add SIMD optimizations where beneficial
3. Optimize branch prediction
4. Final performance validation

## 6. Risk Mitigation

### Compatibility Preservation
- Maintain existing public API surface
- Comprehensive test coverage before/after
- Gradual migration with feature flags

### Performance Monitoring
- Benchmark all phases against current implementation
- Memory usage tracking
- Latency measurements for query execution

### Rollback Strategy
- Keep original implementation in parallel
- Runtime switching capability
- Automated performance regression detection

## Expected Outcomes

- **Maintainability:** 7,077 lines → ~8-10 focused modules (~500-800 lines each)
- **Memory Usage:** 20-40% reduction through efficient data structures
- **Performance:** 15-25% improvement through optimizations
- **Developer Experience:** Clear module boundaries, focused responsibilities

## Implementation Checklist

### Phase 1: Core Extraction
- [ ] Create `src/ferris/sql/execution/` directory structure
- [ ] Extract `FieldValue` to `types/field_value.rs`
- [ ] Extract `StreamRecord` to `types/record.rs`
- [ ] Extract `ExecutionMessage` types to `types/messages.rs`
- [ ] Update imports across codebase
- [ ] Run full test suite to ensure no regressions

### Phase 2: Expression System
- [ ] Create `expressions/mod.rs` module
- [ ] Extract core evaluation logic to `expressions/evaluator.rs`
- [ ] Split operators to `expressions/operators.rs`
- [ ] Move built-in functions to `expressions/functions.rs`
- [ ] Add casting logic to `expressions/casting.rs`
- [ ] Benchmark expression evaluation performance

### Phase 3: Specialized Processors
- [ ] Create `groupby/mod.rs` with state management
- [ ] Extract GROUP BY logic to `groupby/processor.rs`
- [ ] Move aggregate functions to `groupby/aggregates.rs`
- [ ] Create `windows/mod.rs` for window processing
- [ ] Extract join logic to `joins/mod.rs`
- [ ] Implement `GroupByStateManager` with LRU cache

### Phase 4: Memory Optimization
- [ ] Add `smallvec` dependency to Cargo.toml
- [ ] Replace `Vec<StreamRecord>` with `SmallVec` in hot paths
- [ ] Implement string interning for field names
- [ ] Add object pooling for frequently allocated structures
- [ ] Implement memory pressure monitoring

### Phase 5: Performance Tuning
- [ ] Profile current vs. refactored implementation
- [ ] Add SIMD optimizations for numeric aggregations
- [ ] Optimize branch prediction in hot paths
- [ ] Add performance regression tests
- [ ] Document performance characteristics

### Validation
- [ ] All existing tests pass
- [ ] Performance benchmarks show improvement
- [ ] Memory usage decreased by target percentage
- [ ] Code coverage maintained or improved
- [ ] Documentation updated for new module structure