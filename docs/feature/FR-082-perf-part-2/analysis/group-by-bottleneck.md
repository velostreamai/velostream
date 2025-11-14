# FR-082 Phase 4A: SQL Engine Bottleneck Analysis

**Date**: November 5, 2025
**Benchmark Result**: GROUP BY + 5 aggregations runs at **3.58K rec/s** (111x slower than passthrough)
**Root Cause**: Excessive allocations and cloning in GROUP BY implementation

---

## Critical Bottlenecks Identified

### 1. **Hash Table with Vec<String> Keys** ⚠️ CRITICAL
**Location**: `src/velostream/sql/execution/internal.rs:21`

```rust
pub struct GroupByState {
    pub groups: HashMap<Vec<String>, GroupAccumulator>,  // ← BOTTLENECK!
    // ...
}
```

**Problem**:
- Every group key is a `Vec<String>`, requiring allocation
- Each String in the Vec also requires allocation
- For 1M records with 10 groups, this creates **2M+ allocations**
- Standard `HashMap` uses SipHash (cryptographic, slow)
- `Vec<String>` hashing is expensive (hashes each string, then Vec)

**Impact**: **~40% of overhead** (estimated)

---

### 2. **Group State Cloning Per Batch** ⚠️ CRITICAL
**Location**: `src/velostream/sql/execution/engine.rs:444, 449, 1392, 1438, etc.`

```rust
// Called for EVERY batch!
context.group_by_states = self.group_states.clone();  // ← BOTTLENECK!
// ... process batch ...
self.group_states = std::mem::take(&mut context.group_by_states);
```

**Problem**:
- Clones the **entire HashMap** of all groups for every batch
- With 10 groups × 1000 batches = **10,000 full HashMap clones**
- Each clone copies all GroupAccumulator state (sums, mins, maxs, etc.)
- This is completely unnecessary - should use Arc or references

**Impact**: **~30% of overhead** (estimated)

---

### 3. **Record Cloning in Accumulator** ⚠️ MAJOR
**Location**: `src/velostream/sql/execution/aggregation/accumulator.rs:48`

```rust
pub fn process_record_into_accumulator(
    accumulator: &mut GroupAccumulator,
    record: &StreamRecord,
    // ...
) -> Result<(), SqlError> {
    // ...
    if accumulator.sample_record.is_none() {
        accumulator.sample_record = Some(record.clone());  // ← BOTTLENECK!
    }
    // ...
}
```

**Problem**:
- Clones **full StreamRecord** for sample_record in each group
- With 10 groups, this clones 10 full records
- Should use `Arc<StreamRecord>` (Phase 2 optimization not applied here!)

**Impact**: **~10% of overhead** (estimated)

---

###4. **String Allocations in Accumulators** ⚠️ MAJOR
**Location**: `src/velostream/sql/execution/internal.rs:76-93`

```rust
pub struct GroupAccumulator {
    pub non_null_counts: HashMap<String, u64>,    // ← Multiple String allocations
    pub sums: HashMap<String, f64>,               // ← per field name
    pub mins: HashMap<String, FieldValue>,        // ← per record
    pub maxs: HashMap<String, FieldValue>,        // ← per record
    pub numeric_values: HashMap<String, Vec<f64>>,
    pub first_values: HashMap<String, FieldValue>,
    pub last_values: HashMap<String, FieldValue>,
    pub string_values: HashMap<String, Vec<String>>,
    pub distinct_values: HashMap<String, HashSet<String>>,
    // ...
}
```

**Problem**:
- **10+ HashMaps** per accumulator, all with String keys
- For each field name: `.to_string()` creates new allocation
- With 5 aggregations × 10 groups = **50+ String allocations** per field name
- Should use string interning or `&'static str`

**Impact**: **~15% of overhead** (estimated)

---

### 5. **generate_group_key Allocations** ⚠️ MAJOR
**Location**: `src/velostream/sql/execution/aggregation/state.rs:17-27`

```rust
pub fn generate_group_key(
    expressions: &[Expr],
    record: &StreamRecord,
) -> Result<Vec<String>, SqlError> {
    let mut key_values = Vec::new();  // ← BOTTLENECK!

    for expr in expressions {
        let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
        key_values.push(Self::field_value_to_group_key(&value)); // ← String allocation
    }

    Ok(key_values)  // ← Returns new Vec for EVERY record!
}
```

**Problem**:
- Creates new `Vec<String>` for **EVERY RECORD** (1M allocations!)
- Converts FieldValue → String for every key component
- For 1 group-by field: 1M Vec allocations + 1M String allocations
- For 2 group-by fields: 1M Vec + 2M String allocations

**Impact**: **~20% of overhead** (estimated)

---

## Cumulative Impact Analysis

| Bottleneck | Estimated Overhead | Fix Complexity |
|-----------|-------------------|----------------|
| Vec<String> hash keys | ~40% | Medium (change key type) |
| Group state cloning | ~30% | Easy (use Arc) |
| String allocations in accumulators | ~15% | Medium (string interning) |
| generate_group_key allocations | ~20% | Medium (key caching) |
| Record cloning | ~10% | Easy (use Arc<StreamRecord>) |
| **Total** | **~115%** | **(overlapping)** |

**Actual cumulative overhead**: ~99% (111x slowdown measured)

---

## Optimization Priority (Phase 4B & 4C)

### Phase 4B: Hash Table Optimization (HIGH PRIORITY)

**Target**: Reduce overhead from 40% → <10%

**Changes**:
1. **Replace Vec<String> keys with specialized GroupKey type**
   ```rust
   // Before:
   HashMap<Vec<String>, GroupAccumulator>

   // After:
   FxHashMap<GroupKey, GroupAccumulator>  // FxHash is 2-3x faster

   struct GroupKey {
       hash: u64,           // Pre-computed hash
       values: Arc<[FieldValue]>,  // Arc to avoid Vec allocation
   }
   ```

2. **Use FxHashMap instead of HashMap**
   - FxHash is 2-3x faster for integer-like keys
   - No cryptographic overhead

3. **Pre-allocate with capacity**
   - Estimate group count (e.g., 10-100 typical)
   - `FxHashMap::with_capacity_and_hasher(100, ...)`

**Expected improvement**: GROUP BY from 3.58K → 15-20K rec/s (+400-500%)

---

### Phase 4C: Aggregation State Optimization (HIGH PRIORITY)

**Target**: Reduce overhead from ~55% → <20%

**Changes**:
1. **Use Arc for group state** (eliminate cloning)
   ```rust
   // Before:
   context.group_by_states = self.group_states.clone();  // Full clone!

   // After:
   context.group_by_states = Arc::clone(&self.group_by_states);  // Arc bump!
   ```

2. **Use Arc<StreamRecord> in sample_record**
   ```rust
   // Before:
   pub sample_record: Option<StreamRecord>,  // Full clone

   // After:
   pub sample_record: Option<Arc<StreamRecord>>,  // Arc reference
   ```

3. **String interning for field names**
   - Use `&'static str` for known field names
   - Use string interning pool for dynamic names
   - Avoid `.to_string()` in hot path

4. **Group key caching**
   - Cache computed group keys in records
   - Avoid re-computing for same record

**Expected improvement**: GROUP BY from 15-20K → 150-200K rec/s (+750-1000%)

---

## Combined Expected Results

| Phase | Optimization | Throughput | Improvement | Status |
|-------|------------|-----------|-------------|--------|
| Baseline | - | 3.58 K rec/s | - | ✅ Measured |
| **4B** | **Hash table** | **15-20 K rec/s** | **+400-500%** | 📋 Planned |
| **4C** | **Aggregation state** | **150-200 K rec/s** | **+750-1000%** | 📋 Planned |
| **Combined** | **4B + 4C** | **>200 K rec/s** | **+5,500%** | 🎯 **Target** |

**Success Criteria**: GROUP BY overhead < 50% (vs 400K rec/s passthrough baseline)

---

## Files to Modify

### Phase 4B (Hash Table):
- `src/velostream/sql/execution/internal.rs` - Change GroupByState key type
- `src/velostream/sql/execution/aggregation/state.rs` - Update generate_group_key
- Add `FxHashMap` dependency to `Cargo.toml`

### Phase 4C (Aggregation State):
- `src/velostream/sql/execution/internal.rs` - Use Arc<GroupByState>
- `src/velostream/sql/execution/aggregation/accumulator.rs` - Use Arc<StreamRecord>
- `src/velostream/sql/execution/engine.rs` - Remove cloning, use Arc

---

**Next Step**: Begin Phase 4B implementation (hash table optimization)
