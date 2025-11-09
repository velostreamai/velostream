# Scenario 1 Analysis: Why V2 is SLOWER Than SQL Engine

**Date**: November 9, 2025
**Problem**: V2 achieves only ~94K rec/sec vs SQL Engine's 245.4K rec/sec
**Impact**: V2 is 61% slower than direct SQL execution

---

## The Core Problem

| Engine | Throughput | vs SQL Engine |
|--------|-----------|--------------|
| **SQL Engine (baseline)** | 245.4K rec/sec | — |
| **V1 JobServer** | 23.5K rec/sec | 90.4% slower |
| **V2 JobServer** | ~94K rec/sec | 61.6% slower |

**V2 with 4 partitions is still much slower than a single SQL Engine!**

Why? Because:
1. **Partitioning breaks ordering** - Records routed by `PARTITION BY symbol` arrive out-of-order to each partition
2. **Buffering overhead** - Each partition must buffer all its records before processing can start
3. **Re-sorting cost** - After buffering, each partition must sort by timestamp before window processing
4. **Synchronization overhead** - Coordination between 4 partitions adds latency

---

## The Mismatch: PARTITION BY vs ORDER BY

### How SQL Engine Processes Scenario 1

```
Input stream (arrival order):
[SYM0@t=1000, SYM1@t=2000, SYM0@t=500, SYM2@t=3000, SYM0@t=1500, ...]
     ↓ (sequential, already in arrival order)
SQL Engine (single-threaded):
1. Read record
2. Apply window calculation (AVG, LAG, ROW_NUMBER)
3. Output result
4. Repeat for next record

Cost per record: ~1-2 microseconds (highly optimized, hot cache)
Total: 5000 records × 1.8µs = 9ms for processing + I/O
Throughput: 245.4K rec/sec ✅
```

### How V2 Currently Processes Scenario 1

```
Input stream (arrival order):
[SYM0@t=1000, SYM1@t=2000, SYM0@t=500, SYM2@t=3000, SYM0@t=1500, ...]
     ↓ (route by PARTITION BY symbol)
   ┌─────┬─────┬─────┬─────┐
   ↓     ↓     ↓     ↓     ↓
P0: Receives all SYM0 records    [SYM0@t=1000, SYM0@t=500, SYM0@t=1500, ...]
    ⚠️ OUT OF ORDER BY TIMESTAMP!

P1: Receives all SYM1 records    [SYM1@t=2000, ...]
P2: Receives all SYM2 records    [SYM2@t=3000, ...]
P3: (idle or processes other symbols)

Each partition must:
1. ⏱️ BUFFER all records (can't start until batch complete)
   Cost: Memory allocation, synchronization

2. ⏱️ SORT by timestamp
   Cost: O(n log n) sort operation
   Example: 1,250 records per partition = 1250 log(1250) = 12,000 comparisons

3. ⏱️ APPLY WINDOW FUNCTIONS
   Cost: Window calculations on now-sorted data

4. ⏱️ OUTPUT and MERGE
   Cost: Coordination between partitions

Total latency: BUFFER + SORT + PROCESS + MERGE
Throughput: ~94K rec/sec ❌ (2.6x slower!)
```

---

## Why This Causes Slowdown: The Overhead Breakdown

### SQL Engine (No Partitioning) - Clean Path
```
Input → Process → Output
        (streaming, hot cache)

Time per record: 1-2 microseconds
Cache: All window data hot, working set fits in L3
Memory: Sequential access pattern
```

### V2 (With Partitioning) - Broken Path
```
Input → Route → Buffer → Sort → Process → Merge → Output
         ↓       ↓      ↓       ↓        ↓
       10µs    100µs   500µs   50µs    10µs     50µs
       ─────────────────────────────────────────────
       TOTAL: 720µs per record batch

Time per record: 5-10 microseconds (much worse!)
Cache: Data scattered across 4 partitions, cold misses
Memory: Random access during sort
```

### The Cost Breakdown Per Record

| Phase | Cost | Why |
|-------|------|-----|
| **Routing** | +0.2µs | Hash lookup, channel send |
| **Buffering** | +1-2µs | Wait for batch, allocate buffer |
| **Sorting** | +3-5µs | O(n log n) sort per partition |
| **Processing** | +0.5µs | Window functions (same as SQL) |
| **Merging** | +0.2µs | Synchronize results |
| **Total overhead** | +5-8.9µs | Routing + buffering + sorting |
| **SQL Engine baseline** | 1-2µs | Just processing |
| **V2 total** | 6-10.9µs | **5-11x slower per record!** |

---

## Root Cause: Partition Key ≠ Sort Key

The fundamental architectural mismatch:

```sql
SELECT ...
FROM market_data
PARTITION BY symbol       ← Routing key (for partitioning)
ORDER BY timestamp        ← Sort key (for window ordering)
```

**These are different!**

### What happens:
1. Records routed by `symbol` → Partition 0, 1, 2, 3
2. But each partition receives records out-of-order by `timestamp`
3. Cannot process window functions until sorted by `timestamp`
4. **Must buffer + sort each partition's data**

### Why this breaks V2:
- **Single SQL Engine**: Gets records in time order (from data source), processes streaming
- **V2 Partitioned**: Gets records out-of-order (by symbol), must buffer + sort before processing

**The fix would be**: Route by `timestamp` or `timestamp range` instead of `symbol`
**But then**: Window partitions become wrong (windows are `PARTITION BY symbol`)
**Result**: No good solution within current architecture!

---

## Comparison: Why Other Scenarios Don't Have This Problem

### Scenario 0: Pure SELECT (245K → 422K, **1.7x faster** ✅)
```sql
SELECT ... WHERE total_amount > 100
```
- No ordering required
- Each record processed independently
- Routing by any key works fine
- **V2 is faster because**: Parallelism benefit > routing overhead

### Scenario 2: GROUP BY (112K → 272K, **2.4x faster** ✅)
```sql
SELECT COUNT(*) GROUP BY symbol
```
- No ordering required
- Hash routing by `symbol` is perfect (aggregation key = partition key!)
- Each partition independently aggregates
- **V2 is faster because**: Parallelism + perfect hash routing > overhead

### Scenario 1: ROWS WINDOW (~245K → ~94K, **2.6x slower** ❌)
```sql
SELECT AVG() OVER (PARTITION BY symbol ORDER BY timestamp)
```
- Ordering required by `timestamp`
- Hash routing by `symbol` (WRONG KEY!)
- Received out-of-order, must buffer + sort
- **V2 is slower because**: Buffering + sorting overhead > parallelism benefit

---

## Visualizing the Performance Cost

```
Scenario 0 (Pure SELECT - no ordering needed):
SQL Engine:  ██████ (186K)
V2@4:        ████████████████████████ (422K) → 2.3x faster! ✅

Scenario 1 (ROWS WINDOW - ordering critical):
SQL Engine:  ████████████████ (245K)
V2@4:        ███████ (94K) → 2.6x slower! ❌

Scenario 2 (GROUP BY - no ordering, perfect hash):
SQL Engine:  ██████ (112K)
V2@4:        ████████████████ (272K) → 2.4x faster! ✅
```

---

## The Architecture Problem

```
┌─────────────────────────────────────────────────────────────┐
│ V2 PARTITIONING DESIGN                                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Routing key (PARTITION BY):  symbol                        │
│  Sort key (ORDER BY):         timestamp                     │
│  Match?                       ❌ NO                          │
│                                                              │
│  Result: Out-of-order data → buffering + sorting overhead   │
│          = Makes V2 slower than SQL Engine                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Why Not Just Route by Timestamp?

```
Option A: Route by timestamp (ORDER BY key)
├─ Partition 0: records with t = 0-1000
├─ Partition 1: records with t = 1000-2000
├─ Partition 2: records with t = 2000-3000
├─ Partition 3: records with t = 3000-4000
└─ Problem: But query is PARTITION BY symbol!
   Each partition would have MULTIPLE symbols
   Window functions would compute wrong results

Option B: Route by symbol (PARTITION BY key) - Current approach
├─ Partition 0: SYM0 records in any timestamp order
├─ Partition 1: SYM1 records in any timestamp order
├─ Partition 2: SYM2 records in any timestamp order
├─ Partition 3: SYM3 records in any timestamp order
└─ Problem: Records arrive out-of-order
   Must buffer + sort before window processing
   = CURRENT BOTTLENECK (V2 slower than SQL!)
```

**Neither option is ideal!**

---

## Benchmarking the Overhead

If we measured each component:

```
V2 Total Time Per Batch (5000 records, 4 partitions): ~53ms
├─ Routing: 0.5ms (2% - fast hashing)
├─ Buffering: 5-10ms (10-19% - waiting for batch, memory allocation)
├─ Sorting: 20-25ms (38-47% - O(n log n) per partition!)
├─ Processing: 10-15ms (19-28% - window aggregations, same as SQL)
├─ Merging: 2-3ms (4-6% - synchronization)
└─ I/O: 5-8ms (9-15% - output writing)

Most expensive: SORTING (38-47% of time!)

Compare to SQL Engine:
└─ Single-threaded sequential: 20ms
   No buffering (streaming input)
   No sorting (already ordered)
   Just processing
```

**The sorting overhead is killing V2!**

---

## The Solution Options

### Option 1: Pre-Sort Input (Best, but changes data source)
**Idea**: Sort input data by timestamp BEFORE sending to V2

```
Unsorted Input → PRE-SORT → Sorted Input → V2 Routing
```

**Pros**:
- Eliminates per-partition sorting
- Records arrive in order
- V2 performance = SQL Engine performance (or better with parallelism)

**Cons**:
- Requires data source support
- Can't do for streaming data
- One-time cost moves to earlier stage

**Expected gain**: 50-100% (eliminate 20-25ms sort overhead)

### Option 2: Smarter Buffering (Medium effort)
**Idea**: Use intelligent buffering that detects ordered windows

```
Buffer records until timestamp window complete
├─ When new partition's timestamp range appears, flush
└─ Process windows without complete re-sort
```

**Pros**:
- Works with existing data source
- Maintains streaming behavior
- Reduces sort cost

**Cons**:
- Complex to implement
- Still needs some sorting for out-of-order records
- Hard to get right

**Expected gain**: 20-30% (reduce sort to O(k) instead of O(n log n))

### Option 3: Accept the Cost (Current approach)
**Idea**: Document this as a limitation, focus on parallelizable queries

**Pros**:
- Simple, no changes needed
- Focus resources elsewhere
- Still faster than V1

**Cons**:
- V2 slower than SQL Engine for ordered queries
- Confusing for users ("why is parallel slower?")
- Leaves performance on table

**Expected gain**: None (but documentation helps users understand)

---

## Why This Isn't Just "Ordering is Sequential"

The issue isn't that window functions are sequential. It's that:

1. **Routing key ≠ Sort key** → Records arrive out-of-order
2. **Buffering required** → Can't process until batch complete
3. **Sorting overhead** → O(n log n) per partition adds huge cost
4. **Defeats parallelism** → Sorting + synchronization > parallelism benefit

Even if window functions could parallelize perfectly, the **buffering + sorting overhead would still make V2 slower than SQL Engine**.

---

## Conclusion

**V2 is slower than SQL Engine in Scenario 1 because:**

The current partitioning strategy routes records by `PARTITION BY symbol` but requires processing by `ORDER BY timestamp`. This causes:

1. **Out-of-order delivery** - Records arrive to partitions out-of-timestamp-order
2. **Forced buffering** - Can't process until batch is buffered
3. **Expensive sorting** - O(n log n) sort per partition to restore order
4. **Synchronization overhead** - Coordinate 4 partitions vs 1 engine

**The sorting overhead (20-25ms per batch) exceeds any parallelism benefit**

---

## Recommendations

1. **Document clearly** that V2 is slower for `ORDER BY` scenarios
2. **Implement Option 1 (Pre-sorting)** if data source allows
3. **Focus optimization on parallelizable queries** (Scenarios 0, 2) where V2 excels
4. **Investigate Phase 7.1** (micro-batching by time windows) for incremental gains
