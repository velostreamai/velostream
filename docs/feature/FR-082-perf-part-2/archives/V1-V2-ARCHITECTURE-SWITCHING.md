# V1 vs V2 Architecture: Can We Switch?

**Date**: November 7, 2025
**Status**: Architecture analysis for runtime switching capability
**Goal**: Understand current state and plan for flexible V1/V2 switching

---

## Current State: No Switching Yet

**The Reality**:
- ✅ V1 (SimpleJobProcessor) - Fully integrated into StreamJobServer
- ✅ V2 (PartitionedJobCoordinator) - Exists in codebase but NOT integrated
- ❌ **No runtime switching mechanism** - Must choose at compile time or deep refactor

Currently:
```rust
// streamJobServer only uses V1
StreamJobServer {
    jobs: HashMap<String, RunningJob>,
    ...
}

// In run_job():
let processor = SimpleJobProcessor::new(...);  // ← HARDCODED V1
processor.process_stream(...).await;
```

**V2 exists but is orphaned:**
```
src/velostream/server/v2/
├─ coordinator.rs (PartitionedJobCoordinator) ← Not used
├─ hash_router.rs (HashRouter)              ← Not used
├─ partition_manager.rs (PartitionStateManager) ← Not used
└─ metrics.rs (PartitionMetrics)            ← Not used
```

---

## How to Add V1/V2 Switching

### Option 1: Enum-Based Processor (Simple, 1 day)

```rust
pub enum JobProcessor {
    V1(SimpleJobProcessor),
    V2(PartitionedJobCoordinator),
}

impl JobProcessor {
    pub async fn process_stream(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        match self {
            Self::V1(processor) => processor.process_batch(...).await,
            Self::V2(coordinator) => {
                // V2 logic: distribute records to partitions
                coordinator.route_and_process(...).await
            }
        }
    }
}

// Usage:
let processor = if config.use_v2 {
    JobProcessor::V2(PartitionedJobCoordinator::new(v2_config))
} else {
    JobProcessor::V1(SimpleJobProcessor::new(v1_config))
};

processor.process_stream(records, engine).await?;
```

**Pros**:
- Simple to implement
- Can toggle at config time
- No interface changes needed

**Cons**:
- Not trait-based (harder to extend)
- Duplicates interface code
- Match statement in critical path

---

### Option 2: Trait-Based (Better, 2 days)

```rust
pub trait JobProcessorInterface: Send + Sync {
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError>;

    fn num_partitions(&self) -> usize;
    fn metrics(&self) -> &ProcessorMetrics;
}

impl JobProcessorInterface for SimpleJobProcessor {
    // V1 implementation
}

impl JobProcessorInterface for PartitionedJobCoordinator {
    // V2 implementation
    async fn process_batch(...) -> Result<...> {
        // Distribute to 8 partitions in parallel
        // Collect results from all partitions
    }
}

// Usage:
let processor: Arc<dyn JobProcessorInterface> = if config.use_v2 {
    Arc::new(PartitionedJobCoordinator::new(v2_config))
} else {
    Arc::new(SimpleJobProcessor::new(v1_config))
};

processor.process_batch(records, engine).await?;
```

**Pros**:
- Extensible (easy to add V3 later)
- No match statements
- Cleaner code

**Cons**:
- Trait objects have slight overhead
- Needs interface design
- More refactoring

---

### Option 3: Feature Flags (Compile-Time, 3 hours)

```rust
#[cfg(feature = "v2-architecture")]
use crate::velostream::server::v2::PartitionedJobCoordinator as JobProcessor;

#[cfg(not(feature = "v2-architecture"))]
use crate::velostream::server::processors::SimpleJobProcessor as JobProcessor;

// Rest of code uses JobProcessor generically
```

**Pros**:
- Zero runtime overhead
- Forces design compatibility

**Cons**:
- Can't switch at runtime (recompile required)
- Can't A/B test
- Config file can't control it

---

## Implementation Effort: Week 9 Scope

### What Week 9 Actually Needs:

**Current Plan**: "Run V2 baseline with 8 cores"

**What This Requires**:
1. ✅ V2 code exists (PartitionedJobCoordinator)
2. ❌ V2 integration with StreamJobServer
3. ❌ V2 + query execution integration
4. ❌ V2 record routing logic
5. ❌ V2 result collection/merging

### Quick Implementation Path (3 days, fits Week 9):

**Step 1: Create JobProcessorConfig**
```rust
pub enum JobProcessorConfig {
    V1(SimpleJobProcessorConfig),
    V2(PartitionedJobConfig),
}

impl Default for JobProcessorConfig {
    fn default() -> Self {
        Self::V1(SimpleJobProcessorConfig::default())
    }
}
```

**Step 2: Wrap Both in Trait**
```rust
pub trait JobProcessor: Send + Sync {
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError>;
}
```

**Step 3: Implement for V1**
```rust
impl JobProcessor for SimpleJobProcessor {
    async fn process_batch(...) -> Result<...> {
        // Existing SimpleJobProcessor::process_batch() logic
    }
}
```

**Step 4: Implement for V2** (NEW)
```rust
impl JobProcessor for PartitionedJobCoordinator {
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // 1. Create per-partition engines (or shared Arc)
        // 2. Route records by GROUP BY key via HashRouter
        // 3. Process in parallel on separate partitions
        // 4. Merge results from all partitions
        // 5. Return combined results
    }
}
```

**Step 5: Update StreamJobServer**
```rust
pub struct StreamJobServer {
    ...
    processor: Arc<dyn JobProcessor>,
    ...
}

impl StreamJobServer {
    pub fn with_processor(processor_config: JobProcessorConfig) -> Self {
        let processor: Arc<dyn JobProcessor> = match processor_config {
            JobProcessorConfig::V1(config) => {
                Arc::new(SimpleJobProcessor::new(config))
            }
            JobProcessorConfig::V2(config) => {
                Arc::new(PartitionedJobCoordinator::new(config))
            }
        };

        Self { processor, ... }
    }
}
```

**Step 6: Update Config**
```yaml
# application.yml
job:
  processor_architecture: v2  # or "v1"

  # V2 specific
  num_partitions: 8
  processing_mode: batch
  batch_size: 1000
  backpressure_enabled: true
```

---

## What Each Version Requires

### V1 (SimpleJobProcessor)
```
Input: StreamRecord × 1000 per batch
  ↓
Arc<Mutex<StreamExecutionEngine>>
  ├─ Single-threaded
  ├─ All records processed sequentially
  └─ Max: 23.7K rec/sec
  ↓
Output: StreamRecord × 1000 (or 99.8K for EMIT CHANGES)
```

**Required**: Existing code (ready now)

---

### V2 (PartitionedJobCoordinator)
```
Input: StreamRecord × 1000 per batch
  ↓
HashRouter (route by GROUP BY key)
  ├─ Record 1 → Partition 0
  ├─ Record 2 → Partition 3
  └─ Record 1000 → Partition 5
  ↓
Parallel Processing on 8 cores:
  ├─ Partition 0: StreamExecutionEngine (independent)
  ├─ Partition 1: StreamExecutionEngine (independent)
  └─ ... Partition 7
  ↓
Result Merging
  ├─ Collect from all partitions
  ├─ Sort/dedupe if needed
  └─ Return merged output
  ↓
Output: StreamRecord × 1000 (or 99.8K merged)
```

**Required** (NEW - must implement):
1. Per-partition StreamExecutionEngine instances (or shared with partition routing)
2. HashRouter integration with GROUP BY key extraction
3. Parallel partition processing (tokio::spawn for each partition)
4. Result collection from all partitions (mpsc channels)
5. Thread pooling or core affinity

---

## Decision: Which Approach for Week 9?

### **Recommended: Option 2 (Trait-Based)**

**Why**:
- ✅ Extensible (can add Phase 6-7 variants)
- ✅ No runtime overhead vs enum
- ✅ Cleaner code
- ✅ Fits Week 9 timeline (2 days)
- ✅ No compile-time flags (can switch at config time)

**Effort**:
- Design trait: 2 hours
- Implement SimpleJobProcessor adapter: 2 hours
- Implement PartitionedJobCoordinator adapter: 4 hours
- Update StreamJobServer: 2 hours
- Testing & validation: 2 hours
- **Total**: ~12 hours (doable in Week 9)

---

## What Week 9 Baseline Test Needs

**To run V2 baseline test and measure 191K rec/sec target:**

1. ✅ **V2 Coordinator exists** (ready now)
2. ✅ **V2 hash routing exists** (ready now)
3. ✅ **V2 partition state manager exists** (ready now)
4. ❌ **V2 integration with query execution** (need to add)
5. ❌ **V2 switching in StreamJobServer** (need to add)

**Minimal implementation for Week 9 baseline**:
- Create JobProcessor trait
- Wrap SimpleJobProcessor (V1)
- Wrap PartitionedJobCoordinator (V2)
- Add config option to choose
- Run baseline test with V2

**This does NOT require full V2 SQL integration** - just the coordinator structure and partition routing.

---

## Full V2 Integration (Weeks 10+)

After Week 9 baseline, full integration would need:

1. **Per-partition StreamExecutionEngine instances**
   - Clone engine per partition?
   - Or share Arc<Engine> with partition-specific state?
   - Decision: Share Arc with partition-scoped state managers

2. **Query type handling**
   - Window queries: Each partition has own window state
   - GROUP BY: Each partition has own aggregation state
   - Join queries: Careful state coordination needed

3. **EMIT CHANGES with V2**
   - 19.96x amplification × 8 partitions
   - Result merging from all partitions
   - Ordering considerations

4. **Fault tolerance**
   - If partition 3 fails, handle gracefully
   - Replay mechanism?

---

## Config File Example

```yaml
# v1.yml (current default)
job_server:
  processor_architecture: v1
  max_jobs: 100

# v2.yml (with 8 cores)
job_server:
  processor_architecture: v2

  v2:
    num_partitions: 8
    processing_mode: batch
    batch_size: 1000
    enable_core_affinity: false  # Phase 3+

    backpressure:
      enabled: true
      queue_threshold: 1000
      latency_threshold_ms: 100

      throttle:
        min_delay_us: 100
        max_delay_ms: 10
        backoff_multiplier: 2.0
```

---

## Summary

| Aspect | Current | Week 9 Plan |
|--------|---------|-----------|
| **V1 Available** | ✅ Full | ✅ Keep as-is |
| **V2 Available** | ✅ Partial (orphaned) | ✅ Integrate with trait |
| **Switching at Runtime** | ❌ No | ✅ Yes (via config) |
| **Switching Mechanism** | N/A | Trait-based dispatch |
| **Implementation Time** | N/A | 12 hours |
| **Baseline Test Runnable** | ✅ V1 only | ✅ V1 or V2 |
| **Performance Target** | 23.7K rec/sec | 23.7K (V1) or 191K (V2) |

---

## Action Items

### Week 9 Prerequisite (Before baseline testing):
- [ ] Design JobProcessor trait (2 hours)
- [ ] Implement V1 adapter (2 hours)
- [ ] Implement V2 adapter (4 hours)
- [ ] Update StreamJobServer (2 hours)
- [ ] Add config support (1 hour)
- [ ] Write switching tests (2 hours)
- [ ] Run baseline with both V1 and V2 (1 hour)

### If we skip switching (just run V2 directly):
- [ ] Integrate V2 directly into StreamJobServer (simpler but less flexible)
- [ ] Run baseline with V2 only
- [ ] No way to A/B test or compare

---

**Recommendation**: Implement trait-based switching in Week 9. It's only 12 hours extra work but provides huge flexibility for the rest of Phase 6-7.

