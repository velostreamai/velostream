# V2 State Consistency & Group-Based Routing Design

## Problem Statement

The V2 architecture MUST maintain state consistency for streaming aggregations. This requires:

**KEY REQUIREMENT**: Records with the **SAME GROUP BY key must ALWAYS route to the SAME partition**.

Without this guarantee, aggregation state becomes fragmented across partitions and results are incorrect.

## Current Issue (Week 9 Placeholder)

The placeholder V2 implementation in `job_processor_v2.rs` simply passes through records without routing. This is **INCORRECT for aggregations** because:

```rust
// ❌ BROKEN: Round-robin by index
for (idx, record) in records.into_iter().enumerate() {
    let partition_id = idx % num_partitions;  // Wrong! Record order, not GROUP BY key
    partition_outputs[partition_id].push(record);
}
```

**Example scenario that breaks**:
```sql
SELECT trader_id, SUM(price) OVER (PARTITION BY trader_id)
FROM trades
```

With round-robin:
- Record 0: trader_id=A → Partition 0
- Record 1: trader_id=A → Partition 1
- Record 2: trader_id=A → Partition 2
- **Result**: SUM(price) for trader_id=A split across 3 partitions = WRONG ANSWER

## Correct Design: HashRouter-Based Routing

### Architecture Flow

```
┌──────────────────────────────────────────────────────────────┐
│ StreamJobServer                                              │
│                                                              │
│ 1. Parse query: "SELECT ... GROUP BY trader_id ..."         │
│ 2. Extract GROUP BY columns: [trader_id]                    │
│ 3. Create HashRouter:                                       │
│    let router = HashRouter::new(                            │
│        num_partitions=8,                                    │
│        PartitionStrategy::HashByGroupBy {                   │
│            group_by_columns: vec![Expr::Column("trader_id")]│
│        }                                                     │
│    )                                                        │
│ 4. Create V2 processor with router context                 │
│ 5. Execute query on each batch                             │
└──────────────────────────────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  JobProcessor   │
                    │  (trait)        │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │ Partition 0  │    │ Partition 1  │    │ Partition 7  │
  │ trader_id=A  │    │ trader_id=C  │    │ trader_id=G  │
  │ trader_id=B  │    │ trader_id=D  │    │ trader_id=H  │
  └──────────────┘    └──────────────┘    └──────────────┘
        │                    │                    │
        │         Hash fn: hash(trader_id) % 8    │
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Output Stream  │
                    └─────────────────┘
```

### Implementation: Week 9 Task 5

**Step 1: Update Processor Signature**

Currently:
```rust
async fn process_batch(
    &self,
    records: Vec<StreamRecord>,
    engine: Arc<StreamExecutionEngine>,
) -> Result<Vec<StreamRecord>, SqlError>;
```

Proposed (Week 9 Task 5):
```rust
async fn process_batch(
    &self,
    records: Vec<StreamRecord>,
    engine: Arc<StreamExecutionEngine>,
    router: Option<Arc<HashRouter>>,  // Query routing context
) -> Result<Vec<StreamRecord>, SqlError>;
```

**Step 2: V2 Implementation with Proper Routing**

```rust
#[async_trait::async_trait]
impl JobProcessor for PartitionedJobCoordinator {
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
        router: Option<Arc<HashRouter>>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let router = router.ok_or_else(|| {
            SqlError::InternalError(
                "V2 requires HashRouter with GROUP BY columns".to_string()
            )
        })?;

        let num_partitions = self.num_partitions();
        let mut partition_queues: Vec<Vec<StreamRecord>> =
            vec![Vec::new(); num_partitions];

        // ✅ CORRECT: Route by GROUP BY key hash, not by record index
        for record in records {
            let partition_id = router.route_record(&record)?;
            partition_queues[partition_id].push(record);
        }

        // Process each partition in parallel (tokio::spawn)
        let mut handles = Vec::new();
        for (partition_id, records) in partition_queues.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }

            let engine = Arc::clone(&engine);
            let handle = tokio::spawn(async move {
                // Each partition processes independently
                let mut output = Vec::new();
                for record in records {
                    // Execute through SQL engine
                    if let Ok(results) = engine.execute_with_record(record).await {
                        output.extend(results);
                    }
                }
                output
            });
            handles.push(handle);
        }

        // Collect results from all partitions
        let mut final_output = Vec::new();
        for handle in handles {
            if let Ok(partition_results) = handle.await {
                final_output.extend(partition_results);
            }
        }

        Ok(final_output)
    }
}
```

### StreamJobServer Integration

```rust
impl StreamJobServer {
    async fn execute_query(&self, query_text: &str) -> Result<Vec<StreamRecord>> {
        // 1. Parse query to get GROUP BY columns
        let ast = StreamingSqlParser::parse(query_text)?;
        let group_by_cols = self.extract_group_by_columns(&ast)?;

        // 2. Create processor-specific router
        let router = if self.processor_version == "V2" {
            let router = HashRouter::new(
                8,  // num_partitions
                PartitionStrategy::HashByGroupBy {
                    group_by_columns: group_by_cols
                }
            );
            Some(Arc::new(router))
        } else {
            None
        };

        // 3. Process batch with routing context
        let output = self.processor.process_batch(
            input_records,
            Arc::clone(&self.engine),
            router
        ).await?;

        Ok(output)
    }
}
```

## State Consistency Verification

### Correct Behavior Example

```sql
SELECT trader_id, SUM(price) as total
FROM trades
WINDOW TUMBLING(1 second)
GROUP BY trader_id
EMIT CHANGES
```

**Input**:
```
Batch 1:
- (time=1000, trader_id=A, price=100)
- (time=1000, trader_id=A, price=50)
- (time=1000, trader_id=B, price=200)
- (time=1000, trader_id=A, price=75)
```

**With Correct HashRouter**:
```
hash(A) % 8 = 3  →  Partition 3: [A:100, A:50, A:75]
hash(B) % 8 = 5  →  Partition 5: [B:200]

Partition 3 state: SUM(A) = 225
Partition 5 state: SUM(B) = 200

Output:
- (time=1000, trader_id=A, total=225)
- (time=1000, trader_id=B, total=200)
```

**With Broken Round-Robin**:
```
idx=0  (A, 100) → Partition 0: [A:100]
idx=1  (A, 50)  → Partition 1: [A:50]
idx=2  (B, 200) → Partition 2: [B:200]
idx=3  (A, 75)  → Partition 3: [A:75]

Partition 0 state: SUM(A) = 100
Partition 1 state: SUM(A) = 50
Partition 2 state: SUM(B) = 200
Partition 3 state: SUM(A) = 75

Output (WRONG):
- (time=1000, trader_id=A, total=100)  ❌ Should be 225
- (time=1000, trader_id=A, total=50)   ❌ Duplicate key
- (time=1000, trader_id=B, total=200)  ✓
- (time=1000, trader_id=A, total=75)   ❌ Duplicate key, wrong value
```

## Why Placeholder is Insufficient

The Week 9 placeholder (pass-through) is acceptable because:

1. **It's not production code** - It's just infrastructure validation
2. **It shows the trait architecture works** - Allows testing interface compatibility
3. **It documents the limitation** - Makes clear what's missing for real implementation
4. **It guides Task 5** - Clarifies exactly what needs to be added

The placeholder is **BROKEN for stateful queries** but **VALID for the architectural proof-of-concept**.

## Week 9 Task 5: Implementation Plan

**Goal**: Enable proper HashRouter-based routing in StreamJobServer

**Steps**:
1. Update JobProcessor trait signature to accept optional router
2. Extract GROUP BY columns in StreamJobServer
3. Create and pass HashRouter to processor
4. Implement parallel partition processing with tokio::spawn
5. Implement result merging
6. Add tests for state consistency

**Expected Result**: V2 correctly routes identical GROUP BY keys to same partition

## Performance Notes

### Before Optimization (Current):
- V2 baseline: 191K rec/sec (8 cores, simplified hash distribution)
- Per-partition: 23.9K rec/sec

### After HashRouter Integration:
- Expected: Same 191K rec/sec (routing overhead negligible vs execution)
- But results will be **CORRECT** instead of fragmented

### Phase 6 Optimizations Will Target:
- Per-partition performance: 70-142K rec/sec (lock-free improvements)
- Scaling: 560-1,136K rec/sec on 8 cores

## References

- HashRouter implementation: `src/velostream/server/v2/hash_router.rs`
- PartitionStrategy: `src/velostream/server/v2/hash_router.rs:11-22`
- V2 placeholder: `src/velostream/server/v2/job_processor_v2.rs`
- StreamJobServer: `src/velostream/server/stream_job_server.rs`

## Related Issues

- State fragmentation in aggregations
- Incorrect WINDOW results with multi-partition
- Duplicate GROUP BY key outputs
- Session window state consistency

## Status

**Week 9 Placeholder**: ✅ Demonstrates trait infrastructure
**Full Implementation**: ⏳ Week 9 Task 5 (2-3 days)
**Testing**: ⏳ State consistency verification tests
