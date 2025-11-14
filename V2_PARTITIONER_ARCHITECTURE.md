# V2 Partitioner Architecture - Comprehensive Analysis

## Executive Summary

The V2 partitioner architecture is a **pluggable, strategy-based routing system** that distributes records across N partitions for parallel processing. It's designed to achieve 1.5M rec/sec throughput on 8 cores by enabling lock-free, independent partition execution.

**Key Insight**: Partitioner selection currently defaults to `AlwaysHashStrategy` but can be configured programmatically. Automatic partitioner selection based on query analysis is **not yet implemented** - this is the feature gap we need to address.

---

## 1. PARTITIONER IMPLEMENTATIONS

### Location: `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/`

#### Core Files:

| File | Purpose | Lines |
|------|---------|-------|
| `partitioning_strategy.rs` | Base trait + AlwaysHashStrategy | 228 |
| `smart_repartition_strategy.rs` | Alignment detection strategy | 200+ |
| `sticky_partition_strategy.rs` | Source partition affinity | (to explore) |
| `round_robin_strategy.rs` | Pure round-robin distribution | (to explore) |
| `fan_in_strategy.rs` | Concentrates all to partition 0 | (to explore) |
| `strategy_factory.rs` | Factory for creating strategies | 126 |
| `strategy_config.rs` | Configuration enum | 298 |
| `coordinator.rs` | PartitionedJobCoordinator | 500+ |
| `partition_manager.rs` | Per-partition state management | 453 |
| `job_processor_v2.rs` | JobProcessor trait implementation | 250+ |

---

## 2. CURRENT PARTITIONER LOGIC & SELECTION

### Current Implementation (Hard-Coded Default)

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/coordinator.rs` (lines 183-207)

```rust
pub fn new(config: PartitionedJobConfig) -> Self {
    // ... 
    let strategy = if let Some(strategy_name) = &config.partitioning_strategy {
        match crate::velostream::server::v2::StrategyFactory::create_from_str(strategy_name) {
            Ok(s) => {
                info!("Using partitioning strategy: {}", strategy_name);
                s
            }
            Err(e) => {
                warn!("Failed to load strategy '{}': {}. Falling back to AlwaysHashStrategy", 
                      strategy_name, e);
                Arc::new(AlwaysHashStrategy::new())
            }
        }
    } else {
        // DEFAULT: Always AlwaysHashStrategy if no config provided
        Arc::new(AlwaysHashStrategy::new())
    };
}
```

### Current Flow

1. **PartitionedJobConfig** has optional field: `partitioning_strategy: Option<String>` (line 40)
2. If **None** → Defaults to `AlwaysHashStrategy` (always hashing)
3. If **Some(name)** → Factory creates strategy from name string
4. **Factory** (`strategy_factory.rs` lines 30-38) creates Arc-wrapped strategies:
   - `"always_hash"` → AlwaysHashStrategy
   - `"smart_repartition"` → SmartRepartitionStrategy
   - `"round_robin"` → RoundRobinStrategy
   - `"sticky_partition"` → StickyPartitionStrategy
   - `"fan_in"` → FanInStrategy

### Problem: No Automatic Selection

- **Manual configuration only**: User must provide strategy name
- **No query analysis**: System doesn't inspect `StreamingQuery` to decide partitioner
- **No ORDER BY analysis**: Doesn't check if query has `ORDER BY` to suggest appropriate strategy
- **No GROUP BY analysis**: Doesn't extract GROUP BY columns from AST automatically

---

## 3. PARSED AST STRUCTURE

### Root Query Type

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/ast.rs` (line 100+)

```rust
pub enum StreamingQuery {
    Select {
        fields: Vec<SelectField>,
        from: StreamSource,
        from_alias: Option<String>,
        joins: Option<Vec<JoinClause>>,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,           // ← KEY FIELD
        having: Option<Expr>,
        window: Option<WindowSpec>,             // ← Has window info
        order_by: Option<Vec<OrderByExpr>>,    // ← KEY FIELD
        limit: Option<u64>,
        emit_mode: Option<EmitMode>,
        properties: Option<HashMap<String, String>>,
    },
    // ... other variants (CreateStream, CreateTable, etc.)
}
```

### Window Specification

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/ast.rs` (line 479)

```rust
pub enum WindowSpec {
    Tumbling { size: Duration, time_column: Option<String> },
    Sliding { size: Duration, advance: Duration, time_column: Option<String> },
    Session { gap: Duration, time_column: Option<String>, partition_by: Vec<String> },
    Rows { 
        buffer_size: u32,
        partition_by: Vec<Expr>,     // ← Window partition info
        order_by: Vec<OrderByExpr>,  // ← Window ordering
        time_gap: Option<Duration>,
        window_frame: Option<WindowFrame>,
        emit_mode: RowsEmitMode,
        expire_after: RowExpirationMode,
    },
}
```

### ORDER BY Structure

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/ast.rs` (line 527-537)

```rust
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: OrderDirection,  // Asc or Desc
}

pub enum OrderDirection {
    Asc,
    Desc,
}
```

### Expression Type (for GROUP BY and ORDER BY keys)

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/ast.rs` (line 593+)

```rust
pub enum Expr {
    Column(String),                          // ← Simple column reference
    Literal(LiteralValue),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    FunctionCall { name: String, args: Vec<Expr> },
    // ... many more variants
}
```

---

## 4. STREAMRECORD STRUCTURE

### Definition

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/types.rs` (line 1037)

```rust
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,        // Actual data
    pub timestamp: i64,                             // Processing time (ms)
    pub offset: i64,                                // Kafka offset
    pub partition: i32,                             // Source partition (Kafka)
    pub headers: HashMap<String, String>,           // Message headers
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,  // Event-time (optional)
}
```

### Available Methods for Field Access

```rust
// Line 1129: Get effective timestamp for time-based processing
pub fn get_event_time(&self) -> chrono::DateTime<chrono::Utc>

// Line 1145: Check if explicit event-time is set
pub fn has_event_time(&self) -> bool

// NOT SHOWN but available: get_field(column_name: &str) -> Option<&FieldValue>
```

### System Columns (from types.rs, line 128)

```rust
pub mod system_columns {
    pub const TIMESTAMP: &str = "_TIMESTAMP";
    pub const OFFSET: &str = "_OFFSET";
    pub const PARTITION: &str = "_PARTITION";    // ← Source partition!
    pub const EVENT_TIME: &str = "_EVENT_TIME";
    pub const WINDOW_START: &str = "_WINDOW_START";
    pub const WINDOW_END: &str = "_WINDOW_END";
}
```

### Key Finding: Source Partition Information

✅ **StreamRecord HAS partition information**: `partition: i32` field stores the source Kafka partition
✅ **Available via system column**: Can be accessed as `_PARTITION` in queries
✅ **SmartRepartitionStrategy uses it**: Checks `__partition__` field for alignment detection

---

## 5. JOB PROCESSOR INTEGRATION

### Where Partitioner is Used

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/job_processor_v2.rs` (lines 32-96)

```rust
#[async_trait::async_trait]
impl JobProcessor for PartitionedJobCoordinator {
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        _engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Phase 2+ Architecture with pluggable routing strategies
        // Strategy decision happens via self.strategy field
        // But: NO actual routing implementation in this method!
        // Just returns records for downstream processing in process_multi_job()
    }
}
```

### Actual Usage Location

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/coordinator.rs`

The coordinator stores the strategy but the actual routing would happen during:
1. `process_job()` method (line 110+) 
2. Or future `process_multi_job()` for parallel execution

**Current Code Gap**: Records are read and processed but NOT actually routed by the strategy to per-partition channels. This is a **Phase 2+ enhancement** that's planned but not yet implemented.

### Configuration Flow

```
User provides: PartitionedJobConfig
    └── partitioning_strategy: Option<String>
        
PartitionedJobCoordinator::new()
    └── StrategyFactory::create_from_str(strategy_name)
        └── Arc<dyn PartitioningStrategy>
            
Available when processing:
    └── self.strategy: Arc<dyn PartitioningStrategy>
        └── async fn route_record(&self, record: &StreamRecord, context: &RoutingContext) -> Result<usize>
```

---

## 6. KEY FINDINGS & GAPS

### What's Working ✅

1. **PartitioningStrategy Trait is defined** (lines 50-75 of partitioning_strategy.rs)
   - Trait methods: `route_record()`, `name()`, `validate()`
   - Implementations: AlwaysHash, SmartRepartition, RoundRobin, StickyPartition, FanIn

2. **Pluggable Strategy System is functional**
   - Factory pattern works
   - Configuration enum supports all strategies
   - Coordinator can store and use strategies

3. **RoutingContext has necessary info**
   ```rust
   pub struct RoutingContext {
       pub source_partition: Option<usize>,
       pub source_partition_key: Option<String>,
       pub group_by_columns: Vec<String>,
       pub num_partitions: usize,
       pub num_cpu_slots: usize,
   }
   ```

4. **StreamRecord has partition info** 
   - `partition: i32` field
   - System column `_PARTITION` access
   - SmartRepartitionStrategy already uses `__partition__` field

### What's Missing ❌

1. **No Automatic Partitioner Selection**
   - No code analyzes `StreamingQuery` AST
   - No extraction of GROUP BY columns from parsed query
   - No detection of ORDER BY to suggest compatible strategies
   - No window detection

2. **No Query Analysis for Routing Context**
   - `group_by_columns` in RoutingContext must be set manually
   - `source_partition_key` unknown (would need external metadata)
   - No automatic extraction from AST `group_by: Option<Vec<Expr>>`

3. **No Integration with Query Parsing**
   - Parser creates `StreamingQuery` AST but no downstream analysis
   - No pass-through of query metadata to coordinator
   - No extraction utilities for GROUP BY/ORDER BY from Expr types

4. **Actual Routing Not Implemented**
   - `process_batch()` returns records but doesn't route them
   - Per-partition channels not created/managed
   - `process_multi_job()` is defined but minimal implementation

---

## 7. HOW TO EXTRACT INFO FROM AST

### Extract GROUP BY Columns from AST

```rust
match query {
    StreamingQuery::Select { group_by: Some(exprs), .. } => {
        for expr in exprs {
            if let Expr::Column(col_name) = expr {
                group_by_columns.push(col_name.clone());
            }
        }
    }
    _ => {}
}
```

### Extract ORDER BY Keys from AST

```rust
match query {
    StreamingQuery::Select { order_by: Some(order_exprs), .. } => {
        for order_expr in order_exprs {
            if let Expr::Column(col_name) = &order_expr.expr {
                order_by_columns.push(col_name.clone());
            }
        }
    }
    _ => {}
}
```

### Check for Window Function

```rust
match query {
    StreamingQuery::Select { window: Some(window_spec), .. } => {
        match window_spec {
            WindowSpec::Rows { partition_by, order_by, .. } => {
                // Extract from window specification
            }
            _ => {}
        }
    }
    _ => {}
}
```

### Check for GROUP BY with Window

```rust
has_group_by = query.group_by().is_some();
has_window = query.window().is_some();
has_order_by = query.order_by().is_some();
```

---

## 8. SUGGESTED IMPLEMENTATION PLAN

### Phase 1: Query Analysis Module

Create `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/query_analyzer.rs`:

```rust
pub struct QueryAnalysisForPartitioning {
    pub has_group_by: bool,
    pub group_by_columns: Vec<String>,
    pub has_order_by: bool,
    pub order_by_columns: Vec<String>,
    pub has_window: bool,
    pub window_type: Option<String>,  // "tumbling", "sliding", "session", "rows"
}

impl QueryAnalysisForPartitioning {
    pub fn from_ast(query: &StreamingQuery) -> Self { ... }
    pub fn suggest_strategy(&self) -> StrategyConfig { ... }
}
```

### Phase 2: Strategy Selection Logic

**Decision Tree**:
```
1. Has GROUP BY?
   ├─ Yes + data naturally partitioned by GROUP BY key 
   │  └─ SmartRepartitionStrategy (0% overhead if aligned)
   ├─ Yes + data NOT aligned
   │  └─ AlwaysHashStrategy (safe, 5-10% overhead)
   └─ No
      ├─ Has ORDER BY?
      │  └─ StickyPartitionStrategy (maintain order)
      └─ No ORDER BY
         └─ RoundRobinStrategy (max throughput)
```

### Phase 3: Integration Points

1. Extend `PartitionedJobConfig`:
   ```rust
   pub auto_select_strategy: bool,  // New field
   ```

2. Modify `PartitionedJobCoordinator::new()`:
   ```rust
   if auto_select_strategy {
       let analysis = QueryAnalysisForPartitioning::from_ast(query);
       strategy = StrategyFactory::create(analysis.suggest_strategy())?;
   }
   ```

3. Update `process_job()` to pass query for analysis:
   ```rust
   pub async fn process_job(
       &self,
       reader: Box<dyn DataReader>,
       writer: Option<Box<dyn DataWriter>>,
       engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
       query: StreamingQuery,  // ← Now available!
       job_name: String,
       shutdown_rx: mpsc::Receiver<()>,
   ) -> Result<JobExecutionStats, ...>
   ```

---

## 9. KEY FILE LOCATIONS & LINE NUMBERS

| Component | File | Key Lines |
|-----------|------|-----------|
| **Partitioner Trait** | `partitioning_strategy.rs` | 50-75 |
| **AlwaysHashStrategy** | `partitioning_strategy.rs` | 87-168 |
| **SmartRepartitionStrategy** | `smart_repartition_strategy.rs` | 38-164 |
| **RoutingContext** | `partitioning_strategy.rs` | 31-48 |
| **StrategyConfig** | `strategy_config.rs` | 13-50 |
| **StrategyFactory** | `strategy_factory.rs` | 16-54 |
| **PartitionedJobCoordinator** | `coordinator.rs` | 166-217 |
| **Default Strategy Selection** | `coordinator.rs` | 183-207 |
| **StreamingQuery AST** | `sql/ast.rs` | 100-137 |
| **WindowSpec AST** | `sql/ast.rs` | 479-524 |
| **OrderByExpr AST** | `sql/ast.rs` | 527-537 |
| **StreamRecord** | `sql/execution/types.rs` | 1037-1052 |
| **System Columns** | `sql/execution/types.rs` | 128-200 |

---

## 10. EXAMPLE: Smart Strategy Selection

### What SmartRepartitionStrategy Does

**File**: `/Users/navery/RustroverProjects/velostream/src/velostream/server/v2/smart_repartition_strategy.rs`

**Logic** (lines 100-145):
```rust
async fn route_record(&self, record: &StreamRecord, context: &RoutingContext) -> Result<usize> {
    // Check if source partition key matches GROUP BY columns
    let is_aligned = match &context.source_partition_key {
        Some(source_key) => source_key == &context.group_by_columns.join(","),
        None => false,
    };

    // If aligned: use __partition__ field directly (ZERO overhead!)
    if is_aligned {
        if let Some(partition_value) = record.get_field("__partition__") {
            if let FieldValue::Integer(partition_id) = partition_value {
                return Ok((*partition_id as usize) % context.num_partitions);
            }
        }
    }

    // If misaligned: fall back to hashing (8% overhead)
    // ... hash GROUP BY columns ...
}
```

### Real-World Example

**Query**:
```sql
SELECT trader_id, SUM(amount) FROM trades
GROUP BY trader_id
```

**If Kafka topic is partitioned by `trader_id`**:
- ✅ Automatic selection should choose: **SmartRepartitionStrategy**
- 🎯 Result: 0% data movement overhead (records stay in original partition)

**If Kafka topic is partitioned by `symbol`**:
- ✅ Automatic selection should choose: **AlwaysHashStrategy**
- 🎯 Result: 100% repartitioning by `trader_id` (8% overhead is acceptable trade-off for correctness)

---

## Summary

The V2 partitioner architecture is **well-designed but incomplete**:

✅ **Implemented**: Trait system, multiple strategy implementations, factory pattern, coordinator
❌ **Missing**: Automatic strategy selection based on query analysis

**Key Implementation Gap**: No code extracts GROUP BY/ORDER BY from the parsed `StreamingQuery` AST to automatically select an appropriate partitioning strategy.

**Path Forward**: 
1. Create `QueryAnalysisForPartitioning` to analyze AST
2. Implement decision tree for strategy selection
3. Integrate into `PartitionedJobCoordinator` initialization
4. Pass `StreamingQuery` reference through job processing pipeline

