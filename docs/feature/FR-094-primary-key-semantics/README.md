# FR-094: Primary Key Semantics & Keyed State Management

## Status: Proposed (Backlog)

## Priority: Low - No current user impact

The primary use case (Kafka message key routing) already works via FR-089. These are **missing features**, not bugs. No users are losing data or getting wrong results. This FR serves as a roadmap for when keyed state, dedup, or CDC capabilities are needed.

**Quick win**: Add `log::warn!()` when PK is specified on a file sink (~30 min, no FR required).

---

## Summary

Extend PRIMARY KEY from a transport-level annotation (Kafka message key routing, FR-089) to a full semantic concept in the SQL engine. This enables connector-level PK validation, deduplication, upsert/changelog streams, compacted topic integration, and key-based state management for materialized tables.

### Motivation

FR-089 implemented `PRIMARY KEY` as a Kafka message key routing mechanism. The annotation controls *which fields become the Kafka message key*, but the engine has no awareness of key semantics:

- No uniqueness enforcement or deduplication
- No upsert capability (`supports_upsert: false` in all sinks)
- No changelog stream semantics (insert/update/delete)
- No compacted topic awareness
- CREATE TABLE doesn't use PKs for state management
- No point-query lookups against materialized tables by PK
- **PK column names only reach sinks** (via `DataSinkRequirement.primary_keys`, not through execution layer)
- **File sinks silently ignore PK annotations** (no warning)
- **`supports_upsert` trait method exists but is never checked** during query planning
- **No connector-level changelog mode validation** (unlike Flink SQL)

These capabilities are standard in competing streaming SQL engines (Flink SQL, ksqlDB, RisingWave, Materialize) and are critical for real-world use cases like:

- **Financial position tracking**: Maintain latest position per instrument
- **Customer state management**: Keep current customer record, update on changes
- **Real-time dashboards**: Materialized aggregation tables queryable by key
- **CDC processing**: Consume changelog topics with insert/update/delete semantics

---

## Current State (FR-089)

### What EXISTS today

```
Parser (SELECT) ──> AST (key_fields) ──> QueryAnalyzer ──> DataSinkRequirement.primary_keys
                                                                       │
                                                           KafkaDataSink.primary_keys
                                                                       │
                                                           KafkaDataWriter.primary_keys
                                                                       │
                                                        extract_message_key(record)
                                                                       │
                                                          Kafka message key bytes
```

- `StreamingQuery::Select::key_fields: Option<Vec<String>>` (ast.rs:219)
- Parser handles `PRIMARY KEY` annotation after SELECT fields (parser/select.rs:1026-1104)
- Composite keys serialized as pipe-delimited strings
- GROUP BY columns auto-generate keys when no explicit PK
- Key priority: inline PK > GROUP BY > null (round-robin)
- Source key accessible via `_key` pseudo-column

### Transport Key vs Semantic PK

Velostream currently conflates two distinct concepts into `record.key`:

| Concept | Transport Key (`record.key`) | Semantic PK (SQL PRIMARY KEY) |
|---------|------------------------------|-------------------------------|
| **What it is** | Raw Kafka message key bytes | Column names declaring the logical identity of a record |
| **Where it lives** | `StreamRecord.key: Option<FieldValue>` | `DataSinkRequirement.primary_keys: Option<Vec<String>>` |
| **Who sets it** | Kafka consumer (from msg bytes) or GROUP BY (computed) | SQL parser (from PRIMARY KEY annotation) |
| **Composite support** | Lossy: pipe-delimited `"val1\|val2"` string | Clean: list of column names, values in `record.fields` |
| **File source** | `None` (files have no transport key) | Works: PK columns resolve to `record.fields` values |
| **SQL access** | `_key` pseudo-column | Not directly accessible (sink-config only) |
| **Reversible** | No: can't decompose `"A\|B"` back to fields | Yes: look up each column name in `record.fields` |

**Key insight**: The transport key is a Kafka-specific opaque value that may not correspond to any field in the payload (e.g., a producer sets key `"user-123"` on payload `{"name": "Alice"}`). The semantic PK is SQL metadata that refers to named columns whose values exist in `record.fields`.

These overlap when GROUP BY sets `record.key` from group column values, but they are fundamentally different:
- Transport key = "what bytes go on the Kafka message key wire"
- Semantic PK = "which columns uniquely identify this record"

### PK Flow Through the Pipeline Today

```
┌─────────────────────────────────────────────────────────────────┐
│  TRANSPORT KEY (record.key)          │  SEMANTIC PK              │
│  Kafka-specific, opaque value        │  SQL column names         │
├──────────────────────────────────────┼───────────────────────────┤
│                                      │                           │
│  Kafka Source:                       │  SQL Parser:              │
│    msg.key() → record.key            │    PRIMARY KEY annotation │
│    (raw bytes, may not be a field)   │    → AST.key_fields       │
│                                      │    → DataSinkRequirement  │
│  File Source:                        │      .primary_keys        │
│    record.key = None                 │    (column NAMES only)    │
│    (no transport key concept)        │                           │
│                                      │  Works for ANY source:    │
│  GROUP BY execution:                 │    Values resolved from   │
│    record.key = group column values  │    record.fields by name  │
│    (single: raw; compound: "a|b")    │                           │
├──────────────────────────────────────┼───────────────────────────┤
│                                      │                           │
│  KAFKA WRITER (extract_key):         │  FUTURE OPERATORS:        │
│    Priority 1: record.key            │    KeyedTableState        │
│    Priority 2: primary_keys config   │    DedupProcessor         │
│      → extract from record.fields    │    ChangelogProducer      │
│    Priority 3: NULL (round-robin)    │    (all configured with   │
│                                      │     PK columns at init)   │
└──────────────────────────────────────┴───────────────────────────┘
```

### Implications for Each Source Type

| Source | Transport Key | Semantic PK | Notes |
|--------|--------------|-------------|-------|
| **Kafka** | `record.key` from msg bytes | PK columns resolve to `record.fields` | Both available; transport key may differ from PK |
| **File (JSON)** | `None` | PK columns resolve to `record.fields` | PK works; transport key irrelevant |
| **File (CSV)** | `None` | PK columns resolve to `record.fields` | PK works; transport key irrelevant |
| **Batch** | `None` | PK columns resolve to `record.fields` | PK works; transport key irrelevant |

For non-Kafka sources, the transport key is meaningless. The semantic PK is source-independent - it always resolves column names against `record.fields`.

### Adaptive/Hash Routing: NOT a Gap

The adaptive processor's hash-based partition routing is a **third, independent mechanism** that does not depend on `record.key` or PRIMARY KEY:

```
File Source → record = {station: "Hamburg", temperature: 12.0}
                        record.key = None  (irrelevant)
                              ↓
Hash Router: extracts record.fields["station"]  ← GROUP BY column from parsed query
             FNV-1a hash("Hamburg") % 6 → partition 3
                              ↓
Partition 3's SQL Engine: independent GROUP BY aggregation
```

The routing strategy (`partitioning_strategy.rs:116-148`) extracts GROUP BY column values directly from `record.fields`, not from `record.key` or PK annotations. This works correctly for all source types (file, Kafka, batch) because `record.fields` is always populated.

| Mechanism | Purpose | Key source | Depends on PK? |
|-----------|---------|-----------|----------------|
| **Hash routing** (adaptive) | Partition input records to parallel workers | `record.fields[group_by_column]` | No |
| **GROUP BY key** (execution) | Set `record.key` on output records | Computed from group column values | No |
| **PRIMARY KEY** (SQL annotation) | Configure Kafka message key field names | AST → DataSinkRequirement | Yes (is PK) |

These three mechanisms are intentionally decoupled. The 1BRC demo (`demo/1brc/1brc.sql`) demonstrates this: it uses `@job_mode: adaptive` with `@partitioning_strategy: hash` on a file source with GROUP BY, no PRIMARY KEY annotation, and achieves 4.3x parallel speedup.

### Current Gaps

**This is sufficient for the current use case** (Kafka message key routing + parallel aggregation). PK column names travel via configuration to the writer, GROUP BY values travel via `record.key`, and hash routing works independently from `record.fields`.

**What's missing is validation and semantics**, not data flow:
- File sinks silently ignore PKs (should warn)
- `supports_upsert` trait method (traits.rs:101) is never checked during planning
- No ChangelogMode concept to prevent silent data loss
- Future keyed state / dedup operators will receive PK columns at construction time (same pattern as writer)
- `_key` pseudo-column only accesses the transport key, not the semantic PK

### What's MISSING

```
                     ┌─────────────────────────────────────────────┐
                     │         NOT YET IMPLEMENTED                 │
                     │                                             │
                     │  DDL-level PK  ──> Schema metadata          │
                     │  Connector PK  ──> Validation & changelog   │
                     │  ChangelogMode ──> Sink capability declaration│
                     │  Uniqueness    ──> Constraint enforcement    │
                     │  Dedup         ──> Record-level dedup by PK  │
                     │  Upsert        ──> Keyed state update        │
                     │  Changelog     ──> INSERT/UPDATE/DELETE ops  │
                     │  Compaction    ──> Topic cleanup.policy       │
                     │  Point query   ──> Lookup by PK              │
                     └─────────────────────────────────────────────┘
```

---

## Connector-Level PK Gaps (Current State)

### Kafka Sink

PK information flows correctly from SQL annotation to Kafka message key:

```
SQL PRIMARY KEY → AST.key_fields → DataSinkRequirement.primary_keys
    → KafkaDataSink.primary_keys → KafkaDataWriter.primary_keys
    → extract_key(record) → Kafka message key bytes
```

However:
- `supports_upsert()` returns `false` (data_sink.rs:702-704) and is **never checked**
- No changelog mode awareness - all output is append-only
- No compacted topic auto-configuration

### File Sinks (JSON & CSV)

**File sinks completely ignore PK annotations with no warning.**

```rust
// data_sink.rs - File sink creation
// DataSinkRequirement.primary_keys is received but NEVER used
// No warning is logged when PK is specified on a file sink
```

- `supports_upsert()` returns `false` - correct, but unchecked
- `supports_transactions()` returns `false`
- PK columns exist in `DataSinkRequirement` but are silently dropped
- Users get no feedback that their PK annotation has no effect

### DataSource Requirements (Asymmetric)

```rust
// query_analyzer.rs:42-62
pub struct DataSourceRequirement {
    pub name: String,
    pub source_type: DataSourceType,
    pub properties: HashMap<String, String>,
    // NO primary_keys field  <-- Asymmetric with DataSinkRequirement
}

pub struct DataSinkRequirement {
    pub name: String,
    pub sink_type: DataSinkType,
    pub properties: HashMap<String, String>,
    pub primary_keys: Option<Vec<String>>,  // <-- ONLY on sink
}
```

### `supports_upsert` Trait: Defined But Unused

```rust
// traits.rs:101
fn supports_upsert(&self) -> bool;      // Defined on DataSink trait
// Kafka: returns false (data_sink.rs:702)
// File:  returns false (file/data_sink.rs)
// NEVER CHECKED by query planner, validator, or runtime
```

---

## Flink SQL Comparison: Connector-Level PK Handling

Flink SQL provides a mature reference implementation for connector-level PK semantics. Key differences with Velostream:

### 1. NOT ENFORCED Keyword

Flink SQL requires `NOT ENFORCED` on all PRIMARY KEY declarations:

```sql
-- Flink SQL
CREATE TABLE orders (
    order_id BIGINT,
    customer STRING,
    PRIMARY KEY (order_id) NOT ENFORCED  -- Required keyword
) WITH ('connector' = 'kafka', ...);
```

**Rationale**: Streaming engines don't own the data source. They cannot enforce uniqueness constraints on external systems (Kafka topics, files, databases). `NOT ENFORCED` makes this explicit to the user.

**Velostream today**: No equivalent concept. PKs are annotations, not constraints, but this isn't declared explicitly.

### 2. ChangelogMode Framework

Flink connectors self-declare their changelog capabilities at plan time:

```java
// Flink connector SPI
public interface DynamicTableSink {
    ChangelogMode getChangelogMode(ChangelogMode requestedMode);
}

// ChangelogMode options:
// INSERT_ONLY     - Append-only streams (file sinks, regular kafka)
// RETRACT         - Insert + delete pairs (retraction-based updates)
// UPSERT          - Keyed updates + deletes (requires PK)
```

**Flink plan-time validation**: If a query produces UPDATE/DELETE changes but the sink only supports INSERT_ONLY, the planner rejects the query at planning time with a clear error message. This prevents silent data loss.

**Velostream today**: No changelog mode concept. All sinks are implicitly INSERT_ONLY. The `supports_upsert` trait method exists but is never checked.

### 3. Connector PK Behavior Comparison

| Connector | Flink Behavior | Velostream Behavior |
|-----------|---------------|---------------------|
| **kafka** (regular) | Append-only (INSERT_ONLY). PK optional, used as message key. | PK used as message key. No mode validation. |
| **upsert-kafka** | PK REQUIRED. UPSERT mode. Null-value = tombstone/delete. Auto-compaction. | No equivalent connector. |
| **filesystem** | INSERT_ONLY ONLY. Errors if query produces updates/deletes. | Silently ignores PK. No warning. |
| **jdbc** | With PK: UPSERT mode (INSERT ON CONFLICT UPDATE). Without PK: INSERT_ONLY. | No JDBC connector. |
| **elasticsearch** | With PK: UPSERT mode (document ID = PK). Without PK: append. | No ES connector. |

### 4. Key Flink Design Principles Missing in Velostream

| Principle | Flink | Velostream |
|-----------|-------|------------|
| **Explicit changelog modes** | Connectors declare INSERT_ONLY / RETRACT / UPSERT | No concept of changelog mode |
| **Plan-time validation** | Mismatch between query output mode and sink mode = error | No validation at any stage |
| **PK requirement enforcement** | Upsert sinks REQUIRE PK declaration | PK is always optional |
| **Tombstone handling** | Null-value records = deletes in UPSERT mode | No tombstone support |
| **NOT ENFORCED semantics** | Explicit that PKs are hints, not constraints | Implicit (undocumented) |

---

## Proposed Design

### Phase 0: Connector-Level PK Validation & ChangelogMode (NEW)

Before adding keyed state management, establish the foundational infrastructure for PK-aware planning and validation.

#### How PK Information Flows Today (No StreamRecord Changes Needed)

PK column names are a **schema-level** concept, not per-record data. They already flow to where they're needed via two independent paths:

**Path 1 - PK column names (configuration-level):**
```
SQL PRIMARY KEY annotation
    → Parser → AST.key_fields (column names)
    → QueryAnalyzer.extract_key_fields_from_query()
    → DataSinkRequirement.primary_keys (column names)
    → KafkaDataSink.primary_keys → KafkaDataWriter.primary_keys
    → extract_key(): extracts field VALUES from record.fields by column NAME
```

**Path 2 - Computed key value (execution-level):**
```
GROUP BY execution (adapter.rs:1091-1104)
    → record.key = FieldValue from group column values
    → Single column: raw value; compound: pipe-delimited string
```

**At write time** (`KafkaDataWriter.extract_key()`):
1. Check `record.key` (from GROUP BY or incoming Kafka key) - use if present
2. Check `self.primary_keys` config (from SQL annotation) - extract values from `record.fields`
3. Neither → NULL key, round-robin partitioning

**Why no StreamRecord change is needed**: Every operator that needs PK awareness (keyed state, dedup, writer) is configured with the PK column names at construction time. No operator receives records "blindly" without knowing its schema. Putting the same column names on every record would be redundant.

The one per-record field that IS needed later (Phase 4) is an **operation type** for changelog semantics:

```rust
// Phase 4 addition to StreamRecord (not Phase 0)
pub enum ChangeOp { Insert, Update, Delete }
pub struct StreamRecord {
    // ... existing fields
    pub op: Option<ChangeOp>,  // Per-record: one record is Insert, next is Delete
}
```

#### 0a. Add ChangelogMode to DataSink Trait

```rust
// traits.rs - Extend DataSink trait
pub enum ChangelogMode {
    /// Sink accepts INSERT operations only (append-only)
    InsertOnly,
    /// Sink accepts INSERT/UPDATE/DELETE (requires PK)
    Upsert,
    /// Sink accepts INSERT + retraction pairs
    Retract,
}

pub trait DataSink {
    fn supports_upsert(&self) -> bool;           // Existing (currently unused)
    fn changelog_mode(&self) -> ChangelogMode;   // NEW
    fn requires_primary_key(&self) -> bool;      // NEW
    // ...
}
```

#### 0b. Plan-Time Validation in QueryAnalyzer

```rust
// query_analyzer.rs - Add validation during sink creation
fn validate_sink_pk_compatibility(
    sink: &DataSinkRequirement,
    primary_keys: &Option<Vec<String>>,
) -> Result<(), SqlError> {
    match sink.sink_type {
        // File sinks: warn if PK specified (will be ignored)
        DataSinkType::File | DataSinkType::CsvFile => {
            if primary_keys.is_some() {
                log::warn!(
                    "PRIMARY KEY specified on file sink '{}' will be ignored. \
                     File sinks are append-only and do not support keyed operations.",
                    sink.name
                );
            }
        }
        // Kafka: validate PK fields exist in output schema
        DataSinkType::Kafka => {
            // Existing behavior + future upsert-kafka validation
        }
    }
    Ok(())
}
```

#### 0c. NOT ENFORCED Keyword

Add optional `NOT ENFORCED` keyword to PRIMARY KEY declarations:

```sql
-- Both forms accepted (NOT ENFORCED is implied when omitted)
CREATE TABLE positions (
    PRIMARY KEY (symbol) NOT ENFORCED
) AS SELECT ...;

-- Equivalent to:
CREATE TABLE positions AS
SELECT symbol PRIMARY KEY, ...;
```

Since Velostream doesn't own the data source, enforcement is never possible. The keyword serves as explicit documentation for users familiar with Flink/standard SQL semantics.

---

### Phase 1: DDL-Level Primary Key Definition

Add `PRIMARY KEY` constraint to `CREATE TABLE` and `CREATE STREAM` DDL:

#### Syntax

```sql
-- Inline column-level (single key)
CREATE TABLE positions AS
SELECT symbol PRIMARY KEY, SUM(quantity) AS position
FROM trades
GROUP BY symbol;

-- Table-level constraint (composite key) - NEW
CREATE TABLE portfolio_positions (
    PRIMARY KEY (account_id, symbol)
) AS
SELECT account_id, symbol, SUM(quantity) AS position
FROM trades
GROUP BY account_id, symbol;

-- With NOT ENFORCED (Flink-compatible) - NEW
CREATE TABLE portfolio_positions (
    PRIMARY KEY (account_id, symbol) NOT ENFORCED
) AS
SELECT account_id, symbol, SUM(quantity) AS position
FROM trades
GROUP BY account_id, symbol;
```

#### AST Changes

```rust
// ast.rs - Add to CreateTable and CreateStream variants
CreateTable {
    name: String,
    columns: Option<Vec<ColumnDef>>,
    primary_key: Option<PrimaryKeyDef>,  // NEW
    as_select: Box<StreamingQuery>,
    properties: HashMap<String, String>,
    emit_mode: Option<EmitMode>,
}

// NEW struct
pub struct PrimaryKeyDef {
    /// Column names forming the composite primary key
    pub columns: Vec<String>,
    /// Whether key was defined inline (column-level) or as table constraint
    pub constraint_type: PrimaryKeyConstraintType,
    /// Whether NOT ENFORCED was explicitly declared
    pub not_enforced: bool,
}

pub enum PrimaryKeyConstraintType {
    /// Column-level: inherited from SELECT ... PRIMARY KEY
    Inline,
    /// Table-level: PRIMARY KEY (col1, col2)
    TableConstraint,
}
```

#### Parser Changes

```
parse_create_table()
  ├── parse optional column definitions
  │     └── detect PRIMARY KEY (col1, col2) [NOT ENFORCED] clause  ← NEW
  ├── parse AS SELECT
  │     └── existing PRIMARY KEY annotation parsing
  └── merge/validate both key sources
```

Key validation rules:
- If both DDL-level and SELECT-level PKs exist, they must match
- PK columns must appear in the SELECT output fields
- PK columns must be a subset of GROUP BY columns (if GROUP BY present)
- Connector changelog mode validated against PK presence (Phase 0)

---

### Phase 2: Keyed State Management for Tables

Transform `CREATE TABLE` from append-mode to keyed-upsert mode when a PK is defined.

#### Behavior Change

| Feature | Without PK | With PK |
|---------|-----------|---------|
| Record storage | Append-only | Keyed HashMap |
| Duplicate handling | All records kept | Latest value wins |
| Memory model | Unbounded growth | Bounded by unique keys |
| Kafka output | Normal topic | Compacted topic eligible |
| State semantics | Stream | Changelog/KTable |

#### Execution Engine Changes

```rust
// New keyed state store for CREATE TABLE with PK
pub struct KeyedTableState {
    /// Records indexed by primary key
    state: FxHashMap<GroupKey, StreamRecord>,
    /// Primary key column names
    pk_columns: Vec<String>,
    /// Metrics
    total_inserts: u64,
    total_updates: u64,
}

impl KeyedTableState {
    /// Upsert: insert or update record by PK
    fn upsert(&mut self, record: StreamRecord) -> UpsertResult {
        let key = self.extract_key(&record);
        match self.state.entry(key) {
            Entry::Occupied(mut e) => {
                e.insert(record);
                self.total_updates += 1;
                UpsertResult::Updated
            }
            Entry::Vacant(e) => {
                e.insert(record);
                self.total_inserts += 1;
                UpsertResult::Inserted
            }
        }
    }
}

pub enum UpsertResult {
    Inserted,
    Updated,
}
```

#### Wiring into Existing Architecture

The `GroupKey` struct (internal.rs:17-61) already provides:
- Pre-computed hash for O(1) lookups
- `Arc<[FieldValue]>` for zero-copy key storage
- `FxHashMap` compatibility

This can be reused directly for PK-based state indexing - no new key infrastructure needed.

---

### Phase 3: Deduplication

Add record-level deduplication based on PK for input streams.

#### Syntax

```sql
-- Deduplicate by primary key (keep latest)
SELECT order_id PRIMARY KEY, customer, amount
FROM orders
DEDUPLICATE BY order_id;

-- Or via table property
CREATE TABLE latest_orders (
    PRIMARY KEY (order_id)
) WITH (
    'dedup.enabled' = 'true',
    'dedup.strategy' = 'latest'  -- latest | first | custom
) AS
SELECT order_id, customer, amount
FROM orders;
```

#### Dedup Strategies

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `latest` | Keep most recent by event time | CDC, state tracking |
| `first` | Keep first occurrence | Idempotency |
| `within_window` | Dedup within time window | At-least-once consumers |

---

### Phase 4: Changelog Stream Semantics

Enable `INSERT`, `UPDATE`, `DELETE` operations on keyed streams.

#### Consuming Changelog Topics

```sql
-- Consume a Kafka compacted topic as a changelog
CREATE TABLE customer_state (
    PRIMARY KEY (customer_id)
) WITH (
    'source.type' = 'kafka_source',
    'source.topic' = 'customers',
    'source.changelog' = 'true'    -- Enable changelog semantics
) AS
SELECT customer_id, name, email, status
FROM customers;
```

#### Producing Changelog Output

```sql
-- Output stream automatically includes operation type when PK defined
-- Downstream consumers see: {"op": "upsert", "customer_id": "C1", "name": "Alice", ...}
-- Tombstone records (null value) emitted on delete for Kafka compaction
```

---

### Phase 5: Compacted Topic Integration

When a PK is defined on a Kafka sink, automatically configure topic for log compaction.

#### Auto-Configuration

```sql
CREATE TABLE symbol_prices (
    PRIMARY KEY (symbol)
) WITH (
    'sink.type' = 'kafka_sink',
    'sink.topic' = 'latest_prices',
    'sink.compaction' = 'auto'     -- Auto-set cleanup.policy=compact
) AS
SELECT symbol PRIMARY KEY, LAST(price) AS price, LAST(volume) AS volume
FROM market_data
GROUP BY symbol;
```

#### Topic Properties Set Automatically

```properties
cleanup.policy=compact
min.compaction.lag.ms=0
segment.ms=86400000
```

---

## Implementation Phases & Dependencies

```
Phase 0: Connector PK Validation    [No dependencies - FOUNDATIONAL]
    │
    └── Phase 1: DDL-Level PK       [Depends on Phase 0]
            │
            ├── Phase 2: Keyed State    [Depends on Phase 1]
            │       │
            │       ├── Phase 3: Dedup  [Depends on Phase 2]
            │       │
            │       └── Phase 5: Compaction [Depends on Phase 2]
            │
            └── Phase 4: Changelog      [Depends on Phase 1 + Phase 2]
```

### Estimated Scope & Priority

| Phase | Description | LoE | Priority | Trigger |
|-------|-------------|-----|----------|---------|
| 0 | Connector PK validation, ChangelogMode trait | 1-2 days | Low | Quality-of-life; no user blocked |
| 1 | DDL-level PK, NOT ENFORCED | 3-5 days | Low | Syntax convenience; inline PK already works |
| 2 | Keyed state (upsert) | 2-3 weeks | Medium | User needs materialized latest-value-per-key tables |
| 3 | Deduplication | 1-2 weeks | Medium | User needs engine-level dedup (workaround: dedup downstream) |
| 4 | Changelog semantics | 2-3 weeks | Medium | User needs CDC pipeline ingestion |
| 5 | Compacted topics | 2-3 days | Low | Convenience; users can configure Kafka topics manually |

**Total: ~6-8 weeks for all phases.**

### Files Affected

| Phase | Key Files |
|-------|-----------|
| 0 | traits.rs, query_analyzer.rs, validator.rs |
| 1 | ast.rs, parser/select.rs, query_analyzer.rs, validator.rs |
| 2 | internal.rs, window_v2/adapter.rs, new keyed_table.rs |
| 3 | New dedup processor, parser extensions |
| 4 | types.rs (op field), writer.rs (tombstones), consumer |
| 5 | data_sink.rs, topic config |

### Who Needs This and When

| User Type | Phases Needed | Urgency |
|-----------|--------------|---------|
| Streaming analytics (current users) | None | Current PK routing is sufficient |
| Real-time dashboards | Phase 2 | When they need queryable materialized tables |
| CDC pipelines | Phase 4 | When ingesting changelog topics |
| Flink/ksqlDB migrants | All phases | Expect these semantics from prior experience |

### Recommendation

Park this work. FR-094 is a well-documented roadmap for when user demand justifies it. Current priorities should be:

1. Ship v1.0 with solid release pipeline (FR-093)
2. Core SQL engine reliability and performance
3. User adoption and feedback

**Trigger to start**: A user asks "can I maintain a latest-value-per-key table?" or "can I consume a changelog topic?" - that's when Phase 2 or Phase 4 becomes urgent.

---

## Key Files & Current Integration Points

| Component | File | Relevant Lines |
|-----------|------|---------------|
| AST key_fields | `src/velostream/sql/ast.rs` | 219 |
| PK parsing | `src/velostream/sql/parser/select.rs` | 1026-1104 |
| PK token types | `src/velostream/sql/parser/mod.rs` | 287-288 |
| Key extraction | `src/velostream/sql/query_analyzer.rs` | 255-315, 1279-1290 |
| DataSourceRequirement (no PK) | `src/velostream/sql/query_analyzer.rs` | 42-51 |
| DataSinkRequirement (has PK) | `src/velostream/sql/query_analyzer.rs` | 52-62 |
| GroupKey (reusable) | `src/velostream/sql/execution/internal.rs` | 17-81 |
| GroupByState | `src/velostream/sql/execution/internal.rs` | 83-147 |
| Kafka key construction | `src/velostream/sql/execution/window_v2/adapter.rs` | 1091-1104 |
| Writer key extraction | `src/velostream/datasource/kafka/writer.rs` | 818-852 |
| KafkaDataSink (PK storage) | `src/velostream/datasource/kafka/data_sink.rs` | 24-25, 46 |
| File sink (ignores PK) | `src/velostream/datasource/file/data_sink.rs` | 397-504 |
| Join key extraction | `src/velostream/sql/execution/join/key_extractor.rs` | 13-92 |
| supports_upsert trait (unused) | `src/velostream/datasource/traits.rs` | 101 |
| Kafka upsert=false | `src/velostream/datasource/kafka/data_sink.rs` | 702-704 |
| Table model (compact) | `src/velostream/sql/validator.rs` | 382-454 |
| StreamRecord.key (existing) | `src/velostream/sql/execution/types.rs` | 1674 |
| StreamRecord.from_kafka (existing) | `src/velostream/sql/execution/types.rs` | 1694-1707 |
| Kafka consumer key extraction | `src/velostream/kafka/kafka_fast_consumer.rs` | 228-257 |
| Sink creation (PK passthrough) | `src/velostream/server/processors/common.rs` | 1215-1238 |

---

## Relationship to FR-089

FR-089 implemented the **transport layer** for primary keys:
- Parser syntax for `PRIMARY KEY` annotation
- Kafka message key serialization

FR-094 builds on FR-089 to add the **semantic layer**:
- Connector-level PK validation and changelog modes
- DDL-level key definitions (table-level constraint syntax)
- NOT ENFORCED keyword (Flink-compatible)
- Keyed state management (upsert instead of append)
- Deduplication by key
- Changelog stream semantics
- Compacted topic integration

FR-089's infrastructure (parser tokens, AST field, key extraction) is reused and extended, not replaced.

---

## Comparison with Industry

### Feature Matrix

| Feature | Velostream (Current) | Velostream (FR-094) | Flink SQL | ksqlDB | RisingWave |
|---------|---------------------|---------------------|-----------|--------|------------|
| SELECT PK annotation | Yes (FR-089) | Yes | Yes | N/A | Yes |
| DDL-level PK | No | Phase 1 | Yes | Yes | Yes |
| NOT ENFORCED keyword | No | Phase 1 | Required | N/A | No |
| Composite PK | Yes (pipe-delimited) | Yes (structured) | Yes | Yes | Yes |
| Connector changelog modes | No | Phase 0 | Yes (INSERT_ONLY/RETRACT/UPSERT) | Implicit | Yes |
| Per-record change op | No | Phase 4 | Yes (RowKind) | Yes | Yes |
| Plan-time PK validation | No | Phase 0 | Yes (planner rejects mismatches) | Yes | Yes |
| File sink PK warning | No (silent ignore) | Phase 0 | Yes (errors on updates) | N/A | N/A |
| Uniqueness enforcement | No | Phase 2 | Yes | Yes | Yes |
| Upsert semantics | No | Phase 2 | Yes | Yes | Yes |
| Deduplication | No | Phase 3 | Yes | Yes | Yes |
| Changelog streams | No | Phase 4 | Yes | Yes | Yes |
| Compacted topics | No | Phase 5 | Yes | Yes | N/A |

### Connector-Level PK Behavior Comparison

| Connector | Flink Behavior | Velostream (Current) | Velostream (FR-094) |
|-----------|---------------|---------------------|---------------------|
| **kafka** (regular) | INSERT_ONLY. PK optional, used as message key. | PK used as message key. No mode validation. | Same + changelog mode declared |
| **upsert-kafka** | PK REQUIRED. UPSERT mode. Null-value = tombstone. Auto-compaction. | No equivalent. | Phase 5: compacted topic support |
| **filesystem** | INSERT_ONLY ONLY. Errors if query produces updates/deletes. | Silently ignores PK. No warning. | Phase 0: warn on PK + file sink |
| **jdbc** | With PK: UPSERT (INSERT ON CONFLICT UPDATE). Without PK: INSERT_ONLY. | No JDBC connector. | Future consideration |

### Design Principle Alignment

| Principle | Flink | Velostream (Current) | Velostream (FR-094) |
|-----------|-------|---------------------|---------------------|
| Explicit changelog modes | Connectors declare INSERT_ONLY / RETRACT / UPSERT | No concept | Phase 0: ChangelogMode trait |
| Plan-time validation | Mismatch = planner error | No validation | Phase 0: validate in QueryAnalyzer |
| PK requirement enforcement | Upsert sinks REQUIRE PK | PK always optional | Phase 2+: PK required for upsert |
| Tombstone handling | Null-value = delete in UPSERT mode | No support | Phase 4: tombstone records |
| NOT ENFORCED semantics | Explicit keyword required | Implicit (undocumented) | Phase 1: optional keyword |

---

## References

- [FR-089: Compound Keys & Explicit Key Configuration](../FR-089-compound-keys/README.md) - Transport-level PK (predecessor)
- [Flink SQL PRIMARY KEY](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#primary-key)
- [Flink SQL ChangelogMode](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/#table-to-stream-conversion)
- [ksqlDB Key Requirements](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-table/)
- [RisingWave CREATE TABLE](https://docs.risingwave.com/docs/current/sql-create-table/#primary-key)
- [Materialize Sources](https://materialize.com/docs/sql/create-source/)
- [Kafka Log Compaction](https://kafka.apache.org/documentation/#compaction)
