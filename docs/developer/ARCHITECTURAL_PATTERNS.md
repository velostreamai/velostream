# SELECT Processor Architecture: Current vs Phase 3

## Current Architecture (Pre-Phase 3)

```
SelectProcessor::process()
├── Window Detection
│   └── Route to WindowProcessor (no validation issues)
├── Limit Check
├── JOIN Processing (validation internal to JoinProcessor)
├── WHERE Evaluation
│   └── ExpressionEvaluator::evaluate_expression_with_subqueries()
│       └── No upfront field validation
│           └── Runtime errors bubble up
├── GROUP BY Routing
│   └── handle_group_by_record()
│       └── No entry-point validation
├── SELECT Field Processing
│   └── For each field:
│       ├── Process wildcards (OK - no fields referenced)
│       ├── Process columns (OK - direct lookup)
│       ├── Process expressions
│       │   └── ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context()
│       │       └── No upfront validation
│       └── Collect header mutations
├── HAVING Evaluation
│   └── ExpressionEvaluator::evaluate_expression_with_subqueries()
│       └── No upfront validation
│           └── Combined scope (original + computed fields)
├── Result Assembly
└── Return ProcessorResult

VALIDATION COVERAGE: Minimal (relies on expression evaluator)
ERROR TIMING: Late (deep in expression evaluation)
```

## Phase 3 Refactored Architecture

```
SelectProcessor::process()
├── Window Detection
│   └── Route to WindowProcessor ✓ (validation exists)
├── Limit Check
├── JOIN Processing
│   └── JoinProcessor::process_joins()
│       └── [TODO: Explicit validation if needed]
├── WHERE Clause
│   ├── ✓ Validate expressions against current record
│   │   └── FieldValidator::validate_expressions()
│   │       └── ValidationContext::WhereClause
│   └── ExpressionEvaluator::evaluate_expression_with_subqueries()
├── GROUP BY
│   ├── ✓ Validate expressions at entry point
│   │   └── FieldValidator::validate_expressions()
│   │       └── ValidationContext::GroupBy
│   └── handle_group_by_record()
├── SELECT Fields
│   └── For each SelectField:
│       ├── Wildcards (no validation needed)
│       ├── Columns (direct lookup, safe)
│       ├── Expressions
│       │   ├── ✓ Validate before evaluation
│       │   │   └── FieldValidator::validate_expressions()
│       │   │       └── ValidationContext::SelectClause
│       │   └── ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context()
│       └── Header mutations
├── HAVING Clause
│   ├── ✓ Combine field scopes (original + computed)
│   ├── ✓ Validate expressions against combined record
│   │   └── FieldValidator::validate_expressions()
│   │       └── ValidationContext::HavingClause
│   └── ExpressionEvaluator::evaluate_expression_with_subqueries()
├── ORDER BY (NEW)
│   ├── ✓ Validate expressions against final record
│   │   └── FieldValidator::validate_expressions()
│   │       └── ValidationContext::OrderByClause
│   └── [TODO: Implement sorting in Phase 4]
├── Result Assembly
└── Return ProcessorResult

VALIDATION COVERAGE: Comprehensive (all clauses validated)
ERROR TIMING: Early (at validation entry points)
CONSISTENCY: Follows Phase 2 pattern (window processor)
```

## Validation Gate Pattern

Every SQL clause processing follows this pattern:

```rust
// Step 1: Prepare record to validate against
let record_to_validate = &joined_record; // or combined record for HAVING

// Step 2: Extract expressions to validate
let expressions = &[expr.clone()]; // or &[expr1, expr2, ...]

// Step 3: Call FieldValidator
FieldValidator::validate_expressions(
    record_to_validate,
    expressions,
    ValidationContext::SomeClause, // WhereClause, SelectClause, etc.
)
.map_err(|e| e.to_sql_error())?; // Convert to SqlError

// Step 4: Proceed with evaluation (now guaranteed field exists)
let result = ExpressionEvaluator::evaluate_expression_with_subqueries(
    expr,
    record_to_validate,
    &subquery_executor,
    context,
)?;
```

## Field Scope Progression Through Query Pipeline

```
Input Record: {id, symbol, price, quantity, timestamp}

After WHERE: {id, symbol, price, quantity, timestamp}
            (same as input, WHERE just filters)

After SELECT: {id, symbol, price_doubled}
             (potential field projection/aliasing)

HAVING Scope: {id, symbol, price, quantity, timestamp, id, symbol, price_doubled}
             (union of original + result fields for aggregate access)

After ORDER BY: Same as SELECT result
               (sorting doesn't change fields)

Output Record: {id, symbol, price_doubled} ✓
```

## Key Design Decision: Combined Scope for HAVING

Since HAVING evaluates post-SELECT:
- Must validate against original fields (for correlated subqueries)
- Must validate against computed fields (aliases, aggregate results)

Example:
```sql
SELECT id, COUNT(*) as cnt, SUM(price) as total
FROM trades
GROUP BY id
HAVING cnt > 10 AND total > 1000.0
```

HAVING validation scope must include:
- Original fields: id, COUNT(*), SUM(price)
- Computed aliases: cnt, total
- Special aggregates: COUNT(*), SUM(price)

So HAVING record validation includes:
```rust
let mut having_fields = joined_record.fields.clone();  // {id, ...}
having_fields.extend(result_fields.clone());           // Add {cnt, total, ...}
let having_record = StreamRecord { fields: having_fields, ... };
```

## Error Flow Comparison

### Pre-Phase 3: Runtime Error
```
User Query: SELECT price_doubled FROM trades WHERE unknown_field > 100

Flow:
1. SelectProcessor::process() accepts query
2. WHERE clause evaluation begins
3. ExpressionEvaluator::evaluate_expression_with_subqueries()
4. Deep recursion through expression tree
5. Column lookup fails: unknown_field not in record
6. Runtime error returned
7. Error message context lost (doesn't mention WHERE)

Error Message: "Column 'unknown_field' not found"
Problem: Unclear which clause failed
```

### Phase 3: Early Validation Error
```
User Query: SELECT price_doubled FROM trades WHERE unknown_field > 100

Flow:
1. SelectProcessor::process() receives query
2. WHERE clause: FieldValidator::validate_expressions()
3. Extracts field names: {unknown_field}
4. Validation failure detected IMMEDIATELY
5. FieldValidationError::FieldNotFound created with context
6. Converted to SqlError via to_sql_error()
7. Returned to caller

Error Message: "Field 'unknown_field' not found in record during WHERE clause"
Benefit: Clear context, fail-fast, no wasted evaluation
```

## Validation Context Responsibility Matrix

```
Context          | Validates Against        | Clause Location | Phase
---|---|---|---
GroupBy         | Input record             | GROUP BY        | Phase 2 ✓
PartitionBy     | Windowed input record    | PARTITION BY    | Phase 2 ✓
WhereClause     | Input record             | WHERE           | Phase 3 (NEW)
SelectClause    | Input record             | SELECT exprs    | Phase 3 (NEW)
HavingClause    | Original + computed      | HAVING          | Phase 3 (NEW)
OrderByClause   | Result record            | ORDER BY        | Phase 3 (NEW)
JoinCondition   | Joined record            | JOIN ON         | Phase 3 (optional)
Aggregation     | Record in group          | GROUP BY/aggs   | Phase 5 (future)
WindowFrame     | Window boundaries        | WINDOW RANGE    | Phase 5 (future)
```

## Implementation Dependencies

```
Phase 3 Deliverable:
├── ValidationContext enum expansion (5 variants)
├── SelectProcessor::process() modifications (5 validation gates)
└── Test coverage (8+ test cases)

Depends On:
├── FieldValidator (Phase 2) ✓ exists
└── StreamingQuery AST (all clauses present) ✓

Enables:
├── Phase 4: ORDER BY sorting (validation foundation)
└── Phase 5: Type validation (field existence first)

NOT Blocking:
├── Window processor (independent, Phase 2)
├── Aggregation (GROUP BY works, validation improves it)
└── Other processors
```

## Code Location Reference

**Validation Logic**:
- Definition: `src/velostream/sql/execution/validation/field_validator.rs` (lines 1-320)
- Implementation: Window usage (lines 678-683 of window.rs)

**Target for Changes**:
- SelectProcessor: `src/velostream/sql/execution/processors/select.rs`
  - WHERE: lines 393-418
  - GROUP BY: lines 422-465
  - SELECT: lines 468-531
  - HAVING: lines 534-563
  - ORDER BY: NEW SECTION needed

**Tests to Create**:
- `tests/unit/sql/execution/processors/select_validation_test.rs` (NEW)
- Register in: `tests/unit/sql/execution/processors/mod.rs`

---

## Consumer Group Coordination Pattern

### Problem Statement

When running multiple JobServer instances with the same SQL application, we need:
1. Automatic consumer group naming without explicit configuration
2. Transparent partition distribution via Kafka's built-in rebalancing
3. No coordination overhead between servers
4. Backward compatibility with existing deployments

### Solution Architecture

```
┌──────────────────────────────────────────────────┐
│         SQL Application (trading.sql)            │
│                                                  │
│  @application trading_platform                  │
│  @phase production                               │
│                                                  │
│  CREATE STREAM orders AS SELECT ...;             │
│  CREATE STREAM payments AS SELECT ...;           │
└──────────┬───────────────────────────────────────┘
           │
           ├─────────────────────────────────────┐
           │                                     │
           ▼                                     ▼
    ┌─────────────┐                      ┌─────────────┐
    │ SqlApplication                     │ SqlApplication
    │ metadata:                          │ metadata:
    │ application:                       │ application:
    │  "trading_platform"                │  "trading_platform"
    └──────┬──────┘                      └──────┬──────┘
           │                                     │
           ▼                                     ▼
    ┌─────────────────────┐          ┌─────────────────────┐
    │  deploy_job()       │          │  deploy_job()       │
    │  app_name:          │          │  app_name:          │
    │   "trading_platform"│          │   "trading_platform"│
    └──────┬──────────────┘          └──────┬──────────────┘
           │                                 │
           ▼                                 ▼
    ┌────────────────────────┐      ┌────────────────────────┐
    │ create_multi_source_   │      │ create_multi_source_   │
    │ readers()              │      │ readers()              │
    │ app_name: Some("..") │      │ app_name: Some("..") │
    └──────┬─────────────────┘      └──────┬─────────────────┘
           │                                 │
           ▼                                 ▼
    ┌────────────────────────┐      ┌────────────────────────┐
    │ KafkaDataSource        │      │ KafkaDataSource        │
    │ ::from_properties()    │      │ ::from_properties()    │
    │                        │      │                        │
    │ 3-tier fallback:       │      │ 3-tier fallback:       │
    │ 1. Explicit config     │      │ 1. Explicit config     │
    │ 2. App-aware:          │      │ 2. App-aware:          │
    │    velo-trading_..     │      │    velo-trading_..     │
    │ 3. Legacy:             │      │ 3. Legacy:             │
    │    velo-sql-orders     │      │    velo-sql-orders     │
    └──────┬─────────────────┘      └──────┬─────────────────┘
           │                                 │
           └────────────────┬────────────────┘
                            ▼
                    ┌────────────────┐
                    │ Kafka Broker   │
                    │                │
                    │ Consumer Groups│
                    │ - velo-trading │
                    │   _platform-   │
                    │   orders       │
                    │                │
                    │ Partitions:    │
                    │ - P0→Server1   │
                    │ - P1→Server2   │
                    │ - P2→Server3   │
                    │ - P3→Server1   │
                    │ (auto-balanced)│
                    └────────────────┘
```

### Implementation Flow

```
1. User specifies @application annotation
   └─ Enables multi-server coordination

2. Deploy on Multiple Servers
   └─ Each server loads same SQL file

3. SqlApplication metadata extracted
   ├─ app.metadata.application = "trading_platform"
   └─ Available in deploy_sql_application_with_filename()

4. Pass app_name through call chain
   ├─ deploy_job(job_name, version, query, topic, app_name)
   │  └─ create_multi_source_readers(sources, job_name, app_name, ...)
   │     └─ KafkaDataSource::from_properties(..., app_name)

5. Consumer Group Naming (3-tier fallback)
   ├─ Tier 1: Explicit consumer.group.id in YAML
   │  └─ If present, use as-is
   ├─ Tier 2: App-aware naming
   │  └─ If app_name provided: velo-{app_name}-{job_name}
   └─ Tier 3: Legacy fallback
      └─ If no app_name: velo-sql-{job_name}

6. Kafka Automatic Partition Distribution
   ├─ Consumers join same group
   ├─ Kafka detects member count change
   ├─ Rebalancing assigns partitions
   └─ Each member gets equal share (or as close as possible)

7. Transparent Coordination
   └─ No explicit server-to-server communication needed
      (Kafka handles it all via consumer group protocol)
```

### Code Locations

**SQL Application Metadata:**
- `src/velostream/sql/app_parser.rs` - Metadata parsing
- `SqlApplication.metadata.application` - Application name field

**Job Deployment:**
- `src/velostream/server/stream_job_server.rs:1127` - `deploy_sql_application_with_filename()`
- Extracts: `app.metadata.application`
- Passes to: `deploy_job(..., app_name)`

**Job Processing:**
- `src/velostream/server/stream_job_server.rs:506` - `deploy_job()` signature
- Added parameter: `app_name: Option<String>`
- Passes to: `create_multi_source_readers(..., app_name)`

**DataSource Creation:**
- `src/velostream/server/processors/common.rs:956` - `create_multi_source_readers()`
- Added parameter: `app_name: Option<&str>`
- Passes to: `KafkaDataSource::from_properties(..., app_name)`

**Consumer Group Naming:**
- `src/velostream/datasource/kafka/data_source.rs:93` - `from_properties()`
- Added parameter: `app_name: Option<&str>`
- Logic (lines 73-128):
  ```rust
  let group_id = get_source_prop("group_id")
      .or_else(|| {
          app_name.map(|app| format!("velo-{}-{}", app, job_name))
      })
      .unwrap_or_else(|| {
          format!("velo-sql-{}", job_name)
      });
  ```

### Backward Compatibility

**Without @application annotation:**
```sql
CREATE STREAM events AS SELECT * FROM kafka;
-- Consumer group: velo-sql-events (legacy)
```

**With explicit consumer.group.id:**
```sql
-- With YAML config:
consumer.group.id: custom_group_id
-- Consumer group: custom_group_id (explicit takes priority)
```

**With @application annotation:**
```sql
@application myapp
CREATE STREAM events AS SELECT * FROM kafka;
-- Consumer group: velo-myapp-events (new feature)
```

### Benefits

✅ **Zero-configuration multi-server coordination**
- Just add @application annotation
- Kafka handles everything else

✅ **Transparent partition distribution**
- Kafka's built-in rebalancing
- No custom coordination code

✅ **Backward compatible**
- Existing deployments work unchanged
- Optional feature (requires @application)

✅ **Observable**
- Clear consumer group names show app/job relationship
- Easy to monitor with standard Kafka tools

✅ **Scalable**
- Add/remove servers dynamically
- Automatic partition redistribution

### Testing Strategy

**Unit Tests:**
- Consumer group naming logic (3-tier fallback)
- Option handling and edge cases

**Integration Tests:**
- Multiple servers with same app_name
- Partition distribution verification
- Server join/leave scenarios

**End-to-End Tests:**
- Deploy across 3 servers
- Verify balanced partition assignment
- Monitor lag and throughput

