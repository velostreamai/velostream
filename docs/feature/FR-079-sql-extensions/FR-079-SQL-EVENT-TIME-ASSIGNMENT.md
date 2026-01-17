# Velostream SQL-Based Event-Time Assignment Investigation

## Executive Summary

This investigation examines how Velostream currently handles system columns and event-time, and identifies the integration points for implementing SQL-based event-time assignment (e.g., `SELECT ... AS _event_time`).

**Key Finding**: Velostream already has the infrastructure to support SQL-based event-time assignment. The required changes are minimal and localized to 3-4 specific locations.

---

## 1. Current Event-Time Resolution

### StreamRecord Structure
**Location**: `src/velostream/sql/execution/types.rs:963-978`

```rust
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,           // User data
    pub timestamp: i64,                                // _timestamp (processing-time, ms)
    pub offset: i64,                                   // _offset (Kafka)
    pub partition: i32,                                // _partition (Kafka)
    pub headers: HashMap<String, String>,              // Kafka headers
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,  // ← EVENT-TIME
}
```

**Key Methods**:
- `get_event_time()` (line 1055-1068): Returns event_time if set, else converts timestamp
- `extract_event_time_from_field()` (line 1086-1109): Extracts event-time from a field in the record
- `with_event_time_fluent()` (line 1046-1048): Sets event-time for a record

### _EVENT_TIME System Column Support
**Location**: `src/velostream/sql/execution/expression/evaluator.rs:135-142`

```rust
system_columns::EVENT_TIME => {
    // Convert Option<DateTime<Utc>> to milliseconds since epoch
    if let Some(event_time) = record.event_time {
        FieldValue::Integer(event_time.timestamp_millis())
    } else {
        FieldValue::Null
    }
}
```

**Status**: ✅ Already fully implemented in 3 evaluator methods (Phase 1 of FR-080)

---

## 2. SELECT Statement Processing

### SELECT Field Processing
**Location**: `src/velostream/sql/execution/processors/select.rs:315-750`

The SelectProcessor handles different field types:

```rust
for field in fields {
    match field {
        SelectField::Wildcard => {
            result_fields.extend(joined_record.fields.clone());
        }
        SelectField::Column(name) => {
            // Check for system columns first
            let field_value = match name.to_uppercase().as_str() {
                system_columns::TIMESTAMP => Some(FieldValue::Integer(joined_record.timestamp)),
                system_columns::OFFSET => Some(FieldValue::Integer(joined_record.offset)),
                system_columns::PARTITION => Some(FieldValue::Integer(joined_record.partition as i64)),
                _ => joined_record.fields.get(name).cloned()
            };
            if let Some(value) = field_value {
                result_fields.insert(name.clone(), value);
            }
        }
        SelectField::AliasedColumn { column, alias } => {
            // Similar handling with alias assignment
        }
        SelectField::Expression { expr, alias } => {
            // Evaluate expression and insert with alias
            let value = ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context(
                expr,
                &joined_record,
                &alias_context,
                &subquery_executor,
                context,
            )?;
            let field_name = alias.as_ref().unwrap_or(&Self::get_expression_name(expr)).clone();
            result_fields.insert(field_name.clone(), value.clone());
        }
    }
}
```

### Output StreamRecord Creation
**Location**: `src/velostream/sql/execution/processors/select.rs:730-737`

```rust
let final_record = StreamRecord {
    fields: result_fields,           // ← User-selected fields
    timestamp: joined_record.timestamp,
    offset: joined_record.offset,
    partition: joined_record.partition,
    headers: joined_record.headers,
    event_time: None,                // ← **CRITICAL**: Always set to None!
};
```

**Critical Issue**: The `event_time` field is HARDCODED to `None` in the output record.

---

## 3. CREATE STREAM AS SELECT Flow

### Execution Path
**Location**: `src/velostream/sql/execution/engine.rs:516-540`

```rust
pub async fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    mut stream_record: StreamRecord,
) -> Result<(), SqlError> {
    // For windowed queries, try to extract event time from _timestamp field if present
    if let StreamingQuery::Select {
        window: Some(_), ..
    } = query
    {
        if let Some(ts_field) = stream_record.fields.get("_timestamp") {
            match ts_field {
                FieldValue::Integer(ts) => stream_record.timestamp = *ts,
                FieldValue::Float(ts) => stream_record.timestamp = *ts as i64,
                _ => {}
            }
        }
    }
    self.execute_internal(query, stream_record).await
}
```

**Important Note**: The engine already attempts to extract timing info from fields for windowed queries, but only updates the `timestamp` field, not the `event_time` field.

---

## 4. Key Integration Points for SQL-Based Event-Time Assignment

### Point 1: Alias Recognition in SelectProcessor
**Location**: `src/velostream/sql/execution/processors/select.rs:506-595`

**What to Do**: After field processing loop, check if any field was aliased as `_event_time`.

```rust
// PROPOSED: After processing all SELECT fields, check for _event_time alias
let mut extracted_event_time: Option<FieldValue> = None;
for field in fields {
    match field {
        SelectField::AliasedColumn { column, alias } if alias.to_uppercase() == "_EVENT_TIME" => {
            // Capture the aliased field value
            extracted_event_time = result_fields.get(alias).cloned();
        }
        SelectField::Expression { expr, alias } => {
            if let Some(alias_name) = alias {
                if alias_name.to_uppercase() == "_EVENT_TIME" {
                    extracted_event_time = result_fields.get(alias_name).cloned();
                }
            }
        }
        _ => {}
    }
}
```

### Point 2: Output Record Construction
**Location**: `src/velostream/sql/execution/processors/select.rs:730-737`

**What to Do**: Use the captured `_event_time` field to set the StreamRecord's event_time.

```rust
// PROPOSED: Convert captured FieldValue to DateTime and set in output record
let output_event_time = extracted_event_time.and_then(|val| {
    match val {
        FieldValue::Integer(ts_ms) => {
            // Convert milliseconds to DateTime<Utc>
            chrono::DateTime::from_timestamp(ts_ms / 1000, ((ts_ms % 1000) * 1_000_000) as u32)
        }
        FieldValue::Timestamp(naive_dt) => {
            // Convert NaiveDateTime to DateTime<Utc>
            Some(chrono::DateTime::from_naive_utc_and_offset(naive_dt, chrono::Utc))
        }
        _ => None
    }
});

let final_record = StreamRecord {
    fields: result_fields,
    timestamp: joined_record.timestamp,
    offset: joined_record.offset,
    partition: joined_record.partition,
    headers: joined_record.headers,
    event_time: output_event_time,  // ← Now set from SELECT expression
};
```

### Point 3: Alias Recognition Pattern (Already Used!)
**Status**: ✅ The pattern for recognizing aliases is already implemented in the codebase for SET_HEADER and REMOVE_HEADER mutations.

**Location**: `src/velostream/sql/execution/processors/select.rs:1750-1790` (Header mutations)

```rust
fn collect_header_mutations_from_fields(
    fields: &[SelectField],
    record: &StreamRecord,
    mutations: &mut Vec<HeaderMutation>,
) -> Result<(), SqlError> {
    for field in fields {
        match field {
            SelectField::Expression { expr, alias } => {
                Self::collect_header_mutations_from_expr(expr, record, mutations)?;
                // ← Pattern already proven for recognizing special aliases
            }
            _ => {}
        }
    }
    Ok(())
}
```

### Point 4: Alternative: Engine-Level Extraction
**Location**: `src/velostream/sql/execution/engine.rs:516-540`

**Alternative approach**: Extract event_time from result fields at engine level:

```rust
pub async fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    mut stream_record: StreamRecord,
) -> Result<(), SqlError> {
    // ... existing code ...
    
    // After SelectProcessor returns, extract _event_time if present
    if let Some(result) = &output_record {
        if let Some(event_time_field) = result.fields.get("_event_time") {
            match event_time_field {
                FieldValue::Integer(ts_ms) => {
                    // Convert and set
                    if let Some(dt) = chrono::DateTime::from_timestamp(
                        ts_ms / 1000,
                        ((ts_ms % 1000) * 1_000_000) as u32
                    ) {
                        output_record.event_time = Some(dt);
                    }
                }
                _ => {}
            }
        }
    }
}
```

---

## 5. System Column Infrastructure

### System Columns Module
**Location**: `src/velostream/sql/execution/types.rs:54-126`

```rust
pub mod system_columns {
    pub const TIMESTAMP: &str = "_TIMESTAMP";
    pub const OFFSET: &str = "_OFFSET";
    pub const PARTITION: &str = "_PARTITION";
    pub const EVENT_TIME: &str = "_EVENT_TIME";
    pub const WINDOW_START: &str = "_WINDOW_START";
    pub const WINDOW_END: &str = "_WINDOW_END";
    
    pub fn normalize_if_system_column(name: &str) -> Option<&'static str> {
        // UPPERCASE normalization strategy for O(1) performance
    }
    
    pub fn is_system_column_upper(name_upper: &str) -> bool {
        // Fast O(1) lookup
    }
}
```

**Key Insight**: System columns use UPPERCASE normalization strategy - case-insensitive matching with O(1) performance.

### Expression Evaluator Methods (All 3 Support System Columns)
1. `evaluate_expression()` - For WHERE clause evaluation (line 124)
2. `evaluate_expression_value()` - For general expressions (line 377)
3. `evaluate_expression_value_with_subqueries()` - For SELECT expressions (line 1330)

---

## 6. Code Flow Diagram

```
StreamExecutionEngine::execute_with_record(query, record)
    ↓
execute_internal(query, record)
    ↓
apply_query(query, record) [for non-windowed queries]
    ↓
QueryProcessor::process_query(query, record, context)
    ↓
SelectProcessor::process(query, record, context)
    ↓
[Process each SELECT field]
    ├─ SelectField::Wildcard → extend result_fields
    ├─ SelectField::Column → lookup in fields or system columns
    ├─ SelectField::AliasedColumn → lookup + insert with alias
    └─ SelectField::Expression → evaluate + insert with alias
    ↓
[INTEGRATION POINT 1: Check for _event_time alias]
    ├─ If found, capture the FieldValue
    ├─ Suggested: Create intermediate variable `extracted_event_time`
    └─ Location: After field processing loop (line 595)
    ↓
[Create output StreamRecord]
    [INTEGRATION POINT 2: Set event_time field]
    ├─ Convert FieldValue to DateTime<Utc>
    ├─ Handle Integer (ms since epoch)
    ├─ Handle Timestamp (NaiveDateTime)
    ├─ Handle Null/other (set to None)
    └─ Location: When creating final_record (line 730-737)
    ↓
Return ProcessorResult with output record
    ↓
Engine receives result and sends to output channel
```

---

## 7. Detailed Code Locations

### Files That Need Modification

| File | Line(s) | Purpose | Change Type |
|------|---------|---------|-------------|
| `select.rs` | 506-595 | Field processing loop | Add: Alias recognition for `_event_time` |
| `select.rs` | 730-737 | Output record creation | Modify: Set `event_time` from captured value |
| `select.rs` | 1750-1790 | Mutation collection pattern | Reference: Already shows how to recognize special aliases |
| `types.rs` | 1086-1109 | StreamRecord methods | Reference: `extract_event_time_from_field()` shows conversion logic |

### Key Methods to Leverage

```rust
// In StreamRecord (types.rs:1086-1109):
pub fn extract_event_time_from_field(
    &mut self,
    field_name: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    // REUSABLE LOGIC for FieldValue → DateTime conversion
    match self.fields.get(field_name) {
        Some(FieldValue::Integer(timestamp_ms)) => {
            let datetime = chrono::DateTime::from_timestamp(
                *timestamp_ms / 1000,
                ((*timestamp_ms % 1000) * 1_000_000) as u32,
            );
            if let Some(dt) = datetime {
                self.event_time = Some(dt);
                Some(dt)
            } else {
                None
            }
        }
        Some(FieldValue::Timestamp(naive_dt)) => {
            let dt = chrono::DateTime::from_naive_utc_and_offset(*naive_dt, chrono::Utc);
            self.event_time = Some(dt);
            Some(dt)
        }
        _ => None
    }
}
```

---

## 8. Implementation Strategy

### Approach 1: SelectProcessor-Level (Recommended)
**Pros**:
- Localized to SelectProcessor (single source of truth)
- Can recognize alias patterns already used for SET_HEADER
- Applies to all SELECT operations (including CSAS)

**Cons**:
- Slightly more code in SelectProcessor

**Steps**:
1. Add variable to track `_event_time` alias during field loop
2. After loop, convert captured FieldValue to DateTime
3. Set in output StreamRecord

### Approach 2: Engine-Level
**Pros**:
- Separation of concerns (engine handles post-processing)
- Works for any SELECT output

**Cons**:
- Less direct (events need post-processing)
- Applies extraction logic outside processor

**Steps**:
1. Enhanced execute_with_record() to check output fields
2. Extract and convert `_event_time` if present
3. Remove from fields or keep as metadata

### Approach 3: StreamRecord Post-Processing
**Pros**:
- Can be applied as utility function

**Cons**:
- Requires parameter passing through engine

**Steps**:
1. Create `apply_event_time_assignment()` method in StreamRecord
2. Call after SELECT processing
3. Handles conversion logic

---

## 9. Handling Windowed Queries

### Current Behavior
When queries have a WINDOW clause, the engine attempts to extract `_timestamp` from fields:
```rust
if let StreamingQuery::Select { window: Some(_), .. } = query {
    if let Some(ts_field) = stream_record.fields.get("_timestamp") {
        stream_record.timestamp = *ts_field;
    }
}
```

### With Event-Time Assignment
The new system would allow:
```sql
CREATE STREAM output_stream AS
SELECT
    _event_time,  -- Already extracted from input records or calculated
    order_id,
    amount,
    COUNT(*) as order_count
FROM orders
WINDOW TUMBLING(1 HOUR)
GROUP BY order_id
EMIT FINAL;
```

The WindowProcessor already uses `get_event_time()` method, so it will automatically use the assigned event_time if set.

---

## 10. Test Coverage Locations

### Existing System Column Tests
**Location**: `tests/unit/sql/system/system_columns_test.rs`

Tests already cover:
- System column parsing (line 29)
- Aliasing (line 48)
- Execution (line 78)
- WHERE clause (line 165)
- Case insensitivity (line 278)

### New Tests Needed
```rust
#[tokio::test]
async fn test_event_time_assignment_from_field() {
    // SELECT timestamp_field AS _event_time FROM ...
}

#[tokio::test]
async fn test_event_time_assignment_in_windowed_query() {
    // Verify windowed queries use assigned event-time
}

#[tokio::test]
async fn test_event_time_assignment_with_integer() {
    // SELECT 1609459200000 AS _event_time FROM ...
}

#[tokio::test]
async fn test_event_time_assignment_with_timestamp() {
    // SELECT NOW() AS _event_time FROM ...
}
```

---

## 11. Integration Points Summary

### Required Changes (3 locations in 1 file)

**File: `src/velostream/sql/execution/processors/select.rs`**

1. **Location 1** (After line 595):
   - Add: Recognize and capture `_event_time` alias during field processing
   - Code: ~15 lines
   - Risk: Low (isolated to alias recognition)

2. **Location 2** (Line 730-737):
   - Modify: Use captured value to set `output_record.event_time`
   - Code: ~10 lines
   - Risk: Low (only affects event_time field initialization)

3. **Location 3** (Reference):
   - Study: Lines 1750-1790 (header mutation pattern)
   - Purpose: Learn how to recognize special aliases
   - No code change needed

### Infrastructure Already in Place
- ✅ System column support (_EVENT_TIME)
- ✅ Expression evaluation for all field types
- ✅ FieldValue to DateTime conversion logic
- ✅ Alias recognition pattern (used for SET_HEADER)
- ✅ Test infrastructure

---

## 12. SQL Usage Examples

### Example 1: Extract from Existing Field
```sql
CREATE STREAM events_with_event_time AS
SELECT
    event_id,
    request_timestamp AS _event_time,  -- Column contains milliseconds
    amount,
    customer_id
FROM raw_events;
```

### Example 2: Use System Column Directly
```sql
SELECT
    order_id,
    _timestamp AS _event_time,  -- Use processing-time as event-time
    amount
FROM orders;
```

### Example 3: Windowed Query with Event-Time
```sql
CREATE STREAM sales_per_hour AS
SELECT
    _window_start,
    _window_end,
    product_id,
    COUNT(*) as sales,
    SUM(amount) as total
FROM orders
WINDOW TUMBLING(1 HOUR)
GROUP BY product_id
EMIT FINAL;
```
*(Note: This would use event_time if assigned)*

### Example 4: Expression-Based Assignment
```sql
SELECT
    order_id,
    CAST(created_at AS TIMESTAMP) AS _event_time,  -- Convert string to timestamp
    amount
FROM events;
```

---

## Conclusion

Velostream's architecture is well-positioned to support SQL-based event-time assignment. The key insight is that:

1. **System columns are fully supported** - `_event_time` can already be read and written
2. **Alias recognition already exists** - Pattern proven with SET_HEADER/REMOVE_HEADER
3. **Conversion logic exists** - StreamRecord has `extract_event_time_from_field()` method
4. **Integration is localized** - Changes required only in SelectProcessor (1 file, 2-3 locations)

The implementation requires approximately **25-30 lines of code** in a single file, with low risk and high value.

---

## Files Referenced in This Report

- `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/types.rs`
- `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/processors/select.rs`
- `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/expression/evaluator.rs`
- `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/engine.rs`
- `/Users/navery/RustroverProjects/velostream/tests/unit/sql/system/system_columns_test.rs`
- `/Users/navery/RustroverProjects/velostream/docs/feature/FR-080-system-cols.md`

