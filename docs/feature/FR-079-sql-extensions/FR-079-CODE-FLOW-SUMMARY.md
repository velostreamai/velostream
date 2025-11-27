# Code Flow Summary: Event-Time Assignment in SELECT Processing

## Critical Discovery: The Hardcoded `event_time: None`

**Location**: `src/velostream/sql/execution/processors/select.rs:730-737`

```rust
let final_record = StreamRecord {
    fields: result_fields,
    timestamp: joined_record.timestamp,
    offset: joined_record.offset,
    partition: joined_record.partition,
    headers: joined_record.headers,
    event_time: None,  // ← **CRITICAL**: ALWAYS None, never assigned from SELECT!
};
```

This is where the change needs to happen.

---

## Current System: StreamRecord Structure

**File**: `src/velostream/sql/execution/types.rs:963-978`

```rust
#[derive(Debug, Clone, Default)]
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,                           // User data columns
    pub timestamp: i64,                                                // _timestamp
    pub offset: i64,                                                   // _offset
    pub partition: i32,                                                // _partition
    pub headers: HashMap<String, String>,                              // Kafka headers
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,  // ← Currently only set internally
}
```

### Key Methods Already Available

1. **get_event_time()** (lines 1055-1068):
   ```rust
   pub fn get_event_time(&self) -> chrono::DateTime<chrono::Utc> {
       match self.event_time {
           Some(event_time) => event_time,
           None => {
               // Convert processing-time timestamp to DateTime
               chrono::DateTime::from_timestamp(
                   self.timestamp / 1000,
                   ((self.timestamp % 1000) * 1_000_000) as u32,
               ).unwrap_or_else(chrono::Utc::now)
           }
       }
   }
   ```

2. **extract_event_time_from_field()** (lines 1086-1109):
   ```rust
   pub fn extract_event_time_from_field(
       &mut self,
       field_name: &str,
   ) -> Option<chrono::DateTime<chrono::Utc>> {
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

## Current System: System Column Support

**File**: `src/velostream/sql/execution/expression/evaluator.rs:135-142`

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

**Status**: ✅ Already fully supported - `_event_time` can be read and aliased

---

## Current System: Alias Recognition Pattern (Already Implemented!)

The pattern for recognizing special aliases is already proven in the codebase with SET_HEADER and REMOVE_HEADER.

**File**: `src/velostream/sql/execution/processors/select.rs:1750-1790`

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
            }
            _ => {}
        }
    }
    Ok(())
}

fn collect_header_mutations_from_expr(
    expr: &Expr,
    record: &StreamRecord,
    mutations: &mut Vec<HeaderMutation>,
) -> Result<(), SqlError> {
    match expr {
        Expr::Function { name, args } => {
            let name_upper = name.to_uppercase();
            match name_upper.as_str() {
                "SET_HEADER" => {
                    // Extract key and value, create HeaderMutation
                }
                "REMOVE_HEADER" => {
                    // Extract key, create HeaderMutation
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
}
```

**Key Insight**: We can use the same pattern to recognize `_event_time` aliases!

---

## SELECT Field Processing

**File**: `src/velostream/sql/execution/processors/select.rs:506-595`

```rust
let mut result_fields = HashMap::new();
let mut alias_context = SelectAliasContext::new();
let mut header_mutations = Vec::new();

for field in fields {
    match field {
        SelectField::Wildcard => {
            result_fields.extend(joined_record.fields.clone());
        }
        SelectField::Column(name) => {
            // Check for system columns first (case insensitive)
            let field_value = match name.to_uppercase().as_str() {
                system_columns::TIMESTAMP => {
                    Some(FieldValue::Integer(joined_record.timestamp))
                }
                system_columns::OFFSET => {
                    Some(FieldValue::Integer(joined_record.offset))
                }
                system_columns::PARTITION => {
                    Some(FieldValue::Integer(joined_record.partition as i64))
                }
                _ => {
                    // Support qualified names like "c.id" by stripping the alias prefix
                    if let Some(value) = joined_record.fields.get(name).cloned() {
                        Some(value)
                    } else if let Some(pos) = name.rfind('.') {
                        // Try with just the base column name (after the dot)
                        let base_name = &name[pos + 1..];
                        joined_record.fields.get(base_name).cloned()
                    } else {
                        None
                    }
                }
            };

            if let Some(value) = field_value {
                result_fields.insert(name.clone(), value);
            }
        }
        SelectField::AliasedColumn { column, alias } => {
            // Similar pattern to Column
            let field_value = match column.to_uppercase().as_str() {
                system_columns::TIMESTAMP => Some(FieldValue::Integer(joined_record.timestamp)),
                system_columns::OFFSET => Some(FieldValue::Integer(joined_record.offset)),
                system_columns::PARTITION => Some(FieldValue::Integer(joined_record.partition as i64)),
                _ => {
                    if let Some(value) = joined_record.fields.get(column).cloned() {
                        Some(value)
                    } else if let Some(pos) = column.rfind('.') {
                        let base_name = &column[pos + 1..];
                        joined_record.fields.get(base_name).cloned()
                    } else {
                        None
                    }
                }
            };

            if let Some(value) = field_value {
                result_fields.insert(alias.clone(), value.clone());
                alias_context.add_alias(alias.clone(), value);
            }
        }
        SelectField::Expression { expr, alias } => {
            // NEW: Use evaluator that supports BOTH alias context AND subqueries
            let subquery_executor = SelectProcessor;
            let value =
                ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context(
                    expr,
                    &joined_record,
                    &alias_context,
                    &subquery_executor,
                    context,
                )?;
            let field_name = alias
                .as_ref()
                .unwrap_or(&Self::get_expression_name(expr))
                .clone();
            result_fields.insert(field_name.clone(), value.clone());
            // NEW: Add this field's alias to context for next field
            if let Some(alias_name) = alias {
                alias_context.add_alias(alias_name.clone(), value);
            }
        }
    }
}

// ← INTEGRATION POINT 1: HERE IS WHERE WE ADD _event_time ALIAS RECOGNITION
// After the field loop completes, we need to check if any field was aliased as _event_time

// Collect header mutations from fields
Self::collect_header_mutations_from_fields(
    fields,
    &joined_record,
    &mut header_mutations,
)?;

// ← INTEGRATION POINT 2: HERE IS WHERE WE CREATE THE OUTPUT RECORD
// And we need to set event_time based on the captured value
let final_record = StreamRecord {
    fields: result_fields,
    timestamp: joined_record.timestamp,
    offset: joined_record.offset,
    partition: joined_record.partition,
    headers: joined_record.headers,
    event_time: None,  // ← Currently hardcoded, should use captured value
};
```

---

## Implementation: Proposed Code Changes

### Change 1: Add Alias Recognition (After line 595)

```rust
// Extract _event_time if assigned in SELECT
let mut extracted_event_time: Option<FieldValue> = None;
for field in fields {
    match field {
        SelectField::AliasedColumn { column, alias } if alias.to_uppercase() == "_EVENT_TIME" => {
            // Field aliased as _event_time, extract the value
            extracted_event_time = result_fields.get(alias).cloned();
        }
        SelectField::Expression { expr, alias } => {
            if let Some(alias_name) = alias {
                if alias_name.to_uppercase() == "_EVENT_TIME" {
                    // Expression result aliased as _event_time
                    extracted_event_time = result_fields.get(alias_name).cloned();
                }
            }
        }
        _ => {}
    }
}
```

### Change 2: Modify Output Record Creation (Line 730-737)

```rust
// Convert extracted event_time to DateTime<Utc>
let output_event_time = extracted_event_time.and_then(|val| {
    match val {
        FieldValue::Integer(ts_ms) => {
            // Convert milliseconds since epoch to DateTime
            chrono::DateTime::from_timestamp(
                ts_ms / 1000,
                ((ts_ms % 1000) * 1_000_000) as u32
            )
        }
        FieldValue::Timestamp(naive_dt) => {
            // Convert NaiveDateTime to DateTime<Utc>
            Some(chrono::DateTime::from_naive_utc_and_offset(naive_dt, chrono::Utc))
        }
        _ => None  // Other types not supported for event_time
    }
});

let final_record = StreamRecord {
    fields: result_fields,
    timestamp: joined_record.timestamp,
    offset: joined_record.offset,
    partition: joined_record.partition,
    headers: joined_record.headers,
    event_time: output_event_time,  // ← Now set from SELECT expression!
};
```

---

## Test Infrastructure Already Present

**File**: `tests/unit/sql/system/system_columns_test.rs`

The test file already covers:
- System column parsing
- Aliasing with system columns
- Execution with system columns
- WHERE clauses with system columns
- Case insensitivity

Additional tests would verify:
```rust
#[tokio::test]
async fn test_event_time_assignment_from_field() {
    // SELECT timestamp_field AS _event_time
}

#[tokio::test]
async fn test_event_time_assignment_in_windowed_query() {
    // Verify windowed queries use assigned event_time
}
```

---

## Execution Path Summary

```
StreamExecutionEngine::execute_with_record(query, record)
    ↓
execute_internal(query, record)
    ↓
apply_query(query, record)
    ↓
QueryProcessor::process_query(query, record, context)
    ↓
SelectProcessor::process(query, record, context)
    ↓
Loop through SELECT fields
    ├─ Wildcard → extend fields
    ├─ Column → lookup + insert
    ├─ AliasedColumn → lookup + insert with alias
    └─ Expression → evaluate + insert with alias
    ↓
[NEW] Check if any field was aliased as _event_time
    └─ Store the value in extracted_event_time
    ↓
[NEW] Convert extracted_event_time to DateTime<Utc>
    ↓
Create output StreamRecord with event_time set
    ↓
Return to engine, which sends to output channel
```

---

## Key Insight: Why This Works

1. **System columns are fully supported** - `_event_time` can be referenced in SELECT
2. **Alias recognition is proven** - SET_HEADER/REMOVE_HEADER already do this
3. **Conversion logic exists** - StreamRecord.extract_event_time_from_field() has the code
4. **Fields are accessible** - All values flow through result_fields HashMap
5. **WindowProcessor already uses it** - Calls get_event_time() which returns assigned value if set

The only missing piece is **connecting the alias to the event_time field** in the output record.

---

## Files to Modify

1. **Primary**: `src/velostream/sql/execution/processors/select.rs`
   - Lines 595-610: Add alias recognition
   - Lines 730-737: Use captured value for event_time

2. **Testing**: `tests/unit/sql/system/system_columns_test.rs`
   - Add: Event-time assignment tests
   - Add: Windowed query tests with assigned event-time

3. **Documentation**: `docs/feature/FR-079-SQL-EVENT-TIME-ASSIGNMENT.md`
   - Already exists (investigation report)

