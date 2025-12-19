# FR-089: Compound Keys & Explicit Key Configuration

## Status: ✅ Complete

### Implementation Summary (2025-01)

1. **✅ GROUP BY keys now included in output** - Fixed key field exclusion from JSON payload that was breaking downstream GROUP BY operations
2. **✅ Consistent serialization** - JSON/Avro/Protobuf all now serialize only `record.fields` (no metadata injection)
3. **✅ Inline KEY syntax** - Added ksqlDB-style `KEY` annotation in SELECT: `SELECT symbol KEY, price FROM trades`
4. **✅ Hot-path optimization** - Removed debug statements from key extraction for low-latency performance
5. **✅ SQL Migration Complete** - All demo and example SQL files migrated to use inline KEY syntax
6. **✅ Source key available as `_key`** - Inbound Kafka message key accessible via `_key` pseudo-column on StreamRecord
7. **✅ Documentation** - KEY_CONFIGURATION.md guide created with comprehensive examples

### Migration Status

| Component | Status |
|-----------|--------|
| Parser support for KEY annotation | ✅ Complete |
| Key extraction in writer | ✅ Complete |
| Compound key serialization | ✅ Complete |
| demo/trading/*.sql | ✅ Migrated |
| demo/datasource-demo/*.sql | ✅ Migrated |
| demo/test_harness_examples/**/*.sql | ✅ Migrated |
| examples/*.sql | ✅ Migrated |
| Source key as `_key` pseudo-column | ✅ Implemented |

### Optional Future Enhancements

- [ ] `value.fields-include` option (like Flink) to control whether KEY fields appear in value payload
- [ ] JSON key parsing on consume for compound key field extraction

---

## Overview

Implement proper Kafka message key handling for GROUP BY queries with support for compound keys, explicit key configuration, and JSON key parsing on consume.

### Problem Statement

1. **~~GROUP BY keys not propagated~~**: ✅ FIXED - Key fields are now always included in JSON payload
2. **No compound key support**: `GROUP BY region, product` has no way to create a proper compound Kafka key
3. **Silent null keys**: Missing key configuration silently produces null keys (round-robin partitioning)
4. **No key field extraction on consume**: JSON keys from upstream cannot be easily queried

### Solution

- GROUP BY queries automatically generate JSON keys from GROUP BY columns
- **NEW: Inline KEY syntax** - `SELECT symbol KEY, price FROM trades` (ksqlDB-style)
- Explicit `key_field` / `key_fields` configuration for non-GROUP BY queries
- `key_field = 'null'` for explicit round-robin partitioning
- Error if no key configuration and no GROUP BY (prevent accidental null keys)
- Parse JSON keys on consume and extract fields for SQL access

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PRODUCER SIDE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SQL Query                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ SELECT symbol, COUNT(*) FROM trades GROUP BY symbol                 │   │
│  │ SELECT region, product, SUM(x) FROM t GROUP BY region, product      │   │
│  │ SELECT * FROM trades  -- no GROUP BY                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  adapter.rs: build_result_record                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ If GROUP BY:                                                        │   │
│  │   1. Inject individual columns: symbol, region, product, etc.       │   │
│  │   2. Inject _key with JSON: {"symbol":"AAPL"} or                    │   │
│  │                             {"region":"US","product":"Widget"}      │   │
│  │ Else:                                                               │   │
│  │   No _key injected                                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  writer.rs: extract_key                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Priority:                                                           │   │
│  │   1. key_field = "null"      → return None (explicit round-robin)   │   │
│  │   2. key_fields = "a,b"      → combine as JSON {"a":X,"b":Y}        │   │
│  │   3. key_field = "symbol"    → use record.fields["symbol"]          │   │
│  │   4. record.fields["_key"]   → use it (GROUP BY implicit key)       │   │
│  │   5. else                    → ERROR: key configuration required    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│                        Kafka Message                                        │
│                   key: {"symbol":"AAPL"}                                   │
│                   value: {"symbol":"AAPL","count":42}                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

                              │
                              ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                              CONSUMER SIDE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                        Kafka Message                                        │
│                   key: {"symbol":"AAPL"}                                   │
│                   value: {"count":42}                                       │
│                              │                                              │
│                              ▼                                              │
│  reader.rs: create StreamRecord                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 1. Store raw key in record.fields["_key"]                           │   │
│  │    _key = "{\"symbol\":\"AAPL\"}"                                   │   │
│  │                                                                      │   │
│  │ 2. Try parse key as JSON object                                     │   │
│  │    If valid object → extract fields:                                │   │
│  │      record.fields["symbol"] = "AAPL"                               │   │
│  │      record.fields["region"] = "US"                                 │   │
│  │      record.fields["product"] = "Widget"                            │   │
│  │                                                                      │   │
│  │ 3. If not JSON object (plain string "AAPL"):                        │   │
│  │    Just store in _key, no field extraction                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  StreamRecord available for SQL                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ fields: {                                                           │   │
│  │   "_key": "{\"symbol\":\"AAPL\"}",  // raw key always               │   │
│  │   "symbol": "AAPL",                  // extracted from JSON key     │   │
│  │   "count": 42                        // from value                  │   │
│  │ }                                                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  SQL can access:                                                           │
│    WHERE symbol = 'AAPL'            -- extracted field                     │
│    WHERE _key = '{"symbol":"AAPL"}' -- raw key match                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration Examples

```sql
-- GROUP BY: key is implicit, no config needed
CREATE STREAM agg AS
SELECT symbol, COUNT(*) FROM trades
GROUP BY symbol
WITH ('sink.topic' = 'output');
-- Key: {"symbol":"AAPL"} (automatic)

-- GROUP BY compound: key is implicit
CREATE STREAM agg AS
SELECT region, product, SUM(qty) FROM orders
GROUP BY region, product
WITH ('sink.topic' = 'output');
-- Key: {"region":"US","product":"Widget"} (automatic)

-- GROUP BY with override: explicit config wins
CREATE STREAM agg AS
SELECT symbol, COUNT(*) FROM trades
GROUP BY symbol
WITH ('sink.topic' = 'output', 'sink.key_field' = 'null');
-- Key: null (user override for round-robin)

-- No GROUP BY: must specify key
CREATE STREAM filtered AS
SELECT * FROM trades WHERE price > 100
WITH ('sink.topic' = 'output', 'sink.key_field' = 'symbol');
-- Key: "AAPL" (from symbol field)

-- No GROUP BY: explicit null
CREATE STREAM filtered AS
SELECT * FROM trades WHERE price > 100
WITH ('sink.topic' = 'output', 'sink.key_field' = 'null');
-- Key: null (intentional round-robin)

-- No GROUP BY: compound key via config
CREATE STREAM filtered AS
SELECT * FROM trades
WITH ('sink.topic' = 'output', 'sink.key_fields' = 'region,product');
-- Key: {"region":"US","product":"Widget"}

-- No GROUP BY, no key config: ERROR
CREATE STREAM filtered AS
SELECT * FROM trades WHERE price > 100
WITH ('sink.topic' = 'output');
-- ERROR: key_field or key_fields must be specified (use key_field='null' for round-robin)
```

---

## Key Format

| Scenario | Key Format |
|----------|------------|
| Single GROUP BY | `{"symbol":"AAPL"}` |
| Compound GROUP BY | `{"region":"US","product":"Widget"}` |
| Single key_field | `"AAPL"` (raw value) |
| Compound key_fields | `{"region":"US","product":"Widget"}` |
| Explicit null | `null` (no key bytes) |

**Design Decision**: GROUP BY always produces JSON object keys for consistency and structure preservation. Single `key_field` config uses raw value for backwards compatibility.

---

## Implementation Phases

### Phase 1: GROUP BY Key Injection (Producer)
**LoE: 2-3 hours**

**File**: `src/velostream/sql/execution/window_v2/adapter.rs`

**Changes**:
1. In `build_result_record`, when `group_by_info` is present:
   - Already injecting individual GROUP BY columns into `fields` ✓
   - Add: Serialize GROUP BY columns as JSON object
   - Add: Inject `_key` field with JSON string

**Code Location**: `build_result_record` function, after the GROUP BY field injection loop

```rust
// After injecting individual GROUP BY fields...
// Inject _key with JSON-serialized GROUP BY columns
if let Some((group_exprs, group_key)) = group_by_info {
    let key_values = group_key.values();
    let mut key_map = serde_json::Map::new();

    for (idx, expr) in group_exprs.iter().enumerate() {
        if idx < key_values.len() {
            let col_name = match expr {
                Expr::Column(name) => extract_column_name(name),
                _ => format!("key_{}", idx),
            };
            let json_value = field_value_to_json(&key_values[idx]);
            key_map.insert(col_name, json_value);
        }
    }

    let key_json = serde_json::Value::Object(key_map).to_string();
    result_fields.insert("_key".to_string(), FieldValue::String(key_json));
}
```

**Tests**:
- Single GROUP BY produces `_key` with JSON
- Compound GROUP BY produces `_key` with JSON object
- No GROUP BY produces no `_key`

---

### Phase 2: Writer Key Extraction Priority
**LoE: 3-4 hours**

**File**: `src/velostream/datasource/kafka/writer.rs`

**Changes**:
1. Add `key_fields: Option<Vec<String>>` to `KafkaDataWriter` struct
2. Update `extract_key` with new priority logic:

```rust
fn extract_key(&self, record: &StreamRecord) -> Result<Option<String>, KeyError> {
    // 1. Explicit null
    if self.key_field.as_deref() == Some("null") {
        return Ok(None);
    }

    // 2. Compound key_fields config
    if let Some(ref fields) = self.key_fields {
        return self.build_compound_key(record, fields);
    }

    // 3. Single key_field config
    if let Some(ref field) = self.key_field {
        return self.extract_single_key(record, field);
    }

    // 4. Implicit _key from GROUP BY
    if let Some(FieldValue::String(key)) = record.fields.get("_key") {
        return Ok(Some(key.clone()));
    }

    // 5. Error - no key configuration
    Err(KeyError::MissingKeyConfig(
        "key_field or key_fields must be specified (use key_field='null' for round-robin)"
    ))
}

fn build_compound_key(&self, record: &StreamRecord, fields: &[String]) -> Result<Option<String>, KeyError> {
    let mut key_map = serde_json::Map::new();
    for field in fields {
        if let Some(value) = record.fields.get(field) {
            key_map.insert(field.clone(), field_value_to_json(value));
        }
    }
    Ok(Some(serde_json::Value::Object(key_map).to_string()))
}
```

**File**: `src/velostream/datasource/kafka/data_sink.rs`

**Changes**:
1. Extract `key_fields` from properties (comma-separated)
2. Pass to writer

**Tests**:
- `key_field = 'null'` produces null key
- `key_fields = 'a,b'` produces JSON compound key
- `key_field = 'symbol'` extracts single field
- `_key` field used when no config
- Error when no key config and no `_key`

---

### Phase 3: Reader JSON Key Parsing (Consumer)
**LoE: 2-3 hours**

**File**: `src/velostream/datasource/kafka/reader.rs` (or where StreamRecord is created)

**Changes**:
1. After reading key bytes, store raw in `_key` field
2. Attempt JSON parse
3. If valid JSON object, extract fields

```rust
fn process_message_key(key_bytes: Option<&[u8]>, fields: &mut HashMap<String, FieldValue>) {
    let key_str = match key_bytes {
        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
        None => return,
    };

    // Always store raw key
    fields.insert("_key".to_string(), FieldValue::String(key_str.clone()));

    // Try to parse as JSON object and extract fields
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(&key_str) {
        for (k, v) in map {
            let field_value = json_to_field_value(&v);
            fields.insert(k, field_value);
        }
    }
    // If not JSON object, just keep _key (plain string key)
}
```

**Tests**:
- JSON key `{"symbol":"AAPL"}` extracts `symbol` field
- JSON compound key extracts all fields
- Plain string key `"AAPL"` only populates `_key`
- Null key produces no `_key` field

---

### Phase 4: Error Handling & Validation
**LoE: 1-2 hours**

**Changes**:
1. Add `KeyError` enum in writer module
2. Validate key configuration at job creation time
3. Clear error messages

```rust
#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("Key configuration required: {0}")]
    MissingKeyConfig(String),

    #[error("Key field '{0}' not found in record")]
    FieldNotFound(String),

    #[error("Cannot serialize key: {0}")]
    SerializationError(String),
}
```

**Validation at job creation**:
- If no GROUP BY and no `key_field`/`key_fields` → error with helpful message
- Suggest `key_field = 'null'` if round-robin is intended

---

### Phase 5: Documentation & Examples
**LoE: 2-3 hours**

**Files**:
- `docs/sql/KEY_CONFIGURATION.md` - Complete key configuration guide
- `docs/feature/FR-089-compound-keys/MIGRATION.md` - Migration guide for existing queries
- Update `CLAUDE.md` with key configuration rules

**Content**:
- Key configuration options
- GROUP BY implicit keys
- Compound key syntax
- JSON key format
- Consuming JSON keys
- Migration from old behavior

---

## Summary

| Phase | Description | LoE | Files |
|-------|-------------|-----|-------|
| 1 | GROUP BY key injection | 2-3h | adapter.rs |
| 2 | Writer key priority | 3-4h | writer.rs, data_sink.rs |
| 3 | Reader JSON parsing | 2-3h | reader.rs |
| 4 | Error handling | 1-2h | writer.rs, error types |
| 5 | Documentation | 2-3h | docs/ |
| **Total** | | **10-15h** | |

---

## Backwards Compatibility

### Breaking Changes

1. **No key config + no GROUP BY = error** (previously: silent null key)
   - **Migration**: Add `key_field = 'null'` to queries that intentionally use round-robin

2. **GROUP BY now produces `_key` field** (previously: no `_key`)
   - **Migration**: None needed, additive change

### Non-Breaking

- Existing `key_field` config continues to work
- Single `key_field` still produces raw value (not JSON)
- JSON key parsing is transparent to existing queries

---

## Testing Strategy

### Unit Tests
- `adapter.rs`: GROUP BY key injection
- `writer.rs`: Key extraction priority
- `reader.rs`: JSON key parsing

### Integration Tests
- End-to-end GROUP BY → Kafka → consume → query
- Compound key partitioning verification
- Error cases (missing config)

### Test Harness
- Add compound key examples to `demo/test_harness_examples/`
- Verify key extraction in debug output

---

## Open Questions

1. **Should `key_fields` order matter?** (Yes - JSON object field order affects string equality)
2. **Max compound key size?** (No limit, but warn if > 1KB?)
3. **Key field collision**: What if value has field with same name as key field? (Key wins? Value wins? Error?)

---

## References

- [Kafka Key Partitioning](https://kafka.apache.org/documentation/#design_loadbalancing)
- [ksqlDB Key Handling](https://docs.ksqldb.io/en/latest/concepts/keys/)
- FR-084: Test Harness (debug key output)
