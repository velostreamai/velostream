# FR-089: Compound Keys & Explicit Key Configuration

## Status: ✅ Complete

---

## Summary

This feature implements comprehensive Kafka message key handling with SQL-standard `PRIMARY KEY` annotation syntax, following conventions used by Flink SQL, RisingWave, and Materialize.

### Key Features

1. **Inline PRIMARY KEY syntax** - `SELECT symbol PRIMARY KEY, price FROM trades`
2. **Compound keys** - `SELECT region PRIMARY KEY, product PRIMARY KEY, ...`
3. **Alias support** - `SELECT col AS alias PRIMARY KEY` uses alias as key name
4. **GROUP BY implicit keys** - Auto-generated from GROUP BY columns
5. **Source key access** - `_key` pseudo-column for inbound Kafka message key
6. **Dual-write pattern** - Key fields appear in both Kafka key and value payload

---

## Syntax

### PRIMARY KEY Annotation

```sql
-- Single key field
SELECT symbol PRIMARY KEY, price, quantity FROM trades

-- Compound key (multiple fields)
SELECT region PRIMARY KEY, product PRIMARY KEY, SUM(qty) as total
FROM orders
GROUP BY region, product

-- PRIMARY KEY with alias (alias name becomes the key)
SELECT stock_symbol AS sym PRIMARY KEY, price FROM market_data

-- PRIMARY KEY with GROUP BY
SELECT symbol PRIMARY KEY, COUNT(*) as trade_count
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
```

### Key Resolution

The key field name is resolved from the SELECT output:
- If alias present: uses **alias name** (e.g., `col AS sym PRIMARY KEY` → key field is `sym`)
- If no alias: uses **column name** (e.g., `symbol PRIMARY KEY` → key field is `symbol`)

**Important**: The field must exist in the output record. The key is extracted from `record.fields[field_name]`.

---

## Key Configuration Priority

When multiple key sources are available, this priority applies:

| Priority | Method | Example | Result |
|----------|--------|---------|--------|
| 1 | **Inline PRIMARY KEY** | `SELECT symbol PRIMARY KEY, ...` | Uses PRIMARY KEY annotation |
| 2 | **GROUP BY implicit** | `GROUP BY symbol` | Auto-generates from GROUP BY columns |
| 3 | **No configuration** | - | Null key (round-robin) |

---

## Key Format Reference

| Configuration | Key Format | Example |
|--------------|------------|---------|
| Single PRIMARY KEY | Raw value | `"AAPL"` |
| Multiple PRIMARY KEY | Pipe-delimited | `"US\|Widget"` |
| Single GROUP BY | Raw value | `"AAPL"` |
| Multiple GROUP BY | Pipe-delimited | `"T1\|AAPL"` |
| No configuration | Null | `null` (round-robin partitioning) |

---

## Accessing Source Keys (`_key` Pseudo-Column)

The original Kafka message key from the source topic is accessible via `_key`:

```sql
-- Access the source key in SELECT
SELECT _key, symbol, price, volume
FROM market_data
WHERE _key IS NOT NULL;

-- Re-key based on source key combined with other fields
SELECT _key AS original_key, symbol PRIMARY KEY, price
FROM market_data;

-- Filter based on source key
SELECT * FROM market_data
WHERE _key = 'AAPL';
```

---

## Dual-Write Pattern

Following industry standard (ksqlDB, Flink), key fields are written to **both**:
- The Kafka message key (for partitioning)
- The value payload (for downstream SQL access)

This means the key field is always available for querying without special handling.

---

## Implementation Details

### Parser (`src/velostream/sql/parser.rs`)

- Added `TokenType::Primary` for `PRIMARY` keyword
- Added `TokenType::Key` for `KEY` keyword
- Parsing expects `PRIMARY KEY` as two consecutive tokens
- Case-insensitive: `PRIMARY KEY`, `primary key`, `Primary Key` all work

### AST (`src/velostream/sql/ast.rs`)

- `StreamingQuery::Select::key_fields: Option<Vec<String>>` stores marked field names
- Display implementation outputs `field PRIMARY KEY` format

### Key Extraction (`src/velostream/datasource/kafka/writer.rs`)

Priority-based extraction:
1. Check for inline PRIMARY KEY annotation in query
2. Fall back to GROUP BY implicit key
3. Null key if no configuration

---

## Demos & Examples

| Demo | Description |
|------|-------------|
| `demo/test_harness_examples/tier2_aggregations/15_compound_keys.sql` | Dedicated compound key demo with 3 scenarios |
| `demo/test_harness_examples/tier6_edge_cases/52_large_volume.sql` | Compound key with high-volume processing |

---

## Migration Status

| Component | Status |
|-----------|--------|
| Parser support for PRIMARY KEY annotation | ✅ Complete |
| Key extraction in writer | ✅ Complete |
| Compound key serialization (pipe-delimited) | ✅ Complete |
| demo/trading/*.sql | ✅ Migrated |
| demo/datasource-demo/*.sql | ✅ Migrated |
| demo/test_harness_examples/**/*.sql | ✅ Migrated |
| examples/*.sql | ✅ Migrated |
| Source key as `_key` pseudo-column | ✅ Implemented |
| Documentation | ✅ Complete |

---

## Tests

All PRIMARY KEY annotation tests pass (12 tests):

```
test_single_key_annotation
test_compound_key_annotation
test_key_annotation_with_alias
test_key_annotation_in_create_stream
test_no_key_annotation
test_key_annotation_with_expression_aliased
test_key_annotation_case_insensitive
test_key_annotation_with_group_by_and_window
test_key_annotation_display
test_key_annotation_order_preserved
test_key_annotation_qualified_column
test_key_after_as_alias
```

---

## Optional Future Enhancements

- [ ] `value.fields-include` option (like Flink) to control whether PRIMARY KEY fields appear in value payload
- [ ] Key field extraction on consume - parse pipe-delimited compound keys back into individual fields

---

## References

- [KEY_CONFIGURATION.md](../../sql/KEY_CONFIGURATION.md) - Complete key configuration guide
- [COPY_PASTE_EXAMPLES.md](../../sql/COPY_PASTE_EXAMPLES.md) - Working SQL examples
- [PARSER_GRAMMAR.md](../../sql/PARSER_GRAMMAR.md) - Formal grammar reference
- [Flink SQL PRIMARY KEY](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#primary-key)
- [RisingWave PRIMARY KEY](https://docs.risingwave.com/docs/current/sql-create-table/#primary-key)
