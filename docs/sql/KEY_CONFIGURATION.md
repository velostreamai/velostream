# Kafka Message Key Configuration Guide

This guide covers all methods for configuring Kafka message keys in Velostream streaming queries.

## Overview

Kafka message keys determine how messages are partitioned. Proper key configuration ensures:
- **Co-located data**: Records with the same key go to the same partition
- **Ordering guarantees**: Messages with the same key are ordered within a partition
- **Efficient processing**: Downstream consumers can process related records together

## Key Configuration Methods

Velostream supports two ways to configure Kafka message keys:

| Method | Syntax | Use Case |
|--------|--------|----------|
| **Inline PRIMARY KEY** | `SELECT symbol PRIMARY KEY, ...` | Explicit key declaration in SQL (recommended) |
| **GROUP BY implicit key** | `GROUP BY symbol` | Auto-generated from GROUP BY columns |

---

## Method 1: Inline PRIMARY KEY Annotation (Recommended)

The `PRIMARY KEY` keywords mark a field as the Kafka message key directly in SQL. This follows SQL standard syntax used by Flink, RisingWave, and Materialize.

### Single Key

```sql
CREATE STREAM keyed_trades AS
SELECT symbol PRIMARY KEY, price, quantity, event_time
FROM trades
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'keyed_trades.type' = 'kafka_sink',
    'keyed_trades.topic' = 'keyed_trades_output',
    'keyed_trades.format' = 'json'
);
```

**Result**: Kafka key = `"AAPL"` (raw value)

### Compound Key (Multiple Fields)

```sql
CREATE STREAM region_product_stream AS
SELECT region PRIMARY KEY, product PRIMARY KEY, quantity, revenue
FROM orders
WITH (...);
```

**Result**: Kafka key = `"US|Widget"` (pipe-delimited)

### PRIMARY KEY with Alias

```sql
CREATE STREAM aliased_key AS
SELECT stock_symbol AS sym PRIMARY KEY, price
FROM market_data
WITH (...);
```

**Result**: Key field name is `sym` (the alias), not `stock_symbol`

---

## Method 2: GROUP BY Implicit Key

When using `GROUP BY`, Velostream automatically generates a Kafka key from the GROUP BY columns.

### Single GROUP BY Column

```sql
CREATE TABLE symbol_stats AS
SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (...);
```

**Result**: Kafka key = `"AAPL"` (raw value from GROUP BY)

### Multiple GROUP BY Columns

```sql
CREATE TABLE trader_symbol_stats AS
SELECT trader_id, symbol, COUNT(*) as cnt
FROM trades
GROUP BY trader_id, symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (...);
```

**Result**: Kafka key = `"T1|AAPL"` (pipe-delimited compound key)

### Combining PRIMARY KEY with GROUP BY

You can use PRIMARY KEY annotation alongside GROUP BY for explicit control:

```sql
CREATE TABLE explicit_key_agg AS
SELECT symbol PRIMARY KEY, trader_id, COUNT(*) as cnt
FROM trades
GROUP BY symbol, trader_id
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (...);
```

**Result**: Uses PRIMARY KEY annotation, not auto-generated GROUP BY key

---

## Key Configuration Priority

When multiple methods are used, this priority order applies:

1. **Inline PRIMARY KEY** → `SELECT symbol PRIMARY KEY, ...` (recommended)
2. **GROUP BY implicit** → Auto-generated from GROUP BY columns
3. **No configuration** → Null key (round-robin partitioning)

### Priority Example

```sql
-- PRIMARY KEY annotation takes precedence over GROUP BY implicit key
SELECT symbol PRIMARY KEY, trader_id, COUNT(*) as cnt
FROM trades
GROUP BY symbol, trader_id
-- Key will be "symbol" from PRIMARY KEY annotation, not {"symbol":"...", "trader_id":"..."}
```

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

## Best Practices

### 1. Use PRIMARY KEY for Clarity

```sql
-- Explicit and self-documenting (SQL standard)
SELECT customer_id PRIMARY KEY, order_id, amount FROM orders
```

### 2. Match PRIMARY KEY with GROUP BY

When aggregating, use PRIMARY KEY on the same columns as GROUP BY:

```sql
SELECT symbol PRIMARY KEY, COUNT(*) as cnt
FROM trades
GROUP BY symbol
```

### 3. Use Compound Keys for Multi-Dimension Partitioning

```sql
SELECT region PRIMARY KEY, product_type PRIMARY KEY, SUM(sales) as total
FROM sales
GROUP BY region, product_type
```

### 4. Round-Robin Distribution for Fan-Out

When you want round-robin distribution, simply don't specify PRIMARY KEY or GROUP BY:

```sql
-- No PRIMARY KEY = null key = round-robin partitioning
CREATE STREAM fanout_stream AS
SELECT event_id, payload, timestamp
FROM events
WITH (...);
```

---

## Null Key Behavior

Without PRIMARY KEY annotation or GROUP BY, records will have null keys (round-robin partitioning):

```sql
-- No key specified - null key (round-robin partitioning)
CREATE STREAM no_key_stream AS
SELECT price, quantity FROM trades
WITH (...);
```

This is useful for fan-out scenarios where you want even distribution across partitions.

---

## Accessing Source Keys (`_key` Pseudo-Column)

The original Kafka message key from the source topic is accessible via the `_key` pseudo-column on StreamRecord.

### Using `_key` in SQL

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

### How It Works

The `_key` pseudo-column is automatically populated from the Kafka message key when consuming from a source topic:

```rust
// StreamRecord provides access to source key
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,
    pub key: Option<FieldValue>,     // accessible as _key
    pub topic: Option<FieldValue>,   // accessible as _topic
    // ...
}

impl StreamRecord {
    pub fn get_field(&self, name: &str) -> Option<&FieldValue> {
        match name {
            "_key" => self.key.as_ref(),
            "_topic" => self.topic.as_ref(),
            _ => self.fields.get(name),
        }
    }
}
```

### Dual-Write Pattern (Industry Standard)

Following the industry standard (ksqlDB, Flink), key fields are written to **both**:
- The Kafka message key (for partitioning)
- The value payload (for downstream SQL access)

This means you don't typically need `_key` unless:
- Re-keying to a different field
- The source uses a simple string key not present in the value
- Debugging or auditing key propagation

---

## See Also

- [FR-089: Compound Keys & Explicit Key Configuration](../feature/FR-089-compound-keys/README.md)
- [COPY_PASTE_EXAMPLES.md](./COPY_PASTE_EXAMPLES.md) - Working examples
- [PARSER_GRAMMAR.md](./PARSER_GRAMMAR.md) - Formal grammar reference
