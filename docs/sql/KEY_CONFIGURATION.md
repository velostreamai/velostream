# Kafka Message Key Configuration Guide

This guide covers all methods for configuring Kafka message keys in Velostream streaming queries.

## Overview

Kafka message keys determine how messages are partitioned. Proper key configuration ensures:
- **Co-located data**: Records with the same key go to the same partition
- **Ordering guarantees**: Messages with the same key are ordered within a partition
- **Efficient processing**: Downstream consumers can process related records together

## Key Configuration Methods

Velostream supports three ways to configure Kafka message keys:

| Method | Syntax | Use Case |
|--------|--------|----------|
| **Inline KEY annotation** | `SELECT symbol KEY, ...` | Explicit key declaration in SQL |
| **GROUP BY implicit key** | `GROUP BY symbol` | Auto-generated from GROUP BY columns |
| **Property-based** | `sink.key_field = 'symbol'` | Configuration in WITH clause |

---

## Method 1: Inline KEY Annotation (Recommended)

The `KEY` keyword marks a field as the Kafka message key directly in SQL.

### Single Key

```sql
CREATE STREAM keyed_trades AS
SELECT symbol KEY, price, quantity, event_time
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
SELECT region KEY, product KEY, quantity, revenue
FROM orders
WITH (...);
```

**Result**: Kafka key = `{"region":"US","product":"Widget"}` (JSON object)

### KEY with Alias

```sql
CREATE STREAM aliased_key AS
SELECT stock_symbol AS sym KEY, price
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

**Result**: Kafka key = `{"symbol":"AAPL"}` (JSON object from GROUP BY)

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

**Result**: Kafka key = `{"trader_id":"T1","symbol":"AAPL"}` (compound JSON)

### Combining KEY with GROUP BY

You can use KEY annotation alongside GROUP BY for explicit control:

```sql
CREATE TABLE explicit_key_agg AS
SELECT symbol KEY, trader_id, COUNT(*) as cnt
FROM trades
GROUP BY symbol, trader_id
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (...);
```

**Result**: Uses KEY annotation, not auto-generated GROUP BY key

---

## Method 3: Property-Based Configuration

Use WITH clause properties for explicit key configuration:

### Single Key Field

```sql
CREATE STREAM filtered_trades AS
SELECT * FROM trades
WHERE price > 100
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic' = 'trades_input',
    'trades.format' = 'json',
    'filtered_trades.type' = 'kafka_sink',
    'filtered_trades.topic' = 'filtered_output',
    'filtered_trades.format' = 'json',
    'sink.key_field' = 'symbol'
);
```

### Compound Key Fields

```sql
WITH (
    ...
    'sink.key_fields' = 'region,product'
);
```

**Result**: Kafka key = `{"region":"US","product":"Widget"}`

### Explicit Null Key (Round-Robin)

```sql
WITH (
    ...
    'sink.key_field' = 'null'
);
```

**Result**: No Kafka key (null), messages distributed round-robin

---

## Key Configuration Priority

When multiple methods are used, this priority order applies:

1. **`sink.key_field = 'null'`** → Explicit null key (round-robin)
2. **`sink.key_fields = 'a,b'`** → Compound key from config
3. **`sink.key_field = 'symbol'`** → Single key from config
4. **Inline KEY annotation** → `SELECT symbol KEY, ...`
5. **GROUP BY implicit** → Auto-generated from GROUP BY columns
6. **No configuration** → Null key (round-robin partitioning)

### Priority Example

```sql
-- KEY annotation takes precedence over GROUP BY implicit key
SELECT symbol KEY, trader_id, COUNT(*) as cnt
FROM trades
GROUP BY symbol, trader_id
-- Key will be "symbol" from KEY annotation, not {"symbol":"...", "trader_id":"..."}
```

---

## Key Format Reference

| Configuration | Key Format | Example |
|--------------|------------|---------|
| Single KEY annotation | Raw value | `"AAPL"` |
| Multiple KEY annotations | JSON object | `{"region":"US","product":"Widget"}` |
| Single GROUP BY | JSON object | `{"symbol":"AAPL"}` |
| Multiple GROUP BY | JSON object | `{"trader_id":"T1","symbol":"AAPL"}` |
| `sink.key_field` (single) | Raw value | `"AAPL"` |
| `sink.key_fields` (compound) | JSON object | `{"region":"US","product":"Widget"}` |
| `sink.key_field = 'null'` | Null | `null` (no key bytes) |

---

## Best Practices

### 1. Use KEY Annotation for Clarity

```sql
-- Explicit and self-documenting
SELECT customer_id KEY, order_id, amount FROM orders
```

### 2. Match KEY with GROUP BY

When aggregating, use KEY on the same columns as GROUP BY:

```sql
SELECT symbol KEY, COUNT(*) as cnt
FROM trades
GROUP BY symbol
```

### 3. Use Compound Keys for Multi-Dimension Partitioning

```sql
SELECT region KEY, product_type KEY, SUM(sales) as total
FROM sales
GROUP BY region, product_type
```

### 4. Explicit Null for Fan-Out Scenarios

When you want round-robin distribution:

```sql
CREATE STREAM fanout_stream AS
SELECT * FROM events
WITH (
    ...
    'sink.key_field' = 'null'
);
```

---

## Error Handling

### Missing Key Configuration

Without KEY annotation, GROUP BY, or `sink.key_field`, records will have null keys:

```sql
-- No key specified - null key (round-robin partitioning)
CREATE STREAM no_key_stream AS
SELECT price, quantity FROM trades
WITH (...);
```

### Key Field Not Found

If `sink.key_field` references a non-existent field, an error is returned:

```
Error: Key field 'missing_field' not found in record
```

---

## Migration from Property-Based to KEY Annotation

### Before (Property-Based)

```sql
CREATE STREAM keyed_trades AS
SELECT symbol, price FROM trades
WITH (
    ...
    'sink.key_field' = 'symbol'
);
```

### After (KEY Annotation)

```sql
CREATE STREAM keyed_trades AS
SELECT symbol KEY, price FROM trades
WITH (...);
```

Both produce the same result, but KEY annotation is more explicit and self-documenting.

---

## See Also

- [FR-089: Compound Keys & Explicit Key Configuration](../feature/FR-089-compound-keys/README.md)
- [COPY_PASTE_EXAMPLES.md](./COPY_PASTE_EXAMPLES.md) - Working examples
- [PARSER_GRAMMAR.md](./PARSER_GRAMMAR.md) - Formal grammar reference
