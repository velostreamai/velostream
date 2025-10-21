# SELECT Clause Alias Reuse

**Feature Status**: ✅ Production Ready (v0.2+)
**SQL Standard**: Supported in MySQL 8.0+, SQL Server
**Velostream Support**: Full support with complete test coverage

---

## Overview

SELECT clause alias reuse allows you to reference previously-defined column aliases within the same SELECT clause. This eliminates repetitive expressions and makes complex queries more readable and maintainable.

### Quick Example

```sql
-- Define an alias, then reference it in subsequent fields
SELECT
    volume / avg_volume AS spike_ratio,           -- Define alias
    spike_ratio * 100 AS spike_percentage         -- Reference it here ✅
FROM trades;
```

Instead of repeating the expression:

```sql
-- Without alias reuse (repetitive)
SELECT
    volume / avg_volume AS spike_ratio,
    (volume / avg_volume) * 100 AS spike_percentage
FROM trades;
```

---

## Basic Syntax

### Simple Alias Reference

```sql
SELECT
    expression1 AS alias1,
    alias1 + 10 AS result
FROM table_name;
```

**Evaluation Order**: Left-to-right
- `alias1` is computed first
- `alias1` is available for use in subsequent field expressions
- Fields after `alias1` can reference it

### Multiple Aliases

```sql
SELECT
    column1 AS x,
    x + 1 AS y,
    y * 2 AS z
FROM table_name;
```

Chain multiple aliases for complex transformations.

---

## Real-World Examples

### Example 1: Financial Trading Analysis

Calculate spike ratios and classification in a single query:

```sql
SELECT
    symbol,
    MAX(volume) / AVG(volume) AS spike_ratio,
    CASE
        WHEN spike_ratio > 10 THEN 'EXTREME'
        WHEN spike_ratio > 5 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS spike_classification,
    CASE
        WHEN spike_classification = 'EXTREME' THEN 'TRIGGER_BREAKER'
        WHEN spike_classification = 'HIGH' THEN 'ALERT'
        ELSE 'ALLOW'
    END AS circuit_state
FROM market_data
GROUP BY symbol;
```

**Benefits**:
- Cleaner query structure
- Easier to understand logic flow
- Single computation of `spike_ratio`
- Reusable intermediate values

### Example 2: Percentage Calculations

```sql
SELECT
    product_id,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_quantity,
    total_sales / total_quantity AS avg_price,
    avg_price * 0.9 AS discounted_price
FROM orders
GROUP BY product_id;
```

### Example 3: Date Arithmetic

```sql
SELECT
    order_id,
    order_date,
    delivery_date - order_date AS days_to_deliver,
    CASE
        WHEN days_to_deliver > 7 THEN 'LATE'
        ELSE 'ON_TIME'
    END AS delivery_status
FROM shipments;
```

### Example 4: Profitability Analysis

```sql
SELECT
    order_id,
    SUM(revenue) AS total_revenue,
    SUM(cost) AS total_cost,
    total_revenue - total_cost AS gross_profit,
    gross_profit / total_revenue AS profit_margin,
    CASE
        WHEN profit_margin > 0.3 THEN 'EXCELLENT'
        WHEN profit_margin > 0.1 THEN 'GOOD'
        ELSE 'FAIR'
    END AS profitability_rating
FROM order_details
GROUP BY order_id;
```

---

## Advanced Patterns

### Alias Shadowing

An alias can reference the same column name:

```sql
SELECT
    amount AS amount,              -- Alias hides original column
    amount * 1.1 AS amount         -- This uses the alias, not original
FROM transactions;
```

**Important**: Once a column is aliased, subsequent references use the alias value, not the original column.

### Conditional Logic with Aliases

```sql
SELECT
    transaction_id,
    amount,
    amount > 1000 AS is_large,
    CASE
        WHEN is_large THEN 'HIGH_VALUE'
        ELSE 'STANDARD'
    END AS classification
FROM transactions;
```

### Aggregation with Aliases

```sql
SELECT
    symbol,
    COUNT(*) AS trade_count,
    AVG(price) AS avg_price,
    MAX(price) - MIN(price) AS price_range,
    price_range / avg_price AS volatility
FROM trades
GROUP BY symbol
HAVING trade_count > 100;
```

**Note**: HAVING clause can also reference SELECT aliases.

### Window Functions with Aliases

```sql
SELECT
    order_id,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total,
    running_total / 1000 AS thousands,
    CASE
        WHEN thousands >= 10 THEN 'PLATINUM'
        ELSE 'REGULAR'
    END AS tier
FROM orders;
```

---

## Type Handling

### Type Preservation

Aliases preserve the type of the expression:

```sql
SELECT
    100 AS int_val,                    -- Type: Integer
    int_val * 2 AS result              -- Type: Integer (100 * 2 = 200)
FROM data;
```

### Type Conversion

Automatic type conversion occurs when needed:

```sql
SELECT
    price AS price_int,               -- Type: ScaledInteger
    price * 1.5 AS price_scaled,      -- Automatic scaling
    CAST(price_scaled AS Float) AS price_float
FROM products;
```

### NULL Handling

```sql
SELECT
    amount,
    amount AS amount_alias,
    CASE
        WHEN amount_alias IS NULL THEN 0
        ELSE amount_alias
    END AS amount_safe
FROM transactions;
```

---

## Limitations & Constraints

### ❌ NOT Supported in WHERE Clause

Aliases cannot be used in WHERE clauses:

```sql
-- ❌ INVALID - Aliases not available in WHERE
SELECT
    amount / 1000 AS thousands
FROM transactions
WHERE thousands > 5;

-- ✅ VALID - Use original expression
SELECT
    amount / 1000 AS thousands
FROM transactions
WHERE amount / 1000 > 5;

-- ✅ VALID - Use subquery
SELECT * FROM (
    SELECT amount / 1000 AS thousands
    FROM transactions
) WHERE thousands > 5;
```

### ✅ SUPPORTED in Other Clauses

- **SELECT clause**: ✅ Full support (intra-SELECT references)
- **GROUP BY**: ✅ Full support (aliases computed during aggregation)
- **HAVING clause**: ✅ Full support (references computed aggregates)
- **ORDER BY**: ✅ Full support (references SELECT aliases)
- **WHERE clause**: ❌ Not supported (use original expressions)

### Alias Scope

Aliases are only available within the same SELECT clause:

```sql
-- Alias 'ratio' is not available in next query
SELECT amount / 1000 AS ratio FROM orders;
SELECT ratio FROM orders;  -- ❌ Error: ratio not found

-- To reuse across queries, use subqueries
SELECT * FROM (
    SELECT amount / 1000 AS ratio FROM orders
) WHERE ratio > 5;
```

---

## Performance Considerations

### Computation Efficiency

Aliases are evaluated once and reused:

```sql
-- Efficient: spike_ratio computed once
SELECT
    spike_ratio,
    spike_ratio * 100,
    spike_ratio / 10
FROM (
    SELECT volume / avg_volume AS spike_ratio FROM trades
);
```

Without aliases (less efficient):

```sql
-- Inefficient: repeated computation
SELECT
    volume / avg_volume,
    (volume / avg_volume) * 100,
    (volume / avg_volume) / 10
FROM trades;
```

### Query Optimization

Velostream optimizes alias evaluation:
- Each alias computed once per row
- Zero overhead for reference resolution
- Left-to-right evaluation is deterministic
- Full support for parallel aggregation (GROUP BY)

---

## Error Handling

### Undefined Alias

```sql
-- ❌ Error: Using alias before definition
SELECT
    y + 1 AS result,      -- y not yet defined
    10 AS y
FROM data;
```

**Solution**: Define aliases in dependency order

```sql
-- ✅ Correct order
SELECT
    10 AS y,
    y + 1 AS result
FROM data;
```

### Circular References

```sql
-- ❌ Error: Circular reference
SELECT
    x + 1 AS x            -- x cannot reference itself
FROM data;
```

**Solution**: Use different alias names

```sql
-- ✅ Correct
SELECT
    10 AS x,
    x + 1 AS x_plus_one
FROM data;
```

### Type Mismatches

```sql
-- ✅ Handled automatically
SELECT
    price AS price_int,           -- ScaledInteger
    price * 1.1 AS price_scaled   -- Automatic scaling
FROM products;
```

---

## Migration Guide

### From Repetitive Expressions

**Before**:
```sql
SELECT
    (revenue - cost) AS profit,
    ((revenue - cost) / revenue) * 100 AS margin,
    CASE
        WHEN ((revenue - cost) / revenue) * 100 > 30 THEN 'EXCELLENT'
        ELSE 'FAIR'
    END AS rating
FROM financials;
```

**After**:
```sql
SELECT
    revenue - cost AS profit,
    profit / revenue * 100 AS margin,
    CASE
        WHEN margin > 30 THEN 'EXCELLENT'
        ELSE 'FAIR'
    END AS rating
FROM financials;
```

### From Subqueries

**Before (PostgreSQL style, not in Velostream)**:
```sql
SELECT * FROM (
    SELECT
        amount / 1000 AS thousands
    FROM orders
) WHERE thousands > 5;
```

**After (Velostream with aliases)**:
```sql
SELECT
    amount / 1000 AS thousands
FROM orders
HAVING thousands > 5;  -- Or filter in application
```

### From Multiple Queries

**Before**:
```sql
-- Query 1: Calculate ratio
SELECT amount / 1000 AS thousands FROM orders;

-- Query 2: Use result (requires application logic)
-- Filter where thousands > 5
```

**After (single query)**:
```sql
SELECT
    amount / 1000 AS thousands,
    CASE WHEN thousands > 5 THEN 'HIGH' ELSE 'LOW' END AS classification
FROM orders;
```

---

## Best Practices

### 1. Use Descriptive Alias Names

```sql
-- ✅ Good
SELECT
    SUM(sales_amount) AS total_sales,
    COUNT(*) AS transaction_count,
    total_sales / transaction_count AS avg_transaction
FROM sales;

-- ❌ Poor
SELECT
    SUM(sales_amount) AS x,
    COUNT(*) AS y,
    x / y AS z
FROM sales;
```

### 2. Order Aliases by Dependency

```sql
-- ✅ Good (dependency order)
SELECT
    base_amount AS amount,
    amount * tax_rate AS tax,
    amount + tax AS total
FROM invoices;

-- ❌ Confusing (unclear dependencies)
SELECT
    amount + tax AS total,
    amount * tax_rate AS tax,
    base_amount AS amount
FROM invoices;
```

### 3. Limit Alias Chain Depth

```sql
-- ✅ Acceptable (2-3 levels)
SELECT
    price AS base_price,
    base_price * markup AS marked_price,
    marked_price * tax AS final_price
FROM products;

-- ⚠️ Avoid (too many levels)
SELECT
    x AS a,
    a * 2 AS b,
    b + 3 AS c,
    c - 5 AS d,
    d * 7 AS e,
    e / 2 AS f
FROM data;
```

### 4. Document Complex Aliases

```sql
SELECT
    -- Compute spike ratio from volume statistics
    MAX(volume) / AVG(volume) AS spike_ratio,

    -- Classify spike magnitude
    CASE
        WHEN spike_ratio > 10 THEN 'EXTREME'
        WHEN spike_ratio > 5 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS spike_classification,

    -- Determine circuit breaker action
    CASE
        WHEN spike_classification = 'EXTREME' THEN 'TRIGGER_BREAKER'
        ELSE 'ALLOW'
    END AS circuit_action
FROM market_data
GROUP BY symbol;
```

### 5. Use with GROUP BY and HAVING

```sql
-- ✅ Recommended pattern
SELECT
    category,
    SUM(amount) AS total_amount,
    COUNT(*) AS item_count,
    total_amount / item_count AS avg_amount
FROM orders
GROUP BY category
HAVING item_count > 10
ORDER BY avg_amount DESC;
```

---

## Troubleshooting

### Issue: "Undefined column" error

**Problem**: Referencing alias before it's defined

```sql
-- ❌ Error
SELECT
    alias_value + 1 AS result,
    100 AS alias_value
FROM data;
```

**Solution**: Define aliases first

```sql
-- ✅ Works
SELECT
    100 AS alias_value,
    alias_value + 1 AS result
FROM data;
```

### Issue: NULL values in alias results

**Problem**: Expression returns NULL

```sql
SELECT
    amount / NULL AS result  -- NULL result
FROM data;
```

**Solution**: Handle NULL explicitly

```sql
SELECT
    COALESCE(amount / divisor, 0) AS result
FROM data;
```

### Issue: Type mismatch in calculations

**Problem**: Mixing incompatible types

```sql
SELECT
    price * 1.5 AS scaled_price  -- Automatic scaling
FROM products;
```

**Solution**: Velostream handles automatically, but cast if needed

```sql
SELECT
    CAST(price AS Float) * 1.5 AS scaled_price
FROM products;
```

---

## Comparison with Other Databases

| Feature | MySQL 8.0+ | SQL Server | PostgreSQL | Velostream |
|---------|-----------|-----------|-----------|-----------|
| SELECT alias reuse | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| In WHERE clause | ❌ No | ❌ No | ❌ No | ❌ No |
| In HAVING clause | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| Multiple alias chains | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| GROUP BY + aliases | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |

---

## See Also

- [SQL Reference Guide](../reference/complete-syntax.md)
- [GROUP BY Reference](../reference/group-by.md)
- [Aggregation Functions](./aggregation.md)
- [Window Functions](./window.md)
- [HAVING Clauses](../reference/complete-syntax.md#having-clause)

---

**Document Version**: 1.0
**Last Updated**: 2025-10-20
**Feature Status**: ✅ Production Ready
