# Hello World - Your First FerrisStreams Query

Learn the basics of FerrisStreams SQL with simple, working examples.

## Basic SELECT Statements

### Simple Selection
```sql
-- Basic SELECT with filtering
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0
LIMIT 50;
```

**What this does:**
- Selects specific columns: `customer_id`, `amount`, `order_date`
- Filters for orders over $100
- Limits results to 50 rows

### SELECT with Expressions and Aliases
```sql
-- SELECT with expressions and aliases
SELECT
    customer_id,
    amount * 1.1 as amount_with_tax,
    UPPER(product_name) as product_name_upper
FROM orders;
```

**What this does:**
- Calculates tax (amount × 1.1) with alias `amount_with_tax`
- Converts product names to uppercase with alias `product_name_upper`
- Uses aliases to make results clearer

### Wildcard Selection
```sql
-- Wildcard selection
SELECT * FROM orders WHERE status = 'completed';
```

**What this does:**
- Selects ALL columns with `*`
- Filters for completed orders only
- Useful for exploring data

## Basic Filtering with WHERE

### Simple Conditions
```sql
-- Filter by status
SELECT * FROM orders WHERE status = 'pending';

-- Filter by numeric value
SELECT * FROM products WHERE price > 50.0;

-- Filter by date
SELECT * FROM events WHERE event_date > '2024-01-01';
```

### Multiple Conditions with AND
```sql
-- Both conditions must be true
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0 AND status = 'completed';
```

### Alternative Conditions with OR
```sql
-- At least one condition must be true
SELECT customer_id, status, priority
FROM orders
WHERE status = 'urgent' OR priority = 'high';
```

## Essential Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `status = 'active'` |
| `!=` or `<>` | Not equal to | `status != 'cancelled'` |
| `>` | Greater than | `amount > 100.0` |
| `>=` | Greater than or equal | `price >= 50.0` |
| `<` | Less than | `quantity < 10` |
| `<=` | Less than or equal | `discount <= 0.2` |
| `AND` | Both conditions true | `price > 10 AND category = 'books'` |
| `OR` | At least one condition true | `status = 'urgent' OR priority = 'high'` |

## Next Steps

Now that you understand the basics:

1. **Learn Complex Filtering**: [Filter Data Guide](../by-task/filter-data.md)
2. **Calculate Totals**: [Aggregate Data Guide](../by-task/aggregate-data.md)
3. **Combine Data**: [Join Streams Guide](../by-task/join-streams.md)
4. **Explore Functions**: [Essential Functions](../functions/essential.md)

## Quick Reference

**Basic query pattern:**
```sql
SELECT [columns or *]
FROM [table_name]
WHERE [conditions]
LIMIT [number];
```

**Common conditions:**
- `column = 'value'` - exact match
- `column > number` - greater than
- `column LIKE 'pattern%'` - text pattern matching
- `column IN ('val1', 'val2')` - multiple values
- `column IS NOT NULL` - non-empty values