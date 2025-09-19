# Hello World - Your First Velostream Query

Learn the basics of Velostream SQL with simple, working examples.

> **⚠️ Important**: All Velostream queries require data sources and sinks to be defined with `CREATE STREAM` statements and `WITH` clauses. The examples below show complete, runnable queries.

## Your First Complete Query

### Simple Selection with Data Sources
```sql
-- Step 1: Define input data source
CREATE STREAM orders WITH (
    topic = 'orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.deserializer = 'string',
    value.deserializer = 'json'
);

-- Step 2: Define output destination
CREATE STREAM filtered_orders WITH (
    topic = 'filtered-orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.serializer = 'string',
    value.serializer = 'json'
);

-- Step 3: Process data with filtering
INSERT INTO filtered_orders
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0
LIMIT 50;
```

**What this does:**
- **Creates input stream**: Reads from `orders-topic` Kafka topic
- **Creates output stream**: Writes to `filtered-orders-topic` Kafka topic
- **Processes data**: Selects specific columns, filters for orders over $100, limits to 50 rows

### SELECT with Expressions and Aliases
```sql
-- Data source and sink (same as above)
CREATE STREAM orders WITH (
    topic = 'orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.deserializer = 'string',
    value.deserializer = 'json'
);

CREATE STREAM processed_orders WITH (
    topic = 'processed-orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.serializer = 'string',
    value.serializer = 'json'
);

-- SELECT with expressions and aliases
INSERT INTO processed_orders
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
-- Data source and sink
CREATE STREAM orders WITH (
    topic = 'orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.deserializer = 'string',
    value.deserializer = 'json'
);

CREATE STREAM completed_orders WITH (
    topic = 'completed-orders-topic',
    bootstrap.servers = 'localhost:9092',
    key.serializer = 'string',
    value.serializer = 'json'
);

-- Wildcard selection
INSERT INTO completed_orders
SELECT * FROM orders WHERE status = 'completed';
```

**What this does:**
- Selects ALL columns with `*`
- Filters for completed orders only
- Useful for exploring data

## Basic Filtering with WHERE

> **💡 Tip**: You can use configuration files instead of inline Kafka settings. See [examples/configs/](../../examples/configs/) for YAML configuration templates.

### Simple Conditions
```sql
-- Example 1: Filter by status
CREATE STREAM orders WITH (
    topic = 'orders-topic',
    bootstrap.servers = 'localhost:9092',
    value.deserializer = 'json'
);

CREATE STREAM pending_orders WITH (
    topic = 'pending-orders-topic',
    bootstrap.servers = 'localhost:9092',
    value.serializer = 'json'
);

INSERT INTO pending_orders
SELECT * FROM orders WHERE status = 'pending';

-- Example 2: Filter by numeric value
CREATE STREAM products WITH (
    topic = 'products-topic',
    bootstrap.servers = 'localhost:9092',
    value.deserializer = 'json'
);

CREATE STREAM expensive_products WITH (
    topic = 'expensive-products-topic',
    bootstrap.servers = 'localhost:9092',
    value.serializer = 'json'
);

INSERT INTO expensive_products
SELECT * FROM products WHERE price > 50.0;
```

### Multiple Conditions with AND
```sql
-- Using configuration files (recommended for production)
CREATE STREAM orders WITH (
    config_file = 'configs/orders-source.yaml'
);

CREATE STREAM high_value_completed_orders WITH (
    config_file = 'configs/orders-sink.yaml'
);

-- Both conditions must be true
INSERT INTO high_value_completed_orders
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0 AND status = 'completed';
```

### Alternative Conditions with OR
```sql
CREATE STREAM orders WITH (
    config_file = 'configs/orders-source.yaml'
);

CREATE STREAM priority_orders WITH (
    config_file = 'configs/priority-orders-sink.yaml'
);

-- At least one condition must be true
INSERT INTO priority_orders
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

**Complete Velostream query pattern:**
```sql
-- Step 1: Define input stream
CREATE STREAM input_stream WITH (
    topic = 'input-topic',
    bootstrap.servers = 'localhost:9092',
    value.deserializer = 'json'
);

-- Step 2: Define output stream
CREATE STREAM output_stream WITH (
    topic = 'output-topic',
    bootstrap.servers = 'localhost:9092',
    value.serializer = 'json'
);

-- Step 3: Process data
INSERT INTO output_stream
SELECT [columns or *]
FROM input_stream
WHERE [conditions]
LIMIT [number];
```

**Alternative: Using configuration files (recommended)**
```sql
CREATE STREAM input_stream WITH (config_file = 'configs/input.yaml');
CREATE STREAM output_stream WITH (config_file = 'configs/output.yaml');
INSERT INTO output_stream SELECT * FROM input_stream WHERE condition;
```

**Common conditions:**
- `column = 'value'` - exact match
- `column > number` - greater than
- `column LIKE 'pattern%'` - text pattern matching
- `column IN ('val1', 'val2')` - multiple values
- `column IS NOT NULL` - non-empty values