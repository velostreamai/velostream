# GROUP BY Implementation in Velostream

This document describes the complete GROUP BY implementation for the streaming SQL engine, including functionality, features, and usage examples.

## Overview

Velostream now supports comprehensive GROUP BY operations with streaming semantics, allowing aggregation of data records based on grouping expressions. The implementation provides:

- **Full GROUP BY clause parsing and execution**
- **Multiple aggregate functions** (COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT, STDDEV, VARIANCE, FIRST, LAST, STRING_AGG)
- **HAVING clause support** for post-aggregation filtering
- **Multiple grouping columns** support
- **Expression-based grouping** (functions, calculations)
- **Complex data type support** (arrays, maps, structs)

## Features Implemented

### Core GROUP BY Functionality

#### 1. Basic Grouping
```sql
SELECT customer_id, COUNT(*) as order_count 
FROM orders 
GROUP BY customer_id
```

#### 2. Multiple Grouping Columns
```sql
SELECT customer_id, category, COUNT(*) as count, SUM(amount) as total
FROM orders 
GROUP BY customer_id, category
```

#### 3. Expression-based Grouping
```sql
SELECT YEAR(order_date), MONTH(order_date), COUNT(*) as monthly_orders
FROM orders 
GROUP BY YEAR(order_date), MONTH(order_date)
```

### Aggregate Functions

#### Statistical Functions
- **COUNT(*)** - Count all records in group
- **COUNT(column)** - Count non-null values
- **COUNT_DISTINCT(column)** - Count unique non-null values
- **SUM(column)** - Sum numeric values
- **AVG(column)** - Average of numeric values
- **MIN(column)** - Minimum value (works with any comparable type)
- **MAX(column)** - Maximum value (works with any comparable type)
- **STDDEV(column)** - Standard deviation of numeric values
- **VARIANCE(column)** - Variance of numeric values

#### Positional Functions
- **FIRST(column)** - First value in the group
- **LAST(column)** - Last value in the group

#### String Aggregation
- **STRING_AGG(column, separator)** - Concatenate string values with separator
- **GROUP_CONCAT(column, separator)** - Alias for STRING_AGG

### HAVING Clause Support

Filter aggregated results with post-aggregation conditions:

```sql
SELECT customer_id, SUM(amount) as total_spent
FROM orders 
GROUP BY customer_id 
HAVING SUM(amount) > 1000
```

### Complex Data Type Support

The GROUP BY implementation handles all Velostream data types:

#### Primitive Types
- **Integers, Floats, Strings, Booleans**
- **Dates and Timestamps**
- **Decimals**
- **NULL values** (grouped together)

#### Complex Types
- **Arrays** - Grouped by string representation
- **Maps** - Grouped by sorted key-value representation  
- **Structs** - Grouped by sorted field representation

## Implementation Architecture

### Core Components

#### 1. Parser Integration (`parser.rs`)
- Complete GROUP BY clause parsing
- Expression-based grouping support
- Integration with existing SELECT statement parsing

#### 2. AST Support (`ast.rs`)
- `group_by: Option<Vec<Expr>>` field in SELECT queries
- Full expression support for grouping

#### 3. Execution Engine (`execution.rs`)
Key methods implemented:

- **`group_records()`** - Groups records by GROUP BY expressions
- **`evaluate_group_key_expression()`** - Evaluates grouping expressions
- **`field_value_to_group_key()`** - Converts values to grouping keys
- **`evaluate_aggregation_expression()`** - Processes aggregate functions

### Grouping Algorithm

1. **Expression Evaluation**: Each GROUP BY expression is evaluated for every record
2. **Key Generation**: Values are converted to string keys for grouping
3. **Record Grouping**: Records with identical keys are grouped together
4. **Aggregate Calculation**: Aggregate functions are computed per group
5. **Result Generation**: One result record per group
6. **HAVING Filtering**: Groups that don't satisfy HAVING are filtered out

### Memory Management

The implementation uses efficient data structures:
- `HashMap<Vec<String>, Vec<&StreamRecord>>` for grouping
- String-based keys for all data types
- Reference-based record storage to avoid copying

## Usage Examples

### Basic Aggregation
```sql
-- Count orders per customer
SELECT customer_id, COUNT(*) as order_count
FROM orders 
GROUP BY customer_id

-- Sum amounts by category
SELECT category, SUM(amount) as total_sales
FROM sales 
GROUP BY category
```

### Multiple Aggregations
```sql
-- Customer summary statistics
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent,
    AVG(amount) as average_order,
    MIN(amount) as smallest_order,
    MAX(amount) as largest_order
FROM orders 
GROUP BY customer_id
```

### Complex Grouping
```sql
-- Time-based grouping
SELECT 
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as monthly_orders,
    SUM(amount) as monthly_revenue
FROM orders 
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month
```

### String Aggregation
```sql
-- Concatenate product names per customer
SELECT 
    customer_id,
    STRING_AGG(product_name, ', ') as products_ordered
FROM order_items 
GROUP BY customer_id
```

### Statistical Analysis
```sql
-- Price analysis by category
SELECT 
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    STDDEV(price) as price_std_dev,
    VARIANCE(price) as price_variance
FROM products 
GROUP BY category
HAVING COUNT(*) > 10
```

## Streaming Semantics

### Windowed GROUP BY

GROUP BY operations work within window boundaries:

```sql
SELECT customer_id, COUNT(*) as order_count
FROM orders 
GROUP BY customer_id
WINDOW TUMBLING(5m)
```

### Multiple Result Emission

When GROUP BY produces multiple groups, the execution engine:
1. Returns the first group as the primary result
2. Emits additional groups through the output channel
3. Maintains streaming performance

## Testing

The implementation includes comprehensive tests:

### Parser Tests
- Basic GROUP BY parsing
- Multiple column grouping
- Expression-based grouping
- Error handling for invalid syntax

### Execution Tests  
- Single and multiple group processing
- All aggregate function validation
- HAVING clause filtering
- Complex data type handling
- Edge cases (empty groups, NULL handling)

### Performance Tests
- Memory efficiency validation
- Large dataset handling
- Streaming performance metrics

## Performance Characteristics

### Time Complexity
- **Grouping**: O(n * g) where n = records, g = group key evaluation cost
- **Aggregation**: O(n * a) where a = number of aggregate functions
- **Overall**: O(n * (g + a))

### Space Complexity
- **Memory**: O(k * r) where k = number of groups, r = average records per group
- **Key Storage**: O(k * s) where s = average key size

### Optimizations
- String-based grouping keys for efficient HashMap operations
- Reference-based record storage to minimize copying
- Lazy evaluation of aggregate functions
- Efficient NULL handling

## Integration with Existing Features

### Window Operations
GROUP BY works seamlessly with all window types:
- **Tumbling Windows**: Fixed-size time windows
- **Sliding Windows**: Overlapping time windows  
- **Session Windows**: Inactivity-based windows

### JOIN Operations
GROUP BY can be used with JOIN results:
```sql
SELECT c.customer_name, COUNT(*) as order_count
FROM orders o 
JOIN customers c ON o.customer_id = c.id
GROUP BY c.customer_name
```

### Subqueries
GROUP BY supports subquery patterns:
```sql
SELECT customer_id, total_amount
FROM (
    SELECT customer_id, SUM(amount) as total_amount
    FROM orders 
    GROUP BY customer_id
) 
WHERE total_amount > 1000
```

## Error Handling

The implementation provides detailed error messages for:
- Invalid aggregate function arguments
- Type mismatches in aggregation
- Missing GROUP BY columns in SELECT
- HAVING clause evaluation errors

Example error messages:
```
"SUM can only be applied to numeric values"
"COUNT function requires 0 or 1 arguments"
"No groups satisfied HAVING clause"
```

## Future Enhancements

Potential improvements for future versions:

### Advanced Aggregations
- **ROLLUP** and **CUBE** operations
- **Window functions** with OVER clauses
- **Custom aggregate functions**

### Performance Optimizations
- **Parallel grouping** for large datasets
- **Incremental aggregation** for streaming scenarios
- **Memory-mapped storage** for large group sets

### Advanced Features
- **GROUPING SETS** for multiple groupings in one query
- **Materialized aggregate views**
- **Time-based expiration** of groups

## Conclusion

The GROUP BY implementation in Velostream provides a robust, feature-complete aggregation system that maintains streaming performance while supporting complex analytical queries. The implementation follows SQL standards while adding streaming-specific optimizations and features.

The system is production-ready for most use cases and provides a solid foundation for advanced analytical workloads in streaming data processing pipelines.