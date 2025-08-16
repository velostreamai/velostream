/*!
# JOIN Processing Module

This module provides comprehensive JOIN functionality for streaming SQL queries,
supporting all standard SQL JOIN types with streaming optimizations and windowing support.

## Architecture

- **`processor`** - Core JOIN processing logic for all JOIN types with record combination

## Key Features

- **All SQL JOIN Types** - INNER, LEFT, RIGHT, FULL OUTER joins
- **Stream-Stream JOINs** - Join two streaming data sources
- **Stream-Table JOINs** - Optimized joins between streams and materialized tables
- **Windowed JOINs** - Temporal joins with time windows and grace periods
- **Key-based Matching** - Efficient join key extraction and comparison
- **NULL Handling** - Proper SQL-standard NULL semantics for outer joins

## JOIN Types

### INNER JOIN
Returns only records where the join condition is satisfied by both sides.
```sql
SELECT * FROM stream1 s1
INNER JOIN stream2 s2 ON s1.id = s2.user_id
```

### LEFT JOIN (LEFT OUTER JOIN)
Returns all records from the left side, with NULLs for unmatched right side records.
```sql
SELECT * FROM orders o
LEFT JOIN users u ON o.user_id = u.id
```

### RIGHT JOIN (RIGHT OUTER JOIN)
Returns all records from the right side, with NULLs for unmatched left side records.
```sql
SELECT * FROM orders o
RIGHT JOIN products p ON o.product_id = p.id
```

### FULL OUTER JOIN
Returns all records from both sides, with NULLs where no match exists.
```sql
SELECT * FROM stream1 s1
FULL OUTER JOIN stream2 s2 ON s1.key = s2.key
```

## Windowed JOINs

Temporal joins with time-based constraints for streaming data:

```sql
SELECT * FROM orders o
JOIN shipments s ON o.order_id = s.order_id
WITHIN INTERVAL 1 HOUR WITH GRACE PERIOD 30 MINUTES
```

## Usage

```rust,no_run
use crate::ferris::sql::execution::joins::JoinProcessor;
use crate::ferris::sql::ast::{JoinClause, JoinType};

// Example variables (would be provided by your application)
# let left_record = todo!();      // StreamRecord
# let right_record_opt = None;    // Option<StreamRecord>
# let join_clause = todo!();      // JoinClause
# let base_record = todo!();      // StreamRecord
# let join_clauses = vec![];      // Vec<JoinClause>
# let stream_record = todo!();    // StreamRecord
# let table_source = todo!();     // StreamSource
# let join_condition = todo!();   // Expr
# let join_type = JoinType::Inner; // JoinType

// Process a single JOIN operation
let result = JoinProcessor::execute_join(
    &left_record,
    right_record_opt.as_ref(),
    &join_clause
)?;

// Process multiple JOIN clauses in sequence
let final_result = JoinProcessor::process_joins(
    &base_record,
    &join_clauses
)?;

// Optimized stream-table JOIN
let stream_table_result = JoinProcessor::process_stream_table_join(
    &stream_record,
    &table_source,
    &join_condition,
    &join_type
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Stream-Table JOIN Optimization

Stream-table JOINs are optimized for scenarios where one side is a continuous stream
and the other is a materialized table/view:

- **Key-based Lookups** - Efficient table lookups using join keys
- **Caching** - Table data can be cached for repeated lookups
- **Reduced Memory** - No need to buffer table-side records
- **Low Latency** - Immediate join results without waiting for windows

## Record Combination

The module handles proper record combination:

- **Field Aliasing** - Automatic aliasing to prevent field name conflicts
- **Metadata Preservation** - Maintains timestamps, offsets, and headers
- **NULL Injection** - Adds NULL fields for unmatched outer join sides
- **Type Safety** - Preserves field types during combination

## Windowing Support

For streaming JOINs with temporal constraints:

- **Time Windows** - Define maximum time difference between join sides
- **Grace Periods** - Allow late-arriving data within grace period
- **Watermarks** - Handle out-of-order event streams
- **Memory Management** - Automatic cleanup of expired join state

## Integration

JOIN processing integrates with:
- Window operations for temporal joins
- Expression evaluation for join conditions
- Field aliasing and projection
- Aggregate functions in joined results
- Multiple table/stream sources
*/

pub mod processor;

// Re-export all public types for easy access
pub use processor::*;
