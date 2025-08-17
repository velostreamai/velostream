/*!
# GROUP BY Processing Module

This module provides comprehensive GROUP BY functionality for streaming SQL queries,
including state management, record processing, and aggregate function computations.

## Architecture

- **`state`** - GroupByState and GroupAccumulator for managing group aggregation state
- **`processor`** - Core GROUP BY processing logic and record handling
- **`aggregates`** - Helper functions for aggregate computations and utilities

## Key Features

- Multi-column grouping with expression support
- All standard SQL aggregate functions (COUNT, SUM, AVG, MIN, MAX, etc.)
- Statistical aggregates (STDDEV, VARIANCE)
- String aggregation (STRING_AGG, GROUP_CONCAT)
- Distinct counting (COUNT_DISTINCT)
- Proper NULL handling according to SQL standards
- Memory-efficient state management

## Usage

```rust,no_run
use ferrisstreams::ferris::sql::execution::groupby::{GroupByState, GroupByProcessor};

// Example variables (would be provided by your application)
# let group_expressions = vec![];  // Vec<Expr>
# let select_fields = vec![];      // Vec<SelectField>
# let having_clause = None;        // Option<Expr>
# let records = vec![];            // Vec<StreamRecord>

// Create GROUP BY state
let mut group_state = GroupByState::new(group_expressions, select_fields, having_clause);

// Process records
for record in records {
    GroupByProcessor::process_record(&mut group_state, &record)?;
}

// Extract results
for group_key in group_state.group_keys() {
    let accumulator = group_state.get_accumulator(group_key).unwrap();
    let result = GroupByProcessor::compute_group_result_fields(
        accumulator, &group_state.select_fields, &group_state.group_expressions,
        accumulator.sample_record.as_ref()
    )?;
}
# Ok::<(), Box<dyn std::error::Error>>(())
```
*/

pub mod aggregates;
pub mod processor;
pub mod state;

// Re-export all public types for easy access
pub use processor::*;
pub use state::*;
