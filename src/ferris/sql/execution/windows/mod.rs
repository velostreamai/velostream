/*!
# Window Processing Module

This module provides comprehensive window processing functionality for streaming SQL queries,
supporting time-based window operations including tumbling, sliding, and session windows.

## Architecture

- **`state`** - WindowState for managing window buffer, timing, and memory
- **`processor`** - Core window processing logic, emission timing, and aggregation

## Key Features

- **Tumbling Windows** - Fixed-size, non-overlapping time windows
- **Sliding Windows** - Fixed-size windows that advance by specified intervals
- **Session Windows** - Variable-size windows based on inactivity gaps
- Memory-efficient buffering with VecDeque and configurable limits
- Automatic record eviction based on window specifications
- Watermark support for late data handling
- Integration with GROUP BY for windowed aggregation

## Window Types

### Tumbling Windows
Fixed-size, non-overlapping windows that partition the stream into discrete chunks.
```text
[Window 1][Window 2][Window 3]
0--------10--------20--------30
```

### Sliding Windows
Fixed-size windows that advance by smaller intervals, creating overlapping windows.
```text
[Window 1    ]
    [Window 2    ]
        [Window 3    ]
0----5----10----15----20
```

### Session Windows
Variable-size windows that group events separated by no more than a gap threshold.
```text
[Session 1]    [Session 2      ]
|ev|ev|       |ev|    |ev|ev|
```

## Usage

```rust,no_run
use ferrisstreams::ferris::sql::execution::windows::{WindowState, WindowProcessor};
use ferrisstreams::ferris::sql::ast::{WindowSpec, StreamingQuery};
use std::time::Duration;

// Create window state for a tumbling 10-second window
let window_spec = WindowSpec::Tumbling {
    size: Duration::from_secs(10),
    time_column: None
};
let mut window_state = WindowState::new(window_spec);

// Example processing loop (variables would be provided by your application)
# let records = vec![];  // Vec<StreamRecord>
# let query = todo!();   // StreamingQuery
# let current_time = 0i64; // chrono::Utc::now().timestamp_millis()

for record in records {
    WindowProcessor::process_record(&mut window_state, &record)?;

    // Check if window should emit
    if WindowProcessor::should_emit_window(&window_state, current_time) {
        let window_records = WindowProcessor::get_window_records_for_emission(
            &window_state, current_time
        );

        // Execute windowed aggregation
        let result = WindowProcessor::execute_windowed_aggregation(&query, &window_records)?;

        // Update emission timestamp
        window_state.update_last_emit(current_time);
    }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Memory Management

- Configurable buffer size limits to prevent memory overflow
- Automatic eviction of old records outside window boundaries
- Memory usage estimation and pressure detection
- Efficient VecDeque-based buffering for fast insertion/removal

## Integration

Window processing integrates seamlessly with:
- GROUP BY operations for windowed aggregation
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Time-based event processing and watermarks
- Stream-table JOINs with temporal constraints
*/

pub mod processor;
pub mod state;

// Re-export all public types for easy access
pub use processor::*;
pub use state::*;
