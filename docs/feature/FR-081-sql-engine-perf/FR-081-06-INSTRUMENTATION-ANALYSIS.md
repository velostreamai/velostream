# Source Code Instrumentation Patch

## Purpose
Add detailed timing instrumentation to `window.rs` to identify the exact O(N) operation.

## Data Collected So Far

**Instrumented Test Results** (5,000 records):
```
Record     0: 1.001ms   (1.00x baseline)
Record   500: 0.807ms   (0.81x)
Record  1000: 1.640ms   (1.64x)
Record  2000: 3.146ms   (3.15x)
Record  3000: 4.943ms   (4.94x)
Record  4000: 6.637ms   (6.64x)

Overall Growth Ratio: 7.94x ← O(N²) confirmed
Throughput: 242 rec/sec (target: 20,000)
```

## Instrumentation Code to Add

### Location 1: `process_windowed_query` function
**File**: `src/velostream/sql/execution/processors/window.rs`
**Lines**: 124-185

Add timing around each major operation:

```rust
pub fn process_windowed_query(
    query_id: &str,
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<Option<StreamRecord>, SqlError> {
    if let StreamingQuery::Select { window, .. } = query {
        if let Some(window_spec) = window {
            // ========== INSTRUMENTATION START ==========
            use std::time::Instant;
            let _total_start = Instant::now();

            // Extract event time first (minimal CPU overhead)
            let t1 = Instant::now();
            let event_time = Self::extract_event_time(record, window_spec.time_column());
            let extract_time = t1.elapsed();

            // Get or create window state using high-performance context management
            let t2 = Instant::now();
            let window_state = context.get_or_create_window_state(query_id, window_spec);
            let get_state_time = t2.elapsed();

            let buffer_size_before = window_state.buffer.len();

            // Add record to buffer (pre-allocated for performance)
            let t3 = Instant::now();
            window_state.add_record(record.clone());
            let add_record_time = t3.elapsed();

            // Phase 7: For EMIT CHANGES, emit per-record state changes
            let t4 = Instant::now();
            let is_emit_changes = Self::is_emit_changes(query);
            let group_by_cols = Self::get_group_by_columns(query);
            let check_emit_mode_time = t4.elapsed();

            // Check if window should emit
            let t5 = Instant::now();
            let should_emit = if is_emit_changes && group_by_cols.is_some() {
                true
            } else {
                Self::should_emit_window_state(window_state, event_time, window_spec)
            };
            let should_emit_time = t5.elapsed();

            // Log every 100 records or when buffer is large
            if buffer_size_before % 100 == 0 || buffer_size_before > 1000 {
                eprintln!(
                    "INSTR[{:5}]: extract={:?} get_state={:?} add_rec={:?} check_mode={:?} should_emit={:?} buf_size={}",
                    buffer_size_before,
                    extract_time,
                    get_state_time,
                    add_record_time,
                    check_emit_mode_time,
                    should_emit_time,
                    buffer_size_before + 1
                );
            }
            // ========== INSTRUMENTATION END ==========

            if should_emit {
                // ... rest of function
            }
            // ...
        }
    }
}
```

### Location 2: `add_record` in WindowState
**File**: `src/velostream/sql/execution/internal.rs`
**Lines**: 577-579

```rust
/// Add a record to the window buffer
pub fn add_record(&mut self, record: StreamRecord) {
    // INSTRUMENTATION
    use std::time::Instant;
    let t = Instant::now();
    let size_before = self.buffer.len();

    self.buffer.push(record);

    let elapsed = t.elapsed();
    if size_before % 100 == 0 || elapsed.as_micros() > 100 {
        eprintln!("WindowState::add_record: size={} time={:?}", size_before, elapsed);
    }
}
```

### Location 3: `get_or_create_window_state` in ProcessorContext
**File**: `src/velostream/sql/execution/processors/context.rs`
**Lines**: 210-241

```rust
pub fn get_or_create_window_state(
    &mut self,
    query_id: &str,
    window_spec: &crate::velostream::sql::ast::WindowSpec,
) -> &mut WindowState {
    // INSTRUMENTATION
    use std::time::Instant;
    let t = Instant::now();
    let num_states = self.persistent_window_states.len();

    // Check if window state already exists
    for (idx, (stored_query_id, state)) in self.persistent_window_states.iter().enumerate() {
        if stored_query_id == query_id {
            let elapsed = t.elapsed();
            if elapsed.as_micros() > 10 {
                eprintln!("get_or_create (found): idx={} states={} time={:?}", idx, num_states, elapsed);
            }
            // Mark as dirty for persistence
            if idx < 32 {
                self.dirty_window_states |= 1 << (idx as u32);
            }
            return &mut self.persistent_window_states[idx].1;
        }
    }

    // Create new state if not found (happens rarely)
    let new_state = WindowState::new(window_spec.clone());
    let new_idx = self.persistent_window_states.len();
    self.persistent_window_states.push((query_id.to_string(), new_state));

    let elapsed = t.elapsed();
    eprintln!("get_or_create (new): idx={} states={} time={:?}", new_idx, num_states, elapsed);

    // Mark new state as dirty
    if new_idx < 32 {
        self.dirty_window_states |= 1 << (new_idx as u32);
    }

    &mut self.persistent_window_states[new_idx].1
}
```

## How to Apply

1. **Backup original files**:
```bash
cp src/velostream/sql/execution/processors/window.rs src/velostream/sql/execution/processors/window.rs.backup
cp src/velostream/sql/execution/internal.rs src/velostream/sql/execution/internal.rs.backup
cp src/velostream/sql/execution/processors/context.rs src/velostream/sql/execution/processors/context.rs.backup
```

2. **Add instrumentation code** to the three locations above

3. **Run instrumented test**:
```bash
cargo test profile_tumbling_instrumented_standard_path -- --nocapture 2>&1 | grep -E "(INSTR|WindowState|get_or_create)" | head -100
```

4. **Analyze output** to identify which operation scales with buffer size

5. **Restore original files** after collecting data:
```bash
mv src/velostream/sql/execution/processors/window.rs.backup src/velostream/sql/execution/processors/window.rs
mv src/velostream/sql/execution/internal.rs.backup src/velostream/sql/execution/internal.rs
mv src/velostream/sql/execution/processors/context.rs.backup src/velostream/sql/execution/processors/context.rs
```

## Expected Output

If `add_record` is the bottleneck:
```
WindowState::add_record: size=0 time=15µs
WindowState::add_record: size=100 time=45µs      ← growing
WindowState::add_record: size=500 time=250µs     ← growing
WindowState::add_record: size=1000 time=500µs    ← O(N) pattern!
```

If `get_or_create_window_state` is the bottleneck:
```
get_or_create (found): idx=0 states=1 time=5µs
get_or_create (found): idx=0 states=1 time=150µs   ← shouldn't grow!
```

If neither shows growth, the bottleneck is elsewhere (likely hidden in Vec operations or context management).

## Analysis Checklist

After running instrumented code, check:

- [ ] Does `add_record` time correlate with buffer size?
- [ ] Does `get_or_create` time grow (shouldn't for single query)?
- [ ] Does `should_emit` check take noticeable time?
- [ ] Is there a correlation between any operation and buffer size?
- [ ] Are there any operations taking > 100µs that shouldn't?

## Next Steps Based on Results

**If add_record is O(N)**:
- Check if Vec::push is reallocating
- Look for hidden validation/scanning in add_record

**If get_or_create is O(N)**:
- Investigate why state lookup is degrading
- Check if dirty_window_states bit manipulation is expensive

**If no obvious bottleneck**:
- Add instrumentation to `should_emit_window_state`
- Profile Vec operations with more detail
- Check for hidden iterator operations
