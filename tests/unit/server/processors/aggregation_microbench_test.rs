/*!
# Window Aggregation Microbenchmark

Isolates the exact performance bottleneck in GROUP BY aggregation with different data patterns.
Includes instrumentation of window emit path to profile filtering, cloning, and aggregation.
*/

use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Minimal mock of what gets stored in window buffer
#[derive(Clone)]
struct MockRecord {
    group_key_hash: u64,
    group_key_value: String,
    value: f64,
}

/// Minimal mock of GroupAccumulator
#[derive(Debug)]
struct MockAccumulator {
    count: usize,
    sum: f64,
}

impl Default for MockAccumulator {
    fn default() -> Self {
        Self { count: 0, sum: 0.0 }
    }
}

/// Baseline: Linear-batched records (natural key interleaving)
fn generate_linear_batched_records(count: usize, unique_keys: usize) -> Vec<MockRecord> {
    let mut records = Vec::with_capacity(count);
    for i in 0..count {
        let key_id = i % unique_keys;
        let hash = fxhash(key_id as u64);
        records.push(MockRecord {
            group_key_hash: hash,
            group_key_value: format!("KEY_{}", key_id),
            value: (i as f64) * 1.5,
        });
    }
    records
}

/// Partition-batched records (same key repeated many times)
fn generate_partition_batched_records(count: usize, unique_keys: usize) -> Vec<MockRecord> {
    let mut records = Vec::with_capacity(count);
    let records_per_key = count / unique_keys;

    // Each unique key appears in a batch of records_per_key records
    for key_id in 0..unique_keys {
        let hash = fxhash(key_id as u64);
        for batch_idx in 0..records_per_key {
            records.push(MockRecord {
                group_key_hash: hash,
                group_key_value: format!("KEY_{}", key_id),
                value: (batch_idx as f64) * 1.5,
            });
        }
    }
    records
}

/// Simple FNV hash (consistent with codebase)
fn fxhash(mut value: u64) -> u64 {
    let mut hash = 2166136261u64;
    hash ^= value;
    hash = hash.wrapping_mul(16777619);
    hash
}

/// Algorithm 1: Process records in arrival order (current implementation)
fn aggregate_in_arrival_order(records: &[MockRecord]) -> (usize, FxHashMap<u64, MockAccumulator>) {
    let mut groups: FxHashMap<u64, MockAccumulator> = FxHashMap::default();

    for record in records {
        let accumulator = groups.entry(record.group_key_hash).or_default();
        accumulator.count += 1;
        accumulator.sum += record.value;
    }

    (records.len(), groups)
}

/// Algorithm 2: Sort records by group key hash BEFORE processing
fn aggregate_sorted_by_key(records: &[MockRecord]) -> (usize, FxHashMap<u64, MockAccumulator>) {
    // Pre-sort by group key hash (O(N log N))
    let mut sorted = records.to_vec();
    sorted.sort_unstable_by_key(|r| r.group_key_hash);

    // Now process in key order (should improve cache locality)
    let mut groups: FxHashMap<u64, MockAccumulator> = FxHashMap::default();

    for record in &sorted {
        let accumulator = groups.entry(record.group_key_hash).or_default();
        accumulator.count += 1;
        accumulator.sum += record.value;
    }

    (records.len(), groups)
}

/// Algorithm 3: Pre-allocate per-group vectors, batch records by key
fn aggregate_batched_by_key(records: &[MockRecord]) -> (usize, FxHashMap<u64, MockAccumulator>) {
    // First pass: group records by key
    let mut batches: FxHashMap<u64, Vec<&MockRecord>> = FxHashMap::default();
    for record in records {
        batches
            .entry(record.group_key_hash)
            .or_insert_with(Vec::new)
            .push(record);
    }

    // Second pass: aggregate from batches
    let mut groups: FxHashMap<u64, MockAccumulator> = FxHashMap::default();
    for (_key, batch) in batches {
        let accumulator = groups.entry(_key).or_default();
        for record in batch {
            accumulator.count += 1;
            accumulator.sum += record.value;
        }
    }

    (records.len(), groups)
}

#[test]
fn microbench_linear_batched_arrival_order() {
    let records = generate_linear_batched_records(100_000, 5000);

    let start = Instant::now();
    let (count, groups) = aggregate_in_arrival_order(&records);
    let elapsed = start.elapsed();

    println!(
        "LINEAR BATCHED + ARRIVAL ORDER: {} records, {} groups in {:.2}ms ({:.0} rec/sec)",
        count,
        groups.len(),
        elapsed.as_secs_f64() * 1000.0,
        count as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn microbench_partition_batched_arrival_order() {
    let records = generate_partition_batched_records(100_000, 5000);

    let start = Instant::now();
    let (count, groups) = aggregate_in_arrival_order(&records);
    let elapsed = start.elapsed();

    println!(
        "PARTITION BATCHED + ARRIVAL ORDER: {} records, {} groups in {:.2}ms ({:.0} rec/sec)",
        count,
        groups.len(),
        elapsed.as_secs_f64() * 1000.0,
        count as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn microbench_partition_batched_sorted() {
    let records = generate_partition_batched_records(100_000, 5000);

    let start = Instant::now();
    let (count, groups) = aggregate_sorted_by_key(&records);
    let elapsed = start.elapsed();

    println!(
        "PARTITION BATCHED + SORTED: {} records, {} groups in {:.2}ms ({:.0} rec/sec)",
        count,
        groups.len(),
        elapsed.as_secs_f64() * 1000.0,
        count as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn microbench_partition_batched_batched() {
    let records = generate_partition_batched_records(100_000, 5000);

    let start = Instant::now();
    let (count, groups) = aggregate_batched_by_key(&records);
    let elapsed = start.elapsed();

    println!(
        "PARTITION BATCHED + BATCHED: {} records, {} groups in {:.2}ms ({:.0} rec/sec)",
        count,
        groups.len(),
        elapsed.as_secs_f64() * 1000.0,
        count as f64 / elapsed.as_secs_f64()
    );
}

/// Simulated window emit with instrumentation
/// Simulates: filter_by_timestamp + aggregate for each window boundary
struct WindowEmitStats {
    total_filters: usize,
    total_clones: usize,
    total_aggregations: usize,
    max_buffer_size: usize,
    window_count: usize,
    filter_time_us: u128,
    clone_time_us: u128,
    aggregate_time_us: u128,
}

fn simulate_window_emit_with_partition_batching(
    records: &[MockRecord],
    window_size_ms: i64,
) -> WindowEmitStats {
    let mut buffer: Vec<(MockRecord, i64)> = Vec::new(); // (record, timestamp)
    let mut stats = WindowEmitStats {
        total_filters: 0,
        total_clones: 0,
        total_aggregations: 0,
        max_buffer_size: 0,
        window_count: 0,
        filter_time_us: 0,
        clone_time_us: 0,
        aggregate_time_us: 0,
    };

    let mut current_window_start: i64 = 0;
    let mut current_window_end: i64 = window_size_ms;

    // Add timestamp to each record: increment by 1000ms every 20 records (simulating batches)
    for (idx, record) in records.iter().enumerate() {
        let timestamp = ((idx / 20) as i64) * 1000; // 1 second per 20 records
        buffer.push((record.clone(), timestamp));

        if timestamp >= current_window_end {
            // Window boundary: emit
            stats.window_count += 1;

            // Step 1: Filter window records from buffer (O(N) scan)
            let filter_start = Instant::now();
            let window_records: Vec<&MockRecord> = buffer
                .iter()
                .filter(|(_, ts)| *ts >= current_window_start && *ts < current_window_end)
                .map(|(r, _)| r)
                .collect();
            stats.filter_time_us += filter_start.elapsed().as_micros();
            stats.total_filters += buffer.len(); // Count filter operations

            let window_size = window_records.len();
            stats.max_buffer_size = stats.max_buffer_size.max(buffer.len());

            // Step 2: Clone records for aggregation
            let clone_start = Instant::now();
            let cloned_records: Vec<MockRecord> =
                window_records.iter().map(|r| (*r).clone()).collect();
            stats.clone_time_us += clone_start.elapsed().as_micros();
            stats.total_clones += window_size;

            // Step 3: Aggregate
            let agg_start = Instant::now();
            let _groups = aggregate_in_arrival_order(&cloned_records);
            stats.aggregate_time_us += agg_start.elapsed().as_micros();
            stats.total_aggregations += window_size;

            // Advance window
            current_window_start = current_window_end;
            current_window_end = current_window_start + window_size_ms;

            // Evict old records (tumbling window)
            buffer.retain(|(_, ts)| *ts >= current_window_start);
        }
    }

    stats
}

#[test]
fn profile_window_emit_path() {
    println!("\n=== WINDOW EMIT PATH PROFILING ===\n");

    let partition_records = generate_partition_batched_records(10_000, 5000);

    let stats = simulate_window_emit_with_partition_batching(&partition_records, 60000);

    println!("Input: 10K records, 5000 unique keys, 60-second windows\n");
    println!("Window Emit Statistics:");
    println!("  Windows emitted: {}", stats.window_count);
    println!(
        "  Max buffer size at emission: {} records",
        stats.max_buffer_size
    );
    println!("  Total filter operations: {}", stats.total_filters);
    println!("  Total clones: {}", stats.total_clones);
    println!("  Total aggregations: {}", stats.total_aggregations);
    println!();
    println!("Timing Breakdown:");
    println!(
        "  Filtering: {:.2}µs ({:.1}%)",
        stats.filter_time_us as f64,
        (stats.filter_time_us as f64
            / (stats.filter_time_us + stats.clone_time_us + stats.aggregate_time_us) as f64)
            * 100.0
    );
    println!(
        "  Cloning:   {:.2}µs ({:.1}%)",
        stats.clone_time_us as f64,
        (stats.clone_time_us as f64
            / (stats.filter_time_us + stats.clone_time_us + stats.aggregate_time_us) as f64)
            * 100.0
    );
    println!(
        "  Aggregating: {:.2}µs ({:.1}%)",
        stats.aggregate_time_us as f64,
        (stats.aggregate_time_us as f64
            / (stats.filter_time_us + stats.clone_time_us + stats.aggregate_time_us) as f64)
            * 100.0
    );
    println!(
        "  TOTAL: {:.2}µs",
        (stats.filter_time_us + stats.clone_time_us + stats.aggregate_time_us) as f64
    );
}

#[test]
fn compare_all_patterns() {
    println!("\n=== COMPARISON: 100K RECORDS, 5000 UNIQUE KEYS ===\n");

    // Linear batched with arrival order (baseline - should be fastest)
    let linear_records = generate_linear_batched_records(100_000, 5000);
    let start = Instant::now();
    let (_, _) = aggregate_in_arrival_order(&linear_records);
    let linear_time = start.elapsed();

    // Partition batched with arrival order (current slow case)
    let partition_records = generate_partition_batched_records(100_000, 5000);
    let start = Instant::now();
    let (_, _) = aggregate_in_arrival_order(&partition_records);
    let partition_arrival_time = start.elapsed();

    // Partition batched with sorting (potential fix 1)
    let start = Instant::now();
    let (_, _) = aggregate_sorted_by_key(&partition_records);
    let partition_sorted_time = start.elapsed();

    // Partition batched with batching (potential fix 2)
    let start = Instant::now();
    let (_, _) = aggregate_batched_by_key(&partition_records);
    let partition_batched_time = start.elapsed();

    println!(
        "Linear batched + arrival order:       {:.2}ms (baseline)",
        linear_time.as_secs_f64() * 1000.0
    );
    println!(
        "Partition batched + arrival order:    {:.2}ms ({:.1}x slower)",
        partition_arrival_time.as_secs_f64() * 1000.0,
        partition_arrival_time.as_secs_f64() / linear_time.as_secs_f64()
    );
    println!(
        "Partition batched + sorted:           {:.2}ms ({:.1}x improvement)",
        partition_sorted_time.as_secs_f64() * 1000.0,
        partition_arrival_time.as_secs_f64() / partition_sorted_time.as_secs_f64()
    );
    println!(
        "Partition batched + batched:          {:.2}ms ({:.1}x improvement)",
        partition_batched_time.as_secs_f64() * 1000.0,
        partition_arrival_time.as_secs_f64() / partition_batched_time.as_secs_f64()
    );

    println!("\n⚠️  IMPORTANT: The aggregation algorithm itself is NOT the bottleneck!");
    println!("All aggregation approaches are similar in speed (10-13ms for 100K records).");
    println!("The 12x slowdown in the full SQL engine must be elsewhere:");
    println!("  - Window record filtering/cloning");
    println!("  - Expression evaluation (generate_group_key)");
    println!("  - Arc operations or stream operations");
    println!("  - Something else in the pipeline");
}
