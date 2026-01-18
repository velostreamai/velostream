//! Interval Stream-Stream JOIN Performance Benchmark - Tier 3 Operation
//!
//! **FR-085: Stream-Stream Joins Implementation**
//! - **Tier**: Tier 3 (Advanced)
//! - **Use Cases**: Temporal event correlation, clickstream analysis, fraud detection
//!
//! Interval stream-stream JOINs correlate events from two streams within a time window.
//! This benchmark tests the FR-085 `JoinCoordinator` implementation which provides:
//! - Dual state stores with time-based expiration
//! - Interval bounds (events must occur within specified time window)
//! - Watermark-based state cleanup
//! - Memory-bounded operation with configurable limits
//!
//! **Test Pattern**: Measures throughput for:
//! 1. Direct JoinCoordinator (raw join processing)
//! 2. JoinStateStore operations (store/lookup/expiration)
//! 3. End-to-end SQL execution (via StreamExecutionEngine)
//!
//! ## Interval Join Semantics
//!
//! For an interval join with bounds `[-1h, +24h]`:
//! - Left event at time T matches right events where: T - 1h <= right.time <= T + 24h
//! - This enables "order placed, shipment arrives within 24h" patterns

use serial_test::serial;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use velostream::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorConfig, JoinSide, JoinStateStore,
    JoinStateStoreConfig,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::parser::StreamingSqlParser;

use super::super::test_helpers::get_perf_record_count;

// ============================================================================
// Test Data Generation
// ============================================================================

/// Generate correlated order and shipment records for interval join testing
///
/// Creates orders and shipments where:
/// - Each order has order_id, customer_id, amount, and event_time
/// - Shipments reference orders via order_id with slight time offset
/// - ~80% of orders will have matching shipments within the interval
fn generate_interval_join_records(count: usize) -> (Vec<StreamRecord>, Vec<StreamRecord>) {
    let base_time = 1_700_000_000_000i64; // milliseconds

    // Generate orders
    let orders: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let order_id = format!("ORD-{:06}", i);
            let customer_id = (i % 1000) as i64;
            let amount = 100.0 + (i % 500) as f64;
            let event_time = base_time + (i as i64 * 1000); // 1 second apart

            fields.insert("order_id".to_string(), FieldValue::String(order_id));
            fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
            fields.insert("amount".to_string(), FieldValue::Float(amount));
            fields.insert("event_time".to_string(), FieldValue::Integer(event_time));

            StreamRecord::new(fields)
        })
        .collect();

    // Generate shipments (80% match rate, with time offset)
    let shipments: Vec<StreamRecord> = (0..count)
        .filter(|i| i % 5 != 0) // 80% of orders get shipments
        .map(|i| {
            let mut fields = HashMap::new();
            let shipment_id = format!("SHIP-{:06}", i);
            let order_id = format!("ORD-{:06}", i);
            let carrier = match i % 3 {
                0 => "FedEx",
                1 => "UPS",
                _ => "USPS",
            };
            // Shipment arrives 1-12 hours after order
            let time_offset = ((i % 12) + 1) as i64 * 3_600_000; // 1-12 hours in ms
            let event_time = base_time + (i as i64 * 1000) + time_offset;

            fields.insert("shipment_id".to_string(), FieldValue::String(shipment_id));
            fields.insert("order_id".to_string(), FieldValue::String(order_id));
            fields.insert("carrier".to_string(), FieldValue::String(carrier.to_string()));
            fields.insert("event_time".to_string(), FieldValue::Integer(event_time));

            StreamRecord::new(fields)
        })
        .collect();

    (orders, shipments)
}

/// Generate high-cardinality records for stress testing state stores
fn generate_high_cardinality_records(count: usize) -> (Vec<StreamRecord>, Vec<StreamRecord>) {
    let base_time = 1_700_000_000_000i64;

    // Left side: unique keys (worst case for state store)
    let left: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert(
                "key".to_string(),
                FieldValue::String(format!("L-{:08}", i)),
            );
            fields.insert(
                "event_time".to_string(),
                FieldValue::Integer(base_time + (i as i64 * 100)),
            );
            fields.insert("value".to_string(), FieldValue::Integer(i as i64));
            StreamRecord::new(fields)
        })
        .collect();

    // Right side: some overlap with left keys
    let right: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            // 50% unique keys, 50% matching left keys
            let key = if i % 2 == 0 {
                format!("L-{:08}", i / 2)
            } else {
                format!("R-{:08}", i)
            };
            fields.insert("key".to_string(), FieldValue::String(key));
            fields.insert(
                "event_time".to_string(),
                FieldValue::Integer(base_time + (i as i64 * 100) + 5000), // 5 second offset
            );
            fields.insert("data".to_string(), FieldValue::Integer(i as i64 * 10));
            StreamRecord::new(fields)
        })
        .collect();

    (left, right)
}

// ============================================================================
// Benchmark Results
// ============================================================================

#[derive(Clone, Debug)]
struct BenchmarkResult {
    name: String,
    throughput_rec_per_sec: f64,
    records_processed: usize,
    matches_produced: usize,
    duration_ms: f64,
}

impl BenchmarkResult {
    fn print(&self) {
        println!(
            "  {:<35} {:>12.0} rec/sec | {:>8} records | {:>6} matches | {:>8.2}ms",
            self.name, self.throughput_rec_per_sec, self.records_processed, self.matches_produced, self.duration_ms
        );
    }
}

// ============================================================================
// JoinCoordinator Benchmarks
// ============================================================================

/// Benchmark direct JoinCoordinator processing (no SQL overhead)
async fn benchmark_join_coordinator(
    orders: Vec<StreamRecord>,
    shipments: Vec<StreamRecord>,
    name: &str,
) -> BenchmarkResult {
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -3_600_000,  // 1 hour before
        86_400_000,  // 24 hours after
    )
    .with_retention(Duration::from_secs(86400));

    let mut coordinator = JoinCoordinator::new(config);
    let total_records = orders.len() + shipments.len();
    let mut total_matches = 0;

    let start = Instant::now();

    // Process orders (left side)
    for record in orders.iter() {
        match coordinator.process(JoinSide::Left, record.clone()) {
            Ok(matches) => total_matches += matches.len(),
            Err(_) => {}
        }
    }

    // Process shipments (right side)
    for record in shipments.iter() {
        match coordinator.process(JoinSide::Right, record.clone()) {
            Ok(matches) => total_matches += matches.len(),
            Err(_) => {}
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64) / elapsed.as_secs_f64();

    BenchmarkResult {
        name: name.to_string(),
        throughput_rec_per_sec: throughput,
        records_processed: total_records,
        matches_produced: total_matches,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

/// Benchmark JoinCoordinator with memory limits (stress test)
async fn benchmark_join_coordinator_with_limits(
    orders: Vec<StreamRecord>,
    shipments: Vec<StreamRecord>,
    max_records: usize,
) -> BenchmarkResult {
    let join_config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -3_600_000,
        86_400_000,
    )
    .with_retention(Duration::from_secs(3600));

    let coordinator_config = JoinCoordinatorConfig::new(join_config)
        .with_max_records(max_records);

    let mut coordinator = JoinCoordinator::with_config(coordinator_config);
    let total_records = orders.len() + shipments.len();
    let mut total_matches = 0;

    let start = Instant::now();

    for record in orders.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Left, record.clone()) {
            total_matches += matches.len();
        }
    }

    for record in shipments.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Right, record.clone()) {
            total_matches += matches.len();
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64) / elapsed.as_secs_f64();

    BenchmarkResult {
        name: format!("JoinCoordinator (limit={})", max_records),
        throughput_rec_per_sec: throughput,
        records_processed: total_records,
        matches_produced: total_matches,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

// ============================================================================
// JoinStateStore Benchmarks
// ============================================================================

/// Benchmark raw state store operations
async fn benchmark_state_store_operations(count: usize) -> BenchmarkResult {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));
    let base_time = 1_700_000_000_000i64;

    let start = Instant::now();

    // Store operations
    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        let record = StreamRecord::new(fields);
        let key = format!("key-{}", i % 1000); // 1000 unique keys
        store.store(&key, record, base_time + (i as i64 * 100));
    }

    // Lookup operations
    let mut lookup_count = 0;
    for i in 0..count {
        let key = format!("key-{}", i % 1000);
        let time = base_time + (i as i64 * 100);
        let matches = store.lookup(&key, time - 10000, time + 10000);
        lookup_count += matches.len();
    }

    let elapsed = start.elapsed();
    let throughput = (count as f64 * 2.0) / elapsed.as_secs_f64(); // store + lookup

    BenchmarkResult {
        name: "StateStore (store+lookup)".to_string(),
        throughput_rec_per_sec: throughput,
        records_processed: count * 2,
        matches_produced: lookup_count,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

/// Benchmark state store with watermark advancement (expiration)
async fn benchmark_state_store_with_expiration(count: usize) -> BenchmarkResult {
    let mut store = JoinStateStore::with_retention_ms(10_000); // 10 second retention
    let base_time = 1_700_000_000_000i64;

    let start = Instant::now();
    let mut total_expired = 0;

    // Store records with advancing time
    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        let record = StreamRecord::new(fields);
        let key = format!("key-{}", i % 100);
        let event_time = base_time + (i as i64 * 100);

        store.store(&key, record, event_time);

        // Advance watermark every 100 records
        if i % 100 == 99 {
            let watermark = event_time - 10_000; // 10 seconds behind
            total_expired += store.advance_watermark(watermark);
        }
    }

    let elapsed = start.elapsed();
    let throughput = (count as f64) / elapsed.as_secs_f64();

    BenchmarkResult {
        name: "StateStore (with expiration)".to_string(),
        throughput_rec_per_sec: throughput,
        records_processed: count,
        matches_produced: total_expired,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

// ============================================================================
// SQL End-to-End Benchmarks
// ============================================================================

/// SQL query for interval stream-stream join
/// Note: This uses standard JOIN syntax; interval semantics are handled by JoinCoordinator
const INTERVAL_JOIN_SQL: &str = r#"
    SELECT
        o.order_id,
        o.customer_id,
        o.amount,
        s.shipment_id,
        s.carrier
    FROM orders o
    JOIN shipments s ON o.order_id = s.order_id
"#;

/// Benchmark end-to-end SQL execution (sync path)
async fn benchmark_sql_engine_sync(
    orders: Vec<StreamRecord>,
    shipments: Vec<StreamRecord>,
) -> BenchmarkResult {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = match parser.parse(INTERVAL_JOIN_SQL) {
        Ok(q) => q,
        Err(_) => {
            return BenchmarkResult {
                name: "SQL Engine (sync) - PARSE ERROR".to_string(),
                throughput_rec_per_sec: 0.0,
                records_processed: 0,
                matches_produced: 0,
                duration_ms: 0.0,
            };
        }
    };

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let total_records = orders.len() + shipments.len();
    let mut results_count = 0;

    let start = Instant::now();

    // Process orders
    for record in orders.iter() {
        if let Ok(results) = engine.execute_with_record_sync(&parsed_query, record) {
            results_count += results.len();
        }
    }

    // Process shipments
    for record in shipments.iter() {
        if let Ok(results) = engine.execute_with_record_sync(&parsed_query, record) {
            results_count += results.len();
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64) / elapsed.as_secs_f64();

    BenchmarkResult {
        name: "SQL Engine (sync)".to_string(),
        throughput_rec_per_sec: throughput,
        records_processed: total_records,
        matches_produced: results_count,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

/// Benchmark end-to-end SQL execution (async path)
async fn benchmark_sql_engine_async(
    orders: Vec<StreamRecord>,
    shipments: Vec<StreamRecord>,
) -> BenchmarkResult {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = match parser.parse(INTERVAL_JOIN_SQL) {
        Ok(q) => q,
        Err(_) => {
            return BenchmarkResult {
                name: "SQL Engine (async) - PARSE ERROR".to_string(),
                throughput_rec_per_sec: 0.0,
                records_processed: 0,
                matches_produced: 0,
                duration_ms: 0.0,
            };
        }
    };

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let total_records = orders.len() + shipments.len();

    let start = Instant::now();

    // Process all records
    for record in orders.iter() {
        let _ = engine.execute_with_record(&parsed_query, record).await;
    }
    for record in shipments.iter() {
        let _ = engine.execute_with_record(&parsed_query, record).await;
    }

    // Drain results from channel
    let mut results_count = 0;
    while rx.try_recv().is_ok() {
        results_count += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64) / elapsed.as_secs_f64();

    BenchmarkResult {
        name: "SQL Engine (async)".to_string(),
        throughput_rec_per_sec: throughput,
        records_processed: total_records,
        matches_produced: results_count,
        duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

// ============================================================================
// Main Benchmark Tests
// ============================================================================

/// Primary benchmark: Compare all interval join implementations
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_interval_stream_join_performance() {
    let record_count = get_perf_record_count();
    let (orders, shipments) = generate_interval_join_records(record_count);

    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("📊 INTERVAL STREAM-STREAM JOIN BENCHMARK (FR-085)");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("  Records: {} orders + {} shipments = {} total",
             orders.len(), shipments.len(), orders.len() + shipments.len());
    println!("  Interval: [-1h, +24h] (shipment within 24h of order)");
    println!("───────────────────────────────────────────────────────────────────────────\n");

    // JoinCoordinator benchmarks
    println!("🔧 JoinCoordinator (Direct API):");
    let coord_result = benchmark_join_coordinator(
        orders.clone(),
        shipments.clone(),
        "JoinCoordinator (unlimited)",
    ).await;
    coord_result.print();

    let coord_limited = benchmark_join_coordinator_with_limits(
        orders.clone(),
        shipments.clone(),
        record_count / 2,
    ).await;
    coord_limited.print();

    // StateStore benchmarks
    println!("\n📦 JoinStateStore Operations:");
    let store_result = benchmark_state_store_operations(record_count).await;
    store_result.print();

    let store_expiry = benchmark_state_store_with_expiration(record_count).await;
    store_expiry.print();

    // SQL Engine benchmarks
    println!("\n🗄️  SQL Engine (End-to-End):");
    let sql_sync = benchmark_sql_engine_sync(orders.clone(), shipments.clone()).await;
    sql_sync.print();

    let sql_async = benchmark_sql_engine_async(orders.clone(), shipments.clone()).await;
    sql_async.print();

    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("🚀 BENCHMARK_RESULT | interval_stream_join | tier3 | Coordinator: {:.0} | StateStore: {:.0} | SQL Sync: {:.0} | SQL Async: {:.0}",
             coord_result.throughput_rec_per_sec,
             store_result.throughput_rec_per_sec,
             sql_sync.throughput_rec_per_sec,
             sql_async.throughput_rec_per_sec);
    println!("═══════════════════════════════════════════════════════════════════════════\n");
}

/// Stress test: High cardinality keys
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_interval_join_high_cardinality() {
    let record_count = get_perf_record_count();
    let (left, right) = generate_high_cardinality_records(record_count);

    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("📊 HIGH CARDINALITY JOIN STRESS TEST");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("  Left records: {} (unique keys)", left.len());
    println!("  Right records: {} (50% overlap)", right.len());
    println!("───────────────────────────────────────────────────────────────────────────\n");

    let config = JoinConfig::interval_ms(
        "left",
        "right",
        vec![("key".to_string(), "key".to_string())],
        -10_000,  // 10 seconds before
        10_000,   // 10 seconds after
    )
    .with_retention(Duration::from_secs(60));

    let mut coordinator = JoinCoordinator::new(config);
    let total_records = left.len() + right.len();
    let mut total_matches = 0;

    let start = Instant::now();

    for record in left.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Left, record.clone()) {
            total_matches += matches.len();
        }
    }

    for record in right.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Right, record.clone()) {
            total_matches += matches.len();
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64) / elapsed.as_secs_f64();
    let stats = coordinator.stats();

    println!("  Throughput:       {:.0} rec/sec", throughput);
    println!("  Total records:    {}", total_records);
    println!("  Matches produced: {}", total_matches);
    println!("  Left store size:  {}", stats.left_store_size);
    println!("  Right store size: {}", stats.right_store_size);
    println!("  Duration:         {:.2}ms", elapsed.as_secs_f64() * 1000.0);

    println!("\n🚀 BENCHMARK_RESULT | high_cardinality_join | tier3 | Throughput: {:.0} | Matches: {}",
             throughput, total_matches);
    println!("═══════════════════════════════════════════════════════════════════════════\n");
}

/// Memory pressure test: Verify bounded memory operation
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_interval_join_memory_bounded() {
    let record_count = get_perf_record_count();
    let (orders, shipments) = generate_interval_join_records(record_count);

    // Very restrictive memory limit
    let max_records = 100;

    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("📊 MEMORY-BOUNDED JOIN TEST");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("  Input: {} records, Limit: {} per store", orders.len() + shipments.len(), max_records);
    println!("───────────────────────────────────────────────────────────────────────────\n");

    let join_config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -3_600_000,
        86_400_000,
    );

    let coordinator_config = JoinCoordinatorConfig::new(join_config)
        .with_store_config(JoinStateStoreConfig::with_limits(max_records, 0));

    let mut coordinator = JoinCoordinator::with_config(coordinator_config);
    let mut total_matches = 0;

    let start = Instant::now();

    for record in orders.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Left, record.clone()) {
            total_matches += matches.len();
        }
    }

    for record in shipments.iter() {
        if let Ok(matches) = coordinator.process(JoinSide::Right, record.clone()) {
            total_matches += matches.len();
        }
    }

    let elapsed = start.elapsed();
    let stats = coordinator.stats();

    println!("  Duration:         {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Matches produced: {}", total_matches);
    println!("  Left store size:  {} (limit: {})", stats.left_store_size, max_records);
    println!("  Right store size: {} (limit: {})", stats.right_store_size, max_records);
    println!("  Left evictions:   {}", stats.left_evictions);
    println!("  Right evictions:  {}", stats.right_evictions);

    // Verify memory bounds were respected
    assert!(
        stats.left_store_size <= max_records,
        "Left store exceeded limit: {} > {}",
        stats.left_store_size,
        max_records
    );
    assert!(
        stats.right_store_size <= max_records,
        "Right store exceeded limit: {} > {}",
        stats.right_store_size,
        max_records
    );

    println!("\n✅ Memory bounds verified: stores stayed within {} record limit", max_records);
    println!("═══════════════════════════════════════════════════════════════════════════\n");
}
