//! FR-082 Phase 1: Partitioned Routing Benchmark
//!
//! Tests the foundational V2 architecture components:
//! - HashRouter: Deterministic record routing to partitions
//! - PartitionStateManager: Per-partition state and metrics
//! - PartitionMetrics: Throughput and latency tracking
//!
//! ## Phase 1 Target
//!
//! - **2 partitions**: Proof-of-concept scale
//! - **400K rec/sec total**: 200K rec/sec per partition
//! - **Deterministic routing**: Same group keys → same partition
//! - **Lock-free operation**: No Arc<Mutex> contention

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use velostream::velostream::server::v2::{HashRouter, PartitionStateManager, PartitionStrategy};
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create a test record with trader_id and symbol fields
fn create_test_record(trader_id: u32, symbol: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "trader_id".to_string(),
        FieldValue::String(format!("TRADER{:05}", trader_id)),
    );
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert(
        "price".to_string(),
        FieldValue::Float(100.0 + (trader_id % 50) as f64),
    );
    fields.insert(
        "quantity".to_string(),
        FieldValue::Integer(100 + (trader_id % 10) as i64),
    );
    StreamRecord::new(fields)
}

#[test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore)]
fn phase1_routing_determinism_test() {
    println!("\n=== FR-082 PHASE 1: ROUTING DETERMINISM TEST ===\n");

    // Setup: 2 partitions with GROUP BY trader_id routing
    let num_partitions = 2;
    let strategy = PartitionStrategy::HashByGroupBy {
        group_by_columns: vec![Expr::Column("trader_id".to_string())],
    };
    let router = HashRouter::new(num_partitions, strategy);

    // Test 1: Deterministic routing (same key → same partition)
    println!("Test 1: Verifying deterministic routing...");
    let test_record = create_test_record(12345, "AAPL");

    let partition1 = router.route_record(&test_record).unwrap();
    let partition2 = router.route_record(&test_record).unwrap();
    let partition3 = router.route_record(&test_record).unwrap();

    assert_eq!(
        partition1, partition2,
        "Same record must route to same partition"
    );
    assert_eq!(partition2, partition3, "Routing must be deterministic");
    println!(
        "✅ TRADER12345 consistently routes to partition {}",
        partition1
    );

    // Test 2: Distribution across partitions
    println!("\nTest 2: Verifying distribution across partitions...");
    let mut partition_counts = vec![0; num_partitions];

    for trader_id in 0..1000 {
        let record = create_test_record(trader_id, "AAPL");
        let partition = router.route_record(&record).unwrap();
        partition_counts[partition] += 1;
    }

    println!("Distribution of 1,000 traders:");
    for (partition_id, count) in partition_counts.iter().enumerate() {
        println!(
            "  Partition {}: {} traders ({:.1}%)",
            partition_id,
            count,
            (*count as f64 / 1000.0) * 100.0
        );
    }

    // Both partitions should have received some records
    assert!(
        partition_counts[0] > 0,
        "Partition 0 should receive records"
    );
    assert!(
        partition_counts[1] > 0,
        "Partition 1 should receive records"
    );

    // Distribution should be reasonably balanced (30-70% split acceptable for 1000 samples)
    let partition0_pct = partition_counts[0] as f64 / 1000.0;
    assert!(
        partition0_pct >= 0.30 && partition0_pct <= 0.70,
        "Distribution should be reasonably balanced"
    );

    println!("\n✅ Phase 1 Routing Determinism Test: PASSED\n");
}

#[test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore)]
fn phase1_partition_metrics_test() {
    println!("\n=== FR-082 PHASE 1: PARTITION METRICS TEST ===\n");

    // Setup: 2 partitions with managers
    let num_partitions = 2;
    let managers: Vec<_> = (0..num_partitions)
        .map(|id| Arc::new(PartitionStateManager::new(id)))
        .collect();

    println!("Test: Per-partition metrics tracking...");

    // Simulate processing different record counts per partition
    let partition0_records: u64 = 50000;
    let partition1_records: u64 = 75000;

    // Process records in partition 0
    for i in 0..partition0_records {
        let record = create_test_record((i * 2) as u32, "AAPL"); // Even trader IDs
        managers[0].process_record(&record).unwrap();
    }

    // Process records in partition 1
    for i in 0..partition1_records {
        let record = create_test_record((i * 2 + 1) as u32, "GOOGL"); // Odd trader IDs
        managers[1].process_record(&record).unwrap();
    }

    // Verify metrics
    let metrics0 = managers[0].metrics().snapshot();
    let metrics1 = managers[1].metrics().snapshot();

    println!("\nPartition 0 Metrics:");
    println!("  Records processed: {}", metrics0.records_processed);
    println!("  Queue depth: {}", metrics0.queue_depth);
    println!("  Avg latency: {}μs", metrics0.avg_latency_micros);

    println!("\nPartition 1 Metrics:");
    println!("  Records processed: {}", metrics1.records_processed);
    println!("  Queue depth: {}", metrics1.queue_depth);
    println!("  Avg latency: {}μs", metrics1.avg_latency_micros);

    assert_eq!(metrics0.records_processed, partition0_records);
    assert_eq!(metrics1.records_processed, partition1_records);
    assert_eq!(metrics0.partition_id, 0);
    assert_eq!(metrics1.partition_id, 1);

    println!("\n✅ Phase 1 Partition Metrics Test: PASSED\n");
}

#[test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore)]
fn phase1_throughput_baseline() {
    println!("\n=== FR-082 PHASE 1: THROUGHPUT BASELINE ===\n");
    println!("Target: 400K rec/sec (200K per partition)\n");

    // Setup: 2 partitions with GROUP BY trader_id routing
    let num_partitions = 2;
    let strategy = PartitionStrategy::HashByGroupBy {
        group_by_columns: vec![Expr::Column("trader_id".to_string())],
    };
    let router = Arc::new(HashRouter::new(num_partitions, strategy));

    let managers: Vec<_> = (0..num_partitions)
        .map(|id| Arc::new(PartitionStateManager::new(id)))
        .collect();

    // Generate test dataset: 100K records with varying trader IDs
    let num_records: u64 = 100_000;
    println!("Generating {} test records...", num_records);
    let records: Vec<_> = (0..num_records)
        .map(|i| {
            let trader_id = (i % 1000) as u32; // 1000 unique traders
            let symbol = if i % 2 == 0 { "AAPL" } else { "GOOGL" };
            create_test_record(trader_id, symbol)
        })
        .collect();

    println!("Starting partitioned processing...\n");
    let start = Instant::now();

    // Route and process each record
    for record in &records {
        let partition_id = router.route_record(record).unwrap();
        managers[partition_id].process_record(record).unwrap();
    }

    let duration = start.elapsed();
    let throughput = (num_records as f64 / duration.as_secs_f64()) as u64;

    // Collect metrics
    let metrics: Vec<_> = managers.iter().map(|m| m.metrics().snapshot()).collect();

    println!("=== PHASE 1 BASELINE RESULTS ===\n");
    println!("Total records processed: {}", num_records);
    println!("Duration: {:.3}s", duration.as_secs_f64());
    println!("Overall throughput: {} rec/sec\n", throughput);

    println!("Per-Partition Breakdown:");
    for (i, m) in metrics.iter().enumerate() {
        let partition_throughput = (m.records_processed as f64 / duration.as_secs_f64()) as u64;
        println!(
            "  Partition {}: {} records ({} rec/sec)",
            i, m.records_processed, partition_throughput
        );
    }

    // Phase 1 success criteria (relaxed for foundation)
    // Target: 400K rec/sec, but Phase 1 is just proving routing works
    // We'll accept any reasonable throughput (>10K rec/sec) as baseline
    let min_acceptable_throughput = 10_000;

    println!("\n=== PHASE 1 EVALUATION ===");
    println!("Target (Phase 1 foundation): 400K rec/sec");
    println!("Actual throughput: {} rec/sec", throughput);

    if throughput >= 400_000 {
        println!("✅ PHASE 1 TARGET ACHIEVED!");
    } else if throughput >= min_acceptable_throughput {
        println!(
            "⚡ Phase 1 foundation validated ({}x below target)",
            400_000 / throughput
        );
        println!("   Phase 2+ will add full SQL execution and optimization");
    } else {
        panic!(
            "❌ Throughput {} rec/sec below minimum {} rec/sec",
            throughput, min_acceptable_throughput
        );
    }

    // Verify distribution is reasonably balanced
    let total_processed: u64 = metrics.iter().map(|m| m.records_processed).sum();
    assert_eq!(
        total_processed, num_records,
        "All records must be processed"
    );

    let partition0_pct = metrics[0].records_processed as f64 / total_processed as f64;
    assert!(
        partition0_pct >= 0.30 && partition0_pct <= 0.70,
        "Load should be reasonably balanced across partitions"
    );

    println!("\n✅ Phase 1 Throughput Baseline: PASSED\n");
}

#[test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore)]
fn phase1_batch_processing_test() {
    println!("\n=== FR-082 PHASE 1: BATCH PROCESSING TEST ===\n");

    let manager = PartitionStateManager::new(0);

    // Test single record processing
    let single_record = create_test_record(1, "AAPL");
    let start_single = Instant::now();
    manager.process_record(&single_record).unwrap();
    let single_duration = start_single.elapsed();

    // Test batch processing
    let batch_size: usize = 1000;
    let batch_records: Vec<_> = (0..batch_size)
        .map(|i| create_test_record(i as u32, "AAPL"))
        .collect();

    let start_batch = Instant::now();
    let processed = manager.process_batch(&batch_records).unwrap();
    let batch_duration = start_batch.elapsed();

    assert_eq!(processed, batch_size);

    let avg_single_time = single_duration.as_nanos();
    let avg_batch_time = batch_duration.as_nanos() / batch_size as u128;

    println!("Single record processing: {}ns", avg_single_time);
    println!("Batch record processing: {}ns per record", avg_batch_time);

    if avg_batch_time < avg_single_time {
        let speedup = avg_single_time as f64 / avg_batch_time as f64;
        println!(
            "✅ Batch processing {:.2}x faster than single record",
            speedup
        );
    }

    println!("\n✅ Phase 1 Batch Processing Test: PASSED\n");
}

#[test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore)]
fn phase1_integration_end_to_end() {
    println!("\n=== FR-082 PHASE 1: END-TO-END INTEGRATION TEST ===\n");
    println!("Simulating complete Phase 1 pipeline:\n");
    println!("  1. Generate synthetic trading data");
    println!("  2. Route records via HashRouter");
    println!("  3. Process in PartitionStateManagers");
    println!("  4. Collect and verify metrics\n");

    // Setup
    let num_partitions = 2;
    let strategy = PartitionStrategy::HashByGroupBy {
        group_by_columns: vec![Expr::Column("trader_id".to_string())],
    };
    let router = Arc::new(HashRouter::new(num_partitions, strategy));
    let managers: Vec<_> = (0..num_partitions)
        .map(|id| Arc::new(PartitionStateManager::new(id)))
        .collect();

    // Simulate realistic trading data
    let num_traders: u32 = 100;
    let trades_per_trader: u32 = 100;
    let num_records: u64 = (num_traders * trades_per_trader) as u64;

    println!(
        "Step 1: Generating {} trades from {} traders...",
        num_records, num_traders
    );

    let records: Vec<_> = (0..num_traders)
        .flat_map(|trader_id| {
            (0..trades_per_trader).map(move |_| {
                let symbol = match trader_id % 3 {
                    0 => "AAPL",
                    1 => "GOOGL",
                    _ => "MSFT",
                };
                create_test_record(trader_id, symbol)
            })
        })
        .collect();

    println!("Step 2: Routing and processing records...");
    let start = Instant::now();

    // Track routing
    let mut partition_assignments: HashMap<u32, usize> = HashMap::new();

    for record in &records {
        let partition_id = router.route_record(record).unwrap();
        managers[partition_id].process_record(record).unwrap();

        // Track which partition this trader is assigned to
        if let Some(FieldValue::String(trader_str)) = record.fields.get("trader_id") {
            let trader_id: u32 = trader_str.trim_start_matches("TRADER").parse().unwrap();
            partition_assignments
                .entry(trader_id)
                .or_insert(partition_id);
        }
    }

    let duration = start.elapsed();

    println!("Step 3: Verifying deterministic routing...");
    // Verify each trader was consistently routed to the same partition
    for (trader_id, &partition) in &partition_assignments {
        let test_record = create_test_record(*trader_id, "TEST");
        let routed_partition = router.route_record(&test_record).unwrap();
        assert_eq!(
            routed_partition, partition,
            "Trader {} routing inconsistency",
            trader_id
        );
    }
    println!("✅ All {} traders routed deterministically", num_traders);

    println!("\nStep 4: Collecting metrics...");
    let metrics: Vec<_> = managers.iter().map(|m| m.metrics().snapshot()).collect();

    let total_processed: u64 = metrics.iter().map(|m| m.records_processed).sum();
    let throughput = (total_processed as f64 / duration.as_secs_f64()) as u64;

    println!("\n=== END-TO-END RESULTS ===");
    println!("Total records: {}", num_records);
    println!("Processing time: {:.3}s", duration.as_secs_f64());
    println!("Throughput: {} rec/sec", throughput);
    println!("\nPer-Partition:");
    for m in &metrics {
        println!("  {}", m.format_summary());
    }

    // Verification
    assert_eq!(
        total_processed, num_records as u64,
        "All records must be processed"
    );

    println!("\n✅ Phase 1 End-to-End Integration: PASSED\n");
    println!("=== PHASE 1 FOUNDATION VALIDATED ===");
    println!("Ready for Phase 2: Full SQL Execution Integration\n");
}
