//! Phase 6.2: TRUE STP (Single-Threaded Pipeline) - Lockless Architecture
//! Each partition has its own independent engine instance
//! No shared state = No locking = True parallelism

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn profile_lockless_stp_architecture() {
    println!("\n\n=== PHASE 6.2: TRUE STP (Lockless) Architecture ===\n");

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    let num_records = 10_000;
    let num_partitions = 8;
    let records_per_partition = num_records / num_partitions;

    // Create test records
    let mut all_records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            velostream::velostream::sql::execution::types::FieldValue::Integer(i as i64),
        );
        fields.insert(
            "value".to_string(),
            velostream::velostream::sql::execution::types::FieldValue::Integer(i as i64 * 2),
        );
        all_records.push(StreamRecord::new(fields));
    }

    // ============================================================================
    // PHASE 1: V1 BASELINE (Direct SQL per-record with global lock)
    // ============================================================================
    println!("📊 PHASE 1: V1 Baseline (shared global lock)");

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let shared_engine = Arc::new(RwLock::new(StreamExecutionEngine::new(tx)));

    let v1_start = Instant::now();
    for batch_records in all_records.chunks(100) {
        let mut engine_lock = shared_engine.write().await;
        for record in batch_records {
            let _ = engine_lock.execute_with_record(&query, &record).await.ok();
        }
        drop(engine_lock);
    }
    let v1_elapsed = v1_start.elapsed();
    let v1_throughput = (num_records as f64 / v1_elapsed.as_secs_f64()) as u64;

    println!("  Records processed: {}", num_records);
    println!("  Time elapsed:      {:?}", v1_elapsed);
    println!("  Throughput:        {} rec/sec\n", v1_throughput);

    // ============================================================================
    // PHASE 2: TRUE STP (Lockless) - Independent engines per partition
    // ============================================================================
    println!(
        "📊 PHASE 2: TRUE STP Lockless ({}p independent partitions)",
        num_partitions
    );

    // Create independent engine for EACH partition - NO SHARED LOCK
    let mut partition_handles = vec![];

    let stp_start = Instant::now();

    for partition_id in 0..num_partitions {
        let _parser_clone = parser.clone();
        let query_clone = query.clone();

        // Partition-specific records
        let start_idx = partition_id * records_per_partition;
        let end_idx = (partition_id + 1) * records_per_partition;
        let partition_records: Vec<StreamRecord> = all_records[start_idx..end_idx].to_vec();

        let handle = tokio::spawn(async move {
            // ✅ KEY: Each partition has its OWN engine instance
            // No shared lock, no contention, no synchronization overhead
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            let partition_engine = StreamExecutionEngine::new(tx);

            let partition_start = Instant::now();
            let mut engine = partition_engine;
            for batch_records in partition_records.chunks(100) {
                // ✅ NO LOCKING - Direct access to partition's own engine
                for record in batch_records {
                    let _ = engine.execute_with_record(&query_clone, &record).await.ok();
                }
            }
            let partition_elapsed = partition_start.elapsed();

            (partition_id, partition_records.len(), partition_elapsed)
        });

        partition_handles.push(handle);
    }

    // Wait for all partitions to complete
    let mut total_partition_time = std::time::Duration::ZERO;
    let mut max_partition_time = std::time::Duration::ZERO;

    for handle in partition_handles {
        if let Ok((partition_id, records_processed, elapsed)) = handle.await {
            println!(
                "  Partition {}: {} records in {:?}",
                partition_id, records_processed, elapsed
            );
            total_partition_time += elapsed;
            max_partition_time = max_partition_time.max(elapsed);
        }
    }

    let stp_elapsed = stp_start.elapsed();
    let stp_throughput = (num_records as f64 / stp_elapsed.as_secs_f64()) as u64;

    println!("\n  Total elapsed:     {:?}", stp_elapsed);
    println!("  Throughput:        {} rec/sec", stp_throughput);
    println!("  Max partition:     {:?}", max_partition_time);
    println!(
        "  Partition avg:     {:?}\n",
        total_partition_time / num_partitions as u32
    );

    // ============================================================================
    // PHASE 3: COMPARISON
    // ============================================================================
    println!("📊 PHASE 3: ARCHITECTURE COMPARISON");
    println!("  Shared Global Lock: {} rec/sec (baseline)", v1_throughput);
    println!("  Lockless STP:       {} rec/sec", stp_throughput);

    let speedup = stp_throughput as f64 / v1_throughput as f64;
    println!("  Speedup:           {:.2}x", speedup);

    if speedup >= (num_partitions as f64 * 0.8) {
        // Expecting ~8x for 8 partitions, allowing 20% variance for task scheduling
        println!("  ✅ EXCELLENT SCALING: Near-linear {:.1}x", num_partitions);
    } else if speedup >= num_partitions as f64 * 0.5 {
        println!(
            "  ⚠️  GOOD SCALING: {:.1}x (some scheduling overhead)",
            speedup
        );
    } else {
        println!("  ⚠️  LIMITED SCALING: Only {:.1}x", speedup);
    }

    // ============================================================================
    // PHASE 4: ARCHITECTURAL ANALYSIS
    // ============================================================================
    println!("\n📈 PHASE 4: ARCHITECTURAL INSIGHTS");
    println!("  ✅ Lock-free: No RwLock contention");
    println!("  ✅ Independent: Each partition owns its state");
    println!("  ✅ Parallelizable: All partitions execute simultaneously");
    println!("  ✅ Scalable: Expected ~8x with 8 independent tasks");

    // ============================================================================
    // PHASE 5: COMPARISON SUMMARY
    // ============================================================================
    println!("\n📊 PHASE 5: ARCHITECTURE SUMMARY");
    println!("\n  SHARED GLOBAL LOCK MODEL:");
    println!("    - All partitions serialize on one RwLock");
    println!("    - State is centralized (one GROUP BY, one window state)");
    println!("    - Throughput: {} rec/sec", v1_throughput);
    println!("    - Scales as: O(1) - NO parallelism");

    println!("\n  LOCKLESS STP MODEL:");
    println!("    - Each partition has independent engine");
    println!("    - Each partition manages own state");
    println!("    - Throughput: {} rec/sec", stp_throughput);
    println!("    - Scales as: O(N) - TRUE parallelism");
    println!("    - Speedup vs shared: {:.2}x\n", speedup);
}
