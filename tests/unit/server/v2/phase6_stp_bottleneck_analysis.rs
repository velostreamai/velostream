//! Phase 6.3: STP Bottleneck Analysis
//! Identify what's making the lockless STP slow

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn profile_stp_bottlenecks() {
    println!("\n\n=== PHASE 6.3: STP Bottleneck Analysis ===\n");

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
    // BASELINE 1: Sequential (no parallelism, no spawning)
    // ============================================================================
    println!("📊 BASELINE 1: Sequential Processing (no tokio spawn)");

    let seq_start = Instant::now();
    for partition_id in 0..num_partitions {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let start_idx = partition_id * records_per_partition;
        let end_idx = (partition_id + 1) * records_per_partition;

        for record in &all_records[start_idx..end_idx] {
            let _ = engine.execute_with_record(&query, &record).await.ok();
        }
    }
    let seq_elapsed = seq_start.elapsed();
    let seq_throughput = (num_records as f64 / seq_elapsed.as_secs_f64()) as u64;

    println!("  Time: {:?}", seq_elapsed);
    println!("  Throughput: {} rec/sec\n", seq_throughput);

    // ============================================================================
    // BASELINE 2: Tokio spawn with batch processing (no cloning)
    // ============================================================================
    println!("📊 BASELINE 2: Tokio spawn + batch processing (no per-record cloning)");

    let batch_spawn_start = Instant::now();
    let mut handles = vec![];

    for partition_id in 0..num_partitions {
        let parser_clone = parser.clone();
        let query_clone = query.clone();

        let start_idx = partition_id * records_per_partition;
        let end_idx = (partition_id + 1) * records_per_partition;
        let partition_records: Vec<StreamRecord> = all_records[start_idx..end_idx]
            .iter()
            .map(|r| StreamRecord {
                fields: r.fields.clone(),
                timestamp: r.timestamp,
                offset: r.offset,
                partition: r.partition,
                event_time: r.event_time.clone(),
                headers: r.headers.clone(),
                topic: None,
                key: None,
            })
            .collect();

        let handle = tokio::spawn(async move {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            let mut engine = StreamExecutionEngine::new(tx);

            let part_start = Instant::now();
            // Process in batches of 100 WITHOUT cloning each record
            for batch in partition_records.chunks(100) {
                for record in batch {
                    // ⚠️ Still clone here, but at least we reuse the record
                    let _ = engine.execute_with_record(&query_clone, &record).await.ok();
                }
            }
            part_start.elapsed()
        });

        handles.push(handle);
    }

    let mut max_task_time = std::time::Duration::ZERO;
    for handle in handles {
        if let Ok(elapsed) = handle.await {
            max_task_time = max_task_time.max(elapsed);
        }
    }

    let batch_spawn_elapsed = batch_spawn_start.elapsed();
    let batch_spawn_throughput = (num_records as f64 / batch_spawn_elapsed.as_secs_f64()) as u64;

    println!("  Total wall-clock: {:?}", batch_spawn_elapsed);
    println!("  Max partition time: {:?}", max_task_time);
    println!("  Throughput: {} rec/sec\n", batch_spawn_throughput);

    // ============================================================================
    // BOTTLENECK 1: Clone Overhead Measurement
    // ============================================================================
    println!("📊 BOTTLENECK 1: Record Cloning Overhead");

    let clone_start = Instant::now();
    let mut _cloned_records = Vec::new();
    for record in &all_records {
        _cloned_records.push(record.clone());
    }
    let clone_elapsed = clone_start.elapsed();
    let clone_ns_per_record = clone_elapsed.as_nanos() as f64 / num_records as f64;

    println!("  Total clone time: {:?}", clone_elapsed);
    println!("  Per-record clone: {:.2} ns", clone_ns_per_record);
    println!(
        "  Total clone cost: {:.2}% of exec time\n",
        (clone_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );

    // ============================================================================
    // BOTTLENECK 2: Engine Instantiation Overhead
    // ============================================================================
    println!("📊 BOTTLENECK 2: Engine Instantiation Overhead");

    let engine_start = Instant::now();
    for _ in 0..num_partitions {
        let (_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let _engine = StreamExecutionEngine::new(_tx);
    }
    let engine_elapsed = engine_start.elapsed();

    println!(
        "  Time to create {} engines: {:?}",
        num_partitions, engine_elapsed
    );
    println!(
        "  Per-engine: {:.2} μs",
        engine_elapsed.as_micros() as f64 / num_partitions as f64
    );
    println!(
        "  Total engine cost: {:.4}% of exec time\n",
        (engine_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );

    // ============================================================================
    // BOTTLENECK 3: Tokio Task Spawning Overhead
    // ============================================================================
    println!("📊 BOTTLENECK 3: Tokio Spawn Overhead");

    let spawn_start = Instant::now();
    let mut handles = vec![];
    for _ in 0..num_partitions {
        let handle = tokio::spawn(async {
            // Empty task - just measure spawn overhead
        });
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.await;
    }
    let spawn_elapsed = spawn_start.elapsed();

    println!(
        "  Time to spawn {} tasks: {:?}",
        num_partitions, spawn_elapsed
    );
    println!(
        "  Per-spawn: {:.2} μs",
        spawn_elapsed.as_micros() as f64 / num_partitions as f64
    );
    println!(
        "  Total spawn cost: {:.2}% of exec time\n",
        (spawn_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );

    // ============================================================================
    // BOTTLENECK 4: Context Switching on Single Core
    // ============================================================================
    println!("📊 BOTTLENECK 4: Context Switching Analysis");

    let task_time = max_task_time.as_nanos() as f64;
    let wall_time = batch_spawn_elapsed.as_nanos() as f64;
    let ratio = wall_time / task_time;

    println!("  Task time (max partition): {:?}", max_task_time);
    println!("  Wall-clock time (all tasks): {:?}", batch_spawn_elapsed);
    println!("  Serialization ratio: {:.2}x", ratio);

    if ratio > 2.0 {
        println!("  ⚠️  Tasks are running SEQUENTIALLY (context switching overhead)");
    } else if ratio > 1.2 {
        println!("  ⚠️  Limited parallelism (insufficient cores)");
    } else {
        println!("  ✅ Good parallelism");
    }
    println!();

    // ============================================================================
    // SUMMARY
    // ============================================================================
    println!("📊 SUMMARY: Where the Time Goes");
    println!();
    println!("  Sequential (no spawn): {} rec/sec", seq_throughput);
    println!(
        "  Spawned (with spawn):  {} rec/sec",
        batch_spawn_throughput
    );
    println!(
        "  Overhead from spawning: {:.1}%",
        ((seq_throughput as f64 / batch_spawn_throughput as f64) - 1.0) * 100.0
    );
    println!();
    println!(
        "  Cloning overhead: {:.2}%",
        (clone_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Engine creation: {:.4}%",
        (engine_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Spawn overhead: {:.2}%",
        (spawn_elapsed.as_nanos() as f64 / seq_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Task scheduling: {:.2}%",
        ((batch_spawn_elapsed.as_nanos() as f64 - seq_elapsed.as_nanos() as f64)
            / seq_elapsed.as_nanos() as f64)
            * 100.0
    );
    println!();

    // ============================================================================
    // ROOT CAUSE ANALYSIS
    // ============================================================================
    println!("🔍 ROOT CAUSE ANALYSIS");
    println!();

    let spawn_ratio = seq_throughput as f64 / batch_spawn_throughput as f64;

    if spawn_ratio > 1.5 {
        println!("  ❌ SPAWN OVERHEAD IS SIGNIFICANT");
        println!("  Sequential: {} rec/sec", seq_throughput);
        println!("  Spawned:    {} rec/sec", batch_spawn_throughput);
        println!("  Loss:       {:.1}%", (spawn_ratio - 1.0) * 100.0);
        println!();
        println!("  Probable causes:");
        println!("  1. Tokio task overhead > parallelism benefit");
        println!("  2. Small batch size (1250 records) → poor amortization");
        println!("  3. Not enough CPU cores for true parallelism");
        println!("  4. Single-threaded runtime constraints");
    } else if spawn_ratio < 1.0 {
        println!("  ✅ SPAWNING ACTUALLY HELPS!");
        println!("  This suggests proper parallelism with multiple cores");
    } else {
        println!("  ⚠️  SPAWNING HAS MINIMAL IMPACT");
        println!("  Sequential and spawned performance is similar");
        println!("  Spawning cost ≈ Parallelism benefit");
    }
    println!();

    // ============================================================================
    // RECOMMENDATIONS
    // ============================================================================
    println!("📋 RECOMMENDATIONS");
    println!();

    if ratio > 1.5 {
        println!("  1. Run on multi-core machine to see true parallelism");
        println!("  2. Increase batch size (1250 → 10,000) to amortize spawn overhead");
        println!("  3. Consider sync channels instead of tokio::spawn");
        println!("  4. Profile with longer-running tasks to hide spawn overhead");
    } else {
        println!("  1. Architecture is sound - spawn overhead is acceptable");
        println!("  2. Performance on multi-core systems should be excellent");
        println!("  3. Consider reducing clone overhead for better single-core performance");
    }
}
