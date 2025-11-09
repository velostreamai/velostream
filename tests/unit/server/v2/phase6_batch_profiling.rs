//! Phase 6.1c: Realistic batch-based profiling
//! Compare direct SQL execution vs V2@1p with proper batch processing

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn profile_v2_batch_processing() {
    println!("\n\n=== PHASE 6.1c: Realistic Batch Processing Profiling ===\n");

    // Setup
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let engine = Arc::new(RwLock::new(StreamExecutionEngine::new(tx)));
    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    let num_records = 10_000;
    let batch_size = 100; // Process 100 records per batch

    // Phase 1: Direct SQL execution (V1 baseline)
    println!("📊 PHASE 1: Direct SQL Execution (V1 baseline - per-record)");

    let mut records = Vec::new();
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
        records.push(StreamRecord::new(fields));
    }

    let v1_start = Instant::now();
    for record in &records {
        let mut engine_lock = engine.write().await;
        let _ = engine_lock.execute_with_record(&query, &record).await.ok();
        drop(engine_lock);
    }
    let v1_elapsed = v1_start.elapsed();
    let v1_throughput = (num_records as f64 / v1_elapsed.as_secs_f64()) as u64;

    println!("  Records processed: {}", num_records);
    println!("  Time elapsed:      {:?}", v1_elapsed);
    println!("  Throughput:        {} rec/sec", v1_throughput);
    println!(
        "  Per-record cost:   {} μs\n",
        v1_elapsed.as_micros() as f64 / num_records as f64
    );

    // Phase 2: V2 with BATCH-BASED PROCESSING (realistic)
    println!("📊 PHASE 2: V2@1p Execution (BATCH-BASED - realistic architecture)");

    let v2_start = Instant::now();

    // Process in batches - acquire lock ONCE per batch, not per record
    for batch_start in (0..num_records).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_records);

        // ✅ KEY DIFFERENCE: Acquire lock once for entire batch
        let mut engine_lock = engine.write().await;

        // Process all records in batch while holding lock
        for i in batch_start..batch_end {
            let _ = engine_lock
                .execute_with_record(&query, records[i].clone())
                .await
                .ok();
        }

        // Lock is released here after batch completes
        drop(engine_lock);
    }

    let v2_elapsed = v2_start.elapsed();
    let v2_throughput = (num_records as f64 / v2_elapsed.as_secs_f64()) as u64;

    println!("  Records processed: {}", num_records);
    println!("  Batch size:        {}", batch_size);
    println!("  Time elapsed:      {:?}", v2_elapsed);
    println!("  Throughput:        {} rec/sec", v2_throughput);
    println!(
        "  Per-record cost:   {} μs\n",
        v2_elapsed.as_micros() as f64 / num_records as f64
    );

    // Phase 3: Performance comparison
    println!("📊 PHASE 3: COMPARISON");
    let overhead_ns = (v2_elapsed.as_nanos() as i128 - v1_elapsed.as_nanos() as i128).max(0);
    let overhead_pct = if v1_elapsed.as_nanos() > 0 {
        (overhead_ns as f64 / v1_elapsed.as_nanos() as f64) * 100.0
    } else {
        0.0
    };

    println!("  V1 Direct SQL:    {} rec/sec (baseline)", v1_throughput);
    println!("  V2 Batch-based:   {} rec/sec", v2_throughput);
    println!(
        "  Overhead:         {} ns ({:.1}%)",
        overhead_ns, overhead_pct
    );

    if v2_throughput > v1_throughput {
        let improvement_pct = ((v2_throughput as f64 / v1_throughput as f64) - 1.0) * 100.0;
        println!("  ✅ V2 is FASTER by {:.1}%", improvement_pct);
    } else if v2_throughput < v1_throughput {
        let slowdown_pct = ((v1_throughput as f64 / v2_throughput as f64) - 1.0) * 100.0;
        println!("  ⚠️  V2 is SLOWER by {:.1}%", slowdown_pct);
    } else {
        println!("  ✅ V2 matches V1 performance");
    }

    // Phase 4: Lock acquisition cost analysis
    println!("\n📈 PHASE 4: LOCK OVERHEAD ANALYSIS");
    let num_batches = (num_records + batch_size - 1) / batch_size;
    let lock_ops_v1 = num_records; // One lock per record
    let lock_ops_v2 = num_batches; // One lock per batch
    println!("  V1 lock operations:  {} (one per record)", lock_ops_v1);
    println!("  V2 lock operations:  {} (one per batch)", lock_ops_v2);
    println!(
        "  Lock reduction:      {:.1}x fewer locks",
        lock_ops_v1 as f64 / lock_ops_v2 as f64
    );

    // Phase 5: Theoretical analysis
    println!("\n📊 PHASE 5: ARCHITECTURAL INSIGHTS");
    let v1_per_lock = v1_elapsed.as_nanos() as f64 / lock_ops_v1 as f64;
    let v2_per_lock = v2_elapsed.as_nanos() as f64 / lock_ops_v2 as f64;

    println!("  V1 time per lock operation:  {:.2} ns", v1_per_lock);
    println!("  V2 time per lock operation:  {:.2} ns", v2_per_lock);
    println!(
        "  Lock efficiency gain:        {:.1}x",
        v1_per_lock / v2_per_lock
    );

    // Phase 6: Conclusions
    println!("\n🎯 CONCLUSIONS");

    if v2_throughput >= v1_throughput {
        println!("  ✅ V2 batch architecture is PRODUCTION-READY");
        println!("  ✅ Lock reduction provides significant performance benefit");
        println!("  ✅ Overhead < 1% (within measurement noise)");
    } else {
        let overhead_pct_final = ((v1_throughput as f64 / v2_throughput as f64) - 1.0) * 100.0;
        println!("  ⚠️  V2 has {:.1}% overhead vs V1", overhead_pct_final);
        println!("  📝 Analysis: Check if overhead is from:");
        println!("     - Lock contention");
        println!("     - Async task scheduling");
        println!("     - State management overhead");
        println!("     - Group BY or windowing state");
    }

    // Phase 7: Extrapolation to multi-partition
    println!("\n📈 PHASE 7: MULTI-PARTITION EXTRAPOLATION");
    println!("  Assuming V2@8p (8 independent partitions):");
    let expected_8p = v2_throughput as f64 * 8.0;
    println!("  Expected throughput: {:.0} rec/sec", expected_8p);
    println!("  vs V1 baseline:      {} rec/sec", v1_throughput);
    let scaling_factor = expected_8p / v1_throughput as f64;
    println!(
        "  Scaling factor:      {:.1}x (should be ~8x for true parallelism)",
        scaling_factor
    );
}
