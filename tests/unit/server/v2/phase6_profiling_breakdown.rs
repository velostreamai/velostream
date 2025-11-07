//! Phase 6.1b: Detailed performance breakdown profiling
//! Compare direct SQL execution vs V2@1p to identify bottleneck

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn profile_v2_1p_breakdown() {
    println!("\n\n=== PHASE 6.1b: Performance Breakdown Profiling ===\n");

    // Setup
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let engine = Arc::new(RwLock::new(StreamExecutionEngine::new(tx)));
    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    let num_records = 10_000;

    // Phase 1: Direct SQL execution (V1 baseline)
    println!("📊 PHASE 1: Direct SQL Execution (V1 baseline)");

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
        let _ = engine
            .write()
            .await
            .execute_with_record(&query, record.clone())
            .await
            .ok();
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

    // Phase 2: V2@1p with detailed breakdown
    println!("📊 PHASE 2: V2@1p Execution (with breakdown)");

    let mut total_read_time = std::time::Duration::ZERO;
    let mut total_route_time = std::time::Duration::ZERO;
    let mut total_lock_read_time = std::time::Duration::ZERO;
    let mut total_execute_time = std::time::Duration::ZERO;
    let mut total_lock_write_time = std::time::Duration::ZERO;

    let v2_start = Instant::now();

    for (idx, record) in records.iter().enumerate() {
        // Phase 2a: Read simulation
        let read_start = Instant::now();
        let _batch = record;
        total_read_time += read_start.elapsed();

        // Phase 2b: Route simulation (local hash)
        let route_start = Instant::now();
        let _target = idx % 1; // Always partition 0 for 1 partition
        total_route_time += route_start.elapsed();

        // Phase 2c: RwLock write for state mutation (execute_with_record requires &mut self)
        let lock_write_start = Instant::now();
        let mut write_lock = engine.write().await;
        total_lock_read_time += lock_write_start.elapsed();

        // Phase 2d: Execute
        let exec_start = Instant::now();
        let _ = write_lock
            .execute_with_record(&query, record.clone())
            .await
            .ok();
        total_execute_time += exec_start.elapsed();

        // (lock released here)

        // Phase 2e: RwLock write for state update (simulated every 100 records)
        if idx % 100 == 0 {
            let lock_write_start = Instant::now();
            let _write_lock = engine.write().await;
            // Simulate state update
            total_lock_write_time += lock_write_start.elapsed();
        }
    }

    let v2_elapsed = v2_start.elapsed();
    let v2_throughput = (num_records as f64 / v2_elapsed.as_secs_f64()) as u64;

    println!("  Records processed: {}", num_records);
    println!("  Time elapsed:      {:?}", v2_elapsed);
    println!("  Throughput:        {} rec/sec", v2_throughput);
    println!(
        "  Per-record cost:   {} μs\n",
        v2_elapsed.as_micros() as f64 / num_records as f64
    );

    // Phase 3: Breakdown analysis
    println!("📈 BREAKDOWN ANALYSIS");
    println!(
        "  Read time:         {:?} ({:.2}%)",
        total_read_time,
        (total_read_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Route time:        {:?} ({:.2}%)",
        total_route_time,
        (total_route_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Lock read time:    {:?} ({:.2}%)",
        total_lock_read_time,
        (total_lock_read_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Execute time:      {:?} ({:.2}%)",
        total_execute_time,
        (total_execute_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0
    );
    println!(
        "  Lock write time:   {:?} ({:.2}%)\n",
        total_lock_write_time,
        (total_lock_write_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0
    );

    // Phase 4: Overhead calculation
    println!("📊 OVERHEAD ANALYSIS");
    let overhead = v2_elapsed.as_nanos() as i128 - v1_elapsed.as_nanos() as i128;
    let overhead_pct = (overhead as f64 / v1_elapsed.as_nanos() as f64) * 100.0;

    println!("  V1 throughput:     {} rec/sec", v1_throughput);
    println!("  V2 throughput:     {} rec/sec", v2_throughput);
    println!(
        "  Overhead:          {} ns ({:.1}%)",
        overhead, overhead_pct
    );
    println!(
        "  Scaling factor:    {:.2}x slower\n",
        v2_elapsed.as_secs_f64() / v1_elapsed.as_secs_f64()
    );

    // Phase 5: Hypothetical improvements
    println!("🎯 HYPOTHETICAL IMPROVEMENTS");
    if total_lock_read_time.as_nanos() > 0 {
        let potential_gain =
            (total_lock_read_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0;
        println!("  Remove RwLock reads: +{:.1}% throughput", potential_gain);
    }

    if total_lock_write_time.as_nanos() > 0 {
        let potential_gain =
            (total_lock_write_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0;
        println!("  Remove RwLock writes: +{:.1}% throughput", potential_gain);
    }

    if total_route_time.as_nanos() > 0 {
        let potential_gain =
            (total_route_time.as_nanos() as f64 / v2_elapsed.as_nanos() as f64) * 100.0;
        println!("  Remove routing: +{:.1}% throughput", potential_gain);
    }

    println!("\n🔍 CONCLUSION");
    if v2_throughput > v1_throughput {
        println!(
            "  ✅ V2@1p is FASTER than V1 by {:.1}%",
            ((v2_throughput as f64 / v1_throughput as f64) - 1.0) * 100.0
        );
    } else {
        println!(
            "  ❌ V2@1p is SLOWER than V1 by {:.1}%",
            ((v1_throughput as f64 / v2_throughput as f64) - 1.0) * 100.0
        );
        println!("  Main bottleneck: Check the breakdown above");
    }
}
