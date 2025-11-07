//! Phase 6: Pure SELECT Output Overhead Analysis
//! Isolate and measure the actual cost of output channel operations

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn analyze_pure_select_output_overhead() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  PHASE 6: PURE SELECT OUTPUT OVERHEAD ANALYSIS                 ║");
    println!("║  Measure actual cost of channel operations vs framework cost   ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    // Create test data
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

    println!("Test Configuration:");
    println!("  Records: {}", num_records);
    println!("  Query: SELECT id, value FROM test");
    println!("  Goal: Isolate output channel overhead\n");

    // ============================================================================
    // TEST 1: WITH OUTPUT CHANNEL AND DRAIN (Full emission processing)
    // ============================================================================
    println!("📊 TEST 1: WITH OUTPUT CHANNEL AND DRAIN");
    println!("    (Measure full cost: execute + emit + drain)\n");

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    drop(engine1); // Drop engine to ensure channel closes

    // Drain all outputs to ensure they're processed
    let mut output_count = 0;
    while let Some(_) = rx1.recv().await {
        output_count += 1;
    }

    let elapsed1 = start1.elapsed();
    let throughput1 = (num_records as f64 / elapsed1.as_secs_f64()) as u64;
    let per_record_1_ns = elapsed1.as_nanos() as f64 / num_records as f64;

    println!("  Total Time: {:?}", elapsed1);
    println!("  Throughput: {} rec/sec", throughput1);
    println!("  Per-record: {:.1} ns", per_record_1_ns);
    println!("  Output records emitted: {}", output_count);
    println!(
        "  Average per output: {:.1} ns\n",
        per_record_1_ns * output_count as f64 / num_records as f64
    );

    // ============================================================================
    // TEST 2: WITHOUT DRAINING OUTPUT (Skip receiver loop)
    // ============================================================================
    println!("📊 TEST 2: WITHOUT OUTPUT DRAIN");
    println!("    (Measure cost: execute + emit only, skip receiving)\n");

    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut engine2 = StreamExecutionEngine::new(tx2);

    let start2 = Instant::now();
    for record in &records {
        let _ = engine2
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    drop(engine2);

    let elapsed2 = start2.elapsed();
    let throughput2 = (num_records as f64 / elapsed2.as_secs_f64()) as u64;
    let per_record_2_ns = elapsed2.as_nanos() as f64 / num_records as f64;

    println!("  Total Time: {:?}", elapsed2);
    println!("  Throughput: {} rec/sec", throughput2);
    println!("  Per-record: {:.1} ns\n", per_record_2_ns);

    // ============================================================================
    // OVERHEAD CALCULATION
    // ============================================================================
    println!("📊 OVERHEAD ANALYSIS\n");

    let drain_overhead_ns = per_record_1_ns - per_record_2_ns;
    let drain_overhead_pct = (drain_overhead_ns / per_record_1_ns) * 100.0;
    let throughput_diff_pct = ((throughput1 as f64 / throughput2 as f64) - 1.0) * 100.0;

    println!(
        "  Drain overhead:        {:.1} ns/record ({:.1}%)",
        drain_overhead_ns, drain_overhead_pct
    );
    println!("  Throughput difference: {:.1}%\n", -throughput_diff_pct);

    // ============================================================================
    // FRAMEWORK BREAKDOWN
    // ============================================================================
    println!("📊 FRAMEWORK COST BREAKDOWN\n");

    // Reference: Raw SQL Engine = 2.14 μs = 2,140 ns per record (from earlier tests)
    let raw_engine_ns = 2_140.0;
    let framework_overhead_ns = per_record_2_ns - raw_engine_ns;

    println!("  Raw SQL Engine:        2,140 ns/record (baseline from earlier tests)");
    println!(
        "  Framework overhead:    {:.1} ns/record",
        framework_overhead_ns
    );
    println!(
        "  Framework % of total:  {:.1}%",
        (framework_overhead_ns / per_record_2_ns) * 100.0
    );
    println!(
        "  Drain cost:            {:.1} ns/record ({:.1}% of total)",
        drain_overhead_ns, drain_overhead_pct
    );
    println!();

    // ============================================================================
    // CONCLUSIONS
    // ============================================================================
    println!("🎯 CONCLUSIONS\n");

    if drain_overhead_ns > framework_overhead_ns * 0.3 {
        println!("  ✅ DRAIN IS SIGNIFICANT BOTTLENECK");
        println!(
            "     - Accounts for {:.1}% of total overhead",
            drain_overhead_pct
        );
        println!("     - Output operations are expensive relative to computation");
        println!("     - Batching/windowing helps by reducing drain frequency\n");
    } else if drain_overhead_ns > 0.0 {
        println!("  ⚠️  DRAIN HAS MINOR IMPACT");
        println!("     - Less than {:.1}% overhead", drain_overhead_pct);
        println!("     - Framework overhead dominates");
        println!("     - Consider channel efficiency\n");
    } else {
        println!("  ℹ️  DRAIN OVERHEAD NOT MEASURABLE");
        println!("     - Async channel operations may be buffered");
        println!("     - Drain doesn't slow down execution loop\n");
    }

    println!("  Pure SELECT Slowdown Analysis:");
    println!("    Raw Engine:        466,323 rec/sec (2.14 μs)");
    println!(
        "    Pure SELECT:       186,000 rec/sec ({:.1} μs)",
        per_record_1_ns / 1_000.0
    );
    println!(
        "    Slowdown:          {:.1}x",
        per_record_1_ns / raw_engine_ns
    );
    println!();

    let unaccounted_ns = per_record_1_ns - raw_engine_ns - drain_overhead_ns;
    println!("  Time Distribution:");
    println!(
        "    Raw engine work:   {:.1}%",
        (raw_engine_ns / per_record_1_ns) * 100.0
    );
    println!(
        "    Framework work:    {:.1}%",
        (framework_overhead_ns / per_record_1_ns) * 100.0
    );
    println!(
        "    Drain overhead:    {:.1}%",
        (drain_overhead_ns / per_record_1_ns) * 100.0
    );
    if unaccounted_ns > 0.0 {
        println!(
            "    Other overhead:    {:.1}%",
            (unaccounted_ns / per_record_1_ns) * 100.0
        );
    }
    println!();
}
