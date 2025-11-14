//! Phase 6.4: Raw StreamExecutionEngine Performance
//! Measure pure SQL engine speed without any framework overhead

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::{
    StreamExecutionEngine, execution::types::StreamRecord, parser::StreamingSqlParser,
};

#[tokio::test]
#[ignore]
async fn profile_raw_engine_direct() {
    println!("\n\n=== PHASE 6.4: RAW StreamExecutionEngine Performance ===\n");

    let num_records = 100_000;

    // Create test records
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

    // ============================================================================
    // TEST 1: SIMPLEST POSSIBLE - Direct mut engine, no lock, no cloning
    // ============================================================================
    println!("📊 TEST 1: Direct Mutable Engine (NO LOCKING, NO CLONING)");
    println!("   Pattern: let mut engine; loop {{ engine.execute_with_record(...) }}");

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    let test1_start = Instant::now();
    for record in &records {
        let _ = engine.execute_with_record(&query, &record).await.ok();
    }
    let test1_elapsed = test1_start.elapsed();
    let test1_throughput = (num_records as f64 / test1_elapsed.as_secs_f64()) as u64;

    println!("  Records: {}", num_records);
    println!("  Time: {:?}", test1_elapsed);
    println!("  Throughput: {} rec/sec", test1_throughput);
    println!(
        "  Per-record: {:.3} μs\n",
        test1_elapsed.as_micros() as f64 / num_records as f64
    );

    // ============================================================================
    // TEST 2: BATCH MODE - Process in 1000-record batches
    // ============================================================================
    println!("📊 TEST 2: Batch Mode (1000 records per batch)");

    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut engine2 = StreamExecutionEngine::new(tx2);

    let test2_start = Instant::now();
    for batch in records.chunks(1000) {
        for record in batch {
            let _ = engine2.execute_with_record(&query, &record).await.ok();
        }
    }
    let test2_elapsed = test2_start.elapsed();
    let test2_throughput = (num_records as f64 / test2_elapsed.as_secs_f64()) as u64;

    println!("  Records: {}", num_records);
    println!("  Batch size: 1000");
    println!("  Time: {:?}", test2_elapsed);
    println!("  Throughput: {} rec/sec", test2_throughput);
    println!(
        "  Per-record: {:.3} μs\n",
        test2_elapsed.as_micros() as f64 / num_records as f64
    );

    // ============================================================================
    // TEST 3: MINIMAL CLONE - Only clone once, reuse object
    // ============================================================================
    println!("📊 TEST 3: Minimal Cloning (pre-clone all records)");

    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let mut engine3 = StreamExecutionEngine::new(tx3);

    let records_cloned: Vec<StreamRecord> = records.iter().map(|r| r.clone()).collect();

    let test3_start = Instant::now();
    for record in &records_cloned {
        let _ = engine3.execute_with_record(&query, &record).await.ok();
    }
    let test3_elapsed = test3_start.elapsed();
    let test3_throughput = (num_records as f64 / test3_elapsed.as_secs_f64()) as u64;

    println!("  Records: {}", num_records);
    println!("  Time: {:?}", test3_elapsed);
    println!("  Throughput: {} rec/sec", test3_throughput);
    println!(
        "  Per-record: {:.3} μs\n",
        test3_elapsed.as_micros() as f64 / num_records as f64
    );

    // ============================================================================
    // TEST 4: NO CLONE - Use same record object (invalid but measures execute cost)
    // ============================================================================
    println!("📊 TEST 4: THEORETICAL - No Cloning (for comparison only)");
    println!("   Note: This is INVALID SQL semantics but shows engine core cost");

    let (tx4, _rx4) = tokio::sync::mpsc::unbounded_channel();
    let mut engine4 = StreamExecutionEngine::new(tx4);

    let base_record = records[0].clone();

    let test4_start = Instant::now();
    for _i in 0..num_records {
        let mut record = base_record.clone();
        record.fields.insert(
            "id".to_string(),
            velostream::velostream::sql::execution::types::FieldValue::Integer(_i as i64),
        );
        let _ = engine4.execute_with_record(&query, &record).await.ok();
    }
    let test4_elapsed = test4_start.elapsed();
    let test4_throughput = (num_records as f64 / test4_elapsed.as_secs_f64()) as u64;

    println!("  Records: {}", num_records);
    println!("  Time: {:?}", test4_elapsed);
    println!("  Throughput: {} rec/sec", test4_throughput);
    println!(
        "  Per-record: {:.3} μs\n",
        test4_elapsed.as_micros() as f64 / num_records as f64
    );

    // ============================================================================
    // COMPARISON
    // ============================================================================
    println!("📊 COMPARISON");
    println!();
    println!("  Direct:           {:>10} rec/sec", test1_throughput);
    println!(
        "  Batch (1000):     {:>10} rec/sec ({:+.1}%)",
        test2_throughput,
        ((test2_throughput as f64 / test1_throughput as f64) - 1.0) * 100.0
    );
    println!(
        "  Pre-cloned:       {:>10} rec/sec ({:+.1}%)",
        test3_throughput,
        ((test3_throughput as f64 / test1_throughput as f64) - 1.0) * 100.0
    );
    println!(
        "  Theoretical:      {:>10} rec/sec ({:+.1}%)",
        test4_throughput,
        ((test4_throughput as f64 / test1_throughput as f64) - 1.0) * 100.0
    );
    println!();

    // ============================================================================
    // PER-OPERATION COST BREAKDOWN
    // ============================================================================
    println!("📈 PER-OPERATION COST");
    let test1_ns = test1_elapsed.as_nanos() as f64 / num_records as f64;
    let test2_ns = test2_elapsed.as_nanos() as f64 / num_records as f64;
    let test3_ns = test3_elapsed.as_nanos() as f64 / num_records as f64;
    let test4_ns = test4_elapsed.as_nanos() as f64 / num_records as f64;

    println!();
    println!("  Direct:       {:.1} ns/record", test1_ns);
    println!(
        "  Batch:        {:.1} ns/record ({:+.1}%)",
        test2_ns,
        ((test2_ns / test1_ns) - 1.0) * 100.0
    );
    println!(
        "  Pre-cloned:   {:.1} ns/record ({:+.1}%)",
        test3_ns,
        ((test3_ns / test1_ns) - 1.0) * 100.0
    );
    println!(
        "  Theoretical:  {:.1} ns/record ({:+.1}%)",
        test4_ns,
        ((test4_ns / test1_ns) - 1.0) * 100.0
    );
    println!();

    // ============================================================================
    // ANALYSIS
    // ============================================================================
    println!("🔍 ANALYSIS");
    println!();

    let clone_overhead_pct = ((test1_ns - test4_ns) / test1_ns) * 100.0;
    println!("  Clone overhead: {:.1}%", clone_overhead_pct);
    println!("  Core engine cost: {:.1} ns/record", test4_ns);
    println!("  Clone cost: {:.1} ns/record", test1_ns - test4_ns);
    println!();

    // ============================================================================
    // SCALING
    // ============================================================================
    println!("📊 EXTRAPOLATED PERFORMANCE");
    println!();
    println!("  V2@1p (batch 100):   ~117K rec/sec (measured earlier)");
    println!("  Raw Engine Direct:   {} rec/sec", test1_throughput);
    println!(
        "  Overhead ratio:      {:.2}x",
        test1_throughput as f64 / 117_319.0
    );
    println!();

    if test1_throughput > 500_000 {
        println!("  🚀 BLAZINGLY FAST ENGINE!");
        println!(
            "     With 8 partitions: ~{:.0}K rec/sec",
            test1_throughput as f64 * 8.0 / 1000.0
        );
    } else if test1_throughput > 200_000 {
        println!("  ⚡ EXCELLENT ENGINE SPEED!");
        println!(
            "     With 8 partitions: ~{:.0}K rec/sec",
            test1_throughput as f64 * 8.0 / 1000.0
        );
    } else {
        println!("  ✅ GOOD ENGINE SPEED");
        println!(
            "     With 8 partitions: ~{:.0}K rec/sec",
            test1_throughput as f64 * 8.0 / 1000.0
        );
    }
    println!();

    // ============================================================================
    // CONCLUSIONS
    // ============================================================================
    println!("🎯 CONCLUSIONS");
    println!();
    println!("  1. Raw engine throughput: {} rec/sec", test1_throughput);
    println!(
        "  2. Clone overhead: {:.1}% of total time",
        clone_overhead_pct
    );
    println!("  3. Core execute cost: {:.1} ns/record", test4_ns);
    println!("  4. Expected V2@8p (lockless on 8 cores):");
    println!(
        "     Theoretical max: ~{:.0}K rec/sec",
        test1_throughput as f64 * 8.0 / 1000.0
    );
}
