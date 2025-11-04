//! Instrumented profiling of TUMBLING window standard path
//!
//! This test adds detailed timing instrumentation to identify the exact O(N) bottleneck
//! in the standard (non-EMIT CHANGES) TUMBLING window processing.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

#[tokio::test]
#[serial]
async fn profile_tumbling_instrumented_standard_path() {
    println!("\n🔬 INSTRUMENTED: TUMBLING Window Standard Path Analysis");
    println!("{}", "=".repeat(70));
    println!("Goal: Identify exact O(N) operation causing slowdown");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            trader_id,
            symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    // Use smaller dataset for detailed analysis
    let num_records = 5000;
    let sample_interval = 100; // Sample every 100 records for detailed output

    // Phase 1: Record Generation
    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000i64;

    for i in 0..num_records {
        let mut fields = HashMap::new();
        let trader_id = format!("TRADER{}", i % 20);
        let symbol = format!("SYM{}", i % 10);
        let price = 100.0 + (i as f64 % 50.0);
        let quantity = 100 + (i % 1000);
        let timestamp = base_time + (i as i64);

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
        fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    let phase1_duration = phase1_start.elapsed();
    println!(
        "✅ Phase 1: Record generation ({} records): {:?}",
        records.len(),
        phase1_duration
    );

    // Phase 2: Engine Setup
    let phase2_start = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = match parser.parse(sql) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("❌ Failed to parse SQL: {:?}", e);
            return;
        }
    };
    let phase2_duration = phase2_start.elapsed();
    println!(
        "✅ Phase 2: Engine setup + SQL parsing: {:?}",
        phase2_duration
    );

    // Phase 3: INSTRUMENTED Record Execution
    println!("\n🔬 DETAILED INSTRUMENTATION:");
    println!("{}", "─".repeat(70));
    println!(
        "{:>6} | {:>10} | {:>10} | {:>10} | {:>8} | {:>10}",
        "Record", "Total", "Parse", "Execute", "Emit?", "Growth"
    );
    println!("{}", "─".repeat(70));

    let phase3_start = Instant::now();

    // Track timing for different operations
    let mut execution_times = Vec::new();
    let mut first_exec_time = None;

    for (idx, record) in records.iter().enumerate() {
        let record_start = Instant::now();

        // Execute the record
        let _ = engine.execute_with_record(&query, record.clone()).await;

        let total_time = record_start.elapsed();

        // Store timing
        execution_times.push((idx, total_time));

        if idx == 0 {
            first_exec_time = Some(total_time);
        }

        // Print detailed output at sample intervals
        if idx % sample_interval == 0 || idx < 10 {
            let growth = if let Some(first_time) = first_exec_time {
                total_time.as_micros() as f64 / first_time.as_micros() as f64
            } else {
                1.0
            };

            println!(
                "{:6} | {:10.3?} | {:>10} | {:>10} | {:>8} | {:>9.2}x",
                idx,
                total_time,
                "-", // Can't measure parse separately in this test
                "-", // Can't measure execute separately
                "-", // Can't detect emission
                growth
            );
        }
    }

    let phase3_duration = phase3_start.elapsed();
    println!("{}", "─".repeat(70));
    println!(
        "✅ Phase 3: Execute {} records: {:?}",
        records.len(),
        phase3_duration
    );
    println!(
        "   Average per record: {:?}",
        phase3_duration / records.len() as u32
    );

    // Analyze timing patterns
    println!("\n📊 TIMING ANALYSIS:");
    println!("{}", "=".repeat(70));

    if !execution_times.is_empty() {
        let min_time = execution_times.iter().map(|(_, d)| *d).min().unwrap();
        let max_time = execution_times.iter().map(|(_, d)| *d).max().unwrap();

        println!("Min execution time: {:?}", min_time);
        println!("Max execution time: {:?}", max_time);
        println!("Range: {:?} (max - min)", max_time - min_time);

        // Calculate growth at different points
        let checkpoints = [0, 500, 1000, 2000, 3000, 4000];
        println!("\n📈 Growth Analysis at Checkpoints:");
        println!("{}", "─".repeat(70));

        let baseline = execution_times[0].1;
        for &checkpoint in &checkpoints {
            if checkpoint < execution_times.len() {
                let (idx, time) = execution_times[checkpoint];
                let growth = time.as_micros() as f64 / baseline.as_micros() as f64;
                println!(
                    "Record {:5}: {:10.3?} ({:6.2}x baseline)",
                    idx, time, growth
                );
            }
        }

        // Calculate growth ratio
        if execution_times.len() >= 2 {
            let first_time = execution_times[0].1.as_micros() as f64;
            let last_time = execution_times[execution_times.len() - 1].1.as_micros() as f64;
            let growth_ratio = last_time / first_time;

            println!("\n🎯 Overall Growth Ratio: {:.2}x", growth_ratio);

            if growth_ratio > 10.0 {
                println!("❌ SEVERE O(N²) behavior detected!");
            } else if growth_ratio > 2.0 {
                println!("⚠️  O(N²) behavior detected");
            } else {
                println!("✅ Linear or better behavior");
            }
        }

        // Statistical analysis
        println!("\n📊 Statistical Distribution:");
        println!("{}", "─".repeat(70));

        let times: Vec<u128> = execution_times.iter().map(|(_, d)| d.as_micros()).collect();
        let mean = times.iter().sum::<u128>() as f64 / times.len() as f64;

        let variance = times
            .iter()
            .map(|&t| {
                let diff = t as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / times.len() as f64;

        let std_dev = variance.sqrt();

        println!("Mean:   {:.2}µs", mean);
        println!("StdDev: {:.2}µs", std_dev);
        println!(
            "CoV:    {:.2}% (coefficient of variation)",
            (std_dev / mean) * 100.0
        );

        if (std_dev / mean) > 1.0 {
            println!("⚠️  High variance indicates inconsistent performance");
        }
    }

    // Collect results
    let phase7_start = Instant::now();
    let mut results = Vec::new();
    while let Ok(output) = rx.try_recv() {
        results.push(output);
    }
    let phase7_duration = phase7_start.elapsed();

    println!("\n📊 FINAL SUMMARY:");
    println!("{}", "=".repeat(70));

    let total_duration = phase1_duration + phase2_duration + phase3_duration + phase7_duration;
    let throughput = records.len() as f64 / total_duration.as_secs_f64();

    println!("Total time:    {:?}", total_duration);
    println!(
        "Execution:     {:?} ({:.1}%)",
        phase3_duration,
        100.0 * phase3_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!("Results:       {} collected", results.len());
    println!("Throughput:    {:.0} rec/sec", throughput);
    println!("Target:        20,000 rec/sec");

    if throughput < 20000.0 {
        println!(
            "Gap:           {:.0} rec/sec ({:.1}x slower)",
            20000.0 - throughput,
            20000.0 / throughput
        );
    }

    println!("\n🔍 BOTTLENECK IDENTIFICATION:");
    println!("{}", "=".repeat(70));
    println!("Based on growth pattern:");

    let first_time = execution_times[0].1;
    let last_time = execution_times[execution_times.len() - 1].1;
    let growth = last_time.as_micros() as f64 / first_time.as_micros() as f64;

    if growth > 5.0 {
        println!(
            "✓ Growth ratio {:.2}x indicates O(N) cost per record",
            growth
        );
        println!("✓ Total complexity is O(N²)");
        println!("\nMost likely causes:");
        println!("  1. Buffer scanning on every record insertion");
        println!("  2. State validation that scales with buffer size");
        println!("  3. GROUP BY state lookup degrading with size");
        println!("  4. Hidden allocation/reallocation in Vec operations");
        println!("\nRecommended investigation:");
        println!("  - Add timing to window_state.add_record()");
        println!("  - Check if buffer.len() correlates with slowdown");
        println!("  - Profile Vec::push() operations");
        println!("  - Check for hidden iterators in window state management");
    } else {
        println!("✓ Growth ratio {:.2}x is acceptable", growth);
        println!("✓ Bottleneck may be elsewhere or constant overhead");
    }
}
