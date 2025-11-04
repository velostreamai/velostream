//! Detailed profiling of TUMBLING window with EMIT CHANGES performance
//!
//! This test compares TUMBLING window performance with EMIT CHANGES mode enabled
//! to determine if EMIT CHANGES is the root cause of O(N²) behavior.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

#[tokio::test]
#[serial]
async fn profile_tumbling_emit_changes_financial_analytics() {
    println!("\n🔍 TUMBLING Window + EMIT CHANGES Financial Analytics Profile");
    println!("{}", "=".repeat(70));
    println!("⚠️  Testing hypothesis: EMIT CHANGES causes O(N²) behavior");
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
        EMIT CHANGES
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    // Phase 1: Record Generation
    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000i64;

    // Use 10000 records to match the standard TUMBLING test
    for i in 0..10000 {
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

    // Phase 2: Engine Setup and SQL Parsing
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

    // Phase 3: Record Execution (THE CRITICAL PATH)
    let phase3_start = Instant::now();
    let mut execution_times = Vec::new();
    let sample_interval = 1000; // Sample every 1000 records

    for (idx, record) in records.iter().enumerate() {
        let record_start = Instant::now();
        let _ = engine.execute_with_record(&query, record.clone()).await;
        let record_duration = record_start.elapsed();

        if idx % sample_interval == 0 {
            execution_times.push((idx, record_duration));
            println!("   Record {}: {:?}", idx, record_duration);
        }
    }
    let phase3_duration = phase3_start.elapsed();
    println!(
        "✅ Phase 3: Execute {} records: {:?}",
        records.len(),
        phase3_duration
    );
    println!(
        "   Average per record: {:?}",
        phase3_duration / records.len() as u32
    );

    // Analyze execution time distribution
    if !execution_times.is_empty() {
        let max_time = execution_times.iter().map(|(_, d)| *d).max().unwrap();
        let min_time = execution_times.iter().map(|(_, d)| *d).min().unwrap();
        println!("   Min record time: {:?}", min_time);
        println!("   Max record time: {:?}", max_time);

        // Calculate growth ratio
        if execution_times.len() >= 2 {
            let first_time = execution_times[0].1.as_micros() as f64;
            let last_time = execution_times[execution_times.len() - 1].1.as_micros() as f64;
            let growth_ratio = last_time / first_time;
            println!("   Growth ratio (last/first): {:.2}x", growth_ratio);

            if growth_ratio > 2.0 {
                println!("   ⚠️  O(N²) behavior detected! Growth ratio > 2.0");
            } else {
                println!("   ✅ Linear behavior - growth ratio < 2.0");
            }
        }
    }

    // Phase 4: Window Flushing
    let phase4_start = Instant::now();
    let _ = engine.flush_windows().await;
    let phase4_duration = phase4_start.elapsed();
    println!("✅ Phase 4: Flush windows: {:?}", phase4_duration);

    // Phase 5: Group By Flushing
    let phase5_start = Instant::now();
    let _ = engine.flush_group_by_results(&query);
    let phase5_duration = phase5_start.elapsed();
    println!("✅ Phase 5: Flush group by results: {:?}", phase5_duration);

    // Phase 6: Final Processing Sleep
    let phase6_start = Instant::now();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let phase6_duration = phase6_start.elapsed();
    println!("✅ Phase 6: Sleep for emissions: {:?}", phase6_duration);

    // Phase 7: Result Collection
    let phase7_start = Instant::now();
    let mut results = Vec::new();
    while let Ok(output) = rx.try_recv() {
        results.push(output);
    }
    let phase7_duration = phase7_start.elapsed();
    println!(
        "✅ Phase 7: Collect {} results: {:?}",
        results.len(),
        phase7_duration
    );

    // Summary
    let total_duration = phase1_duration
        + phase2_duration
        + phase3_duration
        + phase4_duration
        + phase5_duration
        + phase6_duration
        + phase7_duration;

    println!("\n📊 PERFORMANCE BREAKDOWN (EMIT CHANGES Mode)");
    println!("{}", "=".repeat(70));
    println!(
        "Phase 1 (Record Gen):      {:?} ({:.1}%)",
        phase1_duration,
        100.0 * phase1_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 2 (Setup+Parse):     {:?} ({:.1}%)",
        phase2_duration,
        100.0 * phase2_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 3 (Execution):       {:?} ({:.1}%) ⚠️ CRITICAL",
        phase3_duration,
        100.0 * phase3_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 4 (Flush Windows):   {:?} ({:.1}%)",
        phase4_duration,
        100.0 * phase4_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 5 (Flush GroupBy):   {:?} ({:.1}%)",
        phase5_duration,
        100.0 * phase5_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 6 (Sleep):           {:?} ({:.1}%)",
        phase6_duration,
        100.0 * phase6_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!(
        "Phase 7 (Collect):         {:?} ({:.1}%)",
        phase7_duration,
        100.0 * phase7_duration.as_secs_f64() / total_duration.as_secs_f64()
    );
    println!("{}", "─".repeat(70));
    println!("TOTAL:                     {:?}", total_duration);

    let throughput = records.len() as f64 / total_duration.as_secs_f64();
    println!("\n🔥 Throughput: {:.0} records/sec", throughput);
    println!("🎯 Expected with EMIT CHANGES: ~15,000 rec/sec (emits per record)");
    println!("🎯 Baseline (no EMIT CHANGES): 120 rec/sec");

    // Comparison with baseline
    let baseline_throughput = 120.0;
    if throughput < baseline_throughput {
        println!(
            "⚠️  SLOWER than baseline by {:.0} rec/s ({:.1}x)",
            baseline_throughput - throughput,
            baseline_throughput / throughput
        );
    } else if throughput > baseline_throughput * 2.0 {
        println!(
            "✅ FASTER than baseline by {:.0} rec/s ({:.1}x)",
            throughput - baseline_throughput,
            throughput / baseline_throughput
        );
    } else {
        println!(
            "≈  SIMILAR to baseline ({:.1}x)",
            throughput / baseline_throughput
        );
    }

    println!("\n🔬 HYPOTHESIS TEST RESULTS:");
    println!("{}", "=".repeat(70));

    // Calculate if EMIT CHANGES made it worse or better
    if let Some((_, first_time)) = execution_times.first() {
        if let Some((_, last_time)) = execution_times.last() {
            let growth = last_time.as_micros() as f64 / first_time.as_micros() as f64;

            if growth > 10.0 {
                println!("❌ HYPOTHESIS CONFIRMED: EMIT CHANGES causes severe O(N²) behavior");
                println!(
                    "   Growth ratio: {:.2}x indicates O(N) per-record cost",
                    growth
                );
            } else if growth > 2.0 {
                println!("⚠️  HYPOTHESIS PARTIALLY CONFIRMED: EMIT CHANGES contributes to O(N²)");
                println!(
                    "   Growth ratio: {:.2}x indicates some state accumulation",
                    growth
                );
            } else {
                println!("✅ HYPOTHESIS REJECTED: EMIT CHANGES is NOT the primary cause");
                println!(
                    "   Growth ratio: {:.2}x indicates O(1) or near-constant behavior",
                    growth
                );
                println!("   Root cause must be elsewhere in the code path");
            }
        }
    }

    println!("\n📋 Results per 1000 records:");
    println!("{}", "─".repeat(70));
    for (idx, duration) in &execution_times {
        println!("   Record {}: {:?}", idx, duration);
    }
}
