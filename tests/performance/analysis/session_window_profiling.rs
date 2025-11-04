//! Detailed profiling of SESSION window performance
//!
//! This test instruments the SESSION window benchmark to identify bottlenecks.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

#[tokio::test]
#[serial]
async fn profile_session_window_iot_clustering() {
    println!("\n🔍 SESSION Window IoT Clustering Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            device_id,
            COUNT(*) as reading_count,
            AVG(sensor_value) as avg_value
        FROM sensor_readings
        GROUP BY device_id
        WINDOW SESSION(10m)
    "#;

    // Phase 1: Record Generation
    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000i64;

    // Start with 1000 records to get quick profiling results
    for i in 0..1000 {
        let mut fields = HashMap::new();
        let device_id = format!("DEVICE{}", i % 50);
        let sensor_value = 20.0 + (i as f64 % 80.0);
        let gap = if i % 100 == 0 { 700 } else { 5 };
        let timestamp = base_time + (i as i64 * gap);

        fields.insert("device_id".to_string(), FieldValue::String(device_id));
        fields.insert("sensor_value".to_string(), FieldValue::Float(sensor_value));
        fields.insert("_timestamp".to_string(), FieldValue::Integer(timestamp));

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
    let sample_interval = 100; // Sample every 100 records

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

    println!("\n📊 PERFORMANCE BREAKDOWN");
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
    println!("🎯 Target: >15,000 records/sec");

    if throughput < 15000.0 {
        println!(
            "⚠️  BELOW TARGET by {:.0} rec/s ({:.1}x slower)",
            15000.0 - throughput,
            15000.0 / throughput
        );
    }
}
