//! Detailed profiling of SLIDING window analytics performance
//!
//! This test instruments SLIDING window benchmarks to measure overlapping
//! window processing performance with different advance intervals.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::config::StreamingConfig;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

#[tokio::test]
#[serial]
async fn profile_sliding_window_moving_average() {
    println!("\n🔍 SLIDING Window Moving Average Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            symbol,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            COUNT(*) as trade_count
        FROM market_data
        GROUP BY symbol
        WINDOW SLIDING(event_time, 60s, 30s)
    "#;

    // Phase 1: Record Generation
    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000000i64; // milliseconds

    for i in 0..10000 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 10); // 10 different symbols
        let price = 100.0 + (i as f64 % 50.0) + ((i as f64 / 100.0).sin() * 10.0);
        let timestamp = base_time + (i as i64 * 1000); // 1 second intervals

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("event_time".to_string(), FieldValue::Integer(timestamp));

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

    // Enable enhanced configuration with window_v2 for maximum performance
    // This enables all Phase 2A optimizations including window_v2 architecture
    let config = StreamingConfig::enhanced();
    let mut engine = StreamExecutionEngine::new_with_config(tx, config);

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

            if growth_ratio < 2.0 {
                println!("   ✅ ACCEPTABLE: Growth ratio < 2.0x");
            } else {
                println!("   ⚠️  WARNING: Growth ratio > 2.0x (may indicate performance issue)");
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
    println!("🎯 Target: >15,000 records/sec (Phase 1 baseline)");

    if throughput < 15000.0 {
        println!(
            "⚠️  BELOW TARGET by {:.0} rec/s ({:.1}x slower)",
            15000.0 - throughput,
            15000.0 / throughput
        );
    } else {
        println!(
            "✅ ABOVE TARGET by {:.0} rec/s ({:.1}x faster)",
            throughput - 15000.0,
            throughput / 15000.0
        );
    }

    println!("\n📋 SLIDING Window Characteristics:");
    println!("  - Window size: 60 seconds");
    println!("  - Advance interval: 30 seconds (50% overlap)");
    println!("  - Partitions: 10 (symbol-based)");
    println!("  - Aggregations: AVG, MIN, MAX, COUNT");
    println!("  - Expected emissions: Every 30 seconds");
    println!("  - Expected results: ~{} emissions", results.len());
}

#[tokio::test]
#[serial]
async fn profile_sliding_window_with_short_advance() {
    println!("\n🔍 SLIDING Window with Short Advance Interval Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            trader_id,
            symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW SLIDING(trade_time, 30s, 10s)
    "#;

    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000000i64;

    for i in 0..8000 {
        let mut fields = HashMap::new();
        let trader_id = format!("TRADER{}", i % 15);
        let symbol = format!("SYM{}", i % 8);
        let price = 100.0 + (i as f64 % 50.0);
        let quantity = 100 + (i % 500);
        let timestamp = base_time + (i as i64 * 1000);

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
        fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    let phase1_duration = phase1_start.elapsed();
    println!(
        "✅ Record generation: {} records in {:?}",
        records.len(),
        phase1_duration
    );

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

    let execution_start = Instant::now();
    let mut sample_times = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        let record_start = Instant::now();
        let _ = engine.execute_with_record(&query, record.clone()).await;
        let record_duration = record_start.elapsed();

        if idx % 1000 == 0 {
            sample_times.push((idx, record_duration));
        }
    }
    let execution_duration = execution_start.elapsed();

    let _ = engine.flush_windows().await;
    let _ = engine.flush_group_by_results(&query);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let mut results = Vec::new();
    while let Ok(output) = rx.try_recv() {
        results.push(output);
    }

    let throughput = records.len() as f64 / execution_duration.as_secs_f64();

    println!("\n📊 Short Advance Interval Performance:");
    println!("  Records processed: {}", records.len());
    println!("  Execution time: {:?}", execution_duration);
    println!("  Throughput: {:.0} records/sec", throughput);
    println!("  Results emitted: {}", results.len());
    println!("  Window size: 30 seconds, Advance: 10 seconds (67% overlap)");
    println!("  Expected: More frequent emissions than 50% overlap");

    if !sample_times.is_empty() && sample_times.len() >= 2 {
        let first = sample_times[0].1.as_micros() as f64;
        let last = sample_times[sample_times.len() - 1].1.as_micros() as f64;
        let growth_ratio = last / first;
        println!("  Growth ratio: {:.2}x", growth_ratio);
    }
}

#[tokio::test]
#[serial]
async fn profile_sliding_window_with_long_advance() {
    println!("\n🔍 SLIDING Window with Long Advance Interval Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            symbol,
            AVG(price) as avg_price,
            STDDEV(price) as price_volatility,
            MIN(price) as min_price,
            MAX(price) as max_price,
            COUNT(*) as sample_count
        FROM market_data
        GROUP BY symbol
        WINDOW SLIDING(event_time, 120s, 60s)
    "#;

    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000000i64;

    for i in 0..12000 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 12);
        let price = 100.0 + (i as f64 % 100.0) + ((i as f64 / 200.0).sin() * 20.0);
        let timestamp = base_time + (i as i64 * 1000);

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("event_time".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    let phase1_duration = phase1_start.elapsed();
    println!(
        "✅ Record generation: {} records in {:?}",
        records.len(),
        phase1_duration
    );

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

    let execution_start = Instant::now();
    let mut execution_times = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        let record_start = Instant::now();
        let _ = engine.execute_with_record(&query, record.clone()).await;
        let record_duration = record_start.elapsed();

        if idx % 1500 == 0 {
            execution_times.push((idx, record_duration));
            println!("   Record {}: {:?}", idx, record_duration);
        }
    }
    let execution_duration = execution_start.elapsed();

    let _ = engine.flush_windows().await;
    let _ = engine.flush_group_by_results(&query);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let mut results = Vec::new();
    while let Ok(output) = rx.try_recv() {
        results.push(output);
    }

    let throughput = records.len() as f64 / execution_duration.as_secs_f64();

    println!("\n📊 Long Advance Interval Performance:");
    println!("  Records processed: {}", records.len());
    println!("  Execution time: {:?}", execution_duration);
    println!("  Throughput: {:.0} records/sec", throughput);
    println!("  Results emitted: {}", results.len());
    println!("  Window size: 120 seconds, Advance: 60 seconds (50% overlap)");
    println!("  Aggregations tested: AVG, STDDEV, MIN, MAX, COUNT");

    if !execution_times.is_empty() && execution_times.len() >= 2 {
        let first_time = execution_times[0].1.as_micros() as f64;
        let last_time = execution_times[execution_times.len() - 1].1.as_micros() as f64;
        let growth_ratio = last_time / first_time;
        println!("  Growth ratio: {:.2}x", growth_ratio);

        if growth_ratio < 2.0 {
            println!("  ✅ ACCEPTABLE: Growth ratio < 2.0x");
        } else {
            println!("  ⚠️  WARNING: Growth ratio > 2.0x");
        }
    }

    println!("\n📋 Comparison Notes:");
    println!("  - Longer windows hold more state but emit less frequently");
    println!("  - Expected: Similar throughput to shorter windows");
    println!("  - Memory usage scales with window size × partition count");
}
