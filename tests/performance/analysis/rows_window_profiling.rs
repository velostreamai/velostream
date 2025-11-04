//! Detailed profiling of ROWS WINDOW analytics performance
//!
//! This test instruments ROWS WINDOW benchmarks to measure memory-bounded
//! row-count-based window processing performance.

use serial_test::serial;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

#[tokio::test]
#[serial]
async fn profile_rows_window_moving_average() {
    println!("\n🔍 ROWS WINDOW Moving Average Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            symbol,
            price,
            AVG(price) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as moving_avg,
            MIN(price) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as min_price,
            MAX(price) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as max_price
        FROM market_data
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
        fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

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

        // Calculate growth ratio (should be ~1.0 for bounded buffer)
        if execution_times.len() >= 2 {
            let first_time = execution_times[0].1.as_micros() as f64;
            let last_time = execution_times[execution_times.len() - 1].1.as_micros() as f64;
            let growth_ratio = last_time / first_time;
            println!("   Growth ratio (last/first): {:.2}x", growth_ratio);

            if growth_ratio < 1.5 {
                println!("   ✅ BOUNDED: Growth ratio < 1.5x (expected for ROWS WINDOW)");
            } else {
                println!("   ⚠️  WARNING: Growth ratio > 1.5x (buffer may not be bounded)");
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
    println!("🎯 Target: >20,000 records/sec");

    if throughput < 20000.0 {
        println!(
            "⚠️  BELOW TARGET by {:.0} rec/s ({:.1}x slower)",
            20000.0 - throughput,
            20000.0 / throughput
        );
    } else {
        println!(
            "✅ ABOVE TARGET by {:.0} rec/s ({:.1}x faster)",
            throughput - 20000.0,
            throughput / 20000.0
        );
    }

    println!("\n📋 ROWS WINDOW Characteristics:");
    println!("  - Buffer size: 100 rows (bounded memory)");
    println!("  - Partitions: 10 (symbol-based)");
    println!("  - Aggregations: AVG, MIN, MAX");
    println!("  - Expected behavior: Constant-time per record after buffer fills");
}

#[tokio::test]
#[serial]
async fn profile_rows_window_ranking_functions() {
    println!("\n🔍 ROWS WINDOW Ranking Functions Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            symbol,
            price,
            RANK() OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY price DESC
            ) as price_rank,
            DENSE_RANK() OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY price DESC
            ) as dense_rank,
            ROW_NUMBER() OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as row_num
        FROM market_data
    "#;

    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000000i64;

    for i in 0..5000 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 5);
        let price = 100.0 + (i as f64 % 100.0);
        let timestamp = base_time + (i as i64 * 1000);

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

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
    for record in &records {
        let _ = engine.execute_with_record(&query, record.clone()).await;
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

    println!("\n📊 Ranking Functions Performance:");
    println!("  Records processed: {}", records.len());
    println!("  Execution time: {:?}", execution_duration);
    println!("  Throughput: {:.0} records/sec", throughput);
    println!("  Results emitted: {}", results.len());
    println!("  Functions tested: RANK, DENSE_RANK, ROW_NUMBER");
}

#[tokio::test]
#[serial]
async fn profile_rows_window_offset_functions() {
    println!("\n🔍 ROWS WINDOW Offset Functions (LAG/LEAD) Performance Profile");
    println!("{}", "=".repeat(70));

    let sql = r#"
        SELECT
            symbol,
            price,
            LAG(price, 1) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as prev_price,
            LEAD(price, 1) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as next_price,
            price - LAG(price, 1) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as price_change
        FROM market_data
    "#;

    let phase1_start = Instant::now();
    let mut records = Vec::new();
    let base_time = 1700000000000i64;

    for i in 0..8000 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 8);
        let price = 100.0 + (i as f64 % 50.0) + ((i as f64 / 50.0).sin() * 5.0);
        let timestamp = base_time + (i as i64 * 1000);

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    let phase1_duration = phase1_start.elapsed();

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

    println!("\n📊 Offset Functions Performance:");
    println!("  Records processed: {}", records.len());
    println!("  Execution time: {:?}", execution_duration);
    println!("  Throughput: {:.0} records/sec", throughput);
    println!("  Results emitted: {}", results.len());
    println!("  Functions tested: LAG, LEAD, computed price_change");

    if !sample_times.is_empty() {
        let first = sample_times[0].1.as_micros() as f64;
        let last = sample_times[sample_times.len() - 1].1.as_micros() as f64;
        let growth_ratio = last / first;
        println!("  Growth ratio: {:.2}x", growth_ratio);
    }
}
