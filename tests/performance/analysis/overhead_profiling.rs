/*!
# FR-082 Overhead Analysis: Job Server Component Profiling

Detailed profiling to understand why production throughput (28K rec/sec) is 76% slower
than pure SQL engine performance (127K rec/sec).

## Methodology
Measures incremental overhead of each component:
1. Pure SQL engine (baseline)
2. + Batch coordination
3. + Metrics/profiling
4. + Error handling
5. + Lock contention
6. Full job server

This identifies which components contribute most to the 127K → 28K slowdown.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::common::{
    BatchProcessingResult, JobProcessingConfig, FailureStrategy,
};
use velostream::velostream::server::processors::simple::SimpleJobProcessor;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::StreamingQuery;

/// FR-082 Overhead Analysis: Component-by-component profiling
///
/// Measures the incremental overhead of each job server component to identify
/// the primary bottlenecks causing the 76% performance drop.
#[tokio::test]
async fn profile_overhead_components() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 OVERHEAD ANALYSIS: Component Profiling");
    println!("═══════════════════════════════════════════════════════════\n");
    println!("Goal: Identify which components cause 127K → 28K slowdown\n");

    let num_records = 50_000;
    let sql = r#"
        SELECT
            category,
            COUNT(*) as count,
            SUM(amount) as total,
            AVG(price) as avg_price
        FROM stream
        GROUP BY category
    "#;

    // Generate test data once
    let records = generate_records(num_records);
    println!("✅ Generated {} test records\n", num_records);

    // Component 1: Pure SQL Engine (Baseline)
    let baseline = profile_pure_engine(&records, sql).await;
    println!("📊 Component 1: Pure SQL Engine");
    println!("   Throughput: {:.0} rec/sec (BASELINE)", baseline);
    println!("   Overhead:   0%\n");

    // Component 2: + Batch Coordination
    let with_batching = profile_with_batching(&records, sql).await;
    let batch_overhead = calc_overhead(baseline, with_batching);
    println!("📊 Component 2: + Batch Coordination");
    println!("   Throughput: {:.0} rec/sec", with_batching);
    println!("   Overhead:   {:.1}%\n", batch_overhead);

    // Component 3: + Arc<Mutex<>> Lock Contention
    let with_locks = profile_with_locks(&records, sql).await;
    let lock_overhead = calc_overhead(baseline, with_locks);
    println!("📊 Component 3: + Lock Contention (Arc<Mutex<>>)");
    println!("   Throughput: {:.0} rec/sec", with_locks);
    println!("   Overhead:   {:.1}%\n", lock_overhead);

    // Component 4: + Metrics Collection
    let with_metrics = profile_with_metrics(&records, sql).await;
    let metrics_overhead = calc_overhead(baseline, with_metrics);
    println!("📊 Component 4: + Metrics Collection");
    println!("   Throughput: {:.0} rec/sec", with_metrics);
    println!("   Overhead:   {:.1}%\n", metrics_overhead);

    // Summary
    println!("\n═══════════════════════════════════════════════════════════");
    println!("📈 OVERHEAD BREAKDOWN SUMMARY");
    println!("═══════════════════════════════════════════════════════════");
    println!("Baseline (Pure Engine):        {:.0} rec/sec", baseline);
    println!("+ Batch Coordination:          {:.0} rec/sec ({:.1}% overhead)",
             with_batching, batch_overhead);
    println!("+ Lock Contention:             {:.0} rec/sec ({:.1}% overhead)",
             with_locks, lock_overhead);
    println!("+ Metrics Collection:          {:.0} rec/sec ({:.1}% overhead)",
             with_metrics, metrics_overhead);
    println!();
    println!("Expected Production (~28K):    ~{:.0} rec/sec",
             baseline * 0.22); // 78% overhead
    println!("═══════════════════════════════════════════════════════════\n");

    // Identify top contributor
    let overheads = vec![
        ("Batch Coordination", batch_overhead),
        ("Lock Contention", lock_overhead),
        ("Metrics Collection", metrics_overhead),
    ];
    let max = overheads.iter().max_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).unwrap();

    println!("🎯 PRIMARY BOTTLENECK: {} ({:.1}% overhead)", max.0, max.1);
    println!("   Recommendation: Focus optimization efforts here\n");
}

fn generate_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert("category".to_string(), FieldValue::String(format!("CAT{}", i % 50)));
        fields.insert("amount".to_string(), FieldValue::Float((i % 1000) as f64));
        fields.insert("price".to_string(), FieldValue::Float(100.0 + (i % 500) as f64));
        records.push(StreamRecord::new(fields));
    }
    records
}

async fn profile_pure_engine(records: &[StreamRecord], sql: &str) -> f64 {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    let start = Instant::now();
    for record in records {
        let _ = engine.execute_with_record(&query, record.clone()).await;
    }
    let duration = start.elapsed();

    // Drain channel
    while rx.try_recv().is_ok() {}

    records.len() as f64 / duration.as_secs_f64()
}

async fn profile_with_batching(records: &[StreamRecord], sql: &str) -> f64 {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    let batch_size = 1000;
    let start = Instant::now();

    // Process in batches (simulating batch coordination overhead)
    for chunk in records.chunks(batch_size) {
        let batch = chunk.to_vec(); // Allocation overhead
        for record in batch {
            let _ = engine.execute_with_record(&query, record).await;
        }
    }
    let duration = start.elapsed();

    while rx.try_recv().is_ok() {}
    records.len() as f64 / duration.as_secs_f64()
}

async fn profile_with_locks(records: &[StreamRecord], sql: &str) -> f64 {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    let batch_size = 1000;
    let start = Instant::now();

    // Batch processing with lock contention
    for chunk in records.chunks(batch_size) {
        let batch = chunk.to_vec();

        // Lock acquisition overhead
        let mut engine_guard = engine.lock().await;
        for record in batch {
            let _ = engine_guard.execute_with_record(&query, record).await;
        }
        drop(engine_guard); // Explicit unlock
    }
    let duration = start.elapsed();

    while rx.try_recv().is_ok() {}
    records.len() as f64 / duration.as_secs_f64()
}

async fn profile_with_metrics(records: &[StreamRecord], sql: &str) -> f64 {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    let batch_size = 1000;
    let start = Instant::now();

    // Batch processing with locks + metrics
    let mut total_processed = 0;
    let mut batch_times = Vec::new();

    for chunk in records.chunks(batch_size) {
        let batch_start = Instant::now();
        let batch = chunk.to_vec();

        let mut engine_guard = engine.lock().await;
        for record in batch {
            let _ = engine_guard.execute_with_record(&query, record).await;
            total_processed += 1; // Metrics overhead
        }
        drop(engine_guard);

        batch_times.push(batch_start.elapsed()); // Time tracking overhead
    }
    let duration = start.elapsed();

    // Metrics calculation overhead
    let _avg_batch_time: Duration = batch_times.iter().sum::<Duration>() / batch_times.len() as u32;
    let _throughput = total_processed as f64 / duration.as_secs_f64();

    while rx.try_recv().is_ok() {}
    records.len() as f64 / duration.as_secs_f64()
}

fn calc_overhead(baseline: f64, measured: f64) -> f64 {
    ((baseline - measured) / baseline) * 100.0
}
