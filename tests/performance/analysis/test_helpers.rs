//! Shared Test Helpers for Performance Analysis Tests
//!
//! Common mock implementations and utilities used across profiling tests.

use async_trait::async_trait;
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::common::JobExecutionStats;
use velostream::velostream::sql::ast::StreamingQuery;
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Shared state for MockDataSource to ensure clones see the same batch count
#[derive(Clone)]
struct SharedState {
    batches_read: Arc<AtomicUsize>,
    all_consumed: Arc<AtomicUsize>,
}

/// Mock data source for job server performance testing
#[derive(Clone)]
pub struct MockDataSource {
    batch: Vec<StreamRecord>,
    batch_template: Vec<StreamRecord>,
    total_batches: usize,
    state: SharedState,
}

impl MockDataSource {
    pub fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        let batch: Vec<StreamRecord> = records.iter().take(batch_size).cloned().collect();
        let total_batches = (records.len() + batch_size - 1) / batch_size;

        Self {
            batch: batch.clone(),
            batch_template: batch,
            total_batches,
            state: SharedState {
                batches_read: Arc::new(AtomicUsize::new(0)),
                all_consumed: Arc::new(AtomicUsize::new(0)),
            },
        }
    }

    /// Returns true when all records have been consumed
    pub fn all_consumed(&self) -> bool {
        self.state.all_consumed.load(Ordering::SeqCst) != 0
    }
}

#[async_trait]
impl DataReader for MockDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let batches_read = self.state.batches_read.load(Ordering::SeqCst);
        if batches_read >= self.total_batches {
            // Mark that all records have been consumed
            self.state.all_consumed.store(1, Ordering::SeqCst);
            return Ok(vec![]);
        }

        let batch = std::mem::take(&mut self.batch);
        self.state.batches_read.fetch_add(1, Ordering::SeqCst);
        self.batch = self.batch_template.clone();

        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let batches_read = self.state.batches_read.load(Ordering::SeqCst);
        Ok(batches_read < self.total_batches)
    }
}

/// Mock data writer for job server performance testing
#[derive(Clone)]
pub struct MockDataWriter {
    count: Arc<AtomicUsize>,
    last_record: Arc<Mutex<Option<StreamRecord>>>,
}

impl MockDataWriter {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            last_record: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn get_last_record(&self) -> Option<StreamRecord> {
        self.last_record.lock().unwrap().clone()
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        *self.last_record.lock().unwrap() = Some(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(records.len(), Ordering::SeqCst);
        if let Some(last) = records.last() {
            *self.last_record.lock().unwrap() = Some((**last).clone());
        }
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Generate test records for tumbling window queries
///
/// Creates records with trader_id, symbol, price, quantity, and trade_time fields.
/// This is the standard dataset used for tumbling window performance tests.
pub fn generate_tumbling_window_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000i64;

    for i in 0..count {
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
    records
}

/// Generate test records for GROUP BY queries
///
/// Creates records with category, amount, and price fields.
/// This is the standard dataset used for GROUP BY performance tests.
pub fn generate_group_by_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let mut fields = HashMap::new();
        let category = format!("CAT{}", i % 50);
        let amount = (i % 1000) as f64;
        let price = 100.0 + (i as f64 % 500.0);

        fields.insert("category".to_string(), FieldValue::String(category));
        fields.insert("amount".to_string(), FieldValue::Float(amount));
        fields.insert("price".to_string(), FieldValue::Float(price));

        records.push(StreamRecord::new(fields));
    }
    records
}

/// Measure pure SQL engine performance without job server
/// Returns (output_record_count, elapsed_microseconds)
pub async fn measure_sql_engine_performance(
    records: Vec<StreamRecord>,
    query: &StreamingQuery,
    optional_final_record_push_time: Option<i64>,
) -> (usize, u128) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in records.iter() {
        let _ = engine.execute_with_record(query, record).await;
    }

    // If provided, inject final record to trigger window closure
    if let Some(final_time) = optional_final_record_push_time {
        if let Some(last_record) = records.last() {
            let mut final_record = last_record.clone();
            final_record.timestamp = final_time;
            let _ = engine.execute_with_record(query, &final_record).await;
        }
    }

    let elapsed = start.elapsed();

    // Flush windows to ensure all results are emitted
    let _ = engine.flush_windows().await;

    // Collect results from channel
    let mut result_count = 0;
    while let Ok(result) = rx.try_recv() {
        result_count += 1;
        debug!(
            "Emitted result: {:?}",
            result.fields.keys().collect::<Vec<_>>()
        );
    }

    (result_count, elapsed.as_micros())
}

/// Calculate and print overhead analysis
pub fn print_overhead_analysis(scenario_name: &str, sql_throughput: usize, job_throughput: f64) {
    let overhead_pct = if sql_throughput > 0 {
        ((sql_throughput as f64 - job_throughput) / sql_throughput as f64) * 100.0
    } else {
        0.0
    };

    let slowdown_factor = if job_throughput > 0.0 {
        sql_throughput as f64 / job_throughput
    } else {
        0.0
    };

    println!("═══════════════════════════════════════════════════════════");
    println!("📊 OVERHEAD ANALYSIS: {}", scenario_name);
    println!("═══════════════════════════════════════════════════════════");
    println!(
        "SQL Engine Throughput: {:.0} rec/sec",
        sql_throughput as f64
    );
    println!("Job Server Throughput: {:.0} rec/sec", job_throughput);
    println!();
    println!("Job Server Overhead:   {:.1}%", overhead_pct);
    println!("Slowdown Factor:       {:.2}x", slowdown_factor);
    println!();

    if overhead_pct < 10.0 {
        println!("✅ Excellent: Minimal job server overhead");
    } else if overhead_pct < 30.0 {
        println!("⚡ Good: Acceptable overhead for production");
    } else if overhead_pct < 50.0 {
        println!("⚠️  Moderate overhead - investigate optimization");
    } else if overhead_pct > 90.0 {
        println!("❌ Significant overhead detected - major bottleneck");
    }

    println!("═══════════════════════════════════════════════════════════\n");
}

/// Print performance comparison results
pub fn print_performance_comparison(
    test_name: &str,
    baseline_name: &str,
    baseline_throughput: f64,
    measured_name: &str,
    measured_throughput: f64,
) {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("📊 {}", test_name);
    println!("═══════════════════════════════════════════════════════════");
    println!("{:<20} {:.0} rec/sec", baseline_name, baseline_throughput);
    println!("{:<20} {:.0} rec/sec", measured_name, measured_throughput);
    println!();

    let slowdown_factor = baseline_throughput / measured_throughput;
    let overhead_percent =
        ((baseline_throughput - measured_throughput) / baseline_throughput) * 100.0;

    println!("Slowdown Factor:     {:.2}x", slowdown_factor);
    println!("Overhead:            {:.1}%", overhead_percent);
    println!();

    if overhead_percent < 10.0 {
        println!("✅ Minimal overhead - well optimized");
    } else if overhead_percent < 30.0 {
        println!("⚡ Acceptable overhead for production features");
    } else if overhead_percent < 50.0 {
        println!("⚠️  Moderate overhead - investigate further");
    } else {
        println!("❌ Significant overhead detected");
    }

    println!("═══════════════════════════════════════════════════════════\n");
}

/// Job metrics comparable to JobServer's JobMetrics structure
///
/// This struct captures performance metrics extracted from JobExecutionStats,
/// providing throughput, latency, and batch efficiency measurements similar to
/// what the JobServer would report.
#[derive(Debug, Clone)]
pub struct JobServerMetrics {
    pub records_processed: u64,
    pub batches_processed: u64,
    pub records_failed: u64,
    pub batches_failed: u64,
    pub throughput_rec_per_sec: f64,
    pub avg_latency_micros: f64,
    pub avg_records_per_batch: f64,
    pub total_duration_micros: u128,
}

impl JobServerMetrics {
    /// Create metrics from JobExecutionStats and elapsed duration
    pub fn from_stats(stats: &JobExecutionStats, duration_micros: u128) -> Self {
        let throughput = if duration_micros > 0 {
            (stats.records_processed as f64 / duration_micros as f64) * 1_000_000.0
        } else {
            0.0
        };

        let avg_latency = if stats.records_processed > 0 {
            duration_micros as f64 / stats.records_processed as f64
        } else {
            0.0
        };

        let avg_records_per_batch = if stats.batches_processed > 0 {
            stats.records_processed as f64 / stats.batches_processed as f64
        } else {
            0.0
        };

        Self {
            records_processed: stats.records_processed,
            batches_processed: stats.batches_processed,
            records_failed: stats.records_failed,
            batches_failed: stats.batches_failed,
            throughput_rec_per_sec: throughput,
            avg_latency_micros: avg_latency,
            avg_records_per_batch,
            total_duration_micros: duration_micros,
        }
    }

    /// Print metrics in a formatted table
    pub fn print_table(&self, label: &str) {
        println!("\n┌─ {} JobMetrics", label);
        println!("│  Records Processed:    {}", self.records_processed);
        println!("│  Records Failed:       {}", self.records_failed);
        println!("│  Batches Processed:    {}", self.batches_processed);
        println!("│  Batches Failed:       {}", self.batches_failed);
        println!(
            "│  Throughput:           {:.0} rec/sec",
            self.throughput_rec_per_sec
        );
        println!(
            "│  Avg Latency:          {:.3} µs/record",
            self.avg_latency_micros
        );
        println!(
            "│  Avg Batch Size:       {:.1} records/batch",
            self.avg_records_per_batch
        );
        println!(
            "│  Total Duration:       {:.2} ms",
            self.total_duration_micros as f64 / 1000.0
        );
        println!("└─\n");
    }

    /// Print metrics in compact single-line format
    pub fn print_compact(&self, label: &str) {
        println!(
            "{}: {:.0} rec/sec | {} records | {} batches | {:.3} µs/record",
            label,
            self.throughput_rec_per_sec,
            self.records_processed,
            self.batches_processed,
            self.avg_latency_micros
        );
    }
}

/// Validate that a benchmark result has meaningful content
pub fn validate_benchmark_record(record: &Option<StreamRecord>, scenario_name: &str) -> bool {
    match record {
        Some(rec) => {
            if rec.fields.is_empty() {
                eprintln!("❌ {} - Last record has no fields!", scenario_name);
                return false;
            }

            // Log the fields present in the last record for verification
            let field_names: Vec<&String> = rec.fields.keys().collect();
            println!(
                "  ✓ {} - Last record has {} fields: {:?}",
                scenario_name,
                field_names.len(),
                field_names
            );

            true
        }
        None => {
            eprintln!("❌ {} - No records were written!", scenario_name);
            false
        }
    }
}
