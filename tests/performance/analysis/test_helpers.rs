//! Shared Test Helpers for Performance Analysis Tests
//!
//! Common mock implementations and utilities used across profiling tests.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Mock data source for job server performance testing
pub struct MockDataSource {
    batch: Vec<StreamRecord>,
    batch_template: Vec<StreamRecord>,
    batches_read: usize,
    total_batches: usize,
}

impl MockDataSource {
    pub fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        let batch: Vec<StreamRecord> = records.iter().take(batch_size).cloned().collect();
        let total_batches = (records.len() + batch_size - 1) / batch_size;

        Self {
            batch: batch.clone(),
            batch_template: batch,
            batches_read: 0,
            total_batches,
        }
    }
}

#[async_trait]
impl DataReader for MockDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.batches_read >= self.total_batches {
            return Ok(vec![]);
        }

        let batch = std::mem::take(&mut self.batch);
        self.batches_read += 1;
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
        Ok(self.batches_read < self.total_batches)
    }
}

/// Mock data writer for job server performance testing
pub struct MockDataWriter {
    count: Arc<AtomicUsize>,
}

impl MockDataWriter {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(records.len(), Ordering::SeqCst);
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(
        &mut self,
        _key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer(quantity as i64),
        );
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
    let overhead_percent = ((baseline_throughput - measured_throughput) / baseline_throughput) * 100.0;

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
