//! Multi-Source Sink Write Performance Benchmark
//!
//! This benchmark measures the throughput and latency of writing to multiple sinks
//! from multi-source processors, comparing Simple vs Transactional processor performance.
//!
//! Metrics measured:
//! - Records per second (throughput)
//! - Average batch write latency
//! - Total processing time
//! - Write success rate
//!
//! Test scenarios:
//! - Varying number of sinks (2, 4, 8)
//! - Varying record counts (100, 1000, 10000)
//! - Simple vs Transactional processors

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, SimpleJobProcessor, TransactionalJobProcessor,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

/// Mock DataReader for benchmarking
#[derive(Debug)]
pub struct BenchmarkDataReader {
    pub name: String,
    pub records: Vec<StreamRecord>,
    pub current_index: usize,
    pub batch_size: usize,
}

impl BenchmarkDataReader {
    pub fn new(name: &str, record_count: usize, batch_size: usize) -> Self {
        let mut records = Vec::with_capacity(record_count);
        for i in 0..record_count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "name".to_string(),
                FieldValue::String(format!("record_{}", i)),
            );
            fields.insert("source".to_string(), FieldValue::String(name.to_string()));
            fields.insert("value".to_string(), FieldValue::Float((i as f64) * 1.5));

            records.push(StreamRecord {
                fields,
                headers: HashMap::new(),
                event_time: None,
                timestamp: 1640995200000 + (i as i64 * 1000),
                offset: i as i64,
                partition: 0,
            });
        }

        Self {
            name: name.to_string(),
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for BenchmarkDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let remaining = self.records.len() - self.current_index;
        let batch_size = self.batch_size.min(remaining);

        if batch_size == 0 {
            return Ok(vec![]);
        }

        let end_index = self.current_index + batch_size;
        let batch = self.records[self.current_index..end_index].to_vec();
        self.current_index = end_index;

        Ok(batch)
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::types::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_index < self.records.len())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        false
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Mock DataWriter with performance metrics
#[derive(Debug, Clone)]
pub struct BenchmarkDataWriter {
    pub name: String,
    pub written_records: Arc<StdMutex<Vec<StreamRecord>>>,
    pub write_times: Arc<StdMutex<Vec<Duration>>>,
    pub batch_count: Arc<StdMutex<usize>>,
}

impl BenchmarkDataWriter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            written_records: Arc::new(StdMutex::new(Vec::new())),
            write_times: Arc::new(StdMutex::new(Vec::new())),
            batch_count: Arc::new(StdMutex::new(0)),
        }
    }

    pub fn get_written_count(&self) -> usize {
        self.written_records.lock().unwrap().len()
    }

    pub fn get_batch_count(&self) -> usize {
        *self.batch_count.lock().unwrap()
    }

    pub fn get_average_write_time(&self) -> Duration {
        let times = self.write_times.lock().unwrap();
        if times.is_empty() {
            return Duration::from_nanos(0);
        }
        let total: Duration = times.iter().sum();
        total / times.len() as u32
    }

    pub fn get_metrics(&self) -> BenchmarkMetrics {
        let written_count = self.get_written_count();
        let batch_count = self.get_batch_count();
        let avg_write_time = self.get_average_write_time();

        BenchmarkMetrics {
            total_records: written_count,
            total_batches: batch_count,
            avg_batch_write_time: avg_write_time,
        }
    }
}

#[async_trait]
impl DataWriter for BenchmarkDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        self.written_records.lock().unwrap().push(record);
        self.write_times.lock().unwrap().push(start.elapsed());
        *self.batch_count.lock().unwrap() += 1;
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        self.written_records.lock().unwrap().extend(records);
        self.write_times.lock().unwrap().push(start.elapsed());
        *self.batch_count.lock().unwrap() += 1;
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

    fn supports_transactions(&self) -> bool {
        true
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(true)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    pub total_records: usize,
    pub total_batches: usize,
    pub avg_batch_write_time: Duration,
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub processor_type: String,
    pub num_sources: usize,
    pub num_sinks: usize,
    pub total_records: usize,
    pub duration: Duration,
    pub records_per_second: f64,
    pub avg_batch_write_time: Duration,
    pub total_batches: usize,
    pub success_rate: f64,
}

impl BenchmarkResult {
    pub fn print(&self) {
        println!("\n========================================");
        println!("Benchmark Results: {}", self.processor_type);
        println!("========================================");
        println!("Configuration:");
        println!("  Sources:  {}", self.num_sources);
        println!("  Sinks:    {}", self.num_sinks);
        println!("  Records:  {}", self.total_records);
        println!("\nPerformance:");
        println!("  Duration:           {:?}", self.duration);
        println!("  Throughput:         {:.2} records/sec", self.records_per_second);
        println!("  Total Batches:      {}", self.total_batches);
        println!("  Avg Batch Time:     {:?}", self.avg_batch_write_time);
        println!("  Success Rate:       {:.2}%", self.success_rate * 100.0);
        println!("========================================\n");
    }
}

/// Run benchmark for simple processor
async fn benchmark_simple_processor(
    num_sources: usize,
    num_sinks: usize,
    records_per_source: usize,
    batch_size: usize,
) -> BenchmarkResult {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: batch_size,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    // Create sources
    let mut readers = HashMap::new();
    for i in 0..num_sources {
        let source_name = format!("source_{}", i);
        readers.insert(
            source_name.clone(),
            Box::new(BenchmarkDataReader::new(&source_name, records_per_source, batch_size))
                as Box<dyn DataReader>,
        );
    }

    // Create sinks
    let mut writers = HashMap::new();
    let mut writer_clones = Vec::new();
    for i in 0..num_sinks {
        let sink_name = format!("sink_{}", i);
        let writer = BenchmarkDataWriter::new(&sink_name);
        writer_clones.push(writer.clone());
        writers.insert(sink_name, Box::new(writer) as Box<dyn DataWriter>);
    }

    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();

    // Run processor
    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(
                readers,
                writers,
                engine,
                query,
                "benchmark_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Wait for processing to complete
    let _ = job_handle.await;
    let duration = start.elapsed();

    // Collect metrics from all sinks
    let total_records: usize = writer_clones.iter().map(|w| w.get_written_count()).sum();
    let total_batches: usize = writer_clones.iter().map(|w| w.get_batch_count()).sum();
    let avg_write_times: Vec<Duration> =
        writer_clones.iter().map(|w| w.get_average_write_time()).collect();
    let avg_batch_write_time = if !avg_write_times.is_empty() {
        avg_write_times.iter().sum::<Duration>() / avg_write_times.len() as u32
    } else {
        Duration::from_nanos(0)
    };

    let expected_records = num_sources * records_per_source * num_sinks;
    let success_rate = total_records as f64 / expected_records as f64;
    let records_per_second = total_records as f64 / duration.as_secs_f64();

    BenchmarkResult {
        processor_type: "Simple".to_string(),
        num_sources,
        num_sinks,
        total_records,
        duration,
        records_per_second,
        avg_batch_write_time,
        total_batches,
        success_rate,
    }
}

/// Run benchmark for transactional processor
async fn benchmark_transactional_processor(
    num_sources: usize,
    num_sinks: usize,
    records_per_source: usize,
    batch_size: usize,
) -> BenchmarkResult {
    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: batch_size,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
    };

    let processor = TransactionalJobProcessor::new(config);

    // Create sources
    let mut readers = HashMap::new();
    for i in 0..num_sources {
        let source_name = format!("source_{}", i);
        readers.insert(
            source_name.clone(),
            Box::new(BenchmarkDataReader::new(&source_name, records_per_source, batch_size))
                as Box<dyn DataReader>,
        );
    }

    // Create sinks
    let mut writers = HashMap::new();
    let mut writer_clones = Vec::new();
    for i in 0..num_sinks {
        let sink_name = format!("sink_{}", i);
        let writer = BenchmarkDataWriter::new(&sink_name);
        writer_clones.push(writer.clone());
        writers.insert(sink_name, Box::new(writer) as Box<dyn DataWriter>);
    }

    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();

    // Run processor
    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(
                readers,
                writers,
                engine,
                query,
                "benchmark_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Wait for processing to complete
    let _ = job_handle.await;
    let duration = start.elapsed();

    // Collect metrics from all sinks
    let total_records: usize = writer_clones.iter().map(|w| w.get_written_count()).sum();
    let total_batches: usize = writer_clones.iter().map(|w| w.get_batch_count()).sum();
    let avg_write_times: Vec<Duration> =
        writer_clones.iter().map(|w| w.get_average_write_time()).collect();
    let avg_batch_write_time = if !avg_write_times.is_empty() {
        avg_write_times.iter().sum::<Duration>() / avg_write_times.len() as u32
    } else {
        Duration::from_nanos(0)
    };

    let expected_records = num_sources * records_per_source * num_sinks;
    let success_rate = total_records as f64 / expected_records as f64;
    let records_per_second = total_records as f64 / duration.as_secs_f64();

    BenchmarkResult {
        processor_type: "Transactional".to_string(),
        num_sources,
        num_sinks,
        total_records,
        duration,
        records_per_second,
        avg_batch_write_time,
        total_batches,
        success_rate,
    }
}

// Benchmark Tests

#[tokio::test]
async fn benchmark_2_sinks_100_records() {
    println!("\n=== Benchmark: 2 Sinks, 100 Records per Source ===");

    let simple = benchmark_simple_processor(2, 2, 100, 10).await;
    simple.print();

    let transactional = benchmark_transactional_processor(2, 2, 100, 10).await;
    transactional.print();

    println!("Comparison:");
    println!(
        "  Simple vs Transactional throughput: {:.2}x",
        simple.records_per_second / transactional.records_per_second
    );
}

#[tokio::test]
async fn benchmark_4_sinks_1000_records() {
    println!("\n=== Benchmark: 4 Sinks, 1000 Records per Source ===");

    let simple = benchmark_simple_processor(2, 4, 1000, 50).await;
    simple.print();

    let transactional = benchmark_transactional_processor(2, 4, 1000, 50).await;
    transactional.print();

    println!("Comparison:");
    println!(
        "  Simple vs Transactional throughput: {:.2}x",
        simple.records_per_second / transactional.records_per_second
    );
}

#[tokio::test]
async fn benchmark_8_sinks_10000_records() {
    println!("\n=== Benchmark: 8 Sinks, 10000 Records per Source ===");

    let simple = benchmark_simple_processor(2, 8, 10000, 100).await;
    simple.print();

    let transactional = benchmark_transactional_processor(2, 8, 10000, 100).await;
    transactional.print();

    println!("Comparison:");
    println!(
        "  Simple vs Transactional throughput: {:.2}x",
        simple.records_per_second / transactional.records_per_second
    );
}

#[tokio::test]
async fn benchmark_scalability_test() {
    println!("\n=== Benchmark: Scalability Test (Varying Sink Count) ===");

    let mut results_simple = Vec::new();
    let mut results_transactional = Vec::new();

    for num_sinks in [2, 4, 8] {
        println!("\n--- Testing with {} sinks ---", num_sinks);

        let simple = benchmark_simple_processor(2, num_sinks, 1000, 50).await;
        simple.print();
        results_simple.push(simple);

        let transactional = benchmark_transactional_processor(2, num_sinks, 1000, 50).await;
        transactional.print();
        results_transactional.push(transactional);
    }

    println!("\n=== Scalability Summary ===");
    println!("\nSimple Processor:");
    for result in &results_simple {
        println!(
            "  {} sinks: {:.2} records/sec, {:.2}% success",
            result.num_sinks,
            result.records_per_second,
            result.success_rate * 100.0
        );
    }

    println!("\nTransactional Processor:");
    for result in &results_transactional {
        println!(
            "  {} sinks: {:.2} records/sec, {:.2}% success",
            result.num_sinks,
            result.records_per_second,
            result.success_rate * 100.0
        );
    }
}
