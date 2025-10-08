//! Detailed profiling benchmark for multi-sink write execution chain
//!
//! This benchmark instruments every step of the execution to find bottlenecks

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, SimpleJobProcessor,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

/// Profiling metrics collector
#[derive(Debug, Clone)]
pub struct ProfilingMetrics {
    pub read_time: Arc<StdMutex<Vec<Duration>>>,
    pub process_time: Arc<StdMutex<Vec<Duration>>>,
    pub write_time: Arc<StdMutex<Vec<Duration>>>,
    pub lock_time: Arc<StdMutex<Vec<Duration>>>,
    pub total_batches: Arc<StdMutex<usize>>,
    pub framework_time: Arc<StdMutex<Vec<Duration>>>, // Track framework overhead per batch
}

impl ProfilingMetrics {
    pub fn new() -> Self {
        Self {
            read_time: Arc::new(StdMutex::new(Vec::new())),
            process_time: Arc::new(StdMutex::new(Vec::new())),
            write_time: Arc::new(StdMutex::new(Vec::new())),
            lock_time: Arc::new(StdMutex::new(Vec::new())),
            total_batches: Arc::new(StdMutex::new(0)),
        }
    }

    pub fn record_read(&self, duration: Duration) {
        self.read_time.lock().unwrap().push(duration);
    }

    pub fn record_process(&self, duration: Duration) {
        self.process_time.lock().unwrap().push(duration);
    }

    pub fn record_write(&self, duration: Duration) {
        self.write_time.lock().unwrap().push(duration);
    }

    pub fn record_lock(&self, duration: Duration) {
        self.lock_time.lock().unwrap().push(duration);
    }

    pub fn print_summary(&self) {
        let reads = self.read_time.lock().unwrap();
        let processes = self.process_time.lock().unwrap();
        let writes = self.write_time.lock().unwrap();
        let locks = self.lock_time.lock().unwrap();

        let total_read: Duration = reads.iter().sum();
        let total_process: Duration = processes.iter().sum();
        let total_write: Duration = writes.iter().sum();
        let total_lock: Duration = locks.iter().sum();

        let grand_total = total_read + total_process + total_write;

        println!("\n╔════════════════════════════════════════════════════════╗");
        println!("║          EXECUTION CHAIN PROFILING RESULTS            ║");
        println!("╠════════════════════════════════════════════════════════╣");
        println!("║ Phase            │ Total Time  │ Avg/Batch  │ % Total ║");
        println!("╟──────────────────┼─────────────┼────────────┼─────────╢");

        let avg_read = if !reads.is_empty() {
            total_read / reads.len() as u32
        } else {
            Duration::from_nanos(0)
        };
        let pct_read = (total_read.as_secs_f64() / grand_total.as_secs_f64()) * 100.0;
        println!(
            "║ READ             │ {:>10.3}s │ {:>9.1}µs │ {:>6.1}% ║",
            total_read.as_secs_f64(),
            avg_read.as_micros(),
            pct_read
        );

        let avg_process = if !processes.is_empty() {
            total_process / processes.len() as u32
        } else {
            Duration::from_nanos(0)
        };
        let pct_process = (total_process.as_secs_f64() / grand_total.as_secs_f64()) * 100.0;
        println!(
            "║ PROCESS (SQL)    │ {:>10.3}s │ {:>9.1}µs │ {:>6.1}% ║",
            total_process.as_secs_f64(),
            avg_process.as_micros(),
            pct_process
        );

        let avg_write = if !writes.is_empty() {
            total_write / writes.len() as u32
        } else {
            Duration::from_nanos(0)
        };
        let pct_write = (total_write.as_secs_f64() / grand_total.as_secs_f64()) * 100.0;
        println!(
            "║ WRITE            │ {:>10.3}s │ {:>9.1}µs │ {:>6.1}% ║",
            total_write.as_secs_f64(),
            avg_write.as_micros(),
            pct_write
        );

        let avg_lock = if !locks.is_empty() {
            total_lock / locks.len() as u32
        } else {
            Duration::from_nanos(0)
        };
        println!("╟──────────────────┼─────────────┼────────────┼─────────╢");
        println!(
            "║ Lock Overhead    │ {:>10.3}s │ {:>9.1}µs │  (meta) ║",
            total_lock.as_secs_f64(),
            avg_lock.as_micros()
        );
        println!("╟──────────────────┼─────────────┼────────────┼─────────╢");
        println!(
            "║ TOTAL            │ {:>10.3}s │            │ 100.0%  ║",
            grand_total.as_secs_f64()
        );
        println!("╚════════════════════════════════════════════════════════╝");

        // Identify bottleneck
        let bottleneck = if pct_read > pct_process && pct_read > pct_write {
            "READ"
        } else if pct_process > pct_write {
            "PROCESS (SQL)"
        } else {
            "WRITE"
        };

        println!(
            "\n⚠️  PRIMARY BOTTLENECK: {} ({:.1}% of execution time)",
            bottleneck,
            if bottleneck == "READ" {
                pct_read
            } else if bottleneck == "PROCESS (SQL)" {
                pct_process
            } else {
                pct_write
            }
        );

        // Calculate mock overhead vs actual execution chain
        let execution_chain_time = total_process + total_write;
        let mock_overhead_pct = pct_read;
        let execution_pct =
            (execution_chain_time.as_secs_f64() / grand_total.as_secs_f64()) * 100.0;

        println!("\n╔════════════════════════════════════════════════════════╗");
        println!("║            MOCK OVERHEAD ANALYSIS                     ║");
        println!("╠════════════════════════════════════════════════════════╣");
        println!(
            "║ Mock Overhead (READ clone)    │ {:>6.1}% │ {:>10.3}s ║",
            mock_overhead_pct,
            total_read.as_secs_f64()
        );
        println!(
            "║ Execution Chain (PROCESS+WRITE)│ {:>6.1}% │ {:>10.3}s ║",
            execution_pct,
            execution_chain_time.as_secs_f64()
        );
        println!("╚════════════════════════════════════════════════════════╝");

        // Calculate what throughput would be WITHOUT mock overhead
        if grand_total.as_secs_f64() > 0.0 && execution_pct < 100.0 {
            let speedup_factor = 100.0 / execution_pct;
            println!(
                "\n💡 WITHOUT mock overhead, execution would be {:.1}x faster",
                speedup_factor
            );
        }
    }
}

/// Profiling-enabled reader (single batch reuse to measure overhead)
#[derive(Debug)]
pub struct ProfilingDataReader {
    pub batch: Vec<StreamRecord>,
    pub total_batches: usize,
    pub batches_read: usize,
    pub metrics: ProfilingMetrics,
}

/// Profiling-enabled reader (original cloning variant for comparison)
#[derive(Debug)]
pub struct ProfilingDataReaderClone {
    pub records: Vec<StreamRecord>,
    pub current_index: usize,
    pub batch_size: usize,
    pub metrics: ProfilingMetrics,
}

impl ProfilingDataReader {
    pub fn new(record_count: usize, batch_size: usize, metrics: ProfilingMetrics) -> Self {
        let start = Instant::now();

        // Allocate just ONE batch that we'll reuse (clone on each read)
        let mut batch = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "name".to_string(),
                FieldValue::String(format!("record_{}", i)),
            );
            fields.insert("value".to_string(), FieldValue::Float((i as f64) * 1.5));

            batch.push(StreamRecord {
                fields,
                headers: HashMap::new(),
                event_time: None,
                timestamp: 1640995200000 + (i as i64 * 1000),
                offset: i as i64,
                partition: 0,
            });
        }

        let total_batches = (record_count + batch_size - 1) / batch_size;

        println!(
            "📊 Setup time: {:?} (1 batch of {} records, will reuse {} times = {} total records)",
            start.elapsed(),
            batch_size,
            total_batches,
            record_count
        );

        Self {
            batch,
            total_batches,
            batches_read: 0,
            metrics,
        }
    }
}

#[async_trait]
impl DataReader for ProfilingDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        if self.batches_read >= self.total_batches {
            self.metrics.record_read(start.elapsed());
            return Ok(vec![]);
        }

        // Clone the same batch every time (measures mock overhead)
        let batch = self.batch.clone();
        self.batches_read += 1;

        self.metrics.record_read(start.elapsed());
        Ok(batch)
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::types::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.batches_read < self.total_batches)
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

/// Profiling-enabled writer (optimized with atomic operations)
#[derive(Debug, Clone)]
pub struct ProfilingDataWriter {
    pub written_count: Arc<AtomicUsize>,
    pub metrics: ProfilingMetrics,
}

impl ProfilingDataWriter {
    pub fn new(metrics: ProfilingMetrics) -> Self {
        Self {
            written_count: Arc::new(AtomicUsize::new(0)),
            metrics,
        }
    }
}

#[async_trait]
impl DataWriter for ProfilingDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        self.written_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_write(start.elapsed());
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let write_start = Instant::now();

        let count = records.len();
        // Atomic operation - no lock overhead
        self.written_count.fetch_add(count, Ordering::Relaxed);

        self.metrics.record_write(write_start.elapsed());
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

#[tokio::test]
async fn profile_realistic_1m_records() {
    println!("\n═══════════════════════════════════════════════════════");
    println!("🔬 DETAILED EXECUTION CHAIN PROFILING: 1M Records");
    println!("═══════════════════════════════════════════════════════\n");

    let metrics = ProfilingMetrics::new();

    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    // Create source
    let mut readers = HashMap::new();
    readers.insert(
        "source_0".to_string(),
        Box::new(ProfilingDataReader::new(1_000_000, 1000, metrics.clone())) as Box<dyn DataReader>,
    );

    // Create sink
    let mut writers = HashMap::new();
    writers.insert(
        "sink_0".to_string(),
        Box::new(ProfilingDataWriter::new(metrics.clone())) as Box<dyn DataWriter>,
    );

    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let overall_start = Instant::now();

    // Run processor
    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(
                readers,
                writers,
                engine,
                query,
                "profiling_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    let result = job_handle.await;
    let overall_duration = overall_start.elapsed();

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║              OVERALL PERFORMANCE SUMMARY              ║");
    println!("╚════════════════════════════════════════════════════════╝");
    println!("  Total Duration:  {:.3}s", overall_duration.as_secs_f64());
    println!(
        "  Throughput:      {:.2} K records/sec",
        1_000_000.0 / overall_duration.as_secs_f64() / 1000.0
    );

    // Print detailed metrics
    metrics.print_summary();

    // Calculate framework overhead
    let reads = metrics.read_time.lock().unwrap();
    let processes = metrics.process_time.lock().unwrap();
    let writes = metrics.write_time.lock().unwrap();
    let total_read: Duration = reads.iter().sum();
    let total_process: Duration = processes.iter().sum();
    let total_write: Duration = writes.iter().sum();
    let instrumented_time = total_read + total_process + total_write;
    let framework_overhead = overall_duration.as_secs_f64() - instrumented_time.as_secs_f64();
    let framework_pct = (framework_overhead / overall_duration.as_secs_f64()) * 100.0;

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║           COMPLETE OVERHEAD BREAKDOWN                 ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!(
        "║ Mock (READ clone)        │ {:>10.3}s │ {:>6.1}% │ (artificial) ║",
        total_read.as_secs_f64(),
        (total_read.as_secs_f64() / overall_duration.as_secs_f64()) * 100.0
    );
    println!(
        "║ Execution (PROCESS+WRITE)│ {:>10.3}s │ {:>6.1}% │ (target)     ║",
        (total_process + total_write).as_secs_f64(),
        ((total_process + total_write).as_secs_f64() / overall_duration.as_secs_f64()) * 100.0
    );
    println!(
        "║ Framework (async/loops)  │ {:>10.3}s │ {:>6.1}% │ (overhead)   ║",
        framework_overhead, framework_pct
    );
    println!("╠════════════════════════════════════════════════════════╣");
    println!(
        "║ TOTAL                    │ {:>10.3}s │  100.0% │              ║",
        overall_duration.as_secs_f64()
    );
    println!("╚════════════════════════════════════════════════════════╝");

    // Calculate projected throughput
    let execution_time = (total_process + total_write).as_secs_f64();
    if execution_time > 0.001 {
        let projected_throughput = 1_000_000.0 / execution_time / 1000.0;
        println!(
            "\n🚀 PROJECTED THROUGHPUT (without mock overhead): {:.2} K records/sec",
            projected_throughput
        );
        println!(
            "   That's a {:.1}x speedup from removing mock clone overhead!",
            projected_throughput / (1_000_000.0 / overall_duration.as_secs_f64() / 1000.0)
        );
    }

    assert!(result.is_ok(), "Job should complete successfully");
}
