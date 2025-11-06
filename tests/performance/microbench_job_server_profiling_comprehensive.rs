//! Comprehensive profiling benchmarks for complex SQL operations
//!
//! This extends microbench_job_server_profiling.rs to test:
//! - Window functions (tumbling, sliding)
//! - GROUP BY aggregations
//! - Complex filtering and transformations
//!
//! Purpose: Validate that Phase 2 & 3 optimizations (Arc removal, async boundary
//! elimination) benefit ALL SQL paths, not just passthrough queries.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, SimpleJobProcessor,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

/// Profiling metrics collector (same as main profiling file)
#[derive(Debug, Clone)]
pub struct ProfilingMetrics {
    pub read_time: Arc<StdMutex<Vec<Duration>>>,
    pub process_time: Arc<StdMutex<Vec<Duration>>>,
    pub write_time: Arc<StdMutex<Vec<Duration>>>,
    pub lock_time: Arc<StdMutex<Vec<Duration>>>,
    pub total_batches: Arc<StdMutex<usize>>,
    pub framework_time: Arc<StdMutex<Vec<Duration>>>,
}

impl ProfilingMetrics {
    pub fn new() -> Self {
        Self {
            read_time: Arc::new(StdMutex::new(Vec::new())),
            process_time: Arc::new(StdMutex::new(Vec::new())),
            write_time: Arc::new(StdMutex::new(Vec::new())),
            lock_time: Arc::new(StdMutex::new(Vec::new())),
            total_batches: Arc::new(StdMutex::new(0)),
            framework_time: Arc::new(StdMutex::new(Vec::new())),
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

    pub fn print_summary(&self) {
        let reads = self.read_time.lock().unwrap();
        let processes = self.process_time.lock().unwrap();
        let writes = self.write_time.lock().unwrap();

        let total_read: Duration = reads.iter().sum();
        let total_process: Duration = processes.iter().sum();
        let total_write: Duration = writes.iter().sum();

        let batches = reads.len();

        println!("\n╔════════════════════════════════════════════════════════╗");
        println!("║          EXECUTION CHAIN PROFILING RESULTS            ║");
        println!("╠════════════════════════════════════════════════════════╣");
        println!("║ Phase            │ Total Time  │ Avg/Batch  │ % Total ║");
        println!("╟──────────────────┼─────────────┼────────────┼─────────╢");

        let total = total_read + total_process + total_write;
        let total_secs = if total.as_secs() == 0 && total.subsec_millis() == 0 {
            0.001
        } else {
            total.as_secs_f64()
        };

        println!(
            "║ READ             │ {:>10.3}s │ {:>9}µs │ {:>6.1}% ║",
            total_read.as_secs_f64(),
            if batches > 0 {
                total_read.as_micros() / batches as u128
            } else {
                0
            },
            (total_read.as_secs_f64() / total_secs) * 100.0
        );

        println!(
            "║ PROCESS (SQL)    │ {:>10.3}s │ {:>9}µs │ {:>6.1}% ║",
            total_process.as_secs_f64(),
            if batches > 0 {
                total_process.as_micros() / batches as u128
            } else {
                0
            },
            (total_process.as_secs_f64() / total_secs) * 100.0
        );

        println!(
            "║ WRITE            │ {:>10.3}s │ {:>9}µs │ {:>6.1}% ║",
            total_write.as_secs_f64(),
            if batches > 0 {
                total_write.as_micros() / batches as u128
            } else {
                0
            },
            (total_write.as_secs_f64() / total_secs) * 100.0
        );

        println!("╟──────────────────┼─────────────┼────────────┼─────────╢");
        println!(
            "║ TOTAL            │ {:>10.3}s │            │ 100.0%  ║",
            total_secs
        );
        println!("╚════════════════════════════════════════════════════════╝");
    }
}

/// Profiling data reader - generates records with realistic fields for window/grouping
pub struct ProfilingDataReader {
    total_records: usize,
    batch_size: usize,
    records_sent: AtomicUsize,
    metrics: ProfilingMetrics,
}

impl ProfilingDataReader {
    pub fn new(total_records: usize, batch_size: usize, metrics: ProfilingMetrics) -> Self {
        Self {
            total_records,
            batch_size,
            records_sent: AtomicUsize::new(0),
            metrics,
        }
    }

    fn generate_record(&self, id: usize) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id as i64));
        // Category for GROUP BY (10 distinct values)
        fields.insert(
            "category".to_string(),
            FieldValue::String(format!("cat_{}", id % 10)),
        );
        // Amount for aggregations
        fields.insert("amount".to_string(), FieldValue::Float((id % 1000) as f64));
        // Timestamp for window functions (1 second intervals)
        fields.insert(
            "event_time".to_string(),
            FieldValue::String(format!(
                "2024-01-01T00:{:02}:{:02}Z",
                (id / 60) % 60,
                id % 60
            )),
        );
        fields.insert("value".to_string(), FieldValue::Float(id as f64 * 1.5));

        StreamRecord {
            fields,
            timestamp: 0, // Not used in profiling
            offset: id as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time: None, // Not used in profiling
        }
    }
}

#[async_trait]
impl DataReader for ProfilingDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        let sent = self.records_sent.load(Ordering::SeqCst);
        if sent >= self.total_records {
            self.metrics.record_read(start.elapsed());
            return Ok(Vec::new());
        }

        let remaining = self.total_records - sent;
        let batch_size = std::cmp::min(self.batch_size, remaining);

        // Generate batch with realistic data
        let mut batch = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            batch.push(self.generate_record(sent + i));
        }

        self.records_sent.fetch_add(batch_size, Ordering::SeqCst);
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
        Ok(self.records_sent.load(Ordering::SeqCst) < self.total_records)
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

/// Profiling data writer - minimal overhead, just counts
pub struct ProfilingDataWriter {
    records_written: Arc<AtomicU64>,
    metrics: ProfilingMetrics,
}

impl ProfilingDataWriter {
    pub fn new(metrics: ProfilingMetrics) -> Self {
        Self {
            records_written: Arc::new(AtomicU64::new(0)),
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
        self.records_written.fetch_add(1, Ordering::SeqCst);
        self.metrics.record_write(start.elapsed());
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        self.records_written
            .fetch_add(records.len() as u64, Ordering::SeqCst);
        self.metrics.record_write(start.elapsed());
        Ok(())
    }

    async fn write_batch_shared(
        &mut self,
        records: &[Arc<StreamRecord>],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        self.records_written
            .fetch_add(records.len() as u64, Ordering::SeqCst);
        self.metrics.record_write(start.elapsed());
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

/// Benchmark: Tumbling window with GROUP BY and aggregations
///
/// Query: SELECT category, COUNT(*), SUM(amount), AVG(value)
///        FROM stream
///        WINDOW TUMBLING (SIZE 60 SECONDS)
///        GROUP BY category
///
/// This tests:
/// - Window state buffering
/// - GROUP BY hash table operations
/// - Multiple aggregations (COUNT, SUM, AVG)
/// - Window emit logic
/// - Arc sharing across group state
#[tokio::test]
async fn profile_tumbling_window_group_by_1m_records() {
    println!("\n═══════════════════════════════════════════════════════");
    println!("🔬 TUMBLING WINDOW + GROUP BY PROFILING: 1M Records");
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

    let mut readers = HashMap::new();
    readers.insert(
        "source_0".to_string(),
        Box::new(ProfilingDataReader::new(1_000_000, 1000, metrics.clone())) as Box<dyn DataReader>,
    );

    let mut writers = HashMap::new();
    writers.insert(
        "sink_0".to_string(),
        Box::new(ProfilingDataWriter::new(metrics.clone())) as Box<dyn DataWriter>,
    );

    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));

    let parser = StreamingSqlParser::new();

    // Complex query with window and GROUP BY
    let query = parser
        .parse(
            "SELECT category, COUNT(*) as cnt, SUM(amount) as total, AVG(value) as avg_val \
             FROM test_stream \
             WINDOW TUMBLING (SIZE 60 SECONDS) \
             GROUP BY category",
        )
        .unwrap();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let overall_start = Instant::now();

    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(
                readers,
                writers,
                engine,
                query,
                "window_groupby_profiling".to_string(),
                shutdown_rx,
            )
            .await
    });

    let result = job_handle.await;
    let overall_duration = overall_start.elapsed();

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║       TUMBLING WINDOW + GROUP BY PERFORMANCE          ║");
    println!("╚════════════════════════════════════════════════════════╝");
    println!("  Total Duration:  {:.3}s", overall_duration.as_secs_f64());
    println!(
        "  Throughput:      {:.2} K records/sec",
        1_000_000.0 / overall_duration.as_secs_f64() / 1000.0
    );
    println!("  Query Type:      Tumbling Window + GROUP BY + 3 Aggregations");

    metrics.print_summary();

    // Compare with passthrough baseline (400K rec/s from Phase 3)
    let throughput = 1_000_000.0 / overall_duration.as_secs_f64() / 1000.0;
    let baseline_throughput = 400.0; // Phase 3 passthrough result
    let overhead_pct = ((baseline_throughput - throughput) / baseline_throughput) * 100.0;

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║           WINDOW/GROUP BY OVERHEAD ANALYSIS           ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!(
        "║ Passthrough Baseline  │ {:>10.2} K rec/s │ (Phase 3) ║",
        baseline_throughput
    );
    println!(
        "║ Window+GroupBy Actual │ {:>10.2} K rec/s │ (current) ║",
        throughput
    );
    println!(
        "║ Overhead Impact       │ {:>10.1}% slower │           ║",
        overhead_pct
    );
    println!("╚════════════════════════════════════════════════════════╝");

    println!("\n📊 Analysis:");
    println!("  - If overhead < 50%: Arc optimizations working well for stateful operations");
    println!("  - If overhead > 100%: Window/group state management is new bottleneck");

    assert!(result.is_ok(), "Job should complete successfully");
}

/// Benchmark: GROUP BY with multiple aggregations (no window)
///
/// Query: SELECT category, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg_amt,
///               MIN(amount) as min_amt, MAX(amount) as max_amt
///        FROM stream
///        GROUP BY category
///
/// This tests:
/// - GROUP BY hash table without window overhead
/// - Multiple aggregation accumulators
/// - Hash key extraction overhead
/// - Arc sharing in group state
#[tokio::test]
async fn profile_group_by_aggregations_1m_records() {
    println!("\n═══════════════════════════════════════════════════════");
    println!("🔬 GROUP BY + AGGREGATIONS PROFILING: 1M Records");
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

    let mut readers = HashMap::new();
    readers.insert(
        "source_0".to_string(),
        Box::new(ProfilingDataReader::new(1_000_000, 1000, metrics.clone())) as Box<dyn DataReader>,
    );

    let mut writers = HashMap::new();
    writers.insert(
        "sink_0".to_string(),
        Box::new(ProfilingDataWriter::new(metrics.clone())) as Box<dyn DataWriter>,
    );

    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));

    let parser = StreamingSqlParser::new();

    // GROUP BY with 5 aggregations
    let query = parser
        .parse(
            "SELECT category, \
                    COUNT(*) as cnt, \
                    SUM(amount) as total, \
                    AVG(amount) as avg_amt, \
                    MIN(amount) as min_amt, \
                    MAX(amount) as max_amt \
             FROM test_stream \
             GROUP BY category",
        )
        .unwrap();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let overall_start = Instant::now();

    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(
                readers,
                writers,
                engine,
                query,
                "groupby_profiling".to_string(),
                shutdown_rx,
            )
            .await
    });

    let result = job_handle.await;
    let overall_duration = overall_start.elapsed();

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║         GROUP BY + AGGREGATIONS PERFORMANCE            ║");
    println!("╚════════════════════════════════════════════════════════╝");
    println!("  Total Duration:  {:.3}s", overall_duration.as_secs_f64());
    println!(
        "  Throughput:      {:.2} K records/sec",
        1_000_000.0 / overall_duration.as_secs_f64() / 1000.0
    );
    println!("  Query Type:      GROUP BY + 5 Aggregations (COUNT/SUM/AVG/MIN/MAX)");

    metrics.print_summary();

    let throughput = 1_000_000.0 / overall_duration.as_secs_f64() / 1000.0;
    let baseline_throughput = 400.0; // Phase 3 passthrough result
    let overhead_pct = ((baseline_throughput - throughput) / baseline_throughput) * 100.0;

    println!("\n╔════════════════════════════════════════════════════════╗");
    println!("║           GROUP BY OVERHEAD ANALYSIS                   ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!(
        "║ Passthrough Baseline │ {:>10.2} K rec/s │ (Phase 3)  ║",
        baseline_throughput
    );
    println!(
        "║ GroupBy Actual       │ {:>10.2} K rec/s │ (current)  ║",
        throughput
    );
    println!(
        "║ Overhead Impact      │ {:>10.1}% slower │            ║",
        overhead_pct
    );
    println!("╚════════════════════════════════════════════════════════╝");

    println!("\n📊 Analysis:");
    println!("  - If overhead < 30%: Hash table and aggregation state well optimized");
    println!("  - If overhead > 50%: GROUP BY hash operations are bottleneck");

    assert!(result.is_ok(), "Job should complete successfully");
}
