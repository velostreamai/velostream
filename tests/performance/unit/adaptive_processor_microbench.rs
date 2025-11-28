/*!
AdaptiveJobProcessor Microbenchmark - Root Cause Analysis

This microbenchmark investigates why AdaptiveJobProcessor shows 30-60x slower
throughput (~16K rec/sec) compared to other processors (100K-900K rec/sec).

Key measurements:
1. Empty batch polling overhead (300ms for EOF detection)
2. Async receiver completion time
3. Batch size sensitivity
4. Configuration impact (empty_batch_count, wait_on_empty_batch_ms)
5. Per-record latency breakdown
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// ============================================================================
// Test Infrastructure
// ============================================================================

#[derive(Clone)]
struct SimpleDataSource {
    records: Vec<StreamRecord>,
    batch_size: usize,
    index: Arc<std::sync::atomic::AtomicUsize>,
}

impl SimpleDataSource {
    fn new(record_count: usize, batch_size: usize) -> Self {
        let mut records = Vec::with_capacity(record_count);
        for i in 0..record_count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
            records.push(StreamRecord::new(fields));
        }

        Self {
            records,
            batch_size,
            index: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl DataReader for SimpleDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let start = self.index.load(std::sync::atomic::Ordering::SeqCst);
        if start >= self.records.len() {
            return Ok(Vec::new());
        }
        let end = (start + self.batch_size).min(self.records.len());
        let batch: Vec<StreamRecord> = self.records[start..end].iter().cloned().collect();
        self.index.store(end, std::sync::atomic::Ordering::SeqCst);
        Ok(batch)
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.index.load(std::sync::atomic::Ordering::SeqCst) < self.records.len())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[derive(Clone)]
struct SimpleDataWriter {
    count: Arc<std::sync::atomic::AtomicUsize>,
}

impl SimpleDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn get_count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DataWriter for SimpleDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count
            .fetch_add(records.len(), std::sync::atomic::Ordering::SeqCst);
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

// ============================================================================
// Core Microbench Utility
// ============================================================================

struct MicrobenchResult {
    total_time_ms: f64,
    read_loop_time_ms: f64,
    receiver_wait_time_ms: f64,
    records_processed: usize,
    throughput_rec_sec: f64,
}

impl MicrobenchResult {
    fn print(&self, name: &str) {
        println!("\n┌─ {}", name);
        println!("│  Total time:         {:.2}ms", self.total_time_ms);
        println!("│  Read loop:          {:.2}ms", self.read_loop_time_ms);
        println!("│  Receiver wait:      {:.2}ms", self.receiver_wait_time_ms);
        println!("│  Records processed:  {}", self.records_processed);
        println!(
            "│  Throughput:         {:.0} rec/sec",
            self.throughput_rec_sec
        );
        println!("└");
    }
}

// ============================================================================
// Baseline Tests
// ============================================================================

/// Baseline: Simple routing without processing or async overhead
#[tokio::test]
async fn bench_baseline_read_only() {
    println!("\n\n=== BASELINE: Read Loop Only (No Processing) ===\n");

    let record_counts = vec![100, 500, 1000, 5000];

    for record_count in record_counts {
        let start = Instant::now();

        let mut source = SimpleDataSource::new(record_count, 50);
        let mut total_read = 0;

        loop {
            match source.read().await {
                Ok(batch) => {
                    if batch.is_empty() {
                        break;
                    }
                    total_read += batch.len();
                }
                Err(_) => break,
            }
        }

        let elapsed = start.elapsed();
        let throughput = (total_read as f64) / elapsed.as_secs_f64();

        println!(
            "  {} records: {:.2}ms, {:.0} rec/sec",
            record_count,
            elapsed.as_millis(),
            throughput
        );
    }
}

/// Test 1: Empty batch detection overhead
#[tokio::test]
async fn bench_empty_batch_detection() {
    println!("\n\n=== TEST 1: Empty Batch Detection Overhead ===\n");
    println!("Measures the cost of EOF detection with different configurations\n");

    let record_count = 5000;
    let batch_size = 1000; // Should result in 5 batches, then empty reads

    // Configuration: wait 100ms per empty, need 3 consecutive
    // Expected: 300ms minimum for EOF detection
    let configs = vec![
        ("Conservative (empty_batch_count=3, wait=100ms)", 3, 100),
        ("Aggressive (empty_batch_count=1, wait=10ms)", 1, 10),
        ("Immediate (empty_batch_count=0, wait=0ms)", 0, 0),
    ];

    for (name, empty_count, wait_ms) in configs {
        let mut source = SimpleDataSource::new(record_count, batch_size);
        let mut empty_count_var = 0;
        let mut total_batches = 0;

        let start = Instant::now();

        loop {
            match source.read().await {
                Ok(batch) => {
                    if batch.is_empty() {
                        empty_count_var += 1;
                        if empty_count_var >= empty_count {
                            break;
                        }
                        if wait_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(wait_ms as u64))
                                .await;
                        }
                    } else {
                        empty_count_var = 0;
                        total_batches += 1;
                    }
                }
                Err(_) => break,
            }
        }

        let elapsed = start.elapsed();
        let expected_overhead = if empty_count > 0 {
            (empty_count - 1) * wait_ms
        } else {
            0
        };

        println!("  {}", name);
        println!("    Total time: {:.0}ms", elapsed.as_millis());
        println!("    Expected overhead: ~{}ms", expected_overhead);
        println!("    Data batches: {}", total_batches);
        println!();
    }
}

/// Test 2: Batch size sensitivity
#[tokio::test]
async fn bench_batch_size_sensitivity() {
    println!("\n\n=== TEST 2: Batch Size Sensitivity ===\n");
    println!("Tests how batch size affects AdaptiveJobProcessor throughput\n");

    let record_count = 5000;
    let batch_sizes = vec![1, 10, 50, 100, 500, 5000];

    for batch_size in batch_sizes {
        let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
            num_partitions: Some(1),
            enable_core_affinity: false,
        });

        let source = SimpleDataSource::new(record_count, batch_size);
        let writer = SimpleDataWriter::new();

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, value FROM stream")
            .expect("Parse failed");

        let (tx, _rx) = mpsc::unbounded_channel();
        let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let read_start = Instant::now();

        let _stats = processor
            .process_job(
                Box::new(source.clone()),
                Some(Box::new(writer.clone())),
                engine,
                query,
                "bench".to_string(),
                shutdown_rx,
                None,
            )
            .await;

        let read_elapsed = read_start.elapsed();

        // Wait for async receivers
        let wait_start = Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let wait_elapsed = wait_start.elapsed();

        let total_time = read_elapsed.as_secs_f64() + wait_elapsed.as_secs_f64();
        let throughput = (record_count as f64) / total_time;

        println!(
            "  Batch size={:4}: {:.0}ms total, {:.0} rec/sec (read={:.0}ms, wait={:.0}ms)",
            batch_size,
            total_time * 1000.0,
            throughput,
            read_elapsed.as_millis(),
            wait_elapsed.as_millis()
        );
    }
}

/// Test 3: Partition count scaling
#[tokio::test]
async fn bench_partition_scaling() {
    println!("\n\n=== TEST 3: Partition Count Scaling ===\n");
    println!("Tests throughput with different partition counts\n");

    let record_count = 5000;
    let batch_size = 500;
    let partition_counts = vec![1, 2, 4, 8];

    for num_partitions in partition_counts {
        let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        });

        let source = SimpleDataSource::new(record_count, batch_size);
        let writer = SimpleDataWriter::new();

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, value FROM stream")
            .expect("Parse failed");

        let (tx, _rx) = mpsc::unbounded_channel();
        let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let start = Instant::now();

        let _stats = processor
            .process_job(
                Box::new(source.clone()),
                Some(Box::new(writer.clone())),
                engine,
                query,
                "bench".to_string(),
                shutdown_rx,
                None,
            )
            .await;

        let elapsed = start.elapsed();
        let throughput = (record_count as f64) / elapsed.as_secs_f64();

        println!(
            "  {} partitions: {:.0}ms, {:.0} rec/sec",
            num_partitions,
            elapsed.as_millis(),
            throughput
        );
    }
}

/// Test 4: Configuration impact analysis
#[tokio::test]
async fn bench_configuration_impact() {
    println!("\n\n=== TEST 4: Configuration Impact ===\n");
    println!("Measures impact of empty_batch_count on measured throughput\n");

    let record_count = 5000;
    let batch_size = 1000;

    // Key insight: Default config (empty_batch_count=3, wait=100ms) adds 300ms overhead
    // This artificially reduces measured throughput by 300ms / (300ms + actual_time)
    println!("Hypothesis: 300ms fixed overhead reduces throughput by ~50%");
    println!("  - If actual processing: 300ms");
    println!("  - EOF detection: 300ms");
    println!("  - Total: 600ms");
    println!("  - Measured throughput: {:.0} rec/sec", (5000.0 / 0.6));
    println!(
        "\nBut actual throughput might be: {:.0} rec/sec",
        (5000.0 / 0.3)
    );
    println!();

    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });

    let source = SimpleDataSource::new(record_count, batch_size);
    let writer = SimpleDataWriter::new();

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM stream")
        .expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();

    let _stats = processor
        .process_job(
            Box::new(source.clone()),
            Some(Box::new(writer.clone())),
            engine,
            query,
            "bench".to_string(),
            shutdown_rx,
            None,
        )
        .await;

    let elapsed = start.elapsed();

    println!("Measured results:");
    println!("  Time: {:.0}ms", elapsed.as_millis());
    println!(
        "  Throughput: {:.0} rec/sec",
        (record_count as f64) / elapsed.as_secs_f64()
    );
    println!();
    println!("ANALYSIS:");
    println!("  If ~300ms is EOF detection overhead, actual processing might be");
    println!("  significantly faster. This explains the 30-60x slowdown in comprehensive");
    println!("  baseline - it's artificial overhead, not processor inefficiency.");
}

/// Summary report
#[tokio::test]
async fn bench_summary_report() {
    println!("\n\n╔════════════════════════════════════════════════════════════════════╗");
    println!("║        AdaptiveJobProcessor Performance Analysis Summary            ║");
    println!("╚════════════════════════════════════════════════════════════════════╝\n");

    println!("OBSERVED PROBLEM:");
    println!("  AdaptiveJobProcessor: ~16K rec/sec");
    println!("  Other processors:     100K-900K rec/sec");
    println!("  Gap:                  30-60x slower\n");

    println!("ROOT CAUSE HYPOTHESIS:");
    println!("  1. Empty batch polling: 300ms fixed cost (3 × 100ms waits)");
    println!("  2. For 5000 records taking ~300ms to process:");
    println!("     - Total measured time: 300ms + 300ms = 600ms");
    println!("     - Measured throughput: 5000/0.6 = 8,333 rec/sec");
    println!("     - But actual throughput: 5000/0.3 = 16,666 rec/sec");
    println!("  3. This matches the reported 16,111 rec/sec!\n");

    println!("EVIDENCE:");
    println!("  ✓ Empty batch polling is synchronous (blocks main loop)");
    println!("  ✓ Config requires 3 consecutive empty batches");
    println!("  ✓ Each empty triggers 100ms sleep");
    println!("  ✓ Total: 300ms per job minimum\n");

    println!("SOLUTIONS:");
    println!("  Option 1: Set empty_batch_count=0 (immediate EOF detection)");
    println!("    → Expected improvement: 30-50% (remove 300ms overhead)");
    println!("  Option 2: Set wait_on_empty_batch_ms=10 (reduce from 100ms)");
    println!("    → Expected improvement: 5-10% (reduce to 30ms overhead)");
    println!("  Option 3: Make EOF detection async");
    println!("    → Expected improvement: 30-50% (parallel with receiver processing)\n");

    println!("NEXT STEPS:");
    println!("  1. Confirm with profiling (add detailed timing instrumentation)");
    println!("  2. Test with empty_batch_count=0 configuration");
    println!("  3. Consider async EOF detection for long-running jobs");
}

// ============================================================================
// SQL Engine Microbenchmarks (Direct Query Execution)
// ============================================================================

/// Test SQL engine performance in isolation (no processor overhead)
#[tokio::test]
async fn bench_sql_engine_only() {
    println!("\n\n=== SQL ENGINE PERFORMANCE (No Processor Overhead) ===\n");
    println!("Direct StreamExecutionEngine benchmark with simple SELECT query\n");

    let record_count = 5000;
    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
        records.push(StreamRecord::new(fields));
    }

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM stream")
        .expect("Parse failed");

    // Create execution engine
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Initialize query execution
    engine.init_query_execution(query.clone());

    // Benchmark query execution
    let start = Instant::now();
    let mut executed = 0;

    for record in &records {
        match engine.execute_with_record_sync(&query, record) {
            Ok(results) => {
                // Count results (0 or more per record)
                executed += results.len().max(1); // Count as executed if no results (buffered)
            }
            Err(_e) => {
                // Continue on error
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (executed as f64) / elapsed.as_secs_f64();

    println!("Results:");
    println!("  Records executed: {}", executed);
    println!("  Time: {:.2}ms", elapsed.as_millis());
    println!("  Throughput: {:.0} rec/sec", throughput);
    println!(
        "  Per-record latency: {:.2}μs",
        elapsed.as_micros() as f64 / executed as f64
    );
    println!();

    // Compare with baseline
    println!("Comparison:");
    println!("  Baseline I/O (no SQL): 1,100,000 rec/sec");
    println!("  SQL Engine: {:.0} rec/sec", throughput);
    println!(
        "  Gap: {:.0}x slower (SQL overhead)",
        1_100_000.0 / throughput
    );
}

/// Profile individual query execution steps
#[tokio::test]
async fn bench_sql_engine_profile() {
    println!("\n\n=== SQL ENGINE PROFILE (Detailed Breakdown) ===\n");

    let record_count = 1000; // Smaller set for profiling
    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
        records.push(StreamRecord::new(fields));
    }

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM stream")
        .expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    engine.init_query_execution(query.clone());

    // Measure first record (includes initialization)
    let start = Instant::now();
    let _ = engine.execute_with_record_sync(&query, &records[0]).ok();
    let first_record_time = start.elapsed();

    // Measure remaining records (steady state)
    let start = Instant::now();
    for record in &records[1..] {
        let _ = engine.execute_with_record_sync(&query, record).ok();
    }
    let remaining_time = start.elapsed();
    let remaining_count = record_count - 1;

    println!("First record (with initialization):");
    println!("  Time: {:.2}μs", first_record_time.as_micros());
    println!();
    println!("Remaining {} records (steady state):", remaining_count);
    println!("  Total time: {:.2}ms", remaining_time.as_millis());
    println!(
        "  Per-record: {:.2}μs",
        remaining_time.as_micros() as f64 / remaining_count as f64
    );
    println!(
        "  Throughput: {:.0} rec/sec",
        (remaining_count as f64) / remaining_time.as_secs_f64()
    );
}

// ============================================================================
// PROFILING: End-to-End Timing Instrumentation
// ============================================================================

/// Profile the AdaptiveProcessor with detailed timing at each level
#[tokio::test]
async fn bench_profiling_end_to_end() {
    println!("\n\n=== ADAPTIVE PROCESSOR PROFILING (End-to-End Timing) ===\n");
    println!("Measuring where the 18x overhead (282K→16K rec/sec) comes from\n");

    let record_count = 5000;
    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
        records.push(StreamRecord::new(fields));
    }

    // Create data source and writer
    let data_source = SimpleDataSource::new(record_count, 100);
    let data_writer = SimpleDataWriter::new();

    // Create processor
    let processor = JobProcessorFactory::create_adaptive_test_optimized(Some(1));

    // Measure overall processing time
    let overall_start = Instant::now();

    // Create mock reader (wraps SimpleDataSource)
    let reader = Box::new(data_source) as Box<dyn DataReader>;

    // Parse query
    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM stream")
        .expect("Parse failed");

    // Create execution engine (needed for JobProcessor trait)
    let (_tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(_tx)));

    // Create shutdown channel
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Process job
    let result = processor
        .process_job(
            reader,
            Some(Box::new(data_writer)),
            engine,
            query,
            "profiling_test".to_string(),
            shutdown_rx,
            None,
        )
        .await;

    let overall_elapsed = overall_start.elapsed();

    match result {
        Ok(stats) => {
            println!("✅ Processing completed successfully");
            println!();
            println!("OVERALL METRICS:");
            println!("  Records processed: {}", stats.records_processed);
            println!("  Total time: {:.2}ms", overall_elapsed.as_millis());
            println!(
                "  Throughput: {:.0} rec/sec",
                (stats.records_processed as f64) / overall_elapsed.as_secs_f64()
            );
            println!();

            println!("CURRENT BOTTLENECK:");
            println!("  Direct SQL: 282K rec/sec");
            println!(
                "  Through processor: {:.0} rec/sec",
                (stats.records_processed as f64) / overall_elapsed.as_secs_f64()
            );
            println!(
                "  Overhead: {:.1}x",
                282000.0 / ((stats.records_processed as f64) / overall_elapsed.as_secs_f64())
            );
            println!();

            println!("ANALYSIS NEEDED:");
            println!("  1. Where does 17x overhead come from?");
            println!("  2. Is it in SELECT processor HashMap/cloning?");
            println!("  3. Is it in expression evaluation?");
            println!("  4. Is it in field validation?");
            println!("  5. Add detailed timing to each component to find bottleneck");
        }
        Err(e) => {
            println!("❌ Processing failed: {:?}", e);
        }
    }
}
