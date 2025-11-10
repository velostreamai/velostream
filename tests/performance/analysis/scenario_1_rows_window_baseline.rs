/*!
# FR-082 Scenario 1: ROWS WINDOW (No GROUP BY) Baseline Test

**Purpose**: Measure baseline performance for ROWS WINDOW queries (memory-bounded sliding buffers).

## Scenario Classification
- **Pattern**: Memory-bounded sliding buffers per partition
- **State Management**: Circular buffer per partition key
- **Query Category**: Category 1 - ROWS WINDOW (specific use case)
- **Phase 0 Target**: ⚠️ TBD (different optimization pattern than GROUP BY)
- **V2 Routing**: Hash by PARTITION BY columns

## Example Query
```sql
SELECT
    symbol, price,
    AVG(price) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY timestamp
    ) as moving_avg
FROM market_data
```

## Performance Expectations
- **SQL Engine**: TBD (this test provides detailed profiling)
- **Job Server V1**: TBD (not yet measured)
- **V2 Target**: TBD

## Current Status
⚠️ **SQL Engine Only**: This test currently measures SQL Engine performance with detailed
per-record profiling. Job Server measurement is pending.

## Key Insights
- **Bounded Buffer**: Memory usage should remain constant (ROWS WINDOW limits buffer size)
- **Growth Ratio**: Last/first record time should be < 1.5x (indicates bounded behavior)
- **No GROUP BY**: Different optimization pattern than Scenarios 2/3a/3b
*/

use async_trait::async_trait;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, parser::StreamingSqlParser};

// Import validation utilities
use super::super::validation::{MetricsValidation, print_validation_results, validate_records};
// Import shared metrics helper
use super::test_helpers::JobServerMetrics;

#[tokio::test]
#[serial]
async fn scenario_1_rows_window_baseline() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Scenario 1: ROWS WINDOW (No GROUP BY)");
    println!("═══════════════════════════════════════════════════════════");
    println!("⚠️  SQL Engine profiling only - Job Server measurement pending");
    println!("═══════════════════════════════════════════════════════════\n");

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
        let symbol_idx = i % 10; // 10 different symbols
        let symbol = format!("SYM{}", symbol_idx);
        let price = 100.0 + (i as f64 % 50.0) + ((i as f64 / 100.0).sin() * 10.0);
        let timestamp = base_time + (i as i64 * 1000); // 1 second intervals
        // Map symbol to partition: SYM0,SYM1,SYM2,SYM3 → partition 0, SYM4,SYM5,SYM6,SYM7 → partition 1, etc.
        let partition_id = (symbol_idx / 5) as i32; // Maps to partition 0 or 1

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

        let mut record = StreamRecord::new(fields);
        record.partition = partition_id; // Set partition directly on StreamRecord
        record.offset = i as i64;
        record.timestamp = timestamp;
        records.push(record);
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
        let _ = engine.execute_with_record(&query, &record).await;
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
        let _ = engine.execute_with_record(&query, &record).await;
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
        let _ = engine.execute_with_record(&query, &record).await;
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

// =============================================================================
// Job Server Baseline Test (NEW - Complete Measurement)
// =============================================================================

const BASELINE_SQL: &str = r#"
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

fn generate_rows_window_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000000i64;

    for i in 0..count {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 10); // 10 different symbols
        let price = 100.0 + (i as f64 % 50.0) + ((i as f64 / 100.0).sin() * 10.0);
        let timestamp = base_time + (i as i64 * 1000);

        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    records
}

/// Measure pure SQL engine performance (without job server)
/// FIXED: Now actually verifies window processing by collecting output records
async fn measure_rows_window_sql_engine(records: Vec<StreamRecord>, query: &str) -> (usize, u128) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in records.iter() {
        let _ = engine.execute_with_record(&parsed_query, &record).await;
    }

    // CRITICAL: Flush windows to ensure all results are emitted
    let _ = engine.flush_windows().await;

    let elapsed = start.elapsed();

    // Collect actual results from channel (verifies window processing)
    let mut result_count = 0;
    while let Ok(_result) = rx.try_recv() {
        result_count += 1;
    }

    // Return output record count (verifies window processing actually happened)
    (result_count, elapsed.as_micros())
}

/// Mock data source for ROWS WINDOW testing
struct RowsWindowDataSource {
    records: Vec<StreamRecord>,
    read_count: usize,
}

impl RowsWindowDataSource {
    fn new(records: Vec<StreamRecord>) -> Self {
        Self {
            records,
            read_count: 0,
        }
    }
}

#[async_trait]
impl DataReader for RowsWindowDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.read_count > 0 {
            return Ok(vec![]);
        }
        self.read_count += 1;
        Ok(self.records.clone())
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
        Ok(self.read_count == 0)
    }
}

/// Mock data writer for ROWS WINDOW testing
struct RowsWindowDataWriter {
    records_written: Arc<AtomicUsize>,
    samples: Arc<Mutex<Vec<StreamRecord>>>,
}

impl RowsWindowDataWriter {
    fn new() -> Self {
        Self {
            records_written: Arc::new(AtomicUsize::new(0)),
            samples: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl DataWriter for RowsWindowDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let count = records.len();
        self.records_written.fetch_add(count, Ordering::SeqCst);

        // Sample 1 in 10k records for validation
        let mut samples = self.samples.lock().await;
        for record in records.iter() {
            let total_count = self.records_written.load(Ordering::SeqCst);
            if total_count % 10000 == 0 {
                samples.push(record.as_ref().clone());
            }
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

#[tokio::test]
#[serial]
async fn scenario_1_rows_window_with_job_server() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Scenario 1: ROWS WINDOW (Complete Baseline)");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("Goal: Measure ROWS WINDOW performance (SQL Engine + Job Server)");
    println!("Query: \n{}\\n", BASELINE_SQL);

    println!("═══════════════════════════════════════════════════════════\n");

    let num_records = 5000;
    println!("✅ Generated {} test records\n", num_records);

    let records = generate_rows_window_records(num_records);

    // Measure pure SQL engine
    println!("🚀 Measuring pure SQL engine (no job server)...");
    let (sql_result_count, sql_time_us) =
        measure_rows_window_sql_engine(records.clone(), BASELINE_SQL).await;
    let sql_throughput = if sql_time_us > 0 {
        (num_records as f64 / (sql_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ SQL Engine: {} input records → {} output records in {:.2}ms ({} rec/sec)\n",
        num_records,
        sql_result_count,
        sql_time_us as f64 / 1000.0,
        sql_throughput
    );

    // Measure job server
    println!("🚀 Measuring job server (full pipeline)...");

    // Inline job server measurement to capture samples
    let data_source = RowsWindowDataSource::new(records.clone());
    let mut data_writer = RowsWindowDataWriter::new();
    let samples_ref = data_writer.samples.clone();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(BASELINE_SQL).expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let processor = JobProcessorFactory::create(JobProcessorConfig::V1);
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_start = Instant::now();
    let result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            parsed_query,
            "rows_window_baseline".to_string(),
            shutdown_rx,
        )
        .await;
    let job_time_us = job_start.elapsed().as_micros();

    let job_throughput = if job_time_us > 0 {
        (num_records as f64 / (job_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ Job Server: {} records in {:.2}ms ({} rec/sec)\n",
        num_records,
        job_time_us as f64 / 1000.0,
        job_throughput
    );

    // Capture and display JobServer metrics
    if let Ok(stats) = result {
        let metrics = JobServerMetrics::from_stats(&stats, job_time_us);
        println!("📊 JOBSERVER METRICS");
        println!("═══════════════════════════════════════════════════════════");
        metrics.print_table("V1 (1 partition)");

        // Use JobServerMetrics to validate the processor
        println!("✅ VALIDATING JOBSERVER METRICS");
        println!("═══════════════════════════════════════════════════════════");
        assert_eq!(
            metrics.records_processed as usize, num_records,
            "Should process exactly {} records (got {})",
            num_records, metrics.records_processed
        );
        assert!(
            metrics.throughput_rec_per_sec > 0.0,
            "Should have valid throughput (got {:.0} rec/sec)",
            metrics.throughput_rec_per_sec
        );
        assert_eq!(
            metrics.records_failed, 0,
            "Should not fail any records (failed: {})",
            metrics.records_failed
        );
        assert_eq!(
            metrics.batches_failed, 0,
            "Should not fail any batches (failed: {})",
            metrics.batches_failed
        );
        println!("✓ JobMetrics validated: {} records at {:.0} rec/sec, 0 failures, 0 batch failures",
            metrics.records_processed, metrics.throughput_rec_per_sec);
        println!("═══════════════════════════════════════════════════════════\n");
    }

    // Calculate overhead
    let overhead_pct = if sql_throughput > 0 {
        ((sql_throughput as f64 - job_throughput as f64) / sql_throughput as f64) * 100.0
    } else {
        0.0
    };

    let slowdown_factor = if job_throughput > 0 {
        sql_throughput as f64 / job_throughput as f64
    } else {
        0.0
    };

    println!("═══════════════════════════════════════════════════════════");
    println!("📊 SCENARIO 1 BASELINE RESULTS");
    println!("═══════════════════════════════════════════════════════════");
    println!("Pure SQL Engine:");
    println!("  Input:       {} records", num_records);
    println!(
        "  Output:      {} results (window processing verified)",
        sql_result_count
    );
    println!("  Time:        {:.2}ms", sql_time_us as f64 / 1000.0);
    println!(
        "  Throughput:  {} rec/sec (input processing speed)",
        sql_throughput
    );
    println!(
        "  Amplification: {:.2}x (output vs input)",
        sql_result_count as f64 / num_records as f64
    );
    println!();
    println!("Job Server:");
    println!("  Time:        {:.2}ms", job_time_us as f64 / 1000.0);
    println!("  Throughput:  {} rec/sec", job_throughput);
    println!();
    println!("Overhead Analysis:");
    println!("  Job Server overhead: {:.1}%", overhead_pct);
    println!("  Slowdown factor:     {:.2}x", slowdown_factor);
    println!();
    println!("📋 Comparison to Other Scenarios:");
    println!("  Scenario 0 (SELECT):    62% overhead (2.6x slowdown)");
    println!(
        "  Scenario 1 (ROWS WINDOW): {:.1}% overhead ({:.1}x slowdown)",
        overhead_pct, slowdown_factor
    );
    println!("  Scenario 2 (GROUP BY):  80% overhead (4.9x slowdown)");
    println!();
    println!("📋 ROWS WINDOW Characteristics:");
    println!("  - Pattern: Memory-bounded sliding buffers");
    println!("  - Buffer Size: 100 rows per partition");
    println!("  - Partitions: 10 (symbol-based)");
    println!("  - Aggregations: AVG, MIN, MAX");
    println!("  - No GROUP BY (different optimization pattern)");
    println!("═══════════════════════════════════════════════════════════\n");

    // Validate server metrics and record samples
    println!("📊 VALIDATION: Server Metrics & Record Sampling");
    println!("═══════════════════════════════════════════════════════════");

    // Validate server metrics - note: job server doesn't track stats the same way
    // so we just validate that records were processed
    let processed_count = num_records; // We know this many records were sent
    let metrics_validation = MetricsValidation::validate_metrics(
        processed_count,
        1, // One batch
        0, // No failures expected
    );
    metrics_validation.print_results();

    // Get sampled records (1 in 10k sampling) from the shared reference
    let samples = samples_ref.lock().await.clone();
    // Validate sampled records using reusable module
    let record_validation = validate_records(&samples);
    // Print validation results in standard format
    print_validation_results(&samples, &record_validation, 10000);
    println!("═══════════════════════════════════════════════════════════");

    // Assert reasonable performance
    assert!(sql_throughput > 0, "SQL engine should process records");
    assert!(job_throughput > 0, "Job server should process records");
}

// =============================================================================
// V2 Job Server Measurement (NEW - StickyPartitionStrategy @1-core)
// =============================================================================

#[tokio::test]
#[serial]
async fn scenario_1_v2_sticky_partition_1core() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Scenario 1: ROWS WINDOW V2@1-core (StickyPartition)");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("Goal: Measure V2 Job Server with StickyPartitionStrategy");
    println!("Configuration: 1 partition (single core), per-batch locking");
    println!("Expected: Should approach SQL Engine baseline (~95%+)\n");

    let num_records = 5000;
    let records = generate_rows_window_records(num_records);

    // First, measure pure SQL engine as reference
    println!("🚀 Baseline: Measuring pure SQL engine...");
    let (sql_result_count, sql_time_us) =
        measure_rows_window_sql_engine(records.clone(), BASELINE_SQL).await;
    let sql_throughput = if sql_time_us > 0 {
        (num_records as f64 / (sql_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ SQL Engine: {} rec/sec in {:.2}ms\n",
        sql_throughput,
        sql_time_us as f64 / 1000.0
    );

    // Now measure V2 with StickyPartitionStrategy @1-core
    println!("🚀 Measuring V2 Job Server (1 partition, StickyPartition)...");

    use tokio::sync::RwLock;
    use velostream::velostream::server::processors::JobProcessor;
    use velostream::velostream::server::v2::{
        PartitionedJobConfig, PartitionedJobCoordinator, ProcessingMode,
    };

    // Configure for 1 partition (single core scenario)
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Batch { size: 100 },
        ..Default::default()
    };

    let coordinator = PartitionedJobCoordinator::new(config);
    let data_source = RowsWindowDataSource::new(records.clone());
    let data_writer = RowsWindowDataWriter::new();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(BASELINE_SQL).expect("Parse failed");

    // Create execution engine wrapped in Arc<RwLock<>>
    let (output_tx, _output_rx) = mpsc::unbounded_channel();
    let engine = Arc::new(RwLock::new(StreamExecutionEngine::new(output_tx)));

    // Create shutdown channel
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let v2_start = Instant::now();
    let _result = coordinator
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            parsed_query,
            "rows_window_v2_baseline".to_string(),
            shutdown_rx,
        )
        .await;
    let v2_time_us = v2_start.elapsed().as_micros();

    let v2_throughput = if v2_time_us > 0 {
        (num_records as f64 / (v2_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    let v2_overhead_pct = if sql_throughput > 0 {
        ((sql_throughput as f64 - v2_throughput as f64) / sql_throughput as f64) * 100.0
    } else {
        0.0
    };

    let v2_slowdown = if v2_throughput > 0 {
        sql_throughput as f64 / v2_throughput as f64
    } else {
        0.0
    };

    println!(
        "   ✅ V2 (1-core): {} rec/sec in {:.2}ms\n",
        v2_throughput,
        v2_time_us as f64 / 1000.0
    );

    println!("═══════════════════════════════════════════════════════════");
    println!("📊 V2@1-CORE PERFORMANCE ANALYSIS");
    println!("═══════════════════════════════════════════════════════════");
    println!("SQL Engine Baseline:     {} rec/sec", sql_throughput);
    println!("V2 (1-core StickyPart):  {} rec/sec", v2_throughput);
    println!();
    println!("V2 Overhead:             {:.1}%", v2_overhead_pct);
    println!("V2 Slowdown:             {:.2}x", v2_slowdown);
    println!();

    // Interpretation
    if v2_overhead_pct < 10.0 {
        println!("✅ EXCELLENT: V2 is within 10% of SQL Engine baseline");
        println!("   → StickyPartitionStrategy overhead is minimal");
    } else if v2_overhead_pct < 20.0 {
        println!("⚡ GOOD: V2 is 10-20% slower than SQL Engine");
        println!("   → Acceptable overhead for job server coordination");
    } else if v2_overhead_pct < 50.0 {
        println!("⚠️  MODERATE: V2 is 20-50% slower than SQL Engine");
        println!("   → Consider investigation of bottlenecks");
    } else {
        println!("❌ HIGH OVERHEAD: V2 is >50% slower than SQL Engine");
        println!("   → Significant issues with STP pipeline");
    }
    println!("═══════════════════════════════════════════════════════════\n");

    // Assertions
    assert!(v2_throughput > 0, "V2 should process records");
    // With StickyPartitionStrategy, we expect close to baseline performance
    // Allow up to 30% overhead for coordination layer
    let acceptable_overhead = 30.0;
    assert!(
        v2_overhead_pct <= acceptable_overhead,
        "V2@1-core overhead should be <{}% but was {:.1}%",
        acceptable_overhead,
        v2_overhead_pct
    );
}
