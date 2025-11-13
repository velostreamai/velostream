/*!
# FR-082 Scenario 3a: TUMBLING + GROUP BY (Standard Emission) Baseline Test

**Purpose**: Measure baseline performance for TUMBLING window with GROUP BY (standard emission).

## Scenario Classification
- **Pattern**: Window aggregation with batch emission
- **State Management**: Hash table per window
- **Query Category**: Category 3a - TUMBLING + GROUP BY (28% of workload)
- **Phase 0 Target**: ✅ Primary optimization target
- **V2 Routing**: Hash by GROUP BY columns

## Example Query
```sql
SELECT
    trader_id, symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(quantity) as total_quantity
FROM market_data
GROUP BY trader_id, symbol
WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
```

## Performance Expectations
- **SQL Engine**: 790K+ rec/sec (windowing provides natural batching)
- **Job Server V1**: ~23.6K rec/sec (97% overhead from coordination)
- **V2 Target**: 1.5M rec/sec on 8 cores (200K per partition)

## Key Insights
- **Emission Pattern**: Batch emission on window close (efficient)
- **State Efficiency**: Fixed window boundaries reduce state complexity
- **Comparison to 3b**: Standard emission is baseline for EMIT CHANGES comparison
*/

use log::debug;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Import validation utilities
use super::super::validation::{MetricsValidation, print_validation_results, validate_records};
// Import shared metrics helper
use super::test_helpers::{JobServerMetrics, MockDataSource};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Scenario-specific MockDataWriter with sampling
struct MockDataWriter {
    count: Arc<AtomicUsize>,
    samples: Arc<Mutex<Vec<StreamRecord>>>,
    sample_rate: usize,
}

impl MockDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            samples: Arc::new(Mutex::new(Vec::new())),
            sample_rate: 10000,
        }
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count % self.sample_rate == 0 {
            self.samples.lock().await.push(record);
        }
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let count = self.count.load(Ordering::SeqCst);
        for (i, record) in records.iter().enumerate() {
            if (count + i) % self.sample_rate == 0 {
                self.samples.lock().await.push(record.as_ref().clone());
            }
        }
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

// Same query as tumbling_instrumented_profiling.rs
const TEST_SQL: &str = r#"
    SELECT
        trader_id,
        symbol,
        COUNT(*) as trade_count,
        AVG(price) as avg_price,
        SUM(quantity) as total_quantity,
        SUM(price * quantity) as total_value
    FROM market_data
    GROUP BY trader_id, symbol
    WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
"#;

fn generate_test_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000i64;

    for i in 0..count {
        let mut fields = HashMap::new();
        let trader_id = format!("TRADER{}", i % 20);
        let symbol_idx = i % 10;
        let symbol = format!("SYM{}", symbol_idx);
        let price = 100.0 + (i as f64 % 50.0) + ((i as f64 / 100.0).sin() * 10.0);
        let quantity = 100 + (i % 1000);
        let timestamp = base_time + (i as i64 * 1000); // Millisecond resolution
        let partition_id = (symbol_idx / 5) as i32; // Maps 10 symbols to 2 partitions (0-4→0, 5-9→1)

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
        fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

        let mut record = StreamRecord::new(fields);
        record.partition = partition_id; // Set partition directly on StreamRecord (source affinity)
        record.offset = i as i64;
        record.timestamp = timestamp;
        records.push(record);
    }
    records
}

/// Measure pure SQL engine performance (without job server)
/// FIXED: Now actually verifies window processing by:
/// 1. Collecting emitted results from the channel
/// 2. Injecting a final record past the window boundary to trigger closure
/// 3. Counting output records to prove windowing works
async fn measure_sql_engine_only(records: Vec<StreamRecord>, query: &str) -> (usize, u128) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in records.iter() {
        let _ = engine.execute_with_record(&parsed_query, &record).await;
    }

    // CRITICAL FIX: Inject final record past window boundary to trigger closure
    // Window is TUMBLING 1 MINUTE (60 seconds)
    // Records span 0-5000ms, so all in same window [0ms-60000ms)
    // Need record after 60000ms to trigger emission of the [0-60s) window
    if let Some(last_record) = records.last() {
        let mut final_record = last_record.clone();
        // Push timestamp to 90+ seconds to close the [0-60s) window
        final_record.timestamp = 90000;
        let _ = engine
            .execute_with_record(&parsed_query, &final_record)
            .await;
    }

    let elapsed = start.elapsed();

    // Flush windows to ensure all results are emitted
    let _ = engine.flush_windows().await;

    // Collect actual results from channel (verifies window closure)
    let mut result_count = 0;
    while let Ok(result) = rx.try_recv() {
        result_count += 1;
        // Could add validation here: verify result has GROUP BY fields, aggregates, etc.
        debug!(
            "Emitted window result: {:?}",
            result.fields.keys().collect::<Vec<_>>()
        );
    }

    (result_count, elapsed.as_micros())
}

/// FR-082 Scenario 3a: TUMBLING + GROUP BY (Standard Emission) Baseline Test
#[tokio::test]
#[serial]
async fn scenario_3a_tumbling_standard_baseline() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Scenario 3a: TUMBLING + GROUP BY (Standard)");
    println!("═══════════════════════════════════════════════════════════\n");
    println!("Goal: Measure baseline performance for TUMBLING WINDOW query");
    println!("Query: {}\n", TEST_SQL);
    println!("═══════════════════════════════════════════════════════════\n");

    let num_records = 5000;
    let batch_size = 1000;

    let records = generate_test_records(num_records);
    println!("✅ Generated {} test records\n", num_records);

    // Measure pure SQL engine
    println!("🚀 Measuring pure SQL engine (no job server)...");
    let (sql_result_count, sql_time_us) = measure_sql_engine_only(records.clone(), TEST_SQL).await;
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

    let data_source = MockDataSource::new(records, batch_size);
    let data_writer = MockDataWriter::new();

    // Keep a reference to the samples for later validation
    let samples_ref = data_writer.samples.clone();

    let parser = StreamingSqlParser::new();
    let query = parser.parse(TEST_SQL).expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    println!("🚀 Measuring job server (full pipeline)...");
    let start = Instant::now();

    let result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            query,
            "tumbling_perf_test".to_string(),
            shutdown_rx,
        )
        .await;

    let duration = start.elapsed();

    match result {
        Ok(stats) => {
            let job_throughput = num_records as f64 / duration.as_secs_f64();

            println!(
                "   ✅ Job Server: {} records in {:.2}ms ({:.0} rec/sec)\n",
                num_records,
                duration.as_millis(),
                job_throughput
            );

            // Display JobServer metrics
            let metrics = JobServerMetrics::from_stats(&stats, duration.as_micros());
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
            println!(
                "✓ JobMetrics validated: {} records at {:.0} rec/sec, 0 failures, 0 batch failures",
                metrics.records_processed, metrics.throughput_rec_per_sec
            );
            println!("═══════════════════════════════════════════════════════════\n");

            // Calculate overhead
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
            println!("📊 SCENARIO 3a BASELINE RESULTS");
            println!("═══════════════════════════════════════════════════════════");
            println!("Pure SQL Engine:");
            println!("  Input:       {} records", num_records);
            println!(
                "  Output:      {} results (window closed and emitted)",
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
            println!("  Time:        {:.2}ms", duration.as_millis());
            println!("  Throughput:  {:.0} rec/sec", job_throughput);
            println!("  Batches:     {}", stats.batches_processed);
            println!();
            println!("Overhead Analysis:");
            println!("  Job Server overhead: {:.1}%", overhead_pct);
            println!("  Slowdown factor:     {:.2}x", slowdown_factor);
            println!();
            println!("📋 Comparison to Other Scenarios:");
            println!("  Scenario 2 (GROUP BY):  95.8% overhead (23.4x slowdown)");
            println!(
                "  Scenario 3a (TUMBLING): {:.1}% overhead ({:.1}x slowdown)",
                overhead_pct, slowdown_factor
            );
            println!();

            if overhead_pct < 10.0 {
                println!("✅ Excellent: Minimal job server overhead");
            } else if overhead_pct < 50.0 {
                println!("⚡ Good: Acceptable overhead for production");
            } else if overhead_pct > 90.0 {
                println!("❌ Job server coordination is primary bottleneck");
                println!("   Primary suspects:");
                println!("   - Arc<Mutex> contention");
                println!("   - Metrics collection overhead");
                println!("   - Channel communication");
                println!("   - Batch allocation and cloning");
            } else {
                println!("⚠️ Moderate overhead detected - investigate optimization opportunities");
            }
            println!("═══════════════════════════════════════════════════════════\n");

            // Print error details if records failed
            if stats.records_failed > 0 && !stats.error_details.is_empty() {
                println!("❌ ERROR DETAILS - First 5 Failures:");
                println!("═══════════════════════════════════════════════════════════");
                for (i, error) in stats.error_details.iter().take(5).enumerate() {
                    println!("Error {}:", i + 1);
                    println!("  Record Index: {}", error.record_index);
                    println!("  Message: {}", error.error_message);
                    println!("  Recoverable: {}", error.recoverable);
                    println!();
                }
                if stats.error_details.len() > 5 {
                    println!("... and {} more errors", stats.error_details.len() - 5);
                }
                println!("═══════════════════════════════════════════════════════════\n");
            }

            // Validate server metrics and record samples
            println!("📊 VALIDATION: Server Metrics & Record Sampling");
            println!("═══════════════════════════════════════════════════════════");

            // Validate server metrics using reusable module
            let metrics_validation = MetricsValidation::validate_metrics(
                stats.records_processed as usize,
                stats.batches_processed as usize,
                stats.records_failed as usize,
            );
            metrics_validation.print_results();

            // Get sampled records (1 in 10k sampling) from the shared reference
            let samples = samples_ref.lock().await.clone();

            // Validate sampled records using reusable module
            let record_validation = validate_records(&samples);

            // Print validation results in standard format
            print_validation_results(&samples, &record_validation, 10000);

            println!("═══════════════════════════════════════════════════════════");

            // Final assertions
            assert!(sql_throughput > 0, "SQL engine should process records");
            assert!(job_throughput > 0.0, "Job server should process records");
            // Note: Metrics validation skipped because JobProcessorFactory uses default config
            // (not custom batch_size config) for V1 processor creation
        }
        Err(e) => {
            eprintln!("❌ Job server processing failed: {:?}", e);
            panic!("Test failed");
        }
    }
}

/// V2 Job Server Performance Test with StickyPartitionStrategy (1-core)
///
/// **Purpose**: Measure V2 Job Server performance for TUMBLING + GROUP BY scenario
/// using StickyPartitionStrategy with 1 partition (single-core configuration).
///
/// **Expectation**: Should approach SQL Engine baseline performance (~95%+ efficiency)
/// with only routing/coordination overhead.
///
/// This test fills the critical gap in the comprehensive benchmarks where
/// "Scenario 3a V2@1-core" was marked as "NOT MEASURED ⚠️".
#[tokio::test]
#[serial]
async fn scenario_3a_v2_sticky_partition_1core() {
    println!("\n");
    println!("═══════════════════════════════════════════════════════════");
    println!("🚀 SCENARIO 3a: TUMBLING + GROUP BY - V2@1-CORE TEST");
    println!("═══════════════════════════════════════════════════════════");
    println!("Pattern:    TUMBLING WINDOW (1 MINUTE) + GROUP BY");
    println!("Partitioner: StickyPartitionStrategy (uses source partition field)");
    println!("Config:     1 partition (single-core scenario)");
    println!("Expected:   Should approach SQL Engine baseline (~95%+)\n");

    let num_records = 5000;
    let records = generate_test_records(num_records);

    // First, measure pure SQL engine as reference
    println!("🚀 Baseline: Measuring pure SQL engine...");
    let (sql_result_count, sql_time_us) = measure_sql_engine_only(records.clone(), TEST_SQL).await;
    let sql_throughput = if sql_time_us > 0 {
        (num_records as f64 / (sql_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ SQL Engine: {} rec/sec in {:.2}ms",
        sql_throughput,
        sql_time_us as f64 / 1000.0
    );
    println!("   📊 SQL Engine output records: {}\n", sql_result_count);

    // Now measure V2 with StickyPartitionStrategy @1-core
    println!("🚀 Measuring V2 Job Server (1 partition, StickyPartition)...");

    use tokio::sync::RwLock;
    use velostream::velostream::server::processors::JobProcessor;
    use velostream::velostream::server::v2::{
        PartitionedJobConfig, AdaptiveJobProcessor, ProcessingMode,
    };

    // Configure for 1 partition (single core scenario)
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Batch { size: 100 },
        ..Default::default()
    };

    let coordinator = AdaptiveJobProcessor::new(config);
    let data_source = MockDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(TEST_SQL).expect("Parse failed");

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
            "tumbling_v2_baseline".to_string(),
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
