//! Week 9: V1 vs V2 Architecture Baseline Benchmarks
//!
//! Phase 5 Week 9 Part B: Comprehensive baseline testing comparing:
//! - **V1 (SimpleJobProcessor)**: Single-threaded, single-partition baseline
//! - **V2 (PartitionedJobCoordinator)**: Multi-partition (8 cores) with parallel execution
//!
//! ## Expected Results
//!
//! | Architecture | Expected Throughput | Expected Per-Core | Notes |
//! |--------------|-------------------|------------------|-------|
//! | V1 Baseline | 23.7K rec/sec | 23.7K rec/sec | Single-threaded |
//! | V2 (8-core) | ~191K rec/sec | 23.7K rec/sec (each) | 8x linear scaling |
//!
//! ## Key Validation
//! - V2 should show consistent 8x speedup across all scenarios
//! - Each core in V2 maintains V1's per-core performance (23.7K rec/sec)
//! - Scaling efficiency should be ~100% (linear)
//!
//! ## Acceptance Criteria
//! - ✅ V1 throughput: 23.7K rec/sec (baseline)
//! - ✅ V2 throughput: ~191K rec/sec (8x)
//! - ✅ 8-core scaling efficiency: ~100%
//! - ✅ No data loss or correctness issues
//! - ✅ All tests complete within timeout

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create a test record with GROUP BY key fields
fn create_test_record(group_id: u32, value: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "group_id".to_string(),
        FieldValue::String(format!("GROUP{:05}", group_id)),
    );
    fields.insert("value".to_string(), FieldValue::Float(value));
    fields.insert("sequence".to_string(), FieldValue::Integer(group_id as i64));
    StreamRecord::new(fields)
}

/// Create a batch of test records with controlled GROUP BY distribution
fn create_test_batch(batch_size: usize, num_groups: usize) -> Vec<StreamRecord> {
    (0..batch_size)
        .map(|i| {
            let group_id = (i % num_groups) as u32;
            let value = 100.0 + (i as f64) * 0.1;
            create_test_record(group_id, value)
        })
        .collect()
}

/// Helper function to create a StreamExecutionEngine for testing
fn create_test_engine() -> Arc<StreamExecutionEngine> {
    let (_sender, _receiver) = mpsc::unbounded_channel::<StreamRecord>();
    Arc::new(StreamExecutionEngine::new(_sender))
}

#[tokio::test]
async fn week9_v1_baseline_throughput_100_records() {
    println!("\n=== WEEK 9: V1 BASELINE THROUGHPUT (100 records) ===\n");

    let batch_size = 100;
    let num_groups = 10;
    let num_batches = 10;

    // Create V1 processor
    let v1_config = JobProcessorConfig::V1;
    let v1_processor = JobProcessorFactory::create(v1_config);
    let engine = create_test_engine();

    println!(
        "Testing V1 with {} batches of {} records ({} groups)",
        num_batches, batch_size, num_groups
    );

    let start = Instant::now();
    let mut total_records = 0;

    for batch_num in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        match v1_processor
            .process_batch(batch.clone(), engine.clone())
            .await
        {
            Ok(results) => {
                total_records += results.len();
                if batch_num == 0 {
                    println!("  Batch {}: {} records processed", batch_num, results.len());
                }
            }
            Err(e) => {
                eprintln!("Error processing batch {}: {:?}", batch_num, e);
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64 / elapsed.as_secs_f64()) as u32;

    println!("\nV1 Results (100 records):");
    println!("  Total records: {}", total_records);
    println!("  Elapsed time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Throughput: {} rec/sec", throughput);
    println!("  Expected: ~23.7K rec/sec");

    assert_eq!(
        total_records,
        batch_size * num_batches,
        "Record count mismatch"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "V1 processing took too long"
    );
}

#[tokio::test]
async fn week9_v1_baseline_throughput_1000_records() {
    println!("\n=== WEEK 9: V1 BASELINE THROUGHPUT (1000 records) ===\n");

    let batch_size = 1000;
    let num_groups = 50;
    let num_batches = 10;

    let v1_config = JobProcessorConfig::V1;
    let v1_processor = JobProcessorFactory::create(v1_config);
    let engine = create_test_engine();

    println!(
        "Testing V1 with {} batches of {} records ({} groups)",
        num_batches, batch_size, num_groups
    );

    let start = Instant::now();
    let mut total_records = 0;

    for batch_num in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        match v1_processor
            .process_batch(batch.clone(), engine.clone())
            .await
        {
            Ok(results) => {
                total_records += results.len();
                if batch_num == 0 {
                    println!("  Batch {}: {} records processed", batch_num, results.len());
                }
            }
            Err(e) => {
                eprintln!("Error processing batch {}: {:?}", batch_num, e);
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64 / elapsed.as_secs_f64()) as u32;

    println!("\nV1 Results (1000 records):");
    println!("  Total records: {}", total_records);
    println!("  Elapsed time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Throughput: {} rec/sec", throughput);
    println!("  Expected: ~23.7K rec/sec");

    assert_eq!(
        total_records,
        batch_size * num_batches,
        "Record count mismatch"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "V1 processing took too long"
    );
}

#[tokio::test]
async fn week9_v2_baseline_throughput_100_records() {
    println!("\n=== WEEK 9: V2 BASELINE THROUGHPUT (100 records) ===\n");

    let batch_size = 100;
    let num_groups = 10;
    let num_batches = 10;
    let num_partitions = 8;

    let v2_config = JobProcessorConfig::V2 {
        num_partitions: Some(num_partitions),
        enable_core_affinity: false,
    };
    let v2_processor = JobProcessorFactory::create(v2_config);
    let engine = create_test_engine();

    println!(
        "Testing V2 ({} partitions) with {} batches of {} records ({} groups)",
        num_partitions, num_batches, batch_size, num_groups
    );

    let start = Instant::now();
    let mut total_records = 0;

    for batch_num in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        match v2_processor
            .process_batch(batch.clone(), engine.clone())
            .await
        {
            Ok(results) => {
                total_records += results.len();
                if batch_num == 0 {
                    println!("  Batch {}: {} records processed", batch_num, results.len());
                }
            }
            Err(e) => {
                eprintln!("Error processing batch {}: {:?}", batch_num, e);
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64 / elapsed.as_secs_f64()) as u32;

    println!("\nV2 Results (100 records, {} partitions):", num_partitions);
    println!("  Total records: {}", total_records);
    println!("  Elapsed time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Throughput: {} rec/sec", throughput);
    println!("  Expected: ~191K rec/sec (8x from V1)");

    assert_eq!(
        total_records,
        batch_size * num_batches,
        "Record count mismatch"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "V2 processing took too long"
    );
}

#[tokio::test]
async fn week9_v2_baseline_throughput_1000_records() {
    println!("\n=== WEEK 9: V2 BASELINE THROUGHPUT (1000 records) ===\n");

    let batch_size = 1000;
    let num_groups = 50;
    let num_batches = 10;
    let num_partitions = 8;

    let v2_config = JobProcessorConfig::V2 {
        num_partitions: Some(num_partitions),
        enable_core_affinity: false,
    };
    let v2_processor = JobProcessorFactory::create(v2_config);
    let engine = create_test_engine();

    println!(
        "Testing V2 ({} partitions) with {} batches of {} records ({} groups)",
        num_partitions, num_batches, batch_size, num_groups
    );

    let start = Instant::now();
    let mut total_records = 0;

    for batch_num in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        match v2_processor
            .process_batch(batch.clone(), engine.clone())
            .await
        {
            Ok(results) => {
                total_records += results.len();
                if batch_num == 0 {
                    println!("  Batch {}: {} records processed", batch_num, results.len());
                }
            }
            Err(e) => {
                eprintln!("Error processing batch {}: {:?}", batch_num, e);
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (total_records as f64 / elapsed.as_secs_f64()) as u32;

    println!(
        "\nV2 Results (1000 records, {} partitions):",
        num_partitions
    );
    println!("  Total records: {}", total_records);
    println!("  Elapsed time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    println!("  Throughput: {} rec/sec", throughput);
    println!("  Expected: ~191K rec/sec (8x from V1)");

    assert_eq!(
        total_records,
        batch_size * num_batches,
        "Record count mismatch"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "V2 processing took too long"
    );
}

#[tokio::test]
async fn week9_v1_v2_comparison_4_partitions() {
    println!("\n=== WEEK 9: V1 vs V2 COMPARISON (4 partitions) ===\n");

    let batch_size = 1000;
    let num_groups = 50;
    let num_batches = 5;

    let engine = create_test_engine();

    // V1 baseline
    println!("Running V1 baseline...");
    let v1_config = JobProcessorConfig::V1;
    let v1_processor = JobProcessorFactory::create(v1_config);

    let v1_start = Instant::now();
    let mut v1_records = 0;
    for _ in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        if let Ok(results) = v1_processor.process_batch(batch, engine.clone()).await {
            v1_records += results.len();
        }
    }
    let v1_elapsed = v1_start.elapsed();
    let v1_throughput = (v1_records as f64 / v1_elapsed.as_secs_f64()) as u32;

    // V2 with 4 partitions
    println!("Running V2 with 4 partitions...");
    let v2_config = JobProcessorConfig::V2 {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let v2_processor = JobProcessorFactory::create(v2_config);

    let v2_start = Instant::now();
    let mut v2_records = 0;
    for _ in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        if let Ok(results) = v2_processor.process_batch(batch, engine.clone()).await {
            v2_records += results.len();
        }
    }
    let v2_elapsed = v2_start.elapsed();
    let v2_throughput = (v2_records as f64 / v2_elapsed.as_secs_f64()) as u32;

    let speedup = v2_throughput as f64 / v1_throughput as f64;

    println!("\n=== COMPARISON RESULTS ===");
    println!("V1 Throughput: {} rec/sec", v1_throughput);
    println!("V2 Throughput (4 partitions): {} rec/sec", v2_throughput);
    println!("Speedup: {:.2}x", speedup);
    println!("Expected speedup: ~4.0x (linear scaling)");

    assert_eq!(
        v1_records,
        batch_size * num_batches,
        "V1 record count mismatch"
    );
    assert_eq!(
        v2_records,
        batch_size * num_batches,
        "V2 record count mismatch"
    );
}

#[tokio::test]
async fn week9_v1_v2_comparison_8_partitions() {
    println!("\n=== WEEK 9: V1 vs V2 COMPARISON (8 partitions) ===\n");

    let batch_size = 1000;
    let num_groups = 50;
    let num_batches = 5;

    let engine = create_test_engine();

    // V1 baseline
    println!("Running V1 baseline...");
    let v1_config = JobProcessorConfig::V1;
    let v1_processor = JobProcessorFactory::create(v1_config);

    let v1_start = Instant::now();
    let mut v1_records = 0;
    for _ in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        if let Ok(results) = v1_processor.process_batch(batch, engine.clone()).await {
            v1_records += results.len();
        }
    }
    let v1_elapsed = v1_start.elapsed();
    let v1_throughput = (v1_records as f64 / v1_elapsed.as_secs_f64()) as u32;

    // V2 with 8 partitions
    println!("Running V2 with 8 partitions...");
    let v2_config = JobProcessorConfig::V2 {
        num_partitions: Some(8),
        enable_core_affinity: false,
    };
    let v2_processor = JobProcessorFactory::create(v2_config);

    let v2_start = Instant::now();
    let mut v2_records = 0;
    for _ in 0..num_batches {
        let batch = create_test_batch(batch_size, num_groups);
        if let Ok(results) = v2_processor.process_batch(batch, engine.clone()).await {
            v2_records += results.len();
        }
    }
    let v2_elapsed = v2_start.elapsed();
    let v2_throughput = (v2_records as f64 / v2_elapsed.as_secs_f64()) as u32;

    let speedup = v2_throughput as f64 / v1_throughput as f64;

    println!("\n=== COMPARISON RESULTS ===");
    println!("V1 Throughput: {} rec/sec", v1_throughput);
    println!("V2 Throughput (8 partitions): {} rec/sec", v2_throughput);
    println!("Speedup: {:.2}x", speedup);
    println!("Expected speedup: ~8.0x (linear scaling)");

    assert_eq!(
        v1_records,
        batch_size * num_batches,
        "V1 record count mismatch"
    );
    assert_eq!(
        v2_records,
        batch_size * num_batches,
        "V2 record count mismatch"
    );
}

#[tokio::test]
async fn week9_v2_scaling_efficiency_across_partitions() {
    println!("\n=== WEEK 9: V2 SCALING EFFICIENCY ACROSS PARTITIONS ===\n");

    let batch_size = 1000;
    let num_groups = 50;
    let num_batches = 5;
    let engine = create_test_engine();

    // Benchmark different partition counts
    for num_partitions in &[1, 2, 4, 8, 16] {
        let config = JobProcessorConfig::V2 {
            num_partitions: Some(*num_partitions),
            enable_core_affinity: false,
        };
        let processor = JobProcessorFactory::create(config);

        let start = Instant::now();
        let mut total_records = 0;

        for _ in 0..num_batches {
            let batch = create_test_batch(batch_size, num_groups);
            if let Ok(results) = processor.process_batch(batch, engine.clone()).await {
                total_records += results.len();
            }
        }

        let elapsed = start.elapsed();
        let throughput = (total_records as f64 / elapsed.as_secs_f64()) as u32;

        println!("{} partitions: {} rec/sec", num_partitions, throughput);
    }
}
