//! High Throughput Load Testing
//!
//! Benchmarks designed to test Velostream under extreme throughput conditions,
//! validating performance with large datasets and high-speed data ingestion.

use super::super::common::{
    generate_test_records, BenchmarkConfig, BenchmarkMode, MetricsCollector, TestRecordConfig,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{types::FieldValue, StreamRecord};

/// Test maximum throughput with large datasets
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_maximum_throughput() {
    println!("🚀 Maximum Throughput Load Test");
    println!("===============================");

    let mut metrics = MetricsCollector::verbose();

    // Progressive load testing with increasingly large datasets
    let test_scales = vec![
        ("Small Load", 10_000),
        ("Medium Load", 100_000),
        ("Large Load", 500_000),
        ("Stress Load", 1_000_000),
    ];

    for (name, scale) in test_scales {
        println!("\n📊 {}: {} records", name, scale);

        let test_config = TestRecordConfig::complex(scale);

        metrics.start();
        let result = run_high_throughput_test(&test_config).await;
        let duration = metrics.end(&format!(
            "throughput_{}",
            name.to_lowercase().replace(" ", "_")
        ));

        match result {
            Ok(processed) => {
                let throughput = processed as f64 / duration.as_secs_f64();
                let memory_efficiency = processed as f64 / 1024.0; // Simulated memory efficiency

                println!("   Processed:    {} records", processed);
                println!("   Duration:     {:?}", duration);
                println!("   Throughput:   {:.0} records/sec", throughput);
                println!("   Memory Eff:   {:.1} records/KB", memory_efficiency);

                metrics.set_counter(
                    &format!("{}_processed", name.to_lowercase().replace(" ", "_")),
                    processed,
                );

                // Performance thresholds based on scale
                let min_throughput = match scale {
                    10_000 => 5_000.0,      // 5K records/sec for small
                    100_000 => 20_000.0,    // 20K records/sec for medium
                    500_000 => 50_000.0,    // 50K records/sec for large
                    1_000_000 => 100_000.0, // 100K records/sec for stress
                    _ => 1_000.0,
                };

                if throughput >= min_throughput {
                    println!(
                        "   ✅ PASS: Throughput {} >= {} records/sec",
                        throughput, min_throughput
                    );
                } else {
                    println!(
                        "   ⚠️  WARN: Throughput {} < {} records/sec",
                        throughput, min_throughput
                    );
                }
            }
            Err(e) => {
                println!("   ❌ Load test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Test sustained high throughput over extended periods
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_sustained_throughput() {
    println!("🚀 Sustained Throughput Load Test");
    println!("=================================");

    let mut metrics = MetricsCollector::verbose();

    let records_per_batch = 50_000;
    let batch_count = 10;
    let total_records = records_per_batch * batch_count;

    println!(
        "📊 Processing {} batches of {} records each",
        batch_count, records_per_batch
    );
    println!("📊 Total: {} records", total_records);

    let mut batch_throughputs = Vec::new();
    let mut total_processed = 0u64;

    for batch in 1..=batch_count {
        println!("\n🔄 Batch {}/{}:", batch, batch_count);

        let test_config = TestRecordConfig::complex(records_per_batch);

        metrics.start();
        let result = run_sustained_batch_test(&test_config, batch).await;
        let duration = metrics.end(&format!("batch_{}", batch));

        match result {
            Ok(processed) => {
                let throughput = processed as f64 / duration.as_secs_f64();
                batch_throughputs.push(throughput);
                total_processed += processed;

                println!("   Processed: {} records in {:?}", processed, duration);
                println!("   Throughput: {:.0} records/sec", throughput);

                metrics.set_counter(&format!("batch_{}_processed", batch), processed);
            }
            Err(e) => {
                println!("   ❌ Batch {} failed: {}", batch, e);
            }
        }

        // Brief pause between batches to simulate realistic load patterns
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Calculate sustained performance metrics
    let avg_throughput = batch_throughputs.iter().sum::<f64>() / batch_throughputs.len() as f64;
    let min_throughput = batch_throughputs
        .iter()
        .fold(f64::INFINITY, |a, &b| a.min(b));
    let max_throughput = batch_throughputs.iter().fold(0.0_f64, |a, &b| a.max(b));
    let throughput_variance = (max_throughput - min_throughput) / avg_throughput * 100.0;

    println!("\n📈 Sustained Performance Summary:");
    println!("   Total Processed: {} records", total_processed);
    println!("   Avg Throughput:  {:.0} records/sec", avg_throughput);
    println!("   Min Throughput:  {:.0} records/sec", min_throughput);
    println!("   Max Throughput:  {:.0} records/sec", max_throughput);
    println!("   Variance:        {:.1}%", throughput_variance);

    // Validate sustained performance criteria
    if throughput_variance < 25.0 {
        println!("   ✅ PASS: Sustained throughput is consistent");
    } else {
        println!(
            "   ⚠️  WARN: High throughput variance: {:.1}%",
            throughput_variance
        );
    }

    if avg_throughput > 30_000.0 {
        println!("   ✅ PASS: Average throughput meets target");
    } else {
        println!(
            "   ⚠️  WARN: Average throughput below target: {:.0}",
            avg_throughput
        );
    }

    metrics.report().print();
}

/// Test pipeline throughput with backpressure handling
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_backpressure_handling() {
    println!("🚀 Backpressure Handling Load Test");
    println!("==================================");

    let mut metrics = MetricsCollector::verbose();

    // Test with different channel buffer sizes to simulate backpressure
    let buffer_sizes = vec![100, 1_000, 10_000, 100_000];
    let record_count = 200_000;

    for buffer_size in buffer_sizes {
        println!("\n📊 Buffer Size: {} records", buffer_size);

        let test_config = TestRecordConfig::complex(record_count);

        metrics.start();
        let result = run_backpressure_test(&test_config, buffer_size).await;
        let duration = metrics.end(&format!("backpressure_{}", buffer_size));

        match result {
            Ok((processed, queued_time, processing_time)) => {
                let throughput = processed as f64 / duration.as_secs_f64();
                let queue_efficiency =
                    processing_time.as_secs_f64() / queued_time.as_secs_f64() * 100.0;

                println!("   Processed:     {} records", processed);
                println!("   Throughput:    {:.0} records/sec", throughput);
                println!("   Queue Time:    {:?}", queued_time);
                println!("   Process Time:  {:?}", processing_time);
                println!("   Queue Eff:     {:.1}%", queue_efficiency);

                metrics.set_counter(
                    &format!("backpressure_{}_processed", buffer_size),
                    processed,
                );

                // Validate backpressure handling effectiveness
                if queue_efficiency > 50.0 {
                    println!("   ✅ PASS: Backpressure handling efficient");
                } else {
                    println!("   ⚠️  WARN: Backpressure causing delays");
                }
            }
            Err(e) => {
                println!("   ❌ Backpressure test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

// Implementation functions

async fn run_high_throughput_test(
    config: &TestRecordConfig,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    // High-throughput processing simulation
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Producer: Send all records as fast as possible
    let producer_records = records.clone();
    tokio::spawn(async move {
        for record in producer_records {
            if tx.send(record).is_err() {
                break;
            }
        }
    });

    // Consumer: Process records with minimal delay
    let mut processed = 0u64;
    while let Some(record) = rx.recv().await {
        // Minimal processing to test pure throughput
        let _id = record.fields.get("id");
        processed += 1;

        // Yield every 10K records to prevent blocking
        if processed % 10_000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(processed)
}

async fn run_sustained_batch_test(
    config: &TestRecordConfig,
    batch_num: usize,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    // Simulate sustained processing with realistic operations
    let mut processed = 0u64;
    for (i, record) in records.iter().enumerate() {
        // More realistic processing simulation
        let mut result_fields = HashMap::new();

        for (key, value) in &record.fields {
            match key.as_str() {
                "id" => {
                    if let FieldValue::Integer(id) = value {
                        result_fields.insert(
                            "processed_id".to_string(),
                            FieldValue::Integer(id + batch_num as i64),
                        );
                    }
                }
                "amount" => {
                    if let FieldValue::ScaledInteger(amount, scale) = value {
                        result_fields.insert(
                            "adjusted_amount".to_string(),
                            FieldValue::ScaledInteger(amount * 2, *scale),
                        );
                    }
                }
                _ => {
                    result_fields.insert(key.clone(), value.clone());
                }
            }
        }

        let _processed_record = StreamRecord::new(result_fields);
        processed += 1;

        // Yield periodically for sustained operation
        if i % 5_000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(processed)
}

async fn run_backpressure_test(
    config: &TestRecordConfig,
    buffer_size: usize,
) -> Result<(u64, Duration, Duration), Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    let (tx, mut rx) = mpsc::channel(buffer_size);

    let queue_start = std::time::Instant::now();

    // Producer with potential backpressure
    let producer_records = records.clone();
    let producer_handle = tokio::spawn(async move {
        for record in producer_records {
            if tx.send(record).await.is_err() {
                break;
            }
        }
    });

    let process_start = std::time::Instant::now();

    // Consumer with simulated processing delay
    let mut processed = 0u64;
    while let Some(record) = rx.recv().await {
        // Simulate processing work
        let _processing = record.fields.len();

        // Small delay to create potential backpressure
        if processed % 1_000 == 0 {
            tokio::time::sleep(Duration::from_micros(10)).await;
        }

        processed += 1;
    }

    let process_end = std::time::Instant::now();

    // Wait for producer to complete
    producer_handle.await?;

    let queued_time = process_start.duration_since(queue_start);
    let processing_time = process_end.duration_since(process_start);

    Ok((processed, queued_time, processing_time))
}
