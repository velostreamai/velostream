//! Concurrent Access Load Testing
//!
//! Benchmarks for testing Velostream performance under concurrent access patterns,
//! multi-threading scenarios, and parallel processing workloads.

use super::super::common::{
    BenchmarkConfig, BenchmarkMode, MetricsCollector, TestRecordConfig, generate_test_records,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::sql::execution::{StreamRecord, types::FieldValue};

/// Test concurrent processing with multiple producer/consumer pairs
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_concurrent_processing() {
    println!("🚀 Concurrent Processing Load Test");
    println!("==================================");

    let mut metrics = MetricsCollector::verbose();

    // Test different concurrency levels
    let concurrency_levels = vec![2, 4, 8, 16];
    let records_per_worker = 25_000;

    for concurrency in concurrency_levels {
        println!("\n📊 Concurrency Level: {} workers", concurrency);

        metrics.start();
        let result = run_concurrent_workers(concurrency, records_per_worker).await;
        let duration = metrics.end(&format!("concurrent_{}", concurrency));

        match result {
            Ok((total_processed, worker_results)) => {
                let total_throughput = total_processed as f64 / duration.as_secs_f64();
                let avg_worker_throughput =
                    worker_results.iter().sum::<f64>() / worker_results.len() as f64;

                println!("   Total Processed: {} records", total_processed);
                println!("   Total Throughput: {:.0} records/sec", total_throughput);
                println!(
                    "   Avg Worker Throughput: {:.0} records/sec",
                    avg_worker_throughput
                );

                // Calculate scalability efficiency
                let theoretical_max = avg_worker_throughput * concurrency as f64;
                let scalability_efficiency = (total_throughput / theoretical_max) * 100.0;
                println!("   Scalability Efficiency: {:.1}%", scalability_efficiency);

                metrics.set_counter(
                    &format!("concurrent_{}_processed", concurrency),
                    total_processed,
                );

                // Validate concurrent performance
                if scalability_efficiency > 70.0 {
                    println!("   ✅ PASS: Good scalability efficiency");
                } else {
                    println!(
                        "   ⚠️  WARN: Low scalability efficiency: {:.1}%",
                        scalability_efficiency
                    );
                }
            }
            Err(e) => {
                println!("   ❌ Concurrent processing failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Test shared resource contention under concurrent access
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_shared_resource_contention() {
    println!("🚀 Shared Resource Contention Load Test");
    println!("=======================================");

    let mut metrics = MetricsCollector::verbose();

    let worker_count = 8;
    let operations_per_worker = 10_000;

    println!(
        "📊 Testing {} workers with {} operations each",
        worker_count, operations_per_worker
    );

    metrics.start();
    let result = run_shared_resource_test(worker_count, operations_per_worker).await;
    let duration = metrics.end("shared_resource_contention");

    match result {
        Ok((total_operations, contention_events, avg_wait_time)) => {
            let throughput = total_operations as f64 / duration.as_secs_f64();
            let contention_rate = (contention_events as f64 / total_operations as f64) * 100.0;

            println!("   Total Operations: {}", total_operations);
            println!("   Throughput: {:.0} ops/sec", throughput);
            println!("   Contention Events: {}", contention_events);
            println!("   Contention Rate: {:.2}%", contention_rate);
            println!("   Avg Wait Time: {:?}", avg_wait_time);

            metrics.set_counter("shared_resource_operations", total_operations);
            metrics.set_counter("shared_resource_contentions", contention_events);

            // Validate contention handling
            if contention_rate < 10.0 {
                println!("   ✅ PASS: Low contention rate");
            } else {
                println!("   ⚠️  WARN: High contention rate: {:.2}%", contention_rate);
            }

            if avg_wait_time < Duration::from_millis(10) {
                println!("   ✅ PASS: Low average wait time");
            } else {
                println!("   ⚠️  WARN: High average wait time: {:?}", avg_wait_time);
            }
        }
        Err(e) => {
            println!("   ❌ Shared resource test failed: {}", e);
        }
    }

    metrics.report().print();
}

/// Test parallel stream processing with fan-out/fan-in patterns
#[tokio::test]
#[ignore = "Load test - run with --ignored --nocapture"]
async fn load_test_parallel_stream_processing() {
    println!("🚀 Parallel Stream Processing Load Test");
    println!("======================================");

    let mut metrics = MetricsCollector::verbose();

    let fan_out_levels = vec![2, 4, 8];
    let stream_size = 100_000;

    for fan_out in fan_out_levels {
        println!("\n📊 Fan-out Level: {} parallel streams", fan_out);

        let test_config = TestRecordConfig::complex(stream_size);

        metrics.start();
        let result = run_parallel_stream_test(&test_config, fan_out).await;
        let duration = metrics.end(&format!("parallel_stream_{}", fan_out));

        match result {
            Ok((processed, processing_times)) => {
                let total_throughput = processed as f64 / duration.as_secs_f64();
                let avg_stream_time =
                    processing_times.iter().sum::<Duration>() / processing_times.len() as u32;
                let zero_duration = Duration::from_secs(0);
                let max_stream_time = processing_times.iter().max().unwrap_or(&zero_duration);
                let min_stream_time = processing_times.iter().min().unwrap_or(&zero_duration);

                println!("   Total Processed: {} records", processed);
                println!("   Total Throughput: {:.0} records/sec", total_throughput);
                println!("   Avg Stream Time: {:?}", avg_stream_time);
                println!("   Max Stream Time: {:?}", max_stream_time);
                println!("   Min Stream Time: {:?}", min_stream_time);

                let load_balance_variance = (max_stream_time.as_secs_f64()
                    - min_stream_time.as_secs_f64())
                    / avg_stream_time.as_secs_f64()
                    * 100.0;
                println!("   Load Balance Variance: {:.1}%", load_balance_variance);

                metrics.set_counter(&format!("parallel_stream_{}_processed", fan_out), processed);

                // Validate parallel processing efficiency
                if load_balance_variance < 30.0 {
                    println!("   ✅ PASS: Good load balancing");
                } else {
                    println!(
                        "   ⚠️  WARN: Poor load balancing: {:.1}%",
                        load_balance_variance
                    );
                }
            }
            Err(e) => {
                println!("   ❌ Parallel stream test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

// Implementation functions

async fn run_concurrent_workers(
    worker_count: usize,
    records_per_worker: usize,
) -> Result<(u64, Vec<f64>), Box<dyn std::error::Error + Send + Sync>> {
    let mut handles = Vec::new();
    let mut worker_throughputs = Vec::new();

    // Launch concurrent workers
    for worker_id in 0..worker_count {
        let handle = tokio::spawn(async move {
            let test_config = TestRecordConfig::basic(records_per_worker);
            let records = generate_test_records(&test_config);

            let start_time = std::time::Instant::now();

            // Simulate worker processing
            let mut processed = 0u64;
            for (i, record) in records.iter().enumerate() {
                // Simulate processing work specific to this worker
                let mut result_fields = HashMap::new();

                for (key, value) in &record.fields {
                    match key.as_str() {
                        "id" => {
                            if let FieldValue::Integer(id) = value {
                                result_fields.insert(
                                    format!("worker_{}_id", worker_id),
                                    FieldValue::Integer(id + worker_id as i64),
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

                // Yield occasionally to allow other workers to progress
                if i % 1_000 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            let duration = start_time.elapsed();
            let throughput = processed as f64 / duration.as_secs_f64();

            (processed, throughput)
        });

        handles.push(handle);
    }

    // Collect results from all workers
    let mut total_processed = 0u64;
    for handle in handles {
        let (processed, throughput) = handle.await?;
        total_processed += processed;
        worker_throughputs.push(throughput);
    }

    Ok((total_processed, worker_throughputs))
}

async fn run_shared_resource_test(
    worker_count: usize,
    operations_per_worker: usize,
) -> Result<(u64, u64, Duration), Box<dyn std::error::Error + Send + Sync>> {
    // Shared resource (protected by mutex)
    let shared_counter = Arc::new(Mutex::new(0u64));
    let contention_counter = Arc::new(Mutex::new(0u64));
    let wait_times = Arc::new(Mutex::new(Vec::<Duration>::new()));

    let mut handles = Vec::new();

    // Launch workers that contend for shared resource
    for _worker_id in 0..worker_count {
        let counter_clone = Arc::clone(&shared_counter);
        let contention_clone = Arc::clone(&contention_counter);
        let wait_times_clone = Arc::clone(&wait_times);

        let handle = tokio::spawn(async move {
            let mut operations = 0u64;

            for _i in 0..operations_per_worker {
                let wait_start = std::time::Instant::now();

                // Try to acquire shared resource with timeout
                let lock_result =
                    tokio::time::timeout(Duration::from_millis(100), counter_clone.lock()).await;

                let wait_time = wait_start.elapsed();

                match lock_result {
                    Ok(mut counter) => {
                        // Successfully acquired lock
                        *counter += 1;
                        operations += 1;

                        // Record wait time
                        if let Ok(mut times) = wait_times_clone.try_lock() {
                            times.push(wait_time);
                        }

                        // Hold lock briefly to simulate work
                        tokio::time::sleep(Duration::from_micros(10)).await;
                    }
                    Err(_) => {
                        // Timeout - record contention event
                        if let Ok(mut contention) = contention_clone.try_lock() {
                            *contention += 1;
                        }
                    }
                }

                // Brief yield to allow other workers
                if operations % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            operations
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    let mut total_operations = 0u64;
    for handle in handles {
        total_operations += handle.await?;
    }

    // Collect contention statistics
    let contention_events = *contention_counter.lock().await;
    let wait_time_samples = wait_times.lock().await;
    let avg_wait_time = if !wait_time_samples.is_empty() {
        wait_time_samples.iter().sum::<Duration>() / wait_time_samples.len() as u32
    } else {
        Duration::from_nanos(0)
    };

    Ok((total_operations, contention_events, avg_wait_time))
}

async fn run_parallel_stream_test(
    config: &TestRecordConfig,
    fan_out: usize,
) -> Result<(u64, Vec<Duration>), Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    // Split records among parallel streams
    let records_per_stream = records.len() / fan_out;
    let mut streams = Vec::new();

    for i in 0..fan_out {
        let start_idx = i * records_per_stream;
        let end_idx = if i == fan_out - 1 {
            records.len() // Last stream gets remaining records
        } else {
            (i + 1) * records_per_stream
        };

        let stream_records = records[start_idx..end_idx].to_vec();
        streams.push(stream_records);
    }

    // Process streams in parallel
    let mut handles = Vec::new();

    for (stream_id, stream_records) in streams.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();

            let mut processed = 0u64;
            for record in stream_records {
                // Simulate stream-specific processing
                let mut result_fields = HashMap::new();

                for (key, value) in record.fields {
                    match key.as_str() {
                        "id" => {
                            if let FieldValue::Integer(id) = value {
                                result_fields.insert(
                                    format!("stream_{}_id", stream_id),
                                    FieldValue::Integer(id * (stream_id + 1) as i64),
                                );
                            }
                        }
                        _ => {
                            result_fields.insert(key, value);
                        }
                    }
                }

                let _processed_record = StreamRecord::new(result_fields);
                processed += 1;

                // Yield occasionally
                if processed % 1_000 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            let duration = start_time.elapsed();
            (processed, duration)
        });

        handles.push(handle);
    }

    // Collect results from all parallel streams
    let mut total_processed = 0u64;
    let mut processing_times = Vec::new();

    for handle in handles {
        let (processed, duration) = handle.await?;
        total_processed += processed;
        processing_times.push(duration);
    }

    Ok((total_processed, processing_times))
}
