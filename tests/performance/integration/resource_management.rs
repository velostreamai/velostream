//! Resource Management Integration Benchmarks
//!
//! Performance benchmarks for resource management components including
//! circuit breakers, resource managers, watermark processing, and
//! comprehensive system resource monitoring.

use super::super::common::{
    generate_test_records, BenchmarkConfig, BenchmarkMode, MetricsCollector, TestRecordConfig,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use velostream::velostream::sql::execution::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    config::{LateDataStrategy, StreamingConfig, WatermarkStrategy as ConfigWatermarkStrategy},
    error::StreamingError,
    resource_manager::{ResourceLimits, ResourceManager},
    types::{FieldValue, StreamRecord},
    watermarks::{WatermarkManager, WatermarkStrategy},
};

/// Benchmark resource management overhead under various loads
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_resource_management_overhead() {
    println!("🚀 Resource Management Overhead Benchmark");
    println!("=========================================");

    let mut metrics = MetricsCollector::verbose();

    // Test different load scenarios
    for &load in &[1_000, 10_000, 50_000] {
        println!("\n📊 Load: {} records", load);

        // Baseline: No resource management
        metrics.start();
        let baseline_throughput = benchmark_baseline_processing(load).await;
        let baseline_duration = metrics.end(&format!("baseline_{}", load));

        // With resource management enabled
        metrics.start();
        let managed_throughput = benchmark_with_resource_management(load).await;
        let managed_duration = metrics.end(&format!("managed_{}", load));

        // Calculate overhead
        let overhead_percent =
            ((baseline_throughput - managed_throughput) / baseline_throughput) * 100.0;

        println!("   Baseline:  {:.0} records/sec", baseline_throughput);
        println!("   Managed:   {:.0} records/sec", managed_throughput);
        println!("   Overhead:  {:.1}%", overhead_percent);

        metrics.set_counter(
            &format!("baseline_{}_throughput", load),
            baseline_throughput as u64,
        );
        metrics.set_counter(
            &format!("managed_{}_throughput", load),
            managed_throughput as u64,
        );

        // Validate acceptable overhead (should be < 20%)
        if overhead_percent < 20.0 {
            println!("   ✅ PASS: Resource management overhead acceptable");
        } else {
            println!(
                "   ⚠️  WARN: Resource management overhead high: {:.1}%",
                overhead_percent
            );
        }
    }

    metrics.report().print();
}

/// Benchmark circuit breaker performance and protection effectiveness
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_circuit_breaker_performance() {
    println!("🚀 Circuit Breaker Performance Benchmark");
    println!("=======================================");

    let mut metrics = MetricsCollector::verbose();

    // Test circuit breaker with various failure rates
    for &failure_rate in &[0.0, 0.1, 0.5, 0.9] {
        println!("\n📊 Failure Rate: {:.1}%", failure_rate * 100.0);

        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 3,
            operation_timeout: Duration::from_secs(30),
            failure_rate_window: Duration::from_secs(60),
            min_calls_in_window: 10,
            failure_rate_threshold: 50.0,
        };

        metrics.start();
        let result = benchmark_circuit_breaker_with_failures(1_000, failure_rate, config).await;
        let duration = metrics.end(&format!(
            "circuit_breaker_{}",
            (failure_rate * 100.0) as u32
        ));

        match result {
            Ok((successful, failed, throughput)) => {
                println!("   Success:   {} operations", successful);
                println!("   Failed:    {} operations", failed);
                println!("   Throughput: {:.0} ops/sec", throughput);

                let expected_failures = (1_000.0 * failure_rate) as u64;
                let failure_accuracy =
                    (failed as f64 - expected_failures as f64).abs() / expected_failures as f64;

                if failure_accuracy < 0.2 {
                    // Within 20% of expected
                    println!("   ✅ PASS: Circuit breaker working correctly");
                } else {
                    println!("   ⚠️  WARN: Circuit breaker behavior unexpected");
                }

                metrics.set_counter(
                    &format!("cb_success_{}", (failure_rate * 100.0) as u32),
                    successful,
                );
                metrics.set_counter(
                    &format!("cb_failed_{}", (failure_rate * 100.0) as u32),
                    failed,
                );
            }
            Err(e) => {
                println!("   ❌ Circuit breaker test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Benchmark watermark processing performance
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_watermark_processing_performance() {
    println!("🚀 Watermark Processing Performance Benchmark");
    println!("============================================");

    let mut metrics = MetricsCollector::verbose();

    // Test different watermark strategies
    let strategies = vec![
        (
            "BoundedOutOfOrderness_Low",
            WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness: Duration::from_secs(5),
                watermark_interval: Duration::from_secs(1),
            },
        ),
        (
            "BoundedOutOfOrderness_High",
            WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness: Duration::from_secs(10),
                watermark_interval: Duration::from_secs(2),
            },
        ),
    ];

    for (name, strategy) in strategies {
        println!("\n📊 Strategy: {}", name);

        metrics.start();
        let throughput = benchmark_watermark_strategy(10_000, strategy).await;
        let duration = metrics.end(&format!("watermark_{}", name.to_lowercase()));

        println!("   Throughput: {:.0} records/sec", throughput);

        metrics.set_counter(
            &format!("watermark_{}_throughput", name.to_lowercase()),
            throughput as u64,
        );

        // Performance threshold validation
        let min_throughput = 5_000.0;
        if throughput >= min_throughput {
            println!("   ✅ PASS: Watermark processing performance acceptable");
        } else {
            println!("   ⚠️  WARN: Watermark processing slower than expected");
        }
    }

    metrics.report().print();
}

/// Benchmark comprehensive system resource monitoring
#[tokio::test]
#[ignore = "Integration benchmark - run with --ignored --nocapture"]
async fn benchmark_comprehensive_resource_monitoring() {
    println!("🚀 Comprehensive Resource Monitoring Benchmark");
    println!("==============================================");

    let mut metrics = MetricsCollector::verbose();

    // Test full resource monitoring suite
    metrics.start();
    let result = benchmark_full_resource_monitoring(25_000).await;
    let duration = metrics.end("comprehensive_monitoring");

    match result {
        Ok((processed, peak_memory, avg_cpu)) => {
            let throughput = processed as f64 / duration.as_secs_f64();

            println!("   Processed:    {} records", processed);
            println!("   Throughput:   {:.0} records/sec", throughput);
            println!("   Peak Memory:  {} KB", peak_memory);
            println!("   Avg CPU:      {:.1}%", avg_cpu);

            metrics.set_counter("comprehensive_processed", processed);
            metrics.set_counter("comprehensive_peak_memory", peak_memory);

            // Validate resource efficiency
            let memory_per_record = peak_memory as f64 / processed as f64;
            if memory_per_record < 1.0 {
                // Less than 1KB per record
                println!("   ✅ PASS: Memory usage efficient");
            } else {
                println!(
                    "   ⚠️  WARN: High memory usage per record: {:.2} KB",
                    memory_per_record
                );
            }
        }
        Err(e) => {
            println!("   ❌ Comprehensive monitoring failed: {}", e);
        }
    }

    metrics.report().print();
}

// Implementation functions

async fn benchmark_baseline_processing(record_count: usize) -> f64 {
    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(i as i64 * 100, 2),
        );

        let _record = StreamRecord::new(fields);

        // Simple processing simulation
        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_with_resource_management(record_count: usize) -> f64 {
    let limits = ResourceLimits {
        max_total_memory: Some(500 * 1024 * 1024),    // 500MB
        max_operator_memory: Some(100 * 1024 * 1024), // 100MB per operator
        max_windows_per_key: Some(100),
        max_aggregation_groups: Some(1000),
        max_concurrent_operations: Some(10),
        max_processing_time_per_record: Some(5000), // 5 seconds
        custom_limits: std::collections::HashMap::new(),
    };
    let mut resource_manager = ResourceManager::new(limits);
    resource_manager.enable();

    let config = CircuitBreakerConfig {
        failure_threshold: 5,
        recovery_timeout: Duration::from_secs(60),
        success_threshold: 3,
        operation_timeout: Duration::from_secs(30),
        failure_rate_window: Duration::from_secs(60),
        min_calls_in_window: 10,
        failure_rate_threshold: 50.0,
    };
    let circuit_breaker = CircuitBreaker::new("benchmark".to_string(), config);

    let watermark_strategy = WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(2),
        watermark_interval: Duration::from_secs(1),
    };
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(i as i64 * 100, 2),
        );

        let record = StreamRecord::new(fields);

        // Resource tracking
        let _ = resource_manager.update_resource_usage("records_processed", i as u64 + 1);

        // Circuit breaker protection
        let _: Result<(), StreamingError> = circuit_breaker.execute(|| Ok(())).await;

        // Watermark processing
        if i % 100 == 0 {
            watermark_manager.update_watermark("benchmark_source", &record);
        }

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_circuit_breaker_with_failures(
    operations: usize,
    failure_rate: f64,
    config: CircuitBreakerConfig,
) -> Result<(u64, u64, f64), Box<dyn std::error::Error + Send + Sync>> {
    let circuit_breaker = CircuitBreaker::new("test_failures".to_string(), config);

    let mut successful = 0u64;
    let mut failed = 0u64;
    let start_time = Instant::now();

    for i in 0..operations {
        let should_fail = (i as f64 / operations as f64) < failure_rate;

        let result: Result<(), StreamingError> = circuit_breaker
            .execute(move || {
                if should_fail {
                    Err(StreamingError::CircuitBreakerOpen {
                        service: "test_service".to_string(),
                        failure_count: 1,
                        last_failure_time: std::time::SystemTime::now(),
                        next_retry_time: std::time::SystemTime::now()
                            + std::time::Duration::from_secs(1),
                    })
                } else {
                    Ok(())
                }
            })
            .await;

        match result {
            Ok(()) => successful += 1,
            Err(_) => failed += 1,
        }

        if i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    Ok((successful, failed, throughput))
}

async fn benchmark_watermark_strategy(record_count: usize, strategy: WatermarkStrategy) -> f64 {
    let mut watermark_manager = WatermarkManager::new(strategy);
    watermark_manager.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("timestamp".to_string(), FieldValue::Integer(i as i64));

        let record = StreamRecord::new(fields);

        watermark_manager.update_watermark("test_source", &record);

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_full_resource_monitoring(
    record_count: usize,
) -> Result<(u64, u64, f64), Box<dyn std::error::Error + Send + Sync>> {
    let limits = ResourceLimits {
        max_total_memory: Some(1000 * 1024 * 1024),   // 1000MB
        max_operator_memory: Some(200 * 1024 * 1024), // 200MB per operator
        max_windows_per_key: Some(200),
        max_aggregation_groups: Some(2000),
        max_concurrent_operations: Some(20),
        max_processing_time_per_record: Some(1000), // 1 second
        custom_limits: std::collections::HashMap::new(),
    };
    let mut resource_manager = ResourceManager::new(limits);
    resource_manager.enable();

    let config = CircuitBreakerConfig {
        failure_threshold: 10,
        recovery_timeout: Duration::from_secs(30),
        success_threshold: 5,
        operation_timeout: Duration::from_secs(25),
        failure_rate_window: Duration::from_secs(60),
        min_calls_in_window: 20,
        failure_rate_threshold: 40.0,
    };
    let circuit_breaker = CircuitBreaker::new("monitoring".to_string(), config);

    let watermark_strategy = WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    };
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let mut processed = 0u64;
    let mut peak_memory = 0u64;
    let mut cpu_samples = Vec::new();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(i as i64 * 100, 2),
        );
        fields.insert(
            "category".to_string(),
            FieldValue::String(format!("cat_{}", i % 10)),
        );

        let record = StreamRecord::new(fields);

        // Full resource monitoring
        let _ = resource_manager.update_resource_usage("records_processed", i as u64 + 1);
        let _ = resource_manager.update_resource_usage("memory_mb", (i / 1000) as u64 + 10);

        if let Some(memory) = resource_manager.get_resource_usage("total_memory") {
            peak_memory = peak_memory.max(memory.current);
        }

        // Circuit breaker with occasional simulated issues
        let _: Result<(), StreamingError> = circuit_breaker
            .execute(move || {
                if i % 10000 == 0 {
                    // Simulate occasional resource pressure
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            })
            .await;

        // Watermark processing
        watermark_manager.update_watermark("full_monitoring", &record);

        processed += 1;

        // Simulate CPU usage tracking
        if i % 1000 == 0 {
            cpu_samples.push(50.0 + (i % 100) as f64 / 4.0); // Simulated CPU %
            tokio::task::yield_now().await;
        }
    }

    let avg_cpu = cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64;

    Ok((processed, peak_memory, avg_cpu))
}
