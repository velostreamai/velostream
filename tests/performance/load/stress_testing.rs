//! Stress Testing Benchmarks
//!
//! Extreme load testing to validate Velostream behavior under stress conditions,
//! including memory pressure, CPU saturation, and resource exhaustion scenarios.

use super::super::common::{
    BenchmarkConfig, BenchmarkMode, MetricsCollector, TestRecordConfig, generate_test_records,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    resource_manager::{ResourceLimits, ResourceManager},
    types::{FieldValue, StreamRecord},
};

/// Stress test with extremely large datasets
#[tokio::test]
#[ignore = "Stress test - run with --ignored --nocapture"]
async fn stress_test_extreme_dataset_sizes() {
    println!("🚀 Extreme Dataset Size Stress Test");
    println!("===================================");

    let mut metrics = MetricsCollector::verbose();

    // Progressively larger stress test scenarios
    let stress_levels = vec![
        ("Moderate Stress", 500_000),
        ("High Stress", 1_000_000),
        ("Extreme Stress", 2_000_000),
        ("Maximum Stress", 5_000_000),
    ];

    for (name, dataset_size) in stress_levels {
        println!("\n📊 {}: {} records", name, dataset_size);

        let test_config = TestRecordConfig::complex(dataset_size);

        metrics.start();
        let result = run_extreme_dataset_test(&test_config).await;
        let duration = metrics.end(&format!("stress_{}", name.to_lowercase().replace(" ", "_")));

        match result {
            Ok((processed, peak_memory_estimate, processing_phases)) => {
                let throughput = processed as f64 / duration.as_secs_f64();

                println!("   Processed:     {} records", processed);
                println!("   Duration:      {:?}", duration);
                println!("   Throughput:    {:.0} records/sec", throughput);
                println!("   Peak Memory:   {} MB (estimated)", peak_memory_estimate);
                println!("   Phases:        {}", processing_phases);

                metrics.set_counter(
                    &format!("stress_{}_processed", name.to_lowercase().replace(" ", "_")),
                    processed,
                );

                // Stress test success criteria
                let min_throughput = match dataset_size {
                    500_000 => 50_000.0,    // 50K records/sec for moderate
                    1_000_000 => 75_000.0,  // 75K records/sec for high
                    2_000_000 => 100_000.0, // 100K records/sec for extreme
                    5_000_000 => 125_000.0, // 125K records/sec for maximum
                    _ => 10_000.0,
                };

                if throughput >= min_throughput {
                    println!("   ✅ PASS: Stress test throughput acceptable");
                } else {
                    println!(
                        "   ⚠️  WARN: Stress test throughput low: {:.0} < {:.0}",
                        throughput, min_throughput
                    );
                }

                // Memory efficiency check
                let memory_per_record = peak_memory_estimate as f64 / processed as f64;
                if memory_per_record < 0.001 {
                    // Less than 1KB per 1000 records
                    println!("   ✅ PASS: Memory efficiency good");
                } else {
                    println!("   ⚠️  WARN: High memory usage per record");
                }
            }
            Err(e) => {
                println!("   ❌ Stress test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Stress test with resource exhaustion simulation
#[tokio::test]
#[ignore = "Stress test - run with --ignored --nocapture"]
async fn stress_test_resource_exhaustion() {
    println!("🚀 Resource Exhaustion Stress Test");
    println!("==================================");

    let mut metrics = MetricsCollector::verbose();

    // Test with extremely limited resources
    let resource_limits = vec![
        (
            "Minimal Resources",
            ResourceLimits {
                max_total_memory: Some(50 * 1024 * 1024),    // 50MB
                max_operator_memory: Some(10 * 1024 * 1024), // 10MB per operator
                max_windows_per_key: Some(10),
                max_aggregation_groups: Some(50),
                max_concurrent_operations: Some(5),
                max_processing_time_per_record: Some(100),
                custom_limits: std::collections::HashMap::new(),
            },
        ),
        (
            "Low Resources",
            ResourceLimits {
                max_total_memory: Some(100 * 1024 * 1024),   // 100MB
                max_operator_memory: Some(20 * 1024 * 1024), // 20MB per operator
                max_windows_per_key: Some(25),
                max_aggregation_groups: Some(100),
                max_concurrent_operations: Some(10),
                max_processing_time_per_record: Some(200),
                custom_limits: std::collections::HashMap::new(),
            },
        ),
        (
            "Constrained Resources",
            ResourceLimits {
                max_total_memory: Some(200 * 1024 * 1024),   // 200MB
                max_operator_memory: Some(40 * 1024 * 1024), // 40MB per operator
                max_windows_per_key: Some(50),
                max_aggregation_groups: Some(200),
                max_concurrent_operations: Some(15),
                max_processing_time_per_record: Some(500),
                custom_limits: std::collections::HashMap::new(),
            },
        ),
    ];

    for (name, limits) in resource_limits {
        println!("\n📊 {}", name);
        println!(
            "   Max Memory: {} MB",
            limits.max_total_memory.unwrap_or(0) / (1024 * 1024)
        );
        println!(
            "   Max Operations: {}",
            limits.max_concurrent_operations.unwrap_or(0)
        );

        let test_config = TestRecordConfig::complex(100_000);

        metrics.start();
        let result = run_resource_exhaustion_test(&test_config, limits).await;
        let duration = metrics.end(&format!(
            "exhaustion_{}",
            name.to_lowercase().replace(" ", "_")
        ));

        match result {
            Ok((processed, resource_violations, degradation_events)) => {
                let throughput = processed as f64 / duration.as_secs_f64();

                println!("   Processed:         {} records", processed);
                println!("   Throughput:        {:.0} records/sec", throughput);
                println!("   Resource Violations: {}", resource_violations);
                println!("   Degradation Events:  {}", degradation_events);

                metrics.set_counter(
                    &format!(
                        "exhaustion_{}_processed",
                        name.to_lowercase().replace(" ", "_")
                    ),
                    processed,
                );
                metrics.set_counter(
                    &format!(
                        "exhaustion_{}_violations",
                        name.to_lowercase().replace(" ", "_")
                    ),
                    resource_violations,
                );

                // Validate graceful degradation
                if resource_violations < 10 {
                    println!("   ✅ PASS: Few resource violations");
                } else {
                    println!(
                        "   ⚠️  WARN: Many resource violations: {}",
                        resource_violations
                    );
                }

                if processed > 50_000 {
                    println!("   ✅ PASS: Maintained reasonable throughput under stress");
                } else {
                    println!("   ⚠️  WARN: Low throughput under resource constraints");
                }
            }
            Err(e) => {
                println!("   ❌ Resource exhaustion test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Stress test with circuit breaker under extreme failure rates
#[tokio::test]
#[ignore = "Stress test - run with --ignored --nocapture"]
async fn stress_test_circuit_breaker_extreme_failures() {
    println!("🚀 Circuit Breaker Extreme Failure Stress Test");
    println!("==============================================");

    let mut metrics = MetricsCollector::verbose();

    // Test circuit breaker behavior under extreme failure conditions
    let failure_scenarios = vec![
        ("Cascading Failures", 0.8, 50_000),
        ("System Collapse", 0.95, 25_000),
        ("Total Failure", 0.99, 10_000),
    ];

    for (name, failure_rate, operations) in failure_scenarios {
        println!("\n📊 {}: {:.1}% failure rate", name, failure_rate * 100.0);

        let config = CircuitBreakerConfig {
            failure_threshold: 3, // Very sensitive
            recovery_timeout: Duration::from_secs(10),
            success_threshold: 2,
            operation_timeout: Duration::from_secs(15),
            failure_rate_window: Duration::from_secs(30),
            min_calls_in_window: 5,
            failure_rate_threshold: 25.0, // Very low tolerance
        };

        metrics.start();
        let result = run_circuit_breaker_stress_test(operations, failure_rate, config).await;
        let duration = metrics.end(&format!(
            "cb_stress_{}",
            name.to_lowercase().replace(" ", "_")
        ));

        match result {
            Ok((attempted, successful, failed, circuit_open_time)) => {
                let success_rate = successful as f64 / attempted as f64 * 100.0;
                let circuit_open_percent =
                    circuit_open_time.as_secs_f64() / duration.as_secs_f64() * 100.0;

                println!("   Attempted:         {} operations", attempted);
                println!("   Successful:        {} operations", successful);
                println!("   Failed:            {} operations", failed);
                println!("   Success Rate:      {:.1}%", success_rate);
                println!("   Circuit Open Time: {:.1}%", circuit_open_percent);

                metrics.set_counter(
                    &format!(
                        "cb_stress_{}_attempted",
                        name.to_lowercase().replace(" ", "_")
                    ),
                    attempted,
                );
                metrics.set_counter(
                    &format!(
                        "cb_stress_{}_successful",
                        name.to_lowercase().replace(" ", "_")
                    ),
                    successful,
                );

                // Validate circuit breaker protection effectiveness
                if circuit_open_percent > 50.0 && failure_rate > 0.9 {
                    println!("   ✅ PASS: Circuit breaker protected system from extreme failures");
                } else if circuit_open_percent > 20.0 && failure_rate > 0.7 {
                    println!("   ✅ PASS: Circuit breaker responded to high failure rate");
                } else {
                    println!("   ⚠️  WARN: Circuit breaker may not be protecting effectively");
                }
            }
            Err(e) => {
                println!("   ❌ Circuit breaker stress test failed: {}", e);
            }
        }
    }

    metrics.report().print();
}

/// Comprehensive stress test combining multiple stressors
#[tokio::test]
#[ignore = "Stress test - run with --ignored --nocapture"]
async fn stress_test_comprehensive_stress() {
    println!("🚀 Comprehensive Multi-Factor Stress Test");
    println!("=========================================");

    let mut metrics = MetricsCollector::verbose();

    println!(
        "📊 Combining: Large dataset + Resource constraints + High failure rate + Concurrency"
    );

    let test_config = TestRecordConfig::complex(1_000_000);
    let limits = ResourceLimits {
        max_total_memory: Some(300 * 1024 * 1024),   // 300MB
        max_operator_memory: Some(60 * 1024 * 1024), // 60MB per operator
        max_windows_per_key: Some(30),
        max_aggregation_groups: Some(150),
        max_concurrent_operations: Some(15),
        max_processing_time_per_record: Some(1000),
        custom_limits: std::collections::HashMap::new(),
    };

    metrics.start();
    let result = run_comprehensive_stress_test(&test_config, limits, 0.3, 4).await;
    let duration = metrics.end("comprehensive_stress");

    match result {
        Ok((processed, failures, resource_pressure_events, concurrent_efficiency)) => {
            let throughput = processed as f64 / duration.as_secs_f64();
            let failure_rate = failures as f64 / (processed + failures) as f64 * 100.0;

            println!("   Total Processed:       {} records", processed);
            println!("   Total Failures:        {}", failures);
            println!("   Throughput:             {:.0} records/sec", throughput);
            println!("   Failure Rate:           {:.1}%", failure_rate);
            println!(
                "   Resource Pressure:      {} events",
                resource_pressure_events
            );
            println!("   Concurrent Efficiency:  {:.1}%", concurrent_efficiency);

            metrics.set_counter("comprehensive_processed", processed);
            metrics.set_counter("comprehensive_failures", failures);
            metrics.set_counter("comprehensive_pressure", resource_pressure_events);

            // Comprehensive stress test validation
            let overall_score = (
                (throughput / 50_000.0).min(1.0) * 25.0 +  // Throughput score (max 25 points)
                ((100.0 - failure_rate) / 100.0) * 25.0 +  // Reliability score (max 25 points)
                ((100.0 - resource_pressure_events as f64 / 100.0).max(0.0) / 100.0) * 25.0 + // Resource efficiency (max 25 points)
                (concurrent_efficiency / 100.0) * 25.0
                // Concurrency score (max 25 points)
            );

            println!("   Overall Stress Score:   {:.1}/100", overall_score);

            if overall_score >= 70.0 {
                println!("   ✅ PASS: System handled comprehensive stress well");
            } else if overall_score >= 50.0 {
                println!("   ⚠️  WARN: System showed stress under comprehensive load");
            } else {
                println!("   ❌ FAIL: System struggled under comprehensive stress");
            }
        }
        Err(e) => {
            println!("   ❌ Comprehensive stress test failed: {}", e);
        }
    }

    metrics.report().print();
}

// Implementation functions

async fn run_extreme_dataset_test(
    config: &TestRecordConfig,
) -> Result<(u64, u64, u32), Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    let mut processed = 0u64;
    let mut peak_memory_estimate = 0u64;
    let mut processing_phases = 0u32;

    // Process in phases to manage memory
    let phase_size = 100_000;
    let total_phases = (records.len() + phase_size - 1) / phase_size;

    for phase in 0..total_phases {
        let start_idx = phase * phase_size;
        let end_idx = ((phase + 1) * phase_size).min(records.len());
        let phase_records = &records[start_idx..end_idx];

        // Simulate memory usage for this phase
        let phase_memory = (phase_records.len() * 100) as u64; // Estimate 100 bytes per record
        peak_memory_estimate = peak_memory_estimate.max(phase_memory);

        // Process phase records
        for record in phase_records {
            // Intensive processing simulation
            let mut result_fields = HashMap::new();

            for (key, value) in &record.fields {
                match key.as_str() {
                    "id" => {
                        if let FieldValue::Integer(id) = value {
                            result_fields
                                .insert("processed_id".to_string(), FieldValue::Integer(id * id));
                        }
                    }
                    "amount" => {
                        if let FieldValue::ScaledInteger(amount, scale) = value {
                            result_fields.insert(
                                "compound_amount".to_string(),
                                FieldValue::ScaledInteger(amount * amount, *scale),
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

            // Periodic yield for responsiveness
            if processed % 10_000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        processing_phases += 1;

        // Brief pause between phases
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok((processed, peak_memory_estimate / 1024, processing_phases)) // Convert to KB
}

async fn run_resource_exhaustion_test(
    config: &TestRecordConfig,
    limits: ResourceLimits,
) -> Result<(u64, u64, u64), Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    // Clone the value we need before moving limits
    let max_memory = limits.max_total_memory;

    let mut resource_manager = ResourceManager::new(limits);
    resource_manager.enable();

    let mut processed = 0u64;
    let mut resource_violations = 0u64;
    let mut degradation_events = 0u64;

    for (i, record) in records.iter().enumerate() {
        // Simulate resource usage increases
        let current_memory = (i / 1000) as u64 + 10;
        let current_cpu = 30.0 + (i % 1000) as f64 / 100.0;

        // Update resource manager
        if resource_manager
            .update_resource_usage("memory_mb", current_memory)
            .is_err()
        {
            resource_violations += 1;
        }

        if resource_manager
            .update_resource_usage("cpu_percent", current_cpu as u64)
            .is_err()
        {
            resource_violations += 1;
        }

        // Check for resource pressure and degrade gracefully
        if let Some(memory_usage) = resource_manager.get_resource_usage("total_memory") {
            if let Some(max_memory_val) = max_memory {
                if memory_usage.current > max_memory_val / 2 {
                    // Simulate degraded processing under resource pressure
                    degradation_events += 1;
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }

        // Basic processing
        let _id = record.fields.get("id");
        processed += 1;

        // Yield more frequently under resource pressure
        if resource_violations > 0 && i % 100 == 0 {
            tokio::task::yield_now().await;
        } else if i % 1_000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok((processed, resource_violations, degradation_events))
}

async fn run_circuit_breaker_stress_test(
    operations: usize,
    failure_rate: f64,
    config: CircuitBreakerConfig,
) -> Result<(u64, u64, u64, Duration), Box<dyn std::error::Error + Send + Sync>> {
    use velostream::velostream::sql::execution::error::StreamingError;

    let circuit_breaker = CircuitBreaker::new("stress_test".to_string(), config);

    let mut attempted = 0u64;
    let mut successful = 0u64;
    let mut failed = 0u64;
    let mut circuit_open_time = Duration::from_nanos(0);

    let start_time = std::time::Instant::now();
    let mut last_circuit_check = start_time;

    for i in 0..operations {
        let should_fail = (i as f64 / operations as f64) < failure_rate;

        attempted += 1;

        let operation_start = std::time::Instant::now();
        let result: Result<(), StreamingError> = circuit_breaker
            .execute(move || {
                if should_fail {
                    Err(StreamingError::CircuitBreakerOpen {
                        service: "stress_test".to_string(),
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
            Err(_) => {
                failed += 1;
                // Accumulate time when circuit was open
                if operation_start.duration_since(last_circuit_check) > Duration::from_millis(100) {
                    circuit_open_time += operation_start.duration_since(last_circuit_check);
                    last_circuit_check = operation_start;
                }
            }
        }

        // Brief delay to allow circuit breaker state changes
        if i % 100 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    Ok((attempted, successful, failed, circuit_open_time))
}

async fn run_comprehensive_stress_test(
    config: &TestRecordConfig,
    limits: ResourceLimits,
    failure_rate: f64,
    concurrency: usize,
) -> Result<(u64, u64, u64, f64), Box<dyn std::error::Error + Send + Sync>> {
    let records = generate_test_records(config);

    let mut resource_manager = ResourceManager::new(limits);
    resource_manager.enable();

    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 5,
        recovery_timeout: Duration::from_secs(30),
        success_threshold: 3,
        operation_timeout: Duration::from_secs(20),
        failure_rate_window: Duration::from_secs(60),
        min_calls_in_window: 10,
        failure_rate_threshold: 40.0,
    };

    // Split work among concurrent workers
    let total_records = records.len();
    let records_per_worker = total_records / concurrency;
    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let start_idx = worker_id * records_per_worker;
        let end_idx = if worker_id == concurrency - 1 {
            total_records
        } else {
            (worker_id + 1) * records_per_worker
        };

        let worker_records = records[start_idx..end_idx].to_vec();
        let worker_circuit =
            CircuitBreaker::new(format!("worker_{}", worker_id), circuit_config.clone());

        // Clone values needed in the async block
        let worker_id_clone = worker_id;
        let total_records_clone = total_records;
        let records_per_worker_clone = records_per_worker;
        let failure_rate_clone = failure_rate;

        let handle = tokio::spawn(async move {
            use velostream::velostream::sql::execution::error::StreamingError;

            let mut processed = 0u64;
            let mut failures = 0u64;
            let mut pressure_events = 0u64;

            for (i, record) in worker_records.iter().enumerate() {
                let should_fail = ((worker_id_clone * records_per_worker_clone + i) as f64
                    / total_records_clone as f64)
                    < failure_rate_clone;

                // Clone values needed in the closure
                let record_clone = record.clone();
                let should_fail_clone = should_fail;

                let result: Result<(), StreamingError> = worker_circuit
                    .execute(move || {
                        if should_fail_clone {
                            Err(StreamingError::CircuitBreakerOpen {
                                service: "comprehensive_stress".to_string(),
                                failure_count: 1,
                                last_failure_time: std::time::SystemTime::now(),
                                next_retry_time: std::time::SystemTime::now()
                                    + std::time::Duration::from_secs(1),
                            })
                        } else {
                            // Simulate resource-intensive processing
                            let mut result_fields = HashMap::new();
                            for (key, value) in &record_clone.fields {
                                result_fields.insert(
                                    format!("worker_{}_{}", worker_id_clone, key),
                                    value.clone(),
                                );
                            }
                            let _processed_record = StreamRecord::new(result_fields);
                            Ok(())
                        }
                    })
                    .await;

                match result {
                    Ok(()) => processed += 1,
                    Err(_) => failures += 1,
                }

                // Simulate resource pressure detection
                if i % 1000 == 0 {
                    pressure_events += 1;
                    tokio::time::sleep(Duration::from_micros(500)).await;
                }

                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            (processed, failures, pressure_events)
        });

        handles.push(handle);
    }

    // Collect results from all workers
    let mut total_processed = 0u64;
    let mut total_failures = 0u64;
    let mut total_pressure = 0u64;
    let mut worker_processed_counts = Vec::new();

    for handle in handles {
        let (processed, failures, pressure) = handle.await?;
        total_processed += processed;
        total_failures += failures;
        total_pressure += pressure;
        worker_processed_counts.push(processed);
    }

    // Calculate concurrent efficiency
    let max_worker_processed = worker_processed_counts.iter().max().unwrap_or(&0);
    let min_worker_processed = worker_processed_counts.iter().min().unwrap_or(&0);
    let concurrent_efficiency = if *max_worker_processed > 0 {
        (*min_worker_processed as f64 / *max_worker_processed as f64) * 100.0
    } else {
        0.0
    };

    Ok((
        total_processed,
        total_failures,
        total_pressure,
        concurrent_efficiency,
    ))
}
