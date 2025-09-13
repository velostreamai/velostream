/*!
# Phase 3: Performance Benchmarks

Comprehensive performance benchmarks comparing legacy vs enhanced streaming modes
to validate production readiness and measure overhead of new features.

## Benchmark Categories:

1. Record Processing Speed - Legacy vs Enhanced throughput
2. Memory Usage Patterns - Resource consumption analysis
3. Latency Measurements - End-to-end processing time
4. Scalability Tests - Performance under increasing load
5. Feature Overhead - Individual feature performance impact
6. Real-world Scenarios - Production-like workload simulation
*/

use chrono::{DateTime, Utc};
use ferrisstreams::ferris::sql::execution::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    config::{LateDataStrategy, StreamingConfig, WatermarkStrategy as ConfigWatermarkStrategy},
    error::StreamingError,
    resource_manager::{ResourceLimits, ResourceManager},
    watermarks::{WatermarkManager, WatermarkStrategy},
};
use ferrisstreams::ferris::sql::execution::{types::FieldValue, StreamRecord};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;

// Benchmark configuration constants
const SMALL_DATASET: usize = 1_000;
const MEDIUM_DATASET: usize = 10_000;
const LARGE_DATASET: usize = 100_000;
const STRESS_DATASET: usize = 1_000_000;

// ===== RECORD PROCESSING BENCHMARKS =====

#[tokio::test]
#[ignore = "Performance benchmark - run with --ignored --nocapture"]
async fn benchmark_record_processing_throughput() {
    println!("🚀 Record Processing Throughput Benchmark");
    println!("==========================================");

    // Test different dataset sizes
    for &dataset_size in &[SMALL_DATASET, MEDIUM_DATASET, LARGE_DATASET] {
        println!("\n📊 Dataset Size: {} records", dataset_size);

        // 1. Legacy Mode Benchmark
        let legacy_throughput = benchmark_legacy_processing(dataset_size).await;

        // 2. Enhanced Mode Benchmark (all features)
        let enhanced_throughput = benchmark_enhanced_processing(dataset_size).await;

        // 3. Partial Enhancement Benchmarks
        let watermarks_only = benchmark_watermarks_only(dataset_size).await;
        let resources_only = benchmark_resources_only(dataset_size).await;
        let circuit_breaker_only = benchmark_circuit_breaker_only(dataset_size).await;

        // Results Analysis
        println!(
            "  Legacy mode:           {:>8.0} records/sec",
            legacy_throughput
        );
        println!(
            "  Enhanced mode (all):   {:>8.0} records/sec ({:>6.2}x overhead)",
            enhanced_throughput,
            legacy_throughput / enhanced_throughput
        );
        println!(
            "  Watermarks only:       {:>8.0} records/sec ({:>6.2}x overhead)",
            watermarks_only,
            legacy_throughput / watermarks_only
        );
        println!(
            "  Resources only:        {:>8.0} records/sec ({:>6.2}x overhead)",
            resources_only,
            legacy_throughput / resources_only
        );
        println!(
            "  Circuit breaker only:  {:>8.0} records/sec ({:>6.2}x overhead)",
            circuit_breaker_only,
            legacy_throughput / circuit_breaker_only
        );

        // Validate performance is acceptable
        let max_acceptable_overhead = 3.0; // 3x slowdown is maximum acceptable
        let actual_overhead = legacy_throughput / enhanced_throughput;
        assert!(
            actual_overhead <= max_acceptable_overhead,
            "Enhanced mode overhead too high: {:.2}x (max: {:.1}x)",
            actual_overhead,
            max_acceptable_overhead
        );
    }
}

async fn benchmark_legacy_processing(record_count: usize) -> f64 {
    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("amount".to_string(), FieldValue::Float(i as f64 * 10.5));
        fields.insert(
            "category".to_string(),
            FieldValue::String(format!("cat_{}", i % 10)),
        );
        let record = StreamRecord::new(fields);

        // Basic legacy processing
        let _id = record.fields.get("id");
        let _amount = record.fields.get("amount");

        // Simulate some computation
        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_enhanced_processing(record_count: usize) -> f64 {
    // Initialize all enhanced features
    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(1000),
        max_total_memory: Some(1024 * 1024 * 1024), // 1GB
        ..Default::default()
    });
    resource_manager.enable();

    let watermark_strategy = WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(10),
        watermark_interval: Duration::from_secs(1),
    };
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let mut circuit_breaker = CircuitBreaker::with_default_config("benchmark".to_string());
    circuit_breaker.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("amount".to_string(), FieldValue::Float(i as f64 * 10.5));
        fields.insert(
            "category".to_string(),
            FieldValue::String(format!("cat_{}", i % 10)),
        );
        let mut record = StreamRecord::new(fields);

        // Enhanced processing with event time
        let event_time = Utc::now();
        record.event_time = Some(event_time);

        // Resource tracking
        let _ = resource_manager.update_resource_usage("records_processed", i as u64 + 1);

        // Circuit breaker protection
        let _: Result<(), StreamingError> = circuit_breaker
            .execute(|| {
                // Simple processing inside circuit breaker
                Ok(())
            })
            .await;

        // Watermark processing (outside circuit breaker to avoid borrowing issues)
        if i % 100 == 0 {
            watermark_manager.update_watermark("benchmark_source", &record);
        }

        // Simulate processing
        let _id = record.fields.get("id");
        let _amount = record.fields.get("amount");
        let _event_time = record.event_time;

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_watermarks_only(record_count: usize) -> f64 {
    let watermark_strategy = WatermarkStrategy::Ascending;
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("amount".to_string(), FieldValue::Float(i as f64 * 10.5));
        let mut record = StreamRecord::new(fields);
        record.event_time = Some(Utc::now());

        if i % 100 == 0 {
            watermark_manager.update_watermark("benchmark_source", &record);
        }

        let _id = record.fields.get("id");
        let _event_time = record.event_time;

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_resources_only(record_count: usize) -> f64 {
    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(10000),
        ..Default::default()
    });
    resource_manager.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("amount".to_string(), FieldValue::Float(i as f64 * 10.5));
        let record = StreamRecord::new(fields);

        let _ = resource_manager.update_resource_usage("records_processed", i as u64 + 1);

        let _id = record.fields.get("id");

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

async fn benchmark_circuit_breaker_only(record_count: usize) -> f64 {
    let mut circuit_breaker = CircuitBreaker::with_default_config("benchmark".to_string());
    circuit_breaker.enable();

    let start_time = Instant::now();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("amount".to_string(), FieldValue::Float(i as f64 * 10.5));
        let record = StreamRecord::new(fields);

        let _: Result<(), StreamingError> = circuit_breaker
            .execute(|| {
                // Simple processing
                Ok(())
            })
            .await;

        // Process record fields outside circuit breaker
        let _id = record.fields.get("id");

        if i % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    record_count as f64 / duration.as_secs_f64()
}

// ===== MEMORY USAGE BENCHMARKS =====

#[tokio::test]
#[ignore = "Performance benchmark - run with --ignored --nocapture"]
async fn benchmark_memory_usage_patterns() {
    println!("🧠 Memory Usage Patterns Benchmark");
    println!("===================================");

    // Note: This is a simplified memory usage analysis
    // In a real production environment, you would use profiling tools

    let dataset_size = MEDIUM_DATASET;

    // 1. Legacy mode memory usage
    println!("\n📊 Legacy Mode Memory Pattern");
    let legacy_peak_usage = simulate_memory_usage_legacy(dataset_size).await;
    println!("  Estimated peak usage: {} MB", legacy_peak_usage);

    // 2. Enhanced mode memory usage
    println!("\n📊 Enhanced Mode Memory Pattern");
    let enhanced_peak_usage = simulate_memory_usage_enhanced(dataset_size).await;
    println!("  Estimated peak usage: {} MB", enhanced_peak_usage);

    let memory_overhead_mb = enhanced_peak_usage - legacy_peak_usage;
    let memory_overhead_percent = (memory_overhead_mb / legacy_peak_usage) * 100.0;

    println!("\n📈 Memory Overhead Analysis:");
    println!("  Additional memory: {:.1} MB", memory_overhead_mb);
    println!("  Overhead percentage: {:.1}%", memory_overhead_percent);

    // Memory overhead should be reasonable for production
    assert!(
        memory_overhead_percent < 50.0,
        "Memory overhead too high: {:.1}%",
        memory_overhead_percent
    );
}

async fn simulate_memory_usage_legacy(record_count: usize) -> f64 {
    // Simulate baseline memory usage for record processing
    let base_record_size = 200; // bytes per record estimate
    let processing_overhead = 1.2; // 20% processing overhead

    (record_count * base_record_size) as f64 * processing_overhead / (1024.0 * 1024.0)
}

async fn simulate_memory_usage_enhanced(record_count: usize) -> f64 {
    // Enhanced mode has additional overhead for:
    // - Watermark tracking
    // - Resource monitoring
    // - Circuit breaker statistics
    // - Event time storage

    let base_memory = simulate_memory_usage_legacy(record_count).await;
    let watermark_overhead = 5.0; // MB for watermark tracking
    let resource_overhead = 2.0; // MB for resource monitoring
    let circuit_breaker_overhead = 1.0; // MB for circuit breaker stats
    let event_time_overhead = (record_count as f64 * 24.0) / (1024.0 * 1024.0); // DateTime size

    base_memory
        + watermark_overhead
        + resource_overhead
        + circuit_breaker_overhead
        + event_time_overhead
}

// ===== LATENCY BENCHMARKS =====

#[tokio::test]
#[ignore = "Performance benchmark - run with --ignored --nocapture"]
async fn benchmark_end_to_end_latency() {
    println!("⏱️  End-to-End Latency Benchmark");
    println!("=================================");

    const SAMPLE_SIZE: usize = 1000;

    // 1. Legacy mode latency
    let mut legacy_latencies = Vec::with_capacity(SAMPLE_SIZE);
    for _ in 0..SAMPLE_SIZE {
        let latency = measure_legacy_processing_latency().await;
        legacy_latencies.push(latency);
    }

    // 2. Enhanced mode latency
    let mut enhanced_latencies = Vec::with_capacity(SAMPLE_SIZE);
    let enhanced_components = setup_enhanced_components().await;

    for _ in 0..SAMPLE_SIZE {
        let latency = measure_enhanced_processing_latency(&enhanced_components).await;
        enhanced_latencies.push(latency);
    }

    // Statistical analysis
    let legacy_stats = calculate_latency_stats(&legacy_latencies);
    let enhanced_stats = calculate_latency_stats(&enhanced_latencies);

    println!("\n📊 Latency Statistics (microseconds):");
    println!("  Legacy Mode:");
    println!("    Mean: {:.1}μs", legacy_stats.mean);
    println!("    Median: {:.1}μs", legacy_stats.median);
    println!("    P95: {:.1}μs", legacy_stats.p95);
    println!("    P99: {:.1}μs", legacy_stats.p99);

    println!("  Enhanced Mode:");
    println!("    Mean: {:.1}μs", enhanced_stats.mean);
    println!("    Median: {:.1}μs", enhanced_stats.median);
    println!("    P95: {:.1}μs", enhanced_stats.p95);
    println!("    P99: {:.1}μs", enhanced_stats.p99);

    let latency_overhead = enhanced_stats.mean / legacy_stats.mean;
    println!("\n📈 Latency Overhead: {:.2}x", latency_overhead);

    // Latency overhead should be acceptable for production
    assert!(
        latency_overhead < 5.0,
        "Latency overhead too high: {:.2}x",
        latency_overhead
    );
}

async fn measure_legacy_processing_latency() -> f64 {
    let start = Instant::now();

    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(42));
    fields.insert(
        "data".to_string(),
        FieldValue::String("test_data".to_string()),
    );
    let record = StreamRecord::new(fields);

    // Simulate basic processing
    let _id = record.fields.get("id");
    let _data = record.fields.get("data");

    start.elapsed().as_micros() as f64
}

struct EnhancedComponents {
    resource_manager: ResourceManager,
    watermark_manager: WatermarkManager,
    circuit_breaker: CircuitBreaker,
}

async fn setup_enhanced_components() -> EnhancedComponents {
    let mut resource_manager = ResourceManager::new(ResourceLimits::default());
    resource_manager.enable();

    let watermark_strategy = WatermarkStrategy::Ascending;
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let mut circuit_breaker = CircuitBreaker::with_default_config("latency_test".to_string());
    circuit_breaker.enable();

    EnhancedComponents {
        resource_manager,
        watermark_manager,
        circuit_breaker,
    }
}

async fn measure_enhanced_processing_latency(components: &EnhancedComponents) -> f64 {
    let start = Instant::now();

    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(42));
    fields.insert(
        "data".to_string(),
        FieldValue::String("test_data".to_string()),
    );
    let mut record = StreamRecord::new(fields);
    record.event_time = Some(Utc::now());

    // Enhanced processing
    let _ = components
        .resource_manager
        .update_resource_usage("latency_test", 1);

    let _: Result<(), StreamingError> = components
        .circuit_breaker
        .execute(|| {
            // Simple processing inside circuit breaker
            Ok(())
        })
        .await;

    // Process record fields outside circuit breaker
    let _id = record.fields.get("id");
    let _data = record.fields.get("data");
    let _event_time = record.event_time;

    start.elapsed().as_micros() as f64
}

#[derive(Debug)]
struct LatencyStats {
    mean: f64,
    median: f64,
    p95: f64,
    p99: f64,
}

fn calculate_latency_stats(latencies: &[f64]) -> LatencyStats {
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median = sorted[sorted.len() / 2];
    let p95_index = (sorted.len() as f64 * 0.95) as usize;
    let p99_index = (sorted.len() as f64 * 0.99) as usize;
    let p95 = sorted[p95_index.min(sorted.len() - 1)];
    let p99 = sorted[p99_index.min(sorted.len() - 1)];

    LatencyStats {
        mean,
        median,
        p95,
        p99,
    }
}

// ===== SCALABILITY BENCHMARKS =====

#[tokio::test]
#[ignore = "Performance benchmark - run with --ignored --nocapture"]
async fn benchmark_scalability_characteristics() {
    println!("📈 Scalability Characteristics Benchmark");
    println!("========================================");

    let dataset_sizes = vec![1_000, 5_000, 10_000, 50_000, 100_000];

    println!("\n📊 Throughput vs Load:");
    println!("Records    Legacy (rec/s)    Enhanced (rec/s)    Overhead");
    println!("-------    --------------    ----------------    --------");

    for &size in &dataset_sizes {
        let legacy_throughput = benchmark_legacy_processing(size).await;
        let enhanced_throughput = benchmark_enhanced_processing(size).await;
        let overhead = legacy_throughput / enhanced_throughput;

        println!(
            "{:>7}    {:>10.0}        {:>12.0}        {:>5.2}x",
            size, legacy_throughput, enhanced_throughput, overhead
        );

        // Validate scalability - overhead should not increase significantly with load
        if size >= 50_000 {
            assert!(
                overhead <= 4.0,
                "Scalability issue: overhead {:.2}x at {} records",
                overhead,
                size
            );
        }
    }
}

// ===== STRESS TESTING BENCHMARKS =====

#[tokio::test]
#[ignore = "Stress test benchmark - run with --ignored --nocapture"]
async fn benchmark_stress_testing() {
    println!("💪 Stress Testing Benchmark");
    println!("===========================");

    const STRESS_DURATION: Duration = Duration::from_secs(10);
    const TARGET_RPS: usize = 10_000; // Records per second target

    // Setup enhanced mode for stress testing
    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(50_000),
        max_total_memory: Some(2 * 1024 * 1024 * 1024), // 2GB
        ..Default::default()
    });
    resource_manager.enable();

    let watermark_strategy = WatermarkStrategy::Ascending;
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    let mut circuit_breaker = CircuitBreaker::with_default_config("stress_test".to_string());
    circuit_breaker.enable();

    println!(
        "🚀 Starting stress test: {} RPS for {:?}",
        TARGET_RPS, STRESS_DURATION
    );

    let start_time = Instant::now();
    let mut records_processed = 0;
    let mut error_count = 0;

    // Stress test loop
    while start_time.elapsed() < STRESS_DURATION {
        let batch_start = Instant::now();

        // Process batch to achieve target RPS
        for i in 0..100 {
            // Process in batches of 100
            let mut fields = HashMap::new();
            fields.insert(
                "id".to_string(),
                FieldValue::Integer(records_processed as i64),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer(Utc::now().timestamp()),
            );
            fields.insert("value".to_string(), FieldValue::Float(i as f64));
            let mut record = StreamRecord::new(fields);
            record.event_time = Some(Utc::now());

            // Resource management
            if let Err(_) =
                resource_manager.update_resource_usage("stress_test", records_processed as u64)
            {
                error_count += 1;
                continue;
            }

            // Circuit breaker protection
            let has_id = record.fields.get("id").is_some();
            let has_event_time = record.event_time.is_some();
            let result: Result<(), StreamingError> = circuit_breaker
                .execute(move || {
                    // Simulate processing work
                    let _processing = has_id && has_event_time;
                    Ok(())
                })
                .await;

            match result {
                Ok(_) => records_processed += 1,
                Err(_) => error_count += 1,
            }

            // Update watermark periodically
            if records_processed % 1000 == 0 {
                watermark_manager.update_watermark("stress_source", &record);
            }
        }

        // Rate limiting to achieve target RPS
        let batch_duration = batch_start.elapsed();
        let target_batch_duration = Duration::from_millis(10); // 100 records in 10ms = 10k RPS

        if batch_duration < target_batch_duration {
            tokio::time::sleep(target_batch_duration - batch_duration).await;
        }
    }

    let total_duration = start_time.elapsed();
    let actual_rps = records_processed as f64 / total_duration.as_secs_f64();
    let error_rate = error_count as f64 / (records_processed + error_count) as f64 * 100.0;

    println!("\n📊 Stress Test Results:");
    println!("  Duration: {:?}", total_duration);
    println!("  Records processed: {}", records_processed);
    println!("  Errors encountered: {}", error_count);
    println!("  Actual RPS: {:.0}", actual_rps);
    println!("  Error rate: {:.2}%", error_rate);

    // Validate stress test results
    assert!(
        actual_rps >= TARGET_RPS as f64 * 0.8,
        "Stress test throughput too low: {:.0} RPS (target: {} RPS)",
        actual_rps,
        TARGET_RPS
    );
    assert!(
        error_rate < 5.0,
        "Stress test error rate too high: {:.2}%",
        error_rate
    );

    // Check component health after stress test
    let resource_stats = resource_manager.get_all_resource_usage();
    let circuit_stats = circuit_breaker.get_stats();

    println!("\n🏥 Component Health After Stress:");
    println!(
        "  Resource violations: {}",
        resource_stats
            .values()
            .map(|m| m.limit_violations)
            .sum::<u32>()
    );
    println!(
        "  Circuit breaker failures: {}",
        circuit_stats.total_failures
    );
    println!(
        "  Circuit breaker success rate: {:.2}%",
        (circuit_stats.total_successes as f64 / circuit_stats.total_calls as f64) * 100.0
    );
}

// ===== PRODUCTION SCENARIO BENCHMARKS =====

#[tokio::test]
#[ignore = "Production scenario benchmark - run with --ignored --nocapture"]
async fn benchmark_production_scenarios() {
    println!("🏭 Production Scenario Benchmark");
    println!("================================");

    // Scenario 1: Financial Trading Data Processing
    println!("\n💰 Financial Trading Scenario:");
    let trading_throughput = benchmark_financial_trading_scenario().await;
    println!(
        "  Trading data throughput: {:.0} trades/sec",
        trading_throughput
    );

    // Scenario 2: IoT Sensor Data Processing
    println!("\n🌡️  IoT Sensor Data Scenario:");
    let iot_throughput = benchmark_iot_sensor_scenario().await;
    println!(
        "  IoT sensor throughput: {:.0} readings/sec",
        iot_throughput
    );

    // Scenario 3: Web Analytics Processing
    println!("\n🌐 Web Analytics Scenario:");
    let analytics_throughput = benchmark_web_analytics_scenario().await;
    println!(
        "  Web analytics throughput: {:.0} events/sec",
        analytics_throughput
    );

    // All scenarios should achieve reasonable production throughput
    assert!(trading_throughput >= 1000.0, "Trading throughput too low");
    assert!(iot_throughput >= 5000.0, "IoT throughput too low");
    assert!(
        analytics_throughput >= 2000.0,
        "Analytics throughput too low"
    );
}

async fn benchmark_financial_trading_scenario() -> f64 {
    // Simulate high-frequency trading data with strict latency requirements
    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(10_000),
        max_total_memory: Some(1024 * 1024 * 1024), // 1GB
        ..Default::default()
    });
    resource_manager.enable();

    let mut circuit_breaker = CircuitBreaker::new(
        "trading".to_string(),
        CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout: Duration::from_millis(100), // Fast recovery for trading
            ..Default::default()
        },
    );
    circuit_breaker.enable();

    const TRADE_COUNT: usize = 10_000;
    let start_time = Instant::now();

    for i in 0..TRADE_COUNT {
        let mut fields = HashMap::new();
        fields.insert(
            "trade_id".to_string(),
            FieldValue::String(format!("T{}", i)),
        );
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert(
            "price".to_string(),
            FieldValue::Float(150.0 + (i as f64 * 0.01)),
        );
        fields.insert(
            "volume".to_string(),
            FieldValue::Integer(100 + (i as i64 * 10)),
        );
        let mut record = StreamRecord::new(fields);
        record.event_time = Some(Utc::now());

        // High-frequency processing with circuit breaker protection
        let _: Result<(), StreamingError> = circuit_breaker
            .execute(|| {
                // Simple processing inside circuit breaker
                Ok(())
            })
            .await;

        // Trade validation and processing outside circuit breaker
        let _price = record.fields.get("price");
        let _volume = record.fields.get("volume");
        let _ = resource_manager.update_resource_usage("active_trades", i as u64 + 1);
    }

    let duration = start_time.elapsed();
    TRADE_COUNT as f64 / duration.as_secs_f64()
}

async fn benchmark_iot_sensor_scenario() -> f64 {
    // Simulate IoT sensor data with high volume but relaxed latency
    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(20_000),
        ..Default::default()
    });
    resource_manager.enable();

    let watermark_strategy = WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(30), // IoT can have delays
        watermark_interval: Duration::from_secs(2),
    };
    let mut watermark_manager = WatermarkManager::new(watermark_strategy);
    watermark_manager.enable();

    const SENSOR_READINGS: usize = 50_000;
    let start_time = Instant::now();

    for i in 0..SENSOR_READINGS {
        let mut fields = HashMap::new();
        fields.insert(
            "sensor_id".to_string(),
            FieldValue::String(format!("S{}", i % 1000)),
        );
        fields.insert(
            "temperature".to_string(),
            FieldValue::Float(20.0 + (i as f64 % 10.0)),
        );
        fields.insert(
            "humidity".to_string(),
            FieldValue::Float(50.0 + (i as f64 % 30.0)),
        );
        let mut record = StreamRecord::new(fields);

        // Add some out-of-order data
        let event_offset = if i % 100 == 0 { -30 } else { 0 };
        let event_time = Utc::now() + chrono::Duration::seconds(event_offset);
        record.event_time = Some(event_time);

        // Process with watermark tracking
        resource_manager
            .update_resource_usage("sensor_readings", i as u64 + 1)
            .ok();

        if i % 1000 == 0 {
            watermark_manager.update_watermark("iot_sensors", &record);
        }

        // Yield occasionally for other tasks
        if i % 5000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    SENSOR_READINGS as f64 / duration.as_secs_f64()
}

async fn benchmark_web_analytics_scenario() -> f64 {
    // Simulate web analytics events with moderate volume and latency tolerance
    let config = StreamingConfig {
        enable_watermarks: true,
        enable_resource_monitoring: true,
        enable_circuit_breakers: true,
        watermark_strategy: ConfigWatermarkStrategy::BoundedOutOfOrderness,
        late_data_strategy: LateDataStrategy::IncludeInNextWindow,
        ..Default::default()
    };

    let mut resource_manager = ResourceManager::new(ResourceLimits {
        max_concurrent_operations: Some(15_000),
        ..Default::default()
    });
    resource_manager.enable();

    let wm_strategy = WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(60),
        watermark_interval: Duration::from_secs(5),
    };
    let mut watermark_manager = WatermarkManager::new(wm_strategy);
    watermark_manager.enable();

    let mut circuit_breaker = CircuitBreaker::with_default_config("analytics".to_string());
    circuit_breaker.enable();

    const PAGE_VIEWS: usize = 25_000;
    let start_time = Instant::now();

    for i in 0..PAGE_VIEWS {
        let mut fields = HashMap::new();
        fields.insert(
            "session_id".to_string(),
            FieldValue::String(format!("session_{}", i % 5000)),
        );
        fields.insert(
            "page_url".to_string(),
            FieldValue::String(format!("/page/{}", i % 100)),
        );
        fields.insert(
            "user_agent".to_string(),
            FieldValue::String("Mozilla/5.0".to_string()),
        );
        fields.insert(
            "response_time".to_string(),
            FieldValue::Integer(100 + (i as i64 % 500)),
        );
        let mut record = StreamRecord::new(fields);

        let event_time = Utc::now() - chrono::Duration::seconds((i % 300) as i64); // Some historical events
        record.event_time = Some(event_time);

        // Analytics processing with full enhanced features
        let has_session = record.fields.get("session_id").is_some();
        let has_response_time = record.fields.get("response_time").is_some();
        let resource_usage = i as u64 + 1;

        let resource_result = resource_manager.update_resource_usage("page_views", resource_usage);
        let _: Result<(), StreamingError> = circuit_breaker
            .execute(move || {
                resource_result?;

                // Simulate analytics computation
                let _processing = has_session && has_response_time;

                Ok(())
            })
            .await;

        // Update watermark for analytics windows
        if i % 500 == 0 {
            watermark_manager.update_watermark("web_analytics", &record);
        }

        if i % 2500 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let duration = start_time.elapsed();
    PAGE_VIEWS as f64 / duration.as_secs_f64()
}
