// Performance Tests - Wrapper for examples/performance/
// These tests are actually implemented as examples in examples/performance/
// This module provides a way to run them as tests for CI/CD integration

// Note: Run these with `cargo test --example performance_test_name`
// Or use the performance test scripts in scripts/


#[test]
fn test_json_performance() {
    // Placeholder - actual test is in examples/performance/json_perf_test
    // Run with: cargo run --example json_performance_test
    println!("To run JSON performance test: cargo run --example json_performance_test");
}

#[test]
fn test_raw_bytes_performance() {
    // Placeholder - actual test is in examples/performance/raw_bytes_performance_test.rs
    // Run with: cargo run --example raw_bytes_performance_test
    println!("To run raw bytes performance test: cargo run --example raw_bytes_performance_test");
}

#[test]
fn test_latency_performance() {
    // Placeholder - actual test is in examples/performance/latency_performance_test.rs
    // Run with: cargo run --example latency_performance_test
    println!("To run latency performance test: cargo run --example latency_performance_test");
}

#[test]
fn test_zero_copy_performance() {
    // Placeholder - actual test is in examples/performance/simple_zero_copy_test.rs
    // Run with: cargo run --example simple_zero_copy_test
    println!("To run zero copy test: cargo run --example simple_zero_copy_test");
}

#[test]
fn test_async_optimization_performance() {
    // Placeholder - actual test is in examples/performance/simple_async_optimization_test.rs
    // Run with: cargo run --example simple_async_optimization_test
    println!("To run async optimization test: cargo run --example simple_async_optimization_test");
}

#[test]
fn test_resource_monitoring_performance() {
    // Placeholder - actual test is in examples/performance/resource_monitoring_test.rs
    // Run with: cargo run --example resource_monitoring_test
    println!("To run resource monitoring test: cargo run --example resource_monitoring_test");
}