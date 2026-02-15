//! Tests for observability utility functions
//!
//! Covers edge cases in calculate_throughput and with_observability_try_lock.

use velostream::velostream::server::processors::observability_utils::calculate_throughput;

#[test]
fn test_calculate_throughput_zero_records() {
    assert_eq!(calculate_throughput(0, 1000), 0.0);
}

#[test]
fn test_calculate_throughput_zero_records_zero_duration() {
    assert_eq!(calculate_throughput(0, 0), 0.0);
}

#[test]
fn test_calculate_throughput_sub_millisecond_duration() {
    // Sub-millisecond (duration_ms == 0) with records should treat as 1ms
    // 10 records in 1ms = 10,000 rps
    let result = calculate_throughput(10, 0);
    assert_eq!(result, 10_000.0);
}

#[test]
fn test_calculate_throughput_normal_case() {
    // 100 records in 1000ms = 100 rps
    let result = calculate_throughput(100, 1000);
    assert_eq!(result, 100.0);
}

#[test]
fn test_calculate_throughput_fractional_result() {
    // 1 record in 3000ms = 0.333... rps
    let result = calculate_throughput(1, 3000);
    assert!((result - 0.3333333).abs() < 0.001);
}

#[test]
fn test_calculate_throughput_high_throughput() {
    // 1_000_000 records in 100ms = 10_000_000 rps
    let result = calculate_throughput(1_000_000, 100);
    assert_eq!(result, 10_000_000.0);
}

#[test]
fn test_calculate_throughput_single_record_1ms() {
    // 1 record in 1ms = 1000 rps
    let result = calculate_throughput(1, 1);
    assert_eq!(result, 1000.0);
}
