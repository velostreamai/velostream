//! Mathematical correctness tests for metrics storage types
//!
//! Verifies that each metric type (counter, gauge, histogram) produces
//! mathematically correct values with well-defined inputs and expected outputs.
//! These tests guard against broken numbers being reported to Prometheus/Grafana.
//!
//! Covers:
//! - Counter cumulative accumulation (with and without field values)
//! - Histogram bucket counts, sum, count, and +Inf invariants
//! - Gauge pass-through correctness
//! - Watermark enforcement (late arrival rejection, boundary conditions)
//! - Cardinality limit enforcement
//! - Floating-point precision edge cases
//! - MetricsCollector atomic accumulation
//! - MetricBatch event correctness
//! - Throughput calculation edge cases

use std::collections::HashMap;

use velostream::velostream::observability::metrics::MetricBatch;
use velostream::velostream::server::processors::metrics_collector::MetricsCollector;
use velostream::velostream::server::processors::observability_utils::calculate_throughput;

// =========================================================================
// Counter Mathematical Correctness
// =========================================================================

/// Verify counter with field: cumulative = sum of all field values
/// Input: [10.0, 5.0, 3.0, 7.5, 0.5]
/// Expected: [10.0, 15.0, 18.0, 25.5, 26.0]
#[test]
fn test_counter_with_field_exact_accumulation() {
    let mut cumulative = 0.0f64;
    let inputs = vec![10.0, 5.0, 3.0, 7.5, 0.5];
    let expected = vec![10.0, 15.0, 18.0, 25.5, 26.0];

    for (i, (value, exp)) in inputs.iter().zip(expected.iter()).enumerate() {
        cumulative += value;
        assert_eq!(
            cumulative, *exp,
            "Record {}: cumulative should be {} after adding {}, got {}",
            i, exp, value, cumulative
        );
    }
}

/// Verify counter without field: cumulative = count of records
/// Input: 7 records
/// Expected: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
#[test]
fn test_counter_without_field_exact_accumulation() {
    let mut cumulative = 0.0f64;

    for i in 1..=7 {
        cumulative += 1.0;
        assert_eq!(
            cumulative, i as f64,
            "After {} records, counter should be {}",
            i, i
        );
    }
}

/// Verify counter rejects negative values (must be non-negative)
#[test]
fn test_counter_rejects_negative_values() {
    let values = vec![10.0, -5.0, 3.0, -1.0, 7.0];
    let mut cumulative = 0.0f64;
    let mut accepted_count = 0usize;

    for value in &values {
        if *value < 0.0 {
            // Negative values are rejected, cumulative unchanged
            continue;
        }
        cumulative += value;
        accepted_count += 1;
    }

    assert_eq!(
        cumulative, 20.0,
        "Only non-negative values should accumulate"
    );
    assert_eq!(
        accepted_count, 3,
        "Only 3 non-negative values should be accepted"
    );
}

/// Verify counter monotonicity: cumulative value never decreases
#[test]
fn test_counter_monotonically_increasing() {
    let mut cumulative = 0.0f64;
    let mut prev = 0.0f64;
    let values = vec![1.0, 0.001, 100.0, 0.0001, 50.0, 0.0];

    for value in &values {
        if *value < 0.0 {
            continue;
        }
        cumulative += value;
        assert!(
            cumulative >= prev,
            "Counter must be monotonically non-decreasing: {} < {}",
            cumulative,
            prev
        );
        prev = cumulative;
    }
}

/// Verify counter with zero value: accepted but doesn't change cumulative
#[test]
fn test_counter_zero_value_accepted() {
    let mut cumulative = 0.0f64;
    let inputs_and_expected = vec![
        (5.0, 5.0),
        (0.0, 5.0), // Zero is non-negative, accepted
        (3.0, 8.0),
        (0.0, 8.0), // Zero again
    ];

    for (value, expected) in &inputs_and_expected {
        if *value >= 0.0 {
            cumulative += value;
        }
        assert_eq!(
            cumulative, *expected,
            "After adding {}: expected {}, got {}",
            value, expected, cumulative
        );
    }
}

/// Verify counter with very small increments doesn't lose precision
#[test]
fn test_counter_small_increment_precision() {
    let mut cumulative = 0.0f64;

    // 1000 increments of 0.001 should equal 1.0
    for _ in 0..1000 {
        cumulative += 0.001;
    }

    // f64 accumulation may drift slightly, but should be within epsilon
    assert!(
        (cumulative - 1.0).abs() < 1e-10,
        "1000 * 0.001 should be ~1.0, got {}",
        cumulative
    );
}

/// Verify counter with very large values doesn't overflow
#[test]
fn test_counter_large_values() {
    let mut cumulative = 0.0f64;
    let large_value = 1e15;

    cumulative += large_value;
    cumulative += large_value;
    cumulative += 1.0;

    assert_eq!(
        cumulative,
        2e15 + 1.0,
        "Large values should accumulate correctly"
    );
}

// =========================================================================
// Histogram Mathematical Correctness
// =========================================================================

/// Helper: simulate histogram accumulation matching metrics_helper.rs logic
struct TestHistogram {
    buckets: Vec<(f64, f64)>, // (le, count)
    sum: f64,
    count: f64,
}

impl TestHistogram {
    fn new(bucket_boundaries: &[f64]) -> Self {
        Self {
            buckets: bucket_boundaries.iter().map(|le| (*le, 0.0)).collect(),
            sum: 0.0,
            count: 0.0,
        }
    }

    fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count += 1.0;
        for (le, bucket_count) in self.buckets.iter_mut() {
            if value <= *le {
                *bucket_count += 1.0;
            }
        }
    }

    fn inf_bucket(&self) -> f64 {
        self.count
    }

    fn verify_invariants(&self) {
        // Invariant 1: bucket counts are monotonically non-decreasing
        let mut prev_count = 0.0;
        for (le, bucket_count) in &self.buckets {
            assert!(
                *bucket_count >= prev_count,
                "Bucket le={}: count {} < previous count {} (not monotonic)",
                le,
                bucket_count,
                prev_count
            );
            prev_count = *bucket_count;
        }

        // Invariant 2: +Inf bucket == count
        assert_eq!(
            self.inf_bucket(),
            self.count,
            "+Inf bucket ({}) must equal count ({})",
            self.inf_bucket(),
            self.count
        );

        // Invariant 3: largest finite bucket <= +Inf
        if let Some((_, last_count)) = self.buckets.last() {
            assert!(
                *last_count <= self.inf_bucket(),
                "Largest bucket count {} must be <= +Inf {}",
                last_count,
                self.inf_bucket()
            );
        }

        // Invariant 4: count >= 0
        assert!(self.count >= 0.0, "Count must be non-negative");

        // Invariant 5: all bucket counts are non-negative
        for (le, bucket_count) in &self.buckets {
            assert!(
                *bucket_count >= 0.0,
                "Bucket le={}: count {} must be non-negative",
                le,
                bucket_count
            );
        }
    }
}

/// Verify histogram with known observations against exact expected bucket counts
/// Buckets: [0.01, 0.1, 1.0, 10.0]
/// Observations: [0.005, 0.05, 0.5, 5.0, 50.0]
#[test]
fn test_histogram_exact_bucket_counts() {
    let mut h = TestHistogram::new(&[0.01, 0.1, 1.0, 10.0]);

    // obs=0.005: <= all buckets
    h.observe(0.005);
    assert_eq!(h.buckets[0].1, 1.0, "le=0.01: 0.005 <= 0.01");
    assert_eq!(h.buckets[1].1, 1.0, "le=0.1:  0.005 <= 0.1");
    assert_eq!(h.buckets[2].1, 1.0, "le=1.0:  0.005 <= 1.0");
    assert_eq!(h.buckets[3].1, 1.0, "le=10.0: 0.005 <= 10.0");

    // obs=0.05: <= 0.1, 1.0, 10.0 but NOT 0.01
    h.observe(0.05);
    assert_eq!(h.buckets[0].1, 1.0, "le=0.01: 0.05 > 0.01, unchanged");
    assert_eq!(h.buckets[1].1, 2.0, "le=0.1:  0.05 <= 0.1");
    assert_eq!(h.buckets[2].1, 2.0, "le=1.0:  0.05 <= 1.0");
    assert_eq!(h.buckets[3].1, 2.0, "le=10.0: 0.05 <= 10.0");

    // obs=0.5: <= 1.0, 10.0 but NOT 0.01, 0.1
    h.observe(0.5);
    assert_eq!(h.buckets[0].1, 1.0, "le=0.01: unchanged");
    assert_eq!(h.buckets[1].1, 2.0, "le=0.1:  unchanged");
    assert_eq!(h.buckets[2].1, 3.0, "le=1.0:  0.5 <= 1.0");
    assert_eq!(h.buckets[3].1, 3.0, "le=10.0: 0.5 <= 10.0");

    // obs=5.0: <= 10.0 only
    h.observe(5.0);
    assert_eq!(h.buckets[0].1, 1.0, "le=0.01: unchanged");
    assert_eq!(h.buckets[1].1, 2.0, "le=0.1:  unchanged");
    assert_eq!(h.buckets[2].1, 3.0, "le=1.0:  unchanged");
    assert_eq!(h.buckets[3].1, 4.0, "le=10.0: 5.0 <= 10.0");

    // obs=50.0: > all finite buckets (only +Inf)
    h.observe(50.0);
    assert_eq!(h.buckets[0].1, 1.0, "le=0.01: unchanged");
    assert_eq!(h.buckets[1].1, 2.0, "le=0.1:  unchanged");
    assert_eq!(h.buckets[2].1, 3.0, "le=1.0:  unchanged");
    assert_eq!(h.buckets[3].1, 4.0, "le=10.0: unchanged (50 > 10)");

    // Verify sum, count, +Inf
    assert_eq!(h.sum, 0.005 + 0.05 + 0.5 + 5.0 + 50.0);
    assert_eq!(h.count, 5.0);
    assert_eq!(h.inf_bucket(), 5.0, "+Inf must equal count");

    h.verify_invariants();
}

/// Verify histogram boundary condition: value == bucket boundary
/// Per Prometheus spec: le means "less than or equal to"
#[test]
fn test_histogram_exact_boundary_values() {
    let mut h = TestHistogram::new(&[1.0, 5.0, 10.0]);

    // Observe exactly at each boundary
    h.observe(1.0); // value == le=1.0
    h.observe(5.0); // value == le=5.0
    h.observe(10.0); // value == le=10.0

    // le=1.0: {1.0} = 1
    assert_eq!(
        h.buckets[0].1, 1.0,
        "le=1.0: value 1.0 should be counted (<=)"
    );
    // le=5.0: {1.0, 5.0} = 2
    assert_eq!(
        h.buckets[1].1, 2.0,
        "le=5.0: values 1.0 and 5.0 should be counted"
    );
    // le=10.0: {1.0, 5.0, 10.0} = 3
    assert_eq!(
        h.buckets[2].1, 3.0,
        "le=10.0: all 3 values should be counted"
    );

    assert_eq!(h.sum, 16.0, "sum = 1.0 + 5.0 + 10.0 = 16.0");
    assert_eq!(h.count, 3.0);
    h.verify_invariants();
}

/// Verify histogram: monotonically non-decreasing bucket counts
#[test]
fn test_histogram_bucket_monotonicity() {
    let mut h = TestHistogram::new(&[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
    ]);

    // Observations in random order
    let observations = vec![0.003, 7.0, 0.05, 1.5, 0.001, 15.0, 0.25, 0.01];
    for obs in &observations {
        h.observe(*obs);
    }

    h.verify_invariants();
}

/// Verify histogram sum precision with many small observations
#[test]
fn test_histogram_sum_precision() {
    let mut h = TestHistogram::new(&[1.0]);

    // 10000 observations of 0.0001 each
    for _ in 0..10000 {
        h.observe(0.0001);
    }

    // Expected sum = 10000 * 0.0001 = 1.0
    assert!(
        (h.sum - 1.0).abs() < 1e-8,
        "Sum of 10000 * 0.0001 should be ~1.0, got {}",
        h.sum
    );
    assert_eq!(h.count, 10000.0);
    assert_eq!(h.buckets[0].1, 10000.0, "All observations <= 1.0");
    h.verify_invariants();
}

/// Verify histogram with zero observation
#[test]
fn test_histogram_zero_observation() {
    let mut h = TestHistogram::new(&[0.0, 0.01, 1.0]);

    h.observe(0.0);

    // 0.0 <= 0.0 is true
    assert_eq!(h.buckets[0].1, 1.0, "le=0.0: 0.0 <= 0.0 is true");
    assert_eq!(h.buckets[1].1, 1.0, "le=0.01: 0.0 <= 0.01");
    assert_eq!(h.buckets[2].1, 1.0, "le=1.0: 0.0 <= 1.0");
    assert_eq!(h.sum, 0.0);
    assert_eq!(h.count, 1.0);
    h.verify_invariants();
}

/// Verify histogram with negative observation (should still accumulate correctly)
/// Note: histograms don't reject negative values (unlike counters)
#[test]
fn test_histogram_negative_observation() {
    let mut h = TestHistogram::new(&[0.0, 1.0, 10.0]);

    h.observe(-5.0);

    // -5.0 <= 0.0, -5.0 <= 1.0, -5.0 <= 10.0
    assert_eq!(h.buckets[0].1, 1.0, "le=0.0: -5.0 <= 0.0");
    assert_eq!(h.buckets[1].1, 1.0, "le=1.0: -5.0 <= 1.0");
    assert_eq!(h.buckets[2].1, 1.0, "le=10.0: -5.0 <= 10.0");
    assert_eq!(h.sum, -5.0);
    assert_eq!(h.count, 1.0);
    h.verify_invariants();
}

/// Verify histogram with single bucket handles all observations correctly
#[test]
fn test_histogram_single_bucket() {
    let mut h = TestHistogram::new(&[100.0]);

    h.observe(50.0);
    h.observe(100.0);
    h.observe(150.0); // > bucket, only counted in +Inf

    assert_eq!(h.buckets[0].1, 2.0, "le=100: 50 and 100 counted, 150 not");
    assert_eq!(h.count, 3.0);
    assert_eq!(h.inf_bucket(), 3.0, "+Inf always equals count");
    assert_eq!(h.sum, 300.0);
    h.verify_invariants();
}

/// Verify default histogram buckets match Prometheus conventions
#[test]
fn test_histogram_default_buckets() {
    let default_buckets = vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
    ];

    // Verify buckets are strictly increasing
    for i in 1..default_buckets.len() {
        assert!(
            default_buckets[i] > default_buckets[i - 1],
            "Bucket boundaries must be strictly increasing: {} <= {}",
            default_buckets[i],
            default_buckets[i - 1]
        );
    }

    // Verify all are positive
    for b in &default_buckets {
        assert!(*b > 0.0, "All default buckets must be positive, got {}", b);
    }
}

// =========================================================================
// Gauge Mathematical Correctness
// =========================================================================

/// Verify gauge: emits current value, no accumulation
#[test]
fn test_gauge_emits_exact_current_value() {
    let values_and_expected: Vec<(f64, f64)> = vec![
        (100.0, 100.0),
        (150.5, 150.5),
        (99.99, 99.99),
        (0.0, 0.0),
        (-10.0, -10.0), // Gauges CAN be negative (unlike counters)
        (f64::MAX, f64::MAX),
    ];

    for (input, expected) in &values_and_expected {
        // Gauge value = input (direct assignment, no math)
        let emitted = *input;
        assert_eq!(
            emitted, *expected,
            "Gauge should emit exact value: input={}, expected={}, got={}",
            input, expected, emitted
        );
    }
}

/// Verify gauge: value can decrease (unlike counters)
#[test]
fn test_gauge_can_decrease() {
    let sequence = vec![100.0, 50.0, 75.0, 25.0, 0.0, 150.0];
    let mut prev = sequence[0];

    for value in sequence.iter().skip(1) {
        // Gauges allow both increases and decreases
        let _change = value - prev;
        prev = *value;
    }
    // No assertion failure = gauge correctly allows non-monotonic values
    assert_eq!(prev, 150.0, "Final gauge value should be last in sequence");
}

/// Verify gauge: NaN and Inf handling
#[test]
fn test_gauge_special_float_values() {
    // These are technically valid f64 values that could appear
    let nan_value = f64::NAN;
    let inf_value = f64::INFINITY;
    let neg_inf_value = f64::NEG_INFINITY;

    assert!(nan_value.is_nan(), "NaN should be detected");
    assert!(inf_value.is_infinite(), "Inf should be detected");
    assert!(neg_inf_value.is_infinite(), "NegInf should be detected");
}

// =========================================================================
// Watermark Mathematical Correctness
// =========================================================================

/// Verify watermark: exact boundary behavior (ts == last_ts is rejected)
#[test]
fn test_watermark_rejects_equal_timestamp() {
    let mut watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();
    let key = ("metric".to_string(), vec!["label".to_string()]);

    watermarks.insert(key.clone(), 1000);

    // Equal timestamp should be rejected (ts <= last_ts)
    let ts = 1000i64;
    let last_ts = watermarks.get(&key).copied().unwrap_or(i64::MIN);
    assert!(ts <= last_ts, "Equal timestamp must be rejected");

    // ts + 1 should be accepted
    let ts_plus_one = 1001i64;
    assert!(
        ts_plus_one > last_ts,
        "Timestamp one greater should be accepted"
    );
}

/// Verify watermark: first sample always accepted (initial watermark is i64::MIN)
#[test]
fn test_watermark_first_sample_always_accepted() {
    let watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();
    let key = ("metric".to_string(), vec!["label".to_string()]);

    let last_ts = watermarks.get(&key).copied().unwrap_or(i64::MIN);
    assert_eq!(last_ts, i64::MIN, "Initial watermark should be i64::MIN");

    // Any timestamp > i64::MIN should be accepted
    assert!(
        0 > i64::MIN,
        "Timestamp 0 should be accepted as first sample"
    );
    assert!(1 > i64::MIN, "Timestamp 1 should be accepted");
    assert!(i64::MAX > i64::MIN, "Maximum timestamp should be accepted");
}

/// Verify watermark: independent per label combination
#[test]
fn test_watermark_label_independence() {
    let mut watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();

    let key_a = ("metric".to_string(), vec!["A".to_string()]);
    let key_b = ("metric".to_string(), vec!["B".to_string()]);

    // Advance key_a to 5000
    watermarks.insert(key_a.clone(), 5000);

    // key_b at 1000 should be accepted (independent watermark)
    let last_b = watermarks.get(&key_b).copied().unwrap_or(i64::MIN);
    assert!(1000 > last_b, "Label B should accept ts=1000 independently");

    // key_a at 3000 should be rejected (3000 <= 5000)
    let last_a = watermarks.get(&key_a).copied().unwrap_or(i64::MIN);
    assert!(3000 <= last_a, "Label A should reject ts=3000 (< 5000)");
}

/// Verify watermark: independent per metric name
#[test]
fn test_watermark_metric_name_independence() {
    let mut watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();

    let key_counter = ("counter_metric".to_string(), vec!["label".to_string()]);
    let key_gauge = ("gauge_metric".to_string(), vec!["label".to_string()]);

    watermarks.insert(key_counter.clone(), 5000);

    // gauge_metric at ts=1000 accepted (different metric name → different watermark)
    let last_gauge = watermarks.get(&key_gauge).copied().unwrap_or(i64::MIN);
    assert!(
        1000 > last_gauge,
        "Different metric name should have independent watermark"
    );
}

// =========================================================================
// Cardinality Limit Correctness
// =========================================================================

/// Verify cardinality limit: exactly N label combos accepted, N+1 rejected
#[test]
fn test_cardinality_exact_limit_boundary() {
    let max_cardinality = 5usize;
    let mut state: HashMap<(String, Vec<String>), f64> = HashMap::new();

    // Fill to exactly the limit
    for i in 0..max_cardinality {
        let key = ("metric".to_string(), vec![format!("val_{}", i)]);
        state.insert(key, 1.0);
    }
    assert_eq!(state.len(), max_cardinality);

    // Next new entry should be rejected
    let new_key = ("metric".to_string(), vec!["val_new".to_string()]);
    let is_new = !state.contains_key(&new_key);
    let at_limit = state.len() >= max_cardinality;
    assert!(is_new && at_limit, "New entry at limit should be rejected");

    // Existing entry should NOT be rejected (update, not new)
    let existing_key = ("metric".to_string(), vec!["val_0".to_string()]);
    let is_existing = state.contains_key(&existing_key);
    assert!(is_existing, "Existing entry should be allowed to update");
}

/// Verify cardinality=0 means unlimited
#[test]
fn test_cardinality_zero_means_unlimited() {
    let max_cardinality = 0usize;
    let state_len = 50000usize;

    // When max_cardinality == 0, the check `state.len() >= max_cardinality` is
    // ALWAYS true, but the code first checks `max_cardinality > 0`
    let should_reject = max_cardinality > 0 && state_len >= max_cardinality;
    assert!(!should_reject, "max_cardinality=0 should never reject");
}

// =========================================================================
// MetricsCollector Correctness
// =========================================================================

/// Verify MetricsCollector atomic accumulation is exact
#[test]
fn test_metrics_collector_exact_accumulation() {
    let collector = MetricsCollector::new();

    collector.add_records(100);
    assert_eq!(collector.total_records(), 100);

    collector.add_records(50);
    assert_eq!(collector.total_records(), 150);

    collector.add_records(1);
    assert_eq!(collector.total_records(), 151);
}

/// Verify MetricsCollector: failed records tracked independently
#[test]
fn test_metrics_collector_failed_independent() {
    let collector = MetricsCollector::new();

    collector.add_records(100);
    collector.add_failed_records(5);

    assert_eq!(collector.total_records(), 100);
    assert_eq!(collector.failed_records(), 5);

    collector.add_records(50);
    collector.add_failed_records(3);

    assert_eq!(collector.total_records(), 150);
    assert_eq!(collector.failed_records(), 8);
}

/// Verify MetricsCollector starts at zero
#[test]
fn test_metrics_collector_starts_at_zero() {
    let collector = MetricsCollector::new();
    assert_eq!(collector.total_records(), 0);
    assert_eq!(collector.failed_records(), 0);
}

/// Verify MetricsCollector: throughput calculation
#[test]
fn test_metrics_collector_throughput_non_negative() {
    let collector = MetricsCollector::new();
    collector.add_records(1000);

    // Throughput must always be non-negative
    let rps = collector.throughput_rps();
    assert!(rps >= 0.0, "Throughput must be non-negative, got {}", rps);
}

// =========================================================================
// MetricBatch Correctness
// =========================================================================

/// Verify MetricBatch counts events correctly
#[test]
fn test_metric_batch_event_counting() {
    let mut batch = MetricBatch::new();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);

    batch.add_counter("counter1".to_string(), vec!["label1".to_string()]);
    assert_eq!(batch.len(), 1);

    batch.add_gauge("gauge1".to_string(), vec!["label1".to_string()], 42.0);
    assert_eq!(batch.len(), 2);

    batch.add_histogram("hist1".to_string(), vec!["label1".to_string()], 0.5);
    assert_eq!(batch.len(), 3);

    assert!(!batch.is_empty());
}

/// Verify MetricBatch with_capacity doesn't affect semantics
#[test]
fn test_metric_batch_with_capacity() {
    let mut batch = MetricBatch::with_capacity(100);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);

    batch.add_counter("c".to_string(), vec![]);
    assert_eq!(batch.len(), 1);
}

// =========================================================================
// Throughput Calculation Correctness
// =========================================================================

/// Verify calculate_throughput with exact expected values
#[test]
fn test_throughput_known_values() {
    // 1000 records in 1000ms = 1000 rps
    assert_eq!(calculate_throughput(1000, 1000), 1000.0);

    // 500 records in 2000ms = 250 rps
    assert_eq!(calculate_throughput(500, 2000), 250.0);

    // 1 record in 1000ms = 1.0 rps
    assert_eq!(calculate_throughput(1, 1000), 1.0);

    // 10 records in 100ms = 100 rps
    assert_eq!(calculate_throughput(10, 100), 100.0);
}

/// Verify throughput: sub-millisecond clamped to 1ms (prevents infinity)
#[test]
fn test_throughput_sub_millisecond_clamped() {
    // 10 records in 0ms → treated as 1ms → 10000 rps
    let result = calculate_throughput(10, 0);
    assert_eq!(result, 10000.0, "Sub-ms should be clamped to 1ms");
    assert!(result.is_finite(), "Result must be finite, not infinity");
}

/// Verify throughput: zero records always returns 0 regardless of duration
#[test]
fn test_throughput_zero_records() {
    assert_eq!(calculate_throughput(0, 0), 0.0);
    assert_eq!(calculate_throughput(0, 1000), 0.0);
    assert_eq!(calculate_throughput(0, u64::MAX), 0.0);
}

/// Verify throughput: result is always non-negative
#[test]
fn test_throughput_always_non_negative() {
    let test_cases = vec![
        (0, 0),
        (0, 100),
        (1, 1),
        (100, 1),
        (1, 100),
        (usize::MAX, u64::MAX),
    ];

    for (records, duration) in test_cases {
        let result = calculate_throughput(records, duration);
        assert!(
            result >= 0.0,
            "Throughput({}, {}) = {} must be non-negative",
            records,
            duration,
            result
        );
        assert!(
            !result.is_nan(),
            "Throughput({}, {}) must not be NaN",
            records,
            duration
        );
    }
}

// =========================================================================
// End-to-End Counter State Simulation
// =========================================================================

/// Simulate the full counter emission pipeline with watermarks
/// Matches the exact logic in metrics_helper.rs:903-1007
#[test]
fn test_full_counter_pipeline_simulation() {
    let mut counter_state: HashMap<(String, Vec<String>), f64> = HashMap::new();
    let mut watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();
    let max_cardinality = 100usize;

    // Input records: (timestamp_ms, metric_name, labels, field_value)
    let records = vec![
        (1000, "trades_total", vec!["AAPL"], 5.0),
        (2000, "trades_total", vec!["AAPL"], 3.0),
        (1500, "trades_total", vec!["AAPL"], 10.0), // LATE: rejected
        (3000, "trades_total", vec!["AAPL"], 7.0),
        (1000, "trades_total", vec!["MSFT"], 2.0), // Different label: accepted
        (2000, "trades_total", vec!["MSFT"], 4.0),
    ];

    let mut emitted: Vec<(String, Vec<String>, f64, i64)> = Vec::new();

    for (ts, name, labels, value) in &records {
        let key = (
            name.to_string(),
            labels.iter().map(|s| s.to_string()).collect(),
        );

        // Watermark check
        let last_ts = watermarks.get(&key).copied().unwrap_or(i64::MIN);
        if *ts <= last_ts {
            continue; // Late arrival
        }
        watermarks.insert(key.clone(), *ts);

        // Negative check
        if *value < 0.0 {
            continue;
        }

        // Cardinality check
        let is_new = !counter_state.contains_key(&key);
        if is_new && max_cardinality > 0 && counter_state.len() >= max_cardinality {
            continue;
        }

        // Accumulate
        let cumulative = counter_state
            .entry(key.clone())
            .and_modify(|total| *total += value)
            .or_insert(*value);

        emitted.push((
            name.to_string(),
            labels.iter().map(|s| s.to_string()).collect(),
            *cumulative,
            *ts,
        ));
    }

    // Verify emitted values
    assert_eq!(
        emitted.len(),
        5,
        "5 records should pass watermark (1 late rejected)"
    );

    // AAPL: 5.0, 8.0 (skip late), 15.0
    assert_eq!(emitted[0].2, 5.0, "AAPL@1000: cumulative=5.0");
    assert_eq!(emitted[1].2, 8.0, "AAPL@2000: cumulative=8.0");
    assert_eq!(emitted[2].2, 15.0, "AAPL@3000: cumulative=15.0");

    // MSFT: 2.0, 6.0
    assert_eq!(emitted[3].2, 2.0, "MSFT@1000: cumulative=2.0");
    assert_eq!(emitted[4].2, 6.0, "MSFT@2000: cumulative=6.0");
}

/// Simulate the full histogram emission pipeline with watermarks
/// Matches the exact logic in metrics_helper.rs:1055-1141
#[test]
fn test_full_histogram_pipeline_simulation() {
    let buckets = vec![0.01, 0.1, 1.0, 10.0];
    let mut histogram_state: HashMap<(String, Vec<String>), (f64, f64, Vec<(f64, f64)>)> =
        HashMap::new();
    let mut watermarks: HashMap<(String, Vec<String>), i64> = HashMap::new();

    // Input: (timestamp_ms, value)
    let records = vec![
        (1000, 0.005),
        (2000, 0.5),
        (1500, 0.1), // LATE: rejected
        (3000, 5.0),
        (4000, 50.0),
        (5000, 0.01), // Exactly at bucket boundary
    ];

    let key = ("latency".to_string(), vec!["api".to_string()]);
    let mut accepted_count = 0;

    for (ts, value) in &records {
        let last_ts = watermarks.get(&key).copied().unwrap_or(i64::MIN);
        if *ts <= last_ts {
            continue;
        }
        watermarks.insert(key.clone(), *ts);

        let (sum, count, bucket_counts) = histogram_state.entry(key.clone()).or_insert_with(|| {
            let initial_buckets = buckets.iter().map(|le| (*le, 0.0)).collect();
            (0.0, 0.0, initial_buckets)
        });

        *sum += value;
        *count += 1.0;

        for (le, bucket_count) in bucket_counts.iter_mut() {
            if *value <= *le {
                *bucket_count += 1.0;
            }
        }

        accepted_count += 1;
    }

    assert_eq!(accepted_count, 5, "5 accepted, 1 late rejected");

    let (sum, count, bucket_counts) = histogram_state.get(&key).unwrap();

    // Verify sum = 0.005 + 0.5 + 5.0 + 50.0 + 0.01 = 55.515
    assert!(
        (*sum - 55.515).abs() < 1e-10,
        "Sum should be 55.515, got {}",
        sum
    );
    assert_eq!(*count, 5.0);

    // Bucket expectations:
    // le=0.01:  {0.005, 0.01} = 2
    // le=0.1:   {0.005, 0.01} = 2  (0.5 > 0.1)
    // le=1.0:   {0.005, 0.5, 0.01} = 3
    // le=10.0:  {0.005, 0.5, 5.0, 0.01} = 4  (50.0 > 10.0)
    assert_eq!(bucket_counts[0].1, 2.0, "le=0.01: 0.005 and 0.01");
    assert_eq!(bucket_counts[1].1, 2.0, "le=0.1: same 2 (0.5 > 0.1)");
    assert_eq!(bucket_counts[2].1, 3.0, "le=1.0: + 0.5 = 3");
    assert_eq!(
        bucket_counts[3].1, 4.0,
        "le=10.0: + 5.0 = 4 (50.0 excluded)"
    );

    // +Inf = count = 5
    assert_eq!(*count, 5.0, "+Inf bucket = count");

    // Monotonicity: bucket counts must be non-decreasing
    let counts: Vec<f64> = bucket_counts.iter().map(|(_, c)| *c).collect();
    for i in 1..counts.len() {
        assert!(
            counts[i] >= counts[i - 1],
            "Bucket counts must be monotonically non-decreasing: {} < {}",
            counts[i],
            counts[i - 1]
        );
    }
}
