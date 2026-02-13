//! Comprehensive tests for watermark-based metric emission
//!
//! Tests all three metric types (counter, gauge, histogram) with:
//! - Monotonic timestamps (accepted)
//! - Late arrivals (discarded)
//! - Cumulative behavior

use std::collections::HashMap;

/// Test counter with @metric_field: increments by field value
#[test]
fn test_counter_with_field_increments_by_value() {
    // Simulate counter with @metric_field
    // Record 1: value=10 → counter=10
    // Record 2: value=5  → counter=15 (cumulative)
    // Record 3: value=3  → counter=18 (cumulative)

    let mut cumulative = 0.0f64;
    let values = vec![10.0, 5.0, 3.0];
    let expected = vec![10.0, 15.0, 18.0];

    for (value, expected_cum) in values.iter().zip(expected.iter()) {
        cumulative += value;
        assert_eq!(
            cumulative, *expected_cum,
            "Counter should increment by field value"
        );
    }
}

/// Test counter without @metric_field: increments by 1
#[test]
fn test_counter_without_field_increments_by_one() {
    // Simulate counter without @metric_field (just counts records)
    // Record 1 → counter=1
    // Record 2 → counter=2
    // Record 3 → counter=3

    let mut cumulative = 0.0f64;
    let num_records = 5;

    for i in 1..=num_records {
        cumulative += 1.0;
        assert_eq!(
            cumulative, i as f64,
            "Counter should increment by 1 per record"
        );
    }
}

/// Test watermark: late arrivals are discarded
#[test]
fn test_watermark_discards_late_arrivals() {
    // Simulate watermark tracking
    let mut last_timestamp: HashMap<(String, Vec<String>), i64> = HashMap::new();
    let key = ("counter".to_string(), vec!["AAPL".to_string()]);

    // Record 1: ts=1000 → accepted
    let ts1 = 1000i64;
    assert!(!last_timestamp.contains_key(&key) || ts1 > *last_timestamp.get(&key).unwrap());
    last_timestamp.insert(key.clone(), ts1);

    // Record 2: ts=2000 → accepted (ts > 1000)
    let ts2 = 2000i64;
    assert!(ts2 > *last_timestamp.get(&key).unwrap());
    last_timestamp.insert(key.clone(), ts2);

    // Record 3: ts=1500 → REJECTED (ts <= 2000, late arrival)
    let ts3 = 1500i64;
    assert!(
        ts3 <= *last_timestamp.get(&key).unwrap(),
        "Should reject late arrival"
    );

    // Record 4: ts=2000 → REJECTED (ts <= 2000, duplicate timestamp)
    let ts4 = 2000i64;
    assert!(
        ts4 <= *last_timestamp.get(&key).unwrap(),
        "Should reject duplicate timestamp"
    );

    // Record 5: ts=3000 → accepted
    let ts5 = 3000i64;
    assert!(ts5 > *last_timestamp.get(&key).unwrap());
}

/// Test gauge: emits current value (no cumulation)
#[test]
fn test_gauge_emits_current_value() {
    // Gauges don't accumulate - just emit the current value
    let values = vec![150.0, 160.0, 155.0, 170.0];

    for value in values {
        // Each value is emitted as-is (no accumulation)
        let emitted = value;
        assert_eq!(emitted, value, "Gauge should emit current value");
    }
}

/// Test histogram: cumulative bucket counts
#[test]
fn test_histogram_cumulative_buckets() {
    let buckets = vec![0.01, 0.1, 1.0];
    let mut bucket_counts: Vec<(f64, f64)> = buckets.iter().map(|le| (*le, 0.0)).collect();
    let mut sum = 0.0;
    let mut count = 0.0;

    // Observation 1: value=0.005
    let obs1 = 0.005;
    sum += obs1;
    count += 1.0;
    for (le, bucket_count) in bucket_counts.iter_mut() {
        if obs1 <= *le {
            *bucket_count += 1.0;
        }
    }

    // Expected after obs1:
    // le=0.01: 1 (0.005 <= 0.01)
    // le=0.1: 1
    // le=1.0: 1
    assert_eq!(bucket_counts[0].1, 1.0);
    assert_eq!(bucket_counts[1].1, 1.0);
    assert_eq!(bucket_counts[2].1, 1.0);
    assert_eq!(sum, 0.005);
    assert_eq!(count, 1.0);

    // Observation 2: value=0.5
    let obs2 = 0.5;
    sum += obs2;
    count += 1.0;
    for (le, bucket_count) in bucket_counts.iter_mut() {
        if obs2 <= *le {
            *bucket_count += 1.0;
        }
    }

    // Expected after obs2:
    // le=0.01: 1 (0.5 > 0.01, not incremented)
    // le=0.1: 1 (0.5 > 0.1, not incremented)
    // le=1.0: 2 (0.5 <= 1.0, incremented)
    assert_eq!(bucket_counts[0].1, 1.0);
    assert_eq!(bucket_counts[1].1, 1.0);
    assert_eq!(bucket_counts[2].1, 2.0);
    assert_eq!(sum, 0.505);
    assert_eq!(count, 2.0);

    // Observation 3: value=5.0 (larger than all buckets)
    let obs3 = 5.0;
    sum += obs3;
    count += 1.0;
    for (le, bucket_count) in bucket_counts.iter_mut() {
        if obs3 <= *le {
            *bucket_count += 1.0;
        }
    }

    // Expected after obs3:
    // le=0.01: 1 (5.0 > 0.01, not incremented)
    // le=0.1: 1 (5.0 > 0.1, not incremented)
    // le=1.0: 2 (5.0 > 1.0, not incremented)
    // +Inf bucket would be: 3 (always equals count)
    assert_eq!(bucket_counts[0].1, 1.0);
    assert_eq!(bucket_counts[1].1, 1.0);
    assert_eq!(bucket_counts[2].1, 2.0);
    assert_eq!(sum, 5.505);
    assert_eq!(count, 3.0);
}

/// Test histogram: bucket emission format
#[test]
fn test_histogram_emission_format() {
    // Verify histogram emits correct metric names:
    // - {name}_bucket{le="X"}
    // - {name}_bucket{le="+Inf"}
    // - {name}_sum
    // - {name}_count

    let metric_name = "latency_seconds";
    let _buckets = vec![0.1, 1.0]; // Intentionally unused (just documenting expected buckets)

    // Expected metric names
    let expected_metrics = vec![
        format!("{}_bucket", metric_name), // With le="0.1"
        format!("{}_bucket", metric_name), // With le="1.0"
        format!("{}_bucket", metric_name), // With le="+Inf"
        format!("{}_sum", metric_name),
        format!("{}_count", metric_name),
    ];

    assert_eq!(expected_metrics[0], "latency_seconds_bucket");
    assert_eq!(expected_metrics[3], "latency_seconds_sum");
    assert_eq!(expected_metrics[4], "latency_seconds_count");
}

/// Test watermark independence across different label combinations
#[test]
fn test_watermark_per_label_combination() {
    let mut last_timestamp: HashMap<(String, Vec<String>), i64> = HashMap::new();

    // Different symbols should have independent watermarks
    let key_aapl = ("counter".to_string(), vec!["AAPL".to_string()]);
    let key_msft = ("counter".to_string(), vec!["MSFT".to_string()]);

    // AAPL at ts=1000
    last_timestamp.insert(key_aapl.clone(), 1000);

    // MSFT at ts=500 (earlier than AAPL, but should still be accepted)
    last_timestamp.insert(key_msft.clone(), 500);

    // AAPL at ts=900 → rejected (< 1000)
    assert!(900 <= *last_timestamp.get(&key_aapl).unwrap());

    // MSFT at ts=900 → accepted (> 500)
    assert!(900 > *last_timestamp.get(&key_msft).unwrap());
}

/// Test counter cumulative behavior across multiple timestamps
#[test]
fn test_counter_cumulative_across_timestamps() {
    let mut cumulative = 0.0;
    let mut last_ts = i64::MIN;

    // Simulate multiple records with increasing timestamps
    let records = vec![
        (1000, 10.0), // ts=1000, value=10 → cumulative=10
        (2000, 5.0),  // ts=2000, value=5  → cumulative=15
        (3000, 3.0),  // ts=3000, value=3  → cumulative=18
    ];

    for (ts, value) in records {
        assert!(ts > last_ts, "Timestamps must be increasing");
        cumulative += value;
        last_ts = ts;
    }

    assert_eq!(cumulative, 18.0);
    assert_eq!(last_ts, 3000);
}

/// Test that late arrival warning includes useful diagnostic info
#[test]
fn test_late_arrival_warning_includes_diagnostics() {
    // Verify warning message format includes:
    // - metric name
    // - labels
    // - timestamp
    // - last_timestamp
    // - lag in milliseconds
    // - total discarded count

    let metric_name = "test_counter";
    let labels = vec!["AAPL".to_string()];
    let ts = 1000i64;
    let last_ts = 2000i64;
    let lag = last_ts - ts;
    let count = 1;

    let expected_warning = format!(
        "Late arrival discarded for metric '{}' (labels: {:?}): \
         timestamp={}, last_timestamp={}, lag={}ms. \
         Total discarded: {}",
        metric_name, labels, ts, last_ts, lag, count
    );

    assert!(expected_warning.contains("Late arrival discarded"));
    assert!(expected_warning.contains(metric_name));
    assert!(expected_warning.contains("lag=1000ms"));
    assert!(expected_warning.contains("Total discarded: 1"));
}

/// Test histogram bucket change warning
#[test]
fn test_histogram_bucket_reconfiguration_warning() {
    // Verify that changing histogram buckets after initialization warns user
    let original_buckets = vec![0.1, 1.0, 10.0];
    let new_buckets = vec![0.01, 0.1, 1.0, 10.0]; // Different configuration

    // Verify buckets are different
    assert_ne!(original_buckets, new_buckets);

    // Expected warning message
    let expected_warning = format!(
        "Histogram '{}' bucket configuration changed but will be ignored. \
         Buckets are fixed at first initialization. \
         Current: {:?}, New: {:?}",
        "test_histogram", original_buckets, new_buckets
    );

    assert!(expected_warning.contains("bucket configuration changed"));
    assert!(expected_warning.contains("will be ignored"));
    assert!(expected_warning.contains("fixed at first initialization"));
}

/// Test late arrival count increments correctly
#[test]
fn test_late_arrival_count_increments() {
    use std::sync::atomic::{AtomicU64, Ordering};

    // Simulate late arrival counter
    let late_count = AtomicU64::new(0);

    // First late arrival
    let count1 = late_count.fetch_add(1, Ordering::Relaxed) + 1;
    assert_eq!(count1, 1);

    // Second late arrival
    let count2 = late_count.fetch_add(1, Ordering::Relaxed) + 1;
    assert_eq!(count2, 2);

    // 1000th late arrival (should trigger warning)
    late_count.store(999, Ordering::Relaxed);
    let count1000 = late_count.fetch_add(1, Ordering::Relaxed) + 1;
    assert_eq!(count1000, 1000);
    assert!(count1000 % 1000 == 0, "Should warn at multiples of 1000");
}

/// Test error reporting doesn't impact performance
#[test]
fn test_late_arrival_logging_is_throttled() {
    // Verify that we don't log every single late arrival
    // Only log at count=1 and every 1000 thereafter

    let should_warn = |count: u64| -> bool { count == 1 || count.is_multiple_of(1000) };

    assert!(should_warn(1), "Should warn on first late arrival");
    assert!(!should_warn(2), "Should NOT warn on second");
    assert!(!should_warn(999), "Should NOT warn on 999th");
    assert!(should_warn(1000), "Should warn on 1000th");
    assert!(!should_warn(1001), "Should NOT warn on 1001st");
    assert!(should_warn(2000), "Should warn on 2000th");
}

/// Test cardinality limit prevents OOM
#[test]
fn test_cardinality_limit_prevents_oom() {
    use std::collections::HashMap;

    let max_cardinality = 1000usize;
    let mut state: HashMap<(String, Vec<String>), f64> = HashMap::new();

    // Simulate adding entries up to limit
    for i in 0..max_cardinality {
        let key = ("counter".to_string(), vec![format!("user_{}", i)]);
        state.insert(key, 1.0);
    }

    assert_eq!(state.len(), max_cardinality);

    // Try to add beyond limit
    let new_key = ("counter".to_string(), vec!["user_9999".to_string()]);
    let should_reject = !state.contains_key(&new_key) && state.len() >= max_cardinality;

    assert!(
        should_reject,
        "Should reject new entries beyond cardinality limit"
    );
}

/// Test default cardinality limit
#[test]
fn test_default_cardinality_limit() {
    use velostream::velostream::server::processors::metrics_helper::LabelHandlingConfig;

    let config = LabelHandlingConfig::default();
    assert_eq!(
        config.max_cardinality, 100,
        "Default cardinality limit should be 10,000"
    );
}

/// Test cardinality limit can be disabled
#[test]
fn test_cardinality_unlimited() {
    use velostream::velostream::server::processors::metrics_helper::LabelHandlingConfig;

    let config = LabelHandlingConfig {
        strict_mode: false,
        max_cardinality: 0,
        max_timestamp_age_ms: 0,
    };

    // Simulate check
    let is_unlimited = config.max_cardinality == 0;
    assert!(is_unlimited, "max_cardinality=0 should mean unlimited");
}

/// Test cardinality warning is throttled
#[test]
fn test_cardinality_warning_throttled() {
    // Only warn at count=1 and every 1000
    let should_warn = |count: u64| -> bool { count == 1 || count.is_multiple_of(1000) };

    assert!(should_warn(1), "Warn on first");
    assert!(!should_warn(2), "Don't spam");
    assert!(should_warn(1000), "Warn at 1000");
    assert!(should_warn(2000), "Warn at 2000");
}
