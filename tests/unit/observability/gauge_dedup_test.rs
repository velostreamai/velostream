//! Tests for gauge metric deduplication in remote-write emission.
//!
//! When multiple output records share the same (metric_name, labels, timestamp),
//! Prometheus rejects duplicate samples.  The fix defers gauge emissions into a
//! HashMap keyed by (name, label_values, timestamp_ms), keeping the **last** value
//! per unique key.  This mirrors the deduplication logic in
//! `ProcessorMetricsHelper::emit_metrics_generic()`.

use std::collections::HashMap;

/// Simulates the gauge dedup pattern used in `emit_metrics_generic`.
///
/// The real code accumulates gauge samples into a HashMap during the record loop
/// and flushes them once before `flush_remote_write()`.  This test verifies that
/// the dedup logic itself (last-value-wins per unique key) works correctly.
fn gauge_dedup(
    samples: &[(String, Vec<String>, i64, f64)],
) -> Vec<(String, Vec<String>, i64, f64)> {
    let mut dedup: HashMap<(String, Vec<String>, i64), f64> = HashMap::new();

    for (name, labels, ts, value) in samples {
        let key = (name.clone(), labels.clone(), *ts);
        dedup.insert(key, *value);
    }

    let mut result: Vec<_> = dedup
        .into_iter()
        .map(|((name, labels, ts), value)| (name, labels, ts, value))
        .collect();

    // Sort for deterministic test assertions
    result.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
    result
}

/// Single sample passes through unchanged.
#[test]
fn test_gauge_dedup_single_sample() {
    let samples = vec![(
        "cpu_usage".to_string(),
        vec!["host-1".to_string()],
        1700000000000_i64,
        0.85,
    )];

    let result = gauge_dedup(&samples);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].3, 0.85);
}

/// Two samples with same key — last value wins.
#[test]
fn test_gauge_dedup_duplicate_same_timestamp() {
    let samples = vec![
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.85,
        ),
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.92,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        1,
        "Duplicates should be collapsed to one sample"
    );
    assert_eq!(
        result[0].3, 0.92,
        "Last value (0.92) should win over first (0.85)"
    );
}

/// Three duplicates — last value wins.
#[test]
fn test_gauge_dedup_triple_duplicate() {
    let samples = vec![
        (
            "mem_used".to_string(),
            vec!["node-a".to_string()],
            1700000000000_i64,
            1024.0,
        ),
        (
            "mem_used".to_string(),
            vec!["node-a".to_string()],
            1700000000000_i64,
            2048.0,
        ),
        (
            "mem_used".to_string(),
            vec!["node-a".to_string()],
            1700000000000_i64,
            3072.0,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].3, 3072.0, "Last of three should win");
}

/// Different timestamps are NOT deduped (they are distinct samples).
#[test]
fn test_gauge_dedup_different_timestamps_preserved() {
    let samples = vec![
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.85,
        ),
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000001000_i64,
            0.92,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        2,
        "Different timestamps should produce distinct samples"
    );
}

/// Different label values are NOT deduped.
#[test]
fn test_gauge_dedup_different_labels_preserved() {
    let samples = vec![
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.85,
        ),
        (
            "cpu_usage".to_string(),
            vec!["host-2".to_string()],
            1700000000000_i64,
            0.60,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        2,
        "Different labels should produce distinct samples"
    );
}

/// Different metric names are NOT deduped.
#[test]
fn test_gauge_dedup_different_metric_names_preserved() {
    let samples = vec![
        (
            "cpu_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.85,
        ),
        (
            "mem_usage".to_string(),
            vec!["host-1".to_string()],
            1700000000000_i64,
            0.60,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        2,
        "Different metric names should produce distinct samples"
    );
}

/// Mixed scenario: some duplicates, some unique.
#[test]
fn test_gauge_dedup_mixed_scenario() {
    let samples = vec![
        // Duplicate pair (same metric, labels, timestamp) — second wins
        (
            "price".to_string(),
            vec!["AAPL".to_string()],
            1700000000000_i64,
            150.0,
        ),
        (
            "price".to_string(),
            vec!["AAPL".to_string()],
            1700000000000_i64,
            151.5,
        ),
        // Unique sample (different symbol)
        (
            "price".to_string(),
            vec!["GOOG".to_string()],
            1700000000000_i64,
            2800.0,
        ),
        // Unique sample (different timestamp)
        (
            "price".to_string(),
            vec!["AAPL".to_string()],
            1700000001000_i64,
            152.0,
        ),
        // Another duplicate for GOOG
        (
            "price".to_string(),
            vec!["GOOG".to_string()],
            1700000000000_i64,
            2810.0,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        3,
        "Should have 3 unique samples: AAPL@t0, GOOG@t0, AAPL@t1"
    );

    // Find specific results
    let aapl_t0 = result
        .iter()
        .find(|s| s.1 == vec!["AAPL".to_string()] && s.2 == 1700000000000)
        .expect("Should have AAPL@t0");
    assert_eq!(aapl_t0.3, 151.5, "AAPL@t0 should be 151.5 (last value)");

    let goog_t0 = result
        .iter()
        .find(|s| s.1 == vec!["GOOG".to_string()] && s.2 == 1700000000000)
        .expect("Should have GOOG@t0");
    assert_eq!(goog_t0.3, 2810.0, "GOOG@t0 should be 2810.0 (last value)");

    let aapl_t1 = result
        .iter()
        .find(|s| s.1 == vec!["AAPL".to_string()] && s.2 == 1700000001000)
        .expect("Should have AAPL@t1");
    assert_eq!(aapl_t1.3, 152.0, "AAPL@t1 should be 152.0");
}

/// Empty input produces empty output.
#[test]
fn test_gauge_dedup_empty_input() {
    let samples: Vec<(String, Vec<String>, i64, f64)> = vec![];
    let result = gauge_dedup(&samples);
    assert!(result.is_empty(), "Empty input should produce empty output");
}

/// Multi-label deduplication works correctly.
#[test]
fn test_gauge_dedup_multi_label() {
    let samples = vec![
        (
            "trade_volume".to_string(),
            vec!["AAPL".to_string(), "NYSE".to_string()],
            1700000000000_i64,
            1000.0,
        ),
        (
            "trade_volume".to_string(),
            vec!["AAPL".to_string(), "NYSE".to_string()],
            1700000000000_i64,
            1500.0,
        ),
        // Same symbol, different exchange — should NOT be deduped
        (
            "trade_volume".to_string(),
            vec!["AAPL".to_string(), "NASDAQ".to_string()],
            1700000000000_i64,
            2000.0,
        ),
    ];

    let result = gauge_dedup(&samples);
    assert_eq!(
        result.len(),
        2,
        "Same symbol+exchange deduped, different exchange kept"
    );

    let nyse = result
        .iter()
        .find(|s| s.1 == vec!["AAPL".to_string(), "NYSE".to_string()])
        .expect("Should have AAPL+NYSE");
    assert_eq!(nyse.3, 1500.0, "NYSE should be 1500.0 (last value)");

    let nasdaq = result
        .iter()
        .find(|s| s.1 == vec!["AAPL".to_string(), "NASDAQ".to_string()])
        .expect("Should have AAPL+NASDAQ");
    assert_eq!(nasdaq.3, 2000.0, "NASDAQ should be 2000.0");
}
