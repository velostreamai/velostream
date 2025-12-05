//! Time simulation tests for test harness
//!
//! Tests for FR-085: Time Simulation for Test Harness Data Generator

use chrono::{Duration, Utc};
use velostream::velostream::test_harness::{
    SchemaDataGenerator, TimeSimulationConfig, parse_time_spec,
};

/// Test parsing relative time specifications
#[test]
fn test_parse_relative_time_hours() {
    let now = Utc::now();
    let parsed = parse_time_spec("-4h").unwrap();

    // Should be approximately 4 hours ago (within 1 second tolerance)
    let diff = now - parsed;
    assert!(
        diff > Duration::hours(3) && diff < Duration::hours(5),
        "Expected ~4 hours ago, got diff: {:?}",
        diff
    );
}

#[test]
fn test_parse_relative_time_minutes() {
    let now = Utc::now();
    let parsed = parse_time_spec("-30m").unwrap();

    let diff = now - parsed;
    assert!(
        diff > Duration::minutes(29) && diff < Duration::minutes(31),
        "Expected ~30 minutes ago, got diff: {:?}",
        diff
    );
}

#[test]
fn test_parse_relative_time_seconds() {
    let now = Utc::now();
    let parsed = parse_time_spec("-120s").unwrap();

    let diff = now - parsed;
    assert!(
        diff > Duration::seconds(119) && diff < Duration::seconds(121),
        "Expected ~120 seconds ago, got diff: {:?}",
        diff
    );
}

#[test]
fn test_parse_relative_time_days() {
    let now = Utc::now();
    let parsed = parse_time_spec("-2d").unwrap();

    let diff = now - parsed;
    assert!(
        diff > Duration::days(1) && diff < Duration::days(3),
        "Expected ~2 days ago, got diff: {:?}",
        diff
    );
}

#[test]
fn test_parse_now() {
    let now = Utc::now();
    let parsed = parse_time_spec("now").unwrap();

    let diff = (now - parsed).num_seconds().abs();
    assert!(
        diff < 2,
        "Expected 'now' to be current time, diff: {}s",
        diff
    );
}

#[test]
fn test_parse_absolute_time() {
    let parsed = parse_time_spec("2024-01-15T09:30:00Z").unwrap();

    assert_eq!(parsed.format("%Y-%m-%d").to_string(), "2024-01-15");
}

/// Test TimeSimulationConfig defaults
#[test]
fn test_time_simulation_config_defaults() {
    let config: TimeSimulationConfig = serde_yaml::from_str("{}").unwrap();

    assert_eq!(config.time_scale, 1.0);
    assert!(config.sequential);
    assert_eq!(config.batch_size, 1);
    assert!(config.events_per_second.is_none());
    assert!(config.jitter_ms.is_none());
    assert!(config.start_time.is_none());
}

/// Test TimeSimulationConfig parsing from YAML
#[test]
fn test_time_simulation_config_yaml_parsing() {
    let yaml = r#"
start_time: "-4h"
end_time: "now"
time_scale: 10.0
events_per_second: 100
sequential: true
jitter_ms: 50
batch_size: 10
"#;

    let config: TimeSimulationConfig = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(config.start_time, Some("-4h".to_string()));
    assert_eq!(config.end_time, Some("now".to_string()));
    assert_eq!(config.time_scale, 10.0);
    assert_eq!(config.events_per_second, Some(100.0));
    assert!(config.sequential);
    assert_eq!(config.jitter_ms, Some(50));
    assert_eq!(config.batch_size, 10);
}

/// Test generator with time simulation produces sequential timestamps
#[test]
fn test_generator_sequential_timestamps() {
    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator.set_time_simulation(&config, 100).unwrap();

    // Generate timestamps and verify they are sequential
    let mut prev_ts: Option<chrono::NaiveDateTime> = None;

    for _ in 0..10 {
        let ts = generator.next_simulated_timestamp().unwrap();
        if let Some(prev) = prev_ts {
            assert!(
                ts > prev,
                "Timestamps should be sequential: {} should be > {}",
                ts,
                prev
            );
        }
        prev_ts = Some(ts);
    }
}

/// Test time simulation state with different record counts
#[test]
fn test_time_simulation_interval_calculation() {
    // With 1 hour span and 100 records, each record should be ~36 seconds apart
    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator.set_time_simulation(&config, 100).unwrap();

    let first_ts = generator.next_simulated_timestamp().unwrap();
    let second_ts = generator.next_simulated_timestamp().unwrap();

    let interval = second_ts - first_ts;
    // With 100 records over 1 hour, interval should be ~36 seconds
    let expected_interval_secs = 3600.0 / 100.0; // 36 seconds

    let actual_interval_secs = interval.num_milliseconds() as f64 / 1000.0;

    // Allow some tolerance
    assert!(
        (actual_interval_secs - expected_interval_secs).abs() < 1.0,
        "Interval should be ~{:.1}s, got {:.1}s",
        expected_interval_secs,
        actual_interval_secs
    );
}

/// Test clearing time simulation
#[test]
fn test_clear_time_simulation() {
    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: None,
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator.set_time_simulation(&config, 100).unwrap();

    // Should have time state
    assert!(generator.next_simulated_timestamp().is_some());

    // Clear it
    generator.clear_time_simulation();

    // Should no longer have time state
    assert!(generator.next_simulated_timestamp().is_none());
}

/// Test jitter adds variation
#[test]
fn test_time_simulation_with_jitter() {
    let config = TimeSimulationConfig {
        start_time: Some("-1h".to_string()),
        end_time: Some("now".to_string()),
        time_scale: 1.0,
        events_per_second: None,
        sequential: true,
        jitter_ms: Some(100), // 100ms jitter
        batch_size: 1,
    };

    let mut generator = SchemaDataGenerator::new(Some(42));
    generator.set_time_simulation(&config, 1000).unwrap();

    // Generate multiple timestamps and collect intervals
    let mut intervals: Vec<i64> = Vec::new();
    let mut prev_ts: Option<chrono::NaiveDateTime> = None;

    for _ in 0..100 {
        let ts = generator.next_simulated_timestamp().unwrap();
        if let Some(prev) = prev_ts {
            intervals.push((ts - prev).num_milliseconds());
        }
        prev_ts = Some(ts);
    }

    // With jitter, intervals should vary
    let min_interval = *intervals.iter().min().unwrap();
    let max_interval = *intervals.iter().max().unwrap();

    // Intervals should have some variation (jitter effect)
    // Note: With 1000 records over 1 hour, base interval is 3600ms
    // With ±100ms jitter, we should see variation
    assert!(
        max_interval - min_interval > 50,
        "Expected variation due to jitter, but min={} max={}",
        min_interval,
        max_interval
    );
}
