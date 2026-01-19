//! Join Watermark Tracker Tests
//!
//! Tests for the JoinWatermarkTracker component.

use velostream::velostream::sql::execution::join::{
    JoinSide, JoinWatermarkTracker, WatermarkConfig,
};

#[test]
fn test_watermark_advancement() {
    let mut tracker = JoinWatermarkTracker::new();

    tracker.observe_event_time(JoinSide::Left, 1000, 0);
    assert_eq!(tracker.watermark(JoinSide::Left), 1000);

    tracker.observe_event_time(JoinSide::Left, 2000, 100);
    assert_eq!(tracker.watermark(JoinSide::Left), 2000);

    assert_eq!(tracker.watermark(JoinSide::Right), 0);

    assert_eq!(tracker.combined_watermark(), 0);
}

#[test]
fn test_combined_watermark() {
    let mut tracker = JoinWatermarkTracker::new();

    tracker.set_watermark(JoinSide::Left, 1000);
    tracker.set_watermark(JoinSide::Right, 500);

    assert_eq!(tracker.combined_watermark(), 500);

    tracker.set_watermark(JoinSide::Right, 1500);
    assert_eq!(tracker.combined_watermark(), 1000);
}

#[test]
fn test_lateness_detection() {
    let config = WatermarkConfig::with_lateness(100);
    let mut tracker = JoinWatermarkTracker::with_config(config);

    tracker.observe_event_time(JoinSide::Left, 1000, 0);
    assert_eq!(tracker.watermark(JoinSide::Left), 900);

    assert!(!tracker.is_late(JoinSide::Left, 950));
    assert!(tracker.is_late(JoinSide::Left, 850));
    assert!(!tracker.is_late(JoinSide::Left, 900));
}

#[test]
fn test_lateness_detection_with_set_watermark() {
    let config = WatermarkConfig::with_lateness(100);
    let mut tracker = JoinWatermarkTracker::with_config(config);

    tracker.set_watermark(JoinSide::Left, 900);

    assert!(!tracker.is_late(JoinSide::Left, 950));
    assert!(tracker.is_late(JoinSide::Left, 800));
}

#[test]
fn test_late_record_counting() {
    let config = WatermarkConfig::strict();
    let mut tracker = JoinWatermarkTracker::with_config(config);

    tracker.observe_event_time(JoinSide::Left, 1000, 0);

    let result = tracker.observe_event_time(JoinSide::Left, 500, 100);
    assert!(!result);

    assert_eq!(tracker.late_record_count(JoinSide::Left), 1);
}

#[test]
fn test_allow_late_data() {
    let config = WatermarkConfig {
        max_lateness_ms: 100,
        idle_timeout_ms: 60_000,
        allow_late_data: true,
    };
    let mut tracker = JoinWatermarkTracker::with_config(config);

    tracker.observe_event_time(JoinSide::Left, 1000, 0);

    let result = tracker.observe_event_time(JoinSide::Left, 500, 100);
    assert!(result);

    assert_eq!(tracker.late_record_count(JoinSide::Left), 1);
}

#[test]
fn test_idle_advancement() {
    let config = WatermarkConfig {
        max_lateness_ms: 0,
        idle_timeout_ms: 1000,
        allow_late_data: true,
    };
    let mut tracker = JoinWatermarkTracker::with_config(config);

    tracker.observe_event_time(JoinSide::Left, 5000, 0);
    tracker.observe_event_time(JoinSide::Right, 3000, 0);

    assert_eq!(tracker.combined_watermark(), 3000);

    let advanced = tracker.check_idle_advance(2000);
    assert!(advanced);

    assert_eq!(tracker.watermark(JoinSide::Right), 5000);
    assert_eq!(tracker.combined_watermark(), 5000);
}

#[test]
fn test_explicit_set_only_advances() {
    let mut tracker = JoinWatermarkTracker::new();

    tracker.set_watermark(JoinSide::Left, 1000);
    assert_eq!(tracker.watermark(JoinSide::Left), 1000);

    tracker.set_watermark(JoinSide::Left, 500);
    assert_eq!(tracker.watermark(JoinSide::Left), 1000);

    tracker.set_watermark(JoinSide::Left, 1500);
    assert_eq!(tracker.watermark(JoinSide::Left), 1500);
}

#[test]
fn test_stats() {
    let mut tracker = JoinWatermarkTracker::new();

    tracker.observe_event_time(JoinSide::Left, 1000, 0);
    tracker.observe_event_time(JoinSide::Right, 2000, 0);

    let stats = tracker.stats();
    assert_eq!(stats.left_watermark, 1000);
    assert_eq!(stats.right_watermark, 2000);
    assert_eq!(stats.combined_watermark, 1000);
}
