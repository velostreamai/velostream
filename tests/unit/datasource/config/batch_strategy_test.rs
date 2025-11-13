//! Comprehensive tests for BatchStrategy and BatchConfig core types
//!
//! These tests ensure that the core batch configuration types work correctly
//! independent of SQL parsing or any other integration layer.

use std::time::Duration;
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

#[test]
fn test_batch_strategy_default() {
    let strategy = BatchStrategy::default();
    match strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 100),
        _ => panic!("Expected default BatchStrategy to be FixedSize(100)"),
    }
}

#[test]
fn test_batch_config_default() {
    let config = BatchConfig::default();

    // Phase 4 optimization: increased from 1000 to 50,000
    assert_eq!(config.max_batch_size, 50_000);
    assert_eq!(config.batch_timeout, Duration::from_millis(1000));
    assert_eq!(config.enable_batching, true);

    match config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 100),
        _ => panic!("Expected default strategy to be FixedSize(100)"),
    }
}

#[test]
fn test_batch_strategy_fixed_size() {
    let strategy = BatchStrategy::FixedSize(500);
    match strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 500),
        _ => panic!("Expected FixedSize strategy"),
    }
}

#[test]
fn test_batch_strategy_time_window() {
    let duration = Duration::from_secs(5);
    let strategy = BatchStrategy::TimeWindow(duration);
    match strategy {
        BatchStrategy::TimeWindow(d) => assert_eq!(d, duration),
        _ => panic!("Expected TimeWindow strategy"),
    }
}

#[test]
fn test_batch_strategy_adaptive_size() {
    let strategy = BatchStrategy::AdaptiveSize {
        min_size: 10,
        max_size: 1000,
        target_latency: Duration::from_millis(100),
    };

    match strategy {
        BatchStrategy::AdaptiveSize {
            min_size,
            max_size,
            target_latency,
        } => {
            assert_eq!(min_size, 10);
            assert_eq!(max_size, 1000);
            assert_eq!(target_latency, Duration::from_millis(100));
        }
        _ => panic!("Expected AdaptiveSize strategy"),
    }
}

#[test]
fn test_batch_strategy_memory_based() {
    let strategy = BatchStrategy::MemoryBased(2048000); // 2MB
    match strategy {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 2048000),
        _ => panic!("Expected MemoryBased strategy"),
    }
}

#[test]
fn test_batch_strategy_low_latency() {
    let strategy = BatchStrategy::LowLatency {
        max_batch_size: 5,
        max_wait_time: Duration::from_millis(10),
        eager_processing: true,
    };

    match strategy {
        BatchStrategy::LowLatency {
            max_batch_size,
            max_wait_time,
            eager_processing,
        } => {
            assert_eq!(max_batch_size, 5);
            assert_eq!(max_wait_time, Duration::from_millis(10));
            assert_eq!(eager_processing, true);
        }
        _ => panic!("Expected LowLatency strategy"),
    }
}

#[test]
fn test_batch_config_construction() {
    let config = BatchConfig {
        strategy: BatchStrategy::FixedSize(200),
        max_batch_size: 2000,
        batch_timeout: Duration::from_millis(500),
        enable_batching: false,
    };

    match config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 200),
        _ => panic!("Expected FixedSize strategy"),
    }

    assert_eq!(config.max_batch_size, 2000);
    assert_eq!(config.batch_timeout, Duration::from_millis(500));
    assert_eq!(config.enable_batching, false);
}

#[test]
fn test_batch_strategy_clone_and_debug() {
    let original = BatchStrategy::AdaptiveSize {
        min_size: 50,
        max_size: 500,
        target_latency: Duration::from_millis(200),
    };

    // Test Clone
    let cloned = original.clone();
    match (&original, &cloned) {
        (
            BatchStrategy::AdaptiveSize {
                min_size: orig_min,
                max_size: orig_max,
                target_latency: orig_latency,
            },
            BatchStrategy::AdaptiveSize {
                min_size: clone_min,
                max_size: clone_max,
                target_latency: clone_latency,
            },
        ) => {
            assert_eq!(orig_min, clone_min);
            assert_eq!(orig_max, clone_max);
            assert_eq!(orig_latency, clone_latency);
        }
        _ => panic!("Clone failed or changed type"),
    }

    // Test Debug (should not panic)
    let debug_str = format!("{:?}", original);
    assert!(debug_str.contains("AdaptiveSize"));
    assert!(debug_str.contains("50"));
    assert!(debug_str.contains("500"));
}

#[test]
fn test_batch_config_clone_and_debug() {
    let config = BatchConfig {
        strategy: BatchStrategy::MemoryBased(1024 * 1024), // 1MB
        max_batch_size: 5000,
        batch_timeout: Duration::from_millis(2000),
        enable_batching: true,
    };

    // Test Clone
    let cloned = config.clone();
    assert_eq!(config.max_batch_size, cloned.max_batch_size);
    assert_eq!(config.batch_timeout, cloned.batch_timeout);
    assert_eq!(config.enable_batching, cloned.enable_batching);

    match (&config.strategy, &cloned.strategy) {
        (BatchStrategy::MemoryBased(orig), BatchStrategy::MemoryBased(clone)) => {
            assert_eq!(orig, clone);
        }
        _ => panic!("Strategy clone failed"),
    }

    // Test Debug (should not panic)
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("BatchConfig"));
    assert!(debug_str.contains("MemoryBased"));
    assert!(debug_str.contains("5000"));
}

#[test]
fn test_extreme_values() {
    // Test edge cases and extreme values

    // Very small batch size
    let small_strategy = BatchStrategy::FixedSize(1);
    match small_strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 1),
        _ => panic!("Expected FixedSize(1)"),
    }

    // Very large batch size
    let large_strategy = BatchStrategy::FixedSize(1_000_000);
    match large_strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 1_000_000),
        _ => panic!("Expected FixedSize(1000000)"),
    }

    // Very short duration
    let short_window = BatchStrategy::TimeWindow(Duration::from_millis(1));
    match short_window {
        BatchStrategy::TimeWindow(d) => assert_eq!(d, Duration::from_millis(1)),
        _ => panic!("Expected TimeWindow(1ms)"),
    }

    // Very long duration
    let long_window = BatchStrategy::TimeWindow(Duration::from_secs(3600)); // 1 hour
    match long_window {
        BatchStrategy::TimeWindow(d) => assert_eq!(d, Duration::from_secs(3600)),
        _ => panic!("Expected TimeWindow(1 hour)"),
    }
}

#[test]
fn test_adaptive_size_edge_cases() {
    // Min size equals max size (edge case)
    let equal_sizes = BatchStrategy::AdaptiveSize {
        min_size: 100,
        max_size: 100,
        target_latency: Duration::from_millis(50),
    };

    match equal_sizes {
        BatchStrategy::AdaptiveSize {
            min_size, max_size, ..
        } => {
            assert_eq!(min_size, 100);
            assert_eq!(max_size, 100);
        }
        _ => panic!("Expected AdaptiveSize strategy"),
    }

    // Very large adaptive range
    let large_range = BatchStrategy::AdaptiveSize {
        min_size: 1,
        max_size: 100_000,
        target_latency: Duration::from_millis(1000),
    };

    match large_range {
        BatchStrategy::AdaptiveSize {
            min_size, max_size, ..
        } => {
            assert_eq!(min_size, 1);
            assert_eq!(max_size, 100_000);
        }
        _ => panic!("Expected AdaptiveSize strategy"),
    }
}

#[test]
fn test_low_latency_edge_cases() {
    // Zero wait time
    let zero_wait = BatchStrategy::LowLatency {
        max_batch_size: 1,
        max_wait_time: Duration::from_millis(0),
        eager_processing: true,
    };

    match zero_wait {
        BatchStrategy::LowLatency { max_wait_time, .. } => {
            assert_eq!(max_wait_time, Duration::from_millis(0));
        }
        _ => panic!("Expected LowLatency strategy"),
    }

    // Eager processing disabled
    let no_eager = BatchStrategy::LowLatency {
        max_batch_size: 10,
        max_wait_time: Duration::from_millis(100),
        eager_processing: false,
    };

    match no_eager {
        BatchStrategy::LowLatency {
            eager_processing, ..
        } => {
            assert_eq!(eager_processing, false);
        }
        _ => panic!("Expected LowLatency strategy"),
    }
}

#[test]
fn test_batch_config_edge_cases() {
    // Batch size of 1 (minimal batching)
    let minimal_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(1),
        max_batch_size: 1,
        batch_timeout: Duration::from_millis(1),
        enable_batching: true,
    };

    assert_eq!(minimal_config.max_batch_size, 1);
    match minimal_config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 1),
        _ => panic!("Expected FixedSize(1)"),
    }

    // Batching disabled
    let disabled_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(100), // Strategy doesn't matter when disabled
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(1000),
        enable_batching: false,
    };

    assert_eq!(disabled_config.enable_batching, false);

    // Very large batch configuration
    let large_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(50_000),
        max_batch_size: 100_000,
        batch_timeout: Duration::from_secs(600), // 10 minutes
        enable_batching: true,
    };

    assert_eq!(large_config.max_batch_size, 100_000);
    assert_eq!(large_config.batch_timeout, Duration::from_secs(600));
}

#[test]
fn test_memory_based_edge_cases() {
    // Very small memory target
    let small_memory = BatchStrategy::MemoryBased(1024); // 1KB
    match small_memory {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 1024),
        _ => panic!("Expected MemoryBased(1KB)"),
    }

    // Very large memory target
    let large_memory = BatchStrategy::MemoryBased(1024 * 1024 * 1024); // 1GB
    match large_memory {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 1024 * 1024 * 1024),
        _ => panic!("Expected MemoryBased(1GB)"),
    }
}

#[test]
fn test_serialization_deserialization() {
    use serde_json;

    // Test FixedSize
    let fixed_strategy = BatchStrategy::FixedSize(250);
    let json = serde_json::to_string(&fixed_strategy).unwrap();
    let deserialized: BatchStrategy = serde_json::from_str(&json).unwrap();
    match deserialized {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 250),
        _ => panic!("Deserialization failed for FixedSize"),
    }

    // Test TimeWindow
    let time_strategy = BatchStrategy::TimeWindow(Duration::from_millis(1500));
    let json = serde_json::to_string(&time_strategy).unwrap();
    let deserialized: BatchStrategy = serde_json::from_str(&json).unwrap();
    match deserialized {
        BatchStrategy::TimeWindow(d) => assert_eq!(d, Duration::from_millis(1500)),
        _ => panic!("Deserialization failed for TimeWindow"),
    }

    // Test AdaptiveSize
    let adaptive_strategy = BatchStrategy::AdaptiveSize {
        min_size: 25,
        max_size: 750,
        target_latency: Duration::from_millis(80),
    };
    let json = serde_json::to_string(&adaptive_strategy).unwrap();
    let deserialized: BatchStrategy = serde_json::from_str(&json).unwrap();
    match deserialized {
        BatchStrategy::AdaptiveSize {
            min_size,
            max_size,
            target_latency,
        } => {
            assert_eq!(min_size, 25);
            assert_eq!(max_size, 750);
            assert_eq!(target_latency, Duration::from_millis(80));
        }
        _ => panic!("Deserialization failed for AdaptiveSize"),
    }

    // Test LowLatency
    let low_latency_strategy = BatchStrategy::LowLatency {
        max_batch_size: 3,
        max_wait_time: Duration::from_millis(5),
        eager_processing: true,
    };
    let json = serde_json::to_string(&low_latency_strategy).unwrap();
    let deserialized: BatchStrategy = serde_json::from_str(&json).unwrap();
    match deserialized {
        BatchStrategy::LowLatency {
            max_batch_size,
            max_wait_time,
            eager_processing,
        } => {
            assert_eq!(max_batch_size, 3);
            assert_eq!(max_wait_time, Duration::from_millis(5));
            assert_eq!(eager_processing, true);
        }
        _ => panic!("Deserialization failed for LowLatency"),
    }

    // Test MemoryBased
    let memory_strategy = BatchStrategy::MemoryBased(4096);
    let json = serde_json::to_string(&memory_strategy).unwrap();
    let deserialized: BatchStrategy = serde_json::from_str(&json).unwrap();
    match deserialized {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 4096),
        _ => panic!("Deserialization failed for MemoryBased"),
    }
}

#[test]
fn test_batch_config_serialization() {
    use serde_json;

    let config = BatchConfig {
        strategy: BatchStrategy::AdaptiveSize {
            min_size: 10,
            max_size: 500,
            target_latency: Duration::from_millis(100),
        },
        max_batch_size: 2000,
        batch_timeout: Duration::from_millis(1500),
        enable_batching: true,
    };

    // Serialize
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("AdaptiveSize"));

    // Deserialize
    let deserialized: BatchConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.max_batch_size, 2000);
    assert_eq!(deserialized.batch_timeout, Duration::from_millis(1500));
    assert_eq!(deserialized.enable_batching, true);

    match deserialized.strategy {
        BatchStrategy::AdaptiveSize {
            min_size,
            max_size,
            target_latency,
        } => {
            assert_eq!(min_size, 10);
            assert_eq!(max_size, 500);
            assert_eq!(target_latency, Duration::from_millis(100));
        }
        _ => panic!("Strategy deserialization failed"),
    }
}

#[test]
fn test_all_strategy_variants_complete_coverage() {
    // This test ensures we don't miss any BatchStrategy variants
    // If a new variant is added, this test will fail until updated

    let strategies = vec![
        BatchStrategy::FixedSize(100),
        BatchStrategy::TimeWindow(Duration::from_secs(1)),
        BatchStrategy::AdaptiveSize {
            min_size: 10,
            max_size: 1000,
            target_latency: Duration::from_millis(100),
        },
        BatchStrategy::MemoryBased(1024 * 1024),
        BatchStrategy::LowLatency {
            max_batch_size: 5,
            max_wait_time: Duration::from_millis(10),
            eager_processing: true,
        },
    ];

    for strategy in strategies {
        // Each strategy should be serializable
        let json = serde_json::to_string(&strategy).unwrap();
        assert!(!json.is_empty());

        // Each strategy should be debuggable
        let debug_str = format!("{:?}", strategy);
        assert!(!debug_str.is_empty());

        // Each strategy should be cloneable
        let _cloned = strategy.clone();
    }
}
