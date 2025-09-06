//! Tests for batch configuration via SQL WITH clauses
//!
//! This module tests the integration of batch configuration with SQL WITH clauses,
//! ensuring that batch settings can be specified and parsed correctly in streaming queries.

use ferrisstreams::ferris::datasource::{BatchConfig, BatchStrategy};
use ferrisstreams::ferris::sql::config::with_clause_parser::{WithClauseParser, WithClauseError};
use std::time::Duration;

#[test]
fn test_basic_batch_configuration() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'fixed_size',
        'sink.batch.size' = '200',
        'sink.batch.max_size' = '1000',
        'sink.batch.timeout' = '500ms'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    
    assert!(config.batch_config.is_some());
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, true);
    assert_eq!(batch_config.max_batch_size, 1000);
    assert_eq!(batch_config.batch_timeout, Duration::from_millis(500));
    
    match batch_config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 200),
        _ => panic!("Expected FixedSize strategy"),
    }
}

#[test]
fn test_time_window_batch_strategy() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.strategy' = 'time_window',
        'sink.batch.window' = '2s',
        'sink.batch.max_size' = '500'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::TimeWindow(duration) => assert_eq!(duration, Duration::from_secs(2)),
        _ => panic!("Expected TimeWindow strategy"),
    }
    
    assert_eq!(batch_config.max_batch_size, 500);
}

#[test]
fn test_memory_based_batch_strategy() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.strategy' = 'memory_based',
        'sink.batch.memory_size' = '2097152',
        'sink.batch.timeout' = '1s'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 2097152), // 2MB
        _ => panic!("Expected MemoryBased strategy"),
    }
    
    assert_eq!(batch_config.batch_timeout, Duration::from_secs(1));
}

#[test]
fn test_adaptive_size_batch_strategy() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.strategy' = 'adaptive_size',
        'sink.batch.min_size' = '50',
        'sink.batch.adaptive_max_size' = '800',
        'sink.batch.target_latency' = '150ms'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::AdaptiveSize { min_size, max_size, target_latency } => {
            assert_eq!(min_size, 50);
            assert_eq!(max_size, 800);
            assert_eq!(target_latency, Duration::from_millis(150));
        },
        _ => panic!("Expected AdaptiveSize strategy"),
    }
}

#[test]
fn test_low_latency_batch_strategy() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.strategy' = 'low_latency',
        'sink.batch.low_latency_max_size' = '5',
        'sink.batch.low_latency_wait' = '2ms',
        'sink.batch.eager_processing' = 'true'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::LowLatency { max_batch_size, max_wait_time, eager_processing } => {
            assert_eq!(max_batch_size, 5);
            assert_eq!(max_wait_time, Duration::from_millis(2));
            assert_eq!(eager_processing, true);
        },
        _ => panic!("Expected LowLatency strategy"),
    }
}

#[test]
fn test_batch_configuration_defaults() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.enable' = 'true'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    // Should use defaults for missing values
    assert_eq!(batch_config.enable_batching, true);
    assert_eq!(batch_config.max_batch_size, 1000); // Default max size
    assert_eq!(batch_config.batch_timeout, Duration::from_millis(1000)); // Default timeout
    
    // Should use default strategy (FixedSize with default size)
    match batch_config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 100), // Default strategy/size
        _ => panic!("Expected default FixedSize strategy"),
    }
}

#[test]
fn test_batch_disabled() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.enable' = 'false'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, false);
}

#[test]
fn test_case_insensitive_strategy_names() {
    let parser = WithClauseParser::new();
    
    // Test various case combinations
    let test_cases = vec![
        ("FIXED_SIZE", "fixedsize", "FixedSize", "fixed_size"),
        ("TIME_WINDOW", "timewindow", "TimeWindow", "time_window"), 
        ("MEMORY_BASED", "memorybased", "MemoryBased", "memory_based"),
        ("ADAPTIVE_SIZE", "adaptivesize", "AdaptiveSize", "adaptive_size"),
        ("LOW_LATENCY", "lowlatency", "LowLatency", "low_latency"),
    ];

    for variations in test_cases {
        for strategy_name in [variations.0, variations.1, variations.2, variations.3] {
            let with_clause = format!(r#"
                'sink.bootstrap.servers' = 'localhost:9092',
                'sink.topic' = 'test-topic',
                'sink.batch.strategy' = '{}'
            "#, strategy_name);

            let config = parser.parse_with_clause(&with_clause).unwrap();
            assert!(config.batch_config.is_some(), "Failed to parse strategy: {}", strategy_name);
        }
    }
}

#[test]
fn test_alternative_batch_key_prefixes() {
    let parser = WithClauseParser::new();
    
    // Test both 'batch.' and 'sink.batch.' prefixes
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'batch.enable' = 'true',
        'batch.strategy' = 'fixed_size',
        'batch.size' = '150'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, true);
    match batch_config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 150),
        _ => panic!("Expected FixedSize strategy"),
    }
}

#[test]
fn test_mixed_batch_key_prefixes() {
    let parser = WithClauseParser::new();
    
    // Test mixing 'batch.' and 'sink.batch.' prefixes (sink.batch. should take precedence)
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'batch.enable' = 'false',
        'sink.batch.enable' = 'true',
        'batch.strategy' = 'time_window',
        'sink.batch.strategy' = 'fixed_size',
        'sink.batch.size' = '300'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    // sink.batch.* should take precedence
    assert_eq!(batch_config.enable_batching, true);
    match batch_config.strategy {
        BatchStrategy::FixedSize(size) => assert_eq!(size, 300),
        _ => panic!("Expected FixedSize strategy from sink.batch.strategy"),
    }
}

#[test]
fn test_invalid_batch_strategy() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.strategy' = 'invalid_strategy'
    "#;

    let result = parser.parse_with_clause(with_clause);
    assert!(result.is_err());
    
    match result.unwrap_err() {
        WithClauseError::InvalidValue { key, value, .. } => {
            assert_eq!(key, "sink.batch.strategy");
            assert_eq!(value, "invalid_strategy");
        },
        _ => panic!("Expected InvalidValue error"),
    }
}

#[test]
fn test_invalid_batch_size() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.size' = 'not_a_number'
    "#;

    let result = parser.parse_with_clause(with_clause);
    assert!(result.is_err());
    
    match result.unwrap_err() {
        WithClauseError::InvalidValue { key, value, .. } => {
            assert_eq!(key, "sink.batch.max_size");
            assert_eq!(value, "not_a_number");
        },
        _ => panic!("Expected InvalidValue error for invalid batch size"),
    }
}

#[test]
fn test_invalid_batch_timeout() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.timeout' = 'invalid_duration'
    "#;

    let result = parser.parse_with_clause(with_clause);
    assert!(result.is_err());
    
    match result.unwrap_err() {
        WithClauseError::InvalidValue { key, value, .. } => {
            assert_eq!(key, "sink.batch.timeout");
            assert_eq!(value, "invalid_duration");
        },
        _ => panic!("Expected InvalidValue error for invalid timeout"),
    }
}

#[test]
fn test_invalid_boolean_values() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.enable' = 'maybe'
    "#;

    let result = parser.parse_with_clause(with_clause);
    assert!(result.is_err());
    
    match result.unwrap_err() {
        WithClauseError::InvalidValue { key, value, .. } => {
            assert_eq!(key, "sink.batch.enable");
            assert_eq!(value, "maybe");
        },
        _ => panic!("Expected InvalidValue error for invalid boolean"),
    }
}

#[test]
fn test_comprehensive_batch_configuration() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'comprehensive-test',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'adaptive_size',
        'sink.batch.min_size' = '25',
        'sink.batch.adaptive_max_size' = '750',
        'sink.batch.max_size' = '1500',
        'sink.batch.timeout' = '750ms',
        'sink.batch.target_latency' = '80ms'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    
    // Should have both regular config and batch config
    assert!(config.batch_config.is_some());
    assert!(config.raw_config.contains_key("sink.bootstrap.servers"));
    assert!(config.raw_config.contains_key("sink.topic"));
    
    let batch_config = config.batch_config.unwrap();
    assert_eq!(batch_config.enable_batching, true);
    assert_eq!(batch_config.max_batch_size, 1500);
    assert_eq!(batch_config.batch_timeout, Duration::from_millis(750));
    
    match batch_config.strategy {
        BatchStrategy::AdaptiveSize { min_size, max_size, target_latency } => {
            assert_eq!(min_size, 25);
            assert_eq!(max_size, 750);
            assert_eq!(target_latency, Duration::from_millis(80));
        },
        _ => panic!("Expected AdaptiveSize strategy"),
    }
}

#[test]
fn test_no_batch_configuration() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'no-batch-config',
        'sink.value.format' = 'json'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    
    // Should not have batch configuration when no batch keys are present
    assert!(config.batch_config.is_none());
    assert!(config.raw_config.contains_key("sink.bootstrap.servers"));
    assert!(config.raw_config.contains_key("sink.topic"));
    assert!(config.raw_config.contains_key("sink.value.format"));
}

#[test] 
fn test_batch_configuration_with_file_sink() {
    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.path' = '/tmp/output.json',
        'sink.format' = 'json',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'memory_based',
        'sink.batch.memory_size' = '524288'
    "#;

    let config = parser.parse_with_clause(with_clause).unwrap();
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, true);
    match batch_config.strategy {
        BatchStrategy::MemoryBased(size) => assert_eq!(size, 524288), // 512KB
        _ => panic!("Expected MemoryBased strategy"),
    }
    
    // Should still have file sink properties
    assert_eq!(config.raw_config.get("sink.path").unwrap(), "/tmp/output.json");
    assert_eq!(config.raw_config.get("sink.format").unwrap(), "json");
}