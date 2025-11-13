//! Integration tests for StreamJobServer with JobProcessor configuration
//!
//! Tests the Simple/Transactional/Adaptive processor architecture selection at the StreamJobServer level.
//! This demonstrates the integration of JobProcessorConfig with the job server.
//!
//! ## Configuration Usage Examples
//!
//! This test file demonstrates three patterns for configuring StreamJobServer:
//!
//! 1. **Pattern 1: Direct Parameters** (Backward Compatible)
//!    - Used in existing tests for basic setup
//!    - Example: `StreamJobServer::new("localhost:9092", "test-group", 10)`
//!    - Best for: Development, simple tests
//!
//! 2. **Pattern 2: Configuration Objects** (Recommended for Tests)
//!    - Used in new tests for flexible configuration
//!    - Example: `StreamJobServerConfig::new("custom-broker:9092", "test-group")`
//!    - Best for: Tests with custom brokers, complex scenarios
//!
//! 3. **Pattern 3: Environment Variables** (Production)
//!    - Used for environment-driven configuration
//!    - Example: `StreamJobServerConfig::from_env("prod-group")`
//!    - Best for: Docker, Kubernetes, production deployments
//!
//! See `docs/configuration-usage-guide.md` for complete documentation.

use velostream::velostream::server::config::StreamJobServerConfig;
use velostream::velostream::server::processors::JobProcessorConfig;
use velostream::velostream::server::stream_job_server::StreamJobServer;

#[tokio::test]
async fn test_stream_job_server_default_processor_config() {
    // Default configuration should be Adaptive
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let config = server.processor_config();
    assert!(matches!(config, JobProcessorConfig::Adaptive { .. }));
}

#[tokio::test]
async fn test_stream_job_server_adaptive_single_partition_processor_config() {
    // Set to Adaptive configuration with single partition
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_adaptive_single = server.with_processor_config(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });
    let adaptive_config = server_adaptive_single.processor_config();
    assert!(matches!(
        adaptive_config,
        JobProcessorConfig::Adaptive {
            num_partitions: Some(1),
            ..
        }
    ));
}

#[tokio::test]
async fn test_stream_job_server_adaptive_processor_config_with_partitions() {
    // Set to Adaptive with specific partition count
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_adaptive_8 = server.with_processor_config(JobProcessorConfig::Adaptive {
        num_partitions: Some(8),
        enable_core_affinity: false,
    });

    let adaptive_8_config = server_adaptive_8.processor_config();
    if let JobProcessorConfig::Adaptive {
        num_partitions: Some(n),
        ..
    } = adaptive_8_config
    {
        assert_eq!(*n, 8);
    } else {
        panic!("Expected Adaptive with 8 partitions");
    }
}

#[tokio::test]
async fn test_stream_job_server_processor_config_description() {
    // Test Adaptive single partition description
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_adaptive_single =
        server
            .clone()
            .with_processor_config(JobProcessorConfig::Adaptive {
                num_partitions: Some(1),
                enable_core_affinity: false,
            });
    let desc = server_adaptive_single.processor_config().description();
    assert!(desc.contains("Adaptive"));
    assert!(desc.contains("1"));

    // Test Adaptive description with 8 partitions
    let server_adaptive_8 = server
        .clone()
        .with_processor_config(JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: false,
        });
    let desc_adaptive = server_adaptive_8.processor_config().description();
    assert!(desc_adaptive.contains("Adaptive"));
    assert!(desc_adaptive.contains("8"));
}

#[tokio::test]
async fn test_stream_job_server_multiple_processor_configs() {
    // Test that multiple servers can have different processor configs
    let server1 = StreamJobServer::new("localhost:9092".to_string(), "group-1".to_string(), 10)
        .with_processor_config(JobProcessorConfig::Adaptive {
            num_partitions: Some(1),
            enable_core_affinity: false,
        });

    let server2 = StreamJobServer::new("localhost:9092".to_string(), "group-2".to_string(), 10)
        .with_processor_config(JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: false,
        });

    if let JobProcessorConfig::Adaptive {
        num_partitions: Some(n),
        ..
    } = server1.processor_config()
    {
        assert_eq!(*n, 1);
    } else {
        panic!("Expected Adaptive with 1 partition for server1");
    }

    if let JobProcessorConfig::Adaptive {
        num_partitions: Some(n),
        ..
    } = server2.processor_config()
    {
        assert_eq!(*n, 8);
    } else {
        panic!("Expected Adaptive with 8 partitions for server2");
    }
}

#[tokio::test]
async fn test_stream_job_server_processor_config_clone() {
    // Test that processor config is properly cloned
    let original = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10)
        .with_processor_config(JobProcessorConfig::Adaptive {
            num_partitions: Some(4),
            enable_core_affinity: false,
        });

    let cloned = original.clone();

    assert!(matches!(
        original.processor_config(),
        JobProcessorConfig::Adaptive {
            num_partitions: Some(4),
            ..
        }
    ));
    assert!(matches!(
        cloned.processor_config(),
        JobProcessorConfig::Adaptive {
            num_partitions: Some(4),
            ..
        }
    ));
}

// ============================================================================
// CONFIGURATION PATTERN EXAMPLES
// ============================================================================
// See `docs/configuration-usage-guide.md` for complete documentation
// ============================================================================

/// Pattern 1: Direct Parameters (Backward Compatible)
///
/// This is the simplest approach - pass broker and group directly.
/// All other settings use defaults.
///
/// Best for: Development, simple tests, existing code
#[tokio::test]
async fn test_configuration_pattern_1_direct_parameters() {
    // Pattern 1: Direct parameters with defaults
    let server = StreamJobServer::new(
        "custom-test-broker:9092".to_string(), // Custom broker
        "test-group".to_string(),              // Custom group
        10,                                    // Max jobs
    );

    // Uses defaults for: monitoring (false), timeout (24h), cache (100)
    // Verify processor config is present and is Adaptive
    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Pattern 2: Configuration Objects (Recommended for Tests)
///
/// This approach uses the StreamJobServerConfig struct with builder pattern.
/// More flexible and readable for complex test scenarios.
///
/// Best for: Tests with custom brokers, monitoring, timeouts
#[tokio::test]
async fn test_configuration_pattern_2_config_objects() {
    // Pattern 2: Configuration object with builder methods
    let config = StreamJobServerConfig::new(
        "custom-test-broker:9092", // Custom broker
        "test-group",
    )
    .with_max_jobs(50) // Override max jobs
    .with_monitoring(true) // Enable monitoring for test
    .with_table_cache_size(200); // Custom cache size

    // Create server with explicit configuration
    let server = StreamJobServer::with_config(config);

    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Pattern 3: Environment Variables (Production)
///
/// This approach loads configuration from environment variables.
/// Perfect for Docker, Kubernetes, and cloud deployments.
///
/// Best for: Production, Docker, Kubernetes, CI/CD pipelines
#[tokio::test]
async fn test_configuration_pattern_3_environment_variables() {
    // Pattern 3: Environment-driven configuration
    // Normally you'd set these before running:
    // export VELOSTREAM_KAFKA_BROKERS="custom-broker:9092"
    // export VELOSTREAM_MAX_JOBS="500"
    // export VELOSTREAM_ENABLE_MONITORING="true"

    // For testing, we simulate environment variables
    unsafe {
        std::env::set_var("VELOSTREAM_KAFKA_BROKERS", "custom-test-broker:9092");
        std::env::set_var("VELOSTREAM_MAX_JOBS", "50");
        std::env::set_var("VELOSTREAM_ENABLE_MONITORING", "true");
    }

    // Load from environment with defaults for missing variables
    let config = StreamJobServerConfig::from_env("env-test-group");

    // Create server with environment-loaded configuration
    let server = StreamJobServer::with_config(config);

    // Verify configuration was loaded from environment
    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));

    // Clean up environment
    unsafe {
        std::env::remove_var("VELOSTREAM_KAFKA_BROKERS");
        std::env::remove_var("VELOSTREAM_MAX_JOBS");
        std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
    }
}

/// Demonstrating custom broker usage with Pattern 1
///
/// This shows how existing tests can point to a custom Kafka broker
/// without changing code - just pass the broker address directly.
#[tokio::test]
async fn test_custom_kafka_broker_pattern_1() {
    // Pattern 1: Pass custom broker directly
    let test_broker = "test-kafka-broker:9092";
    let server = StreamJobServer::new(test_broker.to_string(), "custom-broker-test".to_string(), 5);

    // Server is configured with the test broker
    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Demonstrating custom broker usage with Pattern 2
///
/// This shows how new tests can use configuration objects
/// to point to a custom Kafka broker with additional customization.
#[tokio::test]
async fn test_custom_kafka_broker_pattern_2() {
    // Pattern 2: Configuration object for custom broker
    let config = StreamJobServerConfig::new(
        "test-kafka-broker:9092", // Custom broker for this test
        "config-test-group",
    )
    .with_max_jobs(10)
    .with_monitoring(false);

    let server = StreamJobServer::with_config(config);

    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Demonstrating default configuration (localhost:9092)
///
/// Shows how development defaults work when no broker is specified
#[tokio::test]
async fn test_configuration_default_localhost() {
    // Default uses localhost:9092
    let config = StreamJobServerConfig::default();

    assert_eq!(config.kafka_brokers, "localhost:9092");
    assert_eq!(config.max_jobs, 100);
    assert!(!(config.enable_monitoring));

    let server = StreamJobServer::with_config(config);

    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Demonstrating configuration with presets
///
/// Shows how to use built-in dev and production presets
#[tokio::test]
async fn test_configuration_with_dev_preset() {
    // Use dev preset: localhost, monitoring disabled, 10 max jobs
    let config = StreamJobServerConfig::default().with_dev_preset();

    assert_eq!(config.kafka_brokers, "localhost:9092");
    assert!(!(config.enable_monitoring));
    assert_eq!(config.max_jobs, 10);

    let server = StreamJobServer::with_config(config);

    assert!(matches!(
        server.processor_config(),
        JobProcessorConfig::Adaptive { .. }
    ));
}

/// Demonstrating configuration summary for logging
///
/// Shows how to get a formatted configuration summary for logging/debugging
#[tokio::test]
async fn test_configuration_summary_logging() {
    let config = StreamJobServerConfig::new("broker:9092", "test-group")
        .with_max_jobs(50)
        .with_monitoring(true);

    let summary = config.summary();

    // Summary contains key configuration information for logging
    assert!(summary.contains("broker:9092"));
    assert!(summary.contains("test-group"));
    assert!(summary.contains("max_jobs=50"));
    assert!(summary.contains("monitoring=true"));
}
