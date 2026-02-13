//! Integration tests for JobProcessorFactory and MockJobProcessor integration
//!
//! This test module verifies:
//! - JobProcessorFactory creates all processor types correctly
//! - MockJobProcessor provides proper testing capabilities
//! - Factory pattern enables processor abstraction for testing

use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory, MockJobProcessor,
};

#[test]
fn test_factory_create_simple() {
    let processor = JobProcessorFactory::create_simple();
    assert_eq!(processor.processor_version(), "V1");
    assert_eq!(processor.processor_name(), "SimpleJobProcessor");
    assert_eq!(processor.num_partitions(), 1);
}

#[test]
fn test_factory_create_transactional() {
    let processor = JobProcessorFactory::create_transactional();
    assert_eq!(processor.processor_version(), "V1-Transactional");
    assert_eq!(processor.processor_name(), "TransactionalJobProcessor");
    assert_eq!(processor.num_partitions(), 1);
}

#[test]
fn test_factory_create_adaptive_default() {
    let processor = JobProcessorFactory::create_adaptive_default();
    assert_eq!(processor.processor_version(), "V2");
    assert!(processor.num_partitions() > 0);
}

#[test]
fn test_factory_create_adaptive_with_partitions() {
    let processor = JobProcessorFactory::create_adaptive_with_partitions(4);
    assert_eq!(processor.processor_version(), "V2");
    assert_eq!(processor.num_partitions(), 4);
}

#[test]
fn test_factory_create_from_config_simple() {
    let config = JobProcessorConfig::Simple;
    let processor = JobProcessorFactory::create(config);
    assert_eq!(processor.processor_version(), "V1");
}

#[test]
fn test_factory_create_from_config_transactional() {
    let config = JobProcessorConfig::Transactional;
    let processor = JobProcessorFactory::create(config);
    assert_eq!(processor.processor_version(), "V1-Transactional");
}

#[test]
fn test_factory_create_from_config_adaptive() {
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(8),
        enable_core_affinity: false,
    };
    let processor = JobProcessorFactory::create(config);
    assert_eq!(processor.processor_version(), "V2");
    assert_eq!(processor.num_partitions(), 8);
}

#[test]
fn test_factory_create_mock() {
    let mock = JobProcessorFactory::create_mock();
    assert_eq!(mock.processor_version(), "Mock");
    assert_eq!(mock.processor_name(), "Mock Test Processor");
    assert_eq!(mock.num_partitions(), 1);
}

#[test]
fn test_mock_processor_new() {
    let mock = MockJobProcessor::new();
    assert_eq!(mock.processor_version(), "Mock");
    assert_eq!(mock.processor_name(), "Mock Test Processor");
    assert_eq!(mock.num_partitions(), 1);
}

#[test]
fn test_mock_processor_metrics() {
    let mock = MockJobProcessor::new();
    let metrics = mock.metrics();
    assert_eq!(metrics.version, "Mock");
    assert_eq!(metrics.name, "Mock Test Processor");
    assert_eq!(metrics.total_records, 0);
    assert_eq!(metrics.failed_records, 0);
}

#[test]
fn test_mock_processor_cloneable() {
    let mock1 = MockJobProcessor::new();
    let mock2 = mock1.clone();

    // Both should share internal state (via Arc)
    // This is important for testing where we want cloned instances to share state
    assert_eq!(mock1.processor_version(), mock2.processor_version());
}

#[test]
fn test_mock_processor_default() {
    let _mock = MockJobProcessor::default();
    assert_eq!(_mock.processor_version(), "Mock");
}

#[tokio::test]
async fn test_mock_processor_lifecycle_operations() {
    let mock = MockJobProcessor::new();

    // All lifecycle operations should succeed
    assert!(mock.start().await.is_ok());
    assert!(mock.pause().await.is_ok());
    assert!(mock.resume().await.is_ok());
    assert!(mock.stop().await.is_ok());
}

#[test]
fn test_mock_processor_batch_passthrough_sync_check() {
    // Basic check that MockJobProcessor is present and can be instantiated
    let mock = MockJobProcessor::new();
    assert_eq!(mock.processor_version(), "Mock");
    assert_eq!(mock.processor_name(), "Mock Test Processor");
}

#[test]
fn test_processor_config_description() {
    let config_simple = JobProcessorConfig::Simple;
    let desc = config_simple.description();
    assert!(desc.contains("Simple"));

    let config_transactional = JobProcessorConfig::Transactional;
    let desc = config_transactional.description();
    assert!(desc.contains("Transactional"));

    let config_adaptive = JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let desc = config_adaptive.description();
    assert!(desc.contains("Adaptive"));
    assert!(desc.contains("4"));
}

#[test]
fn test_factory_create_from_string_simple() {
    let processor = JobProcessorFactory::create_from_str("simple").unwrap();
    assert_eq!(processor.processor_version(), "V1");
}

#[test]
fn test_factory_create_from_string_transactional() {
    let processor = JobProcessorFactory::create_from_str("transactional").unwrap();
    assert_eq!(processor.processor_version(), "V1-Transactional");
}

#[test]
fn test_factory_create_from_string_adaptive() {
    let processor = JobProcessorFactory::create_from_str("adaptive").unwrap();
    assert_eq!(processor.processor_version(), "V2");
}

#[test]
fn test_factory_create_from_string_adaptive_with_partitions() {
    let processor = JobProcessorFactory::create_from_str("adaptive:8").unwrap();
    assert_eq!(processor.processor_version(), "V2");
    assert_eq!(processor.num_partitions(), 8);
}

#[test]
fn test_factory_invalid_config_string() {
    let result = JobProcessorFactory::create_from_str("invalid_config");
    assert!(result.is_err());
}

#[test]
fn test_all_processors_implement_trait() {
    // Verify all factory methods return Arc<dyn JobProcessor>
    let simple = JobProcessorFactory::create_simple();
    let transactional = JobProcessorFactory::create_transactional();
    let adaptive = JobProcessorFactory::create_adaptive_default();
    let mock = JobProcessorFactory::create_mock();

    // All should be non-zero length versions
    assert!(!simple.processor_version().is_empty());
    assert!(!transactional.processor_version().is_empty());
    assert!(!adaptive.processor_version().is_empty());
    assert!(!mock.processor_version().is_empty());
}

#[test]
fn test_processor_config_from_string_parsing() {
    // Test that JobProcessorConfig can be parsed from various string formats
    let config1: JobProcessorConfig = "simple".parse().unwrap();
    assert!(matches!(config1, JobProcessorConfig::Simple));

    let config2: JobProcessorConfig = "transactional".parse().unwrap();
    assert!(matches!(config2, JobProcessorConfig::Transactional));

    let config3: JobProcessorConfig = "adaptive".parse().unwrap();
    assert!(matches!(config3, JobProcessorConfig::Adaptive { .. }));

    let config4: JobProcessorConfig = "adaptive:16".parse().unwrap();
    if let JobProcessorConfig::Adaptive {
        num_partitions: Some(n),
        ..
    } = config4
    {
        assert_eq!(n, 16);
    } else {
        panic!("Expected Adaptive with 16 partitions");
    }
}

#[test]
fn test_factory_creates_independent_instances() {
    // Verify that factory creates independent instances
    let processor1 = JobProcessorFactory::create_simple();
    let processor2 = JobProcessorFactory::create_simple();

    // Both should have same version but be different Arc instances
    assert_eq!(
        processor1.processor_version(),
        processor2.processor_version()
    );
    // Arc pointers should be different (different allocations)
    assert!(!std::ptr::eq(processor1.as_ref(), processor2.as_ref()));
}

#[test]
fn test_mock_processor_testing_capabilities() {
    // Verify MockJobProcessor is useful for testing
    let mock = MockJobProcessor::new();

    // Mock processor should accept all standard operations
    assert_eq!(mock.processor_version(), "Mock");
    assert_eq!(mock.processor_name(), "Mock Test Processor");
    assert_eq!(mock.num_partitions(), 1);

    // Metrics should be available
    let metrics = mock.metrics();
    assert_eq!(metrics.total_records, 0);
    assert_eq!(metrics.failed_records, 0);

    // Mock is cloneable for test scenarios
    let mock_clone = mock.clone();
    assert_eq!(mock_clone.processor_version(), "Mock");
}
