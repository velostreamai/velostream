//! Integration tests for StreamJobServer with JobProcessor configuration
//!
//! Tests the V1/V2 processor architecture selection at the StreamJobServer level.
//! This demonstrates the integration of JobProcessorConfig with the job server.

use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::server::processors::JobProcessorConfig;

#[tokio::test]
async fn test_stream_job_server_default_processor_config() {
    // Default configuration should be V2
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let config = server.processor_config();
    assert!(matches!(config, JobProcessorConfig::V2 { .. }));
}

#[tokio::test]
async fn test_stream_job_server_v1_processor_config() {
    // Set to V1 configuration
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_v1 = server.with_processor_config(JobProcessorConfig::V1);
    let v1_config = server_v1.processor_config();
    assert!(matches!(v1_config, JobProcessorConfig::V1));
}

#[tokio::test]
async fn test_stream_job_server_v2_processor_config_with_partitions() {
    // Set to V2 with specific partition count
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_v2_8 = server.with_processor_config(JobProcessorConfig::V2 {
        num_partitions: Some(8),
        enable_core_affinity: false,
    });

    let v2_8_config = server_v2_8.processor_config();
    if let JobProcessorConfig::V2 {
        num_partitions: Some(n),
        ..
    } = v2_8_config
    {
        assert_eq!(*n, 8);
    } else {
        panic!("Expected V2 with 8 partitions");
    }
}

#[tokio::test]
async fn test_stream_job_server_processor_config_description() {
    // Test V1 description
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10);
    let server_v1 = server.clone().with_processor_config(JobProcessorConfig::V1);
    let desc = server_v1.processor_config().description();
    assert!(desc.contains("V1"));

    // Test V2 description with partitions
    let server_v2_8 = server.clone().with_processor_config(JobProcessorConfig::V2 {
        num_partitions: Some(8),
        enable_core_affinity: false,
    });
    let desc_v2 = server_v2_8.processor_config().description();
    assert!(desc_v2.contains("V2"));
    assert!(desc_v2.contains("8"));
}

#[tokio::test]
async fn test_stream_job_server_multiple_processor_configs() {
    // Test that multiple servers can have different processor configs
    let server1 = StreamJobServer::new("localhost:9092".to_string(), "group-1".to_string(), 10)
        .with_processor_config(JobProcessorConfig::V1);

    let server2 = StreamJobServer::new("localhost:9092".to_string(), "group-2".to_string(), 10)
        .with_processor_config(JobProcessorConfig::V2 {
            num_partitions: Some(8),
            enable_core_affinity: false,
        });

    assert!(matches!(server1.processor_config(), JobProcessorConfig::V1));
    if let JobProcessorConfig::V2 {
        num_partitions: Some(n),
        ..
    } = server2.processor_config()
    {
        assert_eq!(*n, 8);
    } else {
        panic!("Expected V2 with 8 partitions for server2");
    }
}

#[tokio::test]
async fn test_stream_job_server_processor_config_clone() {
    // Test that processor config is properly cloned
    let original = StreamJobServer::new("localhost:9092".to_string(), "test-group".to_string(), 10)
        .with_processor_config(JobProcessorConfig::V1);

    let cloned = original.clone();

    assert!(matches!(original.processor_config(), JobProcessorConfig::V1));
    assert!(matches!(cloned.processor_config(), JobProcessorConfig::V1));
}
