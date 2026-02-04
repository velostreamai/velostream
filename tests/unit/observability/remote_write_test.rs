//! Tests for Prometheus remote-write client functionality
//!
//! These tests verify that the remote-write client correctly buffers and
//! prepares metrics with event timestamps for pushing to Prometheus.

use velostream::velostream::observability::remote_write::{RemoteWriteClient, RemoteWriteError};

/// Test that the remote-write client can be created with valid configuration
#[test]
fn test_remote_write_client_creation() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000);
    assert!(client.is_ok());

    let client = client.unwrap();
    assert_eq!(client.buffered_count(), 0);
}

/// Test that empty endpoint produces an error
#[test]
fn test_remote_write_client_empty_endpoint_error() {
    let client = RemoteWriteClient::new("", 100, 5000);
    assert!(client.is_err());

    match client {
        Err(RemoteWriteError::ConfigError(msg)) => {
            assert!(msg.contains("empty"), "Error should mention empty: {}", msg);
        }
        _ => panic!("Expected ConfigError for empty endpoint"),
    }
}

/// Test that push_gauge buffers samples
#[test]
fn test_push_gauge_buffers_sample() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    assert_eq!(client.buffered_count(), 0);

    client.push_gauge(
        "test_gauge",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        100.0,
        1704067200000, // 2024-01-01 00:00:00 UTC
    );

    assert_eq!(client.buffered_count(), 1);
}

/// Test that multiple samples are buffered correctly
#[test]
fn test_multiple_samples_buffered() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    // Push multiple samples with different timestamps
    for i in 0..10 {
        client.push_gauge(
            "test_metric",
            &["symbol".to_string()],
            &[format!("SYM{}", i)],
            (i as f64) * 10.0,
            1704067200000 + (i as i64) * 60000, // 1 minute apart
        );
    }

    assert_eq!(client.buffered_count(), 10);
}

/// Test that push_counter works like push_gauge
#[test]
fn test_push_counter_buffers_sample() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    client.push_counter(
        "test_counter",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        1.0,
        1704067200000,
    );

    assert_eq!(client.buffered_count(), 1);
}

/// Test that push_histogram_observation buffers sample
#[test]
fn test_push_histogram_observation_buffers_sample() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    client.push_histogram_observation(
        "test_histogram",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        42.5,
        1704067200000,
    );

    assert_eq!(client.buffered_count(), 1);
}

/// Test samples with multiple labels
#[test]
fn test_samples_with_multiple_labels() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    client.push_gauge(
        "trade_volume",
        &[
            "symbol".to_string(),
            "exchange".to_string(),
            "side".to_string(),
        ],
        &["AAPL".to_string(), "NYSE".to_string(), "BUY".to_string()],
        1000.0,
        1704067200000,
    );

    assert_eq!(client.buffered_count(), 1);
}

/// Test that inactive client (after shutdown) doesn't buffer
#[tokio::test]
async fn test_inactive_client_does_not_buffer() {
    let mut client =
        RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    // Shutdown the client (this will mark it inactive)
    // Note: shutdown will try to flush which will fail (no real endpoint), but that's ok
    let _ = client.shutdown().await;

    // Now pushes should be ignored
    client.push_gauge(
        "test_gauge",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        100.0,
        1704067200000,
    );

    assert_eq!(client.buffered_count(), 0);
}

/// Test client cloning shares the buffer
#[test]
fn test_client_cloning_shares_buffer() {
    let client1 = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();
    let client2 = client1.clone();

    client1.push_gauge(
        "test_gauge",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        100.0,
        1704067200000,
    );

    // Both clients should see the same buffer
    assert_eq!(client1.buffered_count(), 1);
    assert_eq!(client2.buffered_count(), 1);

    client2.push_gauge(
        "test_gauge",
        &["symbol".to_string()],
        &["MSFT".to_string()],
        200.0,
        1704067260000,
    );

    assert_eq!(client1.buffered_count(), 2);
    assert_eq!(client2.buffered_count(), 2);
}

/// Test Debug trait implementation
#[test]
fn test_debug_trait() {
    let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

    let debug_str = format!("{:?}", client);
    assert!(debug_str.contains("RemoteWriteClient"));
    assert!(debug_str.contains("endpoint"));
    assert!(debug_str.contains("batch_size"));
}

/// Test RetryConfig default values
#[test]
fn test_retry_config_default() {
    use velostream::velostream::observability::remote_write::RetryConfig;

    let config = RetryConfig::default();
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.initial_delay.as_millis(), 100);
    assert_eq!(config.max_delay.as_secs(), 10);
    assert_eq!(config.backoff_multiplier, 2.0);
    assert_eq!(config.request_timeout.as_secs(), 30);
}

/// Test RetryConfig no_retries preset
#[test]
fn test_retry_config_no_retries() {
    use velostream::velostream::observability::remote_write::RetryConfig;

    let config = RetryConfig::no_retries();
    assert_eq!(config.max_retries, 0);
}

/// Test RetryConfig aggressive preset
#[test]
fn test_retry_config_aggressive() {
    use velostream::velostream::observability::remote_write::RetryConfig;

    let config = RetryConfig::aggressive();
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.initial_delay.as_millis(), 50);
    assert_eq!(config.max_delay.as_secs(), 30);
    assert_eq!(config.request_timeout.as_secs(), 60);
}

/// Test client with custom retry config
#[test]
fn test_client_with_custom_retry_config() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 5,
        initial_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(20),
        backoff_multiplier: 1.5,
        request_timeout: Duration::from_secs(60),
    };

    let client = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    )
    .unwrap();

    assert_eq!(client.buffered_count(), 0);
}

/// Test client with no retries configuration
#[test]
fn test_client_with_no_retries() {
    use velostream::velostream::observability::remote_write::RetryConfig;

    let client = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        RetryConfig::no_retries(),
    )
    .unwrap();

    // Client should still work for buffering
    client.push_gauge(
        "test_gauge",
        &["symbol".to_string()],
        &["AAPL".to_string()],
        100.0,
        1704067200000,
    );

    assert_eq!(client.buffered_count(), 1);
}

// ============================================================================
// URL Validation Tests
// ============================================================================

/// Test that invalid URL format is rejected
#[test]
fn test_invalid_url_format_rejected() {
    let result = RemoteWriteClient::new("not-a-valid-url", 100, 5000);
    assert!(result.is_err());

    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("Invalid endpoint URL"),
                "Error should mention invalid URL: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for invalid URL"),
    }
}

/// Test that non-http/https scheme is rejected
#[test]
fn test_invalid_scheme_rejected() {
    let result = RemoteWriteClient::new("ftp://localhost:9090/api/v1/write", 100, 5000);
    assert!(result.is_err());

    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("scheme") && msg.contains("ftp"),
                "Error should mention invalid scheme: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for invalid scheme"),
    }
}

/// Test that https scheme is accepted
#[test]
fn test_https_scheme_accepted() {
    let result = RemoteWriteClient::new("https://prometheus.example.com/api/v1/write", 100, 5000);
    assert!(result.is_ok(), "HTTPS scheme should be accepted");
}

/// Test that file:// URL without proper host is rejected
#[test]
fn test_file_scheme_rejected() {
    // file:// URLs should be rejected (wrong scheme)
    let result = RemoteWriteClient::new("file:///api/v1/write", 100, 5000);
    assert!(result.is_err());
    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("scheme"),
                "Error should mention scheme: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for file scheme"),
    }
}

/// Test valid URLs with different ports
#[test]
fn test_valid_urls_with_ports() {
    // Standard port
    assert!(RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).is_ok());

    // High port
    assert!(RemoteWriteClient::new("http://localhost:65535/api/v1/write", 100, 5000).is_ok());

    // Port 1
    assert!(RemoteWriteClient::new("http://localhost:1/api/v1/write", 100, 5000).is_ok());

    // Default port (no explicit port)
    assert!(RemoteWriteClient::new("http://localhost/api/v1/write", 100, 5000).is_ok());
}

// ============================================================================
// Configuration Validation Tests
// ============================================================================

/// Test that batch_size of 0 is rejected
#[test]
fn test_batch_size_zero_rejected() {
    let result = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 0, 5000);
    assert!(result.is_err());

    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("batch_size"),
                "Error should mention batch_size: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for batch_size 0"),
    }
}

/// Test that invalid backoff_multiplier is rejected
#[test]
fn test_invalid_backoff_multiplier_rejected() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 0.0, // Invalid: must be > 0
        request_timeout: Duration::from_secs(30),
    };

    let result = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    );

    assert!(result.is_err());
    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("backoff_multiplier"),
                "Error should mention backoff_multiplier: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for invalid backoff_multiplier"),
    }
}

/// Test that too large backoff_multiplier is rejected
#[test]
fn test_excessive_backoff_multiplier_rejected() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 15.0, // Too aggressive
        request_timeout: Duration::from_secs(30),
    };

    let result = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    );

    assert!(result.is_err());
}

/// Test that initial_delay > max_delay is rejected
#[test]
fn test_initial_delay_exceeds_max_delay_rejected() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_secs(20), // Larger than max_delay
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 2.0,
        request_timeout: Duration::from_secs(30),
    };

    let result = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    );

    assert!(result.is_err());
    match result {
        Err(
            velostream::velostream::observability::remote_write::RemoteWriteError::ConfigError(msg),
        ) => {
            assert!(
                msg.contains("initial_delay") && msg.contains("max_delay"),
                "Error should mention delay mismatch: {}",
                msg
            );
        }
        _ => panic!("Expected ConfigError for delay mismatch"),
    }
}

/// Test that zero request_timeout is rejected
#[test]
fn test_zero_request_timeout_rejected() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 2.0,
        request_timeout: Duration::ZERO,
    };

    let result = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    );

    assert!(result.is_err());
}

/// Test that excessive request_timeout is rejected
#[test]
fn test_excessive_request_timeout_rejected() {
    use std::time::Duration;
    use velostream::velostream::observability::remote_write::RetryConfig;

    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 2.0,
        request_timeout: Duration::from_secs(600), // 10 minutes, too long
    };

    let result = RemoteWriteClient::with_retry_config(
        "http://localhost:9090/api/v1/write",
        100,
        5000,
        retry_config,
    );

    assert!(result.is_err());
}

/// Test RetryConfig validate method directly
#[test]
fn test_retry_config_validate() {
    use velostream::velostream::observability::remote_write::RetryConfig;

    // Valid config should pass
    let valid = RetryConfig::default();
    assert!(valid.validate().is_ok());

    // Negative backoff should fail
    let invalid = RetryConfig {
        backoff_multiplier: -1.0,
        ..Default::default()
    };
    assert!(invalid.validate().is_err());
}
