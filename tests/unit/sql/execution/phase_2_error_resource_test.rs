/*!
# Phase 2: Error & Resource Enhancements Tests

Comprehensive unit tests for Phase 2 streaming engine enhancements including:
1. StreamingError enum with circuit breaker integration
2. ResourceManager for monitoring and limits
3. CircuitBreaker for fault tolerance
4. Enhanced StreamingConfig with Phase 2 features

These tests validate all Phase 2 functionality including edge cases,
error scenarios, and integration with existing Phase 1A/1B components.
*/

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio;
use velostream::velostream::sql::execution::{
    circuit_breaker::{
        CircuitBreaker, CircuitBreakerConfig, CircuitBreakerRegistry, CircuitBreakerState,
    },
    config::{
        CircuitBreakerConfig as ConfigCircuitBreakerConfig, ResourceMonitoringConfig,
        StreamingConfig,
    },
    error::{ErrorSeverity, StreamingError, ThresholdSeverity},
    resource_manager::{MonitoringConfig, ResourceLimits, ResourceManager},
};

// ===== STREAMING ERROR TESTS =====

#[test]
fn test_streaming_error_recoverability() {
    let resource_error = StreamingError::ResourceExhausted {
        resource_type: "memory".to_string(),
        current_usage: 1024,
        limit: 1000,
        message: "Out of memory".to_string(),
    };
    assert!(!resource_error.is_recoverable());

    let watermark_error = StreamingError::WatermarkError {
        source_id: "source1".to_string(),
        message: "Late watermark".to_string(),
        recoverable: true,
    };
    assert!(watermark_error.is_recoverable());

    let late_data_error = StreamingError::LateDataError {
        strategy: "Drop".to_string(),
        lateness: Duration::from_secs(10),
        message: "Data too late".to_string(),
    };
    assert!(late_data_error.is_recoverable());
}

#[test]
fn test_streaming_error_severity_classification() {
    let warning_violation = StreamingError::ResourceThresholdViolation {
        resource_type: "cpu".to_string(),
        current_value: 80.0,
        threshold: 70.0,
        severity: ThresholdSeverity::Warning,
    };
    assert_eq!(warning_violation.severity(), ErrorSeverity::Low);

    let critical_violation = StreamingError::ResourceThresholdViolation {
        resource_type: "memory".to_string(),
        current_value: 95.0,
        threshold: 90.0,
        severity: ThresholdSeverity::Critical,
    };
    assert_eq!(critical_violation.severity(), ErrorSeverity::High);

    let fatal_violation = StreamingError::ResourceThresholdViolation {
        resource_type: "disk".to_string(),
        current_value: 100.0,
        threshold: 95.0,
        severity: ThresholdSeverity::Fatal,
    };
    assert_eq!(fatal_violation.severity(), ErrorSeverity::Critical);
}

#[test]
fn test_streaming_error_retry_delays() {
    let resource_error = StreamingError::ResourceExhausted {
        resource_type: "memory".to_string(),
        current_usage: 1024,
        limit: 1000,
        message: "Out of memory".to_string(),
    };
    assert_eq!(
        resource_error.suggested_retry_delay(),
        Some(Duration::from_secs(5))
    );

    let late_data_error = StreamingError::LateDataError {
        strategy: "Drop".to_string(),
        lateness: Duration::from_secs(10),
        message: "Data too late".to_string(),
    };
    assert_eq!(
        late_data_error.suggested_retry_delay(),
        Some(Duration::from_millis(50))
    );

    let retry_exhausted = StreamingError::RetryExhausted {
        operation: "test_op".to_string(),
        attempt_count: 5,
        max_attempts: 5,
        last_error_message: "connection failed".to_string(),
    };
    assert_eq!(retry_exhausted.suggested_retry_delay(), None);
}

#[test]
fn test_circuit_breaker_open_error() {
    let now = SystemTime::now();
    let cb_error = StreamingError::CircuitBreakerOpen {
        service: "test_service".to_string(),
        failure_count: 5,
        last_failure_time: now,
        next_retry_time: now + Duration::from_secs(60),
    };

    assert!(!cb_error.is_recoverable());
    assert_eq!(cb_error.severity(), ErrorSeverity::Medium);

    let retry_delay = cb_error.suggested_retry_delay();
    assert!(retry_delay.is_some());
    assert!(retry_delay.unwrap() <= Duration::from_secs(60));
}

// ===== RESOURCE MANAGER TESTS =====

#[test]
fn test_resource_manager_disabled_by_default() {
    let limits = ResourceLimits::default();
    let manager = ResourceManager::new(limits);
    assert!(!manager.is_enabled());
}

#[test]
fn test_resource_manager_enable_disable() {
    let limits = ResourceLimits::default();
    let mut manager = ResourceManager::new(limits);

    manager.enable();
    assert!(manager.is_enabled());

    manager.disable();
    assert!(!manager.is_enabled());
}

#[test]
fn test_resource_usage_tracking_when_disabled() {
    let limits = ResourceLimits {
        max_total_memory: Some(100), // Very low limit
        ..Default::default()
    };
    let manager = ResourceManager::new(limits); // Not enabled

    // Should succeed even with low limits when disabled
    assert!(manager.update_resource_usage("total_memory", 1000).is_ok());
    assert!(manager.check_allocation("total_memory", 1000).is_ok());
    assert!(manager.check_all_resources().is_empty());
}

#[test]
fn test_resource_usage_tracking_when_enabled() {
    let limits = ResourceLimits {
        max_total_memory: Some(1024),
        ..Default::default()
    };
    let mut manager = ResourceManager::new(limits);
    manager.enable();

    // Should work within limits
    assert!(manager.update_resource_usage("total_memory", 512).is_ok());

    let usage = manager.get_resource_usage("total_memory").unwrap();
    assert_eq!(usage.current, 512);
    assert_eq!(usage.peak, 512);
    assert_eq!(usage.limit_violations, 0);
}

#[test]
fn test_resource_limit_enforcement() {
    let limits = ResourceLimits {
        max_total_memory: Some(1024),
        ..Default::default()
    };
    let mut manager = ResourceManager::new(limits);
    manager.enable();

    // Should fail when exceeding limits
    let result = manager.update_resource_usage("total_memory", 2048);
    assert!(result.is_err());

    match result.err().unwrap() {
        StreamingError::ResourceExhausted {
            current_usage,
            limit,
            ..
        } => {
            assert_eq!(current_usage, 2048);
            assert_eq!(limit, 1024);
        }
        _ => panic!("Expected ResourceExhausted error"),
    }
}

#[test]
fn test_resource_allocation_checking() {
    let limits = ResourceLimits {
        max_total_memory: Some(1024),
        ..Default::default()
    };
    let mut manager = ResourceManager::new(limits);
    manager.enable();

    // Set current usage
    manager.update_resource_usage("total_memory", 800).unwrap();

    // Should allow allocation within limit
    assert!(manager.check_allocation("total_memory", 200).is_ok());

    // Should reject allocation that would exceed limit
    let result = manager.check_allocation("total_memory", 300);
    assert!(result.is_err());
}

#[test]
fn test_resource_utilization_calculation() {
    let limits = ResourceLimits {
        max_total_memory: Some(1000),
        ..Default::default()
    };
    let mut manager = ResourceManager::new(limits);
    manager.enable();

    manager.update_resource_usage("total_memory", 800).unwrap();

    let utilization = manager.get_resource_utilization("total_memory").unwrap();
    assert_eq!(utilization, 80.0);

    // Test utilization for non-existent resource
    assert!(manager.get_resource_utilization("non_existent").is_none());
}

#[test]
fn test_resource_threshold_violations() {
    let limits = ResourceLimits {
        max_total_memory: Some(1000),
        ..Default::default()
    };
    let config = MonitoringConfig {
        warning_threshold: 0.7,  // 70%
        critical_threshold: 0.9, // 90%
        ..Default::default()
    };
    let mut manager = ResourceManager::with_monitoring_config(limits, config);
    manager.enable();

    // Set usage below warning threshold
    manager.update_resource_usage("total_memory", 650).unwrap();
    assert_eq!(manager.check_all_resources().len(), 0);

    // Set usage above warning threshold
    manager.update_resource_usage("total_memory", 750).unwrap();
    let violations = manager.check_all_resources();
    assert_eq!(violations.len(), 1);

    match &violations[0] {
        StreamingError::ResourceThresholdViolation {
            severity,
            current_value,
            ..
        } => {
            assert_eq!(*severity, ThresholdSeverity::Warning);
            assert_eq!(*current_value, 75.0);
        }
        _ => panic!("Expected ResourceThresholdViolation"),
    }

    // Set usage above critical threshold
    manager.update_resource_usage("total_memory", 950).unwrap();
    let violations = manager.check_all_resources();
    assert_eq!(violations.len(), 1);

    match &violations[0] {
        StreamingError::ResourceThresholdViolation {
            severity,
            current_value,
            ..
        } => {
            assert_eq!(*severity, ThresholdSeverity::Critical);
            assert_eq!(*current_value, 95.0);
        }
        _ => panic!("Expected ResourceThresholdViolation"),
    }
}

#[test]
fn test_resource_convenience_methods() {
    let limits = ResourceLimits {
        max_total_memory: Some(1000),
        ..Default::default()
    };
    let mut manager = ResourceManager::new(limits);
    manager.enable();

    // Test memory convenience methods
    assert!(manager.update_memory_usage("heap", 400).is_ok());
    assert!(manager.check_memory_allocation("heap", 200).is_ok());
    assert!(manager.check_memory_allocation("heap", 700).is_err());

    // Test concurrent operations
    assert!(manager.update_concurrent_operations(50).is_ok());

    // Test window counting
    assert!(manager.update_window_count("user_123", 5).is_ok());

    // Test processing time recording
    assert!(
        manager
            .record_processing_time("select_operation", Duration::from_millis(100))
            .is_ok()
    );
}

// ===== CIRCUIT BREAKER TESTS =====

#[test]
fn test_circuit_breaker_disabled_by_default() {
    let breaker = CircuitBreaker::with_default_config("test_service".to_string());
    assert!(!breaker.is_enabled());
    assert_eq!(breaker.get_state(), CircuitBreakerState::Closed);
}

#[test]
fn test_circuit_breaker_enable_disable() {
    let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());

    breaker.enable();
    assert!(breaker.is_enabled());

    breaker.disable();
    assert!(!breaker.is_enabled());
}

#[tokio::test]
async fn test_circuit_breaker_disabled_allows_all_operations() {
    let breaker = CircuitBreaker::with_default_config("test_service".to_string());
    // Breaker is disabled by default

    // Should allow operation even if it would normally fail
    let result: Result<&str, StreamingError> = breaker
        .execute(|| {
            Err(StreamingError::MessagePassingError {
                operation: "test".to_string(),
                message: "test error".to_string(),
                retry_possible: true,
            })
        })
        .await;

    assert!(result.is_err()); // Error passes through, but circuit breaker didn't interfere
    assert_eq!(breaker.get_state(), CircuitBreakerState::Closed); // State unchanged

    let stats = breaker.get_stats();
    assert_eq!(stats.total_calls, 0); // No stats tracked when disabled
}

#[tokio::test]
async fn test_successful_circuit_breaker_operation() {
    let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
    breaker.enable();

    let result = breaker.execute(|| Ok("success")).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");

    let stats = breaker.get_stats();
    assert_eq!(stats.total_calls, 1);
    assert_eq!(stats.total_successes, 1);
    assert_eq!(stats.total_failures, 0);
    assert_eq!(stats.consecutive_failures, 0);
}

#[tokio::test]
async fn test_failed_circuit_breaker_operation() {
    let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
    breaker.enable();

    let result: Result<&str, StreamingError> = breaker
        .execute(|| {
            Err(StreamingError::MessagePassingError {
                operation: "test".to_string(),
                message: "test failure".to_string(),
                retry_possible: true,
            })
        })
        .await;

    assert!(result.is_err());

    let stats = breaker.get_stats();
    assert_eq!(stats.total_calls, 1);
    assert_eq!(stats.total_successes, 0);
    assert_eq!(stats.total_failures, 1);
    assert_eq!(stats.consecutive_failures, 1);
    assert_eq!(breaker.get_state(), CircuitBreakerState::Closed); // Still closed after 1 failure
}

#[tokio::test]
async fn test_circuit_breaker_opens_after_failures() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2, // Open after 2 failures
        ..Default::default()
    };
    let mut breaker = CircuitBreaker::new("test_service".to_string(), config);
    breaker.enable();

    // First failure
    let _: Result<&str, StreamingError> = breaker
        .execute(|| {
            Err(StreamingError::MessagePassingError {
                operation: "test".to_string(),
                message: "failure 1".to_string(),
                retry_possible: true,
            })
        })
        .await;
    assert_eq!(breaker.get_state(), CircuitBreakerState::Closed);

    // Second failure should open circuit
    let _: Result<(), StreamingError> = breaker
        .execute(|| {
            Err(StreamingError::MessagePassingError {
                operation: "test".to_string(),
                message: "failure 2".to_string(),
                retry_possible: true,
            })
        })
        .await;
    assert_eq!(breaker.get_state(), CircuitBreakerState::Open);
}

#[tokio::test]
async fn test_circuit_breaker_open_rejects_calls() {
    let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
    breaker.enable();
    breaker.force_open(); // Force circuit open

    let result = breaker.execute(|| Ok("should not execute")).await;
    assert!(result.is_err());

    match result.err().unwrap() {
        StreamingError::CircuitBreakerOpen { service, .. } => {
            assert_eq!(service, "test_service");
        }
        _ => panic!("Expected CircuitBreakerOpen error"),
    }
}

#[tokio::test]
async fn test_circuit_breaker_timeout() {
    let config = CircuitBreakerConfig {
        operation_timeout: Duration::from_millis(10), // Very short timeout
        ..Default::default()
    };
    let mut breaker = CircuitBreaker::new("test_service".to_string(), config);
    breaker.enable();

    // Operation that takes longer than timeout
    let result = breaker
        .execute(|| {
            std::thread::sleep(Duration::from_millis(100));
            Ok("should timeout")
        })
        .await;

    assert!(result.is_err());

    let stats = breaker.get_stats();
    assert_eq!(stats.total_calls, 1);
    assert_eq!(stats.total_timeouts, 1);
}

#[test]
fn test_circuit_breaker_force_operations() {
    let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
    breaker.enable();

    breaker.force_open();
    assert_eq!(breaker.get_state(), CircuitBreakerState::Open);

    breaker.force_close();
    assert_eq!(breaker.get_state(), CircuitBreakerState::Closed);

    let stats = breaker.get_stats();
    assert_eq!(stats.consecutive_failures, 0);

    breaker.reset();
    let stats = breaker.get_stats();
    assert_eq!(stats.total_calls, 0);
}

#[test]
fn test_circuit_breaker_registry() {
    let config = CircuitBreakerConfig::default();
    let registry = CircuitBreakerRegistry::new(config);

    let breaker1 = registry.get_or_create("service1");
    let breaker2 = registry.get_or_create("service2");
    let breaker1_again = registry.get_or_create("service1");

    assert!(Arc::ptr_eq(&breaker1, &breaker1_again)); // Same instance
    assert!(!Arc::ptr_eq(&breaker1, &breaker2)); // Different instances

    // Breakers from registry should be enabled by default
    assert!(breaker1.is_enabled());
    assert!(breaker2.is_enabled());

    let all_stats = registry.get_all_statistics();
    assert_eq!(all_stats.len(), 2);
    assert!(all_stats.contains_key("service1"));
    assert!(all_stats.contains_key("service2"));

    registry.reset_all();
    let stats1 = breaker1.get_stats();
    assert_eq!(stats1.total_calls, 0);
}

// ===== CONFIGURATION INTEGRATION TESTS =====

#[test]
fn test_streaming_config_phase2_defaults() {
    let config = StreamingConfig::default();

    // Phase 2 should be disabled by default for backward compatibility
    assert!(!config.enable_circuit_breakers);
    assert!(!config.enable_resource_monitoring);
    assert!(config.circuit_breaker_config.is_none());
    assert!(config.resource_monitoring_config.is_none());
}

#[test]
fn test_streaming_config_enhanced_includes_phase2() {
    let config = StreamingConfig::enhanced();

    // Enhanced config should enable all Phase 2 features
    assert!(config.enable_circuit_breakers);
    assert!(config.enable_resource_monitoring);
    assert!(config.circuit_breaker_config.is_some());
    assert!(config.resource_monitoring_config.is_some());
}

#[test]
fn test_streaming_config_phase2_fluent_api() {
    let config = StreamingConfig::new()
        .with_circuit_breakers()
        .with_resource_monitoring()
        .with_enhanced_error_handling();

    assert!(config.enable_circuit_breakers);
    assert!(config.enable_resource_monitoring);
    assert!(config.enable_enhanced_errors);

    // Test custom configurations
    let custom_cb_config = ConfigCircuitBreakerConfig::development();
    let custom_monitor_config = ResourceMonitoringConfig::development();

    let custom_config = StreamingConfig::new()
        .with_circuit_breaker_config(custom_cb_config.clone())
        .with_resource_monitoring_config(custom_monitor_config.clone());

    assert!(custom_config.enable_circuit_breakers);
    assert!(custom_config.enable_resource_monitoring);
    assert!(custom_config.circuit_breaker_config.is_some());
    assert!(custom_config.resource_monitoring_config.is_some());
}

#[test]
fn test_phase2_config_presets() {
    // Development preset
    let dev_cb_config = ConfigCircuitBreakerConfig::development();
    assert_eq!(dev_cb_config.failure_threshold, 3);
    assert_eq!(dev_cb_config.recovery_timeout_seconds, 10);

    let dev_monitor_config = ResourceMonitoringConfig::development();
    assert_eq!(dev_monitor_config.check_interval_seconds, 10);
    assert!(dev_monitor_config.enable_auto_cleanup);

    // Production preset
    let prod_cb_config = ConfigCircuitBreakerConfig::production();
    assert_eq!(prod_cb_config.failure_threshold, 10);
    assert_eq!(prod_cb_config.recovery_timeout_seconds, 300);

    let prod_monitor_config = ResourceMonitoringConfig::production();
    assert_eq!(prod_monitor_config.check_interval_seconds, 60);
    assert!(!prod_monitor_config.enable_auto_cleanup);
}

// ===== INTEGRATION TESTS =====

#[tokio::test]
async fn test_phase2_components_integration() {
    // Test that all Phase 2 components work together
    let limits = ResourceLimits {
        max_total_memory: Some(1000),
        ..Default::default()
    };
    let mut resource_manager = ResourceManager::new(limits);
    resource_manager.enable();

    let mut circuit_breaker = CircuitBreaker::with_default_config("integration_test".to_string());
    circuit_breaker.enable();

    // Simulate resource usage that would trigger circuit breaker
    resource_manager
        .update_resource_usage("total_memory", 1200)
        .unwrap_err(); // Should error

    // Simulate circuit breaker protecting against failures
    let result: Result<(), StreamingError> = circuit_breaker
        .execute(|| {
            Err(StreamingError::ResourceExhausted {
                resource_type: "memory".to_string(),
                current_usage: 1200,
                limit: 1000,
                message: "Resource exhausted".to_string(),
            })
        })
        .await;

    assert!(result.is_err());
    let stats = circuit_breaker.get_stats();
    assert_eq!(stats.total_failures, 1);
}

#[test]
fn test_backward_compatibility_preservation() {
    // Test that Phase 2 features don't break existing functionality
    let default_config = StreamingConfig::default();

    // All Phase 2 features should be disabled by default
    assert!(!default_config.enable_circuit_breakers);
    assert!(!default_config.enable_resource_monitoring);

    // Resource manager should allow all operations when disabled
    let limits = ResourceLimits {
        max_total_memory: Some(1),
        ..Default::default()
    };
    let resource_manager = ResourceManager::new(limits);
    assert!(
        resource_manager
            .update_resource_usage("total_memory", 1000000)
            .is_ok()
    );

    // Circuit breaker should allow all operations when disabled
    let circuit_breaker = CircuitBreaker::with_default_config("test".to_string());
    assert!(!circuit_breaker.is_enabled());
}
