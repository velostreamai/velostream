/*!
# Circuit Breaker for Streaming Operations

Circuit breaker implementation for streaming SQL operations with retry logic
and automatic recovery. Prevents cascading failures and provides graceful
degradation during system overload or repeated failures.

## Design Philosophy

This circuit breaker system follows Phase 2 requirements:
- **Optional Component**: Only activated when enhanced error handling is enabled
- **Backward Compatible**: Existing operations continue without circuit breakers
- **Progressive Enhancement**: Can be enabled via StreamingConfig
- **Production Ready**: Comprehensive failure detection and recovery strategies
*/

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use crate::ferris::sql::execution::error::StreamingError;

/// Circuit breaker states following the standard circuit breaker pattern
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    /// Circuit is closed - normal operation
    Closed,
    /// Circuit is open - failing fast to prevent cascade failures  
    Open,
    /// Circuit is half-open - testing if service has recovered
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    pub failure_threshold: u32,

    /// Time to wait before attempting recovery (half-open state)
    pub recovery_timeout: Duration,

    /// Number of successful calls in half-open state before closing circuit
    pub success_threshold: u32,

    /// Timeout for individual operations
    pub operation_timeout: Duration,

    /// Window size for failure rate calculation
    pub failure_rate_window: Duration,

    /// Minimum number of calls in window before calculating failure rate
    pub min_calls_in_window: u32,

    /// Failure rate threshold (percentage) to open circuit
    pub failure_rate_threshold: f64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 3,
            operation_timeout: Duration::from_secs(10),
            failure_rate_window: Duration::from_secs(60),
            min_calls_in_window: 10,
            failure_rate_threshold: 50.0, // 50%
        }
    }
}

/// Circuit breaker for protecting streaming operations
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Current state of the circuit breaker
    state: Arc<RwLock<CircuitBreakerState>>,

    /// Configuration
    config: CircuitBreakerConfig,

    /// Statistics tracking
    stats: Arc<RwLock<CircuitBreakerStats>>,

    /// Service name for logging/monitoring
    service_name: String,

    /// Whether circuit breaker is enabled
    enabled: bool,
}

#[derive(Debug, Clone)]
struct CircuitBreakerStats {
    /// Consecutive failure count
    consecutive_failures: u32,

    /// Consecutive success count in half-open state
    consecutive_successes: u32,

    /// Time when circuit was last opened
    last_failure_time: Option<SystemTime>,

    /// Time when circuit should attempt recovery
    next_retry_time: Option<SystemTime>,

    /// Recent call history for failure rate calculation
    call_history: Vec<CallRecord>,

    /// Total statistics
    total_calls: u64,
    total_successes: u64,
    total_failures: u64,
    total_timeouts: u64,
}

#[derive(Debug, Clone)]
struct CallRecord {
    timestamp: SystemTime,
    success: bool,
    duration: Duration,
}

impl Default for CircuitBreakerStats {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_failure_time: None,
            next_retry_time: None,
            call_history: Vec::new(),
            total_calls: 0,
            total_successes: 0,
            total_failures: 0,
            total_timeouts: 0,
        }
    }
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(service_name: String, config: CircuitBreakerConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
            config,
            stats: Arc::new(RwLock::new(CircuitBreakerStats::default())),
            service_name,
            enabled: false, // Disabled by default for backward compatibility
        }
    }

    /// Create circuit breaker with default configuration
    pub fn with_default_config(service_name: String) -> Self {
        Self::new(service_name, CircuitBreakerConfig::default())
    }

    /// Enable circuit breaker
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable circuit breaker
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Check if circuit breaker is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Execute an operation with circuit breaker protection
    pub async fn execute<F, R>(&self, operation: F) -> Result<R, StreamingError>
    where
        F: FnOnce() -> Result<R, StreamingError> + Send + 'static,
        R: Send + 'static,
    {
        if !self.enabled {
            return operation();
        }

        // Check if circuit allows the call
        self.check_circuit_state()?;

        let start_time = SystemTime::now();

        // Execute the operation with timeout
        let result = tokio::time::timeout(self.config.operation_timeout, async {
            tokio::task::spawn_blocking(operation).await.map_err(|e| {
                StreamingError::MessagePassingError {
                    operation: "circuit_breaker_execution".to_string(),
                    message: format!("Task execution failed: {}", e),
                    retry_possible: true,
                }
            })?
        })
        .await;

        let duration = start_time.elapsed().unwrap_or(Duration::ZERO);

        match result {
            Ok(Ok(value)) => {
                self.record_success(duration);
                Ok(value)
            }
            Ok(Err(error)) => {
                self.record_failure(duration);
                Err(error)
            }
            Err(_) => {
                // Timeout occurred
                self.record_timeout(duration);
                Err(StreamingError::MessagePassingError {
                    operation: self.service_name.clone(),
                    message: format!(
                        "Operation timed out after {:?}",
                        self.config.operation_timeout
                    ),
                    retry_possible: true,
                })
            }
        }
    }

    /// Check current circuit breaker state
    pub fn get_state(&self) -> CircuitBreakerState {
        self.state.read().unwrap().clone()
    }

    /// Get circuit breaker statistics
    pub fn get_stats(&self) -> CircuitBreakerStatistics {
        let stats = self.stats.read().unwrap();
        CircuitBreakerStatistics {
            state: self.get_state(),
            consecutive_failures: stats.consecutive_failures,
            consecutive_successes: stats.consecutive_successes,
            total_calls: stats.total_calls,
            total_successes: stats.total_successes,
            total_failures: stats.total_failures,
            total_timeouts: stats.total_timeouts,
            failure_rate: self.calculate_failure_rate(),
            last_failure_time: stats.last_failure_time,
            next_retry_time: stats.next_retry_time,
        }
    }

    /// Force circuit breaker to open (for testing/maintenance)
    pub fn force_open(&self) {
        *self.state.write().unwrap() = CircuitBreakerState::Open;
        let mut stats = self.stats.write().unwrap();
        stats.last_failure_time = Some(SystemTime::now());
        stats.next_retry_time = Some(SystemTime::now() + self.config.recovery_timeout);
    }

    /// Force circuit breaker to close (for testing/recovery)
    pub fn force_close(&self) {
        *self.state.write().unwrap() = CircuitBreakerState::Closed;
        let mut stats = self.stats.write().unwrap();
        stats.consecutive_failures = 0;
        stats.consecutive_successes = 0;
        stats.last_failure_time = None;
        stats.next_retry_time = None;
    }

    /// Reset circuit breaker statistics
    pub fn reset(&self) {
        *self.stats.write().unwrap() = CircuitBreakerStats::default();
        *self.state.write().unwrap() = CircuitBreakerState::Closed;
    }

    // Private implementation methods

    fn check_circuit_state(&self) -> Result<(), StreamingError> {
        let current_state = self.get_state();

        match current_state {
            CircuitBreakerState::Closed => Ok(()),
            CircuitBreakerState::HalfOpen => Ok(()), // Allow call in half-open state
            CircuitBreakerState::Open => {
                let stats = self.stats.read().unwrap();
                if let Some(next_retry) = stats.next_retry_time {
                    if SystemTime::now() >= next_retry {
                        // Transition to half-open
                        drop(stats);
                        *self.state.write().unwrap() = CircuitBreakerState::HalfOpen;
                        Ok(())
                    } else {
                        Err(StreamingError::CircuitBreakerOpen {
                            service: self.service_name.clone(),
                            failure_count: stats.consecutive_failures,
                            last_failure_time: stats.last_failure_time.unwrap_or(SystemTime::now()),
                            next_retry_time: next_retry,
                        })
                    }
                } else {
                    // No retry time set, transition to half-open
                    drop(stats);
                    *self.state.write().unwrap() = CircuitBreakerState::HalfOpen;
                    Ok(())
                }
            }
        }
    }

    fn record_success(&self, duration: Duration) {
        let mut stats = self.stats.write().unwrap();
        stats.total_calls += 1;
        stats.total_successes += 1;
        stats.consecutive_failures = 0;

        // Add to call history
        stats.call_history.push(CallRecord {
            timestamp: SystemTime::now(),
            success: true,
            duration,
        });
        self.cleanup_call_history(&mut stats.call_history);

        let current_state = self.get_state();
        match current_state {
            CircuitBreakerState::HalfOpen => {
                stats.consecutive_successes += 1;
                if stats.consecutive_successes >= self.config.success_threshold {
                    // Close the circuit
                    drop(stats);
                    *self.state.write().unwrap() = CircuitBreakerState::Closed;
                }
            }
            _ => {}
        }
    }

    fn record_failure(&self, duration: Duration) {
        let mut stats = self.stats.write().unwrap();
        stats.total_calls += 1;
        stats.total_failures += 1;
        stats.consecutive_failures += 1;
        stats.consecutive_successes = 0;
        stats.last_failure_time = Some(SystemTime::now());

        // Add to call history
        stats.call_history.push(CallRecord {
            timestamp: SystemTime::now(),
            success: false,
            duration,
        });
        self.cleanup_call_history(&mut stats.call_history);

        // Check if we should open the circuit
        let should_open = stats.consecutive_failures >= self.config.failure_threshold
            || self.calculate_failure_rate() >= self.config.failure_rate_threshold;

        if should_open {
            stats.next_retry_time = Some(SystemTime::now() + self.config.recovery_timeout);
            drop(stats);
            *self.state.write().unwrap() = CircuitBreakerState::Open;
        }
    }

    fn record_timeout(&self, duration: Duration) {
        let mut stats = self.stats.write().unwrap();
        stats.total_calls += 1;
        stats.total_timeouts += 1;
        stats.consecutive_failures += 1;
        stats.consecutive_successes = 0;
        stats.last_failure_time = Some(SystemTime::now());

        // Treat timeout as failure
        stats.call_history.push(CallRecord {
            timestamp: SystemTime::now(),
            success: false,
            duration,
        });
        self.cleanup_call_history(&mut stats.call_history);
    }

    fn calculate_failure_rate(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        let cutoff_time = SystemTime::now() - self.config.failure_rate_window;

        let recent_calls: Vec<_> = stats
            .call_history
            .iter()
            .filter(|call| call.timestamp > cutoff_time)
            .collect();

        if recent_calls.len() < self.config.min_calls_in_window as usize {
            return 0.0; // Not enough data
        }

        let failure_count = recent_calls.iter().filter(|call| !call.success).count();
        (failure_count as f64 / recent_calls.len() as f64) * 100.0
    }

    fn cleanup_call_history(&self, history: &mut Vec<CallRecord>) {
        let cutoff_time = SystemTime::now() - self.config.failure_rate_window;
        history.retain(|call| call.timestamp > cutoff_time);

        // Also limit total size to prevent unbounded growth
        const MAX_HISTORY_SIZE: usize = 1000;
        if history.len() > MAX_HISTORY_SIZE {
            history.drain(0..history.len() - MAX_HISTORY_SIZE);
        }
    }
}

/// Public statistics structure
#[derive(Debug, Clone)]
pub struct CircuitBreakerStatistics {
    pub state: CircuitBreakerState,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub total_calls: u64,
    pub total_successes: u64,
    pub total_failures: u64,
    pub total_timeouts: u64,
    pub failure_rate: f64,
    pub last_failure_time: Option<SystemTime>,
    pub next_retry_time: Option<SystemTime>,
}

/// Circuit breaker registry for managing multiple circuit breakers
#[derive(Debug)]
pub struct CircuitBreakerRegistry {
    breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
    default_config: CircuitBreakerConfig,
}

impl CircuitBreakerRegistry {
    pub fn new(default_config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: Arc::new(RwLock::new(HashMap::new())),
            default_config,
        }
    }

    pub fn get_or_create(&self, service_name: &str) -> Arc<CircuitBreaker> {
        let mut breakers = self.breakers.write().unwrap();
        breakers
            .entry(service_name.to_string())
            .or_insert_with(|| {
                let mut breaker =
                    CircuitBreaker::new(service_name.to_string(), self.default_config.clone());
                breaker.enable(); // Enable by default in registry
                Arc::new(breaker)
            })
            .clone()
    }

    pub fn get_all_statistics(&self) -> HashMap<String, CircuitBreakerStatistics> {
        let breakers = self.breakers.read().unwrap();
        breakers
            .iter()
            .map(|(name, breaker)| (name.clone(), breaker.get_stats()))
            .collect()
    }

    pub fn reset_all(&self) {
        let breakers = self.breakers.read().unwrap();
        for breaker in breakers.values() {
            breaker.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

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
        let result: Result<(), StreamingError> = breaker
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
    }

    #[tokio::test]
    async fn test_successful_operation() {
        let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
        breaker.enable();

        let result: Result<&str, StreamingError> = breaker.execute(|| Ok("success")).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");

        let stats = breaker.get_stats();
        assert_eq!(stats.total_calls, 1);
        assert_eq!(stats.total_successes, 1);
        assert_eq!(stats.total_failures, 0);
    }

    #[tokio::test]
    async fn test_failed_operation() {
        // Add timeout to prevent hanging in CI
        let test_result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
            breaker.enable();

            let result: Result<(), StreamingError> = breaker
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
        })
        .await;

        assert!(test_result.is_ok(), "Test timed out after 5 seconds");
    }

    #[tokio::test]
    async fn test_circuit_opens_after_failures() {
        // Add timeout to prevent hanging in CI
        let test_result = tokio::time::timeout(Duration::from_secs(8), async {
            let config = CircuitBreakerConfig {
                failure_threshold: 2, // Open after 2 failures
                ..Default::default()
            };
            let mut breaker = CircuitBreaker::new("test_service".to_string(), config);
            breaker.enable();

            // First failure
            let _: Result<(), StreamingError> = breaker
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
        })
        .await;

        assert!(test_result.is_ok(), "Test timed out after 8 seconds");
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
    async fn test_force_operations() {
        let mut breaker = CircuitBreaker::with_default_config("test_service".to_string());
        breaker.enable();

        breaker.force_open();
        assert_eq!(breaker.get_state(), CircuitBreakerState::Open);

        breaker.force_close();
        assert_eq!(breaker.get_state(), CircuitBreakerState::Closed);

        let stats = breaker.get_stats();
        assert_eq!(stats.consecutive_failures, 0);
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
    }
}
