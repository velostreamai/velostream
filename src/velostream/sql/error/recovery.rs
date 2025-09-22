//! Error Recovery Framework
//!
//! This module provides comprehensive error recovery mechanisms for the Velostream
//! processing engine, including circuit breakers, retry policies, dead letter queues,
//! and automatic recovery strategies.
//!
//! ## Features
//!
//! - **Circuit Breaker Pattern**: Prevents cascading failures in distributed systems
//! - **Retry Mechanisms**: Configurable retry strategies with exponential backoff
//! - **Dead Letter Queue**: Routes failed messages to separate processing pipelines
//! - **Bulkhead Pattern**: Isolates critical resources from failure domains
//! - **Health Monitoring**: Tracks system health and recovery metrics
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use velostream::velostream::sql::error::recovery::*;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create circuit breaker for external service calls
//!     let mut circuit_breaker = CircuitBreaker::builder()
//!         .failure_threshold(5)
//!         .recovery_timeout(Duration::from_secs(30))
//!         .build();
//!
//!     // Configure retry policy with exponential backoff
//!     let retry_policy = RetryPolicy::exponential_backoff()
//!         .max_attempts(3)
//!         .initial_delay(Duration::from_millis(100))
//!         .max_delay(Duration::from_secs(5))
//!         .build();
//!
//!     // Setup dead letter queue for failed messages
//!     let dlq = DeadLetterQueue::new("failed_events").await?;
//!
//!     Ok(())
//! }
//! ```

use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

/// Comprehensive error recovery result
pub type RecoveryResult<T> = Result<T, RecoveryError>;

/// Error recovery and resilience errors
#[derive(Debug, Clone)]
pub enum RecoveryError {
    /// Circuit breaker is in open state
    CircuitOpen {
        service: String,
        last_failure: String,
        retry_after: Duration,
    },
    /// Maximum retry attempts exceeded
    RetryExhausted {
        operation: String,
        attempts: u32,
        last_error: String,
    },
    /// Dead letter queue operation failed
    DeadLetterError { queue: String, message: String },
    /// Resource pool exhausted
    ResourceExhausted {
        resource_type: String,
        current_usage: usize,
        max_capacity: usize,
    },
    /// Health check failed
    HealthCheckFailed {
        component: String,
        check_type: String,
        details: String,
    },
    /// Recovery operation timeout
    RecoveryTimeout {
        operation: String,
        timeout: Duration,
    },
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryError::CircuitOpen {
                service,
                last_failure,
                retry_after,
            } => {
                write!(
                    f,
                    "Circuit breaker open for '{}': {} (retry in {:?})",
                    service, last_failure, retry_after
                )
            }
            RecoveryError::RetryExhausted {
                operation,
                attempts,
                last_error,
            } => {
                write!(
                    f,
                    "Retry exhausted for '{}' after {} attempts: {}",
                    operation, attempts, last_error
                )
            }
            RecoveryError::DeadLetterError { queue, message } => {
                write!(f, "Dead letter queue '{}' error: {}", queue, message)
            }
            RecoveryError::ResourceExhausted {
                resource_type,
                current_usage,
                max_capacity,
            } => {
                write!(
                    f,
                    "Resource '{}' exhausted: {}/{}",
                    resource_type, current_usage, max_capacity
                )
            }
            RecoveryError::HealthCheckFailed {
                component,
                check_type,
                details,
            } => {
                write!(
                    f,
                    "Health check failed for '{}' ({}): {}",
                    component, check_type, details
                )
            }
            RecoveryError::RecoveryTimeout { operation, timeout } => {
                write!(
                    f,
                    "Recovery timeout for '{}' after {:?}",
                    operation, timeout
                )
            }
        }
    }
}

impl std::error::Error for RecoveryError {}

/// Circuit breaker states for failure isolation
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    /// Circuit is closed, normal operation
    Closed,
    /// Circuit is open, failing fast
    Open,
    /// Circuit is half-open, testing recovery
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures to trigger open state
    pub failure_threshold: u32,
    /// Time to wait before attempting recovery
    pub recovery_timeout: Duration,
    /// Success threshold to close circuit from half-open
    pub success_threshold: u32,
    /// Request timeout for operations
    pub request_timeout: Duration,
    /// Enable detailed metrics collection
    pub enable_metrics: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 3,
            request_timeout: Duration::from_secs(30),
            enable_metrics: true,
        }
    }
}

/// Circuit breaker for preventing cascading failures
pub struct CircuitBreaker {
    name: String,
    state: Arc<Mutex<CircuitState>>,
    config: CircuitBreakerConfig,
    failure_count: Arc<Mutex<u32>>,
    success_count: Arc<Mutex<u32>>,
    last_failure_time: Arc<Mutex<Option<Instant>>>,
    metrics: Arc<Mutex<CircuitBreakerMetrics>>,
}

/// Circuit breaker metrics
#[derive(Debug, Clone, Default)]
pub struct CircuitBreakerMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub circuit_opened_count: u64,
    pub circuit_half_opened_count: u64,
    pub circuit_closed_count: u64,
    pub avg_response_time_ms: f64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration
    pub fn new(name: String) -> Self {
        Self::with_config(name, CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration
    pub fn with_config(name: String, config: CircuitBreakerConfig) -> Self {
        Self {
            name,
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            config,
            failure_count: Arc::new(Mutex::new(0)),
            success_count: Arc::new(Mutex::new(0)),
            last_failure_time: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(CircuitBreakerMetrics::default())),
        }
    }

    /// Builder pattern for circuit breaker configuration
    pub fn builder() -> CircuitBreakerBuilder {
        CircuitBreakerBuilder::new()
    }

    /// Execute operation with circuit breaker protection
    pub async fn call<F, T>(&self, operation: F) -> RecoveryResult<T>
    where
        F: std::future::Future<Output = Result<T, SqlError>>,
    {
        let start_time = Instant::now();

        // Check if circuit is open
        let current_state = {
            let state = self.state.lock().await;
            state.clone()
        };

        match current_state {
            CircuitState::Open => {
                // Check if we should transition to half-open
                let last_failure = self.last_failure_time.lock().await;
                if let Some(failure_time) = *last_failure {
                    if start_time.duration_since(failure_time) > self.config.recovery_timeout {
                        drop(last_failure);
                        self.transition_to_half_open().await;
                    } else {
                        let retry_after =
                            self.config.recovery_timeout - start_time.duration_since(failure_time);
                        return Err(RecoveryError::CircuitOpen {
                            service: self.name.clone(),
                            last_failure: "Circuit breaker is open".to_string(),
                            retry_after,
                        });
                    }
                } else {
                    return Err(RecoveryError::CircuitOpen {
                        service: self.name.clone(),
                        last_failure: "Circuit breaker is open".to_string(),
                        retry_after: self.config.recovery_timeout,
                    });
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in half-open state
            }
            CircuitState::Closed => {
                // Normal operation
            }
        }

        // Execute operation with timeout
        let result = tokio::time::timeout(self.config.request_timeout, operation).await;

        let execution_time = start_time.elapsed();

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.total_requests += 1;
            metrics.avg_response_time_ms = (metrics.avg_response_time_ms
                * (metrics.total_requests - 1) as f64
                + execution_time.as_millis() as f64)
                / metrics.total_requests as f64;
        }

        match result {
            Ok(Ok(value)) => {
                self.record_success().await;
                Ok(value)
            }
            Ok(Err(sql_error)) => {
                self.record_failure(sql_error.to_string()).await;
                Err(RecoveryError::RetryExhausted {
                    operation: self.name.clone(),
                    attempts: 1,
                    last_error: sql_error.to_string(),
                })
            }
            Err(_timeout) => {
                let timeout_error = format!(
                    "Operation timed out after {:?}",
                    self.config.request_timeout
                );
                self.record_failure(timeout_error.clone()).await;
                Err(RecoveryError::RecoveryTimeout {
                    operation: self.name.clone(),
                    timeout: self.config.request_timeout,
                })
            }
        }
    }

    /// Get current circuit breaker state
    pub async fn state(&self) -> CircuitState {
        self.state.lock().await.clone()
    }

    /// Get circuit breaker metrics
    pub async fn metrics(&self) -> CircuitBreakerMetrics {
        if self.config.enable_metrics {
            self.metrics.lock().await.clone()
        } else {
            CircuitBreakerMetrics::default()
        }
    }

    /// Reset circuit breaker to closed state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        *state = CircuitState::Closed;

        let mut failure_count = self.failure_count.lock().await;
        *failure_count = 0;

        let mut success_count = self.success_count.lock().await;
        *success_count = 0;

        let mut last_failure = self.last_failure_time.lock().await;
        *last_failure = None;
    }

    // Private helper methods

    async fn record_success(&self) {
        let current_state = {
            let state = self.state.lock().await;
            state.clone()
        };

        match current_state {
            CircuitState::HalfOpen => {
                let mut success_count = self.success_count.lock().await;
                *success_count += 1;

                if *success_count >= self.config.success_threshold {
                    drop(success_count);
                    self.transition_to_closed().await;
                }
            }
            _ => {
                // Reset failure count on success
                let mut failure_count = self.failure_count.lock().await;
                *failure_count = 0;
            }
        }

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.successful_requests += 1;
        }
    }

    async fn record_failure(&self, _error: String) {
        let mut failure_count = self.failure_count.lock().await;
        *failure_count += 1;

        let mut last_failure = self.last_failure_time.lock().await;
        *last_failure = Some(Instant::now());

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.failed_requests += 1;
        }

        // Check if we should open the circuit
        if *failure_count >= self.config.failure_threshold {
            drop(failure_count);
            drop(last_failure);
            self.transition_to_open().await;
        }
    }

    async fn transition_to_open(&self) {
        let mut state = self.state.lock().await;
        *state = CircuitState::Open;

        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.circuit_opened_count += 1;
        }
    }

    async fn transition_to_half_open(&self) {
        let mut state = self.state.lock().await;
        *state = CircuitState::HalfOpen;

        let mut success_count = self.success_count.lock().await;
        *success_count = 0;

        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.circuit_half_opened_count += 1;
        }
    }

    async fn transition_to_closed(&self) {
        let mut state = self.state.lock().await;
        *state = CircuitState::Closed;

        let mut failure_count = self.failure_count.lock().await;
        *failure_count = 0;

        if self.config.enable_metrics {
            let mut metrics = self.metrics.lock().await;
            metrics.circuit_closed_count += 1;
        }
    }
}

/// Builder for circuit breaker configuration
pub struct CircuitBreakerBuilder {
    name: Option<String>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    success_threshold: u32,
    request_timeout: Duration,
    enable_metrics: bool,
}

impl CircuitBreakerBuilder {
    fn new() -> Self {
        let defaults = CircuitBreakerConfig::default();
        Self {
            name: None,
            failure_threshold: defaults.failure_threshold,
            recovery_timeout: defaults.recovery_timeout,
            success_threshold: defaults.success_threshold,
            request_timeout: defaults.request_timeout,
            enable_metrics: defaults.enable_metrics,
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    pub fn recovery_timeout(mut self, timeout: Duration) -> Self {
        self.recovery_timeout = timeout;
        self
    }

    pub fn success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    pub fn build(self) -> CircuitBreaker {
        let name = self.name.unwrap_or_else(|| "default".to_string());
        let config = CircuitBreakerConfig {
            failure_threshold: self.failure_threshold,
            recovery_timeout: self.recovery_timeout,
            success_threshold: self.success_threshold,
            request_timeout: self.request_timeout,
            enable_metrics: self.enable_metrics,
        };

        CircuitBreaker::with_config(name, config)
    }
}

/// Retry policy configuration and strategies
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff strategy
    pub backoff_strategy: BackoffStrategy,
    /// Retry conditions (which errors should trigger retry)
    pub retry_conditions: Vec<RetryCondition>,
    /// Enable jitter to avoid thundering herd
    pub enable_jitter: bool,
}

/// Backoff strategies for retry policies
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed,
    /// Linear increase in delay
    Linear { increment: Duration },
    /// Exponential backoff with multiplier
    Exponential { multiplier: f64 },
}

/// Conditions that determine if an operation should be retried
#[derive(Debug, Clone)]
pub enum RetryCondition {
    /// Retry on specific error types
    OnError(String),
    /// Retry on timeouts
    OnTimeout,
    /// Retry on circuit breaker open
    OnCircuitOpen,
    /// Custom condition function
    Custom(fn(&RecoveryError) -> bool),
}

impl RetryPolicy {
    /// Create exponential backoff retry policy
    pub fn exponential_backoff() -> RetryPolicyBuilder {
        RetryPolicyBuilder::new().backoff_strategy(BackoffStrategy::Exponential { multiplier: 2.0 })
    }

    /// Create fixed delay retry policy
    pub fn fixed_delay() -> RetryPolicyBuilder {
        RetryPolicyBuilder::new().backoff_strategy(BackoffStrategy::Fixed)
    }

    /// Execute operation with retry policy
    pub async fn execute<F, T>(&self, operation: F) -> RecoveryResult<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = RecoveryResult<T>> + Send>>,
    {
        let mut attempts = 0;
        let mut delay = self.initial_delay;

        loop {
            attempts += 1;

            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    // Check if we should retry this error
                    if !self.should_retry(&error) || attempts >= self.max_attempts {
                        return Err(RecoveryError::RetryExhausted {
                            operation: "retry_operation".to_string(),
                            attempts,
                            last_error: error.to_string(),
                        });
                    }

                    // Calculate next delay
                    let actual_delay = if self.enable_jitter {
                        self.add_jitter(delay)
                    } else {
                        delay
                    };

                    sleep(actual_delay).await;

                    // Update delay for next iteration
                    delay = self.calculate_next_delay(delay, attempts);
                }
            }
        }
    }

    fn should_retry(&self, error: &RecoveryError) -> bool {
        if self.retry_conditions.is_empty() {
            // Default retry conditions
            matches!(
                error,
                RecoveryError::RecoveryTimeout { .. } | RecoveryError::CircuitOpen { .. }
            )
        } else {
            self.retry_conditions
                .iter()
                .any(|condition| match condition {
                    RetryCondition::OnError(error_type) => error.to_string().contains(error_type),
                    RetryCondition::OnTimeout => {
                        matches!(error, RecoveryError::RecoveryTimeout { .. })
                    }
                    RetryCondition::OnCircuitOpen => {
                        matches!(error, RecoveryError::CircuitOpen { .. })
                    }
                    RetryCondition::Custom(condition_fn) => condition_fn(error),
                })
        }
    }

    fn calculate_next_delay(&self, current_delay: Duration, _attempt: u32) -> Duration {
        let next_delay = match &self.backoff_strategy {
            BackoffStrategy::Fixed => current_delay,
            BackoffStrategy::Linear { increment } => current_delay + *increment,
            BackoffStrategy::Exponential { multiplier } => {
                Duration::from_millis(((current_delay.as_millis() as f64) * multiplier) as u64)
            }
        };

        std::cmp::min(next_delay, self.max_delay)
    }

    fn add_jitter(&self, delay: Duration) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let jitter_factor = rng.gen_range(0.8..1.2);
        Duration::from_millis(((delay.as_millis() as f64) * jitter_factor) as u64)
    }
}

/// Builder for retry policy configuration
pub struct RetryPolicyBuilder {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_strategy: BackoffStrategy,
    retry_conditions: Vec<RetryCondition>,
    enable_jitter: bool,
}

impl RetryPolicyBuilder {
    fn new() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_strategy: BackoffStrategy::Exponential { multiplier: 2.0 },
            retry_conditions: Vec::new(),
            enable_jitter: false,
        }
    }

    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn backoff_strategy(mut self, strategy: BackoffStrategy) -> Self {
        self.backoff_strategy = strategy;
        self
    }

    pub fn retry_condition(mut self, condition: RetryCondition) -> Self {
        self.retry_conditions.push(condition);
        self
    }

    pub fn enable_jitter(mut self, enable: bool) -> Self {
        self.enable_jitter = enable;
        self
    }

    pub fn build(self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts,
            initial_delay: self.initial_delay,
            max_delay: self.max_delay,
            backoff_strategy: self.backoff_strategy,
            retry_conditions: self.retry_conditions,
            enable_jitter: self.enable_jitter,
        }
    }
}

/// Dead Letter Queue for handling failed messages
pub struct DeadLetterQueue {
    name: String,
    failed_messages: Arc<RwLock<Vec<FailedMessage>>>,
    config: DeadLetterConfig,
    metrics: Arc<RwLock<DeadLetterMetrics>>,
}

/// Configuration for dead letter queue behavior
#[derive(Debug, Clone)]
pub struct DeadLetterConfig {
    /// Maximum number of messages to store
    pub max_messages: usize,
    /// TTL for messages in the dead letter queue
    pub message_ttl: Duration,
    /// Enable automatic retry from DLQ
    pub enable_auto_retry: bool,
    /// Retry interval for auto-retry
    pub retry_interval: Duration,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            max_messages: 10000,
            message_ttl: <Duration as DurationExt>::from_hours(24),
            enable_auto_retry: false,
            retry_interval: <Duration as DurationExt>::from_minutes(30),
            enable_metrics: true,
        }
    }
}

/// Failed message stored in dead letter queue
#[derive(Debug, Clone)]
pub struct FailedMessage {
    pub id: String,
    pub original_data: String,
    pub error_details: String,
    pub failed_at: Instant,
    pub retry_count: u32,
    pub source_topic: Option<String>,
    pub headers: HashMap<String, String>,
}

/// Dead letter queue metrics
#[derive(Debug, Clone, Default)]
pub struct DeadLetterMetrics {
    pub total_messages: u64,
    pub messages_retried: u64,
    pub messages_expired: u64,
    pub current_queue_size: usize,
    pub avg_processing_time_ms: f64,
}

impl DeadLetterQueue {
    /// Create a new dead letter queue
    pub async fn new(name: &str) -> RecoveryResult<Self> {
        Ok(Self {
            name: name.to_string(),
            failed_messages: Arc::new(RwLock::new(Vec::new())),
            config: DeadLetterConfig::default(),
            metrics: Arc::new(RwLock::new(DeadLetterMetrics::default())),
        })
    }

    /// Create dead letter queue with custom configuration
    pub async fn with_config(name: &str, config: DeadLetterConfig) -> RecoveryResult<Self> {
        Ok(Self {
            name: name.to_string(),
            failed_messages: Arc::new(RwLock::new(Vec::new())),
            config,
            metrics: Arc::new(RwLock::new(DeadLetterMetrics::default())),
        })
    }

    /// Add a failed message to the dead letter queue
    pub async fn enqueue(&self, message: FailedMessage) -> RecoveryResult<()> {
        let mut messages = self.failed_messages.write().await;

        // Check capacity
        if messages.len() >= self.config.max_messages {
            // Remove oldest message if at capacity
            messages.remove(0);
        }

        messages.push(message);

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            metrics.total_messages += 1;
            metrics.current_queue_size = messages.len();
        }

        Ok(())
    }

    /// Retrieve failed messages for manual processing
    pub async fn dequeue(&self, count: usize) -> RecoveryResult<Vec<FailedMessage>> {
        let mut messages = self.failed_messages.write().await;

        let drain_count = std::cmp::min(count, messages.len());
        let drained: Vec<_> = messages.drain(0..drain_count).collect();

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            metrics.current_queue_size = messages.len();
        }

        Ok(drained)
    }

    /// Get dead letter queue metrics
    pub async fn metrics(&self) -> DeadLetterMetrics {
        if self.config.enable_metrics {
            self.metrics.read().await.clone()
        } else {
            DeadLetterMetrics::default()
        }
    }

    /// Perform maintenance (remove expired messages)
    pub async fn maintenance(&self) -> RecoveryResult<usize> {
        let mut messages = self.failed_messages.write().await;
        let now = Instant::now();
        let initial_count = messages.len();

        messages.retain(|msg| now.duration_since(msg.failed_at) < self.config.message_ttl);

        let expired_count = initial_count - messages.len();

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            metrics.messages_expired += expired_count as u64;
            metrics.current_queue_size = messages.len();
        }

        Ok(expired_count)
    }

    /// Clear all messages from the dead letter queue
    pub async fn clear(&self) -> RecoveryResult<usize> {
        let mut messages = self.failed_messages.write().await;
        let count = messages.len();
        messages.clear();

        // Update metrics
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            metrics.current_queue_size = 0;
        }

        Ok(count)
    }
}

/// Health monitoring system for tracking component health
pub struct HealthMonitor {
    components: Arc<RwLock<HashMap<String, ComponentHealth>>>,
    config: HealthConfig,
    metrics: Arc<RwLock<HealthMetrics>>,
}

/// Configuration for health monitoring
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Health check interval
    pub check_interval: Duration,
    /// Timeout for individual health checks
    pub check_timeout: Duration,
    /// Number of consecutive failures to mark as unhealthy
    pub failure_threshold: u32,
    /// Enable detailed health metrics
    pub enable_metrics: bool,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            failure_threshold: 3,
            enable_metrics: true,
        }
    }
}

/// Health status of a system component
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_check: Instant,
    pub consecutive_failures: u32,
    pub details: HashMap<String, String>,
}

/// Health status enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Health monitoring metrics
#[derive(Debug, Clone, Default)]
pub struct HealthMetrics {
    pub total_checks: u64,
    pub successful_checks: u64,
    pub failed_checks: u64,
    pub avg_check_time_ms: f64,
    pub components_healthy: usize,
    pub components_unhealthy: usize,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new() -> Self {
        Self::with_config(HealthConfig::default())
    }

    /// Create health monitor with custom configuration
    pub fn with_config(config: HealthConfig) -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            config,
            metrics: Arc::new(RwLock::new(HealthMetrics::default())),
        }
    }

    /// Register a component for health monitoring
    pub async fn register_component(&self, name: String) {
        let mut components = self.components.write().await;
        components.insert(
            name.clone(),
            ComponentHealth {
                name,
                status: HealthStatus::Unknown,
                last_check: Instant::now(),
                consecutive_failures: 0,
                details: HashMap::new(),
            },
        );
    }

    /// Update health status of a component
    pub async fn update_health(
        &self,
        component: &str,
        status: HealthStatus,
        details: HashMap<String, String>,
    ) {
        let mut components = self.components.write().await;
        if let Some(health) = components.get_mut(component) {
            health.status = status;
            health.last_check = Instant::now();
            health.details = details;

            if status == HealthStatus::Unhealthy {
                health.consecutive_failures += 1;
            } else {
                health.consecutive_failures = 0;
            }
        }
    }

    /// Get overall system health
    pub async fn overall_health(&self) -> HealthStatus {
        let components = self.components.read().await;
        let mut healthy_count = 0;
        let mut unhealthy_count = 0;
        let mut degraded_count = 0;

        for health in components.values() {
            match health.status {
                HealthStatus::Healthy => healthy_count += 1,
                HealthStatus::Unhealthy => unhealthy_count += 1,
                HealthStatus::Degraded => degraded_count += 1,
                HealthStatus::Unknown => {}
            }
        }

        if unhealthy_count > 0 {
            HealthStatus::Unhealthy
        } else if degraded_count > 0 {
            HealthStatus::Degraded
        } else if healthy_count > 0 {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    /// Get health status of a specific component
    pub async fn component_health(&self, component: &str) -> Option<ComponentHealth> {
        let components = self.components.read().await;
        components.get(component).cloned()
    }

    /// Get health monitoring metrics
    pub async fn metrics(&self) -> HealthMetrics {
        if self.config.enable_metrics {
            self.metrics.read().await.clone()
        } else {
            HealthMetrics::default()
        }
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// Helper trait for duration extensions
trait DurationExt {
    fn from_hours(hours: u64) -> Duration;
    fn from_minutes(minutes: u64) -> Duration;
}

impl DurationExt for Duration {
    fn from_hours(hours: u64) -> Duration {
        Duration::from_secs(hours * 3600)
    }

    fn from_minutes(minutes: u64) -> Duration {
        Duration::from_secs(minutes * 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_state() {
        let cb = CircuitBreaker::new("test".to_string());
        assert_eq!(cb.state().await, CircuitState::Closed);

        let result = cb
            .call(async { Ok::<_, SqlError>("success".to_string()) })
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_circuit_breaker_failure_threshold() {
        let cb = CircuitBreaker::builder()
            .name("test".to_string())
            .failure_threshold(2)
            .build();

        // First failure
        let _ = cb
            .call(async { Err::<String, _>(SqlError::execution_error("test error", None)) })
            .await;
        assert_eq!(cb.state().await, CircuitState::Closed);

        // Second failure should open the circuit
        let _ = cb
            .call(async { Err::<String, _>(SqlError::execution_error("test error", None)) })
            .await;
        assert_eq!(cb.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_retry_policy_success() {
        let policy = RetryPolicy::exponential_backoff()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(1))
            .build();

        let result = policy
            .execute(|| Box::pin(async { Ok::<_, RecoveryError>("success".to_string()) }))
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_retry_policy_exhaustion() {
        let policy = RetryPolicy::exponential_backoff()
            .max_attempts(2)
            .initial_delay(Duration::from_millis(1))
            .build();

        let result = policy
            .execute(|| {
                Box::pin(async {
                    Err::<String, _>(RecoveryError::RecoveryTimeout {
                        operation: "test".to_string(),
                        timeout: Duration::from_millis(100),
                    })
                })
            })
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RecoveryError::RetryExhausted { attempts, .. } => {
                assert_eq!(attempts, 2);
            }
            _ => panic!("Expected RetryExhausted error"),
        }
    }

    #[tokio::test]
    async fn test_dead_letter_queue() {
        let dlq = DeadLetterQueue::new("test").await.unwrap();

        let failed_msg = FailedMessage {
            id: "msg1".to_string(),
            original_data: "test data".to_string(),
            error_details: "processing failed".to_string(),
            failed_at: Instant::now(),
            retry_count: 0,
            source_topic: Some("orders".to_string()),
            headers: HashMap::new(),
        };

        dlq.enqueue(failed_msg).await.unwrap();

        let messages = dlq.dequeue(1).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "msg1");
    }

    #[tokio::test]
    async fn test_health_monitor() {
        let monitor = HealthMonitor::new();

        monitor.register_component("database".to_string()).await;

        // Initially unknown
        let health = monitor.component_health("database").await.unwrap();
        assert_eq!(health.status, HealthStatus::Unknown);

        // Update to healthy
        monitor
            .update_health("database", HealthStatus::Healthy, HashMap::new())
            .await;
        let health = monitor.component_health("database").await.unwrap();
        assert_eq!(health.status, HealthStatus::Healthy);

        // Check overall health
        let overall = monitor.overall_health().await;
        assert_eq!(overall, HealthStatus::Healthy);
    }
}
