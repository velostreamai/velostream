//! Configuration for the streaming execution engine
//!
//! This module provides configuration options for enhanced streaming capabilities
//! while maintaining backward compatibility with existing code.

/// Configuration for streaming engine enhancements
///
/// All enhancements are disabled by default to maintain backward compatibility.
/// Users can opt-in to specific features as needed.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Enable watermark-based event-time processing
    /// Default: false (uses existing processing-time behavior)
    pub enable_watermarks: bool,

    /// Enable enhanced error handling with circuit breakers and retry logic
    /// Default: false (uses existing error handling)
    pub enable_enhanced_errors: bool,

    /// Enable resource limits and state management
    /// Default: false (no resource checking)
    pub enable_resource_limits: bool,

    /// Message passing mode for execution
    /// Default: Legacy (exact same behavior as current implementation)
    pub message_passing_mode: MessagePassingMode,

    /// Strategy for handling late-arriving data
    /// Default: Drop (ignore late data)
    pub late_data_strategy: LateDataStrategy,

    /// Watermark generation strategy
    /// Default: None (no watermarks)
    pub watermark_strategy: WatermarkStrategy,

    /// Memory limit for total engine state (in bytes)
    /// Default: None (no limit)
    pub max_total_memory: Option<usize>,

    /// Maximum number of concurrent operations
    /// Default: None (no limit)
    pub max_concurrent_operations: Option<usize>,

    // === PHASE 2: ERROR & RESOURCE ENHANCEMENTS ===
    /// Enable circuit breaker protection for streaming operations
    /// Default: false (no circuit breaker protection)
    pub enable_circuit_breakers: bool,

    /// Enable resource monitoring and alerting
    /// Default: false (no resource monitoring)
    pub enable_resource_monitoring: bool,

    /// Circuit breaker configuration
    /// Default: None (use default circuit breaker settings)
    pub circuit_breaker_config: Option<CircuitBreakerConfig>,

    /// Resource monitoring configuration
    /// Default: None (use default monitoring settings)
    pub resource_monitoring_config: Option<ResourceMonitoringConfig>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            // All defaults preserve existing behavior - zero breaking changes
            enable_watermarks: false,
            enable_enhanced_errors: false,
            enable_resource_limits: false,
            message_passing_mode: MessagePassingMode::Legacy,
            late_data_strategy: LateDataStrategy::Drop,
            watermark_strategy: WatermarkStrategy::None,
            max_total_memory: None,
            max_concurrent_operations: None,
            // Phase 2 defaults - all disabled for backward compatibility
            enable_circuit_breakers: false,
            enable_resource_monitoring: false,
            circuit_breaker_config: None,
            resource_monitoring_config: None,
        }
    }
}

impl StreamingConfig {
    /// Create a new configuration with all enhancements disabled
    /// This is identical to Default::default() but more explicit
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration with conservative enhancements
    /// Enables better error reporting and resource limits, but keeps execution model unchanged
    pub fn conservative() -> Self {
        Self {
            enable_enhanced_errors: true,
            enable_resource_limits: true,
            max_total_memory: Some(1024 * 1024 * 1024), // 1GB default limit
            ..Self::default()
        }
    }

    /// Create a configuration with all enhanced features enabled
    /// Only use this when you're ready for full streaming capabilities
    pub fn enhanced() -> Self {
        Self {
            enable_watermarks: true,
            enable_enhanced_errors: true,
            enable_resource_limits: true,
            message_passing_mode: MessagePassingMode::Full,
            late_data_strategy: LateDataStrategy::DeadLetterQueue,
            watermark_strategy: WatermarkStrategy::BoundedOutOfOrderness,
            max_total_memory: Some(2048 * 1024 * 1024), // 2GB for enhanced mode
            max_concurrent_operations: Some(100),
            // Phase 2 enhanced features
            enable_circuit_breakers: true,
            enable_resource_monitoring: true,
            circuit_breaker_config: Some(CircuitBreakerConfig::production()),
            resource_monitoring_config: Some(ResourceMonitoringConfig::production()),
        }
    }

    /// Enable watermark support with bounded out-of-orderness
    pub fn with_watermarks(mut self) -> Self {
        self.enable_watermarks = true;
        self.watermark_strategy = WatermarkStrategy::BoundedOutOfOrderness;
        self
    }

    /// Enable enhanced error handling
    pub fn with_enhanced_errors(mut self) -> Self {
        self.enable_enhanced_errors = true;
        self
    }

    /// Enable resource limits
    pub fn with_resource_limits(mut self, max_memory: Option<usize>) -> Self {
        self.enable_resource_limits = true;
        self.max_total_memory = max_memory;
        self
    }

    /// Set the message passing mode
    pub fn with_message_passing_mode(mut self, mode: MessagePassingMode) -> Self {
        self.message_passing_mode = mode;
        self
    }

    /// Configure late data handling strategy (Phase 1B)
    pub fn with_late_data_strategy(mut self, strategy: LateDataStrategy) -> Self {
        self.late_data_strategy = strategy;
        self
    }

    /// Configure watermark generation strategy (Phase 1B)
    pub fn with_watermark_strategy(mut self, strategy: WatermarkStrategy) -> Self {
        self.watermark_strategy = strategy;
        self.enable_watermarks = true; // Auto-enable watermarks when strategy is set
        self
    }

    // === PHASE 2: ERROR & RESOURCE ENHANCEMENTS ===

    /// Enable circuit breaker protection (Phase 2)
    pub fn with_circuit_breakers(mut self) -> Self {
        self.enable_circuit_breakers = true;
        self
    }

    /// Enable circuit breakers with custom configuration (Phase 2)
    pub fn with_circuit_breaker_config(mut self, config: CircuitBreakerConfig) -> Self {
        self.enable_circuit_breakers = true;
        self.circuit_breaker_config = Some(config);
        self
    }

    /// Enable resource monitoring and alerting (Phase 2)
    pub fn with_resource_monitoring(mut self) -> Self {
        self.enable_resource_monitoring = true;
        self
    }

    /// Enable resource monitoring with custom configuration (Phase 2)
    pub fn with_resource_monitoring_config(mut self, config: ResourceMonitoringConfig) -> Self {
        self.enable_resource_monitoring = true;
        self.resource_monitoring_config = Some(config);
        self
    }

    /// Enable enhanced error handling with all Phase 2 features (Phase 2)
    pub fn with_enhanced_error_handling(mut self) -> Self {
        self.enable_enhanced_errors = true;
        self.enable_circuit_breakers = true;
        self.enable_resource_monitoring = true;
        self
    }
}

/// Message passing execution modes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagePassingMode {
    /// Use existing synchronous execution - identical to current implementation
    /// No performance impact, no new features
    Legacy,

    /// Hybrid mode - selectively use enhanced features
    /// Uses enhancements only for queries that benefit from them
    Hybrid,

    /// Full message-passing mode - complete FR-058 implementation
    /// All execution goes through enhanced message-passing architecture
    Full,
}

/// Strategies for handling late-arriving data
#[derive(Debug, Clone)]
pub enum LateDataStrategy {
    /// Drop late records (existing behavior)
    Drop,

    /// Send late records to dead letter queue for manual processing
    DeadLetterQueue,

    /// Include late records in the next window
    IncludeInNextWindow,

    /// Attempt to update previous window results (complex)
    UpdatePreviousWindow,
}

/// Watermark generation strategies
#[derive(Debug, Clone)]
pub enum WatermarkStrategy {
    /// No watermarks - use processing time (existing behavior)
    None,

    /// Bounded out-of-orderness - allow records up to N seconds late
    BoundedOutOfOrderness,

    /// Strictly ascending timestamps - no out-of-order allowed
    AscendingTimestamps,

    /// Custom watermark generation logic
    Custom,
}

/// Resource limits configuration
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum total memory usage (bytes)
    pub max_total_memory: Option<usize>,

    /// Maximum memory per operator (bytes)
    pub max_operator_memory: Option<usize>,

    /// Maximum number of windows per key
    pub max_windows_per_key: Option<usize>,

    /// Maximum number of aggregation groups
    pub max_aggregation_groups: Option<usize>,

    /// Maximum concurrent operations
    pub max_concurrent_operations: Option<usize>,

    /// Maximum processing time per record (milliseconds)
    pub max_processing_time_per_record: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_total_memory: None,
            max_operator_memory: None,
            max_windows_per_key: None,
            max_aggregation_groups: None,
            max_concurrent_operations: None,
            max_processing_time_per_record: None,
        }
    }
}

impl ResourceLimits {
    /// Create resource limits suitable for development/testing
    pub fn development() -> Self {
        Self {
            max_total_memory: Some(512 * 1024 * 1024),    // 512MB
            max_operator_memory: Some(128 * 1024 * 1024), // 128MB
            max_windows_per_key: Some(100),
            max_aggregation_groups: Some(10000),
            max_concurrent_operations: Some(50),
            max_processing_time_per_record: Some(1000), // 1 second
        }
    }

    /// Create resource limits suitable for production
    pub fn production() -> Self {
        Self {
            max_total_memory: Some(4 * 1024 * 1024 * 1024), // 4GB
            max_operator_memory: Some(1024 * 1024 * 1024),  // 1GB
            max_windows_per_key: Some(1000),
            max_aggregation_groups: Some(100000),
            max_concurrent_operations: Some(200),
            max_processing_time_per_record: Some(5000), // 5 seconds
        }
    }
}

// === PHASE 2: ERROR & RESOURCE ENHANCEMENTS ===

/// Circuit breaker configuration for Phase 2 error handling
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    pub failure_threshold: u32,

    /// Time to wait before attempting recovery
    pub recovery_timeout_seconds: u64,

    /// Number of successful calls in half-open state before closing circuit
    pub success_threshold: u32,

    /// Timeout for individual operations (seconds)
    pub operation_timeout_seconds: u64,

    /// Failure rate threshold (percentage) to open circuit
    pub failure_rate_threshold: f64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_seconds: 60,
            success_threshold: 3,
            operation_timeout_seconds: 10,
            failure_rate_threshold: 50.0,
        }
    }
}

impl CircuitBreakerConfig {
    /// Configuration optimized for development/testing
    pub fn development() -> Self {
        Self {
            failure_threshold: 3,         // Fail faster in dev
            recovery_timeout_seconds: 10, // Recover faster
            success_threshold: 2,
            operation_timeout_seconds: 5,
            failure_rate_threshold: 40.0, // Lower threshold
        }
    }

    /// Configuration optimized for production
    pub fn production() -> Self {
        Self {
            failure_threshold: 10,         // More tolerant
            recovery_timeout_seconds: 300, // 5 minutes recovery
            success_threshold: 5,
            operation_timeout_seconds: 30,
            failure_rate_threshold: 60.0, // Higher threshold
        }
    }
}

/// Resource monitoring configuration for Phase 2
#[derive(Debug, Clone)]
pub struct ResourceMonitoringConfig {
    /// How often to check resource usage (seconds)
    pub check_interval_seconds: u64,

    /// Warning threshold (percentage of limit)
    pub warning_threshold_percent: f64,

    /// Critical threshold (percentage of limit)
    pub critical_threshold_percent: f64,

    /// How long to keep resource history (minutes)
    pub history_retention_minutes: u64,

    /// Whether to enable automatic cleanup
    pub enable_auto_cleanup: bool,

    /// Maximum memory usage before triggering alerts (MB)
    pub max_memory_mb: Option<u64>,

    /// Maximum CPU usage before triggering alerts (percentage)
    pub max_cpu_percent: Option<f64>,
}

impl Default for ResourceMonitoringConfig {
    fn default() -> Self {
        Self {
            check_interval_seconds: 30,
            warning_threshold_percent: 80.0,
            critical_threshold_percent: 95.0,
            history_retention_minutes: 60,
            enable_auto_cleanup: false, // Conservative default
            max_memory_mb: None,
            max_cpu_percent: None,
        }
    }
}

impl ResourceMonitoringConfig {
    /// Configuration optimized for development/testing
    pub fn development() -> Self {
        Self {
            check_interval_seconds: 10, // Check more frequently
            warning_threshold_percent: 70.0,
            critical_threshold_percent: 85.0,
            history_retention_minutes: 30,
            enable_auto_cleanup: true, // Enable for testing
            max_memory_mb: Some(512),  // 512MB limit
            max_cpu_percent: Some(80.0),
        }
    }

    /// Configuration optimized for production
    pub fn production() -> Self {
        Self {
            check_interval_seconds: 60, // Less frequent checks
            warning_threshold_percent: 85.0,
            critical_threshold_percent: 98.0,
            history_retention_minutes: 240, // 4 hours
            enable_auto_cleanup: false,     // Manual control in prod
            max_memory_mb: Some(4096),      // 4GB limit
            max_cpu_percent: Some(90.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_backward_compatible() {
        let config = StreamingConfig::default();

        // Verify all enhancements are disabled by default
        assert!(!config.enable_watermarks);
        assert!(!config.enable_enhanced_errors);
        assert!(!config.enable_resource_limits);
        assert_eq!(config.message_passing_mode, MessagePassingMode::Legacy);
        assert!(matches!(config.late_data_strategy, LateDataStrategy::Drop));
        assert!(matches!(config.watermark_strategy, WatermarkStrategy::None));
        assert!(config.max_total_memory.is_none());
        assert!(config.max_concurrent_operations.is_none());
        // Phase 2 defaults
        assert!(!config.enable_circuit_breakers);
        assert!(!config.enable_resource_monitoring);
        assert!(config.circuit_breaker_config.is_none());
        assert!(config.resource_monitoring_config.is_none());
    }

    #[test]
    fn test_conservative_config() {
        let config = StreamingConfig::conservative();

        // Should enable safe enhancements only
        assert!(!config.enable_watermarks); // Complex feature disabled
        assert!(config.enable_enhanced_errors); // Safe enhancement enabled
        assert!(config.enable_resource_limits); // Safety feature enabled
        assert_eq!(config.message_passing_mode, MessagePassingMode::Legacy); // Keep same execution
        assert!(config.max_total_memory.is_some()); // Should have memory limit
    }

    #[test]
    fn test_enhanced_config() {
        let config = StreamingConfig::enhanced();

        // Should enable all features
        assert!(config.enable_watermarks);
        assert!(config.enable_enhanced_errors);
        assert!(config.enable_resource_limits);
        assert_eq!(config.message_passing_mode, MessagePassingMode::Full);
        assert!(config.max_total_memory.is_some());
        assert!(config.max_concurrent_operations.is_some());
    }

    #[test]
    fn test_fluent_api() {
        let config = StreamingConfig::new()
            .with_watermarks()
            .with_enhanced_errors()
            .with_resource_limits(Some(1024 * 1024 * 1024))
            .with_message_passing_mode(MessagePassingMode::Hybrid);

        assert!(config.enable_watermarks);
        assert!(config.enable_enhanced_errors);
        assert!(config.enable_resource_limits);
        assert_eq!(config.message_passing_mode, MessagePassingMode::Hybrid);
        assert_eq!(config.max_total_memory, Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_enhanced_config_includes_phase2() {
        let config = StreamingConfig::enhanced();

        // Should enable all Phase 2 features
        assert!(config.enable_circuit_breakers);
        assert!(config.enable_resource_monitoring);
        assert!(config.circuit_breaker_config.is_some());
        assert!(config.resource_monitoring_config.is_some());
    }

    #[test]
    fn test_phase2_fluent_api() {
        let config = StreamingConfig::new()
            .with_circuit_breakers()
            .with_resource_monitoring()
            .with_enhanced_error_handling();

        assert!(config.enable_circuit_breakers);
        assert!(config.enable_resource_monitoring);
        assert!(config.enable_enhanced_errors);
    }

    #[test]
    fn test_circuit_breaker_configs() {
        let dev_config = CircuitBreakerConfig::development();
        assert_eq!(dev_config.failure_threshold, 3);
        assert_eq!(dev_config.recovery_timeout_seconds, 10);

        let prod_config = CircuitBreakerConfig::production();
        assert_eq!(prod_config.failure_threshold, 10);
        assert_eq!(prod_config.recovery_timeout_seconds, 300);
    }

    #[test]
    fn test_resource_monitoring_configs() {
        let dev_config = ResourceMonitoringConfig::development();
        assert_eq!(dev_config.check_interval_seconds, 10);
        assert!(dev_config.enable_auto_cleanup);
        assert_eq!(dev_config.max_memory_mb, Some(512));

        let prod_config = ResourceMonitoringConfig::production();
        assert_eq!(prod_config.check_interval_seconds, 60);
        assert!(!prod_config.enable_auto_cleanup);
        assert_eq!(prod_config.max_memory_mb, Some(4096));
    }
}
