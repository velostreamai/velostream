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

    // === PHASE 4: ADVANCED OBSERVABILITY ===
    /// Enable OpenTelemetry distributed tracing
    /// Default: false (no distributed tracing)
    pub enable_distributed_tracing: bool,

    /// Enable Prometheus metrics export
    /// Default: false (no metrics export)
    pub enable_prometheus_metrics: bool,

    /// Enable performance profiling
    /// Default: false (no continuous profiling)
    pub enable_performance_profiling: bool,

    /// Enable query plan explanation and topology analysis
    /// Default: false (no query plan analysis)
    pub enable_query_analysis: bool,

    /// OpenTelemetry configuration
    /// Default: None (use default tracing settings)
    pub tracing_config: Option<TracingConfig>,

    /// Prometheus metrics configuration
    /// Default: None (use default metrics settings)
    pub prometheus_config: Option<PrometheusConfig>,

    /// Performance profiling configuration
    /// Default: None (use default profiling settings)
    pub profiling_config: Option<ProfilingConfig>,
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
            // Phase 4 defaults - all disabled for backward compatibility
            enable_distributed_tracing: false,
            enable_prometheus_metrics: false,
            enable_performance_profiling: false,
            enable_query_analysis: false,
            tracing_config: None,
            prometheus_config: None,
            profiling_config: None,
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
            // Phase 4 enhanced features - conservative defaults for production
            enable_distributed_tracing: false, // Requires explicit opt-in due to overhead
            enable_prometheus_metrics: true,
            enable_performance_profiling: false, // Requires explicit opt-in due to overhead
            enable_query_analysis: true,
            tracing_config: None,
            prometheus_config: Some(PrometheusConfig::enterprise()),
            profiling_config: None,
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

    // === PHASE 4: ADVANCED OBSERVABILITY ===

    /// Enable OpenTelemetry distributed tracing (Phase 4)
    pub fn with_distributed_tracing(mut self) -> Self {
        self.enable_distributed_tracing = true;
        self.tracing_config = Some(TracingConfig::default());
        self
    }

    /// Enable distributed tracing with custom configuration (Phase 4)
    pub fn with_tracing_config(mut self, config: TracingConfig) -> Self {
        self.enable_distributed_tracing = true;
        self.tracing_config = Some(config);
        self
    }

    /// Enable Prometheus metrics export (Phase 4)
    pub fn with_prometheus_metrics(mut self) -> Self {
        self.enable_prometheus_metrics = true;
        self.prometheus_config = Some(PrometheusConfig::default());
        self
    }

    /// Enable Prometheus metrics with custom configuration (Phase 4)
    pub fn with_prometheus_config(mut self, config: PrometheusConfig) -> Self {
        self.enable_prometheus_metrics = true;
        self.prometheus_config = Some(config);
        self
    }

    /// Enable performance profiling (Phase 4)
    pub fn with_performance_profiling(mut self) -> Self {
        self.enable_performance_profiling = true;
        self.profiling_config = Some(ProfilingConfig::default());
        self
    }

    /// Enable performance profiling with custom configuration (Phase 4)
    pub fn with_profiling_config(mut self, config: ProfilingConfig) -> Self {
        self.enable_performance_profiling = true;
        self.profiling_config = Some(config);
        self
    }

    /// Enable query plan analysis and topology introspection (Phase 4)
    pub fn with_query_analysis(mut self) -> Self {
        self.enable_query_analysis = true;
        self
    }

    /// Enable comprehensive observability with all Phase 4 features (Phase 4)
    pub fn with_full_observability(mut self) -> Self {
        self.enable_distributed_tracing = true;
        self.enable_prometheus_metrics = true;
        self.enable_performance_profiling = true;
        self.enable_query_analysis = true;

        // Create default configs for all observability features
        if self.tracing_config.is_none() {
            self.tracing_config = Some(TracingConfig::default());
        }
        if self.prometheus_config.is_none() {
            self.prometheus_config = Some(PrometheusConfig::default());
        }
        if self.profiling_config.is_none() {
            self.profiling_config = Some(ProfilingConfig::default());
        }

        self
    }

    /// Create observability-focused configuration (Phase 4)
    /// Enables metrics and analysis but not heavy features like tracing/profiling
    pub fn observability() -> Self {
        Self {
            enable_prometheus_metrics: true,
            enable_query_analysis: true,
            prometheus_config: Some(PrometheusConfig::lightweight()),
            ..Self::default()
        }
    }

    /// Create enterprise-grade configuration (Phase 4)
    /// Enables all observability features with production-ready defaults
    pub fn enterprise() -> Self {
        Self {
            // Enable all Phase 4 observability features for enterprise
            enable_distributed_tracing: true,
            enable_prometheus_metrics: true,
            enable_performance_profiling: true, // Enterprise includes profiling
            enable_query_analysis: true,

            // Use production-grade configs for enterprise
            tracing_config: Some(TracingConfig::production()),
            prometheus_config: Some(PrometheusConfig::enterprise()),
            profiling_config: Some(ProfilingConfig::production()),

            // Include all enhanced features
            ..Self::enhanced()
        }
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

// === PHASE 4: OBSERVABILITY CONFIGURATION STRUCTS ===

/// OpenTelemetry distributed tracing configuration
#[derive(Debug, Clone, PartialEq)]
pub struct TracingConfig {
    /// Service name for tracing
    pub service_name: String,
    /// Service version for tracing
    pub service_version: String,
    /// OTLP endpoint for trace export
    pub otlp_endpoint: Option<String>,
    /// Sampling ratio (0.0 to 1.0)
    pub sampling_ratio: f64,
    /// Enable console output for traces
    pub enable_console_output: bool,
    /// Maximum span duration in seconds before timeout
    pub max_span_duration_seconds: u64,
    /// Batch export timeout in milliseconds
    pub batch_export_timeout_ms: u64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "ferris-streams".to_string(),
            service_version: "1.0.0".to_string(),
            otlp_endpoint: None,
            sampling_ratio: 0.1,
            enable_console_output: false,
            max_span_duration_seconds: 300,
            batch_export_timeout_ms: 30000,
        }
    }
}

impl TracingConfig {
    /// Development configuration with higher sampling and console output
    pub fn development() -> Self {
        Self {
            service_name: "ferris-streams-dev".to_string(),
            service_version: "dev".to_string(),
            otlp_endpoint: Some("http://localhost:4317".to_string()),
            sampling_ratio: 1.0,
            enable_console_output: true,
            max_span_duration_seconds: 60,
            batch_export_timeout_ms: 5000,
        }
    }

    /// Production configuration with optimized sampling
    pub fn production() -> Self {
        Self {
            service_name: "ferris-streams".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            otlp_endpoint: Some("https://traces.example.com:4317".to_string()),
            sampling_ratio: 0.01,
            enable_console_output: false,
            max_span_duration_seconds: 300,
            batch_export_timeout_ms: 30000,
        }
    }
}

/// Prometheus metrics export configuration
#[derive(Debug, Clone, PartialEq)]
pub struct PrometheusConfig {
    /// Metrics endpoint path
    pub metrics_path: String,
    /// Metrics server bind address
    pub bind_address: String,
    /// Metrics server port
    pub port: u16,
    /// Enable histogram metrics (higher memory usage)
    pub enable_histograms: bool,
    /// Enable detailed query metrics
    pub enable_query_metrics: bool,
    /// Enable streaming operation metrics
    pub enable_streaming_metrics: bool,
    /// Metrics collection interval in seconds
    pub collection_interval_seconds: u64,
    /// Maximum number of metrics labels per metric
    pub max_labels_per_metric: usize,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            metrics_path: "/metrics".to_string(),
            bind_address: "0.0.0.0".to_string(),
            port: 9090,
            enable_histograms: true,
            enable_query_metrics: true,
            enable_streaming_metrics: true,
            collection_interval_seconds: 15,
            max_labels_per_metric: 10,
        }
    }
}

impl PrometheusConfig {
    /// Lightweight configuration for basic monitoring
    pub fn lightweight() -> Self {
        Self {
            metrics_path: "/metrics".to_string(),
            bind_address: "127.0.0.1".to_string(),
            port: 9090,
            enable_histograms: false,
            enable_query_metrics: true,
            enable_streaming_metrics: false,
            collection_interval_seconds: 30,
            max_labels_per_metric: 5,
        }
    }

    /// Enterprise configuration with comprehensive metrics
    pub fn enterprise() -> Self {
        Self {
            metrics_path: "/metrics".to_string(),
            bind_address: "0.0.0.0".to_string(),
            port: 9090,
            enable_histograms: true,
            enable_query_metrics: true,
            enable_streaming_metrics: true,
            collection_interval_seconds: 10,
            max_labels_per_metric: 20,
        }
    }
}

/// Performance profiling configuration
#[derive(Debug, Clone, PartialEq)]
pub struct ProfilingConfig {
    /// Enable CPU profiling
    pub enable_cpu_profiling: bool,
    /// Enable memory profiling
    pub enable_memory_profiling: bool,
    /// Profiling output directory
    pub output_directory: String,
    /// Profiling sample rate (samples per second)
    pub sample_rate_hz: u32,
    /// Enable flame graph generation
    pub enable_flame_graphs: bool,
    /// Profile data retention days
    pub retention_days: u32,
    /// Enable automatic bottleneck detection
    pub enable_bottleneck_detection: bool,
    /// CPU usage threshold for alerts (percentage)
    pub cpu_threshold_percent: f64,
    /// Memory usage threshold for alerts (percentage)
    pub memory_threshold_percent: f64,
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            output_directory: "./profiling".to_string(),
            sample_rate_hz: 100,
            enable_flame_graphs: true,
            retention_days: 7,
            enable_bottleneck_detection: true,
            cpu_threshold_percent: 80.0,
            memory_threshold_percent: 85.0,
        }
    }
}

impl ProfilingConfig {
    /// Development configuration with high-frequency profiling
    pub fn development() -> Self {
        Self {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            output_directory: "./dev-profiling".to_string(),
            sample_rate_hz: 1000,
            enable_flame_graphs: true,
            retention_days: 3,
            enable_bottleneck_detection: true,
            cpu_threshold_percent: 70.0,
            memory_threshold_percent: 75.0,
        }
    }

    /// Production configuration with optimized overhead
    pub fn production() -> Self {
        Self {
            enable_cpu_profiling: true,
            enable_memory_profiling: false,
            output_directory: "/var/log/ferris-profiling".to_string(),
            sample_rate_hz: 50,
            enable_flame_graphs: false,
            retention_days: 30,
            enable_bottleneck_detection: true,
            cpu_threshold_percent: 90.0,
            memory_threshold_percent: 95.0,
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

    // === PHASE 4: OBSERVABILITY CONFIGURATION TESTS ===

    #[test]
    fn test_tracing_config_defaults() {
        let config = TracingConfig::default();
        assert_eq!(config.service_name, "ferris-streams");
        assert_eq!(config.service_version, "1.0.0");
        assert_eq!(config.sampling_ratio, 0.1);
        assert!(!config.enable_console_output);
        assert_eq!(config.max_span_duration_seconds, 300);
    }

    #[test]
    fn test_tracing_config_development() {
        let config = TracingConfig::development();
        assert_eq!(config.service_name, "ferris-streams-dev");
        assert_eq!(config.sampling_ratio, 1.0);
        assert!(config.enable_console_output);
        assert_eq!(
            config.otlp_endpoint,
            Some("http://localhost:4317".to_string())
        );
    }

    #[test]
    fn test_tracing_config_production() {
        let config = TracingConfig::production();
        assert_eq!(config.service_name, "ferris-streams");
        assert_eq!(config.sampling_ratio, 0.01);
        assert!(!config.enable_console_output);
        assert_eq!(
            config.otlp_endpoint,
            Some("https://traces.example.com:4317".to_string())
        );
    }

    #[test]
    fn test_prometheus_config_defaults() {
        let config = PrometheusConfig::default();
        assert_eq!(config.metrics_path, "/metrics");
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.port, 9090);
        assert!(config.enable_histograms);
        assert!(config.enable_query_metrics);
        assert!(config.enable_streaming_metrics);
        assert_eq!(config.collection_interval_seconds, 15);
    }

    #[test]
    fn test_prometheus_config_lightweight() {
        let config = PrometheusConfig::lightweight();
        assert_eq!(config.bind_address, "127.0.0.1");
        assert!(!config.enable_histograms);
        assert!(config.enable_query_metrics);
        assert!(!config.enable_streaming_metrics);
        assert_eq!(config.collection_interval_seconds, 30);
        assert_eq!(config.max_labels_per_metric, 5);
    }

    #[test]
    fn test_prometheus_config_enterprise() {
        let config = PrometheusConfig::enterprise();
        assert_eq!(config.bind_address, "0.0.0.0");
        assert!(config.enable_histograms);
        assert!(config.enable_query_metrics);
        assert!(config.enable_streaming_metrics);
        assert_eq!(config.collection_interval_seconds, 10);
        assert_eq!(config.max_labels_per_metric, 20);
    }

    #[test]
    fn test_profiling_config_defaults() {
        let config = ProfilingConfig::default();
        assert!(config.enable_cpu_profiling);
        assert!(config.enable_memory_profiling);
        assert_eq!(config.output_directory, "./profiling");
        assert_eq!(config.sample_rate_hz, 100);
        assert!(config.enable_flame_graphs);
        assert_eq!(config.retention_days, 7);
        assert!(config.enable_bottleneck_detection);
    }

    #[test]
    fn test_profiling_config_development() {
        let config = ProfilingConfig::development();
        assert_eq!(config.output_directory, "./dev-profiling");
        assert_eq!(config.sample_rate_hz, 1000);
        assert_eq!(config.retention_days, 3);
        assert_eq!(config.cpu_threshold_percent, 70.0);
        assert_eq!(config.memory_threshold_percent, 75.0);
    }

    #[test]
    fn test_profiling_config_production() {
        let config = ProfilingConfig::production();
        assert!(config.enable_cpu_profiling);
        assert!(!config.enable_memory_profiling);
        assert_eq!(config.output_directory, "/var/log/ferris-profiling");
        assert_eq!(config.sample_rate_hz, 50);
        assert!(!config.enable_flame_graphs);
        assert_eq!(config.retention_days, 30);
        assert_eq!(config.cpu_threshold_percent, 90.0);
    }

    #[test]
    fn test_phase4_config_helpers() {
        let config = StreamingConfig::default()
            .with_distributed_tracing()
            .with_prometheus_metrics()
            .with_performance_profiling()
            .with_query_analysis();

        assert!(config.enable_distributed_tracing);
        assert!(config.enable_prometheus_metrics);
        assert!(config.enable_performance_profiling);
        assert!(config.enable_query_analysis);
        assert!(config.tracing_config.is_some());
        assert!(config.prometheus_config.is_some());
        assert!(config.profiling_config.is_some());
    }

    #[test]
    fn test_full_observability_config() {
        let config = StreamingConfig::default().with_full_observability();

        assert!(config.enable_distributed_tracing);
        assert!(config.enable_prometheus_metrics);
        assert!(config.enable_performance_profiling);
        assert!(config.enable_query_analysis);

        // Verify all configs are present
        assert!(config.tracing_config.is_some());
        assert!(config.prometheus_config.is_some());
        assert!(config.profiling_config.is_some());

        // Verify configs use default values
        let tracing = config.tracing_config.unwrap();
        assert_eq!(tracing.service_name, "ferris-streams");
        assert_eq!(tracing.sampling_ratio, 0.1);
    }

    #[test]
    fn test_observability_config() {
        let config = StreamingConfig::observability();

        assert!(!config.enable_distributed_tracing);
        assert!(config.enable_prometheus_metrics);
        assert!(!config.enable_performance_profiling);
        assert!(config.enable_query_analysis);

        // Only prometheus config should be present
        assert!(config.tracing_config.is_none());
        assert!(config.prometheus_config.is_some());
        assert!(config.profiling_config.is_none());

        // Verify prometheus uses lightweight config
        let prometheus = config.prometheus_config.unwrap();
        assert_eq!(prometheus.bind_address, "127.0.0.1");
        assert!(!prometheus.enable_histograms);
    }

    #[test]
    fn test_enterprise_config() {
        let config = StreamingConfig::enterprise();

        // All Phase 4 features should be enabled
        assert!(config.enable_distributed_tracing);
        assert!(config.enable_prometheus_metrics);
        assert!(config.enable_performance_profiling);
        assert!(config.enable_query_analysis);

        // All configs should use production/enterprise settings
        let tracing = config.tracing_config.unwrap();
        assert_eq!(tracing.sampling_ratio, 0.01);

        let prometheus = config.prometheus_config.unwrap();
        assert_eq!(prometheus.max_labels_per_metric, 20);

        let profiling = config.profiling_config.unwrap();
        assert_eq!(profiling.sample_rate_hz, 50);
        assert!(!profiling.enable_memory_profiling);
    }

    #[test]
    fn test_phase4_backward_compatibility() {
        let default_config = StreamingConfig::default();

        // Phase 4 features should be disabled by default
        assert!(!default_config.enable_distributed_tracing);
        assert!(!default_config.enable_prometheus_metrics);
        assert!(!default_config.enable_performance_profiling);
        assert!(!default_config.enable_query_analysis);

        // Phase 4 config objects should be None by default
        assert!(default_config.tracing_config.is_none());
        assert!(default_config.prometheus_config.is_none());
        assert!(default_config.profiling_config.is_none());
    }
}
