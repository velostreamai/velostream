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
}
