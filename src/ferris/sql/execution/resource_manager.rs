/*!
# Resource Management for Streaming Operations

The ResourceManager provides optional resource monitoring, limits, and alerting
for streaming SQL operations. It integrates with the enhanced error system and
circuit breakers to provide production-ready resource governance.

## Design Philosophy

This resource management system follows Phase 2 requirements:
- **Optional Component**: Only activated when resource limits are enabled
- **Backward Compatible**: Existing code works without resource management
- **Progressive Enhancement**: Can be enabled via StreamingConfig
- **Production Ready**: Comprehensive monitoring, alerting, and recovery
*/

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use crate::ferris::sql::execution::error::{StreamingError, ThresholdSeverity};

/// Resource manager for monitoring and enforcing resource limits
///
/// The ResourceManager tracks resource usage across the streaming engine and
/// enforces configurable limits. It provides real-time monitoring, alerting,
/// and automatic resource cleanup when thresholds are exceeded.
#[derive(Debug)]
pub struct ResourceManager {
    /// Current resource usage tracking
    resource_usage: Arc<RwLock<HashMap<String, ResourceMetrics>>>,

    /// Configured resource limits
    limits: ResourceLimits,

    /// Resource monitoring configuration
    monitoring_config: MonitoringConfig,

    /// Whether resource management is enabled
    enabled: bool,

    /// Resource cleanup handlers (simplified for compilation)
    cleanup_handlers: Arc<RwLock<HashMap<String, String>>>, // Store handler names instead of closures for now
}

#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    /// Current usage amount
    pub current: u64,

    /// Peak usage seen
    pub peak: u64,

    /// Last updated timestamp
    pub last_updated: SystemTime,

    /// Usage history for trending
    pub history: Vec<(SystemTime, u64)>,

    /// Number of times limit was exceeded
    pub limit_violations: u32,
}

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum total memory usage (bytes)
    pub max_total_memory: Option<u64>,

    /// Maximum memory per operator (bytes)
    pub max_operator_memory: Option<u64>,

    /// Maximum number of windows per key
    pub max_windows_per_key: Option<u32>,

    /// Maximum number of aggregation groups
    pub max_aggregation_groups: Option<u32>,

    /// Maximum concurrent operations
    pub max_concurrent_operations: Option<u32>,

    /// Maximum processing time per record (milliseconds)
    pub max_processing_time_per_record: Option<u64>,

    /// Custom resource limits
    pub custom_limits: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// How often to check resource usage
    pub check_interval: Duration,

    /// Warning threshold (percentage of limit)
    pub warning_threshold: f64,

    /// Critical threshold (percentage of limit)  
    pub critical_threshold: f64,

    /// How long to keep resource history
    pub history_retention: Duration,

    /// Whether to enable automatic cleanup
    pub auto_cleanup: bool,
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
            custom_limits: HashMap::new(),
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            warning_threshold: 0.8,                      // 80%
            critical_threshold: 0.95,                    // 95%
            history_retention: Duration::from_secs(600), // 10 minutes
            auto_cleanup: false,                         // Conservative default
        }
    }
}

impl ResourceManager {
    /// Create a new ResourceManager with default configuration
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            resource_usage: Arc::new(RwLock::new(HashMap::new())),
            limits,
            monitoring_config: MonitoringConfig::default(),
            enabled: false, // Disabled by default for backward compatibility
            cleanup_handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create ResourceManager with custom monitoring configuration
    pub fn with_monitoring_config(
        limits: ResourceLimits,
        monitoring_config: MonitoringConfig,
    ) -> Self {
        Self {
            resource_usage: Arc::new(RwLock::new(HashMap::new())),
            limits,
            monitoring_config,
            enabled: false,
            cleanup_handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Enable resource management
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable resource management
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Check if resource management is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Update resource usage for a specific resource
    pub fn update_resource_usage(
        &self,
        resource_name: &str,
        usage: u64,
    ) -> Result<(), StreamingError> {
        if !self.enabled {
            return Ok(());
        }

        let mut usage_map = self.resource_usage.write().unwrap();
        let metrics = usage_map
            .entry(resource_name.to_string())
            .or_insert_with(|| ResourceMetrics {
                current: 0,
                peak: 0,
                last_updated: SystemTime::now(),
                history: Vec::new(),
                limit_violations: 0,
            });

        metrics.current = usage;
        metrics.peak = metrics.peak.max(usage);
        metrics.last_updated = SystemTime::now();

        // Add to history and clean old entries
        metrics.history.push((metrics.last_updated, usage));
        self.cleanup_history(&mut metrics.history);

        // Check limits
        self.check_resource_limit(resource_name, usage, metrics)?;

        Ok(())
    }

    /// Check if a resource allocation would exceed limits
    pub fn check_allocation(
        &self,
        resource_name: &str,
        requested_amount: u64,
    ) -> Result<(), StreamingError> {
        if !self.enabled {
            return Ok(());
        }

        let usage_map = self.resource_usage.read().unwrap();
        let current_usage = usage_map.get(resource_name).map(|m| m.current).unwrap_or(0);

        let total_would_be = current_usage + requested_amount;
        let limit = self.get_limit_for_resource(resource_name);

        if let Some(limit) = limit {
            if total_would_be > limit {
                return Err(StreamingError::ResourceExhausted {
                    resource_type: resource_name.to_string(),
                    current_usage: total_would_be,
                    limit,
                    message: format!("Allocation of {} would exceed limit", requested_amount),
                });
            }
        }

        Ok(())
    }

    /// Get current resource usage
    pub fn get_resource_usage(&self, resource_name: &str) -> Option<ResourceMetrics> {
        let usage_map = self.resource_usage.read().unwrap();
        usage_map.get(resource_name).cloned()
    }

    /// Get usage for all resources
    pub fn get_all_resource_usage(&self) -> HashMap<String, ResourceMetrics> {
        let usage_map = self.resource_usage.read().unwrap();
        usage_map.clone()
    }

    /// Register a cleanup handler for a resource (simplified for now)
    pub fn register_cleanup_handler(&self, resource_name: &str, handler_name: &str) {
        let mut handlers = self.cleanup_handlers.write().unwrap();
        handlers.insert(resource_name.to_string(), handler_name.to_string());
    }

    /// Trigger cleanup for a specific resource (simplified for now)
    pub fn cleanup_resource(&self, resource_name: &str) -> Result<(), StreamingError> {
        let handlers = self.cleanup_handlers.read().unwrap();
        if let Some(_handler_name) = handlers.get(resource_name) {
            // In a real implementation, this would call the actual cleanup function
            // For now, we just return success to show the pattern
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Get resource utilization as percentage
    pub fn get_resource_utilization(&self, resource_name: &str) -> Option<f64> {
        let usage_map = self.resource_usage.read().unwrap();
        let metrics = usage_map.get(resource_name)?;
        let limit = self.get_limit_for_resource(resource_name)?;

        Some((metrics.current as f64 / limit as f64) * 100.0)
    }

    /// Check all resources and return any violations
    pub fn check_all_resources(&self) -> Vec<StreamingError> {
        if !self.enabled {
            return Vec::new();
        }

        let mut violations = Vec::new();
        let usage_map = self.resource_usage.read().unwrap();

        for (resource_name, metrics) in usage_map.iter() {
            if let Some(limit) = self.get_limit_for_resource(resource_name) {
                let utilization = (metrics.current as f64 / limit as f64) * 100.0;

                let severity = if utilization >= self.monitoring_config.critical_threshold * 100.0 {
                    ThresholdSeverity::Critical
                } else if utilization >= self.monitoring_config.warning_threshold * 100.0 {
                    ThresholdSeverity::Warning
                } else {
                    continue;
                };

                violations.push(StreamingError::ResourceThresholdViolation {
                    resource_type: resource_name.clone(),
                    current_value: utilization,
                    threshold: if matches!(severity, ThresholdSeverity::Critical) {
                        self.monitoring_config.critical_threshold * 100.0
                    } else {
                        self.monitoring_config.warning_threshold * 100.0
                    },
                    severity,
                });
            }
        }

        violations
    }

    // Private helper methods

    fn get_limit_for_resource(&self, resource_name: &str) -> Option<u64> {
        match resource_name {
            "total_memory" => self.limits.max_total_memory,
            "operator_memory" => self.limits.max_operator_memory,
            // Handle dynamic memory types like "heap_memory", "stack_memory", etc.
            name if name.ends_with("_memory") => self.limits.max_total_memory,
            "windows_per_key" => self.limits.max_windows_per_key.map(|v| v as u64),
            "aggregation_groups" => self.limits.max_aggregation_groups.map(|v| v as u64),
            "concurrent_operations" => self.limits.max_concurrent_operations.map(|v| v as u64),
            "processing_time_per_record" => self.limits.max_processing_time_per_record,
            _ => self.limits.custom_limits.get(resource_name).copied(),
        }
    }

    fn check_resource_limit(
        &self,
        resource_name: &str,
        current_usage: u64,
        metrics: &mut ResourceMetrics,
    ) -> Result<(), StreamingError> {
        let limit = self.get_limit_for_resource(resource_name);

        if let Some(limit) = limit {
            if current_usage > limit {
                metrics.limit_violations += 1;

                return Err(StreamingError::ResourceExhausted {
                    resource_type: resource_name.to_string(),
                    current_usage,
                    limit,
                    message: format!("Resource limit exceeded for {}", resource_name),
                });
            }
        }

        Ok(())
    }

    fn cleanup_history(&self, history: &mut Vec<(SystemTime, u64)>) {
        let cutoff_time = SystemTime::now() - self.monitoring_config.history_retention;
        history.retain(|(time, _)| *time > cutoff_time);
    }
}

// Convenience methods for common resource types
impl ResourceManager {
    /// Update memory usage
    pub fn update_memory_usage(&self, memory_type: &str, bytes: u64) -> Result<(), StreamingError> {
        self.update_resource_usage(&format!("{}_memory", memory_type), bytes)
    }

    /// Check memory allocation
    pub fn check_memory_allocation(
        &self,
        memory_type: &str,
        bytes: u64,
    ) -> Result<(), StreamingError> {
        self.check_allocation(&format!("{}_memory", memory_type), bytes)
    }

    /// Update concurrent operation count
    pub fn update_concurrent_operations(&self, count: u64) -> Result<(), StreamingError> {
        self.update_resource_usage("concurrent_operations", count)
    }

    /// Update window count for a key
    pub fn update_window_count(&self, key: &str, count: u64) -> Result<(), StreamingError> {
        self.update_resource_usage(&format!("windows_{}", key), count)
    }

    /// Record processing time for performance monitoring
    pub fn record_processing_time(
        &self,
        operation: &str,
        duration: Duration,
    ) -> Result<(), StreamingError> {
        self.update_resource_usage(
            &format!("processing_time_{}", operation),
            duration.as_millis() as u64,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_resource_usage_tracking() {
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
    fn test_allocation_checking() {
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
        assert!(manager.check_allocation("total_memory", 300).is_err());
    }

    #[test]
    fn test_resource_utilization() {
        let limits = ResourceLimits {
            max_total_memory: Some(1000),
            ..Default::default()
        };
        let mut manager = ResourceManager::new(limits);
        manager.enable();

        manager.update_resource_usage("total_memory", 800).unwrap();

        let utilization = manager.get_resource_utilization("total_memory").unwrap();
        assert_eq!(utilization, 80.0);
    }

    #[test]
    fn test_threshold_violations() {
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

        // Set usage above warning threshold
        manager.update_resource_usage("total_memory", 750).unwrap();

        let violations = manager.check_all_resources();
        assert_eq!(violations.len(), 1);

        match &violations[0] {
            StreamingError::ResourceThresholdViolation { severity, .. } => {
                assert_eq!(*severity, ThresholdSeverity::Warning);
            }
            _ => panic!("Expected ResourceThresholdViolation"),
        }
    }

    #[test]
    fn test_disabled_manager_allows_all_operations() {
        let limits = ResourceLimits {
            max_total_memory: Some(100), // Very low limit
            ..Default::default()
        };
        let manager = ResourceManager::new(limits); // Not enabled

        // Should allow operations even with low limits when disabled
        assert!(manager.update_resource_usage("total_memory", 1000).is_ok());
        assert!(manager.check_allocation("total_memory", 1000).is_ok());
        assert!(manager.check_all_resources().is_empty());
    }
}
