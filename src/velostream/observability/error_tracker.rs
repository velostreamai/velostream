//! Error message tracking with rolling buffer and message type aggregation
//!
//! This module provides efficient tracking of error messages with:
//! - Rolling buffer of last 10 error messages
//! - Count tracking for each unique error message type
//! - Thread-safe access via Arc<Mutex<>>
//! - Useful for error reporting and diagnostics

use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Deployment context for enriching error reports with infrastructure metadata
#[derive(Debug, Clone)]
pub struct DeploymentContext {
    /// Node identifier (e.g., "prod-trading-cluster-1")
    pub node_id: Option<String>,
    /// Human-readable node name (e.g., "Production Trading Analytics Platform")
    pub node_name: Option<String>,
    /// AWS region or deployment region (e.g., "us-east-1")
    pub region: Option<String>,
    /// Application version (e.g., "1.0.0")
    pub version: Option<String>,
}

impl DeploymentContext {
    /// Create a new empty deployment context
    pub fn new() -> Self {
        Self {
            node_id: None,
            node_name: None,
            region: None,
            version: None,
        }
    }

    /// Create a deployment context with all fields set
    pub fn with_all(node_id: String, node_name: String, region: String, version: String) -> Self {
        Self {
            node_id: Some(node_id),
            node_name: Some(node_name),
            region: Some(region),
            version: Some(version),
        }
    }

    /// Check if context has any fields set
    pub fn has_any(&self) -> bool {
        self.node_id.is_some()
            || self.node_name.is_some()
            || self.region.is_some()
            || self.version.is_some()
    }
}

impl Default for DeploymentContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a single error message entry with timestamp and deployment context
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    /// The error message text
    pub message: String,
    /// Unix timestamp when error occurred (seconds)
    pub timestamp: u64,
    /// Count of instances of this exact message
    pub count: u64,
    /// Optional node/instance ID where error occurred
    pub node_id: Option<String>,
    /// Full deployment context (node_id, node_name, region, version)
    pub deployment_context: Option<DeploymentContext>,
}

impl ErrorEntry {
    /// Create a new error entry with current timestamp
    pub fn new(message: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            message,
            timestamp,
            count: 1,
            node_id: None,
            deployment_context: None,
        }
    }

    /// Create a new error entry with node context
    pub fn with_node(message: String, node_id: String) -> Self {
        let mut entry = Self::new(message);
        entry.node_id = Some(node_id);
        entry
    }

    /// Create a new error entry with full deployment context
    pub fn with_deployment_context(message: String, deployment_context: DeploymentContext) -> Self {
        let mut entry = Self::new(message);
        // Set node_id for backward compatibility if available
        if let Some(ref node_id) = deployment_context.node_id {
            entry.node_id = Some(node_id.clone());
        }
        entry.deployment_context = Some(deployment_context);
        entry
    }

    /// Update the deployment context for this entry
    pub fn set_deployment_context(&mut self, context: DeploymentContext) {
        if let Some(ref node_id) = context.node_id {
            self.node_id = Some(node_id.clone());
        }
        self.deployment_context = Some(context);
    }
}

impl fmt::Display for ErrorEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut context_parts = Vec::new();

        // Build context string from deployment context if available
        if let Some(ref ctx) = self.deployment_context {
            if let Some(ref node_id) = ctx.node_id {
                context_parts.push(format!("node_id={}", node_id));
            }
            if let Some(ref node_name) = ctx.node_name {
                context_parts.push(format!("node_name={}", node_name));
            }
            if let Some(ref region) = ctx.region {
                context_parts.push(format!("region={}", region));
            }
            if let Some(ref version) = ctx.version {
                context_parts.push(format!("version={}", version));
            }
        } else if let Some(ref node_id) = self.node_id {
            // Fallback to node_id for backward compatibility
            context_parts.push(format!("node={}", node_id));
        }

        let context_str = if !context_parts.is_empty() {
            format!("[{}] ", context_parts.join(", "))
        } else {
            String::new()
        };

        // Place deployment context at the beginning of the message for visibility
        write!(f, "{}{} (count: {})", context_str, self.message, self.count)
    }
}

/// Tracks error messages with rolling buffer and message type counting
///
/// ## Features
/// - Maintains rolling buffer of last 10 unique error messages
/// - Counts instances of each unique error message type
/// - Thread-safe operations
/// - Efficient memory usage
///
/// ## Example
/// ```rust
/// # use velostream::velostream::observability::error_tracker::ErrorMessageBuffer;
/// let mut buffer = ErrorMessageBuffer::new();
///
/// // Add error messages
/// buffer.add_error("Connection timeout".to_string());
/// buffer.add_error("Connection timeout".to_string());  // Same message increments count
/// buffer.add_error("Invalid query".to_string());
///
/// // Get statistics
/// let stats = buffer.get_stats();
/// assert_eq!(stats.total_errors, 3);
/// assert_eq!(stats.unique_errors, 2);
/// assert_eq!(stats.message_counts.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ErrorMessageBuffer {
    /// Rolling buffer of last 10 error message entries (FIFO queue)
    /// Newest entries at the end
    messages: Vec<ErrorEntry>,
    /// HashMap tracking count of each unique error message
    /// Used for efficient aggregation without scanning buffer
    message_counts: HashMap<String, u64>,
    /// Total number of errors recorded (never decreases)
    total_errors: u64,
    /// Optional node/instance ID for error tracking context
    node_id: Option<String>,
    /// Full deployment context (node_id, node_name, region, version)
    deployment_context: Option<DeploymentContext>,
}

impl ErrorMessageBuffer {
    /// Create a new empty error message buffer
    pub fn new() -> Self {
        Self {
            messages: Vec::with_capacity(10),
            message_counts: HashMap::new(),
            total_errors: 0,
            node_id: None,
            deployment_context: None,
        }
    }

    /// Set the node/instance ID for error tracking context
    ///
    /// This ID will be attached to error entries for distributed tracing purposes.
    pub fn set_node_id(&mut self, node_id: Option<String>) {
        self.node_id = node_id.clone();
        // Also update deployment context if present
        if let Some(ref mut ctx) = self.deployment_context {
            ctx.node_id = node_id;
        }
    }

    /// Set the full deployment context for error tracking
    ///
    /// This context (node_id, node_name, region, version) will be attached to all error
    /// entries for comprehensive deployment tracking.
    pub fn set_deployment_context(&mut self, context: DeploymentContext) {
        if let Some(ref node_id) = context.node_id {
            self.node_id = Some(node_id.clone());
        }
        self.deployment_context = Some(context);
    }

    /// Get the current deployment context
    pub fn get_deployment_context(&self) -> Option<&DeploymentContext> {
        self.deployment_context.as_ref()
    }

    /// Add an error message to the buffer
    ///
    /// - If message already exists in buffer, increments its count
    /// - If buffer is full (10 messages), removes oldest message
    /// - Updates message type count tracking
    /// - Attaches node_id and deployment context if set
    pub fn add_error(&mut self, message: String) {
        self.total_errors += 1;

        // Update overall count for this message type
        *self.message_counts.entry(message.clone()).or_insert(0) += 1;

        // Check if this exact message is already in the buffer
        if let Some(entry) = self.messages.iter_mut().find(|e| e.message == message) {
            entry.count += 1;
            return;
        }

        // New unique message - add to buffer with deployment context
        let mut entry = ErrorEntry::new(message);

        // Attach node_id for backward compatibility
        if let Some(ref node_id) = self.node_id {
            entry.node_id = Some(node_id.clone());
        }

        // Attach full deployment context
        if let Some(ref ctx) = self.deployment_context {
            entry.set_deployment_context(ctx.clone());
        }

        self.messages.push(entry);

        // Keep buffer size at max 10 entries
        if self.messages.len() > 10 {
            let removed = self.messages.remove(0);
            // Decrement the global count for removed message
            if let Some(count) = self.message_counts.get_mut(&removed.message) {
                *count = count.saturating_sub(removed.count);
                if *count == 0 {
                    self.message_counts.remove(&removed.message);
                }
            }
        }
    }

    /// Get all error messages in buffer (oldest first)
    pub fn get_messages(&self) -> Vec<ErrorEntry> {
        self.messages.clone()
    }

    /// Get count of a specific error message
    pub fn get_message_count(&self, message: &str) -> u64 {
        *self.message_counts.get(message).unwrap_or(&0)
    }

    /// Get all unique error messages and their counts
    pub fn get_message_counts(&self) -> HashMap<String, u64> {
        self.message_counts.clone()
    }

    /// Get comprehensive statistics about tracked errors
    pub fn get_stats(&self) -> ErrorStats {
        ErrorStats {
            total_errors: self.total_errors,
            unique_errors: self.message_counts.len(),
            buffered_messages: self.messages.len(),
            message_counts: self.message_counts.clone(),
        }
    }

    /// Reset all error tracking
    pub fn reset(&mut self) {
        self.messages.clear();
        self.message_counts.clear();
        self.total_errors = 0;
    }

    /// Get number of messages currently in buffer
    pub fn buffer_size(&self) -> usize {
        self.messages.len()
    }

    /// Check if buffer is at maximum capacity (10 messages)
    pub fn is_full(&self) -> bool {
        self.messages.len() >= 10
    }

    /// Format all errors with deployment context for reporting
    ///
    /// Returns a formatted string suitable for error reports or logs with full deployment metadata
    pub fn format_for_report(&self) -> String {
        if self.messages.is_empty() {
            return "No errors recorded".to_string();
        }

        let mut report = String::new();

        // Build deployment context header
        let header = if let Some(ref ctx) = self.deployment_context {
            let mut parts = Vec::new();
            if let Some(ref node_id) = ctx.node_id {
                parts.push(format!("node_id={}", node_id));
            }
            if let Some(ref node_name) = ctx.node_name {
                parts.push(format!("node_name={}", node_name));
            }
            if let Some(ref region) = ctx.region {
                parts.push(format!("region={}", region));
            }
            if let Some(ref version) = ctx.version {
                parts.push(format!("version={}", version));
            }
            if !parts.is_empty() {
                format!("=== Error Report [{}] ===\n", parts.join(", "))
            } else {
                "=== Error Report ===\n".to_string()
            }
        } else if let Some(ref node_id) = self.node_id {
            format!("=== Error Report [Node: {}] ===\n", node_id)
        } else {
            "=== Error Report ===\n".to_string()
        };

        report.push_str(&header);

        report.push_str(&format!(
            "Total Errors: {}\n\
             Unique Errors: {}\n\
             Buffered Messages: {}\n\n",
            self.total_errors,
            self.message_counts.len(),
            self.messages.len()
        ));

        report.push_str("Recent Errors (most recent first):\n");
        report.push_str("================================\n");

        for (idx, entry) in self.messages.iter().rev().enumerate() {
            report.push_str(&format!("{}. {}\n", idx + 1, entry));
        }

        report
    }
}

impl Default for ErrorMessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about error messages in buffer
#[derive(Debug, Clone)]
pub struct ErrorStats {
    /// Total number of errors recorded (cumulative)
    pub total_errors: u64,
    /// Number of unique error message types
    pub unique_errors: usize,
    /// Number of messages currently buffered (max 10)
    pub buffered_messages: usize,
    /// Count for each unique error message
    pub message_counts: HashMap<String, u64>,
}

impl ErrorStats {
    /// Get list of error messages sorted by frequency (most common first)
    pub fn get_sorted_by_frequency(&self) -> Vec<(String, u64)> {
        let mut messages: Vec<_> = self
            .message_counts
            .iter()
            .map(|(msg, count)| (msg.clone(), *count))
            .collect();
        messages.sort_by(|a, b| b.1.cmp(&a.1)); // Sort descending by count
        messages
    }

    /// Get top N most common error messages
    pub fn get_top_errors(&self, limit: usize) -> Vec<(String, u64)> {
        let mut all = self.get_sorted_by_frequency();
        all.truncate(limit);
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_buffer_creation() {
        let buffer = ErrorMessageBuffer::new();
        assert_eq!(buffer.buffer_size(), 0);
        assert_eq!(buffer.total_errors, 0);
    }

    #[test]
    fn test_add_single_error() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Test error".to_string());

        assert_eq!(buffer.buffer_size(), 1);
        assert_eq!(buffer.total_errors, 1);
        assert_eq!(buffer.get_message_count("Test error"), 1);
    }

    #[test]
    fn test_add_duplicate_error_increments_count() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Timeout".to_string());
        buffer.add_error("Timeout".to_string());
        buffer.add_error("Timeout".to_string());

        // Should be stored as single entry with count=3
        assert_eq!(buffer.buffer_size(), 1);
        assert_eq!(buffer.total_errors, 3);
        assert_eq!(buffer.get_message_count("Timeout"), 3);
    }

    #[test]
    fn test_add_multiple_unique_errors() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Error 1".to_string());
        buffer.add_error("Error 2".to_string());
        buffer.add_error("Error 3".to_string());

        assert_eq!(buffer.buffer_size(), 3);
        assert_eq!(buffer.total_errors, 3);
        assert_eq!(buffer.get_message_count("Error 1"), 1);
        assert_eq!(buffer.get_message_count("Error 2"), 1);
        assert_eq!(buffer.get_message_count("Error 3"), 1);
    }

    #[test]
    fn test_buffer_respects_max_10_entries() {
        let mut buffer = ErrorMessageBuffer::new();

        // Add 15 unique errors
        for i in 0..15 {
            buffer.add_error(format!("Error {}", i));
        }

        // Buffer should only have 10 entries (entries 5-14, oldest 0-4 removed)
        assert_eq!(buffer.buffer_size(), 10);
        assert_eq!(buffer.total_errors, 15);

        // First 5 errors should be removed
        assert_eq!(buffer.get_message_count("Error 0"), 0);
        assert_eq!(buffer.get_message_count("Error 4"), 0);

        // Last 10 should remain
        assert_eq!(buffer.get_message_count("Error 5"), 1);
        assert_eq!(buffer.get_message_count("Error 14"), 1);
    }

    #[test]
    fn test_mixed_duplicate_and_unique_errors() {
        let mut buffer = ErrorMessageBuffer::new();

        buffer.add_error("Timeout".to_string());
        buffer.add_error("Invalid query".to_string());
        buffer.add_error("Timeout".to_string());
        buffer.add_error("Connection refused".to_string());
        buffer.add_error("Timeout".to_string());
        buffer.add_error("Invalid query".to_string());

        assert_eq!(buffer.total_errors, 6);
        assert_eq!(buffer.buffer_size(), 3); // 3 unique messages
        assert_eq!(buffer.get_message_count("Timeout"), 3);
        assert_eq!(buffer.get_message_count("Invalid query"), 2);
        assert_eq!(buffer.get_message_count("Connection refused"), 1);
    }

    #[test]
    fn test_get_stats() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Error A".to_string());
        buffer.add_error("Error A".to_string());
        buffer.add_error("Error B".to_string());

        let stats = buffer.get_stats();
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.unique_errors, 2);
        assert_eq!(stats.buffered_messages, 2);
        assert_eq!(stats.message_counts.len(), 2);
    }

    #[test]
    fn test_get_sorted_by_frequency() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Common error".to_string());
        buffer.add_error("Common error".to_string());
        buffer.add_error("Common error".to_string());
        buffer.add_error("Rare error".to_string());

        let stats = buffer.get_stats();
        let sorted = stats.get_sorted_by_frequency();

        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0].0, "Common error");
        assert_eq!(sorted[0].1, 3);
        assert_eq!(sorted[1].0, "Rare error");
        assert_eq!(sorted[1].1, 1);
    }

    #[test]
    fn test_get_top_errors() {
        let mut buffer = ErrorMessageBuffer::new();
        for i in 0..5 {
            for _ in 0..(i + 1) {
                buffer.add_error(format!("Error {}", i));
            }
        }

        let stats = buffer.get_stats();
        let top_3 = stats.get_top_errors(3);

        assert_eq!(top_3.len(), 3);
        assert_eq!(top_3[0].1, 5); // Error 4 occurred 5 times
        assert_eq!(top_3[1].1, 4); // Error 3 occurred 4 times
        assert_eq!(top_3[2].1, 3); // Error 2 occurred 3 times
    }

    #[test]
    fn test_reset() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Error".to_string());
        buffer.add_error("Error".to_string());

        assert_eq!(buffer.total_errors, 2);
        assert_eq!(buffer.buffer_size(), 1);

        buffer.reset();

        assert_eq!(buffer.total_errors, 0);
        assert_eq!(buffer.buffer_size(), 0);
        assert_eq!(buffer.get_message_count("Error"), 0);
    }

    #[test]
    fn test_is_full() {
        let mut buffer = ErrorMessageBuffer::new();

        assert!(!buffer.is_full());

        for i in 0..10 {
            buffer.add_error(format!("Error {}", i));
            if i < 9 {
                assert!(!buffer.is_full());
            }
        }

        assert!(buffer.is_full());
    }

    #[test]
    fn test_buffer_eviction_with_counts() {
        let mut buffer = ErrorMessageBuffer::new();

        // Add 12 unique errors
        for i in 0..12 {
            buffer.add_error(format!("Error {}", i));
        }

        // Buffer should have 10 entries
        assert_eq!(buffer.buffer_size(), 10);

        // First 2 should be evicted (Error 0 and Error 1)
        assert_eq!(buffer.get_message_count("Error 0"), 0);
        assert_eq!(buffer.get_message_count("Error 1"), 0);

        // But total errors should still reflect all 12
        assert_eq!(buffer.total_errors, 12);

        // Message counts should only track current buffer entries
        let stats = buffer.get_stats();
        let total_in_counts: u64 = stats.message_counts.values().sum();
        assert_eq!(total_in_counts, 10); // Only currently buffered messages
    }

    #[test]
    fn test_error_entry_creation_with_timestamp() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entry = ErrorEntry::new("Test".to_string());

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(entry.timestamp >= before && entry.timestamp <= after + 1);
        assert_eq!(entry.message, "Test");
        assert_eq!(entry.count, 1);
    }

    #[test]
    fn test_get_messages_returns_clone() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Error 1".to_string());
        buffer.add_error("Error 2".to_string());

        let messages = buffer.get_messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message, "Error 1");
        assert_eq!(messages[1].message, "Error 2");
    }

    #[test]
    fn test_message_counts_hashmap() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("A".to_string());
        buffer.add_error("B".to_string());
        buffer.add_error("A".to_string());

        let counts = buffer.get_message_counts();
        assert_eq!(counts.get("A"), Some(&2));
        assert_eq!(counts.get("B"), Some(&1));
    }

    #[test]
    fn test_large_error_messages() {
        let mut buffer = ErrorMessageBuffer::new();
        let long_msg = "A".repeat(1000);

        buffer.add_error(long_msg.clone());
        buffer.add_error(long_msg.clone());

        assert_eq!(buffer.buffer_size(), 1);
        assert_eq!(buffer.get_message_count(&long_msg), 2);
    }

    #[test]
    fn test_special_characters_in_messages() {
        let mut buffer = ErrorMessageBuffer::new();

        buffer.add_error("Error: Connection failed\n at line 42".to_string());
        buffer.add_error("Error: \"Quoted\" and 'nested' strings".to_string());
        buffer.add_error("Error: Unicode émojis 🚀 supported".to_string());

        assert_eq!(buffer.buffer_size(), 3);
        assert_eq!(buffer.total_errors, 3);
    }

    #[test]
    fn test_error_entry_display() {
        let entry = ErrorEntry::new("Test error".to_string());
        let display_str = entry.to_string();

        assert!(display_str.contains("Test error"));
        assert!(display_str.contains("count: 1"));
        assert!(!display_str.contains("node_id="));
    }

    #[test]
    fn test_error_entry_display_with_node() {
        let entry = ErrorEntry::with_node("Test error".to_string(), "prod-node-1".to_string());
        let display_str = entry.to_string();

        assert!(display_str.contains("Test error"));
        assert!(display_str.contains("node=prod-node-1"));
        assert!(display_str.contains("count: 1"));
    }

    #[test]
    fn test_format_for_report_empty() {
        let buffer = ErrorMessageBuffer::new();
        let report = buffer.format_for_report();

        assert_eq!(report, "No errors recorded");
    }

    #[test]
    fn test_format_for_report_with_errors() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.add_error("Connection timeout".to_string());
        buffer.add_error("Invalid query".to_string());
        buffer.add_error("Connection timeout".to_string());

        let report = buffer.format_for_report();

        assert!(report.contains("=== Error Report ==="));
        assert!(report.contains("Total Errors: 3"));
        assert!(report.contains("Unique Errors: 2"));
        assert!(report.contains("Connection timeout"));
        assert!(report.contains("Invalid query"));
    }

    #[test]
    fn test_format_for_report_with_deployment_context() {
        let mut buffer = ErrorMessageBuffer::new();
        buffer.set_node_id(Some("prod-trading-node-1".to_string()));
        buffer.add_error("Market data error".to_string());
        buffer.add_error("Order execution failed".to_string());

        let report = buffer.format_for_report();

        assert!(report.contains("=== Error Report [Node: prod-trading-node-1] ==="));
        assert!(report.contains("Market data error"));
        assert!(report.contains("Order execution failed"));
        assert!(report.contains("[node=prod-trading-node-1]"));
    }

    // === Deployment Context Tests ===

    #[test]
    fn test_deployment_context_creation() {
        let ctx = DeploymentContext::new();
        assert!(ctx.node_id.is_none());
        assert!(ctx.node_name.is_none());
        assert!(ctx.region.is_none());
        assert!(ctx.version.is_none());
        assert!(!ctx.has_any());
    }

    #[test]
    fn test_deployment_context_with_all() {
        let ctx = DeploymentContext::with_all(
            "prod-trading-cluster-1".to_string(),
            "Production Trading Analytics Platform".to_string(),
            "us-east-1".to_string(),
            "1.0.0".to_string(),
        );

        assert_eq!(ctx.node_id, Some("prod-trading-cluster-1".to_string()));
        assert_eq!(
            ctx.node_name,
            Some("Production Trading Analytics Platform".to_string())
        );
        assert_eq!(ctx.region, Some("us-east-1".to_string()));
        assert_eq!(ctx.version, Some("1.0.0".to_string()));
        assert!(ctx.has_any());
    }

    #[test]
    fn test_error_entry_with_deployment_context() {
        let ctx = DeploymentContext::with_all(
            "prod-node-1".to_string(),
            "Production Node".to_string(),
            "us-west-2".to_string(),
            "2.0.0".to_string(),
        );

        let entry = ErrorEntry::with_deployment_context("Database error".to_string(), ctx.clone());

        assert_eq!(entry.message, "Database error");
        assert_eq!(entry.node_id, Some("prod-node-1".to_string()));
        assert!(entry.deployment_context.is_some());

        let display_str = entry.to_string();
        assert!(display_str.contains("Database error"));
        assert!(display_str.contains("node_id=prod-node-1"));
        assert!(display_str.contains("node_name=Production Node"));
        assert!(display_str.contains("region=us-west-2"));
        assert!(display_str.contains("version=2.0.0"));
    }

    #[test]
    fn test_error_buffer_with_deployment_context() {
        let mut buffer = ErrorMessageBuffer::new();

        let ctx = DeploymentContext::with_all(
            "prod-trading-cluster-1".to_string(),
            "Trading Analytics".to_string(),
            "us-east-1".to_string(),
            "1.0.0".to_string(),
        );

        buffer.set_deployment_context(ctx);
        buffer.add_error("Market data error".to_string());
        buffer.add_error("Order execution error".to_string());

        let messages = buffer.get_messages();
        assert_eq!(messages.len(), 2);

        // Verify first message has deployment context
        let first_msg = &messages[0];
        assert!(first_msg.deployment_context.is_some());
        assert_eq!(
            first_msg.deployment_context.as_ref().unwrap().node_id,
            Some("prod-trading-cluster-1".to_string())
        );

        // Verify display includes deployment metadata
        let display_str = first_msg.to_string();
        assert!(display_str.contains("node_id=prod-trading-cluster-1"));
        assert!(display_str.contains("node_name=Trading Analytics"));
        assert!(display_str.contains("region=us-east-1"));
        assert!(display_str.contains("version=1.0.0"));
    }

    #[test]
    fn test_format_for_report_with_full_deployment_context() {
        let mut buffer = ErrorMessageBuffer::new();

        let ctx = DeploymentContext::with_all(
            "prod-trading-cluster-1".to_string(),
            "Trading Analytics Platform".to_string(),
            "eu-west-1".to_string(),
            "2.5.0".to_string(),
        );

        buffer.set_deployment_context(ctx);
        buffer.add_error("Connection timeout".to_string());
        buffer.add_error("Query timeout".to_string());

        let report = buffer.format_for_report();

        assert!(report.contains("node_id=prod-trading-cluster-1"));
        assert!(report.contains("node_name=Trading Analytics Platform"));
        assert!(report.contains("region=eu-west-1"));
        assert!(report.contains("version=2.5.0"));
        assert!(report.contains("Total Errors: 2"));
        assert!(report.contains("Unique Errors: 2"));
    }

    #[test]
    fn test_error_buffer_get_deployment_context() {
        let mut buffer = ErrorMessageBuffer::new();
        assert!(buffer.get_deployment_context().is_none());

        let ctx = DeploymentContext::with_all(
            "node1".to_string(),
            "Node One".to_string(),
            "us-east-1".to_string(),
            "1.0.0".to_string(),
        );

        buffer.set_deployment_context(ctx.clone());
        let retrieved = buffer.get_deployment_context();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().node_id, Some("node1".to_string()));
        assert_eq!(retrieved.unwrap().node_name, Some("Node One".to_string()));
    }

    #[test]
    fn test_deployment_context_partial_fields() {
        let ctx = DeploymentContext {
            node_id: Some("prod-node".to_string()),
            node_name: None,
            region: Some("us-east-1".to_string()),
            version: None,
        };

        assert!(ctx.has_any());

        let entry = ErrorEntry::with_deployment_context("Error".to_string(), ctx);
        let display_str = entry.to_string();

        assert!(display_str.contains("node_id=prod-node"));
        assert!(display_str.contains("region=us-east-1"));
        assert!(!display_str.contains("node_name="));
        assert!(!display_str.contains("version="));
    }

    #[test]
    fn test_set_deployment_context_on_entry() {
        let mut entry = ErrorEntry::new("Test error".to_string());
        assert!(entry.deployment_context.is_none());

        let ctx = DeploymentContext::with_all(
            "node1".to_string(),
            "Test Node".to_string(),
            "us-west-2".to_string(),
            "1.5.0".to_string(),
        );

        entry.set_deployment_context(ctx);
        assert!(entry.deployment_context.is_some());

        let display_str = entry.to_string();
        assert!(display_str.contains("node_id=node1"));
        assert!(display_str.contains("Test Node"));
    }
}
