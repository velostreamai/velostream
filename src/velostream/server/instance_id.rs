//! Instance ID generation for Kafka client identification
//!
//! Generates unique instance identifiers for Kafka client.id properties.
//! Works across both Kubernetes (POD_NAME) and CLI (HOSTNAME) deployments.
//!
//! Format: {hostname|pod_name}-{uuid[0..4]}-{MMDDHHMMSS}
//! Example: trading-pod-5f3a-1119101530
//!
//! The MMDDHHMMSS timestamp captures startup time for:
//! - Operator visibility into run freshness
//! - Chronological sorting across month boundaries
//! - Uptime estimation from current time

use std::sync::OnceLock;
use uuid::Uuid;

/// Cache for the instance ID to ensure consistency across the application
static INSTANCE_ID: OnceLock<String> = OnceLock::new();

/// Get or generate a unique instance identifier
///
/// Uses a hybrid strategy for maximum compatibility:
/// 1. POD_NAME env var (Kubernetes)
/// 2. HOSTNAME env var (CLI/VM deployments)
/// 3. NODE_ID env var (manual override)
/// 4. Generate: {hostname|unknown}-{uuid[0..4]}-{MMDDHHMMSS}
///
/// The result is cached to ensure consistency across the application.
/// Timestamp is captured at first call and cached with the instance ID.
///
/// # Returns
/// A unique instance identifier suitable for Kafka client.id
/// Format: {hostname}-{uuid4_suffix}-{MMDDHHMMSS}
///
/// # Examples
/// - Kubernetes: `trading-pod-a3f2-1119101530`
/// - CLI: `server1-b4e9-1119101530`
/// - Fallback: `unknown-x7c8-1119101530`
pub fn get_instance_id() -> String {
    INSTANCE_ID
        .get_or_init(|| {
            let pod_name = std::env::var("POD_NAME").ok();
            let hostname = std::env::var("HOSTNAME").ok();
            let node_id = std::env::var("NODE_ID").ok();

            // Try to get base identifier from environment
            let base_id = pod_name
                .or(hostname)
                .or(node_id)
                .unwrap_or_else(|| "unknown".to_string());

            // Generate short UUID suffix (4 characters)
            let uuid_suffix = Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(4)
                .collect::<String>();

            // Capture startup timestamp in MMDDHHMMSS format
            let now = chrono::Utc::now();
            let timestamp = now.format("%m%d%H%M%S").to_string();

            // Sanitize the base ID (replace spaces/special chars with dashes)
            let sanitized = base_id
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '-' || c == '_' {
                        c.to_ascii_lowercase()
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
                .trim_matches('-')
                .to_string();

            // Combine: {base}-{uuid_suffix}-{MMDDHHMMSS}
            format!("{}-{}-{}", sanitized, uuid_suffix, timestamp)
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_id_format() {
        let id = get_instance_id();
        // Should contain dashes (format: base-uuid-timestamp)
        let dash_count = id.matches('-').count();
        assert!(
            dash_count >= 2,
            "Should have at least 2 dashes in format base-uuid-timestamp"
        );
        // Should be lowercase
        assert_eq!(id, id.to_lowercase());
        // Should end with MMDDHHMMSS timestamp (10 digits)
        let parts: Vec<&str> = id.split('-').collect();
        assert!(
            parts.len() >= 3,
            "Should have at least 3 parts: base, uuid, timestamp"
        );
        let timestamp_part = parts[parts.len() - 1];
        assert_eq!(
            timestamp_part.len(),
            10,
            "Timestamp should be MMDDHHMMSS (10 chars)"
        );
        // Verify timestamp is numeric
        assert!(
            timestamp_part.chars().all(|c| c.is_numeric()),
            "Timestamp should be all numeric"
        );
    }

    #[test]
    fn test_instance_id_consistency() {
        let id1 = get_instance_id();
        let id2 = get_instance_id();
        // Should return same ID on multiple calls
        assert_eq!(id1, id2);
    }
}
