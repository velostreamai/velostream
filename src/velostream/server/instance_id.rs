//! Instance ID generation for Kafka client identification
//!
//! Generates unique instance identifiers for Kafka client.id properties.
//! Works across both Kubernetes (POD_NAME) and CLI (HOSTNAME) deployments.

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
/// 4. Generate: {hostname|unknown}-{uuid[0..8]}
///
/// The result is cached to ensure consistency across the application.
///
/// # Returns
/// A unique instance identifier suitable for Kafka client.id
///
/// # Examples
/// - Kubernetes: `trading-pod-a3f2`
/// - CLI: `server1-b4e9`
/// - Fallback: `unknown-x7c8`
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

            // Generate short UUID suffix (8 characters)
            let uuid_suffix = Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(4)
                .collect::<String>();

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

            // Combine: {base}-{uuid_suffix}
            format!("{}-{}", sanitized, uuid_suffix)
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_id_format() {
        let id = get_instance_id();
        // Should contain a dash
        assert!(id.contains('-'));
        // Should be lowercase (mostly)
        assert_eq!(id, id.to_lowercase());
        // Should be reasonably short (base + 4 char uuid)
        assert!(id.len() < 50);
    }

    #[test]
    fn test_instance_id_consistency() {
        let id1 = get_instance_id();
        let id2 = get_instance_id();
        // Should return same ID on multiple calls
        assert_eq!(id1, id2);
    }
}
