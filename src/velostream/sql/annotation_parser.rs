//! SQL Annotation Parser for Deployment Context and Job Configuration Extraction
//!
//! This module provides functionality to parse annotations from SQL files for both:
//! - Deployment context configuration
//! - Job processing and partitioning configuration
//!
//! Supported Deployment Annotations:
//! - `@deployment.node_id`: Node identifier (e.g., "prod-trading-cluster-${TRADING_POD_ID:1}")
//! - `@deployment.node_name`: Human-readable node name (e.g., "Production Trading Analytics Platform")
//! - `@deployment.region`: AWS region or deployment region (e.g., "${AWS_REGION:us-east-1}")
//!
//! Supported Job Annotations (defaults listed):
//! - `@job_mode`: Processor type - simple (default), transactional, or adaptive
//! - `@batch_size`: Batch processing size (integer, default: None)
//! - `@num_partitions`: Number of partitions for adaptive mode (integer, default: None)
//! - `@partitioning_strategy`: Routing strategy - sticky, hash, smart, roundrobin, fanin (default: None)
//!
//! Environment Variable Substitution:
//! - Format: `${VAR_NAME:default_value}`
//! - If VAR_NAME exists in environment, use its value
//! - Otherwise, use the default_value
//! - If no default provided and env var not found, keeps the original string

use crate::velostream::observability::error_tracker::DeploymentContext;
use crate::velostream::sql::ast::{JobProcessorMode, PartitioningStrategyType};
use log::{debug, info};
use std::collections::HashMap;
use std::env;

/// Parser for SQL annotations related to deployment context
#[derive(Debug, Clone)]
pub struct SqlAnnotationParser;

impl SqlAnnotationParser {
    /// Parse deployment context annotations from SQL content
    ///
    /// Extracts @deployment.* annotations and performs environment variable substitution.
    /// Returns a DeploymentContext with extracted and substituted values.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // SQL file content:
    /// // -- @deployment.node_id: prod-trading-cluster-${TRADING_POD_ID:1}
    /// // -- @deployment.node_name: Production Trading Analytics Platform
    /// // -- @deployment.region: ${AWS_REGION:us-east-1}
    ///
    /// let sql = "-- @deployment.node_id: prod-trading-cluster-${TRADING_POD_ID:1}
    ///            -- @deployment.region: ${AWS_REGION:us-east-1}";
    ///
    /// let ctx = SqlAnnotationParser::parse_deployment_context(sql);
    /// // Returns DeploymentContext with substituted values
    /// ```
    pub fn parse_deployment_context(sql_content: &str) -> DeploymentContext {
        let annotations = Self::extract_annotations(sql_content);

        let node_id = annotations
            .get("deployment.node_id")
            .map(|v| Self::substitute_env_vars(v))
            .filter(|v| !v.is_empty());

        let node_name = annotations
            .get("deployment.node_name")
            .map(|v| Self::substitute_env_vars(v))
            .filter(|v| !v.is_empty());

        let region = annotations
            .get("deployment.region")
            .map(|v| Self::substitute_env_vars(v))
            .filter(|v| !v.is_empty());

        let version = env::var("APP_VERSION").ok();

        // Log the parsed context
        if node_id.is_some() || node_name.is_some() || region.is_some() {
            info!(
                "🔍 Deployment context parsed from SQL annotations: \
                 node_id={:?}, node_name={:?}, region={:?}",
                node_id, node_name, region
            );
        }

        DeploymentContext {
            node_id,
            node_name,
            region,
            version,
        }
    }

    /// Parse job processing annotations from SQL content
    ///
    /// Extracts @job_mode, @batch_size, @num_partitions, and @partitioning_strategy annotations.
    /// Returns a tuple of (job_mode, batch_size, num_partitions, partitioning_strategy).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let sql = "-- @job_mode: adaptive
    ///            -- @batch_size: 1000
    ///            -- @num_partitions: 8
    ///            -- @partitioning_strategy: hash
    ///            SELECT * FROM market_data;";
    ///
    /// let (job_mode, batch_size, num_partitions, strategy) =
    ///     SqlAnnotationParser::parse_job_annotations(sql);
    /// // Returns:
    /// // job_mode: Some(JobProcessorMode::Adaptive)
    /// // batch_size: Some(1000)
    /// // num_partitions: Some(8)
    /// // strategy: Some(PartitioningStrategyType::Hash)
    /// ```
    pub fn parse_job_annotations(
        sql_content: &str,
    ) -> (
        Option<JobProcessorMode>,
        Option<usize>,
        Option<usize>,
        Option<PartitioningStrategyType>,
    ) {
        let annotations = Self::extract_annotations(sql_content);

        let job_mode = annotations
            .get("job_mode")
            .and_then(|v| Self::parse_job_mode(v));

        let batch_size = annotations
            .get("batch_size")
            .and_then(|v| v.parse::<usize>().ok());

        let num_partitions = annotations
            .get("num_partitions")
            .and_then(|v| v.parse::<usize>().ok());

        let partitioning_strategy = annotations
            .get("partitioning_strategy")
            .and_then(|v| Self::parse_partitioning_strategy(v));

        // Log parsed job annotations if any exist
        if job_mode.is_some() || batch_size.is_some() || num_partitions.is_some() || partitioning_strategy.is_some() {
            debug!(
                "🔍 Job annotations parsed from SQL: job_mode={:?}, batch_size={:?}, \
                 num_partitions={:?}, strategy={:?}",
                job_mode, batch_size, num_partitions, partitioning_strategy
            );
        }

        (job_mode, batch_size, num_partitions, partitioning_strategy)
    }

    /// Parse a job mode string into JobProcessorMode enum
    fn parse_job_mode(value: &str) -> Option<JobProcessorMode> {
        match value.to_lowercase().trim() {
            "simple" => Some(JobProcessorMode::Simple),
            "transactional" => Some(JobProcessorMode::Transactional),
            "adaptive" => Some(JobProcessorMode::Adaptive),
            _ => {
                debug!("Unknown job_mode value: '{}'. Valid values are: simple, transactional, adaptive", value);
                None
            }
        }
    }

    /// Parse a partitioning strategy string into PartitioningStrategyType enum
    fn parse_partitioning_strategy(value: &str) -> Option<PartitioningStrategyType> {
        match value.to_lowercase().trim() {
            "sticky" => Some(PartitioningStrategyType::Sticky),
            "hash" => Some(PartitioningStrategyType::Hash),
            "smart" => Some(PartitioningStrategyType::Smart),
            "roundrobin" => Some(PartitioningStrategyType::RoundRobin),
            "fanin" => Some(PartitioningStrategyType::FanIn),
            _ => {
                debug!("Unknown partitioning_strategy value: '{}'. Valid values are: sticky, hash, smart, roundrobin, fanin", value);
                None
            }
        }
    }

    /// Extract all @annotation keys from SQL content
    ///
    /// Supports both deployment annotations (@deployment.*) and job annotations
    /// (@job_mode, @batch_size, @num_partitions, @partitioning_strategy)
    fn extract_annotations(sql_content: &str) -> HashMap<String, String> {
        let mut annotations = HashMap::new();

        for line in sql_content.lines() {
            let trimmed = line.trim();

            // Look for lines starting with -- @
            if trimmed.starts_with("--") {
                let rest = &trimmed[2..].trim_start();

                // Match both deployment and job annotations
                if rest.starts_with("@deployment.") || rest.starts_with("@job_mode")
                    || rest.starts_with("@batch_size") || rest.starts_with("@num_partitions")
                    || rest.starts_with("@partitioning_strategy")
                {
                    // Extract key and value
                    if let Some(colon_pos) = rest.find(':') {
                        let key = &rest[1..colon_pos]; // Skip the @ prefix
                        let value = rest[colon_pos + 1..].trim();

                        // Clean up value (remove trailing comments if any)
                        let clean_value = if let Some(comment_pos) = value.find("--") {
                            value[..comment_pos].trim()
                        } else {
                            value
                        };

                        if !clean_value.is_empty() {
                            annotations.insert(key.to_string(), clean_value.to_string());
                            debug!("Parsed annotation: @{} = \"{}\"", key, clean_value);
                        }
                    }
                }
            }
        }

        annotations
    }

    /// Substitute environment variables in a value string
    ///
    /// Format: `${VAR_NAME:default_value}`
    /// - Extracts VAR_NAME and default_value
    /// - If VAR_NAME is in environment, returns its value
    /// - Otherwise returns default_value
    /// - If no default and env var not found, returns original string
    ///
    /// # Examples
    ///
    /// With env var `TRADING_POD_ID=42`:
    /// - `prod-cluster-${TRADING_POD_ID:1}` → `prod-cluster-42`
    /// - `${AWS_REGION:us-east-1}` → (if AWS_REGION not set) → `us-east-1`
    fn substitute_env_vars(value: &str) -> String {
        let mut result = value.to_string();
        let mut start_pos = 0;

        // Find all ${...} patterns
        while let Some(dollar_pos) = result[start_pos..].find("${") {
            let abs_dollar = start_pos + dollar_pos;

            if let Some(close_brace) = result[abs_dollar..].find('}') {
                let abs_close = abs_dollar + close_brace;

                // Extract content between ${ and } before any borrows
                let var_expr = result[abs_dollar + 2..abs_close].to_string();

                // Parse VAR_NAME:default_value
                let (var_name, default_value) = if let Some(colon_pos) = var_expr.find(':') {
                    let name = var_expr[..colon_pos].trim().to_string();
                    let default = var_expr[colon_pos + 1..].trim().to_string();
                    (name, Some(default))
                } else {
                    (var_expr.trim().to_string(), None)
                };

                // Get substitution value
                let substituted = env::var(&var_name)
                    .ok()
                    .or_else(|| default_value.clone())
                    .unwrap_or_else(|| format!("${{{}}}", var_expr)); // Keep original if no env var or default

                debug!(
                    "Substituted ${{{}}}: {} → {}",
                    var_expr, var_expr, substituted
                );

                // Replace the ${...} expression with substituted value
                result.replace_range(abs_dollar..abs_close + 1, &substituted);

                // Update start position to continue searching after the substitution
                start_pos = abs_dollar + substituted.len();
            } else {
                // Malformed ${...}, skip it
                start_pos = abs_dollar + 2;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_deployment_context_with_annotations() {
        let sql = r#"
-- @deployment.node_id: prod-trading-cluster-1
-- @deployment.node_name: Production Trading Analytics Platform
-- @deployment.region: us-east-1
SELECT * FROM market_data;
        "#;

        let ctx = SqlAnnotationParser::parse_deployment_context(sql);
        assert_eq!(ctx.node_id, Some("prod-trading-cluster-1".to_string()));
        assert_eq!(
            ctx.node_name,
            Some("Production Trading Analytics Platform".to_string())
        );
        assert_eq!(ctx.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_parse_deployment_context_no_annotations() {
        let sql = "SELECT * FROM market_data;";
        let ctx = SqlAnnotationParser::parse_deployment_context(sql);
        assert_eq!(ctx.node_id, None);
        assert_eq!(ctx.node_name, None);
        assert_eq!(ctx.region, None);
    }

    #[test]
    fn test_substitute_env_vars_with_defaults() {
        // This test uses default values since env vars may not be set
        let result = SqlAnnotationParser::substitute_env_vars("prod-cluster-${NONEXISTENT_VAR:1}");
        assert_eq!(result, "prod-cluster-1");

        let result = SqlAnnotationParser::substitute_env_vars("${AWS_REGION:us-east-1}");
        // Result will be either actual AWS_REGION value or us-east-1
        assert!(
            result == "us-east-1" || !result.contains("${"),
            "Result should be resolved: {}",
            result
        );
    }

    #[test]
    fn test_substitute_multiple_env_vars() {
        let result = SqlAnnotationParser::substitute_env_vars(
            "cluster-${NONEXISTENT1:prod}-node-${NONEXISTENT2:1}",
        );
        assert_eq!(result, "cluster-prod-node-1");
    }

    #[test]
    fn test_extract_annotations() {
        let sql = r#"
-- @deployment.node_id: node-1
-- Some random comment
-- @deployment.region: us-west-2
SELECT * FROM data;
        "#;

        let annotations = SqlAnnotationParser::extract_annotations(sql);
        assert_eq!(
            annotations.get("deployment.node_id"),
            Some(&"node-1".to_string())
        );
        assert_eq!(
            annotations.get("deployment.region"),
            Some(&"us-west-2".to_string())
        );
        assert_eq!(annotations.get("deployment.node_name"), None);
    }
}
