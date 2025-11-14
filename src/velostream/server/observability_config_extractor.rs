//! Observability Configuration Extraction
//!
//! This module provides utilities to extract observability settings from SQL applications
//! and individual SQL queries, supporting both application-level and query-level configurations.

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::{
    ProfilingConfig, PrometheusConfig, StreamingConfig, TracingConfig,
};
use log::debug;

/// Extracts and merges observability configurations from multiple sources
#[derive(Debug, Clone)]
pub struct ObservabilityConfigExtractor;

impl ObservabilityConfigExtractor {
    /// Extract observability settings from SQL string comment annotations
    ///
    /// Parses comment annotations like:
    /// - -- @observability.metrics.enabled: true
    /// - -- @observability.tracing.enabled: true
    /// - -- @observability.profiling.enabled: prod
    ///
    /// # Arguments
    /// * `sql` - SQL string that may contain observability annotations
    ///
    /// # Returns
    /// * `Result<StreamingConfig, SqlError>` - Config with extracted observability settings
    pub fn extract_from_sql_string(sql: &str) -> Result<StreamingConfig, SqlError> {
        let mut config = StreamingConfig::default();

        // Extract @observability.metrics.enabled: true/false
        if let Some(cap_pos) = sql.find("@observability.metrics.enabled") {
            let after = &sql[cap_pos..];
            if let Some(colon_pos) = after.find(':') {
                let value_part = &after[colon_pos + 1..];
                let value = value_part
                    .trim_start()
                    .split(|c: char| c.is_whitespace() || c == '\n' || c == '\r')
                    .next()
                    .unwrap_or("true")
                    .to_lowercase();
                if value == "true" {
                    config.enable_prometheus_metrics = true;
                    debug!("Extracted from SQL annotation: observability.metrics.enabled = true");

                    if config.prometheus_config.is_none() {
                        config.prometheus_config = Some(PrometheusConfig::default());
                    }
                }
            }
        }

        // Extract @observability.tracing.enabled: true/false
        if let Some(cap_pos) = sql.find("@observability.tracing.enabled") {
            let after = &sql[cap_pos..];
            if let Some(colon_pos) = after.find(':') {
                let value_part = &after[colon_pos + 1..];
                let value = value_part
                    .trim_start()
                    .split(|c: char| c.is_whitespace() || c == '\n' || c == '\r')
                    .next()
                    .unwrap_or("true")
                    .to_lowercase();
                if value == "true" {
                    config.enable_distributed_tracing = true;
                    debug!("Extracted from SQL annotation: observability.tracing.enabled = true");

                    if config.tracing_config.is_none() {
                        config.tracing_config = Some(TracingConfig::development());
                    }
                }
            }
        }

        // Extract @observability.profiling.enabled: prod/dev/off
        if let Some(cap_pos) = sql.find("@observability.profiling.enabled") {
            let after = &sql[cap_pos..];
            if let Some(colon_pos) = after.find(':') {
                let value_part = &after[colon_pos + 1..];
                let value = value_part
                    .trim_start()
                    .split(|c: char| c.is_whitespace() || c == '\n' || c == '\r')
                    .next()
                    .unwrap_or("prod")
                    .to_lowercase();
                if value != "off" {
                    config.enable_performance_profiling = true;
                    debug!(
                        "Extracted from SQL annotation: observability.profiling.enabled = {} (mode)",
                        value
                    );

                    if config.profiling_config.is_none() {
                        config.profiling_config = Some(ProfilingConfig::development());
                    }
                }
            }
        }

        Ok(config)
    }

    /// Merge annotation-based config with base config, preserving explicit settings
    ///
    /// Application-level observability settings are merged into job-level configs
    /// with a hierarchy: job-level WITH clause > application-level annotations
    ///
    /// # Arguments
    /// * `base_config` - Base configuration (typically from WITH clause)
    /// * `annotation_config` - Configuration extracted from annotations
    ///
    /// # Returns
    /// * Merged configuration where explicit settings are preserved
    pub fn merge_configs(
        mut base_config: StreamingConfig,
        annotation_config: StreamingConfig,
    ) -> StreamingConfig {
        // Only apply annotation settings if not already explicitly set
        if !base_config.enable_distributed_tracing && annotation_config.enable_distributed_tracing {
            base_config.enable_distributed_tracing = true;
            if base_config.tracing_config.is_none() {
                base_config.tracing_config = annotation_config.tracing_config;
            }
        }

        if !base_config.enable_prometheus_metrics && annotation_config.enable_prometheus_metrics {
            base_config.enable_prometheus_metrics = true;
            if base_config.prometheus_config.is_none() {
                base_config.prometheus_config = annotation_config.prometheus_config;
            }
        }

        if !base_config.enable_performance_profiling
            && annotation_config.enable_performance_profiling
        {
            base_config.enable_performance_profiling = true;
            if base_config.profiling_config.is_none() {
                base_config.profiling_config = annotation_config.profiling_config;
            }
        }

        base_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metrics_enabled() {
        let sql =
            "-- @observability.metrics.enabled: true\nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(config.enable_prometheus_metrics);
        assert!(config.prometheus_config.is_some());
    }

    #[test]
    fn test_extract_tracing_enabled() {
        let sql =
            "-- @observability.tracing.enabled: true\nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(config.enable_distributed_tracing);
        assert!(config.tracing_config.is_some());
    }

    #[test]
    fn test_extract_profiling_enabled() {
        let sql =
            "-- @observability.profiling.enabled: prod\nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(config.enable_performance_profiling);
        assert!(config.profiling_config.is_some());
    }

    #[test]
    fn test_extract_profiling_off() {
        let sql =
            "-- @observability.profiling.enabled: off\nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(!config.enable_performance_profiling);
    }

    #[test]
    fn test_extract_multiple_settings() {
        let sql = r#"
            -- @observability.metrics.enabled: true
            -- @observability.tracing.enabled: true
            -- @observability.profiling.enabled: prod
            CREATE STREAM test AS SELECT * FROM data
        "#;
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(config.enable_prometheus_metrics);
        assert!(config.enable_distributed_tracing);
        assert!(config.enable_performance_profiling);
    }

    #[test]
    fn test_extract_no_annotations() {
        let sql = "CREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(!config.enable_prometheus_metrics);
        assert!(!config.enable_distributed_tracing);
        assert!(!config.enable_performance_profiling);
    }

    #[test]
    fn test_merge_configs_preserves_explicit_settings() {
        let base_config = StreamingConfig {
            enable_prometheus_metrics: true, // Explicitly set in WITH clause
            ..StreamingConfig::default()
        };

        let annotation_config = StreamingConfig {
            enable_prometheus_metrics: false, // Not set in annotations
            enable_distributed_tracing: true, // Set in annotations
            ..StreamingConfig::default()
        };

        let merged = ObservabilityConfigExtractor::merge_configs(base_config, annotation_config);

        // Explicitly set in base config should be preserved
        assert!(merged.enable_prometheus_metrics);
        // New setting from annotation should be applied
        assert!(merged.enable_distributed_tracing);
    }

    #[test]
    fn test_merge_configs_applies_annotations() {
        let base_config = StreamingConfig::default(); // No explicit settings

        let annotation_config = StreamingConfig {
            enable_prometheus_metrics: true,
            enable_distributed_tracing: true,
            ..StreamingConfig::default()
        };

        let merged = ObservabilityConfigExtractor::merge_configs(base_config, annotation_config);

        // Annotation settings should be applied
        assert!(merged.enable_prometheus_metrics);
        assert!(merged.enable_distributed_tracing);
    }

    #[test]
    fn test_extract_metrics_disabled() {
        let sql =
            "-- @observability.metrics.enabled: false\nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(!config.enable_prometheus_metrics);
    }

    #[test]
    fn test_extract_annotation_with_whitespace() {
        let sql = "-- @observability.metrics.enabled:   true   \nCREATE STREAM test AS SELECT * FROM data";
        let config = ObservabilityConfigExtractor::extract_from_sql_string(sql).unwrap();
        assert!(config.enable_prometheus_metrics);
    }
}
