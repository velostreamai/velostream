//! Report Generation Module
//!
//! Handles generation of validation reports and summaries.

use super::result_types::{ApplicationValidationResult, QueryValidationResult};
use super::ApplicationMetadata;
use std::path::Path;

/// Generates validation reports and summaries
pub struct ValidationReportGenerator {
    include_performance_tips: bool,
    include_configuration_summary: bool,
}

impl ValidationReportGenerator {
    /// Create a new report generator
    pub fn new() -> Self {
        Self {
            include_performance_tips: true,
            include_configuration_summary: true,
        }
    }

    /// Create a minimal report generator (errors and warnings only)
    pub fn new_minimal() -> Self {
        Self {
            include_performance_tips: false,
            include_configuration_summary: false,
        }
    }

    /// Enable or disable performance tips
    pub fn with_performance_tips(mut self, enabled: bool) -> Self {
        self.include_performance_tips = enabled;
        self
    }

    /// Enable or disable configuration summary
    pub fn with_configuration_summary(mut self, enabled: bool) -> Self {
        self.include_configuration_summary = enabled;
        self
    }

    /// Generate application validation result
    pub fn generate_application_result(
        &self,
        file_path: &Path,
        metadata: ApplicationMetadata,
        query_results: Vec<QueryValidationResult>,
    ) -> ApplicationValidationResult {
        let mut result = ApplicationValidationResult::new(
            file_path.to_string_lossy().to_string(),
            metadata.name.clone(),
        );

        result.description = metadata.description;
        result.version = metadata.version;
        result.query_results = query_results;

        // Update statistics
        result.update_statistics();

        // Generate recommendations
        self.generate_recommendations(&mut result);

        // Generate configuration summary
        if self.include_configuration_summary {
            self.generate_configuration_summary(&mut result);
        }

        result
    }

    // Private implementation methods
    fn generate_recommendations(&self, result: &mut ApplicationValidationResult) {
        let mut recommendations = Vec::new();

        // Collect unique recommendations from all queries
        let mut seen_recommendations = std::collections::HashSet::new();
        for query_result in &result.query_results {
            for warning in &query_result.performance_warnings {
                if warning.starts_with("⚠️") || warning.starts_with("💡") {
                    if seen_recommendations.insert(warning.clone()) {
                        recommendations.push(warning.clone());
                    }
                }
            }
        }

        // Add performance tips if enabled
        if self.include_performance_tips {
            self.add_performance_tips(&mut recommendations, result);
        }

        // Add architecture recommendations
        self.add_architecture_recommendations(&mut recommendations, result);

        result.recommendations = recommendations;
    }

    fn add_performance_tips(
        &self,
        recommendations: &mut Vec<String>,
        result: &ApplicationValidationResult,
    ) {
        let total_sources = result
            .query_results
            .iter()
            .map(|r| r.sources_found.len())
            .sum::<usize>();

        let total_sinks = result
            .query_results
            .iter()
            .map(|r| r.sinks_found.len())
            .sum::<usize>();

        if total_sources > 3 || total_sinks > 3 {
            recommendations.push(
                "💡 Consider using connection pooling for multiple Kafka sources/sinks".to_string(),
            );
        }

        let has_joins = result
            .query_results
            .iter()
            .any(|r| r.query_text.contains("JOIN"));

        if has_joins {
            recommendations
                .push("💡 Review JOIN operations for proper time-based windowing".to_string());
        }

        let has_aggregations = result
            .query_results
            .iter()
            .any(|r| r.query_text.contains("GROUP BY"));

        if has_aggregations {
            recommendations
                .push("💡 Consider state management for aggregation operations".to_string());
        }
    }

    fn add_architecture_recommendations(
        &self,
        recommendations: &mut Vec<String>,
        result: &ApplicationValidationResult,
    ) {
        if result.total_queries > 5 {
            recommendations.push(
                "💡 Consider splitting large applications into multiple files or deployment stages"
                    .to_string(),
            );
        }

        let has_complex_queries = result
            .query_results
            .iter()
            .any(|r| r.query_text.len() > 1000);

        if has_complex_queries {
            recommendations.push(
                "💡 Complex queries can be broken down into simpler streaming steps".to_string(),
            );
        }

        let error_rate = if result.total_queries > 0 {
            (result.total_queries - result.valid_queries) as f64 / result.total_queries as f64
        } else {
            0.0
        };

        if error_rate > 0.3 {
            recommendations.push(
                "⚠️ High error rate detected - review SQL syntax and configuration files"
                    .to_string(),
            );
        }
    }

    fn generate_configuration_summary(&self, result: &mut ApplicationValidationResult) {
        let mut summary = Vec::new();

        // Summarize data sources
        let mut all_sources = std::collections::HashSet::new();
        let mut all_sinks = std::collections::HashSet::new();

        for query_result in &result.query_results {
            all_sources.extend(query_result.sources_found.iter().cloned());
            all_sinks.extend(query_result.sinks_found.iter().cloned());
        }

        if !all_sources.is_empty() {
            summary.push(format!(
                "📊 Data Sources: {}",
                all_sources.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }

        if !all_sinks.is_empty() {
            summary.push(format!(
                "📤 Data Sinks: {}",
                all_sinks.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }

        // Summarize configuration completeness
        let mut missing_configs = 0;
        for query_result in &result.query_results {
            missing_configs += query_result.missing_source_configs.len();
            missing_configs += query_result.missing_sink_configs.len();
        }

        if missing_configs > 0 {
            summary.push(format!(
                "⚠️ Missing Configurations: {} source/sink configs need attention",
                missing_configs
            ));
        } else {
            summary.push("✅ All source/sink configurations found".to_string());
        }

        // Summarize serialization formats
        let mut formats = std::collections::HashSet::new();
        for query_result in &result.query_results {
            for warning in &query_result.performance_warnings {
                if warning.contains("Serialization format detected as") {
                    if let Some(format_start) = warning.find("detected as '") {
                        if let Some(format_end) = warning[format_start + 13..].find('\'') {
                            let format =
                                &warning[format_start + 13..format_start + 13 + format_end];
                            formats.insert(format.to_string());
                        }
                    }
                }
            }
        }

        if !formats.is_empty() {
            summary.push(format!(
                "🔄 Serialization Formats: {}",
                formats.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }

        result.configuration_summary = summary;
    }
}

impl Default for ValidationReportGenerator {
    fn default() -> Self {
        Self::new()
    }
}
