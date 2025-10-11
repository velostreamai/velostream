//! SQL Validator Module
//!
//! A modular, object-oriented SQL validation system for Velostream applications.
//! This module provides validation for SQL queries, configurations, and applications
//! with clear separation of concerns and extensible architecture.

use serde::{Deserialize, Serialize};

// Re-export main components
pub mod config_validator;
pub mod error_formatter;
pub mod function_registry;
pub mod query_validator;
pub mod report_generator;
pub mod result_types;
pub mod semantic_validator;

// Re-export for convenience
pub use config_validator::*;
pub use error_formatter::*;
pub use query_validator::*;
pub use report_generator::*;
pub use result_types::*;

/// Main entry point for SQL validation
pub struct SqlValidationService {
    query_validator: QueryValidator,
    config_validator: ConfigurationValidator,
    report_generator: ValidationReportGenerator,
    error_formatter: ValidationErrorFormatter,
    strict_mode: bool,
    performance_checks: bool,
}

impl Default for SqlValidationService {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlValidationService {
    /// Create a new SQL validation service
    pub fn new() -> Self {
        Self {
            query_validator: QueryValidator::new(),
            config_validator: ConfigurationValidator::new(),
            report_generator: ValidationReportGenerator::new(),
            error_formatter: ValidationErrorFormatter::new(),
            strict_mode: false,
            performance_checks: true,
        }
    }

    /// Create validator in strict mode (fails on warnings)
    pub fn new_strict() -> Self {
        Self {
            query_validator: QueryValidator::new_strict(),
            config_validator: ConfigurationValidator::new_strict(),
            report_generator: ValidationReportGenerator::new(),
            error_formatter: ValidationErrorFormatter::new(),
            strict_mode: true,
            performance_checks: true,
        }
    }

    /// Enable or disable performance checks
    pub fn with_performance_checks(mut self, enabled: bool) -> Self {
        self.performance_checks = enabled;
        self.query_validator = self.query_validator.with_performance_checks(enabled);
        self
    }

    /// Validate a single SQL query
    pub fn validate_query(&self, query: &str) -> QueryValidationResult {
        self.query_validator.validate_query(query)
    }

    /// Validate an entire SQL application file
    pub fn validate_application(&self, file_path: &std::path::Path) -> ApplicationValidationResult {
        // Read the file
        let content = match std::fs::read_to_string(file_path) {
            Ok(content) => content,
            Err(e) => {
                return ApplicationValidationResult::file_error(
                    file_path.to_string_lossy().to_string(),
                    format!("Failed to read file: {}", e),
                );
            }
        };

        // Parse application metadata
        let app_metadata = self.extract_application_metadata(&content);

        // Split into individual queries
        let queries = self.split_queries(&content);

        // Validate each query
        let mut query_results = Vec::new();
        for (index, query_text) in queries.iter().enumerate() {
            let mut result = self.query_validator.validate_query(query_text);
            result.query_index = index;
            result.start_line = self.calculate_query_start_line(&content, index);
            query_results.push(result);
        }

        // Validate configurations
        self.config_validator
            .validate_configurations(&mut query_results);

        // Generate comprehensive application result
        self.generate_application_result(file_path, app_metadata, query_results)
    }

    /// Validate multiple SQL files in a directory
    pub fn validate_directory(
        &self,
        dir_path: &std::path::Path,
    ) -> Vec<ApplicationValidationResult> {
        let mut results = Vec::new();

        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    results.push(self.validate_application(&path));
                }
            }
        }

        results
    }

    // Private helper methods
    fn extract_application_metadata(&self, content: &str) -> ApplicationMetadata {
        // Extract metadata from SQL comments
        let mut metadata = ApplicationMetadata::default();

        for line in content.lines().take(20) {
            let line = line.trim();
            if line.starts_with("-- Application:") {
                metadata.name = line
                    .strip_prefix("-- Application:")
                    .unwrap_or("")
                    .trim()
                    .to_string();
            } else if line.starts_with("-- Description:") {
                metadata.description = line
                    .strip_prefix("-- Description:")
                    .unwrap_or("")
                    .trim()
                    .to_string();
            } else if line.starts_with("-- Version:") {
                metadata.version = line
                    .strip_prefix("-- Version:")
                    .unwrap_or("")
                    .trim()
                    .to_string();
            }
        }

        metadata
    }

    fn split_queries(&self, content: &str) -> Vec<String> {
        // Split queries by semicolon (excluding those in strings or comments)
        let mut queries = Vec::new();
        let mut current_query = String::new();
        let mut in_string = false;
        let mut in_comment = false;
        let mut chars = content.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '\'' if !in_comment => in_string = !in_string,
                '-' if !in_string && !in_comment => {
                    if chars.peek() == Some(&'-') {
                        chars.next(); // consume second -
                        in_comment = true;
                        current_query.push_str("--");
                        continue;
                    }
                }
                '\n' if in_comment => in_comment = false,
                ';' if !in_string && !in_comment => {
                    current_query.push(ch);
                    let trimmed = current_query.trim();
                    if !trimmed.is_empty() && !trimmed.starts_with("--") {
                        queries.push(current_query.trim().to_string());
                    }
                    current_query.clear();
                    continue;
                }
                _ => {}
            }
            current_query.push(ch);
        }

        // Add remaining content
        let trimmed = current_query.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            queries.push(trimmed.to_string());
        }

        queries
    }

    fn calculate_query_start_line(&self, content: &str, query_index: usize) -> usize {
        // Calculate the starting line number for a query
        let queries = self.split_queries(content);
        if query_index >= queries.len() {
            return 1;
        }

        let mut line_number = 1;
        let target_query = &queries[query_index];

        for line in content.lines() {
            if line
                .trim()
                .starts_with(&target_query[..std::cmp::min(50, target_query.len())])
            {
                return line_number;
            }
            line_number += 1;
        }

        1
    }

    fn generate_application_result(
        &self,
        file_path: &std::path::Path,
        metadata: ApplicationMetadata,
        query_results: Vec<QueryValidationResult>,
    ) -> ApplicationValidationResult {
        self.report_generator
            .generate_application_result(file_path, metadata, query_results)
    }
}

/// Application metadata extracted from SQL comments
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApplicationMetadata {
    pub name: String,
    pub description: String,
    pub version: String,
}
