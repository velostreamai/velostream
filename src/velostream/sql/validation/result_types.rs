//! Result Types for SQL Validation
//!
//! This module contains all the result types used throughout the validation system.

use crate::velostream::sql::execution::processors::BatchValidationTarget;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Detailed parsing error with location information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsingError {
    pub message: String,
    pub line: usize,
    pub column: usize,
    pub position: usize,
    pub error_type: String,
    pub error_indicator: String,
}

/// Validation result for a single SQL query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryValidationResult {
    pub query_text: String,
    pub query_index: usize,
    pub start_line: usize,
    pub is_valid: bool,
    pub parsing_errors: Vec<ParsingError>,
    pub configuration_errors: Vec<String>,
    pub semantic_errors: Vec<String>,
    pub warnings: Vec<String>,
    pub sources_found: Vec<String>,
    pub sinks_found: Vec<String>,
    pub source_configs: HashMap<String, HashMap<String, String>>, // source_name -> flattened config
    pub sink_configs: HashMap<String, HashMap<String, String>>,   // sink_name -> flattened config
    pub missing_source_configs: Vec<String>,
    pub missing_sink_configs: Vec<String>,
    pub syntax_issues: Vec<String>,
    pub performance_warnings: Vec<String>,
}

impl QueryValidationResult {
    /// Create a new empty query validation result
    pub fn new(query_text: String) -> Self {
        Self {
            query_text,
            query_index: 0,
            start_line: 1,
            is_valid: true,
            parsing_errors: Vec::new(),
            configuration_errors: Vec::new(),
            semantic_errors: Vec::new(),
            warnings: Vec::new(),
            sources_found: Vec::new(),
            sinks_found: Vec::new(),
            source_configs: HashMap::new(),
            sink_configs: HashMap::new(),
            missing_source_configs: Vec::new(),
            missing_sink_configs: Vec::new(),
            syntax_issues: Vec::new(),
            performance_warnings: Vec::new(),
        }
    }

    /// Create a result for a parsing error
    pub fn parsing_error(query_text: String, error: ParsingError) -> Self {
        let mut result = Self::new(query_text);
        result.is_valid = false;
        result.parsing_errors.push(error);
        result
    }

    /// Add a configuration error
    pub fn add_configuration_error(&mut self, error: String) {
        self.configuration_errors.push(error);
        self.is_valid = false;
    }

    /// Add a semantic error
    pub fn add_semantic_error(&mut self, error: String) {
        self.semantic_errors.push(error);
        self.is_valid = false;
    }

    /// Add a syntax issue
    pub fn add_syntax_issue(&mut self, issue: String) {
        self.syntax_issues.push(issue);
    }

    /// Check if the query has any errors
    pub fn has_errors(&self) -> bool {
        !self.parsing_errors.is_empty()
            || !self.configuration_errors.is_empty()
            || !self.semantic_errors.is_empty()
    }

    /// Check if the query has any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty() || !self.syntax_issues.is_empty()
    }

    /// Get a summary of issues
    pub fn issue_summary(&self) -> String {
        let error_count = self.parsing_errors.len()
            + self.configuration_errors.len()
            + self.semantic_errors.len();
        let warning_count =
            self.warnings.len() + self.syntax_issues.len() + self.performance_warnings.len();

        match (error_count, warning_count) {
            (0, 0) => "No issues".to_string(),
            (0, w) => format!("{} warning(s)", w),
            (e, 0) => format!("{} error(s)", e),
            (e, w) => format!("{} error(s), {} warning(s)", e, w),
        }
    }
}

impl BatchValidationTarget for QueryValidationResult {
    fn query_text(&self) -> &str {
        &self.query_text
    }

    fn sources_found(&self) -> &[String] {
        &self.sources_found
    }

    fn sinks_found(&self) -> &[String] {
        &self.sinks_found
    }

    fn source_configs(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.source_configs
    }

    fn sink_configs(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.sink_configs
    }

    fn add_warning(&mut self, message: String) {
        self.warnings.push(message);
    }

    fn add_performance_warning(&mut self, message: String) {
        self.performance_warnings.push(message);
    }
}

/// Validation result for an entire SQL application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationValidationResult {
    pub file_path: String,
    pub application_name: String,
    pub description: String,
    pub version: String,
    pub is_valid: bool,
    pub total_queries: usize,
    pub valid_queries: usize,
    pub query_results: Vec<QueryValidationResult>,
    pub recommendations: Vec<String>,
    pub configuration_summary: Vec<String>,
    pub file_errors: Vec<String>,
}

impl ApplicationValidationResult {
    /// Create a new application validation result
    pub fn new(file_path: String, application_name: String) -> Self {
        Self {
            file_path,
            application_name,
            description: String::new(),
            version: String::new(),
            is_valid: true,
            total_queries: 0,
            valid_queries: 0,
            query_results: Vec::new(),
            recommendations: Vec::new(),
            configuration_summary: Vec::new(),
            file_errors: Vec::new(),
        }
    }

    /// Create a result for a file error (e.g., file not found)
    pub fn file_error(file_path: String, error: String) -> Self {
        let mut result = Self::new(file_path, "Unknown".to_string());
        result.is_valid = false;
        result.file_errors.push(error);
        result
    }

    /// Update validation statistics
    pub fn update_statistics(&mut self) {
        self.total_queries = self.query_results.len();
        self.valid_queries = self.query_results.iter().filter(|r| r.is_valid).count();
        self.is_valid = self.file_errors.is_empty() && self.valid_queries == self.total_queries;
    }

    /// Get overall validation status as string
    pub fn status_string(&self) -> String {
        if !self.file_errors.is_empty() {
            "FILE ERROR".to_string()
        } else if self.is_valid {
            "VALID".to_string()
        } else {
            "INVALID".to_string()
        }
    }

    /// Get status emoji
    pub fn status_emoji(&self) -> &'static str {
        if !self.file_errors.is_empty() {
            "🚫"
        } else if self.is_valid {
            "✅"
        } else {
            "❌"
        }
    }

    /// Get validation summary
    pub fn validation_summary(&self) -> String {
        if !self.file_errors.is_empty() {
            format!("File errors: {}", self.file_errors.len())
        } else {
            format!(
                "Queries: {}/{} valid",
                self.valid_queries, self.total_queries
            )
        }
    }
}
