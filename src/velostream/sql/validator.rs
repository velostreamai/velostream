//! SQL Validator Module
//!
//! Provides comprehensive SQL validation for Velostream applications
//! before deployment to StreamJobServer to prevent runtime failures.

use crate::velostream::sql::{
    ast::{Expr, SelectField, StreamingQuery, SubqueryType},
    config::with_clause_parser::WithClauseParser,
    parser::StreamingSqlParser,
    query_analyzer::{
        DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType, QueryAnalyzer,
    },
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidationError {
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
    pub severity: ErrorSeverity,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(line) = self.line {
            write!(f, "Line {}: {}", line, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum ErrorSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone)]
pub struct QueryValidationResult {
    pub query_text: String,
    pub query_index: usize,
    pub start_line: usize,
    pub is_valid: bool,
    pub parsing_errors: Vec<ValidationError>,
    pub configuration_errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationError>,
    pub sources_found: Vec<DataSourceRequirement>,
    pub sinks_found: Vec<DataSinkRequirement>,
    pub missing_source_configs: Vec<ConfigurationIssue>,
    pub missing_sink_configs: Vec<ConfigurationIssue>,
}

#[derive(Debug, Clone)]
pub struct ConfigurationIssue {
    pub name: String,
    pub required_keys: Vec<String>,
    pub missing_keys: Vec<String>,
    pub has_batch_config: bool,
    pub has_failure_strategy: bool,
}

#[derive(Debug, Clone)]
pub struct ApplicationValidationResult {
    pub file_path: String,
    pub application_name: Option<String>,
    pub is_valid: bool,
    pub total_queries: usize,
    pub valid_queries: usize,
    pub query_results: Vec<QueryValidationResult>,
    pub global_errors: Vec<String>,
    pub configuration_summary: ConfigurationSummary,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConfigurationSummary {
    pub sources: HashMap<String, String>,
    pub sinks: HashMap<String, String>,
    pub missing_configurations: Vec<String>,
    pub duplicate_names: Vec<String>,
}

/// Main SQL Validator for Velostream
pub struct SqlValidator {
    parser: StreamingSqlParser,
    analyzer: QueryAnalyzer,
    with_clause_parser: WithClauseParser,
    strict_mode: bool,
    pub check_performance: bool,
}

impl SqlValidator {
    /// Create a new SQL validator
    pub fn new() -> Self {
        Self {
            parser: StreamingSqlParser::new(),
            analyzer: QueryAnalyzer::new("sql-validator".to_string()),
            with_clause_parser: WithClauseParser::new(),
            strict_mode: false,
            check_performance: true,
        }
    }

    /// Create validator in strict mode (fails on warnings)
    pub fn new_strict() -> Self {
        Self {
            parser: StreamingSqlParser::new(),
            analyzer: QueryAnalyzer::new("sql-validator".to_string()),
            with_clause_parser: WithClauseParser::new(),
            strict_mode: true,
            check_performance: true,
        }
    }

    /// Validate SQL content from string - main integration point for StreamJobServer
    pub fn validate_sql_content(&self, content: &str) -> ApplicationValidationResult {
        let mut result = ApplicationValidationResult {
            file_path: "<content>".to_string(),
            application_name: None,
            is_valid: true,
            total_queries: 0,
            valid_queries: 0,
            query_results: Vec::new(),
            global_errors: Vec::new(),
            configuration_summary: ConfigurationSummary {
                sources: HashMap::new(),
                sinks: HashMap::new(),
                missing_configurations: Vec::new(),
                duplicate_names: Vec::new(),
            },
            recommendations: Vec::new(),
        };

        // Extract application name from comments
        result.application_name = self.extract_application_name(content);

        // Split content into individual SQL statements
        let queries = self.split_sql_statements(content);
        result.total_queries = queries.len();

        for (i, (query, start_line)) in queries.iter().enumerate() {
            let query_result = self.validate_query(query, i, *start_line, content);

            if query_result.is_valid {
                result.valid_queries += 1;
            } else {
                result.is_valid = false;
            }

            result.query_results.push(query_result);
        }

        // Analyze configuration completeness
        self.analyze_configuration_completeness(&mut result);

        result
    }

    /// Validate multiple SQL files in a directory
    pub fn validate_directory(&self, dir_path: &Path) -> Vec<ApplicationValidationResult> {
        let mut results = Vec::new();

        if let Ok(entries) = fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    results.push(self.validate_application_file(&path));
                }
            }
        }

        results
    }

    /// Alias for validate_application_file for compatibility
    pub fn validate_application(&self, file_path: &Path) -> ApplicationValidationResult {
        self.validate_application_file(file_path)
    }

    /// Validate a SQL application file
    pub fn validate_application_file(&self, file_path: &Path) -> ApplicationValidationResult {
        let mut result = ApplicationValidationResult {
            file_path: file_path.to_string_lossy().to_string(),
            application_name: None,
            is_valid: true,
            total_queries: 0,
            valid_queries: 0,
            query_results: Vec::new(),
            global_errors: Vec::new(),
            configuration_summary: ConfigurationSummary {
                sources: HashMap::new(),
                sinks: HashMap::new(),
                missing_configurations: Vec::new(),
                duplicate_names: Vec::new(),
            },
            recommendations: Vec::new(),
        };

        // Read the SQL file
        let content = match fs::read_to_string(file_path) {
            Ok(c) => c,
            Err(e) => {
                result
                    .global_errors
                    .push(format!("Failed to read file: {}", e));
                result.is_valid = false;
                return result;
            }
        };

        // Extract application name from comments
        result.application_name = self.extract_application_name(&content);

        // Split into individual queries with line tracking
        let queries = self.split_sql_statements(&content);
        result.total_queries = queries.len();

        for (i, (query, start_line)) in queries.iter().enumerate() {
            let query_result = self.validate_query(query, i, *start_line, &content);
            if query_result.is_valid {
                result.valid_queries += 1;
            } else {
                result.is_valid = false;
            }
            result.query_results.push(query_result);
        }

        // Analyze configuration completeness
        self.analyze_configuration_completeness(&mut result);
        result
    }

    /// Validate a single SQL query string
    pub fn validate_query(
        &self,
        query: &str,
        query_index: usize,
        start_line: usize,
        _full_content: &str,
    ) -> QueryValidationResult {
        let mut result = QueryValidationResult {
            query_text: query.to_string(),
            query_index,
            start_line,
            is_valid: true,
            parsing_errors: Vec::new(),
            configuration_errors: Vec::new(),
            warnings: Vec::new(),
            sources_found: Vec::new(),
            sinks_found: Vec::new(),
            missing_source_configs: Vec::new(),
            missing_sink_configs: Vec::new(),
        };

        // Try to parse the SQL statement
        let parsed_query = match self.parser.parse(query) {
            Ok(q) => q,
            Err(e) => {
                let (line, column, context) = self.extract_error_location(&e, query, start_line);
                result.parsing_errors.push(ValidationError {
                    message: format!("Parsing error: {}\n{}", e, context),
                    line: Some(line),
                    column: Some(column),
                    severity: ErrorSeverity::Error,
                });
                result.is_valid = false;
                return result;
            }
        };

        // Analyze the query for data sources and sinks
        match self.analyzer.analyze(&parsed_query) {
            Ok(analysis) => {
                result.sources_found = analysis.required_sources.clone();
                result.sinks_found = analysis.required_sinks.clone();

                // Validate source and sink configurations
                self.validate_source_configurations(&analysis.required_sources, &mut result);
                self.validate_sink_configurations(&analysis.required_sinks, &mut result);
            }
            Err(e) => {
                result.parsing_errors.push(ValidationError {
                    message: format!("Analysis error: {}", e),
                    line: Some(start_line),
                    column: None,
                    severity: ErrorSeverity::Error,
                });
                result.is_valid = false;
            }
        }

        // Add subquery validation using parsed AST
        self.validate_subquery_patterns_ast(&parsed_query, &mut result);

        result
    }

    // Helper methods - robust SQL statement splitting
    fn split_sql_statements(&self, content: &str) -> Vec<(String, usize)> {
        let mut queries = Vec::new();
        let mut current_query = String::new();
        let mut query_start_line = 0;
        let mut current_line = 0;
        let mut in_query = false;

        for line in content.lines() {
            current_line += 1;
            let trimmed = line.trim();

            // Skip empty lines and full-line comments
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            // Mark the start of a new query
            if !in_query {
                query_start_line = current_line;
                in_query = true;
            }

            current_query.push_str(line);
            current_query.push('\n');

            // Check for query termination
            if trimmed.ends_with(";") || trimmed.contains("EMIT CHANGES") {
                if !current_query.trim().is_empty() {
                    queries.push((current_query.trim().to_string(), query_start_line - 1));
                }
                current_query.clear();
                in_query = false;
            }
        }

        // Add any remaining query
        if !current_query.trim().is_empty() {
            queries.push((current_query.trim().to_string(), query_start_line - 1));
        }

        queries
    }

    fn extract_application_name(&self, content: &str) -> Option<String> {
        for line in content.lines() {
            if line.trim_start().starts_with("-- SQL Application:") {
                return Some(line.trim_start()[19..].trim().to_string());
            }
        }
        None
    }

    fn validate_source_configurations(
        &self,
        sources: &[DataSourceRequirement],
        result: &mut QueryValidationResult,
    ) {
        for source in sources {
            let properties = &source.properties;
            match &source.source_type {
                DataSourceType::Kafka => {
                    self.validate_kafka_source_config(properties, &source.name, result)
                }
                DataSourceType::File => {
                    self.validate_file_source_config(properties, &source.name, result)
                }
                _ => {
                    result.warnings.push(ValidationError {
                        message: format!("Unknown source type: {:?}", source.source_type),
                        line: None,
                        column: None,
                        severity: ErrorSeverity::Warning,
                    });
                }
            }
        }
    }

    fn validate_sink_configurations(
        &self,
        sinks: &[DataSinkRequirement],
        result: &mut QueryValidationResult,
    ) {
        for sink in sinks {
            let properties = &sink.properties;
            match &sink.sink_type {
                DataSinkType::Kafka => {
                    self.validate_kafka_sink_config(properties, &sink.name, result)
                }
                DataSinkType::File => {
                    self.validate_file_sink_config(properties, &sink.name, result)
                }
                _ => {
                    result.warnings.push(ValidationError {
                        message: format!("Unknown sink type: {:?}", sink.sink_type),
                        line: None,
                        column: None,
                        severity: ErrorSeverity::Warning,
                    });
                }
            }
        }
    }

    fn validate_kafka_source_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["bootstrap.servers", "topic"];
        let mut missing_keys = Vec::new();

        for key in &required_keys {
            if !properties.contains_key(*key) {
                missing_keys.push(key.to_string());
            }
        }

        if !missing_keys.is_empty() {
            result.missing_source_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: required_keys.iter().map(|s| s.to_string()).collect(),
                missing_keys,
                has_batch_config: properties.contains_key("batch.size")
                    || properties.contains_key("max.poll.records"),
                has_failure_strategy: properties.contains_key("failure_strategy"),
            });
            result.is_valid = false;
        }
    }

    fn validate_kafka_sink_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["bootstrap.servers", "topic"];
        let mut missing_keys = Vec::new();

        for key in &required_keys {
            if !properties.contains_key(*key) {
                missing_keys.push(key.to_string());
            }
        }

        if !missing_keys.is_empty() {
            result.missing_sink_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: required_keys.iter().map(|s| s.to_string()).collect(),
                missing_keys,
                has_batch_config: properties.contains_key("batch.size")
                    || properties.contains_key("linger.ms"),
                has_failure_strategy: properties.contains_key("failure_strategy"),
            });
            result.is_valid = false;
        }
    }

    fn validate_file_source_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["path", "format"];
        let mut missing_keys = Vec::new();

        for key in &required_keys {
            if !properties.contains_key(*key) {
                missing_keys.push(key.to_string());
            }
        }

        if !missing_keys.is_empty() {
            result.missing_source_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: required_keys.iter().map(|s| s.to_string()).collect(),
                missing_keys,
                has_batch_config: false,
                has_failure_strategy: properties.contains_key("failure_strategy"),
            });
            result.is_valid = false;
        }
    }

    fn validate_file_sink_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["path", "format"];
        let mut missing_keys = Vec::new();

        for key in &required_keys {
            if !properties.contains_key(*key) {
                missing_keys.push(key.to_string());
            }
        }

        if !missing_keys.is_empty() {
            result.missing_sink_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: required_keys.iter().map(|s| s.to_string()).collect(),
                missing_keys,
                has_batch_config: false,
                has_failure_strategy: properties.contains_key("failure_strategy"),
            });
            result.is_valid = false;
        }
    }

    fn analyze_configuration_completeness(&self, result: &mut ApplicationValidationResult) {
        // Collect all configuration issues from query results
        for query_result in &result.query_results {
            for source_issue in &query_result.missing_source_configs {
                result
                    .configuration_summary
                    .missing_configurations
                    .push(format!(
                        "Source '{}': missing {}",
                        source_issue.name,
                        source_issue.missing_keys.join(", ")
                    ));
            }
            for sink_issue in &query_result.missing_sink_configs {
                result
                    .configuration_summary
                    .missing_configurations
                    .push(format!(
                        "Sink '{}': missing {}",
                        sink_issue.name,
                        sink_issue.missing_keys.join(", ")
                    ));
            }
        }
    }

    /// Extract line, column, and context information from SQL parsing error
    fn extract_error_location(
        &self,
        error: &crate::velostream::sql::error::SqlError,
        query: &str,
        start_line: usize,
    ) -> (usize, usize, String) {
        // Try to extract position from error message
        let error_str = format!("{:?}", error);
        let position = self.extract_position_from_error(&error_str).unwrap_or(0);

        // Convert character position to line and column
        let (line_offset, column) = self.position_to_line_column(query, position);
        let actual_line = start_line + line_offset;

        // Generate context around the error
        let context = self.generate_error_context(query, position, line_offset, column);

        (actual_line, column, context)
    }

    /// Extract position number from error message
    fn extract_position_from_error(&self, error_str: &str) -> Option<usize> {
        // Look for patterns like "position 188" or "at position 188"
        let patterns = ["position ", "at position ", "character position "];

        for pattern in &patterns {
            if let Some(start) = error_str.find(pattern) {
                let after_pattern = &error_str[start + pattern.len()..];
                if let Some(end) = after_pattern.find(|c: char| !c.is_ascii_digit()) {
                    if let Ok(pos) = after_pattern[..end].parse::<usize>() {
                        return Some(pos);
                    }
                } else if let Ok(pos) = after_pattern.parse::<usize>() {
                    return Some(pos);
                }
            }
        }
        None
    }

    /// Convert character position to line and column numbers
    fn position_to_line_column(&self, text: &str, position: usize) -> (usize, usize) {
        let mut line = 0;
        let mut column = 0;
        let mut current_pos = 0;

        for ch in text.chars() {
            if current_pos >= position {
                break;
            }

            if ch == '\n' {
                line += 1;
                column = 0;
            } else {
                column += 1;
            }
            current_pos += ch.len_utf8();
        }

        (line, column)
    }

    /// Generate context showing where the error occurred
    fn generate_error_context(
        &self,
        query: &str,
        _position: usize,
        line_offset: usize,
        column: usize,
    ) -> String {
        let lines: Vec<&str> = query.lines().collect();

        if line_offset >= lines.len() {
            return "Error context not available".to_string();
        }

        let error_line = lines[line_offset];
        let mut context = String::new();

        // Show the line with the error
        context.push_str(&format!("  Line {}: {}\n", line_offset + 1, error_line));

        // Show pointer to the error location
        context.push_str(&format!(
            "  {}{}^ Error here",
            " ".repeat(8 + line_offset.to_string().len()), // Align with line number
            " ".repeat(column.saturating_sub(1))
        ));

        // If position is beyond the line, show it at the end
        if column >= error_line.len() {
            context.push_str(" (at end of line)");
        }

        context
    }

    /// Comprehensive subquery pattern validation
    fn validate_subquery_patterns(&self, result: &mut QueryValidationResult) {
        let query_text = &result.query_text;

        // Detect subquery patterns
        let has_exists_subquery = query_text.contains("EXISTS (") || query_text.contains("EXISTS(");
        let has_in_subquery = query_text.contains("IN (SELECT") || query_text.contains("IN(SELECT");
        let has_scalar_subquery = Self::detect_scalar_subquery(query_text);
        let has_any_subquery = query_text.matches("SELECT").count() > 1;

        if !has_any_subquery {
            return; // No subqueries detected
        }

        // Basic subquery warning
        if has_any_subquery && !has_exists_subquery && !has_in_subquery && !has_scalar_subquery {
            result.warnings.push(ValidationError {
                message:
                    "Complex subqueries detected - ensure they are supported in streaming context"
                        .to_string(),
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });
        }

        // EXISTS subquery validation
        if has_exists_subquery {
            result.warnings.push(ValidationError {
                message: "EXISTS subqueries require careful memory management in streaming - consider table materialization".to_string(),
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });

            // Check for correlation patterns
            if Self::detect_correlation_patterns(query_text) {
                result.warnings.push(ValidationError {
                    message: "Correlated EXISTS subquery detected - may impact streaming performance significantly".to_string(),
                    line: Some(result.start_line),
                    column: None,
                    severity: ErrorSeverity::Warning,
                });
            }
        }

        // IN subquery validation
        if has_in_subquery {
            result.warnings.push(ValidationError {
                message: "IN subqueries with SELECT may cause performance issues - consider JOIN or EXISTS".to_string(),
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });

            // Check for correlation in IN subquery
            if Self::detect_correlation_patterns(query_text) {
                result.warnings.push(ValidationError {
                    message: "Correlated IN subquery detected - consider rewriting as correlated EXISTS for better performance".to_string(),
                    line: Some(result.start_line),
                    column: None,
                    severity: ErrorSeverity::Warning,
                });
            }
        }

        // Scalar subquery validation
        if has_scalar_subquery {
            result.warnings.push(ValidationError {
                message: "Scalar subqueries in SELECT/WHERE detected - ensure single value return in streaming context".to_string(),
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });
        }

        // Complex nesting warning
        let select_count = query_text.matches("SELECT").count();
        if select_count > 3 {
            result.warnings.push(ValidationError {
                message: format!(
                    "Deeply nested subqueries detected ({} levels) - consider query simplification",
                    select_count
                ),
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });
        }

        // Check for unsupported WHERE clause patterns in subqueries
        if query_text.contains("WHERE") && has_any_subquery {
            let query_text_clone = query_text.clone();
            Self::validate_subquery_where_clauses(&query_text_clone, result);
        }
    }

    /// Detect scalar subqueries in SELECT or WHERE clauses
    fn detect_scalar_subquery(query_text: &str) -> bool {
        // Look for SELECT (subquery) patterns outside of EXISTS/IN contexts
        let select_positions: Vec<_> = query_text.match_indices("SELECT").collect();

        for (pos, _) in select_positions.iter().skip(1) {
            // Skip main SELECT
            let context_start = pos.saturating_sub(20);
            let context_end = (pos + 50).min(query_text.len());
            let context = &query_text[context_start..context_end];

            // Not part of EXISTS or IN subquery
            if !context.contains("EXISTS") && !context.contains(" IN ") {
                // Check if it's in a scalar context (SELECT list, WHERE condition)
                if context.contains("= (SELECT")
                    || context.contains("> (SELECT")
                    || context.contains("< (SELECT")
                    || context.contains(", (SELECT")
                    || context.starts_with("(SELECT")
                {
                    return true;
                }
            }
        }
        false
    }

    /// Detect correlation patterns (outer table references in subqueries)
    fn detect_correlation_patterns(query_text: &str) -> bool {
        // Look for table.column patterns that might indicate correlation
        // This is a heuristic - actual correlation detection would require full parsing

        // Common correlation patterns
        let correlation_indicators = [
            ".id",
            ".customer_id",
            ".order_id",
            ".user_id",
            ".product_id",
            "outer.",
            "main.",
            "parent.",
            "t1.",
            "t2.",
            "a.",
            "b.",
        ];

        for indicator in &correlation_indicators {
            if query_text.contains(indicator) {
                // Check if this appears in a subquery context
                if let Some(subquery_start) = query_text.find("(SELECT") {
                    let subquery_part = &query_text[subquery_start..];
                    if subquery_part.contains(indicator) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Validate WHERE clauses within subqueries for unsupported patterns
    fn validate_subquery_where_clauses(query_text: &str, result: &mut QueryValidationResult) {
        // Look for potentially problematic WHERE patterns in subqueries
        let problematic_patterns = [
            (
                "LIKE '%",
                "LIKE patterns starting with % in subqueries can be slow",
            ),
            (
                "REGEXP",
                "Regular expressions in subquery WHERE clauses may impact performance",
            ),
            (
                "SUBSTRING(",
                "String functions in subquery WHERE clauses may not be optimized",
            ),
            (
                "CASE WHEN",
                "Complex CASE expressions in subquery WHERE clauses may be slow",
            ),
        ];

        for (pattern, warning) in &problematic_patterns {
            if query_text.contains(pattern) {
                // Check if it's likely in a subquery
                if let Some(subquery_start) = query_text.find("(SELECT") {
                    let subquery_part = &query_text[subquery_start..];
                    if subquery_part.contains(pattern) {
                        result.warnings.push(ValidationError {
                            message: warning.to_string(),
                            line: Some(result.start_line),
                            column: None,
                            severity: ErrorSeverity::Warning,
                        });
                    }
                }
            }
        }
    }

    /// Enhanced AST-based subquery validation using parsed query structure
    fn validate_subquery_patterns_ast(
        &self,
        parsed_query: &StreamingQuery,
        result: &mut QueryValidationResult,
    ) {
        let mut subquery_analyzer = SubqueryAnalyzer::new();
        subquery_analyzer.analyze_query(parsed_query);

        // Report findings based on AST analysis
        for finding in subquery_analyzer.findings {
            result.warnings.push(ValidationError {
                message: finding.message,
                line: Some(result.start_line),
                column: None,
                severity: ErrorSeverity::Warning,
            });
        }
    }
}

/// AST-based subquery analyzer for precise detection
struct SubqueryAnalyzer {
    findings: Vec<SubqueryFinding>,
    subquery_depth: usize,
    max_depth: usize,
}

struct SubqueryFinding {
    message: String,
    subquery_type: Option<SubqueryType>,
}

impl SubqueryAnalyzer {
    fn new() -> Self {
        Self {
            findings: Vec::new(),
            subquery_depth: 0,
            max_depth: 10, // Prevent infinite recursion
        }
    }

    fn analyze_query(&mut self, query: &StreamingQuery) {
        // Prevent infinite recursion
        if self.subquery_depth >= self.max_depth {
            self.findings.push(SubqueryFinding {
                message: format!(
                    "Maximum subquery depth exceeded ({}) - possible circular reference",
                    self.max_depth
                ),
                subquery_type: None,
            });
            return;
        }

        match query {
            StreamingQuery::Select {
                where_clause,
                fields,
                ..
            } => {
                // Analyze WHERE clause for subqueries
                if let Some(where_expr) = where_clause {
                    self.analyze_expression(where_expr);
                }

                // Analyze SELECT fields for subqueries
                for field in fields {
                    if let SelectField::Expression { expr, .. } = field {
                        self.analyze_expression(expr);
                    }
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                self.analyze_query(as_select);
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                self.analyze_query(as_select);
            }
            StreamingQuery::CreateStreamInto { as_select, .. } => {
                self.analyze_query(as_select);
            }
            StreamingQuery::CreateTableInto { as_select, .. } => {
                self.analyze_query(as_select);
            }
            StreamingQuery::InsertInto { .. } => {
                // INSERT queries don't typically have subqueries we need to analyze
            }
            StreamingQuery::Update { where_clause, .. } => {
                if let Some(where_expr) = where_clause {
                    self.analyze_expression(where_expr);
                }
            }
            StreamingQuery::Delete { where_clause, .. } => {
                if let Some(where_expr) = where_clause {
                    self.analyze_expression(where_expr);
                }
            }
            StreamingQuery::Union { .. } => {
                // Union queries can contain nested SELECT statements but are handled at a higher level
                self.findings.push(SubqueryFinding {
                    message: "Complex subqueries detected (UNION) - ensure they are supported in streaming context".to_string(),
                    subquery_type: None,
                });
            }
            StreamingQuery::StartJob { query, .. } => {
                self.analyze_query(query);
            }
            // Other variants don't contain subqueries we need to analyze
            _ => {}
        }

        // Report nesting depth if significant
        if self.subquery_depth > 2 {
            self.findings.push(SubqueryFinding {
                message: format!(
                    "Deeply nested subqueries detected ({} levels) - consider query simplification",
                    self.subquery_depth + 1
                ),
                subquery_type: None,
            });
        }
    }

    fn analyze_expression(&mut self, expr: &Expr) {
        match expr {
            Expr::Subquery {
                query,
                subquery_type,
            } => {
                self.subquery_depth += 1;

                // Generate specific warnings based on subquery type
                match subquery_type {
                    SubqueryType::Exists => {
                        self.findings.push(SubqueryFinding {
                            message: "EXISTS subqueries require careful memory management in streaming - consider table materialization".to_string(),
                            subquery_type: Some(SubqueryType::Exists),
                        });

                        // Check for correlation by analyzing the subquery
                        if self.has_correlation_patterns(query) {
                            self.findings.push(SubqueryFinding {
                                message: "Correlated EXISTS subquery detected - may impact streaming performance significantly".to_string(),
                                subquery_type: Some(SubqueryType::Exists),
                            });
                        }
                    }
                    SubqueryType::NotExists => {
                        self.findings.push(SubqueryFinding {
                            message: "NOT EXISTS subqueries require careful memory management in streaming".to_string(),
                            subquery_type: Some(SubqueryType::NotExists),
                        });
                    }
                    SubqueryType::In => {
                        self.findings.push(SubqueryFinding {
                            message: "IN subqueries with SELECT may cause performance issues - consider JOIN or EXISTS".to_string(),
                            subquery_type: Some(SubqueryType::In),
                        });

                        if self.has_correlation_patterns(query) {
                            self.findings.push(SubqueryFinding {
                                message: "Correlated IN subquery detected - consider rewriting as correlated EXISTS for better performance".to_string(),
                                subquery_type: Some(SubqueryType::In),
                            });
                        }
                    }
                    SubqueryType::NotIn => {
                        self.findings.push(SubqueryFinding {
                            message: "NOT IN subqueries can be problematic with NULL values - consider NOT EXISTS".to_string(),
                            subquery_type: Some(SubqueryType::NotIn),
                        });
                    }
                    SubqueryType::Scalar => {
                        self.findings.push(SubqueryFinding {
                            message: "Scalar subqueries in SELECT/WHERE detected - ensure single value return in streaming context".to_string(),
                            subquery_type: Some(SubqueryType::Scalar),
                        });
                    }
                    SubqueryType::Any | SubqueryType::All => {
                        self.findings.push(SubqueryFinding {
                            message: format!(
                                "{:?} subqueries may have limited support in streaming context",
                                subquery_type
                            ),
                            subquery_type: Some(subquery_type.clone()),
                        });
                    }
                }

                // Recursively analyze the subquery
                self.analyze_query(query);
                self.subquery_depth -= 1;
            }
            Expr::BinaryOp { left, right, .. } => {
                self.analyze_expression(left);
                self.analyze_expression(right);
            }
            Expr::UnaryOp { expr, .. } => {
                self.analyze_expression(expr);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    self.analyze_expression(arg);
                }
            }
            Expr::WindowFunction { args, .. } => {
                for arg in args {
                    self.analyze_expression(arg);
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result_expr) in when_clauses {
                    self.analyze_expression(condition);
                    self.analyze_expression(result_expr);
                }
                if let Some(else_expr) = else_clause {
                    self.analyze_expression(else_expr);
                }
            }
            Expr::List(exprs) => {
                for expr in exprs {
                    self.analyze_expression(expr);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.analyze_expression(expr);
                self.analyze_expression(low);
                self.analyze_expression(high);
            }
            // Leaf nodes don't contain subqueries
            Expr::Column(_) | Expr::Literal(_) => {}
        }
    }

    /// Heuristic to detect correlation patterns in subqueries
    fn has_correlation_patterns(&self, _query: &StreamingQuery) -> bool {
        // For now, use a simple heuristic - could be enhanced with proper symbol table analysis
        // This would require analyzing if the subquery references columns from outer query
        // For demonstration, we'll return false and rely on the outer table.column pattern detection
        false
    }

    /// Split SQL content into individual queries with line tracking (from working binary implementation)
    fn split_sql_queries_with_lines(&self, content: &str) -> Vec<(String, usize)> {
        let mut queries = Vec::new();
        let mut current_query = String::new();
        let mut query_start_line = 0;
        let mut current_line = 0;
        let mut in_query = false;

        for line in content.lines() {
            current_line += 1;
            let trimmed = line.trim();

            // Skip empty lines and full-line comments
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            // Mark the start of a new query
            if !in_query {
                query_start_line = current_line;
                in_query = true;
            }

            current_query.push_str(line);
            current_query.push('\n');

            // Check for query termination
            if trimmed.ends_with(";") || trimmed.contains("EMIT CHANGES") {
                if !current_query.trim().is_empty() {
                    queries.push((current_query.trim().to_string(), query_start_line - 1));
                }
                current_query.clear();
                in_query = false;
            }
        }

        // Add any remaining query
        if !current_query.trim().is_empty() {
            queries.push((current_query.trim().to_string(), query_start_line - 1));
        }

        queries
    }

    /// Build configuration summary from all queries (adapted for library data structures)
    fn build_configuration_summary(
        &self,
        query_results: &[QueryValidationResult],
        summary: &mut ConfigurationSummary,
    ) {
        let mut source_names = std::collections::HashSet::new();
        let mut sink_names = std::collections::HashSet::new();

        for query_result in query_results {
            // Track source names for duplicates
            for source in &query_result.sources_found {
                let source_name = &source.name;
                if !source_names.insert(source_name.clone()) {
                    summary
                        .duplicate_names
                        .push(format!("Source '{}' defined multiple times", source_name));
                }
            }

            // Track sink names for duplicates
            for sink in &query_result.sinks_found {
                let sink_name = &sink.name;
                if !sink_names.insert(sink_name.clone()) {
                    summary
                        .duplicate_names
                        .push(format!("Sink '{}' defined multiple times", sink_name));
                }
            }

            // Collect missing configurations (convert ConfigurationIssue to string format)
            for config_issue in &query_result.missing_source_configs {
                summary.missing_configurations.push(format!(
                    "Source '{}': missing keys [{}]",
                    config_issue.name,
                    config_issue.missing_keys.join(", ")
                ));
            }
            for config_issue in &query_result.missing_sink_configs {
                summary.missing_configurations.push(format!(
                    "Sink '{}': missing keys [{}]",
                    config_issue.name,
                    config_issue.missing_keys.join(", ")
                ));
            }
        }
    }
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
}
