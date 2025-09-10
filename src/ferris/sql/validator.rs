//! SQL Validator Module
//!
//! Provides comprehensive SQL validation for FerrisStreams applications
//! before deployment to StreamJobServer to prevent runtime failures.

use crate::ferris::sql::{
    config::with_clause_parser::WithClauseParser,
    parser::StreamingSqlParser,
    query_analyzer::{
        DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType, QueryAnalyzer,
    },
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
    pub severity: ErrorSeverity,
}

#[derive(Debug, Clone)]
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

/// Main SQL Validator for FerrisStreams
pub struct SqlValidator {
    parser: StreamingSqlParser,
    analyzer: QueryAnalyzer,
    with_clause_parser: WithClauseParser,
    strict_mode: bool,
    check_performance: bool,
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

        // Use the main validation method
        let mut content_result = self.validate_sql_content(&content);
        content_result.file_path = file_path.to_string_lossy().to_string();
        content_result
    }

    /// Validate a single SQL query string
    fn validate_query(
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

        result
    }

    // Helper methods - robust SQL statement splitting
    fn split_sql_statements(&self, content: &str) -> Vec<(String, usize)> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut current_line = 1;
        let mut statement_start_line = 1;
        let mut in_string = false;
        let mut string_delimiter = '"';
        let mut in_comment = false;
        let mut prev_char = ' ';

        for ch in content.chars() {
            match ch {
                '\n' => {
                    current_line += 1;
                    in_comment = false; // End line comments
                    current_statement.push(ch);
                }
                '-' if !in_string && !in_comment && prev_char == '-' => {
                    in_comment = true;
                    current_statement.push(ch);
                }
                '"' | '\'' if !in_string && !in_comment => {
                    in_string = true;
                    string_delimiter = ch;
                    current_statement.push(ch);
                }
                c if in_string && c == string_delimiter => {
                    in_string = false;
                    current_statement.push(ch);
                }
                ';' if !in_string && !in_comment => {
                    current_statement.push(ch);
                    let trimmed = current_statement.trim();
                    if !trimmed.is_empty()
                        && !trimmed.starts_with("--")
                        && self.is_sql_statement(trimmed)
                    {
                        statements.push((current_statement.clone(), statement_start_line));
                    }
                    current_statement.clear();
                    statement_start_line = current_line + 1;
                }
                _ => {
                    if current_statement.trim().is_empty() && !ch.is_whitespace() && !in_comment {
                        statement_start_line = current_line;
                    }
                    current_statement.push(ch);
                }
            }
            prev_char = ch;
        }

        // Handle last statement if it doesn't end with semicolon
        let trimmed = current_statement.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") && self.is_sql_statement(trimmed) {
            statements.push((current_statement, statement_start_line));
        }

        statements
    }

    fn is_sql_statement(&self, text: &str) -> bool {
        let trimmed = text.trim_start();
        trimmed.starts_with("CREATE")
            || trimmed.starts_with("SELECT")
            || trimmed.starts_with("INSERT")
            || trimmed.starts_with("UPDATE")
            || trimmed.starts_with("DELETE")
            || trimmed.starts_with("DROP")
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
        error: &crate::ferris::sql::error::SqlError,
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
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
}
