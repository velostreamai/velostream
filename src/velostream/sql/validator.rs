//! SQL Validator Module
//!
//! Provides comprehensive SQL validation for Velostream applications
//! before deployment to StreamJobServer to prevent runtime failures.

use crate::velostream::sql::{
    ast::{Expr, SelectField, StreamingQuery, SubqueryType},
    config::with_clause_parser::WithClauseParser,
    execution::expression::is_aggregate_function,
    parser::{
        StreamingSqlParser, annotations::parse_metric_annotations, validator::AggregateValidator,
    },
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
    pub reference_output: Option<String>,
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
    errors: std::cell::RefCell<Vec<ValidationError>>,
    warnings: std::cell::RefCell<Vec<ValidationError>>,
    /// Base directory for resolving relative config_file paths (set from SQL file location)
    base_dir: Option<std::path::PathBuf>,
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
            errors: std::cell::RefCell::new(Vec::new()),
            warnings: std::cell::RefCell::new(Vec::new()),
            base_dir: None,
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
            errors: std::cell::RefCell::new(Vec::new()),
            warnings: std::cell::RefCell::new(Vec::new()),
            base_dir: None,
        }
    }

    /// Create a validator with a base directory for resolving relative config_file paths
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Self {
        let base = base_dir.as_ref();
        Self {
            parser: StreamingSqlParser::new(),
            analyzer: QueryAnalyzer::with_base_dir("sql-validator".to_string(), base),
            with_clause_parser: WithClauseParser::new(),
            strict_mode: false,
            check_performance: true,
            errors: std::cell::RefCell::new(Vec::new()),
            warnings: std::cell::RefCell::new(Vec::new()),
            base_dir: Some(base.to_path_buf()),
        }
    }

    /// Get the base directory for resolving relative paths
    pub fn base_dir(&self) -> Option<&Path> {
        self.base_dir.as_deref()
    }

    /// Validate a StreamingQuery AST node
    pub fn validate(&self, query: &StreamingQuery) {
        // Clear previous errors/warnings
        self.errors.borrow_mut().clear();
        self.warnings.borrow_mut().clear();

        match query {
            StreamingQuery::CreateTable {
                name,
                as_select,
                properties,
                emit_mode,
                ..
            } => {
                self.validate_create_table(name, as_select, properties, emit_mode);
            }
            StreamingQuery::CreateStream {
                name,
                as_select,
                properties,
                emit_mode,
                ..
            } => {
                self.validate_create_stream(name, as_select, properties, emit_mode);
            }
            _ => {
                // Other query types - basic validation
                self.validate_inner_select(query);
            }
        }
    }

    /// Get collected errors
    pub fn errors(&self) -> Vec<ValidationError> {
        self.errors.borrow().clone()
    }

    /// Get collected warnings
    pub fn warnings(&self) -> Vec<ValidationError> {
        self.warnings.borrow().clone()
    }

    /// Clear all errors and warnings
    fn clear_errors_warnings(&self) {
        self.errors.borrow_mut().clear();
        self.warnings.borrow_mut().clear();
    }

    /// Add an error
    fn add_error(&self, message: String) {
        self.errors.borrow_mut().push(ValidationError {
            message,
            line: None,
            column: None,
            severity: ErrorSeverity::Error,
        });
    }

    /// Add a warning
    fn add_warning(&self, message: String) {
        self.warnings.borrow_mut().push(ValidationError {
            message,
            line: None,
            column: None,
            severity: ErrorSeverity::Warning,
        });
    }

    /// Validate CREATE TABLE statement
    fn validate_create_table(
        &self,
        name: &str,
        as_select: &StreamingQuery,
        properties: &HashMap<String, String>,
        emit_mode: &Option<crate::velostream::sql::ast::EmitMode>,
    ) {
        use crate::velostream::sql::ast::EmitMode;

        // Validate table name
        self.validate_table_stream_name(name, "Table");

        // Validate properties
        self.validate_ctas_properties(properties);

        // Validate EMIT mode compatibility
        if let Some(EmitMode::Final) = emit_mode {
            // Check if inner SELECT has a window
            if !self.has_window_clause(as_select) {
                self.add_warning(
                    "EMIT FINAL without WINDOW clause — results will be emitted on source exhaustion. For unbounded sources (e.g., Kafka), this may never occur.".to_string()
                );
            }
        }

        // Validate inner SELECT for aggregations
        self.validate_aggregation_rules(as_select);

        // Check for memory warnings based on properties
        self.validate_memory_configuration(properties);

        // Recursively validate inner SELECT
        self.validate_inner_select(as_select);
    }

    /// Validate CREATE STREAM statement
    fn validate_create_stream(
        &self,
        name: &str,
        as_select: &StreamingQuery,
        properties: &HashMap<String, String>,
        emit_mode: &Option<crate::velostream::sql::ast::EmitMode>,
    ) {
        use crate::velostream::sql::ast::EmitMode;

        // Validate stream name
        self.validate_table_stream_name(name, "Stream");

        // Validate EMIT mode compatibility
        if let Some(EmitMode::Final) = emit_mode {
            if !self.has_window_clause(as_select) {
                self.add_warning(
                    "EMIT FINAL without WINDOW clause — results will be emitted on source exhaustion. For unbounded sources (e.g., Kafka), this may never occur.".to_string()
                );
            }
        }

        // Validate inner SELECT for aggregations
        self.validate_aggregation_rules(as_select);

        // Recursively validate inner SELECT
        self.validate_inner_select(as_select);
    }

    /// Validate CREATE TABLE with named sink configuration
    fn validate_create_table_with_properties(
        &self,
        name: &str,
        as_select: &StreamingQuery,
        properties: &crate::velostream::sql::ast::ConfigProperties,
        emit_mode: &Option<crate::velostream::sql::ast::EmitMode>,
    ) {
        use crate::velostream::sql::ast::EmitMode;

        // Validate table name
        self.validate_table_stream_name(name, "Table");

        // Validate inline properties
        self.validate_ctas_properties(&properties.inline_properties);

        // Validate EMIT mode
        if let Some(EmitMode::Final) = emit_mode {
            if !self.has_window_clause(as_select) {
                self.add_warning(
                    "EMIT FINAL without WINDOW clause — results will be emitted on source exhaustion. For unbounded sources (e.g., Kafka), this may never occur.".to_string()
                );
            }
        }

        // Validate aggregations
        self.validate_aggregation_rules(as_select);

        // Recursively validate inner SELECT
        self.validate_inner_select(as_select);
    }

    /// Validate CREATE STREAM with named sink configuration
    fn validate_create_stream_with_properties(
        &self,
        name: &str,
        as_select: &StreamingQuery,
        properties: &crate::velostream::sql::ast::ConfigProperties,
        emit_mode: &Option<crate::velostream::sql::ast::EmitMode>,
    ) {
        use crate::velostream::sql::ast::EmitMode;

        // Validate stream name
        self.validate_table_stream_name(name, "Stream");

        // Validate EMIT mode
        if let Some(EmitMode::Final) = emit_mode {
            if !self.has_window_clause(as_select) {
                self.add_warning(
                    "EMIT FINAL without WINDOW clause — results will be emitted on source exhaustion. For unbounded sources (e.g., Kafka), this may never occur.".to_string()
                );
            }
        }

        // Validate aggregations
        self.validate_aggregation_rules(as_select);

        // Recursively validate inner SELECT
        self.validate_inner_select(as_select);
    }

    /// Validate table or stream name
    fn validate_table_stream_name(&self, name: &str, entity_type: &str) {
        // Check for empty or whitespace-only names
        if name.trim().is_empty() {
            self.add_error(format!(
                "{} name cannot be empty or whitespace-only",
                entity_type
            ));
            return;
        }

        // Check for potentially invalid characters
        let has_special_chars =
            name.contains('-') || name.contains(' ') || name.contains('.') || name.contains('@');

        if has_special_chars {
            self.add_warning(format!(
                "{} name '{}' contains special characters that may cause issues",
                entity_type, name
            ));
        }

        // Check if name starts with a number
        if name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            self.add_warning(format!(
                "{} name '{}' starts with a number - this may cause parsing issues",
                entity_type, name
            ));
        }
    }

    /// Validate CTAS/CSAS properties
    fn validate_ctas_properties(&self, properties: &HashMap<String, String>) {
        // Validate table_model
        if let Some(table_model) = properties.get("table_model") {
            if table_model != "normal" && table_model != "compact" {
                self.add_error(format!(
                    "Invalid table_model '{}'. Must be 'normal' or 'compact'",
                    table_model
                ));
            }
        }

        // Validate retention format
        if let Some(retention) = properties.get("retention") {
            if !self.is_valid_retention_format(retention) {
                self.add_error(format!(
                    "Invalid retention format '{}'. Expected format: '7 days', '24 hours', etc.",
                    retention
                ));
            }
        }

        // Validate compression
        if let Some(compression) = properties.get("compression") {
            let valid_compressions = ["snappy", "gzip", "zstd", "lz4"];
            if !valid_compressions.contains(&compression.as_str()) {
                self.add_error(format!(
                    "Invalid compression '{}'. Must be one of: {}",
                    compression,
                    valid_compressions.join(", ")
                ));
            }
        }

        // Validate kafka.batch.size
        if let Some(batch_size) = properties.get("kafka.batch.size") {
            if batch_size.parse::<usize>().is_err() {
                self.add_error(format!(
                    "Invalid kafka.batch.size '{}'. Must be an integer",
                    batch_size
                ));
            }
        }
    }

    /// Check if retention format is valid
    fn is_valid_retention_format(&self, retention: &str) -> bool {
        // Simple validation: should contain a number followed by a time unit
        let parts: Vec<&str> = retention.split_whitespace().collect();
        if parts.len() != 2 {
            return false;
        }

        // Check if first part is a number
        if parts[0].parse::<u64>().is_err() {
            return false;
        }

        // Check if second part is a valid time unit
        let valid_units = [
            "day", "days", "hour", "hours", "minute", "minutes", "second", "seconds",
        ];
        valid_units.contains(&parts[1])
    }

    /// Validate memory configuration
    fn validate_memory_configuration(&self, properties: &HashMap<String, String>) {
        let retention = properties.get("retention");
        let table_model = properties.get("table_model");

        // Check for long retention without compact model
        if let Some(retention_str) = retention {
            if let Some(days) = self.parse_retention_days(retention_str) {
                if days > 30 && table_model.is_none_or(|m| m == "normal") {
                    self.add_warning(
                        format!(
                            "Long retention ({}) with 'normal' table model may cause high memory usage. Consider using 'table_model' = 'compact'",
                            retention_str
                        )
                    );
                }
            }
        }
    }

    /// Parse retention string to days
    fn parse_retention_days(&self, retention: &str) -> Option<u64> {
        let parts: Vec<&str> = retention.split_whitespace().collect();
        if parts.len() != 2 {
            return None;
        }

        let value = parts[0].parse::<u64>().ok()?;
        let unit = parts[1];

        match unit {
            "day" | "days" => Some(value),
            "hour" | "hours" => Some(value / 24),
            "minute" | "minutes" => Some(value / (24 * 60)),
            _ => None,
        }
    }

    /// Check if query has a window clause
    fn has_window_clause(&self, query: &StreamingQuery) -> bool {
        match query {
            StreamingQuery::Select { window, .. } => window.is_some(),
            _ => false,
        }
    }

    /// Validate aggregation rules
    fn validate_aggregation_rules(&self, query: &StreamingQuery) {
        use crate::velostream::sql::ast::SelectField;

        match query {
            StreamingQuery::Select {
                fields, group_by, ..
            } => {
                // Check if any field uses aggregation functions
                let has_aggregation = fields.iter().any(|field| {
                    if let SelectField::Expression { expr, .. } = field {
                        self.is_aggregation_expr(expr)
                    } else {
                        false
                    }
                });

                // If aggregation is used without GROUP BY, warn
                if has_aggregation && group_by.is_none() {
                    self.add_warning(
                        "Aggregation functions used without GROUP BY clause - this creates a global aggregation".to_string()
                    );
                }
            }
            _ => {}
        }
    }

    /// Check if expression contains aggregation functions.
    ///
    /// Uses the centralized function catalog instead of a hardcoded list.
    fn is_aggregation_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => is_aggregate_function(name),
            Expr::BinaryOp { left, right, .. } => {
                self.is_aggregation_expr(left) || self.is_aggregation_expr(right)
            }
            _ => false,
        }
    }

    /// Validate inner SELECT query
    fn validate_inner_select(&self, query: &StreamingQuery) {
        // This would recursively validate the inner SELECT
        // For now, we just check basic structure
        // More complex validation can be added later
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
            reference_output: Some(self.generate_system_columns_reference()),
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
        // Config file paths are resolved relative to the current working directory (CWD)
        // This allows SQL files in subdirectories to reference configs relative to the project root
        // Example: SQL in `sql/app.sql` can reference `configs/source.yaml` relative to CWD
        //
        // If you need SQL-relative path resolution, use SqlValidator::with_base_dir()
        // to explicitly set the base directory for config file resolution.
        let effective_validator = self;

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
            reference_output: Some(effective_validator.generate_system_columns_reference()),
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
        result.application_name = effective_validator.extract_application_name(&content);

        // Validate @metric annotations (FR-073)
        effective_validator.validate_metric_annotations(&content, &mut result);

        // Split into individual queries with line tracking
        let queries = effective_validator.split_sql_statements(&content);
        result.total_queries = queries.len();

        for (i, (query, start_line)) in queries.iter().enumerate() {
            let query_result = effective_validator.validate_query(query, i, *start_line, &content);
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

        // Validate aggregate function usage semantics
        // This catches queries like "SELECT id, COUNT(*) FROM orders" without GROUP BY
        if result.is_valid {
            match AggregateValidator::validate(&parsed_query) {
                Ok(_) => {
                    // Query passed aggregate validation
                }
                Err(e) => {
                    // Add semantic validation error
                    result.parsing_errors.push(ValidationError {
                        message: format!("Semantic validation: {}", e),
                        line: Some(start_line),
                        column: None,
                        severity: ErrorSeverity::Error,
                    });
                    result.is_valid = false;
                }
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
        let mut pending_annotations = String::new();
        let mut query_start_line = 0;
        let mut current_line = 0;
        let mut in_query = false;

        for line in content.lines() {
            current_line += 1;
            let trimmed = line.trim();

            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }

            // Collect annotation comments (-- @...) to preserve them with the query
            if trimmed.starts_with("--") {
                // Check if this is an annotation line (starts with -- @)
                let after_dashes = trimmed[2..].trim_start();
                if after_dashes.starts_with("@") {
                    // Preserve annotation comments
                    pending_annotations.push_str(line);
                    pending_annotations.push('\n');
                }
                // Skip all comment lines from the main query text
                // but annotations are preserved in pending_annotations
                continue;
            }

            // Mark the start of a new query
            if !in_query {
                query_start_line = current_line;
                in_query = true;
                // Prepend any pending annotation comments
                if !pending_annotations.is_empty() {
                    current_query.push_str(&pending_annotations);
                    pending_annotations.clear();
                }
            }

            current_query.push_str(line);
            current_query.push('\n');

            // Check for query termination - only on semicolon
            if trimmed.ends_with(";") {
                if !current_query.trim().is_empty() {
                    queries.push((current_query.trim().to_string(), query_start_line - 1));
                }
                current_query.clear();
                pending_annotations.clear();
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

    /// Check if bootstrap.servers exists in any of the standard Kafka config locations
    fn has_bootstrap_servers(&self, properties: &HashMap<String, String>) -> bool {
        properties.contains_key("bootstrap.servers")
            || properties.contains_key("datasource.consumer_config.bootstrap.servers")
            || properties.contains_key("datasource.config.bootstrap.servers")
            || properties.contains_key("datasink.producer_config.bootstrap.servers")
            || properties.contains_key("datasink.config.bootstrap.servers")
    }

    /// Check if topic exists in any of the standard Kafka config locations
    fn has_topic(&self, properties: &HashMap<String, String>) -> bool {
        properties.contains_key("topic")
            || properties.contains_key("topic.name")
            || properties.contains_key("datasource.config.topic")
            || properties.contains_key("datasink.config.topic")
            || properties.contains_key("datasource.topic.name")
            || properties.contains_key("datasink.topic.name")
    }

    /// Check if path exists in any of the standard file config locations
    fn has_file_path(&self, properties: &HashMap<String, String>) -> bool {
        properties.contains_key("path")
            || properties.contains_key("data_source.path")
            || properties.contains_key("datasource.path")
            || properties.contains_key("datasource.config.path")
            || properties.contains_key("file.path")
    }

    /// Check if format exists in any of the standard file config locations
    fn has_file_format(&self, properties: &HashMap<String, String>) -> bool {
        properties.contains_key("format")
            || properties.contains_key("data_source.format")
            || properties.contains_key("datasource.format")
            || properties.contains_key("datasource.config.format")
            || properties.contains_key("file.format")
    }

    fn validate_kafka_source_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let mut missing_keys = Vec::new();

        // Check for bootstrap.servers in multiple possible locations
        if !self.has_bootstrap_servers(properties) {
            missing_keys.push("bootstrap.servers".to_string());
        }

        // Check for topic in multiple possible locations
        if !self.has_topic(properties) {
            missing_keys.push("topic".to_string());
        }

        if !missing_keys.is_empty() {
            result.missing_source_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: vec!["bootstrap.servers".to_string(), "topic".to_string()],
                missing_keys,
                has_batch_config: properties.contains_key("batch.size")
                    || properties.contains_key("max.poll.records")
                    || properties.contains_key("datasource.consumer_config.max.poll.records"),
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
        let mut missing_keys = Vec::new();

        // Check for bootstrap.servers in multiple possible locations
        if !self.has_bootstrap_servers(properties) {
            missing_keys.push("bootstrap.servers".to_string());
        }

        // Check for topic in multiple possible locations
        if !self.has_topic(properties) {
            missing_keys.push("topic".to_string());
        }

        if !missing_keys.is_empty() {
            result.missing_sink_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: vec!["bootstrap.servers".to_string(), "topic".to_string()],
                missing_keys,
                has_batch_config: properties.contains_key("batch.size")
                    || properties.contains_key("linger.ms")
                    || properties.contains_key("datasink.producer_config.batch.size"),
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
        let mut missing_keys = Vec::new();

        // Check for path in multiple possible locations (flat or nested under data_source)
        if !self.has_file_path(properties) {
            missing_keys.push("path".to_string());
        }

        // Check for format in multiple possible locations
        if !self.has_file_format(properties) {
            missing_keys.push("format".to_string());
        }

        if !missing_keys.is_empty() {
            result.missing_source_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: vec!["path".to_string(), "format".to_string()],
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
        let mut missing_keys = Vec::new();

        // Check for path in multiple possible locations (flat or nested under data_source)
        if !self.has_file_path(properties) {
            missing_keys.push("path".to_string());
        }

        // Check for format in multiple possible locations
        if !self.has_file_format(properties) {
            missing_keys.push("format".to_string());
        }

        if !missing_keys.is_empty() {
            result.missing_sink_configs.push(ConfigurationIssue {
                name: name.to_string(),
                required_keys: vec!["path".to_string(), "format".to_string()],
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

    /// Validate @metric annotations in SQL comments (FR-073)
    fn validate_metric_annotations(&self, content: &str, result: &mut ApplicationValidationResult) {
        // Extract all comment lines
        let comments: Vec<String> = content
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.starts_with("--") {
                    // Remove the leading "--" and trim
                    Some(trimmed[2..].trim().to_string())
                } else {
                    None
                }
            })
            .collect();

        // Parse metric annotations
        match parse_metric_annotations(&comments) {
            Ok(annotations) => {
                if !annotations.is_empty() {
                    result.recommendations.push(format!(
                        "✓ Found {} valid @metric annotation(s) (FR-073 SQL-Native Observability)",
                        annotations.len()
                    ));
                }
            }
            Err(e) => {
                result
                    .global_errors
                    .push(format!("❌ @metric annotation error: {}", e));
                result.is_valid = false;
            }
        }
    }

    /// Generate system columns reference output for CLI
    fn generate_system_columns_reference(&self) -> String {
        let mut output = String::new();

        output.push_str(
            "\n═══════════════════════════════════════════════════════════════════════════════\n",
        );
        output.push_str("📚 VELOSTREAM SYSTEM COLUMNS & HEADER FUNCTIONS REFERENCE\n");
        output.push_str(
            "═══════════════════════════════════════════════════════════════════════════════\n\n",
        );

        // System Columns Section
        output.push_str("🔍 AVAILABLE SYSTEM COLUMNS\n");
        output.push_str("───────────────────────────\n\n");

        output.push_str(
            "| Column Name      | Type    | Description                              |\n",
        );
        output.push_str(
            "|------------------|---------|------------------------------------------|\n",
        );
        output.push_str(
            "| _timestamp       | INT64   | Message processing time (ms since epoch) |\n",
        );
        output.push_str(
            "| _offset          | INT64   | Kafka partition offset (message number)  |\n",
        );
        output.push_str(
            "| _partition       | INT32   | Kafka partition number                   |\n",
        );
        output.push_str(
            "| _event_time      | TIMESTAMP| Event-time watermark                     |\n",
        );
        output.push_str(
            "| _window_start    | INT64   | Tumbling window start time (ms)          |\n",
        );
        output.push_str(
            "| _window_end      | INT64   | Tumbling window end time (ms)            |\n\n",
        );

        // Header Functions Section
        output.push_str("🏷️  KAFKA HEADER FUNCTIONS\n");
        output.push_str("──────────────────────────\n\n");

        output.push_str("| Function              | Purpose                                  |\n");
        output.push_str("|----------------------|------------------------------------------|\n");
        output.push_str("| HEADER(key)          | Get header value (returns STRING or NULL)|\n");
        output.push_str("| HAS_HEADER(key)      | Check if header exists (returns BOOLEAN) |\n");
        output.push_str("| HEADER_KEYS()        | List all headers (comma-separated)       |\n");
        output.push_str("| SET_HEADER(key,val)  | Add/update header (with dynamic values)  |\n");
        output.push_str("| REMOVE_HEADER(key)   | Delete header from message               |\n\n");

        // Quick Examples
        output.push_str("⚡ QUICK EXAMPLES\n");
        output.push_str("────────────────\n\n");

        output.push_str("Access system metadata:\n");
        output.push_str("  SELECT _partition, _offset, _timestamp, customer_id FROM orders;\n\n");

        output.push_str("Distributed tracing with headers:\n");
        output.push_str("  SELECT\n");
        output.push_str("      SET_HEADER('trace-id', HEADER('trace-id')) as trace,\n");
        output.push_str(
            "      SET_HEADER('span-id', 'span-' || _partition || '-' || _offset) as span,\n",
        );
        output.push_str("      customer_id, amount\n");
        output.push_str("  FROM orders\n");
        output.push_str("  WHERE HAS_HEADER('trace-id')\n");
        output.push_str("  EMIT CHANGES;\n\n");

        output.push_str("Message lineage tracking:\n");
        output.push_str("  SELECT\n");
        output.push_str("      HEADER('trace-id') as trace_id,\n");
        output.push_str("      _partition as partition,\n");
        output.push_str("      _offset as message_offset,\n");
        output.push_str("      FROM_UNIXTIME(_timestamp / 1000) as processed_at\n");
        output.push_str("  FROM orders;\n\n");

        // Competitive Advantage
        output.push_str("🏆 VELOSTREAM'S UNIQUE ADVANTAGES\n");
        output.push_str("──────────────────────────────────\n\n");

        output.push_str("| Feature                    | Velostream | Apache Flink | ksqlDB |\n");
        output.push_str("|----------------------------|------------|--------------|--------|\n");
        output.push_str(
            "| System columns in SQL      | ✅ Yes     | ❌ DataStream API only | ❌ No  |\n",
        );
        output.push_str("| Header access in SQL       | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Dynamic header mutations   | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Partition metadata access  | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Pure SQL distributed tracing| ✅ Yes     | ❌ Impossible| ❌ No  |\n");
        output.push_str("| Offset-based processing    | ✅ Yes     | ❌ No        | ❌ No  |\n\n");

        // Learn More
        output.push_str("📖 FOR MORE INFORMATION\n");
        output.push_str("───────────────────────\n\n");
        output.push_str("• System Columns Guide: docs/sql/system-columns.md\n");
        output.push_str("• Header Access Guide: docs/sql/header-access.md\n");
        output.push_str(
            "• Distributed Tracing Example: docs/sql/examples/distributed-tracing.md\n\n",
        );

        output.push_str(
            "═══════════════════════════════════════════════════════════════════════════════\n\n",
        );

        output
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
                // Analyze WHERE clause for subqueries and performance patterns
                if let Some(where_expr) = where_clause {
                    self.analyze_expression(where_expr);
                    self.analyze_performance_patterns(where_expr);
                }

                // Analyze SELECT fields for subqueries and performance patterns
                for field in fields {
                    if let SelectField::Expression { expr, .. } = field {
                        self.analyze_expression(expr);
                        self.analyze_performance_patterns(expr);
                    }
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                self.analyze_query(as_select);
            }
            StreamingQuery::CreateTable { as_select, .. } => {
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

    /// Analyze expressions for performance anti-patterns
    fn analyze_performance_patterns(&mut self, expr: &Expr) {
        match expr {
            Expr::Function { name, args } => {
                // Detect performance-problematic patterns
                if name.to_lowercase() == "like" && args.len() == 2 {
                    // Check for leading wildcard pattern: LIKE '%...'
                    if let Expr::Literal(crate::velostream::sql::ast::LiteralValue::String(
                        pattern,
                    )) = &args[1]
                    {
                        if pattern.starts_with('%') {
                            self.findings.push(SubqueryFinding {
                                message: "LIKE patterns starting with % prevent index usage and may cause performance issues".to_string(),
                                subquery_type: None,
                            });
                        }
                    }
                } else if name.to_lowercase() == "regexp" {
                    self.findings.push(SubqueryFinding {
                        message:
                            "Regular expressions can be expensive operations in streaming contexts"
                                .to_string(),
                        subquery_type: None,
                    });
                } else if name.to_lowercase() == "substring" {
                    self.findings.push(SubqueryFinding {
                        message: "String functions in WHERE clauses may prevent index usage"
                            .to_string(),
                        subquery_type: None,
                    });
                }

                // Recursively analyze function arguments
                for arg in args {
                    self.analyze_performance_patterns(arg);
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                self.findings.push(SubqueryFinding {
                    message: "Complex CASE expressions in subqueries may impact performance"
                        .to_string(),
                    subquery_type: None,
                });

                // Analyze all clauses
                for (condition, result) in when_clauses {
                    self.analyze_performance_patterns(condition);
                    self.analyze_performance_patterns(result);
                }
                if let Some(else_expr) = else_clause {
                    self.analyze_performance_patterns(else_expr);
                }
            }
            Expr::BinaryOp { left, right, op } => {
                // Check for LIKE patterns
                if matches!(
                    op,
                    crate::velostream::sql::ast::BinaryOperator::Like
                        | crate::velostream::sql::ast::BinaryOperator::NotLike
                ) {
                    // Check for leading wildcard pattern: LIKE '%...'
                    if let Expr::Literal(crate::velostream::sql::ast::LiteralValue::String(
                        pattern,
                    )) = &**right
                    {
                        if pattern.starts_with('%') {
                            self.findings.push(SubqueryFinding {
                                message: "LIKE patterns starting with % prevent index usage and may cause performance issues".to_string(),
                                subquery_type: None,
                            });
                        }
                    }
                }
                self.analyze_performance_patterns(left);
                self.analyze_performance_patterns(right);
            }
            Expr::UnaryOp { expr, .. } => {
                self.analyze_performance_patterns(expr);
            }
            Expr::Subquery { query, .. } => {
                // Recursively analyze nested subqueries
                let mut nested_analyzer = SubqueryAnalyzer::new();
                nested_analyzer.analyze_query(query);
                self.findings.extend(nested_analyzer.findings);
            }
            _ => {}
        }
    }

    /// Heuristic to detect correlation patterns in subqueries
    fn has_correlation_patterns(&self, query: &StreamingQuery) -> bool {
        // Analyze if the subquery references columns that could be from outer query
        // Get the subquery's own table aliases to distinguish from outer references
        match query {
            StreamingQuery::Select {
                where_clause,
                from_alias,
                from,
                ..
            } => {
                if let Some(expr) = where_clause {
                    // Collect this subquery's table identifiers
                    let mut local_tables = Vec::new();
                    if let Some(alias) = from_alias {
                        local_tables.push(alias.as_str());
                    }
                    // Add the actual table name as fallback
                    if let crate::velostream::sql::ast::StreamSource::Stream(table_name) = from {
                        local_tables.push(table_name.as_str());
                    }
                    if let crate::velostream::sql::ast::StreamSource::Table(table_name) = from {
                        local_tables.push(table_name.as_str());
                    }

                    self.has_correlation_in_expr(expr, &local_tables)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Check if expression contains correlation patterns (outer table references)
    fn has_correlation_in_expr(&self, expr: &Expr, local_tables: &[&str]) -> bool {
        match expr {
            Expr::Column(col_name) => {
                // Check if this column reference uses a table alias not from this subquery
                if col_name.contains('.') {
                    let table_part = col_name.split('.').next().unwrap();
                    // If table part is NOT in local_tables, it's likely from outer query
                    !local_tables.contains(&table_part)
                } else {
                    false
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.has_correlation_in_expr(left, local_tables)
                    || self.has_correlation_in_expr(right, local_tables)
            }
            Expr::UnaryOp { expr, .. } => self.has_correlation_in_expr(expr, local_tables),
            Expr::Function { args, .. } => args
                .iter()
                .any(|arg| self.has_correlation_in_expr(arg, local_tables)),
            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                when_clauses.iter().any(|(condition, result)| {
                    self.has_correlation_in_expr(condition, local_tables)
                        || self.has_correlation_in_expr(result, local_tables)
                }) || else_clause
                    .as_ref()
                    .is_some_and(|expr| self.has_correlation_in_expr(expr, local_tables))
            }
            Expr::Subquery { query, .. } => self.has_correlation_patterns(query),
            _ => false,
        }
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

            // Check for query termination - only on semicolon
            if trimmed.ends_with(";") {
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

    /// Generate system columns reference output for CLI
    fn generate_system_columns_reference(&self) -> String {
        let mut output = String::new();

        output.push_str(
            "\n═══════════════════════════════════════════════════════════════════════════════\n",
        );
        output.push_str("📚 VELOSTREAM SYSTEM COLUMNS & HEADER FUNCTIONS REFERENCE\n");
        output.push_str(
            "═══════════════════════════════════════════════════════════════════════════════\n\n",
        );

        // System Columns Section
        output.push_str("🔍 AVAILABLE SYSTEM COLUMNS\n");
        output.push_str("───────────────────────────\n\n");

        output.push_str(
            "| Column Name      | Type    | Description                              |\n",
        );
        output.push_str(
            "|------------------|---------|------------------------------------------|\n",
        );
        output.push_str(
            "| _timestamp       | INT64   | Message processing time (ms since epoch) |\n",
        );
        output.push_str(
            "| _offset          | INT64   | Kafka partition offset (message number)  |\n",
        );
        output.push_str(
            "| _partition       | INT32   | Kafka partition number                   |\n",
        );
        output.push_str(
            "| _event_time      | TIMESTAMP| Event-time watermark                     |\n",
        );
        output.push_str(
            "| _window_start    | INT64   | Tumbling window start time (ms)          |\n",
        );
        output.push_str(
            "| _window_end      | INT64   | Tumbling window end time (ms)            |\n\n",
        );

        // Header Functions Section
        output.push_str("🏷️  KAFKA HEADER FUNCTIONS\n");
        output.push_str("──────────────────────────\n\n");

        output.push_str("| Function              | Purpose                                  |\n");
        output.push_str("|----------------------|------------------------------------------|\n");
        output.push_str("| HEADER(key)          | Get header value (returns STRING or NULL)|\n");
        output.push_str("| HAS_HEADER(key)      | Check if header exists (returns BOOLEAN) |\n");
        output.push_str("| HEADER_KEYS()        | List all headers (comma-separated)       |\n");
        output.push_str("| SET_HEADER(key,val)  | Add/update header (with dynamic values)  |\n");
        output.push_str("| REMOVE_HEADER(key)   | Delete header from message               |\n\n");

        // Quick Examples
        output.push_str("⚡ QUICK EXAMPLES\n");
        output.push_str("────────────────\n\n");

        output.push_str("Access system metadata:\n");
        output.push_str("  SELECT _partition, _offset, _timestamp, customer_id FROM orders;\n\n");

        output.push_str("Distributed tracing with headers:\n");
        output.push_str("  SELECT\n");
        output.push_str("      SET_HEADER('trace-id', HEADER('trace-id')) as trace,\n");
        output.push_str(
            "      SET_HEADER('span-id', 'span-' || _partition || '-' || _offset) as span,\n",
        );
        output.push_str("      customer_id, amount\n");
        output.push_str("  FROM orders\n");
        output.push_str("  WHERE HAS_HEADER('trace-id')\n");
        output.push_str("  EMIT CHANGES;\n\n");

        output.push_str("Message lineage tracking:\n");
        output.push_str("  SELECT\n");
        output.push_str("      HEADER('trace-id') as trace_id,\n");
        output.push_str("      _partition as partition,\n");
        output.push_str("      _offset as message_offset,\n");
        output.push_str("      FROM_UNIXTIME(_timestamp / 1000) as processed_at\n");
        output.push_str("  FROM orders;\n\n");

        // Competitive Advantage
        output.push_str("🏆 VELOSTREAM'S UNIQUE ADVANTAGES\n");
        output.push_str("──────────────────────────────────\n\n");

        output.push_str("| Feature                    | Velostream | Apache Flink | ksqlDB |\n");
        output.push_str("|----------------------------|------------|--------------|--------|\n");
        output.push_str(
            "| System columns in SQL      | ✅ Yes     | ❌ DataStream API only | ❌ No  |\n",
        );
        output.push_str("| Header access in SQL       | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Dynamic header mutations   | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Partition metadata access  | ✅ Yes     | ❌ No        | ❌ No  |\n");
        output.push_str("| Pure SQL distributed tracing| ✅ Yes     | ❌ Impossible| ❌ No  |\n");
        output.push_str("| Offset-based processing    | ✅ Yes     | ❌ No        | ❌ No  |\n\n");

        // Learn More
        output.push_str("📖 FOR MORE INFORMATION\n");
        output.push_str("───────────────────────\n\n");
        output.push_str("• System Columns Guide: docs/sql/system-columns.md\n");
        output.push_str("• Header Access Guide: docs/sql/header-access.md\n");
        output.push_str(
            "• Distributed Tracing Example: docs/sql/examples/distributed-tracing.md\n\n",
        );

        output.push_str(
            "═══════════════════════════════════════════════════════════════════════════════\n\n",
        );

        output
    }
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
}
