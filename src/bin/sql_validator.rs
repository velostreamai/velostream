//! SQL Query and Application Validator
//!
//! Comprehensive validation tool for FerrisStreams SQL queries and applications.
//! Checks for:
//! - SQL parsing correctness
//! - Missing source/sink configurations  
//! - Configuration completeness
//! - Syntax compatibility
//! - Performance warnings

use ferrisstreams::ferris::sql::{
    ast::StreamingQuery,
    config::with_clause_parser::WithClauseParser,
    query_analyzer::{
        DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType, QueryAnalyzer,
    },
    StreamingSqlParser,
};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Detailed parsing error with location information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsingError {
    pub message: String,
    pub line: usize,
    pub column: usize,
    pub position: usize,
    pub context_lines: Vec<String>,
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
    pub warnings: Vec<String>,
    pub sources_found: Vec<String>,
    pub sinks_found: Vec<String>,
    pub missing_source_configs: Vec<String>,
    pub missing_sink_configs: Vec<String>,
    pub syntax_issues: Vec<String>,
    pub performance_warnings: Vec<String>,
}

/// Validation result for an entire SQL application
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Summary of all configurations found in the application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationSummary {
    pub sources: HashMap<String, SourceConfigSummary>,
    pub sinks: HashMap<String, SinkConfigSummary>,
    pub missing_configurations: Vec<String>,
    pub duplicate_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfigSummary {
    pub source_type: String,
    pub config_keys: Vec<String>,
    pub required_keys: Vec<String>,
    pub missing_keys: Vec<String>,
    pub has_batch_config: bool,
    pub has_failure_strategy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfigSummary {
    pub sink_type: String,
    pub config_keys: Vec<String>,
    pub required_keys: Vec<String>,
    pub missing_keys: Vec<String>,
    pub has_batch_config: bool,
    pub has_failure_strategy: bool,
}

/// Main SQL Validator
pub struct SqlValidator {
    parser: StreamingSqlParser,
    analyzer: QueryAnalyzer,
    with_clause_parser: WithClauseParser,
    strict_mode: bool,
    check_performance: bool,
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
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

    /// Validate a single SQL query string
    pub fn validate_query(
        &self,
        query: &str,
        query_index: usize,
        start_line: usize,
        full_content: &str,
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
            syntax_issues: Vec::new(),
            performance_warnings: Vec::new(),
        };

        // Step 1: Parse the SQL query
        let parsed_query = match self.parser.parse(query) {
            Ok(q) => q,
            Err(e) => {
                let parsing_error =
                    self.create_parsing_error(&e.to_string(), query, start_line, full_content);
                result.parsing_errors.push(parsing_error);
                result.is_valid = false;
                return result;
            }
        };

        // Step 2: Analyze query structure
        match self.analyzer.analyze(&parsed_query) {
            Ok(analysis) => {
                // Extract sources and sinks
                for source in &analysis.required_sources {
                    result.sources_found.push(source.name.clone());
                }
                for sink in &analysis.required_sinks {
                    result.sinks_found.push(sink.name.clone());
                }

                // Validate configurations
                self.validate_source_configurations(&analysis.required_sources, &mut result);
                self.validate_sink_configurations(&analysis.required_sinks, &mut result);
            }
            Err(e) => {
                result
                    .configuration_errors
                    .push(format!("Query analysis failed: {}", e));
                result.is_valid = false;
            }
        }

        // Step 3: Check for syntax compatibility issues
        self.check_syntax_compatibility(&parsed_query, &mut result);

        // Step 4: Performance analysis
        if self.check_performance {
            self.analyze_performance_implications(&parsed_query, &mut result);
        }

        // Step 5: Final validation
        if !result.parsing_errors.is_empty() || !result.configuration_errors.is_empty() {
            result.is_valid = false;
        }

        if self.strict_mode
            && (!result.warnings.is_empty() || !result.performance_warnings.is_empty())
        {
            result.is_valid = false;
        }

        result
    }

    /// Create a detailed parsing error with location information
    fn create_parsing_error(
        &self,
        error_msg: &str,
        query: &str,
        query_start_line: usize,
        full_content: &str,
    ) -> ParsingError {
        let position = self.extract_position(error_msg).unwrap_or(0);
        let (line, column) = self.position_to_line_column(query, position);
        let absolute_line = query_start_line + line;

        let context_lines =
            self.get_error_context(full_content, absolute_line, column, position, query);
        let error_indicator = self.create_error_indicator(query, position, column);

        ParsingError {
            message: error_msg.to_string(),
            line: absolute_line,
            column,
            position,
            context_lines,
            error_indicator,
        }
    }

    /// Extract position from error message
    fn extract_position(&self, error_msg: &str) -> Option<usize> {
        if let Some(pos_start) = error_msg.find("position ") {
            let pos_str = &error_msg[pos_start + 9..];
            if let Some(pos_end) = pos_str.find(":") {
                pos_str[..pos_end].parse().ok()
            } else {
                pos_str.split_whitespace().next()?.parse().ok()
            }
        } else {
            None
        }
    }

    /// Convert character position to line and column
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

    /// Get context lines around the error
    fn get_error_context(
        &self,
        content: &str,
        error_line: usize,
        error_column: usize,
        _position: usize,
        _query: &str,
    ) -> Vec<String> {
        let lines: Vec<&str> = content.lines().collect();
        let start_line = error_line.saturating_sub(2);
        let end_line = std::cmp::min(error_line + 3, lines.len());

        let mut context = Vec::new();

        for (i, line_content) in lines[start_line..end_line].iter().enumerate() {
            let line_number = start_line + i + 1;
            let prefix = if line_number == error_line + 1 {
                format!("→{:4} ", line_number)
            } else {
                format!(" {:4} ", line_number)
            };

            context.push(format!("{}{}", prefix, line_content));
        }

        // Add error indicator line
        if error_line < lines.len() {
            let indicator_spaces = " ".repeat(6 + error_column);
            let indicator = format!("{}^ ERROR HERE", indicator_spaces);
            context.push(indicator);
        }

        context
    }

    /// Create visual error indicator
    fn create_error_indicator(&self, query: &str, position: usize, _column: usize) -> String {
        let chars: Vec<char> = query.chars().collect();
        if position >= chars.len() {
            return format!("Error at end of query (position {})", position);
        }

        let error_char = chars.get(position).unwrap_or(&' ');
        let start = position.saturating_sub(20);
        let end = if position + 20 < chars.len() {
            position + 20
        } else {
            chars.len()
        };

        let before: String = chars[start..position].iter().collect();
        let after: String = chars[position + 1..end].iter().collect();

        format!("{}⟨{}⟩{}", before, error_char, after)
    }

    /// Validate source configurations
    fn validate_source_configurations(
        &self,
        sources: &[DataSourceRequirement],
        result: &mut QueryValidationResult,
    ) {
        for source in sources {
            match source.source_type {
                DataSourceType::Kafka => {
                    self.validate_kafka_source_config(&source.properties, &source.name, result);
                }
                DataSourceType::File => {
                    self.validate_file_source_config(&source.properties, &source.name, result);
                }
                DataSourceType::S3 => {
                    result.warnings.push(format!(
                        "S3 source '{}' is not fully supported yet",
                        source.name
                    ));
                }
                _ => {
                    result
                        .configuration_errors
                        .push(format!("Unknown source type for '{}'", source.name));
                }
            }
        }
    }

    /// Validate sink configurations
    fn validate_sink_configurations(
        &self,
        sinks: &[DataSinkRequirement],
        result: &mut QueryValidationResult,
    ) {
        for sink in sinks {
            match sink.sink_type {
                DataSinkType::Kafka => {
                    self.validate_kafka_sink_config(&sink.properties, &sink.name, result);
                }
                DataSinkType::File => {
                    self.validate_file_sink_config(&sink.properties, &sink.name, result);
                }
                DataSinkType::Generic(ref sink_type) if sink_type == "stdout" => {
                    // Stdout doesn't require configuration
                }
                _ => {
                    result
                        .configuration_errors
                        .push(format!("Unknown sink type for '{}'", sink.name));
                }
            }
        }
    }

    /// Validate Kafka source configuration
    fn validate_kafka_source_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["bootstrap.servers", "topic"];
        let recommended_keys = vec!["value.format", "group.id", "failure_strategy"];

        for key in &required_keys {
            if !properties.contains_key(*key) {
                result.missing_source_configs.push(format!(
                    "Kafka source '{}' missing required config: {}",
                    name, key
                ));
                result.is_valid = false;
            }
        }

        for key in &recommended_keys {
            if !properties.contains_key(*key) {
                result.warnings.push(format!(
                    "Kafka source '{}' missing recommended config: {}",
                    name, key
                ));
            }
        }

        // Check for batch configuration
        if self.has_batch_config(properties) {
            result.warnings.push(format!(
                "Kafka source '{}' has batch configuration - ensure this is intended",
                name
            ));
        }
    }

    /// Validate Kafka sink configuration
    fn validate_kafka_sink_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["bootstrap.servers", "topic"];
        let recommended_keys = vec!["value.format", "failure_strategy"];

        for key in &required_keys {
            if !properties.contains_key(*key) {
                result.missing_sink_configs.push(format!(
                    "Kafka sink '{}' missing required config: {}",
                    name, key
                ));
                result.is_valid = false;
            }
        }

        for key in &recommended_keys {
            if !properties.contains_key(*key) {
                result.warnings.push(format!(
                    "Kafka sink '{}' missing recommended config: {}",
                    name, key
                ));
            }
        }
    }

    /// Validate File source configuration
    fn validate_file_source_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["path", "format"];
        let _optional_keys = ["has_headers", "watching", "failure_strategy"];

        for key in &required_keys {
            if !properties.contains_key(*key) {
                result.missing_source_configs.push(format!(
                    "File source '{}' missing required config: {}",
                    name, key
                ));
                result.is_valid = false;
            }
        }

        // Check if file exists (if path is provided)
        if let Some(path) = properties.get("path") {
            if !Path::new(path).exists() {
                result.warnings.push(format!(
                    "File source '{}' path does not exist: {}",
                    name, path
                ));
            }
        }
    }

    /// Validate File sink configuration  
    fn validate_file_sink_config(
        &self,
        properties: &HashMap<String, String>,
        name: &str,
        result: &mut QueryValidationResult,
    ) {
        let required_keys = vec!["path", "format"];

        for key in &required_keys {
            if !properties.contains_key(*key) {
                result.missing_sink_configs.push(format!(
                    "File sink '{}' missing required config: {}",
                    name, key
                ));
                result.is_valid = false;
            }
        }

        // Check if output directory exists
        if let Some(path) = properties.get("path") {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.exists() {
                    result.warnings.push(format!(
                        "File sink '{}' output directory does not exist: {}",
                        name,
                        parent.display()
                    ));
                }
            }
        }
    }

    /// Check if configuration has batch settings
    fn has_batch_config(&self, properties: &HashMap<String, String>) -> bool {
        properties.keys().any(|k| {
            k.contains("batch.strategy") || k.contains("batch.size") || k.contains("batch.timeout")
        })
    }

    /// Check for syntax compatibility issues
    fn check_syntax_compatibility(
        &self,
        _query: &StreamingQuery,
        result: &mut QueryValidationResult,
    ) {
        // Check for unsupported window syntax
        let query_text = &result.query_text;
        if query_text.contains("WINDOW TUMBLING") || query_text.contains("WINDOW SLIDING") {
            result.syntax_issues.push("WINDOW clauses in GROUP BY are not fully supported - use simplified window functions".to_string());
        }

        if query_text.contains("SESSION(") {
            result
                .syntax_issues
                .push("SESSION windows may have limited support - test thoroughly".to_string());
        }

        // Check for potentially problematic subqueries
        if query_text.contains("SELECT") && query_text.matches("SELECT").count() > 1 {
            result.warnings.push(
                "Complex subqueries detected - ensure they are supported in streaming context"
                    .to_string(),
            );
        }

        // Check for ORDER BY without LIMIT
        if query_text.contains("ORDER BY") && !query_text.contains("LIMIT") {
            result.performance_warnings.push(
                "ORDER BY without LIMIT can cause memory issues in streaming queries".to_string(),
            );
        }
    }

    /// Analyze performance implications
    fn analyze_performance_implications(
        &self,
        _query: &StreamingQuery,
        result: &mut QueryValidationResult,
    ) {
        let query_text = &result.query_text;

        // Check for expensive operations
        if query_text.contains("JOIN") && !query_text.contains("WINDOW") {
            result
                .performance_warnings
                .push("Stream-to-stream JOINs without time windows can be expensive".to_string());
        }

        if query_text.contains("GROUP BY") && query_text.contains("DISTINCT") {
            result
                .performance_warnings
                .push("GROUP BY with DISTINCT operations can be memory-intensive".to_string());
        }

        if query_text.contains("LAG(") || query_text.contains("LEAD(") {
            result.performance_warnings.push(
                "Window functions like LAG/LEAD require state management - ensure proper windowing"
                    .to_string(),
            );
        }

        // Check for missing batch configuration on high-throughput scenarios
        if result.sources_found.len() > 1 && !query_text.contains("batch.strategy") {
            result.performance_warnings.push(
                "Multi-source query without batch configuration may have suboptimal throughput"
                    .to_string(),
            );
        }
    }

    /// Validate an entire SQL application file
    pub fn validate_application(&self, file_path: &Path) -> ApplicationValidationResult {
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
        let queries_with_lines = self.split_sql_queries_with_lines(&content);
        result.total_queries = queries_with_lines.len();

        // Validate each query
        for (index, (query, start_line)) in queries_with_lines.iter().enumerate() {
            let query_result = self.validate_query(query, index, *start_line, &content);
            if query_result.is_valid {
                result.valid_queries += 1;
            }
            result.query_results.push(query_result);
        }

        // Build configuration summary
        self.build_configuration_summary(&result.query_results, &mut result.configuration_summary);

        // Generate recommendations
        self.generate_recommendations(&result.query_results, &mut result.recommendations);

        // Final validation
        if result.valid_queries != result.total_queries {
            result.is_valid = false;
        }

        result
    }

    /// Extract application name from SQL comments
    fn extract_application_name(&self, content: &str) -> Option<String> {
        for line in content.lines() {
            if line.starts_with("-- SQL Application:") {
                return Some(line.replace("-- SQL Application:", "").trim().to_string());
            }
        }
        None
    }

    /// Split SQL content into individual queries with line number tracking
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

    /// Split SQL content into individual queries (legacy method)
    fn split_sql_queries(&self, content: &str) -> Vec<String> {
        self.split_sql_queries_with_lines(content)
            .into_iter()
            .map(|(query, _line)| query)
            .collect()
    }

    /// Build configuration summary from all queries
    fn build_configuration_summary(
        &self,
        query_results: &[QueryValidationResult],
        summary: &mut ConfigurationSummary,
    ) {
        let mut source_names = HashSet::new();
        let mut sink_names = HashSet::new();

        for query_result in query_results {
            // Track source names for duplicates
            for source in &query_result.sources_found {
                if !source_names.insert(source.clone()) {
                    summary
                        .duplicate_names
                        .push(format!("Source '{}' defined multiple times", source));
                }
            }

            // Track sink names for duplicates
            for sink in &query_result.sinks_found {
                if !sink_names.insert(sink.clone()) {
                    summary
                        .duplicate_names
                        .push(format!("Sink '{}' defined multiple times", sink));
                }
            }

            // Collect missing configurations
            summary
                .missing_configurations
                .extend(query_result.missing_source_configs.clone());
            summary
                .missing_configurations
                .extend(query_result.missing_sink_configs.clone());
        }
    }

    /// Generate recommendations based on validation results
    fn generate_recommendations(
        &self,
        query_results: &[QueryValidationResult],
        recommendations: &mut Vec<String>,
    ) {
        let mut has_performance_issues = false;
        let mut has_missing_configs = false;
        let mut has_syntax_issues = false;

        for query_result in query_results {
            if !query_result.performance_warnings.is_empty() {
                has_performance_issues = true;
            }
            if !query_result.missing_source_configs.is_empty()
                || !query_result.missing_sink_configs.is_empty()
            {
                has_missing_configs = true;
            }
            if !query_result.syntax_issues.is_empty() {
                has_syntax_issues = true;
            }
        }

        if has_missing_configs {
            recommendations.push(
                "Add missing source and sink configurations to ensure proper operation".to_string(),
            );
        }

        if has_performance_issues {
            recommendations.push(
                "Consider adding batch processing configuration for better throughput".to_string(),
            );
            recommendations.push(
                "Review window specifications and JOIN conditions for performance optimization"
                    .to_string(),
            );
        }

        if has_syntax_issues {
            recommendations
                .push("Update SQL syntax to use FerrisStreams-compatible constructs".to_string());
        }

        if query_results.len() > 5 {
            recommendations.push("Large applications benefit from splitting into multiple files or deployment stages".to_string());
        }
    }

    /// Validate multiple SQL files
    pub fn validate_directory(&self, dir_path: &Path) -> Vec<ApplicationValidationResult> {
        let mut results = Vec::new();

        if let Ok(entries) = fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                    results.push(self.validate_application(&path));
                }
            }
        }

        results
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: {} <sql_file_or_directory> [--strict] [--no-performance]",
            args[0]
        );
        std::process::exit(1);
    }

    let path = Path::new(&args[1]);
    let strict_mode = args.contains(&"--strict".to_string());
    let check_performance = !args.contains(&"--no-performance".to_string());

    let mut validator = if strict_mode {
        SqlValidator::new_strict()
    } else {
        SqlValidator::new()
    };

    validator.check_performance = check_performance;

    info!("Starting SQL validation for: {}", path.display());

    if path.is_file() {
        // Validate single file
        let result = validator.validate_application(path);
        print_validation_result(&result);

        if !result.is_valid {
            std::process::exit(1);
        }
    } else if path.is_dir() {
        // Validate directory
        let results = validator.validate_directory(path);
        let mut all_valid = true;

        for result in &results {
            print_validation_result(result);
            if !result.is_valid {
                all_valid = false;
            }
            println!("{}", "=".repeat(80));
        }

        println!("\nSUMMARY: Validated {} SQL files", results.len());
        let valid_count = results.iter().filter(|r| r.is_valid).count();
        println!(
            "Valid: {}, Invalid: {}",
            valid_count,
            results.len() - valid_count
        );

        if !all_valid {
            std::process::exit(1);
        }
    } else {
        eprintln!("Path does not exist: {}", path.display());
        std::process::exit(1);
    }

    info!("SQL validation completed successfully");
    Ok(())
}

fn print_validation_result(result: &ApplicationValidationResult) {
    println!("╔══════════════════════════════════════════════════════════════════════");
    println!("║ SQL APPLICATION VALIDATION REPORT");
    println!("╠══════════════════════════════════════════════════════════════════════");
    println!("║ File: {}", result.file_path);

    if let Some(app_name) = &result.application_name {
        println!("║ Application: {}", app_name);
    }

    println!(
        "║ Status: {}",
        if result.is_valid {
            "✅ VALID"
        } else {
            "❌ INVALID"
        }
    );
    println!(
        "║ Queries: {}/{} valid",
        result.valid_queries, result.total_queries
    );
    println!("╚══════════════════════════════════════════════════════════════════════");

    // Global errors
    if !result.global_errors.is_empty() {
        println!("\n🚨 GLOBAL ERRORS:");
        for error in &result.global_errors {
            println!("  ❌ {}", error);
        }
    }

    // Query-level results
    for (index, query_result) in result.query_results.iter().enumerate() {
        if !query_result.is_valid || !query_result.warnings.is_empty() {
            println!(
                "\n📝 Query #{} ({}): {}",
                index + 1,
                if query_result.is_valid {
                    "✅ Valid"
                } else {
                    "❌ Invalid"
                },
                query_result
                    .query_text
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim_start_matches("--")
                    .trim()
            );

            if !query_result.parsing_errors.is_empty() {
                println!("  🚫 Parsing Errors:");
                for error in &query_result.parsing_errors {
                    println!(
                        "    • {} (Line {}, Column {})",
                        error.message,
                        error.line + 1,
                        error.column + 1
                    );
                    println!("      📍 Location in file:");
                    for context_line in &error.context_lines {
                        println!("        {}", context_line);
                    }
                    println!("      🔍 Error context: {}", error.error_indicator);
                    println!();
                }
            }

            if !query_result.configuration_errors.is_empty() {
                println!("  ⚙️ Configuration Errors:");
                for error in &query_result.configuration_errors {
                    println!("    • {}", error);
                }
            }

            if !query_result.missing_source_configs.is_empty() {
                println!("  📥 Missing Source Configs:");
                for missing in &query_result.missing_source_configs {
                    println!("    • {}", missing);
                }
            }

            if !query_result.missing_sink_configs.is_empty() {
                println!("  📤 Missing Sink Configs:");
                for missing in &query_result.missing_sink_configs {
                    println!("    • {}", missing);
                }
            }

            if !query_result.syntax_issues.is_empty() {
                println!("  🔧 Syntax Issues:");
                for issue in &query_result.syntax_issues {
                    println!("    • {}", issue);
                }
            }

            if !query_result.warnings.is_empty() {
                println!("  ⚠️ Warnings:");
                for warning in &query_result.warnings {
                    println!("    • {}", warning);
                }
            }

            if !query_result.performance_warnings.is_empty() {
                println!("  ⚡ Performance Warnings:");
                for warning in &query_result.performance_warnings {
                    println!("    • {}", warning);
                }
            }
        }
    }

    // Configuration summary
    if !result
        .configuration_summary
        .missing_configurations
        .is_empty()
        || !result.configuration_summary.duplicate_names.is_empty()
    {
        println!("\n📊 CONFIGURATION SUMMARY:");

        if !result
            .configuration_summary
            .missing_configurations
            .is_empty()
        {
            println!("  Missing Configurations:");
            for missing in &result.configuration_summary.missing_configurations {
                println!("    • {}", missing);
            }
        }

        if !result.configuration_summary.duplicate_names.is_empty() {
            println!("  Duplicate Names:");
            for dup in &result.configuration_summary.duplicate_names {
                println!("    • {}", dup);
            }
        }
    }

    // Recommendations
    if !result.recommendations.is_empty() {
        println!("\n💡 RECOMMENDATIONS:");
        for rec in &result.recommendations {
            println!("  • {}", rec);
        }
    }
}
