//! Query Validation Module
//!
//! Handles SQL query parsing, syntax validation, and analysis.

use super::result_types::{ParsingError, QueryValidationResult};
use crate::velostream::sql::{
    ast::StreamingQuery,
    error::SqlError,
    parser::StreamingSqlParser,
    query_analyzer::{DataSinkRequirement, DataSourceRequirement, QueryAnalyzer},
};
use std::collections::HashMap;

/// Handles SQL query validation and parsing
pub struct QueryValidator {
    parser: StreamingSqlParser,
    analyzer: QueryAnalyzer,
    strict_mode: bool,
    performance_checks: bool,
}

impl QueryValidator {
    /// Create a new query validator
    pub fn new() -> Self {
        Self {
            parser: StreamingSqlParser::new(),
            analyzer: QueryAnalyzer::new("query-validator".to_string()),
            strict_mode: false,
            performance_checks: true,
        }
    }

    /// Create validator in strict mode
    pub fn new_strict() -> Self {
        Self {
            parser: StreamingSqlParser::new(),
            analyzer: QueryAnalyzer::new("query-validator".to_string()),
            strict_mode: true,
            performance_checks: true,
        }
    }

    /// Enable or disable performance checks
    pub fn with_performance_checks(mut self, enabled: bool) -> Self {
        self.performance_checks = enabled;
        self
    }

    /// Validate a single SQL query
    pub fn validate_query(&self, query_text: &str) -> QueryValidationResult {
        let mut result = QueryValidationResult::new(query_text.to_string());

        // Step 1: Parse the SQL query
        let parsed_query = match self.parse_query(query_text) {
            Ok(query) => query,
            Err(parsing_error) => {
                result.parsing_errors.push(parsing_error);
                result.is_valid = false;
                return result;
            }
        };

        // Step 2: Analyze query for sources, sinks, and configurations
        match self.analyze_query(&parsed_query) {
            Ok(analysis) => {
                result.sources_found = analysis
                    .required_sources
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();
                result.sinks_found = analysis
                    .required_sinks
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();

                // Extract configurations from analysis
                for source in &analysis.required_sources {
                    if let Some(config) = self.build_source_config(source) {
                        result.source_configs.insert(source.name.clone(), config);
                    }
                }

                for sink in &analysis.required_sinks {
                    if let Some(config) = self.build_sink_config(sink) {
                        result.sink_configs.insert(sink.name.clone(), config);
                    }
                }
            }
            Err(analysis_error) => {
                result
                    .add_configuration_error(format!("Query analysis failed: {}", analysis_error));
            }
        }

        // Step 3: Syntax compatibility checks
        self.check_syntax_compatibility(&parsed_query, &mut result);

        // Step 4: Performance analysis (if enabled)
        if self.performance_checks {
            self.analyze_performance(&parsed_query, &mut result);
        }

        result
    }

    // Private implementation methods
    fn parse_query(&self, query_text: &str) -> Result<StreamingQuery, ParsingError> {
        match self.parser.parse(query_text) {
            Ok(query) => Ok(query),
            Err(sql_error) => {
                let parsing_error = self.create_parsing_error(query_text, sql_error);
                Err(parsing_error)
            }
        }
    }

    fn analyze_query(
        &self,
        query: &StreamingQuery,
    ) -> Result<crate::velostream::sql::query_analyzer::QueryAnalysis, SqlError> {
        self.analyzer.analyze(query)
    }

    fn create_parsing_error(&self, query_text: &str, sql_error: SqlError) -> ParsingError {
        let error_msg = format!("{}", sql_error);
        let position = self.extract_position(&error_msg).unwrap_or(0);
        let (line, column) = self.position_to_line_column(query_text, position);
        let error_indicator = self.create_error_indicator(query_text, position, column);

        ParsingError {
            message: error_msg.clone(),
            line,
            column,
            position,
            error_type: self.classify_error_type(&error_msg),
            error_indicator,
        }
    }

    fn extract_position(&self, error_msg: &str) -> Option<usize> {
        // Extract position from error messages like "at position 123"
        if let Some(pos_start) = error_msg.find("position ") {
            let pos_str = &error_msg[pos_start + 9..];
            if let Some(pos_end) = pos_str.find(|c: char| !c.is_ascii_digit()) {
                pos_str[..pos_end].parse().ok()
            } else {
                pos_str.parse().ok()
            }
        } else {
            None
        }
    }

    fn position_to_line_column(&self, text: &str, position: usize) -> (usize, usize) {
        let mut line = 1;
        let mut column = 1;
        let chars: Vec<char> = text.chars().collect();

        for (_i, &ch) in chars.iter().enumerate().take(position) {
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }

        (line, column)
    }

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

    fn classify_error_type(&self, error_msg: &str) -> String {
        if error_msg.contains("Expected") {
            "syntax".to_string()
        } else if error_msg.contains("Unknown") {
            "semantic".to_string()
        } else if error_msg.contains("Parse") {
            "parse".to_string()
        } else {
            "general".to_string()
        }
    }

    fn build_source_config(
        &self,
        source: &DataSourceRequirement,
    ) -> Option<HashMap<String, String>> {
        // Return the actual properties from the source requirement
        // which now include the properly flattened YAML config
        Some(source.properties.clone())
    }

    fn build_sink_config(&self, sink: &DataSinkRequirement) -> Option<HashMap<String, String>> {
        // Return the actual properties from the sink requirement
        // which include the properly flattened YAML config
        Some(sink.properties.clone())
    }

    fn check_syntax_compatibility(
        &self,
        _query: &StreamingQuery,
        result: &mut QueryValidationResult,
    ) {
        // Check for unsupported syntax patterns
        let query_text = result.query_text.clone();

        if query_text.contains("WINDOW") && query_text.contains("GROUP BY") {
            result.add_syntax_issue("WINDOW clauses in GROUP BY are not fully supported - use simplified window functions".to_string());
        }

        if query_text.contains("RECURSIVE") {
            result.add_syntax_issue(
                "RECURSIVE CTEs have limited support - test thoroughly".to_string(),
            );
        }

        if query_text.contains("MERGE") {
            result.add_syntax_issue(
                "MERGE statements are not yet supported - use INSERT/UPDATE/DELETE".to_string(),
            );
        }
    }

    fn analyze_performance(&self, query: &StreamingQuery, result: &mut QueryValidationResult) {
        // Check for performance anti-patterns - use AST analysis for better accuracy
        self.check_join_performance(query, result);
        self.check_other_performance_patterns(result);
    }

    fn check_join_performance(&self, query: &StreamingQuery, result: &mut QueryValidationResult) {
        use crate::velostream::sql::ast::{StreamSource, StreamingQuery as SQ};

        match query {
            SQ::Select { from, joins, .. } => {
                // Check if main source is a stream
                let main_is_stream = self.is_stream_source(from);

                // Analyze each JOIN if they exist
                if let Some(join_list) = joins {
                    for join in join_list {
                        let right_is_stream = self.is_stream_source(&join.right_source);

                        // Only warn for stream-to-stream JOINs without time windows
                        if main_is_stream && right_is_stream {
                            // Check if there's a window specification
                            let has_window = join.window.is_some()
                                || result.query_text.contains("WINDOW")
                                || result.query_text.contains("INTERVAL");

                            if !has_window {
                                result
                                    .performance_warnings
                                    .push("Stream-to-stream JOINs without time windows can be expensive. Consider adding WINDOW clauses for bounded joins.".to_string());
                            }
                        }
                        // Stream-to-Table JOINs are efficient and don't need warnings
                    }
                }
            }
            _ => {
                // For non-SELECT queries, fall back to simple string matching
                if result.query_text.contains("JOIN") && !result.query_text.contains("WINDOW") {
                    result.performance_warnings.push(
                        "JOINs without time windows may impact performance in streaming contexts"
                            .to_string(),
                    );
                }
            }
        }
    }

    fn is_stream_source(&self, source: &crate::velostream::sql::ast::StreamSource) -> bool {
        use crate::velostream::sql::ast::StreamSource;

        match source {
            StreamSource::Stream(_) => true,
            StreamSource::Table(_) => false,
            StreamSource::Uri(uri) => {
                // For URI sources, use heuristics based on common naming patterns
                uri.ends_with("_stream")
                    || uri.ends_with("_events")
                    || uri.contains("stream")
                    || uri.ends_with("_feed")
                    || uri.ends_with("_log")
            }
            StreamSource::Subquery(_) => {
                // Subqueries could be either stream or table-like,
                // assume table-like for conservative performance warnings
                false
            }
        }
    }

    fn check_other_performance_patterns(&self, result: &mut QueryValidationResult) {
        let query_text = &result.query_text;

        if query_text.contains("ORDER BY") && !query_text.contains("LIMIT") {
            result
                .performance_warnings
                .push("ORDER BY without LIMIT can cause memory issues in streaming".to_string());
        }

        if query_text.contains("GROUP BY")
            && query_text
                .split_whitespace()
                .filter(|&w| w == "GROUP")
                .count()
                > 3
        {
            result
                .performance_warnings
                .push("Multiple GROUP BY operations may impact performance".to_string());
        }
    }
}

impl Default for QueryValidator {
    fn default() -> Self {
        Self::new()
    }
}
