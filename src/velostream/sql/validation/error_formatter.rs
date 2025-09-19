//! Error Formatting Module
//!
//! Handles formatting of validation errors and results for display.

use super::result_types::{ApplicationValidationResult, ParsingError, QueryValidationResult};
use std::collections::HashMap;

/// Formats validation errors and results for display
pub struct ValidationErrorFormatter {
    show_context: bool,
    color_output: bool,
    verbose_mode: bool,
}

impl ValidationErrorFormatter {
    /// Create a new error formatter
    pub fn new() -> Self {
        Self {
            show_context: true,
            color_output: true,
            verbose_mode: false,
        }
    }

    /// Create a minimal formatter (no colors, minimal context)
    pub fn new_minimal() -> Self {
        Self {
            show_context: false,
            color_output: false,
            verbose_mode: false,
        }
    }

    /// Enable or disable context display
    pub fn with_context(mut self, enabled: bool) -> Self {
        self.show_context = enabled;
        self
    }

    /// Enable or disable color output
    pub fn with_colors(mut self, enabled: bool) -> Self {
        self.color_output = enabled;
        self
    }

    /// Enable or disable verbose mode
    pub fn with_verbose(mut self, enabled: bool) -> Self {
        self.verbose_mode = enabled;
        self
    }

    /// Format parsing error with context
    pub fn format_parsing_error(&self, error: &ParsingError, content: &str) -> Vec<String> {
        let mut output = Vec::new();

        // Main error message
        output.push(format!("🚫 Parsing Errors:"));
        output.push(format!("  • {}", error.message));

        if self.show_context {
            // Add location information
            output.push(format!("at line {}, column {}", error.line, error.column));

            // Add context lines
            let context_lines =
                self.get_error_context(content, error.line, error.column, error.position);
            for line in context_lines {
                output.push(format!("| {}", line));
            }

            // Add error indicator
            output.push(format!("🔍 Error context: {}", error.error_indicator));
        }

        output
    }

    /// Format query validation result
    pub fn format_query_result(
        &self,
        result: &QueryValidationResult,
        query_number: usize,
    ) -> Vec<String> {
        let mut output = Vec::new();

        // Query header
        let status_emoji = if result.is_valid { "✅" } else { "❌" };
        let status_text = if result.is_valid { "Valid" } else { "Invalid" };

        let query_preview = self.get_query_preview(&result.query_text);
        output.push(format!(
            "📝 Query #{} ({} {}): {}",
            query_number, status_emoji, status_text, query_preview
        ));

        // Parsing errors
        if !result.parsing_errors.is_empty() {
            output.push("  🚫 Parsing Errors:".to_string());
            for error in &result.parsing_errors {
                output.push(format!("    • {}", error.message));
                if self.show_context {
                    output.push(format!("  at line {}, column {}", error.line, error.column));
                    output.push(format!("      🔍 Error context: {}", error.error_indicator));
                }
            }
        }

        // Configuration errors
        if !result.configuration_errors.is_empty() {
            output.push("  ⚙️ Configuration Errors:".to_string());
            for error in &result.configuration_errors {
                output.push(format!("    • {}", error));
            }
        }

        // Syntax issues
        if !result.syntax_issues.is_empty() {
            output.push("  🔧 Syntax Issues:".to_string());
            for issue in &result.syntax_issues {
                output.push(format!("    • {}", issue));
            }
        }

        // Warnings
        if !result.warnings.is_empty() {
            output.push("  ⚠️ Warnings:".to_string());
            for warning in &result.warnings {
                output.push(format!("    • {}", warning));
            }
        }

        // Performance warnings
        if !result.performance_warnings.is_empty() && self.verbose_mode {
            output.push("  ⚡ Performance Warnings:".to_string());
            for warning in &result.performance_warnings {
                output.push(format!("    • {}", warning));
            }
        }

        output
    }

    /// Format application validation result
    pub fn format_application_result(&self, result: &ApplicationValidationResult) -> Vec<String> {
        let mut output = Vec::new();

        // Header
        output.push(
            "╔══════════════════════════════════════════════════════════════════════".to_string(),
        );
        output.push("║ SQL APPLICATION VALIDATION REPORT".to_string());
        output.push(
            "╠══════════════════════════════════════════════════════════════════════".to_string(),
        );
        output.push(format!("║ File: {}", result.file_path));

        if !result.application_name.is_empty() {
            output.push(format!("║ Application: {}", result.application_name));
        }

        output.push(format!(
            "║ Status: {} {}",
            result.status_emoji(),
            result.status_string()
        ));
        output.push(format!("║ {}", result.validation_summary()));
        output.push(
            "╚══════════════════════════════════════════════════════════════════════".to_string(),
        );
        output.push("".to_string());

        // File errors
        if !result.file_errors.is_empty() {
            output.push("🚫 File Errors:".to_string());
            for error in &result.file_errors {
                output.push(format!("  • {}", error));
            }
            output.push("".to_string());
        }

        // Query results
        for (index, query_result) in result.query_results.iter().enumerate() {
            let query_output = self.format_query_result(query_result, index + 1);
            output.extend(query_output);
            output.push("".to_string());
        }

        // Configuration summary
        if !result.configuration_summary.is_empty() {
            output.push("📋 Configuration Summary:".to_string());
            for summary_item in &result.configuration_summary {
                output.push(format!("  {}", summary_item));
            }
            output.push("".to_string());
        }

        // Recommendations
        if !result.recommendations.is_empty() {
            output.push("💡 RECOMMENDATIONS:".to_string());
            for recommendation in &result.recommendations {
                output.push(format!("  • {}", recommendation));
            }
        }

        output
    }

    // Private helper methods
    fn get_query_preview(&self, query_text: &str) -> String {
        let first_line = query_text.lines().next().unwrap_or("").trim();
        if first_line.len() > 50 {
            format!("{}...", &first_line[..47])
        } else {
            first_line.to_string()
        }
    }

    fn get_error_context(
        &self,
        content: &str,
        error_line: usize,
        _error_column: usize,
        _position: usize,
    ) -> Vec<String> {
        let lines: Vec<&str> = content.lines().collect();
        let start_line = error_line.saturating_sub(2);
        let end_line = std::cmp::min(error_line + 3, lines.len());

        let mut context = Vec::new();

        for (i, line_content) in lines[start_line..end_line].iter().enumerate() {
            let line_number = start_line + i + 1;
            let prefix = if line_number == error_line {
                format!("→{:4} ", line_number)
            } else {
                format!(" {:4} ", line_number)
            };

            context.push(format!("{}{}", prefix, line_content));
        }

        context
    }
}

impl Default for ValidationErrorFormatter {
    fn default() -> Self {
        Self::new()
    }
}
