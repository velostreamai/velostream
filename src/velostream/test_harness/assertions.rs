//! Assertion framework for test validation
//!
//! Provides assertion types for validating query outputs:
//! - Record count assertions
//! - Schema validation
//! - Field value checks
//! - Aggregate validations
//! - JOIN coverage analysis

use super::executor::CapturedOutput;
use super::file_io::FileSourceFactory;
use super::spec::{
    AggregateCheckAssertion, AggregateFunction, AssertionConfig, ComparisonOperator,
    CompletenessAssertion, ContainsMode, DataQualityAssertion, DistributionAssertion,
    DistributionType, DlqCountAssertion, ErrorRateAssertion, EventOrderingAssertion,
    ExecutionTimeAssertion, FieldInSetAssertion, FieldValuesAssertion, FileContainsAssertion,
    FileExistsAssertion, FileMatchesAssertion, FileRowCountAssertion, JoinCoverageAssertion,
    LatencyAssertion, MemoryUsageAssertion, NoDuplicatesAssertion, NoNullsAssertion,
    OrderDirection, OrderingAssertion, PercentileAssertion, PercentileMode, RecordCountAssertion,
    SchemaContainsAssertion, TableFreshnessAssertion, TemplateAssertion, ThroughputAssertion,
    WindowBoundaryAssertion,
};
use super::utils::{field_value_to_string, resolve_path};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Result of an assertion check
#[derive(Debug, Clone)]
pub struct AssertionResult {
    /// Assertion type name
    pub assertion_type: String,

    /// Whether assertion passed
    pub passed: bool,

    /// Human-readable message
    pub message: String,

    /// Expected value (for display)
    pub expected: Option<String>,

    /// Actual value (for display)
    pub actual: Option<String>,

    /// Additional details for debugging
    pub details: HashMap<String, String>,
}

impl AssertionResult {
    /// Create passing result
    pub fn pass(assertion_type: &str, message: &str) -> Self {
        Self {
            assertion_type: assertion_type.to_string(),
            passed: true,
            message: message.to_string(),
            expected: None,
            actual: None,
            details: HashMap::new(),
        }
    }

    /// Create failing result
    pub fn fail(assertion_type: &str, message: &str, expected: &str, actual: &str) -> Self {
        Self {
            assertion_type: assertion_type.to_string(),
            passed: false,
            message: message.to_string(),
            expected: Some(expected.to_string()),
            actual: Some(actual.to_string()),
            details: HashMap::new(),
        }
    }

    /// Add detail to result
    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }
}

/// Assertion runner
pub struct AssertionRunner {
    /// Context for template variable substitution
    context: AssertionContext,
    /// Base directory for resolving relative file paths
    base_dir: PathBuf,
}

/// Context for assertion evaluation
#[derive(Debug, Default, Clone)]
pub struct AssertionContext {
    /// Input record counts by source name
    pub input_counts: HashMap<String, usize>,

    /// Output record counts by sink name
    pub output_counts: HashMap<String, usize>,

    /// Custom variables
    pub variables: HashMap<String, String>,

    /// Input records by source name (for JOIN coverage analysis)
    pub input_records: HashMap<String, Vec<HashMap<String, FieldValue>>>,
}

impl AssertionContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Add input records for a source
    pub fn with_input_records(
        mut self,
        source_name: &str,
        records: Vec<HashMap<String, FieldValue>>,
    ) -> Self {
        let count = records.len();
        self.input_records.insert(source_name.to_string(), records);
        self.input_counts.insert(source_name.to_string(), count);
        self
    }

    /// Add output record count
    pub fn with_output_count(mut self, sink_name: &str, count: usize) -> Self {
        self.output_counts.insert(sink_name.to_string(), count);
        self
    }

    /// Add custom variable
    pub fn with_variable(mut self, name: &str, value: &str) -> Self {
        self.variables.insert(name.to_string(), value.to_string());
        self
    }

    /// Resolve a template variable like "{{inputs.source_name.count}}"
    pub fn resolve_template(&self, template: &str) -> Option<String> {
        // Check if it's a template expression
        if !template.starts_with("{{") || !template.ends_with("}}") {
            return None;
        }

        // Extract the expression inside {{ }}
        let expr = template[2..template.len() - 2].trim();

        // Parse the expression path (e.g., "inputs.source_name.count")
        let parts: Vec<&str> = expr.split('.').collect();

        match parts.as_slice() {
            // {{inputs.source_name.count}}
            ["inputs", source_name, "count"] => {
                self.input_counts.get(*source_name).map(|c| c.to_string())
            }

            // {{outputs.sink_name.count}}
            ["outputs", sink_name, "count"] => {
                self.output_counts.get(*sink_name).map(|c| c.to_string())
            }

            // {{variables.var_name}}
            ["variables", var_name] => self.variables.get(*var_name).cloned(),

            // {{var_name}} - direct variable lookup
            [var_name] => self.variables.get(*var_name).cloned(),

            _ => None,
        }
    }

    /// Resolve template or return original string
    pub fn resolve_or_original(&self, value: &str) -> String {
        self.resolve_template(value)
            .unwrap_or_else(|| value.to_string())
    }

    /// Evaluate an expression that may contain template variables and arithmetic
    ///
    /// Supports:
    /// - Template variables: `{{inputs.source_name.count}}`
    /// - Arithmetic operators: +, -, *, /
    /// - Comparisons: >, <, >=, <=, ==, !=
    /// - Numeric literals
    ///
    /// Examples:
    /// - `{{inputs.source.count}}` -> resolves to input count
    /// - `{{inputs.source.count}} * 0.9` -> 90% of input count
    /// - `{{outputs.sink.count}} >= {{inputs.source.count}} * 0.95`
    pub fn evaluate_expression(&self, expr: &str) -> Result<f64, String> {
        // First, resolve all template variables in the expression
        let resolved = self.resolve_all_templates(expr);

        // Then evaluate the arithmetic expression
        self.evaluate_arithmetic(&resolved)
    }

    /// Resolve all template variables in a string
    fn resolve_all_templates(&self, expr: &str) -> String {
        let mut result = expr.to_string();

        // Find all {{...}} patterns and resolve them
        while let Some(start) = result.find("{{") {
            if let Some(end) = result[start..].find("}}") {
                let template = &result[start..start + end + 2];
                if let Some(value) = self.resolve_template(template) {
                    result = result.replacen(template, &value, 1);
                } else {
                    // Template not found, replace with 0 to allow expression to continue
                    log::warn!(
                        "Template variable '{}' not found, defaulting to 0",
                        template
                    );
                    result = result.replacen(template, "0", 1);
                }
            } else {
                break; // Malformed template, stop processing
            }
        }

        result
    }

    /// Evaluate an arithmetic expression (after templates are resolved)
    fn evaluate_arithmetic(&self, expr: &str) -> Result<f64, String> {
        let expr = expr.trim();

        // Handle comparison operators (for boolean expressions)
        for op in &[">=", "<=", "!=", "==", ">", "<"] {
            if let Some(pos) = expr.find(op) {
                let left = self.evaluate_arithmetic(&expr[..pos])?;
                let right = self.evaluate_arithmetic(&expr[pos + op.len()..])?;
                let result = match *op {
                    ">=" => left >= right,
                    "<=" => left <= right,
                    "!=" => (left - right).abs() > f64::EPSILON,
                    "==" => (left - right).abs() <= f64::EPSILON,
                    ">" => left > right,
                    "<" => left < right,
                    _ => false,
                };
                return Ok(if result { 1.0 } else { 0.0 });
            }
        }

        // Handle addition and subtraction (lowest precedence)
        let mut depth = 0;
        for (i, c) in expr.char_indices().rev() {
            match c {
                ')' => depth += 1,
                '(' => depth -= 1,
                '+' | '-' if depth == 0 && i > 0 => {
                    // Check if this is a binary operator (not a negative sign)
                    let prev_char = expr[..i].trim().chars().last();
                    if let Some(pc) = prev_char {
                        if pc.is_numeric() || pc == ')' {
                            let left = self.evaluate_arithmetic(&expr[..i])?;
                            let right = self.evaluate_arithmetic(&expr[i + 1..])?;
                            return Ok(if c == '+' { left + right } else { left - right });
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle multiplication and division (higher precedence)
        depth = 0;
        for (i, c) in expr.char_indices().rev() {
            match c {
                ')' => depth += 1,
                '(' => depth -= 1,
                '*' | '/' if depth == 0 => {
                    let left = self.evaluate_arithmetic(&expr[..i])?;
                    let right = self.evaluate_arithmetic(&expr[i + 1..])?;
                    return Ok(if c == '*' {
                        left * right
                    } else {
                        if right == 0.0 {
                            return Err("Division by zero".to_string());
                        }
                        left / right
                    });
                }
                _ => {}
            }
        }

        // Handle parentheses
        if expr.starts_with('(') && expr.ends_with(')') {
            return self.evaluate_arithmetic(&expr[1..expr.len() - 1]);
        }

        // Try to parse as a number
        expr.parse::<f64>()
            .map_err(|_| format!("Cannot parse '{}' as a number", expr))
    }

    /// Evaluate an expression and compare to actual value
    ///
    /// Returns Ok(true) if the expression evaluates to true when compared with actual
    pub fn evaluate_comparison(&self, expr: &str, actual: f64) -> Result<bool, String> {
        // Check for comparison operators in the expression
        for op in &[">=", "<=", "!=", "==", ">", "<"] {
            if expr.contains(op) {
                // If expression already has a comparison, evaluate it directly
                let result = self.evaluate_expression(expr)?;
                return Ok(result != 0.0);
            }
        }

        // If no comparison operator, evaluate the expression and compare to actual
        let expected = self.evaluate_expression(expr)?;

        // Check if the expression contains % for tolerance
        let tolerance = if expr.contains('%') { 0.01 } else { 0.001 };

        Ok((actual - expected).abs() <= expected.abs() * tolerance)
    }
}

impl AssertionRunner {
    /// Create new assertion runner
    pub fn new() -> Self {
        Self {
            context: AssertionContext::default(),
            base_dir: PathBuf::from("."),
        }
    }

    /// Set context for template evaluation
    pub fn with_context(mut self, context: AssertionContext) -> Self {
        self.context = context;
        self
    }

    /// Set base directory for resolving relative file paths
    pub fn with_base_dir(mut self, base_dir: impl Into<PathBuf>) -> Self {
        self.base_dir = base_dir.into();
        self
    }

    /// Resolve a file path relative to base_dir
    fn resolve_file_path(&self, path: &str) -> PathBuf {
        resolve_path(path, &self.base_dir)
    }

    /// Run all assertions for a captured output
    pub fn run_assertions(
        &self,
        output: &CapturedOutput,
        assertions: &[AssertionConfig],
    ) -> Vec<AssertionResult> {
        assertions
            .iter()
            .map(|assertion| self.run_assertion(output, assertion))
            .collect()
    }

    /// Run single assertion
    pub fn run_assertion(
        &self,
        output: &CapturedOutput,
        assertion: &AssertionConfig,
    ) -> AssertionResult {
        match assertion {
            AssertionConfig::RecordCount(config) => self.assert_record_count(output, config),
            AssertionConfig::SchemaContains(config) => self.assert_schema_contains(output, config),
            AssertionConfig::NoNulls(config) => self.assert_no_nulls(output, config),
            AssertionConfig::FieldInSet(config) => self.assert_field_in_set(output, config),
            AssertionConfig::FieldValues(config) => self.assert_field_values(output, config),
            AssertionConfig::AggregateCheck(config) => self.assert_aggregate(output, config),
            AssertionConfig::JoinCoverage(config) => self.assert_join_coverage(output, config),
            AssertionConfig::Template(config) => self.assert_template(output, config),
            AssertionConfig::ExecutionTime(config) => self.assert_execution_time(output, config),
            AssertionConfig::MemoryUsage(config) => self.assert_memory_usage(output, config),
            AssertionConfig::Throughput(config) => self.assert_throughput(output, config),
            // Streaming-specific assertions
            AssertionConfig::DlqCount(config) => self.assert_dlq_count(output, config),
            AssertionConfig::ErrorRate(config) => self.assert_error_rate(output, config),
            AssertionConfig::NoDuplicates(config) => self.assert_no_duplicates(output, config),
            AssertionConfig::Ordering(config) => self.assert_ordering(output, config),
            AssertionConfig::Completeness(config) => self.assert_completeness(output, config),
            AssertionConfig::TableFreshness(config) => self.assert_table_freshness(output, config),
            AssertionConfig::DataQuality(config) => self.assert_data_quality(output, config),
            // File-specific assertions
            AssertionConfig::FileExists(config) => self.assert_file_exists(config),
            AssertionConfig::FileRowCount(config) => self.assert_file_row_count(config),
            AssertionConfig::FileContains(config) => self.assert_file_contains(config),
            AssertionConfig::FileMatches(config) => self.assert_file_matches(config),
            // Temporal & Statistical assertions (P1.3)
            AssertionConfig::WindowBoundary(config) => self.assert_window_boundary(output, config),
            AssertionConfig::Latency(config) => self.assert_latency(output, config),
            AssertionConfig::Distribution(config) => self.assert_distribution(output, config),
            AssertionConfig::Percentile(config) => self.assert_percentile(output, config),
            AssertionConfig::EventOrdering(config) => self.assert_event_ordering(output, config),
            // Metric assertions - these require executor access, not output records
            // Use AssertionRunner::run_metric_assertion() instead
            AssertionConfig::MetricCounter(config) => AssertionResult::fail(
                "metric_counter",
                &format!(
                    "Metric assertion '{}' requires executor access. Use run_metric_assertions() instead.",
                    config.name
                ),
                "executor with metrics enabled",
                "standard assertion runner",
            ),
            AssertionConfig::MetricGauge(config) => AssertionResult::fail(
                "metric_gauge",
                &format!(
                    "Metric assertion '{}' requires executor access. Use run_metric_assertions() instead.",
                    config.name
                ),
                "executor with metrics enabled",
                "standard assertion runner",
            ),
            AssertionConfig::MetricExists(config) => AssertionResult::fail(
                "metric_exists",
                &format!(
                    "Metric assertion '{}' requires executor access. Use run_metric_assertions() instead.",
                    config.name
                ),
                "executor with metrics enabled",
                "standard assertion runner",
            ),
        }
    }

    /// Assert record count
    fn assert_record_count(
        &self,
        output: &CapturedOutput,
        config: &RecordCountAssertion,
    ) -> AssertionResult {
        let actual = output.records.len();
        let location = output.location();

        // Check exact equals
        if let Some(expected) = config.equals {
            if actual == expected {
                return AssertionResult::pass(
                    "record_count",
                    &format!("Record count matches: {} (from {})", actual, location),
                );
            }
            return AssertionResult::fail(
                "record_count",
                &format!("Record count mismatch (from {})", location),
                &expected.to_string(),
                &actual.to_string(),
            );
        }

        // Check greater_than
        if let Some(min) = config.greater_than {
            if actual <= min {
                return AssertionResult::fail(
                    "record_count",
                    &format!("Expected > {} records from {}", min, location),
                    &format!("> {}", min),
                    &actual.to_string(),
                );
            }
        }

        // Check less_than
        if let Some(max) = config.less_than {
            if actual >= max {
                return AssertionResult::fail(
                    "record_count",
                    &format!("Expected < {} records from {}", max, location),
                    &format!("< {}", max),
                    &actual.to_string(),
                );
            }
        }

        // Check between
        if let Some((min, max)) = config.between {
            if actual < min || actual > max {
                return AssertionResult::fail(
                    "record_count",
                    &format!(
                        "Expected between {} and {} records from {}",
                        min, max, location
                    ),
                    &format!("[{}, {}]", min, max),
                    &actual.to_string(),
                );
            }
        }

        // Check expression
        if let Some(ref expr) = config.expression {
            match self.context.evaluate_expression(expr) {
                Ok(expected) => {
                    // Expression evaluates to expected count
                    let expected_int = expected.round() as usize;
                    // Allow small tolerance for floating point arithmetic
                    let tolerance = (expected * 0.001).max(1.0) as usize;
                    if (actual as i64 - expected_int as i64).unsigned_abs() as usize > tolerance {
                        return AssertionResult::fail(
                            "record_count",
                            &format!("Record count from {} doesn't match expression", location),
                            &format!("{} (from '{}')", expected_int, expr),
                            &actual.to_string(),
                        );
                    }
                }
                Err(e) => {
                    return AssertionResult::fail(
                        "record_count",
                        &format!("Failed to evaluate expression '{}': {}", expr, e),
                        expr,
                        &actual.to_string(),
                    );
                }
            }
        }

        AssertionResult::pass(
            "record_count",
            &format!("Record count: {} (from {})", actual, location),
        )
    }

    /// Assert schema contains required fields
    fn assert_schema_contains(
        &self,
        output: &CapturedOutput,
        config: &SchemaContainsAssertion,
    ) -> AssertionResult {
        let location = output.location();

        if output.records.is_empty() {
            return AssertionResult::fail(
                "schema_contains",
                &format!("No records to check schema from {}", location),
                &config.fields.join(", "),
                "(no records)",
            );
        }

        // Get fields from first record (value payload)
        let actual_value_fields: std::collections::HashSet<_> =
            output.records[0].fields.keys().cloned().collect();

        // Check for missing value fields
        let missing_value_fields: Vec<&str> = config
            .fields
            .iter()
            .filter(|f| !actual_value_fields.contains(*f))
            .map(String::as_str)
            .collect();

        // Check key field if specified
        let key_field_ok = if let Some(key_field_name) = &config.key_field {
            // Verify we have message keys (from StreamRecord.key) and at least one is non-empty
            let has_keys = output
                .records
                .iter()
                .any(|r| matches!(&r.key, Some(FieldValue::String(s)) if !s.is_empty()));
            if !has_keys {
                return AssertionResult::fail(
                    "schema_contains",
                    &format!(
                        "Expected key field '{}' in {}, but no message keys found",
                        key_field_name, location
                    ),
                    &format!("key_field: {}", key_field_name),
                    "no message keys present",
                );
            }
            true
        } else {
            true
        };

        // Build expected string including key_field if present
        let expected_parts: Vec<String> = if let Some(key_field) = &config.key_field {
            let mut parts = vec![format!("key_field: {}", key_field)];
            parts.push(format!("value_fields: [{}]", config.fields.join(", ")));
            parts
        } else {
            vec![config.fields.join(", ")]
        };
        let expected_str = expected_parts.join(", ");

        // Build actual string including key info
        let mut sorted_actual: Vec<_> = actual_value_fields.iter().cloned().collect();
        sorted_actual.sort();
        let actual_str = if config.key_field.is_some() {
            let key_sample = output
                .records
                .iter()
                .find_map(|r| match &r.key {
                    Some(FieldValue::String(s)) => Some(s.clone()),
                    Some(other) => Some(format!("{:?}", other)),
                    None => None,
                })
                .unwrap_or_else(|| "(none)".to_string());
            format!(
                "key: '{}' (sample), value_fields: [{}]",
                key_sample,
                sorted_actual.join(", ")
            )
        } else {
            sorted_actual.join(", ")
        };

        if missing_value_fields.is_empty() && key_field_ok {
            AssertionResult::pass(
                "schema_contains",
                &format!(
                    "All required fields present in {}: {}",
                    location, expected_str
                ),
            )
        } else {
            let missing_str = missing_value_fields.join(", ");

            AssertionResult::fail(
                "schema_contains",
                &format!("Missing required fields in {}: [{}]", location, missing_str),
                &expected_str,
                &actual_str,
            )
        }
    }

    /// Assert no null values
    fn assert_no_nulls(
        &self,
        output: &CapturedOutput,
        config: &NoNullsAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let fields_to_check: Vec<&str> = if config.fields.is_empty() {
            // Check all fields
            if output.records.is_empty() {
                return AssertionResult::pass(
                    "no_nulls",
                    &format!("No records to check from {}", location),
                );
            }
            output.records[0]
                .fields
                .keys()
                .map(|s| s.as_str())
                .collect()
        } else {
            config.fields.iter().map(|s| s.as_str()).collect()
        };

        let mut null_fields = Vec::new();

        for field in &fields_to_check {
            for (idx, record) in output.records.iter().enumerate() {
                if let Some(FieldValue::Null) = record.fields.get(*field) {
                    null_fields.push(format!("{}[{}]", field, idx));
                }
            }
        }

        if null_fields.is_empty() {
            AssertionResult::pass(
                "no_nulls",
                &format!(
                    "No null values in {} for: {}",
                    location,
                    fields_to_check.join(", ")
                ),
            )
        } else {
            AssertionResult::fail(
                "no_nulls",
                &format!("Found null values in {}", location),
                "no nulls",
                &format!("nulls at: {}", null_fields.join(", ")),
            )
        }
    }

    /// Assert field values are in allowed set
    fn assert_field_in_set(
        &self,
        output: &CapturedOutput,
        config: &FieldInSetAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let allowed: std::collections::HashSet<_> = config.values.iter().collect();
        let mut invalid_values = Vec::new();

        for (idx, record) in output.records.iter().enumerate() {
            if let Some(value) = record.fields.get(&config.field) {
                let value_str = field_value_to_string(value);
                if !allowed.contains(&value_str) {
                    invalid_values.push(format!("[{}]={}", idx, value_str));
                }
            }
        }

        if invalid_values.is_empty() {
            AssertionResult::pass(
                "field_in_set",
                &format!(
                    "All '{}' values in {} are in allowed set",
                    config.field, location
                ),
            )
        } else {
            AssertionResult::fail(
                "field_in_set",
                &format!(
                    "Field '{}' in {} has invalid values",
                    config.field, location
                ),
                &format!("one of: {}", config.values.join(", ")),
                &format!("invalid: {}", invalid_values.join(", ")),
            )
        }
    }

    /// Assert field values with comparison operator
    fn assert_field_values(
        &self,
        output: &CapturedOutput,
        config: &FieldValuesAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let mut failures = Vec::new();

        for (idx, record) in output.records.iter().enumerate() {
            if let Some(value) = record.fields.get(&config.field) {
                let matches = compare_field_value(value, &config.operator, &config.value);
                if !matches {
                    failures.push(format!("[{}]={}", idx, field_value_to_string(value)));
                }
            }
        }

        if failures.is_empty() {
            AssertionResult::pass(
                "field_values",
                &format!(
                    "All '{}' values in {} satisfy {:?}",
                    config.field, location, config.operator
                ),
            )
        } else {
            AssertionResult::fail(
                "field_values",
                &format!(
                    "Field '{}' in {} has values that don't match",
                    config.field, location
                ),
                &format!("{:?} {:?}", config.operator, config.value),
                &format!("failures: {}", failures.join(", ")),
            )
        }
    }

    /// Assert aggregate value
    fn assert_aggregate(
        &self,
        output: &CapturedOutput,
        config: &AggregateCheckAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let values: Vec<f64> = output
            .records
            .iter()
            .filter_map(|r| r.fields.get(&config.field))
            .filter_map(|v| field_value_to_f64(v))
            .collect();

        if values.is_empty() {
            return AssertionResult::fail(
                "aggregate_check",
                &format!(
                    "No values found for field '{}' in {}",
                    config.field, location
                ),
                &config.expected,
                "(no values)",
            );
        }

        let actual = match config.function {
            AggregateFunction::Sum => values.iter().sum(),
            AggregateFunction::Count => values.len() as f64,
            AggregateFunction::Avg => values.iter().sum::<f64>() / values.len() as f64,
            AggregateFunction::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
            AggregateFunction::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        };

        // Resolve template variables in expected value (e.g., "{{inputs.source_name.count}}")
        let expected_str = self.context.resolve_or_original(&config.expected);

        // Parse expected value
        let expected: f64 = expected_str.parse().unwrap_or_else(|_| {
            log::warn!(
                "Could not parse expected value '{}' as f64, using 0.0",
                expected_str
            );
            0.0
        });
        let tolerance = config.tolerance.unwrap_or(0.0001);

        if (actual - expected).abs() <= tolerance {
            AssertionResult::pass(
                "aggregate_check",
                &format!(
                    "{:?}({}) in {} = {} (expected: {})",
                    config.function, config.field, location, actual, expected
                ),
            )
        } else {
            AssertionResult::fail(
                "aggregate_check",
                &format!(
                    "{:?}({}) mismatch in {}",
                    config.function, config.field, location
                ),
                &expected.to_string(),
                &actual.to_string(),
            )
        }
    }

    /// Assert JOIN coverage
    ///
    /// Analyzes JOIN output coverage based on input record counts.
    /// The match rate is calculated as: output_records / left_input_records
    ///
    /// For detailed key overlap analysis, provide both `left_source` and `right_source`
    /// in the config and use `with_input_records()` on the `AssertionContext`.
    fn assert_join_coverage(
        &self,
        output: &CapturedOutput,
        config: &JoinCoverageAssertion,
    ) -> AssertionResult {
        let total_output = output.records.len();

        // Get the left source name (default to "left" if not specified)
        let left_source = config.left_source.as_deref().unwrap_or("left");

        // Get the right source name (default to "right" if not specified)
        let right_source = config.right_source.as_deref().unwrap_or("right");

        // If no output records, check if we have input to compare
        if total_output == 0 {
            // Check if we have left input records
            if let Some(left_count) = self.context.input_counts.get(left_source) {
                if *left_count == 0 {
                    return AssertionResult::pass(
                        "join_coverage",
                        "No input records, no output expected",
                    );
                }
                return AssertionResult::fail(
                    "join_coverage",
                    "No output records from JOIN",
                    &format!(
                        ">= {:.1}% of {} left records",
                        config.min_match_rate * 100.0,
                        left_count
                    ),
                    "0 records (0%)",
                );
            }
            return AssertionResult::fail(
                "join_coverage",
                "No output records to analyze",
                &format!(">= {:.1}% match rate", config.min_match_rate * 100.0),
                "0 records",
            );
        }

        // Try to get left input count for match rate calculation
        let left_count = match self.context.input_counts.get(left_source) {
            Some(&count) => count,
            None => {
                // No input tracking - fall back to output-only analysis
                return AssertionResult::pass(
                    "join_coverage",
                    &format!(
                        "JOIN produced {} records (input tracking not available)",
                        total_output
                    ),
                )
                .with_detail("note", "Enable input tracking for detailed JOIN analysis");
            }
        };

        if left_count == 0 {
            // Left has 0 records
            return AssertionResult::pass(
                "join_coverage",
                "No left input records, no matches expected",
            );
        }

        // Calculate match rate as output / left input
        let match_rate = total_output as f64 / left_count as f64;

        let mut result = if match_rate >= config.min_match_rate {
            AssertionResult::pass(
                "join_coverage",
                &format!(
                    "JOIN match rate: {:.1}% ({}/{} records)",
                    match_rate * 100.0,
                    total_output,
                    left_count
                ),
            )
        } else {
            AssertionResult::fail(
                "join_coverage",
                "JOIN match rate below threshold",
                &format!(">= {:.1}%", config.min_match_rate * 100.0),
                &format!(
                    "{:.1}% ({}/{} records)",
                    match_rate * 100.0,
                    total_output,
                    left_count
                ),
            )
        };

        // Add diagnostic details
        result = result
            .with_detail("left_source", left_source)
            .with_detail("right_source", right_source)
            .with_detail("left_record_count", &left_count.to_string())
            .with_detail("output_record_count", &total_output.to_string())
            .with_detail("match_rate", &format!("{:.2}", match_rate));

        // Check right input if available for additional diagnostics
        if let Some(&right_count) = self.context.input_counts.get(right_source) {
            result = result.with_detail("right_record_count", &right_count.to_string());

            // Provide helpful diagnostics about potential issues
            if right_count == 0 {
                result = result.with_detail(
                    "warning",
                    "Right source has no records - check data generation",
                );
            }
        }

        result
    }

    /// Assert custom template
    ///
    /// Supports Jinja-like template expressions:
    /// - Variable access: `{{ records.length }}`, `{{ records[0].field }}`
    /// - Comparisons: `{{ records.length == 100 }}`, `{{ sum > 0 }}`
    /// - Loops: `{% for record in records %}...{% endfor %}`
    /// - Conditions: `{% if condition %}...{% endif %}`
    /// - Built-in functions: `all()`, `any()`, `sum()`, `count()`, `avg()`
    ///
    /// Examples:
    /// - `{{ records.length == 100 }}` - Check record count
    /// - `{{ all(record.price > 0 for record in records) }}` - All prices positive
    /// - `{{ sum(record.amount for record in records) > 1000 }}` - Sum check
    fn assert_template(
        &self,
        output: &CapturedOutput,
        config: &TemplateAssertion,
    ) -> AssertionResult {
        let expression = config.expression.trim();
        let description = config
            .description
            .as_deref()
            .unwrap_or("Template assertion");

        // Evaluate the template expression
        match self.evaluate_template_expression(expression, output) {
            Ok(TemplateResult::Boolean(true)) => {
                AssertionResult::pass("template", &format!("{}: passed", description))
            }
            Ok(TemplateResult::Boolean(false)) => {
                AssertionResult::fail("template", description, "true", "false")
                    .with_detail("expression", expression)
            }
            Ok(TemplateResult::Value(v)) => {
                // Non-boolean result - check if it's truthy
                if is_truthy(&v) {
                    AssertionResult::pass(
                        "template",
                        &format!("{}: {}", description, template_value_to_string(&v)),
                    )
                } else {
                    AssertionResult::fail("template", description, "truthy value", "falsy value")
                        .with_detail("expression", expression)
                        .with_detail("result", &template_value_to_string(&v))
                }
            }
            Ok(TemplateResult::Error(msg)) => AssertionResult::fail(
                "template",
                &format!("Template evaluation error: {}", msg),
                "valid expression",
                &msg,
            )
            .with_detail("expression", expression),
            Err(e) => AssertionResult::fail(
                "template",
                &format!("Template error: {}", e),
                "valid expression",
                &e,
            )
            .with_detail("expression", expression),
        }
    }

    /// Assert execution time is within bounds
    ///
    /// Checks that the query execution time is within the specified min/max bounds.
    fn assert_execution_time(
        &self,
        output: &CapturedOutput,
        config: &ExecutionTimeAssertion,
    ) -> AssertionResult {
        let actual_ms = output.execution_time_ms;

        // Check max_ms constraint
        if let Some(max_ms) = config.max_ms {
            if actual_ms > max_ms {
                return AssertionResult::fail(
                    "execution_time",
                    &format!(
                        "Execution time {}ms exceeds maximum {}ms",
                        actual_ms, max_ms
                    ),
                    &format!("<= {}ms", max_ms),
                    &format!("{}ms", actual_ms),
                )
                .with_detail("constraint", "max_ms");
            }
        }

        // Check min_ms constraint
        if let Some(min_ms) = config.min_ms {
            if actual_ms < min_ms {
                return AssertionResult::fail(
                    "execution_time",
                    &format!("Execution time {}ms below minimum {}ms", actual_ms, min_ms),
                    &format!(">= {}ms", min_ms),
                    &format!("{}ms", actual_ms),
                )
                .with_detail("constraint", "min_ms");
            }
        }

        // Both constraints passed (or no constraints specified)
        let msg = match (config.max_ms, config.min_ms) {
            (Some(max), Some(min)) => {
                format!(
                    "Execution time {}ms within bounds [{}ms, {}ms]",
                    actual_ms, min, max
                )
            }
            (Some(max), None) => format!("Execution time {}ms <= {}ms", actual_ms, max),
            (None, Some(min)) => format!("Execution time {}ms >= {}ms", actual_ms, min),
            (None, None) => format!("Execution time: {}ms (no constraints)", actual_ms),
        };

        AssertionResult::pass("execution_time", &msg)
            .with_detail("actual_ms", &actual_ms.to_string())
    }

    /// Assert memory usage is within bounds
    ///
    /// Checks peak memory usage and memory growth against configured limits.
    fn assert_memory_usage(
        &self,
        output: &CapturedOutput,
        config: &MemoryUsageAssertion,
    ) -> AssertionResult {
        // Check if memory tracking was enabled
        if output.memory_peak_bytes.is_none() && output.memory_growth_bytes.is_none() {
            return AssertionResult::pass(
                "memory_usage",
                "Memory tracking not available - assertion skipped",
            )
            .with_detail("note", "Enable memory tracking to use this assertion");
        }

        // Check max_bytes constraint
        if let Some(max_bytes) = config.max_bytes {
            if let Some(peak) = output.memory_peak_bytes {
                if peak > max_bytes {
                    return AssertionResult::fail(
                        "memory_usage",
                        &format!(
                            "Peak memory {} bytes exceeds maximum {} bytes",
                            format_bytes(peak),
                            format_bytes(max_bytes)
                        ),
                        &format!("<= {}", format_bytes(max_bytes)),
                        &format!("{}", format_bytes(peak)),
                    )
                    .with_detail("constraint", "max_bytes")
                    .with_detail("peak_bytes", &peak.to_string());
                }
            }
        }

        // Check max_mb constraint (convenience for megabytes)
        if let Some(max_mb) = config.max_mb {
            let max_bytes = (max_mb * 1024.0 * 1024.0) as u64;
            if let Some(peak) = output.memory_peak_bytes {
                if peak > max_bytes {
                    return AssertionResult::fail(
                        "memory_usage",
                        &format!(
                            "Peak memory {:.2} MB exceeds maximum {:.2} MB",
                            peak as f64 / 1024.0 / 1024.0,
                            max_mb
                        ),
                        &format!("<= {:.2} MB", max_mb),
                        &format!("{:.2} MB", peak as f64 / 1024.0 / 1024.0),
                    )
                    .with_detail("constraint", "max_mb")
                    .with_detail("peak_bytes", &peak.to_string());
                }
            }
        }

        // Check max_growth_bytes constraint
        if let Some(max_growth) = config.max_growth_bytes {
            if let Some(growth) = output.memory_growth_bytes {
                if growth > max_growth {
                    return AssertionResult::fail(
                        "memory_usage",
                        &format!(
                            "Memory growth {} bytes exceeds maximum {} bytes",
                            growth, max_growth
                        ),
                        &format!("<= {} bytes growth", max_growth),
                        &format!("{} bytes growth", growth),
                    )
                    .with_detail("constraint", "max_growth_bytes")
                    .with_detail("growth_bytes", &growth.to_string());
                }
            }
        }

        // All constraints passed
        let mut msg = String::from("Memory usage within bounds");
        if let Some(peak) = output.memory_peak_bytes {
            msg = format!("{} (peak: {})", msg, format_bytes(peak));
        }
        if let Some(growth) = output.memory_growth_bytes {
            let growth_str = if growth >= 0 {
                format!("+{}", format_bytes(growth as u64))
            } else {
                format!("-{}", format_bytes((-growth) as u64))
            };
            msg = format!("{} (growth: {})", msg, growth_str);
        }

        let mut result = AssertionResult::pass("memory_usage", &msg);
        if let Some(peak) = output.memory_peak_bytes {
            result = result.with_detail("peak_bytes", &peak.to_string());
        }
        if let Some(growth) = output.memory_growth_bytes {
            result = result.with_detail("growth_bytes", &growth.to_string());
        }
        result
    }

    /// Assert throughput rate is within bounds
    ///
    /// Calculates records per second and validates against configured thresholds.
    fn assert_throughput(
        &self,
        output: &CapturedOutput,
        config: &ThroughputAssertion,
    ) -> AssertionResult {
        let record_count = output.records.len();
        let execution_time_ms = output.execution_time_ms;

        // Calculate throughput (records per second)
        let throughput = if execution_time_ms > 0 {
            (record_count as f64) / (execution_time_ms as f64 / 1000.0)
        } else {
            // If execution time is 0, we have infinite throughput (or no records)
            if record_count == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        };

        // Check min_records_per_second constraint
        if let Some(min_rps) = config.min_records_per_second {
            if throughput < min_rps {
                return AssertionResult::fail(
                    "throughput",
                    &format!(
                        "Throughput {:.2} records/sec below minimum {:.2} records/sec",
                        throughput, min_rps
                    ),
                    &format!(">= {:.2} records/sec", min_rps),
                    &format!("{:.2} records/sec", throughput),
                )
                .with_detail("constraint", "min_records_per_second")
                .with_detail("record_count", &record_count.to_string())
                .with_detail("execution_time_ms", &execution_time_ms.to_string());
            }
        }

        // Check max_records_per_second constraint
        if let Some(max_rps) = config.max_records_per_second {
            if throughput > max_rps {
                return AssertionResult::fail(
                    "throughput",
                    &format!(
                        "Throughput {:.2} records/sec exceeds maximum {:.2} records/sec",
                        throughput, max_rps
                    ),
                    &format!("<= {:.2} records/sec", max_rps),
                    &format!("{:.2} records/sec", throughput),
                )
                .with_detail("constraint", "max_records_per_second")
                .with_detail("record_count", &record_count.to_string())
                .with_detail("execution_time_ms", &execution_time_ms.to_string());
            }
        }

        // Check expected_records_per_second with tolerance
        if let Some(expected_rps) = config.expected_records_per_second {
            let tolerance_fraction = config.tolerance_percent / 100.0;
            let min_allowed = expected_rps * (1.0 - tolerance_fraction);
            let max_allowed = expected_rps * (1.0 + tolerance_fraction);

            if throughput < min_allowed || throughput > max_allowed {
                return AssertionResult::fail(
                    "throughput",
                    &format!(
                        "Throughput {:.2} records/sec outside expected range [{:.2}, {:.2}] records/sec (expected {:.2} ± {:.1}%)",
                        throughput, min_allowed, max_allowed, expected_rps, config.tolerance_percent
                    ),
                    &format!("{:.2} ± {:.1}% records/sec", expected_rps, config.tolerance_percent),
                    &format!("{:.2} records/sec", throughput),
                )
                .with_detail("constraint", "expected_records_per_second")
                .with_detail("expected", &expected_rps.to_string())
                .with_detail("tolerance_percent", &config.tolerance_percent.to_string())
                .with_detail("record_count", &record_count.to_string())
                .with_detail("execution_time_ms", &execution_time_ms.to_string());
            }
        }

        // All constraints passed
        let msg = match (
            config.min_records_per_second,
            config.max_records_per_second,
            config.expected_records_per_second,
        ) {
            (Some(min), Some(max), _) => {
                format!(
                    "Throughput {:.2} records/sec within bounds [{:.2}, {:.2}]",
                    throughput, min, max
                )
            }
            (Some(min), None, _) => {
                format!("Throughput {:.2} records/sec >= {:.2}", throughput, min)
            }
            (None, Some(max), _) => {
                format!("Throughput {:.2} records/sec <= {:.2}", throughput, max)
            }
            (None, None, Some(expected)) => {
                format!(
                    "Throughput {:.2} records/sec within {:.1}% of expected {:.2}",
                    throughput, config.tolerance_percent, expected
                )
            }
            (None, None, None) => {
                format!("Throughput: {:.2} records/sec (no constraints)", throughput)
            }
        };

        AssertionResult::pass("throughput", &msg)
            .with_detail("throughput_rps", &format!("{:.2}", throughput))
            .with_detail("record_count", &record_count.to_string())
            .with_detail("execution_time_ms", &execution_time_ms.to_string())
    }

    // ==================== File Assertion Methods ====================

    /// Assert file exists and optionally check size
    fn assert_file_exists(&self, config: &FileExistsAssertion) -> AssertionResult {
        let path = self.resolve_file_path(&config.path);

        if !path.exists() {
            return AssertionResult::fail(
                "file_exists",
                &format!("File does not exist: {}", config.path),
                "file exists",
                "file not found",
            );
        }

        // Check file size if specified
        if let Ok(metadata) = std::fs::metadata(path) {
            let size = metadata.len();

            if let Some(min_size) = config.min_size_bytes {
                if size < min_size {
                    return AssertionResult::fail(
                        "file_exists",
                        &format!(
                            "File size {} bytes is below minimum {} bytes",
                            size, min_size
                        ),
                        &format!(">= {} bytes", min_size),
                        &format!("{} bytes", size),
                    );
                }
            }

            if let Some(max_size) = config.max_size_bytes {
                if size > max_size {
                    return AssertionResult::fail(
                        "file_exists",
                        &format!(
                            "File size {} bytes exceeds maximum {} bytes",
                            size, max_size
                        ),
                        &format!("<= {} bytes", max_size),
                        &format!("{} bytes", size),
                    );
                }
            }

            AssertionResult::pass(
                "file_exists",
                &format!("File exists: {} ({} bytes)", config.path, size),
            )
            .with_detail("path", &config.path)
            .with_detail("size_bytes", &size.to_string())
        } else {
            AssertionResult::pass("file_exists", &format!("File exists: {}", config.path))
                .with_detail("path", &config.path)
        }
    }

    /// Assert file row count
    fn assert_file_row_count(&self, config: &FileRowCountAssertion) -> AssertionResult {
        let path = self.resolve_file_path(&config.path);

        if !path.exists() {
            return AssertionResult::fail(
                "file_row_count",
                &format!("File does not exist: {}", config.path),
                "file exists",
                "file not found",
            );
        }

        // Load and count records
        let records = match FileSourceFactory::load_records(&path, &config.format) {
            Ok(r) => r,
            Err(e) => {
                return AssertionResult::fail(
                    "file_row_count",
                    &format!("Failed to load file: {}", e),
                    "valid file",
                    &e.to_string(),
                );
            }
        };

        let actual = records.len();

        // Check exact equals
        if let Some(expected) = config.equals {
            if actual == expected {
                return AssertionResult::pass(
                    "file_row_count",
                    &format!("Row count matches: {}", actual),
                )
                .with_detail("path", &config.path);
            }
            return AssertionResult::fail(
                "file_row_count",
                "Row count mismatch",
                &expected.to_string(),
                &actual.to_string(),
            )
            .with_detail("path", &config.path);
        }

        // Check greater_than
        if let Some(min) = config.greater_than {
            if actual <= min {
                return AssertionResult::fail(
                    "file_row_count",
                    &format!("Expected > {} rows", min),
                    &format!("> {}", min),
                    &actual.to_string(),
                )
                .with_detail("path", &config.path);
            }
        }

        // Check less_than
        if let Some(max) = config.less_than {
            if actual >= max {
                return AssertionResult::fail(
                    "file_row_count",
                    &format!("Expected < {} rows", max),
                    &format!("< {}", max),
                    &actual.to_string(),
                )
                .with_detail("path", &config.path);
            }
        }

        AssertionResult::pass("file_row_count", &format!("Row count: {}", actual))
            .with_detail("path", &config.path)
            .with_detail("row_count", &actual.to_string())
    }

    /// Assert file contains specific values
    fn assert_file_contains(&self, config: &FileContainsAssertion) -> AssertionResult {
        let path = self.resolve_file_path(&config.path);

        if !path.exists() {
            return AssertionResult::fail(
                "file_contains",
                &format!("File does not exist: {}", config.path),
                "file exists",
                "file not found",
            );
        }

        // Load records
        let records = match FileSourceFactory::load_records(&path, &config.format) {
            Ok(r) => r,
            Err(e) => {
                return AssertionResult::fail(
                    "file_contains",
                    &format!("Failed to load file: {}", e),
                    "valid file",
                    &e.to_string(),
                );
            }
        };

        // Extract field values from records
        let actual_values: std::collections::HashSet<String> = records
            .iter()
            .filter_map(|r| r.fields.get(&config.field))
            .map(field_value_to_string)
            .collect();

        let expected_set: std::collections::HashSet<&String> =
            config.expected_values.iter().collect();

        match config.mode {
            ContainsMode::All => {
                let missing: Vec<&String> = expected_set
                    .iter()
                    .filter(|v| !actual_values.contains(**v))
                    .copied()
                    .collect();

                if missing.is_empty() {
                    AssertionResult::pass(
                        "file_contains",
                        &format!("All expected values found in field '{}'", config.field),
                    )
                    .with_detail("path", &config.path)
                    .with_detail("field", &config.field)
                } else {
                    AssertionResult::fail(
                        "file_contains",
                        &format!("Missing values in field '{}'", config.field),
                        &format!("all of: {}", config.expected_values.join(", ")),
                        &format!(
                            "missing: {}",
                            missing
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    )
                    .with_detail("path", &config.path)
                    .with_detail("field", &config.field)
                }
            }
            ContainsMode::Any => {
                let found = expected_set.iter().any(|v| actual_values.contains(*v));

                if found {
                    AssertionResult::pass(
                        "file_contains",
                        &format!(
                            "At least one expected value found in field '{}'",
                            config.field
                        ),
                    )
                    .with_detail("path", &config.path)
                    .with_detail("field", &config.field)
                } else {
                    AssertionResult::fail(
                        "file_contains",
                        &format!("No expected values found in field '{}'", config.field),
                        &format!("any of: {}", config.expected_values.join(", ")),
                        "none found",
                    )
                    .with_detail("path", &config.path)
                    .with_detail("field", &config.field)
                }
            }
        }
    }

    /// Assert file matches expected content
    fn assert_file_matches(&self, config: &FileMatchesAssertion) -> AssertionResult {
        let actual_path = self.resolve_file_path(&config.actual_path);
        let expected_path = self.resolve_file_path(&config.expected_path);

        // Check both files exist
        if !actual_path.exists() {
            return AssertionResult::fail(
                "file_matches",
                &format!("Actual file does not exist: {}", config.actual_path),
                "file exists",
                "file not found",
            );
        }
        if !expected_path.exists() {
            return AssertionResult::fail(
                "file_matches",
                &format!("Expected file does not exist: {}", config.expected_path),
                "file exists",
                "file not found",
            );
        }

        // Load records from both files
        let actual_records = match FileSourceFactory::load_records(&actual_path, &config.format) {
            Ok(r) => r,
            Err(e) => {
                return AssertionResult::fail(
                    "file_matches",
                    &format!("Failed to load actual file: {}", e),
                    "valid file",
                    &e.to_string(),
                );
            }
        };

        let expected_records = match FileSourceFactory::load_records(&expected_path, &config.format)
        {
            Ok(r) => r,
            Err(e) => {
                return AssertionResult::fail(
                    "file_matches",
                    &format!("Failed to load expected file: {}", e),
                    "valid file",
                    &e.to_string(),
                );
            }
        };

        // Check row counts
        if actual_records.len() != expected_records.len() {
            return AssertionResult::fail(
                "file_matches",
                "Row count mismatch",
                &format!("{} rows", expected_records.len()),
                &format!("{} rows", actual_records.len()),
            )
            .with_detail("actual_path", &config.actual_path)
            .with_detail("expected_path", &config.expected_path);
        }

        // Determine fields to compare
        let fields_to_compare: Vec<&String> = if config.compare_fields.is_empty() {
            // Compare all fields except ignored
            let all_fields: std::collections::HashSet<&String> = expected_records
                .iter()
                .flat_map(|r| r.fields.keys())
                .collect();
            let ignore_set: std::collections::HashSet<&String> =
                config.ignore_fields.iter().collect();
            all_fields
                .into_iter()
                .filter(|f| !ignore_set.contains(f))
                .collect()
        } else {
            config.compare_fields.iter().collect()
        };

        // Compare records
        let tolerance = config.numeric_tolerance.unwrap_or(0.0001);

        if config.ignore_order {
            // Sort both sets by converting to strings for comparison
            let actual_set: std::collections::HashSet<String> = actual_records
                .iter()
                .map(|r| record_to_compare_string(r, &fields_to_compare))
                .collect();
            let expected_set: std::collections::HashSet<String> = expected_records
                .iter()
                .map(|r| record_to_compare_string(r, &fields_to_compare))
                .collect();

            if actual_set != expected_set {
                let missing: Vec<&String> = expected_set.difference(&actual_set).collect();
                let extra: Vec<&String> = actual_set.difference(&expected_set).collect();
                return AssertionResult::fail(
                    "file_matches",
                    "Record content mismatch",
                    &format!("{} records", expected_records.len()),
                    &format!("missing: {}, extra: {}", missing.len(), extra.len()),
                )
                .with_detail("actual_path", &config.actual_path)
                .with_detail("expected_path", &config.expected_path);
            }
        } else {
            // Compare in order
            for (i, (actual, expected)) in actual_records
                .iter()
                .zip(expected_records.iter())
                .enumerate()
            {
                for field in &fields_to_compare {
                    let actual_val = actual.fields.get(*field);
                    let expected_val = expected.fields.get(*field);

                    if !values_equal(actual_val, expected_val, tolerance) {
                        return AssertionResult::fail(
                            "file_matches",
                            &format!("Mismatch at row {} field '{}'", i, field),
                            &format!("{:?}", expected_val),
                            &format!("{:?}", actual_val),
                        )
                        .with_detail("actual_path", &config.actual_path)
                        .with_detail("expected_path", &config.expected_path)
                        .with_detail("row", &i.to_string());
                    }
                }
            }
        }

        AssertionResult::pass(
            "file_matches",
            &format!("Files match ({} rows)", actual_records.len()),
        )
        .with_detail("actual_path", &config.actual_path)
        .with_detail("expected_path", &config.expected_path)
        .with_detail("row_count", &actual_records.len().to_string())
    }

    // =========================================================================
    // Temporal & Statistical Assertions (P1.3)
    // =========================================================================

    /// Assert window boundary - verifies records fall in correct time windows
    fn assert_window_boundary(
        &self,
        output: &CapturedOutput,
        config: &WindowBoundaryAssertion,
    ) -> AssertionResult {
        let location = output.location();

        if output.records.is_empty() {
            return AssertionResult::pass(
                "window_boundary",
                &format!("No records to check (from {})", location),
            );
        }

        let mut violations = Vec::new();

        for (idx, record) in output.records.iter().enumerate() {
            // Get timestamp value
            let ts_value = match record.fields.get(&config.timestamp_field) {
                Some(v) => v,
                None => continue,
            };

            let ts_ms = self.extract_timestamp_ms(ts_value);
            if ts_ms.is_none() {
                continue;
            }
            let ts_ms = ts_ms.unwrap();

            // Check window containment if window start/end fields exist
            if config.verify_containment {
                if let (Some(start_field), Some(end_field)) =
                    (&config.window_start_field, &config.window_end_field)
                {
                    let window_start = record
                        .fields
                        .get(start_field)
                        .and_then(|v| self.extract_timestamp_ms(v));
                    let window_end = record
                        .fields
                        .get(end_field)
                        .and_then(|v| self.extract_timestamp_ms(v));

                    if let (Some(start), Some(end)) = (window_start, window_end) {
                        let tolerance = config.tolerance_ms as i64;
                        if ts_ms < start - tolerance || ts_ms > end + tolerance {
                            violations.push(format!(
                                "Record {}: timestamp {} outside window [{}, {}]",
                                idx, ts_ms, start, end
                            ));
                        }
                    }
                }
            }

            // Check window size alignment
            if config.verify_alignment {
                if let Some(window_size) = config.window_size_ms {
                    if let Some(start_field) = &config.window_start_field {
                        if let Some(window_start) = record
                            .fields
                            .get(start_field)
                            .and_then(|v| self.extract_timestamp_ms(v))
                        {
                            // Window start should be aligned to window size
                            if window_start % (window_size as i64) != 0 {
                                violations.push(format!(
                                    "Record {}: window start {} not aligned to window size {}",
                                    idx, window_start, window_size
                                ));
                            }
                        }
                    }
                }
            }
        }

        if !violations.is_empty() {
            return AssertionResult::fail(
                "window_boundary",
                &format!(
                    "Found {} window boundary violations (from {})",
                    violations.len(),
                    location
                ),
                "0 violations",
                &format!("{} violations", violations.len()),
            )
            .with_detail(
                "sample_violations",
                &violations
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; "),
            );
        }

        AssertionResult::pass(
            "window_boundary",
            &format!(
                "All records within window boundaries ({} records) from {}",
                output.records.len(),
                location
            ),
        )
    }

    /// Assert latency - verifies processing latency bounds
    fn assert_latency(
        &self,
        output: &CapturedOutput,
        config: &LatencyAssertion,
    ) -> AssertionResult {
        let location = output.location();

        // Extract latencies from records
        let latencies: Vec<f64> = output
            .records
            .iter()
            .filter_map(|r| {
                r.fields
                    .get(&config.timestamp_field)
                    .and_then(|v| self.extract_timestamp_ms(v))
            })
            .map(|ts| {
                // Calculate latency from event time to capture time
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                (now - ts).max(0) as f64
            })
            .collect();

        if latencies.len() < config.min_records {
            return AssertionResult::pass(
                "latency",
                &format!(
                    "Insufficient records for latency check ({} < {}) from {}",
                    latencies.len(),
                    config.min_records,
                    location
                ),
            );
        }

        let mut sorted_latencies = latencies.clone();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let max_latency = sorted_latencies.last().copied().unwrap_or(0.0);
        let avg_latency = sorted_latencies.iter().sum::<f64>() / sorted_latencies.len() as f64;

        // Check max latency
        if let Some(max_allowed) = config.max_latency_ms {
            if max_latency > max_allowed as f64 {
                return AssertionResult::fail(
                    "latency",
                    &format!("Max latency exceeded (from {})", location),
                    &format!("<= {} ms", max_allowed),
                    &format!("{:.2} ms", max_latency),
                );
            }
        }

        // Check average latency
        if let Some(avg_allowed) = config.avg_latency_ms {
            if avg_latency > avg_allowed as f64 {
                return AssertionResult::fail(
                    "latency",
                    &format!("Average latency exceeded (from {})", location),
                    &format!("<= {} ms", avg_allowed),
                    &format!("{:.2} ms", avg_latency),
                );
            }
        }

        // Check percentiles
        let check_percentile =
            |percentile: f64, threshold: Option<u64>, name: &str| -> Option<AssertionResult> {
                if let Some(max) = threshold {
                    let idx = ((percentile / 100.0) * sorted_latencies.len() as f64) as usize;
                    let idx = idx.min(sorted_latencies.len().saturating_sub(1));
                    let value = sorted_latencies[idx];
                    if value > max as f64 {
                        return Some(AssertionResult::fail(
                            "latency",
                            &format!("{} latency exceeded (from {})", name, location),
                            &format!("<= {} ms", max),
                            &format!("{:.2} ms", value),
                        ));
                    }
                }
                None
            };

        if let Some(result) = check_percentile(99.0, config.p99_latency_ms, "P99") {
            return result;
        }
        if let Some(result) = check_percentile(95.0, config.p95_latency_ms, "P95") {
            return result;
        }
        if let Some(result) = check_percentile(50.0, config.p50_latency_ms, "P50") {
            return result;
        }

        AssertionResult::pass(
            "latency",
            &format!(
                "Latency within bounds (avg: {:.2}ms, max: {:.2}ms, {} records) from {}",
                avg_latency,
                max_latency,
                latencies.len(),
                location
            ),
        )
    }

    /// Assert distribution - verifies output matches expected statistical distribution
    fn assert_distribution(
        &self,
        output: &CapturedOutput,
        config: &DistributionAssertion,
    ) -> AssertionResult {
        let location = output.location();

        // Extract numeric values
        let values: Vec<f64> = output
            .records
            .iter()
            .filter_map(|r| r.fields.get(&config.field))
            .filter_map(|v| self.field_value_to_f64(v))
            .collect();

        if values.len() < config.min_records {
            return AssertionResult::pass(
                "distribution",
                &format!(
                    "Insufficient records for distribution check ({} < {}) from {}",
                    values.len(),
                    config.min_records,
                    location
                ),
            );
        }

        // Calculate statistics
        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let std_dev = variance.sqrt();

        match config.distribution_type {
            DistributionType::Normal => {
                // Check mean
                if let Some(expected_mean) = config.mean {
                    let tolerance = expected_mean.abs() * config.tolerance + 1e-6;
                    if (mean - expected_mean).abs() > tolerance {
                        return AssertionResult::fail(
                            "distribution",
                            &format!("Mean outside expected range (from {})", location),
                            &format!("{:.4} ± {:.4}", expected_mean, tolerance),
                            &format!("{:.4}", mean),
                        );
                    }
                }

                // Check standard deviation
                if let Some(expected_std) = config.std_dev {
                    let tolerance = expected_std.abs() * config.tolerance + 1e-6;
                    if (std_dev - expected_std).abs() > tolerance {
                        return AssertionResult::fail(
                            "distribution",
                            &format!("Std dev outside expected range (from {})", location),
                            &format!("{:.4} ± {:.4}", expected_std, tolerance),
                            &format!("{:.4}", std_dev),
                        );
                    }
                }
            }
            DistributionType::Uniform => {
                // Check min/max bounds
                let actual_min = values.iter().cloned().fold(f64::INFINITY, f64::min);
                let actual_max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

                if let Some(expected_min) = config.min_value {
                    if actual_min < expected_min * (1.0 - config.tolerance) {
                        return AssertionResult::fail(
                            "distribution",
                            &format!("Values below expected minimum (from {})", location),
                            &format!(">= {:.4}", expected_min),
                            &format!("{:.4}", actual_min),
                        );
                    }
                }

                if let Some(expected_max) = config.max_value {
                    if actual_max > expected_max * (1.0 + config.tolerance) {
                        return AssertionResult::fail(
                            "distribution",
                            &format!("Values above expected maximum (from {})", location),
                            &format!("<= {:.4}", expected_max),
                            &format!("{:.4}", actual_max),
                        );
                    }
                }
            }
            DistributionType::Exponential | DistributionType::Custom => {
                // Basic sanity check - values should be non-negative for exponential
                if matches!(config.distribution_type, DistributionType::Exponential) {
                    let negative_count = values.iter().filter(|&&v| v < 0.0).count();
                    if negative_count > 0 {
                        return AssertionResult::fail(
                            "distribution",
                            &format!(
                                "Exponential distribution has {} negative values (from {})",
                                negative_count, location
                            ),
                            "0 negative values",
                            &format!("{} negative values", negative_count),
                        );
                    }
                }
            }
        }

        AssertionResult::pass(
            "distribution",
            &format!(
                "Distribution matches (mean: {:.4}, std: {:.4}, {} records) from {}",
                mean,
                std_dev,
                values.len(),
                location
            ),
        )
    }

    /// Assert percentile - verifies field values against percentile thresholds
    fn assert_percentile(
        &self,
        output: &CapturedOutput,
        config: &PercentileAssertion,
    ) -> AssertionResult {
        let location = output.location();

        // Extract numeric values
        let mut values: Vec<f64> = output
            .records
            .iter()
            .filter_map(|r| r.fields.get(&config.field))
            .filter_map(|v| self.field_value_to_f64(v))
            .collect();

        if values.len() < config.min_records {
            return AssertionResult::pass(
                "percentile",
                &format!(
                    "Insufficient records for percentile check ({} < {}) from {}",
                    values.len(),
                    config.min_records,
                    location
                ),
            );
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let calc_percentile = |p: f64| -> f64 {
            let idx = ((p / 100.0) * values.len() as f64) as usize;
            let idx = idx.min(values.len().saturating_sub(1));
            values[idx]
        };

        let check = |p: f64, threshold: Option<f64>, name: &str| -> Option<AssertionResult> {
            if let Some(limit) = threshold {
                let value = calc_percentile(p);
                let pass = match config.mode {
                    PercentileMode::LessThan => value <= limit,
                    PercentileMode::GreaterThan => value >= limit,
                };
                if !pass {
                    let op = match config.mode {
                        PercentileMode::LessThan => "<=",
                        PercentileMode::GreaterThan => ">=",
                    };
                    return Some(AssertionResult::fail(
                        "percentile",
                        &format!("{} check failed (from {})", name, location),
                        &format!("{} {:.4}", op, limit),
                        &format!("{:.4}", value),
                    ));
                }
            }
            None
        };

        if let Some(result) = check(50.0, config.p50, "P50") {
            return result;
        }
        if let Some(result) = check(75.0, config.p75, "P75") {
            return result;
        }
        if let Some(result) = check(90.0, config.p90, "P90") {
            return result;
        }
        if let Some(result) = check(95.0, config.p95, "P95") {
            return result;
        }
        if let Some(result) = check(99.0, config.p99, "P99") {
            return result;
        }
        if let Some(result) = check(99.9, config.p999, "P99.9") {
            return result;
        }

        AssertionResult::pass(
            "percentile",
            &format!(
                "All percentile thresholds met ({} records) from {}",
                values.len(),
                location
            ),
        )
    }

    /// Assert event ordering - verifies ordering within time windows
    fn assert_event_ordering(
        &self,
        output: &CapturedOutput,
        config: &EventOrderingAssertion,
    ) -> AssertionResult {
        let location = output.location();

        if output.records.is_empty() {
            return AssertionResult::pass(
                "event_ordering",
                &format!("No records to check (from {})", location),
            );
        }

        // Group records by partition if partition field specified
        let groups: Vec<Vec<(usize, i64)>> =
            if let Some(ref partition_field) = config.partition_field {
                let mut partition_map: HashMap<String, Vec<(usize, i64)>> = HashMap::new();
                for (idx, record) in output.records.iter().enumerate() {
                    let partition_key = record
                        .fields
                        .get(partition_field)
                        .map(|v| field_value_to_string(v))
                        .unwrap_or_default();
                    let ts = record
                        .fields
                        .get(&config.timestamp_field)
                        .and_then(|v| self.extract_timestamp_ms(v))
                        .unwrap_or(0);
                    partition_map
                        .entry(partition_key)
                        .or_default()
                        .push((idx, ts));
                }
                partition_map.into_values().collect()
            } else {
                let timestamps: Vec<(usize, i64)> = output
                    .records
                    .iter()
                    .enumerate()
                    .map(|(idx, r)| {
                        let ts = r
                            .fields
                            .get(&config.timestamp_field)
                            .and_then(|v| self.extract_timestamp_ms(v))
                            .unwrap_or(0);
                        (idx, ts)
                    })
                    .collect();
                vec![timestamps]
            };

        let mut out_of_order_count = 0;
        let mut violations = Vec::new();
        let total_records = output.records.len();

        for group in groups {
            for window in group.windows(2) {
                let (idx1, ts1) = window[0];
                let (idx2, ts2) = window[1];

                let is_ordered = match config.direction {
                    OrderDirection::Ascending => ts2 >= ts1,
                    OrderDirection::Descending => ts2 <= ts1,
                };

                if !is_ordered {
                    out_of_order_count += 1;
                    if violations.len() < 5 {
                        violations.push(format!(
                            "Records {} ({}) and {} ({}) out of order",
                            idx1, ts1, idx2, ts2
                        ));
                    }
                }

                // Check max gap
                if let Some(max_gap) = config.max_gap_ms {
                    let gap = (ts2 - ts1).unsigned_abs();
                    if gap > max_gap {
                        if violations.len() < 5 {
                            violations.push(format!(
                                "Gap of {}ms between records {} and {} exceeds max {}ms",
                                gap, idx1, idx2, max_gap
                            ));
                        }
                    }
                }
            }
        }

        // Check out-of-order percentage
        if let Some(max_percent) = config.max_out_of_order_percent {
            let actual_percent = (out_of_order_count as f64 / total_records as f64) * 100.0;
            if actual_percent > max_percent {
                return AssertionResult::fail(
                    "event_ordering",
                    &format!("Too many out-of-order records (from {})", location),
                    &format!("<= {:.2}%", max_percent),
                    &format!("{:.2}% ({} records)", actual_percent, out_of_order_count),
                );
            }
        } else if out_of_order_count > 0 && !config.allow_gaps {
            return AssertionResult::fail(
                "event_ordering",
                &format!("Found out-of-order records (from {})", location),
                "0 out-of-order",
                &format!("{} out-of-order", out_of_order_count),
            )
            .with_detail("sample_violations", &violations.join("; "));
        }

        AssertionResult::pass(
            "event_ordering",
            &format!(
                "Event ordering verified ({} records, {} out-of-order) from {}",
                total_records, out_of_order_count, location
            ),
        )
    }

    /// Helper: Extract timestamp in milliseconds from FieldValue
    fn extract_timestamp_ms(&self, value: &FieldValue) -> Option<i64> {
        match value {
            FieldValue::Integer(i) => Some(*i),
            FieldValue::Float(f) => Some(*f as i64),
            FieldValue::Timestamp(dt) => Some(dt.and_utc().timestamp_millis()),
            FieldValue::String(s) => {
                // Try parsing ISO 8601 or unix timestamp
                s.parse::<i64>().ok().or_else(|| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.timestamp_millis())
                })
            }
            _ => None,
        }
    }

    /// Helper: Convert FieldValue to f64
    fn field_value_to_f64(&self, value: &FieldValue) -> Option<f64> {
        match value {
            FieldValue::Integer(i) => Some(*i as f64),
            FieldValue::Float(f) => Some(*f),
            FieldValue::ScaledInteger(v, scale) => {
                let divisor = 10_i64.pow(*scale as u32) as f64;
                Some(*v as f64 / divisor)
            }
            FieldValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Evaluate a template expression
    fn evaluate_template_expression(
        &self,
        expression: &str,
        output: &CapturedOutput,
    ) -> Result<TemplateResult, String> {
        let expression = expression.trim();

        // Handle {{ expression }} syntax
        let expr = if expression.starts_with("{{") && expression.ends_with("}}") {
            expression[2..expression.len() - 2].trim()
        } else {
            expression
        };

        // Create evaluation context
        let ctx = TemplateContext {
            records: &output.records,
            context: &self.context,
        };

        // Parse and evaluate the expression
        self.eval_expr(expr, &ctx)
    }

    /// Evaluate an expression in the template context
    fn eval_expr(&self, expr: &str, ctx: &TemplateContext) -> Result<TemplateResult, String> {
        let expr = expr.trim();

        // Check for comparison operators (in order of precedence)
        for (op, op_fn) in [
            (
                "==",
                compare_eq as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "!=",
                compare_neq as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                ">=",
                compare_gte as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "<=",
                compare_lte as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                ">",
                compare_gt as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "<",
                compare_lt as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
        ] {
            if let Some(pos) = find_top_level_operator(expr, op) {
                let left = self.eval_expr(&expr[..pos], ctx)?;
                let right = self.eval_expr(&expr[pos + op.len()..], ctx)?;

                let left_val = result_to_value(left)?;
                let right_val = result_to_value(right)?;

                return Ok(TemplateResult::Boolean(op_fn(&left_val, &right_val)));
            }
        }

        // Check for logical operators
        if let Some(pos) = find_top_level_operator(expr, " and ") {
            let left = self.eval_expr(&expr[..pos], ctx)?;
            let right = self.eval_expr(&expr[pos + 5..], ctx)?;
            return Ok(TemplateResult::Boolean(
                result_to_bool(left)? && result_to_bool(right)?,
            ));
        }

        if let Some(pos) = find_top_level_operator(expr, " or ") {
            let left = self.eval_expr(&expr[..pos], ctx)?;
            let right = self.eval_expr(&expr[pos + 4..], ctx)?;
            return Ok(TemplateResult::Boolean(
                result_to_bool(left)? || result_to_bool(right)?,
            ));
        }

        // Check for arithmetic operators
        for (op, op_fn) in [
            ("+", arith_add as fn(f64, f64) -> f64),
            ("-", arith_sub as fn(f64, f64) -> f64),
            ("*", arith_mul as fn(f64, f64) -> f64),
            ("/", arith_div as fn(f64, f64) -> f64),
        ] {
            if let Some(pos) = find_top_level_operator(expr, op) {
                let left = self.eval_expr(&expr[..pos], ctx)?;
                let right = self.eval_expr(&expr[pos + op.len()..], ctx)?;

                let left_num = result_to_f64(left)?;
                let right_num = result_to_f64(right)?;

                return Ok(TemplateResult::Value(TemplateValue::Number(op_fn(
                    left_num, right_num,
                ))));
            }
        }

        // Handle function calls
        if let Some(result) = self.try_eval_function(expr, ctx)? {
            return Ok(result);
        }

        // Handle property access (records.length, records[0].field)
        if let Some(result) = self.try_eval_property(expr, ctx)? {
            return Ok(result);
        }

        // Handle literals
        if let Some(result) = try_parse_literal(expr) {
            return Ok(TemplateResult::Value(result));
        }

        // Unknown expression
        Err(format!("Unknown expression: {}", expr))
    }

    /// Try to evaluate a function call
    fn try_eval_function(
        &self,
        expr: &str,
        ctx: &TemplateContext,
    ) -> Result<Option<TemplateResult>, String> {
        // all(condition for item in items)
        if expr.starts_with("all(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_all_any(inner, ctx, true)?));
        }

        // any(condition for item in items)
        if expr.starts_with("any(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_all_any(inner, ctx, false)?));
        }

        // sum(expr for item in items) or sum(field)
        if expr.starts_with("sum(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_aggregate(inner, ctx, AggType::Sum)?));
        }

        // count(items) or count(condition for item in items)
        if expr.starts_with("count(") && expr.ends_with(')') {
            let inner = &expr[6..expr.len() - 1];
            return Ok(Some(self.eval_aggregate(inner, ctx, AggType::Count)?));
        }

        // avg(expr for item in items) or avg(field)
        if expr.starts_with("avg(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_aggregate(inner, ctx, AggType::Avg)?));
        }

        // min(expr for item in items) or min(field)
        if expr.starts_with("min(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_aggregate(inner, ctx, AggType::Min)?));
        }

        // max(expr for item in items) or max(field)
        if expr.starts_with("max(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1];
            return Ok(Some(self.eval_aggregate(inner, ctx, AggType::Max)?));
        }

        // len(items)
        if expr.starts_with("len(") && expr.ends_with(')') {
            let inner = &expr[4..expr.len() - 1].trim();
            if *inner == "records" {
                return Ok(Some(TemplateResult::Value(TemplateValue::Number(
                    ctx.records.len() as f64,
                ))));
            }
        }

        Ok(None)
    }

    /// Evaluate all() or any() function
    fn eval_all_any(
        &self,
        inner: &str,
        ctx: &TemplateContext,
        is_all: bool,
    ) -> Result<TemplateResult, String> {
        // Parse "condition for item in items"
        if let Some(for_pos) = inner.find(" for ") {
            let condition = inner[..for_pos].trim();
            let rest = &inner[for_pos + 5..];

            if let Some(in_pos) = rest.find(" in ") {
                let item_name = rest[..in_pos].trim();
                let items_name = rest[in_pos + 4..].trim();

                // Only support "records" as items for now
                if items_name != "records" {
                    return Err(format!("Unknown collection: {}", items_name));
                }

                // Evaluate condition for each record
                for record in ctx.records {
                    let record_ctx = RecordContext {
                        item_name,
                        record,
                        parent: ctx,
                    };

                    let result = self.eval_record_expr(condition, &record_ctx)?;

                    if is_all {
                        if !result_to_bool(result)? {
                            return Ok(TemplateResult::Boolean(false));
                        }
                    } else {
                        // any
                        if result_to_bool(result)? {
                            return Ok(TemplateResult::Boolean(true));
                        }
                    }
                }

                return Ok(TemplateResult::Boolean(is_all));
            }
        }

        Err(format!(
            "Invalid {} expression: {}",
            if is_all { "all" } else { "any" },
            inner
        ))
    }

    /// Evaluate aggregate function
    fn eval_aggregate(
        &self,
        inner: &str,
        ctx: &TemplateContext,
        agg_type: AggType,
    ) -> Result<TemplateResult, String> {
        // Check for "expr for item in items" syntax
        if let Some(for_pos) = inner.find(" for ") {
            let expr = inner[..for_pos].trim();
            let rest = &inner[for_pos + 5..];

            if let Some(in_pos) = rest.find(" in ") {
                let item_name = rest[..in_pos].trim();
                let items_name = rest[in_pos + 4..].trim();

                if items_name != "records" {
                    return Err(format!("Unknown collection: {}", items_name));
                }

                let mut values: Vec<f64> = Vec::new();

                for record in ctx.records {
                    let record_ctx = RecordContext {
                        item_name,
                        record,
                        parent: ctx,
                    };

                    let result = self.eval_record_expr(expr, &record_ctx)?;
                    if let Ok(num) = result_to_f64(result) {
                        values.push(num);
                    }
                }

                return Ok(TemplateResult::Value(compute_aggregate(&values, agg_type)));
            }
        }

        // Simple field name - aggregate over records.field
        let field = inner.trim();
        let mut values: Vec<f64> = Vec::new();

        for record in ctx.records {
            if let Some(value) = record.fields.get(field) {
                if let Some(num) = field_value_to_f64(value) {
                    values.push(num);
                }
            }
        }

        Ok(TemplateResult::Value(compute_aggregate(&values, agg_type)))
    }

    /// Evaluate expression in record context
    fn eval_record_expr(&self, expr: &str, ctx: &RecordContext) -> Result<TemplateResult, String> {
        let expr = expr.trim();

        // Check for comparison operators
        for (op, op_fn) in [
            (
                "==",
                compare_eq as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "!=",
                compare_neq as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                ">=",
                compare_gte as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "<=",
                compare_lte as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                ">",
                compare_gt as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
            (
                "<",
                compare_lt as fn(&TemplateValue, &TemplateValue) -> bool,
            ),
        ] {
            if let Some(pos) = find_top_level_operator(expr, op) {
                let left = self.eval_record_expr(&expr[..pos], ctx)?;
                let right = self.eval_record_expr(&expr[pos + op.len()..], ctx)?;

                let left_val = result_to_value(left)?;
                let right_val = result_to_value(right)?;

                return Ok(TemplateResult::Boolean(op_fn(&left_val, &right_val)));
            }
        }

        // Check for field access: item.field or item["field"]
        if expr.starts_with(ctx.item_name) {
            let rest = &expr[ctx.item_name.len()..];
            if rest.starts_with('.') {
                let field = &rest[1..];
                if let Some(value) = ctx.record.fields.get(field) {
                    return Ok(TemplateResult::Value(field_value_to_template_value(value)));
                }
                return Err(format!("Unknown field: {}", field));
            }
        }

        // Handle literals
        if let Some(result) = try_parse_literal(expr) {
            return Ok(TemplateResult::Value(result));
        }

        Err(format!("Unknown expression in record context: {}", expr))
    }

    /// Try to evaluate property access
    fn try_eval_property(
        &self,
        expr: &str,
        ctx: &TemplateContext,
    ) -> Result<Option<TemplateResult>, String> {
        // records.length
        if expr == "records.length" || expr == "records.len" {
            return Ok(Some(TemplateResult::Value(TemplateValue::Number(
                ctx.records.len() as f64,
            ))));
        }

        // records[n].field
        if expr.starts_with("records[") {
            if let Some(bracket_end) = expr.find(']') {
                let index_str = &expr[8..bracket_end];
                if let Ok(index) = index_str.parse::<usize>() {
                    if index < ctx.records.len() {
                        let rest = &expr[bracket_end + 1..];
                        if rest.starts_with('.') {
                            let field = &rest[1..];
                            if let Some(value) = ctx.records[index].fields.get(field) {
                                return Ok(Some(TemplateResult::Value(
                                    field_value_to_template_value(value),
                                )));
                            }
                            return Err(format!("Unknown field: {}", field));
                        }
                    } else {
                        return Err(format!("Index out of bounds: {}", index));
                    }
                }
            }
        }

        // Context variables: inputs.X.count, outputs.X.count
        if let Some(resolved) = ctx.context.resolve_template(&format!("{{{{{}}}}}", expr)) {
            if let Ok(num) = resolved.parse::<f64>() {
                return Ok(Some(TemplateResult::Value(TemplateValue::Number(num))));
            }
            return Ok(Some(TemplateResult::Value(TemplateValue::String(resolved))));
        }

        Ok(None)
    }

    // ==================== Streaming-Specific Assertions ====================

    /// Assert DLQ (Dead Letter Queue) count
    /// Note: DLQ records should be passed via the context or captured separately
    fn assert_dlq_count(
        &self,
        output: &CapturedOutput,
        config: &DlqCountAssertion,
    ) -> AssertionResult {
        let location = output.location();

        // DLQ count is tracked via warnings that contain "DLQ" or "error" messages
        // In a full implementation, this would integrate with DlqCapture
        let dlq_count = output
            .warnings
            .iter()
            .filter(|w| w.to_lowercase().contains("dlq") || w.to_lowercase().contains("error"))
            .count();

        // Check exact equals
        if let Some(expected) = config.equals {
            if dlq_count == expected {
                return AssertionResult::pass(
                    "dlq_count",
                    &format!("DLQ count matches: {} (from {})", dlq_count, location),
                );
            }
            return AssertionResult::fail(
                "dlq_count",
                &format!("DLQ count mismatch (from {})", location),
                &expected.to_string(),
                &dlq_count.to_string(),
            );
        }

        // Check max
        if let Some(max) = config.max {
            if dlq_count > max {
                return AssertionResult::fail(
                    "dlq_count",
                    &format!("DLQ count exceeds maximum (from {})", location),
                    &format!("<= {}", max),
                    &dlq_count.to_string(),
                );
            }
        }

        // Check min (for negative testing - expecting errors)
        if let Some(min) = config.min {
            if dlq_count < min {
                return AssertionResult::fail(
                    "dlq_count",
                    &format!("DLQ count below minimum (from {})", location),
                    &format!(">= {}", min),
                    &dlq_count.to_string(),
                );
            }
        }

        AssertionResult::pass(
            "dlq_count",
            &format!("DLQ count: {} (from {})", dlq_count, location),
        )
    }

    /// Assert error rate is within bounds
    fn assert_error_rate(
        &self,
        output: &CapturedOutput,
        config: &ErrorRateAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let total_records = output.records.len();

        // Need minimum records for meaningful rate calculation
        if total_records < config.min_records {
            return AssertionResult::pass(
                "error_rate",
                &format!(
                    "Skipped: only {} records (need {} for rate calculation) from {}",
                    total_records, config.min_records, location
                ),
            );
        }

        // Count errors from warnings
        let error_count = output
            .warnings
            .iter()
            .filter(|w| {
                let w_lower = w.to_lowercase();
                if config.error_types.is_empty() {
                    w_lower.contains("error") || w_lower.contains("failed")
                } else {
                    config
                        .error_types
                        .iter()
                        .any(|t| w_lower.contains(&t.to_lowercase()))
                }
            })
            .count();

        let error_rate = error_count as f64 / total_records as f64;
        let error_percent = error_rate * 100.0;

        // Check max_rate (0.0 to 1.0)
        if let Some(max_rate) = config.max_rate {
            if error_rate > max_rate {
                return AssertionResult::fail(
                    "error_rate",
                    &format!("Error rate exceeds maximum (from {})", location),
                    &format!("<= {:.2}%", max_rate * 100.0),
                    &format!("{:.2}%", error_percent),
                );
            }
        }

        // Check max_percent (0 to 100)
        if let Some(max_percent) = config.max_percent {
            if error_percent > max_percent {
                return AssertionResult::fail(
                    "error_rate",
                    &format!("Error percentage exceeds maximum (from {})", location),
                    &format!("<= {:.2}%", max_percent),
                    &format!("{:.2}%", error_percent),
                );
            }
        }

        AssertionResult::pass(
            "error_rate",
            &format!(
                "Error rate: {:.2}% ({}/{} records) from {}",
                error_percent, error_count, total_records, location
            ),
        )
    }

    /// Assert no duplicate records based on key fields
    fn assert_no_duplicates(
        &self,
        output: &CapturedOutput,
        config: &NoDuplicatesAssertion,
    ) -> AssertionResult {
        let location = output.location();

        if config.key_fields.is_empty() {
            return AssertionResult::fail(
                "no_duplicates",
                "No key fields specified for duplicate check",
                "key_fields to be specified",
                "empty key_fields",
            );
        }

        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut duplicates: Vec<String> = Vec::new();

        for record in &output.records {
            // Build composite key from key fields
            let key_parts: Vec<String> = config
                .key_fields
                .iter()
                .map(|f| {
                    record
                        .fields
                        .get(f)
                        .map(field_value_to_string)
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            let composite_key = key_parts.join("|");

            if !seen_keys.insert(composite_key.clone()) {
                if !config.allow_updates {
                    duplicates.push(composite_key);
                }
            }
        }

        let total_records = output.records.len();
        let duplicate_count = duplicates.len();
        let duplicate_percent = if total_records > 0 {
            (duplicate_count as f64 / total_records as f64) * 100.0
        } else {
            0.0
        };

        // Check max_duplicate_percent
        if let Some(max_percent) = config.max_duplicate_percent {
            if duplicate_percent > max_percent {
                return AssertionResult::fail(
                    "no_duplicates",
                    &format!("Duplicate percentage exceeds maximum (from {})", location),
                    &format!("<= {:.2}%", max_percent),
                    &format!("{:.2}% ({} duplicates)", duplicate_percent, duplicate_count),
                )
                .with_detail(
                    "sample_duplicates",
                    &duplicates
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
        } else if !duplicates.is_empty() {
            // No tolerance specified - any duplicate is a failure
            return AssertionResult::fail(
                "no_duplicates",
                &format!(
                    "Found {} duplicate records (from {})",
                    duplicate_count, location
                ),
                "0 duplicates",
                &format!("{} duplicates", duplicate_count),
            )
            .with_detail("key_fields", &config.key_fields.join(", "))
            .with_detail(
                "sample_duplicates",
                &duplicates
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        AssertionResult::pass(
            "no_duplicates",
            &format!(
                "No duplicates found in {} records (from {})",
                total_records, location
            ),
        )
        .with_detail("key_fields", &config.key_fields.join(", "))
    }

    /// Assert record ordering
    fn assert_ordering(
        &self,
        output: &CapturedOutput,
        config: &OrderingAssertion,
    ) -> AssertionResult {
        let location = output.location();

        if output.records.len() < 2 {
            return AssertionResult::pass(
                "ordering",
                &format!(
                    "Ordering check skipped: only {} records (from {})",
                    output.records.len(),
                    location
                ),
            );
        }

        // Group by partition if specified
        let groups: Vec<Vec<&StreamRecord>> = if let Some(ref partition_field) = config.partition_by
        {
            let mut partitions: HashMap<String, Vec<&StreamRecord>> = HashMap::new();
            for record in &output.records {
                let partition_key = record
                    .fields
                    .get(partition_field)
                    .map(field_value_to_string)
                    .unwrap_or_else(|| "NULL".to_string());
                partitions.entry(partition_key).or_default().push(record);
            }
            partitions.into_values().collect()
        } else {
            vec![output.records.iter().collect()]
        };

        // Check ordering within each group
        for group in groups {
            for window in group.windows(2) {
                let prev = window[0];
                let curr = window[1];

                let prev_val = prev.fields.get(&config.field);
                let curr_val = curr.fields.get(&config.field);

                let is_ordered = match (&config.direction, prev_val, curr_val) {
                    (OrderDirection::Ascending, Some(p), Some(c)) => {
                        let cmp = compare_field_values(p, c);
                        if config.allow_equal {
                            cmp <= 0
                        } else {
                            cmp < 0
                        }
                    }
                    (OrderDirection::Descending, Some(p), Some(c)) => {
                        let cmp = compare_field_values(p, c);
                        if config.allow_equal {
                            cmp >= 0
                        } else {
                            cmp > 0
                        }
                    }
                    _ => true, // Skip null comparisons
                };

                if !is_ordered {
                    return AssertionResult::fail(
                        "ordering",
                        &format!(
                            "Records not in {:?} order by '{}' (from {})",
                            config.direction, config.field, location
                        ),
                        &format!("{:?} order", config.direction),
                        &format!(
                            "{:?} followed by {:?}",
                            prev_val.map(field_value_to_string),
                            curr_val.map(field_value_to_string)
                        ),
                    );
                }
            }
        }

        AssertionResult::pass(
            "ordering",
            &format!(
                "Records correctly ordered by '{}' {:?} (from {})",
                config.field, config.direction, location
            ),
        )
    }

    /// Assert data completeness (no data loss between input and output)
    fn assert_completeness(
        &self,
        output: &CapturedOutput,
        config: &CompletenessAssertion,
    ) -> AssertionResult {
        let location = output.location();

        // Get input records from context
        let input_records = match self.context.input_records.get(&config.input_source) {
            Some(records) => records,
            None => {
                return AssertionResult::fail(
                    "completeness",
                    &format!(
                        "Input source '{}' not found in context",
                        config.input_source
                    ),
                    &config.input_source,
                    "not found",
                );
            }
        };

        // Build set of input keys
        let input_keys: HashSet<String> = input_records
            .iter()
            .map(|r| {
                r.get(&config.key_field)
                    .map(field_value_to_string)
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect();

        // Build set of output keys
        let output_keys: HashSet<String> = output
            .records
            .iter()
            .map(|r| {
                r.fields
                    .get(&config.key_field)
                    .map(field_value_to_string)
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect();

        // Calculate completeness
        let matched_keys: HashSet<_> = input_keys.intersection(&output_keys).collect();
        let completeness = if input_keys.is_empty() {
            1.0
        } else {
            matched_keys.len() as f64 / input_keys.len() as f64
        };

        if completeness < config.min_completeness {
            let missing: Vec<_> = input_keys
                .difference(&output_keys)
                .take(10)
                .cloned()
                .collect();
            return AssertionResult::fail(
                "completeness",
                &format!("Data completeness below threshold (from {})", location),
                &format!(">= {:.1}%", config.min_completeness * 100.0),
                &format!("{:.1}%", completeness * 100.0),
            )
            .with_detail("input_count", &input_keys.len().to_string())
            .with_detail("output_count", &output_keys.len().to_string())
            .with_detail("sample_missing", &missing.join(", "));
        }

        // Check required fields if specified
        if !config.required_fields.is_empty() {
            for record in &output.records {
                for field in &config.required_fields {
                    if !record.contains_key(field) {
                        return AssertionResult::fail(
                            "completeness",
                            &format!(
                                "Required field '{}' missing in some records (from {})",
                                field, location
                            ),
                            &format!("field '{}' present", field),
                            "field missing",
                        );
                    }
                }
            }
        }

        AssertionResult::pass(
            "completeness",
            &format!(
                "Data completeness: {:.1}% ({}/{} records) from {}",
                completeness * 100.0,
                matched_keys.len(),
                input_keys.len(),
                location
            ),
        )
    }

    /// Assert table freshness for CTAS materialized tables
    fn assert_table_freshness(
        &self,
        output: &CapturedOutput,
        config: &TableFreshnessAssertion,
    ) -> AssertionResult {
        let location = format!("table '{}'", config.table);

        // Check minimum records
        if let Some(min_records) = config.min_records {
            if output.records.len() < min_records {
                return AssertionResult::fail(
                    "table_freshness",
                    &format!("Table has fewer records than required ({})", location),
                    &format!(">= {} records", min_records),
                    &format!("{} records", output.records.len()),
                );
            }
        }

        // For freshness, we'd need timestamp fields in the records
        // Look for common timestamp field names
        let timestamp_fields = [
            "event_time",
            "timestamp",
            "created_at",
            "updated_at",
            "_timestamp",
        ];
        let mut latest_timestamp: Option<i64> = None;

        for record in &output.records {
            for ts_field in &timestamp_fields {
                if let Some(value) = record.fields.get(*ts_field) {
                    if let Some(ts) = field_value_to_timestamp_ms(value) {
                        latest_timestamp = Some(latest_timestamp.map_or(ts, |l| l.max(ts)));
                    }
                }
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Check max_age_ms
        if let Some(max_age_ms) = config.max_age_ms {
            if let Some(latest) = latest_timestamp {
                let age_ms = now_ms - latest;
                if age_ms > max_age_ms as i64 {
                    return AssertionResult::fail(
                        "table_freshness",
                        &format!("Table data too old ({})", location),
                        &format!("<= {}ms old", max_age_ms),
                        &format!("{}ms old", age_ms),
                    );
                }
            } else {
                return AssertionResult::pass(
                    "table_freshness",
                    &format!(
                        "No timestamp field found for freshness check ({})",
                        location
                    ),
                )
                .with_detail("warning", "Could not determine data freshness");
            }
        }

        // Check max_lag_ms (would need source timestamp comparison - simplified here)
        if let Some(max_lag_ms) = config.max_lag_ms {
            if let Some(latest) = latest_timestamp {
                let lag_ms = now_ms - latest;
                if lag_ms > max_lag_ms as i64 {
                    return AssertionResult::fail(
                        "table_freshness",
                        &format!("Table lag exceeds maximum ({})", location),
                        &format!("<= {}ms lag", max_lag_ms),
                        &format!("{}ms lag", lag_ms),
                    );
                }
            }
        }

        AssertionResult::pass(
            "table_freshness",
            &format!(
                "Table '{}' is fresh ({} records)",
                config.table,
                output.records.len()
            ),
        )
    }

    /// Assert comprehensive data quality rules
    fn assert_data_quality(
        &self,
        output: &CapturedOutput,
        config: &DataQualityAssertion,
    ) -> AssertionResult {
        let location = output.location();

        let mut violations: Vec<String> = Vec::new();

        // Check for nulls in specified fields
        for field in &config.no_nulls_in {
            for (i, record) in output.records.iter().enumerate() {
                match record.fields.get(field) {
                    None | Some(FieldValue::Null) => {
                        violations.push(format!("NULL in '{}' at row {}", field, i));
                    }
                    _ => {}
                }
            }
        }

        // Check for empty strings in specified fields
        for field in &config.no_empty_strings_in {
            for (i, record) in output.records.iter().enumerate() {
                if let Some(FieldValue::String(s)) = record.fields.get(field) {
                    if s.trim().is_empty() {
                        violations.push(format!("Empty string in '{}' at row {}", field, i));
                    }
                }
            }
        }

        // Check numeric ranges
        for range_check in &config.numeric_ranges {
            for (i, record) in output.records.iter().enumerate() {
                if let Some(value) = record.fields.get(&range_check.field) {
                    if let Some(num) = field_value_to_f64(value) {
                        if let Some(min) = range_check.min {
                            if num < min {
                                violations.push(format!(
                                    "'{}' value {} < {} at row {}",
                                    range_check.field, num, min, i
                                ));
                            }
                        }
                        if let Some(max) = range_check.max {
                            if num > max {
                                violations.push(format!(
                                    "'{}' value {} > {} at row {}",
                                    range_check.field, num, max, i
                                ));
                            }
                        }
                    }
                }
            }
        }

        // Check string patterns
        for pattern_check in &config.string_patterns {
            let regex = match regex::Regex::new(&pattern_check.pattern) {
                Ok(r) => r,
                Err(e) => {
                    violations.push(format!("Invalid regex '{}': {}", pattern_check.pattern, e));
                    continue;
                }
            };

            for (i, record) in output.records.iter().enumerate() {
                if let Some(value) = record.fields.get(&pattern_check.field) {
                    let value_str = field_value_to_string(value);
                    if !regex.is_match(&value_str) {
                        violations.push(format!(
                            "'{}' value '{}' doesn't match pattern '{}' at row {}",
                            pattern_check.field, value_str, pattern_check.pattern, i
                        ));
                    }
                }
            }
        }

        // Check referential integrity
        for ref_check in &config.referential_integrity {
            // Get reference values from context
            if let Some(ref_records) = self.context.input_records.get(&ref_check.reference_source) {
                let valid_values: HashSet<String> = ref_records
                    .iter()
                    .filter_map(|r| r.get(&ref_check.reference_field))
                    .map(field_value_to_string)
                    .collect();

                for (i, record) in output.records.iter().enumerate() {
                    if let Some(value) = record.fields.get(&ref_check.field) {
                        let value_str = field_value_to_string(value);
                        if !valid_values.contains(&value_str) {
                            violations.push(format!(
                                "'{}' value '{}' not found in {}.{} at row {}",
                                ref_check.field,
                                value_str,
                                ref_check.reference_source,
                                ref_check.reference_field,
                                i
                            ));
                        }
                    }
                }
            }
        }

        if !violations.is_empty() {
            let violation_count = violations.len();
            return AssertionResult::fail(
                "data_quality",
                &format!(
                    "Found {} data quality violations (from {})",
                    violation_count, location
                ),
                "0 violations",
                &format!("{} violations", violation_count),
            )
            .with_detail(
                "sample_violations",
                &violations
                    .iter()
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; "),
            );
        }

        AssertionResult::pass(
            "data_quality",
            &format!(
                "All data quality checks passed ({} records) from {}",
                output.records.len(),
                location
            ),
        )
    }
}

// ==================== Template Types ====================

/// Result of template evaluation
#[derive(Debug, Clone)]
enum TemplateResult {
    Boolean(bool),
    Value(TemplateValue),
    Error(String),
}

/// Template value types
#[derive(Debug, Clone)]
enum TemplateValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

/// Aggregate type
#[derive(Debug, Clone, Copy)]
enum AggType {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// Template evaluation context
struct TemplateContext<'a> {
    records: &'a [StreamRecord],
    context: &'a AssertionContext,
}

/// Record iteration context
struct RecordContext<'a> {
    item_name: &'a str,
    record: &'a StreamRecord,
    parent: &'a TemplateContext<'a>,
}

// ==================== Template Helper Functions ====================

/// Find operator position at top level (not inside parentheses)
fn find_top_level_operator(expr: &str, op: &str) -> Option<usize> {
    let mut depth = 0;
    let bytes = expr.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            _ if depth == 0 && i + op_bytes.len() <= bytes.len() => {
                if &bytes[i..i + op_bytes.len()] == op_bytes {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Convert TemplateResult to bool
fn result_to_bool(result: TemplateResult) -> Result<bool, String> {
    match result {
        TemplateResult::Boolean(b) => Ok(b),
        TemplateResult::Value(v) => Ok(is_truthy(&v)),
        TemplateResult::Error(e) => Err(e),
    }
}

/// Convert TemplateResult to f64
fn result_to_f64(result: TemplateResult) -> Result<f64, String> {
    match result {
        TemplateResult::Value(TemplateValue::Number(n)) => Ok(n),
        TemplateResult::Value(TemplateValue::Boolean(b)) => Ok(if b { 1.0 } else { 0.0 }),
        TemplateResult::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        _ => Err("Cannot convert to number".to_string()),
    }
}

/// Convert TemplateResult to TemplateValue
fn result_to_value(result: TemplateResult) -> Result<TemplateValue, String> {
    match result {
        TemplateResult::Boolean(b) => Ok(TemplateValue::Boolean(b)),
        TemplateResult::Value(v) => Ok(v),
        TemplateResult::Error(e) => Err(e),
    }
}

/// Check if a value is truthy
fn is_truthy(value: &TemplateValue) -> bool {
    match value {
        TemplateValue::Null => false,
        TemplateValue::Boolean(b) => *b,
        TemplateValue::Number(n) => *n != 0.0,
        TemplateValue::String(s) => !s.is_empty(),
    }
}

/// Convert TemplateValue to string
fn template_value_to_string(value: &TemplateValue) -> String {
    match value {
        TemplateValue::Null => "null".to_string(),
        TemplateValue::Boolean(b) => b.to_string(),
        TemplateValue::Number(n) => n.to_string(),
        TemplateValue::String(s) => s.clone(),
    }
}

/// Convert FieldValue to TemplateValue
fn field_value_to_template_value(value: &FieldValue) -> TemplateValue {
    match value {
        FieldValue::Null => TemplateValue::Null,
        FieldValue::Boolean(b) => TemplateValue::Boolean(*b),
        FieldValue::Integer(i) => TemplateValue::Number(*i as f64),
        FieldValue::Float(f) => TemplateValue::Number(*f),
        FieldValue::String(s) => TemplateValue::String(s.clone()),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            TemplateValue::Number(*v as f64 / scale as f64)
        }
        _ => TemplateValue::String(format!("{:?}", value)),
    }
}

/// Try to parse a literal value
fn try_parse_literal(expr: &str) -> Option<TemplateValue> {
    let expr = expr.trim();

    // Boolean literals
    if expr == "true" {
        return Some(TemplateValue::Boolean(true));
    }
    if expr == "false" {
        return Some(TemplateValue::Boolean(false));
    }
    if expr == "null" || expr == "None" {
        return Some(TemplateValue::Null);
    }

    // Number literals
    if let Ok(n) = expr.parse::<f64>() {
        return Some(TemplateValue::Number(n));
    }

    // String literals (quoted)
    if (expr.starts_with('"') && expr.ends_with('"'))
        || (expr.starts_with('\'') && expr.ends_with('\''))
    {
        return Some(TemplateValue::String(expr[1..expr.len() - 1].to_string()));
    }

    None
}

/// Comparison functions
fn compare_eq(left: &TemplateValue, right: &TemplateValue) -> bool {
    match (left, right) {
        (TemplateValue::Number(l), TemplateValue::Number(r)) => (l - r).abs() < 1e-10,
        (TemplateValue::Boolean(l), TemplateValue::Boolean(r)) => l == r,
        (TemplateValue::String(l), TemplateValue::String(r)) => l == r,
        (TemplateValue::Null, TemplateValue::Null) => true,
        _ => false,
    }
}

fn compare_neq(left: &TemplateValue, right: &TemplateValue) -> bool {
    !compare_eq(left, right)
}

fn compare_gt(left: &TemplateValue, right: &TemplateValue) -> bool {
    match (left, right) {
        (TemplateValue::Number(l), TemplateValue::Number(r)) => l > r,
        _ => false,
    }
}

fn compare_lt(left: &TemplateValue, right: &TemplateValue) -> bool {
    match (left, right) {
        (TemplateValue::Number(l), TemplateValue::Number(r)) => l < r,
        _ => false,
    }
}

fn compare_gte(left: &TemplateValue, right: &TemplateValue) -> bool {
    compare_gt(left, right) || compare_eq(left, right)
}

fn compare_lte(left: &TemplateValue, right: &TemplateValue) -> bool {
    compare_lt(left, right) || compare_eq(left, right)
}

/// Arithmetic functions
fn arith_add(left: f64, right: f64) -> f64 {
    left + right
}

fn arith_sub(left: f64, right: f64) -> f64 {
    left - right
}

fn arith_mul(left: f64, right: f64) -> f64 {
    left * right
}

fn arith_div(left: f64, right: f64) -> f64 {
    if right == 0.0 { f64::NAN } else { left / right }
}

/// Compute aggregate value
fn compute_aggregate(values: &[f64], agg_type: AggType) -> TemplateValue {
    if values.is_empty() {
        return match agg_type {
            AggType::Count => TemplateValue::Number(0.0),
            _ => TemplateValue::Null,
        };
    }

    match agg_type {
        AggType::Sum => TemplateValue::Number(values.iter().sum()),
        AggType::Count => TemplateValue::Number(values.len() as f64),
        AggType::Avg => TemplateValue::Number(values.iter().sum::<f64>() / values.len() as f64),
        AggType::Min => TemplateValue::Number(values.iter().cloned().fold(f64::INFINITY, f64::min)),
        AggType::Max => {
            TemplateValue::Number(values.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
        }
    }
}

impl Default for AssertionRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Convert FieldValue to f64 for numeric comparisons
fn field_value_to_f64(value: &FieldValue) -> Option<f64> {
    match value {
        FieldValue::Integer(i) => Some(*i as f64),
        FieldValue::Float(f) => Some(*f),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            Some(*v as f64 / scale as f64)
        }
        _ => None,
    }
}

/// Convert FieldValue to timestamp in milliseconds
fn field_value_to_timestamp_ms(value: &FieldValue) -> Option<i64> {
    match value {
        FieldValue::Integer(i) => Some(*i), // Assume milliseconds if integer
        FieldValue::Timestamp(ts) => Some(ts.and_utc().timestamp_millis()),
        FieldValue::String(s) => {
            // Try parsing as ISO 8601
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_millis())
                .or_else(|| {
                    // Try parsing as epoch milliseconds
                    s.parse::<i64>().ok()
                })
        }
        _ => None,
    }
}

/// Compare two FieldValues for ordering (returns -1, 0, or 1)
fn compare_field_values(a: &FieldValue, b: &FieldValue) -> i32 {
    // Try numeric comparison first
    if let (Some(a_f64), Some(b_f64)) = (field_value_to_f64(a), field_value_to_f64(b)) {
        return if a_f64 < b_f64 {
            -1
        } else if a_f64 > b_f64 {
            1
        } else {
            0
        };
    }

    // Try timestamp comparison
    if let (Some(a_ts), Some(b_ts)) = (
        field_value_to_timestamp_ms(a),
        field_value_to_timestamp_ms(b),
    ) {
        return if a_ts < b_ts {
            -1
        } else if a_ts > b_ts {
            1
        } else {
            0
        };
    }

    // Fall back to string comparison
    let a_str = field_value_to_string(a);
    let b_str = field_value_to_string(b);
    a_str.cmp(&b_str) as i32
}

/// Compare FieldValue with operator
fn compare_field_value(
    value: &FieldValue,
    operator: &ComparisonOperator,
    expected: &serde_yaml::Value,
) -> bool {
    let value_str = field_value_to_string(value);

    match operator {
        ComparisonOperator::Equals => {
            if let Some(exp) = expected.as_bool() {
                // Boolean comparison - check FieldValue::Boolean or string representation
                match value {
                    FieldValue::Boolean(b) => *b == exp,
                    _ => value_str == exp.to_string(),
                }
            } else if let Some(exp) = expected.as_str() {
                value_str == exp
            } else if let Some(exp) = expected.as_i64() {
                field_value_to_f64(value).is_some_and(|v| (v - exp as f64).abs() < 0.0001)
            } else if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).is_some_and(|v| (v - exp).abs() < 0.0001)
            } else {
                false
            }
        }
        ComparisonOperator::NotEquals => {
            !compare_field_value(value, &ComparisonOperator::Equals, expected)
        }
        ComparisonOperator::GreaterThan => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).is_some_and(|v| v > exp)
            } else {
                false
            }
        }
        ComparisonOperator::LessThan => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).is_some_and(|v| v < exp)
            } else {
                false
            }
        }
        ComparisonOperator::GreaterThanOrEquals => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).is_some_and(|v| v >= exp)
            } else {
                false
            }
        }
        ComparisonOperator::LessThanOrEquals => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).is_some_and(|v| v <= exp)
            } else {
                false
            }
        }
        ComparisonOperator::Contains => {
            if let Some(exp) = expected.as_str() {
                value_str.contains(exp)
            } else {
                false
            }
        }
        ComparisonOperator::StartsWith => {
            if let Some(exp) = expected.as_str() {
                value_str.starts_with(exp)
            } else {
                false
            }
        }
        ComparisonOperator::EndsWith => {
            if let Some(exp) = expected.as_str() {
                value_str.ends_with(exp)
            } else {
                false
            }
        }
        ComparisonOperator::Matches => {
            if let Some(exp) = expected.as_str() {
                regex::Regex::new(exp)
                    .map(|r| r.is_match(&value_str))
                    .unwrap_or(false)
            } else {
                false
            }
        }
    }
}

// ==================== File Assertion Helper Functions ====================

/// Convert a record to a string for set-based comparison
fn record_to_compare_string(record: &StreamRecord, fields: &[&String]) -> String {
    let mut parts: Vec<String> = fields
        .iter()
        .map(|f| {
            record
                .fields
                .get(*f)
                .map(field_value_to_string)
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect();
    parts.sort();
    parts.join("|")
}

/// Compare two optional field values with tolerance for numeric values
fn values_equal(
    actual: Option<&FieldValue>,
    expected: Option<&FieldValue>,
    tolerance: f64,
) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(FieldValue::Null), None) | (None, Some(FieldValue::Null)) => true,
        (Some(a), Some(e)) => {
            // Try numeric comparison first
            if let (Some(a_f64), Some(e_f64)) = (field_value_to_f64(a), field_value_to_f64(e)) {
                (a_f64 - e_f64).abs() <= tolerance
            } else {
                // Fall back to string comparison
                field_value_to_string(a) == field_value_to_string(e)
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_output(records: Vec<HashMap<String, FieldValue>>) -> CapturedOutput {
        CapturedOutput {
            query_name: "test_query".to_string(),
            sink_name: "test_sink".to_string(),
            topic: Some("test_topic".to_string()),
            records: records.into_iter().map(StreamRecord::new).collect(),
            execution_time_ms: 100,
            warnings: vec![],
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        }
    }

    fn create_test_output_with_keys(
        records: Vec<HashMap<String, FieldValue>>,
        keys: Vec<Option<String>>,
    ) -> CapturedOutput {
        let stream_records: Vec<StreamRecord> = records
            .into_iter()
            .zip(keys)
            .map(|(fields, key)| {
                let mut rec = StreamRecord::new(fields);
                rec.key = key.map(|k| FieldValue::String(k));
                rec
            })
            .collect();
        CapturedOutput {
            query_name: "test_query".to_string(),
            sink_name: "test_sink".to_string(),
            topic: Some("test_topic".to_string()),
            records: stream_records,
            execution_time_ms: 100,
            warnings: vec![],
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        }
    }

    // ==================== AssertionResult Tests ====================

    #[test]
    fn test_assertion_result_pass() {
        let result = AssertionResult::pass("test_type", "Test passed");
        assert!(result.passed);
        assert_eq!(result.assertion_type, "test_type");
        assert_eq!(result.message, "Test passed");
        assert!(result.expected.is_none());
        assert!(result.actual.is_none());
    }

    #[test]
    fn test_assertion_result_fail() {
        let result = AssertionResult::fail("test_type", "Test failed", "expected", "actual");
        assert!(!result.passed);
        assert_eq!(result.assertion_type, "test_type");
        assert_eq!(result.expected, Some("expected".to_string()));
        assert_eq!(result.actual, Some("actual".to_string()));
    }

    #[test]
    fn test_assertion_result_with_detail() {
        let result = AssertionResult::pass("test", "msg")
            .with_detail("key1", "value1")
            .with_detail("key2", "value2");
        assert_eq!(result.details.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.details.get("key2"), Some(&"value2".to_string()));
    }

    // ==================== Record Count Tests ====================

    #[test]
    fn test_record_count_equals_pass() {
        let output = create_test_output(vec![
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
        ]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: Some(2),
            greater_than: None,
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_record_count_equals_fail() {
        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: Some(5),
            greater_than: None,
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(!result.passed);
        assert_eq!(result.expected, Some("5".to_string()));
        assert_eq!(result.actual, Some("1".to_string()));
    }

    #[test]
    fn test_record_count_greater_than_pass() {
        let output = create_test_output(vec![
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(3))]),
        ]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: None,
            greater_than: Some(2),
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_record_count_greater_than_fail() {
        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: None,
            greater_than: Some(5),
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(!result.passed);
    }

    #[test]
    fn test_record_count_less_than_pass() {
        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: None,
            greater_than: None,
            less_than: Some(5),
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_record_count_between_pass() {
        let output = create_test_output(vec![
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(3))]),
        ]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: None,
            greater_than: None,
            less_than: None,
            between: Some((1, 5)),
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_record_count_between_fail() {
        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: None,
            greater_than: None,
            less_than: None,
            between: Some((5, 10)),
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(!result.passed);
    }

    // ==================== Schema Contains Tests ====================

    #[test]
    fn test_schema_contains_pass() {
        let output = create_test_output(vec![HashMap::from([
            ("id".to_string(), FieldValue::Integer(1)),
            ("name".to_string(), FieldValue::String("test".to_string())),
            ("value".to_string(), FieldValue::Float(2.5)),
        ])]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string(), "name".to_string()],
            key_field: None,
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_schema_contains_fail() {
        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string(), "missing_field".to_string()],
            key_field: None,
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(!result.passed, "Expected assertion to fail");

        // Message should contain the missing fields
        assert!(
            result.message.contains("missing_field"),
            "Message should contain 'missing_field', got: {}",
            result.message
        );

        // Actual should contain fields that ARE present
        let actual = result.actual.as_ref().expect("actual should be set");
        assert!(
            actual.contains("id"),
            "Actual should contain 'id', got: {}",
            actual
        );

        // Expected should contain all required fields
        let expected = result.expected.as_ref().expect("expected should be set");
        assert!(
            expected.contains("id"),
            "Expected should contain 'id', got: {}",
            expected
        );
        assert!(
            expected.contains("missing_field"),
            "Expected should contain 'missing_field', got: {}",
            expected
        );
    }

    #[test]
    fn test_schema_contains_empty_records() {
        let output = create_test_output(vec![]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string()],
            key_field: None,
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(!result.passed);
    }

    // ==================== No Nulls Tests ====================

    #[test]
    fn test_no_nulls_pass() {
        let output = create_test_output(vec![
            HashMap::from([
                ("id".to_string(), FieldValue::Integer(1)),
                ("name".to_string(), FieldValue::String("test".to_string())),
            ]),
            HashMap::from([
                ("id".to_string(), FieldValue::Integer(2)),
                ("name".to_string(), FieldValue::String("test2".to_string())),
            ]),
        ]);

        let runner = AssertionRunner::new();
        let config = NoNullsAssertion {
            fields: vec!["id".to_string(), "name".to_string()],
        };

        let result = runner.assert_no_nulls(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_no_nulls_fail() {
        let output = create_test_output(vec![HashMap::from([
            ("id".to_string(), FieldValue::Integer(1)),
            ("name".to_string(), FieldValue::Null),
        ])]);

        let runner = AssertionRunner::new();
        let config = NoNullsAssertion {
            fields: vec!["name".to_string()],
        };

        let result = runner.assert_no_nulls(&output, &config);
        assert!(!result.passed);
        assert!(result.actual.unwrap().contains("name[0]"));
    }

    #[test]
    fn test_no_nulls_all_fields() {
        let output = create_test_output(vec![HashMap::from([
            ("id".to_string(), FieldValue::Integer(1)),
            ("name".to_string(), FieldValue::Null),
        ])]);

        let runner = AssertionRunner::new();
        let config = NoNullsAssertion { fields: vec![] }; // Check all fields

        let result = runner.assert_no_nulls(&output, &config);
        assert!(!result.passed);
    }

    // ==================== Field In Set Tests ====================

    #[test]
    fn test_field_in_set_pass() {
        let output = create_test_output(vec![
            HashMap::from([(
                "status".to_string(),
                FieldValue::String("active".to_string()),
            )]),
            HashMap::from([(
                "status".to_string(),
                FieldValue::String("pending".to_string()),
            )]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldInSetAssertion {
            field: "status".to_string(),
            values: vec![
                "active".to_string(),
                "pending".to_string(),
                "completed".to_string(),
            ],
        };

        let result = runner.assert_field_in_set(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_field_in_set_fail() {
        let output = create_test_output(vec![HashMap::from([(
            "status".to_string(),
            FieldValue::String("invalid".to_string()),
        )])]);

        let runner = AssertionRunner::new();
        let config = FieldInSetAssertion {
            field: "status".to_string(),
            values: vec!["active".to_string(), "pending".to_string()],
        };

        let result = runner.assert_field_in_set(&output, &config);
        assert!(!result.passed);
        assert!(result.actual.unwrap().contains("invalid"));
    }

    // ==================== Field Values Tests ====================

    #[test]
    fn test_field_values_equals_pass() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Integer(100))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(100))]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "value".to_string(),
            operator: ComparisonOperator::Equals,
            value: serde_yaml::Value::Number(serde_yaml::Number::from(100)),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_field_values_equals_boolean_pass() {
        let output = create_test_output(vec![
            HashMap::from([("active".to_string(), FieldValue::Boolean(true))]),
            HashMap::from([("active".to_string(), FieldValue::Boolean(true))]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "active".to_string(),
            operator: ComparisonOperator::Equals,
            value: serde_yaml::Value::Bool(true),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(
            result.passed,
            "Boolean comparison should pass: {}",
            result.message
        );
    }

    #[test]
    fn test_field_values_equals_boolean_false_pass() {
        let output = create_test_output(vec![
            HashMap::from([("active".to_string(), FieldValue::Boolean(false))]),
            HashMap::from([("active".to_string(), FieldValue::Boolean(false))]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "active".to_string(),
            operator: ComparisonOperator::Equals,
            value: serde_yaml::Value::Bool(false),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(
            result.passed,
            "Boolean false comparison should pass: {}",
            result.message
        );
    }

    #[test]
    fn test_field_values_equals_boolean_mismatch_fail() {
        let output = create_test_output(vec![
            HashMap::from([("active".to_string(), FieldValue::Boolean(true))]),
            HashMap::from([("active".to_string(), FieldValue::Boolean(false))]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "active".to_string(),
            operator: ComparisonOperator::Equals,
            value: serde_yaml::Value::Bool(true),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(
            !result.passed,
            "Boolean comparison with mismatch should fail"
        );
    }

    #[test]
    fn test_field_values_greater_than_pass() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Float(15.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(20.0))]),
        ]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "value".to_string(),
            operator: ComparisonOperator::GreaterThan,
            value: serde_yaml::Value::Number(serde_yaml::Number::from(10)),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_field_values_contains_pass() {
        let output = create_test_output(vec![HashMap::from([(
            "name".to_string(),
            FieldValue::String("hello world".to_string()),
        )])]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "name".to_string(),
            operator: ComparisonOperator::Contains,
            value: serde_yaml::Value::String("world".to_string()),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_field_values_starts_with_pass() {
        let output = create_test_output(vec![HashMap::from([(
            "name".to_string(),
            FieldValue::String("prefix_test".to_string()),
        )])]);

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "name".to_string(),
            operator: ComparisonOperator::StartsWith,
            value: serde_yaml::Value::String("prefix".to_string()),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(result.passed);
    }

    // ==================== Aggregate Check Tests ====================

    #[test]
    fn test_aggregate_sum_pass() {
        let output = create_test_output(vec![
            HashMap::from([("amount".to_string(), FieldValue::Float(10.0))]),
            HashMap::from([("amount".to_string(), FieldValue::Float(20.0))]),
            HashMap::from([("amount".to_string(), FieldValue::Float(30.0))]),
        ]);

        let runner = AssertionRunner::new();
        let config = AggregateCheckAssertion {
            field: "amount".to_string(),
            function: AggregateFunction::Sum,
            expected: "60.0".to_string(),
            tolerance: Some(0.01),
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_aggregate_avg_pass() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Integer(10))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(20))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(30))]),
        ]);

        let runner = AssertionRunner::new();
        let config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Avg,
            expected: "20.0".to_string(),
            tolerance: Some(0.01),
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_aggregate_count_pass() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(2))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(3))]),
        ]);

        let runner = AssertionRunner::new();
        let config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Count,
            expected: "3.0".to_string(),
            tolerance: Some(0.01),
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_aggregate_min_max() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Float(5.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(15.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(10.0))]),
        ]);

        let runner = AssertionRunner::new();

        // Test MIN
        let min_config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Min,
            expected: "5.0".to_string(),
            tolerance: Some(0.01),
        };
        assert!(runner.assert_aggregate(&output, &min_config).passed);

        // Test MAX
        let max_config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Max,
            expected: "15.0".to_string(),
            tolerance: Some(0.01),
        };
        assert!(runner.assert_aggregate(&output, &max_config).passed);
    }

    #[test]
    fn test_aggregate_no_values() {
        let output = create_test_output(vec![]);

        let runner = AssertionRunner::new();
        let config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Sum,
            expected: "0".to_string(),
            tolerance: None,
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(!result.passed);
    }

    // ==================== Helper Function Tests ====================

    #[test]
    fn test_field_value_to_string() {
        assert_eq!(field_value_to_string(&FieldValue::Null), "NULL");
        assert_eq!(field_value_to_string(&FieldValue::Boolean(true)), "true");
        assert_eq!(field_value_to_string(&FieldValue::Integer(42)), "42");
        assert_eq!(field_value_to_string(&FieldValue::Float(2.5)), "2.5");
        assert_eq!(
            field_value_to_string(&FieldValue::String("test".to_string())),
            "test"
        );
        // ScaledInteger: 12345 with scale 2 = 123.45
        assert_eq!(
            field_value_to_string(&FieldValue::ScaledInteger(12345, 2)),
            "123.45"
        );
    }

    #[test]
    fn test_field_value_to_f64() {
        assert_eq!(field_value_to_f64(&FieldValue::Integer(42)), Some(42.0));
        assert_eq!(field_value_to_f64(&FieldValue::Float(2.5)), Some(2.5));
        assert_eq!(
            field_value_to_f64(&FieldValue::ScaledInteger(12345, 2)),
            Some(123.45)
        );
        assert_eq!(
            field_value_to_f64(&FieldValue::String("test".to_string())),
            None
        );
        assert_eq!(field_value_to_f64(&FieldValue::Null), None);
    }

    // ==================== Run Assertions Tests ====================

    #[test]
    fn test_run_multiple_assertions() {
        let output = create_test_output(vec![
            HashMap::from([
                ("id".to_string(), FieldValue::Integer(1)),
                (
                    "status".to_string(),
                    FieldValue::String("active".to_string()),
                ),
            ]),
            HashMap::from([
                ("id".to_string(), FieldValue::Integer(2)),
                (
                    "status".to_string(),
                    FieldValue::String("active".to_string()),
                ),
            ]),
        ]);

        let runner = AssertionRunner::new();
        let assertions = vec![
            AssertionConfig::RecordCount(RecordCountAssertion {
                equals: Some(2),
                greater_than: None,
                less_than: None,
                between: None,
                expression: None,
            }),
            AssertionConfig::SchemaContains(SchemaContainsAssertion {
                fields: vec!["id".to_string(), "status".to_string()],
                key_field: None,
            }),
        ];

        let results = runner.run_assertions(&output, &assertions);
        assert_eq!(results.len(), 2);
        assert!(results[0].passed);
        assert!(results[1].passed);
    }

    // ==================== Template Variable Tests ====================

    #[test]
    fn test_context_resolve_inputs_count() {
        let context = AssertionContext::new().with_input_records(
            "market_data",
            vec![
                HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
                HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
                HashMap::from([("id".to_string(), FieldValue::Integer(3))]),
            ],
        );

        assert_eq!(
            context.resolve_template("{{inputs.market_data.count}}"),
            Some("3".to_string())
        );
    }

    #[test]
    fn test_context_resolve_outputs_count() {
        let context = AssertionContext::new().with_output_count("results_sink", 42);

        assert_eq!(
            context.resolve_template("{{outputs.results_sink.count}}"),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_context_resolve_variables() {
        let context = AssertionContext::new()
            .with_variable("expected_count", "100")
            .with_variable("tolerance", "0.01");

        assert_eq!(
            context.resolve_template("{{variables.expected_count}}"),
            Some("100".to_string())
        );
        assert_eq!(
            context.resolve_template("{{expected_count}}"),
            Some("100".to_string())
        );
    }

    #[test]
    fn test_context_resolve_or_original() {
        let context = AssertionContext::new().with_input_records(
            "source",
            vec![HashMap::from([("id".to_string(), FieldValue::Integer(1))])],
        );

        // Template should resolve
        assert_eq!(context.resolve_or_original("{{inputs.source.count}}"), "1");

        // Non-template should return original
        assert_eq!(context.resolve_or_original("42.0"), "42.0");

        // Unknown template should return original
        assert_eq!(
            context.resolve_or_original("{{inputs.unknown.count}}"),
            "{{inputs.unknown.count}}"
        );
    }

    // ==================== Expression Evaluation Tests ====================

    #[test]
    fn test_expression_simple_number() {
        let context = AssertionContext::new();
        assert_eq!(context.evaluate_expression("42").unwrap(), 42.0);
        assert_eq!(context.evaluate_expression("3.15").unwrap(), 3.15);
    }

    #[test]
    fn test_expression_arithmetic() {
        let context = AssertionContext::new();
        assert_eq!(context.evaluate_expression("10 + 5").unwrap(), 15.0);
        assert_eq!(context.evaluate_expression("10 - 5").unwrap(), 5.0);
        assert_eq!(context.evaluate_expression("10 * 5").unwrap(), 50.0);
        assert_eq!(context.evaluate_expression("10 / 5").unwrap(), 2.0);
    }

    #[test]
    fn test_expression_with_templates() {
        let context = AssertionContext::new()
            .with_input_records(
                "source",
                vec![
                    HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(3))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(4))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(5))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(6))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(7))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(8))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(9))]),
                    HashMap::from([("id".to_string(), FieldValue::Integer(10))]),
                ],
            )
            .with_output_count("sink", 8);

        // Test template resolution
        assert_eq!(
            context
                .evaluate_expression("{{inputs.source.count}}")
                .unwrap(),
            10.0
        );

        // Test template with arithmetic
        assert_eq!(
            context
                .evaluate_expression("{{inputs.source.count}} * 0.9")
                .unwrap(),
            9.0
        );

        // Test comparison using output
        assert_eq!(
            context
                .evaluate_expression("{{outputs.sink.count}} / {{inputs.source.count}}")
                .unwrap(),
            0.8
        );
    }

    #[test]
    fn test_expression_precedence() {
        let context = AssertionContext::new();

        // Multiplication before addition
        assert_eq!(context.evaluate_expression("2 + 3 * 4").unwrap(), 14.0);

        // Parentheses
        assert_eq!(context.evaluate_expression("(2 + 3) * 4").unwrap(), 20.0);
    }

    #[test]
    fn test_expression_comparison() {
        let context = AssertionContext::new()
            .with_input_records(
                "source",
                (0..100)
                    .map(|i| HashMap::from([("id".to_string(), FieldValue::Integer(i))]))
                    .collect(),
            )
            .with_output_count("sink", 95);

        // Test >= comparison - true case
        assert_eq!(
            context
                .evaluate_expression("{{outputs.sink.count}} >= {{inputs.source.count}} * 0.9")
                .unwrap(),
            1.0 // true
        );

        // Test > comparison - false case
        assert_eq!(
            context
                .evaluate_expression("{{outputs.sink.count}} > {{inputs.source.count}}")
                .unwrap(),
            0.0 // false
        );
    }

    #[test]
    fn test_expression_division_by_zero() {
        let context = AssertionContext::new();
        let result = context.evaluate_expression("10 / 0");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Division by zero"));
    }

    #[test]
    fn test_expression_invalid_number() {
        let context = AssertionContext::new();
        let result = context.evaluate_expression("abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot parse"));
    }

    #[test]
    fn test_aggregate_with_template_variable() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Integer(10))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(20))]),
            HashMap::from([("value".to_string(), FieldValue::Integer(30))]),
        ]);

        // Create context with expected input count matching output sum
        let context = AssertionContext::new().with_input_records(
            "source_data",
            (0..60)
                .map(|i| HashMap::from([("id".to_string(), FieldValue::Integer(i))]))
                .collect(),
        );

        let runner = AssertionRunner::new().with_context(context);
        let config = AggregateCheckAssertion {
            field: "value".to_string(),
            function: AggregateFunction::Sum,
            expected: "{{inputs.source_data.count}}".to_string(), // Should resolve to 60
            tolerance: Some(0.01),
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(
            result.passed,
            "Expected SUM(value)=60 to match input count 60"
        );
    }

    // ==================== JOIN Coverage Tests ====================

    #[test]
    fn test_join_coverage_with_input_tracking() {
        // Simulate left input with 4 records
        let left_records = vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("AAPL".to_string()))]),
            HashMap::from([(
                "symbol".to_string(),
                FieldValue::String("GOOGL".to_string()),
            )]),
            HashMap::from([("symbol".to_string(), FieldValue::String("MSFT".to_string()))]),
            HashMap::from([("symbol".to_string(), FieldValue::String("AMZN".to_string()))]),
        ];

        // Simulate right input (reference table) with 3 records
        let right_records = vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("AAPL".to_string()))]),
            HashMap::from([(
                "symbol".to_string(),
                FieldValue::String("GOOGL".to_string()),
            )]),
            HashMap::from([("symbol".to_string(), FieldValue::String("MSFT".to_string()))]),
        ];

        // Simulate output: 3 matched records (75% of left input)
        let output = create_test_output(vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("AAPL".to_string()))]),
            HashMap::from([(
                "symbol".to_string(),
                FieldValue::String("GOOGL".to_string()),
            )]),
            HashMap::from([("symbol".to_string(), FieldValue::String("MSFT".to_string()))]),
        ]);

        let context = AssertionContext::new()
            .with_input_records("market_data", left_records)
            .with_input_records("instruments", right_records);

        let runner = AssertionRunner::new().with_context(context);
        let config = JoinCoverageAssertion {
            left_source: Some("market_data".to_string()),
            right_source: Some("instruments".to_string()),
            min_match_rate: 0.75, // 75% = 3/4 records matched
        };

        let result = runner.assert_join_coverage(&output, &config);
        assert!(result.passed, "Expected 75% match rate (3/4 records)");
        assert!(result.details.contains_key("output_record_count"));
        assert_eq!(
            result.details.get("output_record_count"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn test_join_coverage_below_threshold() {
        // Simulate left input with 4 records
        let left_records = vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("AAPL".to_string()))]),
            HashMap::from([(
                "symbol".to_string(),
                FieldValue::String("GOOGL".to_string()),
            )]),
            HashMap::from([("symbol".to_string(), FieldValue::String("MSFT".to_string()))]),
            HashMap::from([("symbol".to_string(), FieldValue::String("AMZN".to_string()))]),
        ];

        // Simulate output: only 1 matched record (25% match rate)
        let output = create_test_output(vec![HashMap::from([(
            "symbol".to_string(),
            FieldValue::String("AAPL".to_string()),
        )])]);

        let context = AssertionContext::new().with_input_records("market_data", left_records);

        let runner = AssertionRunner::new().with_context(context);
        let config = JoinCoverageAssertion {
            left_source: Some("market_data".to_string()),
            right_source: Some("instruments".to_string()),
            min_match_rate: 0.80, // 80% required, but only 25% achieved
        };

        let result = runner.assert_join_coverage(&output, &config);
        assert!(!result.passed, "Expected failure: 25% < 80% threshold");
        assert!(result.details.contains_key("match_rate"));
    }

    #[test]
    fn test_join_coverage_no_output_with_inputs() {
        // Left has 2 records
        let left_records = vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("A".to_string()))]),
            HashMap::from([("symbol".to_string(), FieldValue::String("B".to_string()))]),
        ];

        // Right has 2 records
        let right_records = vec![
            HashMap::from([("symbol".to_string(), FieldValue::String("X".to_string()))]),
            HashMap::from([("symbol".to_string(), FieldValue::String("Y".to_string()))]),
        ];

        // Output is empty (no matches)
        let output = create_test_output(vec![]);

        let context = AssertionContext::new()
            .with_input_records("left", left_records)
            .with_input_records("right", right_records);

        let runner = AssertionRunner::new().with_context(context);
        let config = JoinCoverageAssertion {
            left_source: Some("left".to_string()),
            right_source: Some("right".to_string()),
            min_match_rate: 0.50,
        };

        let result = runner.assert_join_coverage(&output, &config);
        assert!(!result.passed);
        // Should fail because 0% < 50% threshold
    }

    #[test]
    fn test_join_coverage_without_input_tracking() {
        // No context set - should gracefully handle
        let output = create_test_output(vec![HashMap::from([(
            "symbol".to_string(),
            FieldValue::String("AAPL".to_string()),
        )])]);

        let runner = AssertionRunner::new(); // No context
        let config = JoinCoverageAssertion {
            left_source: Some("market_data".to_string()),
            right_source: Some("instruments".to_string()),
            min_match_rate: 0.80,
        };

        let result = runner.assert_join_coverage(&output, &config);
        // Should pass with a note about input tracking not being available
        assert!(result.passed);
        assert!(result.details.contains_key("note"));
    }

    #[test]
    fn test_join_coverage_default_source_names() {
        // Test that default source names "left" and "right" are used when not specified
        let left_records = vec![
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
        ];

        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let context = AssertionContext::new().with_input_records("left", left_records); // Using default name "left"

        let runner = AssertionRunner::new().with_context(context);
        let config = JoinCoverageAssertion {
            left_source: None,    // Will default to "left"
            right_source: None,   // Will default to "right"
            min_match_rate: 0.50, // 50% = 1/2 records
        };

        let result = runner.assert_join_coverage(&output, &config);
        assert!(result.passed, "Expected 50% match rate (1/2 records)");
        assert_eq!(result.details.get("left_source"), Some(&"left".to_string()));
    }

    #[test]
    fn test_join_coverage_right_source_diagnostics() {
        // Test that right source count is included in diagnostics
        let left_records = vec![
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
            HashMap::from([("id".to_string(), FieldValue::Integer(2))]),
        ];

        let right_records = vec![HashMap::from([("id".to_string(), FieldValue::Integer(1))])];

        let output = create_test_output(vec![HashMap::from([(
            "id".to_string(),
            FieldValue::Integer(1),
        )])]);

        let context = AssertionContext::new()
            .with_input_records("orders", left_records)
            .with_input_records("customers", right_records);

        let runner = AssertionRunner::new().with_context(context);
        let config = JoinCoverageAssertion {
            left_source: Some("orders".to_string()),
            right_source: Some("customers".to_string()),
            min_match_rate: 0.50,
        };

        let result = runner.assert_join_coverage(&output, &config);
        assert!(result.passed);
        assert_eq!(
            result.details.get("right_record_count"),
            Some(&"1".to_string())
        );
        assert_eq!(
            result.details.get("left_record_count"),
            Some(&"2".to_string())
        );
    }

    // ==================== Location in Message Tests ====================
    // These tests verify that assertion messages include the topic/sink location

    fn create_test_output_with_topic(
        records: Vec<HashMap<String, FieldValue>>,
        topic: Option<&str>,
        sink_name: &str,
    ) -> CapturedOutput {
        CapturedOutput {
            query_name: "test_query".to_string(),
            sink_name: sink_name.to_string(),
            topic: topic.map(|t| t.to_string()),
            records: records.into_iter().map(StreamRecord::new).collect(),
            execution_time_ms: 100,
            warnings: vec![],
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        }
    }

    #[test]
    fn test_record_count_message_includes_topic_location() {
        // Test with topic set
        let output = create_test_output_with_topic(
            vec![HashMap::from([("id".to_string(), FieldValue::Integer(1))])],
            Some("my_output_topic"),
            "my_sink",
        );

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: Some(1),
            greater_than: None,
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'my_output_topic'"),
            "Message should include topic location. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_record_count_message_includes_sink_location_when_no_topic() {
        // Test with no topic (file sink case)
        let output = create_test_output_with_topic(
            vec![HashMap::from([("id".to_string(), FieldValue::Integer(1))])],
            None,
            "output_file.jsonl",
        );

        let runner = AssertionRunner::new();
        let config = RecordCountAssertion {
            equals: Some(1),
            greater_than: None,
            less_than: None,
            between: None,
            expression: None,
        };

        let result = runner.assert_record_count(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("sink 'output_file.jsonl'"),
            "Message should include sink location when no topic. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_schema_contains_message_includes_location() {
        let output = create_test_output_with_topic(
            vec![HashMap::from([
                ("id".to_string(), FieldValue::Integer(1)),
                ("name".to_string(), FieldValue::String("test".to_string())),
            ])],
            Some("schema_test_topic"),
            "schema_sink",
        );

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string(), "name".to_string()],
            key_field: None,
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'schema_test_topic'"),
            "Schema contains message should include topic. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_schema_contains_with_key_field_pass() {
        // Create output with message keys (simulating GROUP BY symbol producing "AAPL" as key)
        let output = create_test_output_with_keys(
            vec![HashMap::from([
                ("trade_count".to_string(), FieldValue::Integer(10)),
                ("total_volume".to_string(), FieldValue::Float(1000.0)),
            ])],
            vec![Some("AAPL".to_string())],
        );

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["trade_count".to_string(), "total_volume".to_string()],
            key_field: Some("symbol".to_string()),
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(
            result.passed,
            "Expected assertion to pass with key_field. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_schema_contains_with_key_field_missing_keys() {
        // Create output without message keys (keys are None)
        let output = create_test_output(vec![HashMap::from([
            ("trade_count".to_string(), FieldValue::Integer(10)),
            ("total_volume".to_string(), FieldValue::Float(1000.0)),
        ])]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["trade_count".to_string(), "total_volume".to_string()],
            key_field: Some("symbol".to_string()),
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(
            !result.passed,
            "Expected assertion to fail when key_field specified but keys are missing"
        );
        assert!(
            result.message.contains("symbol") || result.message.contains("key"),
            "Message should mention missing key field. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_schema_contains_validates_key_values() {
        // Create output with multiple records and different keys (simulating GROUP BY symbol)
        let output = create_test_output_with_keys(
            vec![
                HashMap::from([
                    ("trade_count".to_string(), FieldValue::Integer(10)),
                    ("total_volume".to_string(), FieldValue::Float(1000.0)),
                ]),
                HashMap::from([
                    ("trade_count".to_string(), FieldValue::Integer(20)),
                    ("total_volume".to_string(), FieldValue::Float(2000.0)),
                ]),
                HashMap::from([
                    ("trade_count".to_string(), FieldValue::Integer(30)),
                    ("total_volume".to_string(), FieldValue::Float(3000.0)),
                ]),
            ],
            vec![
                Some("AAPL".to_string()),
                Some("GOOGL".to_string()),
                Some("MSFT".to_string()),
            ],
        );

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["trade_count".to_string(), "total_volume".to_string()],
            key_field: Some("symbol".to_string()),
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(
            result.passed,
            "Expected assertion to pass with multiple key values. Got: {}",
            result.message
        );

        // Verify message mentions key field was validated
        assert!(
            result.message.contains("key field") || result.message.contains("symbol"),
            "Message should mention key field validation. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_schema_contains_with_empty_key_values() {
        // Create output with empty string keys (edge case)
        let output = create_test_output_with_keys(
            vec![HashMap::from([(
                "trade_count".to_string(),
                FieldValue::Integer(10),
            )])],
            vec![Some("".to_string())], // Empty key value
        );

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["trade_count".to_string()],
            key_field: Some("symbol".to_string()),
        };

        // Empty string keys should fail - a valid key must have content
        let result = runner.assert_schema_contains(&output, &config);
        assert!(
            !result.passed,
            "Empty string key should fail validation. Got: {}",
            result.message
        );
        assert!(
            result.message.contains("no message keys found"),
            "Should report no valid keys. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_no_nulls_message_includes_location() {
        let output = create_test_output_with_topic(
            vec![HashMap::from([
                ("id".to_string(), FieldValue::Integer(1)),
                ("name".to_string(), FieldValue::String("test".to_string())),
            ])],
            Some("no_nulls_topic"),
            "no_nulls_sink",
        );

        let runner = AssertionRunner::new();
        let config = NoNullsAssertion {
            fields: vec!["id".to_string()],
        };

        let result = runner.assert_no_nulls(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'no_nulls_topic'"),
            "No nulls message should include topic. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_field_in_set_message_includes_location() {
        let output = create_test_output_with_topic(
            vec![HashMap::from([(
                "status".to_string(),
                FieldValue::String("active".to_string()),
            )])],
            Some("field_set_topic"),
            "field_set_sink",
        );

        let runner = AssertionRunner::new();
        let config = FieldInSetAssertion {
            field: "status".to_string(),
            values: vec!["active".to_string(), "inactive".to_string()],
        };

        let result = runner.assert_field_in_set(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'field_set_topic'"),
            "Field in set message should include topic. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_field_values_message_includes_location() {
        let output = create_test_output_with_topic(
            vec![HashMap::from([(
                "value".to_string(),
                FieldValue::Integer(100),
            )])],
            Some("field_values_topic"),
            "field_values_sink",
        );

        let runner = AssertionRunner::new();
        let config = FieldValuesAssertion {
            field: "value".to_string(),
            operator: ComparisonOperator::Equals,
            value: serde_yaml::Value::Number(serde_yaml::Number::from(100)),
        };

        let result = runner.assert_field_values(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'field_values_topic'"),
            "Field values message should include topic. Got: {}",
            result.message
        );
    }

    #[test]
    fn test_aggregate_message_includes_location() {
        let output = create_test_output_with_topic(
            vec![
                HashMap::from([("amount".to_string(), FieldValue::Float(10.0))]),
                HashMap::from([("amount".to_string(), FieldValue::Float(20.0))]),
                HashMap::from([("amount".to_string(), FieldValue::Float(30.0))]),
            ],
            Some("aggregate_topic"),
            "aggregate_sink",
        );

        let runner = AssertionRunner::new();
        let config = AggregateCheckAssertion {
            field: "amount".to_string(),
            function: AggregateFunction::Sum,
            expected: "60".to_string(),
            tolerance: None,
        };

        let result = runner.assert_aggregate(&output, &config);
        assert!(result.passed);
        assert!(
            result.message.contains("topic 'aggregate_topic'"),
            "Aggregate message should include topic. Got: {}",
            result.message
        );
    }

    // ==================== Window Boundary Tests ====================

    #[test]
    fn test_window_boundary_empty_records() {
        let output = create_test_output(vec![]);

        let runner = AssertionRunner::new();
        let config = WindowBoundaryAssertion {
            timestamp_field: "event_time".to_string(),
            window_start_field: Some("window_start".to_string()),
            window_end_field: Some("window_end".to_string()),
            window_size_ms: None,
            tolerance_ms: 0,
            verify_containment: true,
            verify_alignment: false,
        };

        let result = runner.assert_window_boundary(&output, &config);
        assert!(result.passed);
        assert!(result.message.contains("No records"));
    }

    #[test]
    fn test_window_boundary_containment_pass() {
        let output = create_test_output(vec![
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(1000)),
                ("window_start".to_string(), FieldValue::Integer(0)),
                ("window_end".to_string(), FieldValue::Integer(2000)),
            ]),
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(1500)),
                ("window_start".to_string(), FieldValue::Integer(0)),
                ("window_end".to_string(), FieldValue::Integer(2000)),
            ]),
        ]);

        let runner = AssertionRunner::new();
        let config = WindowBoundaryAssertion {
            timestamp_field: "event_time".to_string(),
            window_start_field: Some("window_start".to_string()),
            window_end_field: Some("window_end".to_string()),
            window_size_ms: None,
            tolerance_ms: 0,
            verify_containment: true,
            verify_alignment: false,
        };

        let result = runner.assert_window_boundary(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_window_boundary_containment_fail() {
        let output = create_test_output(vec![HashMap::from([
            ("event_time".to_string(), FieldValue::Integer(5000)),
            ("window_start".to_string(), FieldValue::Integer(0)),
            ("window_end".to_string(), FieldValue::Integer(2000)),
        ])]);

        let runner = AssertionRunner::new();
        let config = WindowBoundaryAssertion {
            timestamp_field: "event_time".to_string(),
            window_start_field: Some("window_start".to_string()),
            window_end_field: Some("window_end".to_string()),
            window_size_ms: None,
            tolerance_ms: 0,
            verify_containment: true,
            verify_alignment: false,
        };

        let result = runner.assert_window_boundary(&output, &config);
        assert!(!result.passed);
        assert!(result.message.contains("violations"));
    }

    #[test]
    fn test_window_boundary_alignment_pass() {
        let output = create_test_output(vec![
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(1000)),
                ("window_start".to_string(), FieldValue::Integer(0)),
            ]),
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(6000)),
                ("window_start".to_string(), FieldValue::Integer(5000)),
            ]),
        ]);

        let runner = AssertionRunner::new();
        let config = WindowBoundaryAssertion {
            timestamp_field: "event_time".to_string(),
            window_start_field: Some("window_start".to_string()),
            window_end_field: None,
            window_size_ms: Some(5000),
            tolerance_ms: 0,
            verify_containment: false,
            verify_alignment: true,
        };

        let result = runner.assert_window_boundary(&output, &config);
        assert!(result.passed);
    }

    // ==================== Latency Tests ====================

    #[test]
    fn test_latency_insufficient_records() {
        let output = create_test_output(vec![HashMap::from([(
            "event_time".to_string(),
            FieldValue::Integer(chrono::Utc::now().timestamp_millis()),
        )])]);

        let runner = AssertionRunner::new();
        let config = LatencyAssertion {
            timestamp_field: "event_time".to_string(),
            max_latency_ms: Some(5000),
            avg_latency_ms: None,
            p50_latency_ms: None,
            p95_latency_ms: None,
            p99_latency_ms: None,
            min_records: 10,
        };

        let result = runner.assert_latency(&output, &config);
        assert!(result.passed);
        assert!(result.message.contains("Insufficient records"));
    }

    #[test]
    fn test_latency_within_bounds() {
        let now = chrono::Utc::now().timestamp_millis();
        let output = create_test_output(vec![
            HashMap::from([("event_time".to_string(), FieldValue::Integer(now - 100))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(now - 200))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(now - 150))]),
        ]);

        let runner = AssertionRunner::new();
        let config = LatencyAssertion {
            timestamp_field: "event_time".to_string(),
            max_latency_ms: Some(5000),
            avg_latency_ms: Some(1000),
            p50_latency_ms: None,
            p95_latency_ms: None,
            p99_latency_ms: None,
            min_records: 3,
        };

        let result = runner.assert_latency(&output, &config);
        assert!(result.passed);
    }

    // ==================== Distribution Tests ====================

    #[test]
    fn test_distribution_insufficient_records() {
        let output = create_test_output(vec![HashMap::from([(
            "value".to_string(),
            FieldValue::Float(10.0),
        )])]);

        let runner = AssertionRunner::new();
        let config = DistributionAssertion {
            field: "value".to_string(),
            distribution_type: DistributionType::Normal,
            mean: Some(10.0),
            std_dev: Some(1.0),
            min_value: None,
            max_value: None,
            tolerance: 0.1,
            min_records: 30,
        };

        let result = runner.assert_distribution(&output, &config);
        assert!(result.passed);
        assert!(result.message.contains("Insufficient records"));
    }

    #[test]
    fn test_distribution_normal_pass() {
        // Generate data with known mean and std dev
        let values: Vec<f64> = vec![
            9.5, 10.0, 10.5, 9.8, 10.2, 10.1, 9.9, 10.0, 9.7, 10.3, 10.0, 9.6, 10.4, 9.9, 10.1,
            10.0, 9.8, 10.2, 10.0, 9.9,
        ];
        let output = create_test_output(
            values
                .iter()
                .map(|v| HashMap::from([("value".to_string(), FieldValue::Float(*v))]))
                .collect(),
        );

        let runner = AssertionRunner::new();
        let config = DistributionAssertion {
            field: "value".to_string(),
            distribution_type: DistributionType::Normal,
            mean: Some(10.0),
            std_dev: None,
            min_value: None,
            max_value: None,
            tolerance: 0.2,
            min_records: 10,
        };

        let result = runner.assert_distribution(&output, &config);
        assert!(
            result.passed,
            "Distribution check failed: {}",
            result.message
        );
    }

    #[test]
    fn test_distribution_uniform_pass() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Float(5.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(7.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(8.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(10.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(3.0))]),
        ]);

        let runner = AssertionRunner::new();
        let config = DistributionAssertion {
            field: "value".to_string(),
            distribution_type: DistributionType::Uniform,
            mean: None,
            std_dev: None,
            min_value: Some(0.0),
            max_value: Some(15.0),
            tolerance: 0.1,
            min_records: 5,
        };

        let result = runner.assert_distribution(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_distribution_exponential_negative_values_fail() {
        let output = create_test_output(vec![
            HashMap::from([("value".to_string(), FieldValue::Float(1.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(-2.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(3.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(4.0))]),
            HashMap::from([("value".to_string(), FieldValue::Float(5.0))]),
        ]);

        let runner = AssertionRunner::new();
        let config = DistributionAssertion {
            field: "value".to_string(),
            distribution_type: DistributionType::Exponential,
            mean: None,
            std_dev: None,
            min_value: None,
            max_value: None,
            tolerance: 0.1,
            min_records: 5,
        };

        let result = runner.assert_distribution(&output, &config);
        assert!(!result.passed);
        assert!(result.message.contains("negative values"));
    }

    // ==================== Percentile Tests ====================

    #[test]
    fn test_percentile_insufficient_records() {
        let output = create_test_output(vec![HashMap::from([(
            "latency".to_string(),
            FieldValue::Integer(100),
        )])]);

        let runner = AssertionRunner::new();
        let config = PercentileAssertion {
            field: "latency".to_string(),
            p50: Some(500.0),
            p75: None,
            p90: None,
            p95: None,
            p99: None,
            p999: None,
            mode: PercentileMode::LessThan,
            min_records: 10,
        };

        let result = runner.assert_percentile(&output, &config);
        assert!(result.passed);
        assert!(result.message.contains("Insufficient records"));
    }

    #[test]
    fn test_percentile_less_than_pass() {
        let output = create_test_output(
            (1..=100)
                .map(|i| HashMap::from([("latency".to_string(), FieldValue::Integer(i))]))
                .collect(),
        );

        let runner = AssertionRunner::new();
        let config = PercentileAssertion {
            field: "latency".to_string(),
            p50: Some(60.0),
            p95: Some(100.0),
            p99: Some(100.0),
            p75: None,
            p90: None,
            p999: None,
            mode: PercentileMode::LessThan,
            min_records: 10,
        };

        let result = runner.assert_percentile(&output, &config);
        assert!(result.passed, "Percentile check failed: {}", result.message);
    }

    #[test]
    fn test_percentile_greater_than_pass() {
        let output = create_test_output(
            (100..=200)
                .map(|i| HashMap::from([("score".to_string(), FieldValue::Integer(i))]))
                .collect(),
        );

        let runner = AssertionRunner::new();
        let config = PercentileAssertion {
            field: "score".to_string(),
            p50: Some(100.0),
            p75: None,
            p90: None,
            p95: None,
            p99: None,
            p999: None,
            mode: PercentileMode::GreaterThan,
            min_records: 10,
        };

        let result = runner.assert_percentile(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_percentile_p50_fail() {
        let output = create_test_output(
            (1..=100)
                .map(|i| HashMap::from([("latency".to_string(), FieldValue::Integer(i))]))
                .collect(),
        );

        let runner = AssertionRunner::new();
        let config = PercentileAssertion {
            field: "latency".to_string(),
            p50: Some(10.0), // P50 should be ~50, this will fail
            p75: None,
            p90: None,
            p95: None,
            p99: None,
            p999: None,
            mode: PercentileMode::LessThan,
            min_records: 10,
        };

        let result = runner.assert_percentile(&output, &config);
        assert!(!result.passed);
        assert!(result.message.contains("P50"));
    }

    // ==================== Event Ordering Tests ====================

    #[test]
    fn test_event_ordering_empty_records() {
        let output = create_test_output(vec![]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: None,
            window_size_ms: None,
            direction: OrderDirection::Ascending,
            max_gap_ms: None,
            max_out_of_order_percent: None,
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(result.passed);
        assert!(result.message.contains("No records"));
    }

    #[test]
    fn test_event_ordering_ascending_pass() {
        let output = create_test_output(vec![
            HashMap::from([("event_time".to_string(), FieldValue::Integer(1000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(2000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(3000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(4000))]),
        ]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: None,
            window_size_ms: None,
            direction: OrderDirection::Ascending,
            max_gap_ms: None,
            max_out_of_order_percent: None,
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_event_ordering_descending_pass() {
        let output = create_test_output(vec![
            HashMap::from([("event_time".to_string(), FieldValue::Integer(4000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(3000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(2000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(1000))]),
        ]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: None,
            window_size_ms: None,
            direction: OrderDirection::Descending,
            max_gap_ms: None,
            max_out_of_order_percent: None,
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_event_ordering_out_of_order_fail() {
        let output = create_test_output(vec![
            HashMap::from([("event_time".to_string(), FieldValue::Integer(1000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(3000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(2000))]), // Out of order
            HashMap::from([("event_time".to_string(), FieldValue::Integer(4000))]),
        ]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: None,
            window_size_ms: None,
            direction: OrderDirection::Ascending,
            max_gap_ms: None,
            max_out_of_order_percent: None,
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(!result.passed);
        assert!(result.message.contains("out-of-order"));
    }

    #[test]
    fn test_event_ordering_with_max_out_of_order_percent() {
        let output = create_test_output(vec![
            HashMap::from([("event_time".to_string(), FieldValue::Integer(1000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(3000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(2000))]), // Out of order
            HashMap::from([("event_time".to_string(), FieldValue::Integer(4000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(5000))]),
            HashMap::from([("event_time".to_string(), FieldValue::Integer(6000))]),
        ]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: None,
            window_size_ms: None,
            direction: OrderDirection::Ascending,
            max_gap_ms: None,
            max_out_of_order_percent: Some(50.0), // Allow up to 50% out of order
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(result.passed);
    }

    #[test]
    fn test_event_ordering_by_partition() {
        // Each partition should be ordered independently
        let output = create_test_output(vec![
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(1000)),
                ("partition".to_string(), FieldValue::String("A".to_string())),
            ]),
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(500)),
                ("partition".to_string(), FieldValue::String("B".to_string())),
            ]),
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(2000)),
                ("partition".to_string(), FieldValue::String("A".to_string())),
            ]),
            HashMap::from([
                ("event_time".to_string(), FieldValue::Integer(600)),
                ("partition".to_string(), FieldValue::String("B".to_string())),
            ]),
        ]);

        let runner = AssertionRunner::new();
        let config = EventOrderingAssertion {
            timestamp_field: "event_time".to_string(),
            partition_field: Some("partition".to_string()),
            window_size_ms: None,
            direction: OrderDirection::Ascending,
            max_gap_ms: None,
            max_out_of_order_percent: None,
            allow_gaps: false,
        };

        let result = runner.assert_event_ordering(&output, &config);
        assert!(result.passed);
    }
}
