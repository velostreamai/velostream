//! Assertion framework for test validation
//!
//! Provides assertion types for validating query outputs:
//! - Record count assertions
//! - Schema validation
//! - Field value checks
//! - Aggregate validations
//! - JOIN coverage analysis

use super::error::{TestHarnessError, TestHarnessResult};
use super::executor::CapturedOutput;
use super::spec::{
    AggregateCheckAssertion, AggregateFunction, AssertionConfig, ComparisonOperator,
    ExecutionTimeAssertion, FieldInSetAssertion, FieldValuesAssertion, JoinCoverageAssertion,
    MemoryUsageAssertion, NoNullsAssertion, RecordCountAssertion, SchemaContainsAssertion,
    TemplateAssertion,
};
use crate::velostream::sql::execution::types::FieldValue;
use std::collections::HashMap;

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
}

impl AssertionRunner {
    /// Create new assertion runner
    pub fn new() -> Self {
        Self {
            context: AssertionContext::default(),
        }
    }

    /// Set context for template evaluation
    pub fn with_context(mut self, context: AssertionContext) -> Self {
        self.context = context;
        self
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
        }
    }

    /// Assert record count
    fn assert_record_count(
        &self,
        output: &CapturedOutput,
        config: &RecordCountAssertion,
    ) -> AssertionResult {
        let actual = output.records.len();

        // Check exact equals
        if let Some(expected) = config.equals {
            if actual == expected {
                return AssertionResult::pass(
                    "record_count",
                    &format!("Record count matches: {}", actual),
                );
            } else {
                return AssertionResult::fail(
                    "record_count",
                    "Record count mismatch",
                    &expected.to_string(),
                    &actual.to_string(),
                );
            }
        }

        // Check greater_than
        if let Some(min) = config.greater_than {
            if actual <= min {
                return AssertionResult::fail(
                    "record_count",
                    &format!("Expected > {} records", min),
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
                    &format!("Expected < {} records", max),
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
                    &format!("Expected between {} and {} records", min, max),
                    &format!("[{}, {}]", min, max),
                    &actual.to_string(),
                );
            }
        }

        // Check expression
        if let Some(ref expr) = config.expression {
            // TODO: Implement expression evaluation
            log::warn!("Expression evaluation not yet implemented: {}", expr);
        }

        AssertionResult::pass("record_count", &format!("Record count: {}", actual))
    }

    /// Assert schema contains required fields
    fn assert_schema_contains(
        &self,
        output: &CapturedOutput,
        config: &SchemaContainsAssertion,
    ) -> AssertionResult {
        if output.records.is_empty() {
            return AssertionResult::fail(
                "schema_contains",
                "No records to check schema",
                &config.fields.join(", "),
                "(no records)",
            );
        }

        // Get fields from first record
        let actual_fields: std::collections::HashSet<_> =
            output.records[0].keys().cloned().collect();

        let missing: Vec<_> = config
            .fields
            .iter()
            .filter(|f| !actual_fields.contains(*f))
            .collect();

        if missing.is_empty() {
            AssertionResult::pass(
                "schema_contains",
                &format!("All required fields present: {}", config.fields.join(", ")),
            )
        } else {
            AssertionResult::fail(
                "schema_contains",
                "Missing required fields",
                &config.fields.join(", "),
                &format!(
                    "missing: {}",
                    missing
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        }
    }

    /// Assert no null values
    fn assert_no_nulls(
        &self,
        output: &CapturedOutput,
        config: &NoNullsAssertion,
    ) -> AssertionResult {
        let fields_to_check: Vec<&str> = if config.fields.is_empty() {
            // Check all fields
            if output.records.is_empty() {
                return AssertionResult::pass("no_nulls", "No records to check");
            }
            output.records[0].keys().map(|s| s.as_str()).collect()
        } else {
            config.fields.iter().map(|s| s.as_str()).collect()
        };

        let mut null_fields = Vec::new();

        for field in &fields_to_check {
            for (idx, record) in output.records.iter().enumerate() {
                if let Some(FieldValue::Null) = record.get(*field) {
                    null_fields.push(format!("{}[{}]", field, idx));
                }
            }
        }

        if null_fields.is_empty() {
            AssertionResult::pass(
                "no_nulls",
                &format!("No null values in: {}", fields_to_check.join(", ")),
            )
        } else {
            AssertionResult::fail(
                "no_nulls",
                "Found null values",
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
        let allowed: std::collections::HashSet<_> = config.values.iter().collect();
        let mut invalid_values = Vec::new();

        for (idx, record) in output.records.iter().enumerate() {
            if let Some(value) = record.get(&config.field) {
                let value_str = field_value_to_string(value);
                if !allowed.contains(&value_str) {
                    invalid_values.push(format!("[{}]={}", idx, value_str));
                }
            }
        }

        if invalid_values.is_empty() {
            AssertionResult::pass(
                "field_in_set",
                &format!("All '{}' values in allowed set", config.field),
            )
        } else {
            AssertionResult::fail(
                "field_in_set",
                &format!("Field '{}' has invalid values", config.field),
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
        let mut failures = Vec::new();

        for (idx, record) in output.records.iter().enumerate() {
            if let Some(value) = record.get(&config.field) {
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
                    "All '{}' values satisfy {:?}",
                    config.field, config.operator
                ),
            )
        } else {
            AssertionResult::fail(
                "field_values",
                &format!("Field '{}' has values that don't match", config.field),
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
        let values: Vec<f64> = output
            .records
            .iter()
            .filter_map(|r| r.get(&config.field))
            .filter_map(|v| field_value_to_f64(v))
            .collect();

        if values.is_empty() {
            return AssertionResult::fail(
                "aggregate_check",
                &format!("No values found for field '{}'", config.field),
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
                    "{:?}({}) = {} (expected: {})",
                    config.function, config.field, actual, expected
                ),
            )
        } else {
            AssertionResult::fail(
                "aggregate_check",
                &format!("{:?}({}) mismatch", config.function, config.field),
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
        let left_source = config
            .left_source
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("left");

        // Get the right source name (default to "right" if not specified)
        let right_source = config
            .right_source
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("right");

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
            .as_ref()
            .map(|s| s.as_str())
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
            if let Some(value) = record.get(field) {
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
                if let Some(value) = ctx.record.get(field) {
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
                            if let Some(value) = ctx.records[index].get(field) {
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
    records: &'a [HashMap<String, FieldValue>],
    context: &'a AssertionContext,
}

/// Record iteration context
struct RecordContext<'a> {
    item_name: &'a str,
    record: &'a HashMap<String, FieldValue>,
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

/// Convert FieldValue to string for comparison
fn field_value_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Null => "null".to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::String(s) => s.clone(),
        FieldValue::Timestamp(ts) => ts.to_string(),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            format!("{:.prec$}", *v as f64 / scale as f64, prec = *s as usize)
        }
        _ => format!("{:?}", value),
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

/// Compare FieldValue with operator
fn compare_field_value(
    value: &FieldValue,
    operator: &ComparisonOperator,
    expected: &serde_yaml::Value,
) -> bool {
    let value_str = field_value_to_string(value);

    match operator {
        ComparisonOperator::Equals => {
            if let Some(exp) = expected.as_str() {
                value_str == exp
            } else if let Some(exp) = expected.as_i64() {
                field_value_to_f64(value).map_or(false, |v| (v - exp as f64).abs() < 0.0001)
            } else if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).map_or(false, |v| (v - exp).abs() < 0.0001)
            } else {
                false
            }
        }
        ComparisonOperator::NotEquals => {
            !compare_field_value(value, &ComparisonOperator::Equals, expected)
        }
        ComparisonOperator::GreaterThan => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).map_or(false, |v| v > exp)
            } else {
                false
            }
        }
        ComparisonOperator::LessThan => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).map_or(false, |v| v < exp)
            } else {
                false
            }
        }
        ComparisonOperator::GreaterThanOrEquals => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).map_or(false, |v| v >= exp)
            } else {
                false
            }
        }
        ComparisonOperator::LessThanOrEquals => {
            if let Some(exp) = expected.as_f64() {
                field_value_to_f64(value).map_or(false, |v| v <= exp)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_output(records: Vec<HashMap<String, FieldValue>>) -> CapturedOutput {
        CapturedOutput {
            query_name: "test_query".to_string(),
            sink_name: "test_sink".to_string(),
            records,
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
            ("value".to_string(), FieldValue::Float(3.14)),
        ])]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string(), "name".to_string()],
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
        };

        let result = runner.assert_schema_contains(&output, &config);
        assert!(!result.passed);
        assert!(result.actual.unwrap().contains("missing_field"));
    }

    #[test]
    fn test_schema_contains_empty_records() {
        let output = create_test_output(vec![]);

        let runner = AssertionRunner::new();
        let config = SchemaContainsAssertion {
            fields: vec!["id".to_string()],
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
        assert_eq!(field_value_to_string(&FieldValue::Null), "null");
        assert_eq!(field_value_to_string(&FieldValue::Boolean(true)), "true");
        assert_eq!(field_value_to_string(&FieldValue::Integer(42)), "42");
        assert_eq!(field_value_to_string(&FieldValue::Float(3.14)), "3.14");
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
        assert_eq!(field_value_to_f64(&FieldValue::Float(3.14)), Some(3.14));
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
}
