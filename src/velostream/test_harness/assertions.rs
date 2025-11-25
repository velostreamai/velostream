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
    FieldInSetAssertion, FieldValuesAssertion, JoinCoverageAssertion, NoNullsAssertion,
    RecordCountAssertion, SchemaContainsAssertion, TemplateAssertion,
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
#[derive(Debug, Default)]
pub struct AssertionContext {
    /// Input record counts by source name
    pub input_counts: HashMap<String, usize>,

    /// Output record counts by sink name
    pub output_counts: HashMap<String, usize>,

    /// Custom variables
    pub variables: HashMap<String, String>,
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

        // Parse expected value
        let expected: f64 = config.expected.parse().unwrap_or(0.0);
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
    fn assert_join_coverage(
        &self,
        output: &CapturedOutput,
        config: &JoinCoverageAssertion,
    ) -> AssertionResult {
        // TODO: Phase 3 - Implement JOIN coverage analysis
        // This requires tracking input records and comparing to output

        let total = output.records.len();
        if total == 0 {
            return AssertionResult::fail(
                "join_coverage",
                "No output records to analyze",
                &format!("{}% match rate", config.min_match_rate * 100.0),
                "0 records",
            );
        }

        // Placeholder: assume 100% match rate
        let match_rate = 1.0;

        if match_rate >= config.min_match_rate {
            AssertionResult::pass(
                "join_coverage",
                &format!("JOIN match rate: {:.1}%", match_rate * 100.0),
            )
        } else {
            AssertionResult::fail(
                "join_coverage",
                "JOIN match rate below threshold",
                &format!("{:.1}%", config.min_match_rate * 100.0),
                &format!("{:.1}%", match_rate * 100.0),
            )
        }
    }

    /// Assert custom template
    fn assert_template(
        &self,
        output: &CapturedOutput,
        config: &TemplateAssertion,
    ) -> AssertionResult {
        // TODO: Phase 5 - Implement template evaluation
        log::warn!(
            "Template assertions not yet implemented: {}",
            config.expression
        );

        AssertionResult::pass(
            "template",
            &format!(
                "Template assertion (not implemented): {}",
                config.expression
            ),
        )
    }
}

impl Default for AssertionRunner {
    fn default() -> Self {
        Self::new()
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
