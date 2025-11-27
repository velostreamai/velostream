//! Test specification parsing and validation
//!
//! Defines the test_spec.yaml format for configuring test execution:
//! - Query definitions with inputs and assertions
//! - Input chaining from previous outputs
//! - Assertion configuration

use super::error::{TestHarnessError, TestHarnessResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Test specification for a SQL application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSpec {
    /// Application name
    pub application: String,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,

    /// Default timeout per query in milliseconds
    #[serde(default = "default_timeout")]
    pub default_timeout_ms: u64,

    /// Default record count for data generation
    #[serde(default = "default_records")]
    pub default_records: usize,

    /// Global configuration overrides
    #[serde(default)]
    pub config: HashMap<String, String>,

    /// Query test definitions
    pub queries: Vec<QueryTest>,
}

fn default_timeout() -> u64 {
    30000
}

fn default_records() -> usize {
    1000
}

/// Test definition for a single query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTest {
    /// Query name (must match CREATE STREAM name in SQL)
    pub name: String,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,

    /// Whether to skip this query
    #[serde(default)]
    pub skip: bool,

    /// Input configuration
    pub inputs: Vec<InputConfig>,

    /// Assertions to run on output
    pub assertions: Vec<AssertionConfig>,

    /// Override timeout for this query
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Input configuration for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    /// Source name (from SQL)
    pub source: String,

    /// Schema to use for data generation
    #[serde(default)]
    pub schema: Option<String>,

    /// Number of records to generate
    #[serde(default)]
    pub records: Option<usize>,

    /// Use output from previous query as input
    #[serde(default)]
    pub from_previous: Option<String>,

    /// Static data file (CSV/JSON)
    #[serde(default)]
    pub data_file: Option<String>,
}

/// Assertion configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AssertionConfig {
    /// Check record count
    #[serde(rename = "record_count")]
    RecordCount(RecordCountAssertion),

    /// Check schema contains required fields
    #[serde(rename = "schema_contains")]
    SchemaContains(SchemaContainsAssertion),

    /// Check no null values in field
    #[serde(rename = "no_nulls")]
    NoNulls(NoNullsAssertion),

    /// Check field values against set
    #[serde(rename = "field_in_set")]
    FieldInSet(FieldInSetAssertion),

    /// Check field values with operator
    #[serde(rename = "field_values")]
    FieldValues(FieldValuesAssertion),

    /// Check aggregate value
    #[serde(rename = "aggregate_check")]
    AggregateCheck(AggregateCheckAssertion),

    /// Check JOIN coverage
    #[serde(rename = "join_coverage")]
    JoinCoverage(JoinCoverageAssertion),

    /// Custom template assertion
    #[serde(rename = "template")]
    Template(TemplateAssertion),

    /// Execution time constraint
    #[serde(rename = "execution_time")]
    ExecutionTime(ExecutionTimeAssertion),

    /// Memory usage constraint
    #[serde(rename = "memory_usage")]
    MemoryUsage(MemoryUsageAssertion),
}

/// Record count assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordCountAssertion {
    /// Expected count (exact match)
    #[serde(default)]
    pub equals: Option<usize>,

    /// Minimum count
    #[serde(default)]
    pub greater_than: Option<usize>,

    /// Maximum count
    #[serde(default)]
    pub less_than: Option<usize>,

    /// Range (min, max)
    #[serde(default)]
    pub between: Option<(usize, usize)>,

    /// Template expression for expected value
    #[serde(default)]
    pub expression: Option<String>,
}

/// Schema contains assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaContainsAssertion {
    /// Required fields
    pub fields: Vec<String>,
}

/// No nulls assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoNullsAssertion {
    /// Fields to check (empty = all fields)
    #[serde(default)]
    pub fields: Vec<String>,
}

/// Field in set assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInSetAssertion {
    /// Field name
    pub field: String,

    /// Allowed values
    pub values: Vec<String>,
}

/// Field values assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValuesAssertion {
    /// Field name
    pub field: String,

    /// Operator
    pub operator: ComparisonOperator,

    /// Value to compare
    pub value: serde_yaml::Value,
}

/// Comparison operators
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEquals,
    LessThanOrEquals,
    Contains,
    StartsWith,
    EndsWith,
    Matches, // regex
}

/// Aggregate check assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateCheckAssertion {
    /// Aggregate function
    pub function: AggregateFunction,

    /// Field to aggregate
    pub field: String,

    /// Expected value or expression
    pub expected: String,

    /// Tolerance for floating point comparisons
    #[serde(default)]
    pub tolerance: Option<f64>,
}

/// Aggregate functions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// JOIN coverage assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCoverageAssertion {
    /// Minimum match rate (0.0 to 1.0)
    pub min_match_rate: f64,

    /// Left side source
    #[serde(default)]
    pub left_source: Option<String>,

    /// Right side source
    #[serde(default)]
    pub right_source: Option<String>,
}

/// Template assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateAssertion {
    /// Template expression (Jinja-like)
    pub expression: String,

    /// Description of what this checks
    #[serde(default)]
    pub description: Option<String>,
}

/// Execution time assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTimeAssertion {
    /// Maximum allowed execution time in milliseconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<u64>,

    /// Minimum required execution time in milliseconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_ms: Option<u64>,
}

/// Memory usage assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUsageAssertion {
    /// Maximum allowed peak memory in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,

    /// Maximum allowed peak memory in megabytes (convenience)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_mb: Option<f64>,

    /// Maximum allowed memory growth in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_growth_bytes: Option<i64>,
}

impl TestSpec {
    /// Load test spec from YAML file
    pub fn from_file(path: impl AsRef<Path>) -> TestHarnessResult<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        Self::from_yaml(&content, path.display().to_string())
    }

    /// Parse test spec from YAML string
    pub fn from_yaml(yaml: &str, file_name: String) -> TestHarnessResult<Self> {
        serde_yaml::from_str(yaml).map_err(|e| TestHarnessError::SpecParseError {
            message: e.to_string(),
            file: file_name,
        })
    }

    /// Validate the test spec
    pub fn validate(&self) -> TestHarnessResult<()> {
        // Check for duplicate query names
        let mut seen = std::collections::HashSet::new();
        for query in &self.queries {
            if !seen.insert(&query.name) {
                return Err(TestHarnessError::SpecParseError {
                    message: format!("Duplicate query name: {}", query.name),
                    file: self.application.clone(),
                });
            }
        }

        // Validate from_previous references
        let query_names: std::collections::HashSet<_> =
            self.queries.iter().map(|q| &q.name).collect();

        for query in &self.queries {
            for input in &query.inputs {
                if let Some(ref prev) = input.from_previous {
                    if !query_names.contains(prev) {
                        return Err(TestHarnessError::SpecParseError {
                            message: format!(
                                "Query '{}': from_previous references unknown query '{}'",
                                query.name, prev
                            ),
                            file: self.application.clone(),
                        });
                    }
                }
            }
        }

        // Validate JOIN coverage assertions
        for query in &self.queries {
            for assertion in &query.assertions {
                if let AssertionConfig::JoinCoverage(jc) = assertion {
                    if jc.min_match_rate < 0.0 || jc.min_match_rate > 1.0 {
                        return Err(TestHarnessError::SpecParseError {
                            message: format!(
                                "Query '{}': join_coverage min_match_rate must be between 0.0 and 1.0",
                                query.name
                            ),
                            file: self.application.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Get query by name
    pub fn get_query(&self, name: &str) -> Option<&QueryTest> {
        self.queries.iter().find(|q| q.name == name)
    }

    /// Get queries in execution order (respecting dependencies)
    pub fn execution_order(&self) -> Vec<&QueryTest> {
        // TODO: Implement topological sort based on from_previous dependencies
        // For now, return in definition order
        self.queries.iter().filter(|q| !q.skip).collect()
    }
}
