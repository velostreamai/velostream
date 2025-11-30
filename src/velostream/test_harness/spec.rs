//! Test specification parsing and validation
//!
//! Defines the test_spec.yaml format for configuring test execution:
//! - Query definitions with inputs and assertions
//! - Input chaining from previous outputs
//! - File-based sources and sinks (CSV, JSON)
//! - Assertion configuration

use super::error::{TestHarnessError, TestHarnessResult};
pub use crate::velostream::datasource::file::config::FileFormat;
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

    /// Output/sink configuration (optional - defaults to Kafka)
    /// Legacy format: single output
    #[serde(default)]
    pub output: Option<OutputConfig>,

    /// Multiple output configurations with per-sink assertions
    /// New format: list of sinks with their own assertions
    #[serde(default)]
    pub outputs: Vec<SinkOutputConfig>,

    /// Assertions to run on output (legacy format - top-level assertions)
    #[serde(default)]
    pub assertions: Vec<AssertionConfig>,

    /// Override timeout for this query
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Output configuration with per-sink assertions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkOutputConfig {
    /// Sink name (must match sink topic from SQL config)
    pub sink: String,

    /// Assertions specific to this sink
    #[serde(default)]
    pub assertions: Vec<AssertionConfig>,
}

impl QueryTest {
    /// Get all assertions for this query
    /// Combines top-level assertions with per-sink assertions
    pub fn all_assertions(&self) -> Vec<&AssertionConfig> {
        let mut all: Vec<&AssertionConfig> = self.assertions.iter().collect();
        for output in &self.outputs {
            all.extend(output.assertions.iter());
        }
        all
    }

    /// Get assertions for a specific sink
    pub fn assertions_for_sink(&self, sink_name: &str) -> Vec<&AssertionConfig> {
        // First check outputs for sink-specific assertions
        for output in &self.outputs {
            if output.sink == sink_name {
                return output.assertions.iter().collect();
            }
        }
        // Fall back to top-level assertions
        self.assertions.iter().collect()
    }

    /// Get all sink names configured for this query
    pub fn sink_names(&self) -> Vec<&str> {
        self.outputs.iter().map(|o| o.sink.as_str()).collect()
    }
}

/// Input configuration for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    /// Source name (from SQL)
    pub source: String,

    /// Source type configuration (defaults to Kafka if not specified)
    #[serde(default)]
    pub source_type: Option<SourceType>,

    /// Schema to use for data generation
    #[serde(default)]
    pub schema: Option<String>,

    /// Number of records to generate
    #[serde(default)]
    pub records: Option<usize>,

    /// Use output from previous query as input
    #[serde(default)]
    pub from_previous: Option<String>,

    /// Static data file (CSV/JSON) - shorthand for file source
    #[serde(default)]
    pub data_file: Option<String>,
}

/// Source type configuration for inputs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceType {
    /// Kafka topic source (default)
    Kafka {
        /// Optional topic override (defaults to source name)
        topic: Option<String>,
    },
    /// File-based source
    File {
        /// File path (relative to test spec or absolute)
        path: String,
        /// File format (csv, json) - required field
        format: FileFormat,
        /// Watch for file changes (for streaming tests)
        #[serde(default = "default_watch")]
        watch: bool,
    },
}

fn default_watch() -> bool {
    false
}

/// Output/sink configuration for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Sink type configuration
    #[serde(default)]
    pub sink_type: SinkType,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            sink_type: SinkType::Kafka { topic: None },
        }
    }
}

/// Sink type configuration for outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkType {
    /// Kafka topic sink (default)
    Kafka {
        /// Optional topic override (defaults to query name)
        topic: Option<String>,
    },
    /// File-based sink
    File {
        /// Output file path
        path: String,
        /// File format (csv, json) - required field
        format: FileFormat,
    },
}

impl Default for SinkType {
    fn default() -> Self {
        SinkType::Kafka { topic: None }
    }
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

    /// DLQ (Dead Letter Queue) count assertion
    #[serde(rename = "dlq_count")]
    DlqCount(DlqCountAssertion),

    /// Error rate assertion
    #[serde(rename = "error_rate")]
    ErrorRate(ErrorRateAssertion),

    /// No duplicates assertion
    #[serde(rename = "no_duplicates")]
    NoDuplicates(NoDuplicatesAssertion),

    /// Record ordering assertion
    #[serde(rename = "ordering")]
    Ordering(OrderingAssertion),

    /// Data completeness assertion
    #[serde(rename = "completeness")]
    Completeness(CompletenessAssertion),

    /// Table freshness assertion (for CTAS)
    #[serde(rename = "table_freshness")]
    TableFreshness(TableFreshnessAssertion),

    /// Data quality assertion
    #[serde(rename = "data_quality")]
    DataQuality(DataQualityAssertion),

    // ==================== File-specific assertions ====================
    /// File exists assertion - verifies output file was created
    #[serde(rename = "file_exists")]
    FileExists(FileExistsAssertion),

    /// File row count assertion - verifies number of rows in output file
    #[serde(rename = "file_row_count")]
    FileRowCount(FileRowCountAssertion),

    /// File contains assertion - verifies file contains specific values
    #[serde(rename = "file_contains")]
    FileContains(FileContainsAssertion),

    /// File matches assertion - verifies file matches expected content
    #[serde(rename = "file_matches")]
    FileMatches(FileMatchesAssertion),
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

/// DLQ count assertion - verifies error record counts in Dead Letter Queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqCountAssertion {
    /// DLQ topic to check (defaults to {output_topic}-dlq)
    #[serde(default)]
    pub topic: Option<String>,

    /// Expected exact count
    #[serde(default)]
    pub equals: Option<usize>,

    /// Maximum allowed errors
    #[serde(default)]
    pub max: Option<usize>,

    /// Minimum expected errors (for negative testing)
    #[serde(default)]
    pub min: Option<usize>,

    /// Expected error types (e.g., ["deserialization", "schema_validation"])
    #[serde(default)]
    pub error_types: Vec<String>,
}

/// Error rate assertion - checks error rate stays within bounds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRateAssertion {
    /// Maximum allowed error rate (0.0 to 1.0)
    #[serde(default)]
    pub max_rate: Option<f64>,

    /// Maximum allowed error percentage (0 to 100)
    #[serde(default)]
    pub max_percent: Option<f64>,

    /// Specific error types to count
    #[serde(default)]
    pub error_types: Vec<String>,

    /// Minimum records required for rate calculation
    #[serde(default = "default_min_records")]
    pub min_records: usize,
}

fn default_min_records() -> usize {
    10
}

/// No duplicates assertion - verifies uniqueness of records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoDuplicatesAssertion {
    /// Key fields to check for uniqueness
    pub key_fields: Vec<String>,

    /// Whether to allow duplicates with different values
    #[serde(default)]
    pub allow_updates: bool,

    /// Maximum allowed duplicate percentage (0 = no duplicates)
    #[serde(default)]
    pub max_duplicate_percent: Option<f64>,
}

/// Ordering assertion - verifies record ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingAssertion {
    /// Field to check ordering on
    pub field: String,

    /// Expected order direction
    #[serde(default)]
    pub direction: OrderDirection,

    /// Partition field (ordering is per-partition)
    #[serde(default)]
    pub partition_by: Option<String>,

    /// Allow equal values (for non-strict ordering)
    #[serde(default = "default_allow_equal")]
    pub allow_equal: bool,
}

fn default_allow_equal() -> bool {
    true
}

/// Order direction
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    #[default]
    Ascending,
    Descending,
}

/// Data completeness assertion - verifies no data loss
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletenessAssertion {
    /// Input source to compare against
    pub input_source: String,

    /// Key field to match records
    pub key_field: String,

    /// Minimum completeness rate (0.0 to 1.0)
    #[serde(default = "default_completeness")]
    pub min_completeness: f64,

    /// Fields that must be present in output
    #[serde(default)]
    pub required_fields: Vec<String>,
}

fn default_completeness() -> f64 {
    1.0
}

/// Table freshness assertion - for CTAS materialized tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFreshnessAssertion {
    /// Table name to check
    pub table: String,

    /// Maximum age of latest record in milliseconds
    #[serde(default)]
    pub max_age_ms: Option<u64>,

    /// Maximum lag behind source in milliseconds
    #[serde(default)]
    pub max_lag_ms: Option<u64>,

    /// Minimum record count in table
    #[serde(default)]
    pub min_records: Option<usize>,
}

/// Data quality assertion - comprehensive quality checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityAssertion {
    /// Check for null values in required fields
    #[serde(default)]
    pub no_nulls_in: Vec<String>,

    /// Check for empty strings in fields
    #[serde(default)]
    pub no_empty_strings_in: Vec<String>,

    /// Check numeric ranges
    #[serde(default)]
    pub numeric_ranges: Vec<NumericRangeCheck>,

    /// Check string patterns (regex)
    #[serde(default)]
    pub string_patterns: Vec<StringPatternCheck>,

    /// Check referential integrity
    #[serde(default)]
    pub referential_integrity: Vec<ReferentialIntegrityCheck>,

    /// Minimum quality score (0.0 to 1.0)
    #[serde(default)]
    pub min_quality_score: Option<f64>,
}

/// Numeric range check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericRangeCheck {
    /// Field to check
    pub field: String,

    /// Minimum value (inclusive)
    #[serde(default)]
    pub min: Option<f64>,

    /// Maximum value (inclusive)
    #[serde(default)]
    pub max: Option<f64>,
}

/// String pattern check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringPatternCheck {
    /// Field to check
    pub field: String,

    /// Regex pattern the value must match
    pub pattern: String,
}

/// Referential integrity check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferentialIntegrityCheck {
    /// Field in output to check
    pub field: String,

    /// Reference source (input topic or table)
    pub reference_source: String,

    /// Reference field that must contain the value
    pub reference_field: String,
}

// ==================== File-specific assertion structs ====================

/// File exists assertion - verifies output file was created
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileExistsAssertion {
    /// Path to file (can be relative to test spec directory or absolute)
    pub path: String,

    /// Minimum file size in bytes (optional)
    #[serde(default)]
    pub min_size_bytes: Option<u64>,

    /// Maximum file size in bytes (optional)
    #[serde(default)]
    pub max_size_bytes: Option<u64>,
}

/// File row count assertion - verifies number of rows in output file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRowCountAssertion {
    /// Path to file
    pub path: String,

    /// File format for parsing
    pub format: FileFormat,

    /// Expected exact row count
    #[serde(default)]
    pub equals: Option<usize>,

    /// Minimum row count
    #[serde(default)]
    pub greater_than: Option<usize>,

    /// Maximum row count
    #[serde(default)]
    pub less_than: Option<usize>,

    /// Whether to skip header row for CSV (default true for CSV format)
    #[serde(default)]
    pub skip_header: Option<bool>,
}

/// File contains assertion - verifies file contains specific values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileContainsAssertion {
    /// Path to file
    pub path: String,

    /// File format for parsing
    pub format: FileFormat,

    /// Field to check
    pub field: String,

    /// Expected values that must exist in the file
    pub expected_values: Vec<String>,

    /// Check mode: "all" (all values must be present) or "any" (at least one)
    #[serde(default = "default_contains_mode")]
    pub mode: ContainsMode,
}

fn default_contains_mode() -> ContainsMode {
    ContainsMode::All
}

/// Contains check mode
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainsMode {
    /// All expected values must be present
    #[default]
    All,
    /// At least one expected value must be present
    Any,
}

/// File matches assertion - verifies file matches expected content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMatchesAssertion {
    /// Path to actual output file
    pub actual_path: String,

    /// Path to expected file
    pub expected_path: String,

    /// File format for parsing
    pub format: FileFormat,

    /// Fields to compare (empty means all fields)
    #[serde(default)]
    pub compare_fields: Vec<String>,

    /// Fields to ignore in comparison
    #[serde(default)]
    pub ignore_fields: Vec<String>,

    /// Whether to ignore row order
    #[serde(default = "default_ignore_order")]
    pub ignore_order: bool,

    /// Tolerance for numeric comparisons
    #[serde(default)]
    pub numeric_tolerance: Option<f64>,
}

fn default_ignore_order() -> bool {
    true
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
                if let Some(ref prev) = input.from_previous
                    && !query_names.contains(prev)
                {
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

        // Validate JOIN coverage assertions
        for query in &self.queries {
            for assertion in &query.assertions {
                if let AssertionConfig::JoinCoverage(jc) = assertion
                    && (jc.min_match_rate < 0.0 || jc.min_match_rate > 1.0)
                {
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
