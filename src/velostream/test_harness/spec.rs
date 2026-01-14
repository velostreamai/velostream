//! Test specification parsing and validation
//!
//! Defines the test_spec.yaml format for configuring test execution:
//! - Query definitions with inputs and assertions
//! - Input chaining from previous outputs
//! - File-based sources and sinks (CSV, JSON)
//! - Assertion configuration

use super::capture::CaptureFormat;
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

    /// Topic naming configuration for CI/CD isolation
    #[serde(default)]
    pub topic_naming: Option<TopicNamingConfig>,

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

// =============================================================================
// Topic Naming Configuration (P1.2)
// =============================================================================

/// Topic naming configuration for CI/CD isolation and customization
///
/// Allows pattern-based topic naming with placeholders for dynamic values.
/// Useful for CI/CD pipelines, branch isolation, and multi-tenant testing.
///
/// # Supported Placeholders
/// - `{run_id}` - Unique run identifier (auto-generated)
/// - `{branch}` - Git branch name (from env or config)
/// - `{user}` - User/CI identity (from env or config)
/// - `{timestamp}` - Unix timestamp
/// - `{namespace}` - Custom namespace prefix
/// - `{base}` - Original topic name
///
/// # Example
/// ```yaml
/// topic_naming:
///   pattern: "test_{branch}_{run_id}_{base}"
///   namespace: "ci"
///   branch_from_env: "GITHUB_REF_NAME"
///   user_from_env: "GITHUB_ACTOR"
///   hash_long_names: true
///   max_topic_length: 249
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicNamingConfig {
    /// Topic name pattern with placeholders
    /// Default: "test_{run_id}_{base}"
    #[serde(default = "default_topic_pattern")]
    pub pattern: String,

    /// Custom namespace prefix (replaces {namespace} placeholder)
    #[serde(default)]
    pub namespace: Option<String>,

    /// Environment variable for branch name (default: GITHUB_REF_NAME)
    #[serde(default = "default_branch_env")]
    pub branch_from_env: String,

    /// Environment variable for user identity (default: GITHUB_ACTOR)
    #[serde(default = "default_user_env")]
    pub user_from_env: String,

    /// Static branch name override (takes precedence over env)
    #[serde(default)]
    pub branch: Option<String>,

    /// Static user name override (takes precedence over env)
    #[serde(default)]
    pub user: Option<String>,

    /// Hash topic names longer than max_topic_length
    #[serde(default = "default_hash_long_names")]
    pub hash_long_names: bool,

    /// Maximum topic name length (Kafka default: 249)
    #[serde(default = "default_max_topic_length")]
    pub max_topic_length: usize,
}

fn default_topic_pattern() -> String {
    "test_{run_id}_{base}".to_string()
}

fn default_branch_env() -> String {
    "GITHUB_REF_NAME".to_string()
}

fn default_user_env() -> String {
    "GITHUB_ACTOR".to_string()
}

fn default_hash_long_names() -> bool {
    true
}

fn default_max_topic_length() -> usize {
    249
}

impl Default for TopicNamingConfig {
    fn default() -> Self {
        Self {
            pattern: default_topic_pattern(),
            namespace: None,
            branch_from_env: default_branch_env(),
            user_from_env: default_user_env(),
            branch: None,
            user: None,
            hash_long_names: default_hash_long_names(),
            max_topic_length: default_max_topic_length(),
        }
    }
}

impl TopicNamingConfig {
    /// Create a new config with custom pattern
    pub fn with_pattern(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            ..Default::default()
        }
    }

    /// Create a config for CI/CD with branch isolation
    pub fn ci_pattern() -> Self {
        Self {
            pattern: "test_{branch}_{run_id}_{base}".to_string(),
            ..Default::default()
        }
    }

    /// Create a config with namespace prefix
    pub fn with_namespace(namespace: &str) -> Self {
        Self {
            pattern: "{namespace}_{run_id}_{base}".to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        }
    }

    /// Resolve a topic name using this configuration
    pub fn resolve_topic_name(&self, base_name: &str, run_id: &str) -> String {
        let mut topic = self.pattern.clone();

        // Replace placeholders
        topic = topic.replace("{run_id}", run_id);
        topic = topic.replace("{base}", base_name);

        // Replace {timestamp}
        if topic.contains("{timestamp}") {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            topic = topic.replace("{timestamp}", &ts.to_string());
        }

        // Replace {branch} - from static config or env
        if topic.contains("{branch}") {
            let branch = self
                .branch
                .clone()
                .or_else(|| std::env::var(&self.branch_from_env).ok())
                .unwrap_or_else(|| "local".to_string());
            // Sanitize branch name for Kafka topic (replace / and other invalid chars)
            let branch = sanitize_topic_component(&branch);
            topic = topic.replace("{branch}", &branch);
        }

        // Replace {user} - from static config or env
        if topic.contains("{user}") {
            let user = self
                .user
                .clone()
                .or_else(|| std::env::var(&self.user_from_env).ok())
                .unwrap_or_else(|| "unknown".to_string());
            let user = sanitize_topic_component(&user);
            topic = topic.replace("{user}", &user);
        }

        // Replace {namespace}
        if topic.contains("{namespace}") {
            let ns = self.namespace.as_deref().unwrap_or("test");
            topic = topic.replace("{namespace}", ns);
        }

        // Handle long topic names
        if self.hash_long_names && topic.len() > self.max_topic_length {
            topic = hash_long_topic_name(&topic, self.max_topic_length);
        }

        topic
    }
}

/// Sanitize a string for use in Kafka topic names
/// Kafka topics can contain: a-z, A-Z, 0-9, '.', '_', '-'
fn sanitize_topic_component(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Hash a long topic name to fit within max length
fn hash_long_topic_name(topic: &str, max_len: usize) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    let hash = hasher.finish();

    // Keep prefix and add hash suffix
    let hash_suffix = format!("_{:016x}", hash);
    let prefix_len = max_len.saturating_sub(hash_suffix.len());
    let prefix = &topic[..prefix_len.min(topic.len())];

    format!("{}{}", prefix, hash_suffix)
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

    /// Dependencies that must be executed before this query (in order)
    /// These are names of other statements in the SQL file (e.g., reference tables)
    #[serde(default)]
    pub dependencies: Vec<String>,

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

    /// Serialization format for capturing output (json, avro, protobuf)
    /// Required for non-JSON serializers to properly deserialize captured messages
    #[serde(default)]
    pub capture_format: CaptureFormat,

    /// Schema JSON/definition for Avro/Protobuf capture deserialization
    /// Can be inline JSON or a path to schema file (relative to test spec)
    #[serde(default)]
    pub capture_schema: Option<String>,
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

    /// Time simulation configuration for controlled event streaming
    /// Enables rate-limited publishing and sequential timestamps
    #[serde(default)]
    pub time_simulation: Option<TimeSimulationConfig>,

    /// Field to use as Kafka message key (optional)
    /// Overrides key_field from schema if specified
    #[serde(default)]
    pub key_field: Option<String>,
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

// =============================================================================
// Time Simulation Configuration
// =============================================================================

/// Time simulation configuration for realistic event streaming
///
/// Enables controlled event timing and timestamp progression for testing
/// streaming applications with realistic data patterns.
///
/// # Example
/// ```yaml
/// inputs:
///   - source: market_data
///     records: 10000
///     time_simulation:
///       start_time: "-4h"        # Start 4 hours ago
///       time_scale: 10.0         # 10x acceleration
///       events_per_second: 100   # Publish rate
///       sequential: true         # Ordered timestamps
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSimulationConfig {
    /// Start time for event timestamps.
    /// Supports relative format ("-4h", "-30m", "-1d") or absolute ISO 8601.
    /// Defaults to current time if not specified.
    #[serde(default)]
    pub start_time: Option<String>,

    /// End time for event timestamps.
    /// Supports relative format ("now", "+1h") or absolute ISO 8601.
    /// Defaults to "now" if not specified.
    #[serde(default)]
    pub end_time: Option<String>,

    /// Time acceleration factor.
    /// - 1.0 = real-time (wall-clock matches simulated time)
    /// - 10.0 = 10x faster (4 hours of events in 24 minutes)
    /// - 0.5 = half speed (slow motion)
    #[serde(default = "default_time_scale")]
    pub time_scale: f64,

    /// Target events per second (actual publish rate).
    /// If not set, publishes as fast as possible (bulk mode).
    #[serde(default)]
    pub events_per_second: Option<f64>,

    /// Generate sequential timestamps (each timestamp > previous).
    /// When true, timestamps progress linearly through the time range.
    /// When false, timestamps are randomly distributed (legacy behavior).
    #[serde(default = "default_sequential")]
    pub sequential: bool,

    /// Random jitter in milliseconds (±jitter_ms per event).
    /// Adds realistic variation to publish timing.
    #[serde(default)]
    pub jitter_ms: Option<u64>,

    /// Batch size for publishing (publish N events together, then delay).
    /// Useful for simulating burst patterns.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_time_scale() -> f64 {
    1.0
}

fn default_sequential() -> bool {
    true
}

fn default_batch_size() -> usize {
    1
}

impl Default for TimeSimulationConfig {
    fn default() -> Self {
        Self {
            start_time: None,
            end_time: None,
            time_scale: default_time_scale(),
            events_per_second: None,
            sequential: default_sequential(),
            jitter_ms: None,
            batch_size: default_batch_size(),
        }
    }
}

impl TimeSimulationConfig {
    /// Calculate the inter-event delay for rate-controlled publishing
    pub fn inter_event_delay(&self) -> Option<std::time::Duration> {
        self.events_per_second.map(|eps| {
            let delay_secs = (self.batch_size as f64) / eps;
            std::time::Duration::from_secs_f64(delay_secs)
        })
    }

    /// Check if rate limiting is enabled
    pub fn has_rate_limit(&self) -> bool {
        self.events_per_second.is_some()
    }

    /// Check if sequential timestamps are enabled
    pub fn is_sequential(&self) -> bool {
        self.sequential
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

    /// Throughput rate constraint
    #[serde(rename = "throughput")]
    Throughput(ThroughputAssertion),

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

    // ==================== Temporal & Statistical assertions (P1.3) ====================
    /// Window boundary assertion - verifies records fall in correct time windows
    #[serde(rename = "window_boundary")]
    WindowBoundary(WindowBoundaryAssertion),

    /// Latency assertion - verifies end-to-end processing latency bounds
    #[serde(rename = "latency")]
    Latency(LatencyAssertion),

    /// Distribution assertion - verifies output matches expected statistical distribution
    #[serde(rename = "distribution")]
    Distribution(DistributionAssertion),

    /// Percentile assertion - verifies field values against percentile thresholds
    #[serde(rename = "percentile")]
    Percentile(PercentileAssertion),

    /// Event time ordering assertion - verifies ordering within time windows
    #[serde(rename = "event_ordering")]
    EventOrdering(EventOrderingAssertion),
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
    /// Required fields in the message value payload
    pub fields: Vec<String>,

    /// Expected Kafka message key field name (optional)
    /// When specified, verifies that message keys match this field's expected values
    #[serde(default)]
    pub key_field: Option<String>,
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

/// Throughput assertion - verifies processing rate meets requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputAssertion {
    /// Minimum required records per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_records_per_second: Option<f64>,

    /// Maximum allowed records per second (for rate limiting tests)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_second: Option<f64>,

    /// Expected approximate records per second (with tolerance)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_records_per_second: Option<f64>,

    /// Tolerance percentage for expected rate (default 20%)
    #[serde(default = "default_throughput_tolerance")]
    pub tolerance_percent: f64,
}

fn default_throughput_tolerance() -> f64 {
    20.0
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

// =============================================================================
// Temporal & Statistical Assertions (P1.3)
// =============================================================================

/// Window boundary assertion - verifies records fall in correct time windows
///
/// # Example
/// ```yaml
/// assertions:
///   - type: window_boundary
///     timestamp_field: event_time
///     window_start_field: window_start
///     window_end_field: window_end
///     window_size_ms: 60000  # 1 minute tumbling window
///     tolerance_ms: 1000
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowBoundaryAssertion {
    /// Field containing the event timestamp
    pub timestamp_field: String,

    /// Field containing the window start time (from windowed aggregation)
    #[serde(default)]
    pub window_start_field: Option<String>,

    /// Field containing the window end time (from windowed aggregation)
    #[serde(default)]
    pub window_end_field: Option<String>,

    /// Expected window size in milliseconds
    #[serde(default)]
    pub window_size_ms: Option<u64>,

    /// Tolerance for boundary checks in milliseconds
    #[serde(default = "default_boundary_tolerance")]
    pub tolerance_ms: u64,

    /// Check that all events fall within their assigned window
    #[serde(default = "default_true")]
    pub verify_containment: bool,

    /// Check that window boundaries are aligned to expected intervals
    #[serde(default)]
    pub verify_alignment: bool,
}

fn default_boundary_tolerance() -> u64 {
    0
}

fn default_true() -> bool {
    true
}

/// Latency assertion - verifies end-to-end processing latency
///
/// # Example
/// ```yaml
/// assertions:
///   - type: latency
///     timestamp_field: event_time
///     max_latency_ms: 5000
///     p99_latency_ms: 3000
///     p95_latency_ms: 2000
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyAssertion {
    /// Field containing the event timestamp (for latency calculation)
    pub timestamp_field: String,

    /// Maximum allowed latency in milliseconds
    #[serde(default)]
    pub max_latency_ms: Option<u64>,

    /// P99 latency threshold in milliseconds
    #[serde(default)]
    pub p99_latency_ms: Option<u64>,

    /// P95 latency threshold in milliseconds
    #[serde(default)]
    pub p95_latency_ms: Option<u64>,

    /// P50 (median) latency threshold in milliseconds
    #[serde(default)]
    pub p50_latency_ms: Option<u64>,

    /// Average latency threshold in milliseconds
    #[serde(default)]
    pub avg_latency_ms: Option<u64>,

    /// Minimum records required for latency calculation
    #[serde(default = "default_latency_min_records")]
    pub min_records: usize,
}

fn default_latency_min_records() -> usize {
    10
}

/// Distribution assertion - verifies output matches expected statistical distribution
///
/// # Example
/// ```yaml
/// assertions:
///   - type: distribution
///     field: price
///     distribution_type: normal
///     mean: 100.0
///     std_dev: 15.0
///     tolerance: 0.1
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionAssertion {
    /// Field to analyze
    pub field: String,

    /// Expected distribution type
    pub distribution_type: DistributionType,

    /// Expected mean (for normal distribution)
    #[serde(default)]
    pub mean: Option<f64>,

    /// Expected standard deviation (for normal distribution)
    #[serde(default)]
    pub std_dev: Option<f64>,

    /// Expected minimum value (for uniform distribution)
    #[serde(default)]
    pub min_value: Option<f64>,

    /// Expected maximum value (for uniform distribution)
    #[serde(default)]
    pub max_value: Option<f64>,

    /// Tolerance for distribution parameters (0.0 to 1.0)
    #[serde(default = "default_distribution_tolerance")]
    pub tolerance: f64,

    /// Minimum records required for distribution analysis
    #[serde(default = "default_distribution_min_records")]
    pub min_records: usize,
}

fn default_distribution_tolerance() -> f64 {
    0.1
}

fn default_distribution_min_records() -> usize {
    100
}

/// Distribution type for statistical validation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionType {
    /// Normal (Gaussian) distribution
    Normal,
    /// Uniform distribution
    Uniform,
    /// Exponential distribution
    Exponential,
    /// Custom distribution with explicit percentile bounds
    Custom,
}

/// Percentile assertion - verifies field values against percentile thresholds
///
/// # Example
/// ```yaml
/// assertions:
///   - type: percentile
///     field: response_time
///     percentiles:
///       p50: 100  # median should be <= 100ms
///       p95: 500  # 95th percentile should be <= 500ms
///       p99: 1000 # 99th percentile should be <= 1000ms
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentileAssertion {
    /// Field to analyze
    pub field: String,

    /// P50 (median) threshold
    #[serde(default)]
    pub p50: Option<f64>,

    /// P75 threshold
    #[serde(default)]
    pub p75: Option<f64>,

    /// P90 threshold
    #[serde(default)]
    pub p90: Option<f64>,

    /// P95 threshold
    #[serde(default)]
    pub p95: Option<f64>,

    /// P99 threshold
    #[serde(default)]
    pub p99: Option<f64>,

    /// P99.9 threshold
    #[serde(default)]
    pub p999: Option<f64>,

    /// Comparison mode: "less_than" (values must be below threshold) or "greater_than"
    #[serde(default = "default_percentile_mode")]
    pub mode: PercentileMode,

    /// Minimum records required for percentile calculation
    #[serde(default = "default_percentile_min_records")]
    pub min_records: usize,
}

fn default_percentile_mode() -> PercentileMode {
    PercentileMode::LessThan
}

fn default_percentile_min_records() -> usize {
    10
}

/// Percentile comparison mode
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PercentileMode {
    /// Percentile values must be less than or equal to threshold
    #[default]
    LessThan,
    /// Percentile values must be greater than or equal to threshold
    GreaterThan,
}

/// Event ordering assertion - verifies ordering within time windows
///
/// # Example
/// ```yaml
/// assertions:
///   - type: event_ordering
///     timestamp_field: event_time
///     partition_field: symbol
///     window_size_ms: 60000
///     direction: ascending
///     allow_gaps: false
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventOrderingAssertion {
    /// Field containing the event timestamp
    pub timestamp_field: String,

    /// Field to partition by (ordering is checked within each partition)
    #[serde(default)]
    pub partition_field: Option<String>,

    /// Window size for ordering check (if windowed)
    #[serde(default)]
    pub window_size_ms: Option<u64>,

    /// Expected ordering direction
    #[serde(default)]
    pub direction: OrderDirection,

    /// Allow gaps in sequence (non-consecutive timestamps)
    #[serde(default = "default_allow_gaps")]
    pub allow_gaps: bool,

    /// Maximum allowed out-of-order percentage
    #[serde(default)]
    pub max_out_of_order_percent: Option<f64>,

    /// Maximum allowed time gap between consecutive events in milliseconds
    #[serde(default)]
    pub max_gap_ms: Option<u64>,
}

fn default_allow_gaps() -> bool {
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
    ///
    /// Uses topological sort to order queries such that any query with
    /// `from_previous` dependencies comes after its dependencies.
    pub fn execution_order(&self) -> Vec<&QueryTest> {
        use std::collections::{HashMap, HashSet, VecDeque};

        // Build query map for quick lookup
        let query_map: HashMap<_, _> = self
            .queries
            .iter()
            .filter(|q| !q.skip)
            .map(|q| (q.name.as_str(), q))
            .collect();

        // Build dependency list for each query
        let mut dependencies: HashMap<&str, Vec<&str>> = HashMap::new();
        for query in self.queries.iter().filter(|q| !q.skip) {
            let deps: Vec<&str> = query
                .inputs
                .iter()
                .filter_map(|input| input.from_previous.as_ref())
                .filter(|prev| query_map.contains_key(prev.as_str()))
                .map(String::as_str)
                .collect();
            dependencies.insert(query.name.as_str(), deps);
        }

        // Calculate in-degree for each node (number of dependencies)
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        for (name, deps) in &dependencies {
            in_degree.insert(name, deps.len());
        }

        // Build reverse map: who depends on each query
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for (name, deps) in &dependencies {
            for dep in deps {
                dependents.entry(dep).or_default().push(name);
            }
        }

        // Start with queries that have no dependencies
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(name, _)| *name)
            .collect();

        let mut result: Vec<&QueryTest> = Vec::new();
        let mut visited: HashSet<&str> = HashSet::new();

        while let Some(name) = queue.pop_front() {
            if visited.contains(name) {
                continue;
            }
            visited.insert(name);

            if let Some(query) = query_map.get(name) {
                result.push(query);
            }

            // Decrement in-degree for dependent queries
            if let Some(deps) = dependents.get(name) {
                for dependent in deps {
                    if let Some(deg) = in_degree.get_mut(dependent) {
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 && !visited.contains(dependent) {
                            queue.push_back(dependent);
                        }
                    }
                }
            }
        }

        // Handle cycles: add any remaining queries in definition order
        if result.len() < query_map.len() {
            log::warn!(
                "Dependency cycle detected in query graph. {} of {} queries ordered.",
                result.len(),
                query_map.len()
            );
            for query in self.queries.iter().filter(|q| !q.skip) {
                if !visited.contains(query.name.as_str()) {
                    result.push(query);
                }
            }
        }

        result
    }

    /// Get the dependency graph as a map of query name -> list of dependencies
    pub fn dependency_graph(&self) -> std::collections::HashMap<String, Vec<String>> {
        let mut graph = std::collections::HashMap::new();

        for query in &self.queries {
            let deps: Vec<String> = query
                .inputs
                .iter()
                .filter_map(|input| input.from_previous.clone())
                .collect();
            graph.insert(query.name.clone(), deps);
        }

        graph
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_simple_query(name: &str) -> QueryTest {
        QueryTest {
            name: name.to_string(),
            description: None,
            skip: false,
            dependencies: Vec::new(),
            inputs: Vec::new(),
            output: None,
            outputs: Vec::new(),
            assertions: Vec::new(),
            timeout_ms: None,
            capture_format: Default::default(),
            capture_schema: None,
        }
    }

    fn create_query_with_deps(name: &str, deps: Vec<&str>) -> QueryTest {
        QueryTest {
            name: name.to_string(),
            description: None,
            skip: false,
            dependencies: deps.iter().map(|s| s.to_string()).collect(),
            inputs: deps
                .into_iter()
                .map(|d| InputConfig {
                    source: format!("{}_input", d),
                    source_type: None,
                    schema: None,
                    records: None,
                    from_previous: Some(d.to_string()),
                    data_file: None,
                    time_simulation: None,
                    key_field: None,
                })
                .collect(),
            output: None,
            outputs: Vec::new(),
            assertions: Vec::new(),
            timeout_ms: None,
            capture_format: Default::default(),
            capture_schema: None,
        }
    }

    fn create_spec(queries: Vec<QueryTest>) -> TestSpec {
        TestSpec {
            application: "test_app".to_string(),
            description: None,
            default_timeout_ms: 30000,
            default_records: 1000,
            topic_naming: None,
            config: std::collections::HashMap::new(),
            queries,
        }
    }

    // ==========================================================================
    // Topic Naming Configuration Tests (P1.2)
    // ==========================================================================

    #[test]
    fn test_topic_naming_default_pattern() {
        let config = TopicNamingConfig::default();
        let topic = config.resolve_topic_name("market_data", "abc123");
        assert_eq!(topic, "test_abc123_market_data");
    }

    #[test]
    fn test_topic_naming_custom_pattern() {
        let config = TopicNamingConfig::with_pattern("myprefix_{run_id}_{base}");
        let topic = config.resolve_topic_name("trades", "xyz789");
        assert_eq!(topic, "myprefix_xyz789_trades");
    }

    #[test]
    fn test_topic_naming_with_namespace() {
        let config = TopicNamingConfig::with_namespace("ci");
        let topic = config.resolve_topic_name("orders", "abc123");
        assert_eq!(topic, "ci_abc123_orders");
    }

    #[test]
    fn test_topic_naming_ci_pattern() {
        // CI pattern includes branch - will use "local" when env not set
        let config = TopicNamingConfig::ci_pattern();
        let topic = config.resolve_topic_name("market_data", "abc123");
        assert!(topic.starts_with("test_"));
        assert!(topic.contains("abc123"));
        assert!(topic.ends_with("_market_data"));
    }

    #[test]
    fn test_topic_naming_with_static_branch() {
        let mut config = TopicNamingConfig::with_pattern("test_{branch}_{run_id}_{base}");
        config.branch = Some("feature-x".to_string());
        let topic = config.resolve_topic_name("trades", "abc123");
        assert_eq!(topic, "test_feature-x_abc123_trades");
    }

    #[test]
    fn test_topic_naming_sanitize_branch() {
        let mut config = TopicNamingConfig::with_pattern("test_{branch}_{base}");
        config.branch = Some("feature/my-branch".to_string());
        let topic = config.resolve_topic_name("trades", "abc123");
        // '/' should be replaced with '_'
        assert_eq!(topic, "test_feature_my-branch_trades");
    }

    #[test]
    fn test_topic_naming_with_user() {
        let mut config = TopicNamingConfig::with_pattern("test_{user}_{run_id}_{base}");
        config.user = Some("testuser".to_string());
        let topic = config.resolve_topic_name("orders", "abc123");
        assert_eq!(topic, "test_testuser_abc123_orders");
    }

    #[test]
    fn test_topic_naming_timestamp_placeholder() {
        let config = TopicNamingConfig::with_pattern("test_{timestamp}_{base}");
        let topic = config.resolve_topic_name("market_data", "abc123");
        // Should contain a numeric timestamp
        let parts: Vec<&str> = topic.split('_').collect();
        assert_eq!(parts[0], "test");
        // Second part should be a numeric timestamp
        assert!(parts[1].parse::<u64>().is_ok());
        assert_eq!(parts[2], "market");
        assert_eq!(parts[3], "data");
    }

    #[test]
    fn test_topic_naming_long_name_hashing() {
        let mut config = TopicNamingConfig::default();
        config.max_topic_length = 50;
        config.hash_long_names = true;

        // Create a long base name that will exceed the limit
        let long_base = "this_is_a_very_long_topic_name_that_exceeds_the_maximum";
        let topic = config.resolve_topic_name(long_base, "abc123");

        assert!(topic.len() <= 50);
        // Should contain hash suffix
        assert!(topic.contains("_"));
    }

    #[test]
    fn test_topic_naming_no_hashing_when_disabled() {
        let mut config = TopicNamingConfig::default();
        config.hash_long_names = false;

        let long_base = "very_long_topic_name";
        let topic = config.resolve_topic_name(long_base, "abc123");

        // Should keep full name even if long
        assert!(topic.contains(long_base));
    }

    #[test]
    fn test_sanitize_topic_component() {
        assert_eq!(sanitize_topic_component("simple"), "simple");
        assert_eq!(sanitize_topic_component("with-dash"), "with-dash");
        assert_eq!(sanitize_topic_component("with.dot"), "with.dot");
        assert_eq!(
            sanitize_topic_component("with_underscore"),
            "with_underscore"
        );
        assert_eq!(sanitize_topic_component("with/slash"), "with_slash");
        assert_eq!(sanitize_topic_component("with space"), "with_space");
        assert_eq!(
            sanitize_topic_component("with@special#chars"),
            "with_special_chars"
        );
    }

    #[test]
    fn test_execution_order_no_deps() {
        let spec = create_spec(vec![
            create_simple_query("query_a"),
            create_simple_query("query_b"),
            create_simple_query("query_c"),
        ]);

        let order = spec.execution_order();
        assert_eq!(order.len(), 3);

        // All queries should be included (order may vary since no deps)
        let names: Vec<_> = order.iter().map(|q| q.name.as_str()).collect();
        assert!(names.contains(&"query_a"));
        assert!(names.contains(&"query_b"));
        assert!(names.contains(&"query_c"));
    }

    #[test]
    fn test_execution_order_linear_deps() {
        // a -> b -> c (c depends on b, b depends on a)
        let spec = create_spec(vec![
            create_simple_query("query_a"),
            create_query_with_deps("query_b", vec!["query_a"]),
            create_query_with_deps("query_c", vec!["query_b"]),
        ]);

        let order = spec.execution_order();
        assert_eq!(order.len(), 3);

        let names: Vec<_> = order.iter().map(|q| q.name.as_str()).collect();

        // a must come before b
        let a_pos = names.iter().position(|n| *n == "query_a").unwrap();
        let b_pos = names.iter().position(|n| *n == "query_b").unwrap();
        let c_pos = names.iter().position(|n| *n == "query_c").unwrap();

        assert!(a_pos < b_pos, "query_a should come before query_b");
        assert!(b_pos < c_pos, "query_b should come before query_c");
    }

    #[test]
    fn test_execution_order_diamond_deps() {
        // Diamond: a -> b, a -> c, b -> d, c -> d
        let spec = create_spec(vec![
            create_simple_query("query_a"),
            create_query_with_deps("query_b", vec!["query_a"]),
            create_query_with_deps("query_c", vec!["query_a"]),
            create_query_with_deps("query_d", vec!["query_b", "query_c"]),
        ]);

        let order = spec.execution_order();
        assert_eq!(order.len(), 4);

        let names: Vec<_> = order.iter().map(|q| q.name.as_str()).collect();

        let a_pos = names.iter().position(|n| *n == "query_a").unwrap();
        let b_pos = names.iter().position(|n| *n == "query_b").unwrap();
        let c_pos = names.iter().position(|n| *n == "query_c").unwrap();
        let d_pos = names.iter().position(|n| *n == "query_d").unwrap();

        assert!(a_pos < b_pos, "query_a should come before query_b");
        assert!(a_pos < c_pos, "query_a should come before query_c");
        assert!(b_pos < d_pos, "query_b should come before query_d");
        assert!(c_pos < d_pos, "query_c should come before query_d");
    }

    #[test]
    fn test_execution_order_skipped_query() {
        let mut skipped = create_simple_query("query_b");
        skipped.skip = true;

        let spec = create_spec(vec![
            create_simple_query("query_a"),
            skipped,
            create_simple_query("query_c"),
        ]);

        let order = spec.execution_order();
        assert_eq!(order.len(), 2);

        let names: Vec<_> = order.iter().map(|q| q.name.as_str()).collect();
        assert!(names.contains(&"query_a"));
        assert!(!names.contains(&"query_b")); // Skipped
        assert!(names.contains(&"query_c"));
    }

    #[test]
    fn test_dependency_graph() {
        let spec = create_spec(vec![
            create_simple_query("query_a"),
            create_query_with_deps("query_b", vec!["query_a"]),
            create_query_with_deps("query_c", vec!["query_a", "query_b"]),
        ]);

        let graph = spec.dependency_graph();

        assert_eq!(graph.get("query_a").unwrap().len(), 0);
        assert_eq!(graph.get("query_b").unwrap(), &vec!["query_a".to_string()]);
        assert_eq!(
            graph.get("query_c").unwrap(),
            &vec!["query_a".to_string(), "query_b".to_string()]
        );
    }

    // ==========================================================================
    // Dependencies Field Tests
    // ==========================================================================

    #[test]
    fn test_query_test_dependencies_default_empty() {
        let query = create_simple_query("test_query");
        assert!(query.dependencies.is_empty());
    }

    #[test]
    fn test_query_test_with_single_dependency() {
        let query = create_query_with_deps("main_query", vec!["ref_table"]);
        assert_eq!(query.dependencies, vec!["ref_table".to_string()]);
    }

    #[test]
    fn test_query_test_with_multiple_dependencies() {
        let query = create_query_with_deps("main_query", vec!["table_a", "table_b", "table_c"]);
        assert_eq!(query.dependencies.len(), 3);
        assert_eq!(query.dependencies[0], "table_a");
        assert_eq!(query.dependencies[1], "table_b");
        assert_eq!(query.dependencies[2], "table_c");
    }

    #[test]
    fn test_dependencies_yaml_deserialization() {
        let yaml = r#"
name: vip_orders
description: Test query
dependencies:
  - vip_customers
  - products_table
inputs: []
assertions: []
"#;
        let query: QueryTest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(query.name, "vip_orders");
        assert_eq!(query.dependencies.len(), 2);
        assert_eq!(query.dependencies[0], "vip_customers");
        assert_eq!(query.dependencies[1], "products_table");
    }

    #[test]
    fn test_dependencies_yaml_default_when_missing() {
        let yaml = r#"
name: simple_query
inputs: []
assertions: []
"#;
        let query: QueryTest = serde_yaml::from_str(yaml).unwrap();
        assert!(query.dependencies.is_empty());
    }

    #[test]
    fn test_dependencies_yaml_empty_list() {
        let yaml = r#"
name: query_no_deps
dependencies: []
inputs: []
assertions: []
"#;
        let query: QueryTest = serde_yaml::from_str(yaml).unwrap();
        assert!(query.dependencies.is_empty());
    }

    #[test]
    fn test_dependencies_preserved_order() {
        // Dependencies should be executed in the order declared
        let deps = vec!["first", "second", "third"];
        let query = create_query_with_deps("main", deps);

        assert_eq!(query.dependencies[0], "first");
        assert_eq!(query.dependencies[1], "second");
        assert_eq!(query.dependencies[2], "third");
    }
}
