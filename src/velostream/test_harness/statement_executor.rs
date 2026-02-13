//! Statement-by-statement SQL execution for debugging
//!
//! Provides step-by-step execution of SQL statements with breakpoints,
//! intermediate result capture, and interactive debugging capabilities.
//!
//! # Features
//!
//! - **Step Mode**: Execute statements one at a time
//! - **Breakpoints**: Set breakpoints on specific statements by name
//! - **Intermediate Results**: Capture and inspect outputs after each statement
//! - **Interactive Session**: Pause, step, continue, inspect state
//!
//! # Usage
//!
//! ```bash
//! # Step through all statements interactively
//! velo-test debug app.sql --spec test_spec.yaml
//!
//! # Run with step mode (pause after each statement)
//! velo-test run app.sql --step
//!
//! # Set breakpoints on specific queries
//! velo-test debug app.sql --break query1 --break query2
//! ```

use super::capture::SinkCapture;
use super::error::{TestHarnessError, TestHarnessResult};
use super::executor::{CapturedOutput, ExecutionResult, ParsedQuery, QueryExecutor};
use super::infra::TestHarnessInfra;
use super::log_capture::{self, CapturedLogEntry};
use super::spec::{QueryTest, TestSpec};
use super::table_state::TableSnapshot;
use crate::velostream::sql::execution::types::{StreamRecord, system_columns};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

/// Execution mode for statement executor
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Execute all statements without pausing
    #[default]
    Full,
    /// Pause after each statement (step-by-step)
    Step,
    /// Pause only at breakpoints
    Breakpoint,
}

/// Result from executing a single SQL statement
#[derive(Debug, Clone)]
pub struct StatementResult {
    /// Statement index (0-based)
    pub index: usize,

    /// Statement/query name
    pub name: String,

    /// Original SQL text for this statement
    pub sql_text: String,

    /// Whether execution succeeded
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Captured output (if any, for stream/sink statements)
    pub output: Option<CapturedOutput>,

    /// Table snapshot (if any, for CREATE TABLE statements)
    pub table_snapshot: Option<TableSnapshot>,

    /// Execution time in milliseconds
    pub execution_time_ms: u64,

    /// Whether this was a breakpoint
    pub hit_breakpoint: bool,

    /// Statement type (CREATE STREAM, CREATE TABLE, etc.)
    pub statement_type: StatementType,

    /// Captured ERROR and WARN logs during execution
    pub captured_logs: Vec<CapturedLogEntry>,
}

/// Type of SQL statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementType {
    CreateStream,
    CreateTable,
    Select,
    Insert,
    Other(String),
}

impl StatementType {
    /// Parse statement type from SQL text
    pub fn from_sql(sql: &str) -> Self {
        // Skip leading comments and whitespace to find the actual SQL statement
        let sql_stripped = Self::strip_leading_comments(sql);
        let sql_upper = sql_stripped.trim().to_uppercase();

        if sql_upper.starts_with("CREATE STREAM") {
            Self::CreateStream
        } else if sql_upper.starts_with("CREATE TABLE") {
            Self::CreateTable
        } else if sql_upper.starts_with("SELECT") {
            Self::Select
        } else if sql_upper.starts_with("INSERT") {
            Self::Insert
        } else {
            // Extract first word as type
            let first_word = sql_upper.split_whitespace().next().unwrap_or("UNKNOWN");
            Self::Other(first_word.to_string())
        }
    }

    /// Strip leading SQL comments (-- and /* */) from SQL text
    fn strip_leading_comments(sql: &str) -> &str {
        let mut remaining = sql.trim();

        loop {
            // Skip single-line comments (-- ...)
            if remaining.starts_with("--") {
                if let Some(newline_pos) = remaining.find('\n') {
                    remaining = remaining[newline_pos + 1..].trim_start();
                    continue;
                } else {
                    // Entire string is a comment
                    return "";
                }
            }

            // Skip multi-line comments (/* ... */)
            if remaining.starts_with("/*") {
                if let Some(end_pos) = remaining.find("*/") {
                    remaining = remaining[end_pos + 2..].trim_start();
                    continue;
                } else {
                    // Unclosed comment
                    return "";
                }
            }

            // No more comments to skip
            break;
        }

        remaining
    }

    /// Get display name
    pub fn display_name(&self) -> &str {
        match self {
            Self::CreateStream => "CREATE STREAM",
            Self::CreateTable => "CREATE TABLE",
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Other(name) => name,
        }
    }
}

/// Debug session state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    /// Session not started
    NotStarted,
    /// Paused at a statement (index)
    Paused(usize),
    /// Running
    Running,
    /// Completed all statements
    Completed,
    /// Stopped due to error
    Error,
}

/// Statement-by-statement executor for debugging SQL applications
pub struct StatementExecutor {
    /// Underlying query executor
    executor: QueryExecutor,

    /// Execution mode
    mode: ExecutionMode,

    /// Breakpoints (statement names to pause at)
    breakpoints: HashSet<String>,

    /// Results from each executed statement
    results: Vec<StatementResult>,

    /// Current session state
    state: SessionState,

    /// Current statement index
    current_index: usize,

    /// Parsed statements from SQL file
    statements: Vec<ParsedStatement>,

    /// Test specification (optional)
    test_spec: Option<TestSpec>,

    /// Callback for pause events (for interactive mode)
    #[allow(clippy::type_complexity)]
    on_pause: Option<Box<dyn Fn(&StatementResult) + Send + Sync>>,
}

/// A parsed SQL statement with metadata
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    /// Statement index
    pub index: usize,

    /// Statement name (from CREATE STREAM/TABLE name, or auto-generated)
    pub name: String,

    /// Original SQL text
    pub sql_text: String,

    /// Statement type
    pub statement_type: StatementType,

    /// Whether this creates output (has sink)
    pub has_sink: bool,

    /// Sink topic (if applicable)
    pub sink_topic: Option<String>,

    /// Source topics this statement reads from
    pub source_topics: Vec<String>,
}

impl StatementExecutor {
    /// Create a new statement executor
    pub fn new(infra: TestHarnessInfra, timeout: Duration) -> Self {
        Self {
            executor: QueryExecutor::new(infra).with_timeout(timeout),
            mode: ExecutionMode::Full,
            breakpoints: HashSet::new(),
            results: Vec::new(),
            state: SessionState::NotStarted,
            current_index: 0,
            statements: Vec::new(),
            test_spec: None,
            on_pause: None,
        }
    }

    /// Create with existing executor
    pub fn with_executor(executor: QueryExecutor) -> Self {
        Self {
            executor,
            mode: ExecutionMode::Full,
            breakpoints: HashSet::new(),
            results: Vec::new(),
            state: SessionState::NotStarted,
            current_index: 0,
            statements: Vec::new(),
            test_spec: None,
            on_pause: None,
        }
    }

    /// Set execution mode
    pub fn with_mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Add a breakpoint on a statement name
    pub fn add_breakpoint(&mut self, name: impl Into<String>) {
        self.breakpoints.insert(name.into());
    }

    /// Remove a breakpoint
    pub fn remove_breakpoint(&mut self, name: &str) -> bool {
        self.breakpoints.remove(name)
    }

    /// Clear all breakpoints
    pub fn clear_breakpoints(&mut self) {
        self.breakpoints.clear();
    }

    /// Get all breakpoints
    pub fn breakpoints(&self) -> &HashSet<String> {
        &self.breakpoints
    }

    /// Set test specification
    pub fn with_spec(mut self, spec: TestSpec) -> Self {
        self.test_spec = Some(spec);
        self
    }

    /// Set pause callback
    pub fn on_pause<F>(mut self, callback: F) -> Self
    where
        F: Fn(&StatementResult) + Send + Sync + 'static,
    {
        self.on_pause = Some(Box::new(callback));
        self
    }

    /// Get current session state
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    /// Get all results
    pub fn results(&self) -> &[StatementResult] {
        &self.results
    }

    /// Get parsed statements
    pub fn statements(&self) -> &[ParsedStatement] {
        &self.statements
    }

    /// Get current statement index
    pub fn current_index(&self) -> usize {
        self.current_index
    }

    /// Load and parse SQL file
    pub fn load_sql(&mut self, sql_file: impl AsRef<Path>) -> TestHarnessResult<()> {
        let sql_file = sql_file.as_ref();
        let content = std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: sql_file.display().to_string(),
        })?;

        self.parse_statements(&content, sql_file)
    }

    /// Parse SQL content into statements
    fn parse_statements(&mut self, sql_content: &str, sql_file: &Path) -> TestHarnessResult<()> {
        // Load SQL into executor to use its parsing capabilities
        self.executor.load_sql_file(sql_file)?;

        // Get parsed queries from executor in SQL file order (preserves statement order)
        let parsed_queries: Vec<ParsedQuery> = self.executor.parsed_queries_ordered().to_vec();

        // Convert to our statement format
        self.statements = parsed_queries
            .into_iter()
            .enumerate()
            .map(|(idx, pq)| ParsedStatement {
                index: idx,
                name: pq.name.clone(),
                sql_text: pq.query_text.clone(),
                statement_type: StatementType::from_sql(&pq.query_text),
                has_sink: !pq.sinks.is_empty(),
                sink_topic: pq.sink_topic,
                source_topics: pq.sources.clone(),
            })
            .collect();

        self.state = SessionState::NotStarted;
        self.current_index = 0;
        self.results.clear();

        log::info!(
            "Loaded {} statements from {}",
            self.statements.len(),
            sql_file.display()
        );

        Ok(())
    }

    /// Execute all statements based on current mode
    pub async fn execute_all(&mut self) -> TestHarnessResult<Vec<StatementResult>> {
        if self.statements.is_empty() {
            return Err(TestHarnessError::ConfigError {
                message: "No statements loaded. Call load_sql() first.".to_string(),
            });
        }

        self.state = SessionState::Running;
        self.current_index = 0;
        self.results.clear();

        while self.current_index < self.statements.len() {
            let result = self.execute_current().await?;
            let should_pause = self.should_pause(&result);

            self.results.push(result.clone());

            if should_pause {
                self.state = SessionState::Paused(self.current_index);

                // Call pause callback if set
                if let Some(ref callback) = self.on_pause {
                    callback(&result);
                }

                // In step mode, we return and wait for step_next()
                if self.mode == ExecutionMode::Step {
                    return Ok(self.results.clone());
                }
            }

            // Stop on error (unless configured otherwise)
            if !result.success {
                self.state = SessionState::Error;
                return Ok(self.results.clone());
            }

            self.current_index += 1;
        }

        self.state = SessionState::Completed;
        Ok(self.results.clone())
    }

    /// Execute a single step (next statement)
    pub async fn step_next(&mut self) -> TestHarnessResult<Option<StatementResult>> {
        if self.current_index >= self.statements.len() {
            self.state = SessionState::Completed;
            return Ok(None);
        }

        self.state = SessionState::Running;
        let result = self.execute_current().await?;
        self.results.push(result.clone());

        if !result.success {
            self.state = SessionState::Error;
        } else {
            self.current_index += 1;
            if self.current_index >= self.statements.len() {
                self.state = SessionState::Completed;
            } else {
                self.state = SessionState::Paused(self.current_index);
            }
        }

        Ok(Some(result))
    }

    /// Continue execution until next breakpoint or completion
    pub async fn continue_execution(&mut self) -> TestHarnessResult<Vec<StatementResult>> {
        let mut new_results = Vec::new();

        while self.current_index < self.statements.len() {
            self.state = SessionState::Running;
            let result = self.execute_current().await?;
            let hit_breakpoint = self.is_breakpoint(&result.name);

            new_results.push(result.clone());
            self.results.push(result.clone());

            if !result.success {
                self.state = SessionState::Error;
                break;
            }

            self.current_index += 1;

            // Stop at breakpoint (but not for the first statement after continue)
            if hit_breakpoint && new_results.len() > 1 {
                self.state = SessionState::Paused(self.current_index.saturating_sub(1));
                break;
            }
        }

        if self.current_index >= self.statements.len() && self.state != SessionState::Error {
            self.state = SessionState::Completed;
        }

        Ok(new_results)
    }

    /// Execute current statement
    async fn execute_current(&mut self) -> TestHarnessResult<StatementResult> {
        let statement = self.statements[self.current_index].clone();
        let start = std::time::Instant::now();

        log::info!(
            "[{}/{}] Executing: {} ({})",
            self.current_index + 1,
            self.statements.len(),
            statement.name,
            statement.statement_type.display_name()
        );

        // Find matching query test from spec (if available)
        let query_test = self
            .test_spec
            .as_ref()
            .and_then(|spec| spec.queries.iter().find(|q| q.name == statement.name))
            .cloned();

        // Execute via underlying executor
        let (success, error, output) = if let Some(query) = query_test {
            match self.executor.execute_query(&query).await {
                Ok(result) => (
                    result.success,
                    result.error,
                    result.outputs.into_iter().next(),
                ),
                Err(e) => (false, Some(e.to_string()), None),
            }
        } else {
            // Execute without spec - create minimal query test
            let minimal_query = QueryTest {
                name: statement.name.clone(),
                description: None,
                skip: false,
                dependencies: vec![],
                inputs: vec![],
                output: None,
                outputs: vec![],
                assertions: vec![],
                timeout_ms: None,
                capture_format: Default::default(),
                capture_schema: None,
                metric_assertions: vec![],
            };
            match self.executor.execute_query(&minimal_query).await {
                Ok(result) => (
                    result.success,
                    result.error,
                    result.outputs.into_iter().next(),
                ),
                Err(e) => (false, Some(e.to_string()), None),
            }
        };

        let execution_time_ms = start.elapsed().as_millis() as u64;
        let hit_breakpoint = self.is_breakpoint(&statement.name);

        // Capture ERROR/WARN logs that occurred during this step
        let captured_logs = log_capture::entries_since(start);

        // For CREATE TABLE statements, create a table snapshot from the captured output
        let table_snapshot = if statement.statement_type == StatementType::CreateTable {
            output.as_ref().map(|out| {
                use super::table_state::TableStateStats;
                use crate::velostream::sql::execution::types::FieldValue;
                use std::collections::HashMap;

                // Convert StreamRecords to the table snapshot format
                let records: HashMap<String, HashMap<String, FieldValue>> = out
                    .records
                    .iter()
                    .enumerate()
                    .map(|(idx, rec)| {
                        // Use index as key if no key field available
                        let key = rec
                            .key
                            .as_ref()
                            .map(|k| match k {
                                FieldValue::String(s) => s.clone(),
                                other => format!("{:?}", other),
                            })
                            .unwrap_or_else(|| format!("row_{}", idx));
                        (key, rec.fields.clone())
                    })
                    .collect();

                let record_count = records.len();

                TableSnapshot {
                    name: statement.name.clone(),
                    records,
                    key_fields: vec![],
                    stats: TableStateStats {
                        record_count,
                        total_inserts: record_count,
                        total_updates: 0,
                        total_deletes: 0,
                        total_tombstones: 0,
                        peak_record_count: record_count,
                    },
                    snapshot_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                }
            })
        } else {
            None
        };

        Ok(StatementResult {
            index: self.current_index,
            name: statement.name,
            sql_text: statement.sql_text,
            success,
            error,
            output,
            table_snapshot,
            execution_time_ms,
            hit_breakpoint,
            statement_type: statement.statement_type,
            captured_logs,
        })
    }

    /// Check if execution should pause after this result
    fn should_pause(&self, result: &StatementResult) -> bool {
        match self.mode {
            ExecutionMode::Full => false,
            ExecutionMode::Step => true,
            ExecutionMode::Breakpoint => result.hit_breakpoint,
        }
    }

    /// Check if a statement name is a breakpoint
    fn is_breakpoint(&self, name: &str) -> bool {
        self.breakpoints.contains(name)
    }

    /// Get a summary of current state for display
    pub fn state_summary(&self) -> String {
        let state_str = match &self.state {
            SessionState::NotStarted => "Not started".to_string(),
            SessionState::Paused(idx) => format!("Paused at statement {}", idx + 1),
            SessionState::Running => "Running".to_string(),
            SessionState::Completed => "Completed".to_string(),
            SessionState::Error => "Error".to_string(),
        };

        let progress = if self.statements.is_empty() {
            "No statements loaded".to_string()
        } else {
            format!(
                "{}/{} statements executed",
                self.results.len(),
                self.statements.len()
            )
        };

        let breakpoints = if self.breakpoints.is_empty() {
            "No breakpoints".to_string()
        } else {
            format!("Breakpoints: {:?}", self.breakpoints)
        };

        format!("{} | {} | {}", state_str, progress, breakpoints)
    }

    /// Get the underlying executor
    pub fn executor(&self) -> &QueryExecutor {
        &self.executor
    }

    /// Get mutable reference to underlying executor
    pub fn executor_mut(&mut self) -> &mut QueryExecutor {
        &mut self.executor
    }

    /// Get reference to infrastructure
    pub fn infra(&self) -> &TestHarnessInfra {
        self.executor.infra()
    }

    /// Get mutable reference to infrastructure
    pub fn infra_mut(&mut self) -> &mut TestHarnessInfra {
        self.executor.infra_mut()
    }

    /// Get output from a specific statement by name
    pub fn get_output(&self, name: &str) -> Option<&CapturedOutput> {
        self.results
            .iter()
            .find(|r| r.name == name)
            .and_then(|r| r.output.as_ref())
    }

    /// Get all captured outputs
    pub fn all_outputs(&self) -> HashMap<String, &CapturedOutput> {
        self.results
            .iter()
            .filter_map(|r| r.output.as_ref().map(|o| (r.name.clone(), o)))
            .collect()
    }
}

/// Debug session controller for interactive debugging
pub struct DebugSession {
    /// Statement executor
    executor: StatementExecutor,

    /// Command history
    history: Vec<DebugCommand>,
}

/// Debug commands
#[derive(Debug, Clone)]
pub enum DebugCommand {
    /// Step to next statement
    Step,
    /// Continue execution
    Continue,
    /// Run all remaining statements
    Run,
    /// Set breakpoint
    Break(String),
    /// Remove breakpoint
    Unbreak(String),
    /// Clear all breakpoints
    Clear,
    /// List statements
    List,
    /// Show current state
    Status,
    /// Inspect output from a specific statement
    Inspect(String),
    /// Inspect all captured outputs
    InspectAll,
    /// Show command history
    History,
    /// List all topics with partition info
    ListTopics,
    /// List all consumers with their state
    ListConsumers,
    /// List all running jobs with state and statistics
    ListJobs,
    /// Show schema for a topic (inferred from last message)
    ShowSchema(String),

    // === Data Visibility Commands ===
    /// Peek at messages from a Kafka topic
    Messages {
        /// Topic name
        topic: String,
        /// Show last N messages (from end)
        last: Option<usize>,
        /// Show first N messages (from beginning)
        first: Option<usize>,
        /// Start from specific offset
        offset: Option<i64>,
        /// Filter by partition
        partition: Option<i32>,
    },
    /// Show first N records from a statement's captured output
    Head {
        /// Statement name
        statement: String,
        /// Number of records (default: 10)
        limit: usize,
    },
    /// Show last N records from a statement's captured output
    Tail {
        /// Statement name
        statement: String,
        /// Number of records (default: 10)
        limit: usize,
    },
    /// Filter records by field value
    Filter {
        /// Statement name
        statement: String,
        /// Field name
        field: String,
        /// Filter operator
        operator: FilterOperator,
        /// Value to compare
        value: String,
    },
    /// Export records to file
    Export {
        /// Statement name
        statement: String,
        /// Output file path
        path: std::path::PathBuf,
        /// Export format
        format: ExportFormat,
    },

    /// Quit session
    Quit,
}

/// Filter operators for the filter command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOperator {
    /// Equals (=)
    Eq,
    /// Not equals (!=)
    Ne,
    /// Greater than (>)
    Gt,
    /// Less than (<)
    Lt,
    /// Greater than or equal (>=)
    Gte,
    /// Less than or equal (<=)
    Lte,
    /// Contains substring
    Contains,
}

impl FilterOperator {
    /// Parse operator from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "=" | "==" => Some(Self::Eq),
            "!=" | "<>" => Some(Self::Ne),
            ">" => Some(Self::Gt),
            "<" => Some(Self::Lt),
            ">=" => Some(Self::Gte),
            "<=" => Some(Self::Lte),
            "~" | "contains" => Some(Self::Contains),
            _ => None,
        }
    }

    /// Get display symbol
    pub fn symbol(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Gte => ">=",
            Self::Lte => "<=",
            Self::Contains => "contains",
        }
    }
}

/// Export format for the export command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportFormat {
    /// JSON format (default)
    Json,
    /// CSV format
    Csv,
}

impl std::fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportFormat::Json => write!(f, "JSON"),
            ExportFormat::Csv => write!(f, "CSV"),
        }
    }
}

/// Topic partition information
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition ID
    pub partition: i32,
    /// Low watermark offset (earliest available message)
    pub low_offset: i64,
    /// High watermark offset (next offset to be written)
    pub high_offset: i64,
    /// Number of messages (high - low)
    pub message_count: i64,
    /// Timestamp of latest message (if available)
    pub latest_timestamp_ms: Option<i64>,
    /// Key of the latest message (if available)
    pub latest_key: Option<String>,
}

/// Topic information for debugging
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name
    pub name: String,
    /// Partition information
    pub partitions: Vec<PartitionInfo>,
    /// Total message count across all partitions
    pub total_messages: i64,
    /// Whether this is a test harness created topic
    pub is_test_topic: bool,
}

/// Consumer state information
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    /// Consumer group ID
    pub group_id: String,
    /// Subscribed topics
    pub subscribed_topics: Vec<String>,
    /// Current position per topic-partition
    pub positions: Vec<ConsumerPosition>,
    /// Consumer state
    pub state: ConsumerState,
}

/// Consumer position for a topic-partition
#[derive(Debug, Clone)]
pub struct ConsumerPosition {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Current offset
    pub offset: i64,
    /// Lag (high watermark - current offset)
    pub lag: i64,
}

/// Consumer state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerState {
    /// Consumer is active and consuming
    Active,
    /// Consumer is paused
    Paused,
    /// Consumer is disconnected/stopped
    Stopped,
    /// Unknown state
    Unknown,
}

/// Type of job (Stream or Table)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobType {
    /// CREATE STREAM AS SELECT - continuous stream processing
    Stream,
    /// CREATE TABLE AS SELECT - materialized table with state
    Table,
    /// Unknown job type
    Unknown,
}

impl JobType {
    /// Get display name
    pub fn display_name(&self) -> &'static str {
        match self {
            JobType::Stream => "STREAM",
            JobType::Table => "TABLE",
            JobType::Unknown => "UNKNOWN",
        }
    }
}

/// Job information for debugging
#[derive(Debug, Clone)]
pub struct JobInfo {
    /// Job name (from SQL query name)
    pub name: String,
    /// Job type (Stream or Table)
    pub job_type: JobType,
    /// The SQL query that defines this job
    pub sql: String,
    /// Job state
    pub state: JobState,
    /// Job statistics
    pub stats: JobStats,
    /// Source topics (legacy, for backward compatibility)
    pub source_topics: Vec<String>,
    /// Sink topics (legacy, for backward compatibility)
    pub sink_topics: Vec<String>,
    /// Detailed data source information
    pub sources: Vec<DataSourceInfo>,
    /// Detailed data sink information
    pub sinks: Vec<DataSinkInfo>,
    /// Partitioner type being used
    pub partitioner: Option<String>,
    /// Table-specific: current record count in table state
    pub table_record_count: Option<u64>,
    /// Table-specific: number of keys in table
    pub table_key_count: Option<u64>,
    /// Table-specific: last update timestamp
    pub table_last_updated: Option<String>,
}

/// Job execution state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    /// Job is initializing
    Initializing,
    /// Job is running and processing records
    Running,
    /// Job is paused
    Paused,
    /// Job completed successfully with output
    Completed,
    /// Job completed but produced no output (warning)
    CompletedNoOutput,
    /// Job failed with error
    Failed,
    /// Job was stopped/cancelled
    Stopped,
}

/// Job execution statistics
#[derive(Debug, Clone, Default)]
pub struct JobStats {
    /// Total records read from sources
    pub records_read: u64,
    /// Total records written to sinks
    pub records_written: u64,
    /// Records in error (sent to DLQ)
    pub records_errored: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Average processing latency in milliseconds
    pub avg_latency_ms: Option<f64>,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
}

/// Type of data source
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSourceType {
    /// Kafka topic source
    Kafka,
    /// File-based source
    File,
    /// Unknown or custom source
    Other(String),
}

/// Information about a job's data source
#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    /// Source type
    pub source_type: DataSourceType,
    /// Source name (topic name or file path)
    pub name: String,
    /// Records read from this source
    pub records_read: u64,
    /// Bytes read from this source
    pub bytes_read: u64,
    /// Timestamp of last record processed (UTC)
    pub last_record_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Kafka-specific: consumer group
    pub consumer_group: Option<String>,
    /// Kafka-specific: partitions assigned
    pub partitions: Option<Vec<i32>>,
    /// Kafka-specific: current offsets per partition
    pub current_offsets: Option<Vec<(i32, i64)>>,
    /// Kafka-specific: lag (high watermark - current offset)
    pub lag: Option<i64>,
    /// Kafka-specific: bootstrap servers
    pub bootstrap_servers: Option<String>,
    /// Kafka-specific: auto offset reset policy
    pub auto_offset_reset: Option<String>,
    /// Kafka-specific: serialization format
    pub format: Option<String>,
    /// File-specific: file path
    pub file_path: Option<String>,
    /// File-specific: is file fully consumed
    pub fully_consumed: Option<bool>,
    /// Configuration properties (key-value pairs)
    pub config: std::collections::HashMap<String, String>,
}

/// Type of data sink
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSinkType {
    /// Kafka topic sink
    Kafka,
    /// File-based sink
    File,
    /// Unknown or custom sink
    Other(String),
}

/// Information about a job's data sink
#[derive(Debug, Clone)]
pub struct DataSinkInfo {
    /// Sink type
    pub sink_type: DataSinkType,
    /// Sink name (topic name or file path)
    pub name: String,
    /// Records written to this sink
    pub records_written: u64,
    /// Bytes written to this sink
    pub bytes_written: u64,
    /// Timestamp of last record written (UTC)
    pub last_record_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Kafka-specific: target partitions
    pub target_partitions: Option<Vec<i32>>,
    /// Kafka-specific: last produced offsets per partition
    pub produced_offsets: Option<Vec<(i32, i64)>>,
    /// Kafka-specific: bootstrap servers
    pub bootstrap_servers: Option<String>,
    /// Kafka-specific: serialization format
    pub format: Option<String>,
    /// Kafka-specific: compression type
    pub compression: Option<String>,
    /// Kafka-specific: acks setting
    pub acks: Option<String>,
    /// File-specific: file path
    pub file_path: Option<String>,
    /// File-specific: is file closed/flushed
    pub is_flushed: Option<bool>,
    /// Configuration properties (key-value pairs)
    pub config: std::collections::HashMap<String, String>,
}

impl DebugSession {
    /// Create new debug session
    pub fn new(executor: StatementExecutor) -> Self {
        Self {
            executor,
            history: Vec::new(),
        }
    }

    /// Get executor reference
    pub fn executor(&self) -> &StatementExecutor {
        &self.executor
    }

    /// Get mutable executor reference
    pub fn executor_mut(&mut self) -> &mut StatementExecutor {
        &mut self.executor
    }

    /// Execute a debug command
    pub async fn execute_command(
        &mut self,
        command: DebugCommand,
    ) -> TestHarnessResult<CommandResult> {
        self.history.push(command.clone());

        match command {
            DebugCommand::Step => {
                let result = self.executor.step_next().await?;
                Ok(CommandResult::StepResult(result))
            }
            DebugCommand::Continue => {
                let results = self.executor.continue_execution().await?;
                Ok(CommandResult::ExecutionResults(results))
            }
            DebugCommand::Run => {
                let results = self.executor.execute_all().await?;
                Ok(CommandResult::ExecutionResults(results))
            }
            DebugCommand::Break(name) => {
                self.executor.add_breakpoint(&name);
                Ok(CommandResult::Message(format!(
                    "Breakpoint set on '{}'",
                    name
                )))
            }
            DebugCommand::Unbreak(name) => {
                if self.executor.remove_breakpoint(&name) {
                    Ok(CommandResult::Message(format!(
                        "Breakpoint removed from '{}'",
                        name
                    )))
                } else {
                    Ok(CommandResult::Message(format!(
                        "No breakpoint on '{}'",
                        name
                    )))
                }
            }
            DebugCommand::Clear => {
                let count = self.executor.breakpoints().len();
                self.executor.clear_breakpoints();
                Ok(CommandResult::Message(format!(
                    "Cleared {} breakpoint(s)",
                    count
                )))
            }
            DebugCommand::List => {
                let statements = self.executor.statements();
                let results = self.executor.results();
                let mut lines: Vec<String> = Vec::new();
                for s in statements {
                    let bp = if self.executor.breakpoints().contains(&s.name) {
                        "*"
                    } else {
                        " "
                    };
                    let current = if s.index == self.executor.current_index() {
                        ">"
                    } else {
                        " "
                    };
                    // Find result for this statement (if executed)
                    let result = results.iter().find(|r| r.name == s.name);
                    let status = match result {
                        Some(r) if r.success => "✅",
                        Some(_) => "❌",
                        None => "⏸️",
                    };
                    // Header line with markers, number, name, type, and status
                    lines.push(format!(
                        "{}{} [{}] {} ({}) {}",
                        current,
                        bp,
                        s.index + 1,
                        s.name,
                        s.statement_type.display_name(),
                        status
                    ));
                    // SQL text, indented
                    for sql_line in s.sql_text.lines() {
                        lines.push(format!("      {}", sql_line));
                    }
                    // Show captured logs for executed statements (top 5)
                    if let Some(r) = result {
                        if !r.captured_logs.is_empty() {
                            let log_count = r.captured_logs.len();
                            let display_count = log_count.min(5);
                            lines.push(format!(
                                "      📋 Logs ({} captured, showing {}):",
                                log_count, display_count
                            ));
                            for entry in r.captured_logs.iter().take(5) {
                                lines.push(format!("         {}", entry.display_short()));
                            }
                        }
                    }
                    lines.push(String::new()); // Blank line between statements
                }
                Ok(CommandResult::Listing(lines))
            }
            DebugCommand::Status => Ok(CommandResult::Message(self.executor.state_summary())),
            DebugCommand::Inspect(name) => {
                if let Some(output) = self.executor.get_output(&name) {
                    // Find captured logs for this statement
                    let logs = self
                        .executor
                        .results()
                        .iter()
                        .find(|r| r.name == name)
                        .map(|r| r.captured_logs.clone())
                        .unwrap_or_default();
                    Ok(CommandResult::OutputWithLogs {
                        output: output.clone(),
                        logs,
                    })
                } else {
                    Ok(CommandResult::Message(format!(
                        "No output found for '{}'",
                        name
                    )))
                }
            }
            DebugCommand::InspectAll => {
                let outputs = self.executor.all_outputs();
                if outputs.is_empty() {
                    Ok(CommandResult::Message(
                        "No outputs captured yet".to_string(),
                    ))
                } else {
                    // Clone outputs for the result
                    let owned_outputs: HashMap<String, CapturedOutput> =
                        outputs.into_iter().map(|(k, v)| (k, v.clone())).collect();
                    Ok(CommandResult::AllOutputs(owned_outputs))
                }
            }
            DebugCommand::History => {
                let history_lines: Vec<String> = self
                    .history
                    .iter()
                    .enumerate()
                    .map(|(idx, cmd)| format!("{:4}  {:?}", idx + 1, cmd))
                    .collect();
                if history_lines.is_empty() {
                    Ok(CommandResult::Message("No command history".to_string()))
                } else {
                    Ok(CommandResult::HistoryListing(history_lines))
                }
            }
            DebugCommand::ListTopics => {
                let topics = self.executor.infra().fetch_topic_info(None).await?;
                if topics.is_empty() {
                    Ok(CommandResult::Message("No topics found".to_string()))
                } else {
                    Ok(CommandResult::TopicListing(topics))
                }
            }
            DebugCommand::ListConsumers => match self.executor.infra().get_consumer_info().await {
                Ok(consumers) => {
                    if consumers.is_empty() {
                        Ok(CommandResult::Message(
                            "No consumer groups found (try running a job first)".to_string(),
                        ))
                    } else {
                        Ok(CommandResult::ConsumerListing(consumers))
                    }
                }
                Err(e) => Ok(CommandResult::Message(format!(
                    "Failed to list consumers: {}",
                    e
                ))),
            },
            DebugCommand::ListJobs => {
                use crate::velostream::server::stream_job_server::JobStatus as ServerJobStatus;
                use std::collections::HashMap;

                // Get parsed statements for source topic info
                let parsed_stmts: HashMap<String, &ParsedStatement> = self
                    .executor
                    .statements()
                    .iter()
                    .map(|s| (s.name.clone(), s))
                    .collect();

                // Get live job info from StreamJobServer
                let server_jobs = self.executor.executor().list_server_jobs().await;
                let server_job_map: HashMap<String, _> =
                    server_jobs.iter().map(|j| (j.name.clone(), j)).collect();

                // Combine info from both sources: executed results and live server jobs
                let mut jobs: Vec<JobInfo> = Vec::new();
                let mut seen_jobs: HashSet<String> = HashSet::new();

                // First, add jobs from the server (live jobs with real-time stats)
                for server_job in &server_jobs {
                    seen_jobs.insert(server_job.name.clone());

                    // Try to get parsed statement info for this job
                    let parsed_stmt = parsed_stmts.get(&server_job.name);

                    // Determine job type from SQL
                    let job_type = if server_job.topic.to_uppercase().contains("TABLE")
                        || parsed_stmt
                            .map(|s| s.statement_type == StatementType::CreateTable)
                            .unwrap_or(false)
                    {
                        JobType::Table
                    } else {
                        JobType::Stream
                    };

                    // Convert server status to our JobState
                    let state = match &server_job.status {
                        ServerJobStatus::Starting => JobState::Initializing,
                        ServerJobStatus::Running => JobState::Running,
                        ServerJobStatus::Paused => JobState::Paused,
                        ServerJobStatus::Stopped => {
                            if server_job.stats.records_processed == 0 {
                                JobState::CompletedNoOutput
                            } else {
                                JobState::Completed
                            }
                        }
                        ServerJobStatus::Failed(_) => JobState::Failed,
                    };

                    let stats = JobStats {
                        records_read: server_job.stats.records_processed,
                        records_written: server_job.stats.records_processed, // Approximation
                        records_errored: server_job.stats.records_failed,
                        execution_time_ms: server_job.stats.total_processing_time.as_millis()
                            as u64,
                        ..Default::default()
                    };

                    // Get source topics from parsed statement
                    let source_topics = parsed_stmt
                        .map(|s| s.source_topics.clone())
                        .unwrap_or_default();

                    // Compute last record time from job stats if available
                    let last_record_time = server_job
                        .stats
                        .last_record_time
                        .map(|_| chrono::Utc::now()); // Use current time as approximation

                    // Build source info
                    let sources: Vec<DataSourceInfo> = source_topics
                        .iter()
                        .map(|topic| DataSourceInfo {
                            source_type: DataSourceType::Kafka,
                            name: topic.clone(),
                            records_read: server_job.stats.records_processed,
                            bytes_read: 0,
                            last_record_time,
                            consumer_group: Some(format!("velo-{}", server_job.name)),
                            partitions: None,
                            current_offsets: None,
                            lag: None,
                            bootstrap_servers: self
                                .executor
                                .infra()
                                .bootstrap_servers()
                                .map(|s| s.to_string()),
                            auto_offset_reset: Some("earliest".to_string()),
                            format: Some("json".to_string()),
                            file_path: None,
                            fully_consumed: None,
                            config: HashMap::new(),
                        })
                        .collect();

                    // Build sink info
                    let sinks: Vec<DataSinkInfo> = vec![DataSinkInfo {
                        sink_type: DataSinkType::Kafka,
                        name: server_job.topic.clone(),
                        records_written: server_job.stats.records_processed,
                        bytes_written: 0,
                        last_record_time,
                        target_partitions: None,
                        produced_offsets: None,
                        bootstrap_servers: self
                            .executor
                            .infra()
                            .bootstrap_servers()
                            .map(|s| s.to_string()),
                        format: Some("json".to_string()),
                        compression: Some("none".to_string()),
                        acks: None, // Actual acks setting determined by producer config
                        file_path: None,
                        is_flushed: Some(true),
                        config: HashMap::new(),
                    }];

                    jobs.push(JobInfo {
                        name: server_job.name.clone(),
                        job_type,
                        sql: String::new(), // SQL not available from server summary
                        state,
                        stats,
                        source_topics: source_topics.clone(),
                        sink_topics: vec![server_job.topic.clone()],
                        sources,
                        sinks,
                        partitioner: Some("default".to_string()),
                        table_record_count: None,
                        table_key_count: None,
                        table_last_updated: None,
                    });
                }

                // Also add jobs from results that might not be in the server anymore
                for r in self.executor.results().iter().filter(|r| {
                    matches!(
                        r.statement_type,
                        StatementType::CreateStream | StatementType::CreateTable
                    ) && !seen_jobs.contains(&r.name)
                }) {
                    // Determine job type
                    let job_type = match r.statement_type {
                        StatementType::CreateStream => JobType::Stream,
                        StatementType::CreateTable => JobType::Table,
                        _ => JobType::Unknown,
                    };

                    // Calculate records written first for state determination
                    let records_written = r
                        .output
                        .as_ref()
                        .map(|o| o.records.len() as u64)
                        .unwrap_or(0);

                    // Determine state - check both success AND output
                    let state = if !r.success {
                        JobState::Failed
                    } else if records_written == 0 {
                        JobState::CompletedNoOutput
                    } else {
                        JobState::Completed
                    };

                    let stats = JobStats {
                        execution_time_ms: r.execution_time_ms,
                        records_written,
                        ..Default::default()
                    };

                    // Get source topics from parsed statement
                    let source_topics = parsed_stmts
                        .get(&r.name)
                        .map(|s| s.source_topics.clone())
                        .unwrap_or_default();

                    // Build detailed source info
                    let sources: Vec<DataSourceInfo> = source_topics
                        .iter()
                        .map(|topic| DataSourceInfo {
                            source_type: DataSourceType::Kafka,
                            name: topic.clone(),
                            records_read: 0,
                            bytes_read: 0,
                            last_record_time: None, // Job from results, no live tracking
                            consumer_group: Some(format!("velo-{}", r.name)),
                            partitions: None,
                            current_offsets: None,
                            lag: None,
                            bootstrap_servers: self
                                .executor
                                .infra()
                                .bootstrap_servers()
                                .map(|s| s.to_string()),
                            auto_offset_reset: Some("earliest".to_string()),
                            format: Some("json".to_string()),
                            file_path: None,
                            fully_consumed: None,
                            config: HashMap::new(),
                        })
                        .collect();

                    // Build detailed sink info
                    let sinks: Vec<DataSinkInfo> = r
                        .output
                        .as_ref()
                        .and_then(|o| o.topic.clone())
                        .into_iter()
                        .map(|topic| DataSinkInfo {
                            sink_type: DataSinkType::Kafka,
                            name: topic.clone(),
                            records_written: r
                                .output
                                .as_ref()
                                .map(|o| o.records.len() as u64)
                                .unwrap_or(0),
                            bytes_written: 0,
                            last_record_time: None, // Job from results, no live tracking
                            target_partitions: None,
                            produced_offsets: None,
                            bootstrap_servers: self
                                .executor
                                .infra()
                                .bootstrap_servers()
                                .map(|s| s.to_string()),
                            format: Some("json".to_string()),
                            compression: Some("none".to_string()),
                            acks: None, // Actual acks setting determined by producer config
                            file_path: None,
                            is_flushed: Some(true),
                            config: HashMap::new(),
                        })
                        .collect();

                    // Table-specific stats
                    let (table_record_count, table_key_count, table_last_updated) =
                        if job_type == JobType::Table {
                            let record_count = r.output.as_ref().map(|o| o.records.len() as u64);
                            (record_count, None, None)
                        } else {
                            (None, None, None)
                        };

                    jobs.push(JobInfo {
                        name: r.name.clone(),
                        job_type,
                        sql: r.sql_text.clone(),
                        state,
                        stats,
                        source_topics: source_topics.clone(),
                        sink_topics: r
                            .output
                            .as_ref()
                            .and_then(|o| o.topic.clone())
                            .into_iter()
                            .collect(),
                        sources,
                        sinks,
                        partitioner: Some("default".to_string()),
                        table_record_count,
                        table_key_count,
                        table_last_updated,
                    });
                }

                if jobs.is_empty() {
                    Ok(CommandResult::Message("No jobs deployed yet".to_string()))
                } else {
                    Ok(CommandResult::JobListing(jobs))
                }
            }
            DebugCommand::ShowSchema(topic_name) => {
                // Fetch schema by consuming a few messages from the topic
                match self
                    .executor
                    .infra()
                    .fetch_topic_schema(&topic_name, 1)
                    .await
                {
                    Ok(schema) => Ok(CommandResult::SchemaDisplay(schema)),
                    Err(e) => Ok(CommandResult::Message(format!(
                        "Failed to fetch schema for '{}': {}",
                        topic_name, e
                    ))),
                }
            }

            // === Data Visibility Commands ===
            DebugCommand::Messages {
                topic,
                last,
                first,
                offset,
                partition,
            } => {
                // Determine how many messages to fetch and from where
                let limit = last.or(first).unwrap_or(5);
                let from_end = last.is_some() || (first.is_none() && offset.is_none());

                match self
                    .executor
                    .infra()
                    .peek_topic_messages(&topic, limit, from_end, offset, partition)
                    .await
                {
                    Ok(messages) => {
                        if messages.is_empty() {
                            Ok(CommandResult::Message(format!(
                                "No messages found in topic '{}'",
                                topic
                            )))
                        } else {
                            Ok(CommandResult::MessagesResult(messages))
                        }
                    }
                    Err(e) => Ok(CommandResult::Message(format!(
                        "Failed to read messages from '{}': {}",
                        topic, e
                    ))),
                }
            }

            DebugCommand::Head { statement, limit } => {
                if let Some(output) = self.executor.get_output(&statement) {
                    let total = output.records.len();
                    // Get first N records (StreamRecords already contain full metadata)
                    let records: Vec<_> = output.records.iter().take(limit).cloned().collect();
                    Ok(CommandResult::RecordsResult {
                        statement,
                        records,
                        total_count: total,
                        showing: format!("first {}", limit),
                    })
                } else {
                    Ok(CommandResult::Message(format!(
                        "No output found for '{}'. Run the statement first.",
                        statement
                    )))
                }
            }

            DebugCommand::Tail { statement, limit } => {
                if let Some(output) = self.executor.get_output(&statement) {
                    let total = output.records.len();
                    let skip = total.saturating_sub(limit);
                    // Get last N records (StreamRecords already contain full metadata)
                    let records: Vec<_> = output.records.iter().skip(skip).cloned().collect();
                    Ok(CommandResult::RecordsResult {
                        statement,
                        records,
                        total_count: total,
                        showing: format!("last {}", limit),
                    })
                } else {
                    Ok(CommandResult::Message(format!(
                        "No output found for '{}'. Run the statement first.",
                        statement
                    )))
                }
            }

            DebugCommand::Filter {
                statement,
                field,
                operator,
                value,
            } => {
                if let Some(output) = self.executor.get_output(&statement) {
                    let total = output.records.len();
                    let filter_expr = format!("{} {} {}", field, operator.symbol(), value);

                    // Filter records that match the condition
                    let matched: Vec<(usize, StreamRecord)> = output
                        .records
                        .iter()
                        .enumerate()
                        .filter(|(_, record)| {
                            if let Some(field_value) = record.fields.get(&field) {
                                match_filter(field_value, &operator, &value)
                            } else {
                                false
                            }
                        })
                        .map(|(idx, rec)| (idx, rec.clone()))
                        .collect();

                    let matched_count = matched.len();
                    Ok(CommandResult::FilteredResult {
                        statement,
                        records: matched,
                        total_count: total,
                        matched_count,
                        filter_expr,
                    })
                } else {
                    Ok(CommandResult::Message(format!(
                        "No output found for '{}'. Run the statement first.",
                        statement
                    )))
                }
            }

            DebugCommand::Export {
                statement,
                path,
                format,
            } => {
                if let Some(output) = self.executor.get_output(&statement) {
                    let record_count = output.records.len();

                    let content = match format {
                        ExportFormat::Json => {
                            // Convert to JSON array (export includes full metadata)
                            let json_records: Vec<serde_json::Value> = output
                                .records
                                .iter()
                                .map(|rec| {
                                    let mut obj: serde_json::Map<String, serde_json::Value> = rec
                                        .fields
                                        .iter()
                                        .map(|(k, v)| (k.clone(), field_value_to_json(v)))
                                        .collect();
                                    // Include Kafka metadata in export
                                    if let Some(key) = &rec.key {
                                        obj.insert("_key".to_string(), field_value_to_json(key));
                                    }
                                    if !rec.headers.is_empty() {
                                        let headers_obj: serde_json::Map<
                                            String,
                                            serde_json::Value,
                                        > = rec
                                            .headers
                                            .iter()
                                            .map(|(k, v)| {
                                                (k.clone(), serde_json::Value::String(v.clone()))
                                            })
                                            .collect();
                                        obj.insert(
                                            "_headers".to_string(),
                                            serde_json::Value::Object(headers_obj),
                                        );
                                    }
                                    obj.insert(
                                        system_columns::PARTITION.to_string(),
                                        serde_json::Value::Number(rec.partition.into()),
                                    );
                                    obj.insert(
                                        system_columns::OFFSET.to_string(),
                                        serde_json::Value::Number(rec.offset.into()),
                                    );
                                    obj.insert(
                                        system_columns::TIMESTAMP.to_string(),
                                        serde_json::Value::Number(rec.timestamp.into()),
                                    );
                                    serde_json::Value::Object(obj)
                                })
                                .collect();
                            serde_json::to_string_pretty(&json_records)
                                .unwrap_or_else(|e| format!("JSON error: {}", e))
                        }
                        ExportFormat::Csv => {
                            // Get all field names from first record
                            if output.records.is_empty() {
                                String::new()
                            } else {
                                let fields: Vec<&String> =
                                    output.records[0].fields.keys().collect();
                                let mut csv = fields
                                    .iter()
                                    .map(|s| s.as_str())
                                    .collect::<Vec<_>>()
                                    .join(",");
                                csv.push('\n');
                                for record in &output.records {
                                    let row: Vec<String> = fields
                                        .iter()
                                        .map(|f| {
                                            record
                                                .fields
                                                .get(*f)
                                                .map(field_value_to_csv_string)
                                                .unwrap_or_default()
                                        })
                                        .collect();
                                    csv.push_str(&row.join(","));
                                    csv.push('\n');
                                }
                                csv
                            }
                        }
                    };

                    match std::fs::write(&path, content) {
                        Ok(()) => Ok(CommandResult::ExportResult {
                            path,
                            record_count,
                            format,
                        }),
                        Err(e) => Ok(CommandResult::Message(format!(
                            "Failed to write to '{}': {}",
                            path.display(),
                            e
                        ))),
                    }
                } else {
                    Ok(CommandResult::Message(format!(
                        "No output found for '{}'. Run the statement first.",
                        statement
                    )))
                }
            }

            DebugCommand::Quit => Ok(CommandResult::Quit),
        }
    }

    /// Get command history
    pub fn history(&self) -> &[DebugCommand] {
        &self.history
    }
}

/// Match a field value against a filter condition
fn match_filter(
    field_value: &crate::velostream::sql::execution::types::FieldValue,
    operator: &FilterOperator,
    value: &str,
) -> bool {
    use crate::velostream::sql::execution::types::FieldValue;

    // Convert field value to string for comparison
    let field_str = match field_value {
        FieldValue::String(s) => s.clone(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::ScaledInteger(v, scale) => {
            let divisor = 10_i64.pow(*scale as u32);
            format!("{}.{}", v / divisor, (v % divisor).abs())
        }
        FieldValue::Null => "null".to_string(),
        _ => format!("{:?}", field_value),
    };

    match operator {
        FilterOperator::Eq => field_str == value,
        FilterOperator::Ne => field_str != value,
        FilterOperator::Contains => field_str.contains(value),
        FilterOperator::Gt | FilterOperator::Lt | FilterOperator::Gte | FilterOperator::Lte => {
            // Try numeric comparison
            if let (Ok(field_num), Ok(value_num)) = (field_str.parse::<f64>(), value.parse::<f64>())
            {
                match operator {
                    FilterOperator::Gt => field_num > value_num,
                    FilterOperator::Lt => field_num < value_num,
                    FilterOperator::Gte => field_num >= value_num,
                    FilterOperator::Lte => field_num <= value_num,
                    _ => false,
                }
            } else {
                // Fall back to string comparison
                match operator {
                    FilterOperator::Gt => field_str.as_str() > value,
                    FilterOperator::Lt => field_str.as_str() < value,
                    FilterOperator::Gte => field_str.as_str() >= value,
                    FilterOperator::Lte => field_str.as_str() <= value,
                    _ => false,
                }
            }
        }
    }
}

/// Convert FieldValue to JSON
fn field_value_to_json(
    fv: &crate::velostream::sql::execution::types::FieldValue,
) -> serde_json::Value {
    use crate::velostream::sql::execution::types::FieldValue;

    match fv {
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
        FieldValue::Integer(i) => serde_json::Value::Number((*i).into()),
        FieldValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        FieldValue::String(s) => serde_json::Value::String(s.clone()),
        FieldValue::ScaledInteger(v, scale) => {
            let divisor = 10_f64.powi(*scale as i32);
            serde_json::Number::from_f64(*v as f64 / divisor)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        FieldValue::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
        FieldValue::Date(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Decimal(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Interval { value, unit } => {
            serde_json::Value::String(format!("{} {:?}", value, unit))
        }
        FieldValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(field_value_to_json).collect())
        }
        FieldValue::Map(map) | FieldValue::Struct(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), field_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Convert FieldValue to CSV-safe string
fn field_value_to_csv_string(fv: &crate::velostream::sql::execution::types::FieldValue) -> String {
    use crate::velostream::sql::execution::types::FieldValue;

    match fv {
        FieldValue::Null => "".to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::String(s) => {
            // Escape quotes and wrap in quotes if contains comma
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        FieldValue::ScaledInteger(v, scale) => {
            let divisor = 10_f64.powi(*scale as i32);
            format!("{}", *v as f64 / divisor)
        }
        FieldValue::Timestamp(ts) => ts.to_string(),
        FieldValue::Date(d) => d.to_string(),
        FieldValue::Decimal(d) => d.to_string(),
        FieldValue::Interval { value, unit } => format!("{} {:?}", value, unit),
        FieldValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(field_value_to_csv_string).collect();
            format!("[{}]", items.join(";"))
        }
        FieldValue::Map(map) | FieldValue::Struct(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}={}", k, field_value_to_csv_string(v)))
                .collect();
            format!("{{{}}}", items.join(";"))
        }
    }
}

/// Result of executing a debug command
#[derive(Debug)]
pub enum CommandResult {
    /// Step executed, result (if any)
    StepResult(Option<StatementResult>),
    /// Multiple statements executed
    ExecutionResults(Vec<StatementResult>),
    /// Simple message
    Message(String),
    /// Statement listing
    Listing(Vec<String>),
    /// Captured output from a single statement
    Output(CapturedOutput),
    /// Captured output with associated logs from execution
    OutputWithLogs {
        output: CapturedOutput,
        logs: Vec<super::log_capture::CapturedLogEntry>,
    },
    /// All captured outputs from executed statements
    AllOutputs(HashMap<String, CapturedOutput>),
    /// Command history listing
    HistoryListing(Vec<String>),
    /// Topic listing with partition info
    TopicListing(Vec<TopicInfo>),
    /// Consumer listing with state
    ConsumerListing(Vec<ConsumerInfo>),
    /// Job listing with state and statistics
    JobListing(Vec<JobInfo>),
    /// Topic schema display
    SchemaDisplay(TopicSchema),

    // === Data Visibility Results ===
    /// Messages from a Kafka topic
    MessagesResult(Vec<TopicMessage>),
    /// Head/tail records from captured output (full StreamRecord with Kafka metadata)
    RecordsResult {
        statement: String,
        /// Records with full Kafka metadata (key, headers, partition, offset, timestamp)
        records: Vec<StreamRecord>,
        total_count: usize,
        showing: String, // "first 10" or "last 10"
    },
    /// Filtered records from captured output
    FilteredResult {
        statement: String,
        /// Filtered records with original index: (index, record)
        records: Vec<(usize, StreamRecord)>,
        total_count: usize,
        matched_count: usize,
        filter_expr: String,
    },
    /// Export confirmation
    ExportResult {
        path: std::path::PathBuf,
        record_count: usize,
        format: ExportFormat,
    },

    /// Session quit
    Quit,
}

/// A message from a Kafka topic
#[derive(Debug, Clone)]
pub struct TopicMessage {
    /// Partition the message came from
    pub partition: i32,
    /// Offset of the message
    pub offset: i64,
    /// Message key (if any)
    pub key: Option<String>,
    /// Message value as JSON string
    pub value: String,
    /// Timestamp (if available)
    pub timestamp_ms: Option<i64>,
    /// Message headers (if any)
    pub headers: Vec<(String, String)>,
}

/// Topic schema information (inferred from message)
#[derive(Debug, Clone)]
pub struct TopicSchema {
    /// Topic name
    pub topic: String,
    /// Field names and types (inferred)
    pub fields: Vec<(String, String)>,
    /// Sample value (first record as JSON)
    pub sample_value: Option<String>,
    /// Whether the topic has message keys
    pub has_keys: bool,
    /// Number of records used for inference
    pub records_sampled: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_type_from_sql() {
        assert_eq!(
            StatementType::from_sql("CREATE STREAM foo AS SELECT * FROM bar"),
            StatementType::CreateStream
        );
        assert_eq!(
            StatementType::from_sql("create table foo AS SELECT * FROM bar"),
            StatementType::CreateTable
        );
        assert_eq!(
            StatementType::from_sql("SELECT * FROM foo"),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("INSERT INTO foo SELECT * FROM bar"),
            StatementType::Insert
        );
        assert_eq!(
            StatementType::from_sql("DROP TABLE foo"),
            StatementType::Other("DROP".to_string())
        );
    }

    #[test]
    fn test_statement_type_with_leading_comments() {
        // Single-line comments
        assert_eq!(
            StatementType::from_sql("-- This is a comment\nCREATE STREAM foo AS SELECT * FROM bar"),
            StatementType::CreateStream
        );

        // Multiple single-line comments
        assert_eq!(
            StatementType::from_sql(
                "-- @name my_query\n-- @description Test query\nCREATE TABLE foo AS SELECT * FROM bar"
            ),
            StatementType::CreateTable
        );

        // Multi-line comments
        assert_eq!(
            StatementType::from_sql("/* This is a block comment */\nSELECT * FROM foo"),
            StatementType::Select
        );

        // Mixed comments
        assert_eq!(
            StatementType::from_sql(
                "-- Line comment\n/* Block comment */\n-- Another line\nINSERT INTO foo SELECT * FROM bar"
            ),
            StatementType::Insert
        );

        // Comment with whitespace
        assert_eq!(
            StatementType::from_sql(
                "  -- Comment with leading space\n  CREATE STREAM x AS SELECT 1"
            ),
            StatementType::CreateStream
        );
    }

    #[test]
    fn test_execution_mode_default() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::Full);
    }

    #[test]
    fn test_breakpoint_management() {
        let infra = TestHarnessInfra::new();
        let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

        assert!(executor.breakpoints().is_empty());

        executor.add_breakpoint("query1");
        executor.add_breakpoint("query2");

        assert_eq!(executor.breakpoints().len(), 2);
        assert!(executor.breakpoints().contains("query1"));
        assert!(executor.breakpoints().contains("query2"));

        assert!(executor.remove_breakpoint("query1"));
        assert!(!executor.remove_breakpoint("query3")); // doesn't exist

        assert_eq!(executor.breakpoints().len(), 1);
        assert!(!executor.breakpoints().contains("query1"));

        executor.clear_breakpoints();
        assert!(executor.breakpoints().is_empty());
    }

    #[test]
    fn test_session_state() {
        let infra = TestHarnessInfra::new();
        let executor = StatementExecutor::new(infra, Duration::from_secs(30));

        assert_eq!(executor.state(), &SessionState::NotStarted);
    }

    #[test]
    fn test_state_summary_empty() {
        let infra = TestHarnessInfra::new();
        let executor = StatementExecutor::new(infra, Duration::from_secs(30));

        let summary = executor.state_summary();
        assert!(summary.contains("Not started"));
        assert!(summary.contains("No statements loaded"));
        assert!(summary.contains("No breakpoints"));
    }

    #[test]
    fn test_strip_leading_comments_edge_cases() {
        // Empty string
        assert_eq!(StatementType::strip_leading_comments(""), "");

        // Only whitespace
        assert_eq!(StatementType::strip_leading_comments("   \n\t  "), "");

        // Only a single line comment (no newline)
        assert_eq!(StatementType::strip_leading_comments("-- comment only"), "");

        // Unclosed block comment
        assert_eq!(StatementType::strip_leading_comments("/* unclosed"), "");

        // Nested comments (outer only stripped)
        let result = StatementType::strip_leading_comments("/* outer */ SELECT /* inner */ *");
        assert!(result.starts_with("SELECT"));

        // Multiple consecutive block comments
        assert_eq!(
            StatementType::strip_leading_comments("/* one *//* two */SELECT *"),
            "SELECT *"
        );

        // Comment with SQL keywords inside
        assert_eq!(
            StatementType::strip_leading_comments("-- SELECT * FROM table\nCREATE TABLE foo"),
            "CREATE TABLE foo"
        );

        // Block comment spanning multiple lines
        assert_eq!(
            StatementType::strip_leading_comments(
                "/*\n * Multi-line\n * block comment\n */\nSELECT 1"
            ),
            "SELECT 1"
        );
    }

    #[test]
    fn test_job_type_display() {
        assert_eq!(JobType::Stream.display_name(), "STREAM");
        assert_eq!(JobType::Table.display_name(), "TABLE");
        assert_eq!(JobType::Unknown.display_name(), "UNKNOWN");
    }

    #[test]
    fn test_job_state_variants() {
        // Test all variants exist and are distinct
        let states = vec![
            JobState::Initializing,
            JobState::Running,
            JobState::Paused,
            JobState::Completed,
            JobState::CompletedNoOutput,
            JobState::Failed,
            JobState::Stopped,
        ];

        // Each state should be unique
        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                assert_ne!(states[i], states[j]);
            }
        }
    }

    #[test]
    fn test_statement_type_display_name() {
        assert_eq!(StatementType::CreateStream.display_name(), "CREATE STREAM");
        assert_eq!(StatementType::CreateTable.display_name(), "CREATE TABLE");
        assert_eq!(StatementType::Select.display_name(), "SELECT");
        assert_eq!(StatementType::Insert.display_name(), "INSERT");
        assert_eq!(
            StatementType::Other("MERGE".to_string()).display_name(),
            "MERGE"
        );
    }

    #[test]
    fn test_job_stats_default() {
        let stats = JobStats::default();
        assert_eq!(stats.records_read, 0);
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.records_errored, 0);
        assert_eq!(stats.execution_time_ms, 0);
    }

    #[test]
    fn test_statement_type_case_insensitive() {
        // Verify case-insensitive matching
        assert_eq!(
            StatementType::from_sql("create stream foo"),
            StatementType::CreateStream
        );
        assert_eq!(
            StatementType::from_sql("CREATE STREAM foo"),
            StatementType::CreateStream
        );
        assert_eq!(
            StatementType::from_sql("Create Stream foo"),
            StatementType::CreateStream
        );
        assert_eq!(
            StatementType::from_sql("CREATE TABLE bar"),
            StatementType::CreateTable
        );
        assert_eq!(
            StatementType::from_sql("create table bar"),
            StatementType::CreateTable
        );
    }
}
