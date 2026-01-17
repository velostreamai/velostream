/*!
# Streaming SQL Abstract Syntax Tree (AST)

This module defines the Abstract Syntax Tree for streaming SQL queries designed specifically
for continuous data processing over Kafka streams. The AST supports streaming-native features
including windowing, real-time aggregations, and CREATE STREAM AS SELECT (CSAS) statements.

## Key Features

- **Streaming-First Design**: Built for continuous queries over unbounded data streams
- **Window Operations**: Support for tumbling, sliding, and session windows
- **Real-time Aggregations**: GROUP BY with streaming semantics
- **Stream Creation**: CSAS and CREATE TABLE AS SELECT (CTAS) for stream transformations
- **Kafka Integration**: System columns for metadata access (_timestamp, _offset, _partition)
- **Header Support**: Access to Kafka message headers via SQL functions

## Example Queries

```sql
-- Simple stream selection with filtering
SELECT customer_id, amount FROM orders WHERE amount > 100 LIMIT 10

-- Windowed aggregation
SELECT customer_id, COUNT(*), AVG(amount)
FROM orders
WHERE amount > 50
GROUP BY customer_id
WINDOW TUMBLING(INTERVAL 5 MINUTES)

-- Stream creation with transformation
CREATE STREAM high_value_orders AS
SELECT customer_id, amount, HEADER('source') AS source
FROM orders
WHERE amount > 1000

-- System column access
SELECT customer_id, amount, _timestamp, _partition
FROM orders
ORDER BY _timestamp DESC
LIMIT 100
```

## Architecture

The AST is designed to be:
- **Immutable**: All nodes are immutable for thread safety
- **Composable**: Complex queries built from simple expressions
- **Extensible**: Easy to add new streaming operations
- **Type-Safe**: Full Rust type checking throughout
*/

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// Job processor mode for selecting execution strategy
///
/// Controls which job processor type is used for query execution:
/// - Simple: Basic processor for simple operations (DEFAULT)
/// - Transactional: Processor with transaction support
/// - Adaptive: High-performance processor with multi-partition parallel execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobProcessorMode {
    /// Default processor for simple operations (no transaction support)
    Simple,
    /// Processor with transaction support
    Transactional,
    /// High-performance processor with multi-partition parallel execution
    Adaptive,
}

impl FromStr for JobProcessorMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(JobProcessorMode::Simple),
            "transactional" => Ok(JobProcessorMode::Transactional),
            "adaptive" => Ok(JobProcessorMode::Adaptive),
            _ => Err(format!("Invalid job processor mode: {}", s)),
        }
    }
}

impl JobProcessorMode {
    /// Convert to string representation
    pub fn as_str(&self) -> &str {
        match self {
            JobProcessorMode::Simple => "simple",
            JobProcessorMode::Transactional => "transactional",
            JobProcessorMode::Adaptive => "adaptive",
        }
    }
}

/// Partitioning strategy type for adaptive processor
///
/// Controls how records are routed to partitions in adaptive mode:
/// - Sticky: Uses record's source partition field (default, zero-overhead)
/// - Hash: Consistent hashing on GROUP BY columns
/// - Smart: Hybrid approach (automatic optimization)
/// - RoundRobin: Uniform distribution across partitions
/// - FanIn: Broadcast to all partitions (for joins)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitioningStrategyType {
    /// Use record's source partition field (default, zero-overhead)
    Sticky,
    /// Consistent hashing on GROUP BY columns
    Hash,
    /// Hybrid approach with automatic optimization
    Smart,
    /// Uniform distribution across partitions
    RoundRobin,
    /// Broadcast to all partitions (for joins)
    FanIn,
}

impl FromStr for PartitioningStrategyType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sticky" | "stickypartition" => Ok(PartitioningStrategyType::Sticky),
            "hash" | "alwayshash" => Ok(PartitioningStrategyType::Hash),
            "smart" | "smartrepartition" => Ok(PartitioningStrategyType::Smart),
            "roundrobin" => Ok(PartitioningStrategyType::RoundRobin),
            "fanin" => Ok(PartitioningStrategyType::FanIn),
            _ => Err(format!("Invalid partitioning strategy: {}", s)),
        }
    }
}

impl PartitioningStrategyType {
    /// Convert to string representation
    pub fn as_str(&self) -> &str {
        match self {
            PartitioningStrategyType::Sticky => "sticky",
            PartitioningStrategyType::Hash => "hash",
            PartitioningStrategyType::Smart => "smart",
            PartitioningStrategyType::RoundRobin => "roundrobin",
            PartitioningStrategyType::FanIn => "fanin",
        }
    }
}

/// Emission mode for streaming query results
///
/// Controls when and how results are emitted from streaming queries.
/// Uses KSQL-style EMIT semantics with intelligent defaults.
#[derive(Debug, Clone, PartialEq)]
pub enum EmitMode {
    /// Emit changes immediately as they occur (CDC-style)
    /// Each input record can trigger result emission
    /// Usage: SELECT ... GROUP BY ... EMIT CHANGES
    Changes,

    /// Emit results only when windows close (requires WINDOW clause)
    /// Accumulates results and emits complete windows
    /// Usage: SELECT ... GROUP BY ... WINDOW ... EMIT FINAL
    Final,
}

/// Root AST node representing different types of streaming SQL queries.
///
/// This enum encompasses all supported query types in the streaming SQL engine,
/// from simple SELECT statements to complex stream creation operations.
///
/// # Examples
///
/// ```rust,no_run
/// use velostream::velostream::sql::ast::{StreamingQuery, SelectField, StreamSource};
///
/// fn main() {
///     // SELECT query
///     let select_query = StreamingQuery::Select {
///         distinct: false,
///         fields: vec![SelectField::Wildcard],
///         key_fields: None,
///         from_alias: None,
///         from: StreamSource::Stream("orders".to_string()),
///         joins: None,
///         where_clause: None,
///         group_by: None,
///         having: None,
///         window: None,
///         order_by: None,
///         limit: Some(100),
///         emit_mode: None,
///         properties: None,
///         job_mode: None,
///         batch_size: None,
///         num_partitions: None,
///         partitioning_strategy: None,
///     };
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingQuery {
    /// Standard SELECT query for streaming data processing.
    ///
    /// Supports all standard SQL SELECT features adapted for streaming:
    /// - Field selection (wildcards, expressions, aliases)
    /// - FROM clause with optional JOINs
    /// - WHERE clause filtering
    /// - GROUP BY aggregations with streaming semantics
    /// - HAVING post-aggregation filtering
    /// - Window operations (tumbling, sliding, session)
    /// - ORDER BY sorting (with memory considerations)
    /// - LIMIT for result set size control
    Select {
        /// Fields to select (columns, expressions, aggregates)
        fields: Vec<SelectField>,
        /// Whether SELECT DISTINCT is specified (removes duplicate rows)
        distinct: bool,
        /// Fields marked with PRIMARY KEY annotation for Kafka message key (SQL standard)
        /// Example: SELECT symbol PRIMARY KEY, price FROM trades
        key_fields: Option<Vec<String>>,
        /// Source stream or table
        from: StreamSource,
        /// Optional alias for the FROM source (e.g., "c" in "FROM customers c")
        from_alias: Option<String>,
        /// Optional JOIN clauses
        joins: Option<Vec<JoinClause>>,
        /// Optional WHERE clause for filtering
        where_clause: Option<Expr>,
        /// Optional GROUP BY expressions for aggregation
        group_by: Option<Vec<Expr>>,
        /// Optional HAVING clause for post-aggregation filtering
        having: Option<Expr>,
        /// Optional window specification for time-based operations
        window: Option<WindowSpec>,
        /// Optional ORDER BY for result sorting
        order_by: Option<Vec<OrderByExpr>>,
        /// Optional LIMIT for result set size control
        limit: Option<u64>,
        /// Emission mode for controlling when results are emitted
        emit_mode: Option<EmitMode>,
        /// Optional WITH clause properties for configuration
        properties: Option<HashMap<String, String>>,
        /// Job processor mode annotation (@job_mode: simple|transactional|adaptive)
        /// Defaults to Simple if not specified
        job_mode: Option<JobProcessorMode>,
        /// Batch size configuration annotation (@batch_size: <integer>)
        /// Applicable to any job mode
        batch_size: Option<usize>,
        /// Number of partitions for adaptive mode annotation (@num_partitions: <integer>)
        /// Only valid with adaptive mode
        num_partitions: Option<usize>,
        /// Partitioning strategy for adaptive mode (@partitioning_strategy: sticky|hash|smart|roundrobin|fanin)
        /// Only valid with adaptive mode, defaults to sticky
        partitioning_strategy: Option<PartitioningStrategyType>,
    },
    /// CREATE STREAM AS SELECT statement for stream transformations.
    ///
    /// Creates a new Kafka stream (topic) populated by continuous execution
    /// of the nested SELECT query. The stream persists indefinitely and
    /// processes new records as they arrive in the source stream.
    CreateStream {
        /// Name of the new stream to create
        name: String,
        /// Optional column definitions with types
        columns: Option<Vec<ColumnDef>>,
        /// SELECT query that defines the stream transformation
        as_select: Box<StreamingQuery>,
        /// Stream properties (replication factor, partitions, etc.)
        properties: HashMap<String, String>,
        /// Emission mode for continuous query results
        emit_mode: Option<EmitMode>,
        /// Metric annotations from SQL comments
        metric_annotations: Vec<crate::velostream::sql::parser::annotations::MetricAnnotation>,
        /// Custom job name from @job_name annotation (overrides auto-generated name)
        job_name: Option<String>,
    },
    /// CREATE TABLE AS SELECT statement for materialized views.
    ///
    /// Creates a materialized table (Table) that maintains the current
    /// state based on the continuous execution of the SELECT query.
    /// Supports aggregations and maintains consistent state.
    CreateTable {
        /// Name of the new table to create
        name: String,
        /// Optional column definitions with types
        columns: Option<Vec<ColumnDef>>,
        /// SELECT query that defines the table population
        as_select: Box<StreamingQuery>,
        /// Table properties (retention, compaction, etc.)
        properties: HashMap<String, String>,
        /// Emission mode for continuous query results
        emit_mode: Option<EmitMode>,
    },
    /// SHOW/LIST commands for discovering available resources.
    ///
    /// Provides metadata about streams, tables, topics, and other resources
    /// available in the streaming SQL context. Essential for data discovery
    /// and debugging.
    Show {
        /// Type of resource to show
        resource_type: ShowResourceType,
        /// Optional pattern for filtering results
        pattern: Option<String>,
    },
    /// START JOB command for initiating continuous job execution.
    ///
    /// Starts a named job that runs continuously until explicitly stopped.
    /// The job can be referenced by name for monitoring and management.
    StartJob {
        /// Unique name for the job
        name: String,
        /// The streaming query to execute continuously
        query: Box<StreamingQuery>,
        /// Optional properties for job execution
        properties: HashMap<String, String>,
    },
    /// STOP JOB command for terminating running jobs.
    ///
    /// Gracefully stops a running job by name, cleaning up resources
    /// and ensuring proper state management.
    StopJob {
        /// Name of the job to stop
        name: String,
        /// Whether to force stop if graceful shutdown fails
        force: bool,
    },
    /// PAUSE JOB command for temporarily suspending job execution.
    ///
    /// Pauses a running job while preserving its state and position.
    /// The job can be resumed later without data loss.
    PauseJob {
        /// Name of the job to pause
        name: String,
    },
    /// RESUME JOB command for restarting paused jobs.
    ///
    /// Resumes a paused job from where it left off, maintaining
    /// exactly-once processing guarantees.
    ResumeJob {
        /// Name of the job to resume
        name: String,
    },
    /// DEPLOY JOB command for versioned job deployment.
    ///
    /// Deploys a new version of a job with optional rollback capabilities.
    /// Supports blue-green deployments and canary releases.
    DeployJob {
        /// Name of the job
        name: String,
        /// Version identifier (semantic versioning recommended)
        version: String,
        /// The streaming query to deploy
        query: Box<StreamingQuery>,
        /// Deployment properties and configuration
        properties: HashMap<String, String>,
        /// Strategy for deployment (blue-green, canary, etc.)
        strategy: DeploymentStrategy,
    },
    /// ROLLBACK JOB command for reverting to previous job version.
    ///
    /// Rolls back a job deployment to a previous version, useful
    /// for handling deployment issues or bugs.
    RollbackJob {
        /// Name of the job to rollback
        name: String,
        /// Target version to rollback to (optional - defaults to previous)
        target_version: Option<String>,
    },
    /// INSERT INTO statement for adding records to streams/tables.
    ///
    /// Inserts new records into an existing stream or table. Supports both
    /// explicit value insertion and insertion from SELECT queries.
    InsertInto {
        /// Target table or stream name
        table_name: String,
        /// Optional column names (if not specified, matches all columns in order)
        columns: Option<Vec<String>>,
        /// Source of data to insert
        source: InsertSource,
    },
    /// UPDATE statement for modifying existing records.
    ///
    /// Updates records in a stream/table that match the specified condition.
    /// In streaming contexts, this creates new records with updated values.
    Update {
        /// Target table or stream name
        table_name: String,
        /// Column assignments (column_name, new_value_expression)
        assignments: Vec<(String, Expr)>,
        /// Optional WHERE clause to filter which records to update
        where_clause: Option<Expr>,
    },
    /// DELETE statement for removing records.
    ///
    /// In streaming contexts, DELETE typically creates tombstone records
    /// or filters out matching records from downstream processing.
    Delete {
        /// Target table or stream name
        table_name: String,
        /// Optional WHERE clause to filter which records to delete
        where_clause: Option<Expr>,
    },
    /// UNION operation for combining result sets from multiple SELECT queries.
    ///
    /// Combines rows from two or more SELECT queries into a single result set.
    /// UNION removes duplicate rows, while UNION ALL preserves all rows.
    Union {
        /// Left SELECT query
        left: Box<StreamingQuery>,
        /// Right SELECT query  
        right: Box<StreamingQuery>,
        /// Whether to preserve duplicates (UNION ALL = true, UNION = false)
        all: bool,
    },
}

/// Source of data for INSERT operations.
///
/// Supports both explicit values and SELECT query results for insertion.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// Insert explicit values: INSERT INTO table VALUES (1, 'abc', true), (2, 'def', false)
    Values {
        /// List of value rows, where each row is a list of expressions
        rows: Vec<Vec<Expr>>,
    },
    /// Insert from SELECT query: INSERT INTO table SELECT * FROM other_table WHERE condition
    Select {
        /// SELECT query providing the data to insert
        query: Box<StreamingQuery>,
    },
}

/// Deployment strategies for versioned job deployments.
///
/// Different strategies provide varying levels of safety and rollback capabilities
/// for production streaming SQL jobs.
#[derive(Debug, Clone, PartialEq)]
pub enum DeploymentStrategy {
    /// Blue-Green deployment: Deploy to new infrastructure, switch traffic
    BlueGreen,
    /// Canary deployment: Gradually roll out to percentage of traffic
    Canary { percentage: u8 },
    /// Rolling deployment: Replace instances one by one
    Rolling,
    /// Direct replacement: Stop old, start new (fastest but riskier)
    Replace,
}

/// Job execution status for monitoring and management.
#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    /// Job is actively processing records
    Running,
    /// Job is temporarily paused but retains state
    Paused,
    /// Job has been stopped and cleaned up
    Stopped,
    /// Job is in error state
    Error { message: String },
    /// Job is being deployed/starting up
    Starting,
    /// Job is being stopped gracefully
    Stopping,
    /// Job deployment is in progress
    Deploying,
}

/// Types of resources that can be shown with SHOW/LIST commands.
///
/// Each resource type provides different metadata about the streaming
/// SQL environment, helping users discover and understand available data sources.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowResourceType {
    /// Show all registered streams (Kafka topics exposed as streams)
    Streams,
    /// Show all registered tables (Tables and materialized views)
    Tables,
    /// Show all available Kafka topics (whether registered or not)
    Topics,
    /// Show all registered functions (built-in and user-defined)
    Functions,
    /// Show schema information for a specific stream or table
    Schema { name: String },
    /// Show properties for a specific resource
    Properties { resource_type: String, name: String },
    /// Show active jobs currently running
    Jobs,
    /// Show detailed job status including versions and deployment info
    JobStatus { name: Option<String> },
    /// Show job versions and deployment history
    JobVersions { name: String },
    /// Show job metrics and performance statistics
    JobMetrics { name: Option<String> },
    /// Show partitions for a specific topic/stream
    Partitions { name: String },
    /// Describe the schema of a stream or table
    Describe { name: String },
}

/// Field selection in SELECT clause
#[derive(Debug, Clone, PartialEq)]
pub enum SelectField {
    /// Simple column reference: column_name
    Column(String),
    /// Aliased column: column_name AS alias
    AliasedColumn { column: String, alias: String },
    /// Expression with optional alias: expr [AS alias]
    Expression { expr: Expr, alias: Option<String> },
    /// Wildcard selection: *
    Wildcard,
}

/// Stream source for FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum StreamSource {
    /// Named stream reference
    Stream(String),
    /// Table reference (Table)
    Table(String),
    /// URI-based data source (FR-047 compliant)
    Uri(String),
    /// Subquery (for future implementation)
    Subquery(Box<StreamingQuery>),
}

/// JOIN clause specification for stream-stream and stream-table joins
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    /// Type of join (INNER, LEFT, RIGHT, FULL OUTER)
    pub join_type: JoinType,
    /// Right side of the join
    pub right_source: StreamSource,
    /// Optional alias for the right source
    pub right_alias: Option<String>,
    /// JOIN condition (ON clause)
    pub condition: Expr,
    /// Optional window specification for windowed joins
    pub window: Option<JoinWindow>,
}

/// Types of JOIN operations supported
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// INNER JOIN - only matching records from both sides
    Inner,
    /// LEFT JOIN - all records from left, matching from right
    Left,
    /// RIGHT JOIN - all records from right, matching from left  
    Right,
    /// FULL OUTER JOIN - all records from both sides
    FullOuter,
}

/// Window specification for stream joins
#[derive(Debug, Clone, PartialEq)]
pub struct JoinWindow {
    /// Time window for join matching
    pub time_window: Duration,
    /// Grace period for late arrivals
    pub grace_period: Option<Duration>,
}

/// Emission mode for ROWS windows
///
/// Controls how records are emitted from ROWS window aggregations.
/// - EveryRecord: Emit result for every incoming record (default, real-time)
/// - BufferFull: Emit results only when buffer reaches capacity (batch-like)
#[derive(Debug, Clone, PartialEq, Default)]
pub enum RowsEmitMode {
    /// Emit aggregation for every record (default, real-time analytics)
    #[default]
    EveryRecord,
    /// Emit aggregation only when buffer reaches capacity (batch processing)
    BufferFull,
}

/// Row expiration mode for handling inactivity gaps in ROWS WINDOW
///
/// Controls when rows are automatically expired from the buffer based on timestamp gaps.
/// This prevents stale data from skewing long-term aggregations.
#[derive(Debug, Clone, PartialEq)]
pub enum RowExpirationMode {
    /// Default: 1 minute inactivity timeout (60,000 milliseconds)
    /// Used when no EXPIRE AFTER clause is specified
    Default,
    /// Custom inactivity gap: expire rows if gap exceeds this duration
    /// Corresponds to: EXPIRE AFTER INTERVAL '...' MINUTE/SECOND/HOUR INACTIVITY
    InactivityGap(Duration),
    /// Never expire: rows are kept indefinitely until buffer reaches capacity
    /// Corresponds to: EXPIRE AFTER NEVER
    Never,
}

/// Window specifications for streaming
#[derive(Debug, Clone, PartialEq)]
pub enum WindowSpec {
    /// Fixed-size tumbling window
    Tumbling {
        size: Duration,
        time_column: Option<String>,
    },
    /// Sliding window with advance interval
    Sliding {
        size: Duration,
        advance: Duration,
        time_column: Option<String>,
    },
    /// Session window with inactivity gap
    Session {
        gap: Duration,
        time_column: Option<String>,
        partition_by: Vec<String>,
    },
    /// Row-count-based analytic window with bounded buffer
    ///
    /// Maintains last N rows and emits aggregations per record.
    /// Perfect for moving averages, LAG/LEAD, and ranking functions.
    /// Supports optional time gap for session-aware semantics.
    Rows {
        /// Maximum number of rows to keep in buffer (e.g., 100, 1000)
        buffer_size: u32,
        /// PARTITION BY expressions (e.g., per symbol, per user)
        partition_by: Vec<Expr>,
        /// ORDER BY specification for ranking and LAG/LEAD
        order_by: Vec<OrderByExpr>,
        /// Optional time gap: reset buffer if this duration passes without new records
        /// Used for session-aware window semantics (e.g., 30 SECONDS)
        time_gap: Option<Duration>,
        /// Optional window frame (ROWS BETWEEN, RANGE BETWEEN)
        /// Allows aggregations over subset of buffer (e.g., last 50 rows only)
        window_frame: Option<WindowFrame>,
        /// When to emit results from the window
        emit_mode: RowsEmitMode,
        /// Row expiration mode: controls when rows are automatically removed from buffer
        /// Prevents stale data from skewing long-term aggregations
        /// - Default: 1 minute inactivity timeout
        /// - InactivityGap(duration): custom timeout duration
        /// - Never: rows kept indefinitely until buffer fills
        expire_after: RowExpirationMode,
    },
}

/// ORDER BY expression with direction
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: OrderDirection,
}

/// Sort direction for ORDER BY
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// OVER clause for window functions
#[derive(Debug, Clone, PartialEq)]
pub struct OverClause {
    /// Optional ROWS WINDOW specification (Phase 8)
    /// When present, contains BUFFER size, PARTITION BY, ORDER BY, and window frame
    pub window_spec: Option<Box<WindowSpec>>,
    /// PARTITION BY columns (used when window_spec is None)
    pub partition_by: Vec<String>,
    /// ORDER BY specification (used when window_spec is None)
    pub order_by: Vec<OrderByExpr>,
    /// Window frame (ROWS/RANGE BETWEEN ... AND ...) (used when window_spec is None)
    pub window_frame: Option<WindowFrame>,
}

/// Window frame specification for window functions
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    /// Frame type (ROWS or RANGE)
    pub frame_type: FrameType,
    /// Start bound
    pub start_bound: FrameBound,
    /// End bound (defaults to CURRENT ROW if not specified)
    pub end_bound: Option<FrameBound>,
}

/// Frame type for window functions
#[derive(Debug, Clone, PartialEq)]
pub enum FrameType {
    Rows,
    Range,
}

/// Frame boundary for window functions
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
    /// Interval-based frame bounds: INTERVAL '1' DAY PRECEDING
    IntervalPreceding {
        value: i64,
        unit: TimeUnit,
    },
    /// Interval-based frame bounds: INTERVAL '1' DAY FOLLOWING
    IntervalFollowing {
        value: i64,
        unit: TimeUnit,
    },
}

/// SQL expressions for WHERE clauses and computations
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference
    Column(String),
    /// Literal values
    Literal(LiteralValue),
    /// Binary operations: expr op expr
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operations: op expr
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// Function calls: func_name(args...)
    Function { name: String, args: Vec<Expr> },
    /// Window functions like LAG(expr) OVER (PARTITION BY col ORDER BY col)
    WindowFunction {
        function_name: String,
        args: Vec<Expr>,
        over_clause: OverClause,
    },
    /// CASE expressions for conditional logic
    Case {
        when_clauses: Vec<(Expr, Expr)>, // (condition, result)
        else_clause: Option<Box<Expr>>,
    },
    /// List expressions for IN operators: (expr1, expr2, expr3)
    List(Vec<Expr>),
    /// Subquery expressions for complex SQL operations
    Subquery {
        query: Box<StreamingQuery>,
        subquery_type: SubqueryType,
    },
    /// BETWEEN expressions: expr BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool, // for NOT BETWEEN
    },
}

/// Types of subquery expressions
#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryType {
    /// Scalar subquery: SELECT (SELECT max_price FROM products) as max_p FROM orders
    Scalar,
    /// EXISTS subquery: WHERE EXISTS (SELECT 1 FROM table WHERE condition)  
    Exists,
    /// NOT EXISTS subquery: WHERE NOT EXISTS (SELECT 1 FROM table WHERE condition)
    NotExists,
    /// IN subquery: WHERE column IN (SELECT column FROM table WHERE condition)
    In,
    /// NOT IN subquery: WHERE column NOT IN (SELECT column FROM table WHERE condition)
    NotIn,
    /// ANY/SOME subquery: WHERE column = ANY (SELECT column FROM table)
    Any,
    /// ALL subquery: WHERE column > ALL (SELECT column FROM table)
    All,
}

/// Literal values in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    /// High-precision decimal literal for financial calculations
    Decimal(String), // Store as string to preserve exact precision during parsing
    /// Time intervals: INTERVAL '5' MINUTE
    Interval {
        value: i64,
        unit: TimeUnit,
    },
}

/// Time units for intervals
/// Supports fine-grained time durations (nanosecond to year granularity)
#[derive(Debug, Clone, PartialEq)]
pub enum TimeUnit {
    Nanosecond,  // 1/1,000,000,000 second
    Microsecond, // 1/1,000,000 second
    Millisecond, // 1/1,000 second
    Second,
    Minute,
    Hour,
    Day,
    Week,  // 7 days
    Month, // Approximate: 30 days
    Year,  // Approximate: 365 days
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String operations
    Like,
    NotLike,
    Concat, // || concatenation operator

    // Set operations
    In,
    NotIn,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
    IsNull,
    IsNotNull,
}

/// Kafka source configuration
#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSource {
    pub topic: String,
    pub brokers: String,
    pub group_id: String,
    pub properties: HashMap<String, String>,
}

/// Column definition for CREATE STREAM
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub properties: HashMap<String, String>,
}

/// Data types supported in streaming SQL
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    Timestamp,
    /// High-precision decimal number for financial calculations
    Decimal,
    /// Array of elements of a specific type
    Array(Box<DataType>),
    /// Map with key-value pairs of specific types
    Map(Box<DataType>, Box<DataType>),
    /// Structured type with named fields
    Struct(Vec<StructField>),
}

/// Field definition for STRUCT data types
#[derive(Debug, Clone, PartialEq)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// INTO clause for CREATE STREAM ... INTO statements
#[derive(Debug, Clone, PartialEq)]
pub struct IntoClause {
    /// Target sink identifier
    pub sink_name: String,
    /// Optional sink-specific properties (future use)
    pub sink_properties: HashMap<String, String>,
}

/// Enhanced configuration properties supporting multiple config files
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConfigProperties {
    /// Source configuration file path (supports extends: inheritance)
    pub source_config: Option<String>,
    /// Sink configuration file path (supports extends: inheritance)
    pub sink_config: Option<String>,
    /// Additional monitoring configuration
    pub monitoring_config: Option<String>,
    /// Security configuration file path
    pub security_config: Option<String>,
    /// Inline properties for direct configuration
    pub inline_properties: HashMap<String, String>,
}

impl ConfigProperties {
    /// Convert to legacy HashMap format for backward compatibility
    pub fn into_legacy_format(self) -> HashMap<String, String> {
        let mut properties = self.inline_properties;

        // Add config file paths as properties
        if let Some(path) = self.source_config {
            properties.insert("source_config".to_string(), path);
        }
        if let Some(path) = self.sink_config {
            properties.insert("sink_config".to_string(), path);
        }
        if let Some(path) = self.monitoring_config {
            properties.insert("monitoring_config".to_string(), path);
        }
        if let Some(path) = self.security_config {
            properties.insert("security_config".to_string(), path);
        }

        properties
    }

    /// Create from legacy HashMap format for backward compatibility
    pub fn from_legacy_format(properties: HashMap<String, String>) -> Self {
        let mut config = Self::default();

        for (key, value) in properties {
            match key.as_str() {
                "source_config" => config.source_config = Some(value),
                "sink_config" => config.sink_config = Some(value),
                "monitoring_config" => config.monitoring_config = Some(value),
                "security_config" => config.security_config = Some(value),
                // Removed: base_source_config and base_sink_config no longer supported
                _ => {
                    config.inline_properties.insert(key, value);
                }
            }
        }

        config
    }
}

impl StreamingQuery {
    /// Check if this query requires windowing
    pub fn has_window(&self) -> bool {
        match self {
            StreamingQuery::Select { window, .. } => window.is_some(),
            StreamingQuery::CreateStream { as_select, .. } => as_select.has_window(),
            StreamingQuery::CreateTable { as_select, .. } => as_select.has_window(),
            StreamingQuery::Show { .. } => false, // SHOW commands don't use windows
            StreamingQuery::StartJob { query, .. } => query.has_window(),
            StreamingQuery::StopJob { .. } => false, // STOP commands don't use windows
            StreamingQuery::PauseJob { .. } => false, // PAUSE commands don't use windows
            StreamingQuery::ResumeJob { .. } => false, // RESUME commands don't use windows
            StreamingQuery::DeployJob { query, .. } => query.has_window(),
            StreamingQuery::RollbackJob { .. } => false, // ROLLBACK commands don't use windows
            StreamingQuery::InsertInto { .. } => false,  // INSERT commands don't use windows
            StreamingQuery::Update { .. } => false,      // UPDATE commands don't use windows
            StreamingQuery::Delete { .. } => false,      // DELETE commands don't use windows
            StreamingQuery::Union { left, right, .. } => left.has_window() || right.has_window(),
        }
    }

    /// Get all column references in the query
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            StreamingQuery::Select {
                fields,
                where_clause,
                ..
            } => {
                let mut columns = Vec::new();

                // Extract from SELECT fields
                for field in fields {
                    columns.extend(field.get_columns());
                }

                // Extract from WHERE clause
                if let Some(where_expr) = where_clause {
                    columns.extend(where_expr.get_columns());
                }

                columns.sort();
                columns.dedup();
                columns
            }
            StreamingQuery::CreateStream { as_select, .. } => as_select.get_columns(),
            StreamingQuery::CreateTable { as_select, .. } => as_select.get_columns(),
            StreamingQuery::Show { .. } => Vec::new(), // SHOW commands don't reference columns
            StreamingQuery::StartJob { query, .. } => query.get_columns(),
            StreamingQuery::StopJob { .. } => Vec::new(), // STOP commands don't reference columns
            StreamingQuery::PauseJob { .. } => Vec::new(), // PAUSE commands don't reference columns
            StreamingQuery::ResumeJob { .. } => Vec::new(), // RESUME commands don't reference columns
            StreamingQuery::DeployJob { query, .. } => query.get_columns(),
            StreamingQuery::RollbackJob { .. } => Vec::new(), // ROLLBACK commands don't reference columns
            StreamingQuery::InsertInto {
                table_name: _,
                columns,
                source,
            } => {
                let mut cols = Vec::new();
                // Add explicit column names if specified
                if let Some(column_names) = columns {
                    cols.extend(column_names.clone());
                }
                // Add columns from source
                match source {
                    InsertSource::Values { rows } => {
                        for row in rows {
                            for expr in row {
                                cols.extend(expr.get_columns());
                            }
                        }
                    }
                    InsertSource::Select { query } => {
                        cols.extend(query.get_columns());
                    }
                }
                cols.sort();
                cols.dedup();
                cols
            }
            StreamingQuery::Update {
                table_name: _,
                assignments,
                where_clause,
            } => {
                let mut cols = Vec::new();
                // Add columns from assignments
                for (col_name, expr) in assignments {
                    cols.push(col_name.clone());
                    cols.extend(expr.get_columns());
                }
                // Add columns from WHERE clause
                if let Some(where_expr) = where_clause {
                    cols.extend(where_expr.get_columns());
                }
                cols.sort();
                cols.dedup();
                cols
            }
            StreamingQuery::Delete {
                table_name: _,
                where_clause,
            } => {
                let mut cols = Vec::new();
                // Add columns from WHERE clause
                if let Some(where_expr) = where_clause {
                    cols.extend(where_expr.get_columns());
                }
                cols
            }
            StreamingQuery::Union { left, right, .. } => {
                let mut columns = left.get_columns();
                columns.extend(right.get_columns());
                columns.sort();
                columns.dedup();
                columns
            }
        }
    }

    /// Check if this query contains a stream-stream join
    ///
    /// A stream-stream join is detected when:
    /// - The query has JOIN clauses
    /// - Both the FROM source and JOIN source are streams (not tables)
    ///
    /// # Returns
    /// `true` if the query contains at least one stream-stream join
    pub fn has_stream_stream_joins(&self) -> bool {
        match self {
            StreamingQuery::Select { from, joins, .. } => {
                if let Some(join_clauses) = joins {
                    let left_is_stream = matches!(from, StreamSource::Stream(_));
                    join_clauses.iter().any(|j| {
                        let right_is_stream = matches!(j.right_source, StreamSource::Stream(_));
                        left_is_stream && right_is_stream
                    })
                } else {
                    false
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => as_select.has_stream_stream_joins(),
            StreamingQuery::CreateTable { as_select, .. } => as_select.has_stream_stream_joins(),
            StreamingQuery::StartJob { query, .. } => query.has_stream_stream_joins(),
            StreamingQuery::DeployJob { query, .. } => query.has_stream_stream_joins(),
            StreamingQuery::Union { left, right, .. } => {
                left.has_stream_stream_joins() || right.has_stream_stream_joins()
            }
            _ => false,
        }
    }

    /// Extract stream-stream join information from the query
    ///
    /// Returns the left source name, right source name, and join type
    /// for the first stream-stream join found.
    ///
    /// # Returns
    /// `Some((left_name, right_name, join_type))` if a stream-stream join is found
    /// `None` if no stream-stream join exists
    pub fn extract_stream_stream_join_info(&self) -> Option<(String, String, JoinType)> {
        match self {
            StreamingQuery::Select { from, joins, .. } => {
                if let Some(join_clauses) = joins {
                    if let StreamSource::Stream(left_name) = from {
                        for join in join_clauses {
                            if let StreamSource::Stream(right_name) = &join.right_source {
                                return Some((
                                    left_name.clone(),
                                    right_name.clone(),
                                    join.join_type.clone(),
                                ));
                            }
                        }
                    }
                }
                None
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                as_select.extract_stream_stream_join_info()
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                as_select.extract_stream_stream_join_info()
            }
            StreamingQuery::StartJob { query, .. } => query.extract_stream_stream_join_info(),
            StreamingQuery::DeployJob { query, .. } => query.extract_stream_stream_join_info(),
            _ => None,
        }
    }

    /// Extract join key columns from the ON clause
    ///
    /// Parses simple equality conditions like `a.col = b.col` to extract
    /// the join key column pairs.
    ///
    /// # Returns
    /// Vector of (left_column, right_column) pairs
    pub fn extract_join_keys(&self) -> Vec<(String, String)> {
        match self {
            StreamingQuery::Select { joins, .. } => {
                if let Some(join_clauses) = joins {
                    if let Some(join) = join_clauses.first() {
                        return Self::extract_keys_from_condition(&join.condition);
                    }
                }
                Vec::new()
            }
            StreamingQuery::CreateStream { as_select, .. } => as_select.extract_join_keys(),
            StreamingQuery::CreateTable { as_select, .. } => as_select.extract_join_keys(),
            StreamingQuery::StartJob { query, .. } => query.extract_join_keys(),
            StreamingQuery::DeployJob { query, .. } => query.extract_join_keys(),
            _ => Vec::new(),
        }
    }

    /// Extract key pairs from a join condition expression
    fn extract_keys_from_condition(expr: &Expr) -> Vec<(String, String)> {
        let mut keys = Vec::new();
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if matches!(op, BinaryOperator::Equal) {
                    // Try to extract column names from both sides
                    if let (Some(left_col), Some(right_col)) = (
                        Self::extract_column_name(left),
                        Self::extract_column_name(right),
                    ) {
                        keys.push((left_col, right_col));
                    }
                } else if matches!(op, BinaryOperator::And) {
                    // Recursively handle AND conditions
                    keys.extend(Self::extract_keys_from_condition(left));
                    keys.extend(Self::extract_keys_from_condition(right));
                }
            }
            _ => {}
        }
        keys
    }

    /// Extract column name from an expression (handling qualified names like a.col)
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => {
                // Handle qualified names like "orders.order_id" -> "order_id"
                Some(name.split('.').last().unwrap_or(name.as_str()).to_string())
            }
            _ => None,
        }
    }
}

impl SelectField {
    /// Get column references from this field
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            SelectField::Column(name) => vec![name.clone()],
            SelectField::AliasedColumn { column, .. } => vec![column.clone()],
            SelectField::Expression { expr, .. } => expr.get_columns(),
            SelectField::Wildcard => Vec::new(),
        }
    }
}

impl Expr {
    /// Extract all column references from this expression
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            Expr::Column(name) => vec![name.clone()],
            Expr::Literal(_) => Vec::new(),
            Expr::BinaryOp { left, right, .. } => {
                let mut columns = left.get_columns();
                columns.extend(right.get_columns());
                columns
            }
            Expr::UnaryOp { expr, .. } => expr.get_columns(),
            Expr::Function { args, .. } => {
                let mut columns = Vec::new();
                for arg in args {
                    columns.extend(arg.get_columns());
                }
                columns
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                let mut columns = Vec::new();
                for (condition, result) in when_clauses {
                    columns.extend(condition.get_columns());
                    columns.extend(result.get_columns());
                }
                if let Some(else_expr) = else_clause {
                    columns.extend(else_expr.get_columns());
                }
                columns
            }
            Expr::WindowFunction {
                args, over_clause, ..
            } => {
                let mut columns = Vec::new();
                // Extract columns from function arguments
                for arg in args {
                    columns.extend(arg.get_columns());
                }
                // Extract columns from PARTITION BY and ORDER BY clauses
                columns.extend(over_clause.partition_by.clone());
                for order_expr in &over_clause.order_by {
                    columns.extend(order_expr.expr.get_columns());
                }
                columns
            }
            Expr::List(items) => {
                let mut columns = Vec::new();
                for item in items {
                    columns.extend(item.get_columns());
                }
                columns
            }
            Expr::Subquery { query, .. } => {
                // Subqueries can reference columns from the outer query scope
                // For now, we'll include columns from the subquery itself
                query.get_columns()
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let mut columns = expr.get_columns();
                columns.extend(low.get_columns());
                columns.extend(high.get_columns());
                columns
            }
        }
    }
}

impl WindowSpec {
    /// Get the time column used for windowing
    pub fn time_column(&self) -> Option<&str> {
        match self {
            WindowSpec::Tumbling { time_column, .. } => time_column.as_deref(),
            WindowSpec::Sliding { time_column, .. } => time_column.as_deref(),
            WindowSpec::Session { time_column, .. } => time_column.as_deref(),
            // ROWS windows don't have an explicit time_column field
            // Time information comes from ORDER BY specification
            WindowSpec::Rows { .. } => None,
        }
    }

    /// Check if this window needs to maintain state
    pub fn is_stateful(&self) -> bool {
        // All window types require state management
        true
    }
}

impl TimeUnit {
    /// Convert to Duration
    pub fn to_duration(&self, value: i64) -> Duration {
        match self {
            TimeUnit::Nanosecond => Duration::from_nanos(value as u64),
            TimeUnit::Microsecond => Duration::from_micros(value as u64),
            TimeUnit::Millisecond => Duration::from_millis(value as u64),
            TimeUnit::Second => Duration::from_secs(value as u64),
            TimeUnit::Minute => Duration::from_secs(value as u64 * 60),
            TimeUnit::Hour => Duration::from_secs(value as u64 * 3600),
            TimeUnit::Day => Duration::from_secs(value as u64 * 86400),
            TimeUnit::Week => Duration::from_secs(value as u64 * 604800),
            TimeUnit::Month => Duration::from_secs(value as u64 * 2592000), // Approximate: 30 days
            TimeUnit::Year => Duration::from_secs(value as u64 * 31536000), // Approximate: 365 days
        }
    }
}

/// Builder for constructing SelectQuery (StreamingQuery::Select) with optional fields.
///
/// This builder pattern provides a clean API for creating SELECT queries without
/// requiring all fields to be explicitly specified. All annotation fields default to None.
///
/// # Example
///
/// ```rust,no_run
/// use velostream::velostream::sql::ast::{SelectBuilder, StreamSource, SelectField};
///
/// let query = SelectBuilder::new(
///     vec![SelectField::Wildcard],
///     StreamSource::Stream("orders".to_string()),
/// )
/// .with_limit(100)
/// .build();
/// ```
pub struct SelectBuilder {
    fields: Vec<SelectField>,
    distinct: bool,
    key_fields: Option<Vec<String>>,
    from: StreamSource,
    from_alias: Option<String>,
    joins: Option<Vec<JoinClause>>,
    where_clause: Option<Expr>,
    group_by: Option<Vec<Expr>>,
    having: Option<Expr>,
    window: Option<WindowSpec>,
    order_by: Option<Vec<OrderByExpr>>,
    limit: Option<u64>,
    emit_mode: Option<EmitMode>,
    properties: Option<HashMap<String, String>>,
    job_mode: Option<JobProcessorMode>,
    batch_size: Option<usize>,
    num_partitions: Option<usize>,
    partitioning_strategy: Option<PartitioningStrategyType>,
}

impl SelectBuilder {
    /// Create a new SelectBuilder with required fields.
    pub fn new(fields: Vec<SelectField>, from: StreamSource) -> Self {
        SelectBuilder {
            fields,
            distinct: false,
            key_fields: None,
            from,
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        }
    }

    /// Set whether the SELECT should be DISTINCT (remove duplicates).
    pub fn with_distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    pub fn with_key_fields(mut self, key_fields: Vec<String>) -> Self {
        self.key_fields = Some(key_fields);
        self
    }

    pub fn with_from_alias(mut self, alias: String) -> Self {
        self.from_alias = Some(alias);
        self
    }

    pub fn with_joins(mut self, joins: Vec<JoinClause>) -> Self {
        self.joins = Some(joins);
        self
    }

    pub fn with_where(mut self, clause: Expr) -> Self {
        self.where_clause = Some(clause);
        self
    }

    pub fn with_group_by(mut self, exprs: Vec<Expr>) -> Self {
        self.group_by = Some(exprs);
        self
    }

    pub fn with_having(mut self, clause: Expr) -> Self {
        self.having = Some(clause);
        self
    }

    pub fn with_window(mut self, spec: WindowSpec) -> Self {
        self.window = Some(spec);
        self
    }

    pub fn with_order_by(mut self, exprs: Vec<OrderByExpr>) -> Self {
        self.order_by = Some(exprs);
        self
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_emit_mode(mut self, mode: EmitMode) -> Self {
        self.emit_mode = Some(mode);
        self
    }

    pub fn with_properties(mut self, props: HashMap<String, String>) -> Self {
        self.properties = Some(props);
        self
    }

    pub fn with_job_mode(mut self, mode: JobProcessorMode) -> Self {
        self.job_mode = Some(mode);
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub fn with_num_partitions(mut self, num: usize) -> Self {
        self.num_partitions = Some(num);
        self
    }

    pub fn with_partitioning_strategy(mut self, strategy: PartitioningStrategyType) -> Self {
        self.partitioning_strategy = Some(strategy);
        self
    }

    /// Build the final StreamingQuery::Select.
    pub fn build(self) -> StreamingQuery {
        StreamingQuery::Select {
            fields: self.fields,
            distinct: self.distinct,
            key_fields: self.key_fields,
            from: self.from,
            from_alias: self.from_alias,
            joins: self.joins,
            where_clause: self.where_clause,
            group_by: self.group_by,
            having: self.having,
            window: self.window,
            order_by: self.order_by,
            limit: self.limit,
            emit_mode: self.emit_mode,
            properties: self.properties,
            job_mode: self.job_mode,
            batch_size: self.batch_size,
            num_partitions: self.num_partitions,
            partitioning_strategy: self.partitioning_strategy,
        }
    }
}

// =============================================================================
// Display implementations for SQL reconstruction
// =============================================================================

impl fmt::Display for EmitMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmitMode::Changes => write!(f, "EMIT CHANGES"),
            EmitMode::Final => write!(f, "EMIT FINAL"),
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::Nanosecond => write!(f, "NANOSECOND"),
            TimeUnit::Microsecond => write!(f, "MICROSECOND"),
            TimeUnit::Millisecond => write!(f, "MILLISECOND"),
            TimeUnit::Second => write!(f, "SECOND"),
            TimeUnit::Minute => write!(f, "MINUTE"),
            TimeUnit::Hour => write!(f, "HOUR"),
            TimeUnit::Day => write!(f, "DAY"),
            TimeUnit::Week => write!(f, "WEEK"),
            TimeUnit::Month => write!(f, "MONTH"),
            TimeUnit::Year => write!(f, "YEAR"),
        }
    }
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::FullOuter => write!(f, "FULL OUTER JOIN"),
        }
    }
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameType::Rows => write!(f, "ROWS"),
            FrameType::Range => write!(f, "RANGE"),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Add => write!(f, "+"),
            BinaryOperator::Subtract => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "%"),
            BinaryOperator::Equal => write!(f, "="),
            BinaryOperator::NotEqual => write!(f, "!="),
            BinaryOperator::LessThan => write!(f, "<"),
            BinaryOperator::LessThanOrEqual => write!(f, "<="),
            BinaryOperator::GreaterThan => write!(f, ">"),
            BinaryOperator::GreaterThanOrEqual => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::Like => write!(f, "LIKE"),
            BinaryOperator::NotLike => write!(f, "NOT LIKE"),
            BinaryOperator::Concat => write!(f, "||"),
            BinaryOperator::In => write!(f, "IN"),
            BinaryOperator::NotIn => write!(f, "NOT IN"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "NOT"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Plus => write!(f, "+"),
            UnaryOperator::IsNull => write!(f, "IS NULL"),
            UnaryOperator::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
            LiteralValue::Integer(i) => write!(f, "{}", i),
            LiteralValue::Float(fl) => write!(f, "{}", fl),
            LiteralValue::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            LiteralValue::Null => write!(f, "NULL"),
            LiteralValue::Decimal(d) => write!(f, "{}", d),
            LiteralValue::Interval { value, unit } => {
                write!(f, "INTERVAL '{}' {}", value, unit)
            }
        }
    }
}

impl fmt::Display for FrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameBound::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING"),
            FrameBound::Preceding(n) => write!(f, "{} PRECEDING", n),
            FrameBound::CurrentRow => write!(f, "CURRENT ROW"),
            FrameBound::Following(n) => write!(f, "{} FOLLOWING", n),
            FrameBound::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING"),
            FrameBound::IntervalPreceding { value, unit } => {
                write!(f, "INTERVAL '{}' {} PRECEDING", value, unit)
            }
            FrameBound::IntervalFollowing { value, unit } => {
                write!(f, "INTERVAL '{}' {} FOLLOWING", value, unit)
            }
        }
    }
}

impl fmt::Display for WindowFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} BETWEEN {}", self.frame_type, self.start_bound)?;
        if let Some(end) = &self.end_bound {
            write!(f, " AND {}", end)?;
        }
        Ok(())
    }
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expr, self.direction)
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{}", name),
            Expr::Literal(lit) => write!(f, "{}", lit),
            Expr::BinaryOp { left, op, right } => {
                // Add parentheses for clarity in nested expressions
                write!(f, "({} {} {})", left, op, right)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::IsNull | UnaryOperator::IsNotNull => {
                    write!(f, "{} {}", expr, op)
                }
                _ => write!(f, "{} {}", op, expr),
            },
            Expr::Function { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Expr::WindowFunction {
                function_name,
                args,
                over_clause,
            } => {
                write!(f, "{}(", function_name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ") OVER ({})", over_clause)
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                write!(f, "CASE")?;
                for (cond, result) in when_clauses {
                    write!(f, " WHEN {} THEN {}", cond, result)?;
                }
                if let Some(else_expr) = else_clause {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
            Expr::List(items) => {
                write!(f, "(")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }
            Expr::Subquery {
                query,
                subquery_type,
            } => match subquery_type {
                SubqueryType::Exists => write!(f, "EXISTS ({})", query),
                SubqueryType::NotExists => write!(f, "NOT EXISTS ({})", query),
                SubqueryType::In => write!(f, "IN ({})", query),
                SubqueryType::NotIn => write!(f, "NOT IN ({})", query),
                SubqueryType::Any => write!(f, "ANY ({})", query),
                SubqueryType::All => write!(f, "ALL ({})", query),
                SubqueryType::Scalar => write!(f, "({})", query),
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    write!(f, "{} NOT BETWEEN {} AND {}", expr, low, high)
                } else {
                    write!(f, "{} BETWEEN {} AND {}", expr, low, high)
                }
            }
        }
    }
}

impl fmt::Display for OverClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If we have a ROWS WINDOW specification, use it
        if let Some(window_spec) = &self.window_spec {
            return write!(f, "{}", window_spec);
        }

        let mut parts = Vec::new();

        if !self.partition_by.is_empty() {
            parts.push(format!("PARTITION BY {}", self.partition_by.join(", ")));
        }

        if !self.order_by.is_empty() {
            let order_strs: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            parts.push(format!("ORDER BY {}", order_strs.join(", ")));
        }

        if let Some(frame) = &self.window_frame {
            parts.push(frame.to_string());
        }

        write!(f, "{}", parts.join(" "))
    }
}

/// Helper to format a Duration as an interval string
fn format_duration(d: &Duration) -> String {
    let secs = d.as_secs();
    if secs.is_multiple_of(86400) && secs >= 86400 {
        format!("{}d", secs / 86400)
    } else if secs.is_multiple_of(3600) && secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs.is_multiple_of(60) && secs >= 60 {
        format!("{}m", secs / 60)
    } else if secs > 0 {
        format!("{}s", secs)
    } else {
        format!("{}ms", d.as_millis())
    }
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowSpec::Tumbling { size, time_column } => {
                write!(f, "WINDOW TUMBLING({})", format_duration(size))?;
                if let Some(col) = time_column {
                    write!(f, " TIME COLUMN {}", col)?;
                }
                Ok(())
            }
            WindowSpec::Sliding {
                size,
                advance,
                time_column,
            } => {
                write!(
                    f,
                    "WINDOW SLIDING({}, {})",
                    format_duration(size),
                    format_duration(advance)
                )?;
                if let Some(col) = time_column {
                    write!(f, " TIME COLUMN {}", col)?;
                }
                Ok(())
            }
            WindowSpec::Session {
                gap,
                time_column,
                partition_by,
            } => {
                write!(f, "WINDOW SESSION({})", format_duration(gap))?;
                if let Some(col) = time_column {
                    write!(f, " TIME COLUMN {}", col)?;
                }
                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY {}", partition_by.join(", "))?;
                }
                Ok(())
            }
            WindowSpec::Rows {
                buffer_size,
                partition_by,
                order_by,
                time_gap,
                window_frame,
                emit_mode,
                expire_after: _,
            } => {
                write!(f, "ROWS WINDOW BUFFER {} ROWS", buffer_size)?;

                if !partition_by.is_empty() {
                    let parts: Vec<String> = partition_by.iter().map(|e| e.to_string()).collect();
                    write!(f, " PARTITION BY {}", parts.join(", "))?;
                }

                if !order_by.is_empty() {
                    let order_strs: Vec<String> = order_by.iter().map(|o| o.to_string()).collect();
                    write!(f, " ORDER BY {}", order_strs.join(", "))?;
                }

                if let Some(gap) = time_gap {
                    write!(f, " TIME GAP {}", format_duration(gap))?;
                }

                if let Some(frame) = window_frame {
                    write!(f, " {}", frame)?;
                }

                match emit_mode {
                    RowsEmitMode::EveryRecord => {} // default, no output
                    RowsEmitMode::BufferFull => write!(f, " EMIT BUFFER FULL")?,
                }

                Ok(())
            }
        }
    }
}

impl fmt::Display for SelectField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectField::Column(name) => write!(f, "{}", name),
            SelectField::AliasedColumn { column, alias } => {
                write!(f, "{} AS {}", column, alias)
            }
            SelectField::Expression { expr, alias } => {
                if let Some(a) = alias {
                    write!(f, "{} AS {}", expr, a)
                } else {
                    write!(f, "{}", expr)
                }
            }
            SelectField::Wildcard => write!(f, "*"),
        }
    }
}

impl fmt::Display for StreamSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamSource::Stream(name) => write!(f, "{}", name),
            StreamSource::Table(name) => write!(f, "{}", name),
            StreamSource::Uri(uri) => write!(f, "'{}'", uri),
            StreamSource::Subquery(query) => write!(f, "({})", query),
        }
    }
}

impl fmt::Display for JoinClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.join_type, self.right_source)?;
        if let Some(alias) = &self.right_alias {
            write!(f, " {}", alias)?;
        }
        write!(f, " ON {}", self.condition)?;
        if let Some(window) = &self.window {
            write!(f, " WITHIN {}", format_duration(&window.time_window))?;
            if let Some(grace) = &window.grace_period {
                write!(f, " GRACE PERIOD {}", format_duration(grace))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for ShowResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShowResourceType::Streams => write!(f, "STREAMS"),
            ShowResourceType::Tables => write!(f, "TABLES"),
            ShowResourceType::Topics => write!(f, "TOPICS"),
            ShowResourceType::Functions => write!(f, "FUNCTIONS"),
            ShowResourceType::Schema { name } => write!(f, "SCHEMA FOR {}", name),
            ShowResourceType::Properties {
                resource_type,
                name,
            } => {
                write!(f, "PROPERTIES FOR {} {}", resource_type, name)
            }
            ShowResourceType::Jobs => write!(f, "JOBS"),
            ShowResourceType::JobStatus { name } => {
                if let Some(n) = name {
                    write!(f, "JOB STATUS {}", n)
                } else {
                    write!(f, "JOB STATUS")
                }
            }
            ShowResourceType::JobVersions { name } => {
                write!(f, "JOB VERSIONS {}", name)
            }
            ShowResourceType::JobMetrics { name } => {
                if let Some(n) = name {
                    write!(f, "JOB METRICS {}", n)
                } else {
                    write!(f, "JOB METRICS")
                }
            }
            ShowResourceType::Partitions { name } => {
                write!(f, "PARTITIONS FOR {}", name)
            }
            ShowResourceType::Describe { name } => write!(f, "DESCRIBE {}", name),
        }
    }
}

impl fmt::Display for DeploymentStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeploymentStrategy::BlueGreen => write!(f, "BLUE_GREEN"),
            DeploymentStrategy::Canary { percentage } => {
                write!(f, "CANARY({}%)", percentage)
            }
            DeploymentStrategy::Rolling => write!(f, "ROLLING"),
            DeploymentStrategy::Replace => write!(f, "REPLACE"),
        }
    }
}

impl fmt::Display for InsertSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertSource::Values { rows } => {
                write!(f, "VALUES ")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, expr) in row.iter().enumerate() {
                        if j > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", expr)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            InsertSource::Select { query } => write!(f, "{}", query),
        }
    }
}

impl fmt::Display for StreamingQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamingQuery::Select {
                fields,
                distinct,
                key_fields,
                from,
                from_alias,
                joins,
                where_clause,
                group_by,
                having,
                window,
                order_by,
                limit,
                emit_mode,
                properties: _,
                job_mode: _,
                batch_size: _,
                num_partitions: _,
                partitioning_strategy: _,
            } => {
                // SELECT clause with optional DISTINCT
                if *distinct {
                    write!(f, "SELECT DISTINCT ")?;
                } else {
                    write!(f, "SELECT ")?;
                }

                // Format fields with PRIMARY KEY annotation if present
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|field| {
                        let field_str = field.to_string();
                        // Check if this field is marked as PRIMARY KEY
                        let field_name = match field {
                            SelectField::Column(name) => Some(name.clone()),
                            SelectField::AliasedColumn { alias, .. } => Some(alias.clone()),
                            SelectField::Expression { alias, expr } => {
                                alias.clone().or_else(|| {
                                    if let Expr::Column(name) = expr {
                                        Some(name.clone())
                                    } else {
                                        None
                                    }
                                })
                            }
                            SelectField::Wildcard => None,
                        };
                        if let Some(keys) = key_fields {
                            if let Some(ref name) = field_name {
                                if keys.contains(name) {
                                    return format!("{} PRIMARY KEY", field_str);
                                }
                            }
                        }
                        field_str
                    })
                    .collect();
                write!(f, "{}", field_strs.join(", "))?;

                // FROM clause
                write!(f, " FROM {}", from)?;
                if let Some(alias) = from_alias {
                    write!(f, " {}", alias)?;
                }

                // JOIN clauses
                if let Some(join_clauses) = joins {
                    for join in join_clauses {
                        write!(f, " {}", join)?;
                    }
                }

                // WHERE clause
                if let Some(where_expr) = where_clause {
                    write!(f, " WHERE {}", where_expr)?;
                }

                // GROUP BY clause
                if let Some(group_exprs) = group_by {
                    let group_strs: Vec<String> =
                        group_exprs.iter().map(|e| e.to_string()).collect();
                    write!(f, " GROUP BY {}", group_strs.join(", "))?;
                }

                // HAVING clause
                if let Some(having_expr) = having {
                    write!(f, " HAVING {}", having_expr)?;
                }

                // WINDOW clause
                if let Some(win) = window {
                    write!(f, " {}", win)?;
                }

                // ORDER BY clause
                if let Some(order_exprs) = order_by {
                    let order_strs: Vec<String> =
                        order_exprs.iter().map(|o| o.to_string()).collect();
                    write!(f, " ORDER BY {}", order_strs.join(", "))?;
                }

                // LIMIT clause
                if let Some(lim) = limit {
                    write!(f, " LIMIT {}", lim)?;
                }

                // EMIT clause
                if let Some(mode) = emit_mode {
                    write!(f, " {}", mode)?;
                }

                Ok(())
            }

            StreamingQuery::CreateStream {
                name,
                columns: _,
                as_select,
                properties: _,
                emit_mode,
                metric_annotations: _,
                job_name: _,
            } => {
                write!(f, "CREATE STREAM {} AS {}", name, as_select)?;
                if let Some(mode) = emit_mode {
                    write!(f, " {}", mode)?;
                }
                Ok(())
            }

            StreamingQuery::CreateTable {
                name,
                columns: _,
                as_select,
                properties: _,
                emit_mode,
            } => {
                write!(f, "CREATE TABLE {} AS {}", name, as_select)?;
                if let Some(mode) = emit_mode {
                    write!(f, " {}", mode)?;
                }
                Ok(())
            }

            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                write!(f, "SHOW {}", resource_type)?;
                if let Some(pat) = pattern {
                    write!(f, " LIKE '{}'", pat)?;
                }
                Ok(())
            }

            StreamingQuery::StartJob {
                name,
                query,
                properties: _,
            } => {
                write!(f, "START JOB {} AS {}", name, query)
            }

            StreamingQuery::StopJob { name, force } => {
                if *force {
                    write!(f, "STOP JOB {} FORCE", name)
                } else {
                    write!(f, "STOP JOB {}", name)
                }
            }

            StreamingQuery::PauseJob { name } => {
                write!(f, "PAUSE JOB {}", name)
            }

            StreamingQuery::ResumeJob { name } => {
                write!(f, "RESUME JOB {}", name)
            }

            StreamingQuery::DeployJob {
                name,
                version,
                query,
                properties: _,
                strategy,
            } => {
                write!(
                    f,
                    "DEPLOY JOB {} VERSION '{}' STRATEGY {} AS {}",
                    name, version, strategy, query
                )
            }

            StreamingQuery::RollbackJob {
                name,
                target_version,
            } => {
                write!(f, "ROLLBACK JOB {}", name)?;
                if let Some(ver) = target_version {
                    write!(f, " TO VERSION '{}'", ver)?;
                }
                Ok(())
            }

            StreamingQuery::InsertInto {
                table_name,
                columns,
                source,
            } => {
                write!(f, "INSERT INTO {}", table_name)?;
                if let Some(cols) = columns {
                    write!(f, " ({})", cols.join(", "))?;
                }
                write!(f, " {}", source)
            }

            StreamingQuery::Update {
                table_name,
                assignments,
                where_clause,
            } => {
                write!(f, "UPDATE {} SET ", table_name)?;
                let assign_strs: Vec<String> = assignments
                    .iter()
                    .map(|(col, expr)| format!("{} = {}", col, expr))
                    .collect();
                write!(f, "{}", assign_strs.join(", "))?;
                if let Some(where_expr) = where_clause {
                    write!(f, " WHERE {}", where_expr)?;
                }
                Ok(())
            }

            StreamingQuery::Delete {
                table_name,
                where_clause,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(where_expr) = where_clause {
                    write!(f, " WHERE {}", where_expr)?;
                }
                Ok(())
            }

            StreamingQuery::Union { left, right, all } => {
                write!(f, "{}", left)?;
                if *all {
                    write!(f, " UNION ALL ")?;
                } else {
                    write!(f, " UNION ")?;
                }
                write!(f, "{}", right)
            }
        }
    }
}
