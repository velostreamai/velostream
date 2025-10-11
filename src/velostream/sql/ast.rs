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
use std::time::Duration;

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
///         fields: vec![SelectField::Wildcard],
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
///         properties: None};
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
    /// PARTITION BY columns
    pub partition_by: Vec<String>,
    /// ORDER BY specification
    pub order_by: Vec<OrderByExpr>,
    /// Window frame (ROWS/RANGE BETWEEN ... AND ...)
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
            WindowSpec::Session { .. } => None, // Session windows use event time implicitly
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
