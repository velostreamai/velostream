use std::time::Duration;
use std::collections::HashMap;

/// Streaming SQL AST designed for continuous queries
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingQuery {
    Select {
        fields: Vec<SelectField>,
        from: StreamSource,
        where_clause: Option<Expr>,
        window: Option<WindowSpec>,
    },
    CreateStream {
        name: String,
        from: KafkaSource,
        schema: Option<Vec<ColumnDef>>,
    },
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
    /// Table reference (KTable)
    Table(String),
    /// Subquery (for future implementation)
    Subquery(Box<StreamingQuery>),
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
        partition_by: Vec<String>,
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
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// Function calls: func_name(args...)
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// CASE expressions for conditional logic
    Case {
        when_clauses: Vec<(Expr, Expr)>, // (condition, result)
        else_clause: Option<Box<Expr>>,
    },
}

/// Literal values in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    /// Time intervals: INTERVAL '5' MINUTE
    Interval { value: i64, unit: TimeUnit },
}

/// Time units for intervals
#[derive(Debug, Clone, PartialEq)]
pub enum TimeUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
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
    String,
    Integer,
    Long,
    Float,
    Double,
    Boolean,
    Timestamp,
    Bytes,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

impl StreamingQuery {
    /// Check if this query requires windowing
    pub fn has_window(&self) -> bool {
        match self {
            StreamingQuery::Select { window, .. } => window.is_some(),
            _ => false,
        }
    }
    
    /// Get all column references in the query
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            StreamingQuery::Select { fields, where_clause, .. } => {
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
            StreamingQuery::CreateStream { .. } => Vec::new(),
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
            Expr::Case { when_clauses, else_clause } => {
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
            TimeUnit::Millisecond => Duration::from_millis(value as u64),
            TimeUnit::Second => Duration::from_secs(value as u64),
            TimeUnit::Minute => Duration::from_secs(value as u64 * 60),
            TimeUnit::Hour => Duration::from_secs(value as u64 * 3600),
            TimeUnit::Day => Duration::from_secs(value as u64 * 86400),
        }
    }
}