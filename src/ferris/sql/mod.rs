// Streaming SQL module for ferrisstreams
// Provides native SQL support for Kafka stream processing

pub mod app_parser;
pub mod ast;
pub mod context;
pub mod error;
pub mod execution;
pub mod parser;
pub mod schema;

// Re-export main API
pub use app_parser::{ApplicationMetadata, SqlApplication, SqlApplicationParser, SqlStatement};
pub use ast::{DataType, Expr, SelectField, StreamingQuery, WindowSpec};
pub use context::StreamingSqlContext;
pub use error::SqlError;
pub use execution::{FieldValue, StreamExecutionEngine, StreamRecord};
pub use parser::StreamingSqlParser;
pub use schema::{FieldDefinition, Schema, StreamHandle};

// Version and feature info
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FEATURES: &[&str] = &[
    "streaming_select",
    "windowing",
    "time_functions",
    "stream_registration",
    "math_functions",    // ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT
    "string_functions",  // CONCAT, LENGTH, TRIM, UPPER, LOWER, REPLACE, LEFT, RIGHT
    "date_functions",    // NOW, CURRENT_TIMESTAMP, DATE_FORMAT, EXTRACT
    "utility_functions", // COALESCE, NULLIF
    "json_processing",   // JSON_VALUE, JSON_EXTRACT
    "header_functions",  // HEADER, HAS_HEADER, HEADER_KEYS
    "system_columns",    // _timestamp, _offset, _partition
];
