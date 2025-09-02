// Streaming SQL module for ferrisstreams
// Provides native SQL support for Kafka stream processing

pub mod app_parser;
pub mod ast;
pub mod config;
pub mod context;
pub mod error;
pub mod execution;
pub mod multi_job_common;
pub mod multi_job_simple;
pub mod multi_job_transactional;
pub mod parser;
pub mod query_analyzer;

// Re-export main API
pub use app_parser::{SqlApplication, SqlApplicationParser};
pub use ast::{DataType, StreamingQuery};
pub use error::SqlError;
pub use execution::{FieldValue, StreamExecutionEngine, StreamRecord};
pub use parser::StreamingSqlParser;

// Version and feature info
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FEATURES: &[&str] = &[
    "streaming_select",
    "windowing",
    "window_functions", // LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK with OVER clause
    "time_functions",   // DATEDIFF for time calculations
    "stream_registration",
    "math_functions",      // ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT
    "string_functions",    // CONCAT, LENGTH, TRIM, UPPER, LOWER, REPLACE, LEFT, RIGHT, POSITION
    "date_functions",      // NOW, CURRENT_TIMESTAMP, DATE_FORMAT, EXTRACT, DATEDIFF
    "utility_functions",   // COALESCE, NULLIF
    "json_processing",     // JSON_VALUE, JSON_EXTRACT
    "header_functions",    // HEADER, HAS_HEADER, HEADER_KEYS
    "system_columns",      // _timestamp, _offset, _partition
    "aggregate_functions", // COUNT, SUM, AVG, MIN, MAX, LISTAGG
    "having_clause",       // Post-aggregation filtering with HAVING
];
