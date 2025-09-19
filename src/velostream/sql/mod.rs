// Streaming SQL module for velostream
// Provides native SQL support for Kafka stream processing

pub mod app_parser;
pub mod ast;
pub mod config;
pub mod context;
pub mod error;
pub mod execution;
// Legacy multi_job modules removed - functionality moved to src/velo/server/processors/
pub mod parser;
pub mod query_analyzer;
pub mod validation;
pub mod validator;

// Re-export main API
pub use app_parser::SqlApplication;
pub use ast::StreamingQuery;
pub use error::SqlError;
pub use execution::{FieldValue, StreamExecutionEngine};
pub use parser::StreamingSqlParser;
pub use validation::SqlValidationService;
pub use validator::SqlValidator;

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
