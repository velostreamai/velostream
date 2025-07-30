// Streaming SQL module for ferrisstreams
// Provides native SQL support for Kafka stream processing

pub mod ast;
pub mod parser;
pub mod context;
pub mod execution;
pub mod schema;
pub mod error;
pub mod app_parser;


// Re-export main API
pub use context::StreamingSqlContext;
pub use ast::{StreamingQuery, SelectField, WindowSpec, Expr, DataType};
pub use parser::StreamingSqlParser;
pub use schema::{Schema, StreamHandle, FieldDefinition};
pub use error::SqlError;
pub use execution::{StreamExecutionEngine, StreamRecord, FieldValue};
pub use app_parser::{SqlApplicationParser, SqlApplication, ApplicationMetadata, SqlStatement};

// Version and feature info
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FEATURES: &[&str] = &[
    "streaming_select",
    "windowing", 
    "time_functions",
    "stream_registration"
];