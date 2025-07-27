// Streaming SQL module for ferrisstreams
// Provides native SQL support for Kafka stream processing

pub mod ast;
pub mod parser;
pub mod context;
pub mod execution;
pub mod schema;
pub mod error;

// Re-export main API
pub use context::StreamingSqlContext;
pub use ast::{StreamingQuery, SelectField, WindowSpec, Expr};
pub use parser::StreamingSqlParser;
pub use schema::{Schema, DataType, StreamHandle};
pub use error::SqlError;

// Version and feature info
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FEATURES: &[&str] = &[
    "streaming_select",
    "windowing", 
    "time_functions",
    "stream_registration"
];