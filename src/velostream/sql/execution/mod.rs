//! Streaming SQL Execution Engine
//!
//! This module implements the execution engine for streaming SQL queries. It processes
//! SQL AST nodes and executes them against streaming data records, supporting real-time
//! query evaluation with expression processing, filtering, and basic aggregations.
//!
//! ## Public API
//!
//! The primary interface for executing SQL queries against streaming data:
//!
//! - [`StreamExecutionEngine`] - Main execution engine
//! - [`StreamRecord`] - Input record format
//! - [`FieldValue`] - Value type system
//!
//! ## Usage
//!
//! ```rust,no_run
//! # use velostream::velostream::sql::execution::{StreamExecutionEngine, StreamRecord};
//! # use velostream::velostream::serialization::JsonFormat;
//! # use velostream::velostream::sql::parser::StreamingSqlParser;
//! # use std::sync::Arc;
//! # use tokio::sync::mpsc;
//! # use std::collections::HashMap;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (output_sender, _receiver) = mpsc::unbounded_channel();
//! let mut engine = StreamExecutionEngine::new(output_sender);
//!
//! // Parse a simple query and execute with a record
//! let parser = StreamingSqlParser::new();
//! let query = parser.parse("SELECT * FROM stream")?;
//! let record = StreamRecord::new(HashMap::new()); // Empty record for example
//! engine.execute_with_record(&query, record).await?;
//! # Ok(())
//! # }
//! ```
//!
//! All other types and methods are internal implementation details.

pub mod aggregation;
pub mod algorithms;
pub mod config;
pub mod engine;
pub mod expression;
pub mod internal;
pub mod performance;
pub mod processors;
pub mod types;
pub mod utils;
pub mod watermarks;

// === PHASE 2: ERROR & RESOURCE ENHANCEMENTS ===
// Enhanced streaming error types with circuit breaker support
pub mod error;

// Resource management and monitoring system
pub mod resource_manager;

// Circuit breaker for fault tolerance and retry logic
pub mod circuit_breaker;

// Re-export public API only
pub use engine::StreamExecutionEngine;
pub use types::{FieldValue, StreamRecord};

// Re-export internal types for testing
