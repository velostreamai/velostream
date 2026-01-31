//! Aggregation engine for streaming SQL queries.
//!
//! This module handles GROUP BY operations and aggregate function processing
//! for streaming SQL queries. It provides support for:
//!
//! - GROUP BY state management
//! - Aggregate function computation (SUM, COUNT, AVG, MIN, MAX, etc.)
//! - Accumulator state for streaming aggregations
//! - Windowed aggregation processing
//!
//! ## Public API
//!
//! The main interface for aggregation operations:
//!
//! - [`AggregationEngine`] - Core aggregation processing
//! - [`GroupByState`] - GROUP BY state management (re-exported from internal)
//! - [`GroupAccumulator`] - Single group accumulation (re-exported from internal)
//!
//! ## Usage
//!
//! ```rust,no_run
//! use velostream::velostream::sql::execution::aggregation::AggregationEngine;
//! use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
//! use velostream::velostream::sql::ast::Expr;
//! use std::collections::HashMap;
//!
//! let engine = AggregationEngine::new();
//! let mut accumulator = engine.create_accumulator();
//!
//! // Process a record through aggregation
//! let record = StreamRecord {
//!     fields: HashMap::new(),
//!     timestamp: 0,
//!     offset: 0,
//!     partition: 0,
//!     headers: Default::default(),
//!     event_time: None,
//!     topic: None,
//!     key: None,
//! };
//!
//! // accumulator.increment_count();
//! // let result = engine.compute_aggregate_value("field", &expr, &accumulator)?;
//! ```

pub mod accumulator;
pub mod compute;
pub mod functions;
pub mod state;

use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::execution::internal::{GroupAccumulator, GroupByState};

// Re-export key types for convenience
pub use self::accumulator::*;
pub use self::functions::*;

/// Core aggregation engine for streaming SQL queries.
///
/// This engine provides the main interface for handling aggregation operations
/// in streaming SQL queries, including GROUP BY state management and aggregate
/// function computation.
pub struct AggregationEngine;

impl AggregationEngine {
    /// Create a new aggregation engine
    pub fn new() -> Self {
        Self
    }

    /// Create a new empty accumulator
    pub fn create_accumulator(&self) -> GroupAccumulator {
        GroupAccumulator::new()
    }

    /// Create a new GROUP BY state
    pub fn create_group_by_state(
        &self,
        group_expressions: Vec<Expr>,
        select_fields: Vec<crate::velostream::sql::ast::SelectField>,
        having_clause: Option<Expr>,
    ) -> GroupByState {
        GroupByState::new(group_expressions, select_fields, having_clause)
    }
}

impl Default for AggregationEngine {
    fn default() -> Self {
        Self::new()
    }
}
