//! Internal types for the streaming SQL execution engine.
//!
//! This module contains implementation details that are not part of the public API.
//! These types support the internal operation of the execution engine including
//! GROUP BY state management, execution messaging, and query lifecycle management.

use super::types::{FieldValue, StreamRecord};
use crate::ferris::sql::ast::{Expr, SelectField, StreamingQuery, WindowSpec};
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};

/// State for tracking GROUP BY aggregations across streaming records
///
/// This structure maintains the accumulated state for all active groups
/// in a GROUP BY query, tracking aggregate values and group membership.
#[derive(Debug, Clone)]
pub struct GroupByState {
    /// Map of group keys to their accumulated state
    pub groups: HashMap<Vec<String>, GroupAccumulator>,
    /// The GROUP BY expressions for this state
    pub group_expressions: Vec<Expr>,
    /// The SELECT fields to compute for each group
    pub select_fields: Vec<SelectField>,
    /// Optional HAVING clause
    pub having_clause: Option<Expr>,
}

impl GroupByState {
    /// Create a new GroupByState for the given expressions and fields
    pub fn new(
        group_expressions: Vec<Expr>,
        select_fields: Vec<SelectField>,
        having_clause: Option<Expr>,
    ) -> Self {
        Self {
            groups: HashMap::new(),
            group_expressions,
            select_fields,
            having_clause,
        }
    }

    /// Get or create a group accumulator for the given key values
    pub fn get_or_create_group(&mut self, key_values: Vec<String>) -> &mut GroupAccumulator {
        self.groups.entry(key_values).or_default()
    }

    /// Get all group keys currently tracked
    pub fn get_group_keys(&self) -> Vec<&Vec<String>> {
        self.groups.keys().collect()
    }

    /// Get a specific group's accumulator
    pub fn get_group(&self, key_values: &[String]) -> Option<&GroupAccumulator> {
        self.groups.get(key_values)
    }

    /// Get a mutable reference to a specific group's accumulator
    pub fn get_group_mut(&mut self, key_values: &[String]) -> Option<&mut GroupAccumulator> {
        self.groups.get_mut(key_values)
    }
}

/// Accumulator for a single group's aggregate state
///
/// This structure maintains all the accumulated values for a single group
/// in a GROUP BY operation, supporting various aggregate functions.
#[derive(Debug, Clone)]
pub struct GroupAccumulator {
    /// Count of records in this group
    pub count: u64,
    /// Count of non-NULL values for COUNT(column) aggregates (field_name -> count)
    pub non_null_counts: HashMap<String, u64>,
    /// Sum values for SUM() aggregates (field_name -> sum)
    pub sums: HashMap<String, f64>,
    /// Values for MIN() aggregates (field_name -> min_value)
    pub mins: HashMap<String, FieldValue>,
    /// Values for MAX() aggregates (field_name -> max_value)
    pub maxs: HashMap<String, FieldValue>,
    /// Values for statistical aggregates (field_name -> [values])
    pub numeric_values: HashMap<String, Vec<f64>>,
    /// First values for FIRST() aggregates
    pub first_values: HashMap<String, FieldValue>,
    /// Last values for LAST() aggregates (updated on each record)
    pub last_values: HashMap<String, FieldValue>,
    /// String values for STRING_AGG
    pub string_values: HashMap<String, Vec<String>>,
    /// Distinct values for COUNT_DISTINCT
    pub distinct_values: HashMap<String, HashSet<String>>,
    /// HyperLogLog estimators for APPROX_COUNT_DISTINCT
    pub approx_distinct_values: HashMap<String, HyperLogLogPlus<String, RandomState>>,
    /// Sample record for non-aggregate fields (takes first record's values)
    pub sample_record: Option<StreamRecord>,
}

impl GroupAccumulator {
    /// Create a new empty accumulator
    pub fn new() -> Self {
        Self {
            count: 0,
            non_null_counts: HashMap::new(),
            sums: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
            numeric_values: HashMap::new(),
            first_values: HashMap::new(),
            last_values: HashMap::new(),
            string_values: HashMap::new(),
            distinct_values: HashMap::new(),
            approx_distinct_values: HashMap::new(),
            sample_record: None,
        }
    }

    /// Increment the count for this group
    pub fn increment_count(&mut self) {
        self.count += 1;
    }

    /// Add a non-null value count for a specific field
    pub fn add_non_null_count(&mut self, field_name: &str) {
        *self
            .non_null_counts
            .entry(field_name.to_string())
            .or_insert(0) += 1;
    }

    /// Add a value to the sum for a specific field
    pub fn add_sum(&mut self, field_name: &str, value: f64) {
        *self.sums.entry(field_name.to_string()).or_insert(0.0) += value;
    }

    /// Update the minimum value for a specific field
    pub fn update_min(&mut self, field_name: &str, value: FieldValue) {
        match self.mins.get(field_name) {
            Some(current_min) => {
                // Compare and update if the new value is smaller
                // This comparison logic should match the values_equal logic from the engine
                if self.is_value_less_than(&value, current_min) {
                    self.mins.insert(field_name.to_string(), value);
                }
            }
            None => {
                self.mins.insert(field_name.to_string(), value);
            }
        }
    }

    /// Update the maximum value for a specific field
    pub fn update_max(&mut self, field_name: &str, value: FieldValue) {
        match self.maxs.get(field_name) {
            Some(current_max) => {
                // Compare and update if the new value is larger
                if self.is_value_greater_than(&value, current_max) {
                    self.maxs.insert(field_name.to_string(), value);
                }
            }
            None => {
                self.maxs.insert(field_name.to_string(), value);
            }
        }
    }

    /// Simple comparison helper for min/max operations
    fn is_value_less_than(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(l), FieldValue::Integer(r)) => l < r,
            (FieldValue::Float(l), FieldValue::Float(r)) => l < r,
            (FieldValue::String(l), FieldValue::String(r)) => l < r,
            // ScaledInteger comparisons
            (
                FieldValue::ScaledInteger(l_val, l_scale),
                FieldValue::ScaledInteger(r_val, r_scale),
            ) => {
                // Convert both to same scale and compare
                if l_scale == r_scale {
                    l_val < r_val
                } else {
                    // Convert to f64 for cross-scale comparison
                    let l_float = *l_val as f64 / 10_i64.pow(*l_scale as u32) as f64;
                    let r_float = *r_val as f64 / 10_i64.pow(*r_scale as u32) as f64;
                    l_float < r_float
                }
            }
            // Mixed comparisons with ScaledInteger
            (FieldValue::ScaledInteger(val, scale), FieldValue::Integer(r)) => {
                let l_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                l_float < (*r as f64)
            }
            (FieldValue::Integer(l), FieldValue::ScaledInteger(val, scale)) => {
                let r_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                (*l as f64) < r_float
            }
            (FieldValue::ScaledInteger(val, scale), FieldValue::Float(r)) => {
                let l_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                l_float < *r
            }
            (FieldValue::Float(l), FieldValue::ScaledInteger(val, scale)) => {
                let r_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                *l < r_float
            }
            _ => false, // For complex comparisons, don't update
        }
    }

    /// Simple comparison helper for min/max operations
    fn is_value_greater_than(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(l), FieldValue::Integer(r)) => l > r,
            (FieldValue::Float(l), FieldValue::Float(r)) => l > r,
            (FieldValue::String(l), FieldValue::String(r)) => l > r,
            // ScaledInteger comparisons
            (
                FieldValue::ScaledInteger(l_val, l_scale),
                FieldValue::ScaledInteger(r_val, r_scale),
            ) => {
                // Convert both to same scale and compare
                if l_scale == r_scale {
                    l_val > r_val
                } else {
                    // Convert to f64 for cross-scale comparison
                    let l_float = *l_val as f64 / 10_i64.pow(*l_scale as u32) as f64;
                    let r_float = *r_val as f64 / 10_i64.pow(*r_scale as u32) as f64;
                    l_float > r_float
                }
            }
            // Mixed comparisons with ScaledInteger
            (FieldValue::ScaledInteger(val, scale), FieldValue::Integer(r)) => {
                let l_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                l_float > (*r as f64)
            }
            (FieldValue::Integer(l), FieldValue::ScaledInteger(val, scale)) => {
                let r_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                (*l as f64) > r_float
            }
            (FieldValue::ScaledInteger(val, scale), FieldValue::Float(r)) => {
                let l_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                l_float > *r
            }
            (FieldValue::Float(l), FieldValue::ScaledInteger(val, scale)) => {
                let r_float = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                *l > r_float
            }
            _ => false, // For complex comparisons, don't update
        }
    }

    /// Set the sample record if not already set
    pub fn set_sample_record(&mut self, record: StreamRecord) {
        if self.sample_record.is_none() {
            self.sample_record = Some(record);
        }
    }

    /// Add a value to the set of distinct values for COUNT_DISTINCT
    pub fn add_to_set(&mut self, field_name: &str, value: FieldValue) {
        let value_str = format!("{:?}", value); // Convert to string representation
        self.distinct_values
            .entry(field_name.to_string())
            .or_default()
            .insert(value_str);
    }

    /// Add a value to the HyperLogLog estimator for APPROX_COUNT_DISTINCT
    pub fn add_to_approx_set(&mut self, field_name: &str, value: FieldValue) {
        let value_str = format!("{:?}", value); // Convert to string representation
        let hll = self
            .approx_distinct_values
            .entry(field_name.to_string())
            .or_insert_with(|| HyperLogLogPlus::new(10, RandomState::new()).unwrap()); // 10-bit precision
        hll.insert(&value_str);
    }

    /// Set the first value for FIRST() aggregates (only if not already set)
    pub fn set_first_value(&mut self, field_name: &str, value: FieldValue) {
        self.first_values
            .entry(field_name.to_string())
            .or_insert(value);
    }

    /// Set the last value for LAST() aggregates (always updates)
    pub fn set_last_value(&mut self, field_name: &str, value: FieldValue) {
        self.last_values.insert(field_name.to_string(), value);
    }

    /// Add a value for statistical functions (STDDEV, VARIANCE)
    pub fn add_value_for_stats(&mut self, field_name: &str, value: f64) {
        self.numeric_values
            .entry(field_name.to_string())
            .or_default()
            .push(value);
    }

    /// Add to string aggregation
    pub fn add_to_string_agg(&mut self, field_name: &str, value: FieldValue, _delimiter: &str) {
        let value_str = match value {
            FieldValue::String(s) => s,
            _ => format!("{:?}", value),
        };

        self.string_values
            .entry(field_name.to_string())
            .or_default()
            .push(value_str);
    }
}

impl Default for GroupAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Messages used for internal execution engine communication
///
/// These messages support asynchronous communication between different
/// parts of the execution engine for job control and result processing.
///
/// Enhanced with correlation IDs for async error handling and new message types
/// for production streaming capabilities.
#[derive(Debug)]
pub enum ExecutionMessage {
    /// Start a new streaming query job
    StartJob {
        job_id: String,
        query: StreamingQuery,
        correlation_id: String,
    },
    /// Stop an existing streaming query job
    StopJob {
        job_id: String,
        correlation_id: String,
    },
    /// Process a single record through the engine
    ProcessRecord {
        stream_name: String,
        record: StreamRecord,
        correlation_id: String,
    },
    /// Deliver a query result
    QueryResult {
        query_id: String,
        result: StreamRecord,
        correlation_id: String,
    },

    // Enhanced message types for FR-058 capabilities (Phase 1B and beyond)
    /// Advance watermark for time-based window processing
    /// Only sent when watermarks are enabled
    AdvanceWatermark {
        watermark_timestamp: chrono::DateTime<chrono::Utc>,
        source_id: String,
        correlation_id: String,
    },

    /// Trigger window emission due to watermark advancement
    /// Only sent when watermark-based windowing is enabled  
    TriggerWindow {
        window_id: String,
        trigger_reason: WindowTriggerReason,
        correlation_id: String,
    },

    /// Request cleanup of expired state
    /// Only sent when resource management is enabled
    CleanupExpiredState {
        retention_duration_ms: u64,
        correlation_id: String,
    },

    /// Error recovery message for enhanced error handling
    /// Only sent when enhanced error handling is enabled
    ErrorRecovery {
        original_correlation_id: String,
        error_type: String,
        retry_attempt: u32,
        correlation_id: String,
    },

    /// Circuit breaker state change notification
    /// Only sent when enhanced error handling is enabled
    CircuitBreakerStateChange {
        component_id: String,
        old_state: String,
        new_state: String,
        correlation_id: String,
    },

    /// Resource limit exceeded notification
    /// Only sent when resource management is enabled
    ResourceLimitExceeded {
        resource_type: String,
        current_usage: u64,
        limit: u64,
        correlation_id: String,
    },
}

/// Reasons for window triggering in enhanced windowing
#[derive(Debug, Clone)]
pub enum WindowTriggerReason {
    /// Window triggered by watermark advancement
    WatermarkAdvancement,
    /// Window triggered by processing time
    ProcessingTime,
    /// Window triggered by element count
    ElementCount(usize),
    /// Window triggered by session timeout
    SessionTimeout,
    /// Manual window trigger
    Manual,
}

impl ExecutionMessage {
    /// Generate a new correlation ID for tracking async operations
    pub fn generate_correlation_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Get the correlation ID from any message
    pub fn correlation_id(&self) -> &str {
        match self {
            ExecutionMessage::StartJob { correlation_id, .. } => correlation_id,
            ExecutionMessage::StopJob { correlation_id, .. } => correlation_id,
            ExecutionMessage::ProcessRecord { correlation_id, .. } => correlation_id,
            ExecutionMessage::QueryResult { correlation_id, .. } => correlation_id,
            ExecutionMessage::AdvanceWatermark { correlation_id, .. } => correlation_id,
            ExecutionMessage::TriggerWindow { correlation_id, .. } => correlation_id,
            ExecutionMessage::CleanupExpiredState { correlation_id, .. } => correlation_id,
            ExecutionMessage::ErrorRecovery { correlation_id, .. } => correlation_id,
            ExecutionMessage::CircuitBreakerStateChange { correlation_id, .. } => correlation_id,
            ExecutionMessage::ResourceLimitExceeded { correlation_id, .. } => correlation_id,
        }
    }

    /// Check if this message type requires enhanced features to be enabled
    pub fn requires_enhanced_features(&self) -> bool {
        matches!(
            self,
            ExecutionMessage::AdvanceWatermark { .. }
                | ExecutionMessage::TriggerWindow { .. }
                | ExecutionMessage::CleanupExpiredState { .. }
                | ExecutionMessage::ErrorRecovery { .. }
                | ExecutionMessage::CircuitBreakerStateChange { .. }
                | ExecutionMessage::ResourceLimitExceeded { .. }
        )
    }
}

/// Header mutation operation for message header processing
///
/// This structure represents a planned modification to message headers
/// during query processing.
#[derive(Debug, Clone)]
pub struct HeaderMutation {
    /// The operation to perform
    pub operation: HeaderOperation,
    /// The header key to modify
    pub key: String,
    /// The value to set (for Set operations) or None (for Remove operations)
    pub value: Option<String>,
}

impl HeaderMutation {
    /// Create a new header set operation
    pub fn set(key: String, value: String) -> Self {
        Self {
            operation: HeaderOperation::Set,
            key,
            value: Some(value),
        }
    }

    /// Create a new header remove operation
    pub fn remove(key: String) -> Self {
        Self {
            operation: HeaderOperation::Remove,
            key,
            value: None,
        }
    }
}

/// Types of header operations that can be performed
#[derive(Debug, Clone)]
pub enum HeaderOperation {
    /// Set a header key to a specific value
    Set,
    /// Remove a header key
    Remove,
}

/// Query execution context and state
///
/// This structure maintains the execution state for a single active query,
/// including its lifecycle management and windowing state.
pub struct QueryExecution {
    /// The streaming query being executed
    pub query: StreamingQuery,
    /// Current execution state
    pub state: ExecutionState,
    /// Window state for windowed queries
    pub window_state: Option<WindowState>,
}

impl QueryExecution {
    /// Create a new query execution context
    pub fn new(query: StreamingQuery) -> Self {
        Self {
            query,
            state: ExecutionState::Running,
            window_state: None,
        }
    }

    /// Get the query being executed
    pub fn query(&self) -> &StreamingQuery {
        &self.query
    }

    /// Get the current execution state
    pub fn state(&self) -> &ExecutionState {
        &self.state
    }

    /// Set the execution state
    pub fn set_state(&mut self, state: ExecutionState) {
        self.state = state;
    }

    /// Check if this query execution is currently running
    pub fn is_running(&self) -> bool {
        matches!(self.state, ExecutionState::Running)
    }
}

/// Execution state for query lifecycle management
#[derive(Debug)]
pub enum ExecutionState {
    /// Query is actively running and processing records
    Running,
    /// Query is paused and not processing records
    Paused,
    /// Query has been stopped and will not process more records
    Stopped,
    /// Query encountered an error and cannot continue
    Error(String),
}

/// Window state for windowed query operations
///
/// This structure maintains the buffered records and timing information
/// needed for window-based query processing.
#[derive(Debug, Clone)]
pub struct WindowState {
    /// Window specification from the query
    pub window_spec: WindowSpec,
    /// Buffer of records within the current window
    pub buffer: Vec<StreamRecord>,
    /// Timestamp of the last window emission
    pub last_emit: i64,
}

impl WindowState {
    /// Create a new window state with the given specification
    pub fn new(window_spec: WindowSpec) -> Self {
        Self {
            window_spec,
            buffer: Vec::new(),
            last_emit: 0,
        }
    }

    /// Add a record to the window buffer
    pub fn add_record(&mut self, record: StreamRecord) {
        self.buffer.push(record);
    }

    /// Clear the window buffer
    pub fn clear_buffer(&mut self) {
        self.buffer.clear();
    }

    /// Get the number of records in the buffer
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Update the last emit timestamp
    pub fn update_last_emit(&mut self, timestamp: i64) {
        self.last_emit = timestamp;
    }
}
