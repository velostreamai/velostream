//! Internal types for the streaming SQL execution engine.
//!
//! This module contains implementation details that are not part of the public API.
//! These types support the internal operation of the execution engine including
//! GROUP BY state management, execution messaging, and query lifecycle management.

use super::types::{FieldValue, StreamRecord};
use crate::velostream::sql::ast::{Expr, RowsEmitMode, SelectField, StreamingQuery, WindowSpec};
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use rustc_hash::FxHashMap;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Optimized group key for high-performance GROUP BY operations
///
/// Phase 4B Optimization:
/// - Uses Arc<[FieldValue]> instead of Vec<String> to eliminate allocations
/// - Pre-computes hash for O(1) lookups
/// - Designed for use with FxHashMap (2-3x faster than standard HashMap)
///
/// Performance Impact:
/// - Eliminates 1M+ Vec allocations per 1M records
/// - Eliminates 1M-2M String allocations for group key components
/// - Reduces hash computation overhead with pre-computed hash
#[derive(Debug, Clone)]
pub struct GroupKey {
    /// Pre-computed hash for fast lookups (computed once, reused many times)
    hash: u64,
    /// Field values forming the group key (Arc for zero-copy sharing)
    values: Arc<[FieldValue]>,
}

impl GroupKey {
    /// Create a new GroupKey from field values
    pub fn new(values: Vec<FieldValue>) -> Self {
        // Pre-compute hash once
        let mut hasher = rustc_hash::FxHasher::default();
        for value in &values {
            value.hash(&mut hasher);
        }
        let hash = hasher.finish();

        Self {
            hash,
            values: Arc::from(values.into_boxed_slice()),
        }
    }

    /// Get the field values
    pub fn values(&self) -> &[FieldValue] {
        &self.values
    }

    /// Get the pre-computed hash
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        // Fast path: compare hashes first
        if self.hash != other.hash {
            return false;
        }
        // Slow path: compare actual values
        self.values.as_ref() == other.values.as_ref()
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash
        state.write_u64(self.hash);
    }
}

/// State for tracking GROUP BY aggregations across streaming records
///
/// This structure maintains the accumulated state for all active groups
/// in a GROUP BY query, tracking aggregate values and group membership.
///
/// Phase 4B Optimization: Uses FxHashMap with GroupKey for 2-3x faster lookups
#[derive(Debug, Clone)]
pub struct GroupByState {
    /// Map of group keys to their accumulated state
    /// Phase 4B: Changed from HashMap<Vec<String>, _> to FxHashMap<GroupKey, _>
    pub groups: FxHashMap<GroupKey, GroupAccumulator>,
    /// The GROUP BY expressions for this state
    pub group_expressions: Vec<Expr>,
    /// The SELECT fields to compute for each group
    pub select_fields: Vec<SelectField>,
    /// Optional HAVING clause
    pub having_clause: Option<Expr>,
}

impl GroupByState {
    /// Create a new GroupByState for the given expressions and fields
    ///
    /// Phase 4B: Pre-allocates FxHashMap with estimated capacity for better performance
    pub fn new(
        group_expressions: Vec<Expr>,
        select_fields: Vec<SelectField>,
        having_clause: Option<Expr>,
    ) -> Self {
        Self {
            // Pre-allocate for typical group count (10-100 groups)
            groups: FxHashMap::with_capacity_and_hasher(100, Default::default()),
            group_expressions,
            select_fields,
            having_clause,
        }
    }

    /// Get or create a group accumulator for the given key
    ///
    /// Phase 4B: Takes GroupKey instead of Vec<String>
    pub fn get_or_create_group(&mut self, key: GroupKey) -> &mut GroupAccumulator {
        self.groups.entry(key).or_default()
    }

    /// Get all group keys currently tracked
    ///
    /// Phase 4B: Returns references to GroupKey instead of Vec<String>
    pub fn get_group_keys(&self) -> Vec<&GroupKey> {
        self.groups.keys().collect()
    }

    /// Get a specific group's accumulator
    ///
    /// Phase 4B: Takes GroupKey instead of &[String]
    pub fn get_group(&self, key: &GroupKey) -> Option<&GroupAccumulator> {
        self.groups.get(key)
    }

    /// Get a mutable reference to a specific group's accumulator
    ///
    /// Phase 4B: Takes GroupKey instead of &[String]
    pub fn get_group_mut(&mut self, key: &GroupKey) -> Option<&mut GroupAccumulator> {
        self.groups.get_mut(key)
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

/// State management for ROWS WINDOW BUFFER processing
///
/// This structure maintains efficient state for row-based window operations with:
/// - VecDeque for fixed-size sliding window buffer
/// - BTreeMap for ranking/ordering indices (timestamps, sequence numbers)
/// - Support for time-gap detection for session-aware semantics
/// - Emission strategy tracking (EveryRecord vs BufferFull)
///
/// Phase 8.2: Core state infrastructure for ROWS WINDOW processing
#[derive(Debug, Clone)]
pub struct RowsWindowState {
    /// Unique identifier for this window state (query_id + partition combination)
    pub state_id: String,

    /// VecDeque buffer storing up to buffer_size records
    /// Provides O(1) insertion at tail and removal at head for sliding windows
    pub row_buffer: VecDeque<StreamRecord>,

    /// Maximum number of rows to maintain in the buffer
    pub buffer_size: u32,

    /// BTreeMap for ranking/ordering index (timestamp -> record index)
    /// Enables efficient RANK/DENSE_RANK/PERCENT_RANK computation
    pub ranking_index: BTreeMap<i64, Vec<usize>>,

    /// Emission strategy for this window
    pub emit_mode: RowsEmitMode,

    /// Last emission timestamp (millis since epoch)
    pub last_emit_timestamp: i64,

    /// Count of records processed in current window
    pub record_count: u32,

    /// Record count at last emission (for BufferFull tracking)
    pub last_emit_count: u32,

    /// Optional time gap threshold (millis) for session-aware gap detection
    pub time_gap: Option<i64>,

    /// Last observed timestamp (for gap detection)
    pub last_timestamp: Option<i64>,

    /// Whether a gap was detected (session boundary)
    pub gap_detected: bool,

    /// Partition key values (empty if no PARTITION BY)
    pub partition_values: Vec<String>,

    /// Row expiration mode: controls when rows are automatically removed from buffer
    /// None = Default (1 minute inactivity timeout)
    /// Some(RowExpirationMode::Never) = Keep rows indefinitely until buffer fills
    /// Some(RowExpirationMode::InactivityGap(duration)) = Custom timeout duration
    pub expire_after: Option<Duration>,

    /// Last time a record was added to this window buffer (for inactivity detection)
    pub last_activity_timestamp: Option<i64>,
}

impl RowsWindowState {
    /// Create a new RowsWindowState with the given configuration
    pub fn new(
        state_id: String,
        buffer_size: u32,
        emit_mode: RowsEmitMode,
        time_gap: Option<i64>,
    ) -> Self {
        Self {
            state_id,
            row_buffer: VecDeque::with_capacity(buffer_size as usize),
            buffer_size,
            ranking_index: BTreeMap::new(),
            emit_mode,
            last_emit_timestamp: 0,
            record_count: 0,
            last_emit_count: 0,
            time_gap,
            last_timestamp: None,
            gap_detected: false,
            partition_values: Vec::new(),
            expire_after: None,
            last_activity_timestamp: None,
        }
    }

    /// Add a record to the window buffer, maintaining size constraints
    ///
    /// Returns true if buffer is full and ready for emission
    pub fn add_record(&mut self, record: StreamRecord, timestamp: i64) -> bool {
        // Check for time gap (session boundary)
        if let Some(gap_threshold) = self.time_gap {
            if let Some(last_ts) = self.last_timestamp {
                if timestamp - last_ts > gap_threshold {
                    self.gap_detected = true;
                }
            }
        }
        self.last_timestamp = Some(timestamp);

        // Add to buffer, remove oldest if at capacity
        if self.row_buffer.len() >= self.buffer_size as usize {
            self.row_buffer.pop_front();
        }
        self.row_buffer.push_back(record);
        self.record_count += 1;

        // Update ranking index
        let index = self.row_buffer.len() - 1;
        self.ranking_index.entry(timestamp).or_default().push(index);

        // Check if buffer is full
        self.row_buffer.len() >= self.buffer_size as usize
    }

    /// Check if window should emit based on emission strategy
    pub fn should_emit(&self) -> bool {
        match self.emit_mode {
            RowsEmitMode::EveryRecord => true, // Emit on every record
            RowsEmitMode::BufferFull => {
                // Emit only when buffer reaches capacity
                self.row_buffer.len() >= self.buffer_size as usize
            }
        }
    }

    /// Check if a session gap was detected (for time-based partitioning)
    pub fn has_time_gap(&self) -> bool {
        self.gap_detected
    }

    /// Clear the gap detection flag after emission
    pub fn clear_gap_flag(&mut self) {
        self.gap_detected = false;
    }

    /// Get current buffer size
    pub fn buffer_len(&self) -> usize {
        self.row_buffer.len()
    }

    /// Clear the buffer and reset state for next window
    pub fn clear(&mut self) {
        self.row_buffer.clear();
        self.ranking_index.clear();
        self.record_count = 0;
        self.gap_detected = false;
    }

    /// Update last emission tracking
    pub fn update_emission(&mut self, timestamp: i64) {
        self.last_emit_timestamp = timestamp;
        self.last_emit_count = self.record_count;
    }

    /// Get reference to the row buffer
    pub fn rows(&self) -> &VecDeque<StreamRecord> {
        &self.row_buffer
    }

    /// Get mutable reference to the row buffer
    pub fn rows_mut(&mut self) -> &mut VecDeque<StreamRecord> {
        &mut self.row_buffer
    }

    /// Get reference to the ranking index
    pub fn index(&self) -> &BTreeMap<i64, Vec<usize>> {
        &self.ranking_index
    }

    /// Check and apply row expiration based on inactivity gap
    ///
    /// Returns true if rows were expired (buffer was cleared)
    pub fn check_and_apply_expiration(&mut self, current_timestamp: i64) -> bool {
        // If expiration is set to Never, don't expire any rows
        if self.expire_after == Some(Duration::from_secs(0)) {
            // Note: Never is represented as a zero duration sentinel value
            self.last_activity_timestamp = Some(current_timestamp);
            return false;
        }

        // Calculate the inactivity threshold in milliseconds
        let timeout_ms = match self.expire_after {
            Some(duration) => duration.as_millis() as i64,
            None => 60_000, // Default: 1 minute (60,000 ms)
        };

        // Check if we have a last activity timestamp
        if let Some(last_ts) = self.last_activity_timestamp {
            let gap = current_timestamp - last_ts;

            // If gap exceeds threshold, expire all rows
            if gap > timeout_ms {
                self.clear();
                self.last_activity_timestamp = Some(current_timestamp);
                return true;
            }
        }

        // Update the last activity timestamp for future checks
        self.last_activity_timestamp = Some(current_timestamp);
        false
    }

    /// Set the expiration timeout for this window
    pub fn set_expire_after(&mut self, duration: Option<Duration>) {
        self.expire_after = duration;
    }
}
