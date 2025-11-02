//! Core Traits for Window Processing V2
//!
//! This module defines the strategy pattern traits for pluggable window processing.

use super::types::SharedRecord;
use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Strategy trait for window boundary detection and buffer management.
///
/// Implementations:
/// - TumblingWindowStrategy: Non-overlapping, size-based windows
/// - SlidingWindowStrategy: Overlapping with advance interval
/// - SessionWindowStrategy: Gap-based event grouping
/// - RowsWindowStrategy: Row-count-based, memory-bounded
pub trait WindowStrategy: Send + Sync {
    /// Add a record to the window buffer.
    ///
    /// Returns true if the window should emit results (window boundary reached).
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError>;

    /// Get all records in the current window.
    fn get_window_records(&self) -> Vec<SharedRecord>;

    /// Check if the window should emit based on time/size criteria.
    fn should_emit(&self, current_time: i64) -> bool;

    /// Clear the window buffer (after emission).
    fn clear(&mut self);

    /// Get window statistics for monitoring.
    fn get_stats(&self) -> WindowStats;
}

/// Strategy trait for controlling when and how window results are emitted.
///
/// Implementations:
/// - EmitFinalStrategy: Emit once per window at end
/// - EmitChangesStrategy: Emit on every record update
pub trait EmissionStrategy: Send + Sync {
    /// Determine if results should be emitted for this record.
    ///
    /// Returns:
    /// - true: Emit results now
    /// - false: Do not emit yet
    fn should_emit_for_record(&self, record: &SharedRecord, window_complete: bool) -> bool;

    /// Process a record and return whether to emit.
    fn process_record(
        &mut self,
        record: SharedRecord,
        window_strategy: &mut dyn WindowStrategy,
    ) -> Result<EmitDecision, SqlError>;
}

/// Decision on whether to emit window results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitDecision {
    /// Emit results now
    Emit,
    /// Do not emit yet
    Skip,
    /// Emit and clear window
    EmitAndClear,
}

/// Window processing statistics for monitoring and optimization.
#[derive(Debug, Clone)]
pub struct WindowStats {
    /// Number of records currently in the window buffer
    pub record_count: usize,

    /// Window start time (milliseconds)
    pub window_start_time: Option<i64>,

    /// Window end time (milliseconds)
    pub window_end_time: Option<i64>,

    /// Number of emissions produced
    pub emission_count: usize,

    /// Total memory used by buffer (estimated bytes)
    pub buffer_size_bytes: usize,
}

/// Strategy trait for grouping window results.
///
/// This trait handles GROUP BY logic within windows.
pub trait GroupByStrategy: Send + Sync {
    /// Add a record to the appropriate group.
    fn add_to_group(
        &mut self,
        record: &SharedRecord,
        group_keys: &[String],
    ) -> Result<(), SqlError>;

    /// Get all group keys currently tracked.
    fn get_group_keys(&self) -> Vec<Vec<FieldValue>>;

    /// Get all records for a specific group.
    fn get_group_records(&self, group_key: &[FieldValue]) -> Vec<SharedRecord>;

    /// Clear all groups.
    fn clear(&mut self);

    /// Get the number of groups.
    fn group_count(&self) -> usize;
}

/// Builder for window strategies.
///
/// Provides a fluent API for constructing window processing pipelines.
pub struct WindowStrategyBuilder {
    window_type: WindowType,
    emission_mode: EmissionMode,
    window_size_ms: Option<i64>,
    advance_interval_ms: Option<i64>,
    gap_duration_ms: Option<i64>,
    buffer_rows: Option<usize>,
}

/// Window type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
    Tumbling,
    Sliding,
    Session,
    Rows,
}

/// Emission mode enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionMode {
    EmitFinal,
    EmitChanges,
}

impl WindowStrategyBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            window_type: WindowType::Tumbling,
            emission_mode: EmissionMode::EmitFinal,
            window_size_ms: None,
            advance_interval_ms: None,
            gap_duration_ms: None,
            buffer_rows: None,
        }
    }

    /// Set the window type.
    pub fn window_type(mut self, window_type: WindowType) -> Self {
        self.window_type = window_type;
        self
    }

    /// Set the emission mode.
    pub fn emission_mode(mut self, mode: EmissionMode) -> Self {
        self.emission_mode = mode;
        self
    }

    /// Set the window size in milliseconds.
    pub fn window_size_ms(mut self, size_ms: i64) -> Self {
        self.window_size_ms = Some(size_ms);
        self
    }

    /// Set the advance interval in milliseconds (for SLIDING windows).
    pub fn advance_interval_ms(mut self, interval_ms: i64) -> Self {
        self.advance_interval_ms = Some(interval_ms);
        self
    }

    /// Set the gap duration in milliseconds (for SESSION windows).
    pub fn gap_duration_ms(mut self, gap_ms: i64) -> Self {
        self.gap_duration_ms = Some(gap_ms);
        self
    }

    /// Set the buffer size in rows (for ROWS windows).
    pub fn buffer_rows(mut self, rows: usize) -> Self {
        self.buffer_rows = Some(rows);
        self
    }
}

impl Default for WindowStrategyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_strategy_builder() {
        let builder = WindowStrategyBuilder::new()
            .window_type(WindowType::Tumbling)
            .emission_mode(EmissionMode::EmitFinal)
            .window_size_ms(60000);

        assert_eq!(builder.window_type, WindowType::Tumbling);
        assert_eq!(builder.emission_mode, EmissionMode::EmitFinal);
        assert_eq!(builder.window_size_ms, Some(60000));
    }

    #[test]
    fn test_emit_decision() {
        assert_eq!(EmitDecision::Emit, EmitDecision::Emit);
        assert_ne!(EmitDecision::Emit, EmitDecision::Skip);
    }

    #[test]
    fn test_window_stats() {
        let stats = WindowStats {
            record_count: 100,
            window_start_time: Some(1000),
            window_end_time: Some(2000),
            emission_count: 5,
            buffer_size_bytes: 10240,
        };

        assert_eq!(stats.record_count, 100);
        assert_eq!(stats.window_start_time, Some(1000));
    }
}
