//! Window state management for streaming SQL window operations.

use std::collections::VecDeque;

use crate::ferris::sql::ast::WindowSpec;
use crate::ferris::sql::execution::types::StreamRecord;

/// State management for window operations in streaming SQL queries
///
/// WindowState maintains all necessary state for window processing including:
/// - Buffered records within the current window
/// - Timing information for window boundaries and emission
/// - Memory management with configurable limits
/// - Automatic record eviction based on window specifications
#[derive(Debug)]
pub struct WindowState {
    /// Window specification (tumbling, sliding, session)
    pub window_spec: WindowSpec,
    /// Buffer of records in the current window
    pub buffer: VecDeque<StreamRecord>,
    /// Timestamp of last window emission
    pub last_emit: i64,
    /// Start time of current window
    pub window_start: Option<i64>,
    /// End time of current window
    pub window_end: Option<i64>,
    /// Maximum buffer size to prevent memory issues
    pub max_buffer_size: usize,
}

impl WindowState {
    /// Create a new window state
    pub fn new(window_spec: WindowSpec) -> Self {
        Self {
            window_spec,
            buffer: VecDeque::new(),
            last_emit: 0,
            window_start: None,
            window_end: None,
            max_buffer_size: 10000, // Default limit
        }
    }

    /// Create a new window state with custom buffer size
    pub fn with_buffer_size(window_spec: WindowSpec, max_buffer_size: usize) -> Self {
        Self {
            window_spec,
            buffer: VecDeque::new(),
            last_emit: 0,
            window_start: None,
            window_end: None,
            max_buffer_size,
        }
    }

    /// Add a record to the window buffer
    pub fn add_record(&mut self, record: StreamRecord) -> Result<(), String> {
        if self.buffer.len() >= self.max_buffer_size {
            return Err(format!(
                "Window buffer overflow: exceeds maximum size of {}",
                self.max_buffer_size
            ));
        }

        self.buffer.push_back(record);
        Ok(())
    }

    /// Get all records in the current window
    pub fn get_window_records(&self) -> &VecDeque<StreamRecord> {
        &self.buffer
    }

    /// Get mutable access to window records
    pub fn get_window_records_mut(&mut self) -> &mut VecDeque<StreamRecord> {
        &mut self.buffer
    }

    /// Clear the window buffer
    pub fn clear_buffer(&mut self) {
        self.buffer.clear();
        self.window_start = None;
        self.window_end = None;
    }

    /// Remove old records that are outside the window
    pub fn evict_old_records(&mut self, current_time: i64) {
        match &self.window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let cutoff_time = current_time - window_size_ms;

                while let Some(record) = self.buffer.front() {
                    if record.timestamp < cutoff_time {
                        self.buffer.pop_front();
                    } else {
                        break;
                    }
                }
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let cutoff_time = current_time - window_size_ms;

                while let Some(record) = self.buffer.front() {
                    if record.timestamp < cutoff_time {
                        self.buffer.pop_front();
                    } else {
                        break;
                    }
                }
            }
            WindowSpec::Session { gap, .. } => {
                let gap_ms = gap.as_millis() as i64;
                let cutoff_time = current_time - gap_ms;

                // For session windows, evict records older than the gap
                while let Some(record) = self.buffer.front() {
                    if record.timestamp < cutoff_time {
                        self.buffer.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    /// Update the last emission time
    pub fn update_last_emit(&mut self, timestamp: i64) {
        self.last_emit = timestamp;
    }

    /// Set window boundaries
    pub fn set_window_boundaries(&mut self, start: i64, end: i64) {
        self.window_start = Some(start);
        self.window_end = Some(end);
    }

    /// Get the current window size in milliseconds
    pub fn get_window_size_ms(&self) -> i64 {
        match &self.window_spec {
            WindowSpec::Tumbling { size, .. } => size.as_millis() as i64,
            WindowSpec::Sliding { size, .. } => size.as_millis() as i64,
            WindowSpec::Session { gap, .. } => gap.as_millis() as i64,
        }
    }

    /// Get the window advance interval in milliseconds (for sliding windows)
    pub fn get_advance_ms(&self) -> Option<i64> {
        match &self.window_spec {
            WindowSpec::Sliding { advance, .. } => Some(advance.as_millis() as i64),
            _ => None,
        }
    }

    /// Check if the window is a session window
    pub fn is_session_window(&self) -> bool {
        matches!(self.window_spec, WindowSpec::Session { .. })
    }

    /// Check if the window is a tumbling window
    pub fn is_tumbling_window(&self) -> bool {
        matches!(self.window_spec, WindowSpec::Tumbling { .. })
    }

    /// Check if the window is a sliding window
    pub fn is_sliding_window(&self) -> bool {
        matches!(self.window_spec, WindowSpec::Sliding { .. })
    }

    /// Get the time column for this window (if specified)
    pub fn get_time_column(&self) -> Option<&str> {
        self.window_spec.time_column()
    }

    /// Get partition keys for session windows
    pub fn get_partition_keys(&self) -> Option<&[String]> {
        match &self.window_spec {
            WindowSpec::Session { partition_by, .. } => Some(partition_by),
            _ => None,
        }
    }

    /// Get the number of records in the current window
    pub fn record_count(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get the earliest timestamp in the window
    pub fn earliest_timestamp(&self) -> Option<i64> {
        self.buffer.front().map(|record| record.timestamp)
    }

    /// Get the latest timestamp in the window
    pub fn latest_timestamp(&self) -> Option<i64> {
        self.buffer.back().map(|record| record.timestamp)
    }

    /// Calculate memory usage estimate
    pub fn estimated_memory_bytes(&self) -> usize {
        // Rough estimate: each record is approximately 1KB
        self.buffer.len() * 1024
    }

    /// Check if the window needs to be flushed due to memory pressure
    pub fn needs_flush(&self) -> bool {
        self.buffer.len() >= self.max_buffer_size * 9 / 10 // 90% of max capacity
    }
}
