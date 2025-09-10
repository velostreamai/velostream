//! Processor execution context and state management

use crate::ferris::datasource::{DataReader, DataWriter, SourceOffset};
use crate::ferris::schema::{Schema, StreamHandle};
use crate::ferris::sql::execution::internal::WindowState;
use crate::ferris::sql::execution::performance::PerformanceMonitor;
use crate::ferris::sql::execution::StreamRecord;
use crate::ferris::sql::SqlError;
use std::collections::HashMap;
use std::sync::Arc;

use super::join_context::JoinContext;

/// Main execution context for query processing
///
/// This struct maintains all the state needed for query execution including:
/// - Data source connections and readers
/// - Window states and processing context  
/// - Performance monitoring and metrics
/// - Schema information and stream handles
pub struct ProcessorContext {
    /// Current record count for limit checking
    pub record_count: u64,
    /// Maximum record count (for LIMIT)
    pub max_records: Option<u64>,
    /// Window processing state (legacy - kept for compatibility)
    pub window_context: Option<WindowContext>,
    /// JOIN processing utilities
    pub join_context: JoinContext,
    /// GROUP BY processing state
    pub group_by_states: HashMap<String, crate::ferris::sql::execution::internal::GroupByState>,
    /// Schema registry for introspection (SHOW/DESCRIBE operations)
    pub schemas: HashMap<String, Schema>,
    /// Stream handles registry
    pub stream_handles: HashMap<String, StreamHandle>,
    /// Data sources for subquery execution
    /// Maps table/stream name to available records for querying
    pub data_sources: HashMap<String, Vec<StreamRecord>>,

    // === PLUGGABLE DATA SOURCE SUPPORT ===
    /// Multiple input data readers (e.g., Kafka + S3 + File)
    /// Maps source name to reader instance for heterogeneous data flow
    pub data_readers: HashMap<String, Box<dyn DataReader>>,
    /// Multiple output data writers (e.g., Iceberg + Kafka + ClickHouse)
    /// Maps sink name to writer instance for heterogeneous data flow
    pub data_writers: HashMap<String, Box<dyn DataWriter>>,
    /// Active source for current read operation
    /// Enables context.read() to work without specifying source each time
    pub active_reader: Option<String>,
    /// Active sink for current write operation
    /// Enables context.write() to work without specifying sink each time
    pub active_writer: Option<String>,
    /// Source positions/offsets for commit/seek operations
    pub source_positions: HashMap<String, SourceOffset>,

    // === HIGH-PERFORMANCE WINDOW STATE MANAGEMENT ===
    /// Persistent window states for queries processed in this context
    /// Using Vec for cache efficiency - most contexts handle 1-2 queries
    pub persistent_window_states: Vec<(String, WindowState)>,
    /// Track which states were modified for efficient persistence (bit mask)
    pub dirty_window_states: u32,
    /// Generic metadata storage for processors (e.g., job management)
    pub metadata: HashMap<String, String>,

    // === PERFORMANCE MONITORING ===
    /// Optional performance monitor for query tracking
    pub performance_monitor: Option<Arc<PerformanceMonitor>>,
}

/// Window processing context
pub struct WindowContext {
    /// Buffered records for windowing
    pub buffer: Vec<StreamRecord>,
    /// Last emission time
    pub last_emit: i64,
    /// Should emit in this processing cycle
    pub should_emit: bool,
}

impl ProcessorContext {
    /// Create a new processor context with a query ID
    pub fn new(_query_id: &str) -> Self {
        Self {
            record_count: 0,
            max_records: None,
            window_context: None,
            join_context: JoinContext::new(),
            group_by_states: HashMap::new(),
            schemas: HashMap::new(),
            stream_handles: HashMap::new(),
            data_sources: HashMap::new(),
            data_readers: HashMap::new(),
            data_writers: HashMap::new(),
            active_reader: None,
            active_writer: None,
            source_positions: HashMap::new(),
            persistent_window_states: Vec::new(),
            dirty_window_states: 0,
            metadata: HashMap::new(),
            performance_monitor: None,
        }
    }

    /// Create a new processor context with heterogeneous sources and sinks
    pub fn new_with_sources(
        query_id: &str,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
    ) -> Self {
        let mut context = Self::new(query_id);
        context.data_readers = readers;
        context.data_writers = writers;

        // Set first reader and writer as active by default
        context.active_reader = context.data_readers.keys().next().cloned();
        context.active_writer = context.data_writers.keys().next().cloned();

        context
    }

    /// Set performance monitor for query tracking
    pub fn set_performance_monitor(&mut self, monitor: Arc<PerformanceMonitor>) {
        self.performance_monitor = Some(monitor);
    }

    /// Get performance monitor if available
    pub fn get_performance_monitor(&self) -> Option<&Arc<PerformanceMonitor>> {
        self.performance_monitor.as_ref()
    }

    /// Set metadata value for job tracking or other purposes
    pub fn set_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    // === HIGH-PERFORMANCE WINDOW STATE METHODS ===

    /// Get or create a window state for a query (O(1) for small contexts, optimized for threading)
    pub fn get_or_create_window_state(
        &mut self,
        query_id: &str,
        window_spec: &crate::ferris::sql::ast::WindowSpec,
    ) -> &mut WindowState {
        // Check if window state already exists
        for (idx, (stored_query_id, _)) in self.persistent_window_states.iter().enumerate() {
            if stored_query_id == query_id {
                // Mark as dirty for persistence
                if idx < 32 {
                    // Protect against bit mask overflow
                    self.dirty_window_states |= 1 << (idx as u32);
                }
                // Return mutable reference (separate borrow)
                return &mut self.persistent_window_states[idx].1;
            }
        }

        // Create new state if not found (happens rarely)
        let new_state = WindowState::new(window_spec.clone());
        let new_idx = self.persistent_window_states.len();
        self.persistent_window_states
            .push((query_id.to_string(), new_state));

        // Mark new state as dirty
        if new_idx < 32 {
            // Protect against bit mask overflow
            self.dirty_window_states |= 1 << (new_idx as u32);
        }

        &mut self.persistent_window_states[new_idx].1
    }

    /// Get window state if it exists (read-only, no dirty marking)
    pub fn get_window_state(&self, query_id: &str) -> Option<&WindowState> {
        self.persistent_window_states
            .iter()
            .find(|(stored_query_id, _)| stored_query_id == query_id)
            .map(|(_, window_state)| window_state)
    }

    /// Load window states from engine (called during context creation)
    pub fn load_window_states(&mut self, states: Vec<(String, WindowState)>) {
        self.persistent_window_states = states;
        self.dirty_window_states = 0; // Start clean
    }

    /// Get modified window states for persistence (returns only changed states)
    pub fn get_dirty_window_states(&self) -> Vec<(String, WindowState)> {
        let mut dirty_states = Vec::new();

        for (idx, (query_id, window_state)) in self.persistent_window_states.iter().enumerate() {
            if idx < 32 && (self.dirty_window_states & (1 << idx)) != 0 {
                dirty_states.push((query_id.clone(), window_state.clone()));
            }
        }

        dirty_states
    }

    /// Clear dirty flags (called after persistence)
    pub fn clear_dirty_flags(&mut self) {
        self.dirty_window_states = 0;
    }

    // === HETEROGENEOUS DATA SOURCE METHODS ===

    /// Register a data reader for a specific source
    pub fn add_reader(&mut self, source_name: &str, reader: Box<dyn DataReader>) {
        self.data_readers.insert(source_name.to_string(), reader);
        // Set as active if no active reader exists
        if self.active_reader.is_none() {
            self.active_reader = Some(source_name.to_string());
        }
    }

    /// Register a data writer for a specific sink
    pub fn add_writer(&mut self, sink_name: &str, writer: Box<dyn DataWriter>) {
        self.data_writers.insert(sink_name.to_string(), writer);
        // Set as active if no active writer exists
        if self.active_writer.is_none() {
            self.active_writer = Some(sink_name.to_string());
        }
    }

    /// Read from a specific data source
    pub async fn read_from(&mut self, source_name: &str) -> Result<Vec<StreamRecord>, SqlError> {
        let reader =
            self.data_readers
                .get_mut(source_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", source_name),
                    query: None,
                })?;

        reader.read().await.map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to read from source '{}': {}", source_name, e),
            query: None,
        })
    }

    /// Write to a specific data sink
    pub async fn write_to(
        &mut self,
        sink_name: &str,
        record: StreamRecord,
    ) -> Result<(), SqlError> {
        let writer =
            self.data_writers
                .get_mut(sink_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", sink_name),
                    query: None,
                })?;

        writer
            .write(record)
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to write to sink '{}': {}", sink_name, e),
                query: None,
            })
    }

    /// Read from the active data source
    pub async fn read(&mut self) -> Result<Vec<StreamRecord>, SqlError> {
        let source_name = self
            .active_reader
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active data source configured".to_string(),
                query: None,
            })?;
        self.read_from(&source_name).await
    }

    /// Write to the active data sink
    pub async fn write(&mut self, record: StreamRecord) -> Result<(), SqlError> {
        let sink_name = self
            .active_writer
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active data sink configured".to_string(),
                query: None,
            })?;
        self.write_to(&sink_name, record).await
    }

    /// Set the active data source for subsequent read() calls
    pub fn set_active_reader(&mut self, source_name: &str) -> Result<(), SqlError> {
        if !self.data_readers.contains_key(source_name) {
            return Err(SqlError::ExecutionError {
                message: format!("Data source '{}' not found in context", source_name),
                query: None,
            });
        }
        self.active_reader = Some(source_name.to_string());
        Ok(())
    }

    /// Set the active data sink for subsequent write() calls
    pub fn set_active_writer(&mut self, sink_name: &str) -> Result<(), SqlError> {
        if !self.data_writers.contains_key(sink_name) {
            return Err(SqlError::ExecutionError {
                message: format!("Data sink '{}' not found in context", sink_name),
                query: None,
            });
        }
        self.active_writer = Some(sink_name.to_string());
        Ok(())
    }

    /// Get all available data source names
    pub fn list_sources(&self) -> Vec<String> {
        self.data_readers.keys().cloned().collect()
    }

    /// Get all available data sink names
    pub fn list_sinks(&self) -> Vec<String> {
        self.data_writers.keys().cloned().collect()
    }

    /// Write a batch of records to a specific sink
    pub async fn write_batch_to(
        &mut self,
        sink_name: &str,
        records: Vec<StreamRecord>,
    ) -> Result<(), SqlError> {
        let writer =
            self.data_writers
                .get_mut(sink_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", sink_name),
                    query: None,
                })?;

        writer
            .write_batch(records)
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to write batch to sink '{}': {}", sink_name, e),
                query: None,
            })
    }

    /// Commit reads from a specific source
    pub async fn commit_source(&mut self, source_name: &str) -> Result<(), SqlError> {
        let reader =
            self.data_readers
                .get_mut(source_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", source_name),
                    query: None,
                })?;

        reader.commit().await.map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to commit source '{}': {}", source_name, e),
            query: None,
        })
    }

    /// Commit writes to a specific sink
    pub async fn commit_sink(&mut self, sink_name: &str) -> Result<(), SqlError> {
        let writer =
            self.data_writers
                .get_mut(sink_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", sink_name),
                    query: None,
                })?;

        writer.commit().await.map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to commit sink '{}': {}", sink_name, e),
            query: None,
        })
    }

    /// Begin a transaction for the active reader
    pub async fn begin_reader_transaction(&mut self) -> Result<bool, SqlError> {
        let reader_name = self
            .active_reader
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active reader set for transaction".to_string(),
                query: None,
            })?;

        let reader =
            self.data_readers
                .get_mut(&reader_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", reader_name),
                    query: None,
                })?;

        reader
            .begin_transaction()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to begin transaction for source '{}': {}",
                    reader_name, e
                ),
                query: None,
            })
    }

    /// Begin a transaction for the active writer
    pub async fn begin_writer_transaction(&mut self) -> Result<bool, SqlError> {
        let writer_name = self
            .active_writer
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active writer set for transaction".to_string(),
                query: None,
            })?;

        let writer =
            self.data_writers
                .get_mut(&writer_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", writer_name),
                    query: None,
                })?;

        writer
            .begin_transaction()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to begin transaction for sink '{}': {}",
                    writer_name, e
                ),
                query: None,
            })
    }

    /// Commit transaction for the active writer
    pub async fn commit_writer(&mut self) -> Result<(), SqlError> {
        let writer_name = self
            .active_writer
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active writer set for transaction commit".to_string(),
                query: None,
            })?;

        let writer =
            self.data_writers
                .get_mut(&writer_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", writer_name),
                    query: None,
                })?;

        writer
            .commit_transaction()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to commit transaction for sink '{}': {}",
                    writer_name, e
                ),
                query: None,
            })
    }

    /// Abort transaction for the active writer
    pub async fn abort_writer(&mut self) -> Result<(), SqlError> {
        let writer_name = self
            .active_writer
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active writer set for transaction abort".to_string(),
                query: None,
            })?;

        let writer =
            self.data_writers
                .get_mut(&writer_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", writer_name),
                    query: None,
                })?;

        writer
            .abort_transaction()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to abort transaction for sink '{}': {}",
                    writer_name, e
                ),
                query: None,
            })
    }

    /// Abort transaction for the active reader  
    pub async fn abort_reader(&mut self) -> Result<(), SqlError> {
        let reader_name = self
            .active_reader
            .clone()
            .ok_or_else(|| SqlError::ExecutionError {
                message: "No active reader set for transaction abort".to_string(),
                query: None,
            })?;

        let reader =
            self.data_readers
                .get_mut(&reader_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", reader_name),
                    query: None,
                })?;

        reader
            .abort_transaction()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to abort transaction for source '{}': {}",
                    reader_name, e
                ),
                query: None,
            })
    }

    /// Seek to a specific position in a data source
    pub async fn seek_source(
        &mut self,
        source_name: &str,
        offset: SourceOffset,
    ) -> Result<(), SqlError> {
        let reader =
            self.data_readers
                .get_mut(source_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", source_name),
                    query: None,
                })?;

        reader
            .seek(offset.clone())
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to seek in source '{}': {}", source_name, e),
                query: None,
            })?;

        // Store the new position
        self.source_positions
            .insert(source_name.to_string(), offset);
        Ok(())
    }

    /// Get current position of a data source
    pub fn get_source_position(&self, source_name: &str) -> Option<&SourceOffset> {
        self.source_positions.get(source_name)
    }

    /// Flush writes to all sinks
    pub async fn flush_all(&mut self) -> Result<(), SqlError> {
        for (sink_name, writer) in self.data_writers.iter_mut() {
            writer.flush().await.map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to flush sink '{}': {}", sink_name, e),
                query: None,
            })?;
        }
        Ok(())
    }

    /// Check if a specific source has more data available
    pub async fn has_more_data(&self, source_name: &str) -> Result<bool, SqlError> {
        let reader =
            self.data_readers
                .get(source_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data source '{}' not found in context", source_name),
                    query: None,
                })?;

        reader
            .has_more()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to check data availability for source '{}': {}",
                    source_name, e
                ),
                query: None,
            })
    }
}
