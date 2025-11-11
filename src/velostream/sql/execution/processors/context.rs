//! Processor execution context and state management

use crate::velostream::datasource::{DataReader, DataWriter, SourceOffset};
use crate::velostream::schema::{Schema, StreamHandle};
use crate::velostream::sql::SqlError;
use crate::velostream::sql::ast::RowsEmitMode;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::internal::{GroupByState, RowsWindowState, WindowState};
use crate::velostream::sql::execution::performance::PerformanceMonitor;
use crate::velostream::sql::execution::watermarks::WatermarkManager;
use crate::velostream::table::unified_table::UnifiedTable;
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
    /// FR-082 Phase 6.5: Single source of truth for group aggregations
    /// Wrapped in Arc<GroupByState> to eliminate cloning without needing an additional Mutex
    /// ProcessorContext owns this state (not borrowed from elsewhere)
    /// Processors modify in place during batch processing
    pub group_by_states: HashMap<String, Arc<GroupByState>>,
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
    /// FR-082 Phase 6.5: Single source of truth for window state (owned by ProcessorContext)
    /// Using Vec for cache efficiency - most contexts handle 1-2 queries
    /// No locks, no duplicates - state is directly owned and modified in place
    pub persistent_window_states: Vec<(String, WindowState)>,
    /// Track which states were modified for efficient persistence (bit mask)
    pub dirty_window_states: u32,
    /// Generic metadata storage for processors (e.g., job management)
    pub metadata: HashMap<String, String>,

    // === PERFORMANCE MONITORING ===
    /// Optional performance monitor for query tracking
    pub performance_monitor: Option<Arc<PerformanceMonitor>>,

    // === PHASE 1B: TIME SEMANTICS & WATERMARKS ===
    /// Optional watermark manager for event-time processing
    /// Only activated when event-time processing is explicitly enabled
    pub watermark_manager: Option<Arc<WatermarkManager>>,

    // === KTable/Table STATE MANAGEMENT ===
    /// State tables for SQL subquery execution
    /// Maps table name to SQL-queryable Table data source for subquery operations
    pub state_tables: HashMap<String, Arc<dyn UnifiedTable>>,

    // === CORRELATION CONTEXT FOR SUBQUERIES ===
    /// Current correlation context for correlated subqueries
    /// This replaces the global lazy_static state for thread safety
    pub correlation_context: Option<TableReference>,

    // === FR-079 PHASE 4: RESULT QUEUE FOR MULTI-EMISSION ===
    /// Queue for pending GROUP BY + EMIT CHANGES results
    /// Maps query_id to Vec of StreamRecords waiting to be emitted
    /// Used for windowed queries that produce multiple group results
    pub pending_results: HashMap<String, Vec<StreamRecord>>,

    // === FR-078: ALIAS REUSE VALIDATION ===
    /// Track which SELECT queries have had their expressions validated
    /// This avoids expensive per-record validation across all datasources
    /// Maps query_id to true after first validation
    pub validated_select_queries: std::collections::HashSet<String>,

    // === PHASE 8.2: ROWS WINDOW STATE MANAGEMENT ===
    /// ROWS WINDOW states for bounded buffer windowing
    /// Maps state_key (format: "rows_window:query_id:partition_key") to RowsWindowState
    /// Used for maintaining per-partition row buffers with configurable emission strategies
    pub rows_window_states: HashMap<String, RowsWindowState>,

    // === FR-081 PHASE 2A: WINDOW V2 STATE MANAGEMENT ===
    /// Window V2 states for trait-based window processing
    /// Maps state_key (format: "window_v2:query_id") to WindowV2State
    /// Used for high-performance window processing with Arc<StreamRecord> zero-copy semantics
    pub window_v2_states: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,

    // === STREAMING ENGINE CONFIGURATION ===
    /// Optional streaming engine configuration
    /// When None, uses default configuration (all enhancements disabled)
    pub streaming_config: Option<crate::velostream::sql::execution::config::StreamingConfig>,
}

/// Table reference with optional alias for SQL parsing and correlation
#[derive(Debug, Clone, PartialEq)]
pub struct TableReference {
    /// The table or stream name
    pub name: String,
    /// Optional alias for the table
    pub alias: Option<String>,
}

impl TableReference {
    /// Create a new table reference without alias
    pub fn new(name: String) -> Self {
        Self { name, alias: None }
    }

    /// Create a table reference with an alias
    pub fn with_alias(name: String, alias: String) -> Self {
        Self {
            name,
            alias: Some(alias),
        }
    }
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
            watermark_manager: None, // Disabled by default for backward compatibility
            state_tables: HashMap::new(),
            correlation_context: None,
            pending_results: HashMap::new(), // FR-079 Phase 4: Initialize result queue
            validated_select_queries: std::collections::HashSet::new(), // FR-078: Track validated SELECT queries
            rows_window_states: HashMap::new(), // Phase 8.2: Initialize ROWS window state map
            window_v2_states: HashMap::new(),   // FR-081 Phase 2A: Initialize window v2 state map
            streaming_config: None,             // Use default config unless explicitly set
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
        window_spec: &crate::velostream::sql::ast::WindowSpec,
    ) -> &mut WindowState {
        // Check if window state already exists
        for (idx, (stored_query_id, state)) in self.persistent_window_states.iter().enumerate() {
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

    /// Get modified window states as mutable references (O(1) - no cloning)
    /// This eliminates the O(N²) buffer cloning issue
    pub fn get_dirty_window_states_mut(&mut self) -> Vec<(String, &mut WindowState)> {
        let mut dirty_states = Vec::new();
        let dirty_mask = self.dirty_window_states;

        for (idx, (query_id, window_state)) in self.persistent_window_states.iter_mut().enumerate()
        {
            if idx < 32 && (dirty_mask & (1 << idx)) != 0 {
                dirty_states.push((query_id.clone(), window_state));
            }
        }

        dirty_states
    }

    /// Clear dirty flags (called after persistence)
    pub fn clear_dirty_flags(&mut self) {
        self.dirty_window_states = 0;
    }

    // === PHASE 8.2: ROWS WINDOW STATE METHODS ===

    /// Get or create a ROWS WINDOW state for a given state key
    ///
    /// Phase 8.3: Provides efficient per-partition ROWS window state management
    /// - state_key: Unique identifier (format: "rows_window:query_id:partition_key")
    /// - buffer_size: Maximum rows to keep in buffer
    /// - emit_mode: Emission strategy (EveryRecord or BufferFull)
    /// - time_gap: Optional gap threshold for time-gap detection
    /// - partition_key: Partition key for this window state
    pub fn get_or_create_rows_window_state(
        &mut self,
        state_key: &str,
        buffer_size: u32,
        emit_mode: RowsEmitMode,
        time_gap: Option<i64>,
        partition_key: String,
    ) -> &mut RowsWindowState {
        // Use entry API for efficient get-or-insert
        self.rows_window_states
            .entry(state_key.to_string())
            .or_insert_with(|| {
                let mut state =
                    RowsWindowState::new(state_key.to_string(), buffer_size, emit_mode, time_gap);
                // Set partition values after creation
                state.partition_values = vec![partition_key];
                state
            })
    }

    /// Get ROWS WINDOW state if it exists (read-only)
    pub fn get_rows_window_state(&self, state_key: &str) -> Option<&RowsWindowState> {
        self.rows_window_states.get(state_key)
    }

    /// Clear all ROWS WINDOW states (for context reset)
    pub fn clear_rows_window_states(&mut self) {
        self.rows_window_states.clear();
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
        records: Vec<std::sync::Arc<StreamRecord>>,
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

    /// Write a shared batch of records to a specific sink (zero-copy for multi-sink scenarios)
    ///
    /// This method is more efficient when writing the same batch to multiple sinks,
    /// as it avoids cloning the entire Vec for each sink.
    pub async fn write_batch_to_shared(
        &mut self,
        sink_name: &str,
        records: &[std::sync::Arc<StreamRecord>],
    ) -> Result<(), SqlError> {
        let writer =
            self.data_writers
                .get_mut(sink_name)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Data sink '{}' not found in context", sink_name),
                    query: None,
                })?;

        writer
            .write_batch_shared(records)
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

    // === PHASE 1B: WATERMARK MANAGEMENT METHODS ===

    /// Enable watermark processing by setting a WatermarkManager
    pub fn enable_watermarks(&mut self, manager: Arc<WatermarkManager>) {
        self.watermark_manager = Some(manager);
    }

    /// Check if watermark processing is enabled
    pub fn has_watermarks_enabled(&self) -> bool {
        self.watermark_manager.is_some()
    }

    /// Update watermark for a source and get watermark event if generated
    pub fn update_watermark(
        &self,
        source_id: &str,
        record: &StreamRecord,
    ) -> Option<crate::velostream::sql::execution::watermarks::WatermarkEvent> {
        self.watermark_manager
            .as_ref()?
            .update_watermark(source_id, record)
    }

    /// Check if a record is late based on current watermark
    pub fn is_late_record(&self, record: &StreamRecord) -> bool {
        self.watermark_manager
            .as_ref()
            .map(|wm| wm.is_late(record))
            .unwrap_or(false)
    }

    /// Get the current global watermark
    pub fn get_global_watermark(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.watermark_manager
            .as_ref()
            .and_then(|wm| wm.get_global_watermark())
    }

    /// Calculate how late a record is
    pub fn calculate_record_lateness(&self, record: &StreamRecord) -> Option<std::time::Duration> {
        self.watermark_manager
            .as_ref()
            .and_then(|wm| wm.calculate_lateness(record))
    }

    // === KTable/Table STATE MANAGEMENT METHODS ===

    /// Get a reference table by name for subquery execution
    ///
    /// Returns a reference to the SQL-queryable table for use in subquery operations.
    /// This is used by the SubqueryExecutor to access Table state during query execution.
    ///
    /// # Arguments
    /// * `table_name` - Name of the reference table
    ///
    /// # Returns
    /// * `Ok(Arc<dyn UnifiedTable>)` - Reference to the queryable table
    /// * `Err(SqlError)` - Table not found or access error
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use velostream::velostream::sql::execution::processors::context::ProcessorContext;
    /// # fn example(context: &ProcessorContext) -> Result<(), velostream::velostream::sql::error::SqlError> {
    /// let user_table = context.get_table("users")?;
    /// let active_users = user_table.sql_filter("active = true")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_table(&self, table_name: &str) -> Result<Arc<dyn UnifiedTable>, SqlError> {
        self.state_tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| SqlError::ExecutionError {
                message: format!(
                    "Reference table '{}' not found in processor context",
                    table_name
                ),
                query: None,
            })
    }

    /// Load a reference table into the processor context
    ///
    /// Registers a Table as a reference data source that can be used for subquery
    /// operations. The table will be available via get_table() for SQL subqueries.
    ///
    /// # Arguments
    /// * `table_name` - Name to register the table under
    /// * `table` - Table instance to register as a data source
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use velostream::velostream::table::unified_table::OptimizedTableImpl;
    /// # use velostream::velostream::table::sql::TableDataSource;
    /// # use velostream::velostream::sql::execution::processors::context::ProcessorContext;
    /// # async fn example(mut context: ProcessorContext) -> Result<(), Box<dyn std::error::Error>> {
    /// // Create an OptimizedTableImpl for user reference data
    /// let user_table = OptimizedTableImpl::new();
    /// let user_datasource = Arc::new(TableDataSource::from_table(user_table));
    ///
    /// // Register it in the processor context
    /// context.load_reference_table("users", user_datasource);
    ///
    /// // Now it's available for subqueries
    /// let table = context.get_table("users")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_reference_table(
        &mut self,
        table_name: &str,
        table: Arc<dyn crate::velostream::table::unified_table::UnifiedTable>,
    ) {
        self.state_tables.insert(table_name.to_string(), table);
    }

    /// Check if a reference table exists in the context
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check
    ///
    /// # Returns
    /// * `true` - Table exists and is available for queries
    /// * `false` - Table not found
    pub fn has_table(&self, table_name: &str) -> bool {
        self.state_tables.contains_key(table_name)
    }

    /// Get all available reference table names
    ///
    /// Returns a list of all table names that have been loaded into the
    /// processor context and are available for subquery operations.
    ///
    /// # Returns
    /// * `Vec<String>` - List of available table names
    pub fn list_tables(&self) -> Vec<String> {
        self.state_tables.keys().cloned().collect()
    }

    /// Remove a reference table from the context
    ///
    /// Unregisters a table, making it unavailable for future subquery operations.
    /// This is useful for cleanup or dynamic table management.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to remove
    ///
    /// # Returns
    /// * `Some(Arc<dyn UnifiedTable>)` - The removed table
    /// * `None` - Table was not found
    pub fn remove_table(&mut self, table_name: &str) -> Option<Arc<dyn UnifiedTable>> {
        self.state_tables.remove(table_name)
    }

    /// Load multiple reference tables from a configuration
    ///
    /// Convenience method for loading multiple tables at once, typically used
    /// during processor initialization to set up all required reference data.
    ///
    /// # Arguments
    /// * `tables` - HashMap mapping table names to SQL-queryable data sources
    pub fn load_reference_tables(&mut self, tables: HashMap<String, Arc<dyn UnifiedTable>>) {
        self.state_tables.extend(tables);
    }

    /// Clear all reference tables from the context
    ///
    /// Removes all loaded tables, typically used during cleanup or context reset.
    pub fn clear_tables(&mut self) {
        self.state_tables.clear();
    }

    // === FR-079 PHASE 4: RESULT QUEUE MANAGEMENT ===

    /// Queue a result for later emission (Phase 4)
    ///
    /// Used by windowed GROUP BY + EMIT CHANGES queries to queue additional group results
    /// for emission in subsequent processing cycles.
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    /// * `result` - The StreamRecord result to queue
    pub fn queue_result(&mut self, query_id: &str, result: StreamRecord) {
        self.pending_results
            .entry(query_id.to_string())
            .or_default()
            .push(result);
    }

    /// Queue multiple results (Phase 4)
    ///
    /// Convenient method to queue multiple results at once.
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    /// * `results` - Vector of StreamRecord results to queue
    pub fn queue_results(&mut self, query_id: &str, results: Vec<StreamRecord>) {
        self.pending_results
            .entry(query_id.to_string())
            .or_default()
            .extend(results);
    }

    /// Dequeue a single result (Phase 4)
    ///
    /// Retrieves the next queued result for a specific query.
    /// Returns None if no results are queued.
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    ///
    /// # Returns
    /// Some(StreamRecord) if a result is available, None otherwise
    pub fn dequeue_result(&mut self, query_id: &str) -> Option<StreamRecord> {
        self.pending_results
            .get_mut(query_id)
            .and_then(|queue| {
                if queue.is_empty() {
                    None
                } else {
                    Some(queue.remove(0))
                }
            })
            .inspect(|result| {
                // Clean up empty queues
                if let Some(queue) = self.pending_results.get(query_id) {
                    if queue.is_empty() {
                        self.pending_results.remove(query_id);
                    }
                }
            })
    }

    /// Check if there are pending results for a query (Phase 4)
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    ///
    /// # Returns
    /// true if there are queued results, false otherwise
    pub fn has_pending_results(&self, query_id: &str) -> bool {
        self.pending_results
            .get(query_id)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false)
    }

    /// Get count of pending results for a query (Phase 4)
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    ///
    /// # Returns
    /// Number of queued results waiting to be emitted
    pub fn pending_result_count(&self, query_id: &str) -> usize {
        self.pending_results
            .get(query_id)
            .map(|queue| queue.len())
            .unwrap_or(0)
    }

    /// Clear all pending results for a query (Phase 4)
    ///
    /// # Arguments
    /// * `query_id` - The query identifier
    pub fn clear_pending_results(&mut self, query_id: &str) {
        self.pending_results.remove(query_id);
    }

    /// Clear all pending results from all queries (Phase 4)
    ///
    /// Used during context cleanup or reset.
    pub fn clear_all_pending_results(&mut self) {
        self.pending_results.clear();
    }

    // === STREAMING CONFIGURATION ===

    /// Set the streaming configuration
    ///
    /// Allows runtime configuration of streaming engine features.
    pub fn set_streaming_config(
        &mut self,
        config: crate::velostream::sql::execution::config::StreamingConfig,
    ) {
        self.streaming_config = Some(config);
    }

    /// Get the streaming configuration (FR-081 Phase 2A)
    ///
    /// Returns the current streaming configuration, or a default config if none is set.
    pub fn get_streaming_config(
        &self,
    ) -> crate::velostream::sql::execution::config::StreamingConfig {
        self.streaming_config.clone().unwrap_or_default()
    }
}
