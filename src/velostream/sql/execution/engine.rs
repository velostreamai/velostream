/*!
# Streaming SQL Execution Engine

This module implements the execution engine for streaming SQL queries. It processes
SQL AST nodes and executes them against streaming data records, supporting real-time
query evaluation with expression processing, filtering, and basic aggregations.

## Public API

The primary interface for executing SQL queries against streaming data:

- [`StreamExecutionEngine`] - Main execution engine
- [`StreamRecord`] - Input record format
- [`FieldValue`] - Value type system

## Usage

```rust,no_run
# use velostream::velostream::sql::execution::{StreamExecutionEngine, StreamRecord};
# use velostream::velostream::serialization::JsonFormat;
# use velostream::velostream::sql::parser::StreamingSqlParser;
# use std::sync::Arc;
# use tokio::sync::mpsc;
# use std::collections::HashMap;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let (output_sender, _receiver) = mpsc::unbounded_channel();
let mut engine = StreamExecutionEngine::new(output_sender);

// Parse a simple query and execute with a record
let parser = StreamingSqlParser::new();
let query = parser.parse("SELECT * FROM stream")?;
let record = StreamRecord::new(HashMap::new());
engine.execute_with_record(&query, &record).await?;
# Ok(())
# }
```

For a complete working example, see the detailed example in the Examples section below.

All other types and methods are internal implementation details.

## Key Features

- **Real-time Processing**: Processes streaming records one at a time as they arrive
- **Expression Evaluation**: Full support for arithmetic, comparison, and logical expressions
- **System Columns**: Access to Kafka metadata (_timestamp, _offset, _partition)
- **Header Functions**: Built-in functions for Kafka message header access
- **Aggregation Support**: Full aggregation functions with GROUP BY support (COUNT, SUM, AVG, MIN, MAX, STDDEV, etc.)
- **Query Lifecycle**: Complete query management from start to stop
- **Error Handling**: Comprehensive error reporting with context information

## Architecture

The execution engine follows a processor-based architecture with clean delegation:

1. **Query Registration**: Queries are registered and maintained in active state
2. **Record Processing**: Incoming records trigger query evaluation via specialized processors
3. **Expression Evaluation**: SQL expressions are evaluated using the ExpressionEvaluator module
4. **Result Generation**: Query results are sent to output channels
5. **State Management**: Query state is managed by dedicated processors and utilities

## Supported Operations

### SELECT Queries
- Field selection (wildcards, specific columns, expressions)
- WHERE clause filtering with complex expressions
- System column access (_timestamp, _offset, _partition)
- LIMIT clause for result set control
- Expression aliases and computed fields

### CREATE STREAM/TABLE
- Stream creation with SELECT query definitions
- Table creation for materialized views
- Property-based configuration

### Built-in Functions
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE
- **Window Functions**: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE
- **Header Functions**: HEADER, HEADER_KEYS, HAS_HEADER, SET_HEADER, REMOVE_HEADER
- **String Functions**: UPPER, LOWER, LENGTH, SUBSTRING, CONCAT
- **Date/Time Functions**: NOW, EXTRACT, DATE_ADD, DATEDIFF

## Examples

```rust,no_run
use velostream::velostream::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::serialization::JsonFormat;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create parser and execution engine
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Execute a simple SELECT query
    let query = parser.parse("SELECT customer_id, amount * 1.1 AS amount_with_tax FROM orders WHERE amount > 100")?;

    // Create test record using FieldValue types
    let mut fields = HashMap::new();
    fields.insert("customer_id".to_string(), FieldValue::String("123".to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(150.0));
    let record = StreamRecord::new(fields);

    engine.execute_with_record(&query, &record).await?;

    // Process results from output channel
    while let Some(_result) = rx.recv().await {
        break; // Just show one result for demo
    }
    Ok(())
}
```

## Performance Characteristics

- **Memory Efficient**: Minimal memory allocation per record
- **Low Latency**: Optimized expression evaluation through specialized processors
- **Streaming Native**: No buffering or batching unless required by windows
- **Type Safe**: Runtime type checking with detailed error messages
*/

use super::config::{MessagePassingMode, StreamingConfig};
use super::internal::{ExecutionMessage, ExecutionState, QueryExecution, WindowState};
// Processor imports for Phase 5B integration
use super::processors::{
    HeaderMutation, HeaderOperation, JoinContext, ProcessorContext, QueryProcessor, WindowContext,
    WindowProcessor,
};
use super::types::{FieldValue, StreamRecord};
// FieldValueConverter no longer needed since we use StreamRecord directly
use crate::velostream::datasource::{DataReader, DataWriter, create_sink, create_source};
use crate::velostream::sql::ast::{Expr, SelectField, StreamSource, StreamingQuery};
use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Type alias for context customizer function
type ContextCustomizer = Arc<dyn Fn(&mut ProcessorContext) + Send + Sync>;

pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::UnboundedSender<ExecutionMessage>,
    message_receiver: Option<mpsc::UnboundedReceiver<ExecutionMessage>>,
    output_sender: mpsc::UnboundedSender<StreamRecord>,
    // FR-082 Phase 5: Optional receiver for draining output in EMIT CHANGES mode
    output_receiver: Option<mpsc::UnboundedReceiver<StreamRecord>>,
    record_count: u64,
    // Performance monitoring
    performance_monitor:
        Option<Arc<crate::velostream::sql::execution::performance::PerformanceMonitor>>,
    // Configuration for enhanced features
    config: StreamingConfig,
    /// Optional context customizer for ProcessorContext initialization.
    ///
    /// **TEST USE ONLY**: This closure-based pattern is for test code that needs
    /// to inject tables into the ProcessorContext. Production code should NOT use this.
    ///
    /// For production code, use `init_query_execution_with_context()`:
    /// ```ignore
    /// let mut context = ProcessorContext::new(&query_id);
    /// context.load_reference_table("my_table", table);
    /// engine.init_query_execution_with_context(query, Arc::new(Mutex::new(context)));
    /// ```
    ///
    /// Production processors (Simple, Transactional, Adaptive) receive tables via
    /// `JobProcessorFactory::create_with_config_and_tables()` and inject them directly.
    #[doc(hidden)]
    pub context_customizer: Option<ContextCustomizer>,
}

// =============================================================================
// MAIN EXECUTION ENGINE IMPLEMENTATION
// =============================================================================

impl StreamExecutionEngine {
    pub fn new(output_sender: mpsc::UnboundedSender<StreamRecord>) -> Self {
        Self::new_with_capacity(output_sender, 100) // Batch-aligned capacity for proper backpressure
    }

    pub fn new_with_capacity(
        output_sender: mpsc::UnboundedSender<StreamRecord>,
        _channel_capacity: usize,
    ) -> Self {
        Self::new_with_config(output_sender, StreamingConfig::default())
    }

    /// Create a new engine with specific configuration
    /// This is the main constructor for enhanced features
    pub fn new_with_config(
        output_sender: mpsc::UnboundedSender<StreamRecord>,
        config: StreamingConfig,
    ) -> Self {
        // Use unbounded channel to prevent deadlocks in current lock-based architecture
        // TODO: Replace with proper message-passing architecture in FR-058
        let (message_sender, receiver) = mpsc::unbounded_channel();
        Self {
            active_queries: HashMap::new(),
            message_sender,
            message_receiver: Some(receiver),
            output_sender,
            output_receiver: None, // FR-082 Phase 5: Set via set_output_receiver() if needed
            record_count: 0,
            performance_monitor: None,
            config,
            context_customizer: None,
        }
    }

    /// Fluent API: Enable watermark support
    pub fn with_watermark_support(mut self) -> Self {
        self.config = self.config.with_watermarks();
        self
    }

    /// Fluent API: Enable enhanced error handling
    pub fn with_enhanced_error_handling(mut self) -> Self {
        self.config = self.config.with_enhanced_errors();
        self
    }

    /// Fluent API: Enable resource limits
    pub fn with_resource_limits(mut self, max_memory: Option<usize>) -> Self {
        self.config = self.config.with_resource_limits(max_memory);
        self
    }

    /// Set performance monitor for tracking query execution metrics
    pub fn set_performance_monitor(
        &mut self,
        monitor: Option<Arc<crate::velostream::sql::execution::performance::PerformanceMonitor>>,
    ) {
        self.performance_monitor = monitor;
    }

    /// Get reference to performance monitor if enabled
    pub fn performance_monitor(
        &self,
    ) -> Option<&Arc<crate::velostream::sql::execution::performance::PerformanceMonitor>> {
        self.performance_monitor.as_ref()
    }

    /// Set streaming configuration (Phase 1B-4 features)
    ///
    /// Configures event-time processing, watermarks, circuit breakers, and observability
    /// from SQL WITH clause properties extracted by StreamJobServer
    pub fn set_streaming_config(&mut self, config: StreamingConfig) {
        self.config = config;
    }

    /// Get reference to current streaming configuration
    pub fn streaming_config(&self) -> &StreamingConfig {
        &self.config
    }

    /// FR-082 Phase 6.5: Get immutable reference to a QueryExecution by ID
    /// Used by batch processor to load state from QueryExecution's ProcessorContext
    pub fn get_query_execution(&self, query_id: &str) -> Option<&QueryExecution> {
        self.active_queries.get(query_id)
    }

    /// FR-082 Phase 6.5: Get mutable reference to a QueryExecution by ID
    /// Used by batch processor to save state back to QueryExecution's ProcessorContext
    pub fn get_query_execution_mut(&mut self, query_id: &str) -> Option<&mut QueryExecution> {
        self.active_queries.get_mut(query_id)
    }

    /// FR-082 Phase 6.5: Initialize a QueryExecution for batch processing
    /// Called by job processors before processing batches to register the query and create the persistent ProcessorContext
    /// Must be called once before any batch processing starts
    ///
    /// IMPORTANT: This method applies `context_customizer` to inject tables into the ProcessorContext,
    /// enabling subquery execution against reference tables (e.g., IN (SELECT ... FROM table)).
    pub fn init_query_execution(&mut self, query: StreamingQuery) {
        // Use the same query_id generation as process_batch_with_output() for consistency
        let query_id = self.generate_query_id(&query);

        // Only create if it doesn't already exist
        // Use create_query_execution() to ensure context_customizer is applied (table injection)
        if !self.active_queries.contains_key(&query_id) {
            let execution = self.create_query_execution(&query_id, query, None);
            self.active_queries.insert(query_id, execution);
        }
    }

    /// FR-082 Option 3: Initialize query execution with an externally-created ProcessorContext.
    ///
    /// This is the preferred method for processors that want to own and manage the ProcessorContext
    /// directly. The processor creates the context, injects tables, and passes it to the engine.
    /// This eliminates the need for `context_customizer` closures.
    ///
    /// # Arguments
    /// * `query` - The streaming query to execute
    /// * `context` - Pre-configured ProcessorContext with tables already injected
    ///
    /// # Example
    /// ```ignore
    /// // Processor creates and configures context
    /// let mut context = ProcessorContext::new(&job_name);
    /// for (name, table) in &tables {
    ///     context.load_reference_table(name, table.clone());
    /// }
    ///
    /// // Pass to engine
    /// engine.init_query_execution_with_context(query, Arc::new(Mutex::new(context)));
    /// ```
    pub fn init_query_execution_with_context(
        &mut self,
        query: StreamingQuery,
        context: Arc<std::sync::Mutex<ProcessorContext>>,
    ) {
        let query_id = self.generate_query_id(&query);

        // Only create if it doesn't already exist
        self.active_queries.entry(query_id).or_insert_with(|| {
            let window_state = match &query {
                StreamingQuery::Select { window, .. } => {
                    window.as_ref().map(|window_spec| WindowState {
                        window_spec: window_spec.clone(),
                        buffer: Vec::new(),
                        last_emit: 0,
                    })
                }
                _ => None,
            };

            QueryExecution {
                query,
                state: ExecutionState::Running,
                window_state,
                processor_context: context,
            }
        });
    }

    /// FR-082 Phase 6.6: Lazy initialize or get existing QueryExecution
    ///
    /// Ensures QueryExecution exists before returning. Creates if needed.
    /// Used by batch processing and other code paths that need guaranteed execution context.
    ///
    /// Returns Arc<Mutex<ProcessorContext>> for ownership transfer to processing loops.
    pub fn ensure_query_execution(
        &mut self,
        query: &StreamingQuery,
    ) -> Option<Arc<std::sync::Mutex<ProcessorContext>>> {
        // Lazy initialize if needed
        if !self
            .active_queries
            .contains_key(&self.generate_query_id(query))
        {
            self.init_query_execution(query.clone());
        }

        // Now retrieve it
        self.get_query_execution(&self.generate_query_id(query))
            .map(|execution| Arc::clone(&execution.processor_context))
    }

    /// FR-082 Phase 6.8: Get existing QueryExecution context without lazy initialization
    ///
    /// **IMPORTANT**: This is a read-only method that assumes QueryExecution already exists.
    /// It MUST be called ONLY after init_query_execution() has been called at startup.
    ///
    /// This eliminates the expensive write lock needed by ensure_query_execution(),
    /// since job processors guarantee initialization before batch processing starts.
    ///
    /// Returns Arc<Mutex<ProcessorContext>> for ownership transfer to processing loops.
    /// Returns None if query not initialized (indicates initialization was skipped).
    pub fn get_query_execution_context(
        &self,
        query: &StreamingQuery,
    ) -> Option<Arc<std::sync::Mutex<ProcessorContext>>> {
        // No lazy initialization - just fetch if it exists
        self.get_query_execution(&self.generate_query_id(query))
            .map(|execution| Arc::clone(&execution.processor_context))
    }

    /// FR-082 Phase 5: Set output receiver for EMIT CHANGES support
    ///
    /// This allows the engine to own the output receiver, enabling batch processing
    /// to drain emitted results directly. Required for EMIT CHANGES queries where
    /// results are sent through the output_sender channel rather than returned directly.
    ///
    /// ## Usage
    ///
    /// ```no_run
    /// use tokio::sync::mpsc;
    /// use velostream::velostream::sql::execution::engine::StreamExecutionEngine;
    ///
    /// let (output_sender, output_receiver) = mpsc::unbounded_channel();
    /// let mut engine = StreamExecutionEngine::new(output_sender);
    /// engine.set_output_receiver(output_receiver);
    /// ```
    pub fn set_output_receiver(&mut self, receiver: mpsc::UnboundedReceiver<StreamRecord>) {
        self.output_receiver = Some(receiver);
    }

    /// FR-082 Phase 5: Try to receive output from the output channel
    ///
    /// Drains all available messages from the output_receiver without blocking.
    /// Returns `Ok(record)` if a message is available, `Err(TryRecvError)` otherwise.
    ///
    /// ## Use Case
    ///
    /// EMIT CHANGES queries emit results through the output_sender channel.
    /// Batch processing uses this method to collect all emitted results after
    /// calling `execute_with_record()`.
    ///
    /// ## Returns
    ///
    /// - `Ok(StreamRecord)` - A record was available and received
    /// - `Err(TryRecvError::Empty)` - No messages available (expected after draining)
    /// - `Err(TryRecvError::Disconnected)` - Channel closed (error condition)
    pub fn try_receive_output(
        &mut self,
    ) -> Result<StreamRecord, tokio::sync::mpsc::error::TryRecvError> {
        if let Some(receiver) = &mut self.output_receiver {
            receiver.try_recv()
        } else {
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        }
    }

    /// FR-082 Phase 5: Take ownership of the output receiver (for batch draining)
    ///
    /// This allows batch processing to temporarily own the receiver, drain all
    /// emitted results, and then return it via `return_output_receiver()`.
    pub fn take_output_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<StreamRecord>> {
        self.output_receiver.take()
    }

    /// FR-082 Phase 5: Return ownership of the output receiver (after batch draining)
    pub fn return_output_receiver(
        &mut self,
        receiver: tokio::sync::mpsc::UnboundedReceiver<StreamRecord>,
    ) {
        self.output_receiver = Some(receiver);
    }

    /// FR-082 Week 8 Optimization 2: Get a clone of the output sender for lock-free batch processing
    ///
    /// This enables batch processing to emit results without holding the engine lock.
    /// The sender is cloned (cheap operation) and used outside the lock for emitting.
    pub fn get_output_sender_for_batch(&self) -> tokio::sync::mpsc::UnboundedSender<StreamRecord> {
        self.output_sender.clone()
    }

    /// Create processor context for new processor-based execution
    /// Create high-performance processor context optimized for threading
    /// Loads only the window states needed for this specific processing call
    /// Get mutable reference to persistent ProcessorContext for a query
    /// FR-082 STP: Returns locked access to the context stored in QueryExecution
    /// This is the single source of truth for state across all batches
    fn get_processor_context(
        &mut self,
        query_id: &str,
    ) -> std::sync::MutexGuard<'_, super::processors::ProcessorContext> {
        self.active_queries
            .get(query_id)
            .expect("QueryExecution must be initialized before processing")
            .processor_context
            .lock()
            .expect("Failed to acquire lock on ProcessorContext")
    }

    fn create_processor_context(&mut self, query_id: &str) -> ProcessorContext {
        let mut context = ProcessorContext::new(query_id);

        // Set engine state
        context.record_count = self.record_count;
        context.window_context = self.get_window_context_for_processors(query_id);
        context.join_context = JoinContext::new();
        context.performance_monitor = self.performance_monitor.as_ref().map(Arc::clone);

        // Pass engine's StreamingConfig to context (enables window_v2 and other optimizations)
        context.streaming_config = Some(self.config.clone());

        // Load window states efficiently (only for queries we're processing)
        context.load_window_states(self.load_window_states_for_context(query_id));

        // Apply any context customization (used by tests)
        if let Some(customizer) = &self.context_customizer {
            customizer(&mut context);
        }

        context
    }

    /// Helper method to create window context for processors
    fn get_window_context_for_processors(&self, query_id: &str) -> Option<WindowContext> {
        // Check if this query has window state in the engine
        if let Some(execution) = self.active_queries.get(query_id) {
            if let Some(window_state) = &execution.window_state {
                // Create WindowContext from engine's window state
                return Some(WindowContext {
                    buffer: window_state.buffer.clone(),
                    last_emit: window_state.last_emit,
                    should_emit: false,
                });
            }
        }

        // If no existing state, create a new window context for windowed queries
        Some(WindowContext {
            buffer: Vec::new(),
            last_emit: 0,
            should_emit: false,
        })
    }

    /// Load window states for a specific context (high-performance, minimal loading)
    /// Only loads states for queries that are actually being processed
    /// OPTIMIZED: Moves state instead of cloning to eliminate O(N²) buffer copies
    fn load_window_states_for_context(&mut self, query_id: &str) -> Vec<(String, WindowState)> {
        let mut states = Vec::with_capacity(1); // Usually just one state per context

        // MOVE the window state from engine to context (zero-copy)
        // This eliminates the O(N²) cloning bottleneck
        if let Some(execution) = self.active_queries.get_mut(query_id) {
            if let Some(window_state) = execution.window_state.take() {
                states.push((query_id.to_string(), window_state));
            }
        }

        states
    }

    /// Process query using the modern processor architecture
    fn apply_query(
        &mut self,
        query: &StreamingQuery,
        record: &StreamRecord,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // All queries now use the processor architecture
        self.apply_query_with_processors(query, record)
    }

    /// Step 3.1: Real processor-based query execution implementation
    fn apply_query_with_processors(
        &mut self,
        query: &StreamingQuery,
        record: &StreamRecord,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Generate a query ID based on the query type and content
        let query_id = self.generate_query_id(query);

        // Lazy initialize QueryExecution if not present
        // This allows tests to call apply_query directly without explicit init_query_execution
        if !self.active_queries.contains_key(&query_id) {
            let execution = self.create_query_execution(&query_id, query.clone(), None);
            self.active_queries.insert(query_id.clone(), execution);
        }

        // Scope the context acquisition - guard drops automatically at end of block
        let result = {
            let mut context = self.get_processor_context(&query_id);

            // Set LIMIT in context if present
            if let StreamingQuery::Select { limit, .. } = query {
                context.max_records = *limit;
            }

            QueryProcessor::process_query(query, record, &mut context)?
            // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
            // Guard drops here automatically
        };

        // NOTE: GROUP BY results emission moved to explicit triggers
        // Emitting after every record was causing performance issues and incorrect results
        // In a complete streaming implementation, GROUP BY results should be emitted based on:
        // - Time windows (every N seconds)
        // - Count windows (every N records)
        // - Memory pressure
        // - Explicit flush commands
        // For now, results accumulate in group_states and can be retrieved via explicit calls

        // Update engine state from context
        if result.should_count && result.record.is_some() {
            self.record_count += 1;
        }

        // Apply header mutations to the output record
        let mut final_record = result.record;
        if let Some(ref mut record) = final_record {
            self.apply_header_mutations_to_record(record, &result.header_mutations)?;
        }

        Ok(final_record)
    }

    /// Generate a consistent query ID from a StreamingQuery
    /// This ID is used internally to track query execution state and is required for partition receiver setup
    pub fn generate_query_id(&self, query: &StreamingQuery) -> String {
        match query {
            StreamingQuery::Select { from, window, .. } => {
                let base = format!(
                    "select_{}",
                    match from {
                        StreamSource::Stream(name) | StreamSource::Table(name) => name,
                        StreamSource::Uri(uri) => uri,
                        StreamSource::Subquery(_) => "subquery",
                    }
                );
                if window.is_some() {
                    format!("{}_windowed", base)
                } else {
                    base
                }
            }
            StreamingQuery::CreateStream { name, .. } => format!("create_stream_{}", name),
            StreamingQuery::CreateTable { name, .. } => format!("create_table_{}", name),
            StreamingQuery::Show { .. } => "show_query".to_string(),
            _ => "unknown_query".to_string(),
        }
    }

    /// Apply header mutations directly to a StreamRecord
    /// This modifies the record's headers HashMap based on SET/REMOVE operations
    fn apply_header_mutations_to_record(
        &self,
        record: &mut StreamRecord,
        mutations: &[HeaderMutation],
    ) -> Result<(), SqlError> {
        for mutation in mutations {
            match &mutation.operation {
                HeaderOperation::Set => {
                    // SET_HEADER: Add or update header value
                    if let Some(value) = &mutation.value {
                        record.headers.insert(mutation.key.clone(), value.clone());
                        log::debug!("Applied header mutation: SET {} = {}", mutation.key, value);
                    }
                }
                HeaderOperation::Remove => {
                    // REMOVE_HEADER: Remove header from record
                    if record.headers.remove(&mutation.key).is_some() {
                        log::debug!("Applied header mutation: REMOVE {}", mutation.key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Executes a SQL query with a StreamRecord directly.
    /// This is the primary execution method - all other execution should use this.
    ///
    /// # Arguments
    ///
    /// * `query` - The parsed SQL query to execute
    /// * `record` - Reference to the complete StreamRecord with fields, metadata, and headers
    ///
    /// ## Phase 6.3b Optimization
    ///
    /// Accepts &StreamRecord instead of owned StreamRecord to eliminate cloning.
    /// For windowed queries, only clones internally when _timestamp adjustment is needed.
    /// For non-windowed queries (common case), uses reference directly without cloning.
    pub async fn execute_with_record(
        &mut self,
        query: &StreamingQuery,
        stream_record: &StreamRecord,
    ) -> Result<(), SqlError> {
        let record_to_process = self.prepare_record_for_execution(query, stream_record);
        self.execute_internal(query, record_to_process).await
    }

    /// Synchronous record execution (Phase 6.7 STP Determinism)
    ///
    /// Processes a single record synchronously and returns all output records directly without
    /// async/await overhead or channel buffering. This enables true Single-Threaded
    /// Pipeline (STP) semantics where the processing loop can immediately decide
    /// commit/fail/rollback without waiting for channel messages.
    ///
    /// # Performance Impact
    ///
    /// Removes 12-18% overhead from async architecture:
    /// - State machine generation: 2-3%
    /// - Context switching: 5-7%
    /// - Channel buffering: 3-5%
    /// - Waker/polling: 2-3%
    ///
    /// Expected improvement: 15% throughput gain (693K → 800K+ rec/sec)
    ///
    /// # Arguments
    ///
    /// * `query` - The parsed SQL query to execute
    /// * `stream_record` - Reference to the complete StreamRecord
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<StreamRecord>)` - All output records (0 or more):
    ///   - Empty vec: Record filtered out or buffered in window
    ///   - Single record: Non-windowed query or first window result
    ///   - Multiple records: GROUP BY emitting multiple groups, or windowed aggregations
    /// * `Err(SqlError)` - If an error occurred during processing
    ///
    /// # Determinism Guarantee
    ///
    /// The caller ALWAYS knows immediately after this method returns whether:
    /// - Processing succeeded with 0+ results (empty vec is valid for buffered/filtered)
    /// - Processing failed with error
    ///
    /// This enables deterministic commit/fail/rollback decisions per record.
    ///
    /// # Multiple Results
    ///
    /// This method returns ALL results per record, supporting:
    /// - GROUP BY queries that emit multiple groups per window
    /// - EMIT CHANGES queries that emit on each state change
    /// - Windowed aggregations with multiple results
    pub fn execute_with_record_sync(
        &mut self,
        query: &StreamingQuery,
        stream_record: &StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let record_to_process = self.prepare_record_for_execution(query, stream_record);
        self.execute_internal_sync(query, record_to_process)
    }

    /// Synchronous internal execute method (Phase 6.7)
    ///
    /// Processes the record synchronously and returns ALL output records.
    /// Includes first result plus any pending results from GROUP BY queue.
    ///
    /// Returns a vector of 0 or more results for the record.
    fn execute_internal_sync(
        &mut self,
        query: &StreamingQuery,
        stream_record: StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Check if this is a windowed query and process accordingly
        let (result, pending_results) = if let StreamingQuery::Select {
            window: Some(window_spec),
            group_by,
            ..
        } = query
        {
            // OPTIMIZATION: Reuse generate_query_id instead of format!("{:?}", query)
            // format! with Debug is expensive when called per-record (CRITICAL HOTPATH)
            let query_id = self.generate_query_id(query);
            if !self.active_queries.contains_key(&query_id) {
                let window_state = Some(WindowState {
                    window_spec: window_spec.clone(),
                    buffer: Vec::new(),
                    last_emit: 0,
                });

                let execution = self.create_query_execution(&query_id, query.clone(), window_state);
                self.active_queries.insert(query_id.clone(), execution);
            }

            // Process using windowed logic with high-performance state management
            let (result, pending) = {
                let mut context = self.get_processor_context(&query_id);
                let result = WindowProcessor::process_windowed_query_enhanced(
                    &query_id,
                    query,
                    &stream_record,
                    &mut context,
                    None, // source_id
                )?;

                // Collect pending results from GROUP BY queue (like async version does)
                let pending_results =
                    Self::collect_pending_results(&mut context, &query_id, group_by.is_some());

                (result, pending_results)
            };
            (result, pending)
        } else {
            // Regular non-windowed processing
            (self.apply_query(query, &stream_record)?, Vec::new())
        };

        // Collect all results into a vector
        let mut all_results = Vec::new();
        if let Some(first_result) = result {
            all_results.push(first_result);
        }
        all_results.extend(pending_results);

        Ok(all_results)
    }

    /// Internal execute method that does the actual query processing
    async fn execute_internal(
        &mut self,
        query: &StreamingQuery,
        stream_record: StreamRecord,
    ) -> Result<(), SqlError> {
        // Check if this is a windowed query and process accordingly
        let result = if let StreamingQuery::Select {
            window: Some(window_spec),
            group_by,
            ..
        } = query
        {
            // For windowed queries, we need to simulate the streaming execution model

            // FR-082 STP FIX: Use consistent query_id matching init_query_execution
            // This ensures execute_internal finds the QueryExecution with persistent ProcessorContext
            // that was initialized by init_query_execution (or reuses existing one)
            // OPTIMIZATION: Use generate_query_id instead of format!("{:?}", query) to avoid expensive Debug formatting
            let query_id = self.generate_query_id(query);
            if !self.active_queries.contains_key(&query_id) {
                let window_state = Some(WindowState {
                    window_spec: window_spec.clone(),
                    buffer: Vec::new(),
                    last_emit: 0,
                });

                let execution = self.create_query_execution(&query_id, query.clone(), window_state);
                self.active_queries.insert(query_id.clone(), execution);
            }

            // Process using windowed logic with high-performance state management
            // Scope context to ensure guard is dropped before returning
            let (result, pending_results) = {
                let mut context = self.get_processor_context(&query_id);
                let result = WindowProcessor::process_windowed_query_enhanced(
                    &query_id,
                    query,
                    &stream_record,
                    &mut context,
                    None, // source_id
                )?;

                // FR-079 Phase 6: Emit pending results from queue
                // After processing the current record, check if there are additional results queued
                let pending_results =
                    Self::collect_pending_results(&mut context, &query_id, group_by.is_some());
                if !pending_results.is_empty() {
                    log::debug!(
                        "FR-079 Phase 6: Dequeued {} pending results for emission",
                        pending_results.len()
                    );
                }

                // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
                // Guard drops here automatically
                (result, pending_results)
            };

            (result, pending_results)
        } else {
            // Regular non-windowed processing
            (self.apply_query(query, &stream_record)?, Vec::new())
        };

        // Unpack result and pending results for remaining processing
        let (result, pending_results) = result;

        // Process result if any
        if let Some(result) = result {
            // Send result through both channels - no conversion needed!
            // Generate correlation ID for async error tracking
            let correlation_id = ExecutionMessage::generate_correlation_id();
            self.message_sender
                .send(ExecutionMessage::QueryResult {
                    query_id: "default".to_string(),
                    result: result.clone(),
                    correlation_id,
                })
                .map_err(|_| SqlError::ExecutionError {
                    message: "Failed to send result".to_string(),
                    query: None,
                })?;

            // Send result to output channel directly (no conversion needed)
            self.output_sender
                .send(result)
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Failed to send result to output channel: {}", e),
                    query: None,
                })?;
        }

        // FR-079 Phase 6: Emit all pending results queued from GROUP BY emission
        for pending_result in pending_results {
            log::debug!("FR-079 Phase 6: Emitting queued result for windowed GROUP BY");
            let correlation_id = ExecutionMessage::generate_correlation_id();
            self.message_sender
                .send(ExecutionMessage::QueryResult {
                    query_id: "default".to_string(),
                    result: pending_result.clone(),
                    correlation_id,
                })
                .map_err(|_| SqlError::ExecutionError {
                    message: "Failed to send pending result".to_string(),
                    query: None,
                })?;

            // Send pending result to output channel
            self.output_sender
                .send(pending_result)
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Failed to send pending result to output channel: {}", e),
                    query: None,
                })?;
        }

        Ok(())
    }

    /// Starts the execution engine's message processing loop.
    ///
    /// This method must be called to begin processing query execution messages.
    ///
    /// # Enhanced Channel Draining
    /// Fixed hanging test issue by properly draining channels and adding timeout handling
    pub async fn start(&mut self) -> Result<(), SqlError> {
        let mut receiver =
            self.message_receiver
                .take()
                .ok_or_else(|| SqlError::ExecutionError {
                    message: "Engine already started".to_string(),
                    query: None,
                })?;

        // Enhanced message processing loop with proper channel draining
        let mut shutdown_requested = false;

        while !shutdown_requested {
            // Use timeout to prevent indefinite blocking
            match tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv()).await {
                Ok(Some(message)) => {
                    match self.handle_execution_message(message).await {
                        Ok(should_continue) => {
                            if !should_continue {
                                shutdown_requested = true;
                            }
                        }
                        Err(e) => {
                            log::error!("Error handling execution message: {}", e);
                            // Continue processing other messages instead of failing completely
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed - normal shutdown
                    log::info!("Execution engine message channel closed - shutting down");
                    shutdown_requested = true;
                }
                Err(_) => {
                    // Timeout - check for pending messages and continue
                    // This prevents hanging when no messages are being sent

                    // Drain any remaining messages non-blockingly
                    while let Ok(message) = receiver.try_recv() {
                        match self.handle_execution_message(message).await {
                            Ok(should_continue) => {
                                if !should_continue {
                                    shutdown_requested = true;
                                    break;
                                }
                            }
                            Err(e) => {
                                log::warn!("Error handling drained message: {}", e);
                            }
                        }
                    }

                    // Continue the loop - timeout is normal behavior
                }
            }
        }

        // Final cleanup - drain any remaining messages
        log::info!("Execution engine shutting down - draining remaining messages");
        while let Ok(message) = receiver.try_recv() {
            if let Err(e) = self.handle_execution_message(message).await {
                log::warn!("Error handling message during shutdown: {}", e);
            }
        }

        Ok(())
    }

    /// Handle a single execution message
    /// Returns Ok(true) to continue processing, Ok(false) to shutdown gracefully
    async fn handle_execution_message(
        &mut self,
        message: ExecutionMessage,
    ) -> Result<bool, SqlError> {
        // Check if message requires enhanced features and if they're enabled
        if message.requires_enhanced_features() && !self.has_enhanced_features_enabled() {
            log::warn!(
                "Received enhanced message type but enhanced features are disabled: {:?}",
                message
            );
            return Ok(true); // Continue processing, but ignore the message
        }

        match message {
            ExecutionMessage::StartJob {
                job_id,
                query,
                correlation_id: _,
            } => {
                self.start_query_execution(job_id, query).await?;
                Ok(true)
            }
            ExecutionMessage::StopJob {
                job_id,
                correlation_id: _,
            } => {
                self.stop_query_execution(&job_id).await?;
                Ok(true)
            }
            ExecutionMessage::ProcessRecord {
                stream_name,
                record,
                correlation_id,
            } => {
                // Log correlation ID for async error tracking
                log::trace!(
                    "Processing record for stream '{}' with correlation_id: {}",
                    stream_name,
                    correlation_id
                );
                self.process_stream_record(&stream_name, record).await?;
                Ok(true)
            }
            ExecutionMessage::QueryResult {
                query_id: _,
                result: _,
                correlation_id,
            } => {
                // Handle query results - log correlation for tracking
                log::trace!(
                    "Query result delivered with correlation_id: {}",
                    correlation_id
                );
                Ok(true)
            }

            // Enhanced message types (Phase 1B+) - Only processed if enhanced features are enabled
            ExecutionMessage::AdvanceWatermark {
                watermark_timestamp,
                source_id,
                correlation_id,
            } => {
                log::debug!(
                    "Advancing watermark for source '{}' to {} (correlation_id: {})",
                    source_id,
                    watermark_timestamp,
                    correlation_id
                );
                // TODO: Implement watermark advancement in Phase 1B
                Ok(true)
            }
            ExecutionMessage::TriggerWindow {
                window_id,
                trigger_reason,
                correlation_id,
            } => {
                log::debug!(
                    "Triggering window '{}' due to {:?} (correlation_id: {})",
                    window_id,
                    trigger_reason,
                    correlation_id
                );
                // TODO: Implement window triggering in Phase 1B
                Ok(true)
            }
            ExecutionMessage::CleanupExpiredState {
                retention_duration_ms,
                correlation_id,
            } => {
                log::debug!(
                    "Cleaning up state older than {}ms (correlation_id: {})",
                    retention_duration_ms,
                    correlation_id
                );
                // TODO: Implement state cleanup in Phase 3
                Ok(true)
            }
            ExecutionMessage::ErrorRecovery {
                original_correlation_id,
                error_type,
                retry_attempt,
                correlation_id,
            } => {
                log::info!(
                    "Error recovery attempt {} for error '{}' (original: {}, current: {})",
                    retry_attempt,
                    error_type,
                    original_correlation_id,
                    correlation_id
                );
                // TODO: Implement error recovery in Phase 2
                Ok(true)
            }
            ExecutionMessage::CircuitBreakerStateChange {
                component_id,
                old_state,
                new_state,
                correlation_id,
            } => {
                log::warn!(
                    "Circuit breaker for component '{}' changed from {} to {} (correlation_id: {})",
                    component_id,
                    old_state,
                    new_state,
                    correlation_id
                );
                // TODO: Implement circuit breaker handling in Phase 2
                Ok(true)
            }
            ExecutionMessage::ResourceLimitExceeded {
                resource_type,
                current_usage,
                limit,
                correlation_id,
            } => {
                log::error!(
                    "Resource limit exceeded: {} usage {}/{} (correlation_id: {})",
                    resource_type,
                    current_usage,
                    limit,
                    correlation_id
                );
                // TODO: Implement resource limit handling in Phase 3
                Ok(true)
            }
        }
    }

    /// Check if enhanced features are enabled
    fn has_enhanced_features_enabled(&self) -> bool {
        self.config.enable_watermarks
            || self.config.enable_enhanced_errors
            || self.config.enable_resource_limits
            || self.config.message_passing_mode != MessagePassingMode::Legacy
    }

    /// Get a clone of the message sender for external message injection
    /// This allows other components to send messages to the execution engine
    pub fn get_message_sender(&self) -> mpsc::UnboundedSender<ExecutionMessage> {
        self.message_sender.clone()
    }

    pub async fn start_query_execution(
        &mut self,
        query_id: String,
        query: StreamingQuery,
    ) -> Result<(), SqlError> {
        let window_state = match &query {
            StreamingQuery::Select { window, .. } => {
                window.as_ref().map(|window_spec| WindowState {
                    window_spec: window_spec.clone(),
                    buffer: Vec::new(),
                    last_emit: 0,
                })
            }
            _ => None,
        };

        let execution = self.create_query_execution(&query_id, query, window_state);
        self.active_queries.insert(query_id.clone(), execution);
        Ok(())
    }

    async fn stop_query_execution(&mut self, query_id: &str) -> Result<(), SqlError> {
        if let Some(mut execution) = self.active_queries.remove(query_id) {
            execution.state = ExecutionState::Stopped;
        }
        Ok(())
    }

    pub async fn process_stream_record(
        &mut self,
        stream_name: &str,
        record: StreamRecord,
    ) -> Result<(), SqlError> {
        // Collect matching queries first
        let matching_queries: Vec<(String, StreamingQuery)> = self
            .active_queries
            .iter()
            .filter_map(|(query_id, execution)| {
                if self.query_matches_stream(&execution.query, stream_name) {
                    match &execution.state {
                        ExecutionState::Running => {
                            Some((query_id.clone(), execution.query.clone()))
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();

        // Process each query - use windowed processing if the query has a window
        let mut results = Vec::new();
        for (query_id, query) in matching_queries {
            let result = if let StreamingQuery::Select {
                window: Some(_), ..
            } = &query
            {
                // Use windowed processing for queries with window specifications
                {
                    let mut context = self.get_processor_context(&query_id);
                    WindowProcessor::process_windowed_query_enhanced(
                        &query_id,
                        &query,
                        &record,
                        &mut context,
                        None, // source_id
                    )?
                    // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
                    // Guard drops here automatically
                }
            } else {
                // Use regular processing for non-windowed queries
                self.apply_query(&query, &record)?
            };

            if let Some(result_record) = result {
                results.push((query_id, result_record));
            }
        }

        for (_query_id, result) in results {
            // Send result directly to output channel - no conversion needed!
            let _ = self.output_sender.send(result);
        }
        Ok(())
    }

    /// Flush any pending window results by processing a final trigger record
    /// Forces emission of any buffered window results for all active queries.
    pub async fn flush_windows(&mut self) -> Result<(), SqlError> {
        // Create a trigger record with a very high timestamp to force window emission
        let mut trigger_fields = HashMap::new();
        // FR-081 Phase 2A+: window_v2 requires "timestamp" field for timestamp extraction
        trigger_fields.insert("timestamp".to_string(), FieldValue::Integer(i64::MAX));

        let trigger_record = StreamRecord {
            fields: trigger_fields,
            timestamp: i64::MAX, // Far future timestamp
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
            event_time: None,
            topic: None,
            key: None,
        };

        // Process the trigger for all active queries to flush any pending windows
        let active_query_ids: Vec<String> = self.active_queries.keys().cloned().collect();
        for query_id in active_query_ids {
            if let Some(execution) = self.active_queries.get(&query_id) {
                let query = execution.query.clone();
                if let StreamingQuery::Select {
                    window: Some(_),
                    group_by,
                    ..
                } = &query
                {
                    // Only flush windowed queries
                    // Scope context acquisition to ensure guard is dropped before using self
                    let (result, pending_results) = {
                        let mut context = self.get_processor_context(&query_id);
                        let result = WindowProcessor::process_windowed_query_enhanced(
                            &query_id,
                            &query,
                            &trigger_record,
                            &mut context,
                            None, // source_id
                        )?;

                        // FR-081 Phase 6: Emit all pending GROUP BY results from queue
                        // When GROUP BY + WINDOW produces multiple groups, additional results are queued
                        let pending_results = Self::collect_pending_results(
                            &mut context,
                            &query_id,
                            group_by.is_some(),
                        );

                        // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
                        // Guard drops here automatically when block ends
                        (result, pending_results)
                    };

                    // Send the first result (if any)
                    if let Some(result_record) = result {
                        // Send the flushed result directly - no conversion needed!
                        let _ = self.output_sender.send(result_record);
                    }

                    // Send pending results
                    for pending_result in pending_results {
                        let _ = self.output_sender.send(pending_result);
                    }
                }
            }
        }
        Ok(())
    }

    fn query_matches_stream(&self, query: &StreamingQuery, stream_name: &str) -> bool {
        match query {
            StreamingQuery::Select { from, .. } => match from {
                StreamSource::Stream(name) | StreamSource::Table(name) => name == stream_name,
                StreamSource::Uri(uri) => uri == stream_name,
                StreamSource::Subquery(_) => false,
            },
            StreamingQuery::CreateStream { as_select, .. } => {
                self.query_matches_stream(as_select, stream_name)
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                self.query_matches_stream(as_select, stream_name)
            }
            StreamingQuery::Show { .. } => false, // SHOW commands don't match streams
            StreamingQuery::StartJob { query, .. } => {
                // START JOB matches if the underlying query matches
                self.query_matches_stream(query, stream_name)
            }
            StreamingQuery::StopJob { .. } => false, // STOP commands don't match streams
            StreamingQuery::PauseJob { .. } => false, // PAUSE commands don't match streams
            StreamingQuery::ResumeJob { .. } => false, // RESUME commands don't match streams
            StreamingQuery::DeployJob { query, .. } => {
                // DEPLOY JOB matches if the underlying query matches
                self.query_matches_stream(query, stream_name)
            }
            StreamingQuery::RollbackJob { .. } => false, // ROLLBACK commands don't match streams
            StreamingQuery::InsertInto { table_name, .. } => {
                // INSERT matches the target table
                table_name == stream_name
            }
            StreamingQuery::Update { table_name, .. } => {
                // UPDATE matches the target table
                table_name == stream_name
            }
            StreamingQuery::Delete { table_name, .. } => {
                // DELETE matches the target table
                table_name == stream_name
            }
            StreamingQuery::Union { left, right, .. } => {
                // UNION matches if either side matches the stream
                self.query_matches_stream(left, stream_name)
                    || self.query_matches_stream(right, stream_name)
            }
        }
    }

    /// Manually flush all accumulated GROUP BY results for a specific query
    pub fn flush_group_by_results(&mut self, query: &StreamingQuery) -> Result<(), SqlError> {
        if let StreamingQuery::Select {
            group_by: Some(_),
            fields,
            having,
            ..
        } = query
        {
            self.emit_group_by_results(fields, having)
        } else {
            Ok(())
        }
    }

    /// Emit accumulated GROUP BY results to the output channel
    /// NOTE: This method is not currently used since state management was moved to partition-level
    /// in Phase 6.4C/6.5. Kept as a stub for backward compatibility if needed in future.
    #[allow(dead_code)]
    fn emit_group_by_results(
        &mut self,
        _fields: &[SelectField],
        _having: &Option<Expr>,
    ) -> Result<(), SqlError> {
        // This method is a stub that is no longer called in the current implementation.
        // GROUP BY states are still maintained in self.group_states (see lines 174, 454, 459)
        // and are used by execute_with_record() for core GROUP BY functionality.
        // State management has been optimized in Phase 6.4C/6.5 to use partition-level management
        // via PartitionStateManager for batch processing. This stub is kept for API compatibility.
        Ok(())
    }

    // === PLUGGABLE DATA SOURCE SUPPORT ===

    /// Execute a query with pluggable data sources
    /// Reads from one or more sources and writes to one or more sinks
    pub async fn execute_with_sources(
        &mut self,
        query: &StreamingQuery,
        source_uris: Vec<&str>,
        sink_uris: Vec<&str>,
    ) -> Result<(), SqlError> {
        // Create readers from source URIs
        let mut readers = HashMap::new();
        for (idx, uri) in source_uris.iter().enumerate() {
            let source = create_source(uri).map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to create source from {}: {}", uri, e),
                query: None,
            })?;

            let reader = source
                .create_reader()
                .await
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Failed to create reader for {}: {}", uri, e),
                    query: None,
                })?;

            readers.insert(format!("source_{}", idx), reader);
        }

        // Create writers from sink URIs
        let mut writers = HashMap::new();
        for (idx, uri) in sink_uris.iter().enumerate() {
            let sink = create_sink(uri).map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to create sink from {}: {}", uri, e),
                query: None,
            })?;

            let writer = sink
                .create_writer()
                .await
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Failed to create writer for {}: {}", uri, e),
                    query: None,
                })?;

            writers.insert(format!("sink_{}", idx), writer);
        }

        // Create context with heterogeneous sources
        let query_id = self.generate_query_id(query);
        let mut context = ProcessorContext::new_with_sources(&query_id, readers, writers);

        // Copy engine state to context
        context.record_count = self.record_count;
        context.performance_monitor = self.performance_monitor.as_ref().map(Arc::clone);
        context.streaming_config = Some(self.config.clone());

        // Process records from all sources
        let source_names: Vec<String> = context.list_sources();
        for source_name in &source_names {
            context.set_active_reader(source_name)?;

            // Process all records from this source
            loop {
                let batch = context.read().await?;
                if batch.is_empty() {
                    break;
                }

                for record in batch {
                    // Apply query processing
                    let result = QueryProcessor::process_query(query, &record, &mut context)?;

                    // Write result to all sinks if present
                    if let Some(output_record) = result.record {
                        for sink_idx in 0..sink_uris.len() {
                            let sink_name = format!("sink_{}", sink_idx);
                            context.write_to(&sink_name, output_record.clone()).await?;
                        }
                    }

                    // Update record count
                    if result.should_count {
                        self.record_count += 1;
                    }
                }
            }

            // Commit this source
            context.commit_source(source_name).await?;
        }

        // Flush and commit all sinks
        context.flush_all().await?;
        for sink_idx in 0..sink_uris.len() {
            let sink_name = format!("sink_{}", sink_idx);
            context.commit_sink(&sink_name).await?;
        }

        // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
        // Guard drops here automatically when context goes out of scope

        Ok(())
    }

    /// Execute a query reading from a single data source URI
    pub async fn execute_from_source(
        &mut self,
        query: &StreamingQuery,
        source_uri: &str,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let source = create_source(source_uri).map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to create source from {}: {}", source_uri, e),
            query: None,
        })?;

        let mut reader = source
            .create_reader()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to create reader: {}", e),
                query: None,
            })?;

        let mut results = Vec::new();
        let query_id = self.generate_query_id(query);

        // Process all records from source
        loop {
            let batch = reader.read().await.map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to read from source: {}", e),
                query: None,
            })?;

            if batch.is_empty() {
                break;
            }

            for record in batch {
                // Scope context to ensure guard is dropped before using self
                let (result_record, should_count) = {
                    let mut context = self.get_processor_context(&query_id);
                    let result = QueryProcessor::process_query(query, &record, &mut context)?;

                    // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
                    // Guard drops here automatically when block ends
                    (result.record, result.should_count)
                };

                if let Some(output_record) = result_record {
                    results.push(output_record);
                }

                if should_count {
                    self.record_count += 1;
                }
            }
        }

        reader
            .commit()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to commit reader: {}", e),
                query: None,
            })?;

        Ok(results)
    }

    /// Process a streaming query with custom data source and sink
    pub async fn stream_process(
        &mut self,
        query: &StreamingQuery,
        mut reader: Box<dyn DataReader>,
        mut writer: Box<dyn DataWriter>,
    ) -> Result<(), SqlError> {
        let query_id = self.generate_query_id(query);

        // Stream processing loop
        loop {
            match reader.read().await {
                Ok(batch) => {
                    if batch.is_empty() {
                        // No more data available
                        break;
                    }

                    for record in batch {
                        // Scope context to ensure guard is dropped before using self
                        let (result_record, should_count) = {
                            let mut context = self.get_processor_context(&query_id);
                            let result =
                                QueryProcessor::process_query(query, &record, &mut context)?;

                            // ProcessorContext state is persistent (via Arc<Mutex>) - no explicit save needed
                            // Guard drops here automatically when block ends
                            (result.record, result.should_count)
                        };

                        if let Some(output_record) = result_record {
                            writer.write(output_record).await.map_err(|e| {
                                SqlError::ExecutionError {
                                    message: format!("Failed to write output: {}", e),
                                    query: None,
                                }
                            })?;
                        }

                        if should_count {
                            self.record_count += 1;
                        }
                    }
                }
                Err(e) => {
                    return Err(SqlError::ExecutionError {
                        message: format!("Read error: {}", e),
                        query: None,
                    });
                }
            }
        }

        // Commit reader and writer
        reader
            .commit()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to commit reader: {}", e),
                query: None,
            })?;

        writer.flush().await.map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to flush writer: {}", e),
            query: None,
        })?;

        writer
            .commit()
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to commit writer: {}", e),
                query: None,
            })?;

        Ok(())
    }

    // =============================================================================
    // PHASE 1 REFACTORING: HELPER METHODS TO REDUCE DUPLICATION
    // =============================================================================

    /// Prepares a record for execution by adjusting timestamps for windowed queries.
    ///
    /// For windowed queries with a `_timestamp` field, extracts and converts it to i64.
    /// For non-windowed queries or records without `_timestamp`, returns a clone.
    ///
    /// # Arguments
    /// * `query` - The SQL query being executed
    /// * `stream_record` - The incoming record with potential timestamp field
    ///
    /// # Returns
    /// A StreamRecord with adjusted timestamp if needed
    fn prepare_record_for_execution(
        &self,
        query: &StreamingQuery,
        stream_record: &StreamRecord,
    ) -> StreamRecord {
        if let StreamingQuery::Select {
            window: Some(_), ..
        } = query
        {
            if let Some(ts_field) = stream_record.fields.get("_timestamp") {
                let mut modified_record = stream_record.clone();
                match ts_field {
                    FieldValue::Integer(ts) => {
                        modified_record.timestamp = *ts;
                    }
                    FieldValue::Float(ts) => {
                        modified_record.timestamp = *ts as i64;
                    }
                    _ => {
                        // Keep existing timestamp if _timestamp field isn't a valid time
                    }
                }
                return modified_record;
            }
        }
        stream_record.clone()
    }

    /// Collects all pending results from the processor context.
    ///
    /// For queries with GROUP BY, drains the pending results queue and collects them.
    /// For non-GROUP BY queries, returns an empty vector.
    ///
    /// # Arguments
    /// * `context` - The processor context to drain results from
    /// * `query_id` - The query ID to retrieve results for
    /// * `has_group_by` - Whether the query has a GROUP BY clause
    ///
    /// # Returns
    /// Vector of all pending StreamRecords
    fn collect_pending_results(
        context: &mut ProcessorContext,
        query_id: &str,
        has_group_by: bool,
    ) -> Vec<StreamRecord> {
        let mut pending_results = Vec::new();
        if has_group_by {
            while context.has_pending_results(query_id) {
                if let Some(pending_result) = context.dequeue_result(query_id) {
                    pending_results.push(pending_result);
                } else {
                    break;
                }
            }
        }
        pending_results
    }

    /// Creates a new QueryExecution with optional window state.
    ///
    /// Initializes a QueryExecution with the given query and window state, applying
    /// any configured context customization for testing.
    ///
    /// # Arguments
    /// * `query_id` - Unique identifier for the query
    /// * `query` - The parsed SQL query
    /// * `window_state` - Optional window state if this is a windowed query
    ///
    /// # Returns
    /// A new QueryExecution ready for processing
    fn create_query_execution(
        &self,
        query_id: &str,
        query: StreamingQuery,
        window_state: Option<WindowState>,
    ) -> QueryExecution {
        let mut context = ProcessorContext::new(query_id);

        // Apply any context customization (mainly for tests)
        if let Some(customizer) = &self.context_customizer {
            customizer(&mut context);
        }

        QueryExecution {
            query,
            state: ExecutionState::Running,
            window_state,
            processor_context: Arc::new(std::sync::Mutex::new(context)),
        }
    }
}
