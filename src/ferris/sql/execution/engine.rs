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
# use ferrisstreams::ferris::sql::execution::{StreamExecutionEngine, StreamRecord};
# use ferrisstreams::ferris::serialization::JsonFormat;
# use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
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
engine.execute_with_record(&query, record).await?;
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
use ferrisstreams::ferris::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::serialization::JsonFormat;
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

    engine.execute_with_record(&query, record).await?;

    // Process results from output channel
    while let Some(result) = rx.recv().await {
        println!("Query result: {:?}", result);
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

use super::aggregation::AggregateFunctions;
use super::config::{MessagePassingMode, StreamingConfig};
use super::expression::ExpressionEvaluator;
use super::internal::{
    ExecutionMessage, ExecutionState, GroupByState, QueryExecution, WindowState,
};
use super::types::{FieldValue, StreamRecord};
// FieldValueConverter no longer needed since we use StreamRecord directly
use crate::ferris::datasource::{create_sink, create_source, DataReader, DataWriter};
use crate::ferris::sql::ast::{Expr, SelectField, StreamSource, StreamingQuery};
use crate::ferris::sql::error::SqlError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
// Processor imports for Phase 5B integration
use super::processors::{
    HeaderMutation as ProcessorHeaderMutation, HeaderOperation as ProcessorHeaderOperation,
    JoinContext, ProcessorContext, QueryProcessor, SelectProcessor, WindowContext, WindowProcessor,
};

pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::UnboundedSender<ExecutionMessage>,
    message_receiver: Option<mpsc::UnboundedReceiver<ExecutionMessage>>,
    output_sender: mpsc::UnboundedSender<StreamRecord>,
    record_count: u64,
    // Stateful GROUP BY support
    group_states: HashMap<String, GroupByState>,
    // Performance monitoring
    performance_monitor:
        Option<Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>>,
    // Configuration for enhanced features
    config: StreamingConfig,
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
            record_count: 0,
            group_states: HashMap::new(),
            performance_monitor: None,
            config,
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
        monitor: Option<Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>>,
    ) {
        self.performance_monitor = monitor;
    }

    /// Get reference to performance monitor if enabled
    pub fn performance_monitor(
        &self,
    ) -> Option<&Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>> {
        self.performance_monitor.as_ref()
    }

    /// Create processor context for new processor-based execution
    /// Create high-performance processor context optimized for threading
    /// Loads only the window states needed for this specific processing call
    fn create_processor_context(&self, query_id: &str) -> ProcessorContext {
        let mut context = ProcessorContext::new(query_id);

        // Set engine state
        context.record_count = self.record_count;
        context.window_context = self.get_window_context_for_processors(query_id);
        context.join_context = JoinContext::new();
        context.performance_monitor = self.performance_monitor.as_ref().map(Arc::clone);

        // Load window states efficiently (only for queries we're processing)
        context.load_window_states(self.load_window_states_for_context(query_id));

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
    fn load_window_states_for_context(&self, query_id: &str) -> Vec<(String, WindowState)> {
        let mut states = Vec::with_capacity(1); // Usually just one state per context

        // Load the specific window state for this query if it exists
        if let Some(execution) = self.active_queries.get(query_id) {
            if let Some(window_state) = &execution.window_state {
                states.push((query_id.to_string(), window_state.clone()));
            }
        }

        states
    }

    /// Save modified window states back to engine (high-performance, saves only dirty states)
    /// Called after processor context completes to persist changes
    fn save_window_states_from_context(&mut self, context: &ProcessorContext) {
        for (query_id, window_state) in context.get_dirty_window_states() {
            if let Some(execution) = self.active_queries.get_mut(&query_id) {
                execution.window_state = Some(window_state);
            }
            // Note: If query execution doesn't exist, we skip saving the state
            // This can happen if the query completed between context creation and persistence
        }
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
        let mut context = self.create_processor_context(&query_id);

        // Set LIMIT in context if present
        if let StreamingQuery::Select { limit, .. } = query {
            context.max_records = *limit;
        }

        // Share engine GROUP BY states with processor context
        context.group_by_states = self.group_states.clone();

        let result = QueryProcessor::process_query(query, record, &mut context)?;

        // Update engine state from context - sync back the GROUP BY states
        self.group_states = std::mem::take(&mut context.group_by_states);

        // NOTE: GROUP BY results emission moved to explicit triggers
        // Emitting after every record was causing performance issues and incorrect results
        // In a complete streaming implementation, GROUP BY results should be emitted based on:
        // - Time windows (every N seconds)
        // - Count windows (every N records)
        // - Memory pressure
        // - Explicit flush commands
        // For now, results accumulate in group_states and can be retrieved via explicit calls

        // Persist window states from context (high-performance, only saves dirty states)
        self.save_window_states_from_context(&context);

        // Update engine state from context
        if result.should_count && result.record.is_some() {
            self.record_count += 1;
        }

        // Apply header mutations
        self.apply_header_mutations(&result.header_mutations)?;

        Ok(result.record)
    }

    /// Generate a consistent query ID for processor context management
    fn generate_query_id(&self, query: &StreamingQuery) -> String {
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
            StreamingQuery::CreateStreamInto { name, .. } => format!("create_stream_into_{}", name),
            StreamingQuery::CreateTableInto { name, .. } => format!("create_table_into_{}", name),
            StreamingQuery::Show { .. } => "show_query".to_string(),
            _ => "unknown_query".to_string(),
        }
    }

    /// Header mutation application handler
    fn apply_header_mutations(
        &mut self,
        mutations: &[ProcessorHeaderMutation],
    ) -> Result<(), SqlError> {
        // Store mutations to apply to output records
        // Implementation depends on how headers are currently handled
        for mutation in mutations {
            match &mutation.operation {
                ProcessorHeaderOperation::Set => {
                    // SET_HEADER implementation would go here
                    log::debug!(
                        "Header mutation: SET {} = {:?}",
                        mutation.key,
                        mutation.value
                    );
                }
                ProcessorHeaderOperation::Remove => {
                    // REMOVE_HEADER implementation would go here
                    log::debug!("Header mutation: REMOVE {}", mutation.key);
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
    /// * `record` - The complete StreamRecord with fields, metadata, and headers
    pub async fn execute_with_record(
        &mut self,
        query: &StreamingQuery,
        mut stream_record: StreamRecord,
    ) -> Result<(), SqlError> {
        // For windowed queries, try to extract event time from _timestamp field if present
        if let StreamingQuery::Select {
            window: Some(_), ..
        } = query
        {
            if let Some(ts_field) = stream_record.fields.get("_timestamp") {
                match ts_field {
                    FieldValue::Integer(ts) => stream_record.timestamp = *ts,
                    FieldValue::Float(ts) => stream_record.timestamp = *ts as i64,
                    _ => {
                        // Keep existing timestamp if _timestamp field isn't a valid time
                    }
                }
            }
        }

        self.execute_internal(query, stream_record).await
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
            ..
        } = query
        {
            // For windowed queries, we need to simulate the streaming execution model

            // Initialize window state if needed for this query
            let query_id = "execute_query".to_string();
            if !self.active_queries.contains_key(&query_id) {
                let window_state = Some(WindowState {
                    window_spec: window_spec.clone(),
                    buffer: Vec::new(),
                    last_emit: 0,
                });

                let execution = QueryExecution {
                    query: query.clone(),
                    state: ExecutionState::Running,
                    window_state,
                };

                self.active_queries.insert(query_id.clone(), execution);
            }

            // Process using windowed logic with high-performance state management
            {
                let mut context = self.create_processor_context(&query_id);
                let result = WindowProcessor::process_windowed_query(
                    &query_id,
                    query,
                    &stream_record,
                    &mut context,
                )?;

                // Efficiently persist only modified window states (zero-copy for unchanged states)
                self.save_window_states_from_context(&context);

                result
            }
        } else {
            // Regular non-windowed processing
            self.apply_query(query, &stream_record)?
        };

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

    #[doc(hidden)]
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

        let execution = QueryExecution {
            query,
            state: ExecutionState::Running,
            window_state,
        };

        self.active_queries.insert(query_id, execution);
        Ok(())
    }

    async fn stop_query_execution(&mut self, query_id: &str) -> Result<(), SqlError> {
        if let Some(mut execution) = self.active_queries.remove(query_id) {
            execution.state = ExecutionState::Stopped;
        }
        Ok(())
    }

    #[doc(hidden)]
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
                    let mut context = self.create_processor_context(&query_id);
                    let result = WindowProcessor::process_windowed_query(
                        &query_id,
                        &query,
                        &record,
                        &mut context,
                    )?;

                    // Persist modified states efficiently
                    self.save_window_states_from_context(&context);

                    result
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
        let trigger_record = StreamRecord {
            fields: HashMap::new(),
            timestamp: i64::MAX, // Far future timestamp
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
            event_time: None,
        };

        // Process the trigger for all active queries to flush any pending windows
        let active_query_ids: Vec<String> = self.active_queries.keys().cloned().collect();
        for query_id in active_query_ids {
            if let Some(execution) = self.active_queries.get(&query_id) {
                let query = execution.query.clone();
                if let StreamingQuery::Select {
                    window: Some(_), ..
                } = &query
                {
                    // Only flush windowed queries
                    let result = {
                        let mut context = self.create_processor_context(&query_id);
                        let result = WindowProcessor::process_windowed_query(
                            &query_id,
                            &query,
                            &trigger_record,
                            &mut context,
                        )?;

                        // Persist modified states efficiently
                        self.save_window_states_from_context(&context);

                        result
                    };
                    if let Some(result_record) = result {
                        // Send the flushed result directly - no conversion needed!
                        let _ = self.output_sender.send(result_record);
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
            StreamingQuery::CreateStreamInto { as_select, .. } => {
                self.query_matches_stream(as_select, stream_name)
            }
            StreamingQuery::CreateTableInto { as_select, .. } => {
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
    fn emit_group_by_results(
        &mut self,
        fields: &[SelectField],
        having: &Option<Expr>,
    ) -> Result<(), SqlError> {
        // Clone the group states to avoid borrow conflicts
        let group_states = self.group_states.clone();

        // Iterate through all accumulated GROUP BY states and emit results
        for group_state in group_states.values() {
            for accumulator in group_state.groups.values() {
                // Generate result record for this group
                let mut result_fields = HashMap::new();

                // Evaluate SELECT fields using the accumulator
                for field in fields {
                    match field {
                        SelectField::Expression { expr, alias } => {
                            let field_name = alias
                                .as_ref()
                                .unwrap_or(&SelectProcessor::get_expression_name(expr))
                                .clone();

                            match expr {
                                Expr::Function { name: _, args: _ } => {
                                    // Delegate to AggregateFunctions module for proper handling
                                    match AggregateFunctions::compute_field_aggregate_value(
                                        &field_name,
                                        expr,
                                        accumulator,
                                    ) {
                                        Ok(value) => {
                                            result_fields.insert(field_name, value);
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "Failed to compute aggregate for {}: {}",
                                                field_name,
                                                e
                                            );
                                            result_fields.insert(field_name, FieldValue::Null);
                                        }
                                    }
                                }
                                Expr::BinaryOp {
                                    left: _,
                                    op: _,
                                    right: _,
                                } => {
                                    // This handles expressions like "amount > 150" in SELECT
                                    // We need to evaluate the expression using the sample record
                                    if let Some(sample_record) = &accumulator.sample_record {
                                        match ExpressionEvaluator::evaluate_expression_value(
                                            expr,
                                            sample_record,
                                        ) {
                                            Ok(value) => {
                                                result_fields.insert(field_name, value);
                                            }
                                            Err(_) => {
                                                result_fields.insert(field_name, FieldValue::Null);
                                            }
                                        }
                                    } else {
                                        result_fields.insert(field_name, FieldValue::Null);
                                    }
                                }
                                _ => {
                                    // For other expressions, get first value
                                    let value = accumulator
                                        .first_values
                                        .get(&field_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null);
                                    result_fields.insert(field_name, value);
                                }
                            }
                        }
                        SelectField::Column(name) => {
                            let value = accumulator
                                .first_values
                                .get(name)
                                .cloned()
                                .unwrap_or(FieldValue::Null);
                            result_fields.insert(name.clone(), value);
                        }
                        SelectField::AliasedColumn { column, alias } => {
                            let value = accumulator
                                .first_values
                                .get(column)
                                .cloned()
                                .unwrap_or(FieldValue::Null);
                            result_fields.insert(alias.clone(), value);
                        }
                        SelectField::Wildcard => {
                            // Add all fields from sample record
                            if let Some(sample_record) = &accumulator.sample_record {
                                result_fields.extend(sample_record.fields.clone());
                            }
                        }
                    }
                }

                // Apply HAVING clause filter if present
                if let Some(having_expr) = having {
                    // Create temporary record to evaluate HAVING
                    let temp_record = StreamRecord {
                        fields: result_fields.clone(),
                        timestamp: accumulator
                            .sample_record
                            .as_ref()
                            .map(|r| r.timestamp)
                            .unwrap_or(0),
                        offset: accumulator
                            .sample_record
                            .as_ref()
                            .map(|r| r.offset)
                            .unwrap_or(0),
                        partition: accumulator
                            .sample_record
                            .as_ref()
                            .map(|r| r.partition)
                            .unwrap_or(0),
                        headers: accumulator
                            .sample_record
                            .as_ref()
                            .map(|r| r.headers.clone())
                            .unwrap_or_default(),
                        event_time: None,
                    };

                    // Skip this group if it doesn't pass HAVING filter
                    if !ExpressionEvaluator::evaluate_expression(having_expr, &temp_record)? {
                        continue;
                    }
                }

                // Create and emit result record
                let output_record = StreamRecord {
                    fields: result_fields,
                    timestamp: accumulator
                        .sample_record
                        .as_ref()
                        .map(|r| r.timestamp)
                        .unwrap_or(0),
                    offset: accumulator
                        .sample_record
                        .as_ref()
                        .map(|r| r.offset)
                        .unwrap_or(0),
                    partition: accumulator
                        .sample_record
                        .as_ref()
                        .map(|r| r.partition)
                        .unwrap_or(0),
                    headers: accumulator
                        .sample_record
                        .as_ref()
                        .map(|r| r.headers.clone())
                        .unwrap_or_default(),
                    event_time: None,
                };

                // Send output record directly - no conversion needed!
                if self.output_sender.send(output_record).is_err() {
                    // Channel closed, continue processing
                }
            }
        }

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
        context.group_by_states = self.group_states.clone();
        context.performance_monitor = self.performance_monitor.as_ref().map(Arc::clone);

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

        // Sync state back to engine
        self.group_states = std::mem::take(&mut context.group_by_states);
        self.save_window_states_from_context(&context);

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
                let mut context = self.create_processor_context(&query_id);
                context.group_by_states = self.group_states.clone();

                let result = QueryProcessor::process_query(query, &record, &mut context)?;

                if let Some(output_record) = result.record {
                    results.push(output_record);
                }

                // Sync state
                self.group_states = std::mem::take(&mut context.group_by_states);
                self.save_window_states_from_context(&context);

                if result.should_count {
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
                        let mut context = self.create_processor_context(&query_id);
                        context.group_by_states = self.group_states.clone();

                        let result = QueryProcessor::process_query(query, &record, &mut context)?;

                        if let Some(output_record) = result.record {
                            writer.write(output_record).await.map_err(|e| {
                                SqlError::ExecutionError {
                                    message: format!("Failed to write output: {}", e),
                                    query: None,
                                }
                            })?;
                        }

                        // Sync state
                        self.group_states = std::mem::take(&mut context.group_by_states);
                        self.save_window_states_from_context(&context);

                        if result.should_count {
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
}
