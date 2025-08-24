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
# use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
# use ferrisstreams::ferris::serialization::JsonFormat;
# use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
# use std::sync::Arc;
# use tokio::sync::mpsc;
# use std::collections::HashMap;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let (output_sender, _receiver) = mpsc::unbounded_channel();
let serialization_format = Arc::new(JsonFormat);
let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);

// Parse a simple query and execute with a record
let parser = StreamingSqlParser::new();
let query = parser.parse("SELECT * FROM stream")?;
let record = HashMap::new(); // Empty record for example
engine.execute(&query, record).await?;
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
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create parser and execution engine
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx, serialization_format);

    // Execute a simple SELECT query
    let query = parser.parse("SELECT customer_id, amount * 1.1 AS amount_with_tax FROM orders WHERE amount > 100")?;

    // Create test record using InternalValue types
    let mut record = HashMap::new();
    record.insert("customer_id".to_string(), InternalValue::String("123".to_string()));
    record.insert("amount".to_string(), InternalValue::Number(150.0));

    engine.execute(&query, record).await?;

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
use super::expression::ExpressionEvaluator;
use super::internal::{
    ExecutionMessage, ExecutionState, GroupByState, QueryExecution, WindowState,
};
use super::types::{FieldValue, StreamRecord};
use super::utils::FieldValueConverter;
use crate::ferris::serialization::{InternalValue, SerializationFormat};
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
    message_sender: mpsc::Sender<ExecutionMessage>,
    message_receiver: Option<mpsc::Receiver<ExecutionMessage>>,
    output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
    _serialization_format: Arc<dyn SerializationFormat>,
    record_count: u64,
    // Stateful GROUP BY support
    group_states: HashMap<String, GroupByState>,
    // Performance monitoring
    performance_monitor: Option<Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>>,
}

// =============================================================================
// MAIN EXECUTION ENGINE IMPLEMENTATION
// =============================================================================

impl StreamExecutionEngine {
    pub fn new(
        output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
        _serialization_format: Arc<dyn SerializationFormat>,
    ) -> Self {
        let (message_sender, receiver) = mpsc::channel(1000);
        Self {
            active_queries: HashMap::new(),
            message_sender,
            message_receiver: Some(receiver),
            output_sender,
            _serialization_format,
            record_count: 0,
            group_states: HashMap::new(),
            performance_monitor: None,
        }
    }

    /// Set performance monitor for tracking query execution metrics
    pub fn set_performance_monitor(&mut self, monitor: Option<Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>>) {
        self.performance_monitor = monitor;
    }

    /// Get reference to performance monitor if enabled
    pub fn performance_monitor(&self) -> Option<&Arc<crate::ferris::sql::execution::performance::PerformanceMonitor>> {
        self.performance_monitor.as_ref()
    }

    /// Create processor context for new processor-based execution
    /// Create high-performance processor context optimized for threading
    /// Loads only the window states needed for this specific processing call
    fn create_processor_context(&self, query_id: &str) -> ProcessorContext {
        let mut context = ProcessorContext {
            record_count: self.record_count,
            max_records: None,
            window_context: self.get_window_context_for_processors(query_id),
            join_context: JoinContext,
            group_by_states: HashMap::new(),
            schemas: HashMap::new(),
            stream_handles: HashMap::new(),
            data_sources: HashMap::new(), // No default data sources - must be provided externally

            // Initialize high-performance window state management
            persistent_window_states: Vec::with_capacity(2), // Most contexts handle 1-2 queries
            dirty_window_states: 0,
            metadata: HashMap::new(),
            performance_monitor: self.performance_monitor.as_ref().map(|m| Arc::clone(m)),
        };

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

    pub async fn execute(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
    ) -> Result<(), SqlError> {
        self.execute_with_headers(query, record, HashMap::new())
            .await
    }

    pub async fn execute_with_headers(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
        headers: HashMap<String, String>,
    ) -> Result<(), SqlError> {
        self.execute_with_metadata(query, record, headers, None, None, None)
            .await
    }

    /// Executes a SQL query with full metadata (headers, timestamp, offset, partition).
    ///
    /// # Arguments
    ///
    /// * `query` - The parsed SQL query to execute
    /// * `record` - The input record data  
    /// * `headers` - Message headers
    /// * `timestamp` - Record timestamp (optional)
    /// * `offset` - Kafka offset (optional)
    /// * `partition` - Kafka partition (optional)
    pub async fn execute_with_metadata(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
        headers: HashMap<String, String>,
        timestamp: Option<i64>,
        offset: Option<i64>,
        partition: Option<i32>,
    ) -> Result<(), SqlError> {
        // Convert input record to StreamRecord
        let fields_map: HashMap<String, FieldValue> = record
            .into_iter()
            .map(|(k, v)| {
                let field_value = FieldValueConverter::internal_to_field_value(v);
                (k, field_value)
            })
            .collect();

        // For windowed queries, try to extract event time from _timestamp field if present
        let record_timestamp = if let StreamingQuery::Select {
            window: Some(_), ..
        } = query
        {
            if let Some(ts_field) = fields_map.get("_timestamp") {
                match ts_field {
                    FieldValue::Integer(ts) => *ts,
                    FieldValue::Float(ts) => *ts as i64,
                    _ => timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                }
            } else {
                timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
            }
        } else {
            timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
        };

        let stream_record = StreamRecord {
            fields: fields_map,
            timestamp: record_timestamp,
            offset: offset.unwrap_or(0),
            partition: partition.unwrap_or(0),
            headers,
        };

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
            // Convert result to InternalValue format using utility
            let internal_result: HashMap<String, InternalValue> = result
                .fields
                .into_iter()
                .map(|(k, v)| (k, FieldValueConverter::field_value_to_internal(v)))
                .collect();

            // Send result through both channels
            self.message_sender
                .send(ExecutionMessage::QueryResult {
                    query_id: "default".to_string(),
                    result: stream_record,
                })
                .await
                .map_err(|_| SqlError::ExecutionError {
                    message: "Failed to send result".to_string(),
                    query: None,
                })?;

            // Send result to output channel (non-async send for unbounded channel)
            self.output_sender
                .send(internal_result)
                .map_err(|_| SqlError::ExecutionError {
                    message: "Failed to send result to output channel".to_string(),
                    query: None,
                })?;
        }

        Ok(())
    }

    /// Starts the execution engine's message processing loop.
    ///
    /// This method must be called to begin processing query execution messages.
    pub async fn start(&mut self) -> Result<(), SqlError> {
        let mut receiver =
            self.message_receiver
                .take()
                .ok_or_else(|| SqlError::ExecutionError {
                    message: "Engine already started".to_string(),
                    query: None,
                })?;

        while let Some(message) = receiver.recv().await {
            match message {
                ExecutionMessage::StartJob { job_id, query } => {
                    self.start_query_execution(job_id, query).await?;
                }
                ExecutionMessage::StopJob { job_id } => {
                    self.stop_query_execution(&job_id).await?;
                }
                ExecutionMessage::ProcessRecord {
                    stream_name,
                    record,
                } => {
                    self.process_stream_record(&stream_name, record).await?;
                }
                ExecutionMessage::QueryResult { .. } => {
                    // Handle query results
                }
            }
        }

        Ok(())
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
            // Convert StreamRecord to HashMap<String, InternalValue> and send to output channel
            let output_record: HashMap<String, InternalValue> = result
                .fields
                .into_iter()
                .map(|(k, v)| {
                    let internal_val = match v {
                        FieldValue::Integer(i) => InternalValue::Integer(i),
                        FieldValue::Float(f) => InternalValue::Number(f),
                        FieldValue::String(s) => InternalValue::String(s),
                        FieldValue::Boolean(b) => InternalValue::Boolean(b),
                        FieldValue::Null => InternalValue::Null,
                        _ => InternalValue::String(format!("{:?}", v)),
                    };
                    (k, internal_val)
                })
                .collect();

            // Send result to output channel (used by tests and external consumers)
            let _ = self.output_sender.send(output_record);
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
                        // Send the flushed result
                        let output_record: HashMap<String, InternalValue> = result_record
                            .fields
                            .into_iter()
                            .map(|(k, v)| {
                                let internal_val = match v {
                                    FieldValue::Integer(i) => InternalValue::Integer(i),
                                    FieldValue::Float(f) => InternalValue::Number(f),
                                    FieldValue::String(s) => InternalValue::String(s),
                                    FieldValue::Boolean(b) => InternalValue::Boolean(b),
                                    FieldValue::Null => InternalValue::Null,
                                    _ => InternalValue::String(format!("{:?}", v)),
                                };
                                (k, internal_val)
                            })
                            .collect();

                        let _ = self.output_sender.send(output_record);
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
        for (_query_key, group_state) in &group_states {
            for (_group_key, accumulator) in &group_state.groups {
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
                };

                // Convert to internal record and send
                let internal_record: HashMap<String, InternalValue> = output_record
                    .fields
                    .into_iter()
                    .map(|(k, v)| (k, FieldValueConverter::field_value_to_internal(v)))
                    .collect();

                if self.output_sender.send(internal_record).is_err() {
                    // Channel closed, continue processing
                }
            }
        }

        Ok(())
    }
}
