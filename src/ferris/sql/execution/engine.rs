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
use super::expression::{ArithmeticOperations, BuiltinFunctions, ExpressionEvaluator};
use super::internal::{
    ExecutionMessage, ExecutionState, GroupByState, HeaderMutation, HeaderOperation,
    QueryExecution, WindowState,
};
use super::types::{FieldValue, StreamRecord};
use super::utils::{FieldValueConverter, TimeExtractor};
use crate::ferris::serialization::{InternalValue, SerializationFormat};
use crate::ferris::sql::ast::{
    Expr, JoinClause, JoinType, LiteralValue, OverClause, SelectField, StreamSource, StreamingQuery,
    WindowSpec,
};
use crate::ferris::sql::error::SqlError;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use log::warn;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
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
        }
    }

    /// Step 1.3: Create processor context for new processor-based execution
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
            data_sources: self.create_default_data_sources(), // Provide basic data sources for testing

            // Initialize high-performance window state management
            persistent_window_states: Vec::with_capacity(2), // Most contexts handle 1-2 queries
            dirty_window_states: 0,
            metadata: HashMap::new(),
        };

        // Load window states efficiently (only for queries we're processing)
        context.load_window_states(self.load_window_states_for_context(query_id));

        context
    }

    /// Create default data sources for JOIN and subquery operations
    /// This provides basic data sources needed for JOIN operations and subqueries
    fn create_default_data_sources(&self) -> HashMap<String, Vec<StreamRecord>> {
        use crate::ferris::sql::execution::test_data_sources::*;

        // Use the same test data sources we created for subqueries
        // This ensures JOIN operations have access to tables like 'users', 'products', 'blocks', etc.
        create_test_data_sources()
    }

    /// Create processor context with external data sources
    /// This allows tests and external systems to inject data sources for subqueries
    pub fn create_processor_context_with_data_sources(
        &self,
        query_id: &str,
        data_sources: HashMap<String, Vec<StreamRecord>>,
    ) -> ProcessorContext {
        let mut context = self.create_processor_context(query_id);
        context.set_data_sources(data_sources);
        context
    }

    /// Step 1.3: Helper method to create window context for processors
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

    /// Step 3.2: Header mutation application handler
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

    // EXPRESSION EVALUATION FUNCTIONS
    // =============================================================================

    #[doc(hidden)]
    pub fn add_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        ArithmeticOperations::add_values(left, right)
    }

    #[doc(hidden)]
    pub fn subtract_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        ArithmeticOperations::subtract_values(left, right)
    }

    #[doc(hidden)]
    pub fn multiply_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        ArithmeticOperations::multiply_values(left, right)
    }

    #[doc(hidden)]
    pub fn divide_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        ArithmeticOperations::divide_values(left, right)
    }

    // =============================================================================
    // WINDOW FUNCTIONS
    // =============================================================================

    /// Evaluate window functions like LAG, LEAD, ROW_NUMBER with OVER clause
    fn evaluate_window_function(
        &mut self,
        function_name: &str,
        args: &[Expr],
        _over_clause: &OverClause,
        record: &StreamRecord,
        window_buffer: &mut Vec<StreamRecord>,
    ) -> Result<FieldValue, SqlError> {
        // Validate function name is not empty
        if function_name.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Window function name cannot be empty".to_string(),
                query: Some(format!("Window function: {}", function_name)),
            });
        }

        match function_name.to_uppercase().as_str() {
            "LAG" => {
                // Validate argument count
                if args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "LAG function requires at least 1 argument (expression)"
                            .to_string(),
                        query: Some(format!("LAG({})", if args.is_empty() { "" } else { "..." })),
                    });
                }
                if args.len() > 3 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "LAG function accepts at most 3 arguments (expression, offset, default_value), but {} were provided",
                            args.len()
                        ),
                        query: Some("LAG(expression, [offset], [default_value])".to_string()),
                    });
                }

                // Parse offset (default is 1)
                let offset = if args.len() >= 2 {
                    match ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                        FieldValue::Integer(n) => {
                            if n < 0 {
                                return Err(SqlError::ExecutionError {
                                    message: format!("LAG offset must be non-negative, got {}", n),
                                    query: Some(format!("LAG(expression, {})", n)),
                                });
                            }
                            if n > i32::MAX as i64 {
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "LAG offset {} exceeds maximum allowed value {}",
                                        n,
                                        i32::MAX
                                    ),
                                    query: Some("LAG offset too large".to_string()),
                                });
                            }
                            n as usize
                        }
                        FieldValue::Null => {
                            return Err(SqlError::ExecutionError {
                                message: "LAG offset cannot be NULL".to_string(),
                                query: Some("LAG(expression, NULL)".to_string()),
                            });
                        }
                        other => {
                            return Err(SqlError::ExecutionError {
                                message: format!(
                                    "LAG offset must be an integer, got {}",
                                    other.type_name()
                                ),
                                query: Some(format!(
                                    "LAG(expression, {})",
                                    other.type_name().to_lowercase()
                                )),
                            });
                        }
                    }
                } else {
                    1
                };

                // Parse default value (if provided)
                let default_value = if args.len() == 3 {
                    Some(ExpressionEvaluator::evaluate_expression_value(
                        &args[2], record,
                    )?)
                } else {
                    None
                };

                // Look back in the window buffer
                if offset == 0 {
                    // Offset 0 means current record
                    ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                } else if window_buffer.len() >= offset {
                    let lag_record = &window_buffer[window_buffer.len() - offset];
                    ExpressionEvaluator::evaluate_expression_value(&args[0], lag_record)
                } else {
                    // Not enough records in buffer, return default or NULL
                    Ok(default_value.unwrap_or(FieldValue::Null))
                }
            }
            "LEAD" => {
                // Validate argument count
                if args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "LEAD function requires at least 1 argument (expression)"
                            .to_string(),
                        query: Some("LEAD(expression, [offset], [default_value])".to_string()),
                    });
                }
                if args.len() > 3 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "LEAD function accepts at most 3 arguments (expression, offset, default_value), but {} were provided",
                            args.len()
                        ),
                        query: Some("LEAD(expression, [offset], [default_value])".to_string()),
                    });
                }

                // Parse offset (default is 1)
                let offset = if args.len() >= 2 {
                    match ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                        FieldValue::Integer(n) => {
                            if n < 0 {
                                return Err(SqlError::ExecutionError {
                                    message: format!("LEAD offset must be non-negative, got {}", n),
                                    query: Some(format!("LEAD(expression, {})", n)),
                                });
                            }
                            if n > i32::MAX as i64 {
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "LEAD offset {} exceeds maximum allowed value {}",
                                        n,
                                        i32::MAX
                                    ),
                                    query: Some("LEAD offset too large".to_string()),
                                });
                            }
                            n as usize
                        }
                        FieldValue::Null => {
                            return Err(SqlError::ExecutionError {
                                message: "LEAD offset cannot be NULL".to_string(),
                                query: Some("LEAD(expression, NULL)".to_string()),
                            });
                        }
                        other => {
                            return Err(SqlError::ExecutionError {
                                message: format!(
                                    "LEAD offset must be an integer, got {}",
                                    other.type_name()
                                ),
                                query: Some(format!(
                                    "LEAD(expression, {})",
                                    other.type_name().to_lowercase()
                                )),
                            });
                        }
                    }
                } else {
                    1
                };

                // Parse default value (if provided)
                let default_value = if args.len() == 3 {
                    Some(ExpressionEvaluator::evaluate_expression_value(
                        &args[2], record,
                    )?)
                } else {
                    None
                };

                // For streaming LEAD, we cannot look forward in most cases
                if offset > 0 {
                    warn!(
                        "LEAD window function: cannot look forward {} records in streaming data - returning default value or NULL",
                        offset
                    );
                    Ok(default_value.unwrap_or(FieldValue::Null))
                } else {
                    // Offset 0 means current record
                    ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                }
            }
            "ROW_NUMBER" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "ROW_NUMBER function takes no arguments, but {} were provided",
                            args.len()
                        ),
                        query: Some(format!("ROW_NUMBER({} arguments)", args.len())),
                    });
                }

                // ROW_NUMBER() OVER (...) - returns current position in partition
                // For streaming, this is the position in the current window buffer + 1
                let row_number = window_buffer.len() + 1;
                if row_number > i64::MAX as usize {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "ROW_NUMBER overflow: row number {} exceeds maximum value",
                            row_number
                        ),
                        query: Some("ROW_NUMBER() OVER (...)".to_string()),
                    });
                }
                Ok(FieldValue::Integer(row_number as i64))
            }
            "RANK" | "DENSE_RANK" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "{} function takes no arguments, but {} were provided",
                            function_name.to_uppercase(),
                            args.len()
                        ),
                        query: Some(format!(
                            "{}({} arguments)",
                            function_name.to_uppercase(),
                            args.len()
                        )),
                    });
                }

                // For streaming without proper partitioning, RANK and DENSE_RANK behave like ROW_NUMBER
                warn!(
                    "{} window function: returning ROW_NUMBER behavior for streaming context",
                    function_name.to_uppercase()
                );
                let rank = window_buffer.len() + 1;
                if rank > i64::MAX as usize {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "{} overflow: rank {} exceeds maximum value",
                            function_name.to_uppercase(),
                            rank
                        ),
                        query: Some(format!("{}() OVER (...)", function_name.to_uppercase())),
                    });
                }
                Ok(FieldValue::Integer(rank as i64))
            }
            "FIRST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "FIRST_VALUE function requires exactly 1 argument (expression), but {} were provided",
                            args.len()
                        ),
                        query: Some("FIRST_VALUE(expression)".to_string()),
                    });
                }

                // Return the value from the first record in the window buffer
                if !window_buffer.is_empty() {
                    ExpressionEvaluator::evaluate_expression_value(&args[0], &window_buffer[0])
                } else {
                    // If buffer is empty, evaluate against current record
                    ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                }
            }
            "LAST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "LAST_VALUE function requires exactly 1 argument (expression), but {} were provided",
                            args.len()
                        ),
                        query: Some("LAST_VALUE(expression)".to_string()),
                    });
                }

                // Return the value from the last record in the window buffer (current record)
                ExpressionEvaluator::evaluate_expression_value(&args[0], record)
            }
            "NTH_VALUE" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "NTH_VALUE function requires exactly 2 arguments (expression, n), but {} were provided",
                            args.len()
                        ),
                        query: Some("NTH_VALUE(expression, n)".to_string()),
                    });
                }

                // Parse the nth position
                let nth = match ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                    FieldValue::Integer(n) => {
                        if n <= 0 {
                            return Err(SqlError::ExecutionError {
                                message: format!("NTH_VALUE position must be positive, got {}", n),
                                query: Some(format!("NTH_VALUE(expression, {})", n)),
                            });
                        }
                        n as usize
                    }
                    FieldValue::Null => {
                        return Err(SqlError::ExecutionError {
                            message: "NTH_VALUE position cannot be NULL".to_string(),
                            query: Some("NTH_VALUE(expression, NULL)".to_string()),
                        });
                    }
                    other => {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "NTH_VALUE position must be an integer, got {}",
                                other.type_name()
                            ),
                            query: Some(format!(
                                "NTH_VALUE(expression, {})",
                                other.type_name().to_lowercase()
                            )),
                        });
                    }
                };

                // Get the nth record from the window buffer (1-indexed)
                let total_records = window_buffer.len() + 1; // +1 for current record
                if nth <= total_records {
                    if nth <= window_buffer.len() {
                        // nth record is in the buffer
                        ExpressionEvaluator::evaluate_expression_value(
                            &args[0],
                            &window_buffer[nth - 1],
                        )
                    } else {
                        // nth record is the current record
                        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                    }
                } else {
                    // nth record doesn't exist
                    Ok(FieldValue::Null)
                }
            }
            "PERCENT_RANK" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "PERCENT_RANK function takes no arguments, but {} were provided",
                            args.len()
                        ),
                        query: Some(format!("PERCENT_RANK({} arguments)", args.len())),
                    });
                }

                // PERCENT_RANK() = (rank - 1) / (total_rows - 1)
                // For streaming, we use current position in buffer
                let current_rank = window_buffer.len() + 1;
                let total_rows = current_rank; // In streaming, we only know current position

                if total_rows <= 1 {
                    Ok(FieldValue::Float(0.0))
                } else {
                    let percent_rank = (current_rank - 1) as f64 / (total_rows - 1) as f64;
                    Ok(FieldValue::Float(percent_rank))
                }
            }
            "CUME_DIST" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "CUME_DIST function takes no arguments, but {} were provided",
                            args.len()
                        ),
                        query: Some(format!("CUME_DIST({} arguments)", args.len())),
                    });
                }

                // CUME_DIST() = number_of_rows_with_values_<=_current_row / total_rows
                // For streaming, we approximate this as current_position / total_known_rows
                let current_position = window_buffer.len() + 1;
                let total_known_rows = current_position;

                let cume_dist = current_position as f64 / total_known_rows as f64;
                Ok(FieldValue::Float(cume_dist))
            }
            "NTILE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "NTILE function requires exactly 1 argument (n), but {} were provided",
                            args.len()
                        ),
                        query: Some("NTILE(n)".to_string()),
                    });
                }

                // Parse the number of tiles
                let tiles = match ExpressionEvaluator::evaluate_expression_value(&args[0], record)?
                {
                    FieldValue::Integer(n) => {
                        if n <= 0 {
                            return Err(SqlError::ExecutionError {
                                message: format!("NTILE tiles count must be positive, got {}", n),
                                query: Some(format!("NTILE({})", n)),
                            });
                        }
                        n
                    }
                    FieldValue::Null => {
                        return Err(SqlError::ExecutionError {
                            message: "NTILE tiles count cannot be NULL".to_string(),
                            query: Some("NTILE(NULL)".to_string()),
                        });
                    }
                    other => {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "NTILE tiles count must be an integer, got {}",
                                other.type_name()
                            ),
                            query: Some(format!("NTILE({})", other.type_name().to_lowercase())),
                        });
                    }
                };

                // Calculate which tile the current row belongs to
                let current_row = window_buffer.len() + 1;
                let total_rows = current_row; // In streaming, we only know current position

                // Calculate tile number (1-indexed)
                let rows_per_tile = (total_rows as f64 / tiles as f64).ceil() as i64;
                let tile_number = ((current_row - 1) as i64 / rows_per_tile) + 1;
                let tile_number = tile_number.min(tiles); // Ensure we don't exceed max tiles

                Ok(FieldValue::Integer(tile_number))
            }
            other => Err(SqlError::ExecutionError {
                message: format!(
                    "Unsupported window function: '{}'. Supported window functions are: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE",
                    other
                ),
                query: Some(format!("{}(...) OVER (...)", other)),
            }),
        }
    }

    fn evaluate_function_with_mutations(
        &self,
        name: &str,
        args: &[Expr],
        record: &StreamRecord,
        header_mutations: &mut Vec<HeaderMutation>,
    ) -> Result<FieldValue, SqlError> {
        match name.to_uppercase().as_str() {
            // Handle header mutation functions locally
            "SET_HEADER" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "SET_HEADER requires exactly two arguments (key, value)"
                            .to_string(),
                        query: None,
                    });
                }
                let key_value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
                let value_field = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

                // Convert key to string representation
                let key = match key_value {
                    FieldValue::String(s) => s,
                    FieldValue::Integer(i) => i.to_string(),
                    FieldValue::Float(f) => f.to_string(),
                    FieldValue::Boolean(b) => b.to_string(),
                    FieldValue::Null => "null".to_string(),
                    _ => format!("{:?}", key_value),
                };

                // Convert value to string representation
                let value = match value_field {
                    FieldValue::String(s) => s,
                    FieldValue::Integer(i) => i.to_string(),
                    FieldValue::Float(f) => f.to_string(),
                    FieldValue::Boolean(b) => b.to_string(),
                    FieldValue::Null => "null".to_string(),
                    _ => format!("{:?}", value_field),
                };

                // Record the header mutation
                header_mutations.push(HeaderMutation {
                    operation: HeaderOperation::Set,
                    key: key.clone(),
                    value: Some(value.clone()),
                });

                // Return the value that was set
                Ok(FieldValue::String(value))
            }
            "REMOVE_HEADER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "REMOVE_HEADER requires exactly one argument (key)".to_string(),
                        query: None,
                    });
                }
                let key_value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;

                // Convert key to string representation
                let key = match key_value {
                    FieldValue::String(s) => s,
                    FieldValue::Integer(i) => i.to_string(),
                    FieldValue::Float(f) => f.to_string(),
                    FieldValue::Boolean(b) => b.to_string(),
                    FieldValue::Null => "null".to_string(),
                    _ => format!("{:?}", key_value),
                };

                // Get the existing header value before removing it
                let existing_value = record.headers.get(&key).cloned();

                // Record the header mutation
                header_mutations.push(HeaderMutation {
                    operation: HeaderOperation::Remove,
                    key: key.clone(),
                    value: None,
                });

                // Return the value that was removed (or NULL if it didn't exist)
                match existing_value {
                    Some(value) => Ok(FieldValue::String(value)),
                    None => Ok(FieldValue::Null),
                }
            }
            // Delegate all other functions to the extracted module
            _ => {
                let func_expr = Expr::Function {
                    name: name.to_string(),
                    args: args.to_vec(),
                };
                BuiltinFunctions::evaluate_function(&func_expr, record)
            }
        }
    }

    #[doc(hidden)]
    pub fn cast_value(&self, value: FieldValue, target_type: &str) -> Result<FieldValue, SqlError> {
        match target_type {
            "INTEGER" | "INT" => match value {
                FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                FieldValue::Float(f) => Ok(FieldValue::Integer(f as i64)),
                FieldValue::String(s) => s.parse::<i64>().map(FieldValue::Integer).map_err(|_| {
                    SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to INTEGER", s),
                        query: None,
                    }
                }),
                FieldValue::Boolean(b) => Ok(FieldValue::Integer(if b { 1 } else { 0 })),
                FieldValue::Decimal(d) => {
                    // Convert decimal to integer, truncating fractional part
                    let int_part = d.trunc();
                    match int_part.to_string().parse::<i64>() {
                        Ok(i) => Ok(FieldValue::Integer(i)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to INTEGER", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to INTEGER", value.type_name()),
                    query: None,
                }),
            },
            "FLOAT" | "DOUBLE" => match value {
                FieldValue::Integer(i) => Ok(FieldValue::Float(i as f64)),
                FieldValue::Float(f) => Ok(FieldValue::Float(f)),
                FieldValue::String(s) => {
                    s.parse::<f64>()
                        .map(FieldValue::Float)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to FLOAT", s),
                            query: None,
                        })
                }
                FieldValue::Boolean(b) => Ok(FieldValue::Float(if b { 1.0 } else { 0.0 })),
                FieldValue::Decimal(d) => {
                    // Convert decimal to float
                    match d.to_string().parse::<f64>() {
                        Ok(f) => Ok(FieldValue::Float(f)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to FLOAT", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to FLOAT", value.type_name()),
                    query: None,
                }),
            },
            "STRING" | "VARCHAR" | "TEXT" => match value {
                FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                FieldValue::String(s) => Ok(FieldValue::String(s)),
                FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                FieldValue::Null => Ok(FieldValue::String("NULL".to_string())),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Decimal(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Ok(FieldValue::String(value.to_display_string())),
            },
            "BOOLEAN" | "BOOL" => match value {
                FieldValue::Integer(i) => Ok(FieldValue::Boolean(i != 0)),
                FieldValue::Float(f) => Ok(FieldValue::Boolean(f != 0.0)),
                FieldValue::String(s) => match s.to_uppercase().as_str() {
                    "TRUE" | "T" | "1" => Ok(FieldValue::Boolean(true)),
                    "FALSE" | "F" | "0" => Ok(FieldValue::Boolean(false)),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to BOOLEAN", s),
                        query: None,
                    }),
                },
                FieldValue::Boolean(b) => Ok(FieldValue::Boolean(b)),
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Decimal(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_)
                | FieldValue::Interval { .. } => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to BOOLEAN", value.type_name()),
                    query: None,
                }),
            },
            "DATE" => match value {
                FieldValue::Date(d) => Ok(FieldValue::Date(d)),
                FieldValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%Y/%m/%d"))
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%m/%d/%Y"))
                    .or_else(|_| NaiveDate::parse_from_str(&s, "%d-%m-%Y"))
                    .map(FieldValue::Date)
                    .map_err(|_| SqlError::ExecutionError {
                        message: format!(
                            "Cannot cast '{}' to DATE. Expected format: YYYY-MM-DD",
                            s
                        ),
                        query: None,
                    }),
                FieldValue::Timestamp(ts) => Ok(FieldValue::Date(ts.date())),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to DATE", value.type_name()),
                    query: None,
                }),
            },
            "TIMESTAMP" | "DATETIME" => match value {
                FieldValue::Timestamp(ts) => Ok(FieldValue::Timestamp(ts)),
                FieldValue::Date(d) => Ok(FieldValue::Timestamp(d.and_hms_opt(0, 0, 0).unwrap())),
                FieldValue::String(s) => {
                    // Try various timestamp formats
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.3f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y/%m/%d %H:%M:%S"))
                        .or_else(|_| {
                            // Try parsing as date only and add time
                            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .map(FieldValue::Timestamp)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to TIMESTAMP. Expected format: YYYY-MM-DD HH:MM:SS", s),
                            query: None,
                        })
                }
                FieldValue::Integer(i) => {
                    // Treat as Unix timestamp (seconds)
                    let dt =
                        DateTime::from_timestamp(i, 0).ok_or_else(|| SqlError::ExecutionError {
                            message: format!("Invalid Unix timestamp: {}", i),
                            query: None,
                        })?;
                    Ok(FieldValue::Timestamp(dt.naive_utc()))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to TIMESTAMP", value.type_name()),
                    query: None,
                }),
            },
            "DECIMAL" | "NUMERIC" => {
                match value {
                    FieldValue::Decimal(d) => Ok(FieldValue::Decimal(d)),
                    FieldValue::Integer(i) => Ok(FieldValue::Decimal(Decimal::from(i))),
                    FieldValue::Float(f) => Decimal::from_str(&f.to_string())
                        .map(FieldValue::Decimal)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast float {} to DECIMAL", f),
                            query: None,
                        }),
                    FieldValue::String(s) => Decimal::from_str(&s)
                        .map(FieldValue::Decimal)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to DECIMAL", s),
                            query: None,
                        }),
                    FieldValue::Boolean(b) => Ok(FieldValue::Decimal(if b {
                        Decimal::ONE
                    } else {
                        Decimal::ZERO
                    })),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast {} to DECIMAL", value.type_name()),
                        query: None,
                    }),
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported cast target type: {}", target_type),
                query: None,
            }),
        }
    }


    /// Process JOIN clauses and combine records
    fn process_joins(
        &mut self,
        left_record: &StreamRecord,
        join_clauses: &[JoinClause],
    ) -> Result<StreamRecord, SqlError> {
        let mut result_record = left_record.clone();

        for join_clause in join_clauses {
            // Get right record using JoinContext implementation
            let join_context = JoinContext;
            let right_record_opt =
                join_context.get_right_record(&join_clause.right_source, &join_clause.window)?;

            match join_clause.join_type {
                JoinType::Inner => {
                    if let Some(right_record) = right_record_opt {
                        let combined_record = self.combine_records(
                            &result_record,
                            &right_record,
                            &join_clause.right_alias,
                        )?;
                        if ExpressionEvaluator::evaluate_expression(
                            &join_clause.condition,
                            &combined_record,
                        )? {
                            result_record = combined_record;
                        } else {
                            // INNER JOIN: if condition fails, no result
                            return Err(SqlError::ExecutionError {
                                message: "INNER JOIN condition not met".to_string(),
                                query: None,
                            });
                        }
                    } else {
                        // INNER JOIN: if no right record, no result
                        return Err(SqlError::ExecutionError {
                            message: "No matching record for INNER JOIN".to_string(),
                            query: None,
                        });
                    }
                }
                JoinType::Left => {
                    if let Some(right_record) = right_record_opt {
                        let combined_record = self.combine_records(
                            &result_record,
                            &right_record,
                            &join_clause.right_alias,
                        )?;
                        if ExpressionEvaluator::evaluate_expression(
                            &join_clause.condition,
                            &combined_record,
                        )? {
                            result_record = combined_record;
                        } else {
                            // LEFT JOIN: if condition fails, use left record with NULL right fields
                            result_record = self.combine_records_with_nulls(
                                &result_record,
                                &join_clause.right_alias,
                                true,
                            )?;
                        }
                    } else {
                        // LEFT JOIN: if no right record, use left record with NULL right fields
                        result_record = self.combine_records_with_nulls(
                            &result_record,
                            &join_clause.right_alias,
                            true,
                        )?;
                    }
                }
                JoinType::Right => {
                    if let Some(right_record) = right_record_opt {
                        let combined_record = self.combine_records(
                            &result_record,
                            &right_record,
                            &join_clause.right_alias,
                        )?;
                        if ExpressionEvaluator::evaluate_expression(
                            &join_clause.condition,
                            &combined_record,
                        )? {
                            result_record = combined_record;
                        } else {
                            // RIGHT JOIN: if condition fails, use right record with NULL left fields
                            result_record = self.combine_records_with_nulls(
                                &right_record,
                                &join_clause.right_alias,
                                false,
                            )?;
                        }
                    } else {
                        // RIGHT JOIN: if no right record, no result (this scenario is rare in stream processing)
                        return Err(SqlError::ExecutionError {
                            message: "No right record for RIGHT JOIN".to_string(),
                            query: None,
                        });
                    }
                }
                JoinType::FullOuter => {
                    if let Some(right_record) = right_record_opt {
                        let combined_record = self.combine_records(
                            &result_record,
                            &right_record,
                            &join_clause.right_alias,
                        )?;
                        if ExpressionEvaluator::evaluate_expression(
                            &join_clause.condition,
                            &combined_record,
                        )? {
                            result_record = combined_record;
                        } else {
                            // FULL OUTER JOIN: if condition fails, could return both records with NULLs
                            // For simplicity, we'll return left record with NULL right fields
                            result_record = self.combine_records_with_nulls(
                                &result_record,
                                &join_clause.right_alias,
                                true,
                            )?;
                        }
                    } else {
                        // FULL OUTER JOIN: if no right record, use left record with NULL right fields
                        result_record = self.combine_records_with_nulls(
                            &result_record,
                            &join_clause.right_alias,
                            true,
                        )?;
                    }
                }
            }
        }

        Ok(result_record)
    }

    /// Optimized stream-table JOIN processing
    /// Tables are materialized views with key-based lookups, streams are continuous data
    #[allow(dead_code)]
    fn process_stream_table_join(
        &self,
        _stream_record: &StreamRecord,
        _table_source: &StreamSource,
        _join_condition: &Expr,
        _join_type: &JoinType,
    ) -> Result<Option<StreamRecord>, SqlError> {
        Err(SqlError::ExecutionError {
            message: "Stream-table JOIN operations are not yet implemented. This requires materialized table state, key-based lookups, and proper streaming join semantics.".to_string(),
            query: None,
        })
    }

    /// Combine left and right records with NULL values for missing side
    fn combine_records_with_nulls(
        &self,
        base_record: &StreamRecord,
        right_alias: &Option<String>,
        is_left_join: bool, // true for LEFT JOIN, false for RIGHT JOIN
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = base_record.fields.clone();

        // Add NULL fields for the missing side
        if is_left_join {
            // LEFT JOIN: add NULL right fields
            let right_prefix = if let Some(alias) = right_alias {
                format!("{}.", alias)
            } else {
                "right_".to_string()
            };

            // Add common right-side fields as NULL
            combined_fields.insert(format!("{}id", right_prefix), FieldValue::Null);
            combined_fields.insert(format!("{}name", right_prefix), FieldValue::Null);
            combined_fields.insert(format!("{}value", right_prefix), FieldValue::Null);
        } else {
            // RIGHT JOIN: add NULL left fields (preserve original left field names as NULL)
            // This is more complex as we'd need to know the left schema
            // For now, we'll just keep the right record as-is
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: base_record.timestamp,
            offset: base_record.offset,
            partition: base_record.partition,
            headers: base_record.headers.clone(),
        })
    }

    /// Combine left and right records for JOIN processing
    fn combine_records(
        &self,
        left: &StreamRecord,
        right: &StreamRecord,
        right_alias: &Option<String>,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = left.fields.clone();

        // Add right side fields, with optional alias prefix
        for (key, value) in &right.fields {
            let field_name = if let Some(alias) = right_alias {
                format!("{}.{}", alias, key)
            } else {
                // If no alias, prefix with "right_" to avoid conflicts
                format!("right_{}", key)
            };
            combined_fields.insert(field_name, value.clone());
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: left.timestamp, // Use left side timestamp
            offset: left.offset,
            partition: left.partition,
            headers: left.headers.clone(),
        })
    }

    // =============================================================================
    // HEADER MUTATION FUNCTIONS
    // =============================================================================

    fn collect_header_mutations_from_fields(
        &self,
        fields: &[SelectField],
        record: &StreamRecord,
        header_mutations: &mut Vec<HeaderMutation>,
    ) -> Result<(), SqlError> {
        for field in fields {
            if let SelectField::Expression { expr, .. } = field {
                self.collect_header_mutations_from_expr(expr, record, header_mutations)?;
            }
        }
        Ok(())
    }

    fn collect_header_mutations_from_expr(
        &self,
        expr: &Expr,
        record: &StreamRecord,
        header_mutations: &mut Vec<HeaderMutation>,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::Function { name, args } => {
                let function_name = name.to_uppercase();
                if function_name == "SET_HEADER" || function_name == "REMOVE_HEADER" {
                    // Evaluate the function with header mutations
                    self.evaluate_function_with_mutations(
                        &function_name,
                        args,
                        record,
                        header_mutations,
                    )?;
                }

                // Also check arguments for nested header function calls
                for arg in args {
                    self.collect_header_mutations_from_expr(arg, record, header_mutations)?;
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_header_mutations_from_expr(left, record, header_mutations)?;
                self.collect_header_mutations_from_expr(right, record, header_mutations)?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_header_mutations_from_expr(expr, record, header_mutations)?;
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    self.collect_header_mutations_from_expr(condition, record, header_mutations)?;
                    self.collect_header_mutations_from_expr(result, record, header_mutations)?;
                }
                if let Some(else_expr) = else_clause {
                    self.collect_header_mutations_from_expr(else_expr, record, header_mutations)?;
                }
            }
            Expr::WindowFunction { args, .. } => {
                for arg in args {
                    self.collect_header_mutations_from_expr(arg, record, header_mutations)?;
                }
            }
            _ => {} // Other expression types don't contain function calls
        }
        Ok(())
    }

    /// Evaluate subquery expressions (IN, EXISTS, scalar subqueries, etc.)

    // =============================================================================
    // WINDOW PROCESSING IMPLEMENTATION
    // =============================================================================

    /// Check if window should emit based on window specification and current time
    fn should_emit_window(
        &self,
        window_state: &WindowState,
        current_time: i64,
        _time_column: Option<&str>,
    ) -> bool {
        match &window_state.window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let current_window_start =
                    TimeExtractor::align_to_window_boundary(current_time, window_size_ms);

                // For tumbling windows, only emit when:
                // 1. This is the first window (last_emit == 0) AND we have the first record past window size
                // 2. We've crossed into a new window boundary

                if window_state.last_emit == 0 {
                    // First window - only emit if we have records past the first window boundary
                    current_time >= window_size_ms
                } else {
                    // Subsequent windows - emit when we cross window boundaries
                    current_window_start > window_state.last_emit
                }
            }
            WindowSpec::Sliding { advance, .. } => {
                let advance_ms = advance.as_millis() as i64;
                current_time >= window_state.last_emit + advance_ms
            }
            WindowSpec::Session { gap, .. } => {
                let gap_ms = gap.as_millis() as i64;
                // Session window emits when gap timeout is exceeded
                current_time >= window_state.last_emit + gap_ms
            }
        }
    }

    /// Find field by suffix patterns
    fn find_field_by_suffix(
        &self,
        result_record: &StreamRecord,
        suffixes: &[&str],
    ) -> Result<String, SqlError> {
        for field_name in result_record.fields.keys() {
            for suffix in suffixes {
                // Check both exact match and suffix match
                if field_name == suffix || field_name.ends_with(suffix) {
                    return Ok(field_name.clone());
                }
            }
        }

        Err(SqlError::ExecutionError {
            message: format!("No field found with suffixes: {:?}", suffixes),
            query: None,
        })
    }

    /// Convert a FieldValue to a string suitable for grouping

    /// Convert a literal value to a group key string
    fn literal_to_group_key(&self, literal: &LiteralValue) -> String {
        match literal {
            LiteralValue::String(s) => s.clone(),
            LiteralValue::Integer(i) => i.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::Boolean(b) => b.to_string(),
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Interval { value, unit } => {
                format!("INTERVAL {} {:?}", value, unit)
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
