/*!
# Streaming SQL Execution Engine

This module implements a streamlined execution engine for streaming SQL queries that
delegates to modular processor components for better maintainability and testability.

## Architecture

The execution engine now uses a modular architecture with specialized processors:

1. **StreamProcessor**: Top-level orchestration of query execution plans
2. **ExpressionEvaluator**: Scalar expression evaluation
3. **FunctionEvaluator**: Built-in function evaluation
4. **GroupByProcessor**: GROUP BY aggregation processing
5. **JoinProcessor**: JOIN operation processing
6. **WindowProcessor**: Window operation processing
7. **QueryPlanner**: Execution plan creation and optimization

## Key Features

- **Modular Design**: Each component has a single, well-defined responsibility
- **Performance**: Efficient streaming processing with minimal buffering
- **Extensibility**: Easy to add new operations and optimizations
- **Type Safety**: Comprehensive error handling and type checking
*/

use crate::ferris::serialization::{InternalValue, SerializationFormat};
use crate::ferris::sql::ast::StreamingQuery;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::stream_processor::{ProcessingResult, StreamProcessor};
use crate::ferris::sql::execution::types::{ExecutionMessage, FieldValue, StreamRecord};
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Main streaming SQL execution engine
///
/// This engine coordinates the execution of streaming SQL queries by delegating
/// to specialized processor components. It maintains minimal state and focuses
/// on orchestration rather than containing business logic.
pub struct StreamExecutionEngine {
    /// Channel for receiving execution messages
    message_sender: mpsc::Sender<ExecutionMessage>,
    message_receiver: Option<mpsc::Receiver<ExecutionMessage>>,

    /// Channel for sending query results
    output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,

    /// Serialization format for output
    _serialization_format: Arc<dyn SerializationFormat>,

    /// Total number of records processed
    record_count: u64,

    /// Active query processors
    active_processors: HashMap<String, StreamProcessor>,
}

/// Query execution state
#[derive(Debug)]
#[allow(dead_code)]
enum ExecutionState {
    Running,
    Paused,
    Stopped,
    Error(String),
}

impl StreamExecutionEngine {
    /// Create a new stream execution engine
    pub fn new(
        output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
        serialization_format: Arc<dyn SerializationFormat>,
    ) -> Self {
        let (message_sender, receiver) = mpsc::channel(1000);
        Self {
            message_sender,
            message_receiver: Some(receiver),
            output_sender,
            _serialization_format: serialization_format,
            record_count: 0,
            active_processors: HashMap::new(),
        }
    }

    /// Execute a query against a record
    pub async fn execute(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
    ) -> Result<(), SqlError> {
        self.execute_with_headers(query, record, HashMap::new())
            .await
    }

    /// Execute a query with headers
    pub async fn execute_with_headers(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
        headers: HashMap<String, String>,
    ) -> Result<(), SqlError> {
        self.execute_with_metadata(query, record, headers, None, None, None)
            .await
    }

    /// Execute a query with full metadata
    pub async fn execute_with_metadata(
        &mut self,
        query: &StreamingQuery,
        record: HashMap<String, InternalValue>,
        headers: HashMap<String, String>,
        timestamp: Option<i64>,
        offset: Option<i64>,
        partition: Option<i32>,
    ) -> Result<(), SqlError> {
        // Convert InternalValue record to StreamRecord
        let stream_record =
            self.convert_to_stream_record(record, headers, timestamp, offset, partition)?;

        // Get or create a processor for this query
        // Use a hash of the query for consistent identification
        let query_id = format!("query_{:?}", query);
        if !self.active_processors.contains_key(&query_id) {
            let processor = StreamProcessor::new(query.clone())?;
            self.active_processors.insert(query_id.clone(), processor);
        }

        // Process the record
        let processor = self.active_processors.get_mut(&query_id).unwrap();
        let result = processor.process_record(stream_record)?;

        // Handle the result
        match result {
            ProcessingResult::Record(output_record) => {
                let output = self.convert_to_internal_values(output_record)?;
                if let Err(e) = self.output_sender.send(output) {
                    return Err(SqlError::ExecutionError {
                        message: format!("Failed to send output: {}", e),
                        query: None,
                    });
                }
            }
            ProcessingResult::NoOutput => {
                // Record was filtered out or buffered
            }
            ProcessingResult::Completed => {
                // Query completed, could remove processor
                self.active_processors.remove(&query_id);
            }
        }

        self.record_count += 1;
        Ok(())
    }

    /// Start the execution engine
    pub async fn start(&mut self) -> Result<(), SqlError> {
        // The new architecture doesn't require a separate message processing loop
        // since processing is done synchronously in execute methods
        Ok(())
    }

    /// Get the message sender for external communication
    pub fn get_sender(&self) -> mpsc::Sender<ExecutionMessage> {
        self.message_sender.clone()
    }

    /// Convert internal values to stream record format
    fn convert_to_stream_record(
        &self,
        record: HashMap<String, InternalValue>,
        headers: HashMap<String, String>,
        timestamp: Option<i64>,
        offset: Option<i64>,
        partition: Option<i32>,
    ) -> Result<StreamRecord, SqlError> {
        let fields = record
            .into_iter()
            .map(|(k, v)| (k, self.internal_to_field_value(v)))
            .collect();

        Ok(StreamRecord {
            fields,
            headers,
            timestamp: timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
            offset: offset.unwrap_or(0),
            partition: partition.unwrap_or(0),
        })
    }

    /// Convert stream record back to internal values for output
    fn convert_to_internal_values(
        &self,
        record: StreamRecord,
    ) -> Result<HashMap<String, InternalValue>, SqlError> {
        Ok(record
            .fields
            .into_iter()
            .map(|(k, v)| (k, self.field_value_to_internal(v)))
            .collect())
    }

    /// Convert FieldValue to InternalValue
    fn field_value_to_internal(&self, value: FieldValue) -> InternalValue {
        match value {
            FieldValue::String(s) => InternalValue::String(s),
            FieldValue::Integer(i) => InternalValue::Integer(i),
            FieldValue::Float(f) => InternalValue::Number(f),
            FieldValue::Boolean(b) => InternalValue::Boolean(b),
            FieldValue::Null => InternalValue::Null,
            FieldValue::Date(d) => InternalValue::String(d.to_string()),
            FieldValue::Timestamp(t) => InternalValue::String(t.to_string()),
            FieldValue::Decimal(d) => InternalValue::Number(d.to_f64().unwrap_or(0.0)),
            FieldValue::Array(arr) => InternalValue::Array(
                arr.into_iter()
                    .map(|v| self.field_value_to_internal(v))
                    .collect(),
            ),
            FieldValue::Map(map) => InternalValue::Object(
                map.into_iter()
                    .map(|(k, v)| (k, self.field_value_to_internal(v)))
                    .collect(),
            ),
            FieldValue::Struct(fields) => InternalValue::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, self.field_value_to_internal(v)))
                    .collect(),
            ),
        }
    }

    /// Convert InternalValue to FieldValue
    fn internal_to_field_value(&self, value: InternalValue) -> FieldValue {
        match value {
            InternalValue::String(s) => FieldValue::String(s),
            InternalValue::Number(n) => {
                if n.fract() == 0.0 && n.abs() <= i64::MAX as f64 {
                    FieldValue::Integer(n as i64)
                } else {
                    FieldValue::Float(n)
                }
            }
            InternalValue::Integer(i) => FieldValue::Integer(i),
            InternalValue::Boolean(b) => FieldValue::Boolean(b),
            InternalValue::Null => FieldValue::Null,
            InternalValue::Array(arr) => FieldValue::Array(
                arr.into_iter()
                    .map(|v| self.internal_to_field_value(v))
                    .collect(),
            ),
            InternalValue::Object(obj) => FieldValue::Map(
                obj.into_iter()
                    .map(|(k, v)| (k, self.internal_to_field_value(v)))
                    .collect(),
            ),
        }
    }

    /// Flush any pending GROUP BY results
    pub async fn flush_group_by_results(&mut self) -> Result<(), SqlError> {
        // Collect results to avoid borrowing issues
        let mut all_results = Vec::new();
        for processor in self.active_processors.values_mut() {
            let results = processor.emit_group_by_results()?;
            all_results.extend(results);
        }

        // Process the results
        for result in all_results {
            let output = self.convert_to_internal_values(result)?;
            if let Err(e) = self.output_sender.send(output) {
                return Err(SqlError::ExecutionError {
                    message: format!("Failed to send GROUP BY output: {}", e),
                    query: None,
                });
            }
        }
        Ok(())
    }

    /// Get processing statistics
    pub fn get_stats(&self) -> ProcessingStats {
        ProcessingStats {
            total_records_processed: self.record_count,
            active_queries: self.active_processors.len(),
            processors_stats: self
                .active_processors
                .iter()
                .map(|(id, processor)| (id.clone(), processor.get_stats()))
                .collect(),
        }
    }
}

/// Overall processing statistics
#[derive(Debug)]
pub struct ProcessingStats {
    pub total_records_processed: u64,
    pub active_queries: usize,
    pub processors_stats:
        HashMap<String, crate::ferris::sql::execution::stream_processor::ProcessingStats>,
}
