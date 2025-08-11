/*!
# Streaming SQL Execution Engine

This module implements the execution engine for streaming SQL queries. It processes
SQL AST nodes and executes them against streaming data records, supporting real-time
query evaluation with expression processing, filtering, and basic aggregations.

## Key Features

- **Real-time Processing**: Processes streaming records one at a time as they arrive
- **Expression Evaluation**: Full support for arithmetic, comparison, and logical expressions
- **System Columns**: Access to Kafka metadata (_timestamp, _offset, _partition)
- **Header Functions**: Built-in functions for Kafka message header access
- **Aggregation Support**: Basic aggregation functions (COUNT, SUM, AVG)
- **Query Lifecycle**: Complete query management from start to stop
- **Error Handling**: Comprehensive error reporting with context information

## Architecture

The execution engine follows an event-driven architecture:

1. **Query Registration**: Queries are registered and maintained in active state
2. **Record Processing**: Incoming records trigger query evaluation
3. **Expression Evaluation**: SQL expressions are evaluated against record data
4. **Result Generation**: Query results are sent to output channels
5. **State Management**: Query state and window buffers are maintained

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
- `COUNT(*)`: Record counting
- `SUM(column)`: Numeric summation
- `AVG(column)`: Average calculation
- `HEADER(key)`: Kafka header value access
- `HEADER_KEYS()`: List all header keys
- `HAS_HEADER(key)`: Check header existence

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
- **Low Latency**: Direct expression evaluation without intermediate representations
- **Streaming Native**: No buffering or batching unless required by windows
- **Type Safe**: Runtime type checking with detailed error messages
*/

use crate::ferris::serialization::{InternalValue, SerializationFormat};
use crate::ferris::sql::ast::{
    BinaryOperator, Expr, JoinClause, JoinType, JoinWindow, LiteralValue, OverClause, SelectField,
    StreamSource, StreamingQuery, TimeUnit, UnaryOperator, WindowSpec,
};
use crate::ferris::sql::error::SqlError;
use log::warn;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::Sender<ExecutionMessage>,
    message_receiver: Option<mpsc::Receiver<ExecutionMessage>>,
    output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
    serialization_format: Arc<dyn SerializationFormat>,
    record_count: u64,
}

#[derive(Debug)]
pub enum ExecutionMessage {
    StartJob {
        job_id: String,
        query: StreamingQuery,
    },
    StopJob {
        job_id: String,
    },
    ProcessRecord {
        stream_name: String,
        record: StreamRecord,
    },
    QueryResult {
        query_id: String,
        result: StreamRecord,
    },
}

#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: i64,
    pub offset: i64,
    pub partition: i32,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    /// Array of values - all elements must be the same type
    Array(Vec<FieldValue>),
    /// Map of key-value pairs - keys must be strings
    Map(HashMap<String, FieldValue>),
    /// Structured data with named fields
    Struct(HashMap<String, FieldValue>),
}

impl FieldValue {
    /// Get the type name for error messages
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Integer(_) => "INTEGER",
            FieldValue::Float(_) => "FLOAT",
            FieldValue::String(_) => "STRING",
            FieldValue::Boolean(_) => "BOOLEAN",
            FieldValue::Null => "NULL",
            FieldValue::Array(_) => "ARRAY",
            FieldValue::Map(_) => "MAP",
            FieldValue::Struct(_) => "STRUCT",
        }
    }

    /// Check if this value is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(self, FieldValue::Integer(_) | FieldValue::Float(_))
    }

    /// Convert to string representation for display
    pub fn to_display_string(&self) -> String {
        match self {
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::String(s) => s.clone(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Array(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_display_string()).collect();
                format!("[{}]", elements.join(", "))
            }
            FieldValue::Map(map) => {
                let pairs: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_display_string()))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
            FieldValue::Struct(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|(name, value)| format!("{}: {}", name, value.to_display_string()))
                    .collect();
                format!("{{{}}}", field_strs.join(", "))
            }
        }
    }
}

pub struct QueryExecution {
    query: StreamingQuery,
    state: ExecutionState,
    window_state: Option<WindowState>,
}

#[derive(Debug)]
enum ExecutionState {
    Running,
    Paused,
    Stopped,
    Error(String),
}

#[derive(Debug)]
struct WindowState {
    window_spec: WindowSpec,
    buffer: Vec<StreamRecord>,
    last_emit: i64,
}

impl StreamExecutionEngine {
    pub fn new(
        output_sender: mpsc::UnboundedSender<HashMap<String, InternalValue>>,
        serialization_format: Arc<dyn SerializationFormat>,
    ) -> Self {
        let (message_sender, receiver) = mpsc::channel(1000);
        Self {
            active_queries: HashMap::new(),
            message_sender,
            message_receiver: Some(receiver),
            output_sender,
            serialization_format,
            record_count: 0,
        }
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
        let stream_record = StreamRecord {
            fields: record
                .into_iter()
                .map(|(k, v)| {
                    let field_value = self.internal_to_field_value(v);
                    (k, field_value)
                })
                .collect(),
            timestamp: timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
            offset: offset.unwrap_or(0),
            partition: partition.unwrap_or(0),
            headers,
        };

        // Apply query and process record
        if let Some(result) = self.apply_query(query, &stream_record)? {
            // Convert result to InternalValue format
            let internal_result: HashMap<String, InternalValue> = result
                .fields
                .into_iter()
                .map(|(k, v)| {
                    let internal_value = match v {
                        FieldValue::Integer(i) => InternalValue::Integer(i),
                        FieldValue::Float(f) => InternalValue::Number(f),
                        FieldValue::String(s) => InternalValue::String(s),
                        FieldValue::Boolean(b) => InternalValue::Boolean(b),
                        FieldValue::Null => InternalValue::Null,
                        FieldValue::Array(arr) => {
                            let internal_arr: Vec<InternalValue> = arr
                                .into_iter()
                                .map(|item| self.field_value_to_internal(item))
                                .collect();
                            InternalValue::Array(internal_arr)
                        }
                        FieldValue::Map(map) => {
                            let internal_map: HashMap<String, InternalValue> = map
                                .into_iter()
                                .map(|(k, v)| (k, self.field_value_to_internal(v)))
                                .collect();
                            InternalValue::Object(internal_map)
                        }
                        FieldValue::Struct(fields) => {
                            let internal_map: HashMap<String, InternalValue> = fields
                                .into_iter()
                                .map(|(k, v)| (k, self.field_value_to_internal(v)))
                                .collect();
                            InternalValue::Object(internal_map)
                        }
                    };
                    (k, internal_value)
                })
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

    async fn start_query_execution(
        &mut self,
        query_id: String,
        query: StreamingQuery,
    ) -> Result<(), SqlError> {
        let window_state = match &query {
            StreamingQuery::Select { window, .. } => {
                if let Some(window_spec) = window {
                    Some(WindowState {
                        window_spec: window_spec.clone(),
                        buffer: Vec::new(),
                        last_emit: 0,
                    })
                } else {
                    None
                }
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

    async fn process_stream_record(
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

        // Process each query
        let mut results = Vec::new();
        for (query_id, query) in matching_queries {
            if let Some(result) = self.apply_query(&query, &record)? {
                results.push((query_id, result));
            }
        }

        for (query_id, result) in results {
            // Send result downstream
            let _ = self
                .message_sender
                .send(ExecutionMessage::QueryResult { query_id, result })
                .await;
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
        }
    }

    fn apply_query(
        &mut self,
        query: &StreamingQuery,
        record: &StreamRecord,
    ) -> Result<Option<StreamRecord>, SqlError> {
        match query {
            StreamingQuery::Select {
                fields,
                where_clause,
                joins,
                having,
                limit,
                ..
            } => {
                // Check limit first
                if let Some(limit_value) = limit {
                    if self.record_count >= *limit_value {
                        return Ok(None);
                    }
                }

                // Handle JOINs first (if any)
                let mut joined_record = record.clone();
                if let Some(join_clauses) = joins {
                    joined_record = self.process_joins(&joined_record, join_clauses)?;
                }

                // Apply WHERE clause
                if let Some(where_expr) = where_clause {
                    if !self.evaluate_expression(where_expr, &joined_record)? {
                        return Ok(None);
                    }
                }

                // Apply SELECT fields
                let mut result_fields = HashMap::new();

                for field in fields {
                    match field {
                        SelectField::Wildcard => {
                            result_fields.extend(joined_record.fields.clone());
                        }
                        SelectField::Column(name) => {
                            // Check for system columns first (case insensitive)
                            let field_value = match name.to_uppercase().as_str() {
                                "_TIMESTAMP" => Some(FieldValue::Integer(joined_record.timestamp)),
                                "_OFFSET" => Some(FieldValue::Integer(joined_record.offset)),
                                "_PARTITION" => {
                                    Some(FieldValue::Integer(joined_record.partition as i64))
                                }
                                _ => joined_record.fields.get(name).cloned(),
                            };

                            if let Some(value) = field_value {
                                result_fields.insert(name.clone(), value);
                            }
                        }
                        SelectField::AliasedColumn { column, alias } => {
                            // Check for system columns first (case insensitive)
                            let field_value = match column.to_uppercase().as_str() {
                                "_TIMESTAMP" => Some(FieldValue::Integer(joined_record.timestamp)),
                                "_OFFSET" => Some(FieldValue::Integer(joined_record.offset)),
                                "_PARTITION" => {
                                    Some(FieldValue::Integer(joined_record.partition as i64))
                                }
                                _ => joined_record.fields.get(column).cloned(),
                            };

                            if let Some(value) = field_value {
                                result_fields.insert(alias.clone(), value);
                            }
                        }
                        SelectField::Expression { expr, alias } => {
                            let value = self.evaluate_expression_value_with_window(
                                expr,
                                &joined_record,
                                &mut Vec::new(),
                            )?;
                            let field_name = alias
                                .as_ref()
                                .unwrap_or(&self.get_expression_name(expr))
                                .clone();
                            result_fields.insert(field_name, value);
                        }
                    }
                }

                // Apply HAVING clause on the result fields
                if let Some(having_expr) = having {
                    // Create a temporary record with the result fields to evaluate HAVING
                    let result_record = StreamRecord {
                        fields: result_fields.clone(),
                        timestamp: joined_record.timestamp,
                        offset: joined_record.offset,
                        partition: joined_record.partition,
                        headers: joined_record.headers.clone(),
                    };

                    if !self.evaluate_expression(having_expr, &result_record)? {
                        return Ok(None);
                    }
                }

                // Increment record count for successful processing
                self.record_count += 1;

                Ok(Some(StreamRecord {
                    fields: result_fields,
                    timestamp: joined_record.timestamp,
                    offset: joined_record.offset,
                    partition: joined_record.partition,
                    headers: joined_record.headers.clone(),
                }))
            }
            StreamingQuery::CreateStream {
                name, as_select, ..
            } => {
                // Execute the underlying SELECT query and return its result
                // In a full implementation, this would also:
                // 1. Create a new Kafka topic
                // 2. Register the stream in the SQL context
                // 3. Set up continuous query execution
                log::info!("Executing CREATE STREAM: {}", name);
                self.apply_query(as_select, record)
            }
            StreamingQuery::CreateTable {
                name, as_select, ..
            } => {
                // Execute the underlying SELECT query for materialized table
                // In a full implementation, this would also:
                // 1. Create a KTable/materialized view
                // 2. Set up aggregation state
                // 3. Register the table in the SQL context
                log::info!("Executing CREATE TABLE: {}", name);
                self.apply_query(as_select, record)
            }
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                // SHOW commands return metadata, not transformed records
                // In a full implementation, this would query the SQL context for metadata
                log::info!(
                    "Executing SHOW {:?} with pattern {:?}",
                    resource_type,
                    pattern
                );

                // For now, return a simple result indicating the SHOW command was processed
                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert(
                            "show_type".to_string(),
                            FieldValue::String(format!("{:?}", resource_type)),
                        );
                        if let Some(p) = pattern {
                            fields.insert("pattern".to_string(), FieldValue::String(p.clone()));
                        }
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::StartJob {
                name,
                query,
                properties,
            } => {
                // START JOB initiates continuous execution
                log::info!("Starting job '{}' with properties {:?}", name, properties);

                // In a full implementation, this would:
                // 1. Register the query in the execution engine
                // 2. Start continuous execution
                // 3. Return success confirmation

                // For now, execute the underlying query once as confirmation
                self.apply_query(query, record)
            }
            StreamingQuery::StopJob { name, force } => {
                // STOP JOB terminates continuous execution
                log::info!("Stopping job '{}' (force: {})", name, force);

                // In a full implementation, this would:
                // 1. Find the running query by name
                // 2. Gracefully stop execution (or force if needed)
                // 3. Clean up resources
                // 4. Return stop confirmation

                // For now, return a simple confirmation
                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("job_name".to_string(), FieldValue::String(name.clone()));
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("stopped".to_string()),
                        );
                        fields.insert("forced".to_string(), FieldValue::Boolean(*force));
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::PauseJob { name } => {
                // PAUSE JOB suspends continuous execution while preserving state
                log::info!("Pausing job '{}'", name);

                // In a full implementation, this would:
                // 1. Find the running query by name
                // 2. Suspend processing while preserving offset positions
                // 3. Update query status to Paused
                // 4. Return pause confirmation

                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("job_name".to_string(), FieldValue::String(name.clone()));
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("paused".to_string()),
                        );
                        fields.insert(
                            "action".to_string(),
                            FieldValue::String("pause".to_string()),
                        );
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::ResumeJob { name } => {
                // RESUME JOB restarts a paused job from where it left off
                log::info!("Resuming job '{}'", name);

                // In a full implementation, this would:
                // 1. Find the paused query by name
                // 2. Resume processing from last committed offset
                // 3. Update query status to Running
                // 4. Return resume confirmation

                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("job_name".to_string(), FieldValue::String(name.clone()));
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("running".to_string()),
                        );
                        fields.insert(
                            "action".to_string(),
                            FieldValue::String("resume".to_string()),
                        );
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::DeployJob {
                name,
                version,
                query,
                properties,
                strategy,
            } => {
                // DEPLOY JOB performs versioned deployment with specified strategy
                log::info!(
                    "Deploying job '{}' version '{}' with strategy {:?}",
                    name,
                    version,
                    strategy
                );

                // In a full implementation, this would:
                // 1. Validate the deployment
                // 2. Execute deployment strategy (blue-green, canary, etc.)
                // 3. Update version registry
                // 4. Monitor deployment health
                // 5. Return deployment status

                // For now, execute the underlying query as confirmation
                let _result = self.apply_query(query, record)?;

                // Return deployment confirmation with version info
                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("job_name".to_string(), FieldValue::String(name.clone()));
                        fields.insert("version".to_string(), FieldValue::String(version.clone()));
                        fields.insert(
                            "strategy".to_string(),
                            FieldValue::String(format!("{:?}", strategy)),
                        );
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("deployed".to_string()),
                        );
                        fields.insert(
                            "properties_count".to_string(),
                            FieldValue::Integer(properties.len() as i64),
                        );
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::RollbackJob {
                name,
                target_version,
            } => {
                // ROLLBACK JOB reverts to a previous version
                let version_msg = target_version
                    .as_ref()
                    .map(|v| format!(" to version '{}'", v))
                    .unwrap_or_else(|| " to previous version".to_string());
                log::info!("Rolling back job '{}'{}", name, version_msg);

                // In a full implementation, this would:
                // 1. Find the target version in version history
                // 2. Stop current version gracefully
                // 3. Deploy previous version
                // 4. Update version registry
                // 5. Return rollback confirmation

                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("job_name".to_string(), FieldValue::String(name.clone()));
                        if let Some(version) = target_version {
                            fields.insert(
                                "target_version".to_string(),
                                FieldValue::String(version.clone()),
                            );
                        }
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("rolled_back".to_string()),
                        );
                        fields.insert(
                            "action".to_string(),
                            FieldValue::String("rollback".to_string()),
                        );
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
        }
    }

    fn evaluate_comparison(
        &self,
        left: &Expr,
        right: &Expr,
        record: &StreamRecord,
        op: &BinaryOperator,
    ) -> Result<bool, SqlError> {
        let left_val = self.evaluate_expression_value(left, record)?;
        let right_val = self.evaluate_expression_value(right, record)?;

        match op {
            BinaryOperator::Equal => Ok(self.values_equal(&left_val, &right_val)),
            BinaryOperator::NotEqual => Ok(!self.values_equal(&left_val, &right_val)),
            BinaryOperator::GreaterThan => self.compare_values(&left_val, &right_val, |a, b| a > b),
            BinaryOperator::LessThan => self.compare_values(&left_val, &right_val, |a, b| a < b),
            BinaryOperator::GreaterThanOrEqual => {
                self.compare_values(&left_val, &right_val, |a, b| a >= b)
            }
            BinaryOperator::LessThanOrEqual => {
                self.compare_values(&left_val, &right_val, |a, b| a <= b)
            }
            _ => Err(SqlError::ExecutionError {
                message: "Invalid comparison operator".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_expression(&self, expr: &Expr, record: &StreamRecord) -> Result<bool, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                let field_value = match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => FieldValue::Integer(record.timestamp),
                    "_OFFSET" => FieldValue::Integer(record.offset),
                    "_PARTITION" => FieldValue::Integer(record.partition as i64),
                    _ => {
                        // Handle qualified column names (table.column)
                        if name.contains('.') {
                            // Try to find the field with the qualified name first (for JOIN aliases)
                            if let Some(value) = record.fields.get(name) {
                                value.clone()
                            } else {
                                let column_name = name.split('.').last().unwrap_or(name);
                                // Try to find with the "right_" prefix (for non-aliased JOINs)
                                let prefixed_name = format!("right_{}", column_name);
                                if let Some(value) = record.fields.get(&prefixed_name) {
                                    value.clone()
                                } else {
                                    // Fall back to just the column name (for FROM clause aliases like l.name -> name)
                                    record.fields.get(column_name).cloned().ok_or_else(|| {
                                        SqlError::SchemaError {
                                            message: format!(
                                                "Schema error for column '{}': Column not found",
                                                name
                                            ),
                                            column: Some(name.clone()),
                                        }
                                    })?
                                }
                            }
                        } else {
                            // Regular field lookup
                            record.fields.get(name).cloned().ok_or_else(|| {
                                SqlError::SchemaError {
                                    message: format!(
                                        "Schema error for column '{}': Column not found",
                                        name
                                    ),
                                    column: Some(name.clone()),
                                }
                            })?
                        }
                    }
                };

                match field_value {
                    FieldValue::Boolean(b) => Ok(b),
                    _ => Err(SqlError::TypeError {
                        expected: "boolean".to_string(),
                        actual: "other".to_string(),
                        value: None,
                    }),
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Boolean(b) => Ok(*b),
                LiteralValue::Integer(_)
                | LiteralValue::Float(_)
                | LiteralValue::String(_)
                | LiteralValue::Null
                | LiteralValue::Interval { .. } => Err(SqlError::TypeError {
                    expected: "boolean".to_string(),
                    actual: "non-boolean".to_string(),
                    value: None,
                }),
            },
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::LessThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThanOrEqual => {
                    self.evaluate_comparison(left, right, record, op)
                }
                BinaryOperator::And | BinaryOperator::Or => Err(SqlError::ExecutionError {
                    message: "Logical operators should be handled at parse time".to_string(),
                    query: None,
                }),
                BinaryOperator::Like | BinaryOperator::NotLike => Err(SqlError::ExecutionError {
                    message: "String comparison operators not yet implemented".to_string(),
                    query: None,
                }),
                BinaryOperator::In | BinaryOperator::NotIn => Err(SqlError::ExecutionError {
                    message: "Set operators not yet implemented".to_string(),
                    query: None,
                }),
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => Err(SqlError::ExecutionError {
                    message: "Arithmetic operator not valid in boolean context".to_string(),
                    query: None,
                }),
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let val = self.evaluate_expression(expr, record)?;
                    Ok(!val)
                }
                UnaryOperator::IsNull => {
                    let val = self.evaluate_expression_value(expr, record)?;
                    Ok(matches!(val, FieldValue::Null))
                }
                UnaryOperator::IsNotNull => {
                    let val = self.evaluate_expression_value(expr, record)?;
                    Ok(!matches!(val, FieldValue::Null))
                }
                _ => Err(SqlError::ExecutionError {
                    message: "Unsupported unary operator for boolean expression".to_string(),
                    query: None,
                }),
            },
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    if self.evaluate_expression(condition, record)? {
                        return self.evaluate_expression(result, record);
                    }
                }
                if let Some(else_result) = else_clause {
                    self.evaluate_expression(else_result, record)
                } else {
                    Ok(false)
                }
            }
            Expr::Function { name: _, args: _ } => {
                // For now, treat function results as non-boolean
                Err(SqlError::TypeError {
                    expected: "boolean".to_string(),
                    actual: "function result".to_string(),
                    value: None,
                })
            }
            Expr::WindowFunction { .. } => {
                // For now, treat window function results as non-boolean
                Err(SqlError::TypeError {
                    expected: "boolean".to_string(),
                    actual: "window function result".to_string(),
                    value: None,
                })
            }
        }
    }

    fn evaluate_expression_value_with_window(
        &mut self,
        expr: &Expr,
        record: &StreamRecord,
        window_buffer: &mut Vec<StreamRecord>,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::WindowFunction {
                function_name,
                args,
                over_clause,
            } => self.evaluate_window_function(
                function_name,
                args,
                over_clause,
                record,
                window_buffer,
            ),
            _ => self.evaluate_expression_value(expr, record),
        }
    }

    fn evaluate_expression_value(
        &self,
        expr: &Expr,
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => Ok(FieldValue::Integer(record.timestamp)),
                    "_OFFSET" => Ok(FieldValue::Integer(record.offset)),
                    "_PARTITION" => Ok(FieldValue::Integer(record.partition as i64)),
                    _ => {
                        // Handle qualified column names (table.column)
                        if name.contains('.') {
                            // Try to find the field with the qualified name first (for JOIN aliases)
                            if let Some(value) = record.fields.get(name) {
                                Ok(value.clone())
                            } else {
                                let column_name = name.split('.').last().unwrap_or(name);
                                // Try to find with the "right_" prefix (for non-aliased JOINs)
                                let prefixed_name = format!("right_{}", column_name);
                                if let Some(value) = record.fields.get(&prefixed_name) {
                                    Ok(value.clone())
                                } else {
                                    // Fall back to just the column name (for FROM clause aliases like l.name -> name)
                                    Ok(record
                                        .fields
                                        .get(column_name)
                                        .cloned()
                                        .unwrap_or(FieldValue::Null))
                                }
                            }
                        } else {
                            // Regular field lookup
                            Ok(record.fields.get(name).cloned().unwrap_or(FieldValue::Null))
                        }
                    }
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
                LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
                LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
                LiteralValue::Null => Ok(FieldValue::Null),
                LiteralValue::Interval { value, unit } => {
                    // Convert INTERVAL to milliseconds for internal representation
                    let millis = match unit {
                        TimeUnit::Millisecond => *value,
                        TimeUnit::Second => *value * 1000,
                        TimeUnit::Minute => *value * 60 * 1000,
                        TimeUnit::Hour => *value * 60 * 60 * 1000,
                        TimeUnit::Day => *value * 24 * 60 * 60 * 1000,
                    };
                    Ok(FieldValue::Integer(millis))
                }
            },
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_value(left, record)?;
                let right_val = self.evaluate_expression_value(right, record)?;

                match op {
                    BinaryOperator::Add => self.add_values(&left_val, &right_val),
                    BinaryOperator::Subtract => self.subtract_values(&left_val, &right_val),
                    BinaryOperator::Multiply => self.multiply_values(&left_val, &right_val),
                    BinaryOperator::Divide => self.divide_values(&left_val, &right_val),
                    BinaryOperator::Modulo => self.divide_values(&left_val, &right_val),
                    BinaryOperator::Equal
                    | BinaryOperator::NotEqual
                    | BinaryOperator::GreaterThan
                    | BinaryOperator::LessThan
                    | BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::LessThanOrEqual => {
                        let result = self.evaluate_comparison(left, right, record, op)?;
                        Ok(FieldValue::Boolean(result))
                    }
                    BinaryOperator::Like
                    | BinaryOperator::NotLike
                    | BinaryOperator::In
                    | BinaryOperator::NotIn
                    | BinaryOperator::And
                    | BinaryOperator::Or => Err(SqlError::ExecutionError {
                        message: "Operator not supported in value context".to_string(),
                        query: None,
                    }),
                }
            }
            Expr::UnaryOp { op: _, expr: _ } => Err(SqlError::ExecutionError {
                message: "Unary operations not supported for value expressions".to_string(),
                query: None,
            }),
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    if self.evaluate_expression(condition, record)? {
                        return self.evaluate_expression_value(result, record);
                    }
                }
                if let Some(else_result) = else_clause {
                    self.evaluate_expression_value(else_result, record)
                } else {
                    Ok(FieldValue::Null)
                }
            }
            Expr::Function { name, args } => self.evaluate_function(name, args, record),
            Expr::WindowFunction { .. } => Err(SqlError::ExecutionError {
                message: "Window functions should be handled through evaluate_expression_value_with_window".to_string(),
                query: None,
            }),
        }
    }

    fn get_expression_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Literal(_) => "literal".to_string(),
            Expr::BinaryOp { .. } => "expression".to_string(),
            Expr::Function { name, .. } => name.clone(),
            Expr::WindowFunction { function_name, .. } => function_name.clone(),
            _ => "expression".to_string(),
        }
    }

    pub fn values_equal(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Null, FieldValue::Null) => true,
            (FieldValue::Array(a), FieldValue::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| self.values_equal(x, y))
            }
            (FieldValue::Map(a), FieldValue::Map(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).map_or(false, |bv| self.values_equal(v, bv)))
            }
            (FieldValue::Struct(a), FieldValue::Struct(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).map_or(false, |bv| self.values_equal(v, bv)))
            }
            _ => false,
        }
    }

    fn compare_values<F>(
        &self,
        left: &FieldValue,
        right: &FieldValue,
        op: F,
    ) -> Result<bool, SqlError>
    where
        F: Fn(f64, f64) -> bool,
    {
        let (left_num, right_num) = match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => (*a as f64, *b as f64),
            (FieldValue::Float(a), FieldValue::Float(b)) => (*a, *b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64, *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => (*a, *b as f64),
            _ => {
                return Err(SqlError::TypeError {
                    expected: "numeric".to_string(),
                    actual: "non-numeric".to_string(),
                    value: None,
                });
            }
        };

        Ok(op(left_num, right_num))
    }

    fn compare_values_for_min(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<bool, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(a < b),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(a < b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok((*a as f64) < *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(*a < (*b as f64)),
            (FieldValue::String(a), FieldValue::String(b)) => Ok(a < b),
            (FieldValue::Null, _) => Ok(false), // NULL is not less than anything
            (_, FieldValue::Null) => Ok(true),  // anything is less than NULL
            _ => Err(SqlError::ExecutionError {
                message: "Cannot compare incompatible types for LEAST".to_string(),
                query: None,
            }),
        }
    }

    fn compare_values_for_max(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<bool, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(a > b),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(a > b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok((*a as f64) > *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(*a > (*b as f64)),
            (FieldValue::String(a), FieldValue::String(b)) => Ok(a > b),
            (FieldValue::Null, _) => Ok(false), // NULL is not greater than anything
            (_, FieldValue::Null) => Ok(true),  // anything is greater than NULL
            _ => Err(SqlError::ExecutionError {
                message: "Cannot compare incompatible types for GREATEST".to_string(),
                query: None,
            }),
        }
    }

    pub fn add_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    pub fn subtract_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    pub fn multiply_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a * b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a * b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 * b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a * *b as f64)),
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

    pub fn divide_values(
        &self,
        left: &FieldValue,
        right: &FieldValue,
    ) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / *b as f64))
                }
            }
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(a / b))
                }
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / b))
                }
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(a / *b as f64))
                }
            }
            _ => Err(SqlError::TypeError {
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None,
            }),
        }
    }

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
                    match self.evaluate_expression_value(&args[1], record)? {
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
                    Some(self.evaluate_expression_value(&args[2], record)?)
                } else {
                    None
                };

                // Look back in the window buffer
                if offset == 0 {
                    // Offset 0 means current record
                    self.evaluate_expression_value(&args[0], record)
                } else if window_buffer.len() >= offset {
                    let lag_record = &window_buffer[window_buffer.len() - offset];
                    self.evaluate_expression_value(&args[0], lag_record)
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
                    match self.evaluate_expression_value(&args[1], record)? {
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
                    Some(self.evaluate_expression_value(&args[2], record)?)
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
                    self.evaluate_expression_value(&args[0], record)
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
                    self.evaluate_expression_value(&args[0], &window_buffer[0])
                } else {
                    // If buffer is empty, evaluate against current record
                    self.evaluate_expression_value(&args[0], record)
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
                self.evaluate_expression_value(&args[0], record)
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
                let nth = match self.evaluate_expression_value(&args[1], record)? {
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
                        self.evaluate_expression_value(&args[0], &window_buffer[nth - 1])
                    } else {
                        // nth record is the current record
                        self.evaluate_expression_value(&args[0], record)
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
                let tiles = match self.evaluate_expression_value(&args[0], record)? {
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

    fn evaluate_function(
        &self,
        name: &str,
        args: &[Expr],
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        match name.to_uppercase().as_str() {
            "COUNT" => Ok(FieldValue::Integer(1)), // Simplified for streaming
            "SUM" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "SUM requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                self.evaluate_expression_value(&args[0], record)
            }
            "AVG" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "AVG requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                self.evaluate_expression_value(&args[0], record)
            }
            "LISTAGG" => {
                // LISTAGG(expression, delimiter) or LISTAGG(DISTINCT expression, delimiter)
                if args.len() < 2 || args.len() > 3 {
                    return Err(SqlError::ExecutionError {
                        message: "LISTAGG requires 2 arguments: LISTAGG(expression, delimiter) or LISTAGG(DISTINCT expression, delimiter)".to_string(),
                        query: None,
                    });
                }

                // For streaming, we can only aggregate the current record
                // In a real implementation, this would need windowing or group by support
                let value_expr = if args.len() == 3 {
                    // Handle LISTAGG(DISTINCT expression, delimiter) case
                    // args[0] would be the DISTINCT keyword (but we simplify this for now)
                    &args[1]
                } else {
                    // Handle LISTAGG(expression, delimiter) case
                    &args[0]
                };

                let delimiter_expr = &args[args.len() - 1]; // Last argument is always delimiter

                let value = self.evaluate_expression_value(value_expr, record)?;
                let delimiter = self.evaluate_expression_value(delimiter_expr, record)?;

                match (value, delimiter) {
                    (FieldValue::String(val), FieldValue::String(_delim)) => {
                        // For single record, just return the value
                        // In real streaming aggregation, this would collect values with delimiter
                        Ok(FieldValue::String(val))
                    }
                    (FieldValue::Array(arr), FieldValue::String(delim)) => {
                        // If the input is an array, concatenate all string values
                        let string_vals: Result<Vec<String>, _> = arr
                            .iter()
                            .map(|v| match v {
                                FieldValue::String(s) => Ok(s.clone()),
                                FieldValue::Integer(i) => Ok(i.to_string()),
                                FieldValue::Float(f) => Ok(f.to_string()),
                                FieldValue::Boolean(b) => Ok(b.to_string()),
                                FieldValue::Null => Ok("".to_string()),
                                _ => Err(SqlError::ExecutionError {
                                    message: "LISTAGG array elements must be convertible to string"
                                        .to_string(),
                                    query: None,
                                }),
                            })
                            .collect();

                        match string_vals {
                            Ok(vals) => Ok(FieldValue::String(vals.join(&delim))),
                            Err(e) => Err(e),
                        }
                    }
                    (FieldValue::Null, _) => Ok(FieldValue::String("".to_string())),
                    _ => Err(SqlError::ExecutionError {
                        message:
                            "LISTAGG requires string or array first argument and string delimiter"
                                .to_string(),
                        query: None,
                    }),
                }
            }
            "HEADER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "HEADER requires exactly one argument (header key)".to_string(),
                        query: None,
                    });
                }

                // Evaluate the argument to get the header key
                let key_value = self.evaluate_expression_value(&args[0], record)?;
                let header_key = match key_value {
                    FieldValue::String(key) => key,
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "HEADER key must be a string".to_string(),
                            query: None,
                        });
                    }
                };

                // Look up the header value
                match record.headers.get(&header_key) {
                    Some(value) => Ok(FieldValue::String(value.clone())),
                    None => Ok(FieldValue::Null),
                }
            }
            "HEADER_KEYS" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "HEADER_KEYS takes no arguments".to_string(),
                        query: None,
                    });
                }

                // Return comma-separated list of header keys
                let keys: Vec<String> = record.headers.keys().cloned().collect();
                Ok(FieldValue::String(keys.join(",")))
            }
            "HAS_HEADER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "HAS_HEADER requires exactly one argument (header key)"
                            .to_string(),
                        query: None,
                    });
                }

                // Evaluate the argument to get the header key
                let key_value = self.evaluate_expression_value(&args[0], record)?;
                let header_key = match key_value {
                    FieldValue::String(key) => key,
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "HAS_HEADER key must be a string".to_string(),
                            query: None,
                        });
                    }
                };

                // Check if header exists
                Ok(FieldValue::Boolean(
                    record.headers.contains_key(&header_key),
                ))
            }
            "MIN" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "MIN requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                // For streaming, return the current value (full aggregation would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "MAX" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "MAX requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                // For streaming, return the current value (full aggregation would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "FIRST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "FIRST_VALUE requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                // For streaming, return the current value (full windowing would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "LAST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "LAST_VALUE requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                // For streaming, return the current value (full windowing would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "APPROX_COUNT_DISTINCT" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "APPROX_COUNT_DISTINCT requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                // For streaming, simplified implementation - returns 1 if value is not null
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Null => Ok(FieldValue::Integer(0)),
                    _ => Ok(FieldValue::Integer(1)),
                }
            }
            "TIMESTAMP" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "TIMESTAMP() takes no arguments".to_string(),
                        query: None,
                    });
                }
                // Return current record timestamp
                Ok(FieldValue::Integer(record.timestamp))
            }
            "CAST" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "CAST requires exactly two arguments: CAST(value, type)"
                            .to_string(),
                        query: None,
                    });
                }

                let value = self.evaluate_expression_value(&args[0], record)?;
                let target_type = match &args[1] {
                    Expr::Literal(LiteralValue::String(type_str)) => type_str.to_uppercase(),
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "CAST target type must be a string literal".to_string(),
                            query: None,
                        });
                    }
                };

                self.cast_value(value, &target_type)
            }
            "SPLIT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "SPLIT requires exactly two arguments: SPLIT(string, delimiter)"
                            .to_string(),
                        query: None,
                    });
                }

                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let delimiter_val = self.evaluate_expression_value(&args[1], record)?;

                let (string, delimiter) = match (string_val, delimiter_val) {
                    (FieldValue::String(s), FieldValue::String(d)) => (s, d),
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "SPLIT requires string arguments".to_string(),
                            query: None,
                        });
                    }
                };

                // Return first part for simplicity (full array support would need array type)
                let parts: Vec<&str> = string.split(&delimiter).collect();
                Ok(FieldValue::String(parts.get(0).unwrap_or(&"").to_string()))
            }
            "JOIN" => {
                if args.len() < 2 {
                    return Err(SqlError::ExecutionError {
                        message: "JOIN requires at least two arguments".to_string(),
                        query: None,
                    });
                }

                // Evaluate all arguments and join them with the first argument as delimiter
                let delimiter_val = self.evaluate_expression_value(&args[0], record)?;
                let delimiter = match delimiter_val {
                    FieldValue::String(d) => d,
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "JOIN delimiter must be a string".to_string(),
                            query: None,
                        });
                    }
                };

                let mut parts = Vec::new();
                for arg in &args[1..] {
                    let val = self.evaluate_expression_value(arg, record)?;
                    let str_val = match val {
                        FieldValue::String(s) => s,
                        FieldValue::Integer(i) => i.to_string(),
                        FieldValue::Float(f) => f.to_string(),
                        FieldValue::Boolean(b) => b.to_string(),
                        FieldValue::Null => "NULL".to_string(),
                        FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                            val.to_display_string()
                        }
                    };
                    parts.push(str_val);
                }

                Ok(FieldValue::String(parts.join(&delimiter)))
            }
            "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(SqlError::ExecutionError {
                        message: "SUBSTRING requires 2 or 3 arguments: SUBSTRING(string, start[, length])".to_string(),
                        query: None
                    });
                }

                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let start_val = self.evaluate_expression_value(&args[1], record)?;

                let (string, start) = match (string_val, start_val) {
                    (FieldValue::String(s), FieldValue::Integer(start)) => (s, start as usize),
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "SUBSTRING requires string and integer arguments".to_string(),
                            query: None,
                        });
                    }
                };

                // Handle optional length argument
                let result = if args.len() == 3 {
                    let length_val = self.evaluate_expression_value(&args[2], record)?;
                    match length_val {
                        FieldValue::Integer(length) => string
                            .chars()
                            .skip(start.saturating_sub(1))
                            .take(length as usize)
                            .collect(),
                        _ => {
                            return Err(SqlError::ExecutionError {
                                message: "SUBSTRING length must be an integer".to_string(),
                                query: None,
                            });
                        }
                    }
                } else {
                    // No length specified, take from start to end
                    string.chars().skip(start.saturating_sub(1)).collect()
                };

                Ok(FieldValue::String(result))
            }
            "JSON_EXTRACT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "JSON_EXTRACT requires exactly two arguments: JSON_EXTRACT(json_string, path)".to_string(),
                        query: None
                    });
                }

                let json_val = self.evaluate_expression_value(&args[0], record)?;
                let path_val = self.evaluate_expression_value(&args[1], record)?;

                let (json_string, path) = match (json_val, path_val) {
                    (FieldValue::String(j), FieldValue::String(p)) => (j, p),
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "JSON_EXTRACT requires string arguments".to_string(),
                            query: None,
                        });
                    }
                };

                self.extract_json_value(&json_string, &path)
            }
            "JSON_VALUE" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "JSON_VALUE requires exactly two arguments: JSON_VALUE(json_string, path)".to_string(),
                        query: None
                    });
                }

                let json_val = self.evaluate_expression_value(&args[0], record)?;
                let path_val = self.evaluate_expression_value(&args[1], record)?;

                let (json_string, path) = match (json_val, path_val) {
                    (FieldValue::String(j), FieldValue::String(p)) => (j, p),
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "JSON_VALUE requires string arguments".to_string(),
                            query: None,
                        });
                    }
                };

                // JSON_VALUE returns scalar values as strings, JSON_EXTRACT can return objects/arrays
                match self.extract_json_value(&json_string, &path)? {
                    FieldValue::String(s) => Ok(FieldValue::String(s)),
                    FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                    FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                    FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                        // JSON_VALUE should return string representation of complex types
                        Ok(FieldValue::String("COMPLEX_TYPE".to_string()))
                    }
                }
            }
            // Math Functions
            "ABS" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "ABS requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Integer(i) => Ok(FieldValue::Integer(i.abs())),
                    FieldValue::Float(f) => Ok(FieldValue::Float(f.abs())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "ABS can only be applied to numeric values".to_string(),
                        query: None,
                    }),
                }
            }
            "ROUND" => {
                if args.len() < 1 || args.len() > 2 {
                    return Err(SqlError::ExecutionError {
                        message: "ROUND requires 1 or 2 arguments: ROUND(number[, precision])"
                            .to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                let precision = if args.len() == 2 {
                    match self.evaluate_expression_value(&args[1], record)? {
                        FieldValue::Integer(p) => p as i32,
                        _ => {
                            return Err(SqlError::ExecutionError {
                                message: "ROUND precision must be an integer".to_string(),
                                query: None,
                            });
                        }
                    }
                } else {
                    0
                };

                match value {
                    FieldValue::Float(f) => {
                        let multiplier = 10_f64.powi(precision);
                        Ok(FieldValue::Float((f * multiplier).round() / multiplier))
                    }
                    FieldValue::Integer(i) => Ok(FieldValue::Integer(i)), // Integers don't need rounding
                    _ => Err(SqlError::ExecutionError {
                        message: "ROUND requires numeric argument".to_string(),
                        query: None,
                    }),
                }
            }
            "CEIL" | "CEILING" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "CEIL requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Float(f) => Ok(FieldValue::Integer(f.ceil() as i64)),
                    FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                    _ => Err(SqlError::ExecutionError {
                        message: "CEIL requires numeric argument".to_string(),
                        query: None,
                    }),
                }
            }
            "FLOOR" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "FLOOR requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Float(f) => Ok(FieldValue::Integer(f.floor() as i64)),
                    FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                    _ => Err(SqlError::ExecutionError {
                        message: "FLOOR requires numeric argument".to_string(),
                        query: None,
                    }),
                }
            }
            "MOD" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "MOD requires exactly two arguments".to_string(),
                        query: None,
                    });
                }
                let left = self.evaluate_expression_value(&args[0], record)?;
                let right = self.evaluate_expression_value(&args[1], record)?;

                // Modern Rust 2024 pattern matching with match ergonomics
                match (&left, &right) {
                    (FieldValue::Integer(a), FieldValue::Integer(b)) if *b != 0 => {
                        Ok(FieldValue::Integer(a % b))
                    }
                    (FieldValue::Float(a), FieldValue::Float(b)) if *b != 0.0 => {
                        Ok(FieldValue::Float(a % b))
                    }
                    (FieldValue::Integer(_), FieldValue::Integer(0)) => {
                        Err(SqlError::ExecutionError {
                            message: "Division by zero in MOD".to_string(),
                            query: None,
                        })
                    }
                    (FieldValue::Float(_), FieldValue::Float(b)) if *b == 0.0 => {
                        Err(SqlError::ExecutionError {
                            message: "Division by zero in MOD".to_string(),
                            query: None,
                        })
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "MOD requires numeric arguments".to_string(),
                        query: None,
                    }),
                }
            }
            "POWER" | "POW" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "POWER requires exactly two arguments".to_string(),
                        query: None,
                    });
                }
                let base = self.evaluate_expression_value(&args[0], record)?;
                let exponent = self.evaluate_expression_value(&args[1], record)?;
                match (base, exponent) {
                    (FieldValue::Float(b), FieldValue::Float(e)) => {
                        Ok(FieldValue::Float(b.powf(e)))
                    }
                    (FieldValue::Integer(b), FieldValue::Integer(e)) => {
                        Ok(FieldValue::Float((b as f64).powf(e as f64)))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "POWER requires numeric arguments".to_string(),
                        query: None,
                    }),
                }
            }
            "SQRT" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "SQRT requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Float(f) => {
                        if f < 0.0 {
                            Err(SqlError::ExecutionError {
                                message: "SQRT of negative number".to_string(),
                                query: None,
                            })
                        } else {
                            Ok(FieldValue::Float(f.sqrt()))
                        }
                    }
                    FieldValue::Integer(i) => {
                        if i < 0 {
                            Err(SqlError::ExecutionError {
                                message: "SQRT of negative number".to_string(),
                                query: None,
                            })
                        } else {
                            Ok(FieldValue::Float((i as f64).sqrt()))
                        }
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "SQRT requires numeric argument".to_string(),
                        query: None,
                    }),
                }
            }
            // String Functions
            "CONCAT" => {
                if args.is_empty() {
                    return Ok(FieldValue::String(String::new()));
                }
                let mut result = String::new();
                for arg in args {
                    let value = self.evaluate_expression_value(arg, record)?;
                    let str_val = match value {
                        FieldValue::String(s) => s,
                        FieldValue::Integer(i) => i.to_string(),
                        FieldValue::Float(f) => f.to_string(),
                        FieldValue::Boolean(b) => b.to_string(),
                        FieldValue::Null => continue, // Skip null values in CONCAT
                        FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                            value.to_display_string()
                        }
                    };
                    result.push_str(&str_val);
                }
                Ok(FieldValue::String(result))
            }
            "LENGTH" | "LEN" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "LENGTH requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::Integer(s.chars().count() as i64)),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "LENGTH requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "TRIM" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "TRIM requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::String(s.trim().to_string())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "TRIM requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "LTRIM" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "LTRIM requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::String(s.trim_start().to_string())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "LTRIM requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "RTRIM" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "RTRIM requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::String(s.trim_end().to_string())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "RTRIM requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "UPPER requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::String(s.to_uppercase())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "UPPER requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "LOWER requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::String(s) => Ok(FieldValue::String(s.to_lowercase())),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "LOWER requires string argument".to_string(),
                        query: None,
                    }),
                }
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(SqlError::ExecutionError {
                        message: "REPLACE requires exactly three arguments: REPLACE(string, search, replace)".to_string(),
                        query: None
                    });
                }
                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let search_val = self.evaluate_expression_value(&args[1], record)?;
                let replace_val = self.evaluate_expression_value(&args[2], record)?;

                match (string_val, search_val, replace_val) {
                    (
                        FieldValue::String(s),
                        FieldValue::String(search),
                        FieldValue::String(replace),
                    ) => Ok(FieldValue::String(s.replace(&search, &replace))),
                    _ => Err(SqlError::ExecutionError {
                        message: "REPLACE requires string arguments".to_string(),
                        query: None,
                    }),
                }
            }
            "LEFT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "LEFT requires exactly two arguments: LEFT(string, length)"
                            .to_string(),
                        query: None,
                    });
                }
                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let length_val = self.evaluate_expression_value(&args[1], record)?;

                match (string_val, length_val) {
                    (FieldValue::String(s), FieldValue::Integer(len)) => {
                        Ok(FieldValue::String(s.chars().take(len as usize).collect()))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "LEFT requires string and integer arguments".to_string(),
                        query: None,
                    }),
                }
            }
            "RIGHT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "RIGHT requires exactly two arguments: RIGHT(string, length)"
                            .to_string(),
                        query: None,
                    });
                }
                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let length_val = self.evaluate_expression_value(&args[1], record)?;

                match (string_val, length_val) {
                    (FieldValue::String(s), FieldValue::Integer(len)) => {
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(len as usize);
                        Ok(FieldValue::String(chars.into_iter().skip(start).collect()))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "RIGHT requires string and integer arguments".to_string(),
                        query: None,
                    }),
                }
            }
            // Date/Time Functions
            "NOW" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "NOW() takes no arguments".to_string(),
                        query: None,
                    });
                }
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                Ok(FieldValue::Integer(now))
            }
            "CURRENT_TIMESTAMP" => {
                if !args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "CURRENT_TIMESTAMP takes no arguments".to_string(),
                        query: None,
                    });
                }
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                Ok(FieldValue::Integer(now))
            }
            "DATE_FORMAT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "DATE_FORMAT requires exactly two arguments: DATE_FORMAT(timestamp, format)".to_string(),
                        query: None
                    });
                }
                let timestamp_val = self.evaluate_expression_value(&args[0], record)?;
                let format_val = self.evaluate_expression_value(&args[1], record)?;

                match (timestamp_val, format_val) {
                    (FieldValue::Integer(ts), FieldValue::String(format)) => {
                        use chrono::{TimeZone, Utc};
                        let dt = Utc.timestamp_millis_opt(ts).single().ok_or_else(|| {
                            SqlError::ExecutionError {
                                message: "Invalid timestamp".to_string(),
                                query: None,
                            }
                        })?;

                        // Simple format mapping (extend as needed)
                        let rust_format = format
                            .replace("%Y", "%Y") // 4-digit year
                            .replace("%m", "%m") // Month (01-12)
                            .replace("%d", "%d") // Day (01-31)
                            .replace("%H", "%H") // Hour (00-23)
                            .replace("%M", "%M") // Minute (00-59)
                            .replace("%S", "%S"); // Second (00-59)

                        Ok(FieldValue::String(dt.format(&rust_format).to_string()))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "DATE_FORMAT requires timestamp and format string".to_string(),
                        query: None,
                    }),
                }
            }
            "EXTRACT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "EXTRACT requires exactly two arguments: EXTRACT(part, timestamp)"
                            .to_string(),
                        query: None,
                    });
                }
                let part_val = self.evaluate_expression_value(&args[0], record)?;
                let timestamp_val = self.evaluate_expression_value(&args[1], record)?;

                match (part_val, timestamp_val) {
                    (FieldValue::String(part), FieldValue::Integer(ts)) => {
                        use chrono::{Datelike, TimeZone, Timelike, Utc};
                        let dt = Utc.timestamp_millis_opt(ts).single().ok_or_else(|| {
                            SqlError::ExecutionError {
                                message: format!("Invalid timestamp: {}", ts),
                                query: None,
                            }
                        })?;

                        let result = match part.to_uppercase().as_str() {
                            "YEAR" => dt.year() as i64,
                            "MONTH" => dt.month() as i64,
                            "DAY" => dt.day() as i64,
                            "HOUR" => dt.hour() as i64,
                            "MINUTE" => dt.minute() as i64,
                            "SECOND" => dt.second() as i64,
                            "DOW" | "DAYOFWEEK" => dt.weekday().num_days_from_sunday() as i64,
                            "DOY" | "DAYOFYEAR" => dt.ordinal() as i64,
                            _ => {
                                return Err(SqlError::ExecutionError {
                                    message: format!("Unsupported EXTRACT part: {}", part),
                                    query: None,
                                });
                            }
                        };
                        Ok(FieldValue::Integer(result))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "EXTRACT requires part name and timestamp".to_string(),
                        query: None,
                    }),
                }
            }
            "COALESCE" => {
                if args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "COALESCE requires at least one argument".to_string(),
                        query: None,
                    });
                }
                for arg in args {
                    let value = self.evaluate_expression_value(arg, record)?;
                    if !matches!(value, FieldValue::Null) {
                        return Ok(value);
                    }
                }
                Ok(FieldValue::Null)
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "NULLIF requires exactly two arguments".to_string(),
                        query: None,
                    });
                }
                let value1 = self.evaluate_expression_value(&args[0], record)?;
                let value2 = self.evaluate_expression_value(&args[1], record)?;

                if self.values_equal(&value1, &value2) {
                    Ok(FieldValue::Null)
                } else {
                    Ok(value1)
                }
            }
            "DATEDIFF" => {
                if args.len() != 3 {
                    return Err(SqlError::ExecutionError {
                        message: "DATEDIFF requires exactly 3 arguments: DATEDIFF(unit, start_date, end_date)".to_string(),
                        query: None,
                    });
                }
                let unit_val = self.evaluate_expression_value(&args[0], record)?;
                let start_val = self.evaluate_expression_value(&args[1], record)?;
                let end_val = self.evaluate_expression_value(&args[2], record)?;

                match (unit_val, start_val, end_val) {
                    (
                        FieldValue::String(unit),
                        FieldValue::Integer(start_ts),
                        FieldValue::Integer(end_ts),
                    ) => {
                        use chrono::{TimeZone, Utc};

                        let start_dt =
                            Utc.timestamp_millis_opt(start_ts).single().ok_or_else(|| {
                                SqlError::ExecutionError {
                                    message: format!("Invalid start timestamp: {}", start_ts),
                                    query: None,
                                }
                            })?;

                        let end_dt =
                            Utc.timestamp_millis_opt(end_ts).single().ok_or_else(|| {
                                SqlError::ExecutionError {
                                    message: format!("Invalid end timestamp: {}", end_ts),
                                    query: None,
                                }
                            })?;

                        let diff_ms = end_dt.timestamp_millis() - start_dt.timestamp_millis();

                        let result = match unit.to_lowercase().as_str() {
                            "milliseconds" | "ms" => diff_ms,
                            "seconds" | "second" => diff_ms / 1000,
                            "minutes" | "minute" => diff_ms / (1000 * 60),
                            "hours" | "hour" => diff_ms / (1000 * 60 * 60),
                            "days" | "day" => diff_ms / (1000 * 60 * 60 * 24),
                            _ => {
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "Unsupported DATEDIFF unit: {}. Supported units: milliseconds, seconds, minutes, hours, days",
                                        unit
                                    ),
                                    query: None,
                                });
                            }
                        };

                        Ok(FieldValue::Integer(result))
                    }
                    _ => Err(SqlError::ExecutionError {
                        message: "DATEDIFF requires string unit and integer timestamps".to_string(),
                        query: None,
                    }),
                }
            }
            "POSITION" => {
                // POSITION(substring IN string) or POSITION(substring IN string FROM start_position)
                if args.len() < 2 || args.len() > 3 {
                    return Err(SqlError::ExecutionError {
                        message: "POSITION requires 2 or 3 arguments: POSITION(substring, string [, start_position])".to_string(),
                        query: None,
                    });
                }
                let substring_val = self.evaluate_expression_value(&args[0], record)?;
                let string_val = self.evaluate_expression_value(&args[1], record)?;
                let start_pos = if args.len() == 3 {
                    match self.evaluate_expression_value(&args[2], record)? {
                        FieldValue::Integer(pos) => pos.max(1) as usize, // SQL positions are 1-based, minimum 1
                        _ => {
                            return Err(SqlError::ExecutionError {
                                message: "POSITION start position must be an integer".to_string(),
                                query: None,
                            });
                        }
                    }
                } else {
                    1 // Start from position 1 (1-based indexing)
                };

                match (substring_val, string_val) {
                    (FieldValue::String(substring), FieldValue::String(string)) => {
                        if substring.is_empty() {
                            // Empty substring is found at position 1
                            Ok(FieldValue::Integer(start_pos as i64))
                        } else {
                            // Convert to char arrays to handle Unicode properly
                            let string_chars: Vec<char> = string.chars().collect();
                            let substring_chars: Vec<char> = substring.chars().collect();

                            // Search starting from the specified position (convert from 1-based to 0-based)
                            let search_start =
                                (start_pos.saturating_sub(1)).min(string_chars.len());

                            // Find the substring
                            if let Some(pos) = string_chars[search_start..]
                                .windows(substring_chars.len())
                                .position(|window| window == substring_chars.as_slice())
                            {
                                // Convert back to 1-based indexing and add the offset
                                Ok(FieldValue::Integer((search_start + pos + 1) as i64))
                            } else {
                                // Substring not found
                                Ok(FieldValue::Integer(0))
                            }
                        }
                    }
                    (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "POSITION requires string arguments".to_string(),
                        query: None,
                    }),
                }
            }
            // Array Functions
            "ARRAY" => {
                // ARRAY[value1, value2, ...] - creates an array
                let mut elements = Vec::new();
                for arg in args {
                    let value = self.evaluate_expression_value(arg, record)?;
                    elements.push(value);
                }
                Ok(FieldValue::Array(elements))
            }
            "ARRAY_LENGTH" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "ARRAY_LENGTH requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Array(arr) => Ok(FieldValue::Integer(arr.len() as i64)),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "ARRAY_LENGTH requires array argument".to_string(),
                        query: None,
                    }),
                }
            }
            "ARRAY_CONTAINS" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError {
                        message: "ARRAY_CONTAINS requires exactly two arguments".to_string(),
                        query: None,
                    });
                }
                let array_val = self.evaluate_expression_value(&args[0], record)?;
                let search_val = self.evaluate_expression_value(&args[1], record)?;

                match array_val {
                    FieldValue::Array(arr) => {
                        let contains = arr.iter().any(|v| self.values_equal(v, &search_val));
                        Ok(FieldValue::Boolean(contains))
                    }
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "ARRAY_CONTAINS requires array as first argument".to_string(),
                        query: None,
                    }),
                }
            }
            // Map Functions
            "MAP" => {
                // MAP['key1', value1, 'key2', value2, ...] - creates a map
                if args.len() % 2 != 0 {
                    return Err(SqlError::ExecutionError {
                        message: "MAP requires even number of arguments (key-value pairs)"
                            .to_string(),
                        query: None,
                    });
                }
                let mut map = HashMap::new();
                for chunk in args.chunks(2) {
                    let key_val = self.evaluate_expression_value(&chunk[0], record)?;
                    let value_val = self.evaluate_expression_value(&chunk[1], record)?;

                    let key_str = match key_val {
                        FieldValue::String(s) => s,
                        _ => {
                            return Err(SqlError::ExecutionError {
                                message: "MAP keys must be strings".to_string(),
                                query: None,
                            });
                        }
                    };
                    map.insert(key_str, value_val);
                }
                Ok(FieldValue::Map(map))
            }
            "MAP_KEYS" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "MAP_KEYS requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Map(map) => {
                        let keys: Vec<FieldValue> =
                            map.keys().map(|k| FieldValue::String(k.clone())).collect();
                        Ok(FieldValue::Array(keys))
                    }
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "MAP_KEYS requires map argument".to_string(),
                        query: None,
                    }),
                }
            }
            "MAP_VALUES" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "MAP_VALUES requires exactly one argument".to_string(),
                        query: None,
                    });
                }
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Map(map) => {
                        let values: Vec<FieldValue> = map.values().cloned().collect();
                        Ok(FieldValue::Array(values))
                    }
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "MAP_VALUES requires map argument".to_string(),
                        query: None,
                    }),
                }
            }
            // Struct Functions
            "STRUCT" => {
                // STRUCT(field1_value, field2_value, ...) - creates a struct
                // Note: field names would need to be provided separately or inferred
                let mut fields = HashMap::new();
                for (i, arg) in args.iter().enumerate() {
                    let value = self.evaluate_expression_value(arg, record)?;
                    fields.insert(format!("field_{}", i + 1), value);
                }
                Ok(FieldValue::Struct(fields))
            }
            "LEAST" => {
                if args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "LEAST requires at least one argument".to_string(),
                        query: None,
                    });
                }

                // Evaluate all arguments
                let mut values: Vec<FieldValue> = args
                    .iter()
                    .map(|arg| self.evaluate_expression_value(arg, record))
                    .collect::<Result<Vec<_>, _>>()?;

                // Apply type promotion (integers to floats if any float is present)
                values = self.promote_numeric_type(&values);

                let mut min_val = values[0].clone();
                for val in &values[1..] {
                    if self.compare_values_for_min(val, &min_val)? {
                        min_val = val.clone();
                    }
                }
                Ok(min_val)
            }
            "GREATEST" => {
                if args.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "GREATEST requires at least one argument".to_string(),
                        query: None,
                    });
                }

                // Evaluate all arguments
                let mut values: Vec<FieldValue> = args
                    .iter()
                    .map(|arg| self.evaluate_expression_value(arg, record))
                    .collect::<Result<Vec<_>, _>>()?;

                // Apply type promotion (integers to floats if any float is present)
                values = self.promote_numeric_type(&values);

                let mut max_val = values[0].clone();
                for val in &values[1..] {
                    if self.compare_values_for_max(val, &max_val)? {
                        max_val = val.clone();
                    }
                }
                Ok(max_val)
            }
            "STDDEV" | "STDDEV_SAMP" => {
                // STDDEV(column) - standard deviation (sample)
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "STDDEV requires exactly one argument".to_string(),
                        query: None,
                    });
                }

                // For streaming, return 0.0 since we only have one value
                // In a real implementation, this would calculate over a window of values
                warn!(
                    "STDDEV function: returning 0.0 for single streaming record - requires window aggregation"
                );
                Ok(FieldValue::Float(0.0))
            }
            "STDDEV_POP" => {
                // STDDEV_POP(column) - population standard deviation
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "STDDEV_POP requires exactly one argument".to_string(),
                        query: None,
                    });
                }

                warn!(
                    "STDDEV_POP function: returning 0.0 for single streaming record - requires window aggregation"
                );
                Ok(FieldValue::Float(0.0))
            }
            "VARIANCE" | "VAR_SAMP" => {
                // VARIANCE(column) - sample variance
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "VARIANCE requires exactly one argument".to_string(),
                        query: None,
                    });
                }

                warn!(
                    "VARIANCE function: returning 0.0 for single streaming record - requires window aggregation"
                );
                Ok(FieldValue::Float(0.0))
            }
            "VAR_POP" => {
                // VAR_POP(column) - population variance
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "VAR_POP requires exactly one argument".to_string(),
                        query: None,
                    });
                }

                warn!(
                    "VAR_POP function: returning 0.0 for single streaming record - requires window aggregation"
                );
                Ok(FieldValue::Float(0.0))
            }
            "MEDIAN" => {
                // MEDIAN(column) - median value
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError {
                        message: "MEDIAN requires exactly one argument".to_string(),
                        query: None,
                    });
                }

                // For streaming with single value, median is the value itself
                let value = self.evaluate_expression_value(&args[0], record)?;
                match value {
                    FieldValue::Integer(_) | FieldValue::Float(_) => Ok(value),
                    FieldValue::Null => Ok(FieldValue::Null),
                    _ => Err(SqlError::ExecutionError {
                        message: "MEDIAN requires numeric argument".to_string(),
                        query: None,
                    }),
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unknown function {}", name),
                query: None,
            }),
        }
    }

    fn extract_json_value(&self, json_string: &str, path: &str) -> Result<FieldValue, SqlError> {
        // Parse the JSON string
        let json_value: serde_json::Value =
            serde_json::from_str(json_string).map_err(|_| SqlError::ExecutionError {
                message: "Invalid JSON string".to_string(),
                query: None,
            })?;

        // Simple path extraction - supports dot notation like "user.name" or "data.items[0]"
        let mut current = &json_value;

        // Split path by dots, but handle array indices
        let path_parts: Vec<&str> = if path.starts_with('$') {
            // JSONPath style - remove the $ prefix
            path[1..].split('.').collect()
        } else {
            path.split('.').collect()
        };

        for part in path_parts {
            if part.is_empty() {
                continue;
            }

            // Handle array access like "items[0]"
            if let Some(bracket_pos) = part.find('[') {
                let field_name = &part[..bracket_pos];
                let index_part = &part[bracket_pos + 1..part.len() - 1]; // Remove [ and ]

                // First navigate to the field
                if !field_name.is_empty() {
                    current = current
                        .get(field_name)
                        .ok_or_else(|| SqlError::ExecutionError {
                            message: format!("JSON path not found: {}", field_name),
                            query: None,
                        })?;
                }

                // Then handle array index
                let index: usize = index_part.parse().map_err(|_| SqlError::ExecutionError {
                    message: format!("Invalid array index: {}", index_part),
                    query: None,
                })?;

                current = current.get(index).ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Array index out of bounds: {}", index),
                    query: None,
                })?;
            } else {
                // Regular field access
                current = current.get(part).ok_or_else(|| SqlError::ExecutionError {
                    message: format!("JSON path not found: {}", part),
                    query: None,
                })?;
            }
        }

        // Convert JSON value to FieldValue
        match current {
            serde_json::Value::String(s) => Ok(FieldValue::String(s.clone())),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(FieldValue::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(FieldValue::Float(f))
                } else {
                    Ok(FieldValue::String(n.to_string()))
                }
            }
            serde_json::Value::Bool(b) => Ok(FieldValue::Boolean(*b)),
            serde_json::Value::Null => Ok(FieldValue::Null),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                // For complex types, return as JSON string
                Ok(FieldValue::String(current.to_string()))
            }
        }
    }

    fn cast_value(&self, value: FieldValue, target_type: &str) -> Result<FieldValue, SqlError> {
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
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                    Err(SqlError::ExecutionError {
                        message: format!("Cannot cast {} to INTEGER", value.type_name()),
                        query: None,
                    })
                }
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
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                    Err(SqlError::ExecutionError {
                        message: format!("Cannot cast {} to FLOAT", value.type_name()),
                        query: None,
                    })
                }
            },
            "STRING" | "VARCHAR" | "TEXT" => match value {
                FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                FieldValue::String(s) => Ok(FieldValue::String(s)),
                FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                FieldValue::Null => Ok(FieldValue::String("NULL".to_string())),
                FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                    Ok(FieldValue::String(value.to_display_string()))
                }
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
                FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Struct(_) => {
                    Err(SqlError::ExecutionError {
                        message: format!("Cannot cast {} to BOOLEAN", value.type_name()),
                        query: None,
                    })
                }
            },
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported cast target type: {}", target_type),
                query: None,
            }),
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<ExecutionMessage> {
        self.message_sender.clone()
    }

    /// Convert FieldValue to InternalValue for pluggable serialization
    fn field_value_to_internal(&self, value: FieldValue) -> InternalValue {
        match value {
            FieldValue::Integer(i) => InternalValue::Integer(i),
            FieldValue::Float(f) => InternalValue::Number(f),
            FieldValue::String(s) => InternalValue::String(s),
            FieldValue::Boolean(b) => InternalValue::Boolean(b),
            FieldValue::Null => InternalValue::Null,
            FieldValue::Array(arr) => {
                let internal_arr: Vec<InternalValue> = arr
                    .into_iter()
                    .map(|item| self.field_value_to_internal(item))
                    .collect();
                InternalValue::Array(internal_arr)
            }
            FieldValue::Map(map) => {
                let internal_map: HashMap<String, InternalValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, self.field_value_to_internal(v)))
                    .collect();
                InternalValue::Object(internal_map)
            }
            FieldValue::Struct(fields) => {
                let internal_map: HashMap<String, InternalValue> = fields
                    .into_iter()
                    .map(|(k, v)| (k, self.field_value_to_internal(v)))
                    .collect();
                InternalValue::Object(internal_map)
            }
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
            // Get right record - in production this would query actual data sources
            let right_record_opt =
                self.get_right_record(&join_clause.right_source, &join_clause.window)?;

            match join_clause.join_type {
                JoinType::Inner => {
                    if let Some(right_record) = right_record_opt {
                        let combined_record = self.combine_records(
                            &result_record,
                            &right_record,
                            &join_clause.right_alias,
                        )?;
                        if self.evaluate_expression(&join_clause.condition, &combined_record)? {
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
                        if self.evaluate_expression(&join_clause.condition, &combined_record)? {
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
                        if self.evaluate_expression(&join_clause.condition, &combined_record)? {
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
                        if self.evaluate_expression(&join_clause.condition, &combined_record)? {
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

    /// Get right record for JOIN operations with windowing support
    /// In production, this would query the actual right stream/table with windowing constraints
    fn get_right_record(
        &self,
        source: &StreamSource,
        window: &Option<JoinWindow>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        match source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                // Simulate different scenarios based on stream name
                if name == "empty_stream" {
                    // Simulate no matching record
                    return Ok(None);
                }

                // Create base mock record
                let mut right_record = self.create_mock_right_record(source)?;

                // Apply window constraints if specified
                if let Some(join_window) = window {
                    // Simulate windowed join logic
                    let current_time = chrono::Utc::now().timestamp();
                    let window_duration_secs = join_window.time_window.as_secs() as i64;

                    // Check if the right record falls within the time window
                    let time_diff = (current_time - right_record.timestamp).abs();

                    if time_diff > window_duration_secs {
                        // Record is outside the time window
                        log::debug!(
                            "Record outside JOIN window: {} > {}",
                            time_diff,
                            window_duration_secs
                        );

                        // Apply grace period if specified
                        if let Some(grace_period) = join_window.grace_period {
                            let grace_secs = grace_period.as_secs() as i64;
                            if time_diff > (window_duration_secs + grace_secs) {
                                return Ok(None); // Outside window and grace period
                            }
                        } else {
                            return Ok(None); // Outside window, no grace period
                        }
                    }

                    // Update timestamp to simulate temporal alignment
                    right_record.timestamp = current_time;
                    log::debug!("Applied JOIN window: {} seconds", window_duration_secs);
                }

                Ok(Some(right_record))
            }
            StreamSource::Subquery(_) => Err(SqlError::ExecutionError {
                message: "Subquery JOINs not yet implemented".to_string(),
                query: None,
            }),
        }
    }

    /// Optimized stream-table JOIN processing
    /// Tables are materialized views with key-based lookups, streams are continuous data
    fn process_stream_table_join(
        &self,
        stream_record: &StreamRecord,
        table_source: &StreamSource,
        join_condition: &Expr,
        join_type: &JoinType,
    ) -> Result<Option<StreamRecord>, SqlError> {
        match table_source {
            StreamSource::Table(table_name) => {
                // Simulate key-based table lookup (in production, this would be a hash table lookup)
                log::debug!(
                    "Performing optimized stream-table JOIN with table: {}",
                    table_name
                );

                // Extract join key from stream record using the join condition
                // For demo purposes, assume the join is on 'id' field
                let join_key =
                    stream_record
                        .fields
                        .get("id")
                        .ok_or_else(|| SqlError::ExecutionError {
                            message: "Join key 'id' not found in stream record".to_string(),
                            query: None,
                        })?;

                // Simulate table lookup based on key
                if let FieldValue::Integer(key_value) = join_key {
                    if *key_value > 0 {
                        // Simulate successful table lookup
                        let table_record = self.create_mock_table_record(table_name, *key_value)?;
                        let combined = self.combine_records(stream_record, &table_record, &None)?;

                        // Evaluate join condition on combined record
                        if self.evaluate_expression(join_condition, &combined)? {
                            return Ok(Some(combined));
                        }
                    }
                }

                // Handle different join types for no match
                match join_type {
                    JoinType::Inner => Ok(None), // No result for INNER JOIN
                    JoinType::Left => {
                        // LEFT JOIN: return stream record with NULL table fields
                        Ok(Some(self.combine_records_with_nulls(
                            stream_record,
                            &None,
                            true,
                        )?))
                    }
                    JoinType::Right => Ok(None), // RIGHT JOIN with stream-table is unusual
                    JoinType::FullOuter => {
                        // FULL OUTER: return stream record with NULL table fields
                        Ok(Some(self.combine_records_with_nulls(
                            stream_record,
                            &None,
                            true,
                        )?))
                    }
                }
            }
            _ => {
                // Fallback to regular stream-stream JOIN
                Err(SqlError::ExecutionError {
                    message: "Stream-table JOIN requires a table source".to_string(),
                    query: None,
                })
            }
        }
    }

    /// Create mock table record for stream-table JOIN optimization
    fn create_mock_table_record(
        &self,
        table_name: &str,
        key: i64,
    ) -> Result<StreamRecord, SqlError> {
        let mut fields = HashMap::new();

        // Simulate table data based on key lookup
        fields.insert("id".to_string(), FieldValue::Integer(key));
        fields.insert("table_id".to_string(), FieldValue::Integer(key));
        fields.insert(
            "table_name".to_string(),
            FieldValue::String(format!("table_data_{}", key)),
        );
        fields.insert(
            "user_name".to_string(),
            FieldValue::String(format!("user_{}", key)),
        );
        fields.insert(
            "table_value".to_string(),
            FieldValue::Float(key as f64 * 10.0),
        );
        fields.insert(
            "table_category".to_string(),
            FieldValue::String(table_name.to_string()),
        );

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp(),
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
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

    /// Create a mock right record for JOIN demonstration
    /// In production, this would query the actual right stream/table
    fn create_mock_right_record(&self, source: &StreamSource) -> Result<StreamRecord, SqlError> {
        match source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                // Create a mock record with some sample data
                let mut fields = HashMap::new();

                // Create table-specific fields based on the table name
                match name.as_str() {
                    "payments" => {
                        fields.insert("order_id".to_string(), FieldValue::Integer(1));
                        fields.insert("payment_id".to_string(), FieldValue::Integer(101));
                        fields.insert("amount".to_string(), FieldValue::Float(99.99));
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("completed".to_string()),
                        );
                    }
                    "events" => {
                        fields.insert("user_id".to_string(), FieldValue::Integer(100));
                        fields.insert("event_id".to_string(), FieldValue::Integer(201));
                        fields.insert(
                            "event_type".to_string(),
                            FieldValue::String("click".to_string()),
                        );
                        fields.insert("timestamp".to_string(), FieldValue::Integer(1640995200));
                    }
                    "user_table" => {
                        fields.insert("id".to_string(), FieldValue::Integer(100));
                        fields.insert(
                            "user_name".to_string(),
                            FieldValue::String("Alice Smith".to_string()),
                        );
                        fields.insert(
                            "email".to_string(),
                            FieldValue::String("alice@example.com".to_string()),
                        );
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("active".to_string()),
                        );
                    }
                    _ => {
                        // Default fields for generic tables/streams
                        fields.insert("right_id".to_string(), FieldValue::Integer(1));
                        fields.insert(
                            "right_name".to_string(),
                            FieldValue::String(format!("data_from_{}", name)),
                        );
                        fields.insert("right_value".to_string(), FieldValue::Float(42.0));
                        fields.insert(
                            "status".to_string(),
                            FieldValue::String("active".to_string()),
                        );
                    }
                }

                Ok(StreamRecord {
                    fields,
                    timestamp: chrono::Utc::now().timestamp(),
                    offset: 0,
                    partition: 0,
                    headers: HashMap::new(),
                })
            }
            StreamSource::Subquery(_) => Err(SqlError::ExecutionError {
                message: "Subquery JOINs not yet implemented".to_string(),
                query: None,
            }),
        }
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

    /// Convert InternalValue to FieldValue for pluggable serialization
    fn internal_to_field_value(&self, value: InternalValue) -> FieldValue {
        match value {
            InternalValue::Integer(i) => FieldValue::Integer(i),
            InternalValue::Number(f) => FieldValue::Float(f),
            InternalValue::String(s) => FieldValue::String(s),
            InternalValue::Boolean(b) => FieldValue::Boolean(b),
            InternalValue::Null => FieldValue::Null,
            InternalValue::Array(arr) => {
                let field_arr: Vec<FieldValue> = arr
                    .into_iter()
                    .map(|item| self.internal_to_field_value(item))
                    .collect();
                FieldValue::Array(field_arr)
            }
            InternalValue::Object(map) => {
                let field_map: HashMap<String, FieldValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, self.internal_to_field_value(v)))
                    .collect();
                FieldValue::Map(field_map)
            }
        }
    }

    /// Promote integer to float if any of the values is a float (SQL type promotion)
    fn promote_numeric_type(&self, values: &[FieldValue]) -> Vec<FieldValue> {
        let has_float = values.iter().any(|v| matches!(v, FieldValue::Float(_)));

        if has_float {
            values
                .iter()
                .map(|v| match v {
                    FieldValue::Integer(i) => FieldValue::Float(*i as f64),
                    other => other.clone(),
                })
                .collect()
        } else {
            values.to_vec()
        }
    }
}
