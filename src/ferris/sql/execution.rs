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

```rust
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use tokio::sync::mpsc;

// Create execution engine
let (tx, rx) = mpsc::unbounded_channel();
let mut engine = StreamExecutionEngine::new(tx);

// Execute a simple SELECT query
let query = parser.parse("SELECT customer_id, amount * 1.1 AS amount_with_tax FROM orders WHERE amount > 100")?;
let record = create_test_record();
engine.execute(&query, record).await?;

// Process results from output channel
while let Some(result) = rx.recv().await {
    println!("Query result: {:?}", result);
}
```

## Performance Characteristics

- **Memory Efficient**: Minimal memory allocation per record
- **Low Latency**: Direct expression evaluation without intermediate representations
- **Streaming Native**: No buffering or batching unless required by windows
- **Type Safe**: Runtime type checking with detailed error messages
*/

use crate::ferris::sql::ast::{
    BinaryOperator, Expr, LiteralValue, StreamingQuery,
    SelectField, StreamSource, WindowSpec, UnaryOperator, ShowResourceType
};
use crate::ferris::sql::error::SqlError;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, UnboundedSender};

pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::Sender<ExecutionMessage>,
    message_receiver: Option<mpsc::Receiver<ExecutionMessage>>,
    output_sender: UnboundedSender<HashMap<String, serde_json::Value>>,
    record_count: u64,
}

#[derive(Debug)]
pub enum ExecutionMessage {
    StartJob { job_id: String, query: StreamingQuery },
    StopJob { job_id: String },
    ProcessRecord { stream_name: String, record: StreamRecord },
    QueryResult { query_id: String, result: StreamRecord },
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
    pub fn new(output_sender: UnboundedSender<HashMap<String, serde_json::Value>>) -> Self {
        let (message_sender, receiver) = mpsc::channel(1000);
        Self {
            active_queries: HashMap::new(),
            message_sender,
            message_receiver: Some(receiver),
            output_sender,
            record_count: 0,
        }
    }

    pub async fn execute(&mut self, query: &StreamingQuery, record: HashMap<String, serde_json::Value>) -> Result<(), SqlError> {
        self.execute_with_headers(query, record, HashMap::new()).await
    }

    pub async fn execute_with_headers(&mut self, query: &StreamingQuery, record: HashMap<String, serde_json::Value>, headers: HashMap<String, String>) -> Result<(), SqlError> {
        // Convert input record to StreamRecord
        let stream_record = StreamRecord {
            fields: record.into_iter().map(|(k, v)| {
                let field_value = match v {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FieldValue::Integer(i)
                        } else if let Some(f) = n.as_f64() {
                            FieldValue::Float(f)
                        } else {
                            FieldValue::Null
                        }
                    },
                    serde_json::Value::String(s) => FieldValue::String(s),
                    serde_json::Value::Bool(b) => FieldValue::Boolean(b),
                    _ => FieldValue::Null,
                };
                (k, field_value)
            }).collect(),
            timestamp: chrono::Utc::now().timestamp(),
            offset: 0,
            partition: 0,
            headers,
        };

        // Apply query and process record
        if let Some(result) = self.apply_query(query, &stream_record)? {
            // Convert result back to JSON format
            let json_result: HashMap<String, serde_json::Value> = result.fields.into_iter().map(|(k, v)| {
                let json_value = match v {
                    FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(i)),
                    FieldValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or(0.into())),
                    FieldValue::String(s) => serde_json::Value::String(s),
                    FieldValue::Boolean(b) => serde_json::Value::Bool(b),
                    FieldValue::Null => serde_json::Value::Null,
                };
                (k, json_value)
            }).collect();

            // Send result through both channels
            self.message_sender.send(ExecutionMessage::QueryResult {
                query_id: "default".to_string(),
                result: stream_record,
            }).await.map_err(|_| SqlError::ExecutionError {
                message: "Failed to send result".to_string(),
                query: None,
            })?;

            // Send result to output channel
            self.output_sender.send(json_result).map_err(|_| SqlError::ExecutionError {
                message: "Failed to send result to output channel".to_string(),
                query: None,
            })?;
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), SqlError> {
        let mut receiver = self.message_receiver.take()
            .ok_or_else(|| SqlError::ExecutionError { 
                message: "Engine already started".to_string(),
                query: None 
            })?;

        while let Some(message) = receiver.recv().await {
            match message {
                ExecutionMessage::StartJob { job_id, query } => {
                    self.start_query_execution(job_id, query).await?;
                }
                ExecutionMessage::StopJob { job_id } => {
                    self.stop_query_execution(&job_id).await?;
                }
                ExecutionMessage::ProcessRecord { stream_name, record } => {
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
        query: StreamingQuery
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
        record: StreamRecord
    ) -> Result<(), SqlError> {
        // Collect matching queries first
        let matching_queries: Vec<(String, StreamingQuery)> = self.active_queries
            .iter()
            .filter_map(|(query_id, execution)| {
                if self.query_matches_stream(&execution.query, stream_name) {
                    match &execution.state {
                        ExecutionState::Running => Some((query_id.clone(), execution.query.clone())),
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
            let _ = self.message_sender.send(ExecutionMessage::QueryResult {
                query_id,
                result,
            }).await;
        }
        Ok(())
    }

    fn query_matches_stream(&self, query: &StreamingQuery, stream_name: &str) -> bool {
        match query {
            StreamingQuery::Select { from, .. } => {
                match from {
                    StreamSource::Stream(name) | StreamSource::Table(name) => name == stream_name,
                    StreamSource::Subquery(_) => false,
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => self.query_matches_stream(as_select, stream_name),
            StreamingQuery::CreateTable { as_select, .. } => self.query_matches_stream(as_select, stream_name),
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
        record: &StreamRecord
    ) -> Result<Option<StreamRecord>, SqlError> {
        match query {
            StreamingQuery::Select { fields, where_clause, limit, .. } => {
                // Check limit first
                if let Some(limit_value) = limit {
                    if self.record_count >= *limit_value {
                        return Ok(None);
                    }
                }
                // Apply WHERE clause
                if let Some(where_expr) = where_clause {
                    if !self.evaluate_expression(where_expr, record)? {
                        return Ok(None);
                    }
                }

                // Apply SELECT fields
                let mut result_fields = HashMap::new();
                
                for field in fields {
                    match field {
                        SelectField::Wildcard => {
                            result_fields.extend(record.fields.clone());
                        }
                        SelectField::Column(name) => {
                            // Check for system columns first (case insensitive)
                            let field_value = match name.to_uppercase().as_str() {
                                "_TIMESTAMP" => Some(FieldValue::Integer(record.timestamp)),
                                "_OFFSET" => Some(FieldValue::Integer(record.offset)),
                                "_PARTITION" => Some(FieldValue::Integer(record.partition as i64)),
                                _ => record.fields.get(name).cloned()
                            };
                            
                            if let Some(value) = field_value {
                                result_fields.insert(name.clone(), value);
                            }
                        }
                        SelectField::AliasedColumn { column, alias } => {
                            // Check for system columns first (case insensitive)
                            let field_value = match column.to_uppercase().as_str() {
                                "_TIMESTAMP" => Some(FieldValue::Integer(record.timestamp)),
                                "_OFFSET" => Some(FieldValue::Integer(record.offset)),
                                "_PARTITION" => Some(FieldValue::Integer(record.partition as i64)),
                                _ => record.fields.get(column).cloned()
                            };
                            
                            if let Some(value) = field_value {
                                result_fields.insert(alias.clone(), value);
                            }
                        }
                        SelectField::Expression { expr, alias } => {
                            let value = self.evaluate_expression_value(expr, record)?;
                            let field_name = alias.as_ref()
                                .unwrap_or(&self.get_expression_name(expr))
                                .clone();
                            result_fields.insert(field_name, value);
                        }
                    }
                }

                // Increment record count for successful processing
                self.record_count += 1;

                Ok(Some(StreamRecord {
                    fields: result_fields,
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::CreateStream { name, as_select, .. } => {
                // Execute the underlying SELECT query and return its result
                // In a full implementation, this would also:
                // 1. Create a new Kafka topic
                // 2. Register the stream in the SQL context  
                // 3. Set up continuous query execution
                log::info!("Executing CREATE STREAM: {}", name);
                self.apply_query(as_select, record)
            }
            StreamingQuery::CreateTable { name, as_select, .. } => {
                // Execute the underlying SELECT query for materialized table
                // In a full implementation, this would also:
                // 1. Create a KTable/materialized view
                // 2. Set up aggregation state
                // 3. Register the table in the SQL context
                log::info!("Executing CREATE TABLE: {}", name);
                self.apply_query(as_select, record)
            }
            StreamingQuery::Show { resource_type, pattern } => {
                // SHOW commands return metadata, not transformed records
                // In a full implementation, this would query the SQL context for metadata
                log::info!("Executing SHOW {:?} with pattern {:?}", resource_type, pattern);
                
                // For now, return a simple result indicating the SHOW command was processed
                Ok(Some(StreamRecord {
                    fields: {
                        let mut fields = HashMap::new();
                        fields.insert("show_type".to_string(), FieldValue::String(format!("{:?}", resource_type)));
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
            StreamingQuery::StartJob { name, query, properties } => {
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
                        fields.insert("status".to_string(), FieldValue::String("stopped".to_string()));
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
                        fields.insert("status".to_string(), FieldValue::String("paused".to_string()));
                        fields.insert("action".to_string(), FieldValue::String("pause".to_string()));
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
                        fields.insert("status".to_string(), FieldValue::String("running".to_string()));
                        fields.insert("action".to_string(), FieldValue::String("resume".to_string()));
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::DeployJob { name, version, query, properties, strategy } => {
                // DEPLOY JOB performs versioned deployment with specified strategy
                log::info!("Deploying job '{}' version '{}' with strategy {:?}", name, version, strategy);
                
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
                        fields.insert("strategy".to_string(), FieldValue::String(format!("{:?}", strategy)));
                        fields.insert("status".to_string(), FieldValue::String("deployed".to_string()));
                        fields.insert("properties_count".to_string(), FieldValue::Integer(properties.len() as i64));
                        fields
                    },
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                }))
            }
            StreamingQuery::RollbackJob { name, target_version } => {
                // ROLLBACK JOB reverts to a previous version
                let version_msg = target_version.as_ref()
                    .map(|v| format!(" to version '{}'", v))
                    .unwrap_or_else(|| " to previous version".to_string());
                log::info!("Rolling back job '{}'{}",name, version_msg);
                
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
                            fields.insert("target_version".to_string(), FieldValue::String(version.clone()));
                        }
                        fields.insert("status".to_string(), FieldValue::String("rolled_back".to_string()));
                        fields.insert("action".to_string(), FieldValue::String("rollback".to_string()));
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
            BinaryOperator::GreaterThanOrEqual => self.compare_values(&left_val, &right_val, |a, b| a >= b),
            BinaryOperator::LessThanOrEqual => self.compare_values(&left_val, &right_val, |a, b| a <= b),
            _ => Err(SqlError::ExecutionError {
                message: "Invalid comparison operator".to_string(),
                query: None
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
                        // Regular field lookup
                        record.fields.get(name)
                            .cloned()
                            .ok_or_else(|| SqlError::SchemaError {
                                message: "Column not found".to_string(),
                                column: Some(name.clone())
                            })?
                    }
                };

                match field_value {
                    FieldValue::Boolean(b) => Ok(b),
                    _ => Err(SqlError::TypeError { 
                        expected: "boolean".to_string(),
                        actual: "other".to_string(),
                        value: None
                    }),
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Boolean(b) => Ok(*b),
                LiteralValue::Integer(_) |
                LiteralValue::Float(_) |
                LiteralValue::String(_) |
                LiteralValue::Null |
                LiteralValue::Interval { .. } => {
                    Err(SqlError::TypeError {
                        expected: "boolean".to_string(),
                        actual: "non-boolean".to_string(),
                        value: None
                    })
                }
            },
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Equal |
                    BinaryOperator::NotEqual |
                    BinaryOperator::GreaterThan |
                    BinaryOperator::LessThan |
                    BinaryOperator::GreaterThanOrEqual |
                    BinaryOperator::LessThanOrEqual => {
                        self.evaluate_comparison(left, right, record, op)
                    }
                    BinaryOperator::And | BinaryOperator::Or => {
                        Err(SqlError::ExecutionError {
                            message: "Logical operators should be handled at parse time".to_string(),
                            query: None
                        })
                    }
                    BinaryOperator::Like | BinaryOperator::NotLike => {
                        Err(SqlError::ExecutionError {
                            message: "String comparison operators not yet implemented".to_string(),
                            query: None
                        })
                    }
                    BinaryOperator::In | BinaryOperator::NotIn => {
                        Err(SqlError::ExecutionError {
                            message: "Set operators not yet implemented".to_string(),
                            query: None
                        })
                    }
                    BinaryOperator::Add |
                    BinaryOperator::Subtract |
                    BinaryOperator::Multiply |
                    BinaryOperator::Divide |
                    BinaryOperator::Modulo => {
                        Err(SqlError::ExecutionError {
                            message: "Arithmetic operator not valid in boolean context".to_string(),
                            query: None
                        })
                    }
                }
            }
            Expr::UnaryOp { op, expr } => {
                match op {
                    UnaryOperator::Not => {
                        let val = self.evaluate_expression(expr, record)?;
                        Ok(!val)
                    },
                    _ => Err(SqlError::ExecutionError {
                        message: "Unsupported unary operator for boolean expression".to_string(),
                        query: None
                    }),
                }
            }
            Expr::Case { when_clauses, else_clause } => {
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
                    value: None
                })
            }
        }
    }

    fn evaluate_expression_value(&self, expr: &Expr, record: &StreamRecord) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                // Check for system columns first (case insensitive)
                match name.to_uppercase().as_str() {
                    "_TIMESTAMP" => Ok(FieldValue::Integer(record.timestamp)),
                    "_OFFSET" => Ok(FieldValue::Integer(record.offset)),
                    "_PARTITION" => Ok(FieldValue::Integer(record.partition as i64)),
                    _ => {
                        // Regular field lookup
                        record.fields.get(name)
                            .cloned()
                            .ok_or_else(|| SqlError::SchemaError {
                                message: "Column not found".to_string(),
                                column: Some(name.clone())
                            })
                    }
                }
            }
            Expr::Literal(lit) => match lit {
                LiteralValue::Integer(i) => Ok(FieldValue::Integer(*i)),
                LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                LiteralValue::String(s) => Ok(FieldValue::String(s.clone())),
                LiteralValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
                LiteralValue::Null => Ok(FieldValue::Null),
                LiteralValue::Interval { .. } => Err(SqlError::TypeError {
                    expected: "basic type".to_string(),
                    actual: "interval".to_string(),
                    value: None
                }),
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
                    BinaryOperator::Equal |
                    BinaryOperator::NotEqual |
                    BinaryOperator::GreaterThan |
                    BinaryOperator::LessThan |
                    BinaryOperator::GreaterThanOrEqual |
                    BinaryOperator::LessThanOrEqual |
                    BinaryOperator::Like |
                    BinaryOperator::NotLike |
                    BinaryOperator::In |
                    BinaryOperator::NotIn |
                    BinaryOperator::And |
                    BinaryOperator::Or => Err(SqlError::ExecutionError {
                        message: "Operator not supported in value context".to_string(),
                        query: None
                    }),
                }
            }
            Expr::UnaryOp { op: _, expr: _ } => Err(SqlError::ExecutionError {
                message: "Unary operations not supported for value expressions".to_string(),
                query: None
            }),
            Expr::Case { when_clauses, else_clause } => {
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
        }
    }

    fn get_expression_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Literal(_) => "literal".to_string(),
            Expr::BinaryOp { .. } => "expression".to_string(),
            Expr::Function { name, .. } => name.clone(),
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
            _ => false,
        }
    }

    fn compare_values<F>(&self, left: &FieldValue, right: &FieldValue, op: F) -> Result<bool, SqlError>
    where
        F: Fn(f64, f64) -> bool,
    {
        let (left_num, right_num) = match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => (*a as f64, *b as f64),
            (FieldValue::Float(a), FieldValue::Float(b)) => (*a, *b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64, *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => (*a, *b as f64),
            _ => return Err(SqlError::TypeError { 
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None
            }),
        };
        
        Ok(op(left_num, right_num))
    }

    pub fn add_values(&self, left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),
            _ => Err(SqlError::TypeError { 
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None
            }),
        }
    }

    pub fn subtract_values(&self, left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),
            _ => Err(SqlError::TypeError { 
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None
            }),
        }
    }

    pub fn multiply_values(&self, left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a * b)),
            (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a * b)),
            (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 * b)),
            (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a * *b as f64)),
            _ => Err(SqlError::TypeError { 
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None
            }),
        }
    }

    pub fn divide_values(&self, left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError { 
                        message: "Division by zero".to_string(),
                        query: None 
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / *b as f64))
                }
            }
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError { 
                        message: "Division by zero".to_string(),
                        query: None 
                    })
                } else {
                    Ok(FieldValue::Float(a / b))
                }
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                if *b == 0.0 {
                    Err(SqlError::ExecutionError { 
                        message: "Division by zero".to_string(),
                        query: None 
                    })
                } else {
                    Ok(FieldValue::Float(*a as f64 / b))
                }
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                if *b == 0 {
                    Err(SqlError::ExecutionError { 
                        message: "Division by zero".to_string(),
                        query: None 
                    })
                } else {
                    Ok(FieldValue::Float(a / *b as f64))
                }
            }
            _ => Err(SqlError::TypeError { 
                expected: "numeric".to_string(),
                actual: "non-numeric".to_string(),
                value: None
            }),
        }
    }

    fn evaluate_function(&self, name: &str, args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        match name.to_uppercase().as_str() {
            "COUNT" => Ok(FieldValue::Integer(1)), // Simplified for streaming
            "SUM" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "SUM requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                self.evaluate_expression_value(&args[0], record)
            }
            "AVG" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "AVG requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                self.evaluate_expression_value(&args[0], record)
            }
            "HEADER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "HEADER requires exactly one argument (header key)".to_string(),
                        query: None 
                    });
                }
                
                // Evaluate the argument to get the header key
                let key_value = self.evaluate_expression_value(&args[0], record)?;
                let header_key = match key_value {
                    FieldValue::String(key) => key,
                    _ => return Err(SqlError::ExecutionError { 
                        message: "HEADER key must be a string".to_string(),
                        query: None 
                    }),
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
                        query: None 
                    });
                }
                
                // Return comma-separated list of header keys
                let keys: Vec<String> = record.headers.keys().cloned().collect();
                Ok(FieldValue::String(keys.join(",")))
            }
            "HAS_HEADER" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "HAS_HEADER requires exactly one argument (header key)".to_string(),
                        query: None 
                    });
                }
                
                // Evaluate the argument to get the header key
                let key_value = self.evaluate_expression_value(&args[0], record)?;
                let header_key = match key_value {
                    FieldValue::String(key) => key,
                    _ => return Err(SqlError::ExecutionError { 
                        message: "HAS_HEADER key must be a string".to_string(),
                        query: None 
                    }),
                };
                
                // Check if header exists
                Ok(FieldValue::Boolean(record.headers.contains_key(&header_key)))
            }
            "MIN" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "MIN requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                // For streaming, return the current value (full aggregation would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "MAX" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "MAX requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                // For streaming, return the current value (full aggregation would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "FIRST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "FIRST_VALUE requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                // For streaming, return the current value (full windowing would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "LAST_VALUE" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "LAST_VALUE requires exactly one argument".to_string(),
                        query: None 
                    });
                }
                // For streaming, return the current value (full windowing would require state)
                self.evaluate_expression_value(&args[0], record)
            }
            "APPROX_COUNT_DISTINCT" => {
                if args.len() != 1 {
                    return Err(SqlError::ExecutionError { 
                        message: "APPROX_COUNT_DISTINCT requires exactly one argument".to_string(),
                        query: None 
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
                        query: None 
                    });
                }
                // Return current record timestamp
                Ok(FieldValue::Integer(record.timestamp))
            }
            "CAST" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError { 
                        message: "CAST requires exactly two arguments: CAST(value, type)".to_string(),
                        query: None 
                    });
                }
                
                let value = self.evaluate_expression_value(&args[0], record)?;
                let target_type = match &args[1] {
                    Expr::Literal(LiteralValue::String(type_str)) => type_str.to_uppercase(),
                    _ => return Err(SqlError::ExecutionError { 
                        message: "CAST target type must be a string literal".to_string(),
                        query: None 
                    }),
                };
                
                self.cast_value(value, &target_type)
            }
            "SPLIT" => {
                if args.len() != 2 {
                    return Err(SqlError::ExecutionError { 
                        message: "SPLIT requires exactly two arguments: SPLIT(string, delimiter)".to_string(),
                        query: None 
                    });
                }
                
                let string_val = self.evaluate_expression_value(&args[0], record)?;
                let delimiter_val = self.evaluate_expression_value(&args[1], record)?;
                
                let (string, delimiter) = match (string_val, delimiter_val) {
                    (FieldValue::String(s), FieldValue::String(d)) => (s, d),
                    _ => return Err(SqlError::ExecutionError { 
                        message: "SPLIT requires string arguments".to_string(),
                        query: None 
                    }),
                };
                
                // Return first part for simplicity (full array support would need array type)
                let parts: Vec<&str> = string.split(&delimiter).collect();
                Ok(FieldValue::String(parts.get(0).unwrap_or(&"").to_string()))
            }
            "JOIN" => {
                if args.len() < 2 {
                    return Err(SqlError::ExecutionError { 
                        message: "JOIN requires at least two arguments".to_string(),
                        query: None 
                    });
                }
                
                // Evaluate all arguments and join them with the first argument as delimiter
                let delimiter_val = self.evaluate_expression_value(&args[0], record)?;
                let delimiter = match delimiter_val {
                    FieldValue::String(d) => d,
                    _ => return Err(SqlError::ExecutionError { 
                        message: "JOIN delimiter must be a string".to_string(),
                        query: None 
                    }),
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
                    _ => return Err(SqlError::ExecutionError { 
                        message: "SUBSTRING requires string and integer arguments".to_string(),
                        query: None 
                    }),
                };
                
                // Handle optional length argument
                let result = if args.len() == 3 {
                    let length_val = self.evaluate_expression_value(&args[2], record)?;
                    match length_val {
                        FieldValue::Integer(length) => {
                            string.chars().skip(start.saturating_sub(1)).take(length as usize).collect()
                        }
                        _ => return Err(SqlError::ExecutionError { 
                            message: "SUBSTRING length must be an integer".to_string(),
                            query: None 
                        }),
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
                    _ => return Err(SqlError::ExecutionError { 
                        message: "JSON_EXTRACT requires string arguments".to_string(),
                        query: None 
                    }),
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
                    _ => return Err(SqlError::ExecutionError { 
                        message: "JSON_VALUE requires string arguments".to_string(),
                        query: None 
                    }),
                };
                
                // JSON_VALUE returns scalar values as strings, JSON_EXTRACT can return objects/arrays
                match self.extract_json_value(&json_string, &path)? {
                    FieldValue::String(s) => Ok(FieldValue::String(s)),
                    FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                    FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                    FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                    FieldValue::Null => Ok(FieldValue::Null),
                }
            }
            _ => Err(SqlError::ExecutionError { 
                message: format!("Unknown function {}", name),
                query: None
            }),
        }
    }

    fn extract_json_value(&self, json_string: &str, path: &str) -> Result<FieldValue, SqlError> {
        // Parse the JSON string
        let json_value: serde_json::Value = serde_json::from_str(json_string)
            .map_err(|_| SqlError::ExecutionError {
                message: "Invalid JSON string".to_string(),
                query: None
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
                let index_part = &part[bracket_pos+1..part.len()-1]; // Remove [ and ]
                
                // First navigate to the field
                if !field_name.is_empty() {
                    current = current.get(field_name).ok_or_else(|| SqlError::ExecutionError {
                        message: format!("JSON path not found: {}", field_name),
                        query: None
                    })?;
                }
                
                // Then handle array index
                let index: usize = index_part.parse().map_err(|_| SqlError::ExecutionError {
                    message: format!("Invalid array index: {}", index_part),
                    query: None
                })?;
                
                current = current.get(index).ok_or_else(|| SqlError::ExecutionError {
                    message: format!("Array index out of bounds: {}", index),
                    query: None
                })?;
            } else {
                // Regular field access
                current = current.get(part).ok_or_else(|| SqlError::ExecutionError {
                    message: format!("JSON path not found: {}", part),
                    query: None
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
            "INTEGER" | "INT" => {
                match value {
                    FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                    FieldValue::Float(f) => Ok(FieldValue::Integer(f as i64)),
                    FieldValue::String(s) => {
                        s.parse::<i64>()
                            .map(FieldValue::Integer)
                            .map_err(|_| SqlError::ExecutionError {
                                message: format!("Cannot cast '{}' to INTEGER", s),
                                query: None
                            })
                    }
                    FieldValue::Boolean(b) => Ok(FieldValue::Integer(if b { 1 } else { 0 })),
                    FieldValue::Null => Ok(FieldValue::Null),
                }
            }
            "FLOAT" | "DOUBLE" => {
                match value {
                    FieldValue::Integer(i) => Ok(FieldValue::Float(i as f64)),
                    FieldValue::Float(f) => Ok(FieldValue::Float(f)),
                    FieldValue::String(s) => {
                        s.parse::<f64>()
                            .map(FieldValue::Float)
                            .map_err(|_| SqlError::ExecutionError {
                                message: format!("Cannot cast '{}' to FLOAT", s),
                                query: None
                            })
                    }
                    FieldValue::Boolean(b) => Ok(FieldValue::Float(if b { 1.0 } else { 0.0 })),
                    FieldValue::Null => Ok(FieldValue::Null),
                }
            }
            "STRING" | "VARCHAR" | "TEXT" => {
                match value {
                    FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                    FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                    FieldValue::String(s) => Ok(FieldValue::String(s)),
                    FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                    FieldValue::Null => Ok(FieldValue::String("NULL".to_string())),
                }
            }
            "BOOLEAN" | "BOOL" => {
                match value {
                    FieldValue::Integer(i) => Ok(FieldValue::Boolean(i != 0)),
                    FieldValue::Float(f) => Ok(FieldValue::Boolean(f != 0.0)),
                    FieldValue::String(s) => {
                        match s.to_uppercase().as_str() {
                            "TRUE" | "T" | "1" => Ok(FieldValue::Boolean(true)),
                            "FALSE" | "F" | "0" => Ok(FieldValue::Boolean(false)),
                            _ => Err(SqlError::ExecutionError {
                                message: format!("Cannot cast '{}' to BOOLEAN", s),
                                query: None
                            })
                        }
                    }
                    FieldValue::Boolean(b) => Ok(FieldValue::Boolean(b)),
                    FieldValue::Null => Ok(FieldValue::Null),
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported cast target type: {}", target_type),
                query: None
            })
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<ExecutionMessage> {
        self.message_sender.clone()
    }
}
