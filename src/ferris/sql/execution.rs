use crate::ferris::sql::ast::*;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::schema::StreamHandle;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct StreamExecutionEngine {
    active_queries: HashMap<String, QueryExecution>,
    message_sender: mpsc::Sender<ExecutionMessage>,
    message_receiver: Option<mpsc::Receiver<ExecutionMessage>>,
}

#[derive(Debug)]
pub enum ExecutionMessage {
    StartQuery { query_id: String, query: StreamingQuery },
    StopQuery { query_id: String },
    ProcessRecord { stream_name: String, record: StreamRecord },
    QueryResult { query_id: String, result: StreamRecord },
}

#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: i64,
    pub offset: i64,
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
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        
        Self {
            active_queries: HashMap::new(),
            message_sender: sender,
            message_receiver: Some(receiver),
        }
    }

    pub async fn start(&mut self) -> Result<(), SqlError> {
        let mut receiver = self.message_receiver.take()
            .ok_or_else(|| SqlError::ExecutionError { 
                message: "Engine already started".to_string(),
                query: None 
            })?;

        while let Some(message) = receiver.recv().await {
            match message {
                ExecutionMessage::StartQuery { query_id, query } => {
                    self.start_query_execution(query_id, query).await?;
                }
                ExecutionMessage::StopQuery { query_id } => {
                    self.stop_query_execution(&query_id).await?;
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
        let mut results = Vec::new();
        for (query_id, execution) in &self.active_queries {
            if self.query_matches_stream(&execution.query, stream_name) {
                match &execution.state {
                    ExecutionState::Running => {
                        if let Some(result) = self.apply_query(&execution.query, &record)? {
                            results.push((query_id.clone(), result));
                        }
                    }
                    _ => continue,
                }
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
            StreamingQuery::CreateStream { .. } => false,
        }
    }

    fn apply_query(
        &self, 
        query: &StreamingQuery, 
        record: &StreamRecord
    ) -> Result<Option<StreamRecord>, SqlError> {
        match query {
            StreamingQuery::Select { fields, where_clause, .. } => {
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
                            if let Some(value) = record.fields.get(name) {
                                result_fields.insert(name.clone(), value.clone());
                            }
                        }
                        SelectField::AliasedColumn { column, alias } => {
                            if let Some(value) = record.fields.get(column) {
                                result_fields.insert(alias.clone(), value.clone());
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

                Ok(Some(StreamRecord {
                    fields: result_fields,
                    timestamp: record.timestamp,
                    offset: record.offset,
                }))
            }
            StreamingQuery::CreateStream { .. } => {
                Err(SqlError::ExecutionError { 
                    message: "CREATE STREAM execution not supported".to_string(),
                    query: None 
                })
            }
        }
    }

    fn evaluate_expression(&self, expr: &Expr, record: &StreamRecord) -> Result<bool, SqlError> {
        match expr {
            Expr::Column(name) => {
                match record.fields.get(name) {
                    Some(FieldValue::Boolean(b)) => Ok(*b),
                    Some(_) => Err(SqlError::TypeError { 
                        expected: "boolean".to_string(),
                        actual: "other".to_string(),
                        value: None
                    }),
                    None => Err(SqlError::ColumnNotFound { column_name: name.clone() }),
                }
            }
            Expr::Literal(LiteralValue::Boolean(b)) => Ok(*b),
            Expr::BinaryOp { left, op, right } => {
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
                        message: "Unsupported boolean operator".to_string(),
                        query: None 
                    }),
                }
            }
            _ => Err(SqlError::ExecutionError { 
                message: "Invalid boolean expression".to_string(),
                query: None 
            }),
        }
    }

    fn evaluate_expression_value(&self, expr: &Expr, record: &StreamRecord) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(name) => {
                record.fields.get(name)
                    .cloned()
                    .ok_or_else(|| SqlError::ColumnNotFound { column_name: name.clone() })
            }
            Expr::Literal(lit) => Ok(match lit {
                LiteralValue::Integer(i) => FieldValue::Integer(*i),
                LiteralValue::Float(f) => FieldValue::Float(*f),
                LiteralValue::String(s) => FieldValue::String(s.clone()),
                LiteralValue::Boolean(b) => FieldValue::Boolean(*b),
                LiteralValue::Null => FieldValue::Null,
            }),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_value(left, record)?;
                let right_val = self.evaluate_expression_value(right, record)?;
                
                match op {
                    BinaryOperator::Add => self.add_values(&left_val, &right_val),
                    BinaryOperator::Subtract => self.subtract_values(&left_val, &right_val),
                    BinaryOperator::Multiply => self.multiply_values(&left_val, &right_val),
                    BinaryOperator::Divide => self.divide_values(&left_val, &right_val),
                    _ => Err(SqlError::ExecutionError { 
                        message: "Unsupported arithmetic operator".to_string(),
                        query: None 
                    }),
                }
            }
            Expr::Function { name, args } => {
                self.evaluate_function(name, args, record)
            }
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
            _ => Err(SqlError::ExecutionError { 
                message: format!("Unknown function: {}", name),
                query: None 
            }),
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<ExecutionMessage> {
        self.message_sender.clone()
    }
}

impl Default for StreamExecutionEngine {
    fn default() -> Self {
        Self::new()
    }
}