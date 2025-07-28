use crate::ferris::sql::ast::StreamingQuery;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::parser::StreamingSqlParser;
use crate::ferris::sql::schema::{Schema, StreamHandle};
use std::collections::HashMap;
use std::sync::Arc;

pub struct StreamingSqlContext {
    registered_streams: HashMap<String, StreamHandle>,
    schemas: HashMap<String, Schema>,
    parser: StreamingSqlParser,
}

impl StreamingSqlContext {
    pub fn new() -> Self {
        Self {
            registered_streams: HashMap::new(),
            schemas: HashMap::new(),
            parser: StreamingSqlParser::new(),
        }
    }

    pub fn register_stream(
        &mut self, 
        name: String, 
        handle: StreamHandle, 
        schema: Schema
    ) -> Result<(), SqlError> {
        if self.registered_streams.contains_key(&name) {
            return Err(SqlError::StreamError {
                stream_name: name,
                message: "Stream already exists".to_string()
            });
        }

        self.registered_streams.insert(name.clone(), handle);
        self.schemas.insert(name, schema);
        Ok(())
    }

    pub fn unregister_stream(&mut self, name: &str) -> Result<(), SqlError> {
        if !self.registered_streams.contains_key(name) {
            return Err(SqlError::StreamError {
                stream_name: name.to_string(),
                message: "Stream not found".to_string()
            });
        }

        self.registered_streams.remove(name);
        self.schemas.remove(name);
        Ok(())
    }

    pub fn execute_query(&self, sql: &str) -> Result<StreamHandle, SqlError> {
        let query = self.parser.parse(sql)?;
        self.validate_query(&query)?;
        self.create_execution_plan(query)
    }

    pub fn get_stream_schema(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name)
    }

    pub fn list_streams(&self) -> Vec<String> {
        self.registered_streams.keys().cloned().collect()
    }

    fn validate_query(&self, query: &StreamingQuery) -> Result<(), SqlError> {
        match query {
            StreamingQuery::Select { from, fields, where_clause, .. } => {
                // Extract stream name from StreamSource
                let stream_name = match from {
                    crate::ferris::sql::ast::StreamSource::Stream(name) => name,
                    crate::ferris::sql::ast::StreamSource::Table(name) => name,
                    crate::ferris::sql::ast::StreamSource::Subquery(_) => {
                        return Err(SqlError::ParseError {
                            message: "Subqueries not yet supported".to_string(),
                            position: None
                        });
                    }
                };

                if !self.registered_streams.contains_key(stream_name) {
                    return Err(SqlError::StreamError {
                        stream_name: stream_name.clone(),
                        message: "Stream not found".to_string()
                    });
                }

                let schema = self.schemas.get(stream_name)
                    .ok_or_else(|| SqlError::StreamError {
                        stream_name: stream_name.clone(),
                        message: "Stream not found".to_string()
                    })?;

                // Validate select fields
                for field in fields {
                    match field {
                        crate::ferris::sql::ast::SelectField::Wildcard => {},
                        crate::ferris::sql::ast::SelectField::Column(name) => {
                            if !schema.has_field(name) {
                                return Err(SqlError::SchemaError {
                                    message: "Column not found".to_string(),
                                    column: Some(name.clone())
                                });
                            }
                        },
                        crate::ferris::sql::ast::SelectField::AliasedColumn { column, .. } => {
                            if !schema.has_field(column) {
                                return Err(SqlError::SchemaError {
                                    message: "Column not found".to_string(),
                                    column: Some(column.clone())
                                });
                            }
                        },
                        crate::ferris::sql::ast::SelectField::Expression { expr, .. } => {
                            self.validate_expression(expr, schema)?;
                        }
                    }
                }

                // Validate where clause
                if let Some(where_expr) = where_clause {
                    self.validate_expression(where_expr, schema)?;
                }

                Ok(())
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                // Validate the underlying SELECT query
                self.validate_query(as_select)
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                // Validate the underlying SELECT query
                self.validate_query(as_select)
            }
        }
    }

    fn validate_expression(
        &self, 
        expr: &crate::ferris::sql::ast::Expr, 
        schema: &Schema
    ) -> Result<(), SqlError> {
        match expr {
            crate::ferris::sql::ast::Expr::Column(name) => {
                if !schema.has_field(name) {
                    return Err(SqlError::SchemaError {
                        message: "Column not found".to_string(),
                        column: Some(name.clone())
                    });
                }
                Ok(())
            },
            crate::ferris::sql::ast::Expr::Literal(_) => Ok(()),
            crate::ferris::sql::ast::Expr::BinaryOp { left, right, .. } => {
                self.validate_expression(left, schema)?;
                self.validate_expression(right, schema)
            }
            crate::ferris::sql::ast::Expr::Function { args, .. } => {
                for arg in args {
                    self.validate_expression(arg, schema)?;
                }
                Ok(())
            }
            _ => Err(SqlError::ParseError {
                message: "Unsupported expression type".to_string(),
                position: None
            })
        }
    }

    fn create_execution_plan(&self, query: StreamingQuery) -> Result<StreamHandle, SqlError> {
        let stream_name = match &query {
            StreamingQuery::Select { from, .. } => {
                match from {
                    crate::ferris::sql::ast::StreamSource::Stream(name) => name,
                    crate::ferris::sql::ast::StreamSource::Table(name) => name,
                    crate::ferris::sql::ast::StreamSource::Subquery(_) => {
                        return Err(SqlError::ParseError {
                            message: "Subqueries not yet supported".to_string(),
                            position: None
                        });
                    }
                }
            }
            StreamingQuery::CreateStream { name, .. } => name,
            StreamingQuery::CreateTable { name, .. } => name,
        };

        let _source_handle = self.registered_streams.get(stream_name)
            .ok_or_else(|| SqlError::StreamError {
                stream_name: stream_name.clone(),
                message: "Stream not found".to_string()
            })?;

        let execution_id = format!("query_{}", uuid::Uuid::new_v4());
        
        Ok(StreamHandle {
            id: execution_id,
            topic: format!("result_{}", stream_name),
            schema_id: format!("schema_{}", stream_name),
        })
    }
}

impl Default for StreamingSqlContext {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SqlQueryExecutor {
    context: Arc<StreamingSqlContext>,
}

impl SqlQueryExecutor {
    pub fn new(context: Arc<StreamingSqlContext>) -> Self {
        Self { context }
    }

    pub async fn execute(&self, sql: &str) -> Result<StreamHandle, SqlError> {
        self.context.execute_query(sql)
    }

    pub fn explain(&self, sql: &str) -> Result<String, SqlError> {
        let query = self.context.parser.parse(sql)?;
        Ok(format!("Execution plan for: {:#?}", query))
    }
}