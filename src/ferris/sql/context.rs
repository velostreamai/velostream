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
            return Err(SqlError::StreamAlreadyExists(name));
        }

        self.registered_streams.insert(name.clone(), handle);
        self.schemas.insert(name, schema);
        Ok(())
    }

    pub fn unregister_stream(&mut self, name: &str) -> Result<(), SqlError> {
        if !self.registered_streams.contains_key(name) {
            return Err(SqlError::StreamNotFound(name.to_string()));
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
        if !self.registered_streams.contains_key(&query.from_stream) {
            return Err(SqlError::StreamNotFound(query.from_stream.clone()));
        }

        let schema = self.schemas.get(&query.from_stream)
            .ok_or_else(|| SqlError::StreamNotFound(query.from_stream.clone()))?;

        for field in &query.select_fields {
            match field {
                crate::ferris::sql::ast::SelectField::Wildcard => {},
                crate::ferris::sql::ast::SelectField::Named { expr, .. } => {
                    self.validate_expression(expr, schema)?;
                }
            }
        }

        if let Some(where_expr) = &query.where_clause {
            self.validate_expression(where_expr, schema)?;
        }

        Ok(())
    }

    fn validate_expression(
        &self, 
        expr: &crate::ferris::sql::ast::Expr, 
        schema: &Schema
    ) -> Result<(), SqlError> {
        match expr {
            crate::ferris::sql::ast::Expr::Column { table: _, name } => {
                if !schema.has_field(name) {
                    return Err(SqlError::ColumnNotFound(name.clone()));
                }
                Ok(())
            }
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
        }
    }

    fn create_execution_plan(&self, query: StreamingQuery) -> Result<StreamHandle, SqlError> {
        let source_handle = self.registered_streams.get(&query.from_stream)
            .ok_or_else(|| SqlError::StreamNotFound(query.from_stream.clone()))?;

        let execution_id = format!("query_{}", uuid::Uuid::new_v4());
        
        Ok(StreamHandle {
            id: execution_id,
            topic: format!("result_{}", query.from_stream),
            schema_id: format!("schema_{}", query.from_stream),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::schema::{DataType, FieldDefinition};

    #[test]
    fn test_context_creation() {
        let context = StreamingSqlContext::new();
        assert_eq!(context.list_streams().len(), 0);
    }

    #[test]
    fn test_stream_registration() {
        let mut context = StreamingSqlContext::new();
        
        let handle = StreamHandle {
            id: "test_stream".to_string(),
            topic: "test_topic".to_string(),
            schema_id: "test_schema".to_string(),
        };
        
        let schema = Schema {
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                FieldDefinition {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
        };
        
        let result = context.register_stream("orders".to_string(), handle, schema);
        assert!(result.is_ok());
        assert_eq!(context.list_streams().len(), 1);
    }

    #[test]
    fn test_duplicate_stream_registration() {
        let mut context = StreamingSqlContext::new();
        
        let handle = StreamHandle {
            id: "test_stream".to_string(),
            topic: "test_topic".to_string(),
            schema_id: "test_schema".to_string(),
        };
        
        let schema = Schema {
            fields: vec![],
        };
        
        context.register_stream("orders".to_string(), handle.clone(), schema.clone()).unwrap();
        let result = context.register_stream("orders".to_string(), handle, schema);
        
        assert!(result.is_err());
        matches!(result.unwrap_err(), SqlError::StreamAlreadyExists(_));
    }
}