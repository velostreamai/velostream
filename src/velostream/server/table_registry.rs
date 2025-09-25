//! Table Registry - Manages shared tables for CTAS (CREATE TABLE AS SELECT) operations
//!
//! This module provides a centralized registry for managing tables created via CTAS
//! that can be referenced by multiple streaming SQL jobs. It handles table lifecycle,
//! background population jobs, and health monitoring.

use crate::velostream::sql::{SqlError, ast::StreamingQuery};
use crate::velostream::table::{CtasExecutor, SqlQueryable};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Statistics and metadata for registered tables
#[derive(Debug, Clone, serde::Serialize)]
pub struct TableMetadata {
    pub name: String,
    pub status: TableStatus,
    pub record_count: usize,
    pub created_at: std::time::SystemTime,
    pub last_updated: std::time::SystemTime,
    pub size_bytes: Option<u64>,
    pub schema: Option<String>,
}

/// Status of a table in the registry
#[derive(Debug, Clone, serde::Serialize)]
pub enum TableStatus {
    Active,
    Populating,
    BackgroundJobFinished,
    NoBackgroundJob,
    Error(String),
}

/// Health information for a table
#[derive(Debug, Clone, serde::Serialize)]
pub struct TableHealth {
    pub table_name: String,
    pub status: TableStatus,
    pub is_healthy: bool,
    pub issues: Vec<String>,
}

/// Configuration for the table registry
#[derive(Debug, Clone)]
pub struct TableRegistryConfig {
    pub max_tables: usize,
    pub enable_ttl: bool,
    pub ttl_duration_secs: Option<u64>,
    pub kafka_brokers: String,
    pub base_group_id: String,
}

impl Default for TableRegistryConfig {
    fn default() -> Self {
        Self {
            max_tables: 100,
            enable_ttl: false,
            ttl_duration_secs: None,
            kafka_brokers: "localhost:9092".to_string(),
            base_group_id: "velostream".to_string(),
        }
    }
}

/// Object-oriented table registry for managing shared SQL tables
pub struct TableRegistry {
    /// Core table storage
    tables: Arc<RwLock<HashMap<String, Arc<dyn SqlQueryable + Send + Sync>>>>,

    /// Background table population jobs
    background_jobs: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,

    /// Table metadata and statistics
    metadata: Arc<RwLock<HashMap<String, TableMetadata>>>,

    /// CTAS executor for creating new tables
    ctas_executor: CtasExecutor,

    /// Registry configuration
    config: TableRegistryConfig,
}

impl TableRegistry {
    /// Create a new table registry with default configuration
    pub fn new() -> Self {
        Self::with_config(TableRegistryConfig::default())
    }

    /// Create a new table registry with custom configuration
    pub fn with_config(config: TableRegistryConfig) -> Self {
        let ctas_executor = CtasExecutor::new(
            config.kafka_brokers.clone(),
            config.base_group_id.clone(),
        );

        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            background_jobs: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            ctas_executor,
            config,
        }
    }

    /// Register a table directly (for testing or external table sources)
    pub async fn register_table(
        &self,
        name: String,
        table: Arc<dyn SqlQueryable + Send + Sync>,
    ) -> Result<(), SqlError> {
        info!("Registering table '{}' directly", name);

        // Check capacity
        let tables = self.tables.read().await;
        if tables.len() >= self.config.max_tables {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "Table registry at maximum capacity ({}). Drop unused tables or increase limit.",
                    self.config.max_tables
                ),
                query: None,
            });
        }

        // Check for duplicates
        if tables.contains_key(&name) {
            return Err(SqlError::ExecutionError {
                message: format!("Table '{}' already exists in registry", name),
                query: None,
            });
        }
        drop(tables);

        // Register the table
        let mut tables = self.tables.write().await;
        tables.insert(name.clone(), table);
        drop(tables);

        // Create metadata entry
        let mut metadata = self.metadata.write().await;
        metadata.insert(
            name.clone(),
            TableMetadata {
                name: name.clone(),
                status: TableStatus::Active,
                record_count: 0,
                created_at: std::time::SystemTime::now(),
                last_updated: std::time::SystemTime::now(),
                size_bytes: None,
                schema: None,
            },
        );
        drop(metadata);

        info!("Successfully registered table '{}'", name);
        Ok(())
    }

    /// Create a new table via CREATE TABLE AS SELECT
    pub async fn create_table(&self, ctas_query: String) -> Result<String, SqlError> {
        info!("Creating table via CTAS: {}", ctas_query);

        // Parse query to extract table name
        let parser = crate::velostream::sql::StreamingSqlParser::new();
        let parsed_query = parser.parse(&ctas_query)?;

        let table_name = match &parsed_query {
            StreamingQuery::CreateTable { name, .. } => name.clone(),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "Query is not a CREATE TABLE statement".to_string(),
                    query: Some(ctas_query),
                });
            }
        };

        // Check if table already exists
        let tables = self.tables.read().await;
        if tables.contains_key(&table_name) {
            return Err(SqlError::ExecutionError {
                message: format!("Table '{}' already exists", table_name),
                query: Some(ctas_query),
            });
        }

        // Check capacity
        if tables.len() >= self.config.max_tables {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "Table registry at maximum capacity ({}). Drop unused tables or increase limit.",
                    self.config.max_tables
                ),
                query: Some(ctas_query),
            });
        }
        drop(tables);

        // Execute CTAS
        let result = self.ctas_executor.execute(&ctas_query).await?;

        // Register the created table
        let mut tables = self.tables.write().await;
        tables.insert(result.table_name.clone(), result.table);
        drop(tables);

        // Register background job
        let mut jobs = self.background_jobs.write().await;
        jobs.insert(result.table_name.clone(), result.background_job);
        drop(jobs);

        // Create metadata entry
        let mut metadata = self.metadata.write().await;
        metadata.insert(
            result.table_name.clone(),
            TableMetadata {
                name: result.table_name.clone(),
                status: TableStatus::Populating,
                record_count: 0,
                created_at: std::time::SystemTime::now(),
                last_updated: std::time::SystemTime::now(),
                size_bytes: None,
                schema: None,
            },
        );
        drop(metadata);

        info!("Successfully created and registered table '{}'", result.table_name);
        Ok(result.table_name)
    }

    /// Get a reference to a table
    pub async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<Arc<dyn SqlQueryable + Send + Sync>, SqlError> {
        let tables = self.tables.read().await;
        match tables.get(table_name) {
            Some(table) => Ok(Arc::clone(table)),
            None => {
                // Provide helpful error message with available tables
                let available_tables: Vec<String> = tables.keys().cloned().collect();
                let available_msg = if available_tables.is_empty() {
                    "No tables are currently registered.".to_string()
                } else {
                    format!("Available tables: {}", available_tables.join(", "))
                };

                Err(SqlError::ExecutionError {
                    message: format!(
                        "Table '{}' not found. {}. Create it first using: CREATE TABLE {} AS SELECT...",
                        table_name, available_msg, table_name
                    ),
                    query: None,
                })
            }
        }
    }

    /// Check if a table exists
    pub async fn exists(&self, table_name: &str) -> bool {
        let tables = self.tables.read().await;
        tables.contains_key(table_name)
    }

    /// List all registered tables
    pub async fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        tables.keys().cloned().collect()
    }

    /// Get detailed list of tables with metadata
    pub async fn list_tables_with_metadata(&self) -> Vec<TableMetadata> {
        let metadata = self.metadata.read().await;
        metadata.values().cloned().collect()
    }

    /// Drop a table and stop its background job
    pub async fn drop_table(&self, table_name: &str) -> Result<(), SqlError> {
        info!("Dropping table '{}'", table_name);

        // Remove from table registry
        let mut tables = self.tables.write().await;
        let removed = tables.remove(table_name);
        drop(tables);

        if removed.is_none() {
            return Err(SqlError::ExecutionError {
                message: format!("Table '{}' not found", table_name),
                query: None,
            });
        }

        // Stop background job if exists
        let mut jobs = self.background_jobs.write().await;
        if let Some(job_handle) = jobs.remove(table_name) {
            job_handle.abort();
            info!("Stopped background population job for table '{}'", table_name);
        }
        drop(jobs);

        // Remove metadata
        let mut metadata = self.metadata.write().await;
        metadata.remove(table_name);
        drop(metadata);

        info!("Successfully dropped table '{}'", table_name);
        Ok(())
    }

    /// Get statistics for a specific table
    pub async fn get_table_stats(&self, table_name: &str) -> Option<TableMetadata> {
        let metadata = self.metadata.read().await;
        metadata.get(table_name).cloned()
    }

    /// Get statistics for all tables
    pub async fn get_all_table_stats(&self) -> HashMap<String, TableMetadata> {
        let metadata = self.metadata.read().await;
        metadata.clone()
    }

    /// Get health status of all tables
    pub async fn get_health_status(&self) -> Vec<TableHealth> {
        let tables = self.tables.read().await;
        let jobs = self.background_jobs.read().await;
        let metadata = self.metadata.read().await;

        let mut health_reports = Vec::new();

        for table_name in tables.keys() {
            let mut issues = Vec::new();

            // Check background job status
            let status = if let Some(job) = jobs.get(table_name) {
                if job.is_finished() {
                    TableStatus::BackgroundJobFinished
                } else {
                    TableStatus::Populating
                }
            } else {
                TableStatus::NoBackgroundJob
            };

            // Check metadata
            let meta = metadata.get(table_name);
            let is_healthy = matches!(
                status,
                TableStatus::Active | TableStatus::BackgroundJobFinished | TableStatus::NoBackgroundJob
            );

            if meta.is_none() {
                issues.push("Missing metadata entry".to_string());
            }

            health_reports.push(TableHealth {
                table_name: table_name.clone(),
                status: meta.map_or(status.clone(), |m| m.status.clone()),
                is_healthy,
                issues,
            });
        }

        health_reports
    }

    /// Update metadata for a table
    pub async fn update_table_metadata(
        &self,
        table_name: &str,
        update_fn: impl FnOnce(&mut TableMetadata),
    ) -> Result<(), SqlError> {
        let mut metadata = self.metadata.write().await;
        match metadata.get_mut(table_name) {
            Some(meta) => {
                update_fn(meta);
                meta.last_updated = std::time::SystemTime::now();
                Ok(())
            }
            None => Err(SqlError::ExecutionError {
                message: format!("Table '{}' metadata not found", table_name),
                query: None,
            }),
        }
    }

    /// Clean up tables based on TTL or other criteria
    pub async fn cleanup_inactive_tables(&self) -> Result<Vec<String>, SqlError> {
        if !self.config.enable_ttl {
            debug!("TTL-based cleanup is disabled");
            return Ok(Vec::new());
        }

        let ttl_duration = match self.config.ttl_duration_secs {
            Some(secs) => std::time::Duration::from_secs(secs),
            None => {
                warn!("TTL enabled but no duration specified");
                return Ok(Vec::new());
            }
        };

        let now = std::time::SystemTime::now();
        let mut tables_to_drop = Vec::new();

        // Identify tables that exceed TTL
        let metadata = self.metadata.read().await;
        for (table_name, meta) in metadata.iter() {
            if let Ok(age) = now.duration_since(meta.last_updated) {
                if age > ttl_duration {
                    tables_to_drop.push(table_name.clone());
                }
            }
        }
        drop(metadata);

        // Drop identified tables
        for table_name in &tables_to_drop {
            if let Err(e) = self.drop_table(table_name).await {
                warn!("Failed to drop table '{}' during cleanup: {:?}", table_name, e);
            } else {
                info!("Dropped inactive table '{}' (exceeded TTL)", table_name);
            }
        }

        Ok(tables_to_drop)
    }

    /// Get the number of registered tables
    pub async fn table_count(&self) -> usize {
        let tables = self.tables.read().await;
        tables.len()
    }

    /// Check if the registry is at capacity
    pub async fn is_at_capacity(&self) -> bool {
        let tables = self.tables.read().await;
        tables.len() >= self.config.max_tables
    }

    /// Get registry configuration
    pub fn config(&self) -> &TableRegistryConfig {
        &self.config
    }

    /// Inject tables into an execution context
    pub async fn inject_tables_into_context<F>(
        &self,
        required_tables: &[String],
        context_customizer: F,
    ) where
        F: Fn(&HashMap<String, Arc<dyn SqlQueryable + Send + Sync>>),
    {
        let tables = self.tables.read().await;
        let mut available_tables = HashMap::new();

        for table_name in required_tables {
            if let Some(table) = tables.get(table_name) {
                available_tables.insert(table_name.clone(), Arc::clone(table));
                info!("Prepared table '{}' for context injection", table_name);
            } else {
                warn!("Table '{}' not found for context injection", table_name);
            }
        }

        context_customizer(&available_tables);
    }

    /// Extract table references from a SQL query
    pub fn extract_table_dependencies(query: &StreamingQuery) -> Vec<String> {
        let mut tables = Vec::new();
        Self::extract_tables_recursive(query, &mut tables);

        // Remove duplicates
        tables.sort();
        tables.dedup();
        tables
    }

    /// Recursively extract table names from query structure
    fn extract_tables_recursive(query: &StreamingQuery, tables: &mut Vec<String>) {
        match query {
            StreamingQuery::Select {
                from,
                joins,
                where_clause,
                ..
            } => {
                // Extract main FROM table
                Self::extract_from_stream_source(from, tables);

                // Extract JOIN tables
                if let Some(join_clauses) = joins {
                    for join in join_clauses {
                        Self::extract_from_stream_source(&join.right_source, tables);
                    }
                }

                // Extract tables from WHERE clause subqueries
                if let Some(where_expr) = where_clause {
                    Self::extract_from_expression(where_expr, tables);
                }
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                // For CREATE TABLE AS SELECT, extract from the SELECT part
                Self::extract_tables_recursive(as_select, tables);
            }
            _ => {}
        }
    }

    /// Extract table names from stream sources
    fn extract_from_stream_source(
        source: &crate::velostream::sql::ast::StreamSource,
        tables: &mut Vec<String>,
    ) {
        use crate::velostream::sql::ast::StreamSource;

        match source {
            StreamSource::Table(name) => {
                tables.push(name.clone());
            }
            StreamSource::Subquery(subquery) => {
                Self::extract_tables_recursive(subquery, tables);
            }
            _ => {
                // URI and Stream sources are not tables in the registry
            }
        }
    }

    /// Extract table names from expressions
    fn extract_from_expression(
        expr: &crate::velostream::sql::ast::Expr,
        tables: &mut Vec<String>,
    ) {
        use crate::velostream::sql::ast::Expr;

        match expr {
            Expr::Subquery { query, .. } => {
                Self::extract_tables_recursive(query, tables);
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::extract_from_expression(left, tables);
                Self::extract_from_expression(right, tables);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::extract_from_expression(arg, tables);
                }
            }
            Expr::UnaryOp { expr, .. } => {
                Self::extract_from_expression(expr, tables);
            }
            Expr::Between { expr, low, high, .. } => {
                Self::extract_from_expression(expr, tables);
                Self::extract_from_expression(low, tables);
                Self::extract_from_expression(high, tables);
            }
            Expr::List(exprs) => {
                for expr in exprs {
                    Self::extract_from_expression(expr, tables);
                }
            }
            _ => {}
        }
    }
}

/// Clone implementation for TableRegistry
impl Clone for TableRegistry {
    fn clone(&self) -> Self {
        Self {
            tables: Arc::clone(&self.tables),
            background_jobs: Arc::clone(&self.background_jobs),
            metadata: Arc::clone(&self.metadata),
            ctas_executor: self.ctas_executor.clone(),
            config: self.config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_registry_basic_operations() {
        let registry = TableRegistry::new();

        // Test listing empty registry
        let tables = registry.list_tables().await;
        assert_eq!(tables.len(), 0);

        // Test checking non-existent table
        assert!(!registry.exists("test_table").await);

        // Test getting non-existent table
        let result = registry.get_table("test_table").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_registry_capacity() {
        let config = TableRegistryConfig {
            max_tables: 2,
            ..Default::default()
        };
        let registry = TableRegistry::with_config(config);

        assert!(!registry.is_at_capacity().await);
        assert_eq!(registry.table_count().await, 0);
    }
}