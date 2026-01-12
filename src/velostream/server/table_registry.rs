//! Table Registry - Manages shared tables for CTAS (CREATE TABLE AS SELECT) operations
//!
//! This module provides a centralized registry for managing tables created via CTAS
//! that can be referenced by multiple streaming SQL jobs. It handles table lifecycle,
//! background population jobs, health monitoring, and real-time progress tracking.

use crate::velostream::server::progress_monitoring::{
    LoadingSummary, ProgressMonitor, TableLoadProgress, TableProgressTracker,
};
use crate::velostream::sql::{SqlError, ast::StreamingQuery};
use crate::velostream::table::{CtasExecutor, UnifiedTable};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
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
#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub enum TableStatus {
    Active,
    Populating,
    BackgroundJobFinished,
    NoBackgroundJob,
    Error(String),
}

impl std::fmt::Display for TableStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableStatus::Active => write!(f, "Active"),
            TableStatus::Populating => write!(f, "Populating"),
            TableStatus::BackgroundJobFinished => write!(f, "BackgroundJobFinished"),
            TableStatus::NoBackgroundJob => write!(f, "NoBackgroundJob"),
            TableStatus::Error(msg) => write!(f, "Error({})", msg),
        }
    }
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
    tables: Arc<RwLock<HashMap<String, Arc<dyn UnifiedTable>>>>,

    /// Background table population jobs
    background_jobs: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,

    /// Table metadata and statistics
    metadata: Arc<RwLock<HashMap<String, TableMetadata>>>,

    /// CTAS executor for creating new tables
    ctas_executor: CtasExecutor,

    /// Registry configuration
    config: TableRegistryConfig,

    /// Progress monitoring for table loading operations
    progress_monitor: Arc<ProgressMonitor>,
}

impl Default for TableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TableRegistry {
    /// Create a new table registry with default configuration
    pub fn new() -> Self {
        Self::with_config(TableRegistryConfig::default())
    }

    /// Create a new table registry with custom configuration
    pub fn with_config(config: TableRegistryConfig) -> Self {
        let ctas_executor =
            CtasExecutor::new(config.kafka_brokers.clone(), config.base_group_id.clone());

        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            background_jobs: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            ctas_executor,
            config,
            progress_monitor: Arc::new(ProgressMonitor::new()),
        }
    }

    /// Register a table directly (for testing or external table sources)
    pub async fn register_table(
        &self,
        name: String,
        table: Arc<dyn UnifiedTable>,
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

    /// Load a file_source table from its config file.
    ///
    /// This handles the full table lifecycle:
    /// 1. Creates metadata with Populating status
    /// 2. Loads and parses config file (async)
    /// 3. Loads and parses data file (async)
    /// 4. Creates and populates table
    /// 5. Registers table and updates status to Active
    ///
    /// # Arguments
    /// * `table_name` - Name to register the table under
    /// * `config_path` - Path to the YAML config file
    /// * `base_dir` - Optional base directory for resolving relative paths
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of rows loaded
    /// * `Err(SqlError)` - If loading fails
    pub async fn load_file_source_table(
        &self,
        table_name: &str,
        config_path: &str,
        base_dir: Option<&std::path::Path>,
    ) -> Result<usize, SqlError> {
        use crate::velostream::sql::execution::types::FieldValue;
        use crate::velostream::table::OptimizedTableImpl;

        info!(
            "Loading file_source table '{}' from config: {}",
            table_name, config_path
        );

        // Check capacity
        {
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
            if tables.contains_key(table_name) {
                return Err(SqlError::ExecutionError {
                    message: format!("Table '{}' already exists in registry", table_name),
                    query: None,
                });
            }
        }

        // Create metadata entry with Populating status
        {
            let mut metadata = self.metadata.write().await;
            metadata.insert(
                table_name.to_string(),
                TableMetadata {
                    name: table_name.to_string(),
                    status: TableStatus::Populating,
                    record_count: 0,
                    created_at: std::time::SystemTime::now(),
                    last_updated: std::time::SystemTime::now(),
                    size_bytes: None,
                    schema: None,
                },
            );
        }

        // Store table_name for status updates
        let table_name_owned = table_name.to_string();

        // Resolve relative config path against base_dir if available
        let resolved_path = if let Some(base) = base_dir {
            let path = std::path::Path::new(config_path);
            if path.is_relative() {
                base.join(path)
            } else {
                path.to_path_buf()
            }
        } else {
            std::path::PathBuf::from(config_path)
        };

        debug!(
            "Resolved config path for table '{}': {:?}",
            table_name, resolved_path
        );

        // Read and parse the config file (async to avoid blocking the runtime)
        let config_content = match tokio::fs::read_to_string(&resolved_path).await {
            Ok(content) => content,
            Err(e) => {
                let error_msg = format!(
                    "Failed to read config file for table '{}': {} (path: {:?})",
                    table_name, e, resolved_path
                );
                // Update status to Error
                {
                    let mut metadata = self.metadata.write().await;
                    if let Some(meta) = metadata.get_mut(&table_name_owned) {
                        meta.status = TableStatus::Error(error_msg.clone());
                        meta.last_updated = std::time::SystemTime::now();
                    }
                }
                return Err(SqlError::ExecutionError {
                    message: error_msg,
                    query: None,
                });
            }
        };

        let config: serde_yaml::Value = match serde_yaml::from_str(&config_content) {
            Ok(config) => config,
            Err(e) => {
                let error_msg = format!(
                    "Failed to parse config file for table '{}': {}",
                    table_name, e
                );
                // Update status to Error
                {
                    let mut metadata = self.metadata.write().await;
                    if let Some(meta) = metadata.get_mut(&table_name_owned) {
                        meta.status = TableStatus::Error(error_msg.clone());
                        meta.last_updated = std::time::SystemTime::now();
                    }
                }
                return Err(SqlError::ExecutionError {
                    message: error_msg,
                    query: None,
                });
            }
        };

        // Extract file path from config
        let file_path = config
            .get("file")
            .and_then(|f| f.get("path"))
            .and_then(|p| p.as_str())
            .ok_or_else(|| {
                let error_msg = format!(
                    "Config file for table '{}' missing 'file.path' field",
                    table_name
                );
                SqlError::ExecutionError {
                    message: error_msg,
                    query: None,
                }
            })?;

        // Resolve file path relative to config file directory
        let config_dir = resolved_path.parent().unwrap_or(std::path::Path::new("."));
        let data_path = config_dir.join(file_path);

        debug!("Loading table '{}' data from: {:?}", table_name, data_path);

        // Read file contents (async to avoid blocking the runtime)
        let file_content = match tokio::fs::read_to_string(&data_path).await {
            Ok(content) => content,
            Err(e) => {
                let error_msg = format!(
                    "Failed to read data file for table '{}': {} (path: {:?})",
                    table_name, e, data_path
                );
                // Update status to Error
                {
                    let mut metadata = self.metadata.write().await;
                    if let Some(meta) = metadata.get_mut(&table_name_owned) {
                        meta.status = TableStatus::Error(error_msg.clone());
                        meta.last_updated = std::time::SystemTime::now();
                    }
                }
                return Err(SqlError::ExecutionError {
                    message: error_msg,
                    query: None,
                });
            }
        };

        // Parse CSV and create table
        let table = Arc::new(OptimizedTableImpl::new());

        // Parse CSV with header
        let mut lines = file_content.lines();
        let headers: Vec<String> = if let Some(header_line) = lines.next() {
            header_line
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        } else {
            let error_msg = format!("Data file for table '{}' is empty", table_name);
            // Update status to Error
            {
                let mut metadata = self.metadata.write().await;
                if let Some(meta) = metadata.get_mut(&table_name_owned) {
                    meta.status = TableStatus::Error(error_msg.clone());
                    meta.last_updated = std::time::SystemTime::now();
                }
            }
            return Err(SqlError::ExecutionError {
                message: error_msg,
                query: None,
            });
        };

        // Find key field from config (first field with key: true, or first field)
        let key_field = config
            .get("schema")
            .and_then(|s| s.get("fields"))
            .and_then(|f| f.as_sequence())
            .and_then(|fields| {
                fields.iter().find_map(|f| {
                    if f.get("key").and_then(|k| k.as_bool()).unwrap_or(false) {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                })
            })
            .unwrap_or_else(|| headers.first().cloned().unwrap_or_default());

        debug!(
            "Using '{}' as key field for table '{}'",
            key_field, table_name
        );

        // Load rows into table
        let mut row_count = 0;
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }

            let values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            let mut row = std::collections::HashMap::new();
            let mut key_value = String::new();

            for (i, header) in headers.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    // Parse value - try integer first, then keep as string
                    let field_value = if let Ok(int_val) = value.parse::<i64>() {
                        FieldValue::Integer(int_val)
                    } else {
                        FieldValue::String(value.to_string())
                    };
                    row.insert(header.clone(), field_value);

                    // Capture key value
                    if header == &key_field {
                        key_value = value.to_string();
                    }
                }
            }

            if let Err(e) = table.insert(key_value, row) {
                warn!("Failed to insert row into table '{}': {}", table_name, e);
            } else {
                row_count += 1;
            }
        }

        // Register table in registry
        {
            let mut tables = self.tables.write().await;
            tables.insert(table_name.to_string(), table);
        }

        // Update metadata to Active status
        {
            let mut metadata = self.metadata.write().await;
            if let Some(meta) = metadata.get_mut(table_name) {
                meta.status = TableStatus::Active;
                meta.record_count = row_count;
                meta.last_updated = std::time::SystemTime::now();
            }
        }

        info!(
            "Successfully loaded {} rows into table '{}' (columns: {:?})",
            row_count, table_name, headers
        );

        Ok(row_count)
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

        info!(
            "Successfully created and registered table '{}'",
            result.table_name
        );
        Ok(result.table_name)
    }

    /// Get a reference to a table
    pub async fn get_table(&self, table_name: &str) -> Result<Arc<dyn UnifiedTable>, SqlError> {
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
            info!(
                "Stopped background population job for table '{}'",
                table_name
            );
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
                TableStatus::Active
                    | TableStatus::BackgroundJobFinished
                    | TableStatus::NoBackgroundJob
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
                warn!(
                    "Failed to drop table '{}' during cleanup: {:?}",
                    table_name, e
                );
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
        F: Fn(&HashMap<String, Arc<dyn UnifiedTable>>),
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

    /// Wait for a table to be ready for use (fully loaded)
    ///
    /// This is CRITICAL for stream-table joins to ensure data consistency.
    /// Streams MUST wait for tables to be ready before starting processing.
    pub async fn wait_for_table_ready(
        &self,
        table_name: &str,
        timeout: std::time::Duration,
    ) -> Result<TableStatus, SqlError> {
        let start_time = std::time::Instant::now();
        let mut check_interval = std::time::Duration::from_millis(100); // Start with 100ms
        let max_interval = std::time::Duration::from_secs(2); // Cap at 2 seconds

        info!(
            "Waiting for table '{}' to be ready (timeout: {:?})",
            table_name, timeout
        );

        loop {
            // Check if timeout exceeded
            if start_time.elapsed() > timeout {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Timeout waiting for table '{}' to be ready after {:?}",
                        table_name, timeout
                    ),
                    query: None,
                });
            }

            // Check table existence
            let tables = self.tables.read().await;
            if !tables.contains_key(table_name) {
                drop(tables);
                return Err(SqlError::ExecutionError {
                    message: format!("Table '{}' does not exist in registry", table_name),
                    query: None,
                });
            }
            drop(tables);

            // Check table status
            let jobs = self.background_jobs.read().await;
            let metadata = self.metadata.read().await;

            let status = if let Some(job) = jobs.get(table_name) {
                if job.is_finished() {
                    // Job completed - table is ready
                    info!(
                        "Table '{}' background job completed, table is ready",
                        table_name
                    );
                    drop(jobs);
                    drop(metadata);

                    // Update metadata status
                    self.update_table_metadata(table_name, |meta| {
                        meta.status = TableStatus::BackgroundJobFinished;
                    })
                    .await?;

                    return Ok(TableStatus::BackgroundJobFinished);
                } else {
                    // Still loading
                    let meta = metadata.get(table_name);
                    let record_count = meta.map_or(0, |m| m.record_count);
                    debug!(
                        "Table '{}' still loading... ({} records loaded)",
                        table_name, record_count
                    );
                    TableStatus::Populating
                }
            } else {
                // No background job - table is immediately ready
                info!(
                    "Table '{}' has no background job, immediately ready",
                    table_name
                );
                drop(jobs);
                drop(metadata);
                return Ok(TableStatus::Active);
            };

            drop(jobs);
            drop(metadata);

            // Wait with exponential backoff
            tokio::time::sleep(check_interval).await;

            // Increase interval with exponential backoff (capped)
            check_interval = std::cmp::min(check_interval * 2, max_interval);
        }
    }

    /// Wait for multiple tables to be ready
    pub async fn wait_for_tables_ready(
        &self,
        table_names: &[String],
        timeout: std::time::Duration,
    ) -> Result<Vec<(String, TableStatus)>, SqlError> {
        let start_time = std::time::Instant::now();
        let mut results = Vec::new();

        for table_name in table_names {
            // Calculate remaining timeout for each table
            let elapsed = start_time.elapsed();
            if elapsed >= timeout {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Timeout waiting for tables - failed at table '{}'",
                        table_name
                    ),
                    query: None,
                });
            }
            let remaining_timeout = timeout - elapsed;

            // Wait for this table
            let status = self
                .wait_for_table_ready(table_name, remaining_timeout)
                .await?;
            results.push((table_name.clone(), status));
        }

        info!("All {} tables are ready for use", table_names.len());
        Ok(results)
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
                properties,
                ..
            } => {
                // Extract main FROM table only if it's not a FROM...WITH external data source
                // If properties are present, this is a FROM...WITH clause defining an external source
                if properties.is_none() {
                    Self::extract_from_stream_source(from, tables);
                }

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
            StreamingQuery::CreateStream { as_select, .. } => {
                // For CREATE STREAM AS SELECT, extract from the SELECT part
                Self::extract_tables_recursive(as_select, tables);
            }
            _ => {}
        }
    }

    /// Extract table names from stream sources
    ///
    /// Both Table and Stream references are added as potential table dependencies.
    /// The parser creates StreamSource::Stream for all named references since it
    /// can't distinguish tables from streams at parse time. The actual check for
    /// whether something exists in the table registry happens in the caller.
    fn extract_from_stream_source(
        source: &crate::velostream::sql::ast::StreamSource,
        tables: &mut Vec<String>,
    ) {
        use crate::velostream::sql::ast::StreamSource;

        match source {
            StreamSource::Table(name) => {
                // Explicit table reference
                tables.push(name.clone());
            }
            StreamSource::Stream(name) => {
                // Stream references could be table dependencies (e.g., in subqueries
                // or JOINs with file_source tables). The caller checks whether each
                // name actually exists in the table registry.
                tables.push(name.clone());
            }
            StreamSource::Subquery(subquery) => {
                Self::extract_tables_recursive(subquery, tables);
            }
            _ => {
                // URI sources are not tables in the registry
            }
        }
    }

    /// Extract table names from expressions
    fn extract_from_expression(expr: &crate::velostream::sql::ast::Expr, tables: &mut Vec<String>) {
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
            Expr::Between {
                expr, low, high, ..
            } => {
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

    // ========== PROGRESS MONITORING METHODS ==========

    /// Start tracking progress for a table loading operation
    pub async fn start_progress_tracking(
        &self,
        table_name: String,
        total_records_expected: Option<usize>,
    ) -> Arc<TableProgressTracker> {
        info!(
            "Starting progress tracking for table '{}' with expected {} records",
            table_name,
            total_records_expected.map_or("unknown".to_string(), |n| n.to_string())
        );

        self.progress_monitor
            .start_tracking(table_name, total_records_expected)
            .await
    }

    /// Stop tracking progress for a table (when loading completes or fails)
    pub async fn stop_progress_tracking(&self, table_name: &str) {
        info!("Stopping progress tracking for table '{}'", table_name);
        self.progress_monitor.stop_tracking(table_name).await
    }

    /// Get loading progress for all actively loading tables
    pub async fn get_loading_progress(&self) -> HashMap<String, TableLoadProgress> {
        self.progress_monitor.get_all_progress().await
    }

    /// Get loading progress for a specific table
    pub async fn get_table_loading_progress(&self, table_name: &str) -> Option<TableLoadProgress> {
        self.progress_monitor.get_table_progress(table_name).await
    }

    /// Subscribe to real-time progress updates for all tables
    pub fn subscribe_to_progress(&self) -> broadcast::Receiver<HashMap<String, TableLoadProgress>> {
        self.progress_monitor.subscribe_to_global_progress()
    }

    /// Subscribe to progress updates for a specific table
    pub async fn subscribe_to_table_progress(
        &self,
        table_name: &str,
    ) -> Option<broadcast::Receiver<TableLoadProgress>> {
        self.progress_monitor
            .subscribe_to_table_progress(table_name)
            .await
    }

    /// Get summary statistics for all loading operations
    pub async fn get_loading_summary(&self) -> LoadingSummary {
        self.progress_monitor.get_loading_summary().await
    }

    /// Wait for a table to be ready with real-time progress monitoring
    pub async fn wait_for_table_ready_with_progress(
        &self,
        table_name: &str,
        timeout: Duration,
    ) -> Result<TableStatus, SqlError> {
        let start_time = std::time::Instant::now();
        let progress_interval = Duration::from_millis(500); // Check progress every 500ms

        info!(
            "Waiting for table '{}' to be ready (timeout: {:?})",
            table_name, timeout
        );

        // Subscribe to progress updates for real-time monitoring
        if let Some(mut progress_receiver) = self.subscribe_to_table_progress(table_name).await {
            // Monitor progress while waiting
            let mut last_log = std::time::Instant::now();

            loop {
                // Check if table is ready
                if let Some(metadata) = self.get_table_stats(table_name).await {
                    match metadata.status {
                        TableStatus::Active
                        | TableStatus::BackgroundJobFinished
                        | TableStatus::NoBackgroundJob => {
                            info!("Table '{}' is ready for use", table_name);
                            return Ok(metadata.status);
                        }
                        TableStatus::Error(msg) => {
                            warn!("Table '{}' failed to load: {}", table_name, msg);
                            return Err(SqlError::ExecutionError {
                                message: format!("Table '{}' failed to load: {}", table_name, msg),
                                query: None,
                            });
                        }
                        TableStatus::Populating => {
                            // Log progress periodically
                            if last_log.elapsed() >= Duration::from_secs(5) {
                                if let Some(progress) =
                                    self.get_table_loading_progress(table_name).await
                                {
                                    info!(
                                        "Table '{}' loading progress: {} records loaded at {:.1} records/sec",
                                        table_name, progress.records_loaded, progress.loading_rate
                                    );
                                    if let Some(eta) = progress.estimated_completion {
                                        info!(
                                            "Table '{}' ETA: {}",
                                            table_name,
                                            eta.format("%Y-%m-%d %H:%M:%S UTC")
                                        );
                                    }
                                }
                                last_log = std::time::Instant::now();
                            }
                        }
                    }
                }

                // Check for timeout
                if start_time.elapsed() >= timeout {
                    warn!(
                        "Timeout waiting for table '{}' after {:?}",
                        table_name, timeout
                    );
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "Timeout waiting for table '{}' after {:?}",
                            table_name, timeout
                        ),
                        query: None,
                    });
                }

                // Wait for progress update or timeout
                tokio::select! {
                    progress_result = progress_receiver.recv() => {
                        if let Ok(progress) = progress_result {
                            debug!("Progress update for table '{}': {} records loaded",
                                   table_name, progress.records_loaded);
                        }
                    }
                    _ = tokio::time::sleep(progress_interval) => {
                        // Continue to next iteration
                    }
                }
            }
        } else {
            // Fall back to original wait_for_table_ready if no progress tracking
            warn!(
                "No progress tracking available for table '{}', falling back to basic wait",
                table_name
            );
            self.wait_for_table_ready(table_name, timeout).await
        }
    }

    /// Enhanced health status that includes loading progress
    pub async fn get_enhanced_health_status(&self) -> Vec<EnhancedTableHealth> {
        let basic_health = self.get_health_status().await;
        let loading_progress = self.get_loading_progress().await;

        let mut enhanced_health = Vec::new();

        for health in basic_health {
            let progress = loading_progress.get(&health.table_name).cloned();
            enhanced_health.push(EnhancedTableHealth {
                table_name: health.table_name,
                status: health.status,
                is_healthy: health.is_healthy,
                issues: health.issues,
                loading_progress: progress,
            });
        }

        enhanced_health
    }
}

/// Enhanced health information that includes loading progress
#[derive(Debug, Clone, serde::Serialize)]
pub struct EnhancedTableHealth {
    pub table_name: String,
    pub status: TableStatus,
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub loading_progress: Option<TableLoadProgress>,
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
            progress_monitor: Arc::clone(&self.progress_monitor),
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
