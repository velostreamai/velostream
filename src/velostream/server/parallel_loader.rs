//! Parallel Table Loading with Dependency Management
//!
//! Coordinates parallel loading of multiple tables while respecting dependencies
//! and resource limits. Uses wave-based loading where each wave contains
//! independent tables that can be loaded concurrently.

use crate::velostream::server::dependency_graph::TableDependencyGraph;
use crate::velostream::server::progress_monitoring::ProgressMonitor;
use crate::velostream::server::table_registry::TableRegistry;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::parser::StreamingSqlParser;
use crate::velostream::table::CtasExecutor;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Configuration for parallel loading operations
#[derive(Debug, Clone)]
pub struct ParallelLoadingConfig {
    /// Maximum number of tables to load concurrently
    pub max_parallel: usize,

    /// Timeout for individual table loading
    pub table_load_timeout: Duration,

    /// Whether to stop loading on first failure
    pub fail_fast: bool,

    /// Whether to continue loading on dependency failures
    pub continue_on_dependency_failure: bool,

    /// Maximum total loading time
    pub total_timeout: Option<Duration>,
}

impl Default for ParallelLoadingConfig {
    fn default() -> Self {
        Self {
            max_parallel: 4,                                // Conservative default
            table_load_timeout: Duration::from_secs(300),   // 5 minutes per table
            fail_fast: false, // Try to load as many tables as possible
            continue_on_dependency_failure: false, // Skip tables with failed dependencies
            total_timeout: Some(Duration::from_secs(1800)), // 30 minutes total
        }
    }
}

impl ParallelLoadingConfig {
    /// Create a fast configuration for testing
    pub fn fast_test() -> Self {
        Self {
            max_parallel: 2,
            table_load_timeout: Duration::from_secs(10),
            fail_fast: false,
            continue_on_dependency_failure: false,
            total_timeout: Some(Duration::from_secs(60)),
        }
    }

    /// Create a configuration with specific parallelism
    pub fn with_max_parallel(max_parallel: usize) -> Self {
        Self {
            max_parallel,
            ..Default::default()
        }
    }
}

/// Result of a parallel loading operation
#[derive(Debug, Clone)]
pub struct ParallelLoadingResult {
    /// Tables that loaded successfully
    pub successful: Vec<String>,

    /// Tables that failed to load (table_name -> error_message)
    pub failed: HashMap<String, String>,

    /// Tables that were skipped due to dependency failures
    pub skipped: Vec<String>,

    /// Total time taken for the loading operation
    pub total_duration: Duration,

    /// Statistics per wave
    pub wave_stats: Vec<WaveStats>,
}

impl ParallelLoadingResult {
    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.successful.len() + self.failed.len() + self.skipped.len();
        if total == 0 {
            return 100.0;
        }
        (self.successful.len() as f64 / total as f64) * 100.0
    }

    /// Check if all tables loaded successfully
    pub fn is_complete_success(&self) -> bool {
        self.failed.is_empty() && self.skipped.is_empty()
    }

    /// Get total number of tables processed
    pub fn total_tables(&self) -> usize {
        self.successful.len() + self.failed.len() + self.skipped.len()
    }
}

/// Statistics for a single loading wave
#[derive(Debug, Clone)]
pub struct WaveStats {
    pub wave_number: usize,
    pub tables: Vec<String>,
    pub duration: Duration,
    pub successful: usize,
    pub failed: usize,
}

/// Parallel table loading coordinator
pub struct ParallelLoader {
    /// Table registry for managing tables
    table_registry: Arc<TableRegistry>,

    /// Progress monitor for real-time updates
    progress_monitor: Arc<ProgressMonitor>,

    /// CTAS executor for creating tables
    ctas_executor: CtasExecutor,

    /// SQL parser for dependency extraction
    sql_parser: StreamingSqlParser,

    /// Configuration
    config: ParallelLoadingConfig,
}

impl ParallelLoader {
    /// Create a new parallel loader
    pub fn new(
        table_registry: Arc<TableRegistry>,
        progress_monitor: Arc<ProgressMonitor>,
        ctas_executor: CtasExecutor,
        config: ParallelLoadingConfig,
    ) -> Self {
        Self {
            table_registry,
            progress_monitor,
            ctas_executor,
            sql_parser: StreamingSqlParser::new(),
            config,
        }
    }

    /// Clone necessary fields for async task (cheap Arc clones)
    fn clone_for_task(&self) -> Self {
        Self {
            table_registry: self.table_registry.clone(),
            progress_monitor: self.progress_monitor.clone(),
            ctas_executor: self.ctas_executor.clone(),
            sql_parser: StreamingSqlParser::new(),
            config: self.config.clone(),
        }
    }

    /// Create with default configuration
    pub fn with_default_config(
        table_registry: Arc<TableRegistry>,
        progress_monitor: Arc<ProgressMonitor>,
        ctas_executor: CtasExecutor,
    ) -> Self {
        Self::new(
            table_registry,
            progress_monitor,
            ctas_executor,
            ParallelLoadingConfig::default(),
        )
    }

    /// Load multiple tables in parallel respecting dependencies
    pub async fn load_tables_with_dependencies(
        &self,
        tables: Vec<TableDefinition>,
    ) -> Result<ParallelLoadingResult, SqlError> {
        let start_time = Instant::now();

        log::info!("Starting parallel load for {} tables", tables.len());

        // Build dependency graph
        let graph = self.build_dependency_graph(&tables)?;

        // Validate graph (check for cycles and missing dependencies)
        graph
            .validate_dependencies()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Dependency validation failed: {}", e),
            })?;

        // Compute loading waves
        let waves = graph
            .compute_loading_waves()
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to compute loading waves: {}", e),
            })?;

        log::info!(
            "Computed {} loading waves for {} tables",
            waves.len(),
            tables.len()
        );

        // Load tables wave by wave
        let mut result = ParallelLoadingResult {
            successful: Vec::new(),
            failed: HashMap::new(),
            skipped: Vec::new(),
            total_duration: Duration::ZERO,
            wave_stats: Vec::new(),
        };

        for (wave_num, wave_tables) in waves.iter().enumerate() {
            log::info!(
                "Loading wave {} with {} tables: {:?}",
                wave_num + 1,
                wave_tables.len(),
                wave_tables
            );

            let wave_start = Instant::now();
            let wave_result = self.load_wave(wave_tables, &tables).await;
            let wave_duration = wave_start.elapsed();

            // Update overall result
            result
                .successful
                .extend(wave_result.successful.iter().cloned());
            result.failed.extend(wave_result.failed.clone());
            result.skipped.extend(wave_result.skipped.iter().cloned());

            result.wave_stats.push(WaveStats {
                wave_number: wave_num + 1,
                tables: wave_tables.clone(),
                duration: wave_duration,
                successful: wave_result.successful.len(),
                failed: wave_result.failed.len(),
            });

            log::info!(
                "Wave {} completed in {:?}: {} successful, {} failed",
                wave_num + 1,
                wave_duration,
                wave_result.successful.len(),
                wave_result.failed.len()
            );

            // Check if we should continue
            if self.config.fail_fast && !wave_result.failed.is_empty() {
                log::warn!(
                    "Stopping parallel load due to fail_fast and {} failures",
                    wave_result.failed.len()
                );
                break;
            }

            // Check total timeout
            if let Some(total_timeout) = self.config.total_timeout {
                if start_time.elapsed() > total_timeout {
                    log::warn!(
                        "Parallel loading exceeded total timeout of {:?}",
                        total_timeout
                    );
                    break;
                }
            }
        }

        result.total_duration = start_time.elapsed();

        log::info!(
            "Parallel loading completed: {} successful, {} failed, {} skipped in {:?}",
            result.successful.len(),
            result.failed.len(),
            result.skipped.len(),
            result.total_duration
        );

        Ok(result)
    }

    /// Load a single wave of independent tables in parallel
    async fn load_wave(
        &self,
        wave_tables: &[String],
        all_tables: &[TableDefinition],
    ) -> WaveLoadResult {
        // Create semaphore to limit parallelism
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel));

        let mut join_set = JoinSet::new();

        for table_name in wave_tables {
            // Find table definition
            let table_def = all_tables.iter().find(|t| t.name == *table_name).cloned();

            if table_def.is_none() {
                log::warn!("Table definition not found for '{}'", table_name);
                continue;
            }

            let table_def = table_def.unwrap();
            let semaphore = semaphore.clone();
            let progress = self.progress_monitor.clone();
            let timeout = self.config.table_load_timeout;

            // Clone self fields needed in async task
            let loader_clone = self.clone_for_task();

            join_set.spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.unwrap();

                log::info!("Starting load for table '{}'", table_def.name);

                // Start progress tracking
                let tracker = progress
                    .start_tracking(table_def.name.clone(), None) // Unknown size
                    .await;

                // Execute actual CTAS table loading
                let result =
                    tokio::time::timeout(timeout, loader_clone.load_table(table_def.clone())).await;

                match result {
                    Ok(Ok(())) => {
                        tracker.set_completed().await;
                        log::info!("Successfully loaded table '{}'", table_def.name);
                        (table_def.name, Ok(()))
                    }
                    Ok(Err(e)) => {
                        let error_msg = e.to_string();
                        tracker.set_error(error_msg.clone()).await;
                        log::error!("Failed to load table '{}': {}", table_def.name, error_msg);
                        (table_def.name, Err(error_msg))
                    }
                    Err(_) => {
                        let timeout_msg = format!("Timeout after {:?}", timeout);
                        tracker.set_error(timeout_msg.clone()).await;
                        log::error!("Timeout loading table '{}'", table_def.name);
                        (table_def.name, Err(timeout_msg))
                    }
                }
            });
        }

        // Wait for all tasks to complete
        let mut wave_result = WaveLoadResult::default();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((table_name, Ok(()))) => {
                    wave_result.successful.push(table_name);
                }
                Ok((table_name, Err(error))) => {
                    wave_result.failed.insert(table_name, error);
                }
                Err(join_error) => {
                    log::error!("Task join error: {}", join_error);
                }
            }
        }

        wave_result
    }

    /// Build dependency graph from table definitions
    fn build_dependency_graph(
        &self,
        tables: &[TableDefinition],
    ) -> Result<TableDependencyGraph, SqlError> {
        let mut graph = TableDependencyGraph::new();

        for table in tables {
            // Parse SQL to automatically extract dependencies
            let deps = if !table.dependencies.is_empty() {
                // Use explicit dependencies if provided
                table.dependencies.clone()
            } else {
                // Auto-extract from SQL
                match self.sql_parser.parse(&table.sql) {
                    Ok(query) => {
                        let table_deps = TableRegistry::extract_table_dependencies(&query);
                        table_deps.into_iter().collect()
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to parse SQL for table '{}', using explicit deps: {}",
                            table.name,
                            e
                        );
                        table.dependencies.clone()
                    }
                }
            };

            graph.add_table(table.name.clone(), deps);
        }

        Ok(graph)
    }

    /// Load a table using CTAS execution
    async fn load_table(&self, table_def: TableDefinition) -> Result<(), SqlError> {
        log::info!("Executing CTAS for table '{}'", table_def.name);

        // Build CTAS query with properties
        let ctas_query = if !table_def.properties.is_empty() {
            // Build WITH clause from properties
            let props: Vec<String> = table_def
                .properties
                .iter()
                .map(|(k, v)| format!("'{}' = '{}'", k, v))
                .collect();

            format!(
                "CREATE TABLE {} AS {} WITH ({})",
                table_def.name,
                table_def.sql,
                props.join(", ")
            )
        } else {
            format!("CREATE TABLE {} AS {}", table_def.name, table_def.sql)
        };

        // Execute CTAS
        let result = self.ctas_executor.execute(&ctas_query).await?;

        // Register table in registry
        self.table_registry
            .register_table(table_def.name.clone(), result.table)
            .await
            .map_err(|e| SqlError::ConfigurationError {
                message: format!("Failed to register table '{}': {}", table_def.name, e),
            })?;

        log::info!(
            "Successfully created and registered table '{}'",
            table_def.name
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct WaveLoadResult {
    successful: Vec<String>,
    failed: HashMap<String, String>,
    skipped: Vec<String>,
}

/// Table definition for loading
#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub name: String,
    pub sql: String,
    pub properties: HashMap<String, String>,
    /// Explicit dependencies (in production, extracted from SQL)
    pub dependencies: std::collections::HashSet<String>,
}

impl TableDefinition {
    /// Create a new table definition
    pub fn new(name: String, sql: String) -> Self {
        Self {
            name,
            sql,
            properties: HashMap::new(),
            dependencies: std::collections::HashSet::new(),
        }
    }

    /// Add a dependency
    pub fn with_dependency(mut self, dep: String) -> Self {
        self.dependencies.insert(dep);
        self
    }

    /// Add multiple dependencies
    pub fn with_dependencies(mut self, deps: Vec<String>) -> Self {
        self.dependencies.extend(deps);
        self
    }

    /// Add a property
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }
}
