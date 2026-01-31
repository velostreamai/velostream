//! StreamJobServer - Concurrent streaming SQL job execution
//!
//! Production-ready streaming SQL engine that can execute multiple concurrent
//! SQL jobs with full isolation. Uses pluggable datasources instead of
//! hardcoded Kafka-only processing.

use crate::velostream::datasource::DataWriter;
use crate::velostream::observability::{
    SharedObservabilityManager, error_tracker::DeploymentContext,
};
use crate::velostream::server::config::StreamJobServerConfig;
use crate::velostream::server::instance_id::get_instance_id;
use crate::velostream::server::observability_config_extractor::ObservabilityConfigExtractor;
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessor, JobProcessorConfig, JobProcessorFactory,
    SharedJobStats, SimpleJobProcessor, TransactionalJobProcessor, create_multi_sink_writers,
    create_multi_source_readers,
};
use crate::velostream::server::shutdown::{ShutdownConfig, ShutdownResult, ShutdownSignal};
use crate::velostream::server::table_registry::{
    TableMetadata as TableStatsInfo, TableRegistry, TableRegistryConfig,
};
use crate::velostream::server::v2::{JoinJobConfig, JoinJobProcessor};
// Note: AdaptiveJobProcessor, PartitionedJobConfig, ProcessingMode are now accessed
// through JobProcessorFactory::create_adaptive_full() - no direct imports needed
use crate::velostream::sql::{
    SqlApplication, SqlError, SqlValidator, StreamExecutionEngine, StreamingSqlParser,
    annotation_parser::SqlAnnotationParser,
    ast::{JobProcessorMode, SelectField, StreamingQuery},
    config::with_clause_parser::WithClauseParser,
    execution::config::StreamingConfig,
    execution::performance::PerformanceMonitor,
    query_analyzer::{DataSourceRequirement, DataSourceType, QueryAnalyzer},
};
use crate::velostream::table::unified_table::UnifiedTable;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct StreamJobServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    base_group_id: String,
    max_jobs: usize,
    performance_monitor: Option<Arc<PerformanceMonitor>>,
    /// Shared table registry for managing CTAS-created tables
    table_registry: TableRegistry,
    /// Observability manager for distributed tracing, metrics, and profiling
    observability: Option<SharedObservabilityManager>,
    /// Job processor architecture configuration (V1 or V2)
    processor_config: JobProcessorConfig,
    /// Base directory for resolving relative paths in SQL config files
    base_dir: Option<std::path::PathBuf>,
}

pub struct RunningJob {
    pub name: String,
    pub version: String,
    pub query: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub execution_handle: JoinHandle<()>,
    pub shutdown_sender: mpsc::Sender<()>,
    pub observability: Option<SharedObservabilityManager>,
    /// Shared stats for real-time monitoring from test harness
    pub shared_stats: SharedJobStats,
}

#[derive(Clone, Debug, serde::Serialize)]
pub enum JobStatus {
    Starting,
    Running,
    Paused,
    Stopped,
    Failed(String),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobSummary {
    pub name: String,
    pub version: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub stats: JobExecutionStats,
}

impl StreamJobServer {
    /// Create server with explicit configuration
    pub fn with_config(config: StreamJobServerConfig) -> Self {
        let table_registry_config = TableRegistryConfig {
            max_tables: config.table_cache_size,
            kafka_brokers: config.kafka_brokers.clone(),
            base_group_id: config.base_group_id.clone(),
        };

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            base_group_id: config.base_group_id,
            max_jobs: config.max_jobs,

            performance_monitor: None,
            table_registry: TableRegistry::with_config(table_registry_config),
            observability: None,
            processor_config: JobProcessorConfig::default(),
            base_dir: config.base_dir,
        }
    }

    /// Create server with brokers and group ID (backward compatible)
    pub fn new(brokers: String, base_group_id: String, max_jobs: usize) -> Self {
        let config = StreamJobServerConfig::new(brokers, base_group_id).with_max_jobs(max_jobs);
        Self::with_config(config)
    }

    /// Create server with monitoring enabled (backward compatible)
    pub async fn new_with_monitoring(
        brokers: String,
        base_group_id: String,
        max_jobs: usize,
        enable_monitoring: bool,
    ) -> Self {
        let config = StreamJobServerConfig::new(brokers, base_group_id)
            .with_max_jobs(max_jobs)
            .with_monitoring(enable_monitoring);
        Self::with_config_and_monitoring(config).await
    }

    /// Create server with explicit configuration and monitoring support
    pub async fn with_config_and_monitoring(config: StreamJobServerConfig) -> Self {
        let performance_monitor = if config.enable_monitoring {
            let monitor = Arc::new(PerformanceMonitor::new());
            info!("Performance monitoring enabled for StreamJobServer");
            Some(monitor)
        } else {
            None
        };

        // Initialize shared observability manager for Prometheus metrics
        let observability = if config.enable_monitoring {
            use crate::velostream::observability::ObservabilityManager;
            use crate::velostream::sql::execution::config::StreamingConfig;

            let streaming_config = StreamingConfig::default().with_prometheus_metrics();

            let mut obs_manager = ObservabilityManager::from_streaming_config(streaming_config);
            match obs_manager.initialize().await {
                Ok(()) => {
                    info!("✅ Server-level observability initialized (shared by all jobs)");
                    Some(Arc::new(RwLock::new(obs_manager)))
                }
                Err(e) => {
                    warn!(
                        "⚠️ Failed to initialize server-level observability: {}. Metrics will be unavailable.",
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        let table_registry_config = TableRegistryConfig {
            max_tables: config.table_cache_size,
            kafka_brokers: config.kafka_brokers.clone(),
            base_group_id: config.base_group_id.clone(),
        };

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            base_group_id: config.base_group_id,
            max_jobs: config.max_jobs,

            performance_monitor,
            table_registry: TableRegistry::with_config(table_registry_config),
            observability,
            processor_config: JobProcessorConfig::default(),
            base_dir: config.base_dir,
        }
    }

    /// Create server with full observability configuration (backward compatible)
    pub async fn new_with_observability(
        brokers: String,
        base_group_id: String,
        max_jobs: usize,
        streaming_config: StreamingConfig,
    ) -> Self {
        let config = StreamJobServerConfig::new(brokers, base_group_id).with_max_jobs(max_jobs);
        Self::with_config_and_observability(config, streaming_config).await
    }

    /// Create server with explicit configuration and full observability (tracing, metrics, profiling)
    pub async fn with_config_and_observability(
        config: StreamJobServerConfig,
        streaming_config: StreamingConfig,
    ) -> Self {
        let performance_monitor = if streaming_config.enable_prometheus_metrics {
            let monitor = Arc::new(PerformanceMonitor::new());
            info!("Performance monitoring enabled for StreamJobServer");
            Some(monitor)
        } else {
            None
        };

        // Initialize shared observability manager with full config (tracing, metrics, profiling)
        let observability = if streaming_config.enable_distributed_tracing
            || streaming_config.enable_prometheus_metrics
            || streaming_config.enable_performance_profiling
        {
            use crate::velostream::observability::ObservabilityManager;

            let mut obs_manager =
                ObservabilityManager::from_streaming_config(streaming_config.clone());
            match obs_manager.initialize().await {
                Ok(()) => {
                    info!("✅ Server-level observability initialized with full configuration");
                    if streaming_config.enable_distributed_tracing {
                        info!("  🔍 Distributed tracing: ACTIVE");
                    }
                    if streaming_config.enable_prometheus_metrics {
                        info!("  📊 Prometheus metrics: ACTIVE");
                    }
                    if streaming_config.enable_performance_profiling {
                        info!("  ⚡ Performance profiling: ACTIVE");
                    }
                    Some(Arc::new(RwLock::new(obs_manager)))
                }
                Err(e) => {
                    warn!(
                        "⚠️ Failed to initialize observability: {}. Continuing without observability.",
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        let table_registry_config = TableRegistryConfig {
            max_tables: config.table_cache_size,
            kafka_brokers: config.kafka_brokers.clone(),
            base_group_id: config.base_group_id.clone(),
        };

        let server = Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            base_group_id: config.base_group_id,
            max_jobs: config.max_jobs,

            performance_monitor,
            table_registry: TableRegistry::with_config(table_registry_config),
            observability: observability.clone(),
            processor_config: JobProcessorConfig::default(),
            base_dir: config.base_dir,
        };

        // Initialize server-level deployment context for system metrics
        if let Some(ref obs_mgr) = observability {
            let deployment_ctx = Self::build_deployment_context("velostream-server", "");
            if let Ok(mut obs_lock) = obs_mgr.try_write() {
                match obs_lock.set_deployment_context_for_job(deployment_ctx.clone()) {
                    Ok(()) => {
                        info!(
                            "✅ Server-level deployment context initialized: node_id={:?}, node_name={:?}, region={:?}",
                            deployment_ctx.node_id, deployment_ctx.node_name, deployment_ctx.region
                        );
                    }
                    Err(e) => {
                        warn!("⚠️ Failed to set server-level deployment context: {}", e);
                    }
                }
            }
        }

        // Spawn background task to periodically collect system metrics
        if let Some(ref obs_mgr) = observability {
            let obs_manager_clone = obs_mgr.clone();
            let jobs_clone = Arc::clone(&server.jobs);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;

                    let obs_lock = obs_manager_clone.read().await;
                    if let Some(metrics_provider) = obs_lock.metrics() {
                        // Collect real system metrics using sysinfo
                        let (cpu_usage, memory_usage) = tokio::task::spawn_blocking(|| {
                            use sysinfo::System;
                            let mut system = System::new_all();
                            system.refresh_all();

                            // Get average CPU usage across all cores
                            let total_cpu: f64 =
                                system.cpus().iter().map(|cpu| cpu.cpu_usage() as f64).sum();
                            let cpu_count = system.cpus().len() as f64;
                            let avg_cpu = if cpu_count > 0.0 {
                                (total_cpu / cpu_count).min(100.0)
                            } else {
                                0.0
                            };

                            // Get used memory
                            let used_memory = system.used_memory();

                            (avg_cpu, used_memory)
                        })
                        .await
                        .unwrap_or((0.0, 0));

                        // Count active jobs (number of currently running jobs)
                        let active_jobs = jobs_clone
                            .try_read()
                            .map(|jobs| jobs.len() as i64)
                            .unwrap_or(0);

                        metrics_provider.update_system_metrics(
                            cpu_usage,
                            memory_usage,
                            active_jobs,
                        );
                        debug!(
                            "Updated system metrics: cpu={:.1}%, memory={}MB, active_jobs={}",
                            cpu_usage,
                            memory_usage / (1024 * 1024),
                            active_jobs
                        );
                    }
                }
            });
            info!("✅ System metrics collection task started (updates every 10s)");
        }

        server
    }

    /// Set the job processor configuration (V1 or V2)
    pub fn with_processor_config(mut self, config: JobProcessorConfig) -> Self {
        let description = config.description();
        self.processor_config = config;
        info!(
            "StreamJobServer processor configuration set to: {}",
            description
        );
        self
    }

    /// Get the current job processor configuration
    pub fn processor_config(&self) -> &JobProcessorConfig {
        &self.processor_config
    }

    /// Get performance metrics (if monitoring is enabled)
    pub fn get_performance_metrics(&self) -> Option<String> {
        // First try to get metrics from server-level ObservabilityManager (Phase 4)
        if let Some(obs_manager) = &self.observability {
            if let Ok(obs_lock) = obs_manager.try_read() {
                if let Some(metrics_provider) = obs_lock.metrics() {
                    // Sync error metrics to Prometheus gauges before export
                    // This ensures error buffer data is current in the exported metrics
                    metrics_provider.sync_error_metrics();

                    // Export Prometheus metrics from the MetricsProvider
                    return metrics_provider.get_metrics_text().ok();
                }
            }
        }

        // Try to get metrics from any running job's ObservabilityManager
        if let Ok(jobs) = self.jobs.try_read() {
            for job in jobs.values() {
                if let Some(job_obs) = &job.observability {
                    if let Ok(obs_lock) = job_obs.try_read() {
                        if let Some(metrics_provider) = obs_lock.metrics() {
                            // Sync error metrics to Prometheus gauges before export
                            // This ensures error buffer data is current in the exported metrics
                            metrics_provider.sync_error_metrics();

                            // Return metrics from the first job that has them
                            // Note: All jobs share the same MetricsProvider via Arc, so any job's metrics will have all data
                            return metrics_provider.get_metrics_text().ok();
                        }
                    }
                }
            }
        }

        // Fall back to performance monitor metrics
        self.performance_monitor
            .as_ref()
            .map(|monitor| monitor.export_prometheus_metrics())
    }

    /// Check if performance monitoring is enabled
    pub fn has_performance_monitoring(&self) -> bool {
        // Check if server-level observability metrics are enabled (Phase 4)
        if let Some(obs_manager) = &self.observability {
            if let Ok(obs_lock) = obs_manager.try_read() {
                if obs_lock.metrics().is_some() {
                    return true;
                }
            }
        }

        // Check if any job has observability metrics enabled
        if let Ok(jobs) = self.jobs.try_read() {
            for job in jobs.values() {
                if let Some(job_obs) = &job.observability {
                    if let Ok(obs_lock) = job_obs.try_read() {
                        if obs_lock.metrics().is_some() {
                            return true;
                        }
                    }
                }
            }
        }

        // Fall back to checking performance monitor
        self.performance_monitor.is_some()
    }

    /// Get performance health status
    pub fn get_health_status(&self) -> Option<String> {
        self.performance_monitor.as_ref().map(|monitor| {
            let health = monitor.health_check();
            serde_json::to_string_pretty(&serde_json::json!({
                "status": format!("{:?}", health.status),
                "issues": health.issues,
                "warnings": health.warnings,
                "metrics": monitor.get_current_metrics(),
                "job_count": self.jobs.try_read().map(|jobs| jobs.len()).unwrap_or(0)
            }))
            .unwrap_or_else(|_| "Error serializing health status".to_string())
        })
    }

    /// Get detailed performance report
    pub fn get_performance_report(&self) -> Option<String> {
        self.performance_monitor.as_ref().map(|monitor| {
            format!(
                "{}\n\n=== Job Information ===\n{}",
                monitor.get_performance_report(),
                self.get_job_summary()
            )
        })
    }

    /// Get summary of all jobs
    fn get_job_summary(&self) -> String {
        if let Ok(jobs) = self.jobs.try_read() {
            if jobs.is_empty() {
                "No active jobs".to_string()
            } else {
                let mut summary = format!("Active Jobs: {}\n", jobs.len());
                for (name, job) in jobs.iter() {
                    let stats = job
                        .shared_stats
                        .read()
                        .map(|s| (s.records_processed, s.records_per_second()))
                        .unwrap_or((0, 0.0));
                    summary.push_str(&format!(
                        "  - {}: {:?} (records: {}, rps: {:.1})\n",
                        name, job.status, stats.0, stats.1
                    ));
                }
                summary
            }
        } else {
            "Unable to read job information".to_string()
        }
    }

    pub async fn deploy_job(
        &self,
        name: String,
        version: String,
        query: String,
        topic: String,
        app_name: Option<String>,
        instance_id: Option<String>,
    ) -> Result<(), SqlError> {
        info!(
            "Deploying job '{}' version '{}' on topic '{}': {}",
            name, version, topic, query
        );

        // Check if we're at max capacity
        let jobs = self.jobs.read().await;
        if jobs.len() >= self.max_jobs {
            return Err(SqlError::ExecutionError {
                message: format!("Maximum jobs limit reached ({})", self.max_jobs),
                query: None,
            });
        }

        // Check if job already exists
        if jobs.contains_key(&name) {
            return Err(SqlError::ExecutionError {
                message: format!("Job '{}' already exists", name),
                query: None,
            });
        }
        drop(jobs);

        // Input validation
        if name.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "job name cannot be empty".to_string(),
                query: None,
            });
        }

        if version.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "job version cannot be empty".to_string(),
                query: None,
            });
        }

        if query.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "query cannot be empty".to_string(),
                query: None,
            });
        }

        if topic.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "topic cannot be empty".to_string(),
                query: None,
            });
        }

        // Parse and validate the query
        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(&query)?;

        // Debug: log metric annotations found after re-parsing
        if let StreamingQuery::CreateStream {
            name: ref stream_name,
            ref metric_annotations,
            ..
        } = parsed_query
        {
            info!(
                "Job '{}': Re-parsed query for stream '{}' — found {} @metric annotations",
                name,
                stream_name,
                metric_annotations.len()
            );
            for ann in metric_annotations {
                info!(
                    "Job '{}':   @metric: {} (type={:?}, field={:?})",
                    name, ann.name, ann.metric_type, ann.field
                );
            }
        }

        // Extract table dependencies and ensure they exist AND ARE READY
        // All tables are passed to processors - they handle which ones they need
        let required_tables = TableRegistry::extract_table_dependencies(&parsed_query);
        let properties = Self::get_query_properties(&parsed_query);

        // Create QueryAnalyzer for determining required resources
        // Use base_dir for resolving relative config file paths
        let mut analyzer = if let Some(ref base_dir) = self.base_dir {
            QueryAnalyzer::with_base_dir(self.base_group_id.clone(), base_dir)
        } else {
            QueryAnalyzer::new(self.base_group_id.clone())
        };
        analyzer.set_job_name(name.to_string());

        if !required_tables.is_empty() {
            info!(
                "Job '{}' has table dependencies: {:?}",
                name, required_tables
            );

            // Identify file_source tables that need to be loaded into the registry
            // Other source types (kafka_source, etc.) are external and don't need registry loading
            let file_source_tables: Vec<String> = required_tables
                .iter()
                .filter(|name| {
                    let type_key = format!("{}.type", name);
                    properties
                        .get(&type_key)
                        .map(|t| t == "file_source")
                        .unwrap_or(false)
                })
                .cloned()
                .collect();

            // Check which file_source tables are missing from the registry
            let mut missing_tables = Vec::new();
            for table_name in &file_source_tables {
                if !self.table_registry.exists(table_name).await {
                    missing_tables.push(table_name.clone());
                }
            }

            // Auto-load missing file_source tables from their config files
            if !missing_tables.is_empty() {
                info!(
                    "Job '{}': Auto-loading {} missing file_source tables...",
                    name,
                    missing_tables.len()
                );

                for table_name in &missing_tables {
                    let config_key = format!("{}.config_file", table_name);
                    if let Some(config_path) = properties.get(&config_key) {
                        match self
                            .table_registry
                            .load_file_source_table(
                                table_name,
                                config_path,
                                self.base_dir.as_deref(),
                            )
                            .await
                        {
                            Ok(row_count) => {
                                info!(
                                    "  ✓ Loaded table '{}' from '{}' ({} rows)",
                                    table_name, config_path, row_count
                                );
                            }
                            Err(e) => {
                                error!(
                                    "  ✗ Failed to load table '{}' from '{}': {}",
                                    table_name, config_path, e
                                );
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "Job '{}' cannot be deployed: failed to load table '{}' from '{}': {}",
                                        name, table_name, config_path, e
                                    ),
                                    query: Some(query),
                                });
                            }
                        }
                    } else {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "Job '{}' cannot be deployed: missing required table '{}' and no config_file specified.\n\
                                Add '{}.config_file' to the WITH clause or create the table using: CREATE TABLE {} AS SELECT ...",
                                name, table_name, table_name, table_name
                            ),
                            query: Some(query),
                        });
                    }
                }
            }

            // Wait for file_source tables to be ready (if any)
            if !file_source_tables.is_empty() {
                info!(
                    "Job '{}': Waiting for {} file_source tables to be ready...",
                    name,
                    file_source_tables.len()
                );

                // CRITICAL: Wait for file_source tables to be fully loaded before starting the stream
                // This prevents missing enrichment data and ensures data consistency
                let table_ready_timeout = Duration::from_secs(60); // Conservative 60s default

                match self
                    .table_registry
                    .wait_for_tables_ready(&file_source_tables, table_ready_timeout)
                    .await
                {
                    Ok(table_statuses) => {
                        info!(
                            "Job '{}': All {} file_source tables are ready!",
                            name,
                            table_statuses.len()
                        );
                        for (table_name, status) in table_statuses {
                            info!("  Table '{}': {:?}", table_name, status);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Job '{}': Failed waiting for tables to be ready: {}",
                            name, e
                        );
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "Job '{}' cannot be deployed: tables not ready after {:?} timeout: {}",
                                name, table_ready_timeout, e
                            ),
                            query: Some(query),
                        });
                    }
                }
            }
            // Register file_source tables as known (they're in the registry)
            // This allows QueryAnalyzer to skip validation for these tables
            // while still analyzing external sources (kafka_source, etc.)
            analyzer.add_known_tables(file_source_tables);
        } else {
            info!("Job '{}' has no table dependencies", name);
        }

        let analysis = analyzer.analyze(&parsed_query)?;

        // Check if this is a reference table CTAS (CREATE TABLE with file_source input, no sink)
        // If so, materialize the table directly instead of starting a streaming job
        let is_ctas = matches!(&parsed_query, StreamingQuery::CreateTable { .. });
        let has_file_source = analysis.required_sources.iter().any(|s| {
            matches!(
                s.source_type,
                crate::velostream::sql::query_analyzer::DataSourceType::File
            )
        });
        let has_no_sink = analysis.required_sinks.is_empty();

        if is_ctas && has_file_source && has_no_sink {
            info!(
                "Job '{}': Reference table CTAS detected (file_source input, no sink) - materializing table directly",
                name
            );
            // Use create_table() to execute CTAS and register the output table in the registry
            let table_name = self.table_registry.create_table(query.clone()).await?;
            info!(
                "Job '{}': Table '{}' created and registered in table registry",
                name, table_name
            );

            // Return early - no streaming job needed for reference tables
            // The table is now available for other jobs to use via subqueries/joins
            return Ok(());
        }

        // Extract batch configuration from WITH clauses
        let batch_config = Self::extract_batch_config_from_query(&parsed_query)?;

        // Extract StreamingConfig from WITH clauses (Phase 1B-4 features)
        let streaming_config = Self::extract_streaming_config_from_query(&parsed_query)?;

        // Extract and merge observability settings from SQL annotations (app-level settings)
        let annotation_config = ObservabilityConfigExtractor::extract_from_sql_string(&query)?;
        let streaming_config =
            ObservabilityConfigExtractor::merge_configs(streaming_config, annotation_config);

        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);

        // Create execution engine for this job with query-driven format
        let (output_sender, output_receiver) = mpsc::unbounded_channel();
        let mut execution_engine = StreamExecutionEngine::new(output_sender);

        // FR-082 Phase 5: Set output receiver for EMIT CHANGES support
        // This allows batch processing to drain emitted results for EMIT CHANGES queries
        execution_engine.set_output_receiver(output_receiver);

        // Apply StreamingConfig to execution engine (Phase 1B-4 wiring)
        execution_engine.set_streaming_config(streaming_config.clone());

        // Use shared observability manager for all jobs (Phase 4)
        // This ensures all metrics are collected in a single Prometheus registry
        let observability_manager = if streaming_config.enable_distributed_tracing
            || streaming_config.enable_prometheus_metrics
            || streaming_config.enable_performance_profiling
        {
            if let Some(shared_obs) = self.observability.clone() {
                info!(
                    "✅ Job '{}': Using shared observability manager (tracing={}, metrics={}, profiling={})",
                    name,
                    streaming_config.enable_distributed_tracing,
                    streaming_config.enable_prometheus_metrics,
                    streaming_config.enable_performance_profiling
                );
                Some(shared_obs)
            } else {
                warn!(
                    "⚠️ Job '{}': Observability requested but server has no shared observability manager. Continuing without observability.",
                    name
                );
                None
            }
        } else {
            info!(
                "Job '{}': Observability NOT enabled (tracing={}, metrics={}, profiling={})",
                name,
                streaming_config.enable_distributed_tracing,
                streaming_config.enable_prometheus_metrics,
                streaming_config.enable_performance_profiling
            );
            None
        };
        info!(
            "Job '{}': Using shared observability: {}",
            name,
            observability_manager.is_some()
        );

        // Initialize deployment context for error tracking and observability
        if let Some(obs_manager) = &observability_manager {
            let deployment_ctx = Self::build_deployment_context(&name, &version);
            if let Ok(mut obs_lock) = obs_manager.try_write() {
                match obs_lock.set_deployment_context_for_job(deployment_ctx.clone()) {
                    Ok(()) => {
                        info!(
                            "Job '{}': Deployment context initialized (node_id={:?}, region={:?}, version={})",
                            name,
                            deployment_ctx.node_id,
                            deployment_ctx.region,
                            deployment_ctx.version.as_deref().unwrap_or("unknown")
                        );
                    }
                    Err(e) => {
                        warn!("Job '{}': Failed to set deployment context: {}", name, e);
                    }
                }
            }
        }

        // Enable performance monitoring for this job if available
        if let Some(monitor) = &self.performance_monitor {
            execution_engine.set_performance_monitor(Some(Arc::clone(monitor)));
        }

        // Extract tables ONCE for both execution_engine and Adaptive processor
        // This avoids duplicate extraction and blocking async calls
        // NOTE: required_tables includes ALL source references (kafka_source, file_source, etc.)
        // Only tables actually in the registry will be extracted - external sources (kafka_source)
        // are not expected to be in the registry and are handled by their respective data sources.
        let tables_for_processor: Option<HashMap<String, Arc<dyn UnifiedTable>>> =
            if !required_tables.is_empty() {
                let mut tables = HashMap::new();

                for table_name in &required_tables {
                    // Silently skip tables not in registry - these may be external sources
                    // (kafka_source, etc.) that don't need registry lookup
                    if let Ok(table) = self.table_registry.get_table(table_name).await {
                        tables.insert(table_name.clone(), table);
                    }
                }

                if tables.is_empty() {
                    None
                } else {
                    info!(
                        "Job '{}': Extracted {} tables for processor: {:?}",
                        name,
                        tables.len(),
                        tables.keys().collect::<Vec<_>>()
                    );
                    Some(tables)
                }
            } else {
                None
            };

        // Note: Tables are now passed directly to processors via JobProcessorFactory
        // in create_processor(), eliminating the need for context_customizer closure pattern.
        // Processors inject tables directly into their ProcessorContext.

        let execution_engine = Arc::new(RwLock::new(execution_engine));

        // Clone data for the job task
        let job_name = name.clone();
        let _topic_clone = topic.clone();
        let batch_config_clone = batch_config.clone();
        let observability_for_spawn = observability_manager.clone();
        let tables_for_spawn = tables_for_processor;

        // Create shared stats for real-time monitoring from test harness
        let shared_stats: SharedJobStats =
            Arc::new(std::sync::RwLock::new(JobExecutionStats::new()));
        let shared_stats_for_spawn = shared_stats.clone();

        // Wire job_mode annotation from query to processor configuration
        // Per-query job_mode takes precedence over server-level configuration
        // Pass raw SQL string for annotation parsing (works with CREATE STREAM and other query types)
        let processor_config_from_query =
            Self::get_processor_config_from_query(&parsed_query, &query, &self.processor_config);

        // Apply source-based partition limits (e.g., file sources only support 1 partition)
        let processor_config_for_spawn = Self::apply_source_partition_limit(
            processor_config_from_query,
            &analysis.required_sources,
        )
        .await;

        // FR-082 Phase 5: Output handler task no longer needed
        // The engine now owns the output_receiver for EMIT CHANGES support.
        // Batch processing drains the receiver synchronously and collects results
        // for sink writing. This provides correct EMIT CHANGES semantics.
        //
        // Previously, this task consumed from the output channel asynchronously,
        // but that architecture didn't work for EMIT CHANGES queries which need
        // synchronous result collection during batch processing.

        // Removed: tokio::spawn output handler task (receiver now owned by engine)

        // Spawn the job execution task using modern datasource approach
        let execution_handle = tokio::spawn(async move {
            info!(
                "Starting job '{}' execution task with {} sources and {} sinks",
                job_name,
                analysis.required_sources.len(),
                analysis.required_sinks.len()
            );

            // Extract job processing configuration EARLY - before creating sources/sinks
            // This allows us to configure transactional.id and isolation.level appropriately
            let job_config = Self::extract_job_config_from_query(&parsed_query);

            // Determine use_transactions from both WITH clause properties AND @job_mode annotation
            // The @job_mode: transactional annotation should enable transactions even without
            // explicit WITH ('mode' = 'transactional') property
            let use_transactions = job_config.use_transactions
                || matches!(
                    processor_config_for_spawn,
                    JobProcessorConfig::Transactional
                );

            info!(
                "Job '{}' transactional mode: {} (config: {}, processor: {:?})",
                job_name, use_transactions, job_config.use_transactions, processor_config_for_spawn
            );

            // Use multi-source processing for all jobs (handles single-source as special case)
            // Thread app_name from SqlApplication metadata for coordinated consumer groups
            // Thread instance_id for unique client.id generation
            // Thread use_transactions for isolation.level=read_committed
            match create_multi_source_readers(
                &analysis.required_sources,
                &job_name,
                app_name.as_deref(),
                instance_id.as_deref(),
                &batch_config_clone,
                use_transactions,
            )
            .await
            {
                Ok(mut readers) => {
                    info!(
                        "Job '{}' successfully created {} data sources",
                        job_name,
                        readers.len()
                    );

                    // Create all sinks
                    // Thread app_name and instance_id for hierarchical client.id generation
                    // Thread use_transactions for transactional.id injection
                    log::debug!(
                        "Job '{}': Creating sinks. Required sinks count: {}, use_transactions: {}",
                        job_name,
                        analysis.required_sinks.len(),
                        use_transactions
                    );
                    for (i, sink) in analysis.required_sinks.iter().enumerate() {
                        log::debug!(
                            "Job '{}': Sink[{}] name='{}', type={:?}, props_count={}",
                            job_name,
                            i,
                            sink.name,
                            sink.sink_type,
                            sink.properties.len()
                        );
                        for (k, v) in &sink.properties {
                            log::debug!("Job '{}': Sink[{}] property: {} = {}", job_name, i, k, v);
                        }
                    }
                    match create_multi_sink_writers(
                        &analysis.required_sinks,
                        &job_name,
                        app_name.as_deref(),
                        instance_id.as_deref(),
                        &batch_config_clone,
                        use_transactions,
                    )
                    .await
                    {
                        Ok(mut writers) => {
                            info!(
                                "Job '{}' successfully created {} data sinks",
                                job_name,
                                writers.len()
                            );
                            log::debug!(
                                "Job '{}': Writers map keys: {:?}",
                                job_name,
                                writers.keys().collect::<Vec<_>>()
                            );

                            // Add stdout as fallback if no sinks were created
                            if writers.is_empty() {
                                log::warn!(
                                    "Job '{}': STDOUT FALLBACK - No sinks were successfully created, adding stdout as default. Required sinks: {}",
                                    job_name,
                                    analysis.required_sinks.len()
                                );
                                writers.insert(
                                    "stdout_default".to_string(),
                                    Box::new(
                                        crate::velostream::datasource::StdoutWriter::new_pretty(),
                                    ) as Box<dyn DataWriter>,
                                );
                            }

                            // Log processing configuration (job_config extracted earlier for source/sink creation)
                            info!(
                                "Job '{}' processing configuration: use_transactions={}, failure_strategy={:?}, max_batch_size={}, batch_timeout={}ms, max_retries={}, retry_backoff={}ms, log_progress={}",
                                job_name,
                                job_config.use_transactions,
                                job_config.failure_strategy,
                                job_config.max_batch_size,
                                job_config.batch_timeout.as_millis(),
                                job_config.max_retries,
                                job_config.retry_backoff.as_millis(),
                                job_config.log_progress
                            );

                            info!(
                                "Job '{}' using JobProcessor architecture: {}",
                                job_name,
                                processor_config_for_spawn.description()
                            );

                            // FR-085: Check for stream-stream joins and route to specialized processor
                            // Note: The parser currently marks ALL joins as stream-stream joins, even
                            // stream-table joins. We need to verify both sources are actually streams
                            // by checking if they exist in the readers map.
                            // Extract join info once and reuse it to avoid
                            // calling extract_stream_stream_join_info() twice.
                            let join_info = if parsed_query.has_stream_stream_joins() {
                                // Extract join information from query
                                if let Some((left_name, right_name, join_type)) =
                                    parsed_query.extract_stream_stream_join_info()
                                {
                                    // Check if both sources exist as readers (i.e., both are streams)
                                    // Reader names use format "source_{idx}_{name}"
                                    let left_exists = readers
                                        .keys()
                                        .any(|k| k.ends_with(&format!("_{}", left_name)));
                                    let right_exists = readers
                                        .keys()
                                        .any(|k| k.ends_with(&format!("_{}", right_name)));

                                    if left_exists && right_exists {
                                        info!(
                                            "Job '{}': Both '{}' and '{}' are streams, using JoinJobProcessor",
                                            job_name, left_name, right_name
                                        );
                                        Some((left_name, right_name, join_type))
                                    } else {
                                        info!(
                                            "Job '{}': Stream-table join detected (left={}, right={}), using standard processor with table lookup",
                                            job_name,
                                            if left_exists { "stream" } else { "table" },
                                            if right_exists { "stream" } else { "table" }
                                        );
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            if let Some((left_name, right_name, join_type)) = join_info {
                                info!(
                                    "Job '{}': Join sources - left='{}', right='{}', type={:?}",
                                    job_name, left_name, right_name, join_type
                                );

                                // Extract join keys from ON clause
                                let join_keys = parsed_query.extract_join_keys();
                                info!("Job '{}': Join keys: {:?}", job_name, join_keys);

                                // Get readers for left and right sources
                                // Reader names use format "source_{idx}_{name}", so we need to find by suffix
                                let left_key = readers
                                    .keys()
                                    .find(|k| k.ends_with(&format!("_{}", left_name)))
                                    .cloned();
                                let right_key = readers
                                    .keys()
                                    .find(|k| k.ends_with(&format!("_{}", right_name)))
                                    .cloned();

                                let left_reader = left_key.and_then(|k| readers.remove(&k));
                                let right_reader = right_key.and_then(|k| readers.remove(&k));

                                match (left_reader, right_reader) {
                                    (Some(left), Some(right)) => {
                                        // Create join configuration
                                        let mut join_config = crate::velostream::sql::execution::processors::IntervalJoinConfig::new(
                                                &left_name,
                                                &right_name,
                                            );

                                        // Add join keys
                                        for (left_key, right_key) in join_keys {
                                            join_config =
                                                join_config.with_key(&left_key, &right_key);
                                        }

                                        // Set time bounds (default 1 hour window for now)
                                        join_config = join_config
                                            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

                                        // Extract SELECT fields for projection
                                        let select_fields = parsed_query.get_select_fields();
                                        let projection = if select_fields.is_empty()
                                            || select_fields
                                                .iter()
                                                .any(|f| matches!(f, SelectField::Wildcard))
                                        {
                                            // No projection needed for wildcard or empty select
                                            None
                                        } else {
                                            Some(select_fields)
                                        };

                                        // Create join job config with projection
                                        let job_config = JoinJobConfig {
                                            join_config,
                                            projection,
                                            ..Default::default()
                                        };

                                        // Create and execute join processor
                                        let join_processor = JoinJobProcessor::new(job_config);

                                        // Get first writer (if any)
                                        let writer = writers.into_values().next();

                                        match join_processor
                                            .process_join(
                                                left_name.clone(),
                                                left,
                                                right_name.clone(),
                                                right,
                                                writer,
                                                Some(shared_stats_for_spawn.clone()),
                                            )
                                            .await
                                        {
                                            Ok(stats) => {
                                                info!(
                                                    "Job '{}' (JoinJobProcessor) completed: {} left + {} right records, {} joins, {} output",
                                                    job_name,
                                                    stats.left_records_read,
                                                    stats.right_records_read,
                                                    stats.join_matches,
                                                    stats.records_written
                                                );
                                                // Update shared stats
                                                if let Ok(mut shared) =
                                                    shared_stats_for_spawn.write()
                                                {
                                                    shared.records_processed = stats
                                                        .left_records_read
                                                        + stats.right_records_read;
                                                }
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Job '{}' (JoinJobProcessor) failed: {:?}",
                                                    job_name, e
                                                );
                                            }
                                        }
                                    }
                                    _ => {
                                        // This should not happen since we verified both sources exist
                                        // before setting use_join_processor = true
                                        error!(
                                            "Job '{}': Unexpected reader state in JoinJobProcessor path",
                                            job_name
                                        );
                                    }
                                }
                            } else {
                                // Normal (non-join) processing path
                                // Create processor using factory pattern
                                let processor = Self::create_processor_for_job(
                                    &processor_config_for_spawn,
                                    &parsed_query,
                                    &job_name,
                                    tables_for_spawn.clone(),
                                    observability_for_spawn.clone(),
                                );

                                // Execute the selected processor (unified API for all three)
                                match processor
                                    .process_multi_job(
                                        readers,
                                        writers,
                                        execution_engine.clone(),
                                        parsed_query,
                                        job_name.clone(),
                                        shutdown_receiver,
                                        Some(shared_stats_for_spawn.clone()), // Pass shared stats for real-time monitoring
                                    )
                                    .await
                                {
                                    Ok(stats) => {
                                        info!(
                                            "Job '{}' completed successfully ({} - {}): {:?}",
                                            job_name,
                                            processor.processor_version(),
                                            processor.processor_name(),
                                            stats
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Job '{}' failed ({} - {}): {:?}",
                                            job_name,
                                            processor.processor_version(),
                                            processor.processor_name(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Job '{}' failed to create sinks: {}, using stdout only",
                                job_name, e
                            );

                            // Create stdout-only writers map for fallback
                            let mut fallback_writers = std::collections::HashMap::new();
                            fallback_writers.insert(
                                "stdout_fallback".to_string(),
                                Box::new(crate::velostream::datasource::StdoutWriter::new_pretty())
                                    as Box<dyn DataWriter>,
                            );

                            // Still proceed with processing using simple processor
                            let config = Self::extract_job_config_from_query(&parsed_query);
                            let processor = SimpleJobProcessor::with_observability(
                                config,
                                observability_for_spawn.clone(),
                            );
                            info!(
                                "Job '{}': Created processor with observability: {}",
                                job_name,
                                observability_for_spawn.is_some()
                            );

                            match processor
                                .process_multi_job(
                                    readers,
                                    fallback_writers,
                                    execution_engine,
                                    parsed_query,
                                    job_name.clone(),
                                    shutdown_receiver,
                                    Some(shared_stats_for_spawn.clone()), // Pass shared stats for real-time monitoring
                                )
                                .await
                            {
                                Ok(stats) => {
                                    info!(
                                        "Job '{}' completed successfully (fallback): {:?}",
                                        job_name, stats
                                    );
                                }
                                Err(e) => {
                                    error!("Job '{}' failed (fallback): {:?}", job_name, e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Job '{}' failed to create data sources: {}", job_name, e);
                }
            }
        });

        // Create the job record
        let job = RunningJob {
            name: name.clone(),
            version: version.clone(),
            query: query.clone(),
            topic: topic.clone(),
            status: JobStatus::Running,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            execution_handle,
            shutdown_sender,
            observability: observability_manager,
            shared_stats, // Store shared stats for real-time monitoring
        };

        // Store the job
        let mut jobs = self.jobs.write().await;
        jobs.insert(name.clone(), job);
        drop(jobs);

        info!(
            "Successfully deployed job '{}' version '{}' on topic '{}'",
            name, version, topic
        );
        Ok(())
    }

    /// Create a shared table via CREATE TABLE AS SELECT
    /// This table will be available for all future SQL jobs to reference
    pub async fn create_table(&self, ctas_query: String) -> Result<String, SqlError> {
        self.table_registry.create_table(ctas_query).await
    }

    /// Get list of all available tables
    pub async fn list_tables(&self) -> Vec<String> {
        self.table_registry.list_tables().await
    }

    /// Check if a table exists in the registry
    pub async fn table_exists(&self, table_name: &str) -> bool {
        self.table_registry.exists(table_name).await
    }

    /// Get a reference to a table (for internal use)
    pub async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<Arc<dyn crate::velostream::table::unified_table::UnifiedTable>, SqlError> {
        self.table_registry.get_table(table_name).await
    }

    /// Drop a table and stop its background population job
    pub async fn drop_table(&self, table_name: &str) -> Result<(), SqlError> {
        self.table_registry.drop_table(table_name).await
    }

    /// Get statistics about all tables
    pub async fn get_table_stats(&self) -> HashMap<String, TableStatsInfo> {
        self.table_registry.get_all_table_stats().await
    }

    /// Clean up inactive tables (placeholder for future cleanup strategies)
    ///
    /// Currently returns an empty list. Tables should be explicitly dropped.
    pub async fn cleanup_inactive_tables(&self) -> Result<Vec<String>, SqlError> {
        self.table_registry.cleanup_inactive_tables().await
    }

    /// Get health status of all tables
    pub async fn get_tables_health(&self) -> HashMap<String, String> {
        let health_reports = self.table_registry.get_health_status().await;
        let mut health = HashMap::new();

        for report in health_reports {
            let status_str = format!("{:?}", report.status);
            health.insert(report.table_name, status_str);
        }

        health
    }

    pub async fn stop_job(&self, name: &str) -> Result<(), SqlError> {
        self.stop_job_with_timeout(name, Duration::from_secs(10))
            .await
    }

    /// Stop a job with a configurable timeout for graceful shutdown
    pub async fn stop_job_with_timeout(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<(), SqlError> {
        // First, get the job and send shutdown signal (while holding write lock briefly)
        let job = {
            let mut jobs = self.jobs.write().await;
            jobs.remove(name)
        };

        if let Some(job) = job {
            info!("Stopping job '{}'", name);

            // Send shutdown signal
            if let Err(e) = job.shutdown_sender.try_send(()) {
                warn!("Failed to send shutdown signal to job '{}': {:?}", name, e);
            }

            // Wait for job to finish gracefully (with timeout)
            let wait_for_completion = async {
                loop {
                    if job.execution_handle.is_finished() {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };

            match tokio::time::timeout(timeout, wait_for_completion).await {
                Ok(_) => {
                    info!("Successfully stopped job '{}' (graceful)", name);
                }
                Err(_) => {
                    warn!(
                        "Job '{}' did not stop within {:?}, forcing abort",
                        name, timeout
                    );
                    job.execution_handle.abort();
                    info!("Successfully stopped job '{}' (forced)", name);
                }
            }

            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    /// Gracefully shutdown all running jobs in response to a signal
    ///
    /// This method:
    /// 1. Stops accepting new jobs
    /// 2. Sends shutdown signals to all running jobs
    /// 3. Waits for jobs to commit offsets and flush sinks (with timeout)
    /// 4. Force-kills any jobs that don't stop within the timeout
    ///
    /// # Arguments
    /// * `signal` - The signal that triggered the shutdown
    /// * `config` - Shutdown configuration including timeout
    ///
    /// # Returns
    /// A `ShutdownResult` with statistics about the shutdown process
    ///
    /// # Example
    /// ```rust,no_run
    /// use velostream::velostream::server::shutdown::{ShutdownConfig, ShutdownSignal, shutdown_signal};
    /// use velostream::velostream::server::stream_job_server::StreamJobServer;
    ///
    /// # async fn example() {
    /// let server = StreamJobServer::new("localhost:9092".to_string(), "myapp".to_string(), 10);
    ///
    /// // Wait for shutdown signal and then gracefully stop
    /// let signal = shutdown_signal().await;
    /// let result = server.graceful_shutdown(signal, ShutdownConfig::default()).await;
    /// println!("{}", result);
    /// # }
    /// ```
    pub async fn graceful_shutdown(
        &self,
        signal: ShutdownSignal,
        config: ShutdownConfig,
    ) -> ShutdownResult {
        let start = std::time::Instant::now();
        let mut jobs_stopped = 0;
        let mut jobs_force_killed = 0;

        if config.verbose {
            info!(
                "Initiating graceful shutdown due to {} (timeout: {:?})",
                signal, config.timeout
            );
        }

        // Dump diagnostics on SIGQUIT if configured
        if signal == ShutdownSignal::Quit && config.dump_on_quit {
            self.dump_diagnostics().await;
        }

        // Get list of all running job names
        let job_names: Vec<String> = {
            let jobs = self.jobs.read().await;
            jobs.keys().cloned().collect()
        };

        let total_jobs = job_names.len();
        if config.verbose {
            info!("Shutting down {} running jobs", total_jobs);
        }

        // Phase 1: Send shutdown signals to all jobs
        {
            let jobs = self.jobs.read().await;
            for name in &job_names {
                if let Some(job) = jobs.get(name) {
                    if config.verbose {
                        debug!("Sending shutdown signal to job '{}'", name);
                    }
                    let _ = job.shutdown_sender.try_send(());
                }
            }
        }

        // Phase 2: Wait for jobs to stop gracefully (with timeout per job)
        let per_job_timeout = if total_jobs > 0 {
            config.timeout / total_jobs as u32
        } else {
            config.timeout
        };

        for name in &job_names {
            let job_start = std::time::Instant::now();

            // Try to gracefully stop the job
            let graceful_stop = async {
                // Check if job has already stopped
                loop {
                    {
                        let jobs = self.jobs.read().await;
                        if !jobs.contains_key(name) {
                            return true; // Job already stopped
                        }
                        if let Some(job) = jobs.get(name) {
                            if job.execution_handle.is_finished() {
                                return true; // Job finished
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            match tokio::time::timeout(per_job_timeout, graceful_stop).await {
                Ok(true) => {
                    jobs_stopped += 1;
                    if config.verbose {
                        info!(
                            "Job '{}' stopped gracefully in {:?}",
                            name,
                            job_start.elapsed()
                        );
                    }
                }
                Ok(false) | Err(_) => {
                    // Timeout or job didn't stop - force kill
                    warn!(
                        "Job '{}' did not stop within {:?}, force killing",
                        name, per_job_timeout
                    );

                    let mut jobs = self.jobs.write().await;
                    if let Some(job) = jobs.remove(name) {
                        job.execution_handle.abort();
                        jobs_force_killed += 1;
                    }
                }
            }
        }

        // Phase 3: Clean up any remaining jobs
        {
            let mut jobs = self.jobs.write().await;
            for (name, job) in jobs.drain() {
                warn!("Force killing remaining job '{}'", name);
                job.execution_handle.abort();
                jobs_force_killed += 1;
            }
        }

        let elapsed = start.elapsed();
        let completed_gracefully = elapsed <= config.timeout && jobs_force_killed == 0;

        let result = ShutdownResult {
            signal,
            jobs_stopped,
            jobs_force_killed,
            completed_gracefully,
            elapsed,
        };

        if config.verbose {
            info!("{}", result);
        }

        result
    }

    /// Dump diagnostic information for debugging (called on SIGQUIT)
    async fn dump_diagnostics(&self) {
        info!("=== VELOSTREAM DIAGNOSTICS DUMP ===");

        // Dump job information
        let jobs = self.jobs.read().await;
        info!("Running jobs: {}", jobs.len());
        for (name, job) in jobs.iter() {
            info!("  Job '{}': {:?}", name, job.status);
            if let Ok(stats) = job.shared_stats.read() {
                info!(
                    "    Records processed: {}, Failed: {}",
                    stats.records_processed, stats.records_failed
                );
            }
        }

        // Dump table registry info
        let table_health = self.get_tables_health().await;
        info!("Table registry health:");
        for (table, status) in &table_health {
            info!("  {}: {}", table, status);
        }

        info!("=== END DIAGNOSTICS DUMP ===");
    }

    pub async fn pause_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(name) {
            // Send shutdown signal to pause consumption
            if let Err(e) = job.shutdown_sender.try_send(()) {
                warn!("Failed to send pause signal to job '{}': {:?}", name, e);
            }

            job.status = JobStatus::Paused;
            job.updated_at = chrono::Utc::now();
            info!("Paused job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    pub async fn list_jobs(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().await;
        jobs.values()
            .map(|job| {
                let stats = job
                    .shared_stats
                    .read()
                    .map(|s| s.clone())
                    .unwrap_or_default();
                JobSummary {
                    name: job.name.clone(),
                    version: job.version.clone(),
                    topic: job.topic.clone(),
                    status: job.status.clone(),
                    created_at: job.created_at,
                    stats,
                }
            })
            .collect()
    }

    pub async fn get_job_status(&self, name: &str) -> Option<JobSummary> {
        let jobs = self.jobs.read().await;
        jobs.get(name).map(|job| {
            // Read real-time stats from shared_stats
            let stats = match job.shared_stats.read() {
                Ok(s) => s.clone(),
                Err(e) => {
                    log::warn!(
                        "Failed to read shared_stats for job '{}': {} - using default",
                        job.name,
                        e
                    );
                    JobExecutionStats::default()
                }
            };

            JobSummary {
                name: job.name.clone(),
                version: job.version.clone(),
                topic: job.topic.clone(),
                status: job.status.clone(),
                created_at: job.created_at,
                stats,
            }
        })
    }

    /// Get reference to the shared observability manager (for test harness metric verification)
    ///
    /// Returns the observability manager if metrics/tracing is enabled, None otherwise.
    pub fn observability(&self) -> Option<&SharedObservabilityManager> {
        self.observability.as_ref()
    }

    /// Deploy multiple jobs from a SQL application
    pub async fn deploy_sql_application(
        &self,
        app: SqlApplication,
        default_topic: Option<String>,
    ) -> Result<Vec<String>, SqlError> {
        self.deploy_sql_application_with_filename(app, default_topic, None)
            .await
    }

    pub async fn deploy_sql_application_with_filename(
        &self,
        app: SqlApplication,
        default_topic: Option<String>,
        source_filename: Option<String>,
    ) -> Result<Vec<String>, SqlError> {
        info!(
            "Deploying SQL application '{}' version '{}'",
            app.metadata.name, app.metadata.version
        );

        // Log SQL application annotations (top-level metadata)
        if let Some(application) = &app.metadata.application {
            info!("  @application: {}", application);
        }
        if let Some(phase) = app.metadata.phase {
            info!("  @phase: {}", phase);
        }
        if let Some(sla_latency) = app.metadata.sla_latency_p99 {
            info!("  @sla.latency.p99: {}", sla_latency);
        }
        if let Some(sla_availability) = app.metadata.sla_availability {
            info!("  @sla.availability: {}", sla_availability);
        }
        if let Some(data_retention) = app.metadata.data_retention {
            info!("  @data_retention: {}", data_retention);
        }
        if let Some(compliance) = app.metadata.compliance {
            info!("  @compliance: {}", compliance);
        }

        // Pre-deployment SQL validation to prevent runtime failures
        // Use base_dir for resolving relative config file paths in SQL
        info!("Validating SQL application before deployment...");
        let validator = if let Some(ref base_dir) = self.base_dir {
            SqlValidator::with_base_dir(base_dir)
        } else {
            SqlValidator::new()
        };

        // Reconstruct the SQL content from the application statements for validation
        let sql_content = app
            .statements
            .iter()
            .map(|stmt| stmt.sql.clone())
            .collect::<Vec<String>>()
            .join(";\n");

        let validation_result = validator.validate_sql_content(&sql_content);

        if !validation_result.is_valid {
            error!(
                "SQL validation failed for application '{}':",
                app.metadata.name
            );
            for query_result in &validation_result.query_results {
                if !query_result.parsing_errors.is_empty()
                    || !query_result.configuration_errors.is_empty()
                {
                    error!(
                        "Query {} (line {}): {}",
                        query_result.query_index + 1,
                        query_result.start_line,
                        query_result
                            .query_text
                            .chars()
                            .take(100)
                            .collect::<String>()
                    );

                    for error in &query_result.parsing_errors {
                        error!(
                            "  Parsing Error: {} (line: {:?})",
                            error.message, error.line
                        );
                    }

                    for error in &query_result.configuration_errors {
                        error!(
                            "  Configuration Error: {} (line: {:?})",
                            error.message, error.line
                        );
                    }
                }
            }

            for global_error in &validation_result.global_errors {
                error!("  Global Error: {}", global_error);
            }

            for missing_config in &validation_result
                .configuration_summary
                .missing_configurations
            {
                error!("  Configuration Issue: {}", missing_config);
            }

            return Err(SqlError::parse_error(
                format!(
                    "SQL validation failed for application '{}'. Found {} invalid queries out of {} total queries. Deployment aborted to prevent runtime failures.",
                    app.metadata.name,
                    validation_result.total_queries - validation_result.valid_queries,
                    validation_result.total_queries
                ),
                None,
            ));
        }

        info!(
            "SQL validation passed: {}/{} queries validated successfully",
            validation_result.valid_queries, validation_result.total_queries
        );

        let mut deployed_jobs = Vec::new();

        // Log app-level observability configuration
        if let Some(metrics_enabled) = app.metadata.observability_metrics_enabled {
            info!("  @observability.metrics.enabled: {}", metrics_enabled);
        }
        if let Some(tracing_enabled) = app.metadata.observability_tracing_enabled {
            info!("  @observability.tracing.enabled: {}", tracing_enabled);
        }
        if let Some(profiling_enabled) = app.metadata.observability_profiling_enabled {
            info!("  @observability.profiling.enabled: {}", profiling_enabled);
        }

        // Deploy statements in order
        for stmt in &app.statements {
            match stmt.statement_type {
                crate::velostream::sql::app_parser::StatementType::StartJob
                | crate::velostream::sql::app_parser::StatementType::DeployJob
                | crate::velostream::sql::app_parser::StatementType::Select
                | crate::velostream::sql::app_parser::StatementType::CreateStream
                | crate::velostream::sql::app_parser::StatementType::CreateTable => {
                    // Extract job name from the SQL statement
                    // Priority: @job_name annotation > stmt.name > auto-generated
                    let job_name = if let Some(name) = &stmt.name {
                        name.clone()
                    } else {
                        // Check for @job_name annotation in parsed query
                        let custom_job_name =
                            if let Ok(parsed_query) = StreamingSqlParser::new().parse(&stmt.sql) {
                                match parsed_query {
                                    StreamingQuery::CreateStream { job_name, .. } => job_name,
                                    _ => None,
                                }
                            } else {
                                None
                            };

                        if let Some(custom_name) = custom_job_name {
                            info!(
                                "Using custom job name from @job_name annotation: '{}'",
                                custom_name
                            );
                            custom_name
                        } else {
                            // Generate compact, meaningful job name: filename_snippet_timestamp_id
                            let file_prefix = source_filename
                                .as_ref()
                                .and_then(|path| {
                                    std::path::Path::new(path)
                                        .file_stem()
                                        .and_then(|s| s.to_str())
                                })
                                .map(|name| {
                                    // Take first few chars of filename, clean it up
                                    name.chars()
                                        .filter(|c| c.is_alphanumeric() || *c == '_')
                                        .take(8)
                                        .collect::<String>()
                                })
                                .unwrap_or_else(|| "app".to_string());

                            let sql_snippet = Self::extract_sql_snippet(&stmt.sql);
                            let timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                % 100000; // Last 5 digits for compactness
                            format!(
                                "{}_{}_{}_{:02}",
                                file_prefix, sql_snippet, timestamp, stmt.order
                            )
                        }
                    };

                    // Determine topic from statement dependencies or use default
                    let topic = if !stmt.dependencies.is_empty() {
                        stmt.dependencies[0].clone()
                    } else if let Some(ref default) = default_topic {
                        default.clone()
                    } else {
                        format!("processed_data_{}", job_name) // Auto-generate topic for SELECT statements
                    };

                    // Merge app-level observability settings with per-stream settings
                    // Per-stream settings override app-level if explicitly set
                    let mut merged_sql = stmt.sql.clone();

                    // Inject app-level observability as comment annotations
                    // These will be parsed by extract_streaming_config_from_query
                    if let Some(true) = app.metadata.observability_metrics_enabled {
                        if !merged_sql.contains("'observability.metrics.enabled'")
                            && !merged_sql.contains("@observability.metrics.enabled")
                        {
                            merged_sql.push_str("\n-- App-level observability injection");
                            merged_sql.push_str("\n-- @observability.metrics.enabled: true");
                        }
                    }
                    if let Some(true) = app.metadata.observability_tracing_enabled {
                        if !merged_sql.contains("'observability.tracing.enabled'")
                            && !merged_sql.contains("@observability.tracing.enabled")
                        {
                            merged_sql.push_str("\n-- @observability.tracing.enabled: true");
                        }
                    }
                    if let Some(profiling_mode) = app.metadata.observability_profiling_enabled {
                        if !merged_sql.contains("'observability.profiling.enabled'")
                            && !merged_sql.contains("@observability.profiling.enabled")
                        {
                            merged_sql.push_str(&format!(
                                "\n-- @observability.profiling.enabled: {}",
                                profiling_mode
                            ));
                        }
                    }

                    // Deploy the job - fail entire deployment if any single job fails
                    match self
                        .deploy_job(
                            job_name.clone(),
                            app.metadata.version.clone(),
                            merged_sql,
                            topic,
                            app.metadata.application.clone(),
                            Some(get_instance_id()),
                        )
                        .await
                    {
                        Ok(()) => {
                            info!("Successfully deployed job '{}' from application", job_name);
                            deployed_jobs.push(job_name);
                        }
                        Err(e) => {
                            error!(
                                "Failed to deploy job '{}' from application '{}': {:?}",
                                job_name, app.metadata.name, e
                            );

                            // CLEANUP: Stop any jobs that were already deployed to prevent partial state
                            if !deployed_jobs.is_empty() {
                                error!(
                                    "Cleaning up {} already-deployed jobs to prevent partial deployment state",
                                    deployed_jobs.len()
                                );
                                for cleanup_job in &deployed_jobs {
                                    if let Err(cleanup_err) = self.stop_job(cleanup_job).await {
                                        warn!(
                                            "Failed to cleanup job '{}' during rollback: {:?}",
                                            cleanup_job, cleanup_err
                                        );
                                    } else {
                                        info!(
                                            "Successfully cleaned up job '{}' during rollback",
                                            cleanup_job
                                        );
                                    }
                                }
                            }

                            // ABORT ENTIRE DEPLOYMENT - any single job failure should stop everything
                            return Err(SqlError::execution_error(
                                format!(
                                    "Deployment of application '{}' aborted: Job '{}' failed to deploy: {}. {} previously deployed jobs were stopped to prevent partial deployment state.",
                                    app.metadata.name,
                                    job_name,
                                    e,
                                    deployed_jobs.len()
                                ),
                                Some(stmt.sql.clone()),
                            ));
                        }
                    }
                }
                _ => {
                    info!("Skipping non-job statement: {:?}", stmt.statement_type);
                }
            }
        }

        info!(
            "Successfully deployed {} jobs from SQL application '{}'",
            deployed_jobs.len(),
            app.metadata.name
        );
        Ok(deployed_jobs)
    }

    /// Extract a meaningful snippet from SQL for job naming
    /// Examples:
    /// - "CREATE STREAM raw_transactions AS SELECT..." -> "stream_raw_transactions"
    /// - "CREATE TABLE merchant_analytics AS SELECT..." -> "table_merchant_analytics"  
    /// - "CREATE SINK high_value_export WITH..." -> "sink_high_value_export"
    /// - "SELECT customer_id, amount FROM transactions" -> "sel_customer_transactions"
    /// - "SELECT COUNT(*) FROM fraud_alerts WHERE..." -> "sel_count_fraud_alerts"
    fn extract_sql_snippet(sql: &str) -> String {
        // Clean and normalize the SQL
        let sql_clean = sql
            .to_lowercase()
            .replace(['\n', '\r', '\t'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        // Ensure it's a valid identifier (alphanumeric + underscore)
        sql_clean
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .get(..30) // Increase limit to 30 chars for more descriptive names
            .unwrap_or("job")
            .to_string()
    }

    /// Extract job processing configuration from query properties
    ///
    /// Supports the 'mode' property to control processor behavior:
    /// - mode='simple': Single-threaded, best-effort delivery (LogAndContinue failures)
    /// - mode='transactional': Single-threaded, at-least-once delivery (FailBatch failures)
    ///
    /// Example:
    /// ```sql
    /// CREATE STREAM analytics AS
    /// SELECT symbol, AVG(price) FROM market_data GROUP BY symbol
    /// WITH ('mode' = 'transactional', 'max_batch_size' = '1000');
    /// ```
    fn extract_job_config_from_query(query: &StreamingQuery) -> JobProcessingConfig {
        let properties = Self::get_query_properties(query);

        let mut config = JobProcessingConfig::default();

        // Extract mode property to determine transaction behavior
        // mode='transactional' sets use_transactions=true and FailBatch strategy
        // mode='simple' keeps use_transactions=false and LogAndContinue strategy (default)
        if let Some(mode) = properties.get("mode") {
            match mode.to_lowercase().as_str() {
                "transactional" => {
                    config.use_transactions = true;
                    config.failure_strategy = FailureStrategy::FailBatch;
                }
                "simple" => {
                    config.use_transactions = false;
                    config.failure_strategy = FailureStrategy::LogAndContinue;
                }
                "adaptive" => {
                    // Adaptive mode can use either strategy; use transactional for safety
                    config.use_transactions = true;
                    config.failure_strategy = FailureStrategy::FailBatch;
                }
                _ => {
                    warn!("Unknown mode: '{}', using default", mode);
                }
            }
        }

        // Legacy support: Extract use_transactions for backward compatibility
        if let Some(use_tx) = properties.get("use_transactions") {
            if use_tx.to_lowercase() == "true" {
                config.use_transactions = true;
                if matches!(config.failure_strategy, FailureStrategy::LogAndContinue) {
                    config.failure_strategy = FailureStrategy::FailBatch;
                }
            }
        }

        // Extract failure_strategy (can override mode-inferred strategy)
        if let Some(strategy) = properties.get("failure_strategy") {
            config.failure_strategy = match strategy.as_str() {
                "RetryWithBackoff" => FailureStrategy::RetryWithBackoff,
                "LogAndContinue" => FailureStrategy::LogAndContinue,
                "FailBatch" => FailureStrategy::FailBatch,
                "SendToDLQ" => FailureStrategy::SendToDLQ,
                _ => config.failure_strategy, // Keep existing
            };
        }

        // Extract retry_backoff (milliseconds)
        if let Some(backoff) = properties.get("retry_backoff") {
            if let Ok(ms) = backoff.parse::<u64>() {
                config.retry_backoff = Duration::from_millis(ms);
            }
        }

        // Extract max_retries
        if let Some(retries) = properties.get("max_retries") {
            if let Ok(max_retries) = retries.parse::<u32>() {
                config.max_retries = max_retries;
            }
        }

        // Extract max_batch_size
        if let Some(batch_size) = properties.get("max_batch_size") {
            if let Ok(size) = batch_size.parse::<usize>() {
                config.max_batch_size = size;
            }
        }

        // Extract batch_timeout (milliseconds)
        if let Some(timeout) = properties.get("batch_timeout") {
            if let Ok(ms) = timeout.parse::<u64>() {
                config.batch_timeout = Duration::from_millis(ms);
            }
        }

        config
    }

    /// Create the appropriate processor based on configuration using factory pattern
    ///
    /// This centralized method handles all processor creation including:
    /// - Simple (single-threaded, best-effort)
    /// - Transactional (single-threaded, at-least-once)
    /// - Adaptive (multi-partition parallel with automatic partitioning strategy)
    fn create_processor_for_job(
        processor_config: &JobProcessorConfig,
        parsed_query: &StreamingQuery,
        job_name: &str,
        table_registry: Option<HashMap<String, Arc<dyn UnifiedTable>>>,
        observability: Option<SharedObservabilityManager>,
    ) -> Arc<dyn JobProcessor> {
        match processor_config {
            JobProcessorConfig::Simple => {
                info!(
                    "Job '{}' using Simple processor (single-threaded, best-effort delivery)",
                    job_name
                );
                // Create processor with observability for @metric annotation support
                let config = JobProcessingConfig {
                    use_transactions: false,
                    failure_strategy: FailureStrategy::LogAndContinue,
                    ..Default::default()
                };
                let mut processor = SimpleJobProcessor::with_observability(config, observability);
                if let Some(tables) = table_registry {
                    processor.set_table_registry(tables);
                }
                Arc::new(processor)
            }
            JobProcessorConfig::Transactional => {
                info!(
                    "Job '{}' using Transactional processor (single-threaded, at-least-once delivery)",
                    job_name
                );
                // Create processor with observability for @metric annotation support
                let config = JobProcessingConfig {
                    use_transactions: true,
                    failure_strategy: FailureStrategy::FailBatch,
                    ..Default::default()
                };
                let mut processor =
                    TransactionalJobProcessor::with_observability(config, observability);
                if let Some(tables) = table_registry {
                    processor.set_table_registry(tables);
                }
                Arc::new(processor)
            }
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => {
                info!(
                    "Job '{}' using Adaptive processor (multi-partition parallel) with {} partitions",
                    job_name,
                    num_partitions.unwrap_or_else(|| num_cpus::get().max(1))
                );

                // Extract partitioning strategy from query properties if specified
                let partitioning_strategy =
                    Self::extract_partitioning_strategy_from_query(parsed_query);

                // Enable auto-selection from query if no explicit strategy provided
                // CRITICAL: User explicit strategy takes priority and is NEVER overridden
                let auto_select_from_query = if partitioning_strategy.is_none() {
                    Some(Arc::new(parsed_query.clone()))
                } else {
                    None
                };

                // Use factory for consistent processor creation with observability for @metric annotations
                JobProcessorFactory::create_adaptive_full_with_observability(
                    *num_partitions,
                    *enable_core_affinity,
                    partitioning_strategy,
                    auto_select_from_query,
                    table_registry,
                    1000, // empty_batch_count - production default
                    1000, // wait_on_empty_batch_ms - production default
                    observability,
                )
            }
        }
    }

    /// Extract partitioning strategy from SQL query properties
    ///
    /// Supports specifying the partitioning strategy via SQL annotations:
    ///
    /// Example:
    /// ```sql
    /// -- partitioning_strategy: smart_repartition
    /// SELECT symbol, AVG(price) FROM market_data GROUP BY symbol
    /// ```
    ///
    /// Supported strategies:
    /// - "always_hash" (default): Hashes GROUP BY columns, guarantees correctness
    /// - "smart_repartition": Detects if source partition key matches GROUP BY key
    /// - "sticky_partition": Uses source partition affinity, zero data movement
    /// - "round_robin": Distributes evenly, only for non-aggregated queries
    fn extract_partitioning_strategy_from_query(query: &StreamingQuery) -> Option<String> {
        let properties = Self::get_query_properties(query);
        properties.get("partitioning_strategy").cloned()
    }

    /// Query data sources for their partition count limits and return the minimum.
    ///
    /// Creates temporary DataSource objects to query their `partition_count()` method.
    /// The minimum partition count across all sources is returned to ensure compatibility.
    ///
    /// # Returns
    /// - `Some(n)` if any source has a fixed partition limit (use the minimum)
    /// - `None` if all sources support dynamic partitioning (use system default)
    async fn query_source_partition_limits(sources: &[DataSourceRequirement]) -> Option<usize> {
        use crate::velostream::datasource::DataSource;
        use crate::velostream::datasource::file::FileDataSource;

        let mut min_partitions: Option<usize> = None;

        for source in sources {
            // Query partition count from DataSource trait implementation
            // Note: We create minimal instances just to query the static partition_count property
            let source_partitions: Option<usize> = match source.source_type {
                DataSourceType::File => {
                    // FileDataSource::new() requires no arguments
                    let ds = FileDataSource::new();
                    ds.partition_count()
                }
                DataSourceType::Kafka => {
                    // Kafka supports dynamic partitioning (trait default returns None)
                    None
                }
                _ => None, // Unknown source types default to dynamic partitioning
            };

            if let Some(count) = source_partitions {
                info!(
                    "Source '{}' ({:?}) supports {} partition(s)",
                    source.name, source.source_type, count
                );
                min_partitions = Some(min_partitions.map_or(count, |min| min.min(count)));
            }
        }

        min_partitions
    }

    /// Adjust processor config to respect source partition limits.
    ///
    /// If any source only supports a limited number of partitions (e.g., file sources
    /// support only 1), the processor config is adjusted accordingly.
    async fn apply_source_partition_limit(
        config: JobProcessorConfig,
        sources: &[DataSourceRequirement],
    ) -> JobProcessorConfig {
        let source_limit = Self::query_source_partition_limits(sources).await;

        match (config, source_limit) {
            // Source has a partition limit - override Adaptive to use that limit
            (
                JobProcessorConfig::Adaptive {
                    enable_core_affinity,
                    num_partitions,
                },
                Some(limit),
            ) => {
                // Use the minimum of: source limit, explicit partition count, or the limit itself
                let effective_partitions = num_partitions.map(|n| n.min(limit)).unwrap_or(limit);

                if num_partitions.is_none_or(|n| n > limit) {
                    info!(
                        "Limiting partition count to {} based on source constraints (requested: {:?})",
                        limit, num_partitions
                    );
                }

                JobProcessorConfig::Adaptive {
                    num_partitions: Some(effective_partitions),
                    enable_core_affinity,
                }
            }
            // No source constraint or non-Adaptive config - keep as-is
            (config, _) => config,
        }
    }

    /// Wire job_mode annotation from StreamingQuery to JobProcessorConfig
    ///
    /// Per-query job_mode takes precedence over the server-level default configuration.
    /// This allows individual SQL queries to specify their execution strategy:
    /// - Simple: Single-threaded, best-effort delivery
    /// - Transactional: Single-threaded, at-least-once with transactions
    /// - Adaptive: Multi-partition parallel execution with configurable strategy
    ///
    /// # Arguments
    /// * `query` - The parsed StreamingQuery to extract job_mode from
    /// * `default_config` - The server-level default JobProcessorConfig (fallback)
    ///
    /// # Returns
    /// The JobProcessorConfig to use for this query (either from query annotation or default)
    fn get_processor_config_from_query(
        query: &StreamingQuery,
        raw_sql: &str,
        default_config: &JobProcessorConfig,
    ) -> JobProcessorConfig {
        // First, try to parse job annotations from raw SQL text
        // This works for all query types including CREATE STREAM, CREATE TABLE, etc.
        let (parsed_job_mode, _, parsed_num_partitions, _) =
            SqlAnnotationParser::parse_job_annotations(raw_sql);

        // Use parsed annotations if available, otherwise fall back to AST fields (for SELECT)
        let (job_mode, num_partitions) = if parsed_job_mode.is_some() {
            (parsed_job_mode, parsed_num_partitions)
        } else if let StreamingQuery::Select {
            job_mode,
            num_partitions,
            ..
        } = query
        {
            (*job_mode, *num_partitions)
        } else {
            (None, None)
        };

        // If we have a job_mode annotation, use it
        if let Some(mode) = job_mode {
            match mode {
                JobProcessorMode::Simple => {
                    info!("Using Simple processor mode from query annotation");
                    JobProcessorConfig::Simple
                }
                JobProcessorMode::Transactional => {
                    info!("Using Transactional processor mode from query annotation");
                    JobProcessorConfig::Transactional
                }
                JobProcessorMode::Adaptive => {
                    info!(
                        "Using Adaptive processor mode from query annotation with {} partitions",
                        num_partitions.unwrap_or(0)
                    );
                    JobProcessorConfig::Adaptive {
                        num_partitions,
                        enable_core_affinity: false,
                    }
                }
            }
        } else {
            // No job_mode annotation, use server default
            default_config.clone()
        }
    }

    /// Extract properties from different query types.
    /// Note: PRIMARY KEY fields are now passed directly to sink constructors via
    /// DataSinkRequirement.primary_keys, not through properties.
    fn get_query_properties(query: &StreamingQuery) -> HashMap<String, String> {
        match query {
            StreamingQuery::CreateStream { properties, .. } => properties.clone(),
            StreamingQuery::CreateTable { properties, .. } => properties.clone(),
            StreamingQuery::StartJob { properties, .. } => properties.clone(),
            _ => HashMap::new(),
        }
    }

    /// Extract batch configuration from SQL WITH clauses
    fn extract_batch_config_from_query(
        query: &StreamingQuery,
    ) -> Result<Option<crate::velostream::datasource::BatchConfig>, SqlError> {
        let properties = Self::get_query_properties(query);

        if properties.is_empty() {
            return Ok(None);
        }

        // Convert properties to WITH clause format
        let with_clause = properties
            .iter()
            .map(|(k, v)| format!("'{}' = '{}'", k, v))
            .collect::<Vec<_>>()
            .join(",\n    ");

        let with_clause_text = format!("WITH (\n    {}\n)", with_clause);

        info!(
            "Parsing WITH clause for batch configuration: {}",
            with_clause_text
        );

        // Parse WITH clause using our batch configuration parser
        let parser = WithClauseParser::new();
        match parser.parse_with_clause(&with_clause_text) {
            Ok(config) => {
                if config.batch_config.is_some() {
                    info!("Successfully extracted batch configuration from WITH clause");
                    Ok(config.batch_config)
                } else {
                    debug!("No batch configuration found in WITH clause");
                    Ok(None)
                }
            }
            Err(e) => {
                warn!("Failed to parse WITH clause for batch configuration: {}", e);
                // Don't fail the job, just log and continue without batch config
                Ok(None)
            }
        }
    }

    /// Extract StreamingConfig from query WITH clause
    ///
    /// Wires Phase 1B-4 configurations from SQL WITH clause to StreamingConfig:
    /// - Event-time processing and watermarks
    /// - Circuit breakers and resource limits
    /// - Observability and tracing
    fn extract_streaming_config_from_query(
        query: &StreamingQuery,
    ) -> Result<StreamingConfig, SqlError> {
        let properties = Self::get_query_properties(query);
        let mut config = StreamingConfig::default();

        if properties.is_empty() {
            debug!("No WITH clause properties found, using default StreamingConfig");
            return Ok(config);
        }

        // ====================================================================
        // PHASE 1B: EVENT-TIME & WATERMARKS
        // ====================================================================

        // Enable watermarks if event-time field is specified
        if properties.contains_key("event.time.field") {
            info!("Enabling watermarks - event.time.field detected");
            config.enable_watermarks = true;
        }

        // Parse watermark strategy
        if let Some(strategy_str) = properties.get("watermark.strategy") {
            use crate::velostream::sql::execution::config::WatermarkStrategy;

            config.watermark_strategy = match strategy_str.as_str() {
                "bounded_out_of_orderness" => {
                    info!("Watermark strategy: BoundedOutOfOrderness");
                    WatermarkStrategy::BoundedOutOfOrderness
                }
                "ascending" | "ascending_timestamps" => {
                    info!("Watermark strategy: AscendingTimestamps");
                    WatermarkStrategy::AscendingTimestamps
                }
                "custom" => {
                    info!("Watermark strategy: Custom");
                    WatermarkStrategy::Custom
                }
                other => {
                    warn!("Unknown watermark strategy '{}', using default", other);
                    WatermarkStrategy::None
                }
            };
        }

        // Parse late data strategy
        if let Some(late_data_str) = properties.get("late.data.strategy") {
            use crate::velostream::sql::execution::config::LateDataStrategy;

            config.late_data_strategy = match late_data_str.as_str() {
                "dead_letter" => {
                    info!("Late data strategy: DeadLetterQueue");
                    LateDataStrategy::DeadLetterQueue
                }
                "update_previous" | "update_previous_window" => {
                    info!("Late data strategy: UpdatePreviousWindow");
                    LateDataStrategy::UpdatePreviousWindow
                }
                "include_in_next" | "include_in_next_window" => {
                    info!("Late data strategy: IncludeInNextWindow");
                    LateDataStrategy::IncludeInNextWindow
                }
                "drop" => {
                    info!("Late data strategy: Drop");
                    LateDataStrategy::Drop
                }
                other => {
                    warn!("Unknown late data strategy '{}', using Drop", other);
                    LateDataStrategy::Drop
                }
            };
        }

        // ====================================================================
        // PHASE 2: CIRCUIT BREAKERS & RESOURCE LIMITS
        // ====================================================================

        // Enable circuit breakers
        if let Some(enabled) = properties.get("circuit.breaker.enabled") {
            if enabled.eq_ignore_ascii_case("true") {
                info!("Enabling circuit breakers");
                config.enable_circuit_breakers = true;
                config.enable_enhanced_errors = true; // Circuit breakers require enhanced errors
            }
        }

        // Enable resource limits
        if let Some(max_memory_str) = properties.get("max.memory.mb") {
            if let Ok(max_memory_mb) = max_memory_str.parse::<usize>() {
                let max_memory_bytes = max_memory_mb * 1024 * 1024;
                info!(
                    "Setting max memory limit: {} MB ({} bytes)",
                    max_memory_mb, max_memory_bytes
                );
                config.max_total_memory = Some(max_memory_bytes);
                config.enable_resource_limits = true;
            }
        }

        // Enable resource monitoring
        if let Some(enabled) = properties.get("resource.monitoring.enabled") {
            if enabled.eq_ignore_ascii_case("true") {
                info!("Enabling resource monitoring");
                config.enable_resource_monitoring = true;
            }
        }

        // ====================================================================
        // PHASE 4: OBSERVABILITY
        // ====================================================================

        // Enable distributed tracing
        if let Some(enabled) = properties.get("observability.tracing.enabled") {
            if enabled.eq_ignore_ascii_case("true") {
                info!("Enabling distributed tracing");
                config.enable_distributed_tracing = true;

                // Initialize tracing config if not already set
                if config.tracing_config.is_none() {
                    use crate::velostream::sql::execution::config::TracingConfig;
                    let mut tracing_config = TracingConfig::development();

                    // Parse span name if provided
                    if let Some(span_name) = properties.get("observability.span.name") {
                        tracing_config.service_name = span_name.clone();
                    }

                    // Parse OTLP endpoint if provided
                    if let Some(endpoint) = properties.get("tracing.otlp_endpoint") {
                        tracing_config.otlp_endpoint = Some(endpoint.clone());
                    }

                    config.tracing_config = Some(tracing_config);
                }
            }
        }

        // Enable Prometheus metrics
        if let Some(enabled) = properties.get("observability.metrics.enabled") {
            if enabled.eq_ignore_ascii_case("true") {
                info!("Enabling Prometheus metrics export");
                config.enable_prometheus_metrics = true;

                // Initialize Prometheus config if not already set
                if config.prometheus_config.is_none() {
                    use crate::velostream::sql::execution::config::PrometheusConfig;
                    let mut prometheus_config = PrometheusConfig::default();

                    // Parse port if provided
                    if let Some(port_str) = properties.get("prometheus.port") {
                        if let Ok(port) = port_str.parse::<u16>() {
                            info!("Setting Prometheus metrics port: {}", port);
                            prometheus_config.port = port;
                        }
                    }

                    // Parse histogram buckets if provided
                    if let Some(buckets_str) = properties.get("prometheus.histogram.buckets") {
                        info!("Custom histogram buckets specified: {}", buckets_str);
                        // Store for later use - the MetricsProvider will parse this
                    }

                    config.prometheus_config = Some(prometheus_config);
                }
            }
        }

        // Enable performance profiling
        if let Some(enabled) = properties.get("observability.profiling.enabled") {
            if enabled.eq_ignore_ascii_case("true") {
                info!("Enabling performance profiling");
                config.enable_performance_profiling = true;

                // Initialize profiling config if not already set
                if config.profiling_config.is_none() {
                    use crate::velostream::sql::execution::config::ProfilingConfig;
                    config.profiling_config = Some(ProfilingConfig::development());
                }
            }
        }

        info!(
            "StreamingConfig extracted: watermarks={}, circuit_breakers={}, tracing={}, metrics={}",
            config.enable_watermarks,
            config.enable_circuit_breakers,
            config.enable_distributed_tracing,
            config.enable_prometheus_metrics
        );

        Ok(config)
    }

    /// Build deployment context from environment and job metadata
    ///
    /// Extracts deployment context (node_id, region, version) from:
    /// 1. Environment variables (NODE_ID, REGION, etc.)
    /// 2. Job metadata (version parameter)
    /// 3. Hostname fallback
    ///
    /// This context is attached to all error messages for production observability.
    fn build_deployment_context(_job_name: &str, version: &str) -> DeploymentContext {
        let node_id = std::env::var("NODE_ID")
            .ok()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .or_else(|| std::env::var("POD_NAME").ok());

        let node_name = std::env::var("NODE_NAME")
            .ok()
            .or_else(|| std::env::var("SERVICE_NAME").ok());

        let region = std::env::var("AWS_REGION")
            .ok()
            .or_else(|| std::env::var("REGION").ok())
            .or_else(|| std::env::var("DEPLOYMENT_REGION").ok());

        // Version comes from job metadata, fallback to environment or "unknown"
        let app_version = if version.is_empty() {
            std::env::var("APP_VERSION").ok()
        } else {
            Some(version.to_string())
        };

        DeploymentContext {
            node_id,
            node_name,
            region,
            version: app_version,
        }
    }

    /// Parse duration string (e.g., "5s", "100ms", "1m") to Duration
    fn parse_duration_string(s: &str) -> Result<Duration, SqlError> {
        let s = s.trim();

        // Try parsing with units
        if let Some(num_str) = s.strip_suffix("ms") {
            if let Ok(ms) = num_str.parse::<u64>() {
                return Ok(Duration::from_millis(ms));
            }
        }

        if let Some(num_str) = s.strip_suffix("s") {
            if let Ok(secs) = num_str.parse::<u64>() {
                return Ok(Duration::from_secs(secs));
            }
        }

        if let Some(num_str) = s.strip_suffix("m") {
            if let Ok(mins) = num_str.parse::<u64>() {
                return Ok(Duration::from_secs(mins * 60));
            }
        }

        if let Some(num_str) = s.strip_suffix("h") {
            if let Ok(hours) = num_str.parse::<u64>() {
                return Ok(Duration::from_secs(hours * 3600));
            }
        }

        // Try parsing as raw seconds
        if let Ok(secs) = s.parse::<u64>() {
            return Ok(Duration::from_secs(secs));
        }

        Err(SqlError::ExecutionError {
            message: format!(
                "Invalid duration string: '{}'. Expected format like '5s', '100ms', '1m', '1h'",
                s
            ),
            query: None,
        })
    }
}
