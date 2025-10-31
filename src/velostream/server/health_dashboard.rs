//! Health Dashboard Endpoints for Table Loading Progress
//!
//! Provides HTTP REST API endpoints for monitoring table loading progress,
//! health status, and real-time metrics. Designed for integration with
//! monitoring dashboards and alerting systems.

use crate::velostream::server::progress_monitoring::{LoadingSummary, TableLoadProgress};
use crate::velostream::server::progress_streaming::{
    ConnectionStats, ProgressEvent, ProgressStreamingServer,
};
use crate::velostream::server::table_registry::{
    EnhancedTableHealth, TableMetadata, TableRegistry, TableStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Health dashboard service providing REST API endpoints
pub struct HealthDashboard {
    /// Table registry for accessing table information
    table_registry: Arc<TableRegistry>,

    /// Progress streaming server for real-time updates
    progress_streaming: Option<Arc<ProgressStreamingServer>>,
}

impl HealthDashboard {
    /// Create a new health dashboard
    pub fn new(table_registry: Arc<TableRegistry>) -> Self {
        Self {
            table_registry,
            progress_streaming: None,
        }
    }

    /// Create a health dashboard with progress streaming support
    pub fn with_streaming(
        table_registry: Arc<TableRegistry>,
        progress_streaming: Arc<ProgressStreamingServer>,
    ) -> Self {
        Self {
            table_registry,
            progress_streaming: Some(progress_streaming),
        }
    }

    /// GET /health/tables - Get enhanced health status for all tables
    pub async fn get_tables_health(&self) -> Result<TablesHealthResponse, HealthDashboardError> {
        let enhanced_health = self.table_registry.get_enhanced_health_status().await;
        let summary = self.table_registry.get_loading_summary().await;

        let mut healthy_count = 0;
        let mut warning_count = 0;
        let mut critical_count = 0;

        for health in &enhanced_health {
            match health.status {
                TableStatus::Active
                | TableStatus::BackgroundJobFinished
                | TableStatus::NoBackgroundJob => {
                    if health.is_healthy {
                        healthy_count += 1;
                    } else {
                        warning_count += 1;
                    }
                }
                TableStatus::Error(_) => critical_count += 1,
                TableStatus::Populating => {
                    if health.is_healthy {
                        healthy_count += 1;
                    } else {
                        warning_count += 1;
                    }
                }
            }
        }

        let overall_status = if critical_count > 0 {
            OverallHealthStatus::Critical
        } else if warning_count > 0 {
            OverallHealthStatus::Warning
        } else {
            OverallHealthStatus::Healthy
        };

        Ok(TablesHealthResponse {
            overall_status,
            total_tables: enhanced_health.len(),
            healthy_count,
            warning_count,
            critical_count,
            loading_summary: summary,
            tables: enhanced_health,
            timestamp: chrono::Utc::now(),
        })
    }

    /// GET /health/table/{name} - Get detailed health for a specific table
    pub async fn get_table_health(
        &self,
        table_name: &str,
    ) -> Result<TableHealthResponse, HealthDashboardError> {
        // Get basic table metadata
        let metadata = self
            .table_registry
            .get_table_stats(table_name)
            .await
            .ok_or_else(|| HealthDashboardError::TableNotFound(table_name.to_string()))?;

        // Get loading progress if available
        let loading_progress = self
            .table_registry
            .get_table_loading_progress(table_name)
            .await;

        // Check if table exists
        let exists = self.table_registry.exists(table_name).await;

        // Determine health status
        let is_healthy = matches!(
            metadata.status,
            TableStatus::Active
                | TableStatus::BackgroundJobFinished
                | TableStatus::NoBackgroundJob
                | TableStatus::Populating
        ) && exists;

        let mut issues = Vec::new();
        if !exists {
            issues.push("Table does not exist".to_string());
        }
        if let TableStatus::Error(ref msg) = metadata.status {
            issues.push(format!("Error: {}", msg));
        }

        Ok(TableHealthResponse {
            table_name: table_name.to_string(),
            exists,
            metadata,
            loading_progress,
            is_healthy,
            issues,
            timestamp: chrono::Utc::now(),
        })
    }

    /// GET /health/progress - Get loading progress for all tables
    pub async fn get_loading_progress(
        &self,
    ) -> Result<LoadingProgressResponse, HealthDashboardError> {
        let progress = self.table_registry.get_loading_progress().await;
        let summary = self.table_registry.get_loading_summary().await;

        // Calculate overall progress if possible
        let overall_progress = if summary.total_tables > 0 {
            Some((summary.completed as f64 / summary.total_tables as f64) * 100.0)
        } else {
            None
        };

        // Estimate overall completion time based on current loading rates
        let estimated_completion = if summary.loading > 0 {
            let total_loading_rate: f64 = progress
                .values()
                .filter_map(|p| {
                    if matches!(
                        p.status,
                        crate::velostream::server::progress_monitoring::TableLoadStatus::Loading
                    ) {
                        Some(p.loading_rate)
                    } else {
                        None
                    }
                })
                .sum();

            let total_remaining: usize = progress
                .values()
                .filter_map(|p| {
                    if let Some(total) = p.total_records_expected {
                        Some(total.saturating_sub(p.records_loaded))
                    } else {
                        None
                    }
                })
                .sum();

            if total_loading_rate > 0.0 && total_remaining > 0 {
                let eta_seconds = total_remaining as f64 / total_loading_rate;
                Some(chrono::Utc::now() + chrono::Duration::seconds(eta_seconds as i64))
            } else {
                None
            }
        } else {
            None
        };

        Ok(LoadingProgressResponse {
            summary,
            overall_progress,
            estimated_completion,
            tables: progress,
            timestamp: chrono::Utc::now(),
        })
    }

    /// GET /health/connections - Get streaming connection statistics
    pub async fn get_connection_stats(
        &self,
    ) -> Result<ConnectionStatsResponse, HealthDashboardError> {
        if let Some(streaming) = self.progress_streaming.as_ref() {
            let stats = streaming.get_connection_stats();
            Ok(ConnectionStatsResponse {
                stats,
                streaming_available: true,
                timestamp: chrono::Utc::now(),
            })
        } else {
            Ok(ConnectionStatsResponse {
                stats: ConnectionStats {
                    active_connections: 0,
                    max_connections: 0,
                    utilization_percentage: 0.0,
                },
                streaming_available: false,
                timestamp: chrono::Utc::now(),
            })
        }
    }

    /// GET /health/metrics - Get comprehensive health metrics for monitoring systems
    pub async fn get_health_metrics(&self) -> Result<HealthMetricsResponse, HealthDashboardError> {
        let summary = self.table_registry.get_loading_summary().await;
        let progress = self.table_registry.get_loading_progress().await;
        let table_count = self.table_registry.table_count().await;
        let is_at_capacity = self.table_registry.is_at_capacity().await;

        // Calculate performance metrics
        let total_loading_rate: f64 = progress.values().map(|p| p.loading_rate).sum();

        let total_bytes_per_second: f64 = progress.values().map(|p| p.bytes_per_second).sum();

        let avg_loading_rate = if !progress.is_empty() {
            total_loading_rate / progress.len() as f64
        } else {
            0.0
        };

        // Count tables by status
        let mut status_counts = HashMap::new();
        let enhanced_health = self.table_registry.get_enhanced_health_status().await;
        for health in &enhanced_health {
            let status_name = match health.status {
                TableStatus::Active => "active",
                TableStatus::Populating => "populating",
                TableStatus::BackgroundJobFinished => "background_finished",
                TableStatus::NoBackgroundJob => "no_background_job",
                TableStatus::Error(_) => "error",
            };
            *status_counts.entry(status_name.to_string()).or_insert(0) += 1;
        }

        // Connection stats if available
        let connection_stats = if let Some(streaming) = self.progress_streaming.as_ref() {
            Some(streaming.get_connection_stats())
        } else {
            None
        };

        Ok(HealthMetricsResponse {
            loading_summary: summary,
            total_tables: table_count,
            is_at_capacity,
            status_counts,
            performance_metrics: PerformanceMetrics {
                total_loading_rate,
                total_bytes_per_second,
                avg_loading_rate,
                active_loading_tables: progress.len(),
            },
            connection_stats,
            timestamp: chrono::Utc::now(),
        })
    }

    /// POST /health/table/{name}/wait - Wait for a table to be ready with progress monitoring
    pub async fn wait_for_table(
        &self,
        table_name: &str,
        timeout_secs: Option<u64>,
    ) -> Result<TableWaitResponse, HealthDashboardError> {
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(60));
        let start_time = std::time::Instant::now();

        match self
            .table_registry
            .wait_for_table_ready_with_progress(table_name, timeout)
            .await
        {
            Ok(status) => {
                let elapsed = start_time.elapsed();
                let final_progress = self
                    .table_registry
                    .get_table_loading_progress(table_name)
                    .await;

                Ok(TableWaitResponse {
                    table_name: table_name.to_string(),
                    success: true,
                    final_status: status,
                    elapsed_seconds: elapsed.as_secs_f64(),
                    final_progress,
                    error_message: None,
                    timestamp: chrono::Utc::now(),
                })
            }
            Err(err) => {
                let elapsed = start_time.elapsed();
                let final_progress = self
                    .table_registry
                    .get_table_loading_progress(table_name)
                    .await;

                Ok(TableWaitResponse {
                    table_name: table_name.to_string(),
                    success: false,
                    final_status: TableStatus::Error(err.to_string()),
                    elapsed_seconds: elapsed.as_secs_f64(),
                    final_progress,
                    error_message: Some(err.to_string()),
                    timestamp: chrono::Utc::now(),
                })
            }
        }
    }
}

// ========== RESPONSE TYPES ==========

/// Overall health status enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OverallHealthStatus {
    Healthy,
    Warning,
    Critical,
}

/// Response for GET /health/tables
#[derive(Debug, Clone, Serialize)]
pub struct TablesHealthResponse {
    pub overall_status: OverallHealthStatus,
    pub total_tables: usize,
    pub healthy_count: usize,
    pub warning_count: usize,
    pub critical_count: usize,
    pub loading_summary: LoadingSummary,
    pub tables: Vec<EnhancedTableHealth>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Response for GET /health/table/{name}
#[derive(Debug, Clone, Serialize)]
pub struct TableHealthResponse {
    pub table_name: String,
    pub exists: bool,
    pub metadata: TableMetadata,
    pub loading_progress: Option<TableLoadProgress>,
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Response for GET /health/progress
#[derive(Debug, Clone, Serialize)]
pub struct LoadingProgressResponse {
    pub summary: LoadingSummary,
    pub overall_progress: Option<f64>,
    pub estimated_completion: Option<chrono::DateTime<chrono::Utc>>,
    pub tables: HashMap<String, TableLoadProgress>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Response for GET /health/connections
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStatsResponse {
    pub stats: ConnectionStats,
    pub streaming_available: bool,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Performance metrics for monitoring
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceMetrics {
    pub total_loading_rate: f64,
    pub total_bytes_per_second: f64,
    pub avg_loading_rate: f64,
    pub active_loading_tables: usize,
}

/// Response for GET /health/metrics
#[derive(Debug, Clone, Serialize)]
pub struct HealthMetricsResponse {
    pub loading_summary: LoadingSummary,
    pub total_tables: usize,
    pub is_at_capacity: bool,
    pub status_counts: HashMap<String, usize>,
    pub performance_metrics: PerformanceMetrics,
    pub connection_stats: Option<ConnectionStats>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Response for POST /health/table/{name}/wait
#[derive(Debug, Clone, Serialize)]
pub struct TableWaitResponse {
    pub table_name: String,
    pub success: bool,
    pub final_status: TableStatus,
    pub elapsed_seconds: f64,
    pub final_progress: Option<TableLoadProgress>,
    pub error_message: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Errors that can occur in the health dashboard
#[derive(Debug, thiserror::Error)]
pub enum HealthDashboardError {
    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Timeout waiting for table: {0}")]
    TimeoutError(String),
}

// ========== UTILITY FUNCTIONS ==========

impl HealthDashboard {
    /// Create a Prometheus metrics string for integration with monitoring systems
    pub async fn get_prometheus_metrics(&self) -> Result<String, HealthDashboardError> {
        let metrics = self.get_health_metrics().await?;
        let mut prometheus_output = String::new();

        // Table counts
        prometheus_output.push_str(&format!(
            "# HELP velostream_tables_total Total number of tables\n\
             # TYPE velostream_tables_total gauge\n\
             velostream_tables_total {}\n\n",
            metrics.total_tables
        ));

        // Loading summary metrics
        prometheus_output.push_str(&format!(
            "# HELP velostream_tables_loading Number of tables currently loading\n\
             # TYPE velostream_tables_loading gauge\n\
             velostream_tables_loading {}\n\n",
            metrics.loading_summary.loading
        ));

        prometheus_output.push_str(&format!(
            "# HELP velostream_tables_completed Number of completed tables\n\
             # TYPE velostream_tables_completed gauge\n\
             velostream_tables_completed {}\n\n",
            metrics.loading_summary.completed
        ));

        prometheus_output.push_str(&format!(
            "# HELP velostream_tables_failed Number of failed tables\n\
             # TYPE velostream_tables_failed gauge\n\
             velostream_tables_failed {}\n\n",
            metrics.loading_summary.failed
        ));

        // Performance metrics
        prometheus_output.push_str(&format!(
            "# HELP velostream_loading_rate_total Total loading rate in records per second\n\
             # TYPE velostream_loading_rate_total gauge\n\
             velostream_loading_rate_total {}\n\n",
            metrics.performance_metrics.total_loading_rate
        ));

        prometheus_output.push_str(&format!(
            "# HELP velostream_bytes_per_second_total Total bytes processed per second\n\
             # TYPE velostream_bytes_per_second_total gauge\n\
             velostream_bytes_per_second_total {}\n\n",
            metrics.performance_metrics.total_bytes_per_second
        ));

        // Connection stats if available
        if let Some(conn_stats) = metrics.connection_stats {
            prometheus_output.push_str(&format!(
                "# HELP velostream_streaming_connections_active Active streaming connections\n\
                 # TYPE velostream_streaming_connections_active gauge\n\
                 velostream_streaming_connections_active {}\n\n",
                conn_stats.active_connections
            ));

            prometheus_output.push_str(&format!(
                "# HELP velostream_streaming_connections_utilization Connection utilization percentage\n\
                 # TYPE velostream_streaming_connections_utilization gauge\n\
                 velostream_streaming_connections_utilization {}\n\n",
                conn_stats.utilization_percentage
            ));
        }

        Ok(prometheus_output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_dashboard_creation() {
        let registry = Arc::new(TableRegistry::new());
        let dashboard = HealthDashboard::new(registry);

        // Test basic health endpoint
        let health_response = dashboard.get_tables_health().await.unwrap();
        assert_eq!(health_response.total_tables, 0);
        assert_eq!(health_response.healthy_count, 0);
    }

    #[tokio::test]
    async fn test_table_not_found() {
        let registry = Arc::new(TableRegistry::new());
        let dashboard = HealthDashboard::new(registry);

        let result = dashboard.get_table_health("nonexistent").await;
        assert!(matches!(
            result,
            Err(HealthDashboardError::TableNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_loading_progress_response() {
        let registry = Arc::new(TableRegistry::new());
        let dashboard = HealthDashboard::new(registry);

        let progress_response = dashboard.get_loading_progress().await.unwrap();
        assert!(progress_response.tables.is_empty());
        assert_eq!(progress_response.summary.total_tables, 0);
    }

    #[tokio::test]
    async fn test_prometheus_metrics_format() {
        let registry = Arc::new(TableRegistry::new());
        let dashboard = HealthDashboard::new(registry);

        let prometheus_output = dashboard.get_prometheus_metrics().await.unwrap();
        assert!(prometheus_output.contains("velostream_tables_total"));
        assert!(prometheus_output.contains("# HELP"));
        assert!(prometheus_output.contains("# TYPE"));
    }
}
