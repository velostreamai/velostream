// === PHASE 4: OBSERVABILITY MODULE ===
//
// This module provides comprehensive observability features for Velostream:
// - OpenTelemetry distributed tracing
// - Prometheus metrics collection and export
// - Performance profiling integration
// - Automatic instrumentation for SQL query execution
//
// All features are configurable and disabled by default for backward compatibility.

pub mod async_queue;
pub mod background_flusher;
pub mod error_tracker;
pub mod label_extraction;
pub mod metrics;
pub mod profiling;
pub mod query_metadata;
pub mod queue_config;
pub mod queued_span_processor;
pub mod remote_write;
pub mod resource_monitor;
pub mod span_collector;
pub mod telemetry;
pub mod tokio_span_processor;
pub mod trace_propagation;

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::StreamingConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Simplified Prometheus configuration for tests
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub port: u16,
    pub enabled: bool,
}

/// Simplified Telemetry configuration for tests
#[derive(Debug, Clone, Default)]
pub struct TelemetryConfig {
    // Empty for now, can be expanded later
}

impl PrometheusConfig {
    /// Convert to full PrometheusConfig from execution config
    fn to_full_config(&self) -> crate::velostream::sql::execution::config::PrometheusConfig {
        crate::velostream::sql::execution::config::PrometheusConfig {
            metrics_path: "/metrics".to_string(),
            bind_address: if self.port == 0 {
                "127.0.0.1".to_string()
            } else {
                "0.0.0.0".to_string()
            },
            port: self.port,
            enable_histograms: false,
            enable_query_metrics: true,
            enable_streaming_metrics: false,
            collection_interval_seconds: 15,
            max_labels_per_metric: 10,
            remote_write_enabled: false,
            remote_write_endpoint: None,
            remote_write_batch_size: 1000,
            remote_write_flush_interval_ms: 5000,
        }
    }
}

/// Central observability manager for Velostream Phase 4
#[derive(Debug)]
pub struct ObservabilityManager {
    config: StreamingConfig,
    telemetry: Option<telemetry::TelemetryProvider>,
    metrics: Option<metrics::MetricsProvider>,
    profiling: Option<profiling::ProfilingProvider>,
    initialized: bool,
}

impl ObservabilityManager {
    /// Create a new observability manager with the given configuration
    pub fn from_streaming_config(config: StreamingConfig) -> Self {
        Self {
            config,
            telemetry: None,
            metrics: None,
            profiling: None,
            initialized: false,
        }
    }

    /// Create and initialize a new observability manager with simplified configs (for tests)
    pub async fn new(
        prometheus_config: PrometheusConfig,
        _telemetry_config: TelemetryConfig,
    ) -> Result<Self, SqlError> {
        let full_prometheus = prometheus_config.to_full_config();

        let streaming_config = if prometheus_config.enabled {
            StreamingConfig::default().with_prometheus_config(full_prometheus)
        } else {
            StreamingConfig::default()
        };

        let mut manager = Self::from_streaming_config(streaming_config);
        manager.initialize().await?;
        Ok(manager)
    }

    /// Initialize all enabled observability features
    pub async fn initialize(&mut self) -> Result<(), SqlError> {
        if self.initialized {
            return Ok(());
        }

        // Extract deployment context for all providers
        let deployment_node_id = self.config.deployment_node_id.as_ref();
        if let Some(node_id) = deployment_node_id {
            log::info!("📍 Node identification: {}", node_id);
        }

        // Initialize distributed tracing if enabled
        if self.config.enable_distributed_tracing {
            if let Some(tracing_config) = &self.config.tracing_config {
                let mut telemetry =
                    telemetry::TelemetryProvider::new(tracing_config.clone()).await?;
                // Pass deployment config to telemetry
                telemetry.set_deployment_context(
                    self.config.deployment_node_id.clone(),
                    self.config.deployment_node_name.clone(),
                    self.config.deployment_region.clone(),
                )?;
                self.telemetry = Some(telemetry);
                log::info!("✅ Phase 4: Distributed tracing initialized");
            }
        }

        // Initialize Prometheus metrics if enabled
        if self.config.enable_prometheus_metrics {
            if let Some(prometheus_config) = &self.config.prometheus_config {
                let mut metrics = metrics::MetricsProvider::new(prometheus_config.clone()).await?;
                // Pass deployment config to metrics
                metrics.set_node_id(self.config.deployment_node_id.clone())?;
                self.metrics = Some(metrics);
                log::info!(
                    "✅ Phase 4: Prometheus metrics initialized on port {}",
                    prometheus_config.port
                );
            }
        }

        // Initialize performance profiling if enabled
        if self.config.enable_performance_profiling {
            if let Some(profiling_config) = &self.config.profiling_config {
                let mut profiling =
                    profiling::ProfilingProvider::new(profiling_config.clone()).await?;
                // Pass deployment config to profiling
                profiling.set_deployment_context(
                    self.config.deployment_node_id.clone(),
                    self.config.deployment_node_name.clone(),
                    self.config.deployment_region.clone(),
                )?;
                self.profiling = Some(profiling);
                log::info!("✅ Phase 4: Performance profiling initialized");
            }
        }

        self.initialized = true;

        if self.is_any_feature_enabled() {
            log::info!("🚀 Phase 4: Observability infrastructure ready");
        }

        Ok(())
    }

    /// Check if any observability features are enabled
    pub fn is_any_feature_enabled(&self) -> bool {
        self.config.enable_distributed_tracing
            || self.config.enable_prometheus_metrics
            || self.config.enable_performance_profiling
            || self.config.enable_query_analysis
    }

    /// Get telemetry provider if enabled
    pub fn telemetry(&self) -> Option<&telemetry::TelemetryProvider> {
        self.telemetry.as_ref()
    }

    /// Get metrics provider if enabled
    pub fn metrics(&self) -> Option<&metrics::MetricsProvider> {
        self.metrics.as_ref()
    }

    /// Get profiling provider if enabled
    pub fn profiling(&self) -> Option<&profiling::ProfilingProvider> {
        self.profiling.as_ref()
    }

    /// Set deployment context for error tracking (job-level customization)
    ///
    /// This allows setting or updating deployment context after initialization,
    /// enabling per-job deployment metadata in error messages across all observability integrations.
    ///
    /// Updates deployment context in:
    /// - **Metrics**: Error tracker with deployment metadata
    /// - **Telemetry**: Span attributes with node, name, and region
    /// - **Profiling**: Profiling reports with deployment identification
    pub fn set_deployment_context_for_job(
        &mut self,
        deployment_ctx: error_tracker::DeploymentContext,
    ) -> Result<(), SqlError> {
        // Update metrics provider with deployment context (for error tracking, system metrics, and SQL metrics)
        if let Some(metrics) = self.metrics.as_mut() {
            metrics.set_deployment_context(deployment_ctx.clone())?;
        }

        // Update telemetry provider with deployment context (for distributed tracing spans)
        if let Some(telemetry) = self.telemetry.as_mut() {
            telemetry.set_deployment_context(
                deployment_ctx.node_id.clone(),
                deployment_ctx.node_name.clone(),
                deployment_ctx.region.clone(),
            )?;
        }

        // Update profiling provider with deployment context (for performance profiling reports)
        if let Some(profiling) = self.profiling.as_mut() {
            profiling.set_deployment_context(
                deployment_ctx.node_id.clone(),
                deployment_ctx.node_name.clone(),
                deployment_ctx.region.clone(),
            )?;
        }

        log::info!(
            "Deployment context initialized across all observability integrations: \
            node_id={:?}, node_name={:?}, region={:?}, version={:?}",
            deployment_ctx.node_id,
            deployment_ctx.node_name,
            deployment_ctx.region,
            deployment_ctx.version
        );

        Ok(())
    }

    /// Shutdown all observability features gracefully
    pub async fn shutdown(&mut self) -> Result<(), SqlError> {
        if let Some(mut telemetry) = self.telemetry.take() {
            telemetry.shutdown().await?;
            log::info!("🔄 Phase 4: Distributed tracing shut down");
        }

        if let Some(mut metrics) = self.metrics.take() {
            metrics.shutdown().await?;
            log::info!("🔄 Phase 4: Prometheus metrics shut down");
        }

        if let Some(mut profiling) = self.profiling.take() {
            profiling.shutdown().await?;
            log::info!("🔄 Phase 4: Performance profiling shut down");
        }

        self.initialized = false;
        log::info!("✨ Phase 4: Observability infrastructure shut down");
        Ok(())
    }
}

/// Thread-safe shared observability manager
pub type SharedObservabilityManager = Arc<RwLock<ObservabilityManager>>;

/// Create a shared observability manager with the given configuration
pub fn create_shared_manager(config: StreamingConfig) -> SharedObservabilityManager {
    Arc::new(RwLock::new(ObservabilityManager::from_streaming_config(
        config,
    )))
}

/// Initialize observability features for the given configuration
pub async fn initialize_observability(
    config: &StreamingConfig,
) -> Result<SharedObservabilityManager, SqlError> {
    let manager = create_shared_manager(config.clone());

    {
        let mut mgr = manager.write().await;
        mgr.initialize().await?;
    }

    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Configs are imported by the methods that use them

    #[tokio::test]
    async fn test_observability_manager_creation() {
        let config = StreamingConfig::default();
        let manager = ObservabilityManager::from_streaming_config(config);

        assert!(!manager.initialized);
        assert!(!manager.is_any_feature_enabled());
        assert!(manager.telemetry().is_none());
        assert!(manager.metrics().is_none());
        assert!(manager.profiling().is_none());
    }

    #[tokio::test]
    async fn test_observability_manager_with_features() {
        let config = StreamingConfig::default()
            .with_distributed_tracing()
            .with_prometheus_metrics()
            .with_performance_profiling();

        let manager = ObservabilityManager::from_streaming_config(config);
        assert!(manager.is_any_feature_enabled());
    }

    #[tokio::test]
    async fn test_shared_manager_creation() {
        let config = StreamingConfig::default();
        let shared_manager = create_shared_manager(config);

        let manager = shared_manager.read().await;
        assert!(!manager.initialized);
    }
}
