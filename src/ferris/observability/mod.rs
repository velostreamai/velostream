// === PHASE 4: OBSERVABILITY MODULE ===
//
// This module provides comprehensive observability features for FerrisStreams:
// - OpenTelemetry distributed tracing
// - Prometheus metrics collection and export
// - Performance profiling integration
// - Automatic instrumentation for SQL query execution
//
// All features are configurable and disabled by default for backward compatibility.

pub mod metrics;
pub mod profiling;
pub mod telemetry;

use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::config::StreamingConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Central observability manager for FerrisStreams Phase 4
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
    pub fn new(config: StreamingConfig) -> Self {
        Self {
            config,
            telemetry: None,
            metrics: None,
            profiling: None,
            initialized: false,
        }
    }

    /// Initialize all enabled observability features
    pub async fn initialize(&mut self) -> Result<(), SqlError> {
        if self.initialized {
            return Ok(());
        }

        // Initialize distributed tracing if enabled
        if self.config.enable_distributed_tracing {
            if let Some(ref tracing_config) = self.config.tracing_config {
                let telemetry = telemetry::TelemetryProvider::new(tracing_config.clone()).await?;
                self.telemetry = Some(telemetry);
                log::info!("✅ Phase 4: Distributed tracing initialized");
            }
        }

        // Initialize Prometheus metrics if enabled
        if self.config.enable_prometheus_metrics {
            if let Some(ref prometheus_config) = self.config.prometheus_config {
                let metrics = metrics::MetricsProvider::new(prometheus_config.clone()).await?;
                self.metrics = Some(metrics);
                log::info!(
                    "✅ Phase 4: Prometheus metrics initialized on port {}",
                    prometheus_config.port
                );
            }
        }

        // Initialize performance profiling if enabled
        if self.config.enable_performance_profiling {
            if let Some(ref profiling_config) = self.config.profiling_config {
                let profiling = profiling::ProfilingProvider::new(profiling_config.clone()).await?;
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
    Arc::new(RwLock::new(ObservabilityManager::new(config)))
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
        let manager = ObservabilityManager::new(config);

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

        let manager = ObservabilityManager::new(config);
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
