//! ProcessorRegistry - Runtime processor switching and gradual migration strategies
//!
//! This module provides:
//! - Dynamic processor selection based on runtime configuration
//! - Gradual migration strategies (A/B testing, canary deployments)
//! - Unified metrics aggregation across all processor instances
//! - Processor health monitoring and fallback mechanisms

use super::{JobProcessor, JobProcessorConfig, ProcessorMetrics};
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Migration strategy for processor switching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStrategy {
    /// All traffic goes to primary processor
    /// Failover to secondary if primary fails
    PrimaryWithFallback,
    /// Split traffic between primary and secondary
    /// Primary gets `primary_percentage` % of traffic
    CanaryDeployment { primary_percentage: u8 },
    /// Equal split between primary and secondary
    /// Useful for A/B testing
    ABTest,
    /// All traffic to secondary processor
    /// Gradual transition from primary
    FullMigration,
}

impl MigrationStrategy {
    pub fn as_str(&self) -> &str {
        match self {
            MigrationStrategy::PrimaryWithFallback => "primary_with_fallback",
            MigrationStrategy::CanaryDeployment { .. } => "canary_deployment",
            MigrationStrategy::ABTest => "ab_test",
            MigrationStrategy::FullMigration => "full_migration",
        }
    }
}

impl std::fmt::Display for MigrationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationStrategy::CanaryDeployment { primary_percentage } => {
                write!(f, "canary_deployment ({}% primary)", primary_percentage)
            }
            _ => write!(f, "{}", self.as_str()),
        }
    }
}

/// Statistics for processor routing decisions
#[derive(Debug, Clone)]
pub struct RoutingStats {
    /// Total requests routed to primary
    pub primary_routed: u64,
    /// Total requests routed to secondary
    pub secondary_routed: u64,
    /// Primary processor success rate (0-100%)
    pub primary_success_rate: f64,
    /// Secondary processor success rate (0-100%)
    pub secondary_success_rate: f64,
}

impl RoutingStats {
    pub fn new() -> Self {
        Self {
            primary_routed: 0,
            secondary_routed: 0,
            primary_success_rate: 100.0,
            secondary_success_rate: 100.0,
        }
    }

    /// Get effective traffic split percentage
    pub fn primary_traffic_percentage(&self) -> f64 {
        let total = self.primary_routed + self.secondary_routed;
        if total == 0 {
            0.0
        } else {
            (self.primary_routed as f64 / total as f64) * 100.0
        }
    }
}

impl Default for RoutingStats {
    fn default() -> Self {
        Self::new()
    }
}

/// ProcessorRegistry - Manages processor switching and migration strategies
pub struct ProcessorRegistry {
    /// Primary processor (always available)
    primary: Arc<dyn JobProcessor>,
    /// Secondary processor (optional, for migration strategies)
    secondary: Option<Arc<dyn JobProcessor>>,
    /// Active migration strategy
    strategy: MigrationStrategy,
    /// Routing statistics
    stats: Arc<RwLock<RoutingStats>>,
}

impl ProcessorRegistry {
    /// Create a registry with a primary processor
    pub fn new(primary: Arc<dyn JobProcessor>) -> Self {
        Self {
            primary,
            secondary: None,
            strategy: MigrationStrategy::PrimaryWithFallback,
            stats: Arc::new(RwLock::new(RoutingStats::new())),
        }
    }

    /// Add secondary processor for migration strategies
    pub fn with_secondary(mut self, secondary: Arc<dyn JobProcessor>) -> Self {
        self.secondary = Some(secondary);
        self
    }

    /// Set migration strategy
    pub fn with_strategy(mut self, strategy: MigrationStrategy) -> Self {
        self.strategy = strategy;
        info!("ProcessorRegistry: migration strategy set to {}", strategy);
        self
    }

    /// Get the primary processor
    pub fn primary(&self) -> Arc<dyn JobProcessor> {
        Arc::clone(&self.primary)
    }

    /// Get the secondary processor (if available)
    pub fn secondary(&self) -> Option<Arc<dyn JobProcessor>> {
        self.secondary.as_ref().map(Arc::clone)
    }

    /// Get current migration strategy
    pub fn strategy(&self) -> MigrationStrategy {
        self.strategy
    }

    /// Select processor based on current migration strategy
    pub fn select_processor(&self, request_id: u64) -> Arc<dyn JobProcessor> {
        match self.strategy {
            MigrationStrategy::PrimaryWithFallback => {
                debug!("ProcessorRegistry: routing via primary with fallback");
                Arc::clone(&self.primary)
            }
            MigrationStrategy::CanaryDeployment { primary_percentage } => {
                // Use request_id to determine routing (deterministic)
                let hash = request_id.wrapping_mul(2654435761);
                let percentage = (hash % 100) as u8;

                if percentage < primary_percentage {
                    Arc::clone(&self.primary)
                } else {
                    match &self.secondary {
                        Some(secondary) => Arc::clone(secondary),
                        None => {
                            warn!("ProcessorRegistry: secondary not available, using primary");
                            Arc::clone(&self.primary)
                        }
                    }
                }
            }
            MigrationStrategy::ABTest => {
                // Split evenly: even request_ids → primary, odd → secondary
                if request_id.is_multiple_of(2) {
                    Arc::clone(&self.primary)
                } else {
                    match &self.secondary {
                        Some(secondary) => Arc::clone(secondary),
                        None => Arc::clone(&self.primary),
                    }
                }
            }
            MigrationStrategy::FullMigration => {
                // All traffic to secondary if available
                match &self.secondary {
                    Some(secondary) => Arc::clone(secondary),
                    None => {
                        warn!(
                            "ProcessorRegistry: full migration requested but secondary not available"
                        );
                        Arc::clone(&self.primary)
                    }
                }
            }
        }
    }

    /// Record successful routing to primary
    pub async fn record_primary_success(&self) {
        let mut stats = self.stats.write().await;
        stats.primary_routed += 1;
    }

    /// Record successful routing to secondary
    pub async fn record_secondary_success(&self) {
        let mut stats = self.stats.write().await;
        stats.secondary_routed += 1;
    }

    /// Get current routing statistics
    pub async fn routing_stats(&self) -> RoutingStats {
        self.stats.read().await.clone()
    }

    /// Get aggregated metrics from both processors
    pub async fn aggregated_metrics(&self) -> Vec<ProcessorMetrics> {
        let mut metrics = vec![self.primary.metrics()];

        if let Some(secondary) = &self.secondary {
            metrics.push(secondary.metrics());
        }

        metrics
    }

    /// Get routing summary for logging
    pub async fn routing_summary(&self) -> String {
        let stats = self.routing_stats().await;
        let primary_metrics = self.primary.metrics();
        let secondary_metrics = self.secondary.as_ref().map(|p| p.metrics());

        let mut summary = format!(
            "Processors: Primary='{}' ({}), Strategy='{}', Primary traffic: {:.1}%\n  Primary: {} records",
            primary_metrics.name,
            primary_metrics.version,
            self.strategy,
            stats.primary_traffic_percentage(),
            stats.primary_routed
        );

        if let Some(secondary_metrics) = secondary_metrics {
            summary.push_str(&format!(
                "\n  Secondary: '{}' ({}), {} records",
                secondary_metrics.name, secondary_metrics.version, stats.secondary_routed
            ));
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_strategy_display() {
        assert_eq!(
            MigrationStrategy::PrimaryWithFallback.as_str(),
            "primary_with_fallback"
        );
        assert_eq!(MigrationStrategy::ABTest.as_str(), "ab_test");
        assert_eq!(MigrationStrategy::FullMigration.as_str(), "full_migration");
    }

    #[test]
    fn test_migration_strategy_canary_display() {
        let strategy = MigrationStrategy::CanaryDeployment {
            primary_percentage: 80,
        };
        assert_eq!(strategy.to_string(), "canary_deployment (80% primary)");
    }

    #[test]
    fn test_routing_stats_new() {
        let stats = RoutingStats::new();
        assert_eq!(stats.primary_routed, 0);
        assert_eq!(stats.secondary_routed, 0);
        assert_eq!(stats.primary_success_rate, 100.0);
        assert_eq!(stats.secondary_success_rate, 100.0);
    }

    #[test]
    fn test_routing_stats_traffic_percentage() {
        let mut stats = RoutingStats::new();
        stats.primary_routed = 80;
        stats.secondary_routed = 20;

        assert_eq!(stats.primary_traffic_percentage(), 80.0);

        stats.primary_routed = 0;
        stats.secondary_routed = 0;
        assert_eq!(stats.primary_traffic_percentage(), 0.0);
    }

    #[test]
    fn test_routing_stats_default() {
        let stats = RoutingStats::default();
        assert_eq!(stats.primary_routed, 0);
        assert_eq!(stats.secondary_routed, 0);
    }
}
