//! Factory for creating partitioning strategies based on configuration
//!
//! Provides builder-like interface for instantiating strategies with configuration

use crate::velostream::sql::error::SqlError;
use std::sync::Arc;

use super::{
    AlwaysHashStrategy, FanInStrategy, PartitioningStrategy, RoundRobinStrategy,
    SmartRepartitionStrategy, StickyPartitionStrategy, StrategyConfig,
};

/// Factory for creating strategies from configuration
pub struct StrategyFactory;

impl StrategyFactory {
    /// Create a partitioning strategy from configuration
    ///
    /// # Arguments
    /// * `config` - Strategy configuration
    ///
    /// # Returns
    /// Arc-wrapped strategy instance ready for use
    ///
    /// # Example
    /// ```ignore
    /// use velostream::velostream::server::v2::{StrategyFactory, StrategyConfig};
    /// let strategy = StrategyFactory::create(StrategyConfig::AlwaysHash).unwrap();
    /// ```
    pub fn create(config: StrategyConfig) -> Result<Arc<dyn PartitioningStrategy>, SqlError> {
        match config {
            StrategyConfig::AlwaysHash => Ok(Arc::new(AlwaysHashStrategy::new())),
            StrategyConfig::SmartRepartition => Ok(Arc::new(SmartRepartitionStrategy::new())),
            StrategyConfig::RoundRobin => Ok(Arc::new(RoundRobinStrategy::new())),
            StrategyConfig::StickyPartition => Ok(Arc::new(StickyPartitionStrategy::new())),
            StrategyConfig::FanIn => Ok(Arc::new(FanInStrategy::new())),
        }
    }

    /// Create a strategy from a string configuration
    ///
    /// # Arguments
    /// * `strategy_name` - Name of the strategy (e.g., "always_hash", "smart_repartition")
    ///
    /// # Returns
    /// Arc-wrapped strategy instance
    ///
    /// # Errors
    /// Returns error if strategy name is invalid
    pub fn create_from_str(strategy_name: &str) -> Result<Arc<dyn PartitioningStrategy>, SqlError> {
        let config: StrategyConfig = strategy_name.parse()?;
        Self::create(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_create_always_hash() {
        let strategy = StrategyFactory::create(StrategyConfig::AlwaysHash);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "AlwaysHash");
    }

    #[test]
    fn test_factory_create_from_str_always_hash() {
        let strategy = StrategyFactory::create_from_str("always_hash");
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_factory_create_from_str_case_insensitive() {
        let strategy = StrategyFactory::create_from_str("AlwaysHash");
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_factory_create_from_str_invalid() {
        let strategy = StrategyFactory::create_from_str("invalid_strategy");
        assert!(strategy.is_err());
    }

    #[test]
    fn test_factory_create_smart_repartition() {
        let strategy = StrategyFactory::create(StrategyConfig::SmartRepartition);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "SmartRepartition");
    }

    #[test]
    fn test_factory_create_round_robin() {
        let strategy = StrategyFactory::create(StrategyConfig::RoundRobin);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "RoundRobin");
    }

    #[test]
    fn test_factory_create_sticky_partition() {
        let strategy = StrategyFactory::create(StrategyConfig::StickyPartition);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "StickyPartition");
    }

    #[test]
    fn test_factory_create_fan_in() {
        let strategy = StrategyFactory::create(StrategyConfig::FanIn);
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "FanIn");
    }

    #[test]
    fn test_factory_create_from_str_fan_in() {
        let strategy = StrategyFactory::create_from_str("fan_in");
        assert!(strategy.is_ok());
        let strategy = strategy.unwrap();
        assert_eq!(strategy.name(), "FanIn");
    }
}
