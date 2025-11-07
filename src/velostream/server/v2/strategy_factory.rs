//! Factory for creating partitioning strategies based on configuration
//!
//! Provides builder-like interface for instantiating strategies with configuration

use crate::velostream::sql::error::SqlError;
use std::sync::Arc;

use super::{AlwaysHashStrategy, PartitioningStrategy, StrategyConfig};

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
            StrategyConfig::SmartRepartition => Err(SqlError::ConfigurationError {
                message: "SmartRepartitionStrategy - coming in Phase 2".to_string(),
            }),
            StrategyConfig::RoundRobin => Err(SqlError::ConfigurationError {
                message: "RoundRobinStrategy - coming in Phase 3".to_string(),
            }),
            StrategyConfig::StickyPartition => Err(SqlError::ConfigurationError {
                message: "StickyPartitionStrategy - coming in Phase 3".to_string(),
            }),
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
    fn test_factory_unimplemented_strategies() {
        assert!(StrategyFactory::create(StrategyConfig::SmartRepartition).is_err());
        assert!(StrategyFactory::create(StrategyConfig::RoundRobin).is_err());
        assert!(StrategyFactory::create(StrategyConfig::StickyPartition).is_err());
    }
}
