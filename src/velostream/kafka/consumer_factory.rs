//! Consumer Factory for Tier-Based Consumer Creation
//!
//! Provides a unified factory interface for creating Kafka consumers based on
//! configured performance tiers. This enables transparent switching between
//! different consumer implementations optimized for various throughput/latency
//! requirements.
//!
//! # Performance Tiers
//!
//! - **None or Standard**: Direct polling with BaseConsumer (~10K-15K msg/sec, default)
//! - **Buffered**: Batched polling for higher throughput (~50K-75K msg/sec)
//! - **Dedicated**: Dedicated thread for maximum throughput (~100K-150K msg/sec)
//!
//! # Usage
//!
//! ```rust,no_run
//! use velostream::velostream::kafka::{
//!     consumer_factory::ConsumerFactory,
//!     consumer_config::{ConsumerConfig, ConsumerTier},
//!     serialization::JsonSerializer,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create consumer with Standard tier
//! let config = ConsumerConfig::new("localhost:9092", "my-group")
//!     .performance_tier(ConsumerTier::Standard);
//!
//! let consumer = ConsumerFactory::create::<String, String, _, _>(
//!     config,
//!     JsonSerializer,
//!     JsonSerializer,
//! )?;
//!
//! // Use via unified trait interface
//! consumer.subscribe(&["my-topic"])?;
//! let mut stream = consumer.stream();
//! # Ok(())
//! # }
//! ```
//!
//! # Phase 2B Week 2 - Sub-Phase 2B.5
//!
//! This factory is part of Phase 2B's Kafka I/O optimization effort.
//! Initial implementation supports Standard tier, with Buffered and Dedicated
//! tiers to be added in Sub-Phase 2B.6.

use crate::velostream::kafka::{
    consumer_adapters::{BufferedAdapter, DedicatedAdapter, StandardAdapter},
    consumer_config::{ConsumerConfig, ConsumerTier},
    kafka_fast_consumer::Consumer as FastConsumer,
    unified_consumer::KafkaStreamConsumer,
    serialization::Serde,
};
use rdkafka::error::KafkaError;
use std::sync::Arc;

/// Factory for creating tier-based Kafka consumers.
///
/// Provides a unified interface for creating consumers optimized for different
/// performance requirements. The factory selects the appropriate consumer
/// implementation based on the configured `performance_tier`.
pub struct ConsumerFactory;

impl ConsumerFactory {
    /// Creates a Kafka consumer based on the configured performance tier.
    ///
    /// # Type Parameters
    ///
    /// - `K`: Key type
    /// - `V`: Value type
    /// - `KS`: Key serializer implementing `Serde<K>`
    /// - `VS`: Value serializer implementing `Serde<V>`
    ///
    /// # Arguments
    ///
    /// - `config`: Consumer configuration with optional performance tier
    /// - `key_serializer`: Serializer for message keys
    /// - `value_serializer`: Serializer for message values
    ///
    /// # Returns
    ///
    /// A boxed trait object implementing `KafkaStreamConsumer<K, V>`.
    ///
    /// # Errors
    ///
    /// Returns `KafkaError` if consumer creation fails.
    ///
    /// # Tier Selection
    ///
    /// - **`None` or `Standard`**: Direct polling with BaseConsumer (default)
    /// - **`Buffered`**: Batched polling adapter
    /// - **`Dedicated`**: Dedicated thread adapter
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use velostream::velostream::kafka::{
    ///     consumer_factory::ConsumerFactory,
    ///     consumer_config::{ConsumerConfig, ConsumerTier},
    ///     serialization::JsonSerializer,
    /// };
    ///
    /// # fn example() -> Result<(), rdkafka::error::KafkaError> {
    /// let config = ConsumerConfig::new("localhost:9092", "my-group")
    ///     .performance_tier(ConsumerTier::Standard);
    ///
    /// let consumer = ConsumerFactory::create::<String, String, _, _>(
    ///     config,
    ///     JsonSerializer,
    ///     JsonSerializer,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create<K, V, KS, VS>(
        config: ConsumerConfig,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Box<dyn KafkaStreamConsumer<K, V>>, KafkaError>
    where
        K: Send + Sync + Unpin + 'static,
        V: Send + Sync + Unpin + 'static,
        KS: Serde<K> + Send + Sync + 'static,
        VS: Serde<V> + Send + Sync + 'static,
    {
        match config.performance_tier {
            // Standard tier (default): Direct polling with BaseConsumer via StandardAdapter
            None | Some(ConsumerTier::Standard) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?;
                Ok(Box::new(StandardAdapter::new(consumer)))
            }

            // Buffered tier: Batched polling via BufferedAdapter
            Some(ConsumerTier::Buffered { batch_size }) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?;
                Ok(Box::new(BufferedAdapter::new(consumer, batch_size)))
            }

            // Dedicated tier: Dedicated thread via DedicatedAdapter
            Some(ConsumerTier::Dedicated) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?;
                Ok(Box::new(DedicatedAdapter::new(Arc::new(consumer))))
            }
        }
    }

    /// Creates a consumer with explicit serializer boxes.
    ///
    /// This variant accepts boxed serializers, which is useful when the
    /// concrete serializer types are not known at compile time.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use velostream::velostream::kafka::{
    ///     consumer_factory::ConsumerFactory,
    ///     consumer_config::{ConsumerConfig, ConsumerTier},
    ///     serialization::{JsonSerializer, Serde},
    /// };
    ///
    /// # fn example() -> Result<(), rdkafka::error::KafkaError> {
    /// let config = ConsumerConfig::new("localhost:9092", "my-group")
    ///     .performance_tier(ConsumerTier::Standard);
    ///
    /// let key_ser: Box<dyn Serde<String> + Send + Sync> = Box::new(JsonSerializer);
    /// let val_ser: Box<dyn Serde<String> + Send + Sync> = Box::new(JsonSerializer);
    ///
    /// let consumer = ConsumerFactory::create_boxed::<String, String>(
    ///     config,
    ///     key_ser,
    ///     val_ser,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_boxed<K, V>(
        config: ConsumerConfig,
        key_serializer: Box<dyn Serde<K> + Send + Sync>,
        value_serializer: Box<dyn Serde<V> + Send + Sync>,
    ) -> Result<Box<dyn KafkaStreamConsumer<K, V>>, KafkaError>
    where
        K: Send + Sync + Unpin + 'static,
        V: Send + Sync + Unpin + 'static,
    {
        match config.performance_tier {
            // Standard tier (default): Direct polling with BaseConsumer
            None | Some(ConsumerTier::Standard) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    key_serializer,
                    value_serializer,
                )?;
                Ok(Box::new(StandardAdapter::new(consumer)))
            }

            // Buffered tier: Batched polling adapter
            Some(ConsumerTier::Buffered { batch_size }) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    key_serializer,
                    value_serializer,
                )?;
                Ok(Box::new(BufferedAdapter::new(consumer, batch_size)))
            }

            // Dedicated tier: Dedicated thread adapter
            Some(ConsumerTier::Dedicated) => {
                let consumer = FastConsumer::<K, V>::with_config(
                    config,
                    key_serializer,
                    value_serializer,
                )?;
                Ok(Box::new(DedicatedAdapter::new(Arc::new(consumer))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::kafka::serialization::JsonSerializer;

    #[test]
    fn test_factory_tier_selection() {
        // Test that each tier is correctly identified
        let config = ConsumerConfig::new("localhost:9092", "test-group");
        assert!(config.performance_tier.is_none(), "Default should be None");

        let config = config.performance_tier(ConsumerTier::Standard);
        assert_eq!(
            config.performance_tier,
            Some(ConsumerTier::Standard),
            "Should be Standard tier"
        );

        let config = ConsumerConfig::new("localhost:9092", "test-group")
            .performance_tier(ConsumerTier::Buffered { batch_size: 32 });
        assert_eq!(
            config.performance_tier,
            Some(ConsumerTier::Buffered { batch_size: 32 }),
            "Should be Buffered tier with batch_size=32"
        );

        let config = ConsumerConfig::new("localhost:9092", "test-group")
            .performance_tier(ConsumerTier::Dedicated);
        assert_eq!(
            config.performance_tier,
            Some(ConsumerTier::Dedicated),
            "Should be Dedicated tier"
        );
    }

    #[test]
    fn test_factory_create_signature() {
        // This test just verifies the factory signature compiles
        // Actual consumer creation requires real Kafka connection
        let config = ConsumerConfig::new("localhost:9092", "test-group")
            .performance_tier(ConsumerTier::Standard);

        // Test that create() signature accepts correct types
        let _: Result<Box<dyn KafkaStreamConsumer<String, String>>, KafkaError> =
            ConsumerFactory::create(config, JsonSerializer, JsonSerializer);
    }

    #[test]
    fn test_factory_create_boxed_signature() {
        // Test that create_boxed() signature compiles
        let config = ConsumerConfig::new("localhost:9092", "test-group")
            .performance_tier(ConsumerTier::Standard);

        let key_ser: Box<dyn Serde<String> + Send + Sync> = Box::new(JsonSerializer);
        let val_ser: Box<dyn Serde<String> + Send + Sync> = Box::new(JsonSerializer);

        let _: Result<Box<dyn KafkaStreamConsumer<String, String>>, KafkaError> =
            ConsumerFactory::create_boxed(config, key_ser, val_ser);
    }
}
