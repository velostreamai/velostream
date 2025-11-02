//! Unified Kafka Consumer Trait
//!
//! This module provides a unified interface for Kafka consumers, abstracting over different
//! implementations (StreamConsumer-based and BaseConsumer-based) to enable flexible
//! performance tier selection without changing application code.
//!
//! # Design Goals
//!
//! 1. **Abstraction**: Common interface for both `KafkaConsumer` (StreamConsumer) and fast consumers (BaseConsumer)
//! 2. **Backward Compatibility**: Existing code using `KafkaConsumer` continues to work unchanged
//! 3. **Performance Tiers**: Enable opt-in migration to higher-performance tiers
//! 4. **Type Safety**: Preserve generic type parameters for keys and values
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │   KafkaStreamConsumer<K, V>        │  ← Unified trait
//! │   - stream()                        │
//! │   - subscribe()                     │
//! │   - commit()                        │
//! └─────────────────────────────────────┘
//!            ▲                  ▲
//!            │                  │
//!   ┌────────┴────────┐    ┌───┴──────────────┐
//!   │ KafkaConsumer   │    │ Consumer (fast)  │
//!   │ (StreamConsumer)│    │ (BaseConsumer)   │
//!   └─────────────────┘    └──────────────────┘
//! ```
//!
//! # Usage
//!
//! ## Direct Use (Legacy)
//! ```rust,ignore
//! let consumer = KafkaConsumer::new(brokers, group_id, key_ser, value_ser)?;
//! consumer.subscribe(&["my-topic"])?;
//! let mut stream = consumer.stream();
//! ```
//!
//! ## Trait-Based Use (New)
//! ```rust,ignore
//! let consumer: Box<dyn KafkaStreamConsumer<String, MyValue>> =
//!     ConsumerFactory::create(config, key_ser, value_ser)?;
//! consumer.subscribe(&["my-topic"])?;
//! let mut stream = consumer.stream();
//! ```

use crate::velostream::kafka::kafka_error::ConsumerError;
use crate::velostream::kafka::message::Message;
use futures::Stream;
use rdkafka::error::KafkaError;
use rdkafka::TopicPartitionList;
use std::pin::Pin;

/// Unified Kafka consumer interface supporting both StreamConsumer and BaseConsumer implementations.
///
/// This trait provides a common interface for consuming Kafka messages, allowing applications
/// to switch between different consumer implementations (performance tiers) without code changes.
///
/// # Type Parameters
///
/// * `K` - Key type (must be deserializable by the key serializer)
/// * `V` - Value type (must be deserializable by the value serializer)
///
/// # Core Methods
///
/// - `stream()`: Returns an async stream of deserialized messages
/// - `subscribe()`: Subscribe to topics
/// - `commit()`: Manually commit offsets (for manual commit mode)
///
/// # Implementation Notes
///
/// - Implementations must be `Send + Sync` for use in async contexts
/// - The stream lifetime is tied to the consumer (`'_`)
/// - Error types are unified via `ConsumerError` and `KafkaError`
pub trait KafkaStreamConsumer<K, V>: Send + Sync {
    /// Returns a stream of deserialized Kafka messages.
    ///
    /// The stream yields `Result<Message<K, V>, ConsumerError>` items, where:
    /// - `Ok(message)` contains the deserialized key, value, headers, and metadata
    /// - `Err(error)` indicates deserialization or Kafka errors
    ///
    /// # Stream Semantics
    ///
    /// - The stream is lazy (polling begins when stream is polled)
    /// - The stream continues indefinitely until consumer is dropped
    /// - Polling behavior depends on implementation (direct poll vs async machinery)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use futures::StreamExt;
    ///
    /// let consumer: Box<dyn KafkaStreamConsumer<String, MyValue>> =
    ///     create_consumer(...);
    ///
    /// consumer.subscribe(&["my-topic"])?;
    ///
    /// let mut stream = consumer.stream();
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(message) => {
    ///             println!("Key: {:?}", message.key());
    ///             println!("Value: {:?}", message.value());
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    fn stream(&self) -> Pin<Box<dyn Stream<Item = Result<Message<K, V>, ConsumerError>> + Send + '_>>;

    /// Subscribes to the specified Kafka topics.
    ///
    /// # Arguments
    ///
    /// * `topics` - Slice of topic names to subscribe to
    ///
    /// # Returns
    ///
    /// - `Ok(())` if subscription succeeded
    /// - `Err(KafkaError)` if subscription failed (e.g., invalid topic, broker connection issues)
    ///
    /// # Behavior
    ///
    /// - Replaces any existing subscription (does not add to existing topics)
    /// - Consumer group rebalancing occurs after subscription
    /// - Partitions are assigned based on group protocol (range, roundrobin, etc.)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Subscribe to single topic
    /// consumer.subscribe(&["events"])?;
    ///
    /// // Subscribe to multiple topics
    /// consumer.subscribe(&["events", "logs", "metrics"])?;
    /// ```
    fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError>;

    /// Manually commits the current consumer offset.
    ///
    /// Only relevant when `enable.auto.commit=false` in consumer configuration.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if commit succeeded
    /// - `Err(KafkaError)` if commit failed
    ///
    /// # Commit Semantics
    ///
    /// - Commits offsets for all assigned partitions
    /// - Synchronous commit (blocks until acknowledged by broker)
    /// - Default implementation returns `Ok(())` (no-op for auto-commit consumers)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut stream = consumer.stream();
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(message) => {
    ///             process_message(message);
    ///             consumer.commit()?; // Commit after processing
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    fn commit(&self) -> Result<(), KafkaError> {
        Ok(()) // Default: no-op (for auto-commit mode)
    }

    /// Returns the current offset positions for assigned partitions (optional).
    ///
    /// Default implementation returns `None` (not supported by all implementations).
    ///
    /// # Returns
    ///
    /// - `Ok(Some(partition_list))` with current offsets if supported
    /// - `Ok(None)` if not supported by this implementation
    /// - `Err(KafkaError)` if query failed
    ///
    /// # Use Cases
    ///
    /// - Checkpoint tracking for crash recovery
    /// - Progress monitoring
    /// - Offset debugging
    fn current_offsets(&self) -> Result<Option<TopicPartitionList>, KafkaError> {
        Ok(None) // Default: not supported
    }

    /// Returns the current partition assignment (optional).
    ///
    /// Default implementation returns `None` (not supported by all implementations).
    ///
    /// # Returns
    ///
    /// - `Ok(Some(partition_list))` with assigned partitions if supported
    /// - `Ok(None)` if not supported by this implementation
    /// - `Err(KafkaError)` if query failed
    ///
    /// # Use Cases
    ///
    /// - Partition ownership verification
    /// - Rebalancing monitoring
    /// - Load distribution analysis
    fn assignment(&self) -> Result<Option<TopicPartitionList>, KafkaError> {
        Ok(None) // Default: not supported
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::kafka::headers::Headers;
    use futures::stream;
    use std::pin::Pin;

    // Mock implementation for testing
    struct MockConsumer;

    impl KafkaStreamConsumer<String, String> for MockConsumer {
        fn stream(
            &self,
        ) -> Pin<Box<dyn Stream<Item = Result<Message<String, String>, ConsumerError>> + Send + '_>> {
            // Return empty stream for testing
            Box::pin(stream::empty())
        }

        fn subscribe(&self, _topics: &[&str]) -> Result<(), KafkaError> {
            Ok(())
        }

        fn commit(&self) -> Result<(), KafkaError> {
            Ok(())
        }
    }

    #[test]
    fn test_trait_compiles() {
        let consumer = MockConsumer;
        let _ = consumer.subscribe(&["test-topic"]);
        let _ = consumer.commit();
        let _ = consumer.current_offsets();
        let _ = consumer.assignment();
    }

    #[test]
    fn test_default_implementations() {
        let consumer = MockConsumer;

        // Default current_offsets returns None
        assert!(consumer.current_offsets().unwrap().is_none());

        // Default assignment returns None
        assert!(consumer.assignment().unwrap().is_none());

        // Default commit succeeds (no-op)
        assert!(consumer.commit().is_ok());
    }
}
