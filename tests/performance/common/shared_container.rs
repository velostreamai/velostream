//! Shared container infrastructure for performance tests
//!
//! This module provides a singleton Kafka/Redpanda container that can be shared
//! across multiple tests within the same test binary. This dramatically reduces
//! test suite runtime by avoiding container startup/shutdown for each test.
//!
//! ## Design
//!
//! - Uses `OnceCell` for lazy initialization of a global container
//! - Container starts on first use and remains running until process exit
//! - Tests use unique topic prefixes for isolation
//! - Supports both Confluent Kafka and Redpanda (Kafka-compatible, faster startup)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::performance::common::shared_container::get_shared_kafka;
//!
//! #[tokio::test]
//! async fn my_test() {
//!     let kafka = get_shared_kafka().await;
//!     let topic = kafka.create_unique_topic("my_test").await?;
//!     // Use kafka.bootstrap_servers() for producer/consumer config
//! }
//! ```
//!
//! ## Container Selection
//!
//! Set the `VELOSTREAM_TEST_CONTAINER` environment variable:
//! - `kafka` (default) - Confluent Kafka container
//! - `redpanda` - Redpanda container (faster startup, ~3s vs ~10s)
//! - `external` - Use external Kafka at `VELOSTREAM_KAFKA_BROKERS`

use once_cell::sync::OnceCell;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::kafka::KAFKA_PORT;
use testcontainers_redpanda_rs::{Redpanda, REDPANDA_PORT};
use tokio::sync::Mutex;

/// Global shared container instance
static SHARED_CONTAINER: OnceCell<Arc<SharedKafkaContainer>> = OnceCell::new();

/// Counter for generating unique topic names
static TOPIC_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Type of container backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerType {
    /// Confluent Kafka (testcontainers-modules)
    Kafka,
    /// Redpanda (faster startup, Kafka-compatible)
    Redpanda,
    /// External Kafka instance (no container)
    External,
}

impl ContainerType {
    /// Get container type from environment variable
    pub fn from_env() -> Self {
        match std::env::var("VELOSTREAM_TEST_CONTAINER")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "redpanda" => ContainerType::Redpanda,
            "external" => ContainerType::External,
            _ => ContainerType::Kafka, // Default to Kafka
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ContainerType::Kafka => "Confluent Kafka",
            ContainerType::Redpanda => "Redpanda",
            ContainerType::External => "External Kafka",
        }
    }
}

/// Container instance wrapper (holds actual container reference)
enum ContainerInstance {
    Kafka(ContainerAsync<testcontainers_modules::kafka::Kafka>),
    Redpanda(ContainerAsync<Redpanda>),
    External,
}

/// Shared Kafka container that can be used across tests
pub struct SharedKafkaContainer {
    container_type: ContainerType,
    bootstrap_servers: String,
    admin_client: AdminClient<DefaultClientContext>,
    _container: Mutex<Option<ContainerInstance>>,
    run_id: String,
}

impl SharedKafkaContainer {
    /// Create a new shared container
    async fn new(container_type: ContainerType) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (bootstrap_servers, container) = match container_type {
            ContainerType::Kafka => {
                println!("[SharedContainer] Starting Confluent Kafka container...");
                let container = testcontainers_modules::kafka::Kafka::default()
                    .start()
                    .await?;
                let port = container.get_host_port_ipv4(KAFKA_PORT).await?;
                let bootstrap = format!("127.0.0.1:{}", port);
                println!(
                    "[SharedContainer] Kafka container started at {}",
                    bootstrap
                );
                (bootstrap, Some(ContainerInstance::Kafka(container)))
            }
            ContainerType::Redpanda => {
                println!("[SharedContainer] Starting Redpanda container...");
                let container = Redpanda::default().start().await?;
                let port = container.get_host_port_ipv4(REDPANDA_PORT).await?;
                let bootstrap = format!("127.0.0.1:{}", port);
                println!(
                    "[SharedContainer] Redpanda container started at {}",
                    bootstrap
                );
                (bootstrap, Some(ContainerInstance::Redpanda(container)))
            }
            ContainerType::External => {
                let bootstrap = std::env::var("VELOSTREAM_KAFKA_BROKERS")
                    .unwrap_or_else(|_| "localhost:9092".to_string());
                println!(
                    "[SharedContainer] Using external Kafka at {}",
                    bootstrap
                );
                (bootstrap, Some(ContainerInstance::External))
            }
        };

        // Wait for broker to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Create admin client
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()?;

        // Generate unique run ID
        let run_id = format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
                % 0xFFFFFF
        );

        Ok(Self {
            container_type,
            bootstrap_servers,
            admin_client,
            _container: Mutex::new(container),
            run_id,
        })
    }

    /// Get bootstrap servers address
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Get container type
    pub fn container_type(&self) -> ContainerType {
        self.container_type
    }

    /// Get unique run ID
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Create a unique topic with the given base name
    ///
    /// Returns the full topic name with run ID and counter prefix
    pub async fn create_unique_topic(
        &self,
        base_name: &str,
        partitions: i32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let counter = TOPIC_COUNTER.fetch_add(1, Ordering::SeqCst);
        let topic_name = format!("test_{}_{:04}_{}", self.run_id, counter, base_name);

        let new_topic = NewTopic::new(&topic_name, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        // Create topic (ignore if already exists)
        let _ = self.admin_client.create_topics(&[new_topic], &opts).await;

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_millis(500)).await;

        println!(
            "[SharedContainer] Created topic '{}' with {} partitions",
            topic_name, partitions
        );
        Ok(topic_name)
    }

    /// Delete a topic
    pub async fn delete_topic(
        &self,
        topic: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        let _ = self.admin_client.delete_topics(&[topic], &opts).await;
        Ok(())
    }

    /// Check if Redpanda is being used (for feature-specific behavior)
    pub fn is_redpanda(&self) -> bool {
        self.container_type == ContainerType::Redpanda
    }
}

/// Get or create the shared Kafka container
///
/// This function initializes the container on first call and returns
/// a reference to the same container on subsequent calls.
///
/// The container type is determined by the `VELOSTREAM_TEST_CONTAINER` env var.
pub async fn get_shared_kafka() -> Arc<SharedKafkaContainer> {
    // Try to get existing container
    if let Some(container) = SHARED_CONTAINER.get() {
        return Arc::clone(container);
    }

    // Initialize new container
    let container_type = ContainerType::from_env();
    let container = SharedKafkaContainer::new(container_type)
        .await
        .expect("Failed to start shared Kafka container");
    let container = Arc::new(container);

    // Store in global (ignore if another thread beat us)
    let _ = SHARED_CONTAINER.set(Arc::clone(&container));

    // Return the stored instance (may be different if there was a race)
    Arc::clone(SHARED_CONTAINER.get().unwrap())
}

/// Get shared container with a specific type override
///
/// Useful for tests that need a specific container type regardless of env var.
/// Note: The first call wins - subsequent calls with different types will
/// still return the originally created container.
pub async fn get_shared_kafka_with_type(
    container_type: ContainerType,
) -> Arc<SharedKafkaContainer> {
    // Try to get existing container
    if let Some(container) = SHARED_CONTAINER.get() {
        if container.container_type() != container_type {
            eprintln!(
                "[SharedContainer] Warning: Requested {} but {} is already running",
                container_type.name(),
                container.container_type().name()
            );
        }
        return Arc::clone(container);
    }

    // Initialize new container with specified type
    let container = SharedKafkaContainer::new(container_type)
        .await
        .expect("Failed to start shared Kafka container");
    let container = Arc::new(container);

    // Store in global (ignore if another thread beat us)
    let _ = SHARED_CONTAINER.set(Arc::clone(&container));

    Arc::clone(SHARED_CONTAINER.get().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_type_from_env() {
        // SAFETY: These env var operations are safe in single-threaded test contexts
        // Default should be Kafka
        unsafe { std::env::remove_var("VELOSTREAM_TEST_CONTAINER") };
        assert_eq!(ContainerType::from_env(), ContainerType::Kafka);

        // Test each variant
        unsafe { std::env::set_var("VELOSTREAM_TEST_CONTAINER", "redpanda") };
        assert_eq!(ContainerType::from_env(), ContainerType::Redpanda);

        unsafe { std::env::set_var("VELOSTREAM_TEST_CONTAINER", "external") };
        assert_eq!(ContainerType::from_env(), ContainerType::External);

        unsafe { std::env::set_var("VELOSTREAM_TEST_CONTAINER", "kafka") };
        assert_eq!(ContainerType::from_env(), ContainerType::Kafka);

        // Clean up
        unsafe { std::env::remove_var("VELOSTREAM_TEST_CONTAINER") };
    }

    #[test]
    fn test_topic_counter_increments() {
        let c1 = TOPIC_COUNTER.fetch_add(1, Ordering::SeqCst);
        let c2 = TOPIC_COUNTER.fetch_add(1, Ordering::SeqCst);
        assert!(c2 > c1);
    }
}
