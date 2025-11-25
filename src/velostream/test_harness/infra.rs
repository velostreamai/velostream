//! Testcontainers infrastructure management
//!
//! Manages the lifecycle of test infrastructure including:
//! - Kafka container startup/shutdown (requires Docker)
//! - Dynamic port retrieval
//! - Topic creation and cleanup
//! - Temp directory management
//!
//! Note: The testcontainers functionality is only available in tests
//! and requires Docker to be running.

use super::error::{TestHarnessError, TestHarnessResult};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::DefaultConsumerContext;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Manages testcontainers infrastructure for test execution
///
/// This struct manages the lifecycle of test infrastructure. When used with
/// testcontainers (in integration tests), it will spin up a real Kafka
/// container. For unit tests or when Docker is not available, it can work
/// with an external Kafka instance.
pub struct TestHarnessInfra {
    /// Unique identifier for this test run
    run_id: String,

    /// Bootstrap servers address
    bootstrap_servers: Option<String>,

    /// Temporary directory for file sinks
    temp_dir: Option<PathBuf>,

    /// Created topics that need cleanup
    created_topics: Vec<String>,

    /// Whether infrastructure is running
    is_running: bool,

    /// Admin client for topic management
    admin_client: Option<AdminClient<DefaultClientContext>>,

    /// Whether we own the Kafka instance (and should clean up topics)
    owns_kafka: bool,
}

impl TestHarnessInfra {
    /// Create new infrastructure manager
    pub fn new() -> Self {
        let run_id = generate_run_id();
        Self {
            run_id,
            bootstrap_servers: None,
            temp_dir: None,
            created_topics: Vec::new(),
            is_running: false,
            admin_client: None,
            owns_kafka: false,
        }
    }

    /// Create infrastructure that connects to an existing Kafka instance
    ///
    /// Use this when you have an external Kafka (e.g., from testcontainers
    /// managed at the test level, or a development Kafka instance).
    pub fn with_kafka(bootstrap_servers: &str) -> Self {
        let run_id = generate_run_id();
        Self {
            run_id,
            bootstrap_servers: Some(bootstrap_servers.to_string()),
            temp_dir: None,
            created_topics: Vec::new(),
            is_running: false,
            admin_client: None,
            owns_kafka: false,
        }
    }

    /// Get the unique run ID for this test execution
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Start the test infrastructure
    ///
    /// If bootstrap_servers is already set (via `with_kafka`), this will
    /// connect to that instance. Otherwise, it will fail with an error
    /// indicating that no Kafka instance is available.
    ///
    /// For testcontainers support, use the test helper functions that
    /// start the container and then create infrastructure with `with_kafka`.
    pub async fn start(&mut self) -> TestHarnessResult<()> {
        if self.is_running {
            return Ok(());
        }

        log::info!("Starting test infrastructure (run_id: {})", self.run_id);

        // Check if we have bootstrap servers configured
        let bootstrap_servers = self.bootstrap_servers.clone().ok_or_else(|| {
            TestHarnessError::InfraError {
                message: "No Kafka bootstrap servers configured. Use with_kafka() to provide an existing Kafka instance.".to_string(),
                source: None,
            }
        })?;

        log::info!("Connecting to Kafka: {}", bootstrap_servers);

        // Create admin client for topic management
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create admin client: {}", e),
                source: Some(e.to_string()),
            })?;

        // Create temp directory for file sinks
        let temp_dir = std::env::temp_dir().join(format!("velo_test_{}", self.run_id));
        std::fs::create_dir_all(&temp_dir).map_err(|e| TestHarnessError::IoError {
            message: format!("Failed to create temp directory: {}", e),
            path: temp_dir.display().to_string(),
        })?;
        log::info!("Created temp directory: {}", temp_dir.display());

        self.admin_client = Some(admin_client);
        self.temp_dir = Some(temp_dir);
        self.is_running = true;

        log::info!("Test infrastructure started successfully");
        Ok(())
    }

    /// Stop the test infrastructure and cleanup
    pub async fn stop(&mut self) -> TestHarnessResult<()> {
        if !self.is_running {
            return Ok(());
        }

        log::info!("Stopping test infrastructure (run_id: {})", self.run_id);

        // Delete created topics (only if we own the Kafka instance or have topics to clean)
        if let Some(ref admin_client) = self.admin_client {
            if !self.created_topics.is_empty() {
                log::debug!("Deleting {} topics...", self.created_topics.len());
                let topics: Vec<&str> = self.created_topics.iter().map(|s| s.as_str()).collect();
                let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

                if let Err(e) = admin_client.delete_topics(&topics, &options).await {
                    log::warn!("Failed to delete topics: {}", e);
                }
            }
        }

        // Remove temp directory
        if let Some(ref temp_dir) = self.temp_dir {
            if temp_dir.exists() {
                log::debug!("Removing temp directory: {}", temp_dir.display());
                if let Err(e) = std::fs::remove_dir_all(temp_dir) {
                    log::warn!("Failed to remove temp directory: {}", e);
                }
            }
        }

        self.admin_client = None;
        self.created_topics.clear();
        self.is_running = false;

        log::info!("Test infrastructure stopped");
        Ok(())
    }

    /// Get the bootstrap servers address
    pub fn bootstrap_servers(&self) -> Option<&str> {
        self.bootstrap_servers.as_deref()
    }

    /// Get the temp directory path
    pub fn temp_dir(&self) -> Option<&PathBuf> {
        self.temp_dir.as_ref()
    }

    /// Generate a test topic name with run ID prefix
    pub fn topic_name(&self, base_name: &str) -> String {
        format!("test_{}_{}", self.run_id, base_name)
    }

    /// Create a topic for testing
    pub async fn create_topic(&mut self, name: &str, partitions: i32) -> TestHarnessResult<String> {
        let topic_name = self.topic_name(name);

        let admin_client =
            self.admin_client
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Admin client not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        log::debug!(
            "Creating topic: {} (partitions: {})",
            topic_name,
            partitions
        );

        let new_topic = NewTopic::new(&topic_name, partitions, TopicReplication::Fixed(1));
        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        admin_client
            .create_topics(&[new_topic], &options)
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create topic '{}': {}", topic_name, e),
                source: Some(e.to_string()),
            })?;

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        self.created_topics.push(topic_name.clone());
        log::info!("Created topic: {}", topic_name);
        Ok(topic_name)
    }

    /// Delete a topic
    pub async fn delete_topic(&mut self, name: &str) -> TestHarnessResult<()> {
        let topic_name = self.topic_name(name);

        let admin_client =
            self.admin_client
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Admin client not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        log::debug!("Deleting topic: {}", topic_name);

        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        admin_client
            .delete_topics(&[&topic_name], &options)
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to delete topic '{}': {}", topic_name, e),
                source: Some(e.to_string()),
            })?;

        self.created_topics.retain(|t| t != &topic_name);
        log::info!("Deleted topic: {}", topic_name);
        Ok(())
    }

    /// Get configuration overrides for source/sink configs
    pub fn config_overrides(&self) -> HashMap<String, String> {
        let mut overrides = HashMap::new();

        if let Some(ref bs) = self.bootstrap_servers {
            overrides.insert("bootstrap.servers".to_string(), bs.clone());
        }

        overrides
    }

    /// Check if infrastructure is running
    pub fn is_running(&self) -> bool {
        self.is_running
    }

    /// Get a file path in the temp directory
    pub fn temp_file_path(&self, filename: &str) -> Option<PathBuf> {
        self.temp_dir.as_ref().map(|dir| dir.join(filename))
    }

    /// Create a Kafka producer for publishing test data
    pub fn create_producer(
        &self,
    ) -> TestHarnessResult<rdkafka::producer::FutureProducer<DefaultClientContext>> {
        let bootstrap_servers =
            self.bootstrap_servers
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Bootstrap servers not available. Call start() first.".to_string(),
                    source: None,
                })?;

        let producer: rdkafka::producer::FutureProducer<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create producer: {}", e),
                source: Some(e.to_string()),
            })?;

        Ok(producer)
    }

    /// Create a Kafka consumer for capturing output
    pub fn create_consumer(
        &self,
        group_id: &str,
    ) -> TestHarnessResult<rdkafka::consumer::StreamConsumer<DefaultConsumerContext>> {
        let bootstrap_servers =
            self.bootstrap_servers
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Bootstrap servers not available. Call start() first.".to_string(),
                    source: None,
                })?;

        let consumer: rdkafka::consumer::StreamConsumer<DefaultConsumerContext> =
            ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("group.id", group_id)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .map_err(|e| TestHarnessError::InfraError {
                    message: format!("Failed to create consumer: {}", e),
                    source: Some(e.to_string()),
                })?;

        Ok(consumer)
    }
}

impl Default for TestHarnessInfra {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestHarnessInfra {
    fn drop(&mut self) {
        if self.is_running {
            log::warn!("TestHarnessInfra dropped while running. Call stop() for clean shutdown.");
        }
    }
}

/// Generate a unique run ID for test isolation
fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    // Short hex string from timestamp + random component
    format!("{:x}", timestamp % 0xFFFFFF)
}

/// Shared infrastructure that can be used across multiple tests
pub struct SharedTestInfra {
    inner: Arc<RwLock<TestHarnessInfra>>,
}

impl SharedTestInfra {
    /// Create new shared infrastructure
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TestHarnessInfra::new())),
        }
    }

    /// Create shared infrastructure with existing Kafka
    pub fn with_kafka(bootstrap_servers: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TestHarnessInfra::with_kafka(bootstrap_servers))),
        }
    }

    /// Start the infrastructure
    pub async fn start(&self) -> TestHarnessResult<()> {
        let mut infra = self.inner.write().await;
        infra.start().await
    }

    /// Stop the infrastructure
    pub async fn stop(&self) -> TestHarnessResult<()> {
        let mut infra = self.inner.write().await;
        infra.stop().await
    }

    /// Get bootstrap servers
    pub async fn bootstrap_servers(&self) -> Option<String> {
        let infra = self.inner.read().await;
        infra.bootstrap_servers().map(|s| s.to_string())
    }

    /// Create a topic
    pub async fn create_topic(&self, name: &str, partitions: i32) -> TestHarnessResult<String> {
        let mut infra = self.inner.write().await;
        infra.create_topic(name, partitions).await
    }

    /// Get run ID
    pub async fn run_id(&self) -> String {
        let infra = self.inner.read().await;
        infra.run_id().to_string()
    }
}

impl Default for SharedTestInfra {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedTestInfra {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_id_generation() {
        let id1 = generate_run_id();
        let id2 = generate_run_id();
        // IDs should be reasonably short
        assert!(id1.len() <= 6);
        assert!(id2.len() <= 6);
    }

    #[test]
    fn test_topic_naming() {
        let infra = TestHarnessInfra::new();
        let topic = infra.topic_name("market_data");
        assert!(topic.starts_with("test_"));
        assert!(topic.ends_with("_market_data"));
    }

    #[test]
    fn test_config_overrides_empty_before_start() {
        let infra = TestHarnessInfra::new();
        let overrides = infra.config_overrides();
        assert!(overrides.is_empty());
    }

    #[test]
    fn test_with_kafka_has_bootstrap_servers() {
        let infra = TestHarnessInfra::with_kafka("localhost:9092");
        assert_eq!(infra.bootstrap_servers(), Some("localhost:9092"));
    }
}
