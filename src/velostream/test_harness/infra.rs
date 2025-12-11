//! Testcontainers infrastructure management
//!
//! Manages the lifecycle of test infrastructure including:
//! - Kafka container startup/shutdown (requires Docker)
//! - Dynamic port retrieval
//! - Topic creation and cleanup
//! - Temp directory management
//!
//! ## Testcontainers Integration
//!
//! This module provides integration with testcontainers for spinning up
//! Kafka containers automatically. The testcontainers feature is only
//! available in tests. For production use, provide an external Kafka
//! instance via `TestHarnessInfra::with_kafka()`.
//!
//! In tests, use `TestHarnessInfra::with_testcontainers()`:
//!
//! ```rust,ignore
//! use velostream::velostream::test_harness::TestHarnessInfra;
//!
//! #[tokio::test]
//! async fn test_with_kafka() {
//!     let mut infra = TestHarnessInfra::with_testcontainers().await.unwrap();
//!     infra.start().await.unwrap();
//!     // ... run tests
//!     infra.stop().await.unwrap();
//! }
//! ```

use super::error::{TestHarnessError, TestHarnessResult};
use crate::velostream::schema::client::registry_client::SchemaReference;
use crate::velostream::schema::server::registry_backend::{
    InMemorySchemaRegistryBackend, SchemaRegistryBackend,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::DefaultConsumerContext;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Testcontainers support - available in any test context
#[cfg(any(test, feature = "test-support"))]
use testcontainers::ContainerAsync;
#[cfg(any(test, feature = "test-support"))]
use testcontainers::runners::AsyncRunner;
#[cfg(any(test, feature = "test-support"))]
use testcontainers_modules::kafka::KAFKA_PORT;
#[cfg(any(test, feature = "test-support"))]
use testcontainers_redpanda_rs::{Redpanda, REDPANDA_PORT};

/// Type of Kafka-compatible container to use
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerType {
    /// Confluent Kafka (default, widely tested)
    #[default]
    Kafka,
    /// Redpanda (faster startup, Kafka-compatible)
    Redpanda,
}

#[cfg(any(test, feature = "test-support"))]
impl ContainerType {
    /// Get container type from environment variable
    ///
    /// Set `VELOSTREAM_TEST_CONTAINER=redpanda` to use Redpanda
    pub fn from_env() -> Self {
        match std::env::var("VELOSTREAM_TEST_CONTAINER")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "redpanda" => ContainerType::Redpanda,
            _ => ContainerType::Kafka,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ContainerType::Kafka => "Confluent Kafka",
            ContainerType::Redpanda => "Redpanda",
        }
    }
}

/// Container instance enum to hold either Kafka or Redpanda
#[cfg(any(test, feature = "test-support"))]
enum ContainerInstance {
    Kafka(ContainerAsync<testcontainers_modules::kafka::Kafka>),
    Redpanda(ContainerAsync<Redpanda>),
}

/// Manages testcontainers infrastructure for test execution
///
/// This struct manages the lifecycle of test infrastructure. When used with
/// testcontainers (in integration tests), it will spin up a real Kafka
/// container. For unit tests or when Docker is not available, it can work
/// with an external Kafka instance.
///
/// ## Schema Registry Support
///
/// The test harness includes an in-memory schema registry that is API-compatible
/// with Confluent Schema Registry. This allows testing Avro/Protobuf serialization
/// without requiring an external registry service.
///
/// ```rust,ignore
/// let mut infra = TestHarnessInfra::new();
/// infra.start().await?;
///
/// // Register a schema
/// let schema_id = infra.register_schema("market_data-value", r#"{"type":"record",...}"#).await?;
///
/// // Retrieve a schema
/// let schema = infra.get_schema(schema_id).await?;
/// ```
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

    /// In-memory schema registry (API-compatible with Confluent Schema Registry)
    schema_registry: Option<Arc<InMemorySchemaRegistryBackend>>,

    /// Container instance (Kafka or Redpanda) - only in tests
    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    container: Option<ContainerInstance>,
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
            schema_registry: None,
            #[cfg(any(test, feature = "test-support"))]
            container: None,
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
            schema_registry: None,
            #[cfg(any(test, feature = "test-support"))]
            container: None,
        }
    }

    /// Create infrastructure with a Kafka container using testcontainers
    ///
    /// This will start a Kafka container (Confluent Kafka) and configure
    /// the infrastructure to use it. Requires Docker to be running.
    ///
    /// This method is only available in tests.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut infra = TestHarnessInfra::with_testcontainers().await?;
    /// infra.start().await?;
    /// // run tests...
    /// infra.stop().await?;
    /// ```
    #[cfg(any(test, feature = "test-support"))]
    pub async fn with_testcontainers() -> TestHarnessResult<Self> {
        // Use container type from environment variable
        Self::with_testcontainers_type(ContainerType::from_env()).await
    }

    /// Create infrastructure with Redpanda container
    ///
    /// Redpanda is a Kafka-compatible streaming platform with faster startup
    /// time (~3s vs ~10s for Kafka). Use this for faster test iteration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut infra = TestHarnessInfra::with_redpanda().await?;
    /// infra.start().await?;
    /// // run tests...
    /// infra.stop().await?;
    /// ```
    #[cfg(any(test, feature = "test-support"))]
    pub async fn with_redpanda() -> TestHarnessResult<Self> {
        Self::with_testcontainers_type(ContainerType::Redpanda).await
    }

    /// Create infrastructure with a specific container type
    ///
    /// # Arguments
    /// * `container_type` - The type of container to start (Kafka or Redpanda)
    #[cfg(any(test, feature = "test-support"))]
    pub async fn with_testcontainers_type(container_type: ContainerType) -> TestHarnessResult<Self> {
        log::info!("Starting {} container via testcontainers...", container_type.name());

        let (bootstrap_servers, container) = match container_type {
            ContainerType::Kafka => {
                let kafka_container = testcontainers_modules::kafka::Kafka::default()
                    .start()
                    .await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to start Kafka container: {}", e),
                        source: Some(e.to_string()),
                    })?;

                let host_port = kafka_container
                    .get_host_port_ipv4(KAFKA_PORT)
                    .await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to get Kafka port: {}", e),
                        source: Some(e.to_string()),
                    })?;

                let bootstrap = format!("127.0.0.1:{}", host_port);
                (bootstrap, ContainerInstance::Kafka(kafka_container))
            }
            ContainerType::Redpanda => {
                let redpanda_container = Redpanda::default()
                    .start()
                    .await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to start Redpanda container: {}", e),
                        source: Some(e.to_string()),
                    })?;

                let host_port = redpanda_container
                    .get_host_port_ipv4(REDPANDA_PORT)
                    .await
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to get Redpanda port: {}", e),
                        source: Some(e.to_string()),
                    })?;

                let bootstrap = format!("127.0.0.1:{}", host_port);
                (bootstrap, ContainerInstance::Redpanda(redpanda_container))
            }
        };

        log::info!(
            "{} container started, bootstrap servers: {}",
            container_type.name(),
            bootstrap_servers
        );

        // Give the container a moment to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;

        let run_id = generate_run_id();
        Ok(Self {
            run_id,
            bootstrap_servers: Some(bootstrap_servers),
            temp_dir: None,
            created_topics: Vec::new(),
            is_running: false,
            admin_client: None,
            owns_kafka: true,
            schema_registry: None,
            container: Some(container),
        })
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

        // Initialize in-memory schema registry
        let schema_registry = InMemorySchemaRegistryBackend::new(HashMap::new());
        log::info!("Initialized in-memory schema registry");

        self.admin_client = Some(admin_client);
        self.temp_dir = Some(temp_dir);
        self.schema_registry = Some(Arc::new(schema_registry));
        self.is_running = true;

        log::info!("Test infrastructure started successfully");
        Ok(())
    }

    /// Stop the test infrastructure and cleanup
    ///
    /// This method MUST be called before the program exits to ensure
    /// proper cleanup of testcontainers. Failure to call this will
    /// leave orphaned Docker containers running.
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

        // Stop and remove the container (testcontainers)
        // This is critical - without this, containers are left running!
        #[cfg(any(test, feature = "test-support"))]
        if let Some(container) = self.container.take() {
            match container {
                ContainerInstance::Kafka(kafka_container) => {
                    log::info!("Stopping Kafka container...");
                    if let Err(e) = kafka_container.stop().await {
                        log::warn!("Failed to stop Kafka container: {}", e);
                    }
                    if let Err(e) = kafka_container.rm().await {
                        log::warn!("Failed to remove Kafka container: {}", e);
                    }
                    log::info!("Kafka container stopped and removed");
                }
                ContainerInstance::Redpanda(redpanda_container) => {
                    log::info!("Stopping Redpanda container...");
                    if let Err(e) = redpanda_container.stop().await {
                        log::warn!("Failed to stop Redpanda container: {}", e);
                    }
                    if let Err(e) = redpanda_container.rm().await {
                        log::warn!("Failed to remove Redpanda container: {}", e);
                    }
                    log::info!("Redpanda container stopped and removed");
                }
            }
        }

        self.admin_client = None;
        self.schema_registry = None;
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

    /// Create a high-throughput async producer using PolledProducer
    ///
    /// This producer uses a dedicated poll thread for handling delivery callbacks,
    /// which allows non-blocking sends and much higher throughput than FutureProducer.
    pub fn create_async_producer(
        &self,
    ) -> TestHarnessResult<crate::velostream::kafka::polled_producer::AsyncPolledProducer> {
        let bootstrap_servers =
            self.bootstrap_servers
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Bootstrap servers not available. Call start() first.".to_string(),
                    source: None,
                })?;

        crate::velostream::kafka::polled_producer::AsyncPolledProducer::with_high_throughput(
            bootstrap_servers,
        )
        .map_err(|e| TestHarnessError::InfraError {
            message: format!("Failed to create async producer: {}", e),
            source: Some(e.to_string()),
        })
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

    // =========================================================================
    // Schema Registry Methods (In-Memory, Confluent-API Compatible)
    // =========================================================================

    /// Get the schema registry backend
    ///
    /// Returns the in-memory schema registry that implements the same API
    /// as Confluent Schema Registry. Use this to register/retrieve schemas
    /// for Avro/Protobuf serialization testing.
    pub fn schema_registry(&self) -> Option<Arc<dyn SchemaRegistryBackend>> {
        self.schema_registry
            .as_ref()
            .map(|r| Arc::clone(r) as Arc<dyn SchemaRegistryBackend>)
    }

    /// Register a schema for a subject
    ///
    /// This is a convenience method that wraps the schema registry's
    /// `register_schema` method.
    ///
    /// # Arguments
    /// * `subject` - The subject name (e.g., "market_data-value" for Kafka value schemas)
    /// * `schema` - The schema definition (JSON string for Avro, proto string for Protobuf)
    ///
    /// # Returns
    /// The schema ID assigned by the registry
    ///
    /// # Example
    /// ```rust,ignore
    /// let schema_id = infra.register_schema(
    ///     "market_data-value",
    ///     r#"{"type":"record","name":"MarketData","fields":[{"name":"symbol","type":"string"}]}"#
    /// ).await?;
    /// ```
    pub async fn register_schema(&self, subject: &str, schema: &str) -> TestHarnessResult<u32> {
        let registry =
            self.schema_registry
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Schema registry not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        registry
            .register_schema(subject, schema, Vec::new())
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to register schema for '{}': {}", subject, e),
                source: Some(e.to_string()),
            })
    }

    /// Register a schema with references to other schemas
    ///
    /// Use this when your schema references other schemas (e.g., nested Avro types).
    pub async fn register_schema_with_refs(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> TestHarnessResult<u32> {
        let registry =
            self.schema_registry
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Schema registry not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        registry
            .register_schema(subject, schema, references)
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to register schema for '{}': {}", subject, e),
                source: Some(e.to_string()),
            })
    }

    /// Get a schema by its ID
    pub async fn get_schema(&self, id: u32) -> TestHarnessResult<String> {
        let registry =
            self.schema_registry
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Schema registry not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        let response = registry
            .get_schema(id)
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to get schema {}: {}", id, e),
                source: Some(e.to_string()),
            })?;

        Ok(response.schema)
    }

    /// Get the latest schema for a subject
    pub async fn get_latest_schema(&self, subject: &str) -> TestHarnessResult<String> {
        let registry =
            self.schema_registry
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Schema registry not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        let response = registry.get_latest_schema(subject).await.map_err(|e| {
            TestHarnessError::InfraError {
                message: format!("Failed to get latest schema for '{}': {}", subject, e),
                source: Some(e.to_string()),
            }
        })?;

        Ok(response.schema)
    }

    /// Get all registered subjects
    pub async fn get_subjects(&self) -> TestHarnessResult<Vec<String>> {
        let registry =
            self.schema_registry
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Schema registry not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        registry
            .get_subjects()
            .await
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to get subjects: {}", e),
                source: Some(e.to_string()),
            })
    }

    /// Check if schema registry is available
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
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

    #[test]
    fn test_schema_registry_not_available_before_start() {
        let infra = TestHarnessInfra::new();
        assert!(!infra.has_schema_registry());
        assert!(infra.schema_registry().is_none());
    }

    // Note: Schema registry async tests require a Kafka connection to start()
    // These are tested in integration tests with testcontainers
}
