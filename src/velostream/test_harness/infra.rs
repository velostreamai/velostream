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
use super::spec::TopicNamingConfig;
use crate::velostream::kafka::common_config::apply_broker_address_family;
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

use testcontainers::runners::AsyncRunner;
// Testcontainers support - available in any test context

use testcontainers::ContainerAsync;

use testcontainers_modules::kafka::KAFKA_PORT;

use testcontainers_redpanda_rs::{REDPANDA_PORT, Redpanda};

/// Type of Kafka-compatible container to use

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerType {
    /// Confluent Kafka (default, widely tested)
    #[default]
    Kafka,
    /// Redpanda (faster startup, Kafka-compatible)
    Redpanda,
}

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

    /// Get the Docker image name for this container type
    pub fn image_filter(&self) -> &'static str {
        match self {
            ContainerType::Kafka => "confluentinc/cp-kafka",
            ContainerType::Redpanda => "redpandadata/redpanda",
        }
    }

    /// Get the internal port for this container type
    pub fn internal_port(&self) -> u16 {
        match self {
            ContainerType::Kafka => 9093,    // KAFKA_PORT
            ContainerType::Redpanda => 9092, // REDPANDA_PORT
        }
    }
}

/// Label used to identify velostream test containers for reuse
const VELOSTREAM_CONTAINER_LABEL: &str = "velostream.test.reusable";

/// Label for container start timestamp (ISO 8601 format)
const VELOSTREAM_CONTAINER_STARTED_LABEL: &str = "velostream.test.started";

/// Find an available port for the container
fn find_available_port() -> Option<u16> {
    use std::net::TcpListener;

    // Try to bind to port 0 to get an available port
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|addr| addr.port())
}

/// Wait for Kafka to be ready by attempting to connect
async fn wait_for_kafka_ready(bootstrap_servers: &str, timeout: Duration) -> TestHarnessResult<()> {
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        // Try to create an admin client and fetch metadata
        let config_result: Result<AdminClient<DefaultClientContext>, _> =
            create_kafka_config(bootstrap_servers)
                .set("socket.timeout.ms", "5000")
                .set("request.timeout.ms", "5000")
                .create();

        if let Ok(admin) = config_result {
            // Try to fetch metadata
            if admin
                .inner()
                .fetch_metadata(None, Duration::from_secs(5))
                .is_ok()
            {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(TestHarnessError::InfraError {
        message: format!(
            "Kafka at {} not ready after {:?}",
            bootstrap_servers, timeout
        ),
        source: None,
    })
}

/// Find an existing reusable container and return its bootstrap servers
///
/// Looks for containers with the velostream label that are running and healthy.
/// Returns the bootstrap servers address if found.
pub fn find_reusable_container(container_type: ContainerType) -> Option<String> {
    use std::process::Command;

    // Find containers with our label, including the started timestamp
    let output = Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("label={}", VELOSTREAM_CONTAINER_LABEL),
            "--filter",
            "status=running",
            "--format",
            "{{.ID}}\t{{.Image}}\t{{.Label \"velostream.test.started\"}}",
        ])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let image_filter = container_type.image_filter();

    // Find a container matching our image filter
    let (container_id, started_at) = stdout
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 && parts[1].contains(image_filter) {
                let started = if parts.len() >= 3 && !parts[2].is_empty() {
                    parts[2].to_string()
                } else {
                    "unknown".to_string()
                };
                Some((parts[0].trim().to_string(), started))
            } else {
                None
            }
        })
        .next()?;

    if container_id.is_empty() {
        return None;
    }

    log::info!(
        "Found reusable {} container: {} (started: {})",
        container_type.name(),
        container_id,
        started_at
    );

    // Get the mapped port for the container
    let port_output = Command::new("docker")
        .args([
            "port",
            &container_id,
            &container_type.internal_port().to_string(),
        ])
        .output()
        .ok()?;

    let port_mapping = String::from_utf8_lossy(&port_output.stdout);
    // Format is like "0.0.0.0:55001" or "127.0.0.1:55001"
    let port = port_mapping
        .lines()
        .next()?
        .trim()
        .split(':')
        .next_back()?
        .trim();

    if port.is_empty() {
        return None;
    }

    Some(format!("127.0.0.1:{}", port))
}

/// Clean up orphaned test containers
///
/// Removes any containers with the velostream test label that may have been
/// left behind from previous test runs (e.g., after crashes or SIGKILL).
pub fn cleanup_orphaned_containers() {
    use std::process::Command;

    log::debug!("Checking for orphaned test containers...");

    // Find all containers with our label (running or stopped)
    let output = match Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label={}", VELOSTREAM_CONTAINER_LABEL),
        ])
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            log::debug!("Could not check for orphaned containers: {}", e);
            return;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let ids: Vec<&str> = stdout
        .lines()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if ids.is_empty() {
        log::debug!("No orphaned containers found");
        return;
    }

    log::info!("Cleaning up {} orphaned test container(s)...", ids.len());

    for id in ids {
        let _ = Command::new("docker").args(["rm", "-f", id]).output();
    }
}

/// Clean up containers of a different type than requested
///
/// When reusing containers, we only want one container type running.
/// This removes any velostream test containers that don't match the requested type.
fn cleanup_wrong_type_containers(keep_type: ContainerType) {
    use std::process::Command;

    let output = match Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("label={}", VELOSTREAM_CONTAINER_LABEL),
            "--filter",
            "status=running",
            "--format",
            "{{.ID}}\t{{.Image}}",
        ])
        .output()
    {
        Ok(o) => o,
        Err(_) => return,
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let keep_filter = keep_type.image_filter();

    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 {
            let container_id = parts[0].trim();
            let image = parts[1];

            // If this container doesn't match the type we want to keep, remove it
            if !image.contains(keep_filter) && !container_id.is_empty() {
                log::info!(
                    "Removing container {} (wrong type: {}, wanted: {})",
                    container_id,
                    image,
                    keep_type.name()
                );
                let _ = Command::new("docker")
                    .args(["rm", "-f", container_id])
                    .output();
            }
        }
    }
}

/// Create a base Kafka ClientConfig with bootstrap servers and address family configured.
///
/// This helper reduces boilerplate when creating Kafka clients. Additional settings
/// can be chained on the returned config.
///
/// # Example
/// ```rust,ignore
/// let config = create_kafka_config("localhost:9092")
///     .set("group.id", "my-group")
///     .set("auto.offset.reset", "earliest");
/// let consumer: StreamConsumer = config.create()?;
/// ```
pub fn create_kafka_config(bootstrap_servers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_servers);
    apply_broker_address_family(&mut config);
    config
}

/// Container instance enum to hold either Kafka or Redpanda
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

    /// Topic naming configuration for CI/CD isolation (P1.2)
    topic_naming: TopicNamingConfig,

    /// Container instance (Kafka or Redpanda) - only in tests

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
            topic_naming: TopicNamingConfig::default(),

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
            topic_naming: TopicNamingConfig::default(),

            container: None,
        }
    }

    /// Set custom topic naming configuration
    ///
    /// Use this to configure how topics are named for CI/CD isolation.
    pub fn with_topic_naming(mut self, config: TopicNamingConfig) -> Self {
        self.topic_naming = config;
        self
    }

    /// Set topic naming configuration (mutable)
    pub fn set_topic_naming(&mut self, config: TopicNamingConfig) {
        self.topic_naming = config;
    }

    /// Get the current topic naming configuration
    pub fn topic_naming(&self) -> &TopicNamingConfig {
        &self.topic_naming
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
    pub async fn with_redpanda() -> TestHarnessResult<Self> {
        Self::with_testcontainers_type(ContainerType::Redpanda).await
    }

    /// Create infrastructure with a specific container type
    ///
    /// # Arguments
    /// * `container_type` - The type of container to start (Kafka or Redpanda)
    pub async fn with_testcontainers_type(
        container_type: ContainerType,
    ) -> TestHarnessResult<Self> {
        log::info!(
            "Starting {} container via testcontainers...",
            container_type.name()
        );

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
                let redpanda_container = Redpanda::default().start().await.map_err(|e| {
                    TestHarnessError::InfraError {
                        message: format!("Failed to start Redpanda container: {}", e),
                        source: Some(e.to_string()),
                    }
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
            topic_naming: TopicNamingConfig::default(),
            container: Some(container),
        })
    }

    /// Create infrastructure with container reuse support
    ///
    /// This method first checks for an existing reusable container. If found,
    /// it connects to that container instead of starting a new one. This is
    /// much faster for iterative development (~0s vs ~3-10s startup).
    ///
    /// When reusing a container, existing topics are cleaned up to ensure
    /// test isolation.
    ///
    /// # Arguments
    /// * `reuse` - If true, attempt to reuse an existing container
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Fast mode - reuse existing container if available
    /// let mut infra = TestHarnessInfra::with_testcontainers_reuse(true).await?;
    /// infra.start().await?;
    /// // run tests...
    /// infra.stop_with_reuse(true).await?; // Keep container for next run
    /// ```
    pub async fn with_testcontainers_reuse(reuse: bool) -> TestHarnessResult<Self> {
        let container_type = ContainerType::from_env();

        // If reuse is enabled, check for existing container first
        if reuse {
            if let Some(bootstrap_servers) = find_reusable_container(container_type) {
                log::info!(
                    "♻️  Reusing existing {} container at {}",
                    container_type.name(),
                    bootstrap_servers
                );

                let run_id = generate_run_id();
                let mut infra = Self {
                    run_id,
                    bootstrap_servers: Some(bootstrap_servers),
                    temp_dir: None,
                    created_topics: Vec::new(),
                    is_running: false,
                    admin_client: None,
                    owns_kafka: false, // Don't clean up container on stop
                    schema_registry: None,
                    topic_naming: TopicNamingConfig::default(),
                    container: None, // No container handle - it's external
                };

                // Start infra to create admin client, then clean up old topics
                infra.start().await?;
                infra.cleanup_all_test_topics().await?;

                return Ok(infra);
            } else {
                log::info!("No reusable container found, starting new one...");
                // Clean up any containers of the wrong type before starting
                cleanup_wrong_type_containers(container_type);
            }
        } else {
            // Clean up any orphaned containers before starting fresh
            cleanup_orphaned_containers();
        }

        // Start a new container with the reusable label
        Self::start_labeled_container(container_type).await
    }

    /// Start a new container with the velostream reusable label
    async fn start_labeled_container(container_type: ContainerType) -> TestHarnessResult<Self> {
        use std::process::Command;

        log::info!(
            "Starting {} container with reuse label...",
            container_type.name()
        );

        // Use docker run directly so we can add our label
        // This is more reliable than trying to modify testcontainers behavior
        let (image, port_mapping, internal_port) = match container_type {
            ContainerType::Kafka => ("confluentinc/cp-kafka:7.5.0", "9093", 9093),
            ContainerType::Redpanda => ("redpandadata/redpanda:v23.2.14", "9092", 9092),
        };

        // Find an available port
        let host_port = find_available_port().unwrap_or(9092);

        // Generate timestamp label for tracking when container was started
        let started_label = format!(
            "{}={}",
            VELOSTREAM_CONTAINER_STARTED_LABEL,
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );

        let container_id = match container_type {
            ContainerType::Kafka => {
                // Kafka requires specific environment setup
                let output = Command::new("docker")
                    .args([
                        "run", "-d",
                        "--label", VELOSTREAM_CONTAINER_LABEL,
                        "--label", &started_label,
                        "-p", &format!("{}:{}", host_port, internal_port),
                        "-e", "KAFKA_NODE_ID=1",
                        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                        "-e", &format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://127.0.0.1:{}", host_port),
                        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
                        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093",
                        "-e", "KAFKA_LISTENERS=PLAINTEXT://localhost:29092,CONTROLLER://localhost:29093,PLAINTEXT_HOST://0.0.0.0:9093",
                        "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                        "-e", "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
                        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                        image,
                    ])
                    .output()
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to start Kafka container: {}", e),
                        source: Some(e.to_string()),
                    })?;

                if !output.status.success() {
                    return Err(TestHarnessError::InfraError {
                        message: format!(
                            "Failed to start Kafka container: {}",
                            String::from_utf8_lossy(&output.stderr)
                        ),
                        source: None,
                    });
                }

                String::from_utf8_lossy(&output.stdout).trim().to_string()
            }
            ContainerType::Redpanda => {
                // Redpanda is simpler to configure
                let output = Command::new("docker")
                    .args([
                        "run",
                        "-d",
                        "--label",
                        VELOSTREAM_CONTAINER_LABEL,
                        "--label",
                        &started_label,
                        "-p",
                        &format!("{}:{}", host_port, internal_port),
                        image,
                        "redpanda",
                        "start",
                        "--smp",
                        "1",
                        "--memory",
                        "512M",
                        "--overprovisioned",
                        "--node-id",
                        "0",
                        "--kafka-addr",
                        &format!("PLAINTEXT://0.0.0.0:{}", internal_port),
                        "--advertise-kafka-addr",
                        &format!("PLAINTEXT://127.0.0.1:{}", host_port),
                    ])
                    .output()
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to start Redpanda container: {}", e),
                        source: Some(e.to_string()),
                    })?;

                if !output.status.success() {
                    return Err(TestHarnessError::InfraError {
                        message: format!(
                            "Failed to start Redpanda container: {}",
                            String::from_utf8_lossy(&output.stderr)
                        ),
                        source: None,
                    });
                }

                String::from_utf8_lossy(&output.stdout).trim().to_string()
            }
        };

        log::info!(
            "{} container started (id: {}), waiting for readiness...",
            container_type.name(),
            &container_id[..12.min(container_id.len())]
        );

        let bootstrap_servers = format!("127.0.0.1:{}", host_port);

        // Wait for container to be ready
        wait_for_kafka_ready(&bootstrap_servers, Duration::from_secs(30)).await?;

        log::info!(
            "{} container ready at {}",
            container_type.name(),
            bootstrap_servers
        );

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
            topic_naming: TopicNamingConfig::default(),
            container: None, // We're managing via docker CLI, not testcontainers handle
        })
    }

    /// Clean up all test topics (used when reusing containers)
    async fn cleanup_all_test_topics(&mut self) -> TestHarnessResult<()> {
        let admin_client = match &self.admin_client {
            Some(client) => client,
            None => return Ok(()), // No admin client yet
        };

        // Fetch all topics
        let metadata = admin_client
            .inner()
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to fetch topic metadata: {}", e),
                source: Some(e.to_string()),
            })?;

        // Find test topics (those starting with "test_" or matching our patterns)
        let test_topics: Vec<&str> = metadata
            .topics()
            .iter()
            .map(|t| t.name())
            .filter(|name| {
                name.starts_with("test_")
                    || name.starts_with("in_")
                    || name.starts_with("out_")
                    || name.contains("_ts")
                    || name.contains("market_data")
                    || name.contains("trading")
            })
            .filter(|name| !name.starts_with("__")) // Skip internal topics
            .collect();

        if test_topics.is_empty() {
            log::debug!("No test topics to clean up");
            return Ok(());
        }

        log::info!(
            "Cleaning up {} test topic(s) from reused container...",
            test_topics.len()
        );

        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        if let Err(e) = admin_client.delete_topics(&test_topics, &options).await {
            log::warn!("Failed to delete some topics (may not exist): {}", e);
        }

        // Give Kafka/Redpanda time to process topic deletions fully.
        // Redpanda needs sufficient time to complete deletion before topics can be
        // recreated with the same name; otherwise consumers may see stale metadata.
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    /// Stop infrastructure with optional container preservation
    ///
    /// When `keep_for_reuse` is true, the container is kept running for
    /// subsequent test runs. Only topics are cleaned up.
    pub async fn stop_with_reuse(&mut self, keep_for_reuse: bool) -> TestHarnessResult<()> {
        if !self.is_running {
            return Ok(());
        }

        log::info!("Stopping test infrastructure (run_id: {})", self.run_id);

        // Delete created topics
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

        // Handle container based on reuse preference
        if keep_for_reuse {
            log::info!("♻️  Keeping container running for reuse");
            // Don't stop the container - just clear our state
        } else {
            // Stop and remove container if we own it
            self.stop_container().await;
        }

        self.admin_client = None;
        self.schema_registry = None;
        self.created_topics.clear();
        self.is_running = false;

        log::info!("Test infrastructure stopped");
        Ok(())
    }

    /// Stop the container (internal helper)
    async fn stop_container(&mut self) {
        // If we have a testcontainers handle, use it
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
        } else if self.owns_kafka {
            // We started a container via docker CLI - stop it by finding our labeled container
            use std::process::Command;
            let container_type = ContainerType::from_env();

            let output = Command::new("docker")
                .args([
                    "ps",
                    "-q",
                    "--filter",
                    &format!("label={}", VELOSTREAM_CONTAINER_LABEL),
                    "--filter",
                    &format!("ancestor={}", container_type.image_filter()),
                ])
                .output();

            if let Ok(output) = output {
                let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !id.is_empty() {
                    log::info!("Stopping container {}...", &id[..12.min(id.len())]);
                    let _ = Command::new("docker").args(["rm", "-f", &id]).output();
                    log::info!("Container stopped and removed");
                }
            }
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
        let admin_client: AdminClient<DefaultClientContext> =
            create_kafka_config(&bootstrap_servers)
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

    /// Generate a test topic name using the configured naming pattern
    ///
    /// The default pattern is `test_{run_id}_{base}`, but can be customized
    /// via `TopicNamingConfig` for CI/CD isolation.
    ///
    /// # Supported Placeholders
    /// - `{run_id}` - Unique run identifier
    /// - `{base}` - Original topic name
    /// - `{branch}` - Git branch (from env or config)
    /// - `{user}` - User identity (from env or config)
    /// - `{timestamp}` - Unix timestamp
    /// - `{namespace}` - Custom namespace prefix
    pub fn topic_name(&self, base_name: &str) -> String {
        self.topic_naming
            .resolve_topic_name(base_name, &self.run_id)
    }

    /// Generate a test topic name with the original (legacy) pattern
    /// Always uses `test_{run_id}_{base}` regardless of topic_naming config
    pub fn topic_name_legacy(&self, base_name: &str) -> String {
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

    /// Create a topic with the exact name provided (no prefix transformation)
    ///
    /// Use this when you need to create a topic that matches what Kafka jobs will consume from
    /// (e.g., for empty input tests where no records are published but the topic must exist).
    pub async fn create_topic_raw(
        &mut self,
        topic_name: &str,
        partitions: i32,
    ) -> TestHarnessResult<String> {
        let admin_client =
            self.admin_client
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Admin client not initialized. Call start() first.".to_string(),
                    source: None,
                })?;

        log::debug!(
            "Creating topic (raw): {} (partitions: {})",
            topic_name,
            partitions
        );

        let new_topic = NewTopic::new(topic_name, partitions, TopicReplication::Fixed(1));
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

        self.created_topics.push(topic_name.to_string());
        log::info!("Created topic (raw): {}", topic_name);
        Ok(topic_name.to_string())
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

        let producer: rdkafka::producer::FutureProducer<DefaultClientContext> =
            create_kafka_config(bootstrap_servers)
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
    ) -> TestHarnessResult<crate::velostream::kafka::kafka_fast_producer::AsyncPolledProducer> {
        let bootstrap_servers =
            self.bootstrap_servers
                .as_ref()
                .ok_or_else(|| TestHarnessError::InfraError {
                    message: "Bootstrap servers not available. Call start() first.".to_string(),
                    source: None,
                })?;

        crate::velostream::kafka::kafka_fast_producer::AsyncPolledProducer::with_high_throughput(
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
            create_kafka_config(bootstrap_servers)
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

    // =========================================================================
    // Debug Inspection Methods (for list-topics, list-consumers commands)
    // =========================================================================

    /// Get list of topics created by this test harness
    pub fn created_topics(&self) -> &[String] {
        &self.created_topics
    }

    /// Fetch topic metadata including partition info, offsets, and message counts
    ///
    /// Returns information about all topics or a specific topic if name is provided.
    pub async fn fetch_topic_info(
        &self,
        topic_filter: Option<&str>,
    ) -> TestHarnessResult<Vec<super::statement_executor::TopicInfo>> {
        use super::statement_executor::{PartitionInfo, TopicInfo};
        use rdkafka::consumer::Consumer;

        let consumer = self.create_consumer("__topic_inspector")?;

        // Fetch cluster metadata
        let metadata = consumer
            .fetch_metadata(topic_filter, Duration::from_secs(10))
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to fetch metadata: {}", e),
                source: Some(e.to_string()),
            })?;

        let mut topics = Vec::new();

        for topic_metadata in metadata.topics() {
            let topic_name = topic_metadata.name().to_string();

            // Filter by test harness topics if no specific filter provided
            let is_test_topic = self.created_topics.contains(&topic_name);

            // If filter specified, only include matching topic
            if let Some(filter) = topic_filter {
                if topic_name != filter {
                    continue;
                }
            }

            let mut partitions = Vec::new();
            let mut total_messages = 0i64;

            for partition in topic_metadata.partitions() {
                let partition_id = partition.id();

                // Fetch watermarks (low and high offsets)
                let (low, high) = consumer
                    .fetch_watermarks(&topic_name, partition_id, Duration::from_secs(5))
                    .unwrap_or((0, 0));

                let message_count = high - low;
                total_messages += message_count;

                // Fetch the last message to get timestamp and key
                let (latest_timestamp_ms, latest_key) = if high > low {
                    self.fetch_last_message_info(&topic_name, partition_id, high - 1)
                        .await
                        .unwrap_or((None, None))
                } else {
                    (None, None)
                };

                partitions.push(PartitionInfo {
                    partition: partition_id,
                    low_offset: low,
                    high_offset: high,
                    message_count,
                    latest_timestamp_ms,
                    latest_key,
                });
            }

            topics.push(TopicInfo {
                name: topic_name,
                partitions,
                total_messages,
                is_test_topic,
            });
        }

        // Sort by name for consistent output
        topics.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(topics)
    }

    /// Fetch the last message info (timestamp and key) for a specific partition
    async fn fetch_last_message_info(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> TestHarnessResult<(Option<i64>, Option<String>)> {
        use rdkafka::Offset;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;

        let bootstrap_servers = match &self.bootstrap_servers {
            Some(servers) => servers,
            None => return Ok((None, None)),
        };

        // Create a temporary consumer to fetch the specific message
        let consumer: BaseConsumer = match create_kafka_config(bootstrap_servers)
            .set("group.id", format!("__last_msg_inspector_{}", partition))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
        {
            Ok(c) => c,
            Err(_) => return Ok((None, None)),
        };

        // Assign to specific partition and offset
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to set partition offset: {}", e),
                source: Some(e.to_string()),
            })?;

        consumer
            .assign(&tpl)
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to assign partition: {}", e),
                source: Some(e.to_string()),
            })?;

        // Poll for the message with a short timeout
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(msg)) => {
                let timestamp_ms = msg.timestamp().to_millis();
                let key = msg
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string());
                Ok((timestamp_ms, key))
            }
            _ => Ok((None, None)),
        }
    }

    /// Fetch schema information for a topic by consuming sample messages
    ///
    /// Returns inferred schema (field names and types) from the topic's messages.
    pub async fn fetch_topic_schema(
        &self,
        topic: &str,
        max_records: usize,
    ) -> TestHarnessResult<super::statement_executor::TopicSchema> {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;

        let bootstrap_servers = match &self.bootstrap_servers {
            Some(servers) => servers,
            None => {
                return Err(TestHarnessError::InfraError {
                    message: "No bootstrap servers configured".to_string(),
                    source: None,
                });
            }
        };

        // Create a temporary consumer
        let consumer: BaseConsumer = create_kafka_config(bootstrap_servers)
            .set("group.id", format!("__schema_inspector_{}", topic))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create consumer: {}", e),
                source: Some(e.to_string()),
            })?;

        // Subscribe to the topic
        consumer
            .subscribe(&[topic])
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to subscribe to topic '{}': {}", topic, e),
                source: Some(e.to_string()),
            })?;

        // Collect sample messages
        let mut records: Vec<serde_json::Value> = Vec::new();
        let mut has_keys = false;
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(5);

        while records.len() < max_records && start.elapsed() < timeout {
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(msg)) => {
                    // Check for key
                    if msg.key().is_some() {
                        has_keys = true;
                    }

                    // Parse payload as JSON
                    if let Some(payload) = msg.payload() {
                        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload) {
                            records.push(json);
                        }
                    }
                }
                Some(Err(_)) => continue,
                None => {
                    if records.is_empty() {
                        // No messages yet, keep trying
                        continue;
                    } else {
                        // Got some messages, can stop
                        break;
                    }
                }
            }
        }

        if records.is_empty() {
            return Ok(super::statement_executor::TopicSchema {
                topic: topic.to_string(),
                fields: Vec::new(),
                sample_value: None,
                has_keys,
                records_sampled: 0,
            });
        }

        // Infer schema from first record
        let first_record = &records[0];
        let fields: Vec<(String, String)> = if let Some(obj) = first_record.as_object() {
            let mut field_list: Vec<(String, String)> = obj
                .iter()
                .map(|(k, v)| {
                    let type_name = match v {
                        serde_json::Value::Null => "Null".to_string(),
                        serde_json::Value::Bool(_) => "Boolean".to_string(),
                        serde_json::Value::Number(n) => {
                            if n.is_i64() {
                                "Integer".to_string()
                            } else {
                                "Float".to_string()
                            }
                        }
                        serde_json::Value::String(s) => {
                            // Try to detect timestamps
                            if s.contains('T') && s.contains(':') {
                                "DateTime".to_string()
                            } else {
                                "String".to_string()
                            }
                        }
                        serde_json::Value::Array(_) => "Array".to_string(),
                        serde_json::Value::Object(_) => "Object".to_string(),
                    };
                    (k.clone(), type_name)
                })
                .collect();
            field_list.sort_by(|a, b| a.0.cmp(&b.0));
            field_list
        } else {
            Vec::new()
        };

        // Format sample value
        let sample_value = serde_json::to_string_pretty(first_record).ok();

        Ok(super::statement_executor::TopicSchema {
            topic: topic.to_string(),
            fields,
            sample_value,
            has_keys,
            records_sampled: records.len(),
        })
    }

    /// Peek at messages from a Kafka topic
    ///
    /// Reads messages from a topic for debugging purposes. Can read from
    /// the beginning, end, or a specific offset.
    pub async fn peek_topic_messages(
        &self,
        topic: &str,
        limit: usize,
        from_end: bool,
        start_offset: Option<i64>,
        partition_filter: Option<i32>,
    ) -> TestHarnessResult<Vec<super::statement_executor::TopicMessage>> {
        use rdkafka::Offset;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;
        use rdkafka::topic_partition_list::TopicPartitionList;

        let bootstrap_servers = match &self.bootstrap_servers {
            Some(servers) => servers,
            None => {
                return Err(TestHarnessError::InfraError {
                    message: "No bootstrap servers configured".to_string(),
                    source: None,
                });
            }
        };

        // Create a temporary consumer
        let consumer: BaseConsumer = create_kafka_config(bootstrap_servers)
            .set(
                "group.id",
                &format!("__message_peek_{}_{}", topic, uuid::Uuid::new_v4()),
            )
            .set("enable.auto.commit", "false")
            .set(
                "auto.offset.reset",
                if from_end { "latest" } else { "earliest" },
            )
            .create()
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create consumer: {}", e),
                source: Some(e.to_string()),
            })?;

        // Get topic metadata to find partitions
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to fetch metadata for '{}': {}", topic, e),
                source: Some(e.to_string()),
            })?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| TestHarnessError::InfraError {
                message: format!("Topic '{}' not found", topic),
                source: None,
            })?;

        let partitions: Vec<i32> = topic_metadata
            .partitions()
            .iter()
            .map(|p| p.id())
            .filter(|p| partition_filter.is_none_or(|f| *p == f))
            .collect();

        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Assign partitions with appropriate offsets
        let mut tpl = TopicPartitionList::new();
        for partition in &partitions {
            if let Some(offset) = start_offset {
                tpl.add_partition_offset(topic, *partition, Offset::Offset(offset))
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to set offset: {}", e),
                        source: Some(e.to_string()),
                    })?;
            } else if from_end {
                // For "last N" messages, we need to calculate the offset
                let (low, high) = consumer
                    .fetch_watermarks(topic, *partition, Duration::from_secs(5))
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to fetch watermarks: {}", e),
                        source: Some(e.to_string()),
                    })?;
                let start = (high - limit as i64).max(low);
                tpl.add_partition_offset(topic, *partition, Offset::Offset(start))
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to set offset: {}", e),
                        source: Some(e.to_string()),
                    })?;
            } else {
                tpl.add_partition_offset(topic, *partition, Offset::Beginning)
                    .map_err(|e| TestHarnessError::InfraError {
                        message: format!("Failed to set offset: {}", e),
                        source: Some(e.to_string()),
                    })?;
            }
        }

        consumer
            .assign(&tpl)
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to assign partitions: {}", e),
                source: Some(e.to_string()),
            })?;

        // Collect messages
        let mut messages: Vec<super::statement_executor::TopicMessage> = Vec::new();
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(5);

        while messages.len() < limit && start.elapsed() < timeout {
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(msg)) => {
                    let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());

                    let value = msg
                        .payload()
                        .map(|p| String::from_utf8_lossy(p).to_string())
                        .unwrap_or_else(|| "<empty>".to_string());

                    let timestamp_ms = msg.timestamp().to_millis();

                    // Extract headers
                    let headers: Vec<(String, String)> = msg
                        .headers()
                        .map(|hdrs| {
                            use rdkafka::message::Headers;
                            (0..hdrs.count())
                                .map(|i| {
                                    let header = hdrs.get(i);
                                    (
                                        header.key.to_string(),
                                        header
                                            .value
                                            .map(|v| String::from_utf8_lossy(v).to_string())
                                            .unwrap_or_default(),
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    messages.push(super::statement_executor::TopicMessage {
                        partition: msg.partition(),
                        offset: msg.offset(),
                        key,
                        value,
                        timestamp_ms,
                        headers,
                    });
                }
                Some(Err(_)) => continue,
                None => {
                    if messages.is_empty() {
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        // Sort by offset if reading from end
        if from_end {
            messages.sort_by_key(|m| m.offset);
        }

        Ok(messages)
    }

    /// Get consumer group information
    ///
    /// Lists all consumer groups from Kafka and returns their metadata.
    /// This queries the Kafka broker to get actual consumer group state.
    pub async fn get_consumer_info(
        &self,
    ) -> TestHarnessResult<Vec<super::statement_executor::ConsumerInfo>> {
        use super::statement_executor::{ConsumerInfo, ConsumerState};
        use rdkafka::consumer::{BaseConsumer, Consumer};

        let bootstrap_servers = match &self.bootstrap_servers {
            Some(servers) => servers,
            None => {
                return Err(TestHarnessError::InfraError {
                    message: "Bootstrap servers not available. Call start() first.".to_string(),
                    source: None,
                });
            }
        };

        // Create a temporary consumer to fetch group list
        let consumer: BaseConsumer =
            create_kafka_config(bootstrap_servers)
                .create()
                .map_err(|e| TestHarnessError::InfraError {
                    message: format!("Failed to create consumer for group list: {}", e),
                    source: Some(e.to_string()),
                })?;

        // Fetch all consumer groups (None = all groups)
        let group_list = consumer
            .fetch_group_list(None, Duration::from_secs(10))
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to fetch consumer groups: {}", e),
                source: Some(e.to_string()),
            })?;

        let mut consumers = Vec::new();

        for group in group_list.groups() {
            // Filter to only show test harness related consumer groups
            // (groups with "test_harness" or "velo_" prefix - underscore delimiter format)
            let group_id = group.name();
            if !group_id.contains("test_harness") && !group_id.starts_with("velo_") {
                continue;
            }

            // Get group members and their topic subscriptions
            let subscribed_topics: Vec<String> = Vec::new();
            for member in group.members() {
                if let Some(assignment) = member.assignment() {
                    // Parse the assignment bytes to get topic-partitions
                    // The assignment is a serialized list of topic-partitions
                    // For simplicity, we'll just note the member exists
                    log::debug!(
                        "Group {} member {} has {} bytes of assignment",
                        group_id,
                        member.id(),
                        assignment.len()
                    );
                }
            }

            // Determine consumer state based on group state
            let state = match group.state() {
                "Stable" => ConsumerState::Active,
                "Empty" => ConsumerState::Stopped,
                "PreparingRebalance" | "CompletingRebalance" => ConsumerState::Active,
                "Dead" => ConsumerState::Stopped,
                _ => ConsumerState::Unknown,
            };

            // Note: Getting actual topic subscriptions and positions requires
            // more detailed admin API calls which are complex. For now we just
            // report the group and its state.
            consumers.push(ConsumerInfo {
                group_id: group_id.to_string(),
                subscribed_topics,
                positions: vec![], // Would require describe_consumer_groups for details
                state,
            });
        }

        // Sort by group ID for consistent display
        consumers.sort_by(|a, b| a.group_id.cmp(&b.group_id));

        Ok(consumers)
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
