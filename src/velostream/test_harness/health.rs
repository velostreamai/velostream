//! Health checking module for Velostream test infrastructure.
//!
//! This module provides comprehensive health checks for Velostream deployments,
//! including Docker containers, Kafka brokers, topics, processes, and consumer groups.
//!
//! # Overview
//!
//! The health checker can verify:
//! - **Docker containers**: Checks for running Velostream test containers
//! - **Kafka broker**: Validates broker connectivity and metadata
//! - **Topics**: Lists topics with partition and message counts
//! - **Processes**: Detects running velo-test and velo-sql processes
//! - **Consumer groups**: Lists Velostream consumer groups and their state
//!
//! # Usage
//!
//! ## Command Line
//!
//! ```bash
//! # Check all components
//! velo-test health --broker localhost:9092
//!
//! # JSON output for CI/CD pipelines
//! velo-test health --broker localhost:9092 --output json
//!
//! # Check specific components only
//! velo-test health --broker localhost:9092 --check kafka,topics
//!
//! # Custom timeout (in seconds)
//! velo-test health --broker localhost:9092 --timeout 30
//! ```
//!
//! ## Programmatic Usage
//!
//! ```rust,ignore
//! use velostream::velostream::test_harness::health::{HealthChecker, HealthCheckType};
//!
//! // Run all checks
//! let checker = HealthChecker::new()
//!     .with_bootstrap_servers("localhost:9092");
//! let report = checker.run_all().await;
//!
//! // Run specific checks
//! let report = checker.run_checks(&[
//!     HealthCheckType::Kafka,
//!     HealthCheckType::Topics,
//! ]).await;
//!
//! // Check results
//! if report.is_healthy() {
//!     println!("All checks passed!");
//! } else {
//!     eprintln!("Health check failed: {}", report.format_text());
//! }
//! ```
//!
//! # Exit Codes
//!
//! When used via CLI:
//! - `0`: All checks healthy or warning
//! - `1`: One or more checks unhealthy

use super::error::TestHarnessResult;
use super::infra::create_kafka_config;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde::{Deserialize, Serialize};
use std::process::Command;
use std::time::Duration;

// ============================================================================
// Health Check Types
// ============================================================================

/// Types of health checks that can be performed.
///
/// Each check type validates a different aspect of the Velostream infrastructure.
///
/// # Examples
///
/// ```rust
/// use velostream::velostream::test_harness::health::HealthCheckType;
///
/// // Parse from comma-separated string (CLI input)
/// let checks = HealthCheckType::parse_list("docker,kafka,topics");
/// assert_eq!(checks.len(), 3);
///
/// // Get all check types
/// let all = HealthCheckType::all();
/// assert_eq!(all.len(), 5);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckType {
    /// Check Docker containers (velostream.test.reusable labeled + named containers)
    Docker,
    /// Check Kafka broker connectivity and metadata
    Kafka,
    /// Check topic existence, partitions, and message counts
    Topics,
    /// Check running velo-test and velo-sql processes
    Processes,
    /// Check Velostream consumer groups
    ConsumerGroups,
}

impl HealthCheckType {
    /// Parse check types from a comma-separated string.
    ///
    /// Accepts various formats for consumer groups: `consumer_groups`, `consumergroups`, `consumers`.
    /// Unknown check types are silently ignored.
    ///
    /// # Arguments
    ///
    /// * `input` - Comma-separated list of check types (case-insensitive)
    ///
    /// # Returns
    ///
    /// Vector of parsed check types (empty if no valid types found)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use velostream::velostream::test_harness::health::HealthCheckType;
    ///
    /// let checks = HealthCheckType::parse_list("docker,kafka,invalid");
    /// assert_eq!(checks.len(), 2);
    /// assert!(checks.contains(&HealthCheckType::Docker));
    /// assert!(checks.contains(&HealthCheckType::Kafka));
    /// ```
    pub fn parse_list(input: &str) -> Vec<Self> {
        input
            .split(',')
            .filter_map(|s| match s.trim().to_lowercase().as_str() {
                "docker" => Some(HealthCheckType::Docker),
                "kafka" => Some(HealthCheckType::Kafka),
                "topics" => Some(HealthCheckType::Topics),
                "processes" => Some(HealthCheckType::Processes),
                "consumer_groups" | "consumergroups" | "consumers" => {
                    Some(HealthCheckType::ConsumerGroups)
                }
                _ => {
                    if !s.trim().is_empty() {
                        log::warn!("Unknown health check type: '{}'", s.trim());
                    }
                    None
                }
            })
            .collect()
    }

    /// Returns all available check types.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use velostream::velostream::test_harness::health::HealthCheckType;
    ///
    /// let all = HealthCheckType::all();
    /// assert_eq!(all.len(), 5);
    /// ```
    pub fn all() -> Vec<Self> {
        vec![
            HealthCheckType::Docker,
            HealthCheckType::Kafka,
            HealthCheckType::Topics,
            HealthCheckType::Processes,
            HealthCheckType::ConsumerGroups,
        ]
    }

    /// Returns the check type as a lowercase string identifier.
    ///
    /// Useful for serialization and CLI output.
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthCheckType::Docker => "docker",
            HealthCheckType::Kafka => "kafka",
            HealthCheckType::Topics => "topics",
            HealthCheckType::Processes => "processes",
            HealthCheckType::ConsumerGroups => "consumer_groups",
        }
    }
}

impl std::fmt::Display for HealthCheckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthCheckType::Docker => write!(f, "Docker Containers"),
            HealthCheckType::Kafka => write!(f, "Kafka Broker"),
            HealthCheckType::Topics => write!(f, "Topics"),
            HealthCheckType::Processes => write!(f, "Processes"),
            HealthCheckType::ConsumerGroups => write!(f, "Consumer Groups"),
        }
    }
}

// ============================================================================
// Check Status
// ============================================================================

/// Status of a health check result.
///
/// The status determines the overall health report outcome:
/// - `Healthy` + `Warning` = report is considered healthy (exit code 0)
/// - `Unhealthy` = report is considered unhealthy (exit code 1)
/// - `Skipped` = check was not run (e.g., missing configuration)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    /// Check passed successfully
    Healthy,
    /// Check passed with warnings (non-critical issues)
    Warning,
    /// Check failed (critical issue)
    Unhealthy,
    /// Check was skipped (e.g., missing required configuration)
    Skipped,
}

impl CheckStatus {
    /// Returns true if this status indicates a healthy or acceptable state.
    pub fn is_ok(&self) -> bool {
        matches!(
            self,
            CheckStatus::Healthy | CheckStatus::Warning | CheckStatus::Skipped
        )
    }
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Healthy => write!(f, "healthy"),
            CheckStatus::Warning => write!(f, "warning"),
            CheckStatus::Unhealthy => write!(f, "unhealthy"),
            CheckStatus::Skipped => write!(f, "skipped"),
        }
    }
}

// ============================================================================
// Check Result Details
// ============================================================================

/// Information about a Docker container.
///
/// Populated by the Docker health check when containers are found.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContainerInfo {
    /// Container name (from --name or auto-generated)
    pub name: String,
    /// Container ID (short form)
    pub id: String,
    /// Container status string (e.g., "Up 2 hours", "Exited (0) 5 minutes ago")
    pub status: String,
    /// Docker image name and tag
    pub image: String,
    /// Published ports (e.g., ["0.0.0.0:9092->9092/tcp"])
    pub ports: Vec<String>,
    /// True if container status contains "Up"
    pub is_healthy: bool,
}

/// Information about a Kafka topic.
///
/// Populated by the Topics health check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicHealthInfo {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: i32,
    /// Approximate message count (high watermark - low watermark across all partitions)
    pub message_count: i64,
    /// True if message_count is 0
    pub is_empty: bool,
}

/// Information about a running process.
///
/// Populated by the Processes health check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessInfo {
    /// Process pattern that matched (e.g., "velo-sql")
    pub name: String,
    /// Process ID (0 if not running)
    pub pid: u32,
    /// Full command line (may be truncated)
    pub command: String,
    /// True if process is currently running
    pub is_running: bool,
}

/// Information about a Kafka consumer group.
///
/// Populated by the ConsumerGroups health check.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsumerGroupInfo {
    /// Consumer group ID
    pub name: String,
    /// Group state (e.g., "Stable", "Empty", "Dead")
    pub state: String,
    /// Number of members in the group
    pub members: i32,
    /// Total lag across all partitions (0 if not computed)
    pub total_lag: i64,
}

// ============================================================================
// Check Result
// ============================================================================

/// Result of a single health check.
///
/// Contains the check type, status, a human-readable message, and optional
/// detailed information specific to the check type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// The type of check that was performed
    pub check_type: HealthCheckType,
    /// The status of the check
    pub status: CheckStatus,
    /// Human-readable summary message
    pub message: String,
    /// Additional detail messages
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<String>,
    /// Container information (Docker check only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containers: Option<Vec<ContainerInfo>>,
    /// Topic information (Topics check only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<TopicHealthInfo>>,
    /// Process information (Processes check only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes: Option<Vec<ProcessInfo>>,
    /// Consumer group information (ConsumerGroups check only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_groups: Option<Vec<ConsumerGroupInfo>>,
}

impl CheckResult {
    /// Create a healthy check result.
    pub fn healthy(check_type: HealthCheckType, message: impl Into<String>) -> Self {
        Self {
            check_type,
            status: CheckStatus::Healthy,
            message: message.into(),
            details: Vec::new(),
            containers: None,
            topics: None,
            processes: None,
            consumer_groups: None,
        }
    }

    /// Create a warning check result.
    pub fn warning(check_type: HealthCheckType, message: impl Into<String>) -> Self {
        Self {
            check_type,
            status: CheckStatus::Warning,
            message: message.into(),
            details: Vec::new(),
            containers: None,
            topics: None,
            processes: None,
            consumer_groups: None,
        }
    }

    /// Create an unhealthy check result.
    pub fn unhealthy(check_type: HealthCheckType, message: impl Into<String>) -> Self {
        Self {
            check_type,
            status: CheckStatus::Unhealthy,
            message: message.into(),
            details: Vec::new(),
            containers: None,
            topics: None,
            processes: None,
            consumer_groups: None,
        }
    }

    /// Create a skipped check result.
    pub fn skipped(check_type: HealthCheckType, message: impl Into<String>) -> Self {
        Self {
            check_type,
            status: CheckStatus::Skipped,
            message: message.into(),
            details: Vec::new(),
            containers: None,
            topics: None,
            processes: None,
            consumer_groups: None,
        }
    }

    /// Add detail messages to the result.
    pub fn with_details(mut self, details: Vec<String>) -> Self {
        self.details = details;
        self
    }

    /// Add a single detail message to the result.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.details.push(detail.into());
        self
    }

    /// Add container information to the result.
    pub fn with_containers(mut self, containers: Vec<ContainerInfo>) -> Self {
        self.containers = Some(containers);
        self
    }

    /// Add topic information to the result.
    pub fn with_topics(mut self, topics: Vec<TopicHealthInfo>) -> Self {
        self.topics = Some(topics);
        self
    }

    /// Add process information to the result.
    pub fn with_processes(mut self, processes: Vec<ProcessInfo>) -> Self {
        self.processes = Some(processes);
        self
    }

    /// Add consumer group information to the result.
    pub fn with_consumer_groups(mut self, groups: Vec<ConsumerGroupInfo>) -> Self {
        self.consumer_groups = Some(groups);
        self
    }
}

// ============================================================================
// Health Report
// ============================================================================

/// Summary statistics for a health report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthSummary {
    /// Total number of checks performed
    pub total_checks: usize,
    /// Number of healthy checks
    pub healthy: usize,
    /// Number of warning checks
    pub warnings: usize,
    /// Number of unhealthy checks
    pub unhealthy: usize,
    /// Number of skipped checks
    pub skipped: usize,
}

/// Overall health report containing all check results.
///
/// The report aggregates results from multiple health checks and provides
/// an overall status determination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// ISO 8601 timestamp when the report was generated
    pub timestamp: String,
    /// Overall status (unhealthy if any check is unhealthy, warning if any warning)
    pub overall_status: CheckStatus,
    /// Bootstrap servers used for Kafka checks (if any)
    pub bootstrap_servers: Option<String>,
    /// Individual check results
    pub checks: Vec<CheckResult>,
    /// Summary statistics
    pub summary: HealthSummary,
}

impl HealthReport {
    /// Create a new health report from check results.
    ///
    /// The overall status is computed as:
    /// - `Unhealthy` if any check is unhealthy
    /// - `Warning` if any check has warnings (and none unhealthy)
    /// - `Healthy` otherwise
    pub fn new(bootstrap_servers: Option<String>, checks: Vec<CheckResult>) -> Self {
        let summary = HealthSummary {
            total_checks: checks.len(),
            healthy: checks
                .iter()
                .filter(|c| c.status == CheckStatus::Healthy)
                .count(),
            warnings: checks
                .iter()
                .filter(|c| c.status == CheckStatus::Warning)
                .count(),
            unhealthy: checks
                .iter()
                .filter(|c| c.status == CheckStatus::Unhealthy)
                .count(),
            skipped: checks
                .iter()
                .filter(|c| c.status == CheckStatus::Skipped)
                .count(),
        };

        let overall_status = if summary.unhealthy > 0 {
            CheckStatus::Unhealthy
        } else if summary.warnings > 0 {
            CheckStatus::Warning
        } else {
            CheckStatus::Healthy
        };

        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            overall_status,
            bootstrap_servers,
            checks,
            summary,
        }
    }

    /// Returns true if the overall health is acceptable (not unhealthy).
    ///
    /// Healthy and Warning statuses are considered acceptable.
    pub fn is_healthy(&self) -> bool {
        self.overall_status != CheckStatus::Unhealthy
    }

    /// Format the report as human-readable text.
    ///
    /// Suitable for terminal output with Unicode status icons.
    pub fn format_text(&self) -> String {
        let mut output = String::new();

        output.push_str("========================================\n");
        output.push_str("Velostream Health Check Report\n");
        output.push_str("========================================\n\n");

        if let Some(ref bs) = self.bootstrap_servers {
            output.push_str(&format!("Kafka: {}\n", bs));
        }
        output.push_str(&format!("Time: {}\n\n", self.timestamp));

        for (i, check) in self.checks.iter().enumerate() {
            let icon = match check.status {
                CheckStatus::Healthy => "\u{2713}",   // ✓
                CheckStatus::Warning => "\u{26A0}",   // ⚠
                CheckStatus::Unhealthy => "\u{2717}", // ✗
                CheckStatus::Skipped => "\u{25CB}",   // ○
            };

            output.push_str(&format!("{}. {} {}\n", i + 1, check.check_type, icon));
            output.push_str(&format!("   Status: {}\n", check.status));
            output.push_str(&format!("   {}\n", check.message));

            for detail in &check.details {
                output.push_str(&format!("   - {}\n", detail));
            }

            // Show container details
            if let Some(ref containers) = check.containers {
                for container in containers {
                    let status_icon = if container.is_healthy {
                        "\u{2713}"
                    } else {
                        "\u{2717}"
                    };
                    output.push_str(&format!(
                        "   {} {}: {}\n",
                        status_icon, container.name, container.status
                    ));
                }
            }

            // Show topic details
            if let Some(ref topics) = check.topics {
                for topic in topics {
                    let status = if topic.is_empty { "empty" } else { "has data" };
                    output.push_str(&format!(
                        "   - {}: {} messages ({})\n",
                        topic.name, topic.message_count, status
                    ));
                }
            }

            // Show process details
            if let Some(ref processes) = check.processes {
                for process in processes {
                    let status_icon = if process.is_running {
                        "\u{2713}"
                    } else {
                        "\u{2717}"
                    };
                    output.push_str(&format!(
                        "   {} {} (PID: {})\n",
                        status_icon, process.name, process.pid
                    ));
                }
            }

            // Show consumer group details
            if let Some(ref groups) = check.consumer_groups {
                for group in groups {
                    output.push_str(&format!(
                        "   - {}: {} members, lag: {}\n",
                        group.name, group.members, group.total_lag
                    ));
                }
            }

            output.push('\n');
        }

        output.push_str("========================================\n");
        let overall_icon = match self.overall_status {
            CheckStatus::Healthy => "\u{2713}",
            CheckStatus::Warning => "\u{26A0}",
            CheckStatus::Unhealthy => "\u{2717}",
            CheckStatus::Skipped => "\u{25CB}",
        };
        output.push_str(&format!(
            "Overall: {} {}\n",
            self.overall_status.to_string().to_uppercase(),
            overall_icon
        ));
        output.push_str(&format!(
            "Summary: {} healthy, {} warnings, {} unhealthy, {} skipped\n",
            self.summary.healthy,
            self.summary.warnings,
            self.summary.unhealthy,
            self.summary.skipped
        ));

        output
    }

    /// Format the report as JSON.
    ///
    /// Returns pretty-printed JSON suitable for parsing by other tools.
    pub fn format_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| {
            log::error!("Failed to serialize health report to JSON: {}", e);
            "{}".to_string()
        })
    }
}

// ============================================================================
// Health Configuration
// ============================================================================

/// Configuration for health checks.
///
/// Controls which resources to check and how to connect to them.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Kafka bootstrap servers (required for Kafka, Topics, ConsumerGroups checks)
    pub bootstrap_servers: Option<String>,
    /// Timeout for Kafka operations
    pub timeout: Duration,
    /// Container names to check (used by Docker check)
    pub container_names: Vec<String>,
    /// Topic names to check (empty = all topics, used by Topics check)
    pub topic_names: Vec<String>,
    /// Process name patterns to search for (used by Processes check)
    pub process_patterns: Vec<String>,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: None,
            timeout: Duration::from_secs(10),
            container_names: vec!["simple-kafka".to_string(), "simple-zookeeper".to_string()],
            topic_names: Vec::new(),
            process_patterns: vec!["velo-test".to_string(), "velo-sql".to_string()],
        }
    }
}

// ============================================================================
// Health Checker
// ============================================================================

/// Health checker for Velostream infrastructure.
///
/// Provides methods to run individual or combined health checks against
/// Docker, Kafka, and process infrastructure.
///
/// # Examples
///
/// ```rust,ignore
/// use velostream::velostream::test_harness::health::{HealthChecker, HealthCheckType};
///
/// // Create checker with bootstrap servers
/// let checker = HealthChecker::new()
///     .with_bootstrap_servers("localhost:9092");
///
/// // Run all checks
/// let report = checker.run_all().await;
///
/// // Run specific checks
/// let report = checker.run_checks(&[
///     HealthCheckType::Docker,
///     HealthCheckType::Kafka,
/// ]).await;
///
/// // Output results
/// println!("{}", report.format_text());
/// ```
pub struct HealthChecker {
    config: HealthConfig,
}

impl HealthChecker {
    /// Create a new health checker with default configuration.
    pub fn new() -> Self {
        Self {
            config: HealthConfig::default(),
        }
    }

    /// Create a new health checker with custom configuration.
    pub fn with_config(config: HealthConfig) -> Self {
        Self { config }
    }

    /// Set the Kafka bootstrap servers.
    ///
    /// Required for Kafka, Topics, and ConsumerGroups checks.
    pub fn with_bootstrap_servers(mut self, servers: &str) -> Self {
        self.config.bootstrap_servers = Some(servers.to_string());
        self
    }

    /// Set the timeout for Kafka operations.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Run all available health checks.
    pub async fn run_all(&self) -> HealthReport {
        self.run_checks(&HealthCheckType::all()).await
    }

    /// Run specific health checks.
    ///
    /// Checks are executed in the order provided.
    pub async fn run_checks(&self, check_types: &[HealthCheckType]) -> HealthReport {
        let mut results = Vec::with_capacity(check_types.len());

        for check_type in check_types {
            log::debug!("Running health check: {}", check_type);
            let result = match check_type {
                HealthCheckType::Docker => self.check_docker(),
                HealthCheckType::Kafka => self.check_kafka().await,
                HealthCheckType::Topics => self.check_topics().await,
                HealthCheckType::Processes => self.check_processes(),
                HealthCheckType::ConsumerGroups => self.check_consumer_groups().await,
            };
            log::debug!("Health check {} completed: {:?}", check_type, result.status);
            results.push(result);
        }

        HealthReport::new(self.config.bootstrap_servers.clone(), results)
    }

    /// Check Docker containers.
    ///
    /// Looks for containers with the `velostream.test.reusable` label and
    /// containers matching the configured container names.
    fn check_docker(&self) -> CheckResult {
        // Check if Docker is available
        let docker_check = Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output();

        match docker_check {
            Ok(output) if output.status.success() => {
                let mut containers = Vec::new();
                let mut all_healthy = true;

                // Check for velostream test containers (labeled)
                if let Some(labeled) = self.find_labeled_containers() {
                    for container in labeled {
                        if !container.is_healthy {
                            all_healthy = false;
                        }
                        containers.push(container);
                    }
                }

                // Check for named containers
                for name in &self.config.container_names {
                    if let Some(named) = self.find_named_container(name) {
                        // Avoid duplicates
                        if !containers.iter().any(|c| c.id == named.id) {
                            if !named.is_healthy {
                                all_healthy = false;
                            }
                            containers.push(named);
                        }
                    }
                }

                if containers.is_empty() {
                    CheckResult::warning(HealthCheckType::Docker, "No Velostream containers found")
                        .with_details(vec![
                            "No containers with velostream.test.reusable label".to_string(),
                            format!(
                                "No containers named: {}",
                                self.config.container_names.join(", ")
                            ),
                        ])
                } else if all_healthy {
                    CheckResult::healthy(
                        HealthCheckType::Docker,
                        format!("{} container(s) running", containers.len()),
                    )
                    .with_detail(format!("Found {} container(s)", containers.len()))
                    .with_containers(containers)
                } else {
                    let running = containers.iter().filter(|c| c.is_healthy).count();
                    CheckResult::unhealthy(
                        HealthCheckType::Docker,
                        format!("{}/{} containers running", running, containers.len()),
                    )
                    .with_containers(containers)
                }
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                log::warn!("Docker command failed: {}", stderr);
                CheckResult::unhealthy(HealthCheckType::Docker, "Docker daemon not responding")
            }
            Err(e) => {
                log::warn!("Docker not available: {}", e);
                CheckResult::unhealthy(
                    HealthCheckType::Docker,
                    format!("Docker not available: {}", e),
                )
            }
        }
    }

    /// Find containers with the velostream.test.reusable label.
    fn find_labeled_containers(&self) -> Option<Vec<ContainerInfo>> {
        let output = Command::new("docker")
            .args([
                "ps",
                "-a",
                "--filter",
                "label=velostream.test.reusable",
                "--format",
                "{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}",
            ])
            .output()
            .ok()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let containers: Vec<ContainerInfo> = stdout
            .lines()
            .filter_map(|line| Self::parse_container_line(line))
            .collect();

        if containers.is_empty() {
            None
        } else {
            Some(containers)
        }
    }

    /// Find a container by name.
    fn find_named_container(&self, name: &str) -> Option<ContainerInfo> {
        let output = Command::new("docker")
            .args([
                "ps",
                "-a",
                "--filter",
                &format!("name={}", name),
                "--format",
                "{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}",
            ])
            .output()
            .ok()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout.lines().next().and_then(Self::parse_container_line)
    }

    /// Parse a container info line from docker ps output.
    fn parse_container_line(line: &str) -> Option<ContainerInfo> {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 4 {
            let is_running = parts[2].contains("Up");
            Some(ContainerInfo {
                id: parts[0].to_string(),
                name: parts[1].to_string(),
                status: parts[2].to_string(),
                image: parts[3].to_string(),
                ports: parts
                    .get(4)
                    .map(|p| vec![p.to_string()])
                    .unwrap_or_default(),
                is_healthy: is_running,
            })
        } else {
            None
        }
    }

    /// Check Kafka broker connectivity.
    async fn check_kafka(&self) -> CheckResult {
        let bootstrap_servers = match &self.config.bootstrap_servers {
            Some(bs) => bs.clone(),
            None => {
                return CheckResult::skipped(
                    HealthCheckType::Kafka,
                    "No bootstrap servers configured (use --broker)",
                );
            }
        };

        // Try to create a consumer and fetch metadata
        let consumer_result: Result<BaseConsumer, _> = create_kafka_config(&bootstrap_servers)
            .set("socket.timeout.ms", "5000")
            .set("request.timeout.ms", "5000")
            .create();

        match consumer_result {
            Ok(consumer) => match consumer.fetch_metadata(None, self.config.timeout) {
                Ok(metadata) => {
                    let broker_count = metadata.brokers().len();
                    let topic_count = metadata.topics().len();
                    CheckResult::healthy(
                        HealthCheckType::Kafka,
                        format!(
                            "Connected to {} broker(s), {} topic(s)",
                            broker_count, topic_count
                        ),
                    )
                    .with_details(vec![
                        format!("Bootstrap servers: {}", bootstrap_servers),
                        format!("Brokers: {}", broker_count),
                        format!("Topics: {}", topic_count),
                    ])
                }
                Err(e) => {
                    log::warn!("Failed to fetch Kafka metadata: {}", e);
                    CheckResult::unhealthy(
                        HealthCheckType::Kafka,
                        format!("Failed to fetch metadata: {}", e),
                    )
                }
            },
            Err(e) => {
                log::warn!("Failed to connect to Kafka: {}", e);
                CheckResult::unhealthy(HealthCheckType::Kafka, format!("Failed to connect: {}", e))
            }
        }
    }

    /// Check Kafka topics.
    async fn check_topics(&self) -> CheckResult {
        let bootstrap_servers = match &self.config.bootstrap_servers {
            Some(bs) => bs.clone(),
            None => {
                return CheckResult::skipped(
                    HealthCheckType::Topics,
                    "No bootstrap servers configured (use --broker)",
                );
            }
        };

        let consumer_result: Result<BaseConsumer, _> = create_kafka_config(&bootstrap_servers)
            .set("group.id", "__health_checker")
            .set("enable.auto.commit", "false")
            .create();

        match consumer_result {
            Ok(consumer) => {
                match consumer.fetch_metadata(None, self.config.timeout) {
                    Ok(metadata) => {
                        let mut topics = Vec::new();
                        let mut total_messages: i64 = 0;

                        for topic_metadata in metadata.topics() {
                            let topic_name = topic_metadata.name();

                            // Skip internal topics
                            if topic_name.starts_with("__") {
                                continue;
                            }

                            // Filter by configured topics if specified
                            if !self.config.topic_names.is_empty()
                                && !self.config.topic_names.contains(&topic_name.to_string())
                            {
                                continue;
                            }

                            let partition_count = topic_metadata.partitions().len() as i32;
                            let mut message_count: i64 = 0;

                            // Get message count from watermarks
                            for partition in topic_metadata.partitions() {
                                match consumer.fetch_watermarks(
                                    topic_name,
                                    partition.id(),
                                    Duration::from_secs(2),
                                ) {
                                    Ok((low, high)) => {
                                        message_count += high - low;
                                    }
                                    Err(e) => {
                                        log::debug!(
                                            "Failed to fetch watermarks for {}:{}: {}",
                                            topic_name,
                                            partition.id(),
                                            e
                                        );
                                    }
                                }
                            }

                            total_messages += message_count;

                            topics.push(TopicHealthInfo {
                                name: topic_name.to_string(),
                                partitions: partition_count,
                                message_count,
                                is_empty: message_count == 0,
                            });
                        }

                        // Sort by name for consistent output
                        topics.sort_by(|a, b| a.name.cmp(&b.name));

                        let empty_count = topics.iter().filter(|t| t.is_empty).count();
                        let msg = format!(
                            "{} topic(s), {} total messages",
                            topics.len(),
                            total_messages
                        );

                        if topics.is_empty() {
                            CheckResult::warning(HealthCheckType::Topics, "No topics found")
                        } else if empty_count > 0 && empty_count < topics.len() {
                            CheckResult::warning(HealthCheckType::Topics, msg)
                                .with_detail(format!("{} topic(s) are empty", empty_count))
                                .with_topics(topics)
                        } else {
                            CheckResult::healthy(HealthCheckType::Topics, msg).with_topics(topics)
                        }
                    }
                    Err(e) => {
                        log::warn!("Failed to fetch topic metadata: {}", e);
                        CheckResult::unhealthy(
                            HealthCheckType::Topics,
                            format!("Failed to fetch topic metadata: {}", e),
                        )
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to create Kafka consumer: {}", e);
                CheckResult::unhealthy(
                    HealthCheckType::Topics,
                    format!("Failed to create consumer: {}", e),
                )
            }
        }
    }

    /// Check running processes.
    fn check_processes(&self) -> CheckResult {
        let mut processes = Vec::new();
        let mut any_running = false;

        for pattern in &self.config.process_patterns {
            match self.find_processes_by_pattern(pattern) {
                Ok(found) => {
                    if found.is_empty() {
                        processes.push(ProcessInfo {
                            name: pattern.clone(),
                            pid: 0,
                            command: String::new(),
                            is_running: false,
                        });
                    } else {
                        any_running = true;
                        processes.extend(found);
                    }
                }
                Err(e) => {
                    log::debug!("Failed to search for process '{}': {}", pattern, e);
                    processes.push(ProcessInfo {
                        name: pattern.clone(),
                        pid: 0,
                        command: String::new(),
                        is_running: false,
                    });
                }
            }
        }

        if any_running {
            let running_count = processes.iter().filter(|p| p.is_running).count();
            CheckResult::healthy(
                HealthCheckType::Processes,
                format!("{} process(es) running", running_count),
            )
            .with_processes(processes)
        } else {
            CheckResult::warning(
                HealthCheckType::Processes,
                "No Velostream processes running",
            )
            .with_details(vec![
                "velo-test and velo-sql are not running".to_string(),
                "This may be expected if the demo is not started".to_string(),
            ])
            .with_processes(processes)
        }
    }

    /// Find processes matching a pattern using pgrep.
    fn find_processes_by_pattern(&self, pattern: &str) -> TestHarnessResult<Vec<ProcessInfo>> {
        let output = Command::new("pgrep")
            .args(["-f", pattern])
            .output()
            .map_err(|e| super::error::TestHarnessError::InfraError {
                message: format!("Failed to run pgrep: {}", e),
                source: Some(e.to_string()),
            })?;

        if !output.status.success() {
            // pgrep returns non-zero when no processes found - this is not an error
            return Ok(Vec::new());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut processes = Vec::new();

        for pid_str in stdout.lines() {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                let command = Self::get_process_command(pid);
                processes.push(ProcessInfo {
                    name: pattern.to_string(),
                    pid,
                    command,
                    is_running: true,
                });
            }
        }

        Ok(processes)
    }

    /// Get the command line for a process by PID.
    fn get_process_command(pid: u32) -> String {
        #[cfg(target_os = "macos")]
        {
            Command::new("ps")
                .args(["-p", &pid.to_string(), "-o", "command="])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .unwrap_or_default()
        }

        #[cfg(not(target_os = "macos"))]
        {
            std::fs::read_to_string(format!("/proc/{}/cmdline", pid))
                .ok()
                .map(|s| s.replace('\0', " ").trim().to_string())
                .unwrap_or_default()
        }
    }

    /// Check Kafka consumer groups.
    async fn check_consumer_groups(&self) -> CheckResult {
        let bootstrap_servers = match &self.config.bootstrap_servers {
            Some(bs) => bs.clone(),
            None => {
                return CheckResult::skipped(
                    HealthCheckType::ConsumerGroups,
                    "No bootstrap servers configured (use --broker)",
                );
            }
        };

        let consumer_result: Result<BaseConsumer, _> = create_kafka_config(&bootstrap_servers)
            .set("socket.timeout.ms", "5000")
            .set("request.timeout.ms", "5000")
            .create();

        match consumer_result {
            Ok(consumer) => {
                match consumer.fetch_group_list(None, self.config.timeout) {
                    Ok(group_list) => {
                        let mut groups = Vec::new();

                        for group in group_list.groups() {
                            let name = group.name().to_string();

                            // Filter to velostream groups (both dash and underscore formats)
                            if name.starts_with("velo-sql")
                                || name.starts_with("velo-test")
                                || name.starts_with("velo_sql")
                                || name.starts_with("velo_")
                            {
                                groups.push(ConsumerGroupInfo {
                                    name,
                                    state: group.state().to_string(),
                                    members: group.members().len() as i32,
                                    total_lag: 0, // Note: Computing lag requires additional API calls
                                });
                            }
                        }

                        if groups.is_empty() {
                            CheckResult::warning(
                                HealthCheckType::ConsumerGroups,
                                "No Velostream consumer groups found",
                            )
                            .with_detail(
                                "No groups starting with 'velo-sql', 'velo-test', or 'velo_'",
                            )
                        } else {
                            CheckResult::healthy(
                                HealthCheckType::ConsumerGroups,
                                format!("{} consumer group(s) found", groups.len()),
                            )
                            .with_consumer_groups(groups)
                        }
                    }
                    Err(e) => {
                        log::warn!("Failed to list consumer groups: {}", e);
                        CheckResult::unhealthy(
                            HealthCheckType::ConsumerGroups,
                            format!("Failed to list consumer groups: {}", e),
                        )
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to create Kafka consumer: {}", e);
                CheckResult::unhealthy(
                    HealthCheckType::ConsumerGroups,
                    format!("Failed to create consumer: {}", e),
                )
            }
        }
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------------
    // HealthCheckType Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_health_check_type_parse_valid() {
        let types = HealthCheckType::parse_list("docker,kafka,topics");
        assert_eq!(types.len(), 3);
        assert!(types.contains(&HealthCheckType::Docker));
        assert!(types.contains(&HealthCheckType::Kafka));
        assert!(types.contains(&HealthCheckType::Topics));
    }

    #[test]
    fn test_health_check_type_parse_with_spaces() {
        let types = HealthCheckType::parse_list("docker , kafka , topics");
        assert_eq!(types.len(), 3);
    }

    #[test]
    fn test_health_check_type_parse_case_insensitive() {
        let types = HealthCheckType::parse_list("DOCKER,Kafka,TOPICS");
        assert_eq!(types.len(), 3);
    }

    #[test]
    fn test_health_check_type_parse_consumer_groups_variants() {
        assert_eq!(
            HealthCheckType::parse_list("consumer_groups"),
            vec![HealthCheckType::ConsumerGroups]
        );
        assert_eq!(
            HealthCheckType::parse_list("consumergroups"),
            vec![HealthCheckType::ConsumerGroups]
        );
        assert_eq!(
            HealthCheckType::parse_list("consumers"),
            vec![HealthCheckType::ConsumerGroups]
        );
    }

    #[test]
    fn test_health_check_type_parse_empty() {
        let types = HealthCheckType::parse_list("");
        assert!(types.is_empty());
    }

    #[test]
    fn test_health_check_type_parse_invalid_ignored() {
        let types = HealthCheckType::parse_list("docker,invalid,kafka");
        assert_eq!(types.len(), 2);
        assert!(types.contains(&HealthCheckType::Docker));
        assert!(types.contains(&HealthCheckType::Kafka));
    }

    #[test]
    fn test_health_check_type_all() {
        let all = HealthCheckType::all();
        assert_eq!(all.len(), 5);
        assert!(all.contains(&HealthCheckType::Docker));
        assert!(all.contains(&HealthCheckType::Kafka));
        assert!(all.contains(&HealthCheckType::Topics));
        assert!(all.contains(&HealthCheckType::Processes));
        assert!(all.contains(&HealthCheckType::ConsumerGroups));
    }

    #[test]
    fn test_health_check_type_as_str() {
        assert_eq!(HealthCheckType::Docker.as_str(), "docker");
        assert_eq!(HealthCheckType::Kafka.as_str(), "kafka");
        assert_eq!(HealthCheckType::Topics.as_str(), "topics");
        assert_eq!(HealthCheckType::Processes.as_str(), "processes");
        assert_eq!(HealthCheckType::ConsumerGroups.as_str(), "consumer_groups");
    }

    #[test]
    fn test_health_check_type_display() {
        assert_eq!(format!("{}", HealthCheckType::Docker), "Docker Containers");
        assert_eq!(format!("{}", HealthCheckType::Kafka), "Kafka Broker");
        assert_eq!(format!("{}", HealthCheckType::Topics), "Topics");
        assert_eq!(format!("{}", HealthCheckType::Processes), "Processes");
        assert_eq!(
            format!("{}", HealthCheckType::ConsumerGroups),
            "Consumer Groups"
        );
    }

    // ------------------------------------------------------------------------
    // CheckStatus Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_check_status_is_ok() {
        assert!(CheckStatus::Healthy.is_ok());
        assert!(CheckStatus::Warning.is_ok());
        assert!(CheckStatus::Skipped.is_ok());
        assert!(!CheckStatus::Unhealthy.is_ok());
    }

    #[test]
    fn test_check_status_display() {
        assert_eq!(format!("{}", CheckStatus::Healthy), "healthy");
        assert_eq!(format!("{}", CheckStatus::Warning), "warning");
        assert_eq!(format!("{}", CheckStatus::Unhealthy), "unhealthy");
        assert_eq!(format!("{}", CheckStatus::Skipped), "skipped");
    }

    // ------------------------------------------------------------------------
    // CheckResult Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_check_result_constructors() {
        let healthy = CheckResult::healthy(HealthCheckType::Docker, "OK");
        assert_eq!(healthy.status, CheckStatus::Healthy);
        assert_eq!(healthy.message, "OK");

        let warning = CheckResult::warning(HealthCheckType::Kafka, "Warning");
        assert_eq!(warning.status, CheckStatus::Warning);

        let unhealthy = CheckResult::unhealthy(HealthCheckType::Topics, "Failed");
        assert_eq!(unhealthy.status, CheckStatus::Unhealthy);

        let skipped = CheckResult::skipped(HealthCheckType::Processes, "Skipped");
        assert_eq!(skipped.status, CheckStatus::Skipped);
    }

    #[test]
    fn test_check_result_with_details() {
        let result = CheckResult::healthy(HealthCheckType::Docker, "OK")
            .with_details(vec!["Detail 1".to_string(), "Detail 2".to_string()]);
        assert_eq!(result.details.len(), 2);
    }

    #[test]
    fn test_check_result_with_detail() {
        let result =
            CheckResult::healthy(HealthCheckType::Docker, "OK").with_detail("Single detail");
        assert_eq!(result.details.len(), 1);
        assert_eq!(result.details[0], "Single detail");
    }

    #[test]
    fn test_check_result_with_containers() {
        let containers = vec![ContainerInfo {
            name: "test".to_string(),
            id: "abc123".to_string(),
            status: "Up".to_string(),
            image: "test:latest".to_string(),
            ports: vec![],
            is_healthy: true,
        }];
        let result =
            CheckResult::healthy(HealthCheckType::Docker, "OK").with_containers(containers);
        assert!(result.containers.is_some());
        assert_eq!(result.containers.unwrap().len(), 1);
    }

    // ------------------------------------------------------------------------
    // HealthReport Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_health_report_all_healthy() {
        let checks = vec![
            CheckResult::healthy(HealthCheckType::Docker, "OK"),
            CheckResult::healthy(HealthCheckType::Kafka, "OK"),
        ];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);

        assert_eq!(report.overall_status, CheckStatus::Healthy);
        assert!(report.is_healthy());
        assert_eq!(report.summary.total_checks, 2);
        assert_eq!(report.summary.healthy, 2);
        assert_eq!(report.summary.warnings, 0);
        assert_eq!(report.summary.unhealthy, 0);
    }

    #[test]
    fn test_health_report_with_warning() {
        let checks = vec![
            CheckResult::healthy(HealthCheckType::Docker, "OK"),
            CheckResult::warning(HealthCheckType::Topics, "Some empty"),
        ];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);

        assert_eq!(report.overall_status, CheckStatus::Warning);
        assert!(report.is_healthy()); // Warnings are still considered healthy
        assert_eq!(report.summary.warnings, 1);
    }

    #[test]
    fn test_health_report_with_unhealthy() {
        let checks = vec![
            CheckResult::healthy(HealthCheckType::Docker, "OK"),
            CheckResult::unhealthy(HealthCheckType::Kafka, "Failed"),
        ];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);

        assert_eq!(report.overall_status, CheckStatus::Unhealthy);
        assert!(!report.is_healthy());
        assert_eq!(report.summary.unhealthy, 1);
    }

    #[test]
    fn test_health_report_unhealthy_overrides_warning() {
        let checks = vec![
            CheckResult::warning(HealthCheckType::Docker, "Warning"),
            CheckResult::unhealthy(HealthCheckType::Kafka, "Failed"),
        ];
        let report = HealthReport::new(None, checks);

        assert_eq!(report.overall_status, CheckStatus::Unhealthy);
    }

    #[test]
    fn test_health_report_format_text_contains_expected_content() {
        let checks = vec![CheckResult::healthy(
            HealthCheckType::Docker,
            "1 container running",
        )];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);
        let text = report.format_text();

        assert!(text.contains("Velostream Health Check Report"));
        assert!(text.contains("Kafka: localhost:9092"));
        assert!(text.contains("Docker Containers"));
        assert!(text.contains("1 container running"));
        assert!(text.contains("HEALTHY"));
    }

    #[test]
    fn test_health_report_format_json_valid() {
        let checks = vec![CheckResult::healthy(HealthCheckType::Docker, "OK")];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);
        let json = report.format_json();

        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["overall_status"], "healthy");
        assert_eq!(parsed["bootstrap_servers"], "localhost:9092");
    }

    #[test]
    fn test_health_report_serialization_roundtrip() {
        let checks = vec![
            CheckResult::healthy(HealthCheckType::Docker, "OK").with_containers(vec![
                ContainerInfo {
                    name: "test".to_string(),
                    id: "abc".to_string(),
                    status: "Up".to_string(),
                    image: "img".to_string(),
                    ports: vec![],
                    is_healthy: true,
                },
            ]),
        ];
        let report = HealthReport::new(Some("localhost:9092".to_string()), checks);

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: HealthReport = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.overall_status, report.overall_status);
        assert_eq!(deserialized.bootstrap_servers, report.bootstrap_servers);
        assert_eq!(deserialized.summary, report.summary);
    }

    // ------------------------------------------------------------------------
    // HealthConfig Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_health_config_default() {
        let config = HealthConfig::default();

        assert!(config.bootstrap_servers.is_none());
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert!(config.container_names.contains(&"simple-kafka".to_string()));
        assert!(
            config
                .container_names
                .contains(&"simple-zookeeper".to_string())
        );
        assert!(config.process_patterns.contains(&"velo-test".to_string()));
        assert!(config.process_patterns.contains(&"velo-sql".to_string()));
        assert!(config.topic_names.is_empty());
    }

    // ------------------------------------------------------------------------
    // HealthChecker Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_health_checker_builder() {
        let checker = HealthChecker::new()
            .with_bootstrap_servers("localhost:9092")
            .with_timeout(Duration::from_secs(30));

        assert_eq!(
            checker.config.bootstrap_servers,
            Some("localhost:9092".to_string())
        );
        assert_eq!(checker.config.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_health_checker_with_config() {
        let config = HealthConfig {
            bootstrap_servers: Some("custom:9092".to_string()),
            timeout: Duration::from_secs(5),
            container_names: vec!["custom-container".to_string()],
            topic_names: vec!["custom-topic".to_string()],
            process_patterns: vec!["custom-process".to_string()],
        };
        let checker = HealthChecker::with_config(config);

        assert_eq!(
            checker.config.bootstrap_servers,
            Some("custom:9092".to_string())
        );
        assert_eq!(checker.config.container_names, vec!["custom-container"]);
    }
}
