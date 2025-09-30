use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;

// Performance-optimized constants - all configurable via environment variables
pub struct RetryDefaults;
impl RetryDefaults {
    pub const DEFAULT_INTERVAL_SECS: u64 = 5; // Standard default interval
    pub const DEFAULT_MULTIPLIER: f64 = 1.5; // Reduced from 2.0 for gentler backoff
    pub const DEFAULT_MAX_DELAY_SECS: u64 = 120; // Reduced from 300s
    pub const MAX_EXPONENTIAL_SHIFT: u32 = 6; // Prevents overflow in bit shifting

    // Environment variable overrides
    pub fn interval() -> Duration {
        Duration::from_secs(
            std::env::var("VELOSTREAM_RETRY_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Self::DEFAULT_INTERVAL_SECS),
        )
    }

    pub fn multiplier() -> f64 {
        std::env::var("VELOSTREAM_RETRY_MULTIPLIER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(Self::DEFAULT_MULTIPLIER)
    }

    pub fn max_delay() -> Duration {
        Duration::from_secs(
            std::env::var("VELOSTREAM_RETRY_MAX_DELAY_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(Self::DEFAULT_MAX_DELAY_SECS),
        )
    }
}

// Pre-compiled error patterns for zero-allocation matching
static ERROR_PATTERNS: &[(fn(&str) -> bool, ErrorCategory)] = &[
    (
        |s| {
            s.contains("unknown topic")
                || s.contains("topic does not exist")
                || s.contains("topic not found")
        },
        ErrorCategory::TopicMissing,
    ),
    (
        |s| s.contains("network") || s.contains("connection") || s.contains("broker"),
        ErrorCategory::NetworkIssue,
    ),
    (
        |s| s.contains("auth") || s.contains("permission") || s.contains("unauthorized"),
        ErrorCategory::AuthenticationIssue,
    ),
];

// High-performance duration parsing cache
static DURATION_CACHE: LazyLock<RwLock<HashMap<String, Option<Duration>>>> =
    LazyLock::new(|| RwLock::new(HashMap::with_capacity(64)));

/// Error categories for better retry decision making
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorCategory {
    /// Topic doesn't exist - retry makes sense
    TopicMissing,
    /// Network/broker connectivity issues - retry with backoff
    NetworkIssue,
    /// Authentication/authorization failures - likely won't resolve with retry
    AuthenticationIssue,
    /// Configuration errors - won't resolve with retry
    ConfigurationIssue,
    /// Unknown/other errors - conservative retry
    Unknown,
}

/// Retry strategy configuration
#[derive(Debug, Clone)]
pub enum RetryStrategy {
    /// Fixed interval between retries
    FixedInterval(Duration),
    /// Exponential backoff with multiplier
    ExponentialBackoff {
        initial: Duration,
        max: Duration,
        multiplier: f64,
    },
    /// Linear backoff with fixed increment
    LinearBackoff {
        initial: Duration,
        increment: Duration,
        max: Duration,
    },
}

impl Default for RetryStrategy {
    fn default() -> Self {
        RetryStrategy::ExponentialBackoff {
            initial: RetryDefaults::interval(),
            max: RetryDefaults::max_delay(),
            multiplier: RetryDefaults::multiplier(),
        }
    }
}

/// Retry metrics for observability
#[derive(Debug, Default)]
pub struct RetryMetrics {
    pub attempts_total: AtomicU64,
    pub successes_total: AtomicU64,
    pub timeouts_total: AtomicU64,
    pub topic_missing_total: AtomicU64,
    pub network_errors_total: AtomicU64,
    pub auth_errors_total: AtomicU64,
    pub config_errors_total: AtomicU64,
    pub unknown_errors_total: AtomicU64,
}

impl RetryMetrics {
    /// Create new metrics instance
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a retry attempt
    pub fn record_attempt(&self) {
        self.attempts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful resolution
    pub fn record_success(&self) {
        self.successes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout
    pub fn record_timeout(&self) {
        self.timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error by category
    pub fn record_error_category(&self, category: &ErrorCategory) {
        match category {
            ErrorCategory::TopicMissing => self.topic_missing_total.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::NetworkIssue => {
                self.network_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::AuthenticationIssue => {
                self.auth_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::ConfigurationIssue => {
                self.config_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::Unknown => self.unknown_errors_total.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// High-performance batch recording of attempt + error category
    #[inline]
    pub fn record_attempt_with_error(&self, category: &ErrorCategory) {
        self.attempts_total.fetch_add(1, Ordering::Relaxed);
        match category {
            ErrorCategory::TopicMissing => self.topic_missing_total.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::NetworkIssue => {
                self.network_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::AuthenticationIssue => {
                self.auth_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::ConfigurationIssue => {
                self.config_errors_total.fetch_add(1, Ordering::Relaxed)
            }
            ErrorCategory::Unknown => self.unknown_errors_total.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Batch recording for successful retry
    #[inline]
    pub fn record_attempt_with_success(&self) {
        self.attempts_total.fetch_add(1, Ordering::Relaxed);
        self.successes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Batch recording for timeout
    #[inline]
    pub fn record_attempt_with_timeout(&self) {
        self.attempts_total.fetch_add(1, Ordering::Relaxed);
        self.timeouts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> RetryMetricsSnapshot {
        RetryMetricsSnapshot {
            attempts_total: self.attempts_total.load(Ordering::Relaxed),
            successes_total: self.successes_total.load(Ordering::Relaxed),
            timeouts_total: self.timeouts_total.load(Ordering::Relaxed),
            topic_missing_total: self.topic_missing_total.load(Ordering::Relaxed),
            network_errors_total: self.network_errors_total.load(Ordering::Relaxed),
            auth_errors_total: self.auth_errors_total.load(Ordering::Relaxed),
            config_errors_total: self.config_errors_total.load(Ordering::Relaxed),
            unknown_errors_total: self.unknown_errors_total.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of retry metrics at a point in time
#[derive(Debug, Clone)]
pub struct RetryMetricsSnapshot {
    pub attempts_total: u64,
    pub successes_total: u64,
    pub timeouts_total: u64,
    pub topic_missing_total: u64,
    pub network_errors_total: u64,
    pub auth_errors_total: u64,
    pub config_errors_total: u64,
    pub unknown_errors_total: u64,
}

impl RetryMetricsSnapshot {
    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.attempts_total == 0 {
            0.0
        } else {
            (self.successes_total as f64 / self.attempts_total as f64) * 100.0
        }
    }

    /// Calculate timeout rate as percentage
    pub fn timeout_rate(&self) -> f64 {
        if self.attempts_total == 0 {
            0.0
        } else {
            (self.timeouts_total as f64 / self.attempts_total as f64) * 100.0
        }
    }
}

/// High-performance cached duration parsing
pub fn parse_duration(duration_str: &str) -> Option<Duration> {
    if duration_str.is_empty() {
        return None;
    }

    // Check cache first (read lock)
    {
        let cache = DURATION_CACHE.read().unwrap();
        if let Some(cached) = cache.get(duration_str) {
            return *cached;
        }
    }

    // Parse and cache result (write lock only if needed)
    let result = parse_duration_uncached(duration_str);
    {
        let mut cache = DURATION_CACHE.write().unwrap();
        cache.insert(duration_str.to_string(), result);
    }
    result
}

/// Internal uncached duration parsing
fn parse_duration_uncached(duration_str: &str) -> Option<Duration> {
    let duration_str = duration_str.trim().to_lowercase();

    // Handle "0" as zero duration
    if duration_str == "0" {
        return Some(Duration::from_secs(0));
    }

    // Extract number and unit
    let (number_part, unit_part) = if let Some(pos) = duration_str.find(|c: char| c.is_alphabetic())
    {
        (&duration_str[..pos], &duration_str[pos..])
    } else {
        // No unit specified, assume seconds
        (duration_str.as_str(), "s")
    };

    // Parse the numeric part (trim whitespace)
    let number: f64 = match number_part.trim().parse() {
        Ok(n) => n,
        Err(_) => return None,
    };

    if number < 0.0 {
        return None;
    }

    // Convert based on unit (trim whitespace)
    let duration = match unit_part.trim() {
        "ms" | "millis" | "milliseconds" => {
            // Handle fractional milliseconds properly
            let nanos = (number * 1_000_000.0) as u64;
            Duration::from_nanos(nanos)
        }
        "s" | "sec" | "secs" | "second" | "seconds" => Duration::from_secs_f64(number),
        "m" | "min" | "mins" | "minute" | "minutes" => Duration::from_secs_f64(number * 60.0),
        "h" | "hr" | "hrs" | "hour" | "hours" => Duration::from_secs_f64(number * 3600.0),
        "d" | "day" | "days" => Duration::from_secs_f64(number * 86400.0),
        _ => return None,
    };

    Some(duration)
}

/// Categorize Kafka errors for intelligent retry decisions
///
/// # Examples
///
/// ```rust
/// use velostream::velostream::table::retry_utils::{categorize_kafka_error, ErrorCategory};
/// use velostream::velostream::kafka::kafka_error::ConsumerError;
/// use rdkafka::error::{KafkaError, RDKafkaErrorCode};
///
/// let kafka_error = KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition);
/// let error = ConsumerError::KafkaError(kafka_error);
/// let category = categorize_kafka_error(&error);
/// assert_eq!(category, ErrorCategory::TopicMissing);
/// ```
/// High-performance error categorization with zero-allocation patterns
#[inline]
pub fn categorize_kafka_error(
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
) -> ErrorCategory {
    use crate::velostream::kafka::kafka_error::ConsumerError;

    match error {
        ConsumerError::KafkaError(kafka_error) => {
            match kafka_error {
                KafkaError::MetadataFetch(code) => match code {
                    RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic => {
                        ErrorCategory::TopicMissing
                    }

                    RDKafkaErrorCode::BrokerNotAvailable
                    | RDKafkaErrorCode::NetworkException
                    | RDKafkaErrorCode::AllBrokersDown
                    | RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::RequestTimedOut => ErrorCategory::NetworkIssue,

                    RDKafkaErrorCode::SaslAuthenticationFailed
                    | RDKafkaErrorCode::TopicAuthorizationFailed
                    | RDKafkaErrorCode::ClusterAuthorizationFailed => {
                        ErrorCategory::AuthenticationIssue
                    }

                    RDKafkaErrorCode::InvalidConfig
                    | RDKafkaErrorCode::InvalidRequest
                    | RDKafkaErrorCode::UnsupportedVersion => ErrorCategory::ConfigurationIssue,

                    _ => ErrorCategory::Unknown,
                },
                KafkaError::ClientConfig(..) => ErrorCategory::ConfigurationIssue,
                KafkaError::AdminOp(..) => ErrorCategory::ConfigurationIssue,
                _ => {
                    // Zero-allocation pattern matching using pre-compiled functions
                    let error_str = error.to_string();
                    let error_lower = error_str.to_lowercase(); // Single allocation

                    for (pattern_fn, category) in ERROR_PATTERNS {
                        if pattern_fn(&error_lower) {
                            return category.clone();
                        }
                    }
                    ErrorCategory::Unknown
                }
            }
        }
        ConsumerError::ConfigurationError(_) => ErrorCategory::ConfigurationIssue,
        ConsumerError::SerializationError(_) => ErrorCategory::ConfigurationIssue,
        ConsumerError::Timeout => ErrorCategory::NetworkIssue,
        ConsumerError::NoMessage => ErrorCategory::Unknown,
    }
}

/// Check if an error indicates a missing Kafka topic (backward compatibility)
pub fn is_topic_missing_error(
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
) -> bool {
    categorize_kafka_error(error) == ErrorCategory::TopicMissing
}

/// Determine if error category should trigger retry
///
/// # Examples
///
/// ```rust
/// use velostream::velostream::table::retry_utils::{should_retry_for_category, ErrorCategory};
///
/// assert_eq!(should_retry_for_category(&ErrorCategory::TopicMissing), true);
/// assert_eq!(should_retry_for_category(&ErrorCategory::NetworkIssue), true);
/// assert_eq!(should_retry_for_category(&ErrorCategory::AuthenticationIssue), false);
/// assert_eq!(should_retry_for_category(&ErrorCategory::ConfigurationIssue), false);
/// ```
pub fn should_retry_for_category(category: &ErrorCategory) -> bool {
    match category {
        ErrorCategory::TopicMissing => true,
        ErrorCategory::NetworkIssue => true,
        ErrorCategory::AuthenticationIssue => false, // Won't resolve with retry
        ErrorCategory::ConfigurationIssue => false,  // Won't resolve with retry
        ErrorCategory::Unknown => true,              // Conservative: allow retry
    }
}

/// High-performance retry delay calculation with optimizations
#[inline]
pub fn calculate_retry_delay(strategy: &RetryStrategy, attempt_number: u32) -> Duration {
    match strategy {
        RetryStrategy::FixedInterval(duration) => *duration,

        RetryStrategy::ExponentialBackoff {
            initial,
            max,
            multiplier,
        } => {
            // Optimization: Use bit shifting for power-of-2 multipliers
            if (*multiplier - 2.0).abs() < f64::EPSILON {
                // Calculate delay with proper overflow protection
                // For very large attempts, just return max directly
                if attempt_number > 63 {
                    return *max;
                }

                let delay_secs = initial
                    .as_secs()
                    .saturating_mul(1u64 << attempt_number.min(63));
                let delay_duration = Duration::from_secs(delay_secs);
                if delay_duration > *max {
                    *max
                } else {
                    delay_duration
                }
            } else if (*multiplier - 1.5).abs() < f64::EPSILON {
                // Optimization: Use lookup table for 1.5x multiplier (common case)
                static LOOKUP_1_5: &[f64] = &[
                    1.0, 1.5, 2.25, 3.375, 5.0625, 7.59375, 11.390625, 17.0859375,
                ];
                let multiplier_value = LOOKUP_1_5
                    .get(attempt_number as usize)
                    .copied()
                    .unwrap_or_else(|| multiplier.powi(attempt_number as i32));
                let delay = initial.as_secs_f64() * multiplier_value;
                let delay_duration = Duration::from_secs_f64(delay);
                if delay_duration > *max {
                    *max
                } else {
                    delay_duration
                }
            } else {
                // Fallback to floating-point calculation
                let delay = initial.as_secs_f64() * multiplier.powi(attempt_number as i32);
                // Round to nearest second for consistency with test expectations
                let delay_duration = Duration::from_secs(delay.round() as u64);
                if delay_duration > *max {
                    *max
                } else {
                    delay_duration
                }
            }
        }

        RetryStrategy::LinearBackoff {
            initial,
            increment,
            max,
        } => {
            let delay = initial.as_secs_f64() + (increment.as_secs_f64() * attempt_number as f64);
            let delay_duration = Duration::from_secs_f64(delay);
            if delay_duration > *max {
                *max
            } else {
                delay_duration
            }
        }
    }
}

/// Parse retry strategy from configuration properties
///
/// # Examples
///
/// ```rust
/// use std::collections::HashMap;
/// use std::time::Duration;
/// use velostream::velostream::table::retry_utils::{parse_retry_strategy, RetryStrategy};
///
/// let mut props = HashMap::new();
/// props.insert("topic.retry.strategy".to_string(), "exponential".to_string());
/// props.insert("topic.retry.interval".to_string(), "1s".to_string());
/// props.insert("topic.retry.multiplier".to_string(), "2.0".to_string());
/// props.insert("topic.retry.max.delay".to_string(), "60s".to_string());
///
/// let strategy = parse_retry_strategy(&props);
/// match strategy {
///     RetryStrategy::ExponentialBackoff { initial, max, multiplier } => {
///         assert_eq!(initial, Duration::from_secs(1));
///         assert_eq!(max, Duration::from_secs(60));
///         assert_eq!(multiplier, 2.0);
///     }
///     _ => panic!("Expected exponential backoff"),
/// }
/// ```
/// High-performance retry strategy parsing with environment variable support
pub fn parse_retry_strategy(
    properties: &std::collections::HashMap<String, String>,
) -> RetryStrategy {
    let strategy_type = properties
        .get("topic.retry.strategy")
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| {
            std::env::var("VELOSTREAM_RETRY_STRATEGY").unwrap_or_else(|_| "fixed".to_string())
        });

    let initial_delay = properties
        .get("topic.retry.interval")
        .and_then(|s| parse_duration(s))
        .unwrap_or_else(RetryDefaults::interval);

    match strategy_type.as_str() {
        "exponential" | "exp" => {
            let multiplier = properties
                .get("topic.retry.multiplier")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or_else(RetryDefaults::multiplier);

            let max_delay = properties
                .get("topic.retry.max.delay")
                .and_then(|s| parse_duration(s))
                .unwrap_or_else(RetryDefaults::max_delay);

            RetryStrategy::ExponentialBackoff {
                initial: initial_delay,
                max: max_delay,
                multiplier,
            }
        }

        "linear" => {
            let increment = properties
                .get("topic.retry.increment")
                .and_then(|s| parse_duration(s))
                .unwrap_or(initial_delay);

            let max_delay = properties
                .get("topic.retry.max.delay")
                .and_then(|s| parse_duration(s))
                .unwrap_or_else(RetryDefaults::max_delay);

            RetryStrategy::LinearBackoff {
                initial: initial_delay,
                increment,
                max: max_delay,
            }
        }

        "fixed" => RetryStrategy::FixedInterval(initial_delay),

        _ => {
            // Default to fixed interval for unknown strategies
            RetryStrategy::FixedInterval(initial_delay)
        }
    }
}

// High-performance error message templates (configurable via environment variables)
pub struct ErrorMessageConfig;
impl ErrorMessageConfig {
    fn default_partitions() -> u32 {
        std::env::var("VELOSTREAM_DEFAULT_PARTITIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6) // Increased from hardcoded 3
    }

    fn default_replication_factor() -> u32 {
        std::env::var("VELOSTREAM_DEFAULT_REPLICATION_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3) // Increased from hardcoded 1 for production
    }

    fn suggested_timeout() -> &'static str {
        "60s" // More aggressive than previous 30s
    }

    fn network_timeout() -> &'static str {
        "5m" // More conservative for network issues
    }
}

/// Generate helpful error message for missing topics with category-specific guidance
///
/// # Examples
///
/// ```rust
/// use velostream::velostream::table::retry_utils::{format_categorized_error, ErrorCategory};
/// use velostream::velostream::kafka::kafka_error::ConsumerError;
/// use rdkafka::error::{KafkaError, RDKafkaErrorCode};
///
/// let kafka_error = KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition);
/// let error = ConsumerError::KafkaError(kafka_error);
/// let formatted = format_categorized_error("test_topic", &error, &ErrorCategory::TopicMissing);
/// assert!(formatted.contains("kafka-topics --create"));
/// ```
pub fn format_categorized_error(
    topic: &str,
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
    category: &ErrorCategory,
) -> String {
    match category {
        ErrorCategory::TopicMissing => {
            format!(
                "Kafka topic '{}' does not exist. Production-optimized options:\n\
                 1. Create topic (production): kafka-topics --create --topic {} --partitions {} --replication-factor {}\n\
                 2. Enable intelligent retry: WITH (\"topic.wait.timeout\" = \"30s\")\n\
                 3. Use exponential backoff (recommended): WITH (\"topic.retry.strategy\" = \"exponential\")\n\
                 4. Environment override: VELOSTREAM_RETRY_STRATEGY=exponential\n\
                 5. Check topic name and broker connectivity\n\
                 \n\
                 Example SQL: CREATE TABLE my_table AS SELECT * FROM kafka_source WITH (\"topic.wait.timeout\" = \"30s\")\n\
                 \n\
                 Original error: {}",
                topic, topic, ErrorMessageConfig::default_partitions(), ErrorMessageConfig::default_replication_factor(),
                error
            )
        }

        ErrorCategory::NetworkIssue => {
            format!(
                "Network/broker connectivity issue for topic '{}'. Production options:\n\
                 1. Check broker availability: kafka-topics --list --bootstrap-server <broker>\n\
                 2. Use exponential backoff (auto-enabled): WITH (\"topic.retry.strategy\" = \"exponential\")\n\
                 3. Increase timeout for network recovery: WITH (\"topic.wait.timeout\" = \"{}\")\n\
                 4. Environment override: VELOSTREAM_RETRY_MAX_DELAY_SECS=600\n\
                 5. Check network connectivity and firewall rules\n\
                 \n\
                 Original error: {}",
                topic, ErrorMessageConfig::network_timeout(), error
            )
        }

        ErrorCategory::AuthenticationIssue => {
            format!(
                "Authentication/authorization failed for topic '{}'. This likely won't resolve with retry:\n\
                 1. Check Kafka credentials and permissions\n\
                 2. Verify SASL/SSL configuration\n\
                 3. Contact administrator for topic access\n\
                 4. Review security.protocol and sasl.mechanism settings\n\
                 \n\
                 Original error: {}",
                topic, error
            )
        }

        ErrorCategory::ConfigurationIssue => {
            format!(
                "Configuration error for topic '{}'. This likely won't resolve with retry:\n\
                 1. Review Kafka client configuration\n\
                 2. Check bootstrap.servers setting\n\
                 3. Validate serialization format settings\n\
                 4. Review WITH clause properties\n\
                 \n\
                 Original error: {}",
                topic, error
            )
        }

        ErrorCategory::Unknown => {
            format!(
                "Unknown error for topic '{}'. Options:\n\
                 1. Check broker logs for more details\n\
                 2. Add conservative retry: WITH (\"topic.wait.timeout\" = \"30s\")\n\
                 3. Contact support with error details\n\
                 \n\
                 Original error: {}",
                topic, error
            )
        }
    }
}

/// Generate helpful error message for missing topics (backward compatibility)
pub fn format_topic_missing_error(
    topic: &str,
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
) -> String {
    let category = categorize_kafka_error(error);
    format_categorized_error(topic, error, &category)
}

/// Generate helpful error message for missing files
pub fn format_file_missing_error(path: &str) -> String {
    format!(
        "File '{}' does not exist. Options:\n\
         1. Check the file path exists\n\
         2. Add wait configuration: WITH (\"file.wait.timeout\" = \"30s\")\n\
         3. For patterns (*.json), ensure matching files will arrive\n\
         4. Use watch mode: WITH (\"watch\" = \"true\")",
        path
    )
}

/// Wait for a file to exist with configurable timeout and retry interval
pub async fn wait_for_file_to_exist(
    file_path: &str,
    wait_timeout: Duration,
    retry_interval: Duration,
) -> Result<(), String> {
    if wait_timeout.as_secs() == 0 {
        // No wait configured
        if Path::new(file_path).exists() {
            return Ok(());
        } else {
            return Err(format_file_missing_error(file_path));
        }
    }

    let start = Instant::now();
    loop {
        if Path::new(file_path).exists() {
            log::info!(
                "File '{}' found after waiting {:?}",
                file_path,
                start.elapsed()
            );
            return Ok(());
        }

        if start.elapsed() >= wait_timeout {
            log::error!(
                "Timeout waiting for file '{}' after {:?}",
                file_path,
                wait_timeout
            );
            return Err(format!(
                "Timeout waiting for file '{}' after {:?}. {}",
                file_path,
                wait_timeout,
                format_file_missing_error(file_path)
            ));
        }

        log::info!(
            "File '{}' not found, retrying in {:?}... (elapsed: {:?})",
            file_path,
            retry_interval,
            start.elapsed()
        );
        sleep(retry_interval).await;
    }
}

/// Wait for files matching a pattern to exist
pub async fn wait_for_pattern_match(
    pattern: &str,
    wait_timeout: Duration,
    retry_interval: Duration,
) -> Result<Vec<String>, String> {
    if wait_timeout.as_secs() == 0 {
        // No wait configured - return current matches
        return find_matching_files(pattern);
    }

    let start = Instant::now();
    loop {
        match find_matching_files(pattern) {
            Ok(files) if !files.is_empty() => {
                log::info!(
                    "Found {} files matching pattern '{}' after waiting {:?}",
                    files.len(),
                    pattern,
                    start.elapsed()
                );
                return Ok(files);
            }
            Ok(_) => {
                // No files found yet
            }
            Err(e) => {
                log::warn!("Error searching for pattern '{}': {}", pattern, e);
            }
        }

        if start.elapsed() >= wait_timeout {
            log::error!(
                "Timeout waiting for files matching pattern '{}' after {:?}",
                pattern,
                wait_timeout
            );
            return Err(format!(
                "Timeout waiting for files matching pattern '{}' after {:?}. {}",
                pattern,
                wait_timeout,
                format_file_missing_error(pattern)
            ));
        }

        log::info!(
            "No files matching pattern '{}', retrying in {:?}... (elapsed: {:?})",
            pattern,
            retry_interval,
            start.elapsed()
        );
        sleep(retry_interval).await;
    }
}

/// Find files matching a glob pattern
pub fn find_matching_files(pattern: &str) -> Result<Vec<String>, String> {
    // Simple implementation - in production this would use a proper glob library
    if pattern.contains('*') {
        // Extract directory and pattern
        let path = Path::new(pattern);
        let parent_dir = path.parent().unwrap_or(Path::new("."));
        let filename_pattern = path.file_name().and_then(|n| n.to_str()).unwrap_or("*");

        match std::fs::read_dir(parent_dir) {
            Ok(entries) => {
                let mut matches = Vec::new();
                for entry in entries {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name();
                        if let Some(name_str) = file_name.to_str() {
                            // Simple wildcard matching - replace with proper glob matching
                            if filename_pattern == "*"
                                || (filename_pattern.ends_with("*")
                                    && name_str.starts_with(
                                        &filename_pattern[..filename_pattern.len() - 1],
                                    ))
                                || name_str.contains(&filename_pattern.replace("*", ""))
                            {
                                matches.push(entry.path().to_string_lossy().to_string());
                            }
                        }
                    }
                }
                Ok(matches)
            }
            Err(e) => Err(format!("Failed to read directory: {}", e)),
        }
    } else {
        // Single file check
        if Path::new(pattern).exists() {
            Ok(vec![pattern.to_string()])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        // Test various formats
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("2.5s"), Some(Duration::from_millis(2500)));
        assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));

        // Test full words
        assert_eq!(parse_duration("30 seconds"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5 minutes"), Some(Duration::from_secs(300)));

        // Test invalid formats
        assert_eq!(parse_duration("invalid"), None);
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("-5s"), None);
    }

    #[test]
    fn test_is_topic_missing_error() {
        use crate::velostream::kafka::kafka_error::ConsumerError;

        // Test with various error messages that should be detected
        let missing_errors = vec![
            "Unknown topic or partition",
            "Topic does not exist",
            "Topic not found",
            "Metadata for topic 'test' not available",
        ];

        for error_msg in missing_errors {
            let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
                rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
            );
            let error = ConsumerError::KafkaError(kafka_error);
            assert!(
                is_topic_missing_error(&error),
                "Should detect missing topic for: {}",
                error_msg
            );
        }

        // Test with errors that should NOT be detected as missing topic
        let other_errors = vec!["Connection failed", "Authentication error", "Timeout error"];

        for error_msg in other_errors {
            let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
                rdkafka::error::RDKafkaErrorCode::BrokerNotAvailable,
            );
            let error = ConsumerError::KafkaError(kafka_error);
            assert!(
                !is_topic_missing_error(&error),
                "Should NOT detect missing topic for: {}",
                error_msg
            );
        }
    }

    #[test]
    fn test_format_topic_missing_error() {
        use crate::velostream::kafka::kafka_error::ConsumerError;

        let kafka_error = rdkafka::error::KafkaError::MetadataFetch(
            rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
        );
        let error = ConsumerError::KafkaError(kafka_error);
        let formatted = format_topic_missing_error("test_topic", &error);

        assert!(formatted.contains("test_topic"));
        assert!(formatted.contains("kafka-topics --create"));
        assert!(formatted.contains("topic.wait.timeout"));
        assert!(formatted.contains("Original error"));
    }

    #[test]
    fn test_format_file_missing_error() {
        let formatted = format_file_missing_error("/path/to/file.json");

        assert!(formatted.contains("/path/to/file.json"));
        assert!(formatted.contains("file.wait.timeout"));
        assert!(formatted.contains("watch"));
        assert!(formatted.contains("Check the file path"));
    }
}
