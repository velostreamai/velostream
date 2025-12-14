//! Unified Kafka data reader implementation
//!
//! Simplified implementation that relies on Kafka consumer configuration for batching.
//! Configure behavior via standard Kafka consumer properties:
//!
//! ## Batching Properties
//! - `max.poll.records` - Maximum records to return per poll (default: 500)
//! - `fetch.min.bytes` - Minimum bytes to fetch before returning (default: 1)
//! - `fetch.max.bytes` - Maximum bytes to fetch per request (default: 50MB)
//! - `fetch.max.wait.ms` - Maximum wait time for fetch (default: 500ms)
//! - `max.partition.fetch.bytes` - Maximum bytes per partition (default: 1MB)
//!
//! ## Consumer Group Properties
//! - `auto.offset.reset` - Where to start reading: "earliest", "latest", "none"
//! - `enable.auto.commit` - Auto-commit offsets (default: true)
//! - `auto.commit.interval.ms` - Auto-commit interval (default: 5000ms)
//! - `session.timeout.ms` - Session timeout for group membership (default: 30000ms)
//! - `heartbeat.interval.ms` - Heartbeat interval (default: 3000ms)
//! - `max.poll.interval.ms` - Max time between polls before leaving group (default: 300000ms)
//!
//! ## Transaction Properties
//! - `isolation.level` - "read_committed" or "read_uncommitted" (default: read_uncommitted)

use crate::velostream::datasource::{DataReader, EventTimeConfig, SourceOffset};

/// Kafka consumer property keys (standard Kafka configuration names)
mod props {
    // Core properties
    pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
    pub const GROUP_ID: &str = "group.id";

    // Offset management
    pub const AUTO_OFFSET_RESET: &str = "auto.offset.reset";
    pub const ENABLE_AUTO_COMMIT: &str = "enable.auto.commit";
    pub const AUTO_COMMIT_INTERVAL_MS: &str = "auto.commit.interval.ms";

    // Session management
    pub const SESSION_TIMEOUT_MS: &str = "session.timeout.ms";
    pub const HEARTBEAT_INTERVAL_MS: &str = "heartbeat.interval.ms";
    pub const MAX_POLL_INTERVAL_MS: &str = "max.poll.interval.ms";

    // Fetch configuration
    pub const FETCH_MIN_BYTES: &str = "fetch.min.bytes";
    pub const FETCH_MAX_BYTES: &str = "fetch.max.bytes";
    pub const FETCH_MAX_WAIT_MS: &str = "fetch.max.wait.ms";
    pub const MAX_PARTITION_FETCH_BYTES: &str = "max.partition.fetch.bytes";
    pub const MAX_POLL_RECORDS: &str = "max.poll.records";

    // Transaction
    pub const ISOLATION_LEVEL: &str = "isolation.level";

    // Property prefixes to skip (not Kafka consumer config)
    pub const PREFIX_SCHEMA: &str = "schema.";
    pub const PREFIX_VALUE: &str = "value.";
    pub const PREFIX_DATASOURCE: &str = "datasource.";

    // Non-Kafka properties
    pub const TOPIC: &str = "topic";
    pub const CONSUMER_GROUP: &str = "consumer.group";
    pub const PERFORMANCE_PROFILE: &str = "performance_profile";

    /// Valid librdkafka consumer configuration properties (allowlist approach)
    /// Source: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    pub fn valid_consumer_properties() -> std::collections::HashSet<&'static str> {
        [
            // Global configuration
            "bootstrap.servers",
            "client.id",
            "metadata.broker.list",
            "message.max.bytes",
            "receive.message.max.bytes",
            "max.in.flight.requests.per.connection",
            "max.in.flight",
            "topic.metadata.refresh.interval.ms",
            "metadata.max.age.ms",
            "topic.metadata.refresh.fast.interval.ms",
            "topic.metadata.refresh.sparse",
            "topic.metadata.propagation.max.ms",
            "topic.blacklist",
            "debug",
            "socket.timeout.ms",
            "socket.blocking.max.ms",
            "socket.send.buffer.bytes",
            "socket.receive.buffer.bytes",
            "socket.keepalive.enable",
            "socket.nagle.disable",
            "socket.max.fails",
            "broker.address.ttl",
            "broker.address.family",
            "reconnect.backoff.jitter.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
            "statistics.interval.ms",
            "log_level",
            "log.queue",
            "log.thread.name",
            "log.connection.close",
            "api.version.request",
            "api.version.request.timeout.ms",
            "api.version.fallback.ms",
            "broker.version.fallback",
            // Security
            "security.protocol",
            "ssl.cipher.suites",
            "ssl.curves.list",
            "ssl.sigalgs.list",
            "ssl.key.location",
            "ssl.key.password",
            "ssl.key.pem",
            "ssl.certificate.location",
            "ssl.certificate.pem",
            "ssl.ca.location",
            "ssl.ca.pem",
            "ssl.ca.certificate.stores",
            "ssl.crl.location",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.providers",
            "ssl.engine.location",
            "enable.ssl.certificate.verification",
            "ssl.endpoint.identification.algorithm",
            // SASL
            "sasl.mechanisms",
            "sasl.mechanism",
            "sasl.kerberos.service.name",
            "sasl.kerberos.principal",
            "sasl.kerberos.kinit.cmd",
            "sasl.kerberos.keytab",
            "sasl.kerberos.min.time.before.relogin",
            "sasl.username",
            "sasl.password",
            "sasl.oauthbearer.config",
            "enable.sasl.oauthbearer.unsecure.jwt",
            "sasl.oauthbearer.method",
            "sasl.oauthbearer.client.id",
            "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.scope",
            "sasl.oauthbearer.extensions",
            "sasl.oauthbearer.token.endpoint.url",
            // Consumer specific
            "group.id",
            "group.instance.id",
            "partition.assignment.strategy",
            "session.timeout.ms",
            "heartbeat.interval.ms",
            "group.protocol.type",
            "coordinator.query.interval.ms",
            "max.poll.interval.ms",
            "enable.auto.commit",
            "auto.commit.interval.ms",
            "enable.auto.offset.store",
            "queued.min.messages",
            "queued.max.messages.kbytes",
            "fetch.wait.max.ms",
            "fetch.message.max.bytes",
            "fetch.max.bytes",
            "fetch.min.bytes",
            "fetch.error.backoff.ms",
            "max.partition.fetch.bytes",
            "max.poll.records",
            "offset.store.method",
            "isolation.level",
            "consume.callback.max.messages",
            "enable.partition.eof",
            "check.crcs",
            "allow.auto.create.topics",
            "auto.offset.reset",
            // Client rack
            "client.rack",
            // Interceptors
            "plugin.library.paths",
        ]
        .into_iter()
        .collect()
    }
}
use crate::velostream::kafka::consumer_config::ConsumerConfig;
// Import the unified SerializationFormat from the main module
pub use crate::velostream::kafka::serialization_format::SerializationFormat;
use crate::velostream::kafka::{
    kafka_error::ConsumerError, kafka_fast_consumer::Consumer as FastConsumer,
    serialization::StringSerializer,
};
use crate::velostream::serialization::{SerializationCodec, json_codec::JsonCodec};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use log::info;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

/// Type alias for the Kafka consumer with String keys and FieldValue HashMap values
type KafkaFieldValueConsumer =
    FastConsumer<String, HashMap<String, FieldValue>, StringSerializer, SerializationCodec>;

/// Unified Kafka DataReader that handles all serialization formats
///
/// Simplified implementation that relies on Kafka consumer configuration for batching.
/// Configure via Kafka consumer properties (max.poll.records, fetch.max.wait.ms, etc.)
pub struct KafkaDataReader {
    consumer: KafkaFieldValueConsumer,
    event_time_config: Option<EventTimeConfig>,
}

impl KafkaDataReader {
    /// FAIL FAST: Validate topic configuration to prevent misconfiguration
    fn validate_topic_configuration(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        if topic.is_empty() {
            return Err("CONFIGURATION ERROR: Kafka source topic name is empty.\n\
                 \n\
                 A valid Kafka topic name MUST be configured. Please configure via:\n\
                 1. YAML config file: 'topic.name: <topic_name>'\n\
                 2. SQL properties: '<source_name>.topic = <topic_name>'\n\
                 3. Direct parameter when creating KafkaDataReader\n\
                 \n\
                 Example configuration:\n\
                 consumer_config:\n\
                   topic.name: my-input-topic\n\
                   bootstrap.servers: localhost:9092"
                .into());
        }

        // Topic validation - Kafka topic names have strict rules
        if topic.len() > 249 {
            return Err(format!(
                "CONFIGURATION ERROR: Topic name '{}' exceeds maximum length of 249 characters",
                topic
            )
            .into());
        }

        // Valid Kafka topic characters: alphanumeric, '.', '_', '-'
        if !topic
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
        {
            return Err(format!(
                "CONFIGURATION ERROR: Topic name '{}' contains invalid characters.\n\
                 Valid characters: alphanumeric, '.', '_', '-'",
                topic
            )
            .into());
        }

        info!(
            "KafkaDataReader: Topic validation passed - will read from topic '{}'",
            topic
        );

        Ok(())
    }

    /// Create a new Kafka data reader from properties
    ///
    /// This is the primary constructor that handles all configuration.
    /// Consistent with `KafkaDataWriter::from_properties()` API.
    ///
    /// # Arguments
    /// * `brokers` - Kafka bootstrap servers (e.g., "localhost:9092")
    /// * `topic` - Kafka topic to consume from
    /// * `group_id` - Optional consumer group ID
    /// * `consumer_properties` - HashMap of Kafka consumer properties
    /// * `event_time_config` - Optional event time extraction configuration
    pub async fn from_properties(
        brokers: String,
        topic: String,
        group_id: String,
        consumer_properties: &HashMap<String, String>,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::validate_topic_configuration(&topic)?;

        // Configure consumer from properties
        let mut consumer_config = ConsumerConfig::new(brokers, group_id.clone());

        // Apply performance profile first (if specified), then individual properties can override
        consumer_config = Self::apply_performance_profile(consumer_config, consumer_properties);

        // Apply consumer properties from YAML config
        Self::apply_consumer_properties(&mut consumer_config, consumer_properties);

        info!(
            "KafkaDataReader: Creating consumer with max_poll_records={}",
            consumer_config.max_poll_records
        );

        // Create the consumer (batch_size set from config.max_poll_records)
        let consumer = Self::create_consumer(consumer_config)?;

        consumer.subscribe(&[&topic]).map_err(|e| {
            log::error!("Cannot subscribe to topic '{}': {}", topic, e);
            Box::new(e) as Box<dyn Error + Send + Sync>
        })?;

        Ok(Self {
            consumer,
            event_time_config,
        })
    }

    /// Create consumer with the specified configuration
    fn create_consumer(
        consumer_config: ConsumerConfig,
    ) -> Result<KafkaFieldValueConsumer, Box<dyn Error + Send + Sync>> {
        let key_deserializer = StringSerializer;
        let value_deserializer = SerializationCodec::Json(JsonCodec);

        FastConsumer::with_config(consumer_config, key_deserializer, value_deserializer)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    /// Parse a duration in milliseconds from a string value
    fn parse_duration_ms(key: &str, value: &str) -> Option<Duration> {
        match value.parse::<u64>() {
            Ok(ms) => Some(Duration::from_millis(ms)),
            Err(_) => {
                log::warn!("Invalid value '{}' for {}, using default", value, key);
                None
            }
        }
    }

    /// Parse a u32 value from a string
    fn parse_u32(key: &str, value: &str) -> Option<u32> {
        match value.parse::<u32>() {
            Ok(v) => Some(v),
            Err(_) => {
                log::warn!("Invalid value '{}' for {}, using default", value, key);
                None
            }
        }
    }

    /// Apply performance profile to consumer configuration
    ///
    /// Performance profiles provide optimized defaults for common use cases:
    /// - `high_throughput`: Optimized for maximum message throughput
    /// - `low_latency`: Optimized for minimal latency
    /// - `max_durability`: Optimized for data safety and reliability
    /// - `development`: Development-friendly settings
    /// - `streaming`: Balanced settings for continuous processing
    /// - `batch_processing`: Optimized for periodic batch processing
    fn apply_performance_profile(
        config: ConsumerConfig,
        properties: &HashMap<String, String>,
    ) -> ConsumerConfig {
        use crate::velostream::kafka::performance_presets::PerformancePresets;

        let profile = properties
            .get(props::PERFORMANCE_PROFILE)
            .map(|s| s.as_str());

        match profile {
            Some("high_throughput") => {
                info!("KafkaDataReader: Applying 'high_throughput' performance profile");
                config.high_throughput()
            }
            Some("low_latency") => {
                info!("KafkaDataReader: Applying 'low_latency' performance profile");
                config.low_latency()
            }
            Some("max_durability") => {
                info!("KafkaDataReader: Applying 'max_durability' performance profile");
                config.max_durability()
            }
            Some("development") => {
                info!("KafkaDataReader: Applying 'development' performance profile");
                config.development()
            }
            Some("streaming") => {
                info!("KafkaDataReader: Applying 'streaming' performance profile");
                config.streaming()
            }
            Some("batch_processing") => {
                info!("KafkaDataReader: Applying 'batch_processing' performance profile");
                config.batch_processing()
            }
            Some(unknown) => {
                log::warn!(
                    "Unknown performance_profile '{}', using defaults. \
                     Valid options: high_throughput, low_latency, max_durability, \
                     development, streaming, batch_processing",
                    unknown
                );
                config
            }
            None => config,
        }
    }

    /// Apply consumer properties from YAML config to Kafka consumer configuration
    fn apply_consumer_properties(
        consumer_config: &mut ConsumerConfig,
        properties: &HashMap<String, String>,
    ) {
        use crate::velostream::kafka::consumer_config::{IsolationLevel, OffsetReset};

        log::info!(
            "Applying {} consumer properties from config",
            properties.len()
        );

        for (key, value) in properties.iter() {
            // Skip metadata properties (not Kafka consumer config)
            if key.starts_with(props::PREFIX_SCHEMA)
                || key.starts_with(props::PREFIX_VALUE)
                || key.starts_with(props::PREFIX_DATASOURCE)
                || matches!(
                    key.as_str(),
                    props::TOPIC | props::CONSUMER_GROUP | props::PERFORMANCE_PROFILE
                )
            {
                log::debug!("  Skipping property (metadata): {} = {}", key, value);
                continue;
            }

            log::debug!("  Applying consumer property: {} = {}", key, value);

            match key.as_str() {
                // Already set via constructor
                props::BOOTSTRAP_SERVERS | props::GROUP_ID => {}

                // Enum properties
                props::AUTO_OFFSET_RESET => {
                    consumer_config.auto_offset_reset = match value.as_str() {
                        "earliest" => OffsetReset::Earliest,
                        "latest" => OffsetReset::Latest,
                        "none" => OffsetReset::None,
                        _ => {
                            log::warn!(
                                "Unknown {} '{}', using default",
                                props::AUTO_OFFSET_RESET,
                                value
                            );
                            OffsetReset::Earliest
                        }
                    };
                }
                props::ISOLATION_LEVEL => {
                    consumer_config.isolation_level = match value.as_str() {
                        "read_committed" => IsolationLevel::ReadCommitted,
                        "read_uncommitted" => IsolationLevel::ReadUncommitted,
                        _ => {
                            log::warn!(
                                "Unknown {} '{}', using default",
                                props::ISOLATION_LEVEL,
                                value
                            );
                            IsolationLevel::ReadUncommitted
                        }
                    };
                }

                // Boolean property
                props::ENABLE_AUTO_COMMIT => {
                    consumer_config.enable_auto_commit = value.parse().unwrap_or_else(|_| {
                        log::warn!(
                            "Invalid value '{}' for {}",
                            value,
                            props::ENABLE_AUTO_COMMIT
                        );
                        true
                    });
                }

                // Duration properties (milliseconds)
                props::AUTO_COMMIT_INTERVAL_MS => {
                    if let Some(d) = Self::parse_duration_ms(key, value) {
                        consumer_config.auto_commit_interval = d;
                    }
                }
                props::SESSION_TIMEOUT_MS => {
                    if let Some(d) = Self::parse_duration_ms(key, value) {
                        consumer_config.session_timeout = d;
                    }
                }
                props::HEARTBEAT_INTERVAL_MS => {
                    if let Some(d) = Self::parse_duration_ms(key, value) {
                        consumer_config.heartbeat_interval = d;
                    }
                }
                props::FETCH_MAX_WAIT_MS => {
                    if let Some(d) = Self::parse_duration_ms(key, value) {
                        consumer_config.fetch_max_wait = d;
                    }
                }
                props::MAX_POLL_INTERVAL_MS => {
                    if let Some(d) = Self::parse_duration_ms(key, value) {
                        consumer_config.max_poll_interval = d;
                    }
                }

                // Numeric properties (u32)
                props::MAX_POLL_RECORDS => {
                    if let Some(v) = Self::parse_u32(key, value) {
                        consumer_config.max_poll_records = v;
                    }
                }
                props::FETCH_MIN_BYTES => {
                    if let Some(v) = Self::parse_u32(key, value) {
                        consumer_config.fetch_min_bytes = v;
                    }
                }
                props::FETCH_MAX_BYTES => {
                    if let Some(v) = Self::parse_u32(key, value) {
                        consumer_config.fetch_max_bytes = v;
                    }
                }
                props::MAX_PARTITION_FETCH_BYTES => {
                    if let Some(v) = Self::parse_u32(key, value) {
                        consumer_config.max_partition_fetch_bytes = v;
                    }
                }

                // Pass-through to rdkafka (only valid rdkafka properties)
                _ => {
                    let valid_props = props::valid_consumer_properties();
                    let key_lower = key.to_lowercase();
                    if valid_props.contains(key_lower.as_str()) {
                        log::debug!("  Pass-through rdkafka property: {} = {}", key, value);
                        consumer_config
                            .common
                            .custom_config
                            .insert(key.clone(), value.clone());
                    } else {
                        log::debug!("  Skipping non-rdkafka property: {} = {}", key, value);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    /// Read records from Kafka
    ///
    /// Uses blocking poll for maximum throughput. Batch size is controlled by
    /// Kafka's `max.poll.records` configuration.
    ///
    /// The `poll_batch` method on `FastConsumer` converts Kafka messages directly
    /// to `StreamRecord` without intermediate allocation.
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        match self
            .consumer
            .poll_batch(Duration::from_millis(100), self.event_time_config.as_ref())
        {
            Ok(records) => {
                log::debug!(
                    "KafkaDataReader::read() returning {} records",
                    records.len()
                );
                Ok(records)
            }
            Err(ConsumerError::Timeout) => Ok(vec![]),
            Err(err) => Err(Box::new(err) as Box<dyn Error + Send + Sync>),
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        log::debug!("KafkaDataReader: Manual offset commit requested");
        self.consumer
            .commit()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        match offset {
            SourceOffset::Kafka { .. } => {
                Err("Seek operation not yet implemented for Kafka consumer".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // For Kafka, we always potentially have more data
        Ok(true)
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        log::debug!(
            "KafkaDataReader: Begin transaction (consumer side - preparing for exactly-once)"
        );
        Ok(true)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        log::debug!("KafkaDataReader: Commit transaction requested - committing consumer offsets");
        self.commit().await
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        true
    }
}
