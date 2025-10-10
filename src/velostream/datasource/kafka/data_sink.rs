//! Kafka data sink implementation

use crate::velostream::config::{
    ConfigSchemaProvider, GlobalSchemaContext, PropertyDefault, PropertyValidation,
};
use crate::velostream::datasource::config_loader::merge_config_file_properties;
use crate::velostream::datasource::{
    BatchConfig, BatchStrategy, DataSink, DataWriter, SinkConfig, SinkMetadata,
};
// Note: unified config helpers available if needed for more complex validation
use crate::velostream::schema::Schema;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::writer::KafkaDataWriter;

/// Kafka DataSink implementation
pub struct KafkaDataSink {
    brokers: String,
    topic: String,
    config: HashMap<String, String>,
}

impl KafkaDataSink {
    /// Create a new Kafka data sink
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            config: HashMap::new(),
        }
    }

    /// Create a Kafka data sink from properties
    pub fn from_properties(props: &HashMap<String, String>, job_name: &str) -> Self {
        // Load and merge config file with provided properties
        // Uses common config_loader helper
        let merged_props = merge_config_file_properties(props, "KafkaDataSink");

        // Helper function to get property with sink. prefix fallback
        let get_sink_prop = |key: &str| {
            merged_props
                .get(&format!("sink.{}", key))
                .or_else(|| merged_props.get(key))
                .cloned()
        };

        let brokers = get_sink_prop("brokers")
            .or_else(|| get_sink_prop("bootstrap.servers"))
            .or_else(|| {
                merged_props
                    .get("datasink.producer_config.bootstrap.servers")
                    .cloned()
            })
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = get_sink_prop("topic")
            .or_else(|| merged_props.get("datasink.topic.name").cloned())
            .unwrap_or_else(|| format!("{}_output", job_name));

        // Create filtered config with sink. properties
        let mut sink_config = HashMap::new();
        for (key, value) in merged_props.iter() {
            if key.starts_with("sink.") {
                // Remove sink. prefix for the config map
                let config_key = key.strip_prefix("sink.").unwrap().to_string();
                sink_config.insert(config_key, value.clone());
            } else if !key.starts_with("source.")
                && !merged_props.contains_key(&format!("sink.{}", key))
            {
                // Include unprefixed properties only if there's no prefixed version and it's not a source property
                sink_config.insert(key.clone(), value.clone());
            }
        }

        Self {
            brokers,
            topic,
            config: sink_config,
        }
    }

    /// Create a unified writer based on the serialization format in config
    async fn create_unified_writer(
        &self,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        let _value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Map config value to SerializationFormat enum
        // Format is now handled by KafkaDataWriter::from_properties

        // Create writer with properties-based configuration
        KafkaDataWriter::from_properties(&self.brokers, self.topic.clone(), &self.config).await
    }

    /// Create a unified writer with batch configuration optimizations
    async fn create_unified_writer_with_batch_config(
        &self,
        batch_config: BatchConfig,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Let the KafkaDataWriter handle batch configuration directly
        // This provides better integration and more accurate configuration
        KafkaDataWriter::from_properties_with_batch_config(
            &self.brokers,
            self.topic.clone(),
            &self.config,
            batch_config,
        )
        .await
    }

    /// Apply BatchConfig settings to Kafka producer configuration
    /// NOTE: This method is now primarily used for logging. The actual batch configuration
    /// is handled directly by KafkaDataWriter::from_properties_with_batch_config()
    fn apply_batch_config_to_producer(
        &self,
        producer_config: &mut HashMap<String, String>,
        batch_config: &BatchConfig,
    ) {
        if !batch_config.enable_batching {
            // Disable batching - send immediately
            producer_config.insert("batch.size".to_string(), "0".to_string()); // No batching
            producer_config.insert("linger.ms".to_string(), "0".to_string()); // Send immediately
            return;
        }

        // Configure producer based on batch strategy
        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // Set batch.size to accommodate fixed batch size
                let batch_size = (*size * 1024).min(batch_config.max_batch_size * 1024); // KB estimate
                producer_config.insert("batch.size".to_string(), batch_size.to_string());

                // Use moderate linger time for fixed batching
                producer_config.insert("linger.ms".to_string(), "10".to_string());

                // Suggest compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "snappy".to_string());
                    // Efficient compression
                }
            }
            BatchStrategy::TimeWindow(duration) => {
                // Use time-based batching with linger.ms
                let linger_ms = duration.as_millis().min(30000) as u64; // Cap at 30s
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());
                producer_config.insert("batch.size".to_string(), "65536".to_string()); // 64KB batches

                // Suggest fast compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "lz4".to_string());
                    // Fast compression
                }
            }
            BatchStrategy::AdaptiveSize { target_latency, .. } => {
                // Use target latency as linger time
                let linger_ms = target_latency.as_millis().min(5000) as u64; // Cap at 5s
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());
                producer_config.insert("batch.size".to_string(), "32768".to_string()); // 32KB adaptive batches

                // Suggest compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "snappy".to_string());
                }
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                // Set batch.size based on memory target
                let batch_size = (*max_bytes / 2).min(1024 * 1024); // Half of memory target, max 1MB
                producer_config.insert("batch.size".to_string(), batch_size.to_string());
                producer_config.insert("linger.ms".to_string(), "100".to_string()); // Longer linger for large batches

                // Suggest better compression for large batches only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "gzip".to_string());
                    // Better compression for large batches
                }

                // Use memory target for buffer settings
                let buffer_memory = (*max_bytes * 4).min(64 * 1024 * 1024); // 4x batch size, max 64MB
                producer_config.insert("buffer.memory".to_string(), buffer_memory.to_string());
            }
            BatchStrategy::LowLatency { max_wait_time, .. } => {
                // Optimize for minimal latency
                producer_config.insert("batch.size".to_string(), "1024".to_string()); // Very small batches (1KB)
                let linger_ms = max_wait_time.as_millis().min(10) as u64; // Ultra-short linger time
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());

                // Suggest no compression for minimal latency only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "none".to_string());
                    // No compression overhead
                }

                producer_config.insert("acks".to_string(), "1".to_string()); // Fast acknowledgment (leader only)
                producer_config.insert("retries".to_string(), "0".to_string()); // No retries for speed
                producer_config.insert(
                    "max.in.flight.requests.per.connection".to_string(),
                    "5".to_string(),
                ); // Parallel requests
            }
            BatchStrategy::MegaBatch { batch_size, .. } => {
                // High-throughput mega-batch processing (Phase 4 optimization)
                let kafka_batch_size = (*batch_size * 2048).min(1024 * 1024); // 2KB per record estimate, max 1MB
                producer_config.insert("batch.size".to_string(), kafka_batch_size.to_string());
                producer_config.insert("linger.ms".to_string(), "100".to_string()); // Allow time for batching

                // Suggest compression for large batches only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "zstd".to_string());
                    // Best compression
                }

                // Optimize buffer for large batches
                let buffer_memory = (*batch_size * 4096).min(128 * 1024 * 1024); // 4KB per record, max 128MB
                producer_config.insert("buffer.memory".to_string(), buffer_memory.to_string());
            }
        }

        // Apply general batch timeout as request timeout
        let request_timeout_ms = batch_config.batch_timeout.as_millis().min(300000) as u64; // Max 5 minutes
        producer_config.insert(
            "request.timeout.ms".to_string(),
            request_timeout_ms.to_string(),
        );

        // Log the producer configuration
        self.log_producer_config(producer_config, batch_config);
    }

    /// Log the producer configuration for debugging and monitoring
    fn log_producer_config(
        &self,
        producer_config: &HashMap<String, String>,
        batch_config: &BatchConfig,
    ) {
        use log::info;

        info!("=== Kafka Producer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);

        info!("Applied Producer Settings:");
        info!(
            "  - batch.size: {}",
            producer_config
                .get("batch.size")
                .or_else(|| producer_config.get("batch_size"))
                .unwrap_or(&"default".to_string())
        );
        info!(
            "  - linger.ms: {}",
            producer_config
                .get("linger.ms")
                .or_else(|| producer_config.get("linger_ms"))
                .unwrap_or(&"default".to_string())
        );
        info!(
            "  - compression.type: {}",
            producer_config
                .get("compression.type")
                .or_else(|| producer_config.get("compression_type"))
                .unwrap_or(&"none".to_string())
        );
        info!(
            "  - acks: {}",
            producer_config.get("acks").unwrap_or(&"all".to_string())
        );
        info!(
            "  - retries: {}",
            producer_config
                .get("retries")
                .unwrap_or(&"default".to_string())
        );
        info!(
            "  - request.timeout.ms: {}",
            producer_config
                .get("request.timeout.ms")
                .unwrap_or(&"default".to_string())
        );
        if let Some(buffer_memory) = producer_config.get("buffer.memory") {
            info!("  - buffer.memory: {}", buffer_memory);
        }
        info!("  - topic: {}", self.topic);
        info!("  - brokers: {}", self.brokers);
        info!("=====================================");
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    // Getter methods for testing
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Validate Kafka sink configuration properties
    /// Returns (errors, warnings, recommendations) tuples
    pub fn validate_sink_config(
        properties: &HashMap<String, String>,
        name: &str,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        // Configuration keys with common prefixes
        const DATASINK_PREFIX: &str = "datasink";
        const PRODUCER_CONFIG: &str = "producer_config";
        const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
        const TOPIC: &str = "topic";

        // Property names
        const ACKS: &str = "acks";
        const COMPRESSION_TYPE_DOT: &str = "compression.type";
        const COMPRESSION_TYPE_UNDERSCORE: &str = "compression_type";
        const BATCH_SIZE_DOT: &str = "batch.size";
        const BATCH_SIZE_UNDERSCORE: &str = "batch_size";
        const LINGER_MS_DOT: &str = "linger.ms";
        const LINGER_MS_UNDERSCORE: &str = "linger_ms";

        // Message helper functions
        fn missing_required_msg(name: &str, key: &str) -> String {
            format!("Kafka sink '{}' missing required config: {}", name, key)
        }
        fn missing_recommended_msg(name: &str, key: &str) -> String {
            format!("Kafka sink '{}' missing recommended config: {}", name, key)
        }
        fn batch_perf_recommendation(name: &str) -> String {
            format!("Kafka sink '{}' could benefit from batch configuration (batch.size, linger.ms) for better throughput", name)
        }
        fn acks_zero_warning(name: &str) -> String {
            format!("Kafka sink '{}' has acks=0 which may lead to data loss. Consider acks=1 or acks=all", name)
        }

        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();

        // Check for bootstrap.servers in standard Kafka producer_config location
        let has_bootstrap_servers = properties.contains_key(&format!(
            "{}.{}.{}",
            DATASINK_PREFIX, PRODUCER_CONFIG, BOOTSTRAP_SERVERS
        ));

        if !has_bootstrap_servers {
            errors.push(format!(
                "Missing required property for sink '{}': 'datasink.producer_config.bootstrap.servers'. \
                This should be defined in your YAML config file under datasink -> producer_config -> bootstrap.servers",
                name
            ));
        }

        // Check for topic in standard Kafka location
        let has_topic = properties.contains_key(&format!("{}.topic.name", DATASINK_PREFIX));

        if !has_topic {
            errors.push(format!(
                "Missing required property for sink '{}': 'datasink.topic.name'. \
                This should be defined in your YAML config file under datasink -> topic -> name",
                name
            ));
        }

        // Recommended properties with fallback support
        let recommended_keys = vec![
            (ACKS, ACKS),
            (COMPRESSION_TYPE_DOT, COMPRESSION_TYPE_UNDERSCORE),
        ];

        for (dot_key, underscore_key) in &recommended_keys {
            if !properties.contains_key(*dot_key) && !properties.contains_key(*underscore_key) {
                warnings.push(missing_recommended_msg(name, dot_key));
            }
        }

        // Performance recommendations - check for both dot and underscore variants
        let has_batch_size = properties.contains_key(BATCH_SIZE_DOT)
            || properties.contains_key(BATCH_SIZE_UNDERSCORE);
        let has_linger_ms =
            properties.contains_key(LINGER_MS_DOT) || properties.contains_key(LINGER_MS_UNDERSCORE);

        if !has_batch_size && !has_linger_ms {
            recommendations.push(batch_perf_recommendation(name));
        }

        if properties.get(ACKS).is_some_and(|v| v == "0") {
            recommendations.push(acks_zero_warning(name));
        }

        (errors, warnings, recommendations)
    }
}

#[async_trait]
impl DataSink for KafkaDataSink {
    async fn initialize(
        &mut self,
        config: SinkConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SinkConfig::Kafka {
                brokers,
                topic,
                properties,
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.config = properties;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn validate_schema(
        &self,
        _schema: &Schema,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Kafka is schema-flexible, so we accept any schema
        // In practice, this could validate against Schema Registry
        Ok(())
    }

    async fn create_writer(
        &self,
    ) -> Result<Box<dyn DataWriter>, Box<dyn std::error::Error + Send + Sync>> {
        // Create the unified writer
        let writer = self.create_unified_writer().await.map_err(|e| {
            Box::new(KafkaDataSourceError::Configuration(format!(
                "Failed to create writer: {}",
                e
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(writer))
    }

    async fn create_writer_with_batch_config(
        &self,
        batch_config: BatchConfig,
    ) -> Result<Box<dyn DataWriter>, Box<dyn std::error::Error + Send + Sync>> {
        // Create a writer optimized for the specific batch strategy
        let writer = self
            .create_unified_writer_with_batch_config(batch_config)
            .await
            .map_err(|e| {
                Box::new(KafkaDataSourceError::Configuration(format!(
                    "Failed to create batch-optimized writer: {}",
                    e
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;

        Ok(Box::new(writer))
    }

    fn supports_transactions(&self) -> bool {
        true // Kafka supports exactly-once semantics
    }

    fn supports_upsert(&self) -> bool {
        false // Kafka is append-only, upserts require application logic
    }

    fn metadata(&self) -> SinkMetadata {
        SinkMetadata {
            sink_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_transactions: true,
            supports_upsert: false,
            supports_schema_evolution: true,
            capabilities: vec![
                "exactly_once".to_string(),
                "partitioning".to_string(),
                "headers".to_string(),
                "high_throughput".to_string(),
            ],
        }
    }
}

/// ConfigSchemaProvider implementation for KafkaDataSink
/// This provides validation and schema information for Kafka data sink configuration
impl ConfigSchemaProvider for KafkaDataSink {
    fn config_type_id() -> &'static str {
        "kafka_sink"
    }

    fn inheritable_properties() -> Vec<&'static str> {
        vec![
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "compression.type",
            "schema.registry.url",
            "batch.size",
            "batch.timeout",
            "batch.strategy",
            "acks",
            "retries",
        ]
    }

    fn required_named_properties() -> Vec<&'static str> {
        vec!["topic"]
    }

    fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault> {
        let mut defaults = HashMap::new();
        defaults.insert(
            "bootstrap.servers",
            PropertyDefault::GlobalLookup("kafka.bootstrap.servers".to_string()),
        );
        defaults.insert("acks", PropertyDefault::Static("all".to_string()));
        defaults.insert("retries", PropertyDefault::Static("2147483647".to_string())); // Max retries
        defaults.insert("batch.size", PropertyDefault::Static("16384".to_string())); // 16KB
        defaults.insert("linger.ms", PropertyDefault::Static("5".to_string()));
        defaults.insert(
            "compression.type",
            PropertyDefault::Static("snappy".to_string()),
        );
        defaults.insert(
            "enable.idempotence",
            PropertyDefault::Static("true".to_string()),
        );
        defaults.insert(
            "security.protocol",
            PropertyDefault::Static("PLAINTEXT".to_string()),
        );
        defaults
    }

    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "bootstrap.servers" | "brokers" => {
                if value.is_empty() {
                    return Err(vec!["bootstrap.servers cannot be empty".to_string()]);
                }

                // Validate each server in comma-separated list
                for server in value.split(',') {
                    let server = server.trim();
                    if !server.contains(':') {
                        return Err(vec![format!(
                            "Invalid server format '{}'. Expected 'host:port'",
                            server
                        )]);
                    }

                    // Validate port is numeric
                    if let Some(port_str) = server.split(':').nth(1) {
                        if port_str.parse::<u16>().is_err() {
                            return Err(vec![format!(
                                "Invalid port '{}' in server '{}'. Port must be a number between 1-65535", 
                                port_str, server
                            )]);
                        }
                    }
                }
            }
            "topic" => {
                if value.is_empty() {
                    return Err(vec!["topic cannot be empty".to_string()]);
                }

                // Kafka topic naming validation
                let invalid_chars: Vec<char> = value
                    .chars()
                    .filter(|c| !c.is_alphanumeric() && !"-_.".contains(*c))
                    .collect();

                if !invalid_chars.is_empty() {
                    return Err(vec![format!(
                        "topic name '{}' contains invalid characters: {}. Use alphanumeric, '-', '_', or '.' only",
                        value,
                        invalid_chars.iter().collect::<String>()
                    )]);
                }

                if value.len() > 249 {
                    return Err(vec!["topic name cannot exceed 249 characters".to_string()]);
                }
            }
            "acks" => {
                let valid_values = ["0", "1", "all", "-1"];
                if !valid_values.contains(&value) {
                    return Err(vec![format!(
                        "acks must be one of: {}. Got: '{}'",
                        valid_values.join(", "),
                        value
                    )]);
                }
            }
            "compression.type" => {
                let valid_types = ["none", "gzip", "snappy", "lz4", "zstd"];
                if !valid_types.contains(&value) {
                    return Err(vec![format!(
                        "compression.type must be one of: {}. Got: '{}'",
                        valid_types.join(", "),
                        value
                    )]);
                }
            }
            "security.protocol" => {
                let valid_protocols = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
                if !valid_protocols.contains(&value) {
                    return Err(vec![format!(
                        "security.protocol must be one of: {}. Got: '{}'",
                        valid_protocols.join(", "),
                        value
                    )]);
                }
            }
            "sasl.mechanism" => {
                let valid_mechanisms = [
                    "PLAIN",
                    "SCRAM-SHA-256",
                    "SCRAM-SHA-512",
                    "GSSAPI",
                    "OAUTHBEARER",
                ];
                if !valid_mechanisms.contains(&value) {
                    return Err(vec![format!(
                        "sasl.mechanism must be one of: {}. Got: '{}'",
                        valid_mechanisms.join(", "),
                        value
                    )]);
                }
            }
            "retries" => {
                if let Ok(retries) = value.parse::<i32>() {
                    if retries < 0 {
                        return Err(vec!["retries must be non-negative".to_string()]);
                    }
                } else {
                    return Err(vec!["retries must be a valid integer".to_string()]);
                }
            }
            "enable.idempotence" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![
                        "enable.idempotence must be 'true' or 'false'".to_string()
                    ]);
                }
            }
            "batch.size" => {
                if let Ok(size) = value.parse::<u32>() {
                    if size > 1_048_576 {
                        // 1MB max
                        return Err(vec![
                            "batch.size must not exceed 1,048,576 bytes (1MB)".to_string()
                        ]);
                    }
                } else {
                    return Err(vec!["batch.size must be a valid size in bytes".to_string()]);
                }
            }
            "linger.ms" => {
                if let Ok(linger) = value.parse::<u32>() {
                    if linger > 30_000 {
                        // 30 seconds max
                        return Err(vec![
                            "linger.ms must not exceed 30,000ms (30 seconds)".to_string()
                        ]);
                    }
                } else {
                    return Err(vec![
                        "linger.ms must be a valid time in milliseconds".to_string()
                    ]);
                }
            }
            "buffer.memory" => {
                if let Ok(memory) = value.parse::<u64>() {
                    if memory < 1024 {
                        return Err(vec!["buffer.memory must be at least 1024 bytes".to_string()]);
                    }
                    if memory > 1_073_741_824 {
                        // 1GB max
                        return Err(vec![
                            "buffer.memory must not exceed 1,073,741,824 bytes (1GB)".to_string(),
                        ]);
                    }
                } else {
                    return Err(vec![
                        "buffer.memory must be a valid size in bytes".to_string()
                    ]);
                }
            }
            key if key.ends_with(".timeout.ms") => {
                if let Ok(timeout) = value.parse::<u32>() {
                    if timeout == 0 {
                        return Err(vec![format!("{} must be greater than 0", key)]);
                    }
                    if timeout > 3_600_000 {
                        // 1 hour max
                        return Err(vec![format!(
                            "{} must not exceed 3,600,000ms (1 hour)",
                            key
                        )]);
                    }
                } else {
                    return Err(vec![format!(
                        "{} must be a valid timeout in milliseconds",
                        key
                    )]);
                }
            }
            key if key.starts_with("schema.") => {
                // Allow schema-related properties for Avro/Protobuf
                if value.is_empty() {
                    return Err(vec![format!("{} cannot be empty", key)]);
                }
            }
            key if key.starts_with("batch.") => {
                // Delegate batch property validation to BatchConfig
                // This will be handled by the batch config schema provider
            }
            _ => {
                // Allow other Kafka producer properties with basic validation
                if value.is_empty() && !key.starts_with("custom.") {
                    return Err(vec![format!("Property '{}' cannot be empty", key)]);
                }
            }
        }
        Ok(())
    }

    fn json_schema() -> Value {
        serde_json::json!({
            "type": "object",
            "title": "Kafka Data Sink Configuration Schema",
            "description": "Configuration schema for Kafka data sinks in Velostream",
            "properties": {
                "bootstrap.servers": {
                    "type": "string",
                    "description": "Comma-separated list of Kafka broker endpoints in host:port format",
                    "pattern": "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$",
                    "examples": ["localhost:9092", "broker1:9092,broker2:9092"]
                },
                "topic": {
                    "type": "string",
                    "description": "Kafka topic name to produce to",
                    "maxLength": 249,
                    "pattern": "^[a-zA-Z0-9._-]+$",
                    "examples": ["processed-orders", "analytics-output", "user.events.processed"]
                },
                "acks": {
                    "type": "string",
                    "enum": ["0", "1", "all", "-1"],
                    "default": "all",
                    "description": "Producer acknowledgment mode (0=no acks, 1=leader ack, all=all replicas)"
                },
                "compression.type": {
                    "type": "string",
                    "enum": ["none", "gzip", "snappy", "lz4", "zstd"],
                    "default": "snappy",
                    "description": "Compression algorithm for messages"
                },
                "security.protocol": {
                    "type": "string",
                    "enum": ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
                    "default": "PLAINTEXT",
                    "description": "Security protocol for Kafka connection"
                },
                "sasl.mechanism": {
                    "type": "string",
                    "enum": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"],
                    "description": "SASL authentication mechanism (required when using SASL protocols)"
                },
                "retries": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 2147483647,
                    "description": "Maximum number of retries for failed sends"
                },
                "enable.idempotence": {
                    "type": "boolean",
                    "default": true,
                    "description": "Enable idempotent producer to prevent duplicate messages"
                },
                "batch.size": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 1048576,
                    "default": 16384,
                    "description": "Producer batch size in bytes"
                },
                "linger.ms": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 30000,
                    "default": 5,
                    "description": "Time to wait for additional messages before sending batch"
                },
                "buffer.memory": {
                    "type": "integer",
                    "minimum": 1024,
                    "maximum": 1073741824,
                    "description": "Total memory used by producer for buffering"
                },
                "schema.registry.url": {
                    "type": "string",
                    "format": "uri",
                    "description": "URL of the schema registry (for Avro/Protobuf)"
                },
                "value.serializer": {
                    "type": "string",
                    "enum": ["json", "avro", "protobuf", "string", "bytes"],
                    "default": "json",
                    "description": "Serialization format for message values"
                }
            },
            "required": ["topic"],
            "additionalProperties": true
        })
    }

    fn property_validations() -> Vec<PropertyValidation> {
        vec![
            PropertyValidation {
                key: "bootstrap.servers".to_string(),
                required: false,
                default: Some(PropertyDefault::GlobalLookup(
                    "kafka.bootstrap.servers".to_string(),
                )),
                description: "Comma-separated list of Kafka broker endpoints".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$".to_string()),
            },
            PropertyValidation {
                key: "topic".to_string(),
                required: true,
                default: None,
                description: "Kafka topic name to produce to".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^[a-zA-Z0-9._-]+$".to_string()),
            },
            PropertyValidation {
                key: "acks".to_string(),
                required: false,
                default: Some(PropertyDefault::Static("all".to_string())),
                description: "Producer acknowledgment mode".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("0|1|all|-1".to_string()),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true // Allow custom Kafka producer properties
    }

    fn global_schema_dependencies() -> Vec<&'static str> {
        vec!["kafka_global", "batch_config", "security_global"]
    }

    fn resolve_property_with_inheritance(
        &self,
        key: &str,
        local_value: Option<&str>,
        global_context: &GlobalSchemaContext,
    ) -> Result<Option<String>, String> {
        // Local value takes precedence
        if let Some(value) = local_value {
            return Ok(Some(value.to_string()));
        }

        // Property-specific inheritance logic
        match key {
            "bootstrap.servers" => {
                // Try multiple global sources in order
                for source_key in [
                    "kafka.bootstrap.servers",
                    "global.kafka.servers",
                    "KAFKA_BROKERS",
                ] {
                    if let Some(value) = global_context
                        .global_properties
                        .get(source_key)
                        .or_else(|| global_context.environment_variables.get(source_key))
                    {
                        return Ok(Some(value.clone()));
                    }
                }
                // Default fallback
                Ok(Some("localhost:9092".to_string()))
            }
            "security.protocol" => {
                if let Some(global_security) =
                    global_context.global_properties.get("security.protocol")
                {
                    return Ok(Some(global_security.clone()));
                }
                // Environment-based default
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let default_protocol = if env_profile == "production" {
                    "SASL_SSL"
                } else {
                    "PLAINTEXT"
                };
                Ok(Some(default_protocol.to_string()))
            }
            "compression.type" => {
                if let Some(global_compression) = global_context
                    .global_properties
                    .get("compression.type")
                    .or_else(|| global_context.global_properties.get("compression_type"))
                {
                    return Ok(Some(global_compression.clone()));
                }
                // Environment-based compression choice
                let dev_default = "dev".to_string();
                let env_profile = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let default_compression = if env_profile == "production" {
                    "snappy"
                } else {
                    "none"
                };
                Ok(Some(default_compression.to_string()))
            }
            "schema.registry.url" => {
                // Check for schema registry URL in global config or environment
                if let Some(registry_url) = global_context
                    .global_properties
                    .get("schema.registry.url")
                    .or_else(|| {
                        global_context
                            .environment_variables
                            .get("SCHEMA_REGISTRY_URL")
                    })
                {
                    return Ok(Some(registry_url.clone()));
                }
                Ok(None) // Schema registry is optional
            }
            _ => {
                // Check global properties for other keys
                if let Some(global_value) = global_context.global_properties.get(key) {
                    return Ok(Some(global_value.clone()));
                }
                Ok(None)
            }
        }
    }

    fn schema_version() -> &'static str {
        "2.0.0" // Updated version with enhanced validation
    }
}

/// Default implementation for KafkaDataSink (required for schema registry)
impl Default for KafkaDataSink {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "default_topic".to_string(),
            config: HashMap::new(),
        }
    }
}
