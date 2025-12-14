//! Kafka data source implementation

use crate::velostream::config::{
    ConfigSchemaProvider, GlobalSchemaContext, PropertyDefault, PropertyValidation,
};
use crate::velostream::datasource::config_loader::merge_config_file_properties;
use crate::velostream::datasource::{DataReader, DataSource, SourceConfig, SourceMetadata};
// Note: unified config helpers available if needed for more complex validation
use crate::velostream::schema::{FieldDefinition, Schema};
use crate::velostream::sql::ast::DataType;
use crate::velostream::sql::config::PropertyResolver;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::reader::{KafkaDataReader, SerializationFormat};

/// Kafka DataSource implementation
pub struct KafkaDataSource {
    brokers: String,
    topic: String,
    group_id: Option<String>,
    config: HashMap<String, String>,
    event_time_config: Option<crate::velostream::datasource::EventTimeConfig>,
}

impl KafkaDataSource {
    /// Create a Kafka data source from properties
    ///
    /// # Arguments
    /// * `props` - Configuration properties (from YAML or SQL)
    /// * `default_topic` - Default topic name if not in properties
    /// * `job_name` - Name of the job (source/stream name)
    /// * `app_name` - Optional SQL Application name for consumer group generation
    pub fn from_properties(
        props: &HashMap<String, String>,
        default_topic: &str,
        job_name: &str,
        app_name: Option<&str>,
        instance_id: Option<&str>,
    ) -> Self {
        // DEBUG: Log all properties being passed
        log::info!(
            "KafkaDataSource::from_properties for topic '{}', job '{}', app '{}'",
            default_topic,
            job_name,
            app_name.unwrap_or("none")
        );
        log::info!("  Received {} properties:", props.len());
        for (k, v) in props.iter() {
            if k.contains("config_file") || k.contains("bootstrap") || k.contains("schema") {
                log::info!("    {} = {}", k, v);
            }
        }

        // Load and merge config file with provided properties
        // Uses common config_loader helper
        let merged_props = merge_config_file_properties(props, "KafkaDataSource");

        // DEBUG: Log merged properties
        log::info!(
            "KafkaDataSource: After merge, have {} properties:",
            merged_props.len()
        );
        for (k, v) in merged_props.iter() {
            if k.contains("bootstrap")
                || k.contains("topic")
                || k.contains("schema")
                || k.contains("group")
            {
                log::info!("  [merged] {} = {}", k, v);
            }
        }

        // Helper function to get property with source. prefix fallback
        let get_source_prop = |key: &str| {
            let result = merged_props
                .get(&format!("source.{}", key))
                .or_else(|| merged_props.get(key))
                .cloned();
            if let Some(ref val) = result {
                log::debug!("  get_source_prop('{}') = {}", key, val);
            }
            result
        };

        // Extract brokers from properties
        // Priority order: check YAML config keys with ${VAR:default} pattern FIRST,
        // then fall back to explicit/override keys
        let brokers_raw = get_source_prop("datasource.consumer_config.bootstrap.servers")
            .or_else(|| get_source_prop("source.brokers"))
            .or_else(|| get_source_prop("brokers"))
            .or_else(|| get_source_prop("source.bootstrap.servers"))
            .or_else(|| get_source_prop("bootstrap.servers"))
            .unwrap_or_else(|| "localhost:9092".to_string());

        // Apply runtime env var substitution if the value contains ${VAR:default} pattern
        // This handles cases where YAML was loaded before env vars were set (e.g., testcontainers)
        use crate::velostream::sql::config::yaml_loader::substitute_env_vars;
        let brokers = substitute_env_vars(&brokers_raw);

        log::info!(
            "KafkaDataSource: brokers raw='{}', resolved='{}'",
            brokers_raw,
            brokers
        );

        let resolver = PropertyResolver::default();
        // Topic resolution: ENV → config → default_topic
        let topic: String = resolver
            .resolve_optional(
                "KAFKA_TOPIC",
                &[
                    "source.topic",
                    "topic",
                    "datasource.topic.name",
                    "topic.name",
                ],
                &merged_props,
            )
            .unwrap_or_else(|| default_topic.to_string());

        // FAIL FAST validation for suspicious topic names
        Self::validate_topic_name(&topic).expect("Topic validation failed");

        // Generate consumer group ID with app-level coordination support
        // Priority: ENV → explicit config → app_name prefix → default fallback
        let group_id: Option<String> = resolver.resolve_optional(
            "KAFKA_GROUP_ID",
            &["source.group_id", "group_id", "group.id"],
            &merged_props,
        );
        let group_id = group_id.unwrap_or_else(|| {
            // Generate app-aware consumer group using unified format (underscores as delimiters)
            use super::config_helpers::generate_consumer_group_id;
            generate_consumer_group_id(app_name, job_name)
        });

        // Log consumer group assignment for visibility
        // Detect where the group_id came from for observability
        let source_type = if std::env::var(resolver.env_var_name("KAFKA_GROUP_ID")).is_ok() {
            "env var override"
        } else if merged_props
            .get("source.group_id")
            .or_else(|| merged_props.get("group_id"))
            .or_else(|| merged_props.get("group.id"))
            .is_some()
        {
            "explicit config"
        } else {
            "auto-generated"
        };
        log::info!(
            "Kafka consumer group ID: '{}' (source: {}, job: {}, app: {})",
            group_id,
            source_type,
            job_name,
            app_name.unwrap_or("none")
        );

        // Generate and log client ID using shared helper
        use super::config_helpers::{ClientType, generate_client_id, log_client_id};
        let client_id = generate_client_id(app_name, job_name, instance_id, ClientType::Source);
        log_client_id(
            &client_id,
            ClientType::Source,
            app_name,
            job_name,
            instance_id,
        );

        // Create filtered config with source. properties
        // Filter out producer-only properties that shouldn't be passed to consumers
        let producer_only_properties = [
            "linger.ms",
            "batch.size",
            "acks",
            "retries",
            "request.timeout.ms",
            "delivery.timeout.ms",
            "max.in.flight.requests.per.connection",
            "compression.type", // This can be consumer but is typically producer
            "buffer.memory",
            "max.block.ms",
            "transaction.timeout.ms",
            "transactional.id",
            "enable.idempotence",
        ];

        // Build the source name prefix (e.g., "market_data.") to filter out SQL-level properties
        let source_name_prefix = format!("{}.", default_topic);

        let mut source_config = HashMap::new();
        log::info!("KafkaDataSource: Filtering properties for source config");
        for (key, value) in merged_props.iter() {
            // Skip producer-only properties
            if producer_only_properties.contains(&key.as_str()) {
                log::debug!("  Skipping producer-only property: {}", key);
                continue;
            }

            // Skip performance_profiles.* properties (these are templates, not consumer config)
            if key.starts_with("performance_profiles.") || key.starts_with("delivery_profiles.") {
                log::debug!("  Skipping profile template property: {}", key);
                continue;
            }

            // Skip metadata.* properties (these are documentation, not config)
            if key.starts_with("metadata.") {
                log::debug!("  Skipping metadata property: {}", key);
                continue;
            }

            // Skip topic_config.* properties (these are for topic creation, not consumer)
            if key.starts_with("topic_config.") || key.starts_with("topic.") {
                log::debug!("  Skipping topic config property: {}", key);
                continue;
            }

            // Skip properties prefixed with source name (e.g., "market_data.topic", "market_data.datasource.*")
            // These are SQL-level WITH clause properties that have already been processed
            if key.starts_with(&source_name_prefix) {
                log::debug!(
                    "  Skipping source-name prefixed property: {} (prefix: {})",
                    key,
                    source_name_prefix
                );
                continue;
            }

            // Skip datasink.* and type/config_file properties
            if key.starts_with("datasink.")
                || key == "type"
                || key.contains(".type")
                || key.contains(".config_file")
                || key == "config_file"
            {
                log::debug!("  Skipping sink/meta property: {}", key);
                continue;
            }

            // Handle datasource.consumer_config.* properties - strip prefix for consumer usage
            if key.starts_with("datasource.consumer_config.") {
                let config_key = key
                    .strip_prefix("datasource.consumer_config.")
                    .unwrap()
                    .to_string();
                log::debug!(
                    "  Adding consumer config property: {} (from datasource.consumer_config.{})",
                    config_key,
                    config_key
                );
                source_config.insert(config_key, value.clone());
            }
            // Skip datasource.schema.* properties - they are fallbacks from parent config
            // Prefer schema.* properties which come from the actual YAML config file
            else if key.starts_with("datasource.schema.") {
                // Check if there's already a schema.* version of this property
                let schema_key = key.strip_prefix("datasource.").unwrap();
                if merged_props.contains_key(schema_key) {
                    log::debug!(
                        "  Skipping {} - preferring {} from config file",
                        key,
                        schema_key
                    );
                    continue;
                }
                // Only use datasource.schema.* if schema.* doesn't exist
                log::debug!("  Adding {} as fallback for {}", key, schema_key);
                source_config.insert(schema_key.to_string(), value.clone());
            } else if key.starts_with("source.") {
                // Remove source. prefix for the config map
                let config_key = key.strip_prefix("source.").unwrap().to_string();

                // Filter out application-level properties that shouldn't be passed to rdkafka
                // Note: "value.format" is NOT filtered - it's a valid Kafka/serialization property
                let application_properties = [
                    "format",      // Bare format property (application-level)
                    "has_headers", // CSV file header flag
                    "path",        // File path (for file sources)
                    "append",      // File append mode
                ];

                if application_properties.contains(&config_key.as_str()) {
                    log::debug!(
                        "  Skipping application-level property: {} (not for Kafka consumer)",
                        config_key
                    );
                    continue;
                }

                log::debug!(
                    "  Adding source property: {} (from source.{})",
                    config_key,
                    config_key
                );
                source_config.insert(config_key, value.clone());
            } else if !key.starts_with("sink.")
                && !key.starts_with("datasource.")
                && !merged_props.contains_key(&format!("source.{}", key))
            {
                // Include unprefixed properties only if there's no prefixed version and it's not a sink/datasource property
                log::debug!("  Adding unprefixed property: {}", key);
                source_config.insert(key.clone(), value.clone());
            } else {
                log::debug!(
                    "  Skipping property: {} (starts with sink/datasource or has source. version)",
                    key
                );
            }
        }

        // Add client.id to config for per-client observability
        source_config.insert("client.id".to_string(), client_id);

        // CRITICAL: Ensure bootstrap.servers uses the RESOLVED value (after env var substitution)
        // This is essential for testcontainers integration where VELOSTREAM_KAFKA_BROKERS
        // is set AFTER config loading but BEFORE SQL job execution
        source_config.insert("bootstrap.servers".to_string(), brokers.clone());

        log::info!(
            "KafkaDataSource: Final source config has {} properties",
            source_config.len()
        );
        log::info!("KafkaDataSource: Properties that will be passed to Kafka consumer:");
        for (k, v) in source_config.iter() {
            log::info!("    [consumer] {} = {}", k, v);
        }

        Self {
            brokers,
            topic,
            group_id: Some(group_id),
            config: source_config,
            event_time_config: None,
        }
    }

    /// FAIL FAST: Validate topic name to prevent silent data loss
    fn validate_topic_name(topic: &str) -> Result<(), String> {
        // Check for empty topic name
        if topic.is_empty() {
            return Err("CONFIGURATION ERROR: Kafka source topic name is empty.\n\
                 \n\
                 A valid Kafka topic name MUST be configured. Please configure via:\n\
                 1. YAML config file: 'topic: <topic_name>' or 'topic.name: <topic_name>'\n\
                 2. SQL properties: '<source_name>.topic = <topic_name>'\n\
                 3. Named source in SQL: FROM <source_name> (uses source_name as topic)\n\
                 \n\
                 This validation prevents reading from misconfigured topics."
                .to_string());
        }

        // Warn about suspicious topic names that might indicate misconfiguration
        let suspicious_names = [
            "default",
            "test",
            "temp",
            "placeholder",
            "undefined",
            "null",
            "none",
            "example",
            "my-topic",
            "topic-name",
        ];

        if suspicious_names.contains(&topic.to_lowercase().as_str()) {
            return Err(format!(
                "CONFIGURATION ERROR: Kafka source configured with suspicious topic name '{}'.\n\
                 \n\
                 This is a common placeholder/fallback value that indicates configuration \
                 was not properly loaded.\n\
                 \n\
                 Valid topic names should be:\n\
                 1. Extracted from source name in SQL: FROM <source_name> ...\n\
                 2. Configured in YAML: 'topic: <topic_name>' or 'topic.name: <topic_name>'\n\
                 \n\
                 Common misconfiguration causes:\n\
                 - YAML file not found or not loaded\n\
                 - Missing 'topic' or 'topic.name' in YAML\n\
                 - Hardcoded fallback value not updated\n\
                 \n\
                 This validation prevents reading from misconfigured topics.",
                topic
            ));
        }

        log::info!(
            "KafkaDataSource: Topic validation passed - will read from topic '{}'",
            topic
        );

        Ok(())
    }

    /// Create a new Kafka data source
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            group_id: None,
            config: HashMap::new(),
            event_time_config: None,
        }
    }

    /// Set the consumer group ID
    pub fn with_group_id(mut self, group_id: String) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Generate SourceConfig from current state
    pub fn to_source_config(&self) -> SourceConfig {
        SourceConfig::Kafka {
            brokers: self.brokers.clone(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
            properties: self.config.clone(),
            batch_config: Default::default(),
            event_time_config: self.event_time_config.clone(),
        }
    }

    // Getter methods for testing
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn group_id(&self) -> &Option<String> {
        &self.group_id
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Self-initialize with current configuration
    pub async fn self_initialize(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.to_source_config();
        self.initialize(config).await
    }

    /// Create a consumer based on the serialization format in config
    async fn create_unified_reader(
        &self,
        group_id: &str,
        batch_size: Option<usize>,
    ) -> Result<KafkaDataReader, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        // Try multiple common property name patterns
        let value_format = self
            .config
            .get("value.serializer")
            .or_else(|| self.config.get("schema.value.serializer")) // From YAML config
            .or_else(|| self.config.get("datasource.schema.value.serializer")) // Full nested path
            .or_else(|| self.config.get("value.format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Parse format using FromStr with proper error handling
        let format = {
            use std::str::FromStr;
            SerializationFormat::from_str(value_format).unwrap_or(SerializationFormat::Json)
        };

        // Extract schema from config based on format
        let schema = self.extract_schema_for_format(&format)?;

        // DEBUG: Log schema extraction result
        log::info!(
            "KafkaDataSource: Extracted schema for format {:?}: {}",
            format,
            if schema.is_some() {
                format!("YES ({} bytes)", schema.as_ref().unwrap().len())
            } else {
                "NO SCHEMA".to_string()
            }
        );

        // Create unified reader - batch size is now configured via max.poll.records property
        // If batch_size was specified, add it to config
        let mut config = self.config.clone();
        if let Some(size) = batch_size {
            config.insert("max.poll.records".to_string(), size.to_string());
        }

        KafkaDataReader::from_properties(
            self.brokers.clone(),
            self.topic.clone(),
            group_id.to_string(),
            &config,
            self.event_time_config.clone(),
        )
        .await
    }

    /// Extract schema from configuration based on serialization format
    fn extract_schema_for_format(
        &self,
        format: &SerializationFormat,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        match format {
            SerializationFormat::Avro { .. } => {
                // Look for Avro schema in various config keys (common patterns)
                let schema = self
                    .config
                    .get("avro.schema")
                    .or_else(|| self.config.get("value.avro.schema"))
                    .or_else(|| self.config.get("schema.avro"))
                    .or_else(|| self.config.get("avro_schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path (try all common patterns)
                    if let Some(schema_file) = self
                        .config
                        .get("avro.schema.file")
                        .or_else(|| self.config.get("schema.value.schema.file")) // From YAML config
                        .or_else(|| self.config.get("value.schema.file")) // Common pattern
                        .or_else(|| self.config.get("schema.file"))
                        .or_else(|| self.config.get("avro_schema_file"))
                        .or_else(|| self.config.get("datasource.schema.value.schema.file"))
                    // Full nested path
                    {
                        return self.load_schema_from_file(schema_file);
                    }
                }

                Ok(schema)
            }
            SerializationFormat::Protobuf { .. } => {
                // Look for Protobuf schema in various config keys
                let schema = self
                    .config
                    .get("protobuf.schema")
                    .or_else(|| self.config.get("value.protobuf.schema"))
                    .or_else(|| self.config.get("schema.protobuf"))
                    .or_else(|| self.config.get("protobuf_schema"))
                    .or_else(|| self.config.get("proto.schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path
                    if let Some(schema_file) = self
                        .config
                        .get("protobuf.schema.file")
                        .or_else(|| self.config.get("proto.schema.file"))
                        .or_else(|| self.config.get("schema.file"))
                        .or_else(|| self.config.get("protobuf_schema_file"))
                    {
                        return self.load_schema_from_file(schema_file);
                    }
                }

                Ok(schema)
            }
            SerializationFormat::Json
            | SerializationFormat::Bytes
            | SerializationFormat::String => {
                // JSON doesn't require schema, but allow optional schema for validation
                let schema = self
                    .config
                    .get("json.schema")
                    .or_else(|| self.config.get("schema.json"))
                    .cloned();
                Ok(schema)
            }
        }
    }

    /// Load schema content from a file path
    fn load_schema_from_file(
        &self,
        file_path: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        use std::fs;

        match fs::read_to_string(file_path) {
            Ok(content) => Ok(Some(content)),
            Err(e) => Err(format!("Failed to load schema from file '{}': {}", file_path, e).into()),
        }
    }

    /// Validate Kafka source configuration properties
    /// Returns (missing_required, missing_recommended, warnings)
    pub fn validate_source_config(
        properties: &HashMap<String, String>,
        name: &str,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        // Configuration keys with common prefixes
        const DATASOURCE_PREFIX: &str = "datasource";
        const CONSUMER_CONFIG: &str = "consumer_config";
        const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
        const TOPIC: &str = "topic";
        const GROUP_ID_DOT: &str = "group.id";
        const GROUP_ID_UNDERSCORE: &str = "group_id";

        // Error message helper function
        fn missing_required_msg(name: &str, key: &str) -> String {
            format!("Kafka source '{}' missing required config: {}", name, key)
        }
        fn missing_recommended_msg(name: &str, key: &str) -> String {
            format!(
                "Kafka source '{}' missing recommended config: {}",
                name, key
            )
        }
        fn batch_config_warning(name: &str) -> String {
            format!(
                "Kafka source '{}' has batch configuration - ensure this is intended",
                name
            )
        }

        let mut missing_required = Vec::new();
        let mut missing_recommended = Vec::new();
        let mut warnings = Vec::new();

        // Check for bootstrap.servers in standard Kafka consumer_config location
        let has_bootstrap_servers = properties.contains_key(&format!(
            "{}.{}.{}",
            DATASOURCE_PREFIX, CONSUMER_CONFIG, BOOTSTRAP_SERVERS
        ));

        if !has_bootstrap_servers {
            missing_required.push(format!(
                "Missing required property for source '{}': 'datasource.consumer_config.bootstrap.servers'. \
                This should be defined in your YAML config file under datasource -> consumer_config -> bootstrap.servers",
                name
            ));
        }

        // Check for topic in standard Kafka location
        let has_topic = properties.contains_key(&format!("{}.topic.name", DATASOURCE_PREFIX));

        if !has_topic {
            missing_required.push(format!(
                "Missing required property for source '{}': 'datasource.topic.name'. \
                This should be defined in your YAML config file under datasource -> topic -> name",
                name
            ));
        }

        // Recommended properties with fallback support
        let recommended_keys = vec![(GROUP_ID_DOT, GROUP_ID_UNDERSCORE)];

        // Check recommended properties with fallback support
        for (dot_key, underscore_key) in &recommended_keys {
            if !properties.contains_key(*dot_key) && !properties.contains_key(*underscore_key) {
                missing_recommended.push(missing_recommended_msg(name, dot_key));
            }
        }

        // Check for batch configuration presence
        let batch_indicators = ["batch", "poll", "fetch"];
        let has_batch_config = properties.keys().any(|k| {
            batch_indicators
                .iter()
                .any(|indicator| k.contains(indicator))
        });

        if has_batch_config {
            warnings.push(batch_config_warning(name));
        }

        (missing_required, missing_recommended, warnings)
    }
}

#[async_trait]
impl DataSource for KafkaDataSource {
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SourceConfig::Kafka {
                brokers,
                topic,
                group_id,
                properties,
                batch_config,
                event_time_config,
                ..
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.group_id = group_id;
                self.event_time_config = event_time_config;

                // Use provided properties directly for Kafka consumer configuration
                // BatchConfig is deprecated for Kafka - configure via properties instead:
                // - max.poll.records, fetch.max.wait.ms, fetch.min.bytes, etc.
                self.config = properties;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        use crate::velostream::sql::execution::types::StreamRecord;

        // For Kafka, we'll return a generic schema since message format is flexible
        // In practice, this could integrate with Schema Registry
        // Note: offset, partition, timestamp are accessed via system columns (_OFFSET, _PARTITION, _TIMESTAMP)
        // Topic and key are stored as struct fields on StreamRecord (accessible as _topic, _key)
        let fields = vec![
            FieldDefinition::optional(StreamRecord::FIELD_KEY.to_string(), DataType::String),
            FieldDefinition::required(StreamRecord::FIELD_TOPIC.to_string(), DataType::String),
        ];

        Ok(Schema::new(fields))
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let group_id = self.group_id.as_ref().ok_or_else(|| {
            Box::new(KafkaDataSourceError::Configuration(
                "Group ID required for consumer".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Extract batch size from config if available
        let batch_size = self
            .config
            .get("max.poll.records")
            .and_then(|s| s.parse::<usize>().ok());

        // Create the unified reader
        let reader = self.create_unified_reader(group_id, batch_size).await?;

        Ok(Box::new(reader))
    }

    async fn create_reader_with_batch_config(
        &self,
        batch_config: crate::velostream::datasource::BatchConfig,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let group_id = self.group_id.as_ref().ok_or_else(|| {
            Box::new(KafkaDataSourceError::Configuration(
                "Group ID required for consumer".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Convert BatchConfig to max.poll.records setting
        let mut config = self.config.clone();
        config.insert(
            "max.poll.records".to_string(),
            batch_config.max_batch_size.to_string(),
        );

        let reader = KafkaDataReader::from_properties(
            self.brokers.clone(),
            self.topic.clone(),
            group_id.clone(),
            &config,
            self.event_time_config.clone(),
        )
        .await?;

        Ok(Box::new(reader))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        true // Kafka can be used for batch processing
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            source_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: true,
            capabilities: vec![
                "real_time".to_string(),
                "exactly_once".to_string(),
                "schema_registry".to_string(),
                "headers".to_string(),
                "partitioning".to_string(),
            ],
        }
    }
}

/// ConfigSchemaProvider implementation for KafkaDataSource
/// This provides validation and schema information for Kafka data source configuration
impl ConfigSchemaProvider for KafkaDataSource {
    fn config_type_id() -> &'static str {
        "kafka_source"
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
        defaults.insert(
            "auto.offset.reset",
            PropertyDefault::Static("latest".to_string()),
        );
        defaults.insert(
            "enable.auto.commit",
            PropertyDefault::Static("true".to_string()),
        );
        defaults.insert(
            "session.timeout.ms",
            PropertyDefault::Static("30000".to_string()),
        );
        defaults.insert(
            "heartbeat.interval.ms",
            PropertyDefault::Static("3000".to_string()),
        );
        defaults.insert(
            "value.serializer",
            PropertyDefault::Static("json".to_string()),
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
            "group_id" | "group.id" => {
                if value.is_empty() {
                    return Err(vec!["group_id cannot be empty".to_string()]);
                }

                // Kafka consumer group naming validation
                let invalid_chars: Vec<char> = value
                    .chars()
                    .filter(|c| !c.is_alphanumeric() && !"-_.".contains(*c))
                    .collect();

                if !invalid_chars.is_empty() {
                    return Err(vec![format!(
                        "group_id '{}' contains invalid characters: {}. Use alphanumeric, '-', '_', or '.' only",
                        value,
                        invalid_chars.iter().collect::<String>()
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
            "auto.offset.reset" => {
                let valid_values = ["earliest", "latest", "none"];
                if !valid_values.contains(&value) {
                    return Err(vec![format!(
                        "auto.offset.reset must be one of: {}. Got: '{}'",
                        valid_values.join(", "),
                        value
                    )]);
                }
            }
            "value.serializer" | "value.format" => {
                let valid_formats = ["json", "avro", "protobuf", "string", "bytes"];
                if !valid_formats.contains(&value) {
                    return Err(vec![format!(
                        "value.serializer must be one of: {}. Got: '{}'",
                        valid_formats.join(", "),
                        value
                    )]);
                }
            }
            "enable.auto.commit" => {
                if !["true", "false"].contains(&value) {
                    return Err(vec![
                        "enable.auto.commit must be 'true' or 'false'".to_string(),
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
            key if key.ends_with(".interval.ms") => {
                if let Ok(interval) = value.parse::<u32>() {
                    if interval == 0 {
                        return Err(vec![format!("{} must be greater than 0", key)]);
                    }
                } else {
                    return Err(vec![format!(
                        "{} must be a valid interval in milliseconds",
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
                // Allow other Kafka client properties with basic validation
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
            "title": "Kafka Data Source Configuration Schema",
            "description": "Configuration schema for Kafka data sources in Velostream",
            "properties": {
                "bootstrap.servers": {
                    "type": "string",
                    "description": "Comma-separated list of Kafka broker endpoints in host:port format",
                    "pattern": "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$",
                    "examples": ["localhost:9092", "broker1:9092,broker2:9092"]
                },
                "topic": {
                    "type": "string",
                    "description": "Kafka topic name to consume from",
                    "maxLength": 249,
                    "pattern": "^[a-zA-Z0-9._-]+$",
                    "examples": ["orders", "user-events", "inventory.updates"]
                },
                "group.id": {
                    "type": "string",
                    "description": "Kafka consumer group ID",
                    "pattern": "^[a-zA-Z0-9._-]+$",
                    "examples": ["analytics-group", "order-processor"]
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
                "auto.offset.reset": {
                    "type": "string",
                    "enum": ["earliest", "latest", "none"],
                    "default": "latest",
                    "description": "How to handle offset when no initial offset exists"
                },
                "value.serializer": {
                    "type": "string",
                    "enum": ["json", "avro", "protobuf", "string", "bytes"],
                    "default": "json",
                    "description": "Serialization format for message values"
                },
                "enable.auto.commit": {
                    "type": "boolean",
                    "default": true,
                    "description": "Whether to enable automatic offset commits"
                },
                "session.timeout.ms": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3600000,
                    "default": 30000,
                    "description": "Session timeout for consumer group coordination"
                },
                "heartbeat.interval.ms": {
                    "type": "integer",
                    "minimum": 1,
                    "default": 3000,
                    "description": "Heartbeat interval for consumer group coordination"
                },
                "schema.registry.url": {
                    "type": "string",
                    "format": "uri",
                    "description": "URL of the schema registry (for Avro/Protobuf)"
                },
                "avro.schema": {
                    "type": "string",
                    "description": "Inline Avro schema definition"
                },
                "avro.schema.file": {
                    "type": "string",
                    "description": "Path to Avro schema file"
                },
                "protobuf.schema": {
                    "type": "string",
                    "description": "Inline Protobuf schema definition"
                },
                "protobuf.schema.file": {
                    "type": "string",
                    "description": "Path to Protobuf schema file"
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
                description: "Kafka topic name to consume from".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^[a-zA-Z0-9._-]+$".to_string()),
            },
            PropertyValidation {
                key: "group.id".to_string(),
                required: false,
                default: Some(PropertyDefault::Dynamic(|ctx| {
                    let dev_default = "dev".to_string();
                    let unknown_default = "unknown".to_string();
                    let env = ctx
                        .environment_variables
                        .get("ENVIRONMENT")
                        .unwrap_or(&dev_default);
                    let job_id = ctx
                        .system_defaults
                        .get("job.id")
                        .unwrap_or(&unknown_default);
                    format!("velo-{}-{}", env, job_id)
                })),
                description: "Kafka consumer group ID".to_string(),
                json_type: "string".to_string(),
                validation_pattern: Some("^[a-zA-Z0-9._-]+$".to_string()),
            },
        ]
    }

    fn supports_custom_properties() -> bool {
        true // Allow custom Kafka client properties
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
            "group.id" => {
                let dev_default = "dev".to_string();
                let unknown_default = "unknown".to_string();
                let env = global_context
                    .environment_variables
                    .get("ENVIRONMENT")
                    .unwrap_or(&dev_default);
                let job_id = global_context
                    .system_defaults
                    .get("job.id")
                    .unwrap_or(&unknown_default);
                Ok(Some(format!("velo-{}-{}", env, job_id)))
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

/// Default implementation for KafkaDataSource (required for schema registry)
impl Default for KafkaDataSource {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "default_topic".to_string(),
            group_id: Some("default_group".to_string()),
            config: HashMap::new(),
            event_time_config: None,
        }
    }
}
