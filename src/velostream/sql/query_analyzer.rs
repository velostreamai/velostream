//! SQL Query Analysis for Dynamic Resource Creation
//!
//! This module analyzes parsed SQL queries to determine what Kafka consumers, producers,
//! and serializers need to be created dynamically based on the query requirements.

use crate::velostream::config::HierarchicalSchemaRegistry;
use crate::velostream::config::schema_registry::validate_configuration;
use crate::velostream::datasource::config_loader::{
    load_config_file_to_properties_with_base, normalize_topic_property,
};
use crate::velostream::datasource::file::{FileDataSink, FileDataSource};
use crate::velostream::datasource::kafka::data_sink::KafkaDataSink;
use crate::velostream::datasource::kafka::data_source::KafkaDataSource;
use crate::velostream::kafka::serialization_format::SerializationConfig;
use crate::velostream::sql::{
    SqlError,
    ast::{InsertSource, IntoClause, StreamSource, StreamingQuery},
};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

/// Trait for checking if tables exist in a table registry
pub trait TableLookup {
    fn table_exists(&self, table_name: &str) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
}

/// Analysis result containing required datasources for SQL execution
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    /// Data sources required (Kafka, File, etc.)
    pub required_sources: Vec<DataSourceRequirement>,
    /// Data sinks required (Kafka, File, etc.)  
    pub required_sinks: Vec<DataSinkRequirement>,
    /// Custom configuration from WITH clauses
    pub configuration: HashMap<String, String>,
}

/// Datasource requirement extracted from SQL
#[derive(Debug, Clone)]
pub struct DataSourceRequirement {
    /// Source name/identifier
    pub name: String,
    /// Source type (kafka, file, s3, etc.)
    pub source_type: DataSourceType,
    /// Configuration properties including serialization
    pub properties: HashMap<String, String>,
}

/// Data sink requirement extracted from SQL
#[derive(Debug, Clone)]
pub struct DataSinkRequirement {
    /// Sink name/identifier
    pub name: String,
    /// Sink type (kafka, file, s3, etc.)
    pub sink_type: DataSinkType,
    /// Configuration properties including serialization
    pub properties: HashMap<String, String>,
    /// Primary key fields from SELECT clause (for Kafka message key)
    pub primary_keys: Option<Vec<String>>,
}

/// Types of data sources
#[derive(Debug, Clone, PartialEq)]
pub enum DataSourceType {
    Kafka,
    File,
    S3,
    Database,
    Generic(String),
}

/// Types of data sinks
#[derive(Debug, Clone, PartialEq)]
pub enum DataSinkType {
    Kafka,
    File,
    S3,
    Database,
    Iceberg,
    Generic(String),
}

// Keep these for backward compatibility
pub type ConsumerRequirement = DataSourceRequirement;
pub type ProducerRequirement = DataSinkRequirement;
pub type FileSourceRequirement = DataSourceRequirement;
pub type FileSinkRequirement = DataSinkRequirement;

// Note: YAML flattening is now centralized in config_loader.rs
// Use load_config_file_to_props() for consistent handling across all components

/// SQL Query Analyzer for resource requirement extraction
pub struct QueryAnalyzer {
    /// Default consumer group base name
    default_group_id: String,
    /// Schema registry for property validation
    schema_registry: HierarchicalSchemaRegistry,
    /// Known table names (from table registry) that should not be validated as external sources
    known_tables: std::collections::HashSet<String>,
    /// Base directory for resolving relative config_file paths (e.g., SQL file's directory)
    base_dir: Option<PathBuf>,
}

impl QueryAnalyzer {
    /// Create a new query analyzer
    pub fn new(default_group_id: String) -> Self {
        let mut schema_registry = HierarchicalSchemaRegistry::new();

        // Register all schema providers for validation
        schema_registry.register_source_schema::<KafkaDataSource>();
        schema_registry.register_source_schema::<FileDataSource>();
        schema_registry.register_sink_schema::<KafkaDataSink>();
        schema_registry.register_sink_schema::<FileDataSink>();

        Self {
            default_group_id,
            schema_registry,
            known_tables: std::collections::HashSet::new(),
            base_dir: None,
        }
    }

    /// Create a new query analyzer with a base directory for resolving relative config paths
    pub fn with_base_dir<P: AsRef<Path>>(default_group_id: String, base_dir: P) -> Self {
        let mut schema_registry = HierarchicalSchemaRegistry::new();

        // Register all schema providers for validation
        schema_registry.register_source_schema::<KafkaDataSource>();
        schema_registry.register_source_schema::<FileDataSource>();
        schema_registry.register_sink_schema::<KafkaDataSink>();
        schema_registry.register_sink_schema::<FileDataSink>();

        Self {
            default_group_id,
            schema_registry,
            known_tables: std::collections::HashSet::new(),
            base_dir: Some(base_dir.as_ref().to_path_buf()),
        }
    }

    /// Set the base directory for resolving relative config paths
    pub fn set_base_dir<P: AsRef<Path>>(&mut self, base_dir: P) {
        self.base_dir = Some(base_dir.as_ref().to_path_buf());
    }

    /// Load a YAML config file and flatten to properties with topic normalization
    ///
    /// Uses the centralized config_loader for consistent handling across all components:
    /// - KafkaDataSource/Sink
    /// - QueryAnalyzer
    /// - Test Harness
    ///
    /// Resolves relative paths against base_dir if set.
    fn load_config_file_to_props(
        &self,
        config_path: &str,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
        load_config_file_to_properties_with_base(config_path, self.base_dir.as_deref())
    }

    /// Register a table as known (from table registry) to skip external source validation
    pub fn add_known_table(&mut self, table_name: String) {
        self.known_tables.insert(table_name);
    }

    /// Register multiple tables as known (from table registry)
    pub fn add_known_tables(&mut self, table_names: Vec<String>) {
        for table_name in table_names {
            self.known_tables.insert(table_name);
        }
    }

    /// Analyze a parsed SQL query to determine required resources
    pub fn analyze(&self, query: &StreamingQuery) -> Result<QueryAnalysis, SqlError> {
        let analysis = QueryAnalysis {
            required_sources: Vec::new(),
            required_sinks: Vec::new(),
            configuration: HashMap::new(),
        };
        self.analyze_with_context(query, &analysis)
    }

    /// Analyze a query with inherited context from parent query
    fn analyze_with_context(
        &self,
        query: &StreamingQuery,
        parent_context: &QueryAnalysis,
    ) -> Result<QueryAnalysis, SqlError> {
        // Start with parent context configuration
        let mut analysis = QueryAnalysis {
            required_sources: Vec::new(),
            required_sinks: Vec::new(),
            configuration: parent_context.configuration.clone(),
        };

        match query {
            StreamingQuery::Select {
                from,
                joins,
                properties,
                ..
            } => {
                // Extract WITH clause properties if present
                if let Some(props) = properties {
                    for (key, value) in props {
                        analysis.configuration.insert(key.clone(), value.clone());
                    }
                }

                // Analyze main FROM clause
                self.analyze_from_clause(from, &mut analysis)?;

                // Analyze JOIN clauses if present
                if let Some(join_clauses) = joins {
                    for join_clause in join_clauses {
                        self.analyze_from_clause(&join_clause.right_source, &mut analysis)?;
                    }
                }
            }
            StreamingQuery::CreateStream {
                name,
                as_select,
                properties,
                ..
            } => {
                // Extract configuration from properties
                for (key, value) in properties {
                    analysis.configuration.insert(key.clone(), value.clone());
                }

                // Recursively analyze the nested SELECT with context
                let nested_analysis = self.analyze_with_context(as_select, &analysis)?;
                self.merge_analysis(&mut analysis, nested_analysis);

                // ENHANCEMENT: Detect sinks defined in WITH clause
                // Pass the stream name so we can match properties by direct name (e.g., output_stream.type)
                self.detect_sinks_from_config(name, &mut analysis)?;

                // Extract PRIMARY KEY fields from the nested SELECT and inject into sinks
                let primary_keys = Self::extract_key_fields_from_query(as_select);
                if let Some(ref keys) = primary_keys {
                    log::debug!(
                        "CreateStream '{}': Extracted primary_keys from SELECT: {:?}",
                        name,
                        keys
                    );
                    // Inject primary_keys into all sinks created for this stream
                    for sink in &mut analysis.required_sinks {
                        if sink.primary_keys.is_none() {
                            sink.primary_keys = primary_keys.clone();
                        }
                    }
                }

                // VALIDATION: CSAS (CREATE STREAM AS SELECT) requires a sink configuration
                if analysis.required_sinks.is_empty() {
                    return Err(SqlError::ConfigurationError {
                        message: format!(
                            "CREATE STREAM '{}' requires a sink configuration. \
                            Define a sink in the WITH clause with '{}.type' = 'kafka_sink' (or file_sink/s3_sink). \
                            Example: WITH ('{}.type' = 'kafka_sink', '{}.topic' = 'output_topic', ...)",
                            name, name, name, name
                        ),
                    });
                }
            }
            StreamingQuery::CreateTable {
                name,
                as_select,
                properties,
                ..
            } => {
                // Extract configuration from properties
                for (key, value) in properties {
                    analysis.configuration.insert(key.clone(), value.clone());
                }

                // Recursively analyze the nested SELECT with context
                let nested_analysis = self.analyze_with_context(as_select, &analysis)?;
                self.merge_analysis(&mut analysis, nested_analysis);

                // ENHANCEMENT: Detect sinks defined in WITH clause (same as CreateStream)
                // This enables config_file loading for CREATE TABLE statements
                self.detect_sinks_from_config(name, &mut analysis)?;

                // Extract PRIMARY KEY fields from the nested SELECT and inject into sinks
                let primary_keys = Self::extract_key_fields_from_query(as_select);
                if let Some(ref keys) = primary_keys {
                    log::debug!(
                        "CreateTable '{}': Extracted primary_keys from SELECT: {:?}",
                        name,
                        keys
                    );
                    // Inject primary_keys into all sinks created for this table
                    for sink in &mut analysis.required_sinks {
                        if sink.primary_keys.is_none() {
                            sink.primary_keys = primary_keys.clone();
                        }
                    }
                }

                // VALIDATION: CTAS (CREATE TABLE AS SELECT) requires a sink configuration
                // EXCEPTION: Reference tables from file_source don't need a sink - they stay in memory
                let has_file_source = analysis
                    .required_sources
                    .iter()
                    .any(|s| matches!(s.source_type, DataSourceType::File));

                if analysis.required_sinks.is_empty() && !has_file_source {
                    return Err(SqlError::ConfigurationError {
                        message: format!(
                            "CREATE TABLE '{}' requires a sink configuration. \
                            Define a sink in the WITH clause with '{}.type' = 'kafka_sink' (or file_sink/s3_sink). \
                            Example: WITH ('{}.type' = 'kafka_sink', '{}.topic' = 'output_topic', ...)",
                            name, name, name, name
                        ),
                    });
                }
            }
            StreamingQuery::Show { .. } => {
                // SHOW queries don't require consumers/producers
            }
            StreamingQuery::StartJob { .. } => {
                // START JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::StopJob { .. } => {
                // STOP JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::PauseJob { .. } => {
                // PAUSE JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::ResumeJob { .. } => {
                // RESUME JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::DeployJob { .. } => {
                // DEPLOY JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::RollbackJob { .. } => {
                // ROLLBACK JOB commands don't require consumers/producers during analysis
            }
            StreamingQuery::InsertInto {
                table_name, source, ..
            } => {
                // First analyze the source to get configuration properties
                match source {
                    InsertSource::Select { query } => {
                        let nested_analysis = self.analyze_with_context(query, &analysis)?;
                        self.merge_analysis(&mut analysis, nested_analysis);

                        // Now analyze the INSERT INTO target as a sink using the extracted configuration
                        let config_clone = analysis.configuration.clone();
                        self.analyze_into_clause(
                            &IntoClause {
                                sink_name: table_name.clone(),
                                sink_properties: HashMap::new(),
                            },
                            &config_clone, // Use the configuration from the nested query
                            &mut analysis,
                        )?;
                    }
                    InsertSource::Values { .. } => {
                        // Values don't require additional source analysis
                        // But still analyze the target as a sink with current configuration
                        let config_clone = analysis.configuration.clone();
                        self.analyze_into_clause(
                            &IntoClause {
                                sink_name: table_name.clone(),
                                sink_properties: HashMap::new(),
                            },
                            &config_clone,
                            &mut analysis,
                        )?;
                    }
                }
            }
            StreamingQuery::Update { .. } => {
                // UPDATE queries may require producers, but for now skip analysis
            }
            StreamingQuery::Delete { .. } => {
                // DELETE queries may require producers, but for now skip analysis
            }
            StreamingQuery::Union { left, right, .. } => {
                // Analyze both sides of the UNION
                let left_analysis = self.analyze(left)?;
                self.merge_analysis(&mut analysis, left_analysis);

                let right_analysis = self.analyze(right)?;
                self.merge_analysis(&mut analysis, right_analysis);
            }
        }

        Ok(analysis)
    }

    /// Analyze a source table to determine datasource requirements with schema validation
    pub fn analyze_source(
        &self,
        table_name: &str,
        config: &HashMap<String, String>,
        _serialization_config: &SerializationConfig,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        // Skip external source validation for known tables (from table registry)
        if self.known_tables.contains(table_name) {
            return Ok(());
        }

        log::debug!(
            "analyze_source called for '{}' with {} config keys: {:?}",
            table_name,
            config.len(),
            config.keys().collect::<Vec<_>>()
        );

        // Build properties map from named source configuration
        let mut properties = HashMap::new();
        let source_prefix = format!("{}.", table_name);

        // Check for config_file and load YAML configuration using centralized config_loader
        // This ensures consistent topic.name normalization across all components
        let config_file_key = format!("{}.config_file", table_name);
        let mut config_file_error: Option<String> = None;
        if let Some(config_file_path) = config.get(&config_file_key) {
            log::debug!(
                "Loading config file '{}' for source '{}'",
                config_file_path,
                table_name
            );
            match self.load_config_file_to_props(config_file_path) {
                Ok(file_props) => {
                    // Merge file properties into properties map
                    properties.extend(file_props);
                    log::debug!(
                        "Loaded {} properties from config file '{}' for source '{}'",
                        properties.len(),
                        config_file_path,
                        table_name
                    );
                }
                Err(e) => {
                    // Store the error to show in the enhanced error message
                    config_file_error = Some(format!(
                        "Failed to load config file '{}': {}",
                        config_file_path, e
                    ));
                    log::warn!("{}", config_file_error.as_ref().unwrap());
                }
            }
        } else {
            log::debug!(
                "No config_file specified for source '{}' (looking for key '{}')",
                table_name,
                config_file_key
            );
        }

        // Add all source-specific properties (e.g., "kafka_source.bootstrap.servers")
        for (key, value) in config {
            if key.starts_with(&source_prefix) {
                // Skip config_file and .type keys - these are handled separately
                let suffix = &key[source_prefix.len()..];
                if suffix == "config_file" || suffix == "type" {
                    continue;
                }

                // Convert to standard property format (remove source name prefix)
                let standard_key = suffix.to_string();
                properties.insert(standard_key, value.clone());

                // Also preserve original key for legacy compatibility during transition
                properties.insert(key.clone(), value.clone());
            }
        }

        // Determine source type - EXPLICIT ONLY (no autodetection)
        // Uses simple compound type format: {name}.type = '{type}_source'
        // Examples: 'kafka_source', 'file_source', 's3_source'
        let source_type_str = config
            .get(&format!("{}.type", table_name))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                // Build enhanced error message showing loaded properties
                let mut error_msg = format!(
                    "Source type must be explicitly specified for '{}'. Use: '{}.type' with values like 'kafka_source', 'file_source', 's3_source', 'database_source'\n\n",
                    table_name, table_name
                );

                // Show loaded properties for debugging
                if !properties.is_empty() {
                    error_msg.push_str("✓ LOADED PROPERTIES:\n");
                    for (key, value) in &properties {
                        if !key.contains("config_file") {
                            error_msg.push_str(&format!("  • {} = {}\n", key, value));
                        }
                    }
                } else {
                    // Provide detailed diagnostic information
                    if let Some(file_error) = &config_file_error {
                        error_msg.push_str("❌ CONFIGURATION FILE ERROR:\n");
                        error_msg.push_str(&format!("  {}\n\n", file_error));
                        error_msg.push_str("RESOLUTION:\n");
                        error_msg.push_str("  • Verify the config_file path is correct and the file exists\n");
                        error_msg.push_str("  • Ensure the YAML file is valid and contains expected properties\n");
                    } else if config.contains_key(&config_file_key) {
                        error_msg.push_str("⚠ NO PROPERTIES LOADED - config_file exists but is empty or could not be parsed\n");
                        error_msg.push_str(&format!("  config_file: {}\n\n", config.get(&config_file_key).unwrap()));
                    } else {
                        error_msg.push_str("⚠ NO CONFIGURATION PROVIDED\n");
                        error_msg.push_str(&format!("  • Missing '{}.config_file' specification\n", table_name));
                        error_msg.push_str(&format!("  • Define it in the WITH clause or ensure it's in the global configuration\n"));
                    }
                }

                SqlError::ConfigurationError {
                    message: error_msg,
                }
            })?;

        let source_type = match source_type_str {
            "kafka_source" => DataSourceType::Kafka,
            "file_source" => DataSourceType::File,
            "s3_source" => DataSourceType::S3,
            "database_source" => DataSourceType::Database,
            other => {
                return Err(SqlError::ConfigurationError {
                    message: format!(
                        "Invalid source type '{}' for '{}'. Supported values: 'kafka_source', 'file_source', 's3_source', 'database_source'",
                        other, table_name
                    ),
                });
            }
        };

        // Check if config file failed to load (even though source type was provided)
        if let Some(file_error) = config_file_error {
            return Err(SqlError::ConfigurationError {
                message: file_error,
            });
        }

        // Add default properties and validate using schema
        match source_type {
            DataSourceType::Kafka => {
                // Ensure we have broker and topic info
                // Check all possible bootstrap server keys from YAML config paths
                let has_bootstrap = properties.contains_key("bootstrap.servers")
                    || properties.contains_key("brokers")
                    || properties.contains_key("datasource.consumer_config.bootstrap.servers")
                    || properties.contains_key("datasource.config.bootstrap.servers")
                    || properties.contains_key("consumer_config.bootstrap.servers");

                if !has_bootstrap {
                    properties.insert(
                        "bootstrap.servers".to_string(),
                        "localhost:9092".to_string(),
                    );
                }
                // Apply topic normalization (centralized in config_loader)
                normalize_topic_property(&mut properties);
                // Set default topic to table name if still not set
                if !properties.contains_key("topic") {
                    properties.insert("topic".to_string(), table_name.to_string());
                }
                if !properties.contains_key("group.id") {
                    properties.insert(
                        "group.id".to_string(),
                        format!("{}-{}", self.default_group_id, table_name),
                    );
                }

                // Add serialization formats
                if let Some(key_fmt) = config.get("key.serializer") {
                    properties.insert("key.serializer".to_string(), key_fmt.clone());
                }
                if let Some(val_fmt) = config.get("value.serializer") {
                    properties.insert("value.serializer".to_string(), val_fmt.clone());
                }

                // SCHEMA VALIDATION: Validate Kafka source properties
                self.validate_source_properties("kafka_source", &properties)?;
            }
            DataSourceType::File => {
                // SCHEMA VALIDATION: Validate File source properties
                self.validate_source_properties("file_source", &properties)?;
            }
            _ => {
                // For other source types, perform basic validation
                log::warn!(
                    "Schema validation not implemented for source type: {:?}",
                    source_type
                );
            }
        }

        let source_req = DataSourceRequirement {
            name: table_name.to_string(),
            source_type,
            properties,
        };

        analysis.required_sources.push(source_req);
        Ok(())
    }

    /// Analyze a sink table to determine datasink requirements
    pub fn analyze_sink(
        &self,
        table_name: &str,
        config: &HashMap<String, String>,
        _serialization_config: &SerializationConfig,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        // Build properties map from named sink configuration
        let mut properties = HashMap::new();
        let sink_prefix = format!("{}.", table_name);

        // Check for config_file and load YAML configuration using centralized config_loader
        // This ensures consistent topic.name normalization across all components
        let config_file_key = format!("{}.config_file", table_name);
        let mut config_file_error: Option<String> = None;
        if let Some(config_file_path) = config.get(&config_file_key) {
            log::debug!(
                "Loading config file '{}' for sink '{}' (via analyze_sink)",
                config_file_path,
                table_name
            );
            match self.load_config_file_to_props(config_file_path) {
                Ok(file_props) => {
                    // Merge file properties into properties map
                    properties.extend(file_props);
                    log::debug!(
                        "Loaded YAML config for sink '{}' with {} properties",
                        table_name,
                        properties.len()
                    );
                }
                Err(e) => {
                    // Store the error to show in the enhanced error message
                    config_file_error = Some(format!(
                        "Failed to load config file '{}': {}",
                        config_file_path, e
                    ));
                }
            }
        }

        // Add all sink-specific properties (e.g., "kafka_sink.bootstrap.servers")
        // These SQL properties will override YAML properties with the same key
        for (key, value) in config {
            if key.starts_with(&sink_prefix) {
                // Skip config_file and .type keys - these are handled separately
                let suffix = &key[sink_prefix.len()..];
                if suffix == "config_file" || suffix == "type" {
                    continue;
                }

                // Convert to standard property format (remove sink name prefix)
                let standard_key = suffix.to_string();
                properties.insert(standard_key, value.clone());

                // Also preserve original key for legacy compatibility during transition
                properties.insert(key.clone(), value.clone());
            }
        }

        // Determine sink type - EXPLICIT ONLY (no autodetection)
        // Uses simple compound type format: {name}.type = '{type}_sink'
        // Examples: 'kafka_sink', 'file_sink', 's3_sink'
        let sink_type_str = config
            .get(&format!("{}.type", table_name))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                // Build enhanced error message showing loaded properties
                let mut error_msg = format!(
                    "Sink type must be explicitly specified for '{}'. Use: '{}.type' with values like 'kafka_sink', 'file_sink', 's3_sink', 'database_sink', 'iceberg_sink'\n\n",
                    table_name, table_name
                );

                // Show loaded properties for debugging
                if !properties.is_empty() {
                    error_msg.push_str("✓ LOADED PROPERTIES:\n");
                    for (key, value) in &properties {
                        if !key.contains("config_file") {
                            error_msg.push_str(&format!("  • {} = {}\n", key, value));
                        }
                    }
                } else {
                    // Provide detailed diagnostic information
                    if let Some(file_error) = &config_file_error {
                        error_msg.push_str("❌ CONFIGURATION FILE ERROR:\n");
                        error_msg.push_str(&format!("  {}\n\n", file_error));
                        error_msg.push_str("RESOLUTION:\n");
                        error_msg.push_str("  • Verify the config_file path is correct and the file exists\n");
                        error_msg.push_str("  • Ensure the YAML file is valid and contains expected properties\n");
                    } else if config.contains_key(&config_file_key) {
                        error_msg.push_str("⚠ NO PROPERTIES LOADED - config_file exists but is empty or could not be parsed\n");
                        error_msg.push_str(&format!("  config_file: {}\n\n", config.get(&config_file_key).unwrap()));
                    } else {
                        error_msg.push_str("⚠ NO CONFIGURATION PROVIDED\n");
                        error_msg.push_str(&format!("  • Missing '{}.config_file' specification\n", table_name));
                        error_msg.push_str(&format!("  • Define it in the WITH clause or ensure it's in the global configuration\n"));
                    }
                }

                SqlError::ConfigurationError {
                    message: error_msg,
                }
            })?;

        let sink_type = match sink_type_str {
            "kafka_sink" => DataSinkType::Kafka,
            "file_sink" => DataSinkType::File,
            "s3_sink" => DataSinkType::S3,
            "database_sink" => DataSinkType::Database,
            "iceberg_sink" => DataSinkType::Iceberg,
            other => {
                return Err(SqlError::ConfigurationError {
                    message: format!(
                        "Invalid sink type '{}' for '{}'. Supported values: 'kafka_sink', 'file_sink', 's3_sink', 'database_sink', 'iceberg_sink'",
                        other, table_name
                    ),
                });
            }
        };

        // Check if config file failed to load (even though sink type was provided)
        if let Some(file_error) = config_file_error {
            return Err(SqlError::ConfigurationError {
                message: file_error,
            });
        }

        // Add default properties and validate using schema
        match sink_type {
            DataSinkType::Kafka => {
                // Ensure we have broker and topic info
                // Check all possible bootstrap server keys from YAML config paths
                let has_bootstrap = properties.contains_key("bootstrap.servers")
                    || properties.contains_key("brokers")
                    || properties.contains_key("datasink.producer_config.bootstrap.servers")
                    || properties.contains_key("datasink.config.bootstrap.servers")
                    || properties.contains_key("producer_config.bootstrap.servers");

                if !has_bootstrap {
                    properties.insert(
                        "bootstrap.servers".to_string(),
                        "localhost:9092".to_string(),
                    );
                }
                // Apply topic normalization (centralized in config_loader)
                normalize_topic_property(&mut properties);
                // Set default topic to table name if still not set
                if !properties.contains_key("topic") {
                    log::debug!(
                        "No topic found for sink '{}', using table_name as default",
                        table_name
                    );
                    properties.insert("topic".to_string(), table_name.to_string());
                }

                // Add serialization formats
                if let Some(key_fmt) = config.get("key.serializer") {
                    properties.insert("key.serializer".to_string(), key_fmt.clone());
                }
                if let Some(val_fmt) = config.get("value.serializer") {
                    properties.insert("value.serializer".to_string(), val_fmt.clone());
                }

                // SCHEMA VALIDATION: Validate Kafka sink properties
                self.validate_sink_properties("kafka_sink", &properties)?;
            }
            DataSinkType::File => {
                // SCHEMA VALIDATION: Validate File sink properties
                self.validate_sink_properties("file_sink", &properties)?;
            }
            _ => {
                // For other sink types, perform basic validation
                log::warn!(
                    "Schema validation not implemented for sink type: {:?}",
                    sink_type
                );
            }
        }

        let sink_req = DataSinkRequirement {
            name: table_name.to_string(),
            sink_type,
            properties,
            primary_keys: None,
        };

        analysis.required_sinks.push(sink_req);
        Ok(())
    }

    /// Analyze FROM clause to extract source requirements
    fn analyze_from_clause(
        &self,
        from_source: &StreamSource,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        match from_source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                // Clone configuration to avoid borrow checker issues
                let config = analysis.configuration.clone();

                // Parse serialization configuration from existing config
                let serialization_config =
                    SerializationConfig::from_sql_params(&config).map_err(|e| {
                        SqlError::ExecutionError {
                            message: format!("Failed to parse serialization config: {}", e),
                            query: None,
                        }
                    })?;

                self.analyze_source(name, &config, &serialization_config, analysis)?;
            }
            StreamSource::Uri(uri) => {
                // FR-047 URI-based data source
                self.analyze_uri_source(uri, analysis)?;
            }
            StreamSource::Subquery(subquery) => {
                // Recursively analyze the subquery
                let nested_analysis = self.analyze(subquery)?;
                self.merge_analysis(analysis, nested_analysis);
            }
        }
        Ok(())
    }

    /// Analyze URI-based data source (FR-047)
    fn analyze_uri_source(&self, uri: &str, analysis: &mut QueryAnalysis) -> Result<(), SqlError> {
        // Parse URI to determine source type and base properties
        let (source_type, properties) = self.parse_data_source_uri(uri)?;

        let requirement = DataSourceRequirement {
            name: uri.to_string(),
            source_type,
            properties,
        };

        analysis.required_sources.push(requirement);
        Ok(())
    }

    /// Parse data source URI into type and properties
    fn parse_data_source_uri(
        &self,
        uri: &str,
    ) -> Result<(DataSourceType, HashMap<String, String>), SqlError> {
        let mut properties = HashMap::new();

        if let Some(scheme_end) = uri.find("://") {
            let scheme = &uri[..scheme_end];
            let path = &uri[scheme_end + 3..];

            properties.insert("uri".to_string(), uri.to_string());
            properties.insert("path".to_string(), path.to_string());

            let source_type = match scheme {
                "file" => {
                    // Extract format from file extension if possible
                    if let Some(dot_pos) = path.rfind('.') {
                        let extension = &path[dot_pos + 1..];
                        let format = match extension {
                            "csv" => "csv",
                            "json" | "jsonl" => "json",
                            "parquet" => "parquet",
                            "avro" => "avro",
                            _ => "auto",
                        };
                        properties.insert("format".to_string(), format.to_string());
                    }
                    DataSourceType::File
                }
                "kafka" => {
                    // Parse kafka://broker:port/topic format
                    if let Some(slash_pos) = path.find('/') {
                        let broker = &path[..slash_pos];
                        let topic = &path[slash_pos + 1..];
                        properties.insert("brokers".to_string(), broker.to_string());
                        properties.insert("topic".to_string(), topic.to_string());
                    }
                    DataSourceType::Kafka
                }
                "s3" => {
                    // Parse s3://bucket/prefix format
                    if let Some(slash_pos) = path.find('/') {
                        let bucket = &path[..slash_pos];
                        let prefix = &path[slash_pos + 1..];
                        properties.insert("bucket".to_string(), bucket.to_string());
                        properties.insert("prefix".to_string(), prefix.to_string());
                    }
                    DataSourceType::S3
                }
                _ => DataSourceType::Generic(scheme.to_string()),
            };

            Ok((source_type, properties))
        } else {
            Err(SqlError::ParseError {
                message: format!("Invalid URI format: {}", uri),
                position: None,
            })
        }
    }

    /// Analyze INTO clause to extract sink requirements
    fn analyze_into_clause(
        &self,
        into_clause: &IntoClause,
        config: &HashMap<String, String>,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        let sink_name = &into_clause.sink_name;

        // Check if sink name is a URI (FR-047)
        if sink_name.contains("://") {
            // URI-based data sink
            self.analyze_uri_sink(sink_name, analysis)?;
        } else {
            // Named sink with traditional config
            // Merge main config with sink-specific properties from INTO clause
            let mut merged_config = config.clone();
            for (key, value) in &into_clause.sink_properties {
                merged_config.insert(key.clone(), value.clone());
            }

            // Parse serialization configuration
            let serialization_config = SerializationConfig::from_sql_params(&merged_config)
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Failed to parse serialization config: {}", e),
                    query: None,
                })?;

            self.analyze_sink(sink_name, &merged_config, &serialization_config, analysis)?;
        }
        Ok(())
    }

    /// Analyze URI-based data sink (FR-047)
    fn analyze_uri_sink(&self, uri: &str, analysis: &mut QueryAnalysis) -> Result<(), SqlError> {
        // Parse URI to determine sink type and base properties
        let (sink_type, properties) = self.parse_data_sink_uri(uri)?;

        let requirement = DataSinkRequirement {
            name: uri.to_string(),
            sink_type,
            properties,
            primary_keys: None,
        };

        analysis.required_sinks.push(requirement);
        Ok(())
    }

    /// Parse data sink URI into type and properties
    fn parse_data_sink_uri(
        &self,
        uri: &str,
    ) -> Result<(DataSinkType, HashMap<String, String>), SqlError> {
        let mut properties = HashMap::new();

        if let Some(scheme_end) = uri.find("://") {
            let scheme = &uri[..scheme_end];
            let path = &uri[scheme_end + 3..];

            properties.insert("uri".to_string(), uri.to_string());
            properties.insert("path".to_string(), path.to_string());

            let sink_type = match scheme {
                "file" => {
                    // Extract format from file extension if possible
                    if let Some(dot_pos) = path.rfind('.') {
                        let extension = &path[dot_pos + 1..];
                        let format = match extension {
                            "csv" => "csv",
                            "json" | "jsonl" => "json",
                            "parquet" => "parquet",
                            "avro" => "avro",
                            _ => "auto",
                        };
                        properties.insert("format".to_string(), format.to_string());
                    }
                    DataSinkType::File
                }
                "kafka" => {
                    // Parse kafka://broker:port/topic format
                    if let Some(slash_pos) = path.find('/') {
                        let broker = &path[..slash_pos];
                        let topic = &path[slash_pos + 1..];
                        properties.insert("brokers".to_string(), broker.to_string());
                        properties.insert("topic".to_string(), topic.to_string());
                    }
                    DataSinkType::Kafka
                }
                "s3" => {
                    // Parse s3://bucket/prefix format
                    if let Some(slash_pos) = path.find('/') {
                        let bucket = &path[..slash_pos];
                        let prefix = &path[slash_pos + 1..];
                        properties.insert("bucket".to_string(), bucket.to_string());
                        properties.insert("prefix".to_string(), prefix.to_string());
                    }
                    DataSinkType::S3
                }
                _ => DataSinkType::Generic(scheme.to_string()),
            };

            Ok((sink_type, properties))
        } else {
            Err(SqlError::ParseError {
                message: format!("Invalid URI format: {}", uri),
                position: None,
            })
        }
    }

    /// Merge analysis results from nested queries
    fn merge_analysis(&self, target: &mut QueryAnalysis, source: QueryAnalysis) {
        target.required_sources.extend(source.required_sources);
        target.required_sinks.extend(source.required_sinks);

        // Merge configuration, with target taking precedence
        for (key, value) in source.configuration {
            target.configuration.entry(key).or_insert(value);
        }
    }

    /// Extract Kafka-specific properties from configuration
    fn extract_kafka_properties(
        &self,
        config: &HashMap<String, String>,
        prefix: &str,
    ) -> HashMap<String, String> {
        let kafka_prefix = format!("{}.", prefix);
        config
            .iter()
            .filter_map(|(key, value)| {
                if key.starts_with(&kafka_prefix) && !key.contains("serializer") {
                    Some((key.clone(), value.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Extract file-specific properties from configuration  
    fn extract_file_properties(
        &self,
        config: &HashMap<String, String>,
        prefix: &str,
    ) -> HashMap<String, String> {
        let file_prefix = format!("{}.", prefix);
        config
            .iter()
            .filter_map(|(key, value)| {
                if key.starts_with(&file_prefix) {
                    Some((key.clone(), value.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Validate source properties against registered schemas
    fn validate_source_properties(
        &self,
        source_type: &str,
        properties: &HashMap<String, String>,
    ) -> Result<(), SqlError> {
        // Convert properties to the format expected by validate_configuration
        let global_config = HashMap::new(); // No global config in this context
        let named_config = HashMap::new(); // No named config in this context

        // Create a scoped property map with the source type prefix
        let mut scoped_properties = HashMap::new();
        for (key, value) in properties {
            // Add properties both with and without source type prefix for compatibility
            scoped_properties.insert(key.clone(), value.clone());
            scoped_properties.insert(format!("{}.{}", source_type, key), value.clone());
        }

        match validate_configuration(&global_config, &named_config, &scoped_properties) {
            Ok(()) => Ok(()),
            Err(validation_errors) => {
                let error_messages: Vec<String> =
                    validation_errors.iter().map(|e| format!("{}", e)).collect();
                let error_message = format!(
                    "Source '{}' configuration validation failed: {}",
                    source_type,
                    error_messages.join(", ")
                );
                Err(SqlError::ConfigurationError {
                    message: error_message,
                })
            }
        }
    }

    /// Validate sink properties against registered schemas
    fn validate_sink_properties(
        &self,
        sink_type: &str,
        properties: &HashMap<String, String>,
    ) -> Result<(), SqlError> {
        // Convert properties to the format expected by validate_configuration
        let global_config = HashMap::new(); // No global config in this context
        let named_config = HashMap::new(); // No named config in this context

        // Create a scoped property map with the sink type prefix
        let mut scoped_properties = HashMap::new();
        for (key, value) in properties {
            // Add properties both with and without sink type prefix for compatibility
            scoped_properties.insert(key.clone(), value.clone());
            scoped_properties.insert(format!("{}.{}", sink_type, key), value.clone());
        }

        match validate_configuration(&global_config, &named_config, &scoped_properties) {
            Ok(()) => Ok(()),
            Err(validation_errors) => {
                let error_messages: Vec<String> =
                    validation_errors.iter().map(|e| format!("{}", e)).collect();
                let error_message = format!(
                    "Sink '{}' configuration validation failed: {}",
                    sink_type,
                    error_messages.join(", ")
                );
                Err(SqlError::ConfigurationError {
                    message: error_message,
                })
            }
        }
    }

    /// Detect sink definitions from WITH clause configuration
    /// Looks for patterns like '{name}.type' = '{type}_sink'
    fn detect_sinks_from_config(
        &self,
        sink_name: &str,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        let config = &analysis.configuration.clone();

        // Look for sink configuration using direct name matching (e.g., output_stream.type)
        // This matches the naming convention used for sources, avoiding the _sink postfix requirement
        let sink_type_key = format!("{}.type", sink_name);

        if let Some(value) = config.get(&sink_type_key) {
            // Determine if this looks like a sink type (ends with _sink suffix)
            if value.ends_with("_sink") {
                // Extract sink type
                let sink_type_str = value.as_str();
                let sink_type = match sink_type_str {
                    "kafka_sink" => DataSinkType::Kafka,
                    "file_sink" => DataSinkType::File,
                    "s3_sink" => DataSinkType::S3,
                    "database_sink" => DataSinkType::Database,
                    "iceberg_sink" => DataSinkType::Iceberg,
                    other => {
                        log::warn!("Unknown sink type: {}", other);
                        DataSinkType::Generic(other.to_string())
                    }
                };

                // Extract sink-specific properties
                let sink_prefix = format!("{}.", sink_name);
                let mut properties: HashMap<String, String> = config
                    .iter()
                    .filter_map(|(k, v)| {
                        if k.starts_with(&sink_prefix) {
                            // Remove the sink name prefix
                            let key_without_prefix = k.strip_prefix(&sink_prefix).unwrap();
                            Some((key_without_prefix.to_string(), v.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();

                // Check if there's a config_file to load using centralized config_loader
                // This ensures consistent topic.name normalization across all components
                if let Some(config_file) = properties.get("config_file").cloned() {
                    log::debug!(
                        "Loading config file '{}' for sink '{}'",
                        config_file,
                        sink_name
                    );
                    match self.load_config_file_to_props(&config_file) {
                        Ok(file_props) => {
                            // Merge: YAML provides defaults, SQL properties override
                            // Note: file_props already has topic.name normalized to topic
                            for (yaml_key, yaml_value) in file_props {
                                properties.entry(yaml_key).or_insert(yaml_value);
                            }
                            log::debug!(
                                "Loaded {} properties from config file '{}'",
                                properties.len(),
                                config_file
                            );
                            // Remove config_file from properties since we've already loaded and merged it
                            // This prevents the config_loader from trying to reload it with a relative path
                            properties.remove("config_file");
                        }
                        Err(e) => {
                            log::warn!("Failed to load sink config file '{}': {}", config_file, e);
                        }
                    }
                } else {
                    log::debug!("No config_file specified for sink '{}'", sink_name);
                }

                // Apply topic normalization again in case SQL properties had topic.name
                // (centralized loader handles YAML normalization, this handles SQL WITH clause)
                normalize_topic_property(&mut properties);

                // Create sink requirement
                let sink_req = DataSinkRequirement {
                    name: sink_name.to_string(),
                    sink_type,
                    properties,
                    primary_keys: None, // Will be populated from query's key_fields after analysis
                };

                // Add to analysis if not already present
                if !analysis.required_sinks.iter().any(|s| s.name == sink_name) {
                    analysis.required_sinks.push(sink_req);
                }
            }
        }

        Ok(())
    }

    /// Extract key_fields from a query's nested SELECT clause
    /// Used to propagate PRIMARY KEY annotations to sink configuration
    fn extract_key_fields_from_query(query: &StreamingQuery) -> Option<Vec<String>> {
        match query {
            StreamingQuery::CreateStream { as_select, .. } => {
                Self::extract_key_fields_from_query(as_select)
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                Self::extract_key_fields_from_query(as_select)
            }
            StreamingQuery::Select { key_fields, .. } => key_fields.clone(),
            StreamingQuery::StartJob { query, .. } => Self::extract_key_fields_from_query(query),
            _ => None,
        }
    }
}
