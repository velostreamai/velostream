//! SQL Query Analysis for Dynamic Resource Creation
//!
//! This module analyzes parsed SQL queries to determine what Kafka consumers, producers,
//! and serializers need to be created dynamically based on the query requirements.

use crate::velostream::config::schema_registry::validate_configuration;
use crate::velostream::config::HierarchicalSchemaRegistry;
use crate::velostream::datasource::file::{FileDataSink, FileDataSource};
use crate::velostream::datasource::kafka::data_sink::KafkaDataSink;
use crate::velostream::datasource::kafka::data_source::KafkaDataSource;
use crate::velostream::kafka::serialization_format::SerializationConfig;
use crate::velostream::sql::{
    ast::{InsertSource, IntoClause, StreamSource, StreamingQuery},
    config::load_yaml_config,
    SqlError,
};
use std::collections::HashMap;
use std::future::Future;
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

/// SQL Query Analyzer for resource requirement extraction
pub struct QueryAnalyzer {
    /// Default consumer group base name
    default_group_id: String,
    /// Schema registry for property validation
    schema_registry: HierarchicalSchemaRegistry,
    /// Known table names (from table registry) that should not be validated as external sources
    known_tables: std::collections::HashSet<String>,
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
        }
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
                self.detect_sinks_from_config(&mut analysis)?;

                // VALIDATION: CSAS (CREATE STREAM AS SELECT) requires a sink configuration
                if analysis.required_sinks.is_empty() {
                    return Err(SqlError::ConfigurationError {
                        message: format!(
                            "CREATE STREAM '{}' requires a sink configuration. \
                            Define a sink in the WITH clause with '{}_sink.type' = 'kafka_sink' (or file_sink/s3_sink). \
                            Example: WITH ('{}_sink.type' = 'kafka_sink', '{}_sink.topic' = 'output_topic', ...)",
                            name, name, name, name
                        ),
                    });
                }
            }
            StreamingQuery::CreateTable {
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
        // Determine source type - EXPLICIT ONLY (no autodetection)
        // Uses simple compound type format: {name}.type = '{type}_source'
        // Examples: 'kafka_source', 'file_source', 's3_source'
        let source_type_str = config
            .get(&format!("{}.type", table_name))
            .map(|s| s.as_str())
            .ok_or_else(|| SqlError::ConfigurationError {
                message: format!(
                    "Source type must be explicitly specified for '{}'. Use: '{}.type' with values like 'kafka_source', 'file_source', 's3_source', 'database_source'",
                    table_name, table_name
                ),
            })?;

        let source_type = match source_type_str {
            "kafka_source" => DataSourceType::Kafka,
            "file_source" => DataSourceType::File,
            "s3_source" => DataSourceType::S3,
            "database_source" => DataSourceType::Database,
            other => return Err(SqlError::ConfigurationError {
                message: format!(
                    "Invalid source type '{}' for '{}'. Supported values: 'kafka_source', 'file_source', 's3_source', 'database_source'",
                    other, table_name
                ),
            }),
        };

        // Build properties map from named source configuration
        let mut properties = HashMap::new();
        let source_prefix = format!("{}.", table_name);

        // Check for config_file and load YAML configuration
        let config_file_key = format!("{}.config_file", table_name);
        if let Some(config_file_path) = config.get(&config_file_key) {
            match load_yaml_config(config_file_path) {
                Ok(yaml_config) => {
                    // Convert YAML config to properties map with proper nested flattening
                    if let Some(mapping) = yaml_config.config.as_mapping() {
                        for (key, value) in mapping {
                            if let Some(key_str) = key.as_str() {
                                match key_str {
                                    "datasource" => {
                                        // Handle nested datasource configuration with proper prefix
                                        if let Some(datasource_map) = value.as_mapping() {
                                            for (nested_key, nested_value) in datasource_map {
                                                if let Some(nested_k) = nested_key.as_str() {
                                                    if nested_k == "consumer_config" {
                                                        // Flatten consumer_config with datasource prefix
                                                        if let Some(consumer_map) =
                                                            nested_value.as_mapping()
                                                        {
                                                            for (consumer_key, consumer_value) in
                                                                consumer_map
                                                            {
                                                                if let (Some(ck), Some(cv)) = (
                                                                    consumer_key.as_str(),
                                                                    consumer_value.as_str(),
                                                                ) {
                                                                    // Create the full flattened path that validation expects
                                                                    properties.insert(
                                                                        format!(
                                                                            "datasource.consumer_config.{}",
                                                                            ck
                                                                        ),
                                                                        cv.to_string(),
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    } else if let Some(nv) = nested_value.as_str() {
                                                        // Add datasource prefix for direct properties
                                                        properties.insert(
                                                            format!("datasource.{}", nested_k),
                                                            nv.to_string(),
                                                        );
                                                    } else {
                                                        // Handle non-string nested values with datasource prefix
                                                        let value_str = match nested_value {
                                                            serde_yaml::Value::Number(n) => {
                                                                n.to_string()
                                                            }
                                                            serde_yaml::Value::Bool(b) => {
                                                                b.to_string()
                                                            }
                                                            serde_yaml::Value::Null => {
                                                                "null".to_string()
                                                            }
                                                            serde_yaml::Value::Sequence(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::Mapping(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::Tagged(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::String(s) => {
                                                                s.clone()
                                                            }
                                                        };
                                                        properties.insert(
                                                            format!("datasource.{}", nested_k),
                                                            value_str,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        // Handle top-level keys
                                        if let Some(value_str) = value.as_str() {
                                            properties
                                                .insert(key_str.to_string(), value_str.to_string());
                                        } else {
                                            // Handle non-string values (convert to string)
                                            let value_str = match value {
                                                serde_yaml::Value::Number(n) => n.to_string(),
                                                serde_yaml::Value::Bool(b) => b.to_string(),
                                                serde_yaml::Value::Null => "null".to_string(),
                                                serde_yaml::Value::Sequence(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::Mapping(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::Tagged(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::String(s) => s.clone(),
                                            };
                                            properties.insert(key_str.to_string(), value_str);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    println!(
                        "✅ Loaded config from {}: {} properties",
                        config_file_path,
                        properties.len()
                    );
                }
                Err(e) => {
                    return Err(SqlError::ConfigurationError {
                        message: format!(
                            "Failed to load config file '{}' for source '{}': {}",
                            config_file_path, table_name, e
                        ),
                    });
                }
            }
        }

        // Add all source-specific properties (e.g., "kafka_source.bootstrap.servers")
        for (key, value) in config {
            if key.starts_with(&source_prefix) {
                // Convert to standard property format (remove source name prefix)
                let standard_key = key[source_prefix.len()..].to_string();
                properties.insert(standard_key, value.clone());

                // Also preserve original key for legacy compatibility during transition
                properties.insert(key.clone(), value.clone());
            }
        }

        // Add default properties and validate using schema
        match source_type {
            DataSourceType::Kafka => {
                // Ensure we have broker and topic info
                if !properties.contains_key("bootstrap.servers")
                    && !properties.contains_key("brokers")
                {
                    properties.insert(
                        "bootstrap.servers".to_string(),
                        "localhost:9092".to_string(),
                    );
                }
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
        // Determine sink type - EXPLICIT ONLY (no autodetection)
        // Uses simple compound type format: {name}.type = '{type}_sink'
        // Examples: 'kafka_sink', 'file_sink', 's3_sink'
        let sink_type_str = config
            .get(&format!("{}.type", table_name))
            .map(|s| s.as_str())
            .ok_or_else(|| SqlError::ConfigurationError {
                message: format!(
                    "Sink type must be explicitly specified for '{}'. Use: '{}.type' with values like 'kafka_sink', 'file_sink', 's3_sink', 'database_sink', 'iceberg_sink'",
                    table_name, table_name
                ),
            })?;

        let sink_type = match sink_type_str {
            "kafka_sink" => DataSinkType::Kafka,
            "file_sink" => DataSinkType::File,
            "s3_sink" => DataSinkType::S3,
            "database_sink" => DataSinkType::Database,
            "iceberg_sink" => DataSinkType::Iceberg,
            other => return Err(SqlError::ConfigurationError {
                message: format!(
                    "Invalid sink type '{}' for '{}'. Supported values: 'kafka_sink', 'file_sink', 's3_sink', 'database_sink', 'iceberg_sink'",
                    other, table_name
                ),
            }),
        };

        // Build properties map from named sink configuration
        let mut properties = HashMap::new();
        let sink_prefix = format!("{}.", table_name);

        // Check for config_file and load YAML configuration
        let config_file_key = format!("{}.config_file", table_name);
        if let Some(config_file_path) = config.get(&config_file_key) {
            match load_yaml_config(config_file_path) {
                Ok(yaml_config) => {
                    // Convert YAML config to properties map with proper nested flattening
                    if let Some(mapping) = yaml_config.config.as_mapping() {
                        for (key, value) in mapping {
                            if let Some(key_str) = key.as_str() {
                                match key_str {
                                    "datasink" => {
                                        // Handle nested datasink configuration with proper prefix
                                        if let Some(datasink_map) = value.as_mapping() {
                                            for (nested_key, nested_value) in datasink_map {
                                                if let Some(nested_k) = nested_key.as_str() {
                                                    if nested_k == "producer_config" {
                                                        // Flatten producer_config with datasink prefix
                                                        if let Some(producer_map) =
                                                            nested_value.as_mapping()
                                                        {
                                                            for (producer_key, producer_value) in
                                                                producer_map
                                                            {
                                                                if let (Some(pk), Some(pv)) = (
                                                                    producer_key.as_str(),
                                                                    producer_value.as_str(),
                                                                ) {
                                                                    // Create the full flattened path that validation expects
                                                                    properties.insert(
                                                                        format!(
                                                                            "datasink.producer_config.{}",
                                                                            pk
                                                                        ),
                                                                        pv.to_string(),
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    } else if let Some(nv) = nested_value.as_str() {
                                                        // Add datasink prefix for direct properties
                                                        properties.insert(
                                                            format!("datasink.{}", nested_k),
                                                            nv.to_string(),
                                                        );
                                                    } else {
                                                        // Handle non-string nested values with datasink prefix
                                                        let value_str = match nested_value {
                                                            serde_yaml::Value::Number(n) => {
                                                                n.to_string()
                                                            }
                                                            serde_yaml::Value::Bool(b) => {
                                                                b.to_string()
                                                            }
                                                            serde_yaml::Value::Null => {
                                                                "null".to_string()
                                                            }
                                                            serde_yaml::Value::Sequence(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::Mapping(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::Tagged(_) => {
                                                                format!("{:?}", nested_value)
                                                            }
                                                            serde_yaml::Value::String(s) => {
                                                                s.clone()
                                                            }
                                                        };
                                                        properties.insert(
                                                            format!("datasink.{}", nested_k),
                                                            value_str,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        // Handle top-level keys
                                        if let Some(value_str) = value.as_str() {
                                            properties
                                                .insert(key_str.to_string(), value_str.to_string());
                                        } else {
                                            // Handle non-string values (convert to string)
                                            let value_str = match value {
                                                serde_yaml::Value::Number(n) => n.to_string(),
                                                serde_yaml::Value::Bool(b) => b.to_string(),
                                                serde_yaml::Value::Null => "null".to_string(),
                                                serde_yaml::Value::Sequence(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::Mapping(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::Tagged(_) => {
                                                    format!("{:?}", value)
                                                }
                                                serde_yaml::Value::String(s) => s.clone(),
                                            };
                                            properties.insert(key_str.to_string(), value_str);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    println!(
                        "✅ Loaded sink config from {}: {} properties",
                        config_file_path,
                        properties.len()
                    );
                }
                Err(e) => {
                    return Err(SqlError::ConfigurationError {
                        message: format!(
                            "Failed to load config file '{}' for sink '{}': {}",
                            config_file_path, table_name, e
                        ),
                    });
                }
            }
        }

        // Add all sink-specific properties (e.g., "kafka_sink.bootstrap.servers")
        for (key, value) in config {
            if key.starts_with(&sink_prefix) {
                // Convert to standard property format (remove sink name prefix)
                let standard_key = key[sink_prefix.len()..].to_string();
                properties.insert(standard_key, value.clone());

                // Also preserve original key for legacy compatibility during transition
                properties.insert(key.clone(), value.clone());
            }
        }

        // Add default properties and validate using schema
        match sink_type {
            DataSinkType::Kafka => {
                // Ensure we have broker and topic info
                if !properties.contains_key("bootstrap.servers")
                    && !properties.contains_key("brokers")
                {
                    properties.insert(
                        "bootstrap.servers".to_string(),
                        "localhost:9092".to_string(),
                    );
                }
                if !properties.contains_key("topic") {
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
    fn detect_sinks_from_config(&self, analysis: &mut QueryAnalysis) -> Result<(), SqlError> {
        let config = &analysis.configuration.clone();

        // Scan for sink type definitions
        for (key, value) in config {
            if key.ends_with(".type") && value.ends_with("_sink") {
                // Extract sink name from key (remove .type suffix)
                let sink_name = key.strip_suffix(".type").unwrap();

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

                // Check if there's a config_file to load
                if let Some(config_file) = properties.get("config_file") {
                    // Load and merge YAML configuration
                    match super::config::yaml_loader::load_yaml_config(config_file) {
                        Ok(yaml_config) => {
                            // Convert YAML to debug string format (same as source logic)
                            // This maintains compatibility with existing serialization detection
                            if let Some(schema) = yaml_config.config.get("schema") {
                                properties.insert("schema".to_string(), format!("{:?}", schema));
                            }
                            if let Some(topic) = yaml_config.config.get("topic") {
                                properties.insert("topic".to_string(), format!("{:?}", topic));
                            }
                            // Flatten nested YAML structure for sink configs
                            if let Some(mapping) = yaml_config.config.as_mapping() {
                                for (key, value) in mapping {
                                    if let Some(k) = key.as_str() {
                                        match k {
                                            "datasink" => {
                                                // Handle nested datasink configuration
                                                if let Some(datasink_map) = value.as_mapping() {
                                                    for (nested_key, nested_value) in datasink_map {
                                                        if let Some(nested_k) = nested_key.as_str()
                                                        {
                                                            if nested_k == "producer_config" {
                                                                // Flatten producer_config
                                                                if let Some(producer_map) =
                                                                    nested_value.as_mapping()
                                                                {
                                                                    for (prod_key, prod_value) in
                                                                        producer_map
                                                                    {
                                                                        if let (
                                                                            Some(pk),
                                                                            Some(pv),
                                                                        ) = (
                                                                            prod_key.as_str(),
                                                                            prod_value.as_str(),
                                                                        ) {
                                                                            properties.insert(format!("datasink.producer_config.{}", pk), pv.to_string());
                                                                        }
                                                                    }
                                                                }
                                                            } else if let Some(nv) =
                                                                nested_value.as_str()
                                                            {
                                                                properties
                                                                    .entry(nested_k.to_string())
                                                                    .or_insert(nv.to_string());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            _ => {
                                                // Handle direct string values
                                                if let Some(v) = value.as_str() {
                                                    properties
                                                        .entry(k.to_string())
                                                        .or_insert(v.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("Failed to load sink config file '{}': {}", config_file, e);
                        }
                    }
                }

                // Create sink requirement
                let sink_req = DataSinkRequirement {
                    name: sink_name.to_string(),
                    sink_type,
                    properties,
                };

                // Add to analysis if not already present
                if !analysis.required_sinks.iter().any(|s| s.name == sink_name) {
                    analysis.required_sinks.push(sink_req);
                }
            }
        }

        Ok(())
    }
}
