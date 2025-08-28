//! SQL Query Analysis for Dynamic Resource Creation
//!
//! This module analyzes parsed SQL queries to determine what Kafka consumers, producers,
//! and serializers need to be created dynamically based on the query requirements.

use crate::ferris::kafka::serialization_format::{SerializationConfig, SerializationFormat};
use crate::ferris::sql::{
    ast::{IntoClause, StreamSource, StreamingQuery},
    SqlError,
};
use std::collections::HashMap;

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
}

impl QueryAnalyzer {
    /// Create a new query analyzer
    pub fn new(default_group_id: String) -> Self {
        Self { default_group_id }
    }

    /// Analyze a parsed SQL query to determine required resources
    pub fn analyze(&self, query: &StreamingQuery) -> Result<QueryAnalysis, SqlError> {
        let mut analysis = QueryAnalysis {
            required_sources: Vec::new(),
            required_sinks: Vec::new(),
            configuration: HashMap::new(),
        };

        match query {
            StreamingQuery::Select { from, .. } => {
                // For simple SELECT queries, extract source from FROM clause
                self.analyze_from_clause(from, &mut analysis)?;
            }
            StreamingQuery::CreateStream {
                as_select,
                properties,
                ..
            } => {
                // Extract configuration from properties
                for (key, value) in properties {
                    analysis.configuration.insert(key.clone(), value.clone());
                }
                // Recursively analyze the nested SELECT
                let nested_analysis = self.analyze(as_select)?;
                self.merge_analysis(&mut analysis, nested_analysis);
            }
            StreamingQuery::CreateStreamInto {
                as_select,
                into_clause,
                properties,
                ..
            } => {
                // Extract configuration from properties (convert to legacy format)
                let legacy_props = properties.clone().into_legacy_format();
                for (key, value) in &legacy_props {
                    analysis.configuration.insert(key.clone(), value.clone());
                }
                // Recursively analyze the nested SELECT
                let nested_analysis = self.analyze(as_select)?;
                self.merge_analysis(&mut analysis, nested_analysis);

                // Analyze the INTO clause for sink requirements
                self.analyze_into_clause(into_clause, &legacy_props, &mut analysis)?;
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
                // Recursively analyze the nested SELECT
                let nested_analysis = self.analyze(as_select)?;
                self.merge_analysis(&mut analysis, nested_analysis);
            }
            StreamingQuery::CreateTableInto {
                as_select,
                into_clause,
                properties,
                ..
            } => {
                // Extract configuration from properties (convert to legacy format)
                let legacy_props = properties.clone().into_legacy_format();
                for (key, value) in &legacy_props {
                    analysis.configuration.insert(key.clone(), value.clone());
                }
                // Recursively analyze the nested SELECT
                let nested_analysis = self.analyze(as_select)?;
                self.merge_analysis(&mut analysis, nested_analysis);

                // Analyze the INTO clause for sink requirements
                self.analyze_into_clause(into_clause, &legacy_props, &mut analysis)?;
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
            StreamingQuery::InsertInto { .. } => {
                // INSERT INTO queries may require producers, but for now skip analysis
            }
            StreamingQuery::Update { .. } => {
                // UPDATE queries may require producers, but for now skip analysis
            }
            StreamingQuery::Delete { .. } => {
                // DELETE queries may require producers, but for now skip analysis
            }
        }

        Ok(analysis)
    }

    /// Analyze a source table to determine datasource requirements
    fn analyze_source(
        &self,
        table_name: &str,
        config: &HashMap<String, String>,
        _serialization_config: &SerializationConfig,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        // Determine source type from config or table name pattern
        let source_type_str = config
            .get("source.type")
            .map(|s| s.as_str())
            .unwrap_or_else(|| self.infer_source_type(table_name));

        let source_type = match source_type_str {
            "kafka" => DataSourceType::Kafka,
            "file" => DataSourceType::File,
            "s3" => DataSourceType::S3,
            "database" => DataSourceType::Database,
            other => DataSourceType::Generic(other.to_string()),
        };

        // Build properties map including all source config
        let mut properties = HashMap::new();

        // Add all source.* properties
        for (key, value) in config {
            if key.starts_with("source.") {
                properties.insert(key.clone(), value.clone());
            }
        }

        // Add serialization properties if Kafka
        if source_type == DataSourceType::Kafka {
            // Ensure we have broker and topic info
            if !properties.contains_key("source.brokers") {
                properties.insert(
                    "source.brokers".to_string(),
                    config
                        .get("brokers")
                        .cloned()
                        .unwrap_or_else(|| "localhost:9092".to_string()),
                );
            }
            if !properties.contains_key("source.topic") {
                properties.insert("source.topic".to_string(), table_name.to_string());
            }
            if !properties.contains_key("source.group.id") {
                properties.insert(
                    "source.group.id".to_string(),
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
    fn analyze_sink(
        &self,
        table_name: &str,
        config: &HashMap<String, String>,
        _serialization_config: &SerializationConfig,
        analysis: &mut QueryAnalysis,
    ) -> Result<(), SqlError> {
        // Determine sink type from config or table name pattern
        let sink_type_str = config
            .get("sink.type")
            .map(|s| s.as_str())
            .unwrap_or_else(|| self.infer_sink_type(table_name));

        let sink_type = match sink_type_str {
            "kafka" => DataSinkType::Kafka,
            "file" => DataSinkType::File,
            "s3" => DataSinkType::S3,
            "database" => DataSinkType::Database,
            "iceberg" => DataSinkType::Iceberg,
            other => DataSinkType::Generic(other.to_string()),
        };

        // Build properties map including all sink config
        let mut properties = HashMap::new();

        // Add all sink.* properties
        for (key, value) in config {
            if key.starts_with("sink.") {
                properties.insert(key.clone(), value.clone());
            }
        }

        // Add serialization properties if Kafka
        if sink_type == DataSinkType::Kafka {
            // Ensure we have broker and topic info
            if !properties.contains_key("sink.brokers") {
                properties.insert(
                    "sink.brokers".to_string(),
                    config
                        .get("brokers")
                        .cloned()
                        .unwrap_or_else(|| "localhost:9092".to_string()),
                );
            }
            if !properties.contains_key("sink.topic") {
                properties.insert("sink.topic".to_string(), table_name.to_string());
            }

            // Add serialization formats
            if let Some(key_fmt) = config.get("key.serializer") {
                properties.insert("key.serializer".to_string(), key_fmt.clone());
            }
            if let Some(val_fmt) = config.get("value.serializer") {
                properties.insert("value.serializer".to_string(), val_fmt.clone());
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
        // Parse URI to determine source type and properties
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
            // Parse serialization configuration
            let serialization_config =
                SerializationConfig::from_sql_params(config).map_err(|e| {
                    SqlError::ExecutionError {
                        message: format!("Failed to parse serialization config: {}", e),
                        query: None,
                    }
                })?;

            self.analyze_sink(sink_name, config, &serialization_config, analysis)?;
        }
        Ok(())
    }

    /// Analyze URI-based data sink (FR-047)
    fn analyze_uri_sink(&self, uri: &str, analysis: &mut QueryAnalysis) -> Result<(), SqlError> {
        // Parse URI to determine sink type and properties
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

    /// Infer source type from table name patterns
    fn infer_source_type(&self, table_name: &str) -> &str {
        if table_name.contains("kafka") || table_name.ends_with("_topic") {
            "kafka"
        } else if table_name.contains("file")
            || table_name.contains(".csv")
            || table_name.contains(".json")
        {
            "file"
        } else {
            "kafka" // Default to Kafka
        }
    }

    /// Infer sink type from table name patterns
    fn infer_sink_type(&self, table_name: &str) -> &str {
        if table_name.contains("kafka") || table_name.ends_with("_topic") {
            "kafka"
        } else if table_name.contains("file")
            || table_name.contains(".csv")
            || table_name.contains(".json")
        {
            "file"
        } else {
            "kafka" // Default to Kafka
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
}
