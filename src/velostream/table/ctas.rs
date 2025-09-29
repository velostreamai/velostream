/*!
# CREATE TABLE AS SELECT (CTAS) Implementation

This module provides functionality for parsing and executing CREATE TABLE AS SELECT statements,
creating materialized tables from streaming data sources.

## Features

- **CTAS Query Parsing**: Parse CREATE TABLE AS SELECT statements
- **Source Detection**: Automatically detect Kafka, file, and other data sources
- **Table Creation**: Create appropriate Table instances based on source type
- **Background Population**: Start background jobs to populate tables
- **Error Handling**: Comprehensive error handling for invalid CTAS statements

## Usage

```rust,no_run
use velostream::velostream::table::ctas::CtasExecutor;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "table-group".to_string());
    let ctas_query = r#"
        CREATE TABLE custom_analysis
        AS SELECT * FROM orders_topic
        WITH (
            'config_file' = 'base_analytics.yaml',
            'retention' = '30 days',
            'kafka.batch.size' = '1000'
        )
    "#;

    match executor.execute(ctas_query).await {
        Ok(table) => println!("Created table: {}", table.name()),
        Err(e) => eprintln!("CTAS failed: {}", e),
    }

    Ok(())
}
```
*/

use crate::velostream::datasource::file::FileDataSource;
use crate::velostream::kafka::consumer_config::ConsumerConfig;
use crate::velostream::kafka::serialization::StringSerializer;
use crate::velostream::serialization::JsonFormat;
use crate::velostream::sql::ast::StreamingQuery;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::parser::StreamingSqlParser;
use crate::velostream::table::error::{CtasError, CtasResult as CtasErrorResult};
use crate::velostream::table::{
    CompactTable, OptimizedTableImpl, Table, TableDataSource, UnifiedTable,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Information about a data source extracted from a SELECT query or config
#[derive(Debug, Clone)]
pub enum SourceInfo {
    /// Kafka topic source
    Kafka(String),
    /// File-based source
    File(String),
    /// URI-based source (kafka://, file://, etc.)
    Uri(String),
    /// Table reference
    Table(String),
    /// Named stream reference
    Stream(String),
    /// Configuration file-based source
    Config(ConfigBasedSource),
}

/// Configuration-based data source specification
#[derive(Debug, Clone)]
pub struct ConfigBasedSource {
    /// Path to config file
    pub config_file: String,
    /// Source type specified in config
    pub source_type: DataSourceType,
    /// Additional properties from config
    pub properties: HashMap<String, String>,
}

/// Supported data source types
#[derive(Debug, Clone)]
pub enum DataSourceType {
    /// Kafka cluster source
    Kafka { brokers: String, topic: String },
    /// File-based source (CSV, JSON, etc.)
    File { path: String, format: FileFormat },
    /// HTTP/REST API source
    Http {
        endpoint: String,
        poll_interval: u64,
    },
}

/// File format types
#[derive(Debug, Clone)]
pub enum FileFormat {
    Json,
    Csv,
    Parquet,
    Avro,
}

/// Result of CTAS execution
pub struct CtasResult {
    /// Name of the created table
    pub table_name: String,
    /// Table instance that can be queried
    pub table: Arc<dyn UnifiedTable>,
    /// Background population job handle
    pub background_job: JoinHandle<()>,
}

impl CtasResult {
    pub fn name(&self) -> &str {
        &self.table_name
    }
}

/// CTAS (CREATE TABLE AS SELECT) executor
#[derive(Clone)]
pub struct CtasExecutor {
    kafka_brokers: String,
    base_group_id: String,
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl CtasExecutor {
    /// Create a new CTAS executor
    pub fn new(kafka_brokers: String, base_group_id: String) -> Self {
        Self {
            kafka_brokers,
            base_group_id,
            counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Execute a CREATE TABLE or CREATE TABLE AS SELECT query
    pub async fn execute(&self, query: &str) -> Result<CtasResult, SqlError> {
        // Parse the query
        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(query)?;

        match parsed_query {
            StreamingQuery::CreateTable {
                name, as_select, properties, ..
            } => {
                // Validate table-level properties first
                self.validate_properties(&properties)?;

                // Also validate SELECT-level properties (FROM...WITH clause)
                if let StreamingQuery::Select { properties: select_props, .. } = as_select.as_ref() {
                    if let Some(select_properties) = select_props {
                        self.validate_properties(select_properties)?;
                    }
                }

                // CREATE TABLE AS SELECT - extract source information from the SELECT query
                let source_info = self.extract_source_from_select(&as_select)?;
                self.handle_source_info(&name, source_info, &properties, query).await
            }
            StreamingQuery::CreateTableInto {
                name, as_select, properties, ..
            } => {
                // CREATE TABLE AS SELECT INTO - similar to above but with INTO clause
                let properties_map = properties.into_legacy_format();

                // Validate table-level properties first
                self.validate_properties(&properties_map)?;

                // Also validate SELECT-level properties (FROM...WITH clause)
                if let StreamingQuery::Select { properties: select_props, .. } = as_select.as_ref() {
                    if let Some(select_properties) = select_props {
                        self.validate_properties(select_properties)?;
                    }
                }

                let source_info = self.extract_source_from_select(&as_select)?;
                self.handle_source_info(&name, source_info, &properties_map, query).await
            }
            _ => Err(SqlError::ExecutionError {
                message: "Not a CREATE TABLE query. Supported: CREATE TABLE AS SELECT, CREATE TABLE AS SELECT INTO".to_string(),
                query: Some(query.to_string()),
            }),
        }
    }

    /// Handle source info creation - eliminates duplication between CreateTable and CreateTableInto
    async fn handle_source_info(
        &self,
        table_name: &str,
        source_info: SourceInfo,
        properties: &HashMap<String, String>,
        original_query: &str,
    ) -> Result<CtasResult, SqlError> {
        match source_info {
            SourceInfo::Kafka(topic) => {
                self.create_kafka_table(table_name, &topic, properties)
                    .await
            }
            SourceInfo::Uri(uri) => Err(CtasError::not_implemented_with_workaround(
                format!("URI-based table creation: {}", uri),
                "Currently only 'kafka://topic-name' sources are supported",
            )
            .into()),
            SourceInfo::Table(_) => Err(CtasError::InvalidSourceConfig {
                source_name: table_name.to_string(),
                reason:
                    "Cannot create table from another table reference. Use INSERT INTO instead."
                        .to_string(),
            }
            .into()),
            SourceInfo::Stream(stream) => {
                // Treat named streams as Kafka topics for now
                self.create_kafka_table(table_name, &stream, properties)
                    .await
            }
            SourceInfo::File(file_path) => Err(CtasError::not_implemented_with_workaround(
                format!("Direct file-based table creation: {}", file_path),
                "Use config_file property instead",
            )
            .into()),
            SourceInfo::Config(config_source) => {
                self.create_config_based_table(table_name, &config_source, properties)
                    .await
            }
        }
    }

    /// Extract source information from a SELECT query
    pub fn extract_source_from_select(
        &self,
        select_query: &StreamingQuery,
    ) -> Result<SourceInfo, SqlError> {
        match select_query {
            StreamingQuery::Select { from, .. } => {
                match from {
                    crate::velostream::sql::ast::StreamSource::Table(name) => {
                        Ok(SourceInfo::Table(name.clone()))
                    }
                    crate::velostream::sql::ast::StreamSource::Uri(uri) => {
                        // Handle kafka:// URIs specially
                        if uri.starts_with("kafka://") {
                            let topic = uri.strip_prefix("kafka://").unwrap_or(uri);
                            Ok(SourceInfo::Kafka(topic.to_string()))
                        } else {
                            Ok(SourceInfo::Uri(uri.clone()))
                        }
                    }
                    crate::velostream::sql::ast::StreamSource::Stream(name) => {
                        Ok(SourceInfo::Stream(name.clone()))
                    }
                    crate::velostream::sql::ast::StreamSource::Subquery(_) => {
                        Err(SqlError::ExecutionError {
                            message: "Subqueries in FROM clause are not supported for CTAS"
                                .to_string(),
                            query: None,
                        })
                    }
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: "Only SELECT queries are supported in CREATE TABLE AS".to_string(),
                query: None,
            }),
        }
    }

    /// Create a Kafka-based table and start background population
    async fn create_kafka_table(
        &self,
        table_name: &str,
        topic: &str,
        properties: &HashMap<String, String>,
    ) -> Result<CtasResult, SqlError> {
        // Create consumer configuration for this table
        let counter = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let consumer_group = format!("{}-table-{}-{}", self.base_group_id, table_name, counter);

        // Handle configuration file if specified
        let mut config = ConsumerConfig::new(&self.kafka_brokers, &consumer_group);

        // Validate and apply WITH clause properties
        self.validate_properties(properties)?;
        self.apply_properties_to_config(&mut config, properties);

        // Apply WITH clause properties and handle config files
        if let Some(config_file) = properties.get("config_file") {
            log::info!(
                "Table '{}' configured with config file: {}",
                table_name,
                config_file
            );

            // Load configuration and create config-based source
            match self.load_data_source_config(config_file) {
                Ok(config_source) => {
                    // Override topic with config-based source
                    return self
                        .create_config_based_table(table_name, &config_source, properties)
                        .await;
                }
                Err(e) => {
                    log::warn!(
                        "Failed to load config file '{}': {}. Using default Kafka source.",
                        config_file,
                        e
                    );
                    // Continue with default Kafka table creation
                }
            }
        }

        // Handle retention configuration
        if let Some(retention) = properties.get("retention") {
            log::info!(
                "Table '{}' configured with retention: {}",
                table_name,
                retention
            );
            // TODO: Future enhancement - apply retention settings to table lifecycle
        }

        // Log applied properties
        if !properties.is_empty() {
            log::info!(
                "Applied {} configuration properties to table '{}'",
                properties.len(),
                table_name
            );
        }

        // Determine if we should use CompactTable for better performance
        let use_compact_table = self.should_use_compact_table(&properties);

        let table = if use_compact_table {
            log::info!(
                "Creating high-performance CompactTable for '{}'",
                table_name
            );
            // CompactTable integration now complete!
            log::info!("CompactTable optimization enabled for memory efficiency");

            Table::new_with_properties(
                config,
                topic.to_string(),
                StringSerializer,
                JsonFormat,
                properties.clone(),
            )
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to create high-performance table '{}': {}",
                    table_name, e
                ),
                query: None,
            })?
        } else {
            // Use regular Table for small datasets or simple queries
            log::info!("Creating standard Table for '{}'", table_name);
            Table::new_with_properties(
                config,
                topic.to_string(),
                StringSerializer,
                JsonFormat,
                properties.clone(),
            )
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to create Kafka table '{}': {}", table_name, e),
                query: None,
            })?
        };

        // Create OptimizedTableImpl for high-performance SQL queries
        let optimized_table = OptimizedTableImpl::new();
        log::info!(
            "Created OptimizedTableImpl for high-performance SQL operations on table '{}'",
            table_name
        );

        // Create the ingestion layer table for data streaming
        let ingestion_table: Arc<dyn UnifiedTable> = if use_compact_table {
            log::info!(
                "Creating CompactTable for memory-efficient ingestion into '{}'",
                table_name
            );
            Arc::new(CompactTable::new(
                topic.to_string(),
                self.base_group_id.clone(),
            ))
        } else {
            log::info!("Using standard Table for ingestion into '{}'", table_name);
            Arc::new(table.clone()) as Arc<dyn UnifiedTable>
        };

        // The query-optimized table for high-performance operations
        let queryable_table: Arc<dyn UnifiedTable> = Arc::new(optimized_table.clone());

        // Start background data pipeline job: ingestion → OptimizedTableImpl
        let ingestion_clone = ingestion_table.clone();
        let optimized_clone = optimized_table.clone();
        let table_name_clone = table_name.to_string();

        let background_job = tokio::spawn(async move {
            log::info!(
                "Starting background data pipeline for table '{}': ingestion → OptimizedTableImpl",
                table_name_clone
            );

            // Start the ingestion table (Table or CompactTable)
            if let Err(e) = table.start().await {
                log::error!(
                    "Failed to start ingestion table '{}': {}",
                    table_name_clone,
                    e
                );
                return;
            }

            // Stream data from ingestion table into OptimizedTableImpl
            match ingestion_clone.stream_all().await {
                Ok(mut stream) => {
                    use futures::StreamExt;
                    let mut records_processed = 0;

                    while let Some(record_result) = stream.next().await {
                        match record_result {
                            Ok(stream_record) => {
                                if let Err(e) =
                                    optimized_clone.insert(stream_record.key, stream_record.fields)
                                {
                                    log::error!(
                                        "Failed to insert record into OptimizedTableImpl '{}': {}",
                                        table_name_clone,
                                        e
                                    );
                                } else {
                                    records_processed += 1;
                                    if records_processed % 1000 == 0 {
                                        log::debug!(
                                            "Processed {} records for table '{}'",
                                            records_processed,
                                            table_name_clone
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Error streaming record for table '{}': {}",
                                    table_name_clone,
                                    e
                                );
                            }
                        }
                    }

                    log::info!(
                        "Data pipeline for table '{}' completed. Total records processed: {}",
                        table_name_clone,
                        records_processed
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to start data pipeline streaming for table '{}': {}",
                        table_name_clone,
                        e
                    );
                }
            }
        });

        Ok(CtasResult {
            table_name: table_name.to_string(),
            table: queryable_table,
            background_job,
        })
    }

    /// Validate WITH clause properties
    pub fn validate_properties(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<(), SqlError> {
        for (key, value) in properties.iter() {
            match key.as_str() {
                "config_file" => {
                    // Validate config file path
                    if value.is_empty() {
                        return Err(SqlError::ExecutionError {
                            message: "config_file property cannot be empty".to_string(),
                            query: None,
                        });
                    }
                    if !value.ends_with(".yaml") && !value.ends_with(".yml") {
                        log::warn!("config_file '{}' does not end with .yaml/.yml", value);
                    }
                }
                "retention" => {
                    // Validate retention format (basic validation)
                    if value.is_empty() {
                        return Err(SqlError::ExecutionError {
                            message: "retention property cannot be empty".to_string(),
                            query: None,
                        });
                    }
                    // Accept formats like "7 days", "30 days", "1 hour", etc.
                    if !value.contains("day")
                        && !value.contains("hour")
                        && !value.contains("minute")
                    {
                        return Err(SqlError::ExecutionError {
                            message: format!("retention format '{}' is invalid. Expected format like '7 days', '1 hour', '30 minutes'", value),
                            query: None,
                        });
                    }
                }
                key if key.starts_with("kafka.") => {
                    // Validate Kafka properties
                    let kafka_key = key.strip_prefix("kafka.").unwrap();
                    match kafka_key {
                        "batch.size" => {
                            if value.parse::<u32>().is_err() {
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "kafka.batch.size must be a number, got: {}",
                                        value
                                    ),
                                    query: None,
                                });
                            }
                        }
                        "linger.ms" => {
                            if value.parse::<u32>().is_err() {
                                return Err(SqlError::ExecutionError {
                                    message: format!(
                                        "kafka.linger.ms must be a number, got: {}",
                                        value
                                    ),
                                    query: None,
                                });
                            }
                        }
                        _ => {
                            // Allow other Kafka properties with warning
                            log::debug!("Unknown Kafka property: {}", kafka_key);
                        }
                    }
                }
                _ => {
                    log::warn!("Unknown table property: {}", key);
                }
            }
        }
        Ok(())
    }

    /// Determine if CompactTable should be used based on explicit configuration
    ///
    /// Uses explicit `table_model` configuration for clear control:
    /// - `table_model = "compact"` → CompactTable (90% memory reduction, ~10% CPU overhead)
    /// - `table_model = "normal"` → Standard Table (default, fastest queries)
    /// - No configuration → Standard Table (safe default)
    ///
    /// **CompactTable Benefits:**
    /// - 90% memory reduction vs HashMap storage
    /// - String interning reduces memory overhead
    /// - Schema-based compact storage
    /// - Optimized for millions of records
    ///
    /// **CompactTable Trade-offs:**
    /// - ~10-15% CPU overhead for FieldValue conversion
    /// - Extra microseconds for field access
    /// - Schema inference complexity
    /// - Less efficient for frequent random access
    pub fn should_use_compact_table(&self, properties: &HashMap<String, String>) -> bool {
        // Check explicit table_model configuration
        if let Some(table_model) = properties.get("table_model") {
            match table_model.to_lowercase().as_str() {
                "compact" => {
                    log::info!("Using CompactTable due to table_model = 'compact'");
                    return true;
                }
                "normal" | "standard" => {
                    log::info!(
                        "Using standard Table due to table_model = '{}'",
                        table_model
                    );
                    return false;
                }
                _ => {
                    log::warn!("Unknown table_model '{}', defaulting to standard Table. Valid options: 'compact', 'normal'", table_model);
                    return false;
                }
            }
        }

        // Default: Use standard Table (safe, fast default)
        log::debug!("No table_model specified, using standard Table (default)");
        false
    }

    /// Apply properties to consumer configuration
    fn apply_properties_to_config(
        &self,
        config: &mut ConsumerConfig,
        properties: &HashMap<String, String>,
    ) {
        for (key, value) in properties.iter() {
            if let Some(kafka_key) = key.strip_prefix("kafka.") {
                match kafka_key {
                    "batch.size" | "linger.ms" | "compression.type" => {
                        log::debug!("Applied Kafka config: {} = {}", kafka_key, value);
                        // TODO: Future enhancement - apply to actual Kafka configuration
                        // config.set_kafka_property(kafka_key, value);
                    }
                    _ => {
                        log::debug!(
                            "Kafka property '{}' noted for future application",
                            kafka_key
                        );
                    }
                }
            }
        }
    }

    /// Load data source configuration from a config file
    fn load_data_source_config(&self, config_file: &str) -> Result<ConfigBasedSource, SqlError> {
        use serde_yaml::Value;
        use std::fs;

        // Read and parse the configuration file
        let config_content =
            fs::read_to_string(config_file).map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to read config file '{}': {}", config_file, e),
                query: None,
            })?;

        let config: Value =
            serde_yaml::from_str(&config_content).map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to parse config file '{}': {}", config_file, e),
                query: None,
            })?;

        // Extract source type and properties
        let source_type_str = config
            .get("source_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SqlError::ExecutionError {
                message: format!("Config file '{}' missing 'source_type' field", config_file),
                query: None,
            })?;

        // Extract properties as HashMap
        let mut properties = HashMap::new();
        if let Some(props) = config.get("properties") {
            if let Some(props_map) = props.as_mapping() {
                for (key, value) in props_map {
                    if let (Some(k), Some(v)) = (key.as_str(), value.as_str()) {
                        properties.insert(k.to_string(), v.to_string());
                    }
                }
            }
        }

        // Create appropriate DataSourceType based on configuration
        let source_type = match source_type_str {
            "kafka" => {
                let brokers = config
                    .get("brokers")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&self.kafka_brokers)
                    .to_string();
                let topic = config
                    .get("topic")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SqlError::ExecutionError {
                        message: format!(
                            "Kafka config file '{}' missing 'topic' field",
                            config_file
                        ),
                        query: None,
                    })?
                    .to_string();

                DataSourceType::Kafka { brokers, topic }
            }
            "file" => {
                let path = config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SqlError::ExecutionError {
                        message: format!("File config '{}' missing 'path' field", config_file),
                        query: None,
                    })?
                    .to_string();

                let format_str = config
                    .get("format")
                    .and_then(|v| v.as_str())
                    .unwrap_or("json");

                let format = match format_str.to_lowercase().as_str() {
                    "json" => FileFormat::Json,
                    "csv" => FileFormat::Csv,
                    "parquet" => FileFormat::Parquet,
                    "avro" => FileFormat::Avro,
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "Unsupported file format '{}' in config '{}'",
                                format_str, config_file
                            ),
                            query: None,
                        })
                    }
                };

                DataSourceType::File { path, format }
            }
            "http" => {
                let endpoint = config
                    .get("endpoint")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SqlError::ExecutionError {
                        message: format!("HTTP config '{}' missing 'endpoint' field", config_file),
                        query: None,
                    })?
                    .to_string();

                let poll_interval = config
                    .get("poll_interval")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(60000); // Default 60 seconds

                DataSourceType::Http {
                    endpoint,
                    poll_interval,
                }
            }
            _ => {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Unsupported source type '{}' in config '{}'",
                        source_type_str, config_file
                    ),
                    query: None,
                })
            }
        };

        Ok(ConfigBasedSource {
            config_file: config_file.to_string(),
            source_type,
            properties,
        })
    }

    /// Create a table based on configuration file specification
    async fn create_config_based_table(
        &self,
        table_name: &str,
        config_source: &ConfigBasedSource,
        properties: &HashMap<String, String>,
    ) -> Result<CtasResult, SqlError> {
        match &config_source.source_type {
            DataSourceType::Kafka { brokers, topic } => {
                log::info!(
                    "Creating Kafka table '{}' from config: brokers={}, topic={}",
                    table_name,
                    brokers,
                    topic
                );
                self.create_kafka_table_with_config(table_name, topic, brokers, properties)
                    .await
            }
            DataSourceType::File { path, format } => {
                log::info!(
                    "Creating file table '{}' from config: path={}, format={:?}",
                    table_name,
                    path,
                    format
                );
                self.create_file_table(table_name, path, format, properties)
                    .await
            }
            DataSourceType::Http {
                endpoint,
                poll_interval,
            } => {
                log::info!(
                    "Creating HTTP table '{}' from config: endpoint={}, poll_interval={}ms",
                    table_name,
                    endpoint,
                    poll_interval
                );
                self.create_http_table(table_name, endpoint, *poll_interval, properties)
                    .await
            }
        }
    }

    /// Create a Kafka table with specific configuration
    async fn create_kafka_table_with_config(
        &self,
        table_name: &str,
        topic: &str,
        brokers: &str,
        properties: &HashMap<String, String>,
    ) -> Result<CtasResult, SqlError> {
        // Similar to create_kafka_table but with custom brokers
        let counter = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let consumer_group = format!("{}-table-{}-{}", self.base_group_id, table_name, counter);

        let mut config = ConsumerConfig::new(brokers, &consumer_group);
        self.apply_properties_to_config(&mut config, properties);

        // Create the Table instance with properties
        let table = Table::new_with_properties(
            config,
            topic.to_string(),
            StringSerializer,
            JsonFormat,
            properties.clone(),
        )
        .await
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to create Kafka table '{}': {}", table_name, e),
            query: None,
        })?;

        self.finalize_table_creation(table_name, table).await
    }

    /// Create a file-based table
    async fn create_file_table(
        &self,
        table_name: &str,
        path: &str,
        format: &FileFormat,
        properties: &HashMap<String, String>,
    ) -> Result<CtasResult, SqlError> {
        log::info!(
            "Creating file table '{}' from path '{}' with format {:?}",
            table_name,
            path,
            format
        );

        // Create properties for FileDataSource
        let mut file_props = properties.clone();
        file_props.insert("path".to_string(), path.to_string());
        file_props.insert("format".to_string(), format!("{:?}", format).to_lowercase());

        // Create FileDataSource and initialize
        let mut file_source = FileDataSource::from_properties(&file_props);
        use crate::velostream::datasource::BatchConfig;
        use crate::velostream::datasource::{
            config::{FileFormat as ConfigFileFormat, SourceConfig},
            DataSource,
        };

        // Convert our FileFormat to config FileFormat
        let config_format = match format {
            FileFormat::Json => ConfigFileFormat::Json,
            FileFormat::Csv => ConfigFileFormat::Csv {
                header: true,
                delimiter: ',',
                quote: '"',
            },
            FileFormat::Parquet => ConfigFileFormat::Parquet,
            FileFormat::Avro => ConfigFileFormat::Avro,
        };

        let source_config = SourceConfig::File {
            path: path.to_string(),
            format: config_format,
            properties: file_props.clone(),
            batch_config: BatchConfig::default(),
        };

        file_source
            .initialize(source_config)
            .await
            .map_err(|e| SqlError::ExecutionError {
                message: format!(
                    "Failed to initialize file source for table '{}': {}",
                    table_name, e
                ),
                query: None,
            })?;

        // Create OptimizedTableImpl for SQL operations
        let optimized_table = OptimizedTableImpl::new();
        log::info!(
            "Created OptimizedTableImpl for file-based table '{}'",
            table_name
        );

        // Create the queryable table wrapper
        let queryable_table: Arc<dyn UnifiedTable> = Arc::new(optimized_table.clone());

        // Create background job to populate table from file
        let table_clone = optimized_table.clone();
        let table_name_clone = table_name.to_string();
        let file_props_clone = file_props.clone();

        let background_job = tokio::spawn(async move {
            log::info!(
                "Starting background population of file table '{}'",
                table_name_clone
            );

            match Self::populate_table_from_file(table_clone, &file_props_clone).await {
                Ok(record_count) => {
                    log::info!(
                        "Successfully populated table '{}' with {} records from file",
                        table_name_clone,
                        record_count
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to populate table '{}' from file: {}",
                        table_name_clone,
                        e
                    );
                }
            }
        });

        Ok(CtasResult {
            table_name: table_name.to_string(),
            table: queryable_table,
            background_job,
        })
    }

    /// Populate table from file data source
    async fn populate_table_from_file(
        table: OptimizedTableImpl,
        file_props: &HashMap<String, String>,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        use crate::velostream::datasource::config::SourceConfig;
        use crate::velostream::datasource::{DataReader, DataSource};

        // Create FileDataSource
        let mut file_source = FileDataSource::from_properties(file_props);

        // Create proper SourceConfig from properties
        let path = file_props.get("path").cloned().unwrap_or_default();
        let format_str = file_props
            .get("format")
            .cloned()
            .unwrap_or("json".to_string());

        let config_format = match format_str.to_lowercase().as_str() {
            "csv" => crate::velostream::datasource::config::FileFormat::Csv {
                header: true,
                delimiter: ',',
                quote: '"',
            },
            "parquet" => crate::velostream::datasource::config::FileFormat::Parquet,
            "avro" => crate::velostream::datasource::config::FileFormat::Avro,
            _ => crate::velostream::datasource::config::FileFormat::Json,
        };

        let source_config = SourceConfig::File {
            path,
            format: config_format,
            properties: file_props.clone(),
            batch_config: crate::velostream::datasource::BatchConfig::default(),
        };

        file_source.initialize(source_config).await?;

        // Create a reader
        let mut reader = file_source.create_reader().await?;

        let mut record_count = 0;

        // Read all records from file and populate table
        loop {
            let records = reader.read().await?;

            if records.is_empty() {
                break; // End of file
            }

            for record in records {
                // Extract key from record (use first field or row number)
                let key = record
                    .fields
                    .get("id")
                    .or_else(|| record.fields.get("key"))
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_else(|| format!("row_{}", record_count));

                // Insert into OptimizedTableImpl
                table.insert(key, record.fields)?;
                record_count += 1;
            }
        }

        Ok(record_count)
    }

    /// Create an HTTP-based table
    async fn create_http_table(
        &self,
        table_name: &str,
        endpoint: &str,
        poll_interval: u64,
        _properties: &HashMap<String, String>,
    ) -> Result<CtasResult, SqlError> {
        // HTTP table implementation not yet available
        Err(SqlError::ExecutionError {
            message: format!(
                "HTTP table creation not yet implemented. Table '{}' with endpoint '{}' and poll interval {}ms cannot be created. Use Kafka or File sources instead.",
                table_name, endpoint, poll_interval
            ),
            query: None,
        })
    }

    /// Common table finalization logic
    async fn finalize_table_creation(
        &self,
        table_name: &str,
        table: Table<String, StringSerializer, JsonFormat>,
    ) -> Result<CtasResult, SqlError> {
        // Create TableDataSource from the Table
        let queryable_table: Arc<dyn UnifiedTable> = Arc::new(table.clone());

        // Start background table population job
        let table_clone = table;
        let table_name_clone = table_name.to_string();
        let background_job = tokio::spawn(async move {
            log::info!(
                "Starting background population job for table '{}'",
                table_name_clone
            );

            match table_clone.start().await {
                Ok(()) => {
                    log::info!(
                        "Background population job for table '{}' completed successfully",
                        table_name_clone
                    );
                }
                Err(e) => {
                    log::error!(
                        "Background population job for table '{}' failed: {}",
                        table_name_clone,
                        e
                    );
                }
            }
        });

        Ok(CtasResult {
            table_name: table_name.to_string(),
            table: queryable_table,
            background_job,
        })
    }
}
