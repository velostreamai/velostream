//! Schema Providers
//!
//! Implementations of schema discovery for different data source types.
//! Each provider knows how to extract schema information from its specific data source.

use crate::ferris::schema::registry::{ProviderMetadata, SchemaProvider};
use crate::ferris::schema::{
    CompatibilityMode, FieldDefinition, Schema, SchemaError, SchemaMetadata, SchemaResult,
};
use crate::ferris::sql::ast::DataType;
use async_trait::async_trait;

/// Kafka schema provider - discovers schema from Kafka topics
pub struct KafkaSchemaProvider {
    /// Optional Schema Registry URL for Confluent Schema Registry integration
    schema_registry_url: Option<String>,
}

/// File schema provider - infers schema from file content
pub struct FileSchemaProvider {
    /// Maximum number of records to sample for schema inference
    sample_size: usize,
}

/// S3 schema provider - discovers schema from S3 objects
pub struct S3SchemaProvider {
    /// AWS region for S3 operations
    region: String,
    /// Sample size for schema inference
    sample_size: usize,
}

/// Generic JSON schema provider - infers schema from JSON data
pub struct JsonSchemaProvider {
    sample_size: usize,
}

impl Default for KafkaSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSchemaProvider {
    /// Create a new Kafka schema provider
    pub fn new() -> Self {
        Self {
            schema_registry_url: None,
        }
    }

    /// Create a Kafka schema provider with Schema Registry integration
    pub fn with_schema_registry(mut self, registry_url: String) -> Self {
        self.schema_registry_url = Some(registry_url);
        self
    }

    /// Extract topic name from Kafka URI
    fn extract_topic_from_uri(&self, uri: &str) -> SchemaResult<String> {
        // Parse kafka://brokers/topic format
        let parts: Vec<&str> = uri.split('/').collect();
        if parts.len() >= 4 {
            Ok(parts[3].to_string())
        } else {
            Err(SchemaError::Provider {
                source: uri.to_string(),
                message: "Invalid Kafka URI format".to_string(),
            })
        }
    }

    /// Discover schema from Schema Registry if available
    async fn discover_from_schema_registry(&self, topic: &str) -> SchemaResult<Schema> {
        let _registry_url =
            self.schema_registry_url
                .as_ref()
                .ok_or_else(|| SchemaError::Provider {
                    source: topic.to_string(),
                    message: "Schema Registry URL not configured".to_string(),
                })?;

        // TODO: Implement actual Schema Registry client integration
        // For now, return a basic schema
        Ok(self.create_default_kafka_schema(topic))
    }

    /// Create a default schema for Kafka topics when registry is not available
    fn create_default_kafka_schema(&self, topic: &str) -> Schema {
        let fields = vec![
            FieldDefinition::optional("key".to_string(), DataType::String)
                .with_description("Kafka message key".to_string()),
            FieldDefinition::required("value".to_string(), DataType::String)
                .with_description("Kafka message value".to_string()),
            FieldDefinition::required("timestamp".to_string(), DataType::Timestamp)
                .with_description("Message timestamp".to_string()),
            FieldDefinition::required("offset".to_string(), DataType::Integer)
                .with_description("Kafka offset".to_string()),
            FieldDefinition::required("partition".to_string(), DataType::Integer)
                .with_description("Kafka partition".to_string()),
        ];

        Schema {
            fields,
            version: Some("1.0.0".to_string()),
            metadata: SchemaMetadata::new("kafka".to_string())
                .with_compatibility(CompatibilityMode::Forward)
                .with_tag("topic".to_string(), topic.to_string()),
        }
    }
}

#[async_trait]
impl SchemaProvider for KafkaSchemaProvider {
    async fn discover_schema(&self, source_uri: &str) -> SchemaResult<Schema> {
        let topic = self.extract_topic_from_uri(source_uri)?;

        if self.schema_registry_url.is_some() {
            // Try Schema Registry first
            match self.discover_from_schema_registry(&topic).await {
                Ok(schema) => return Ok(schema),
                Err(_) => {
                    // Fallback to default schema if registry fails
                }
            }
        }

        // Return default Kafka schema
        Ok(self.create_default_kafka_schema(&topic))
    }

    fn supports_scheme(&self, scheme: &str) -> bool {
        scheme == "kafka"
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            name: "Kafka Schema Provider".to_string(),
            version: "1.0.0".to_string(),
            supported_schemes: vec!["kafka".to_string()],
            capabilities: vec![
                "schema_registry".to_string(),
                "default_schema".to_string(),
                "topic_discovery".to_string(),
            ],
        }
    }
}

impl Default for FileSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSchemaProvider {
    pub fn new() -> Self {
        Self { sample_size: 100 }
    }

    pub fn with_sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }

    /// Infer schema from file extension and optional sampling
    async fn infer_schema_from_file(&self, file_path: &str) -> SchemaResult<Schema> {
        let extension = std::path::Path::new(file_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "json" | "jsonl" | "ndjson" => self.create_json_schema(file_path),
            "csv" => self.create_csv_schema(file_path),
            "parquet" => self.create_parquet_schema(file_path),
            "avro" => self.create_avro_schema(file_path),
            _ => self.create_generic_schema(file_path),
        }
    }

    fn create_json_schema(&self, file_path: &str) -> SchemaResult<Schema> {
        // TODO: Implement actual JSON schema inference by reading and sampling file
        let fields = vec![
            FieldDefinition::optional("id".to_string(), DataType::String)
                .with_description("Record identifier".to_string()),
            FieldDefinition::optional(
                "data".to_string(),
                DataType::Map(Box::new(DataType::String), Box::new(DataType::String)),
            )
            .with_description("JSON data payload".to_string()),
        ];

        Ok(Schema {
            fields,
            version: Some("inferred".to_string()),
            metadata: SchemaMetadata::new("file".to_string())
                .with_tag("format".to_string(), "json".to_string())
                .with_tag("path".to_string(), file_path.to_string()),
        })
    }

    fn create_csv_schema(&self, file_path: &str) -> SchemaResult<Schema> {
        // TODO: Implement CSV header reading and type inference
        let fields = vec![
            FieldDefinition::required("column_1".to_string(), DataType::String),
            FieldDefinition::required("column_2".to_string(), DataType::String),
        ];

        Ok(Schema {
            fields,
            version: Some("inferred".to_string()),
            metadata: SchemaMetadata::new("file".to_string())
                .with_tag("format".to_string(), "csv".to_string())
                .with_tag("path".to_string(), file_path.to_string()),
        })
    }

    fn create_parquet_schema(&self, file_path: &str) -> SchemaResult<Schema> {
        // TODO: Implement Parquet metadata reading
        let fields = vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("name".to_string(), DataType::String),
        ];

        Ok(Schema {
            fields,
            version: Some("parquet".to_string()),
            metadata: SchemaMetadata::new("file".to_string())
                .with_tag("format".to_string(), "parquet".to_string())
                .with_tag("path".to_string(), file_path.to_string()),
        })
    }

    fn create_avro_schema(&self, file_path: &str) -> SchemaResult<Schema> {
        // TODO: Implement Avro schema reading
        let fields = vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("payload".to_string(), DataType::String),
        ];

        Ok(Schema {
            fields,
            version: Some("avro".to_string()),
            metadata: SchemaMetadata::new("file".to_string())
                .with_tag("format".to_string(), "avro".to_string())
                .with_tag("path".to_string(), file_path.to_string()),
        })
    }

    fn create_generic_schema(&self, file_path: &str) -> SchemaResult<Schema> {
        let fields = vec![
            FieldDefinition::required("line".to_string(), DataType::String)
                .with_description("Raw line content".to_string()),
        ];

        Ok(Schema {
            fields,
            version: Some("generic".to_string()),
            metadata: SchemaMetadata::new("file".to_string())
                .with_tag("format".to_string(), "text".to_string())
                .with_tag("path".to_string(), file_path.to_string()),
        })
    }
}

#[async_trait]
impl SchemaProvider for FileSchemaProvider {
    async fn discover_schema(&self, source_uri: &str) -> SchemaResult<Schema> {
        // Extract file path from file:// URI
        let file_path = source_uri.strip_prefix("file://").unwrap_or(source_uri);

        self.infer_schema_from_file(file_path).await
    }

    fn supports_scheme(&self, scheme: &str) -> bool {
        scheme == "file"
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            name: "File Schema Provider".to_string(),
            version: "1.0.0".to_string(),
            supported_schemes: vec!["file".to_string()],
            capabilities: vec![
                "json_inference".to_string(),
                "csv_inference".to_string(),
                "parquet_metadata".to_string(),
                "avro_schema".to_string(),
            ],
        }
    }
}

impl S3SchemaProvider {
    pub fn new(region: String) -> Self {
        Self {
            region,
            sample_size: 10,
        }
    }

    pub fn with_sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }

    /// Parse S3 URI and extract bucket/key information
    fn parse_s3_uri(&self, uri: &str) -> SchemaResult<(String, String)> {
        // Parse s3://bucket/path format
        let without_scheme = uri
            .strip_prefix("s3://")
            .ok_or_else(|| SchemaError::Provider {
                source: uri.to_string(),
                message: "Invalid S3 URI format".to_string(),
            })?;

        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
        if parts.len() == 2 {
            Ok((parts[0].to_string(), parts[1].to_string()))
        } else {
            Err(SchemaError::Provider {
                source: uri.to_string(),
                message: "S3 URI must include bucket and key".to_string(),
            })
        }
    }

    /// Create schema based on S3 object key pattern
    fn infer_schema_from_key(&self, bucket: &str, key: &str) -> Schema {
        // Infer format from key extension
        let format = if key.ends_with(".json") || key.ends_with(".jsonl") {
            "json"
        } else if key.ends_with(".csv") {
            "csv"
        } else if key.ends_with(".parquet") {
            "parquet"
        } else if key.ends_with(".avro") {
            "avro"
        } else {
            "unknown"
        };

        let fields = match format {
            "json" => vec![
                FieldDefinition::optional("id".to_string(), DataType::String),
                FieldDefinition::optional(
                    "data".to_string(),
                    DataType::Map(Box::new(DataType::String), Box::new(DataType::String)),
                ),
            ],
            "csv" => vec![
                FieldDefinition::required("col1".to_string(), DataType::String),
                FieldDefinition::required("col2".to_string(), DataType::String),
            ],
            "parquet" => vec![
                FieldDefinition::required("id".to_string(), DataType::Integer),
                FieldDefinition::required("value".to_string(), DataType::String),
            ],
            _ => vec![FieldDefinition::required(
                "content".to_string(),
                DataType::String,
            )],
        };

        Schema {
            fields,
            version: Some("inferred".to_string()),
            metadata: SchemaMetadata::new("s3".to_string())
                .with_tag("bucket".to_string(), bucket.to_string())
                .with_tag("key".to_string(), key.to_string())
                .with_tag("format".to_string(), format.to_string())
                .with_tag("region".to_string(), self.region.clone()),
        }
    }
}

#[async_trait]
impl SchemaProvider for S3SchemaProvider {
    async fn discover_schema(&self, source_uri: &str) -> SchemaResult<Schema> {
        let (bucket, key) = self.parse_s3_uri(source_uri)?;

        // TODO: Implement actual S3 object inspection
        // For now, infer from key pattern
        Ok(self.infer_schema_from_key(&bucket, &key))
    }

    fn supports_scheme(&self, scheme: &str) -> bool {
        scheme == "s3"
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            name: "S3 Schema Provider".to_string(),
            version: "1.0.0".to_string(),
            supported_schemes: vec!["s3".to_string()],
            capabilities: vec![
                "object_inspection".to_string(),
                "format_inference".to_string(),
                "metadata_extraction".to_string(),
            ],
        }
    }
}

/// Create a schema registry with default providers
pub fn create_default_registry() -> crate::ferris::schema::registry::SchemaRegistry {
    let mut registry = crate::ferris::schema::registry::SchemaRegistry::new();

    // Register default providers
    registry.register_provider("kafka", std::sync::Arc::new(KafkaSchemaProvider::new()));
    registry.register_provider("file", std::sync::Arc::new(FileSchemaProvider::new()));
    registry.register_provider(
        "s3",
        std::sync::Arc::new(S3SchemaProvider::new("us-east-1".to_string())),
    );

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kafka_schema_provider() {
        let provider = KafkaSchemaProvider::new();
        let schema = provider
            .discover_schema("kafka://localhost:9092/test-topic")
            .await
            .unwrap();

        assert_eq!(schema.fields.len(), 5); // key, value, timestamp, offset, partition
        assert_eq!(schema.metadata.source_type, "kafka");
        assert!(schema.has_field("key"));
        assert!(schema.has_field("value"));
    }

    #[tokio::test]
    async fn test_file_schema_provider() {
        let provider = FileSchemaProvider::new();
        let schema = provider
            .discover_schema("file:///data/test.json")
            .await
            .unwrap();

        assert!(!schema.fields.is_empty());
        assert_eq!(schema.metadata.source_type, "file");
    }

    #[tokio::test]
    async fn test_s3_schema_provider() {
        let provider = S3SchemaProvider::new("us-west-2".to_string());
        let schema = provider
            .discover_schema("s3://my-bucket/data/test.parquet")
            .await
            .unwrap();

        assert!(!schema.fields.is_empty());
        assert_eq!(schema.metadata.source_type, "s3");
        assert_eq!(schema.metadata.tags.get("bucket").unwrap(), "my-bucket");
    }

    #[test]
    fn test_default_registry_creation() {
        let registry = create_default_registry();
        let providers = registry.list_providers();

        assert!(providers.len() >= 3);
        assert!(providers.iter().any(|(scheme, _)| scheme == "kafka"));
        assert!(providers.iter().any(|(scheme, _)| scheme == "file"));
        assert!(providers.iter().any(|(scheme, _)| scheme == "s3"));
    }
}
