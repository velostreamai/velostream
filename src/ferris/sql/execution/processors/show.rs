/*!
# Show Processor for Schema Introspection

Handles SHOW and DESCRIBE queries for discovering streams, tables, schemas, and metadata.
*/

use crate::ferris::sql::ast::ShowResourceType;
use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use crate::ferris::schema::{Schema, StreamHandle};
use crate::ferris::sql::{SqlError, StreamingQuery};
use std::collections::{HashMap, HashSet};

use super::{ProcessorContext, ProcessorResult};

pub struct ShowProcessor;

impl ShowProcessor {
    pub fn process(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        if let StreamingQuery::Show {
            resource_type,
            pattern,
        } = query
        {
            let results = Self::generate_show_results(resource_type, pattern, context)?;

            // Create multiple records for the results
            if results.is_empty() {
                // No results - return empty result
                Ok(ProcessorResult {
                    record: None,
                    header_mutations: Vec::new(),
                    should_count: false,
                })
            } else {
                // For now, return just the first result as a single record
                // In a full implementation, we might want to support multiple result records
                let first_result = &results[0];
                let result_record = StreamRecord {
                    fields: first_result.clone(),
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                };
                Ok(ProcessorResult {
                    record: Some(result_record),
                    header_mutations: Vec::new(),
                    should_count: true,
                })
            }
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for ShowProcessor".to_string(),
                query: None,
            })
        }
    }

    fn generate_show_results(
        resource_type: &ShowResourceType,
        pattern: &Option<String>,
        context: &ProcessorContext,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        match resource_type {
            ShowResourceType::Streams => Self::show_streams(&context.stream_handles, pattern),
            ShowResourceType::Tables => {
                // For now, treat tables same as streams
                Self::show_tables(&context.stream_handles, pattern)
            }
            ShowResourceType::Topics => Self::show_topics(&context.stream_handles, pattern),
            ShowResourceType::Functions => Self::show_functions(pattern),
            ShowResourceType::Schema { name } => Self::show_schema(name, &context.schemas),
            ShowResourceType::Describe { name } => {
                Self::describe_resource(name, &context.schemas, &context.stream_handles)
            }
            ShowResourceType::Properties {
                resource_type,
                name,
            } => Self::show_properties(
                resource_type,
                name,
                &context.schemas,
                &context.stream_handles,
            ),
            ShowResourceType::Jobs => Self::show_jobs(),
            ShowResourceType::JobStatus { name } => Self::show_job_status(name),
            ShowResourceType::JobVersions { name } => Self::show_job_versions(name),
            ShowResourceType::JobMetrics { name } => Self::show_job_metrics(name),
            ShowResourceType::Partitions { name } => {
                Self::show_partitions(name, &context.stream_handles)
            }
        }
    }

    fn show_streams(
        streams: &HashMap<String, StreamHandle>,
        pattern: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        let mut results = Vec::new();

        for (name, handle) in streams {
            if Self::matches_pattern(name, pattern) {
                let mut fields = HashMap::new();
                fields.insert("stream_name".to_string(), FieldValue::String(name.clone()));
                fields.insert(
                    "topic".to_string(),
                    FieldValue::String(handle.topic.clone()),
                );
                fields.insert(
                    "schema_id".to_string(),
                    FieldValue::String(handle.schema_id.clone()),
                );
                fields.insert("type".to_string(), FieldValue::String("STREAM".to_string()));
                results.push(fields);
            }
        }

        Ok(results)
    }

    fn show_tables(
        streams: &HashMap<String, StreamHandle>,
        pattern: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // For now, tables are treated similar to streams but with different type
        let mut results = Vec::new();

        for (name, handle) in streams {
            if Self::matches_pattern(name, pattern) {
                let mut fields = HashMap::new();
                fields.insert("table_name".to_string(), FieldValue::String(name.clone()));
                fields.insert(
                    "topic".to_string(),
                    FieldValue::String(handle.topic.clone()),
                );
                fields.insert(
                    "schema_id".to_string(),
                    FieldValue::String(handle.schema_id.clone()),
                );
                fields.insert("type".to_string(), FieldValue::String("TABLE".to_string()));
                results.push(fields);
            }
        }

        Ok(results)
    }

    fn show_topics(
        streams: &HashMap<String, StreamHandle>,
        pattern: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        let mut results = Vec::new();
        let mut topics: HashSet<String> = HashSet::new();

        // Collect unique topics from registered streams
        for handle in streams.values() {
            if Self::matches_pattern(&handle.topic, pattern) {
                topics.insert(handle.topic.clone());
            }
        }

        for topic in topics {
            let mut fields = HashMap::new();
            fields.insert("topic_name".to_string(), FieldValue::String(topic));
            fields.insert("registered".to_string(), FieldValue::Boolean(true));
            results.push(fields);
        }

        Ok(results)
    }

    fn show_functions(
        _pattern: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Built-in functions available in the system
        let builtin_functions = vec![
            ("ABS", "Mathematical", "Returns absolute value"),
            ("UPPER", "String", "Converts to uppercase"),
            ("LOWER", "String", "Converts to lowercase"),
            ("LENGTH", "String", "Returns string length"),
            ("COUNT", "Aggregate", "Counts non-null values"),
            ("SUM", "Aggregate", "Sums numeric values"),
            ("AVG", "Aggregate", "Calculates average"),
            ("MIN", "Aggregate", "Finds minimum value"),
            ("MAX", "Aggregate", "Finds maximum value"),
            ("ROW_NUMBER", "Window", "Row number within partition"),
            ("RANK", "Window", "Rank within partition"),
            ("DENSE_RANK", "Window", "Dense rank within partition"),
            ("LAG", "Window", "Previous row value"),
            ("LEAD", "Window", "Next row value"),
        ];

        let mut results = Vec::new();
        for (name, category, description) in builtin_functions {
            let mut fields = HashMap::new();
            fields.insert(
                "function_name".to_string(),
                FieldValue::String(name.to_string()),
            );
            fields.insert(
                "category".to_string(),
                FieldValue::String(category.to_string()),
            );
            fields.insert(
                "description".to_string(),
                FieldValue::String(description.to_string()),
            );
            results.push(fields);
        }

        Ok(results)
    }

    fn show_schema(
        name: &str,
        schemas: &HashMap<String, Schema>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        if let Some(schema) = schemas.get(name) {
            let mut results = Vec::new();
            for field in &schema.fields {
                let mut fields = HashMap::new();
                fields.insert(
                    "column_name".to_string(),
                    FieldValue::String(field.name.clone()),
                );
                fields.insert(
                    "data_type".to_string(),
                    FieldValue::String(format!("{:?}", field.data_type)),
                );
                fields.insert("nullable".to_string(), FieldValue::Boolean(field.nullable));
                results.push(fields);
            }
            Ok(results)
        } else {
            Err(SqlError::SchemaError {
                message: format!("Schema not found: {}", name),
                column: None,
            })
        }
    }

    fn describe_resource(
        name: &str,
        schemas: &HashMap<String, Schema>,
        streams: &HashMap<String, StreamHandle>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // DESCRIBE is similar to SHOW SCHEMA but with more details
        if let Some(schema) = schemas.get(name) {
            let stream_handle = streams.get(name);
            let mut results = Vec::new();

            for field in &schema.fields {
                let mut fields = HashMap::new();
                fields.insert(
                    "column_name".to_string(),
                    FieldValue::String(field.name.clone()),
                );
                fields.insert(
                    "data_type".to_string(),
                    FieldValue::String(format!("{:?}", field.data_type)),
                );
                fields.insert("nullable".to_string(), FieldValue::Boolean(field.nullable));

                // Add stream-specific metadata if available
                if let Some(handle) = stream_handle {
                    fields.insert(
                        "topic".to_string(),
                        FieldValue::String(handle.topic.clone()),
                    );
                    fields.insert(
                        "schema_id".to_string(),
                        FieldValue::String(handle.schema_id.clone()),
                    );
                }

                results.push(fields);
            }
            Ok(results)
        } else {
            Err(SqlError::SchemaError {
                message: format!("Resource not found: {}", name),
                column: None,
            })
        }
    }

    fn show_properties(
        resource_type: &str,
        name: &str,
        schemas: &HashMap<String, Schema>,
        streams: &HashMap<String, StreamHandle>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        match resource_type.to_uppercase().as_str() {
            "STREAM" | "TABLE" => {
                if let Some(handle) = streams.get(name) {
                    let mut results = Vec::new();

                    // Stream properties
                    let properties = vec![
                        ("id", handle.id.clone()),
                        ("topic", handle.topic.clone()),
                        ("schema_id", handle.schema_id.clone()),
                        ("type", resource_type.to_uppercase()),
                    ];

                    for (key, value) in properties {
                        let mut fields = HashMap::new();
                        fields.insert("property".to_string(), FieldValue::String(key.to_string()));
                        fields.insert("value".to_string(), FieldValue::String(value));
                        results.push(fields);
                    }

                    // Add schema information if available
                    if let Some(schema) = schemas.get(name) {
                        let mut fields = HashMap::new();
                        fields.insert(
                            "property".to_string(),
                            FieldValue::String("field_count".to_string()),
                        );
                        fields.insert(
                            "value".to_string(),
                            FieldValue::String(schema.fields.len().to_string()),
                        );
                        results.push(fields);
                    }

                    Ok(results)
                } else {
                    Err(SqlError::SchemaError {
                        message: format!("{} not found: {}", resource_type, name),
                        column: None,
                    })
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported resource type: {}", resource_type),
                query: None,
            }),
        }
    }

    fn show_jobs() -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Placeholder for job management
        let mut fields = HashMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::String("Job management not yet implemented".to_string()),
        );
        Ok(vec![fields])
    }

    fn show_job_status(
        _name: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Placeholder for job status
        let mut fields = HashMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::String("Job status not yet implemented".to_string()),
        );
        Ok(vec![fields])
    }

    fn show_job_versions(_name: &str) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Placeholder for job versions
        let mut fields = HashMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::String("Job versions not yet implemented".to_string()),
        );
        Ok(vec![fields])
    }

    fn show_job_metrics(
        _name: &Option<String>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Placeholder for job metrics
        let mut fields = HashMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::String("Job metrics not yet implemented".to_string()),
        );
        Ok(vec![fields])
    }

    fn show_partitions(
        _name: &str,
        _streams: &HashMap<String, StreamHandle>,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        // Placeholder for partition information
        let mut fields = HashMap::new();
        fields.insert(
            "message".to_string(),
            FieldValue::String("Partition information not yet implemented".to_string()),
        );
        Ok(vec![fields])
    }

    fn matches_pattern(name: &str, pattern: &Option<String>) -> bool {
        match pattern {
            None => true,
            Some(p) => {
                // Simple pattern matching with % for wildcards
                if p.contains('%') {
                    // Convert SQL LIKE pattern to basic wildcard matching
                    let parts: Vec<&str> = p.split('%').collect();
                    if parts.len() == 2 && parts[0].is_empty() {
                        // Pattern like "%suffix"
                        name.ends_with(parts[1])
                    } else if parts.len() == 2 && parts[1].is_empty() {
                        // Pattern like "prefix%"
                        name.starts_with(parts[0])
                    } else if parts.len() == 3 && parts[0].is_empty() && parts[2].is_empty() {
                        // Pattern like "%substring%"
                        name.contains(parts[1])
                    } else {
                        // More complex patterns - fallback to simple contains
                        name.contains(&p.replace('%', ""))
                    }
                } else {
                    name == p
                }
            }
        }
    }
}
