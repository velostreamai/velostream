//! Execution Format Factory for SQL Query Processing
//!
//! This module creates the appropriate execution format for the SQL engine
//! based on query analysis, allowing dynamic serialization selection.

use crate::ferris::{
    serialization::{JsonFormat, SerializationFormat as ExecutionFormat},
    sql::{query_analyzer::QueryAnalysis, SqlError},
};
use std::sync::Arc;



/// Factory for creating execution formats based on query analysis
pub struct ExecutionFormatFactory;

impl ExecutionFormatFactory {
    /// Create appropriate execution format based on query analysis
    pub fn create_format(analysis: &QueryAnalysis) -> Result<Arc<dyn ExecutionFormat>, SqlError> {
        // Determine the primary serialization format from datasources
        // Check sinks first since that's what the execution engine will output
        let format_str = if let Some(sink_req) = analysis.required_sinks.first() {
            sink_req
                .properties
                .get("value.serializer")
                .map(|s| s.as_str())
                .unwrap_or("json")
        } else if let Some(source_req) = analysis.required_sources.first() {
            source_req
                .properties
                .get("value.serializer")
                .map(|s| s.as_str())
                .unwrap_or("json")
        } else {
            "json" // Default to JSON if no specific format requirements
        };

        match format_str {
            "json" => Ok(Arc::new(JsonFormat) as Arc<dyn ExecutionFormat>),

            #[cfg(feature = "avro")]
            "avro" => {
                // For Avro, we'd need schema registry info from config
                // This would be enhanced when integrating with schema registry
                // For now, use JSON as fallback
                Ok(Arc::new(JsonFormat) as Arc<dyn ExecutionFormat>)
            }

            #[cfg(feature = "protobuf")]
            "protobuf" | "proto" => {
                // For Protobuf, we'd need message type info
                // This would be enhanced with proper protobuf support
                // For now, use JSON as fallback
                Ok(Arc::new(JsonFormat) as Arc<dyn ExecutionFormat>)
            }

            _ => {
                // For unknown formats, fall back to JSON for execution
                Ok(Arc::new(JsonFormat) as Arc<dyn ExecutionFormat>)
            }
        }
    }

    /// Get format description for logging
    pub fn get_format_description(analysis: &QueryAnalysis) -> String {
        let format_str = if let Some(sink_req) = analysis.required_sinks.first() {
            sink_req
                .properties
                .get("value.serializer")
                .map(|s| s.as_str())
                .unwrap_or("json")
        } else if let Some(source_req) = analysis.required_sources.first() {
            source_req
                .properties
                .get("value.serializer")
                .map(|s| s.as_str())
                .unwrap_or("json")
        } else {
            "json"
        };

        format_str.to_string()
    }
}
