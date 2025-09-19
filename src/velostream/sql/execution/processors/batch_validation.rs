//! Batch Processing Validation
//!
//! This module provides validation for batch processing configurations including:
//! - Batch strategies (FixedSize, TimeWindow, AdaptiveSize, MemoryBased, LowLatency)
//! - Processor types (simple, transactional)
//! - Failure strategies (LogAndContinue, SendToDLQ, Halt)
//! - Serialization format detection and validation

use std::collections::HashMap;

/// Trait for types that support batch processing validation
pub trait BatchValidationTarget {
    /// Get the query text
    fn query_text(&self) -> &str;

    /// Get mutable access to sources found
    fn sources_found(&self) -> &[String];

    /// Get mutable access to sinks found
    fn sinks_found(&self) -> &[String];

    /// Get source configurations
    fn source_configs(&self) -> &HashMap<String, HashMap<String, String>>;

    /// Get sink configurations
    fn sink_configs(&self) -> &HashMap<String, HashMap<String, String>>;

    /// Add a warning message
    fn add_warning(&mut self, message: String);

    /// Add a performance warning message
    fn add_performance_warning(&mut self, message: String);
}

/// Batch processing validator
pub struct BatchProcessingValidator;

impl BatchProcessingValidator {
    /// Create a new batch processing validator
    pub fn new() -> Self {
        Self
    }

    /// Validate batch processing configurations across multiple queries
    pub fn validate_batch_processing<T: BatchValidationTarget>(
        &self,
        query_results: &mut Vec<T>,
        recommendations: &mut Vec<String>,
    ) {
        let mut sources_without_failure_strategy = Vec::new();
        let mut sources_without_value_format = Vec::new();
        let mut has_batch_strategy = false;
        let mut total_sources = 0;
        let mut total_sinks = 0;

        // Check each query for missing batch processing configurations
        for query_result in query_results.iter_mut() {
            let sources_found = query_result.sources_found().to_vec();
            let sinks_found = query_result.sinks_found().to_vec();

            total_sources += sources_found.len();
            total_sinks += sinks_found.len();

            let mut query_sources_missing_batch_strategy = Vec::new();
            let mut query_sources_missing_value_format = Vec::new();
            let mut query_missing_failure_strategy = false;

            // Check for batch.strategy configuration
            if query_result.query_text().contains("batch.strategy") {
                has_batch_strategy = true;
            } else if !sources_found.is_empty() {
                query_sources_missing_batch_strategy = sources_found.clone();
            }

            // Check for processor type configuration
            let has_processor_type = query_result.query_text().contains("processor.type");
            let mut query_missing_processor_type = false;

            if !has_processor_type && (!sources_found.is_empty() || !sinks_found.is_empty()) {
                query_missing_processor_type = true;
            }

            // Check for processor-level failure.strategy configuration
            // failure.strategy applies to both simple and transactional processors
            let has_failure_strategy = query_result.query_text().contains("failure.strategy");

            if !has_failure_strategy && (!sources_found.is_empty() || !sinks_found.is_empty()) {
                query_missing_failure_strategy = true;
                // Add all sources to the global tracking for the summary
                sources_without_failure_strategy.extend(sources_found.clone());
            }

            // Check for missing serialization format in source configs and always display detected format
            for source_name in &sources_found {
                if let Some(source_config) = query_result.source_configs().get(source_name) {
                    let detected_format = self.detect_serialization_format(source_config);
                    let has_serialization_format =
                        self.check_serialization_format_in_config(source_config);

                    // Always display the detected serialization format
                    query_result.add_performance_warning(format!(
                        "📋 Source '{}': Serialization format detected as '{}'",
                        source_name, detected_format
                    ));

                    if !has_serialization_format {
                        sources_without_value_format.push(source_name.clone());
                        query_sources_missing_value_format.push(source_name.clone());
                    }
                }
            }

            // Check for serialization format in sink configs and always display detected format
            for sink_name in &sinks_found {
                if let Some(sink_config) = query_result.sink_configs().get(sink_name) {
                    let detected_format = self.detect_serialization_format(sink_config);

                    // Always display the detected serialization format
                    query_result.add_performance_warning(format!(
                        "📋 Sink '{}': Serialization format detected as '{}'",
                        sink_name, detected_format
                    ));
                }
            }

            // Note: failure_strategy is processor-level, not sink-specific

            // Add batch processing recommendations to this specific query
            if !query_sources_missing_value_format.is_empty() {
                query_result.add_warning(format!(
                    "Serialization - value.format: Missing for sources [{}]. Example: 'value.format' = 'avro'. Possible values: 'json' (dev/simple), 'avro' (prod/schema), 'protobuf' (performance), 'string', 'bytes'",
                    query_sources_missing_value_format.join(", ")
                ));
            }

            if query_missing_processor_type {
                query_result.add_warning(
                    "Job Processing - processor.type: Not specified (default: simple). Example: 'processor.type' = 'simple'".to_string()
                );
                query_result.add_warning(
                    "   Parameters for simple: buffer.size (default: 64KB), parallelism (default: available cores), flush.interval (default: 100ms)".to_string()
                );
                query_result.add_warning(
                    "   Parameters for transactional: transaction.timeout (default: 30s), isolation.level ('read_uncommitted', 'read_committed', 'repeatable_read', 'serializable'), checkpoint.interval (default: 10s)".to_string()
                );
                query_result.add_warning(
                    "   Possible values: 'simple' (fast, low-latency, default), 'transactional' (ACID guarantees)".to_string()
                );
            }

            if query_missing_failure_strategy {
                query_result.add_warning(
                    "Batch Processing - failure.strategy: Recommended for robust error handling (default: LogAndContinue).".to_string()
                );
                query_result.add_warning(
                    "   Example: 'failure.strategy' = 'SendToDLQ', 'dlq.topic' = 'failed_records', 'retry.attempts' = '3'".to_string()
                );
                query_result.add_warning("   Possible values:".to_string());
                query_result.add_warning(
                    "   • 'LogAndContinue' - Log errors and continue processing. Parameters: log.level (default: ERROR)".to_string()
                );
                query_result.add_warning(
                    "   • 'SendToDLQ' - Send failed records to dead letter queue. Parameters: dlq.topic, dlq.bootstrap.servers, retry.attempts (default: 3)".to_string()
                );
                query_result.add_warning(
                    "   • 'Halt' - Stop processing on first error. Parameters: halt.timeout (default: 0s), cleanup.on.halt (default: true)".to_string()
                );
            }

            if !query_sources_missing_batch_strategy.is_empty() {
                query_result.add_warning(
                    "Batch Processing - batch.strategy: Not specified (default: FixedSize). Example: 'batch.strategy' = 'FixedSize', 'batch.size' = '1000', 'batch.timeout' = '5s'".to_string()
                );
                query_result.add_warning("   Possible values:".to_string());
                query_result.add_warning(
                    "   • 'FixedSize' - Fixed number of records per batch. Parameters: batch.size (default: 1000), batch.timeout (default: 10s)".to_string()
                );
                query_result.add_warning(
                    "   • 'TimeWindow' - Time-based batching. Parameters: window.size (default: 1m), window.grace (default: 10s)".to_string()
                );
                query_result.add_warning(
                    "   • 'AdaptiveSize' - Dynamic batch sizing. Parameters: min.batch.size (default: 100), max.batch.size (default: 10000), target.latency (default: 100ms)".to_string()
                );
                query_result.add_warning(
                    "   • 'MemoryBased' - Memory-based batching. Parameters: max.memory (default: 64MB), memory.check.interval (default: 1s)".to_string()
                );
                query_result.add_warning(
                    "   • 'LowLatency' - Minimal delay processing. Parameters: flush.interval (default: 1ms), max.wait (default: 10ms)".to_string()
                );
            }
        }

        // Generate batch processing recommendations
        if !sources_without_value_format.is_empty() {
            recommendations.push(format!(
                "⚠️  Serialization - value.format: Missing for sources [{}]. Example: 'value.format' = 'avro'. Possible values: 'json' (dev/simple), 'avro' (prod/schema), 'protobuf' (performance), 'string', 'bytes'",
                sources_without_value_format.join(", ")
            ));
        }

        if !sources_without_failure_strategy.is_empty() {
            recommendations.push(
                "⚠️  Batch Processing - failure.strategy: Recommended for robust error handling in data processing (default: LogAndContinue). Example: 'failure.strategy' = 'LogAndContinue'. Possible values: 'LogAndContinue' (dev), 'SendToDLQ' (prod), 'Halt'".to_string()
            );
        }

        // WARN about missing batch.strategy with detailed examples
        if !has_batch_strategy && (total_sources > 0 || total_sinks > 0) {
            recommendations.push(
                "⚠️  Batch Processing - batch.strategy: Not specified (default: FixedSize). Example: 'batch.strategy' = 'FixedSize'. Possible values:".to_string()
            );
            recommendations.push(
                "   • 'FixedSize' - Process fixed number of records per batch (good for consistent workloads)".to_string()
            );
            recommendations.push(
                "   • 'TimeWindow' - Process records within time windows (good for real-time processing)".to_string()
            );
            recommendations.push(
                "   • 'AdaptiveSize' - Dynamically adjust batch size based on throughput (good for variable workloads)".to_string()
            );
            recommendations.push(
                "   • 'MemoryBased' - Batch based on memory usage (good for large records)"
                    .to_string(),
            );
            recommendations.push(
                "   • 'LowLatency' - Optimize for minimal processing delay (good for trading/alerts)".to_string()
            );
            recommendations.push(
                "   Example configuration: WITH ('batch.strategy' = 'FixedSize', 'batch.size' = '1000', 'batch.timeout' = '5s')".to_string()
            );
        }

        // Additional recommendations for complex applications
        if total_sources > 1 || total_sinks > 1 {
            recommendations.push(
                "💡 Batch Processing - Multi-source/sink: Consider different strategies per source/sink based on data characteristics and latency requirements".to_string()
            );
        }
    }

    /// Detect serialization format from configuration
    fn detect_serialization_format(&self, config: &HashMap<String, String>) -> String {
        // Check explicit format configurations first
        if let Some(format) = config.get("value.format") {
            return format.clone();
        }
        if let Some(format) = config.get("schema.value.format") {
            return format.clone();
        }
        if let Some(format) = config.get("value_format") {
            return format.clone();
        }
        if let Some(format) = config.get("format") {
            return format.clone();
        }

        // SPECIAL HANDLING: Check if the "schema" key contains serialization info
        if let Some(schema_str) = config.get("schema") {
            if schema_str.contains("\"value.serializer\": String(\"avro\")") {
                return "avro".to_string();
            }
            if schema_str.contains("\"value.serializer\": \"avro\"") {
                return "avro".to_string();
            }
            if schema_str.contains("value.serializer: \"avro\"") {
                return "avro".to_string();
            }
        }

        // Check for schema indicators
        if config.contains_key("avro.schema") || config.contains_key("avro.schema.file") {
            return "avro".to_string();
        }
        if config.contains_key("protobuf.schema") || config.contains_key("protobuf.schema.file") {
            return "protobuf".to_string();
        }

        // Default to JSON when no format detected
        "json".to_string()
    }

    /// Check if configuration has explicit serialization format
    fn check_serialization_format_in_config(&self, config: &HashMap<String, String>) -> bool {
        config.contains_key("value.format")
            || config.contains_key("schema.value.format")
            || config.contains_key("value_format")
            || config.contains_key("format")
    }
}

impl Default for BatchProcessingValidator {
    fn default() -> Self {
        Self::new()
    }
}
