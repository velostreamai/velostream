//! Graceful Degradation for Stream-Table Coordination
//!
//! Handles scenarios where table data is missing, incomplete, or temporarily unavailable
//! during stream-table join operations. Provides configurable strategies for handling
//! these situations gracefully instead of failing hard.

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::types::FieldValue;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::time::Duration;

/// Strategy for handling missing or incomplete table data during stream-table joins
#[derive(Debug, Clone)]
pub enum TableMissingDataStrategy {
    /// Use default values for missing table fields
    /// Allows stream processing to continue with predefined fallback values
    UseDefaults(HashMap<String, FieldValue>),

    /// Skip records that cannot be enriched with table data
    /// Filters out records rather than failing or using defaults
    SkipRecord,

    /// Emit records with NULL values for missing table fields
    /// Preserves all stream records but marks missing enrichment as NULL
    EmitWithNulls,

    /// Wait and retry with exponential backoff
    /// Attempts to retry the table lookup after a delay
    WaitAndRetry {
        max_retries: u32,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_multiplier: f64,
    },

    /// Fail immediately when table data is missing
    /// Default behavior - maintains strict data consistency requirements
    FailFast,
}

impl Default for TableMissingDataStrategy {
    fn default() -> Self {
        // Default to fail-fast for data consistency
        TableMissingDataStrategy::FailFast
    }
}

/// Configuration for graceful degradation behavior
#[derive(Debug, Clone)]
pub struct GracefulDegradationConfig {
    /// Primary strategy for handling missing data
    pub primary_strategy: TableMissingDataStrategy,

    /// Whether to log when degradation strategies are used
    pub enable_logging: bool,

    /// Whether to emit metrics for degradation events
    pub enable_metrics: bool,

    /// Per-table strategy overrides
    pub table_specific_strategies: HashMap<String, TableMissingDataStrategy>,
}

impl Default for GracefulDegradationConfig {
    fn default() -> Self {
        Self {
            primary_strategy: TableMissingDataStrategy::FailFast,
            enable_logging: true,
            enable_metrics: true,
            table_specific_strategies: HashMap::new(),
        }
    }
}

/// Handler for graceful degradation scenarios
pub struct GracefulDegradationHandler {
    config: GracefulDegradationConfig,
}

impl GracefulDegradationHandler {
    /// Create a new graceful degradation handler
    pub fn new(config: GracefulDegradationConfig) -> Self {
        Self { config }
    }

    /// Create a handler with default fail-fast behavior
    pub fn fail_fast() -> Self {
        Self::new(GracefulDegradationConfig::default())
    }

    /// Create a handler that uses default values for missing data
    pub fn with_defaults(defaults: HashMap<String, FieldValue>) -> Self {
        let config = GracefulDegradationConfig {
            primary_strategy: TableMissingDataStrategy::UseDefaults(defaults),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Create a handler that emits records with NULL values
    pub fn emit_with_nulls() -> Self {
        let config = GracefulDegradationConfig {
            primary_strategy: TableMissingDataStrategy::EmitWithNulls,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Create a handler that skips records with missing data
    pub fn skip_records() -> Self {
        let config = GracefulDegradationConfig {
            primary_strategy: TableMissingDataStrategy::SkipRecord,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Create a handler with retry logic
    pub fn with_retry(max_retries: u32, initial_delay: Duration) -> Self {
        let config = GracefulDegradationConfig {
            primary_strategy: TableMissingDataStrategy::WaitAndRetry {
                max_retries,
                initial_delay,
                max_delay: Duration::from_secs(30), // Cap at 30 seconds
                backoff_multiplier: 2.0,
            },
            ..Default::default()
        };
        Self::new(config)
    }

    /// Handle missing table data according to the configured strategy
    pub async fn handle_missing_table_data(
        &self,
        table_name: &str,
        stream_record: &StreamRecord,
        expected_table_fields: &[String],
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Get strategy for this specific table or use primary strategy
        let strategy = self
            .config
            .table_specific_strategies
            .get(table_name)
            .unwrap_or(&self.config.primary_strategy);

        if self.config.enable_logging {
            debug!(
                "Handling missing data for table '{}' using strategy: {:?}",
                table_name, strategy
            );
        }

        match strategy {
            TableMissingDataStrategy::UseDefaults(defaults) => {
                let enriched =
                    self.enrich_with_defaults(stream_record, defaults, expected_table_fields);
                if self.config.enable_logging {
                    info!(
                        "Applied default values for missing table '{}' data",
                        table_name
                    );
                }
                Ok(Some(enriched))
            }

            TableMissingDataStrategy::SkipRecord => {
                if self.config.enable_logging {
                    debug!("Skipping record due to missing table '{}' data", table_name);
                }
                Ok(None)
            }

            TableMissingDataStrategy::EmitWithNulls => {
                let enriched = self.add_null_fields(stream_record, expected_table_fields);
                if self.config.enable_logging {
                    debug!(
                        "Emitting record with NULL values for missing table '{}' data",
                        table_name
                    );
                }
                Ok(Some(enriched))
            }

            TableMissingDataStrategy::WaitAndRetry {
                max_retries,
                initial_delay,
                max_delay,
                backoff_multiplier,
            } => {
                if self.config.enable_logging {
                    warn!(
                        "Table '{}' data missing, will retry up to {} times",
                        table_name, max_retries
                    );
                }
                self.retry_with_backoff(
                    table_name,
                    stream_record,
                    expected_table_fields,
                    *max_retries,
                    *initial_delay,
                    *max_delay,
                    *backoff_multiplier,
                )
                .await
            }

            TableMissingDataStrategy::FailFast => Err(SqlError::ExecutionError {
                message: format!(
                    "Table '{}' data is missing and fail-fast strategy is configured",
                    table_name
                ),
                query: None,
            }),
        }
    }

    /// Enrich a stream record with default values for missing table fields
    fn enrich_with_defaults(
        &self,
        stream_record: &StreamRecord,
        defaults: &HashMap<String, FieldValue>,
        expected_table_fields: &[String],
    ) -> StreamRecord {
        let mut enriched_fields = stream_record.fields.clone();

        // Add default values for expected table fields
        for field_name in expected_table_fields {
            if let Some(default_value) = defaults.get(field_name) {
                enriched_fields.insert(field_name.clone(), default_value.clone());
            } else {
                // If no default provided, use a sensible default based on field name
                let default_value = self.infer_default_value(field_name);
                enriched_fields.insert(field_name.clone(), default_value);
            }
        }

        StreamRecord {
            timestamp: stream_record.timestamp,
            offset: stream_record.offset,
            partition: stream_record.partition,
            fields: enriched_fields,
            headers: stream_record.headers.clone(),
            event_time: stream_record.event_time,
        }
    }

    /// Add NULL values for missing table fields
    fn add_null_fields(
        &self,
        stream_record: &StreamRecord,
        expected_table_fields: &[String],
    ) -> StreamRecord {
        let mut enriched_fields = stream_record.fields.clone();

        // Add NULL values for all expected table fields
        for field_name in expected_table_fields {
            enriched_fields.insert(field_name.clone(), FieldValue::String("NULL".to_string()));
        }

        StreamRecord {
            timestamp: stream_record.timestamp,
            offset: stream_record.offset,
            partition: stream_record.partition,
            fields: enriched_fields,
            headers: stream_record.headers.clone(),
            event_time: stream_record.event_time,
        }
    }

    /// Retry table lookup with exponential backoff
    async fn retry_with_backoff(
        &self,
        table_name: &str,
        stream_record: &StreamRecord,
        expected_table_fields: &[String],
        max_retries: u32,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_multiplier: f64,
    ) -> Result<Option<StreamRecord>, SqlError> {
        let mut current_delay = initial_delay;

        for attempt in 1..=max_retries {
            if self.config.enable_logging {
                debug!(
                    "Retry attempt {}/{} for table '{}' after {:?}",
                    attempt, max_retries, table_name, current_delay
                );
            }

            // Wait before retry
            tokio::time::sleep(current_delay).await;

            // In a real implementation, this would retry the actual table lookup
            // For now, we'll simulate a retry that eventually succeeds or fails
            // TODO: Integrate with actual table lookup retry logic

            // Calculate next delay with exponential backoff
            current_delay = std::cmp::min(
                Duration::from_millis(
                    (current_delay.as_millis() as f64 * backoff_multiplier) as u64,
                ),
                max_delay,
            );

            // For this phase, we'll fall back to NULL values after max retries
            if attempt == max_retries {
                if self.config.enable_logging {
                    warn!(
                        "Max retries ({}) reached for table '{}', falling back to NULL values",
                        max_retries, table_name
                    );
                }
                return Ok(Some(
                    self.add_null_fields(stream_record, expected_table_fields),
                ));
            }
        }

        // Should never reach here, but fallback to error
        Err(SqlError::ExecutionError {
            message: format!(
                "Failed to retry table '{}' lookup after {} attempts",
                table_name, max_retries
            ),
            query: None,
        })
    }

    /// Infer a sensible default value based on field name
    fn infer_default_value(&self, field_name: &str) -> FieldValue {
        match field_name.to_lowercase().as_str() {
            name if name.contains("count") || name.contains("total") || name.contains("amount") => {
                FieldValue::Integer(0)
            }
            name if name.contains("rate") || name.contains("percent") || name.contains("ratio") => {
                FieldValue::Float(0.0)
            }
            name if name.contains("active")
                || name.contains("enabled")
                || name.contains("valid") =>
            {
                FieldValue::Boolean(false)
            }
            name if name.contains("name")
                || name.contains("description")
                || name.contains("status") =>
            {
                FieldValue::String("UNKNOWN".to_string())
            }
            _ => FieldValue::String("DEFAULT".to_string()),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &GracefulDegradationConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: GracefulDegradationConfig) {
        self.config = config;
    }

    /// Add a table-specific strategy
    pub fn add_table_strategy(&mut self, table_name: String, strategy: TableMissingDataStrategy) {
        self.config
            .table_specific_strategies
            .insert(table_name, strategy);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn create_test_stream_record() -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldValue::Integer(123));
        fields.insert(
            "event_type".to_string(),
            FieldValue::String("login".to_string()),
        );

        StreamRecord {
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            fields,
            headers: HashMap::new(),
            event_time: Some(chrono::Utc::now()),
        }
    }

    #[tokio::test]
    async fn test_use_defaults_strategy() {
        let mut defaults = HashMap::new();
        defaults.insert(
            "user_name".to_string(),
            FieldValue::String("Anonymous".to_string()),
        );
        defaults.insert(
            "user_tier".to_string(),
            FieldValue::String("Basic".to_string()),
        );

        let handler = GracefulDegradationHandler::with_defaults(defaults);
        let stream_record = create_test_stream_record();
        let expected_fields = vec!["user_name".to_string(), "user_tier".to_string()];

        let result = handler
            .handle_missing_table_data("user_profiles", &stream_record, &expected_fields)
            .await
            .unwrap();

        assert!(result.is_some());
        let enriched = result.unwrap();
        assert_eq!(
            enriched.fields.get("user_name"),
            Some(&FieldValue::String("Anonymous".to_string()))
        );
        assert_eq!(
            enriched.fields.get("user_tier"),
            Some(&FieldValue::String("Basic".to_string()))
        );
    }

    #[tokio::test]
    async fn test_skip_record_strategy() {
        let handler = GracefulDegradationHandler::skip_records();
        let stream_record = create_test_stream_record();
        let expected_fields = vec!["user_name".to_string()];

        let result = handler
            .handle_missing_table_data("user_profiles", &stream_record, &expected_fields)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_emit_with_nulls_strategy() {
        let handler = GracefulDegradationHandler::emit_with_nulls();
        let stream_record = create_test_stream_record();
        let expected_fields = vec!["user_name".to_string(), "user_tier".to_string()];

        let result = handler
            .handle_missing_table_data("user_profiles", &stream_record, &expected_fields)
            .await
            .unwrap();

        assert!(result.is_some());
        let enriched = result.unwrap();
        assert_eq!(
            enriched.fields.get("user_name"),
            Some(&FieldValue::String("NULL".to_string()))
        );
        assert_eq!(
            enriched.fields.get("user_tier"),
            Some(&FieldValue::String("NULL".to_string()))
        );
    }

    #[tokio::test]
    async fn test_fail_fast_strategy() {
        let handler = GracefulDegradationHandler::fail_fast();
        let stream_record = create_test_stream_record();
        let expected_fields = vec!["user_name".to_string()];

        let result = handler
            .handle_missing_table_data("user_profiles", &stream_record, &expected_fields)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_retry_strategy() {
        let handler = GracefulDegradationHandler::with_retry(2, Duration::from_millis(10));
        let stream_record = create_test_stream_record();
        let expected_fields = vec!["user_name".to_string()];

        let start = std::time::Instant::now();
        let result = handler
            .handle_missing_table_data("user_profiles", &stream_record, &expected_fields)
            .await
            .unwrap();

        let elapsed = start.elapsed();

        // Should have taken at least the retry delays
        assert!(elapsed >= Duration::from_millis(20)); // 2 retries with 10ms+ each

        // Should fall back to NULL values after retries
        assert!(result.is_some());
        let enriched = result.unwrap();
        assert_eq!(
            enriched.fields.get("user_name"),
            Some(&FieldValue::String("NULL".to_string()))
        );
    }
}
