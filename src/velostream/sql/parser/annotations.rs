// SQL Annotation Parser
//
// This module parses annotations from SQL comments to enable declarative
// configuration and metadata directly in SQL files.
//
// Supported annotations:
//
// ## Job Name Annotation
// - @job_name: <custom_name>           (optional)
//   Provides a human-readable name for the job instead of auto-generated name
//
// ## Partitioning Strategy Annotations
// - @partitioning_strategy: <strategy> (optional)
//   Selects partitioning strategy: always_hash, smart_repartition, sticky_partition, round_robin, fan_in
// - @sticky-partition-id: <partition_id> (optional)
//   For sticky_partition strategy: specifies which partition to pin records to (0-based index)
// - @partition-count: <count>          (optional)
//   Specifies number of partitions for V2 architecture (overrides default CPU count)
//
// ## Metric Annotations
// - @metric: <name>                    (required)
// - @metric_type: counter|gauge|histogram  (required)
// - @metric_help: "<description>"      (optional)
// - @metric_labels: label1, label2     (optional)
// - @metric_condition: <expression>    (optional)
// - @metric_sample_rate: <0.0-1.0>    (optional)
// - @metric_field: <field_name>        (optional, required for gauge/histogram)
// - @metric_buckets: [v1, v2, ...]    (optional, for histogram)

use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;

/// Metric annotation parsed from SQL comments
#[derive(Debug, Clone, PartialEq)]
pub struct MetricAnnotation {
    /// Metric name (e.g., "velo_trading_volume_spikes_total")
    pub name: String,

    /// Type of Prometheus metric
    pub metric_type: MetricType,

    /// Help text displayed in Prometheus
    pub help: Option<String>,

    /// Field names to use as Prometheus labels
    pub labels: Vec<String>,

    /// SQL condition expression to filter which records emit metrics
    pub condition: Option<String>,

    /// Sampling rate (0.0 to 1.0), default 1.0 means 100% sampling
    pub sample_rate: f64,

    /// Field name to measure (for gauge and histogram)
    pub field: Option<String>,

    /// Histogram bucket boundaries (for histogram type)
    pub buckets: Option<Vec<f64>>,
}

impl Default for MetricAnnotation {
    fn default() -> Self {
        Self {
            name: String::new(),
            metric_type: MetricType::Counter,
            help: None,
            labels: Vec::new(),
            condition: None,
            sample_rate: 1.0,
            field: None,
            buckets: None,
        }
    }
}

/// Type of Prometheus metric
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Counter - monotonically increasing value
    Counter,
    /// Gauge - value that can increase or decrease
    Gauge,
    /// Histogram - distribution of values across buckets
    Histogram,
}

impl MetricType {
    /// Parse metric type from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, SqlError> {
        match s.to_lowercase().trim() {
            "counter" => Ok(MetricType::Counter),
            "gauge" => Ok(MetricType::Gauge),
            "histogram" => Ok(MetricType::Histogram),
            _ => Err(SqlError::ParseError {
                message: format!(
                    "Invalid metric type '{}'. Must be 'counter', 'gauge', or 'histogram'",
                    s
                ),
                position: None,
            }),
        }
    }
}

/// Partition configuration annotations parsed from SQL comments
///
/// Allows users to control partitioning behavior directly in SQL via annotations:
/// - `@sticky_partition_id`: Which partition to pin records to (for StickyPartition strategy)
/// - `@partition_count`: Number of partitions for V2 architecture
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionAnnotations {
    /// Partition ID for sticky partition strategy (0-based index)
    /// When using StickyPartition strategy, records can be pinned to a specific partition
    pub sticky_partition_id: Option<usize>,

    /// Number of partitions for V2 architecture
    /// Overrides the default (CPU count) if specified
    pub partition_count: Option<usize>,
}

impl Default for PartitionAnnotations {
    fn default() -> Self {
        Self {
            sticky_partition_id: None,
            partition_count: None,
        }
    }
}

/// Parse job name annotation from comment tokens
///
/// Extracts @job_name annotation from SQL comments that appear before
/// a CREATE STREAM statement.
///
/// # Arguments
/// * `comments` - Comment tokens from tokenize_with_comments()
///
/// # Returns
/// * `Ok(Option<String>)` - Parsed job name if present
/// * `Err(SqlError)` - Parse error with details
///
/// # Example
/// ```no_run
/// use velostream::velostream::sql::parser::annotations::parse_job_name;
///
/// let comments = vec![
///     "-- @job_name: tick_buckets".to_string(),
/// ];
/// let job_name = parse_job_name(&comments).unwrap();
/// assert_eq!(job_name, Some("tick_buckets".to_string()));
/// ```
pub fn parse_job_name(comments: &[String]) -> Result<Option<String>, SqlError> {
    for comment in comments {
        let trimmed = comment.trim();

        // Skip non-annotation comments
        if !trimmed.starts_with('@') {
            continue;
        }

        // Parse annotation directive
        if let Some((directive, value)) = parse_annotation_line(trimmed) {
            if directive == "job_name" {
                let name = value.trim().to_string();

                // Validate job name
                validate_job_name(&name)?;

                return Ok(Some(name));
            }
        }
    }

    Ok(None)
}

/// Validate job name follows naming conventions
///
/// Rules: alphanumeric, underscores, hyphens, max 63 characters
fn validate_job_name(name: &str) -> Result<(), SqlError> {
    if name.is_empty() {
        return Err(SqlError::ParseError {
            message: "Job name cannot be empty".to_string(),
            position: None,
        });
    }

    if name.len() > 63 {
        return Err(SqlError::ParseError {
            message: format!(
                "Job name '{}' too long ({}). Maximum 63 characters allowed",
                name,
                name.len()
            ),
            position: None,
        });
    }

    for ch in name.chars() {
        if !ch.is_alphanumeric() && ch != '_' && ch != '-' {
            return Err(SqlError::ParseError {
                message: format!(
                    "Invalid character '{}' in job name '{}'. Only alphanumeric, underscore, and hyphen allowed",
                    ch, name
                ),
                position: None,
            });
        }
    }

    Ok(())
}

/// Parse partition configuration annotations from comment tokens
///
/// Extracts `@sticky_partition_id` and `@partition_count` annotations from SQL comments
/// that appear before a CREATE STREAM statement.
///
/// # Arguments
/// * `comments` - Comment tokens from tokenize_with_comments()
///
/// # Returns
/// * `Ok(PartitionAnnotations)` - Parsed annotations (fields are Option, may be None)
/// * `Err(SqlError)` - Parse error with details
///
/// # Example
/// ```no_run
/// use velostream::velostream::sql::parser::annotations::parse_partition_annotations;
///
/// let comments = vec![
///     "-- @sticky-partition-id: 2".to_string(),
///     "-- @partition-count: 8".to_string(),
/// ];
/// let annotations = parse_partition_annotations(&comments).unwrap();
/// assert_eq!(annotations.sticky_partition_id, Some(2));
/// assert_eq!(annotations.partition_count, Some(8));
/// ```
pub fn parse_partition_annotations(comments: &[String]) -> Result<PartitionAnnotations, SqlError> {
    let mut annotations = PartitionAnnotations::default();

    for comment in comments {
        let trimmed = comment.trim();

        // Skip non-annotation comments
        if !trimmed.starts_with('@') {
            continue;
        }

        // Parse annotation directive
        if let Some((directive, value)) = parse_annotation_line(trimmed) {
            match directive.as_str() {
                "sticky-partition-id" => {
                    let partition_id = value.trim().parse::<usize>().map_err(|_| {
                        SqlError::ParseError {
                            message: format!(
                                "Invalid sticky-partition-id '{}'. Must be a non-negative integer",
                                value
                            ),
                            position: None,
                        }
                    })?;
                    annotations.sticky_partition_id = Some(partition_id);
                }
                "partition-count" => {
                    let count = value.trim().parse::<usize>().map_err(|_| {
                        SqlError::ParseError {
                            message: format!(
                                "Invalid partition-count '{}'. Must be a positive integer",
                                value
                            ),
                            position: None,
                        }
                    })?;

                    if count == 0 {
                        return Err(SqlError::ParseError {
                            message: "partition-count must be at least 1".to_string(),
                            position: None,
                        });
                    }

                    annotations.partition_count = Some(count);
                }
                _ => {
                    // Unknown directive - will be handled by other annotation parsers
                    // or ignored if not a known annotation
                }
            }
        }
    }

    Ok(annotations)
}

/// Parse metric annotations from comment tokens
///
/// Extracts @metric annotations from SQL comments that appear before
/// a CREATE STREAM statement.
///
/// # Arguments
/// * `comments` - Comment tokens from tokenize_with_comments()
///
/// # Returns
/// * `Ok(Vec<MetricAnnotation>)` - Parsed annotations (may be empty)
/// * `Err(SqlError)` - Parse error with details
///
/// # Example
/// ```no_run
/// use velostream::velostream::sql::parser::annotations::parse_metric_annotations;
///
/// let comments = vec![
///     "-- @metric: my_metric_total".to_string(),
///     "-- @metric_type: counter".to_string(),
///     "-- @metric_labels: symbol, status".to_string(),
/// ];
/// let annotations = parse_metric_annotations(&comments).unwrap();
/// assert_eq!(annotations.len(), 1);
/// assert_eq!(annotations[0].name, "my_metric_total");
/// ```
pub fn parse_metric_annotations(comments: &[String]) -> Result<Vec<MetricAnnotation>, SqlError> {
    let mut annotations = Vec::new();
    let mut current_annotation: Option<MetricAnnotation> = None;

    for comment in comments {
        let trimmed = comment.trim();

        // Skip non-annotation comments
        if !trimmed.starts_with('@') {
            continue;
        }

        // Parse annotation directive
        if let Some((directive, value)) = parse_annotation_line(trimmed) {
            match directive.as_str() {
                "metric" => {
                    // Start new annotation
                    if let Some(annotation) = current_annotation.take() {
                        validate_annotation(&annotation)?;
                        annotations.push(annotation);
                    }
                    let new_annotation = MetricAnnotation {
                        name: value.trim().to_string(),
                        ..MetricAnnotation::default()
                    };
                    current_annotation = Some(new_annotation);
                }
                "metric_type" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.metric_type = MetricType::from_str(&value)?;
                    } else {
                        return Err(SqlError::ParseError {
                            message: "@metric_type annotation found without preceding @metric"
                                .to_string(),
                            position: None,
                        });
                    }
                }
                "metric_help" => {
                    if let Some(ref mut annotation) = current_annotation {
                        // Remove surrounding quotes if present
                        let help_text = value.trim().trim_matches('"').trim_matches('\'');
                        annotation.help = Some(help_text.to_string());
                    }
                }
                "metric_labels" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.labels = value
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                }
                "metric_condition" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.condition = Some(value.trim().to_string());
                    }
                }
                "metric_sample_rate" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.sample_rate = value.trim().parse::<f64>().map_err(|_| {
                            SqlError::ParseError {
                                message: format!(
                                    "Invalid sample_rate '{}'. Must be a number between 0.0 and 1.0",
                                    value
                                ),
                                position: None,
                            }
                        })?;

                        // Validate range
                        if annotation.sample_rate < 0.0 || annotation.sample_rate > 1.0 {
                            return Err(SqlError::ParseError {
                                message: format!(
                                    "Sample rate {} out of range. Must be between 0.0 and 1.0",
                                    annotation.sample_rate
                                ),
                                position: None,
                            });
                        }
                    }
                }
                "metric_field" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.field = Some(value.trim().to_string());
                    }
                }
                "metric_buckets" => {
                    if let Some(ref mut annotation) = current_annotation {
                        annotation.buckets = Some(parse_buckets(&value)?);
                    }
                }
                _ => {
                    // Unknown annotation directive - skip with warning
                    log::warn!("Unknown metric annotation directive: @{}", directive);
                }
            }
        }
    }

    // Push the last annotation if any
    if let Some(annotation) = current_annotation {
        validate_annotation(&annotation)?;
        annotations.push(annotation);
    }

    Ok(annotations)
}

/// Parse a single annotation line into (directive, value)
///
/// Examples:
/// - "@metric: my_metric" → Some(("metric", "my_metric"))
/// - "@metric_type: counter" → Some(("metric_type", "counter"))
fn parse_annotation_line(line: &str) -> Option<(String, String)> {
    let line = line.trim().trim_start_matches('@');

    if let Some(colon_pos) = line.find(':') {
        let directive = line[..colon_pos].trim().to_string();
        let value = line[colon_pos + 1..].trim().to_string();
        Some((directive, value))
    } else {
        None
    }
}

/// Parse histogram bucket boundaries from string
///
/// Example: "[0.1, 0.5, 1.0, 5.0]" → vec![0.1, 0.5, 1.0, 5.0]
fn parse_buckets(value: &str) -> Result<Vec<f64>, SqlError> {
    let trimmed = value.trim().trim_start_matches('[').trim_end_matches(']');

    let buckets: Result<Vec<f64>, _> = trimmed
        .split(',')
        .map(|s| s.trim().parse::<f64>())
        .collect();

    buckets.map_err(|_| SqlError::ParseError {
        message: format!(
            "Invalid bucket values: {}. Expected comma-separated numbers",
            value
        ),
        position: None,
    })
}

/// Validate that a metric annotation has all required fields
fn validate_annotation(annotation: &MetricAnnotation) -> Result<(), SqlError> {
    // Check required fields
    if annotation.name.is_empty() {
        return Err(SqlError::ParseError {
            message: "Metric annotation missing required field: @metric".to_string(),
            position: None,
        });
    }

    // Validate metric name (Prometheus naming rules)
    validate_metric_name(&annotation.name)?;

    // Gauge and Histogram require a field
    match annotation.metric_type {
        MetricType::Gauge | MetricType::Histogram => {
            if annotation.field.is_none() {
                return Err(SqlError::ParseError {
                    message: format!(
                        "Metric '{}' with type {:?} requires @metric_field annotation",
                        annotation.name, annotation.metric_type
                    ),
                    position: None,
                });
            }
        }
        MetricType::Counter => {
            // Counters don't require a field
        }
    }

    Ok(())
}

/// Validate metric name follows Prometheus naming conventions
///
/// Rules: [a-zA-Z_:][a-zA-Z0-9_:]*
fn validate_metric_name(name: &str) -> Result<(), SqlError> {
    if name.is_empty() {
        return Err(SqlError::ParseError {
            message: "Metric name cannot be empty".to_string(),
            position: None,
        });
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_alphabetic() && first_char != '_' && first_char != ':' {
        return Err(SqlError::ParseError {
            message: format!(
                "Invalid metric name '{}'. Must start with letter, underscore, or colon",
                name
            ),
            position: None,
        });
    }

    for ch in name.chars() {
        if !ch.is_alphanumeric() && ch != '_' && ch != ':' {
            return Err(SqlError::ParseError {
                message: format!(
                    "Invalid character '{}' in metric name '{}'. Only alphanumeric, underscore, and colon allowed",
                    ch, name
                ),
                position: None,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metric_type() {
        assert_eq!(
            MetricType::from_str("counter").unwrap(),
            MetricType::Counter
        );
        assert_eq!(
            MetricType::from_str("COUNTER").unwrap(),
            MetricType::Counter
        );
        assert_eq!(MetricType::from_str("gauge").unwrap(), MetricType::Gauge);
        assert_eq!(
            MetricType::from_str("histogram").unwrap(),
            MetricType::Histogram
        );

        assert!(MetricType::from_str("invalid").is_err());
    }

    #[test]
    fn test_parse_annotation_line() {
        let result = parse_annotation_line("@metric: test_metric_total");
        assert_eq!(
            result,
            Some(("metric".to_string(), "test_metric_total".to_string()))
        );

        let result = parse_annotation_line("@metric_type: counter");
        assert_eq!(
            result,
            Some(("metric_type".to_string(), "counter".to_string()))
        );
    }

    #[test]
    fn test_parse_buckets() {
        let result = parse_buckets("[0.1, 0.5, 1.0, 5.0]").unwrap();
        assert_eq!(result, vec![0.1, 0.5, 1.0, 5.0]);

        let result = parse_buckets("[1, 10, 100]").unwrap();
        assert_eq!(result, vec![1.0, 10.0, 100.0]);

        assert!(parse_buckets("[invalid]").is_err());
    }

    #[test]
    fn test_validate_metric_name() {
        assert!(validate_metric_name("valid_metric_name").is_ok());
        assert!(validate_metric_name("metric_123").is_ok());
        assert!(validate_metric_name("_metric").is_ok());
        assert!(validate_metric_name("metric:subsystem:name").is_ok());

        assert!(validate_metric_name("").is_err());
        assert!(validate_metric_name("123_invalid").is_err());
        assert!(validate_metric_name("invalid-metric").is_err());
        assert!(validate_metric_name("invalid metric").is_err());
    }

    #[test]
    fn test_parse_partition_annotations_empty() {
        let comments = vec![];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, None);
        assert_eq!(annotations.partition_count, None);
    }

    #[test]
    fn test_parse_partition_annotations_sticky_partition_id() {
        let comments = vec!["-- @sticky-partition-id: 2".to_string()];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, Some(2));
        assert_eq!(annotations.partition_count, None);
    }

    #[test]
    fn test_parse_partition_annotations_partition_count() {
        let comments = vec!["-- @partition-count: 8".to_string()];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, None);
        assert_eq!(annotations.partition_count, Some(8));
    }

    #[test]
    fn test_parse_partition_annotations_both() {
        let comments = vec![
            "-- @sticky-partition-id: 3".to_string(),
            "-- @partition-count: 16".to_string(),
        ];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, Some(3));
        assert_eq!(annotations.partition_count, Some(16));
    }

    #[test]
    fn test_parse_partition_annotations_with_other_comments() {
        let comments = vec![
            "-- This is a regular comment".to_string(),
            "-- @sticky-partition-id: 1".to_string(),
            "-- Another comment".to_string(),
            "-- @partition-count: 4".to_string(),
        ];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, Some(1));
        assert_eq!(annotations.partition_count, Some(4));
    }

    #[test]
    fn test_parse_partition_annotations_invalid_sticky_partition_id() {
        let comments = vec!["-- @sticky-partition-id: invalid".to_string()];
        let result = parse_partition_annotations(&comments);
        assert!(result.is_err());
        if let Err(SqlError::ParseError { message, .. }) = result {
            assert!(message.contains("Invalid sticky-partition-id"));
        }
    }

    #[test]
    fn test_parse_partition_annotations_invalid_partition_count() {
        let comments = vec!["-- @partition-count: not_a_number".to_string()];
        let result = parse_partition_annotations(&comments);
        assert!(result.is_err());
        if let Err(SqlError::ParseError { message, .. }) = result {
            assert!(message.contains("Invalid partition-count"));
        }
    }

    #[test]
    fn test_parse_partition_annotations_zero_partition_count() {
        let comments = vec!["-- @partition-count: 0".to_string()];
        let result = parse_partition_annotations(&comments);
        assert!(result.is_err());
        if let Err(SqlError::ParseError { message, .. }) = result {
            assert!(message.contains("partition-count must be at least 1"));
        }
    }

    #[test]
    fn test_parse_partition_annotations_large_values() {
        let comments = vec![
            "-- @sticky-partition-id: 999".to_string(),
            "-- @partition-count: 1024".to_string(),
        ];
        let annotations = parse_partition_annotations(&comments).unwrap();
        assert_eq!(annotations.sticky_partition_id, Some(999));
        assert_eq!(annotations.partition_count, Some(1024));
    }

    #[test]
    fn test_partition_annotations_default() {
        let annotations = PartitionAnnotations::default();
        assert_eq!(annotations.sticky_partition_id, None);
        assert_eq!(annotations.partition_count, None);
    }
}
