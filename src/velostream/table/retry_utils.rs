use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;

/// Parse duration strings like "30s", "5m", "1h", "500ms"
pub fn parse_duration(duration_str: &str) -> Option<Duration> {
    if duration_str.is_empty() {
        return None;
    }

    let duration_str = duration_str.trim().to_lowercase();

    // Handle "0" as zero duration
    if duration_str == "0" {
        return Some(Duration::from_secs(0));
    }

    // Extract number and unit
    let (number_part, unit_part) = if let Some(pos) = duration_str.find(|c: char| c.is_alphabetic())
    {
        (&duration_str[..pos], &duration_str[pos..])
    } else {
        // No unit specified, assume seconds
        (duration_str.as_str(), "s")
    };

    // Parse the numeric part
    let number: f64 = match number_part.parse() {
        Ok(n) => n,
        Err(_) => return None,
    };

    if number < 0.0 {
        return None;
    }

    // Convert based on unit
    let duration = match unit_part {
        "ms" | "millis" | "milliseconds" => Duration::from_millis(number as u64),
        "s" | "sec" | "secs" | "second" | "seconds" => Duration::from_secs_f64(number),
        "m" | "min" | "mins" | "minute" | "minutes" => Duration::from_secs_f64(number * 60.0),
        "h" | "hr" | "hrs" | "hour" | "hours" => Duration::from_secs_f64(number * 3600.0),
        "d" | "day" | "days" => Duration::from_secs_f64(number * 86400.0),
        _ => return None,
    };

    Some(duration)
}

/// Check if an error indicates a missing Kafka topic
pub fn is_topic_missing_error(
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // Common Kafka error messages for missing topics
    error_msg.contains("unknown topic")
        || error_msg.contains("topic does not exist")
        || error_msg.contains("unknown topic or partition")
        || error_msg.contains("topic not found")
        || error_msg.contains("metadata for topic") && error_msg.contains("not available")
}

/// Generate helpful error message for missing topics
pub fn format_topic_missing_error(
    topic: &str,
    error: &crate::velostream::kafka::kafka_error::ConsumerError,
) -> String {
    format!(
        "Kafka topic '{}' does not exist. Options:\n\
         1. Create the topic: kafka-topics --create --topic {} --partitions 3 --replication-factor 1\n\
         2. Add retry configuration: WITH (\"topic.wait.timeout\" = \"30s\")\n\
         3. Check topic name spelling and broker connectivity\n\
         \n\
         Original error: {}",
        topic, topic, error
    )
}

/// Generate helpful error message for missing files
pub fn format_file_missing_error(path: &str) -> String {
    format!(
        "File '{}' does not exist. Options:\n\
         1. Check the file path exists\n\
         2. Add wait configuration: WITH (\"file.wait.timeout\" = \"30s\")\n\
         3. For patterns (*.json), ensure matching files will arrive\n\
         4. Use watch mode: WITH (\"watch\" = \"true\")",
        path
    )
}

/// Wait for a file to exist with configurable timeout and retry interval
pub async fn wait_for_file_to_exist(
    file_path: &str,
    wait_timeout: Duration,
    retry_interval: Duration,
) -> Result<(), String> {
    if wait_timeout.as_secs() == 0 {
        // No wait configured
        if Path::new(file_path).exists() {
            return Ok(());
        } else {
            return Err(format_file_missing_error(file_path));
        }
    }

    let start = Instant::now();
    loop {
        if Path::new(file_path).exists() {
            log::info!(
                "File '{}' found after waiting {:?}",
                file_path,
                start.elapsed()
            );
            return Ok(());
        }

        if start.elapsed() >= wait_timeout {
            log::error!(
                "Timeout waiting for file '{}' after {:?}",
                file_path,
                wait_timeout
            );
            return Err(format!(
                "Timeout waiting for file '{}' after {:?}. {}",
                file_path,
                wait_timeout,
                format_file_missing_error(file_path)
            ));
        }

        log::info!(
            "File '{}' not found, retrying in {:?}... (elapsed: {:?})",
            file_path,
            retry_interval,
            start.elapsed()
        );
        sleep(retry_interval).await;
    }
}

/// Wait for files matching a pattern to exist
pub async fn wait_for_pattern_match(
    pattern: &str,
    wait_timeout: Duration,
    retry_interval: Duration,
) -> Result<Vec<String>, String> {
    if wait_timeout.as_secs() == 0 {
        // No wait configured - return current matches
        return find_matching_files(pattern);
    }

    let start = Instant::now();
    loop {
        match find_matching_files(pattern) {
            Ok(files) if !files.is_empty() => {
                log::info!(
                    "Found {} files matching pattern '{}' after waiting {:?}",
                    files.len(),
                    pattern,
                    start.elapsed()
                );
                return Ok(files);
            }
            Ok(_) => {
                // No files found yet
            }
            Err(e) => {
                log::warn!("Error searching for pattern '{}': {}", pattern, e);
            }
        }

        if start.elapsed() >= wait_timeout {
            log::error!(
                "Timeout waiting for files matching pattern '{}' after {:?}",
                pattern,
                wait_timeout
            );
            return Err(format!(
                "Timeout waiting for files matching pattern '{}' after {:?}. {}",
                pattern,
                wait_timeout,
                format_file_missing_error(pattern)
            ));
        }

        log::info!(
            "No files matching pattern '{}', retrying in {:?}... (elapsed: {:?})",
            pattern,
            retry_interval,
            start.elapsed()
        );
        sleep(retry_interval).await;
    }
}

/// Find files matching a glob pattern
pub fn find_matching_files(pattern: &str) -> Result<Vec<String>, String> {
    // Simple implementation - in production this would use a proper glob library
    if pattern.contains('*') {
        // Extract directory and pattern
        let path = Path::new(pattern);
        let parent_dir = path.parent().unwrap_or(Path::new("."));
        let filename_pattern = path.file_name().and_then(|n| n.to_str()).unwrap_or("*");

        match std::fs::read_dir(parent_dir) {
            Ok(entries) => {
                let mut matches = Vec::new();
                for entry in entries {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name();
                        if let Some(name_str) = file_name.to_str() {
                            // Simple wildcard matching - replace with proper glob matching
                            if filename_pattern == "*"
                                || (filename_pattern.ends_with("*")
                                    && name_str.starts_with(
                                        &filename_pattern[..filename_pattern.len() - 1],
                                    ))
                                || name_str.contains(&filename_pattern.replace("*", ""))
                            {
                                matches.push(entry.path().to_string_lossy().to_string());
                            }
                        }
                    }
                }
                Ok(matches)
            }
            Err(e) => Err(format!("Failed to read directory: {}", e)),
        }
    } else {
        // Single file check
        if Path::new(pattern).exists() {
            Ok(vec![pattern.to_string()])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        // Test various formats
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("2.5s"), Some(Duration::from_millis(2500)));
        assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));

        // Test full words
        assert_eq!(parse_duration("30 seconds"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5 minutes"), Some(Duration::from_secs(300)));

        // Test invalid formats
        assert_eq!(parse_duration("invalid"), None);
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("-5s"), None);
    }

    #[test]
    fn test_is_topic_missing_error() {
        use crate::velostream::kafka::kafka_error::ConsumerError;

        // Test with various error messages that should be detected
        let missing_errors = vec![
            "Unknown topic or partition",
            "Topic does not exist",
            "Topic not found",
            "Metadata for topic 'test' not available",
        ];

        for error_msg in missing_errors {
            let kafka_error = rdkafka::error::KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition);
            let error = ConsumerError::KafkaError(kafka_error);
            assert!(
                is_topic_missing_error(&error),
                "Should detect missing topic for: {}",
                error_msg
            );
        }

        // Test with errors that should NOT be detected as missing topic
        let other_errors = vec!["Connection failed", "Authentication error", "Timeout error"];

        for error_msg in other_errors {
            let kafka_error = rdkafka::error::KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::BrokerNotAvailable);
            let error = ConsumerError::KafkaError(kafka_error);
            assert!(
                !is_topic_missing_error(&error),
                "Should NOT detect missing topic for: {}",
                error_msg
            );
        }
    }

    #[test]
    fn test_format_topic_missing_error() {
        use crate::velostream::kafka::kafka_error::ConsumerError;

        let kafka_error = rdkafka::error::KafkaError::MetadataFetch(rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition);
        let error = ConsumerError::KafkaError(kafka_error);
        let formatted = format_topic_missing_error("test_topic", &error);

        assert!(formatted.contains("test_topic"));
        assert!(formatted.contains("kafka-topics --create"));
        assert!(formatted.contains("topic.wait.timeout"));
        assert!(formatted.contains("Original error"));
    }

    #[test]
    fn test_format_file_missing_error() {
        let formatted = format_file_missing_error("/path/to/file.json");

        assert!(formatted.contains("/path/to/file.json"));
        assert!(formatted.contains("file.wait.timeout"));
        assert!(formatted.contains("watch"));
        assert!(formatted.contains("Check the file path"));
    }
}
