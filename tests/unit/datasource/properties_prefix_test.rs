//! Tests for datasource property loading with source./sink. prefix support

use std::collections::HashMap;
use velostream::velostream::datasource::file::config::FileFormat;
use velostream::velostream::datasource::file::{FileDataSink, FileDataSource};
use velostream::velostream::datasource::kafka::{KafkaDataSink, KafkaDataSource};

#[cfg(test)]
mod properties_prefix_tests {
    use super::*;

    #[test]
    fn test_kafka_data_source_with_source_prefix() {
        let mut props = HashMap::new();
        props.insert(
            "source.brokers".to_string(),
            "broker1:9092,broker2:9092".to_string(),
        );
        props.insert("source.topic".to_string(), "test_topic".to_string());
        props.insert("source.group_id".to_string(), "test_group".to_string());
        props.insert("source.value.format".to_string(), "json".to_string());

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);

        // Verify that prefixed properties are used
        assert_eq!(kafka_source.brokers(), "broker1:9092,broker2:9092");
        assert_eq!(kafka_source.topic(), "test_topic");
        assert_eq!(kafka_source.group_id(), &Some("test_group".to_string()));

        // Config should have prefixes stripped
        assert_eq!(
            kafka_source.config().get("brokers").unwrap(),
            "broker1:9092,broker2:9092"
        );
        assert_eq!(kafka_source.config().get("topic").unwrap(), "test_topic");
        assert_eq!(kafka_source.config().get("group_id").unwrap(), "test_group");
        assert_eq!(kafka_source.config().get("value.format").unwrap(), "json");
    }

    #[test]
    fn test_kafka_data_source_fallback_to_unprefixed() {
        let mut props = HashMap::new();
        props.insert("brokers".to_string(), "localhost:9092".to_string());
        props.insert("topic".to_string(), "fallback_topic".to_string());
        props.insert("group_id".to_string(), "fallback_group".to_string());

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);

        // Verify that unprefixed properties are used as fallback
        assert_eq!(kafka_source.brokers(), "localhost:9092");
        assert_eq!(kafka_source.topic(), "fallback_topic");
        assert_eq!(kafka_source.group_id(), &Some("fallback_group".to_string()));
    }

    #[test]
    fn test_kafka_data_source_prefix_priority() {
        let mut props = HashMap::new();
        // Both prefixed and unprefixed versions
        props.insert(
            "source.brokers".to_string(),
            "prefixed_broker:9092".to_string(),
        );
        props.insert("brokers".to_string(), "unprefixed_broker:9092".to_string());
        props.insert("source.topic".to_string(), "prefixed_topic".to_string());
        props.insert("topic".to_string(), "unprefixed_topic".to_string());

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);

        // Prefixed properties should take priority
        assert_eq!(kafka_source.brokers(), "prefixed_broker:9092");
        assert_eq!(kafka_source.topic(), "prefixed_topic");

        // Config should only contain the prefixed version (with prefix stripped)
        assert_eq!(
            kafka_source.config().get("brokers").unwrap(),
            "prefixed_broker:9092"
        );
        assert_eq!(
            kafka_source.config().get("topic").unwrap(),
            "prefixed_topic"
        );
        // Unprefixed version should not be in config when prefixed exists
        assert!(!kafka_source.config().contains_key("unprefixed_broker"));
    }

    #[test]
    fn test_kafka_data_source_bootstrap_servers_alias() {
        let mut props = HashMap::new();
        props.insert(
            "source.bootstrap.servers".to_string(),
            "bootstrap_broker:9092".to_string(),
        );

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);

        // bootstrap.servers should be recognized as broker alias
        assert_eq!(kafka_source.brokers(), "bootstrap_broker:9092");
    }

    #[test]
    fn test_kafka_data_source_defaults() {
        let props = HashMap::new();

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);

        // Verify defaults
        assert_eq!(kafka_source.brokers(), "localhost:9092");
        assert_eq!(kafka_source.topic(), "default_topic");
        assert_eq!(kafka_source.group_id(), &Some("velo_test_job".to_string()));
    }

    #[test]
    fn test_kafka_data_sink_with_sink_prefix() {
        let mut props = HashMap::new();
        props.insert("sink.brokers".to_string(), "sink_broker:9092".to_string());
        props.insert("sink.topic".to_string(), "sink_topic".to_string());
        props.insert("sink.value.format".to_string(), "avro".to_string());

        let kafka_sink =
            KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);

        // Verify that prefixed properties are used
        assert_eq!(kafka_sink.brokers(), "sink_broker:9092");
        assert_eq!(kafka_sink.topic(), "sink_topic");

        // Config should have prefixes stripped
        assert_eq!(
            kafka_sink.config().get("brokers").unwrap(),
            "sink_broker:9092"
        );
        assert_eq!(kafka_sink.config().get("topic").unwrap(), "sink_topic");
        assert_eq!(kafka_sink.config().get("value.format").unwrap(), "avro");
    }

    #[test]
    fn test_kafka_data_sink_fallback_and_defaults() {
        let mut props = HashMap::new();
        props.insert(
            "bootstrap.servers".to_string(),
            "fallback_broker:9092".to_string(),
        );

        let kafka_sink =
            KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);

        // Verify fallback and defaults
        assert_eq!(kafka_sink.brokers(), "fallback_broker:9092");
        assert_eq!(kafka_sink.topic(), "test_job_output");
    }

    #[test]
    fn test_file_data_source_with_source_prefix() {
        let mut props = HashMap::new();
        props.insert("source.path".to_string(), "/path/to/source.csv".to_string());
        props.insert("source.format".to_string(), "csv".to_string());
        props.insert("source.watching".to_string(), "true".to_string());
        props.insert("source.has_headers".to_string(), "false".to_string());

        let file_source = FileDataSource::from_properties(&props);

        // Verify configuration was applied
        let config = file_source.config().unwrap();
        assert_eq!(config.path, "/path/to/source.csv");
        assert!(matches!(config.format, FileFormat::Csv));
        assert!(config.watch_for_changes);
        assert!(!config.csv_has_header);
    }

    #[test]
    fn test_file_data_source_fallback_properties() {
        let mut props = HashMap::new();
        props.insert("path".to_string(), "/fallback/path.json".to_string());
        props.insert("format".to_string(), "json".to_string());
        props.insert("watch".to_string(), "false".to_string());
        props.insert("header".to_string(), "true".to_string());

        let file_source = FileDataSource::from_properties(&props);

        // Verify fallback properties are used
        let config = file_source.config().unwrap();
        assert_eq!(config.path, "/fallback/path.json");
        assert!(matches!(config.format, FileFormat::Json));
        assert!(!config.watch_for_changes);
        assert!(config.csv_has_header);
    }

    #[test]
    fn test_file_data_source_prefix_priority() {
        let mut props = HashMap::new();
        props.insert("source.path".to_string(), "/prefixed/path.csv".to_string());
        props.insert("path".to_string(), "/unprefixed/path.csv".to_string());
        props.insert("source.watching".to_string(), "true".to_string());
        props.insert("watch".to_string(), "false".to_string());

        let file_source = FileDataSource::from_properties(&props);

        // Prefixed properties should take priority
        let config = file_source.config().unwrap();
        assert_eq!(config.path, "/prefixed/path.csv");
        assert!(config.watch_for_changes);
    }

    #[test]
    fn test_file_data_source_property_aliases() {
        let mut props = HashMap::new();
        props.insert("source.watching".to_string(), "true".to_string());
        props.insert("watch".to_string(), "false".to_string()); // Should be ignored due to watching

        let file_source = FileDataSource::from_properties(&props);

        let config = file_source.config().unwrap();
        // "watching" should take priority over "watch" alias
        assert!(config.watch_for_changes);
    }

    #[test]
    fn test_file_data_source_defaults() {
        let props = HashMap::new();

        let file_source = FileDataSource::from_properties(&props);

        let config = file_source.config().unwrap();
        assert_eq!(config.path, "./demo_data/sample.csv");
        assert!(matches!(config.format, FileFormat::Csv));
        assert!(!config.watch_for_changes);
        assert!(config.csv_has_header);
    }

    #[test]
    fn test_file_sink_with_sink_prefix() {
        let mut props = HashMap::new();
        props.insert("sink.path".to_string(), "/output/sink.json".to_string());
        props.insert("sink.format".to_string(), "json".to_string());
        props.insert("sink.append".to_string(), "true".to_string());
        props.insert("sink.has_headers".to_string(), "false".to_string());

        let file_sink = FileDataSink::from_properties(&props);

        // Verify configuration was applied
        let config = file_sink.config().unwrap();
        assert_eq!(config.path, "/output/sink.json");
        assert!(matches!(config.format, FileFormat::Json));
        assert!(config.append_if_exists);
        assert!(!config.csv_has_header);
    }

    #[test]
    fn test_file_sink_fallback_and_defaults() {
        let mut props = HashMap::new();
        props.insert("format".to_string(), "csv".to_string());
        props.insert("append".to_string(), "false".to_string());

        let file_sink = FileDataSink::from_properties(&props);

        let config = file_sink.config().unwrap();
        assert_eq!(config.path, "output.json"); // Default
        assert!(matches!(config.format, FileFormat::Csv));
        assert!(!config.append_if_exists);
        assert!(config.csv_has_header); // Default
    }

    #[test]
    fn test_file_sink_jsonlines_format() {
        let mut props = HashMap::new();
        props.insert("sink.format".to_string(), "jsonlines".to_string());

        let file_sink = FileDataSink::from_properties(&props);

        let config = file_sink.config().unwrap();
        assert!(matches!(config.format, FileFormat::JsonLines));
    }

    #[test]
    fn test_mixed_prefix_scenario() {
        // Test a realistic scenario with mixed prefixed and unprefixed properties
        let mut props = HashMap::new();

        // Source properties with mix of prefixed and unprefixed
        props.insert("source.brokers".to_string(), "kafka1:9092".to_string());
        props.insert("topic".to_string(), "fallback_topic".to_string()); // Should be used as fallback for source
        props.insert("source.group_id".to_string(), "special_group".to_string());

        // Sink properties
        props.insert("sink.brokers".to_string(), "kafka2:9092".to_string());
        props.insert("sink.topic".to_string(), "output_topic".to_string());

        // Job-level properties (no prefix)
        props.insert(
            "failure_strategy".to_string(),
            "RetryWithBackoff".to_string(),
        );
        props.insert("batch_size".to_string(), "1000".to_string());

        let kafka_source =
            KafkaDataSource::from_properties(&props, "default_topic", "test_job", None, None);
        let kafka_sink =
            KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);

        // Source should use source-prefixed properties and defaults
        assert_eq!(kafka_source.brokers(), "kafka1:9092");
        assert_eq!(kafka_source.topic(), "fallback_topic"); // fallback_topic used because no source.topic
        assert_eq!(kafka_source.group_id(), &Some("special_group".to_string()));

        // Sink should use sink-prefixed properties
        assert_eq!(kafka_sink.brokers(), "kafka2:9092");
        assert_eq!(kafka_sink.topic(), "output_topic");

        // Source config should only have source properties (stripped of prefix)
        assert!(kafka_source.config().contains_key("brokers"));
        assert!(kafka_source.config().contains_key("group_id"));
        assert!(!kafka_source.config().contains_key("sink.brokers")); // Sink properties excluded
        // Note: Current implementation includes unprefixed job properties - this could be improved
        assert!(kafka_source.config().contains_key("failure_strategy")); // Job properties currently included
        assert!(kafka_source.config().contains_key("batch_size")); // Job properties currently included

        // Sink config should only have sink properties (stripped of prefix)
        assert!(kafka_sink.config().contains_key("brokers"));
        assert!(kafka_sink.config().contains_key("topic"));
        assert!(!kafka_sink.config().contains_key("source.group_id")); // Source properties excluded
        // Note: Current implementation includes unprefixed job properties - this could be improved
        assert!(kafka_sink.config().contains_key("failure_strategy")); // Job properties currently included
        assert!(kafka_sink.config().contains_key("batch_size")); // Job properties currently included
    }
}
