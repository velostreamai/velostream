//! Tests for file data source configuration

use ferrisstreams::ferris::sql::datasource::file::{FileFormat, FileSourceConfig};

#[cfg(test)]
mod file_config_tests {
    use super::*;

    #[test]
    fn test_file_format_parsing() {
        assert_eq!("csv".parse::<FileFormat>().unwrap(), FileFormat::Csv);
        assert_eq!("CSV".parse::<FileFormat>().unwrap(), FileFormat::Csv);
        assert_eq!(
            "jsonl".parse::<FileFormat>().unwrap(),
            FileFormat::JsonLines
        );
        assert_eq!(
            "jsonlines".parse::<FileFormat>().unwrap(),
            FileFormat::JsonLines
        );
        assert_eq!(
            "json_lines".parse::<FileFormat>().unwrap(),
            FileFormat::JsonLines
        );
        assert_eq!("json".parse::<FileFormat>().unwrap(), FileFormat::Json);
        assert_eq!(
            "csvnoheader".parse::<FileFormat>().unwrap(),
            FileFormat::CsvNoHeader
        );
        assert_eq!(
            "csv_no_header".parse::<FileFormat>().unwrap(),
            FileFormat::CsvNoHeader
        );

        // Test invalid format
        assert!("invalid_format".parse::<FileFormat>().is_err());
    }

    #[test]
    fn test_file_format_display() {
        assert_eq!(FileFormat::Csv.to_string(), "csv");
        assert_eq!(FileFormat::CsvNoHeader.to_string(), "csv_no_header");
        assert_eq!(FileFormat::JsonLines.to_string(), "jsonl");
        assert_eq!(FileFormat::Json.to_string(), "json");
    }

    #[test]
    fn test_default_config() {
        let config = FileSourceConfig::default();

        assert!(config.path.is_empty());
        assert_eq!(config.format, FileFormat::Csv);
        assert!(!config.watch_for_changes);
        assert_eq!(config.polling_interval_ms, Some(1000));
        assert_eq!(config.csv_delimiter, ',');
        assert_eq!(config.csv_quote, '"');
        assert_eq!(config.csv_escape, None);
        assert!(config.csv_has_header);
        assert_eq!(config.skip_lines, 0);
        assert_eq!(config.max_records, None);
        assert_eq!(config.buffer_size, 8192);
        assert!(!config.recursive);
        assert_eq!(config.extension_filter, None);
    }

    #[test]
    fn test_config_builder_pattern() {
        let config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv)
            .with_watching(Some(500))
            .with_csv_options(';', '\'', Some('\\'), false)
            .with_buffer_size(16384)
            .with_recursive(true);

        assert_eq!(config.path, "/data/test.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert!(config.watch_for_changes);
        assert_eq!(config.polling_interval_ms, Some(500));
        assert_eq!(config.csv_delimiter, ';');
        assert_eq!(config.csv_quote, '\'');
        assert_eq!(config.csv_escape, Some('\\'));
        assert!(!config.csv_has_header);
        assert_eq!(config.buffer_size, 16384);
        assert!(config.recursive);
    }

    #[test]
    fn test_config_validation_valid() {
        let config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        assert!(config.validate().is_ok());

        let config = FileSourceConfig::new("/data/test.jsonl".to_string(), FileFormat::JsonLines)
            .with_watching(Some(2000));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_invalid() {
        // Empty path
        let config = FileSourceConfig::default();
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("path cannot be empty"));

        // Zero buffer size
        let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        config.buffer_size = 0;
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("Buffer size must be greater than 0"));

        // Zero polling interval
        let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        config.polling_interval_ms = Some(0);
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("Polling interval must be greater than 0"));

        // Same delimiter and quote
        let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        config.csv_delimiter = '"';
        config.csv_quote = '"';
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("delimiter and quote character cannot be the same"));

        // Escape same as delimiter
        let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        config.csv_escape = Some(',');
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("escape character cannot be the same"));

        // Escape same as quote
        let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        config.csv_escape = Some('"');
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .contains("escape character cannot be the same"));
    }

    #[test]
    fn test_uri_parsing_basic() {
        // Test basic file URI
        let config = FileSourceConfig::from_uri("file:///data/test.csv").unwrap();
        assert_eq!(config.path, "/data/test.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert!(!config.watch_for_changes);

        // Test with explicit format
        let config = FileSourceConfig::from_uri("file:///data/test.txt?format=csv").unwrap();
        assert_eq!(config.path, "/data/test.txt");
        assert_eq!(config.format, FileFormat::Csv);
    }

    #[test]
    fn test_uri_parsing_format_detection() {
        // Test CSV extension detection
        let config = FileSourceConfig::from_uri("file:///data/orders.csv").unwrap();
        assert_eq!(config.format, FileFormat::Csv);

        // Test JSONL extension detection
        let config = FileSourceConfig::from_uri("file:///data/events.jsonl").unwrap();
        assert_eq!(config.format, FileFormat::JsonLines);

        // Test JSON extension detection
        let config = FileSourceConfig::from_uri("file:///data/config.json").unwrap();
        assert_eq!(config.format, FileFormat::Json);

        // Test unknown extension defaults to CSV
        let config = FileSourceConfig::from_uri("file:///data/unknown.xyz").unwrap();
        assert_eq!(config.format, FileFormat::Csv);
    }

    #[test]
    fn test_uri_parsing_with_parameters() {
        let config = FileSourceConfig::from_uri(
            "file:///data/test.csv?format=csv&watch=true&poll_interval=1500&delimiter=;&quote='&header=false"
        ).unwrap();

        assert_eq!(config.path, "/data/test.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert!(config.watch_for_changes);
        assert_eq!(config.polling_interval_ms, Some(1500));
        assert_eq!(config.csv_delimiter, ';');
        assert_eq!(config.csv_quote, '\'');
        assert!(!config.csv_has_header);
    }

    #[test]
    fn test_uri_parsing_with_skip_and_limits() {
        let config = FileSourceConfig::from_uri(
            "file:///data/test.csv?skip_lines=2&max_records=1000&buffer_size=4096&recursive=true",
        )
        .unwrap();

        assert_eq!(config.skip_lines, 2);
        assert_eq!(config.max_records, Some(1000));
        assert_eq!(config.buffer_size, 4096);
        assert!(config.recursive);
    }

    #[test]
    fn test_uri_parsing_jsonl_format() {
        let config = FileSourceConfig::from_uri(
            "file:///logs/app.jsonl?format=jsonlines&watch=true&poll_interval=500",
        )
        .unwrap();

        assert_eq!(config.path, "/logs/app.jsonl");
        assert_eq!(config.format, FileFormat::JsonLines);
        assert!(config.watch_for_changes);
        assert_eq!(config.polling_interval_ms, Some(500));
    }

    #[test]
    fn test_uri_parsing_invalid() {
        // Test invalid scheme
        let result = FileSourceConfig::from_uri("http://example.com/file.csv");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("File URI must start with 'file://'"));

        // Test invalid format parameter
        let result = FileSourceConfig::from_uri("file:///data/test.csv?format=invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid format parameter"));

        // Test malformed URI (should fail validation)
        let result = FileSourceConfig::from_uri("file:///?delimiter=,&quote=,");
        assert!(result.is_err());
    }

    #[test]
    fn test_uri_parsing_ignores_unknown_params() {
        // URI with unknown parameters should be parsed successfully, ignoring unknowns
        let config = FileSourceConfig::from_uri(
            "file:///data/test.csv?format=csv&unknown_param=value&another_unknown=123&delimiter=;",
        )
        .unwrap();

        assert_eq!(config.path, "/data/test.csv");
        assert_eq!(config.format, FileFormat::Csv);
        assert_eq!(config.csv_delimiter, ';');
        // Unknown parameters should be ignored without error
    }

    #[test]
    fn test_uri_parsing_boolean_parameters() {
        // Test various boolean representations
        let configs = vec![
            ("file:///test.csv?watch=true", true),
            ("file:///test.csv?watch=false", false),
            ("file:///test.csv?watch=1", false), // Invalid boolean, should default to false
            ("file:///test.csv?watch=yes", false), // Invalid boolean, should default to false
            ("file:///test.csv", false),         // No parameter, should default to false
        ];

        for (uri, expected_watch) in configs {
            let config = FileSourceConfig::from_uri(uri).unwrap();
            assert_eq!(
                config.watch_for_changes, expected_watch,
                "Failed for URI: {}",
                uri
            );
        }
    }

    #[test]
    fn test_uri_parsing_numeric_parameters() {
        // Test valid numeric parameters
        let config = FileSourceConfig::from_uri(
            "file:///data/test.csv?poll_interval=2500&skip_lines=5&max_records=50&buffer_size=16384"
        ).unwrap();

        assert_eq!(config.polling_interval_ms, Some(2500));
        assert_eq!(config.skip_lines, 5);
        assert_eq!(config.max_records, Some(50));
        assert_eq!(config.buffer_size, 16384);

        // Test invalid numeric parameters (should use defaults or be ignored)
        let config = FileSourceConfig::from_uri(
            "file:///data/test.csv?poll_interval=invalid&skip_lines=abc&buffer_size=xyz",
        )
        .unwrap();

        // Invalid numbers should either use default or be ignored (None for poll_interval)
        assert!(config.polling_interval_ms.is_none() || config.polling_interval_ms == Some(1000));
        assert_eq!(config.skip_lines, 0); // Should use default
        assert_eq!(config.buffer_size, 8192); // Should use default
    }

    #[test]
    fn test_uri_parsing_single_character_params() {
        // Test single character parameters for CSV options
        let config =
            FileSourceConfig::from_uri("file:///data/test.csv?delimiter=|&quote='").unwrap();

        assert_eq!(config.csv_delimiter, '|');
        assert_eq!(config.csv_quote, '\''); // Single quote from URL parameter

        // Test multi-character parameters should be ignored for single-char fields
        let config =
            FileSourceConfig::from_uri("file:///data/test.csv?delimiter=multi&quote=chars")
                .unwrap();

        // Multi-character values should be ignored, keeping defaults
        assert_eq!(config.csv_delimiter, ','); // Default
        assert_eq!(config.csv_quote, '"'); // Default
    }

    #[test]
    fn test_source_config_conversion() {
        use ferrisstreams::ferris::sql::datasource::config::SourceConfig;

        let file_config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
        let source_config: SourceConfig = file_config.into();

        match source_config {
            SourceConfig::File(config) => {
                assert_eq!(config.path, "/data/test.csv");
                assert_eq!(config.format, FileFormat::Csv);
            }
            _ => panic!("Expected File source config"),
        }
    }
}
