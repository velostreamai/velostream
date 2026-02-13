//! Tests for file data source configuration
//!
//! Covers: FileFormat parsing/display, FileSourceConfig defaults, builder pattern,
//! validation edge cases, URI parsing, and SourceConfig conversion.

use velostream::velostream::datasource::file::config::{FileFormat, FileSourceConfig};

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

    // Invalid format
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
fn test_config_validation_empty_path() {
    let config = FileSourceConfig::default();
    let err = config.validate().unwrap_err();
    assert!(
        err.to_lowercase().contains("path") && err.to_lowercase().contains("empty"),
        "Expected 'path' and 'empty' in error: {err}"
    );
}

#[test]
fn test_config_validation_zero_buffer_size() {
    let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    config.buffer_size = 0;
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("Buffer size"),
        "Expected buffer size error: {err}"
    );
}

#[test]
fn test_config_validation_zero_polling_interval() {
    let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    config.polling_interval_ms = Some(0);
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("Polling interval"),
        "Expected polling interval error: {err}"
    );
}

#[test]
fn test_config_validation_delimiter_equals_quote() {
    let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    config.csv_delimiter = '"';
    config.csv_quote = '"';
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("delimiter") && err.contains("quote"),
        "Expected delimiter/quote collision error: {err}"
    );
}

#[test]
fn test_config_validation_escape_equals_delimiter() {
    let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    config.csv_escape = Some(',');
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("escape"),
        "Expected escape character error: {err}"
    );
}

#[test]
fn test_config_validation_escape_equals_quote() {
    let mut config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    config.csv_escape = Some('"');
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("escape"),
        "Expected escape character error: {err}"
    );
}

#[test]
fn test_uri_parsing_basic() {
    let config = FileSourceConfig::from_uri("file:///data/test.csv").unwrap();
    assert_eq!(config.path, "/data/test.csv");
    assert_eq!(config.format, FileFormat::Csv);
    assert!(!config.watch_for_changes);
}

#[test]
fn test_uri_parsing_format_detection() {
    let config = FileSourceConfig::from_uri("file:///data/orders.csv").unwrap();
    assert_eq!(config.format, FileFormat::Csv);

    let config = FileSourceConfig::from_uri("file:///data/events.jsonl").unwrap();
    assert_eq!(config.format, FileFormat::JsonLines);

    let config = FileSourceConfig::from_uri("file:///data/config.json").unwrap();
    assert_eq!(config.format, FileFormat::Json);

    // Unknown extension defaults to CSV
    let config = FileSourceConfig::from_uri("file:///data/unknown.xyz").unwrap();
    assert_eq!(config.format, FileFormat::Csv);
}

#[test]
fn test_uri_parsing_with_parameters() {
    let config = FileSourceConfig::from_uri(
        "file:///data/test.csv?format=csv&watch=true&poll_interval=1500&delimiter=;&header=false",
    )
    .unwrap();

    assert_eq!(config.path, "/data/test.csv");
    assert_eq!(config.format, FileFormat::Csv);
    assert!(config.watch_for_changes);
    assert_eq!(config.polling_interval_ms, Some(1500));
    assert_eq!(config.csv_delimiter, ';');
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
fn test_uri_parsing_invalid_scheme() {
    let result = FileSourceConfig::from_uri("http://example.com/file.csv");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("file://"));
}

#[test]
fn test_uri_parsing_invalid_format() {
    let result = FileSourceConfig::from_uri("file:///data/test.csv?format=invalid");
    assert!(result.is_err());
}

#[test]
fn test_uri_parsing_ignores_unknown_params() {
    let config = FileSourceConfig::from_uri(
        "file:///data/test.csv?format=csv&unknown_param=value&delimiter=;",
    )
    .unwrap();

    assert_eq!(config.path, "/data/test.csv");
    assert_eq!(config.format, FileFormat::Csv);
    assert_eq!(config.csv_delimiter, ';');
}

#[test]
fn test_uri_parsing_boolean_parameters() {
    let configs = vec![
        ("file:///test.csv?watch=true", true),
        ("file:///test.csv?watch=false", false),
        ("file:///test.csv?watch=1", false), // Invalid boolean, defaults false
        ("file:///test.csv?watch=yes", false), // Invalid boolean, defaults false
        ("file:///test.csv", false),         // No parameter, defaults false
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
    let config = FileSourceConfig::from_uri(
        "file:///data/test.csv?poll_interval=2500&skip_lines=5&max_records=50&buffer_size=16384",
    )
    .unwrap();

    assert_eq!(config.polling_interval_ms, Some(2500));
    assert_eq!(config.skip_lines, 5);
    assert_eq!(config.max_records, Some(50));
    assert_eq!(config.buffer_size, 16384);
}

#[test]
fn test_uri_parsing_invalid_numeric_uses_defaults() {
    let config = FileSourceConfig::from_uri(
        "file:///data/test.csv?poll_interval=invalid&skip_lines=abc&buffer_size=xyz",
    )
    .unwrap();

    // Invalid numbers should use defaults
    assert_eq!(config.skip_lines, 0);
    assert_eq!(config.buffer_size, 8192);
}

#[test]
fn test_uri_parsing_single_character_params() {
    let config = FileSourceConfig::from_uri("file:///data/test.csv?delimiter=|&quote='").unwrap();

    assert_eq!(config.csv_delimiter, '|');
    assert_eq!(config.csv_quote, '\'');

    // Multi-character values should be ignored for single-char fields
    let config =
        FileSourceConfig::from_uri("file:///data/test.csv?delimiter=multi&quote=chars").unwrap();

    assert_eq!(config.csv_delimiter, ','); // Default kept
    assert_eq!(config.csv_quote, '"'); // Default kept
}

#[test]
fn test_source_config_conversion() {
    use velostream::velostream::datasource::config::SourceConfig;

    let file_config = FileSourceConfig::new("/data/test.csv".to_string(), FileFormat::Csv);
    let source_config: SourceConfig = file_config.into();

    match source_config {
        SourceConfig::File { path, .. } => {
            assert_eq!(path, "/data/test.csv");
        }
        _ => panic!("Expected File source config"),
    }
}

#[test]
fn test_source_config_roundtrip() {
    use velostream::velostream::datasource::config::SourceConfig;

    let original = FileSourceConfig::new("/data/orders.csv".to_string(), FileFormat::Csv)
        .with_csv_options(';', '\'', None, false)
        .with_watching(Some(500));

    let source_config: SourceConfig = original.clone().into();
    let restored = FileSourceConfig::from_generic(&source_config).unwrap();

    assert_eq!(restored.path, original.path);
    assert_eq!(restored.csv_delimiter, original.csv_delimiter);
    assert_eq!(restored.csv_quote, original.csv_quote);
    assert!(!restored.csv_has_header);
}
