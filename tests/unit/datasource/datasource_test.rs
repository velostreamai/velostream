use velostream::velostream::sql::datasource::*;

#[test]
fn test_offset_types() {
    let kafka_offset = SourceOffset::Kafka {
        partition: 0,
        offset: 100,
    };
    let file_offset = SourceOffset::File {
        path: "/data/file.json".to_string(),
        byte_offset: 1024,
        line_number: 42,
    };

    assert!(matches!(kafka_offset, SourceOffset::Kafka { .. }));
    assert!(matches!(file_offset, SourceOffset::File { .. }));
}

#[test]
fn test_metadata() {
    let metadata = SourceMetadata {
        source_type: "kafka".to_string(),
        version: "1.0.0".to_string(),
        supports_streaming: true,
        supports_batch: false,
        supports_schema_evolution: true,
        capabilities: vec!["transactions".to_string()],
    };

    assert_eq!(metadata.source_type, "kafka");
    assert!(metadata.supports_streaming);
}
