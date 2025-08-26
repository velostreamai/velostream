use ferrisstreams::ferris::sql::config::builder::*;

#[test]
fn test_basic_builder() {
    let config = DataSourceConfigBuilder::new()
        .scheme("kafka")
        .host("localhost")
        .port(9092)
        .path("/orders")
        .parameter("group_id", "test")
        .build()
        .unwrap();

    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
    assert_eq!(config.path, Some("/orders".to_string()));
    assert_eq!(config.get_parameter("group_id"), Some(&"test".to_string()));
}

#[test]
fn test_kafka_builder() {
    let config = DataSourceConfigBuilder::new()
        .kafka()
        .brokers("localhost:9092,localhost:9093")
        .topic("orders")
        .group_id("analytics")
        .auto_commit(false)
        .build()
        .unwrap();

    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
    assert_eq!(config.path, Some("/orders".to_string()));
    assert_eq!(
        config.get_parameter("group_id"),
        Some(&"analytics".to_string())
    );
    assert_eq!(config.get_bool_parameter("auto_commit"), Some(false));
}

#[test]
fn test_s3_builder() {
    let config = DataSourceConfigBuilder::new()
        .s3()
        .bucket("my-data-lake")
        .key("data/2024/01/*.parquet")
        .region("us-west-2")
        .credentials("ACCESS_KEY", "SECRET_KEY")
        .build()
        .unwrap();

    assert_eq!(config.scheme, "s3");
    assert_eq!(config.host, Some("my-data-lake".to_string()));
    assert_eq!(config.path, Some("/data/2024/01/*.parquet".to_string()));
    assert_eq!(
        config.get_parameter("region"),
        Some(&"us-west-2".to_string())
    );
    assert_eq!(
        config.get_parameter("access_key"),
        Some(&"ACCESS_KEY".to_string())
    );
}

#[test]
fn test_file_builder() {
    let config = DataSourceConfigBuilder::new()
        .file()
        .file_path("/data/orders.json")
        .format("json")
        .watch(true)
        .build()
        .unwrap();

    assert_eq!(config.scheme, "file");
    assert_eq!(config.path, Some("/data/orders.json".to_string()));
    assert_eq!(config.get_parameter("format"), Some(&"json".to_string()));
    assert_eq!(config.get_bool_parameter("watch"), Some(true));
}

#[test]
fn test_from_uri() {
    let builder =
        DataSourceConfigBuilder::from_uri("kafka://localhost:9092/orders?group_id=test").unwrap();
    let config = builder.build().unwrap();

    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
    assert_eq!(config.path, Some("/orders".to_string()));
    assert_eq!(config.get_parameter("group_id"), Some(&"test".to_string()));
}

#[test]
fn test_templates() {
    let template_names = ConfigTemplate::list_names();
    assert!(!template_names.is_empty());
    assert!(template_names.contains(&"kafka-local".to_string()));

    let builder = DataSourceConfigBuilder::from_template("kafka-local").unwrap();
    let config = builder.topic("test-topic").build().unwrap();

    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
}

#[test]
fn test_validation_failure() {
    let result = DataSourceConfigBuilder::new()
        .kafka()
        .group_id("test")
        // Missing required host and topic
        .build();

    assert!(result.is_err());
    if let Err(ConfigurationError::ValidationFailed(_)) = result {
        // Expected validation error
    } else {
        panic!("Expected validation error");
    }
}

#[test]
fn test_build_unchecked() {
    let config = DataSourceConfigBuilder::new()
        .kafka()
        .group_id("test")
        // Missing required fields but building without validation
        .build_unchecked();

    assert_eq!(config.scheme, "kafka");
    assert!(!config.is_valid()); // Should not be marked as validated
}
