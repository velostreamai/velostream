use ferrisstreams::ferris::sql::config::*;
use ferrisstreams::ferris::sql::config::validation::*;

#[test]
fn test_kafka_validation() {
    let mut config = DataSourceConfig::new("kafka");
    config.host = Some("localhost".to_string());
    config.port = Some(9092);
    config.path = Some("/orders".to_string());

    let validator = KafkaValidator::new();
    let result = validator.validate(&config);
    assert!(result.is_ok());
}

#[test]
fn test_kafka_validation_missing_host() {
    let config = DataSourceConfig::new("kafka");
    let validator = KafkaValidator::new();
    let result = validator.validate(&config);
    assert!(matches!(
        result,
        Err(ValidationError::MissingRequired { .. })
    ));
}

#[test]
fn test_s3_validation() {
    let mut config = DataSourceConfig::new("s3");
    config.host = Some("my-bucket".to_string());
    config.path = Some("/data/*.parquet".to_string());
    config.set_parameter("region", "us-west-2");

    let validator = S3Validator::new();
    let result = validator.validate(&config);
    assert!(result.is_ok());
}

#[test]
fn test_s3_validation_invalid_bucket() {
    let mut config = DataSourceConfig::new("s3");
    config.host = Some("ab".to_string()); // Too short

    let validator = S3Validator::new();
    let result = validator.validate(&config);
    assert!(matches!(result, Err(ValidationError::OutOfRange { .. })));
}

#[test]
fn test_validation_registry() {
    let mut registry = ConfigValidatorRegistry::new();
    let mut config = DataSourceConfig::new("kafka");
    config.host = Some("localhost".to_string());
    config.path = Some("/topic".to_string());

    let result = registry.validate(&config);
    assert!(result.is_ok());

    let stats = registry.stats();
    assert_eq!(stats.total_validations, 1);
    assert_eq!(stats.successful_validations, 1);
}

#[test]
fn test_unknown_scheme_validation() {
    let mut registry = ConfigValidatorRegistry::new();
    let config = DataSourceConfig::new("unknown");

    let result = registry.validate(&config);
    assert!(result.is_ok());

    if let Ok(warnings) = result {
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "UNKNOWN_SCHEME");
    }
}