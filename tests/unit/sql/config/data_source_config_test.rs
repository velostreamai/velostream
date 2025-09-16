use velostream::velostream::sql::config::types::ValidationStats;
use velostream::velostream::sql::config::*;

#[test]
fn test_basic_config_creation() {
    let config = DataSourceConfig::new("kafka");
    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, None);
    assert_eq!(config.port, None);
    assert_eq!(config.path, None);
    assert!(config.parameters.is_empty());
    assert!(!config.validated);
}

#[test]
fn test_parameter_operations() {
    let mut config = DataSourceConfig::new("kafka");

    // Set parameters
    config.set_parameter("group_id", "test");
    config.set_parameter("auto_commit", "false");

    // Get parameters
    assert_eq!(config.get_parameter("group_id"), Some(&"test".to_string()));
    assert_eq!(config.get_parameter_or("missing", "default"), "default");

    // Boolean parameters
    assert_eq!(config.get_bool_parameter("auto_commit"), Some(false));
    config.set_parameter("enabled", "true");
    assert_eq!(config.get_bool_parameter("enabled"), Some(true));

    // Remove parameter
    assert_eq!(
        config.remove_parameter("group_id"),
        Some("test".to_string())
    );
    assert_eq!(config.get_parameter("group_id"), None);
}

#[test]
fn test_uri_generation() {
    let mut config = DataSourceConfig::new("kafka");
    config.host = Some("localhost".to_string());
    config.port = Some(9092);
    config.path = Some("orders".to_string());
    config.set_parameter("group_id", "analytics");

    let uri = config.to_uri();
    assert!(uri.starts_with("kafka://localhost:9092/orders?"));
    assert!(uri.contains("group_id=analytics"));
}

#[test]
fn test_validation_stats() {
    let mut stats = ValidationStats::new();
    assert_eq!(stats.success_rate(), 0.0);

    stats.total_validations = 10;
    stats.successful_validations = 8;
    stats.failed_validations = 2;

    assert_eq!(stats.success_rate(), 80.0);
}
