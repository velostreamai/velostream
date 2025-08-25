use ferrisstreams::ferris::sql::config::*;
use std::env;

#[test]
fn test_basic_environment_config() {
    let env_config = EnvironmentConfig::new();
    assert_eq!(env_config.prefix, "FERRIS_");
    assert!(!env_config.config_files.is_empty());
    assert!(env_config.allow_missing_files);
}

#[test]
fn test_template_expansion() {
    let mut env_config = EnvironmentConfig::new();
    env_config.set_template_var("TEST_VAR", "test_value");

    let expanded = env_config.expand_string("Hello ${TEST_VAR}!").unwrap();
    assert_eq!(expanded, "Hello test_value!");
}

#[test]
fn test_path_expansion() {
    let env_config = EnvironmentConfig::new();
    // Test home directory expansion (if HOME is set)
    if env::var("HOME").is_ok() {
        let expanded = env_config.expand_path("~/test/config.yaml");
        assert!(!expanded.starts_with("~/"));
    }
}

#[test]
fn test_config_value_setting() {
    let env_config = EnvironmentConfig::new();
    let mut config = DataSourceConfig::new("test");

    env_config.set_config_value(&mut config, "host", "localhost");
    env_config.set_config_value(&mut config, "port", "9092");
    env_config.set_config_value(&mut config, "timeout_ms", "5000");

    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
    assert_eq!(config.timeout_ms, Some(5000));
}

#[test]
fn test_global_key_detection() {
    assert!(EnvironmentConfig::is_global_key("timeout_ms"));
    assert!(EnvironmentConfig::is_global_key("max_retries"));
    assert!(!EnvironmentConfig::is_global_key("group_id"));
    assert!(!EnvironmentConfig::is_global_key("topic"));
}

#[test]
fn test_scheme_detection() {
    let env_config = EnvironmentConfig::new();
    let content = r#"
        [kafka]
        host = "localhost"
        
        [s3]
        bucket = "test"
        "#;

    let schemes = env_config.detect_schemes_in_content(content);
    assert!(schemes.contains(&"kafka".to_string()));
    assert!(schemes.contains(&"s3".to_string()));
}