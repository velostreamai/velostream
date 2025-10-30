use std::env;
/// Tests for node identification via SQL annotations with environment variable resolution
use velostream::velostream::sql::app_parser::{DeploymentConfig, SqlApplicationParser};

/// Test simple pattern resolution without environment variables
#[test]
fn test_deployment_config_static_pattern() {
    let pattern = "prod-server-1";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prod-server-1");
}

/// Test single environment variable substitution
#[test]
fn test_deployment_config_single_env_var() {
    unsafe {
        env::set_var("TEST_HOSTNAME", "server-1");
    }
    let pattern = "prod-${TEST_HOSTNAME}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prod-server-1");
    unsafe {
        env::remove_var("TEST_HOSTNAME");
    }
}

/// Test environment variable with prefix and suffix
#[test]
fn test_deployment_config_env_var_with_prefix_suffix() {
    unsafe {
        env::set_var("TEST_NODE", "node-42");
    }
    let pattern = "prod-${TEST_NODE}-us-east-1";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prod-node-42-us-east-1");
    unsafe {
        env::remove_var("TEST_NODE");
    }
}

/// Test default value when environment variable is not set
#[test]
fn test_deployment_config_default_value() {
    unsafe {
        env::remove_var("UNDEFINED_VAR");
    }
    let pattern = "${UNDEFINED_VAR:default-node}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "default-node");
}

/// Test multiple environment variables in single pattern
#[test]
fn test_deployment_config_multiple_env_vars() {
    unsafe {
        env::set_var("TEST_ENV", "prod");
        env::set_var("TEST_REGION", "us-east-1");
    }
    let pattern = "${TEST_ENV}-${TEST_REGION}-server";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prod-us-east-1-server");
    unsafe {
        env::remove_var("TEST_ENV");
        env::remove_var("TEST_REGION");
    }
}

/// Test fallback chain with multiple variables
#[test]
fn test_deployment_config_fallback_chain() {
    // Set only VAR2
    unsafe {
        env::remove_var("TEST_VAR1");
        env::set_var("TEST_VAR2", "value2");
        env::remove_var("TEST_VAR3");
    }

    let pattern = "${TEST_VAR1|TEST_VAR2|TEST_VAR3:default}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "value2");

    unsafe {
        env::remove_var("TEST_VAR2");
    }
}

/// Test fallback chain with default when all vars are missing
#[test]
fn test_deployment_config_fallback_default() {
    unsafe {
        env::remove_var("TEST_VAR1");
        env::remove_var("TEST_VAR2");
        env::remove_var("TEST_VAR3");
    }

    let pattern = "${TEST_VAR1|TEST_VAR2|TEST_VAR3:fallback}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "fallback");
}

/// Test complex pattern with prefix, multiple vars, and defaults
#[test]
fn test_deployment_config_complex_pattern() {
    unsafe {
        env::set_var("TEST_CLUSTER", "cluster1");
        env::remove_var("TEST_REGION");
        env::set_var("TEST_INSTANCE", "i-12345");
    }

    let pattern = "aws-${TEST_CLUSTER}-${TEST_REGION:us-east-1}-${TEST_INSTANCE}-prod";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "aws-cluster1-us-east-1-i-12345-prod");

    unsafe {
        env::remove_var("TEST_CLUSTER");
        env::remove_var("TEST_INSTANCE");
    }
}

/// Test SQL application with @deployment.node_id annotation
#[test]
fn test_app_parser_deployment_node_id() {
    let sql = r#"
-- SQL Application: Test App
-- Version: 1.0.0
-- @deployment.node_id: prod-server-1
-- @deployment.node_name: Production Server
-- @deployment.region: us-east-1

CREATE STREAM test_stream AS
SELECT * FROM kafka_topic;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Failed to parse SQL application");

    assert_eq!(
        app.metadata.deployment_node_id,
        Some("prod-server-1".to_string())
    );
    assert_eq!(
        app.metadata.deployment_node_name,
        Some("Production Server".to_string())
    );
    assert_eq!(
        app.metadata.deployment_region,
        Some("us-east-1".to_string())
    );
}

/// Test SQL application with environment variable substitution in annotations
#[test]
fn test_app_parser_deployment_env_vars() {
    unsafe {
        env::set_var("TEST_APP_NODE", "node-42");
        env::set_var("TEST_APP_REGION", "eu-west-1");
    }

    let sql = r#"
-- SQL Application: Multi-Region App
-- Version: 2.0.0
-- @deployment.node_id: prod-${TEST_APP_NODE}
-- @deployment.region: ${TEST_APP_REGION}

CREATE STREAM data_stream AS
SELECT * FROM kafka_input;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Failed to parse SQL application");

    assert_eq!(
        app.metadata.deployment_node_id,
        Some("prod-node-42".to_string())
    );
    assert_eq!(
        app.metadata.deployment_region,
        Some("eu-west-1".to_string())
    );

    unsafe {
        env::remove_var("TEST_APP_NODE");
        env::remove_var("TEST_APP_REGION");
    }
}

/// Test SQL application with default values in deployment annotations
#[test]
fn test_app_parser_deployment_defaults() {
    unsafe {
        env::remove_var("UNDEFINED_NODE");
    }

    let sql = r#"
-- SQL Application: Safe App
-- Version: 1.5.0
-- @deployment.node_id: ${UNDEFINED_NODE:default-node}
-- @deployment.region: ${UNDEFINED_REGION:us-east-1}

CREATE STREAM safe_stream AS
SELECT * FROM kafka_safe;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Failed to parse SQL application");

    assert_eq!(
        app.metadata.deployment_node_id,
        Some("default-node".to_string())
    );
    assert_eq!(
        app.metadata.deployment_region,
        Some("us-east-1".to_string())
    );
}

/// Test SQL application with complex deployment patterns
#[test]
fn test_app_parser_deployment_complex_pattern() {
    // Clean up any existing vars
    unsafe {
        env::remove_var("TEST_COMPLEX_ENV");
        env::remove_var("TEST_COMPLEX_DC");
        env::remove_var("TEST_COMPLEX_SERVER");

        env::set_var("TEST_COMPLEX_ENV", "prod");
        env::set_var("TEST_COMPLEX_DC", "dc1");
        env::set_var("TEST_COMPLEX_SERVER", "server-5");
    }

    let sql = r#"
-- SQL Application: Complex App
-- Version: 3.0.0
-- @deployment.node_id: ${TEST_COMPLEX_ENV}-${TEST_COMPLEX_DC}-${TEST_COMPLEX_SERVER}
-- @deployment.node_name: Production DataCenter 1 Server 5

CREATE STREAM complex_stream AS
SELECT * FROM kafka_complex;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Failed to parse SQL application");

    assert_eq!(
        app.metadata.deployment_node_id,
        Some("prod-dc1-server-5".to_string())
    );
    assert_eq!(
        app.metadata.deployment_node_name,
        Some("Production DataCenter 1 Server 5".to_string())
    );

    unsafe {
        env::remove_var("TEST_COMPLEX_ENV");
        env::remove_var("TEST_COMPLEX_DC");
        env::remove_var("TEST_COMPLEX_SERVER");
    }
}

/// Test SQL application without deployment annotations (backward compatibility)
#[test]
fn test_app_parser_no_deployment_annotations() {
    let sql = r#"
-- SQL Application: Legacy App
-- Version: 1.0.0

CREATE STREAM legacy_stream AS
SELECT * FROM kafka_legacy;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser
        .parse_application(sql)
        .expect("Failed to parse SQL application");

    assert_eq!(app.metadata.deployment_node_id, None);
    assert_eq!(app.metadata.deployment_node_name, None);
    assert_eq!(app.metadata.deployment_region, None);
}

/// Test pattern resolution with fallback chain and prefixes
#[test]
fn test_deployment_config_fallback_chain_with_prefix() {
    unsafe {
        env::remove_var("TEST_PRIMARY");
        env::remove_var("TEST_SECONDARY");
        env::set_var("TEST_TERTIARY", "fallback-value");
    }

    let pattern = "prefix-${TEST_PRIMARY|TEST_SECONDARY|TEST_TERTIARY:ultimate-default}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prefix-fallback-value");

    unsafe {
        env::remove_var("TEST_TERTIARY");
    }
}

/// Test multi-part deployment pattern with multiple fallbacks
#[test]
fn test_deployment_config_multipart_fallbacks() {
    unsafe {
        env::set_var("TEST_CLUSTER", "prod-cluster");
        env::remove_var("TEST_ZONE");
        env::set_var("TEST_INSTANCE", "i-prod-001");
    }

    let pattern = "${TEST_CLUSTER}-${TEST_ZONE:zone-a}-${TEST_INSTANCE}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    assert_eq!(result, "prod-cluster-zone-a-i-prod-001");

    unsafe {
        env::remove_var("TEST_CLUSTER");
        env::remove_var("TEST_INSTANCE");
    }
}

/// Test that missing environment variables are handled gracefully
#[test]
fn test_deployment_config_missing_var_no_default() {
    unsafe {
        env::remove_var("MISSING_VAR");
    }

    // When a variable is not found and no default is specified, it returns the variable name as-is
    let pattern = "${MISSING_VAR}";
    let result = DeploymentConfig::resolve_pattern(pattern);
    // The pattern resolver returns the variable spec as-is when variable is not found and no default
    assert_eq!(result, "MISSING_VAR");
}

/// Test deployment configuration in streamin config
#[test]
fn test_streaming_config_deployment_helpers() {
    use velostream::velostream::sql::execution::config::StreamingConfig;

    let config = StreamingConfig::default().with_deployment_config(
        Some("prod-node-1".to_string()),
        Some("Production Node 1".to_string()),
        Some("us-east-1".to_string()),
    );

    assert_eq!(config.deployment_node_id, Some("prod-node-1".to_string()));
    assert_eq!(
        config.deployment_node_name,
        Some("Production Node 1".to_string())
    );
    assert_eq!(config.deployment_region, Some("us-east-1".to_string()));
}

/// Test streaming config deployment with None values
#[test]
fn test_streaming_config_deployment_none() {
    use velostream::velostream::sql::execution::config::StreamingConfig;

    let config = StreamingConfig::default().with_deployment_config(None, None, None);

    assert_eq!(config.deployment_node_id, None);
    assert_eq!(config.deployment_node_name, None);
    assert_eq!(config.deployment_region, None);
}
