use velostream::velostream::sql::config::connection_string::ParseError;
use velostream::velostream::sql::config::*;

#[test]
fn test_kafka_uri_parsing() {
    let uri = "kafka://localhost:9092/orders?group_id=analytics&auto_commit=false";
    let conn = ConnectionString::parse(uri).unwrap();
    assert_eq!(conn.scheme, "kafka");
    assert_eq!(conn.hosts.len(), 1);
    assert_eq!(conn.hosts[0].host, "localhost");
    assert_eq!(conn.hosts[0].port, Some(9092));
    assert_eq!(conn.path, Some("/orders".to_string()));
    assert_eq!(conn.query.get("group_id"), Some(&"analytics".to_string()));
    assert_eq!(conn.query.get("auto_commit"), Some(&"false".to_string()));
}

#[test]
fn test_multi_host_kafka() {
    let uri = "kafka://broker1:9092,broker2:9093,broker3:9094/topic";
    let conn = ConnectionString::parse(uri).unwrap();
    assert_eq!(conn.scheme, "kafka");
    assert_eq!(conn.hosts.len(), 3);
    assert_eq!(conn.hosts[0].host, "broker1");
    assert_eq!(conn.hosts[0].port, Some(9092));
    assert_eq!(conn.hosts[1].host, "broker2");
    assert_eq!(conn.hosts[1].port, Some(9093));
    assert_eq!(conn.hosts[2].host, "broker3");
    assert_eq!(conn.hosts[2].port, Some(9094));
}

#[test]
fn test_s3_uri_parsing() {
    let uri = "s3://my-bucket/data/*.parquet?region=us-west-2&access_key=test";
    let conn = ConnectionString::parse(uri).unwrap();
    assert_eq!(conn.scheme, "s3");
    assert_eq!(conn.hosts[0].host, "my-bucket");
    assert_eq!(conn.path, Some("/data/*.parquet".to_string()));
    assert_eq!(conn.query.get("region"), Some(&"us-west-2".to_string()));
    assert_eq!(conn.query.get("access_key"), Some(&"test".to_string()));
}

#[test]
fn test_file_uri_parsing() {
    let uri = "file:///home/data/orders.json?watch=true";
    let conn = ConnectionString::parse(uri).unwrap();
    assert_eq!(conn.scheme, "file");
    assert_eq!(conn.hosts.len(), 0);
    assert_eq!(conn.path, Some("/home/data/orders.json".to_string()));
    assert_eq!(conn.query.get("watch"), Some(&"true".to_string()));
}

#[test]
fn test_postgresql_with_credentials() {
    let uri = "postgresql://user:password@localhost:5432/database?sslmode=require";
    let conn = ConnectionString::parse(uri).unwrap();
    assert_eq!(conn.scheme, "postgresql");
    assert_eq!(conn.userinfo, Some("user:password".to_string()));
    assert_eq!(conn.hosts[0].host, "localhost");
    assert_eq!(conn.hosts[0].port, Some(5432));
    assert_eq!(conn.path, Some("/database".to_string()));
    let components = conn.components();
    assert_eq!(components.username, Some("user".to_string()));
    assert_eq!(components.password, Some("password".to_string()));
}

#[test]
fn test_uri_reconstruction() {
    let original = "kafka://broker1:9092,broker2:9093/topic?group_id=test&auto_commit=false";
    let conn = ConnectionString::parse(original).unwrap();
    let reconstructed = conn.to_uri();
    // Parse both to compare (order of query params might differ)
    let conn2 = ConnectionString::parse(&reconstructed).unwrap();
    assert_eq!(conn.scheme, conn2.scheme);
    assert_eq!(conn.hosts, conn2.hosts);
    assert_eq!(conn.path, conn2.path);
    assert_eq!(conn.query, conn2.query);
}

#[test]
fn test_validation() {
    // Valid Kafka URI
    let kafka_uri = "kafka://localhost:9092/topic";
    let conn = ConnectionString::parse(kafka_uri).unwrap();
    assert!(conn.validate().is_ok());

    // Invalid Kafka URI (no host) - should fail parsing
    let invalid_kafka = "kafka:///topic";
    assert!(ConnectionString::parse(invalid_kafka).is_err());

    // Valid S3 URI
    let s3_uri = "s3://bucket/prefix";
    let conn = ConnectionString::parse(s3_uri).unwrap();
    assert!(conn.validate().is_ok());

    // Valid File URI
    let file_uri = "file:///path/to/file.json";
    let conn = ConnectionString::parse(file_uri).unwrap();
    assert!(conn.validate().is_ok());
}

#[test]
fn test_error_cases() {
    // Empty URI
    assert!(matches!(
        ConnectionString::parse(""),
        Err(ParseError::EmptyUri)
    ));

    // Missing scheme
    assert!(matches!(
        ConnectionString::parse("localhost:9092/topic"),
        Err(ParseError::MissingScheme)
    ));

    // Invalid port
    assert!(matches!(
        ConnectionString::parse("kafka://localhost:abc/topic"),
        Err(ParseError::InvalidPort(_))
    ));
}

#[test]
fn test_config_conversion() {
    let uri = "kafka://localhost:9092/orders?group_id=analytics";
    let conn = ConnectionString::parse(uri).unwrap();
    let config: DataSourceConfig = conn.into();

    assert_eq!(config.scheme, "kafka");
    assert_eq!(config.host, Some("localhost".to_string()));
    assert_eq!(config.port, Some(9092));
    assert_eq!(config.path, Some("/orders".to_string()));
    assert_eq!(
        config.get_parameter("group_id"),
        Some(&"analytics".to_string())
    );
}
