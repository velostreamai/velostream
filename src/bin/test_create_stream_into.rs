/*!
# CREATE STREAM INTO Syntax Test

This test verifies that the new CREATE STREAM ... INTO syntax works correctly
with multi-config file support and environment variable resolution.
*/

use ferrisstreams::ferris::sql::ast::StreamingQuery;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing CREATE STREAM INTO syntax support");

    // Test 1: Basic CREATE STREAM INTO syntax
    println!("\n📋 Test 1: Basic CREATE STREAM INTO syntax");
    test_basic_create_stream_into().await?;

    // Test 2: Multi-config file support
    println!("\n📋 Test 2: Multi-config file support");
    test_multi_config_support().await?;

    // Test 3: Environment variable resolution
    println!("\n📋 Test 3: Environment variable resolution");
    test_environment_variable_resolution().await?;

    // Test 4: CREATE TABLE INTO syntax
    println!("\n📋 Test 4: CREATE TABLE INTO syntax support");
    test_create_table_into_syntax().await?;

    // Test 5: Backward compatibility
    println!("\n📋 Test 5: Backward compatibility with existing syntax");
    test_backward_compatibility().await?;

    println!("\n✅ All CREATE STREAM/TABLE INTO syntax tests passed!");
    Ok(())
}

async fn test_basic_create_stream_into() -> Result<(), Box<dyn std::error::Error>> {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM orders_to_kafka AS 
        SELECT id, customer_id, amount, status 
        FROM csv_source 
        INTO kafka_sink
        WITH (
            "source_config" = "configs/csv_orders.yaml",
            "sink_config" = "configs/kafka_sink.yaml"
        )
    "#;

    println!("Parsing: {}", sql.trim());

    let query = parser.parse(sql)?;

    match query {
        StreamingQuery::CreateStreamInto {
            name,
            into_clause,
            properties,
            ..
        } => {
            println!("✅ Successfully parsed CREATE STREAM INTO");
            println!("   📝 Job name: {}", name);
            println!("   🎯 Sink: {}", into_clause.sink_name);
            println!("   ⚙️  Source config: {:?}", properties.source_config);
            println!("   ⚙️  Sink config: {:?}", properties.sink_config);

            assert_eq!(name, "orders_to_kafka");
            assert_eq!(into_clause.sink_name, "kafka_sink");
            assert_eq!(
                properties.source_config,
                Some("configs/csv_orders.yaml".to_string())
            );
            assert_eq!(
                properties.sink_config,
                Some("configs/kafka_sink.yaml".to_string())
            );
        }
        other => {
            panic!("Expected CreateStreamInto, got: {:?}", other);
        }
    }

    Ok(())
}

async fn test_multi_config_support() -> Result<(), Box<dyn std::error::Error>> {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM db_replication AS 
        SELECT * FROM postgres_source 
        INTO s3_sink
        WITH (
            "base_source_config" = "configs/base_postgres.yaml",
            "source_config" = "configs/postgres_prod.yaml",
            "base_sink_config" = "configs/base_s3.yaml", 
            "sink_config" = "configs/s3_prod.yaml",
            "monitoring_config" = "configs/monitoring_prod.yaml",
            "security_config" = "configs/security.yaml"
        )
    "#;

    println!("Parsing multi-config query...");

    let query = parser.parse(sql)?;

    match query {
        StreamingQuery::CreateStreamInto { properties, .. } => {
            println!("✅ Successfully parsed multi-config CREATE STREAM INTO");
            println!(
                "   📂 Base source config: {:?}",
                properties.base_source_config
            );
            println!("   📂 Source config: {:?}", properties.source_config);
            println!("   📂 Base sink config: {:?}", properties.base_sink_config);
            println!("   📂 Sink config: {:?}", properties.sink_config);
            println!(
                "   📊 Monitoring config: {:?}",
                properties.monitoring_config
            );
            println!("   🔒 Security config: {:?}", properties.security_config);

            assert_eq!(
                properties.base_source_config,
                Some("configs/base_postgres.yaml".to_string())
            );
            assert_eq!(
                properties.source_config,
                Some("configs/postgres_prod.yaml".to_string())
            );
            assert_eq!(
                properties.base_sink_config,
                Some("configs/base_s3.yaml".to_string())
            );
            assert_eq!(
                properties.sink_config,
                Some("configs/s3_prod.yaml".to_string())
            );
            assert_eq!(
                properties.monitoring_config,
                Some("configs/monitoring_prod.yaml".to_string())
            );
            assert_eq!(
                properties.security_config,
                Some("configs/security.yaml".to_string())
            );
        }
        other => {
            panic!("Expected CreateStreamInto, got: {:?}", other);
        }
    }

    Ok(())
}

async fn test_environment_variable_resolution() -> Result<(), Box<dyn std::error::Error>> {
    // Set up test environment variables
    env::set_var("ENVIRONMENT", "test");
    env::set_var("CONFIG_PATH", "/opt/configs");
    env::set_var("SOURCE_TYPE", "postgres");

    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM env_test AS 
        SELECT * FROM source 
        INTO sink
        WITH (
            "source_config" = "${CONFIG_PATH}/${SOURCE_TYPE}_${ENVIRONMENT}.yaml",
            "sink_config" = "${CONFIG_PATH}/kafka_${ENVIRONMENT:-dev}.yaml",
            "batch_size" = "1000"
        )
    "#;

    println!("Parsing query with environment variables...");
    println!("   ENVIRONMENT = {}", env::var("ENVIRONMENT").unwrap());
    println!("   CONFIG_PATH = {}", env::var("CONFIG_PATH").unwrap());
    println!("   SOURCE_TYPE = {}", env::var("SOURCE_TYPE").unwrap());

    let query = parser.parse(sql)?;

    match query {
        StreamingQuery::CreateStreamInto { properties, .. } => {
            println!("✅ Successfully resolved environment variables");
            println!(
                "   📂 Resolved source config: {:?}",
                properties.source_config
            );
            println!("   📂 Resolved sink config: {:?}", properties.sink_config);
            println!(
                "   ⚙️  Inline properties: {:?}",
                properties.inline_properties
            );

            assert_eq!(
                properties.source_config,
                Some("/opt/configs/postgres_test.yaml".to_string())
            );
            assert_eq!(
                properties.sink_config,
                Some("/opt/configs/kafka_test.yaml".to_string())
            );
            assert_eq!(
                properties.inline_properties.get("batch_size"),
                Some(&"1000".to_string())
            );
        }
        other => {
            panic!("Expected CreateStreamInto, got: {:?}", other);
        }
    }

    // Clean up environment variables
    env::remove_var("ENVIRONMENT");
    env::remove_var("CONFIG_PATH");
    env::remove_var("SOURCE_TYPE");

    Ok(())
}

async fn test_create_table_into_syntax() -> Result<(), Box<dyn std::error::Error>> {
    // Set up environment variable for testing
    env::set_var("ENVIRONMENT", "production");

    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE TABLE user_analytics AS 
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_spent,
            AVG(amount) as avg_order_value
        FROM orders_stream 
        GROUP BY customer_id
        INTO analytics_sink
        WITH (
            "base_source_config" = "configs/base_kafka_source.yaml",
            "source_config" = "configs/kafka_orders_${ENVIRONMENT}.yaml",
            "base_sink_config" = "configs/base_postgres_sink.yaml", 
            "sink_config" = "configs/postgres_${ENVIRONMENT}.yaml",
            "batch_size" = "500"
        )
    "#;

    println!("Parsing CREATE TABLE INTO query...");

    let query = parser.parse(sql)?;

    match query {
        StreamingQuery::CreateTableInto {
            name,
            into_clause,
            properties,
            ..
        } => {
            println!("✅ Successfully parsed CREATE TABLE INTO");
            println!("   📊 Table name: {}", name);
            println!("   🎯 Sink: {}", into_clause.sink_name);
            println!(
                "   📂 Base source config: {:?}",
                properties.base_source_config
            );
            println!("   📂 Source config: {:?}", properties.source_config);
            println!("   📂 Base sink config: {:?}", properties.base_sink_config);
            println!("   📂 Sink config: {:?}", properties.sink_config);
            println!(
                "   ⚙️  Inline properties: {:?}",
                properties.inline_properties
            );

            assert_eq!(name, "user_analytics");
            assert_eq!(into_clause.sink_name, "analytics_sink");
            assert_eq!(
                properties.base_source_config,
                Some("configs/base_kafka_source.yaml".to_string())
            );
            assert_eq!(
                properties.base_sink_config,
                Some("configs/base_postgres_sink.yaml".to_string())
            );
            assert_eq!(
                properties.inline_properties.get("batch_size"),
                Some(&"500".to_string())
            );
        }
        other => {
            panic!("Expected CreateTableInto, got: {:?}", other);
        }
    }

    // Test backward compatibility - regular CREATE TABLE should still work
    let legacy_sql = r#"
        CREATE TABLE legacy_table AS 
        SELECT customer_id, SUM(amount) as total 
        FROM orders 
        GROUP BY customer_id
        WITH (
            "compaction" = "true",
            "retention_ms" = "86400000"
        )
    "#;

    println!("Testing backward compatibility for CREATE TABLE...");
    let legacy_query = parser.parse(legacy_sql)?;

    match legacy_query {
        StreamingQuery::CreateTable {
            name, properties, ..
        } => {
            println!("✅ Legacy CREATE TABLE syntax still works");
            println!("   📊 Table name: {}", name);
            println!("   ⚙️  Properties: {:?}", properties);

            assert_eq!(name, "legacy_table");
            assert_eq!(properties.get("compaction"), Some(&"true".to_string()));
            assert_eq!(
                properties.get("retention_ms"),
                Some(&"86400000".to_string())
            );
        }
        other => {
            panic!("Expected CreateTable, got: {:?}", other);
        }
    }

    // Clean up environment variable
    env::remove_var("ENVIRONMENT");

    Ok(())
}

async fn test_backward_compatibility() -> Result<(), Box<dyn std::error::Error>> {
    let parser = StreamingSqlParser::new();

    // Test that existing CREATE STREAM syntax still works
    let sql = r#"
        CREATE STREAM legacy_stream AS 
        SELECT id, name FROM orders 
        WITH (
            "topic" = "processed_orders",
            "replication_factor" = "3"
        )
    "#;

    println!("Parsing legacy CREATE STREAM syntax...");

    let query = parser.parse(sql)?;

    match query {
        StreamingQuery::CreateStream {
            name, properties, ..
        } => {
            println!("✅ Legacy CREATE STREAM syntax still works");
            println!("   📝 Stream name: {}", name);
            println!("   ⚙️  Properties: {:?}", properties);

            assert_eq!(name, "legacy_stream");
            assert_eq!(
                properties.get("topic"),
                Some(&"processed_orders".to_string())
            );
            assert_eq!(properties.get("replication_factor"), Some(&"3".to_string()));
        }
        other => {
            panic!("Expected CreateStream, got: {:?}", other);
        }
    }

    Ok(())
}
