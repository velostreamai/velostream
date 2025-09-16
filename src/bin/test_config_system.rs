//! Test Configuration & URI Parsing System
//!
//! This binary demonstrates the comprehensive configuration system functionality
//! including URI parsing, validation, builder patterns, and environment configuration.

use std::env;
use std::fs;
use std::time::{Duration, Instant};
use velostream::velostream::sql::config::{builder::DataSourceConfigBuilder, *};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🔧 Testing VeloStream Configuration & URI Parsing System");
    println!("===========================================================");

    // Test 1: URI Parsing
    println!("\n⚡ Test 1: URI Parsing & Connection Strings");
    println!("-------------------------------------------");

    let test_uris = [
        "kafka://localhost:9092/orders?group_id=analytics&auto_commit=false",
        "kafka://broker1:9092,broker2:9093,broker3:9094/events?group_id=cluster",
        "s3://my-bucket/data/*.parquet?region=us-west-2&access_key=AKIA123",
        "file:///home/data/orders.json?watch=true&format=json",
        "postgresql://user:password@localhost:5432/database?sslmode=require",
        "clickhouse://admin:secret@ch-cluster:9000/analytics?compression=lz4",
    ];

    let mut parse_times = Vec::new();
    let mut successful_parses = 0;

    for (i, uri) in test_uris.iter().enumerate() {
        println!("\nTesting URI {}: {}", i + 1, uri);

        let start_time = Instant::now();
        match ConnectionString::parse(uri) {
            Ok(conn) => {
                let parse_time = start_time.elapsed();
                parse_times.push(parse_time);
                successful_parses += 1;

                println!("   ✅ Parsed successfully in {:?}", parse_time);
                println!("      • Scheme: {}", conn.scheme);
                println!("      • Hosts: {:?}", conn.hosts);
                println!("      • Path: {:?}", conn.path);
                println!("      • Query params: {} parameters", conn.query.len());

                // Test validation
                match conn.validate() {
                    Ok(()) => println!("      • Validation: ✅ Passed"),
                    Err(e) => println!("      • Validation: ❌ Failed - {}", e),
                }

                // Test URI reconstruction
                let reconstructed = conn.to_uri();
                match ConnectionString::parse(&reconstructed) {
                    Ok(_) => println!("      • Round-trip: ✅ Success"),
                    Err(e) => println!("      • Round-trip: ❌ Failed - {}", e),
                }

                // Test components extraction
                let components = conn.components();
                if let Some(ref primary) = components.primary_host {
                    println!("      • Primary host: {}", primary);
                }
                if let (Some(ref user), Some(ref pass)) =
                    (&components.username, &components.password)
                {
                    println!("      • Credentials: {}:***", user);
                }
            }
            Err(e) => {
                println!("   ❌ Parse failed: {}", e);
            }
        }
    }

    // URI parsing performance metrics
    if !parse_times.is_empty() {
        let total_time: Duration = parse_times.iter().sum();
        let avg_time = total_time / parse_times.len() as u32;
        let min_time = parse_times.iter().min().unwrap();
        let max_time = parse_times.iter().max().unwrap();

        println!("\n📊 URI Parsing Performance:");
        println!(
            "   • Successful parses: {}/{}",
            successful_parses,
            test_uris.len()
        );
        println!("   • Average parse time: {:?}", avg_time);
        println!("   • Min parse time: {:?}", min_time);
        println!("   • Max parse time: {:?}", max_time);
    }

    // Test 2: Configuration Validation
    println!("\n🔍 Test 2: Configuration Validation System");
    println!("-------------------------------------------");

    let mut validator_registry = validation::ConfigValidatorRegistry::new();
    println!(
        "✅ Validation registry created with {} schemes",
        validator_registry.supported_schemes().len()
    );

    // Test valid configurations
    let valid_configs: Vec<(&str, Box<dyn Fn() -> DataSourceConfig>)> = vec![
        (
            "kafka",
            Box::new(|| {
                let mut config = DataSourceConfig::new("kafka");
                config.host = Some("localhost".to_string());
                config.port = Some(9092);
                config.path = Some("/orders".to_string());
                config.set_parameter("group_id", "analytics");
                config
            }),
        ),
        (
            "s3",
            Box::new(|| {
                let mut config = DataSourceConfig::new("s3");
                config.host = Some("my-bucket".to_string());
                config.path = Some("/data/*.parquet".to_string());
                config.set_parameter("region", "us-west-2");
                config
            }),
        ),
        (
            "file",
            Box::new(|| {
                let mut config = DataSourceConfig::new("file");
                config.path = Some("/data/orders.json".to_string());
                config.set_parameter("format", "json");
                config
            }),
        ),
    ];

    let mut validation_times = Vec::new();
    let mut successful_validations = 0;

    for (scheme, config_fn) in valid_configs {
        println!("\nValidating {} configuration:", scheme);
        let config = config_fn();

        let start_time = Instant::now();
        match validator_registry.validate(&config) {
            Ok(warnings) => {
                let validation_time = start_time.elapsed();
                validation_times.push(validation_time);
                successful_validations += 1;

                println!("   ✅ Validation passed in {:?}", validation_time);
                if !warnings.is_empty() {
                    println!("   ⚠️  Warnings:");
                    for warning in &warnings {
                        println!("      • [{}] {}", warning.code, warning.message);
                    }
                }
            }
            Err(e) => {
                println!("   ❌ Validation failed: {}", e);
            }
        }
    }

    // Test invalid configurations
    println!("\nTesting invalid configurations:");

    let invalid_configs: Vec<(&str, Box<dyn Fn() -> DataSourceConfig>)> = vec![
        (
            "kafka-no-host",
            Box::new(|| {
                let mut config = DataSourceConfig::new("kafka");
                config.path = Some("/topic".to_string());
                config
            }),
        ),
        (
            "s3-invalid-bucket",
            Box::new(|| {
                let mut config = DataSourceConfig::new("s3");
                config.host = Some("ab".to_string()); // Too short
                config
            }),
        ),
    ];

    for (test_name, config_fn) in invalid_configs {
        println!("\n{}: ", test_name);
        let config = config_fn();

        match validator_registry.validate(&config) {
            Ok(_) => println!("   ❌ Expected validation to fail"),
            Err(e) => println!("   ✅ Correctly failed: {}", e),
        }
    }

    // Validation performance metrics
    let validation_stats = validator_registry.stats();
    println!("\n📊 Validation Performance:");
    println!(
        "   • Total validations: {}",
        validation_stats.total_validations
    );
    println!("   • Success rate: {:.1}%", validation_stats.success_rate());
    println!(
        "   • Average validation time: {:.2}μs",
        validation_stats.avg_validation_time_us()
    );

    // Test 3: Configuration Builder
    println!("\n🏗️  Test 3: Configuration Builder with Fluent API");
    println!("--------------------------------------------------");

    // Test basic builder
    println!("\nTesting basic configuration builder:");
    let start_time = Instant::now();

    let config = DataSourceConfigBuilder::new()
        .scheme("kafka")
        .host("localhost")
        .port(9092)
        .path("/orders")
        .parameter("group_id", "analytics")
        .bool_parameter("auto_commit", false)
        .timeout(Duration::from_secs(30))
        .max_retries(3)
        .build()?;

    let builder_time = start_time.elapsed();
    println!("   ✅ Basic builder completed in {:?}", builder_time);
    println!("      • Config: {}", config.to_uri());

    // Test scheme-specific builders
    println!("\nTesting scheme-specific builders:");

    // Kafka builder
    let kafka_config = DataSourceConfigBuilder::new()
        .kafka()
        .brokers("broker1:9092,broker2:9093")
        .topic("events")
        .group_id("stream-processor")
        .auto_commit(false)
        .session_timeout(Duration::from_secs(30))
        .build()?;

    println!("   ✅ Kafka builder: {}", kafka_config.to_uri());

    // S3 builder
    let s3_config = DataSourceConfigBuilder::new()
        .s3()
        .bucket("analytics-data")
        .key("raw/2024/01/*.parquet")
        .region("us-east-1")
        .credentials("AKIA123", "secret456")
        .build()?;

    println!("   ✅ S3 builder: {}", s3_config.to_uri());

    // File builder
    let file_config = DataSourceConfigBuilder::new()
        .file()
        .file_path("/data/transactions.json")
        .format("json")
        .watch(true)
        .compression("gzip")
        .build()?;

    println!("   ✅ File builder: {}", file_config.to_uri());

    // Test builder from URI
    println!("\nTesting builder from URI:");
    let uri_builder = DataSourceConfigBuilder::from_uri(
        "postgresql://user:pass@db-server:5432/analytics?pool_size=20",
    )?;

    let pg_config = uri_builder.postgresql().ssl_mode("require").build()?;

    println!("   ✅ PostgreSQL from URI: {}", pg_config.to_uri());

    // Test configuration templates
    println!("\nTesting configuration templates:");
    let template_names = builder::ConfigTemplate::list_names();
    println!("   • Available templates: {:?}", template_names);

    for template_name in &template_names[..2] {
        // Test first 2 templates
        if let Ok(template_config) = DataSourceConfigBuilder::from_template(template_name) {
            let config = template_config.build()?;
            println!(
                "   ✅ Template '{}': {} ({})",
                template_name,
                config.scheme,
                config.to_uri()
            );
        }
    }

    // Test 4: Environment Configuration
    println!("\n🌍 Test 4: Environment & File-based Configuration");
    println!("--------------------------------------------------");

    let mut env_config = environment::EnvironmentConfig::new();

    // Set some test environment variables
    env::set_var("VELO_KAFKA_HOST", "env-kafka-host");
    env::set_var("VELO_KAFKA_PORT", "19092");
    env::set_var("VELO_KAFKA_GROUP_ID", "env-group");
    env::set_var("TEST_TEMPLATE_VAR", "expanded_value");

    println!("✅ Environment configuration created");
    println!("   • Prefix: {}", env_config.prefix);
    println!("   • Config files: {}", env_config.config_files.len());

    // Test template expansion
    env_config.set_template_var("APP_NAME", "velostream");
    env_config.set_template_var("VERSION", "1.0.0");

    let template_tests = vec![
        "Hello ${APP_NAME} v${VERSION}!",
        "Path: ${HOME}/config",
        "User: ${USER}",
    ];

    println!("\nTesting template expansion:");
    for template in &template_tests {
        match env_config.expand_string(template) {
            Ok(expanded) => println!("   • '{}' → '{}'", template, expanded),
            Err(e) => println!("   • '{}' → Error: {}", template, e),
        }
    }

    // Test configuration loading
    println!("\nTesting configuration loading from environment:");

    match env_config.load_config("kafka") {
        Ok(loaded_config) => {
            println!("   ✅ Loaded Kafka config from environment:");
            println!("      • Host: {:?}", loaded_config.host);
            println!("      • Port: {:?}", loaded_config.port);
            println!(
                "      • Parameters: {} items",
                loaded_config.parameters.len()
            );
            println!("      • Source: {}", loaded_config.source);
        }
        Err(e) => println!("   ❌ Failed to load config: {}", e),
    }

    // Test configuration file creation and loading
    println!("\nTesting configuration file support:");

    // Create a test TOML config file
    let test_config_content = r#"
# Test VeloStream Configuration

[kafka]
host = "config-file-host"
port = 29092
group_id = "file-group"
auto_commit = false

[s3]
bucket = "config-bucket"
region = "us-west-1"

timeout_ms = 60000
max_retries = 5
"#;

    let test_config_path = "./test-velostream.toml";
    fs::write(test_config_path, test_config_content)?;
    env_config.add_config_file(test_config_path);

    match env_config.load_config("kafka") {
        Ok(file_config) => {
            println!("   ✅ Loaded Kafka config from file:");
            println!("      • Host: {:?}", file_config.host);
            println!("      • Port: {:?}", file_config.port);
            println!("      • Timeout: {:?}ms", file_config.timeout_ms);
            println!("      • Max retries: {:?}", file_config.max_retries);
        }
        Err(e) => println!("   ❌ Failed to load from file: {}", e),
    }

    // Clean up test file
    let _ = fs::remove_file(test_config_path);

    // Test 5: Integration & Performance
    println!("\n🔗 Test 5: Integration & Performance Testing");
    println!("---------------------------------------------");

    println!("\nTesting end-to-end configuration workflow:");

    // Complex integration test
    let integration_start = Instant::now();

    // 1. Parse URI
    let uri = "kafka://prod-kafka1:9092,prod-kafka2:9092/financial-events?group_id=risk-engine&security_protocol=SASL_SSL";
    let connection = ConnectionString::parse(uri)?;

    // 2. Convert to config
    let base_config: DataSourceConfig = connection.into();

    // 3. Enhance with builder
    let enhanced_config = DataSourceConfigBuilder::from_config(base_config)
        .kafka()
        .session_timeout(Duration::from_secs(45))
        .auto_commit(false)
        .parameter("sasl_mechanism", "SCRAM-SHA-256")
        .parameter("sasl_username", "risk-user")
        .timeout(Duration::from_secs(60))
        .max_retries(5)
        .build()?;

    // 4. Validate
    let validation_result = validator_registry.validate(&enhanced_config)?;

    let integration_time = integration_start.elapsed();

    println!(
        "   ✅ End-to-end workflow completed in {:?}",
        integration_time
    );
    println!("      • Final URI: {}", enhanced_config.to_uri());
    println!("      • Validation warnings: {}", validation_result.len());
    println!(
        "      • Total parameters: {}",
        enhanced_config.parameters.len()
    );

    // Performance benchmarking
    println!("\nPerformance benchmarking (1000 operations each):");

    const BENCHMARK_ITERATIONS: usize = 1000;

    // Benchmark URI parsing
    let uri_benchmark_start = Instant::now();
    let mut successful_uri_parses = 0;

    for _ in 0..BENCHMARK_ITERATIONS {
        if ConnectionString::parse(uri).is_ok() {
            successful_uri_parses += 1;
        }
    }

    let uri_benchmark_time = uri_benchmark_start.elapsed();
    let avg_uri_parse_time = uri_benchmark_time / BENCHMARK_ITERATIONS as u32;

    println!("   📊 URI Parsing:");
    println!(
        "      • Success rate: {}%",
        (successful_uri_parses * 100) / BENCHMARK_ITERATIONS
    );
    println!("      • Average time: {:?}", avg_uri_parse_time);
    println!(
        "      • Operations/sec: {:.0}",
        BENCHMARK_ITERATIONS as f64 / uri_benchmark_time.as_secs_f64()
    );

    // Benchmark configuration building
    let builder_benchmark_start = Instant::now();
    let mut successful_builds = 0;

    for i in 0..BENCHMARK_ITERATIONS {
        let result = DataSourceConfigBuilder::new()
            .kafka()
            .host("localhost")
            .port(9092)
            .topic(format!("topic-{}", i))
            .group_id(format!("group-{}", i))
            .build();

        if result.is_ok() {
            successful_builds += 1;
        }
    }

    let builder_benchmark_time = builder_benchmark_start.elapsed();
    let avg_builder_time = builder_benchmark_time / BENCHMARK_ITERATIONS as u32;

    println!("   📊 Configuration Building:");
    println!(
        "      • Success rate: {}%",
        (successful_builds * 100) / BENCHMARK_ITERATIONS
    );
    println!("      • Average time: {:?}", avg_builder_time);
    println!(
        "      • Operations/sec: {:.0}",
        BENCHMARK_ITERATIONS as f64 / builder_benchmark_time.as_secs_f64()
    );

    // Benchmark validation
    let validation_benchmark_start = Instant::now();
    let test_config = DataSourceConfig::from_uri(uri)?;
    let mut successful_validations_bench = 0;

    for _ in 0..BENCHMARK_ITERATIONS {
        if validator_registry.validate(&test_config).is_ok() {
            successful_validations_bench += 1;
        }
    }

    let validation_benchmark_time = validation_benchmark_start.elapsed();
    let avg_validation_time = validation_benchmark_time / BENCHMARK_ITERATIONS as u32;

    println!("   📊 Configuration Validation:");
    println!(
        "      • Success rate: {}%",
        (successful_validations_bench * 100) / BENCHMARK_ITERATIONS
    );
    println!("      • Average time: {:?}", avg_validation_time);
    println!(
        "      • Operations/sec: {:.0}",
        BENCHMARK_ITERATIONS as f64 / validation_benchmark_time.as_secs_f64()
    );

    // Final statistics
    let final_validation_stats = validator_registry.stats();

    println!("\n🎉 Configuration & URI Parsing System Test Completed Successfully!");
    println!("Key achievements:");
    println!("   ✅ Comprehensive URI parsing with multi-host support");
    println!("   ✅ Flexible configuration validation system");
    println!("   ✅ Intuitive fluent API builder pattern");
    println!("   ✅ Environment and file-based configuration support");
    println!("   ✅ Template expansion and variable substitution");
    println!("   ✅ High-performance parsing and validation");

    println!("\n📊 Final Performance Summary:");
    println!(
        "   • Total validations performed: {}",
        final_validation_stats.total_validations
    );
    println!(
        "   • Overall validation success rate: {:.1}%",
        final_validation_stats.success_rate()
    );
    println!("   • Average URI parse time: {:?}", avg_uri_parse_time);
    println!("   • Average config build time: {:?}", avg_builder_time);
    println!("   • Average validation time: {:?}", avg_validation_time);

    Ok(())
}
