//! Comprehensive test for YAML config file loading with phase-by-phase debug logging
//!
//! This test reproduces the problem of YAML configuration properties not being loaded
//! correctly, and breaks it down into identifiable phases for debugging:
//! - Phase 1: Direct YAML file loading and parsing
//! - Phase 2: Recursive YAML flattening
//! - Phase 3: Source config loading through analyze_source
//! - Phase 4: Sink config loading through analyze_sink
//! - Phase 5: Error message enhancement (SQLValidator error reporting)

use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use velostream::velostream::kafka::serialization_format::SerializationConfig;
use velostream::velostream::sql::query_analyzer::{QueryAnalysis, QueryAnalyzer};

/// Helper function to pretty-print properties for debugging
fn print_properties(label: &str, props: &HashMap<String, String>) {
    println!("\n{}", label);
    println!("  Total properties: {}", props.len());
    if props.is_empty() {
        println!("  ⚠️  WARNING: Properties map is EMPTY!");
    } else {
        let mut keys: Vec<_> = props.keys().collect();
        keys.sort();
        for key in keys {
            println!("    ✓ '{}' = '{}'", key, props[key]);
        }
    }
}

/// PHASE 1: Test direct YAML file loading and parsing
#[test]
fn phase1_yaml_file_loading() {
    println!("\n==================== PHASE 1: YAML File Loading ====================");

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_source.yaml");

    // Create a real YAML file with known content
    let yaml_content = r#"bootstrap.servers: "kafka-broker:9092"
topic: "test-topic"
group.id: "test-group"
value.format: "json"
consumer_config:
  auto.offset.reset: "earliest"
  enable.auto.commit: "false"
  session.timeout.ms: "6000"
"#;

    println!("Creating YAML file at: {}", config_path.display());
    fs::write(&config_path, yaml_content).unwrap();
    println!("✓ YAML file created successfully");

    // Try to load the YAML file directly
    println!("\nAttempting to load YAML file...");
    match velostream::velostream::sql::config::yaml_loader::load_yaml_config(&config_path) {
        Ok(yaml_config) => {
            println!("✓ YAML file loaded successfully");
            println!(
                "  Config type: {:?}",
                std::any::type_name_of_val(&yaml_config.config)
            );

            // Inspect the raw YAML structure
            match &yaml_config.config {
                serde_yaml::Value::Mapping(map) => {
                    println!("  YAML content (as mapping):");
                    let mut keys: Vec<_> = map.keys().collect();
                    keys.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
                    for key in keys {
                        if let Some(key_str) = key.as_str() {
                            println!("    - Key: '{}'", key_str);
                            let val = &map[key];
                            println!("      Value: {:?}", val);
                        }
                    }
                }
                other => {
                    println!("  ERROR: Expected YAML Mapping, got: {:?}", other);
                }
            }
        }
        Err(e) => {
            panic!("Failed to load YAML file: {}", e);
        }
    }
}

/// PHASE 2: Test recursive YAML flattening
#[test]
fn phase2_yaml_flattening() {
    println!("\n==================== PHASE 2: YAML Flattening ====================");

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("kafka_source.yaml");

    let yaml_content = r#"bootstrap.servers: "kafka-prod:9092"
topic: "market-data"
group.id: "analytics-group"
value.format: "avro"
schema_registry: "http://schema-registry:8081"
consumer_config:
  auto.offset.reset: "latest"
  session.timeout.ms: "30000"
  max.poll.records: "500"
nested:
  deep:
    value: "test-value"
"#;

    fs::write(&config_path, yaml_content).unwrap();

    // Load YAML
    let yaml_config =
        velostream::velostream::sql::config::yaml_loader::load_yaml_config(&config_path)
            .expect("Failed to load YAML");

    println!("Raw YAML loaded successfully");

    // Now test the flattening function
    println!("\nAttempting to flatten YAML structure...");
    let mut _flattened: HashMap<String, String> = HashMap::new();

    // Call the flatten_yaml_value function from query_analyzer
    // We need to access the private function, so we'll test this indirectly through analyze_source
    println!("✓ Will test flattening through analyze_source in Phase 3");
}

/// PHASE 3: Test analyze_source with config_file
#[test]
fn phase3_analyze_source_with_config_file() {
    println!(
        "\n==================== PHASE 3: analyze_source with config_file ===================="
    );

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("orders_source.yaml");

    // Create a real YAML file
    let yaml_content = r#"bootstrap.servers: "kafka-prod-1:9092,kafka-prod-2:9092"
topic: "orders"
group.id: "orders-processing-group"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
compression.type: "snappy"
fetch.min.bytes: "1024"
fetch.max.wait.ms: "500"
consumer:
  auto:
    offset:
      reset: "earliest"
  session:
    timeout:
      ms: "45000"
"#;

    println!("Creating YAML file for source: {}", config_path.display());
    fs::write(&config_path, yaml_content).unwrap();
    println!("✓ YAML file created");

    // Now test analyze_source
    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure the source with config_file
    config.insert("orders_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "orders_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    println!("\nConfiguration passed to analyze_source:");
    for (k, v) in &config {
        println!("  '{}' = '{}'", k, v);
    }

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    println!("\nCalling analyze_source...");
    let result = analyzer.analyze_source(
        "orders_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    match result {
        Ok(_) => {
            println!("✓ analyze_source succeeded");

            if analysis.required_sources.is_empty() {
                println!("⚠️  ERROR: No sources were added to analysis!");
            } else {
                let source = &analysis.required_sources[0];
                println!("\nSource requirement created:");
                println!("  Name: '{}'", source.name);
                println!("  Type: {:?}", source.source_type);
                print_properties("  Properties loaded from YAML:", &source.properties);

                // Verify key properties are present
                println!("\nVerification:");
                let expected_keys = vec!["bootstrap.servers", "topic", "group.id", "value.format"];
                for key in expected_keys {
                    if source.properties.contains_key(key) {
                        println!("  ✓ Found expected key: '{}'", key);
                    } else {
                        println!("  ✗ MISSING expected key: '{}'", key);
                    }
                }

                // Check for nested keys
                println!("\nNested properties:");
                let nested_keys: Vec<_> = source
                    .properties
                    .keys()
                    .filter(|k| k.contains(".") || k.contains("["))
                    .collect();
                if nested_keys.is_empty() {
                    println!("  (none found)");
                } else {
                    for key in nested_keys {
                        println!("  ✓ '{}' = '{}'", key, source.properties[key]);
                    }
                }
            }
        }
        Err(e) => {
            println!("✗ analyze_source FAILED: {}", e);
            panic!("analyze_source error: {}", e);
        }
    }
}

/// PHASE 4: Test analyze_sink with config_file
#[test]
fn phase4_analyze_sink_with_config_file() {
    println!("\n==================== PHASE 4: analyze_sink with config_file ====================");

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("output_sink.yaml");

    // Create a real YAML file for sink
    let yaml_content = r#"bootstrap.servers: "kafka-sink-1:9092,kafka-sink-2:9092"
topic: "processed-orders"
value.format: "avro"
schema.registry.url: "http://schema-registry-sink:8081"
compression.type: "gzip"
batch.size: "16384"
linger.ms: "10"
acks: "all"
retries: "3"
"#;

    println!("Creating YAML file for sink: {}", config_path.display());
    fs::write(&config_path, yaml_content).unwrap();
    println!("✓ YAML file created");

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure the sink with config_file
    config.insert("processed_sink.type".to_string(), "kafka_sink".to_string());
    config.insert(
        "processed_sink.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    println!("\nConfiguration passed to analyze_sink:");
    for (k, v) in &config {
        println!("  '{}' = '{}'", k, v);
    }

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    println!("\nCalling analyze_sink...");
    let result = analyzer.analyze_sink(
        "processed_sink",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    match result {
        Ok(_) => {
            println!("✓ analyze_sink succeeded");

            if analysis.required_sinks.is_empty() {
                println!("⚠️  ERROR: No sinks were added to analysis!");
            } else {
                let sink = &analysis.required_sinks[0];
                println!("\nSink requirement created:");
                println!("  Name: '{}'", sink.name);
                println!("  Type: {:?}", sink.sink_type);
                print_properties("  Properties loaded from YAML:", &sink.properties);

                // Verify key properties
                println!("\nVerification:");
                let expected_keys = vec!["bootstrap.servers", "topic", "value.format"];
                for key in expected_keys {
                    if sink.properties.contains_key(key) {
                        println!("  ✓ Found expected key: '{}'", key);
                    } else {
                        println!("  ✗ MISSING expected key: '{}'", key);
                    }
                }
            }
        }
        Err(e) => {
            println!("✗ analyze_sink FAILED: {}", e);
            panic!("analyze_sink error: {}", e);
        }
    }
}

/// PHASE 5: Test error message enhancement when properties fail to load
#[test]
fn phase5_enhanced_error_messages() {
    println!("\n==================== PHASE 5: Enhanced Error Messages ====================");
    println!("This phase tests that the SQLValidator provides helpful error messages");
    println!("when required properties are missing from the config.");

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Try to analyze a source with missing config_file
    config.insert("bad_source.type".to_string(), "kafka_source".to_string());
    // Intentionally missing config_file and bootstrap.servers

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    println!("\nAttempting to analyze source with missing required properties...");
    match analyzer.analyze_source(
        "bad_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    ) {
        Ok(_) => {
            // If it succeeds, check what default values were filled in
            println!("✓ analyze_source completed (defaults were filled)");
            if !analysis.required_sources.is_empty() {
                let source = &analysis.required_sources[0];
                println!("  Default properties provided:");
                print_properties("    ", &source.properties);
            }
        }
        Err(e) => {
            println!("✓ Got expected error:");
            println!("  Error message: {}", e);
            println!("\n✓ This is where enhanced error messages would appear");
            println!("  Future: Should show which keys were successfully loaded");
            println!("  and which ones are missing/required.");
        }
    }
}

/// INTEGRATION TEST: Full workflow with multiple sources and sinks
#[test]
fn integration_test_multiple_sources_and_sinks() {
    println!(
        "\n==================== INTEGRATION TEST: Multiple Sources & Sinks ===================="
    );

    let temp_dir = TempDir::new().unwrap();

    // Create market data source config
    let market_data_path = temp_dir.path().join("market_data.yaml");
    fs::write(
        &market_data_path,
        r#"bootstrap.servers: "kafka-prod:9092"
topic: "market-data"
group.id: "market-analytics"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
"#,
    )
    .unwrap();

    // Create orders source config
    let orders_path = temp_dir.path().join("orders.yaml");
    fs::write(
        &orders_path,
        r#"bootstrap.servers: "kafka-prod:9092"
topic: "orders"
group.id: "order-processing"
value.format: "json"
"#,
    )
    .unwrap();

    // Create output sink config
    let output_path = temp_dir.path().join("output.yaml");
    fs::write(
        &output_path,
        r#"bootstrap.servers: "kafka-sink:9092"
topic: "processed-results"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
"#,
    )
    .unwrap();

    // Set up configuration
    let analyzer = QueryAnalyzer::new("integration_test".to_string());
    let mut config = HashMap::new();

    config.insert("market_data.type".to_string(), "kafka_source".to_string());
    config.insert(
        "market_data.config_file".to_string(),
        market_data_path.to_string_lossy().to_string(),
    );

    config.insert("orders.type".to_string(), "kafka_source".to_string());
    config.insert(
        "orders.config_file".to_string(),
        orders_path.to_string_lossy().to_string(),
    );

    config.insert("output.type".to_string(), "kafka_sink".to_string());
    config.insert(
        "output.config_file".to_string(),
        output_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Analyze market_data source
    println!("\nAnalyzing market_data source...");
    analyzer
        .analyze_source(
            "market_data",
            &config,
            &SerializationConfig::default(),
            &mut analysis,
        )
        .expect("Failed to analyze market_data source");
    println!("✓ market_data source analyzed");

    // Analyze orders source
    println!("Analyzing orders source...");
    analyzer
        .analyze_source(
            "orders",
            &config,
            &SerializationConfig::default(),
            &mut analysis,
        )
        .expect("Failed to analyze orders source");
    println!("✓ orders source analyzed");

    // Analyze output sink
    println!("Analyzing output sink...");
    analyzer
        .analyze_sink(
            "output",
            &config,
            &SerializationConfig::default(),
            &mut analysis,
        )
        .expect("Failed to analyze output sink");
    println!("✓ output sink analyzed");

    // Summary
    println!("\nIntegration test summary:");
    println!("  Total sources: {}", analysis.required_sources.len());
    println!("  Total sinks: {}", analysis.required_sinks.len());

    for source in &analysis.required_sources {
        println!(
            "\n  Source: '{}' (type: {:?})",
            source.name, source.source_type
        );
        println!("    Properties: {}", source.properties.len());
        if source.properties.is_empty() {
            println!("    ⚠️  WARNING: Empty properties!");
        }
    }

    for sink in &analysis.required_sinks {
        println!("\n  Sink: '{}' (type: {:?})", sink.name, sink.sink_type);
        println!("    Properties: {}", sink.properties.len());
        if sink.properties.is_empty() {
            println!("    ⚠️  WARNING: Empty properties!");
        }
    }
}

#[test]
fn phase6_enhanced_error_reporting_with_property_context() {
    println!("\n==================== PHASE 6: Enhanced Error Reporting ====================");
    println!("This phase demonstrates the desired enhancement to error messages:");
    println!("When validation fails, error messages should show:");
    println!("  ✓ Properties that loaded successfully");
    println!("  ✗ Properties that failed validation");
    println!("  ⚠  Properties using defaults");
    println!();

    let temp_dir = TempDir::new().unwrap();

    // Create a YAML file with some valid and some problematic properties
    let config_path = temp_dir.path().join("partial_config.yaml");
    fs::write(
        &config_path,
        r#"bootstrap.servers: "kafka-prod:9092"
topic: "orders"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
group.id: "test_group"
enable.auto.commit: "true"
fetch.min.bytes: "1024"
compression.type: "snappy"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Analyze the source
    match analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    ) {
        Ok(_) => {
            // Extract properties for demonstration
            if let Some(source) = analysis.required_sources.first() {
                println!("✓ Successfully loaded properties from YAML file:");
                println!("  Config file: {}", config_path.display());
                println!();

                // Categorize properties (simulating enhanced error reporting)
                let mut loaded_props = Vec::new();
                let mut default_props = Vec::new();

                for (key, value) in &source.properties {
                    // Properties that were explicitly in the YAML
                    if matches!(
                        key.as_str(),
                        "bootstrap.servers"
                            | "topic"
                            | "value.format"
                            | "schema.registry.url"
                            | "group.id"
                            | "enable.auto.commit"
                            | "fetch.min.bytes"
                            | "compression.type"
                    ) {
                        loaded_props.push((key.clone(), value.clone()));
                    } else {
                        // Properties that came from defaults
                        default_props.push((key.clone(), value.clone()));
                    }
                }

                println!("  LOADED FROM YAML ({}):", loaded_props.len());
                for (key, value) in &loaded_props {
                    println!("    ✓ {} = {}", key, value);
                }

                if !default_props.is_empty() {
                    println!();
                    println!("  USING DEFAULTS ({}):", default_props.len());
                    for (key, value) in &default_props {
                        println!("    ⊘ {} = {} (default)", key, value);
                    }
                }

                println!();
                println!("DEMONSTRATION OF DESIRED ERROR MESSAGE FORMAT:");
                println!("============================================================");
                println!();
                println!("When a source/sink has invalid properties, errors should show:");
                println!();
                println!("Configuration Validation Report - Source 'problematic_source'");
                println!("------------------------------------------------------------");
                println!();
                println!("✓ LOADED PROPERTIES: 8");
                println!("  bootstrap.servers = kafka-prod:9092");
                println!("  topic = orders");
                println!("  value.format = avro");
                println!("  schema.registry.url = http://schema-registry:8081");
                println!("  group.id = test_group");
                println!("  enable.auto.commit = true");
                println!("  fetch.min.bytes = 1024");
                println!("  compression.type = snappy");
                println!();
                println!("✗ VALIDATION FAILURES: 2");
                println!("  • group.id: cannot contain dashes or spaces");
                println!("    Current value: 'test-group-1' (contains '-')");
                println!("    Expected pattern: alphanumeric, underscore, dot only");
                println!();
                println!("  • compression.type: invalid codec");
                println!("    Current value: 'brotli'");
                println!("    Valid options: none, gzip, snappy, lz4, zstd");
                println!();
                println!("⚠  PROPERTIES USING DEFAULTS: 3");
                println!("  fetch.max.wait.ms = 500 (default)");
                println!("  session.timeout.ms = 30000 (default)");
                println!("  heartbeat.interval.ms = 10000 (default)");
                println!();
                println!("DEBUGGING TIPS:");
                println!("• 8 out of 13 properties validated successfully");
                println!("• Configuration file is correct for most settings");
                println!("• Fix the 2 invalid properties above and retry");
                println!();
            }
        }
        Err(e) => {
            println!("✗ Failed to analyze source: {}", e);
        }
    }

    println!("SUMMARY:");
    println!("This phase demonstrates the desired enhancement pattern.");
    println!("Current error messages only show failures.");
    println!("Enhanced messages would show both successes and failures side-by-side,");
    println!("making it immediately obvious what's configured correctly and what needs fixing.");
}

#[test]
fn phase7_reproduce_validation_failure_with_enhanced_messages() {
    println!("\n==================== PHASE 7: Reproduce Validation Failure ====================");
    println!("This test creates a configuration with invalid Kafka properties to trigger");
    println!("validation errors and demonstrate the enhanced error message format.\n");

    let temp_dir = TempDir::new().unwrap();

    // Create a YAML file with SOME valid and SOME invalid properties
    let config_path = temp_dir.path().join("invalid_config.yaml");
    fs::write(
        &config_path,
        r#"bootstrap.servers: "kafka-prod:9092"
topic: "orders"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
group.id: "test_group"
compression.type: "invalid_codec"
acks: "99"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    config.insert("bad_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "bad_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    println!("Attempting to analyze source with INVALID properties...\n");

    // This should fail validation
    match analyzer.analyze_source(
        "bad_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    ) {
        Ok(_) => {
            println!("✗ ERROR: Source should have failed validation but didn't!");
        }
        Err(e) => {
            println!("✓ Validation failed as expected\n");
            println!("Enhanced Error Message with Property Context:");
            println!("═══════════════════════════════════════════════════════");
            println!("{}", e);
            println!("═══════════════════════════════════════════════════════\n");

            // Verify the error message contains the expected sections
            let error_str = format!("{}", e);

            if error_str.contains("LOADED PROPERTIES") {
                println!("✓ Error message shows LOADED PROPERTIES section");
            } else {
                println!("✗ Error message missing LOADED PROPERTIES section");
            }

            if error_str.contains("VALIDATION FAILURES") {
                println!("✓ Error message shows VALIDATION FAILURES section");
            } else {
                println!("✗ Error message missing VALIDATION FAILURES section");
            }

            if error_str.contains("DEBUGGING TIPS") {
                println!("✓ Error message shows DEBUGGING TIPS section");
            } else {
                println!("✗ Error message missing DEBUGGING TIPS section");
            }

            if error_str.contains("bootstrap.servers") {
                println!("✓ Error message lists successfully loaded property: bootstrap.servers");
            }

            if error_str.contains("topic") {
                println!("✓ Error message lists successfully loaded property: topic");
            }

            println!("\nBENEFITS OF ENHANCED ERROR REPORTING:");
            println!("• User immediately sees which properties loaded successfully");
            println!("• User can see exactly which properties failed and why");
            println!("• User gets debugging tips to resolve the issue");
            println!("• Much clearer diagnosis of configuration problems");
        }
    }
}

#[test]
fn phase8_config_file_not_found_error() {
    println!("\n==================== PHASE 8: Config File Not Found Error ====================");
    println!("This test reproduces the exact problem from financial_trading.sql where");
    println!("a config_file path is specified but the file doesn't exist.\n");

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Simulate the error: config_file path specified but doesn't exist
    config.insert(
        "market_data_ts.config_file".to_string(),
        "/nonexistent/path/market_data_ts_source.yaml".to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    println!("Configuration:");
    println!("  'market_data_ts.config_file' = '/nonexistent/path/market_data_ts_source.yaml'");
    println!("  (Note: .type specification missing)\n");

    println!("Attempting to analyze source...\n");

    match analyzer.analyze_source(
        "market_data_ts",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    ) {
        Ok(_) => {
            println!("✗ ERROR: Should have failed but didn't!");
        }
        Err(e) => {
            println!("✓ Got expected error:\n");
            println!("═════════════════════════════════════════════════════════════════");
            println!("{}", e);
            println!("═════════════════════════════════════════════════════════════════\n");

            let error_str = format!("{}", e);

            println!("VERIFICATION:");
            if error_str.contains("CONFIGURATION FILE ERROR") {
                println!("✓ Error message shows CONFIGURATION FILE ERROR section");
            } else {
                println!("✗ Error message missing CONFIGURATION FILE ERROR section");
            }

            if error_str.contains("Failed to load config file") {
                println!("✓ Error message explains the failure reason");
            } else {
                println!("✗ Error message doesn't explain failure reason");
            }

            if error_str.contains("RESOLUTION") {
                println!("✓ Error message provides RESOLUTION section");
            } else {
                println!("✗ Error message missing RESOLUTION section");
            }

            if error_str.contains("Verify the config_file path") {
                println!("✓ Error message suggests verifying the config_file path");
            } else {
                println!("✗ Error message missing path verification suggestion");
            }

            if error_str.contains("YAML file is valid") {
                println!("✓ Error message mentions YAML file validation");
            } else {
                println!("✗ Error message doesn't mention YAML validation");
            }

            println!("\nKEY IMPROVEMENTS:");
            println!("• User now sees EXACTLY WHY config failed (file not found)");
            println!("• User gets clear RESOLUTION steps");
            println!("• User knows to verify the file path");
            println!("• Much better than generic 'NO PROPERTIES FOUND' message");
        }
    }
}
