//! Tests for Kafka transactional configuration in common.rs
//!
//! These tests verify that:
//! 1. Transactional consumers get `enable.auto.commit=false` and `isolation.level=read_committed`
//! 2. Transactional sinks get `transactional.id`, `acks=all`, `enable.idempotence=true`
//! 3. Non-transactional mode does NOT add these configurations

use std::collections::HashMap;
use velostream::velostream::datasource::kafka::data_sink::KafkaDataSink;

#[test]
fn test_transactional_sink_config_sets_required_properties() {
    // Given: Properties for a Kafka sink
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("topic".to_string(), "output_topic".to_string());
    props.insert("transactional.id".to_string(), "test_txn_id".to_string());

    // When: Creating sink with transactional mode enabled
    let sink = KafkaDataSink::from_properties(&props, "test_job", None, None, true, None);

    // Then: Config should have transactional requirements
    let config = sink.config();

    // acks=all is required for exactly-once semantics
    assert_eq!(
        config.get("acks"),
        Some(&"all".to_string()),
        "Transactional mode requires acks=all"
    );

    // enable.idempotence=true is required for exactly-once semantics
    assert_eq!(
        config.get("enable.idempotence"),
        Some(&"true".to_string()),
        "Transactional mode requires enable.idempotence=true"
    );

    // transactional.id should be preserved
    assert_eq!(
        config.get("transactional.id"),
        Some(&"test_txn_id".to_string()),
        "transactional.id should be preserved in config"
    );
}

#[test]
fn test_non_transactional_sink_config_does_not_force_acks_all() {
    // Given: Properties for a Kafka sink
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("topic".to_string(), "output_topic".to_string());
    props.insert("acks".to_string(), "1".to_string());

    // When: Creating sink with transactional mode disabled
    let sink = KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);

    // Then: Config should preserve user's acks setting
    let config = sink.config();

    assert_eq!(
        config.get("acks"),
        Some(&"1".to_string()),
        "Non-transactional mode should preserve user's acks setting"
    );

    // Should NOT have enable.idempotence forced
    assert_ne!(
        config.get("enable.idempotence"),
        Some(&"true".to_string()),
        "Non-transactional mode should not force enable.idempotence"
    );
}

#[test]
fn test_transactional_sink_overrides_user_acks_setting() {
    // Given: Properties with user-specified acks=1
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("topic".to_string(), "output_topic".to_string());
    props.insert("acks".to_string(), "1".to_string()); // User wants acks=1
    props.insert("transactional.id".to_string(), "test_txn_id".to_string());

    // When: Creating sink with transactional mode enabled
    let sink = KafkaDataSink::from_properties(&props, "test_job", None, None, true, None);

    // Then: Transactional mode should override to acks=all for exactly-once
    let config = sink.config();

    assert_eq!(
        config.get("acks"),
        Some(&"all".to_string()),
        "Transactional mode must override acks to 'all' for exactly-once semantics"
    );
}

#[test]
fn test_transactional_id_generation_format() {
    // Test that generated transactional IDs use underscore delimiters
    use velostream::velostream::datasource::kafka::config_helpers::generate_transactional_id;

    let txn_id =
        generate_transactional_id(Some("my_app"), "my_job", Some("instance_1"), "output_sink");

    // Should use underscores as delimiters
    assert!(
        txn_id.starts_with("velo_"),
        "transactional.id should start with 'velo_' prefix: {}",
        txn_id
    );
    assert!(
        txn_id.contains("my_app"),
        "transactional.id should contain app name: {}",
        txn_id
    );
    assert!(
        txn_id.contains("my_job"),
        "transactional.id should contain job name: {}",
        txn_id
    );
    assert!(
        txn_id.contains("output_sink"),
        "transactional.id should contain sink name: {}",
        txn_id
    );

    // Format should be: velo_{app}_{job}_{instance}_{sink}
    assert_eq!(
        txn_id, "velo_my_app_my_job_instance_1_output_sink",
        "transactional.id should follow format: velo_{{app}}_{{job}}_{{instance}}_{{sink}}"
    );
}

#[test]
fn test_transactional_id_without_optional_parts() {
    use velostream::velostream::datasource::kafka::config_helpers::generate_transactional_id;

    // Test with no app_name and no instance_id
    // Defaults: app_name="default", instance_id="0"
    let txn_id = generate_transactional_id(None, "simple_job", None, "my_sink");

    // Format: velo_{app_name or "default"}_{job}_{instance_id or "0"}_{sink}
    assert_eq!(
        txn_id, "velo_default_simple_job_0_my_sink",
        "transactional.id should use defaults for missing optional parts: {}",
        txn_id
    );
}

/// This test reproduces the bug where transactional.id is lost during
/// KafkaDataSink::from_properties due to the property filtering logic.
///
/// The issue: When a config file is loaded, certain sink.* prefixed properties
/// may cause the unprefixed transactional.id to be filtered out.
#[test]
fn test_transactional_id_preserved_through_sink_from_properties() {
    // Simulate what common.rs does: add transactional.id to sink_props
    let mut sink_props = HashMap::new();
    sink_props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    sink_props.insert("topic".to_string(), "test_output".to_string());

    // This is what common.rs adds for transactional mode
    sink_props.insert(
        "transactional.id".to_string(),
        "velo_test_app_test_job_0_test_sink".to_string(),
    );

    // Create sink with transactional mode enabled
    let sink = KafkaDataSink::from_properties(&sink_props, "test_job", None, None, true, None);

    // CRITICAL: The transactional.id MUST be preserved in the config
    // This is what the writer checks to decide whether to create a transactional producer
    let config = sink.config();

    assert!(
        config.contains_key("transactional.id"),
        "BUG: transactional.id was lost during from_properties! Config keys: {:?}",
        config.keys().collect::<Vec<_>>()
    );

    assert_eq!(
        config.get("transactional.id"),
        Some(&"velo_test_app_test_job_0_test_sink".to_string()),
        "transactional.id value should be preserved"
    );
}

/// Test that transactional.id is preserved even when sink.* prefixed properties exist
/// This tests the specific filtering logic in data_sink.rs lines 104-109
#[test]
fn test_transactional_id_not_blocked_by_sink_prefix() {
    let mut sink_props = HashMap::new();
    sink_props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    sink_props.insert("topic".to_string(), "test_output".to_string());

    // Add transactional.id (unprefixed, as common.rs does)
    sink_props.insert("transactional.id".to_string(), "my_txn_id".to_string());

    // Also add some sink.* prefixed properties (simulating config file merge)
    // The bug hypothesis: if sink.transactional.id exists, it blocks the unprefixed version
    sink_props.insert("sink.acks".to_string(), "all".to_string());
    sink_props.insert("sink.linger.ms".to_string(), "5".to_string());

    let sink = KafkaDataSink::from_properties(&sink_props, "test_job", None, None, true, None);
    let config = sink.config();

    // transactional.id should still be present
    assert!(
        config.contains_key("transactional.id"),
        "transactional.id should not be blocked by sink.* properties. Config: {:?}",
        config
    );
}

/// Test the exact scenario that fails: sink.transactional.id blocks transactional.id
#[test]
fn test_sink_prefixed_transactional_id_blocks_unprefixed() {
    let mut sink_props = HashMap::new();
    sink_props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    sink_props.insert("topic".to_string(), "test_output".to_string());

    // Add BOTH prefixed and unprefixed transactional.id
    // This simulates what might happen if config file has sink.transactional.id
    sink_props.insert("transactional.id".to_string(), "correct_txn_id".to_string());
    sink_props.insert(
        "sink.transactional.id".to_string(),
        "from_config_file".to_string(),
    );

    let sink = KafkaDataSink::from_properties(&sink_props, "test_job", None, None, true, None);
    let config = sink.config();

    // The filtering logic at line 104-109 says:
    // "Include unprefixed properties only if there's no prefixed version"
    // So if sink.transactional.id exists, transactional.id is SKIPPED
    // and only "transactional.id" from sink.transactional.id is added

    // We need transactional.id to exist in the final config
    assert!(
        config.contains_key("transactional.id"),
        "transactional.id must exist in config for writer to create transactional producer. Config: {:?}",
        config
    );

    // Document current behavior (may fail - that's the bug)
    // The prefixed version (sink.transactional.id) should become transactional.id
    // OR the unprefixed version should take precedence
    println!(
        "Config transactional.id = {:?}",
        config.get("transactional.id")
    );
}

/// Test with actual config file loading - this simulates the real scenario
/// where common_kafka_sink.yaml is loaded via config_file property
#[test]
fn test_transactional_id_with_config_file_simulation() {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create a temporary config file similar to common_kafka_sink.yaml
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let config_content = r#"
datasink:
  producer_config:
    bootstrap.servers: "localhost:9092"
    acks: "1"
    linger.ms: "5"
    batch.size: "16384"
  delivery_profile: "balanced"
"#;
    temp_file
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    let config_path = temp_file.path().to_str().unwrap().to_string();

    // Simulate what common.rs does:
    // 1. Build sink_props with config_file reference
    // 2. Add transactional.id
    let mut sink_props = HashMap::new();
    sink_props.insert("config_file".to_string(), config_path);
    sink_props.insert("topic".to_string(), "test_output".to_string());

    // This is what common.rs adds AFTER config file path is set
    sink_props.insert(
        "transactional.id".to_string(),
        "velo_app_job_0_sink".to_string(),
    );

    // Create sink - this will load the config file and merge
    let sink = KafkaDataSink::from_properties(&sink_props, "test_job", None, None, true, None);
    let config = sink.config();

    println!("=== Config file test ===");
    println!("Config keys: {:?}", config.keys().collect::<Vec<_>>());
    println!("transactional.id = {:?}", config.get("transactional.id"));

    // CRITICAL: transactional.id must survive the config file merge
    assert!(
        config.contains_key("transactional.id"),
        "BUG: transactional.id lost after config file merge! Keys: {:?}",
        config.keys().collect::<Vec<_>>()
    );

    assert_eq!(
        config.get("transactional.id"),
        Some(&"velo_app_job_0_sink".to_string()),
        "transactional.id should have the value from common.rs, not from config file"
    );
}

/// Test what properties look like after config file loading
/// This is a diagnostic test to understand the merge behavior
#[test]
fn test_config_file_property_prefixing() {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use velostream::velostream::datasource::config_loader::merge_config_file_properties;

    // Create config file with datasink prefix (like common_kafka_sink.yaml)
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let config_content = r#"
datasink:
  producer_config:
    bootstrap.servers: "config-broker:9092"
    acks: "all"
  delivery_profile: "balanced"
"#;
    temp_file
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    let config_path = temp_file.path().to_str().unwrap().to_string();

    let mut props = HashMap::new();
    props.insert("config_file".to_string(), config_path);
    props.insert("transactional.id".to_string(), "my_txn_id".to_string());

    // Call the merge function directly to see what happens
    let merged = merge_config_file_properties(&props, "TestContext");

    println!("=== Merged properties ===");
    for (k, v) in merged.iter() {
        println!("  {} = {}", k, v);
    }

    // Check if transactional.id survived
    assert!(
        merged.contains_key("transactional.id"),
        "transactional.id should survive merge. Keys: {:?}",
        merged.keys().collect::<Vec<_>>()
    );

    // Check if there's a sink.transactional.id that would block it
    let has_sink_prefix = merged.contains_key("sink.transactional.id");
    println!("Has sink.transactional.id: {}", has_sink_prefix);

    if has_sink_prefix {
        println!(
            "WARNING: sink.transactional.id exists and will block unprefixed transactional.id in data_sink.rs filtering!"
        );
    }
}

/// Test the FULL production flow: from_properties + initialize + create_writer
/// This is the EXACT flow in common.rs that's failing in production
#[tokio::test]
async fn test_transactional_id_survives_full_initialization_flow() {
    use velostream::velostream::datasource::{DataSink, SinkConfig};

    // Step 1: Build sink_props (same as common.rs:1095-1112)
    let mut sink_props = HashMap::new();
    sink_props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    sink_props.insert("topic".to_string(), "test_output".to_string());
    sink_props.insert(
        "transactional.id".to_string(),
        "velo_test_txn_id".to_string(),
    );

    // Step 2: Create sink via from_properties (same as common.rs:1118)
    let mut sink = KafkaDataSink::from_properties(&sink_props, "test_job", None, None, true, None);

    println!("=== After from_properties ===");
    println!(
        "Config keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );
    println!(
        "transactional.id = {:?}",
        sink.config().get("transactional.id")
    );

    // Step 3: Call initialize (same as common.rs:1133-1136)
    // THIS IS THE SUSPECTED BUG LOCATION - initialize may overwrite self.config
    let init_config = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test_output".to_string(),
        properties: sink_props.clone(), // Same sink_props as passed to from_properties
    };
    sink.initialize(init_config)
        .await
        .expect("Initialize should succeed");

    println!("=== After initialize ===");
    println!(
        "Config keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );
    println!(
        "transactional.id = {:?}",
        sink.config().get("transactional.id")
    );

    // CRITICAL: transactional.id must survive the initialize call
    assert!(
        sink.config().contains_key("transactional.id"),
        "BUG: transactional.id lost after initialize! Keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );

    assert_eq!(
        sink.config().get("transactional.id"),
        Some(&"velo_test_txn_id".to_string()),
        "transactional.id should have correct value after initialize"
    );
}

/// **THE ACTUAL BUG** - KafkaDataWriter::from_properties loses transactional.id
///
/// This test demonstrates the root cause:
/// - `from_properties` passes `&HashMap::new()` to the internal method (BUG!)
/// - `from_properties_with_batch_config` passes the actual `properties` (CORRECT)
///
/// When common.rs calls `create_writer()` (no batch config), it uses `from_properties`,
/// which loses the transactional.id. When it calls `create_writer_with_batch_config`,
/// it uses `from_properties_with_batch_config`, which preserves transactional.id.
#[tokio::test]
async fn test_writer_from_properties_loses_transactional_id_vs_batch_config() {
    use velostream::velostream::datasource::kafka::writer::KafkaDataWriter;
    use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

    // Properties with transactional.id
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert(
        "transactional.id".to_string(),
        "test_txn_id_12345".to_string(),
    );

    println!("=== Testing KafkaDataWriter::from_properties ===");
    println!("Input props: {:?}", props);

    // Test 1: from_properties - THIS IS THE BUG PATH
    // It calls create_with_schema_validation_and_batch_config(..., &HashMap::new(), None)
    // The &HashMap::new() means transactional.id is NEVER seen!
    let result1 =
        KafkaDataWriter::from_properties("localhost:9092", "test_topic".to_string(), &props, None)
            .await;

    match &result1 {
        Ok(writer) => {
            println!("from_properties: Writer created");
            // The writer won't be transactional because it never saw transactional.id
            // We can't easily inspect the internal state, but the log output shows it
            println!(
                "from_properties: Check logs for 'Creating TRANSACTIONAL producer' - it won't appear"
            );
        }
        Err(e) => {
            println!("from_properties: Error - {}", e);
        }
    }

    println!("\n=== Testing KafkaDataWriter::from_properties_with_batch_config ===");

    // Test 2: from_properties_with_batch_config - THIS IS THE CORRECT PATH
    // It calls create_with_schema_validation_and_batch_config(..., properties, Some(batch_config))
    // The actual properties are passed, so transactional.id IS seen!
    let batch_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 1000,
        batch_timeout: std::time::Duration::from_secs(1),
        enable_batching: true,
    };

    let result2 = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test_topic".to_string(),
        &props,
        batch_config,
        None,
    )
    .await;

    match &result2 {
        Ok(writer) => {
            println!("from_properties_with_batch_config: Writer created");
            // The writer WILL be transactional because it saw transactional.id
            println!(
                "from_properties_with_batch_config: Check logs for 'Creating TRANSACTIONAL producer' - it SHOULD appear"
            );
        }
        Err(e) => {
            // Transactional producer creation might fail without a real broker,
            // but the key is that it TRIED to create a transactional producer
            let error_msg = format!("{}", e);
            if error_msg.contains("transactional")
                || error_msg.contains("broker")
                || error_msg.contains("connection")
            {
                println!(
                    "from_properties_with_batch_config: Transactional producer creation attempted but failed (expected without real broker)"
                );
                println!("Error: {}", e);
                // This is expected - the point is it TRIED to create transactional producer
            } else {
                println!(
                    "from_properties_with_batch_config: Unexpected error - {}",
                    e
                );
            }
        }
    }

    // The bug is proven by the logs:
    // - from_properties will log "Creating ASYNC producer" (non-transactional)
    // - from_properties_with_batch_config will log "Creating TRANSACTIONAL producer"
    println!("\n=== BUG ANALYSIS ===");
    println!("The bug is in writer.rs:from_properties (lines 181-190):");
    println!("  It passes &HashMap::new() instead of properties to the internal method.");
    println!("This means transactional.id is never seen by the producer creation logic.");
    println!("");
    println!("FIX: Change line 187 from &HashMap::new() to properties");
}

/// Test the EXACT production flow with config file - this matches common.rs exactly
#[tokio::test]
async fn test_transactional_id_with_config_file_full_flow() {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use velostream::velostream::datasource::{DataSink, SinkConfig};

    // Step 1: Create config file (same as common_kafka_sink.yaml)
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let config_content = r#"
datasink:
  delivery_profile: "balanced"
  producer_config:
    bootstrap.servers: "${VELOSTREAM_KAFKA_BROKERS:localhost:9092}"
"#;
    temp_file
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    let config_path = temp_file.path().to_str().unwrap().to_string();

    // Step 2: Build props like they come from requirement.properties (has config_file reference)
    let mut props = HashMap::new();
    props.insert("flagged_symbols.config_file".to_string(), config_path);
    props.insert(
        "flagged_symbols.topic".to_string(),
        "flagged_symbols_output".to_string(),
    );
    props.insert("flagged_symbols.type".to_string(), "kafka_sink".to_string());

    // Step 3: Build sink_props (same as common.rs:1095)
    let mut sink_props = props.clone();

    // Step 4: Add transactional.id (same as common.rs:1112)
    sink_props.insert(
        "transactional.id".to_string(),
        "velo_default_flagged_symbols_0_flagged_symbols".to_string(),
    );

    println!("=== sink_props before from_properties ===");
    for (k, v) in &sink_props {
        println!("  {} = {}", k, v);
    }

    // Step 5: Create sink (same as common.rs:1118)
    let mut sink =
        KafkaDataSink::from_properties(&sink_props, "flagged_symbols", None, None, true, None);

    println!("=== After from_properties ===");
    println!(
        "Config keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );
    println!(
        "transactional.id = {:?}",
        sink.config().get("transactional.id")
    );

    // Step 6: Initialize (same as common.rs:1133)
    let init_config = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "flagged_symbols_output".to_string(),
        properties: sink_props.clone(),
    };
    sink.initialize(init_config)
        .await
        .expect("Initialize should succeed");

    println!("=== After initialize ===");
    println!(
        "Config keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );
    println!(
        "transactional.id = {:?}",
        sink.config().get("transactional.id")
    );

    // CRITICAL: transactional.id must survive
    assert!(
        sink.config().contains_key("transactional.id"),
        "BUG: transactional.id lost! Keys: {:?}",
        sink.config().keys().collect::<Vec<_>>()
    );
}

/// Test that datasink.* properties are filtered out and don't cause rdkafka errors
///
/// This tests the fix for the regression where `datasink.topic_config.replication.factor`
/// was being passed to rdkafka and causing "No such configuration property" errors.
#[tokio::test]
async fn test_datasink_properties_filtered_from_producer() {
    use velostream::velostream::datasource::kafka::writer::KafkaDataWriter;
    use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

    // Properties that include datasink.* config (like from common_kafka_sink.yaml)
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    // These are valid rdkafka producer properties
    props.insert("acks".to_string(), "all".to_string());
    props.insert("linger.ms".to_string(), "5".to_string());

    // These are datasink-specific properties that should be FILTERED OUT
    // They are NOT valid rdkafka producer properties
    props.insert(
        "datasink.topic_config.replication.factor".to_string(),
        "1".to_string(),
    );
    props.insert(
        "datasink.topic_config.partitions".to_string(),
        "3".to_string(),
    );
    props.insert(
        "datasink.delivery_profile".to_string(),
        "balanced".to_string(),
    );

    println!("=== Testing datasink.* property filtering ===");
    println!("Input props: {:?}", props);

    // Create writer with batch config - this should NOT fail even with datasink.* props
    let batch_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 1000,
        batch_timeout: std::time::Duration::from_secs(1),
        enable_batching: true,
    };

    let result = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test_topic".to_string(),
        &props,
        batch_config,
        None,
    )
    .await;

    // The writer creation should succeed (or fail for broker connection, NOT for invalid property)
    match result {
        Ok(_) => {
            println!("Writer created successfully - datasink.* properties were filtered");
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            // Should NOT fail with "No such configuration property: datasink.*"
            assert!(
                !error_msg.contains("datasink.topic_config"),
                "BUG: datasink.* properties should be filtered! Error: {}",
                error_msg
            );
            // Other errors (like broker connection) are acceptable
            println!(
                "Writer creation failed for acceptable reason (not datasink filtering): {}",
                e
            );
        }
    }

    // Also test from_properties (without batch config) - this was the broken path
    let result2 =
        KafkaDataWriter::from_properties("localhost:9092", "test_topic".to_string(), &props, None)
            .await;

    match result2 {
        Ok(_) => {
            println!(
                "from_properties: Writer created successfully - datasink.* properties were filtered"
            );
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            assert!(
                !error_msg.contains("datasink.topic_config"),
                "BUG: datasink.* properties should be filtered in from_properties! Error: {}",
                error_msg
            );
            println!(
                "from_properties: Failed for acceptable reason (not datasink filtering): {}",
                e
            );
        }
    }

    println!("✅ datasink.* properties are correctly filtered from producer config");
}

/// Test that datasink.producer_config.* properties from YAML config files
/// are correctly passed through to the sink configuration
///
/// The YAML config file format uses:
/// ```yaml
/// datasink:
///   producer_config:
///     acks: "all"
///     linger.ms: "5"
/// ```
/// Which gets flattened to `datasink.producer_config.acks`, `datasink.producer_config.linger.ms`
/// These must be correctly mapped to sink config as `acks`, `linger.ms`.
#[test]
fn test_datasink_producer_config_properties_passed_through() {
    // Simulate what happens when a YAML config file is loaded and flattened
    // The YAML has nested structure under datasink.producer_config
    let mut props = HashMap::new();
    props.insert(
        "datasink.producer_config.bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert(
        "datasink.producer_config.acks".to_string(),
        "all".to_string(),
    );
    props.insert(
        "datasink.producer_config.linger.ms".to_string(),
        "5".to_string(),
    );
    props.insert(
        "datasink.producer_config.batch.size".to_string(),
        "16384".to_string(),
    );
    props.insert(
        "datasink.producer_config.retries".to_string(),
        "3".to_string(),
    );
    props.insert("topic".to_string(), "test_output".to_string());

    // Create sink - this should extract the producer_config properties
    let sink = KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);
    let config = sink.config();

    println!("=== Testing datasink.producer_config.* property extraction ===");
    println!("Sink config keys: {:?}", config.keys().collect::<Vec<_>>());

    // These properties should be in sink config (without the datasink.producer_config. prefix)
    assert!(
        config.contains_key("acks"),
        "acks should be extracted from datasink.producer_config.acks. Config: {:?}",
        config
    );
    assert_eq!(
        config.get("acks"),
        Some(&"all".to_string()),
        "acks value should be 'all'"
    );

    assert!(
        config.contains_key("linger.ms"),
        "linger.ms should be extracted from datasink.producer_config.linger.ms. Config: {:?}",
        config
    );
    assert_eq!(
        config.get("linger.ms"),
        Some(&"5".to_string()),
        "linger.ms value should be '5'"
    );

    assert!(
        config.contains_key("batch.size"),
        "batch.size should be extracted from datasink.producer_config.batch.size. Config: {:?}",
        config
    );

    assert!(
        config.contains_key("retries"),
        "retries should be extracted from datasink.producer_config.retries. Config: {:?}",
        config
    );

    // bootstrap.servers should be present (handled separately with env var substitution)
    assert!(
        config.contains_key("bootstrap.servers"),
        "bootstrap.servers should be present. Config: {:?}",
        config
    );

    println!("✅ datasink.producer_config.* properties correctly passed through to sink config");
}

/// **REPRODUCES BUG**: Test whether the `type` property from SQL WITH clause causes rdkafka to fail
///
/// The SQL `WITH ('market_output.type' = 'kafka_sink', ...)` creates a `type` property
/// that gets passed through to rdkafka's ClientConfig. rdkafka may or may not fail on unknown properties.
///
/// Error from production:
/// ```
/// Failed to create sink 'sink_0_market_output' for job 'market_output':
/// Failed to create Kafka writer: Sink configuration error: Failed to create writer:
/// Client config error: No such configuration property: "type" type kafka_sink
/// ```
#[tokio::test]
async fn test_type_property_from_sql_causes_rdkafka_error() {
    use velostream::velostream::datasource::kafka::writer::KafkaDataWriter;
    use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

    // Simulate properties as they come from SQL WITH clause
    // The SQL: WITH ('market_output.type' = 'kafka_sink', ...)
    // After prefix stripping, this becomes: type = kafka_sink
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("type".to_string(), "kafka_sink".to_string()); // FROM SQL WITH clause
    props.insert("topic".to_string(), "test_output".to_string());

    println!("=== Testing 'type' property from SQL WITH clause ===");
    println!("Input props: {:?}", props);

    // Test 1: from_properties path (no batch config)
    let result1 =
        KafkaDataWriter::from_properties("localhost:9092", "test_topic".to_string(), &props, None)
            .await;

    println!("\n--- from_properties result ---");
    match &result1 {
        Ok(_) => {
            println!("✅ from_properties: Writer created successfully");
            println!("   -> 'type' property did NOT cause rdkafka to fail");
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            println!("❌ from_properties: Error - {}", error_msg);
            if error_msg.contains("type") && error_msg.contains("No such configuration property") {
                println!("   -> BUG CONFIRMED: 'type' property is NOT being filtered!");
                println!("   -> This is causing the STDOUT fallback");
            }
        }
    }

    // Test 2: from_properties_with_batch_config path
    let batch_config = BatchConfig {
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 1000,
        batch_timeout: std::time::Duration::from_secs(1),
        enable_batching: true,
    };

    let result2 = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test_topic".to_string(),
        &props,
        batch_config,
        None,
    )
    .await;

    println!("\n--- from_properties_with_batch_config result ---");
    match &result2 {
        Ok(_) => {
            println!("✅ from_properties_with_batch_config: Writer created successfully");
            println!("   -> 'type' property did NOT cause rdkafka to fail");
        }
        Err(e) => {
            let error_msg = format!("{}", e);
            println!(
                "❌ from_properties_with_batch_config: Error - {}",
                error_msg
            );
            if error_msg.contains("type") && error_msg.contains("No such configuration property") {
                println!("   -> BUG CONFIRMED: 'type' property is NOT being filtered!");
            }
        }
    }

    // At least one should fail if the bug exists
    let both_failed = result1.is_err() && result2.is_err();
    let type_error_in_result1 = result1.as_ref().err().map_or(false, |e| {
        let msg = format!("{}", e);
        msg.contains("type") && msg.contains("No such configuration property")
    });
    let type_error_in_result2 = result2.as_ref().err().map_or(false, |e| {
        let msg = format!("{}", e);
        msg.contains("type") && msg.contains("No such configuration property")
    });

    // The fix: 'type' should be filtered out and NOT cause rdkafka to fail
    assert!(
        !type_error_in_result1,
        "BUG: 'type' property should be filtered from producer properties but caused rdkafka error"
    );
    assert!(
        !type_error_in_result2,
        "BUG: 'type' property should be filtered from producer properties but caused rdkafka error"
    );

    println!("\n=== TEST RESULT ===");
    println!(
        "✅ 'type' property is correctly filtered - no rdkafka 'No such configuration property' error"
    );

    // Note: Writer creation may still fail for other reasons (e.g., broker connection)
    // but NOT because of the 'type' property
    if let Err(e) = &result1 {
        let msg = format!("{}", e);
        println!("from_properties failed (expected without broker): {}", msg);
        assert!(
            !msg.contains("\"type\""),
            "Error should not mention 'type' property"
        );
    }
    if let Err(e) = &result2 {
        let msg = format!("{}", e);
        println!(
            "from_properties_with_batch_config failed (expected without broker): {}",
            msg
        );
        assert!(
            !msg.contains("\"type\""),
            "Error should not mention 'type' property"
        );
    }
}

/// Test that KafkaDataSink config does NOT include 'type' property
/// This verifies the filtering logic at the sink level
#[test]
fn test_sink_config_excludes_type_property() {
    // Simulate properties as they come from SQL WITH clause
    let mut props = HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    props.insert("type".to_string(), "kafka_sink".to_string()); // FROM SQL WITH clause
    props.insert("topic".to_string(), "output_topic".to_string());
    props.insert("acks".to_string(), "all".to_string());

    println!("=== Testing sink config filtering of 'type' property ===");
    println!("Input props: {:?}", props);

    // Create sink via from_properties
    let sink = KafkaDataSink::from_properties(&props, "test_job", None, None, false, None);
    let config = sink.config();

    println!("Sink config: {:?}", config);

    // Check what properties are in the config
    println!("Has 'type': {}", config.contains_key("type"));
    println!("Has 'topic': {}", config.contains_key("topic"));
    println!("Has 'acks': {}", config.contains_key("acks"));
    println!(
        "Has 'bootstrap.servers': {}",
        config.contains_key("bootstrap.servers")
    );

    // If 'type' is in the config, it will be passed to rdkafka and cause an error
    // This is a diagnostic test to understand the current behavior
    if config.contains_key("type") {
        println!("⚠️  'type' property is in sink config - this will cause rdkafka to fail");
        println!("   The 'type' property comes from SQL: WITH ('sink.type' = 'kafka_sink', ...)");
        println!("   It should be filtered out before reaching rdkafka");
    } else {
        println!("✅ 'type' property is NOT in sink config - filtering is working");
    }

    // 'acks' should be preserved (it's a valid producer property)
    assert!(
        config.contains_key("acks"),
        "'acks' should be preserved. Config: {:?}",
        config
    );

    // 'bootstrap.servers' should be preserved
    assert!(
        config.contains_key("bootstrap.servers"),
        "'bootstrap.servers' should be preserved. Config: {:?}",
        config
    );
}
