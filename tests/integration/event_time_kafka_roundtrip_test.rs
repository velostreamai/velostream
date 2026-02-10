//! Event-time Kafka round-trip integration tests with testcontainers
//!
//! Validates that `_event_time` is correctly:
//!   1. Injected as a Kafka header by `KafkaDataWriter::write()`
//!   2. Set as the Kafka message timestamp
//!   3. Preserved through batch writes with per-record event times
//!   4. Reconstructed by `StreamRecord::from_kafka()` on the consumer side
//!   5. Survives multi-hop pipelines (Writer → Kafka → from_kafka → Writer → Kafka → from_kafka)

use chrono::{DateTime, Utc};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Headers;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::Kafka;
use uuid::Uuid;
use velostream::velostream::datasource::DataWriter;
use velostream::velostream::datasource::kafka::writer::KafkaDataWriter;
use velostream::velostream::kafka::serialization_format::SerializationFormat;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

// =============================================================================
// Test infrastructure
// =============================================================================

struct EventTimeTestEnv {
    _kafka_container: ContainerAsync<Kafka>,
    bootstrap_servers: String,
}

/// A consumed Kafka message with payload, headers, and message-level timestamp.
struct ConsumedMessage {
    #[allow(dead_code)]
    payload: String,
    headers: HashMap<String, String>,
    /// Kafka message timestamp (millis) extracted from CreateTime/LogAppendTime
    timestamp_ms: Option<i64>,
}

impl EventTimeTestEnv {
    async fn new() -> Self {
        let kafka_container = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container (is Docker running?)");

        let kafka_port = kafka_container
            .get_host_port_ipv4(9093)
            .await
            .expect("Failed to get Kafka port");
        let bootstrap_servers = format!("127.0.0.1:{}", kafka_port);

        // Wait for Kafka readiness
        tokio::time::sleep(Duration::from_secs(5)).await;

        Self {
            _kafka_container: kafka_container,
            bootstrap_servers,
        }
    }

    /// Create a unique topic name with UUID suffix to prevent cross-test pollution
    fn unique_topic(prefix: &str) -> String {
        format!("{}_{}", prefix, Uuid::new_v4().to_string().replace('-', ""))
    }

    async fn create_topic(&self, topic: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .expect("Failed to create admin client");

        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        admin
            .create_topics(&[new_topic], &opts)
            .await
            .expect("Failed to create topic");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    /// Consume one message from a topic, returning payload, headers, AND Kafka timestamp.
    fn consume_one(&self, topic: &str) -> ConsumedMessage {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", format!("et-test-{}", topic))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if std::time::Instant::now() > deadline {
                panic!("Timed out waiting for message on topic '{}'", topic);
            }

            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(msg)) => {
                    let payload = msg
                        .payload_view::<str>()
                        .unwrap_or(Ok(""))
                        .unwrap_or("")
                        .to_string();

                    let mut headers = HashMap::new();
                    if let Some(borrowed_headers) = msg.headers() {
                        for i in 0..borrowed_headers.count() {
                            let header = borrowed_headers.get(i);
                            if let Some(v) = header.value {
                                headers.insert(
                                    header.key.to_string(),
                                    String::from_utf8_lossy(v).to_string(),
                                );
                            }
                        }
                    }

                    let timestamp_ms = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(t)
                        | rdkafka::Timestamp::LogAppendTime(t) => Some(t),
                        rdkafka::Timestamp::NotAvailable => None,
                    };

                    return ConsumedMessage {
                        payload,
                        headers,
                        timestamp_ms,
                    };
                }
                Some(Err(e)) => panic!("Kafka consume error: {}", e),
                None => continue,
            }
        }
    }

    /// Consume N messages from a topic.
    fn consume_n(&self, topic: &str, n: usize) -> Vec<ConsumedMessage> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", format!("et-test-batch-{}", topic))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        let mut results = Vec::with_capacity(n);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);

        while results.len() < n {
            if std::time::Instant::now() > deadline {
                panic!(
                    "Timed out waiting for messages on topic '{}' (got {}/{})",
                    topic,
                    results.len(),
                    n
                );
            }

            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(msg)) => {
                    let payload = msg
                        .payload_view::<str>()
                        .unwrap_or(Ok(""))
                        .unwrap_or("")
                        .to_string();

                    let mut headers = HashMap::new();
                    if let Some(borrowed_headers) = msg.headers() {
                        for i in 0..borrowed_headers.count() {
                            let header = borrowed_headers.get(i);
                            if let Some(v) = header.value {
                                headers.insert(
                                    header.key.to_string(),
                                    String::from_utf8_lossy(v).to_string(),
                                );
                            }
                        }
                    }

                    let timestamp_ms = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(t)
                        | rdkafka::Timestamp::LogAppendTime(t) => Some(t),
                        rdkafka::Timestamp::NotAvailable => None,
                    };

                    results.push(ConsumedMessage {
                        payload,
                        headers,
                        timestamp_ms,
                    });
                }
                Some(Err(e)) => panic!("Kafka consume error: {}", e),
                None => continue,
            }
        }

        results
    }

    /// Create a KafkaDataWriter in JSON/async mode (non-transactional).
    async fn create_writer(&self, topic: &str) -> KafkaDataWriter {
        KafkaDataWriter::new_with_config(
            &self.bootstrap_servers,
            topic.to_string(),
            SerializationFormat::Json,
            None,
            None,
        )
        .await
        .expect("Failed to create KafkaDataWriter")
    }

    /// Try to create a KafkaDataWriter in transactional mode.
    /// Returns None if transactional init fails (common on single-node testcontainers Kafka).
    async fn try_create_transactional_writer(
        &self,
        topic: &str,
        txn_id: &str,
    ) -> Option<KafkaDataWriter> {
        let mut properties = HashMap::new();
        properties.insert("transactional.id".to_string(), txn_id.to_string());

        match KafkaDataWriter::from_properties(
            &self.bootstrap_servers,
            topic.to_string(),
            &properties,
            None,
        )
        .await
        {
            Ok(writer) => Some(writer),
            Err(e) => {
                println!(
                    "Transactional producer init failed (expected on single-node Kafka): {}",
                    e
                );
                None
            }
        }
    }
}

/// Helper: create a StreamRecord with given event_time and custom headers
fn make_record(
    event_time: Option<DateTime<Utc>>,
    extra_headers: HashMap<String, String>,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));

    StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time,
        headers: extra_headers,
        topic: None,
        key: None,
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Verifies that `KafkaDataWriter::write()` injects `_event_time` as both a Kafka
/// header and the message-level timestamp when the StreamRecord has an event_time.
#[tokio::test]
#[serial]
async fn test_single_write_injects_event_time_header() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_single_inject");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    // 2026-02-07T12:00:00Z => 1770393600000 ms
    let event_time = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let expected_ms = event_time.timestamp_millis();

    let record = make_record(Some(event_time), HashMap::new());
    writer.write(record).await.expect("write should succeed");

    let consumed = env.consume_one(&topic);

    // Assert: _event_time header present with correct millis value
    let header_val = consumed
        .headers
        .get("_event_time")
        .expect("_event_time header should be present");
    assert_eq!(
        header_val,
        &expected_ms.to_string(),
        "_event_time header should equal event_time in millis"
    );

    // Assert: Kafka message timestamp matches event_time
    assert_eq!(
        consumed.timestamp_ms,
        Some(expected_ms),
        "Kafka message timestamp should be set to event_time millis"
    );

    println!(
        "single write: _event_time header={}, kafka ts={:?}",
        header_val, consumed.timestamp_ms
    );
}

/// Verifies that no `_event_time` header is injected when the StreamRecord has no event_time.
#[tokio::test]
#[serial]
async fn test_single_write_no_event_time_no_header() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_single_no_et");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    let record = make_record(None, HashMap::new());
    writer.write(record).await.expect("write should succeed");

    let consumed = env.consume_one(&topic);

    // Assert: no _event_time header
    assert!(
        !consumed.headers.contains_key("_event_time"),
        "No _event_time header should be present when event_time is None"
    );

    println!(
        "no event_time: headers={:?}, kafka ts={:?}",
        consumed.headers.keys().collect::<Vec<_>>(),
        consumed.timestamp_ms
    );
}

/// Verifies that if the record already has an `_event_time` header, the writer
/// does NOT overwrite it (dedup logic).
#[tokio::test]
#[serial]
async fn test_single_write_preserves_existing_event_time_header() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_single_dedup");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    // Record has event_time AND a pre-existing _event_time header with a DIFFERENT value
    let event_time = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let pre_existing_header_value = "9999999999999"; // different from event_time millis

    let mut headers = HashMap::new();
    headers.insert(
        "_event_time".to_string(),
        pre_existing_header_value.to_string(),
    );

    let record = make_record(Some(event_time), headers);
    writer.write(record).await.expect("write should succeed");

    let consumed = env.consume_one(&topic);

    // Assert: _event_time header should be the PRE-EXISTING value, not overwritten
    let header_val = consumed
        .headers
        .get("_event_time")
        .expect("_event_time header should be present");
    assert_eq!(
        header_val, pre_existing_header_value,
        "Writer should not overwrite existing _event_time header"
    );

    // The Kafka message timestamp should still be set from event_time
    assert_eq!(
        consumed.timestamp_ms,
        Some(event_time.timestamp_millis()),
        "Kafka message timestamp should still be set from event_time"
    );

    println!(
        "dedup: header={} (pre-existing preserved), kafka ts={:?}",
        header_val, consumed.timestamp_ms
    );
}

/// Verifies that `write_batch()` injects per-record `_event_time` headers and
/// Kafka timestamps correctly for each record in the batch.
#[tokio::test]
#[serial]
async fn test_batch_write_injects_event_time_headers() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_batch_inject");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    let times: Vec<DateTime<Utc>> = vec![
        DateTime::parse_from_rfc3339("2026-02-07T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2026-02-07T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    ];

    let records: Vec<Arc<StreamRecord>> = times
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let mut fields = HashMap::new();
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("SYM{}", i)),
            );
            fields.insert("price".to_string(), FieldValue::Float(100.0 + i as f64));
            Arc::new(StreamRecord {
                fields,
                timestamp: Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                event_time: Some(*t),
                headers: HashMap::new(),
                topic: None,
                key: None,
            })
        })
        .collect();

    writer
        .write_batch(records)
        .await
        .expect("batch write should succeed");

    // Flush to ensure async producer delivers all messages
    writer.flush().await.expect("flush should succeed");

    // Brief pause for Kafka to make messages available
    tokio::time::sleep(Duration::from_secs(1)).await;

    let consumed = env.consume_n(&topic, 3);
    assert_eq!(consumed.len(), 3, "Should consume exactly 3 messages");

    for (i, msg) in consumed.iter().enumerate() {
        let expected_ms = times[i].timestamp_millis();

        let header_val = msg
            .headers
            .get("_event_time")
            .unwrap_or_else(|| panic!("Record {} should have _event_time header", i));
        assert_eq!(
            header_val,
            &expected_ms.to_string(),
            "Record {} _event_time header mismatch",
            i
        );

        assert_eq!(
            msg.timestamp_ms,
            Some(expected_ms),
            "Record {} Kafka timestamp mismatch",
            i
        );

        println!(
            "batch[{}]: header={}, kafka ts={:?}",
            i, header_val, msg.timestamp_ms
        );
    }
}

/// Full round-trip: StreamRecord → KafkaDataWriter → Kafka → BaseConsumer → from_kafka() → StreamRecord
///
/// This is the KEY test verifying that event_time survives the entire pipeline.
#[tokio::test]
#[serial]
async fn test_full_round_trip_event_time_preserved() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_roundtrip");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    let event_time = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let mut extra_headers = HashMap::new();
    extra_headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );
    extra_headers.insert("x-custom-header".to_string(), "custom-value".to_string());

    let original = make_record(Some(event_time), extra_headers);
    writer.write(original).await.expect("write should succeed");

    // Consume raw message
    let consumed = env.consume_one(&topic);

    // Reconstruct StreamRecord via from_kafka()
    let payload: serde_json::Value =
        serde_json::from_str(&consumed.payload).expect("payload should be valid JSON");
    let mut fields = HashMap::new();
    if let serde_json::Value::Object(map) = payload {
        for (k, v) in map {
            let fv = match v {
                serde_json::Value::String(s) => FieldValue::String(s),
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        FieldValue::Float(f)
                    } else {
                        FieldValue::Integer(n.as_i64().unwrap_or(0))
                    }
                }
                _ => FieldValue::String(v.to_string()),
            };
            fields.insert(k, fv);
        }
    }

    let reconstructed = StreamRecord::from_kafka(
        fields,
        &topic,
        None,                  // key_bytes
        consumed.timestamp_ms, // Kafka message timestamp
        0,                     // offset
        0,                     // partition
        consumed.headers.clone(),
        None, // event_time_config
    );

    // CRITICAL: event_time should survive the round-trip
    assert!(
        reconstructed.event_time.is_some(),
        "Reconstructed record should have event_time"
    );
    assert_eq!(
        reconstructed.event_time.unwrap().timestamp_millis(),
        event_time.timestamp_millis(),
        "event_time should be exactly preserved through Kafka round-trip"
    );

    // User headers should also survive
    assert_eq!(
        reconstructed.headers.get("traceparent").map(|s| s.as_str()),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        "traceparent header should survive round-trip"
    );
    assert_eq!(
        reconstructed
            .headers
            .get("x-custom-header")
            .map(|s| s.as_str()),
        Some("custom-value"),
        "custom header should survive round-trip"
    );

    println!(
        "round-trip: original_et={}, reconstructed_et={:?}",
        event_time.timestamp_millis(),
        reconstructed.event_time.map(|et| et.timestamp_millis())
    );
}

/// Verifies per-record conditional injection: records WITH event_time get the header,
/// records WITHOUT event_time do not.
#[tokio::test]
#[serial]
async fn test_batch_write_mixed_event_times() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_batch_mixed");
    env.create_topic(&topic).await;

    let mut writer = env.create_writer(&topic).await;

    let et1 = DateTime::parse_from_rfc3339("2026-02-07T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let et3 = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let records: Vec<Arc<StreamRecord>> = vec![
        // Record 0: HAS event_time
        Arc::new(make_record(Some(et1), HashMap::new())),
        // Record 1: NO event_time
        Arc::new(make_record(None, HashMap::new())),
        // Record 2: HAS event_time
        Arc::new(make_record(Some(et3), HashMap::new())),
    ];

    writer
        .write_batch(records)
        .await
        .expect("batch write should succeed");

    writer.flush().await.expect("flush should succeed");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let consumed = env.consume_n(&topic, 3);
    assert_eq!(consumed.len(), 3, "Should consume exactly 3 messages");

    // Record 0: should have _event_time
    let et1_ms_str = et1.timestamp_millis().to_string();
    assert_eq!(
        consumed[0].headers.get("_event_time").map(|s| s.as_str()),
        Some(et1_ms_str.as_str()),
        "Record 0 should have _event_time header"
    );
    assert_eq!(
        consumed[0].timestamp_ms,
        Some(et1.timestamp_millis()),
        "Record 0 Kafka timestamp should match event_time"
    );

    // Record 1: should NOT have _event_time
    assert!(
        !consumed[1].headers.contains_key("_event_time"),
        "Record 1 (no event_time) should NOT have _event_time header"
    );

    // Record 2: should have _event_time
    let et3_ms_str = et3.timestamp_millis().to_string();
    assert_eq!(
        consumed[2].headers.get("_event_time").map(|s| s.as_str()),
        Some(et3_ms_str.as_str()),
        "Record 2 should have _event_time header"
    );
    assert_eq!(
        consumed[2].timestamp_ms,
        Some(et3.timestamp_millis()),
        "Record 2 Kafka timestamp should match event_time"
    );

    println!(
        "mixed batch: rec0 has_et={}, rec1 has_et={}, rec2 has_et={}",
        consumed[0].headers.contains_key("_event_time"),
        consumed[1].headers.contains_key("_event_time"),
        consumed[2].headers.contains_key("_event_time"),
    );
}

/// Multi-hop pipeline: Writer1 → Kafka topic A → from_kafka() → Writer2 → Kafka topic B → from_kafka()
///
/// Verifies event_time is faithfully preserved across two serialization/deserialization hops.
#[tokio::test]
#[serial]
async fn test_event_time_header_survives_multiple_hops() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic_a = EventTimeTestEnv::unique_topic("et_hop_a");
    let topic_b = EventTimeTestEnv::unique_topic("et_hop_b");
    env.create_topic(&topic_a).await;
    env.create_topic(&topic_b).await;

    let event_time = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let expected_ms = event_time.timestamp_millis();

    // === HOP 1: Write original record to topic A ===
    let mut writer_a = env.create_writer(&topic_a).await;
    let original = make_record(Some(event_time), HashMap::new());
    writer_a
        .write(original)
        .await
        .expect("hop 1 write should succeed");

    // === Consume from topic A and reconstruct ===
    let consumed_a = env.consume_one(&topic_a);

    let payload_a: serde_json::Value =
        serde_json::from_str(&consumed_a.payload).expect("hop 1 payload should be valid JSON");
    let mut fields_a = HashMap::new();
    if let serde_json::Value::Object(map) = payload_a {
        for (k, v) in map {
            let fv = match v {
                serde_json::Value::String(s) => FieldValue::String(s),
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        FieldValue::Float(f)
                    } else {
                        FieldValue::Integer(n.as_i64().unwrap_or(0))
                    }
                }
                _ => FieldValue::String(v.to_string()),
            };
            fields_a.insert(k, fv);
        }
    }

    let hop1_record = StreamRecord::from_kafka(
        fields_a,
        &topic_a,
        None,
        consumed_a.timestamp_ms,
        0,
        0,
        consumed_a.headers.clone(),
        None,
    );

    // Verify hop 1 preserved event_time
    let hop1_event_time_ms = hop1_record.event_time.map(|et| et.timestamp_millis());
    assert_eq!(
        hop1_event_time_ms,
        Some(expected_ms),
        "Hop 1 from_kafka should preserve event_time"
    );

    // === HOP 2: Write reconstructed record to topic B ===
    let mut writer_b = env.create_writer(&topic_b).await;
    writer_b
        .write(hop1_record)
        .await
        .expect("hop 2 write should succeed");

    // === Consume from topic B and reconstruct ===
    let consumed_b = env.consume_one(&topic_b);

    let payload_b: serde_json::Value =
        serde_json::from_str(&consumed_b.payload).expect("hop 2 payload should be valid JSON");
    let mut fields_b = HashMap::new();
    if let serde_json::Value::Object(map) = payload_b {
        for (k, v) in map {
            let fv = match v {
                serde_json::Value::String(s) => FieldValue::String(s),
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        FieldValue::Float(f)
                    } else {
                        FieldValue::Integer(n.as_i64().unwrap_or(0))
                    }
                }
                _ => FieldValue::String(v.to_string()),
            };
            fields_b.insert(k, fv);
        }
    }

    let hop2_record = StreamRecord::from_kafka(
        fields_b,
        &topic_b,
        None,
        consumed_b.timestamp_ms,
        0,
        0,
        consumed_b.headers.clone(),
        None,
    );

    // CRITICAL: event_time at hop 2 must equal original
    assert_eq!(
        hop2_record.event_time.map(|et| et.timestamp_millis()),
        Some(expected_ms),
        "Event time must survive 2 hops: Writer → Kafka → from_kafka → Writer → Kafka → from_kafka"
    );

    // Verify _event_time header is present at both hops
    let expected_ms_str = expected_ms.to_string();
    assert_eq!(
        consumed_a.headers.get("_event_time").map(|s| s.as_str()),
        Some(expected_ms_str.as_str()),
        "Hop 1 Kafka message should have _event_time header"
    );
    assert_eq!(
        consumed_b.headers.get("_event_time").map(|s| s.as_str()),
        Some(expected_ms_str.as_str()),
        "Hop 2 Kafka message should have _event_time header"
    );

    println!(
        "multi-hop: original={}, hop1={:?}, hop2={:?}",
        expected_ms,
        hop1_event_time_ms,
        hop2_record.event_time.map(|et| et.timestamp_millis()),
    );
}

// =============================================================================
// Transactional mode tests
// =============================================================================

/// Verifies that `send_transactional()` correctly propagates event_time as both
/// a Kafka header and message timestamp when using transactional writes.
///
/// Gracefully skips if transactional init fails (common on single-node testcontainers Kafka).
#[tokio::test]
#[serial]
async fn test_transactional_single_write_injects_event_time() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_txn_single");
    env.create_topic(&topic).await;

    let txn_id = format!("et-txn-{}", Uuid::new_v4());
    let mut writer = match env.try_create_transactional_writer(&topic, &txn_id).await {
        Some(w) => w,
        None => {
            println!("Skipping: transactional producer not available on this Kafka instance");
            return;
        }
    };

    let event_time = DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let expected_ms = event_time.timestamp_millis();

    // Transactional writes require explicit transaction boundaries
    writer
        .begin_transaction()
        .await
        .expect("begin_transaction should succeed");

    let record = make_record(Some(event_time), HashMap::new());
    writer
        .write(record)
        .await
        .expect("transactional write should succeed");

    writer
        .commit_transaction()
        .await
        .expect("commit_transaction should succeed");

    let consumed = env.consume_one(&topic);

    // Assert: _event_time header present with correct millis value
    let header_val = consumed
        .headers
        .get("_event_time")
        .expect("_event_time header should be present in transactional write");
    assert_eq!(
        header_val,
        &expected_ms.to_string(),
        "Transactional write: _event_time header should equal event_time in millis"
    );

    // Assert: Kafka message timestamp matches event_time
    assert_eq!(
        consumed.timestamp_ms,
        Some(expected_ms),
        "Transactional write: Kafka message timestamp should be set to event_time millis"
    );

    println!(
        "txn single write: _event_time header={}, kafka ts={:?}",
        header_val, consumed.timestamp_ms
    );
}

/// Verifies that transactional batch writes inject per-record `_event_time` headers
/// and Kafka timestamps, with proper transaction commit semantics.
///
/// Gracefully skips if transactional init fails (common on single-node testcontainers Kafka).
#[tokio::test]
#[serial]
async fn test_transactional_batch_write_injects_event_time_headers() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = EventTimeTestEnv::new().await;
    let topic = EventTimeTestEnv::unique_topic("et_txn_batch");
    env.create_topic(&topic).await;

    let txn_id = format!("et-txn-batch-{}", Uuid::new_v4());
    let mut writer = match env.try_create_transactional_writer(&topic, &txn_id).await {
        Some(w) => w,
        None => {
            println!("Skipping: transactional producer not available on this Kafka instance");
            return;
        }
    };

    let times: Vec<DateTime<Utc>> = vec![
        DateTime::parse_from_rfc3339("2026-02-07T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2026-02-07T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2026-02-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    ];

    let records: Vec<Arc<StreamRecord>> = times
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let mut fields = HashMap::new();
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("TXN_SYM{}", i)),
            );
            fields.insert("price".to_string(), FieldValue::Float(200.0 + i as f64));
            Arc::new(StreamRecord {
                fields,
                timestamp: Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                event_time: Some(*t),
                headers: HashMap::new(),
                topic: None,
                key: None,
            })
        })
        .collect();

    // Transactional batch: begin → write_batch → commit
    writer
        .begin_transaction()
        .await
        .expect("begin_transaction should succeed");

    writer
        .write_batch(records)
        .await
        .expect("transactional batch write should succeed");

    writer
        .commit_transaction()
        .await
        .expect("commit_transaction should succeed");

    // Brief pause for committed messages to become visible
    tokio::time::sleep(Duration::from_secs(1)).await;

    let consumed = env.consume_n(&topic, 3);
    assert_eq!(
        consumed.len(),
        3,
        "Should consume exactly 3 transactional messages"
    );

    for (i, msg) in consumed.iter().enumerate() {
        let expected_ms = times[i].timestamp_millis();

        let header_val = msg
            .headers
            .get("_event_time")
            .unwrap_or_else(|| panic!("Txn record {} should have _event_time header", i));
        assert_eq!(
            header_val,
            &expected_ms.to_string(),
            "Txn record {} _event_time header mismatch",
            i
        );

        assert_eq!(
            msg.timestamp_ms,
            Some(expected_ms),
            "Txn record {} Kafka timestamp mismatch",
            i
        );

        println!(
            "txn batch[{}]: header={}, kafka ts={:?}",
            i, header_val, msg.timestamp_ms
        );
    }
}
