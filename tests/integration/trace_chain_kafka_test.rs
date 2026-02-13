//! Multi-hop distributed trace chain test with testcontainers Kafka
//!
//! This test reproduces the production issue where traces were missing intermediate
//! hops. The pipeline simulates 3 apps:
//!
//!   app_market_data → app_price_analytics → app_trading_signals
//!
//! Each hop:
//!   1. Consumes records from an input Kafka topic (extracting traceparent)
//!   2. Creates a child batch span linked to the upstream trace
//!   3. Injects the batch span's trace context into output records
//!   4. Produces the output records to the next Kafka topic
//!
//! The test verifies:
//!   - All spans are collected by the in-memory span processor
//!   - All spans share the same trace_id (single distributed trace)
//!   - Parent-child span relationships form a correct chain
//!   - Kafka headers preserve traceparent through produce/consume round-trips

use opentelemetry::trace::TraceId;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::Kafka;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::StreamRecord;

// Use shared helpers from common
use super::*;

// =============================================================================
// Test infrastructure
// =============================================================================

struct TraceChainTestEnv {
    _kafka_container: ContainerAsync<Kafka>,
    bootstrap_servers: String,
}

impl TraceChainTestEnv {
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

    /// Produce a JSON record with optional Kafka headers to a topic
    async fn produce_with_headers(
        &self,
        topic: &str,
        payload: &str,
        headers: &HashMap<String, String>,
    ) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create producer");

        let mut owned_headers = OwnedHeaders::new();
        for (k, v) in headers {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }

        let record = FutureRecord::to(topic)
            .payload(payload)
            .key("test-key")
            .headers(owned_headers);

        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("Failed to produce message");
    }

    /// Consume one record from a topic, returning (payload, headers)
    fn consume_one(&self, topic: &str) -> (String, HashMap<String, String>) {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &format!("trace-test-{}", topic))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        // Poll until we get a message (with timeout)
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

                    return (payload, headers);
                }
                Some(Err(e)) => panic!("Kafka consume error: {}", e),
                None => continue,
            }
        }
    }
}

// create_test_record, create_test_record_with_headers, create_test_observability_manager,
// extract_trace_id, extract_span_id imported from shared helpers via super::*

// =============================================================================
// Multi-hop trace chain test through Kafka
// =============================================================================

/// Tests that distributed traces chain correctly across 3 processing hops
/// connected by Kafka topics, and that ALL hops produce collected spans.
///
/// This reproduces the production issue where app_market_data batch spans were
/// created but never appeared in Tempo, breaking the parent-child chain.
#[tokio::test]
#[serial]
async fn test_multi_hop_trace_chain_through_kafka() {
    // Skip if Docker is not available
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = TraceChainTestEnv::new().await;

    // Create the topic pipeline: source → hop1_out → hop2_out → hop3_out
    env.create_topic("trace_source").await;
    env.create_topic("trace_hop1_out").await;
    env.create_topic("trace_hop2_out").await;

    // Initialize observability with in-memory span collection
    let obs = create_test_observability_manager("trace-chain-test").await;

    // =========================================================================
    // Stage 0: Produce seed record to source topic (no trace context)
    // =========================================================================
    env.produce_with_headers(
        "trace_source",
        r#"{"symbol":"AAPL","price":150.0}"#,
        &HashMap::new(),
    )
    .await;

    // =========================================================================
    // HOP 1: Simulate app_market_data
    //   Reads from trace_source, creates ROOT batch span, writes to trace_hop1_out
    // =========================================================================
    let (_, source_headers) = env.consume_one("trace_source");
    let source_record = create_test_record_with_headers(source_headers);

    // Create batch span — no upstream trace, so this is the ROOT span
    let hop1_span = ObservabilityHelper::start_batch_span(
        &Some(obs.clone()),
        "market_data_ts",
        1,
        &[source_record],
    );
    assert!(
        hop1_span.is_some(),
        "Hop 1 should create a batch span (ROOT)"
    );

    let hop1_ctx = hop1_span.as_ref().unwrap().span_context().unwrap();
    let root_trace_id = hop1_ctx.trace_id().to_string();
    let hop1_span_id = hop1_ctx.span_id().to_string();

    // Inject trace context into output records
    let mut hop1_output = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &hop1_span,
        &mut hop1_output,
        "market_data_ts",
    );

    // Verify injection worked
    let hop1_traceparent = hop1_output[0]
        .headers
        .get("traceparent")
        .expect("Hop 1 output should have traceparent");
    assert_eq!(
        extract_trace_id(hop1_traceparent),
        root_trace_id,
        "Hop 1 output should carry the root trace_id"
    );

    // Produce to Kafka with trace headers
    env.produce_with_headers(
        "trace_hop1_out",
        r#"{"symbol":"AAPL","price":150.0}"#,
        &hop1_output[0].headers,
    )
    .await;

    // Drop the span so it gets collected
    drop(hop1_span);

    // =========================================================================
    // HOP 2: Simulate app_price_analytics
    //   Reads from trace_hop1_out, creates CHILD batch span, writes to trace_hop2_out
    // =========================================================================
    let (_, hop1_kafka_headers) = env.consume_one("trace_hop1_out");

    // Verify Kafka preserved the traceparent header
    assert!(
        hop1_kafka_headers.contains_key("traceparent"),
        "Kafka should preserve traceparent header through produce/consume"
    );
    let kafka_traceparent = &hop1_kafka_headers["traceparent"];
    assert_eq!(
        extract_trace_id(kafka_traceparent),
        root_trace_id,
        "Kafka should preserve the trace_id"
    );

    let hop2_input = create_test_record_with_headers(hop1_kafka_headers);

    // Create child batch span — extracts upstream trace from input headers
    let hop2_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "price_stats", 1, &[hop2_input]);
    assert!(
        hop2_span.is_some(),
        "Hop 2 should create a batch span (CHILD)"
    );

    let hop2_ctx = hop2_span.as_ref().unwrap().span_context().unwrap();
    let hop2_trace_id = hop2_ctx.trace_id().to_string();
    let hop2_span_id = hop2_ctx.span_id().to_string();

    // Child span should inherit the same trace_id
    assert_eq!(
        hop2_trace_id, root_trace_id,
        "Hop 2 should inherit root trace_id (same distributed trace)"
    );
    // But have a different span_id
    assert_ne!(
        hop2_span_id, hop1_span_id,
        "Hop 2 should have its own span_id"
    );

    // Inject into output and produce to next topic
    let mut hop2_output = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &hop2_span,
        &mut hop2_output,
        "price_stats",
    );

    env.produce_with_headers(
        "trace_hop2_out",
        r#"{"symbol":"AAPL","price":150.0}"#,
        &hop2_output[0].headers,
    )
    .await;

    drop(hop2_span);

    // =========================================================================
    // HOP 3: Simulate app_trading_signals
    //   Reads from trace_hop2_out, creates GRANDCHILD batch span
    // =========================================================================
    let (_, hop2_kafka_headers) = env.consume_one("trace_hop2_out");

    assert!(
        hop2_kafka_headers.contains_key("traceparent"),
        "Hop 2 output should have traceparent in Kafka"
    );
    let hop2_kafka_traceparent = &hop2_kafka_headers["traceparent"];
    assert_eq!(
        extract_trace_id(hop2_kafka_traceparent),
        root_trace_id,
        "Hop 2→3 Kafka should carry root trace_id"
    );

    let hop3_input = create_test_record_with_headers(hop2_kafka_headers);

    let hop3_span = ObservabilityHelper::start_batch_span(
        &Some(obs.clone()),
        "volume_spike_analysis",
        1,
        &[hop3_input],
    );
    assert!(
        hop3_span.is_some(),
        "Hop 3 should create a batch span (GRANDCHILD)"
    );

    let hop3_ctx = hop3_span.as_ref().unwrap().span_context().unwrap();
    let hop3_trace_id = hop3_ctx.trace_id().to_string();
    let hop3_span_id = hop3_ctx.span_id().to_string();

    assert_eq!(
        hop3_trace_id, root_trace_id,
        "Hop 3 should inherit root trace_id (same distributed trace)"
    );
    assert_ne!(
        hop3_span_id, hop2_span_id,
        "Hop 3 should have its own span_id"
    );
    assert_ne!(
        hop3_span_id, hop1_span_id,
        "Hop 3 span_id should differ from hop 1"
    );

    drop(hop3_span);

    // =========================================================================
    // VERIFICATION: Check collected spans
    // =========================================================================

    // Give the span processor a moment to collect
    tokio::time::sleep(Duration::from_millis(200)).await;

    let obs_lock = obs.read().await;
    let telemetry = obs_lock
        .telemetry()
        .expect("Telemetry should be initialized");
    let collected = telemetry.collected_spans();

    println!("\n=== Collected Spans ===");
    for span in &collected {
        let trace_id = span.span_context.trace_id().to_string();
        let span_id = span.span_context.span_id().to_string();
        let parent = span.parent_span_id.to_string();
        println!(
            "  {} | trace={} span={} parent={}",
            span.name, trace_id, span_id, parent
        );
    }
    println!("======================\n");

    // Filter spans belonging to our trace
    let our_spans: Vec<_> = collected
        .iter()
        .filter(|s| s.span_context.trace_id().to_string() == root_trace_id)
        .collect();

    // CRITICAL ASSERTION: All 3 hops should produce collected spans
    assert!(
        our_spans.len() >= 3,
        "Expected at least 3 spans in trace {} (one per hop), got {}. \
         This was the production bug: intermediate hop spans were created \
         but never exported.",
        root_trace_id,
        our_spans.len()
    );

    // Verify span names match our pipeline
    let span_names: Vec<String> = our_spans.iter().map(|s| s.name.to_string()).collect();
    assert!(
        span_names.contains(&"batch:market_data_ts".to_string()),
        "Missing hop 1 (market_data_ts) span. Found: {:?}",
        span_names
    );
    assert!(
        span_names.contains(&"batch:price_stats".to_string()),
        "Missing hop 2 (price_stats) span. Found: {:?}",
        span_names
    );
    assert!(
        span_names.contains(&"batch:volume_spike_analysis".to_string()),
        "Missing hop 3 (volume_spike_analysis) span. Found: {:?}",
        span_names
    );

    // Verify parent-child chain: hop2.parent == hop1.span_id, hop3.parent == hop2.span_id
    let hop1_collected = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:market_data_ts")
        .expect("Hop 1 span not found");
    let hop2_collected = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:price_stats")
        .expect("Hop 2 span not found");
    let hop3_collected = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:volume_spike_analysis")
        .expect("Hop 3 span not found");

    // Hop 1 should be a root span (no parent, or parent is invalid/zero)
    let hop1_parent = hop1_collected.parent_span_id.to_string();
    println!("Hop 1 parent_span_id: {}", hop1_parent);

    // Hop 2 parent should point to Hop 1's span_id
    let hop2_parent = hop2_collected.parent_span_id.to_string();
    let hop1_sid = hop1_collected.span_context.span_id().to_string();
    assert_eq!(
        hop2_parent, hop1_sid,
        "Hop 2's parent ({}) should be Hop 1's span_id ({})",
        hop2_parent, hop1_sid
    );

    // Hop 3 parent should point to Hop 2's span_id
    let hop3_parent = hop3_collected.parent_span_id.to_string();
    let hop2_sid = hop2_collected.span_context.span_id().to_string();
    assert_eq!(
        hop3_parent, hop2_sid,
        "Hop 3's parent ({}) should be Hop 2's span_id ({})",
        hop3_parent, hop2_sid
    );

    println!("✅ All 3 hops collected with correct parent-child chain:");
    println!(
        "   Hop 1 (market_data_ts):        span={} parent=ROOT",
        hop1_sid
    );
    println!(
        "   Hop 2 (price_stats):            span={} parent={}",
        hop2_sid, hop1_sid
    );
    println!(
        "   Hop 3 (volume_spike_analysis):  span={} parent={}",
        hop3_collected.span_context.span_id(),
        hop2_sid
    );
}

/// Tests that Kafka preserves traceparent headers through multiple
/// produce/consume cycles without corruption.
#[tokio::test]
#[serial]
async fn test_kafka_preserves_trace_headers_round_trip() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker-based test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    let env = TraceChainTestEnv::new().await;
    env.create_topic("header_roundtrip").await;

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let tracestate = "velostream=test";

    let mut headers = HashMap::new();
    headers.insert("traceparent".to_string(), traceparent.to_string());
    headers.insert("tracestate".to_string(), tracestate.to_string());

    env.produce_with_headers("header_roundtrip", r#"{"test":true}"#, &headers)
        .await;

    let (_payload, consumed_headers) = env.consume_one("header_roundtrip");

    assert_eq!(
        consumed_headers.get("traceparent").map(|s| s.as_str()),
        Some(traceparent),
        "traceparent header should survive Kafka round-trip"
    );
    assert_eq!(
        consumed_headers.get("tracestate").map(|s| s.as_str()),
        Some(tracestate),
        "tracestate header should survive Kafka round-trip"
    );
}

/// Tests that the ObservabilityHelper span chain works correctly in-memory
/// (no Kafka) to verify the trace propagation logic itself is sound.
#[tokio::test]
#[serial]
async fn test_span_chain_in_memory_three_hops() {
    let obs = create_test_observability_manager("trace-chain-test").await;

    // HOP 1: Root span (no upstream)
    let hop1_input = vec![create_test_record()]; // no traceparent
    let hop1_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "producer_job", 1, &hop1_input);
    assert!(hop1_span.is_some());

    let hop1_ctx = hop1_span.as_ref().unwrap().span_context().unwrap();
    let root_trace_id = hop1_ctx.trace_id();

    // Inject into output records
    let mut hop1_output = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &hop1_span,
        &mut hop1_output,
        "producer_job",
    );
    drop(hop1_span);

    // HOP 2: Child span (extracts from hop1 output headers)
    let hop2_input = vec![StreamRecord {
        headers: hop1_output[0].headers.clone(),
        ..create_test_record()
    }];
    let hop2_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "transform_job", 1, &hop2_input);
    assert!(hop2_span.is_some());

    let hop2_ctx = hop2_span.as_ref().unwrap().span_context().unwrap();
    assert_eq!(
        hop2_ctx.trace_id(),
        root_trace_id,
        "Hop 2 should inherit root trace_id"
    );

    let mut hop2_output = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &hop2_span,
        &mut hop2_output,
        "transform_job",
    );
    drop(hop2_span);

    // HOP 3: Grandchild span
    let hop3_input = vec![StreamRecord {
        headers: hop2_output[0].headers.clone(),
        ..create_test_record()
    }];
    let hop3_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "sink_job", 1, &hop3_input);
    assert!(hop3_span.is_some());

    let hop3_ctx = hop3_span.as_ref().unwrap().span_context().unwrap();
    assert_eq!(
        hop3_ctx.trace_id(),
        root_trace_id,
        "Hop 3 should inherit root trace_id"
    );
    drop(hop3_span);

    // Wait for span collection
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify collected spans
    let obs_lock = obs.read().await;
    let telemetry = obs_lock.telemetry().expect("Telemetry should exist");
    let collected = telemetry.collected_spans();

    let our_spans: Vec<_> = collected
        .iter()
        .filter(|s| s.span_context.trace_id() == root_trace_id)
        .collect();

    assert_eq!(
        our_spans.len(),
        3,
        "All 3 hops should produce collected spans, got {}",
        our_spans.len()
    );

    // Verify parent chain
    let hop1_span_data = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:producer_job")
        .expect("Missing hop 1 span");
    let hop2_span_data = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:transform_job")
        .expect("Missing hop 2 span");
    let hop3_span_data = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "batch:sink_job")
        .expect("Missing hop 3 span");

    assert_eq!(
        hop2_span_data.parent_span_id,
        hop1_span_data.span_context.span_id(),
        "Hop 2 parent should be Hop 1"
    );
    assert_eq!(
        hop3_span_data.parent_span_id,
        hop2_span_data.span_context.span_id(),
        "Hop 3 parent should be Hop 2"
    );
}

/// Tests that a broken trace chain (missing traceparent header) starts
/// a new trace instead of silently dropping the span.
#[tokio::test]
#[serial]
async fn test_broken_chain_starts_new_trace() {
    let obs = create_test_observability_manager("trace-chain-test").await;

    // HOP 1: Root span
    let hop1_input = vec![create_test_record()];
    let hop1_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "producer", 1, &hop1_input);
    let hop1_trace_id = hop1_span
        .as_ref()
        .unwrap()
        .span_context()
        .unwrap()
        .trace_id();

    let mut hop1_output = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &hop1_span,
        &mut hop1_output,
        "producer",
    );
    drop(hop1_span);

    // HOP 2: Receives record WITHOUT traceparent (simulating header loss)
    let hop2_input = vec![create_test_record()]; // no headers!
    let hop2_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "consumer", 1, &hop2_input);
    assert!(hop2_span.is_some(), "Should still create a span");

    let hop2_trace_id = hop2_span
        .as_ref()
        .unwrap()
        .span_context()
        .unwrap()
        .trace_id();

    // Should start a NEW trace since no upstream context
    assert_ne!(
        hop2_trace_id, hop1_trace_id,
        "Broken chain should start a new trace, not inherit old one"
    );
    assert_ne!(
        hop2_trace_id,
        TraceId::INVALID,
        "New trace should have valid trace_id"
    );

    drop(hop2_span);
}
