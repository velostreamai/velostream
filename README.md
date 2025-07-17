# ferris streams

![Rust CI](https://github.com/bluemonk3y/ferrisstreams/workflows/Rust%20CI/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/ferrisstreams.svg)](https://crates.io/crates/ferrisstreams)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](./LICENSE)

A Rust-idiomatic and robust client library for Apache Kafka, designed for high-performance, fault-tolerant, and flexible processing of **multiple Kafka topics and data streams**.

## 🌟 Features (WIP)

* **Asynchronous Kafka Interaction:** Built on `rdkafka` & `tokio` for efficient, non-blocking I/O with Kafka brokers.
* **Comprehensive Client Support:** Includes robust implementations for:
    * **Producers:** Reliably send messages to Kafka topics.
    * **Consumers:** Efficiently consume messages from Kafka, supporting group management.
    * **Parallel Consumers:** Leverage concurrent processing for high-throughput message handling across multiple partitions and topics.
* **Flexible Serialization/Deserialization (`serde`):** Provides a modular `serde` framework, with out-of-the-box support for JSON and extensible traits for custom formats (e.g., Avro, Protobuf via feature flags).
* **KTable-like Stateful Processing:** Build and manage local, fault-tolerant state stores for stream processing applications, enabling aggregations, joins, and materializing views across various input streams.
* **Robust Error Handling:** Utilizes `thiserror` for precise, user-friendly error types and `anyhow` for convenient error propagation.
* **Configurable and Extensible:** Designed with builder patterns and traits to allow for easy customization and integration.


## 🚀 Work in Progress
- Basic producer and consumer implementations including Context support
- Consume from time 'time-X'
- Compacting Producer (high throughput, low latency for ticking data sources)
- Serialization and deserialization using json serde


Higher order functions and more advanced features are planned for future releases, including:
- Fan in (multiple topics to single topic)
- Fan out (single topic to multiple topics)
- KTable-like stateful processing
- Bridging to other Kafka topics including support for filtering, mapping, and reducing streams and header propogation
- 


## ⚡️ Quick Start

Add `ferrisstreams` to your `Cargo.toml`:

```toml
[dependencies]
ferrisstreams = "0.1.0" # Use the latest version available on crates.io
tokio = { version = "1", features = ["full"] } # Or specific tokio features