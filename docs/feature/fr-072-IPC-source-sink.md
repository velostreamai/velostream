# FR-072: In-Memory Source and Sink Architecture

**Status**: Draft
**Author**: Velostream Team
**Created**: 2025-10-02
**RFC Type**: Feature Request

## Summary

Velostream introduces a process-local streaming fabric — a zero-copy, bounded, back-pressured memory bus that connects SQL pipelines (CSAS, CTAS, and subqueries) directly inside a single runtime.

Unlike Kafka, which externalizes every stream boundary, or Flink, which compiles a monolithic DAG of operators, Velostream’s design allows independently defined SQL queries to communicate through lightweight, in-process channels. Each channel behaves like a stream — but it never leaves memory, never serializes, and never touches a broker.

This creates a new latency class: <100µs per stage, suitable for AI feature extraction, financial tick analytics, IoT anomaly detection, and real-time joins where every microsecond matters.

By combining the composability of Kafka with the latency of shared memory, Velostream defines a new category:

> The embedded streaming database — where pipelines live inside your process, not across a cluster.


This makes it uniquely suited for ultra-low-latency pipelines, AI feature extraction, and financial analytics, where intra-pipeline roundtrips below 100µs enable entirely new use cases — such as real-time joins, sliding aggregations, and anomaly detection directly on live streams.

### Problem Statement

Current Velostream deployments require external systems (Kafka, Files) to link query stages together:

```sql
-- Stage 1: Filter high-value orders (writes to Kafka)
CREATE STREAM filtered_orders AS
SELECT * FROM kafka_orders_source
WHERE amount > 1000
INTO kafka_filtered_sink;

-- Stage 2: Aggregate (reads from Kafka)
CREATE STREAM order_stats AS
SELECT customer_id, SUM(amount) as total
FROM kafka_filtered_source  -- Requires external Kafka topic!
GROUP BY customer_id
INTO file_stats_sink;
```

**Issues with current approach:**
1. **Latency overhead** - Serialization + network + deserialization adds 10-100ms per stage
2. **External dependencies** - Requires Kafka/filesystem for intermediate results
3. **Resource waste** - Serialize data only to immediately deserialize it
4. **Complexity** - Need to manage external topics/files for transient data
5. **Testing friction** - Integration tests require Kafka infrastructure



## Differentiation
| Feature                  | **Velostream (In-Memory Fabric)**      | **Flink / Arroyo** | **Kafka / Materialize**  |
| ------------------------ | -------------------------------------- | ------------------ | ------------------------ |
| **Latency per stage**    | <100µs                                 | 1–10ms             | 10–100ms                 |
| **Serialization**        | Zero-copy (`Arc<StreamRecord>`)        | Arrow / Protobuf   | Avro / JSON              |
| **Deployment**           | Single Rust binary                     | JVM cluster        | External cluster         |
| **SQL composability**    | ✅ Native (`memory://`)                 | Limited            | ❌                        |
| **Concurrency model**    | Lock-free, async MPMC                  | Task-based DAG     | External topics          |
| **Schema visibility**    | Shared memory structs                  | Arrow schema only  | External Avro registry   |
| **State model**          | Row-native, window-aware               | Columnar, batchy   | None                     |
| **Use cases**            | Sub-millisecond analytics, AI, finance | Batchy real-time   | Event integration        |
| **Integration overhead** | Zero (in-process)                      | High (task graph)  | Very high (network hops) |


### Use Cases

**1. Multi-Stage Pipelines**
```sql
-- All stages run in same process with zero serialization
CREATE STREAM stage1 AS
SELECT * FROM kafka_source WHERE valid = true
INTO memory://validated_orders;

CREATE STREAM stage2 AS
SELECT customer_id, COUNT(*) as order_count
FROM memory://validated_orders
GROUP BY customer_id
INTO memory://customer_counts;

CREATE STREAM stage3 AS
SELECT * FROM memory://customer_counts
WHERE order_count > 10
INTO kafka_high_value_customers;
```

**2. Fan-Out Processing**
```sql
-- One source, multiple consumers (like Kafka consumer groups)
CREATE STREAM raw_events AS
SELECT * FROM kafka_source
INTO memory://events;

-- Multiple parallel consumers
CREATE STREAM fraud_detection AS
SELECT * FROM memory://events WHERE suspicious = true
INTO kafka_alerts;

CREATE STREAM analytics AS
SELECT * FROM memory://events
GROUP BY category
INTO clickhouse_stats;
```

**3. Testing and Development**
```sql
-- No external dependencies for tests
CREATE STREAM test_pipeline AS
SELECT * FROM memory://test_input
WHERE amount > 100
INTO memory://test_output;
```

**4. Stream-Table Join Optimization**
```sql
-- Materialize frequently-joined reference data in memory
CREATE TABLE customer_lookup AS
SELECT * FROM kafka_customers
INTO memory://customer_table;  -- Materialized in memory

-- Fast in-memory joins
CREATE STREAM enriched_orders AS
SELECT o.*, c.name, c.tier
FROM kafka_orders o
JOIN memory://customer_table c ON o.customer_id = c.customer_id
INTO kafka_enriched;
```

### Performance Goals

| Metric | Current (Kafka) | Target (In-Memory) | Improvement |
|--------|----------------|-------------------|-------------|
| **Latency per stage** | 10-100ms | <100µs | **100-1000x** |
| **Throughput** | 50K msg/sec | 5M msg/sec | **100x** |
| **Serialization** | Required | Zero-copy | **∞** |
| **Memory overhead** | 2x (ser+deser) | 1x (native) | **50%** |

## Design

### Memory Visibility and Concurrency Model

**Explicit Guarantees:**

The in-memory channel system is designed for **multi-threaded, MPMC (Multiple Producer Multiple Consumer) parallel execution** within a single Velostream process. Channels are NOT process-local or query-local — they are **process-global** and thread-safe.

#### Visibility Scope

| Scope | Visible | Use Case |
|-------|---------|----------|
| **Across threads** | ✅ Yes | Multiple SQL queries execute on thread pool |
| **Across async tasks** | ✅ Yes | Each query stage runs as async task |
| **Across queries** | ✅ Yes | Query 1 writes, Query 2 reads |
| **Across processes** | ❌ No (V1) | Use Kafka for multi-process |
| **Across machines** | ❌ No | Use Kafka for distributed |

#### Concurrency Semantics

```rust
// Channel is Arc-wrapped and shared across the entire process
pub struct InMemoryChannel {
    name: String,
    queue: Arc<SegQueue<Arc<StreamRecord>>>,  // Lock-free MPMC
    // ... other fields
}

// Global registry provides process-wide visibility
static CHANNEL_REGISTRY: Lazy<InMemoryChannelRegistry> = Lazy::new(|| {
    InMemoryChannelRegistry::new()
});
```

**Threading Model:**

1. **Producer threads**: Multiple queries can write to same channel concurrently
   - Lock-free push operations via `crossbeam::SegQueue`
   - No coordination required between producers
   - Each `write()` is atomic and thread-safe

2. **Consumer threads**: Multiple queries can read from same channel concurrently
   - Consumer groups provide partitioning for load balancing
   - Each consumer group sees all records (broadcast)
   - Within a consumer group, records are distributed (round-robin/hash-based)

3. **Async task isolation**: Each SQL query executes as independent async task
   - Queries run on tokio thread pool (typically N cores)
   - Channel operations are `async fn` for back-pressure
   - `Arc<StreamRecord>` allows zero-copy sharing across tasks

**Example: Parallel Execution**

```rust
// Three queries running in parallel on thread pool

// Query 1 (Thread Pool Worker 1)
tokio::spawn(async move {
    loop {
        let record = source.read().await?;
        channel.write(Arc::new(transform(record))).await?;  // ← Thread-safe write
    }
});

// Query 2 (Thread Pool Worker 2)
tokio::spawn(async move {
    loop {
        let record = channel.read(Some("group-1")).await?;  // ← Thread-safe read
        process(record);
    }
});

// Query 3 (Thread Pool Worker 3)
tokio::spawn(async move {
    loop {
        let record = channel.read(Some("group-2")).await?;  // ← Independent read
        analyze(record);
    }
});
```

**Not Intra-Query:**

Channels are **NOT** limited to single query scope. They are explicitly designed for **inter-query communication**:

```sql
-- Query 1: Writes to channel (runs on worker thread 1)
CREATE STREAM producer AS
SELECT * FROM kafka_source
INTO memory_channel;

-- Query 2: Reads from same channel (runs on worker thread 2)
CREATE STREAM consumer AS
SELECT * FROM memory_channel
WHERE amount > 100
INTO file_output;
```

Both queries execute **concurrently** on different threads, coordinating via the shared `memory_channel`.

**Safety Guarantees:**

- **Memory safety**: Rust ownership + `Arc` prevents data races
- **MPMC correctness**: Lock-free queue guarantees consistency
- **No data loss**: Bounded queue with back-pressure (not drop)
- **Ordering**: FIFO per partition (within consumer group)
- **Atomicity**: Each `write()`/`read()` is atomic operation

**When NOT to Use:**

- **Cross-process communication** → Use Kafka (network boundaries)
- **Durable storage** → Use Kafka/File (persistence required)
- **Very large buffers** → Use Kafka (disk-backed)
- **Multi-machine clusters** → Use Kafka (distributed architecture)

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Velostream Engine                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐         ┌──────────────┐                     │
│  │   Query 1    │         │   Query 2    │                     │
│  │  (Producer)  │         │  (Consumer)  │                     │
│  └──────┬───────┘         └───────▲──────┘                     │
│         │                         │                             │
│         │ write()                 │ read()                      │
│         ▼                         │                             │
│  ┌─────────────────────────────────────────────────┐           │
│  │         InMemoryChannel                         │           │
│  │  ┌───────────────────────────────────────────┐ │           │
│  │  │  Bounded MPMC Queue                       │ │           │
│  │  │  - Lock-free when possible                │ │           │
│  │  │  - Back-pressure via capacity             │ │           │
│  │  │  - Arc<StreamRecord> for zero-copy       │ │           │
│  │  └───────────────────────────────────────────┘ │           │
│  │                                                 │           │
│  │  ┌───────────────────────────────────────────┐ │           │
│  │  │  Schema Registry                          │ │           │
│  │  │  - Schema validation                      │ │           │
│  │  │  - Schema evolution tracking              │ │           │
│  │  └───────────────────────────────────────────┘ │           │
│  │                                                 │           │
│  │  ┌───────────────────────────────────────────┐ │           │
│  │  │  Metrics & Monitoring                     │ │           │
│  │  │  - Throughput tracking                    │ │           │
│  │  │  - Queue depth monitoring                 │ │           │
│  │  │  - Back-pressure detection                │ │           │
│  │  └───────────────────────────────────────────┘ │           │
│  └─────────────────────────────────────────────────┘           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. InMemoryDataSource

```rust
pub struct InMemoryDataSource {
    channel_name: String,
    channel: Arc<InMemoryChannel>,
    config: InMemorySourceConfig,
}

pub struct InMemorySourceConfig {
    /// Channel name (like Kafka topic)
    pub channel_name: String,

    /// Consumer group for load balancing (optional)
    pub group_id: Option<String>,

    /// Start from beginning or latest
    pub offset_reset: OffsetReset,

    /// Enable schema validation
    pub validate_schema: bool,
}

impl DataSource for InMemoryDataSource {
    async fn fetch_schema(&self) -> Result<Schema> {
        self.channel.get_schema().await
    }

    async fn create_reader(&self) -> Result<Box<dyn DataReader>> {
        Ok(Box::new(InMemoryReader::new(
            self.channel.clone(),
            self.config.group_id.clone(),
        )))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        false
    }
}
```

#### 2. InMemoryDataSink

```rust
pub struct InMemoryDataSink {
    channel_name: String,
    channel: Arc<InMemoryChannel>,
    config: InMemorySinkConfig,
}

pub struct InMemorySinkConfig {
    /// Channel name (like Kafka topic)
    pub channel_name: String,

    /// Maximum queue capacity (back-pressure)
    pub max_capacity: usize,

    /// Enable schema enforcement
    pub enforce_schema: bool,

    /// Overflow strategy (block, drop-oldest, error)
    pub overflow_strategy: OverflowStrategy,
}

impl DataSink for InMemoryDataSink {
    async fn validate_schema(&self, schema: &Schema) -> Result<()> {
        if self.config.enforce_schema {
            self.channel.validate_schema(schema).await
        } else {
            Ok(())
        }
    }

    async fn create_writer(&self) -> Result<Box<dyn DataWriter>> {
        Ok(Box::new(InMemoryWriter::new(
            self.channel.clone(),
            self.config.overflow_strategy,
        )))
    }

    fn supports_transactions(&self) -> bool {
        false  // V1: No transactions
    }

    fn supports_upsert(&self) -> bool {
        false  // V1: Append-only
    }
}
```

#### 3. InMemoryChannel (Core)

```rust
pub struct InMemoryChannel {
    name: String,
    queue: Arc<SegQueue<Arc<StreamRecord>>>,  // Lock-free MPMC queue
    capacity: AtomicUsize,
    current_size: AtomicUsize,
    schema: RwLock<Option<Schema>>,
    metrics: Arc<ChannelMetrics>,
    consumer_groups: RwLock<HashMap<String, Arc<ConsumerGroup>>>,
}

impl InMemoryChannel {
    /// Push record to channel (producer)
    pub async fn write(&self, record: Arc<StreamRecord>) -> Result<()> {
        // Check capacity
        let current = self.current_size.load(Ordering::Relaxed);
        let capacity = self.capacity.load(Ordering::Relaxed);

        if current >= capacity {
            // Back-pressure: wait or drop based on strategy
            self.handle_overflow().await?;
        }

        // Validate schema if enforced
        if let Some(schema) = self.schema.read().await.as_ref() {
            self.validate_record(&record, schema)?;
        }

        // Zero-copy push (Arc increment only)
        self.queue.push(record);
        self.current_size.fetch_add(1, Ordering::Relaxed);

        // Update metrics
        self.metrics.records_written.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Pop record from channel (consumer)
    pub async fn read(&self, group_id: Option<&str>) -> Result<Option<Arc<StreamRecord>>> {
        if let Some(gid) = group_id {
            // Consumer group: load balancing across consumers
            self.read_with_group(gid).await
        } else {
            // Independent consumer: gets all records
            self.read_independent().await
        }
    }

    /// Get current schema
    pub async fn get_schema(&self) -> Result<Schema> {
        self.schema.read().await
            .clone()
            .ok_or(InMemoryError::SchemaNotSet)
    }

    /// Set or update schema
    pub async fn set_schema(&self, schema: Schema) -> Result<()> {
        let mut s = self.schema.write().await;
        if let Some(existing) = s.as_ref() {
            // Validate schema compatibility
            self.check_schema_compatibility(existing, &schema)?;
        }
        *s = Some(schema);
        Ok(())
    }
}
```

#### 4. Consumer Groups

```rust
pub struct ConsumerGroup {
    group_id: String,
    consumers: RwLock<Vec<ConsumerState>>,
    partition_assignment: RwLock<HashMap<String, usize>>,  // consumer_id -> partition
}

impl ConsumerGroup {
    /// Assign consumer to partition (round-robin)
    pub async fn register_consumer(&self, consumer_id: String) -> usize {
        let mut consumers = self.consumers.write().await;
        let partition = consumers.len();
        consumers.push(ConsumerState {
            consumer_id,
            last_read: Instant::now(),
        });
        partition
    }

    /// Check if record belongs to this consumer's partition
    pub fn should_consume(&self, consumer_id: &str, record: &StreamRecord) -> bool {
        // Hash key to partition
        let partition = self.hash_key(record.key.as_ref()) % self.num_partitions();

        // Check if this consumer owns this partition
        self.partition_assignment.read()
            .get(consumer_id)
            .map(|&p| p == partition)
            .unwrap_or(false)
    }
}
```

### SQL Integration

#### Configuration Syntax

```sql
-- Basic in-memory channel
CREATE STREAM filtered_data AS
SELECT * FROM kafka_source WHERE valid = true
INTO memory_filtered
WITH (
    'memory_filtered.config_file' = 'config/memory_sink.yaml',
    'memory_filtered.channel_name' = 'validated_orders',
    'memory_filtered.max_capacity' = '10000'
);

-- Consume from in-memory channel
CREATE STREAM aggregated AS
SELECT customer_id, COUNT(*) as count
FROM memory_filtered
GROUP BY customer_id
INTO file_results
WITH (
    'memory_filtered.config_file' = 'config/memory_source.yaml',
    'memory_filtered.channel_name' = 'validated_orders',
    'memory_filtered.group_id' = 'aggregator-1'
);
```

#### Configuration Files

```yaml
# config/memory_source.yaml
type: memory_source
offset_reset: earliest  # earliest | latest
validate_schema: true

# config/memory_sink.yaml
type: memory_sink
max_capacity: 10000
enforce_schema: true
overflow_strategy: block  # block | drop_oldest | error
```

### Channel Registry

```rust
pub struct InMemoryChannelRegistry {
    channels: RwLock<HashMap<String, Arc<InMemoryChannel>>>,
}

impl InMemoryChannelRegistry {
    /// Get or create channel
    pub async fn get_or_create(&self, name: &str, config: &InMemoryConfig) -> Arc<InMemoryChannel> {
        let mut channels = self.channels.write().await;

        if let Some(channel) = channels.get(name) {
            channel.clone()
        } else {
            let channel = Arc::new(InMemoryChannel::new(name, config));
            channels.insert(name.to_string(), channel.clone());
            channel
        }
    }

    /// List all channels
    pub async fn list(&self) -> Vec<String> {
        self.channels.read().await.keys().cloned().collect()
    }

    /// Get channel metrics
    pub async fn get_metrics(&self, name: &str) -> Option<ChannelMetrics> {
        self.channels.read().await
            .get(name)
            .map(|c| c.metrics.snapshot())
    }
}
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1-2)

**Goals**: Basic in-memory channel with single producer/consumer

**Tasks**:
- [ ] Create `InMemoryChannel` with `crossbeam::queue::SegQueue`
- [ ] Implement `InMemoryDataSource` trait
- [ ] Implement `InMemoryDataSink` trait
- [ ] Add `InMemoryReader` and `InMemoryWriter`
- [ ] Basic schema validation
- [ ] Unit tests for core components

**Success Criteria**:
- Can write records to channel
- Can read records from channel
- Zero-copy (Arc-based) operation verified
- Basic throughput: >1M records/sec

### Phase 2: Consumer Groups & Load Balancing (Week 3)

**Goals**: Multiple consumers with load balancing

**Tasks**:
- [ ] Implement `ConsumerGroup` logic
- [ ] Add partition assignment (hash-based)
- [ ] Consumer registration and heartbeat
- [ ] Rebalancing on consumer join/leave
- [ ] Integration tests with multiple consumers

**Success Criteria**:
- Multiple consumers can read from same channel
- Load is balanced across consumers
- No duplicate reads within consumer group
- Different groups see all records

### Phase 3: Back-Pressure & Flow Control (Week 4)

**Goals**: Prevent memory exhaustion with bounded queues

**Tasks**:
- [ ] Implement capacity limits
- [ ] Add overflow strategies (block, drop-oldest, error)
- [ ] Back-pressure metrics and monitoring
- [ ] Adaptive capacity scaling (optional)
- [ ] Stress tests with slow consumers

**Success Criteria**:
- Queue depth stays within configured limits
- Slow consumers don't crash system
- Back-pressure signals visible in metrics
- Graceful degradation under load

### Phase 4: SQL Integration (Week 5)

**Goals**: Full SQL syntax support

**Tasks**:
- [ ] Add `memory://` URI parser
- [ ] Configuration system integration
- [ ] Factory registration
- [ ] Multi-stage pipeline support
- [ ] Documentation and examples

**Success Criteria**:
- Can use `memory://` in FROM and INTO clauses
- Configuration via YAML files
- Multi-stage pipelines work end-to-end
- Examples run successfully

### Phase 5: Observability & Production Readiness (Week 6)

**Goals**: Metrics, monitoring, and debugging tools

**Tasks**:
- [ ] Prometheus metrics export
- [ ] CLI commands for channel inspection
- [ ] Health checks
- [ ] Memory usage tracking
- [ ] Performance benchmarks vs Kafka

**Success Criteria**:
- Metrics available in Prometheus
- CLI can list/inspect channels
- Memory usage predictable and bounded
- Performance meets goals (>1M msg/sec)

## Performance Considerations

### Zero-Copy Architecture

```rust
// Producer side (zero serialization)
let record = Arc::new(StreamRecord {
    key: Some("customer-123".into()),
    value: FieldValue::Integer(1000),
    timestamp: Utc::now(),
    metadata: RecordMetadata::default(),
});

// Write: only Arc clone (pointer copy)
channel.write(record.clone()).await?;  // ~10ns

// Consumer side (zero deserialization)
let record = channel.read(Some("group-1")).await?;  // ~50ns
// Already in native FieldValue format!
process_record(&record);  // Direct access
```

### Memory Management

**Bounded Queue with Capacity**:
```rust
pub struct InMemoryChannel {
    queue: Arc<SegQueue<Arc<StreamRecord>>>,
    capacity: AtomicUsize,  // Maximum records
    current_size: AtomicUsize,  // Current count
}
```

**Memory Limits**:
- Default: 10,000 records per channel
- Configurable per channel
- Automatic overflow handling
- Memory usage = `records * avg_record_size`

**Example**:
- 10K records × 1KB/record = 10MB per channel
- 100 channels = 1GB total (bounded)

### Lock-Free Operations

Use `crossbeam::queue::SegQueue` for MPMC:
- Lock-free push/pop when possible
- Scales with CPU cores
- No lock contention bottleneck

**Benchmarks** (expected):
- Single producer, single consumer: 10M ops/sec
- Multiple producers, multiple consumers: 5M ops/sec
- Latency: p50=50ns, p99=500ns

## Alternatives Considered

### Alternative 1: tokio::mpsc Channels

**Pros**:
- Built-in to tokio ecosystem
- Well-tested and stable
- Good async integration

**Cons**:
- Not MPMC (only MPSC)
- Requires multiple channels for fan-out
- Less performant than lock-free queues

**Decision**: Use crossbeam for better MPMC support

### Alternative 2: Shared Memory (shmem)

**Pros**:
- True zero-copy across processes
- Could support multi-process Velostream

**Cons**:
- Complex lifecycle management
- Platform-specific
- Over-engineered for in-process use case

**Decision**: Stick with in-process for V1

### Alternative 3: Apache Arrow Buffers

**Pros**:
- Columnar format for analytics
- Standard format
- Good for batch operations

**Cons**:
- Requires conversion from row format
- Overhead for streaming (record-at-a-time)
- Additional dependency

**Decision**: Keep FieldValue format, consider Arrow for V2

## Prior Art

### Flink In-Memory Channels
- Uses JVM buffers between operators
- Network-based for distributed
- Heavy on serialization

**Velostream Improvement**: Zero-copy with Arc, lighter weight

### Materialize Differential Dataflow
- In-memory computation
- Uses timely dataflow operators
- Complex coordination

**Velostream Improvement**: Simpler channel abstraction

### RisingWave Streaming
- PostgreSQL-compatible
- Uses shared buffers
- MVCC for consistency

**Velostream Improvement**: Simpler, no MVCC overhead

### Arroyo State Backend
- In-memory state for operators
- Parquet-based checkpointing

**Velostream Improvement**: Focus on channels not state

## Unresolved Questions

### 1. Persistence Strategy

**Question**: Should in-memory channels support optional persistence?

**Options**:
- A) No persistence (pure in-memory)
- B) Optional spill-to-disk when full
- C) WAL-based durability (like Kafka)

**Recommendation**: Start with (A), add (B) if needed for large buffers

### 2. Cross-Process Support

**Question**: Support channels across multiple Velostream processes on same host?

**Options**:

**A) Single process only (V1 - Current)**
- Simplest implementation
- Lock-free MPMC in-process
- Zero serialization overhead
- Sufficient for most use cases

**B) Unix Domain Sockets (V2 - Recommended for single-host)**

Extend channel transport to support local IPC:

```rust
pub enum ChannelTransport {
    InProcess(Arc<SegQueue<Arc<StreamRecord>>>),
    UnixSocket(UnixStream),  // Same host, different processes
}

impl InMemoryChannel {
    pub async fn new(name: &str, transport: ChannelTransport) -> Self {
        match transport {
            ChannelTransport::InProcess(queue) => {
                // Zero-copy, lock-free (current)
                Self::new_in_process(name, queue)
            }
            ChannelTransport::UnixSocket(stream) => {
                // Fast serialization via Unix socket
                Self::new_unix_socket(name, stream)
            }
        }
    }
}
```

**Pros**:
- ✅ Standard POSIX (works everywhere)
- ✅ Easy cleanup (filesystem-based)
- ✅ Portable across Unix-like systems
- ✅ Still very fast: ~1µs latency (vs 10ms for TCP)
- ✅ Simpler than shared memory
- ✅ Automatic process isolation and security

**Cons**:
- ❌ Requires serialization (no zero-copy across processes)
- ❌ Not as fast as in-process (1µs vs 50ns)
- ❌ Limited to single host
- ❌ Need socket path management

**C) Shared Memory (Complex)**
- True zero-copy across processes
- Platform-specific APIs (not portable)
- Complex lifecycle management
- Overkill for most use cases

**D) Use Kafka for cross-process**
- Best for multi-host distributed
- Already implemented
- Adds network overhead
- Overkill for single-host

**Recommendation**:

**V1 (Current)**: Option A - Single process only
- Covers 80% of use cases
- Simplest, fastest implementation
- Focus on lock-free in-process first

**V2 (Future)**: Option B - Add Unix Domain Sockets for single-host cross-process
- When multiple Velostream processes on same host need low-latency IPC
- Use case: Process isolation for security/sandboxing
- Still 100x faster than Kafka (1µs vs 10-100ms)
- Standard and portable

**V3+ (Distributed)**: Option D - Use Kafka for multi-host
- When processes are on different machines
- Need durability and replication
- Can fall back to existing Kafka infrastructure

**Implementation Path**:

```rust
// V1: In-process only
let channel = InMemoryChannel::new("my-channel",
    ChannelTransport::InProcess(queue)
);

// V2: Unix socket for cross-process (same host)
let channel = InMemoryChannel::new("my-channel",
    ChannelTransport::UnixSocket("/tmp/velostream/my-channel.sock")
);

// V3: Kafka for distributed
let channel = KafkaChannel::new("my-channel", "kafka://broker:9092/topic");
```

**Configuration**:

```yaml
# In-process (V1)
memory_channel:
  type: memory
  transport: in_process
  capacity: 10000

# Unix socket (V2)
memory_channel:
  type: memory
  transport: unix_socket
  socket_path: /tmp/velostream/channels/my-channel.sock
  capacity: 10000
  serialization: messagepack  # Fast binary format

# Kafka (V3)
kafka_channel:
  type: kafka
  brokers: localhost:9092
  topic: my-channel
```

### 3. Schema Evolution

**Question**: How to handle schema changes in running channels?

**Options**:
- A) Schema immutable after first write
- B) Schema can evolve with compatibility checks
- C) Multiple schema versions supported

**Recommendation**: (A) for V1, (B) for V2 with Avro-style evolution

### 4. Cleanup Strategy

**Question**: When to clean up unused channels?

**Options**:
- A) Manual cleanup via API
- B) TTL-based (expire after no activity)
- C) Reference counting (when no readers/writers)
- D) Explicit DROP CHANNEL command

**Recommendation**: (C) for automatic, (D) for explicit control

### 5. Ordering Guarantees

**Question**: What ordering guarantees across consumer groups?

**Options**:
- A) No ordering (like Kafka with multiple partitions)
- B) FIFO per key (hash-based partitioning)
- C) Total ordering (single partition)

**Recommendation**: (B) with configurable partition count

## Success Metrics

### Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Throughput** | >5M records/sec | Single producer, single consumer |
| **Latency (p50)** | <100ns | Time between write() and read() |
| **Latency (p99)** | <1µs | 99th percentile |
| **Memory per record** | <100 bytes | Overhead beyond data |
| **CPU overhead** | <5% | Compared to Kafka-based pipeline |

### Functional Goals

- [ ] Zero-copy operation (Arc-based)
- [ ] MPMC support (multiple producers/consumers)
- [ ] Consumer group load balancing
- [ ] Back-pressure handling
- [ ] Schema validation
- [ ] Prometheus metrics
- [ ] CLI inspection tools
- [ ] Complete SQL integration

## Documentation Requirements

### User Documentation

- [ ] SQL syntax guide for `memory://` URIs
- [ ] Configuration reference
- [ ] Multi-stage pipeline patterns
- [ ] Performance tuning guide
- [ ] Troubleshooting common issues

### Developer Documentation

- [ ] Architecture design doc
- [ ] API reference for `InMemoryChannel`
- [ ] Testing guide
- [ ] Benchmark methodology
- [ ] Contributing guidelines

### Examples

- [ ] Basic producer/consumer
- [ ] Multi-stage pipeline
- [ ] Consumer groups
- [ ] Fan-out pattern
- [ ] Performance testing

## Testing Strategy

### Unit Tests

- Channel creation and lifecycle
- Write/read operations
- Schema validation
- Consumer group logic
- Back-pressure handling
- Overflow strategies

### Integration Tests

- Multi-stage SQL pipelines
- Multiple producers/consumers
- Consumer group rebalancing
- Schema evolution scenarios
- Memory leak detection

### Performance Tests

- Throughput benchmarks
- Latency measurements
- Memory usage profiling
- CPU utilization
- Comparison vs Kafka

### Chaos Tests

- Consumer failures
- Producer failures
- Memory pressure
- Slow consumers
- Schema conflicts

## Migration Path

### For Existing Users

**No breaking changes** - in-memory channels are opt-in:

```sql
-- Old: External Kafka
CREATE STREAM filtered AS
SELECT * FROM kafka_source
INTO kafka_filtered;

-- New: In-memory (drop-in replacement)
CREATE STREAM filtered AS
SELECT * FROM kafka_source
INTO memory_filtered
WITH ('memory_filtered.channel_name' = 'filtered_orders');
```

### Gradual Adoption

1. **Test environment**: Use memory channels for dev/test
2. **Staging**: Use for intermediate stages in production
3. **Production**: Use for performance-critical paths

## References

- [Crossbeam Queue Documentation](https://docs.rs/crossbeam-queue/)
- [Flink Network Stack](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/flink-architecture/)
- [Materialize Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow)
- [RisingWave Architecture](https://github.com/risingwavelabs/risingwave)
- [Arroyo State Backend](https://github.com/ArroyoSystems/arroyo)

## Appendix A: Complete Example

```sql
-- Multi-stage pipeline with in-memory channels

-- Stage 1: Filter and validate
CREATE STREAM validated_orders AS
SELECT
    order_id,
    customer_id,
    amount,
    order_time
FROM kafka_raw_orders
WHERE amount > 0 AND customer_id IS NOT NULL
INTO memory_validated
WITH (
    'memory_validated.config_file' = 'config/memory_sink.yaml',
    'memory_validated.channel_name' = 'validated',
    'memory_validated.max_capacity' = '50000'
);

-- Stage 2: Enrich with customer data
CREATE STREAM enriched_orders AS
SELECT
    v.order_id,
    v.customer_id,
    v.amount,
    c.name,
    c.tier
FROM memory_validated v
JOIN customer_table c ON v.customer_id = c.customer_id
INTO memory_enriched
WITH (
    'memory_validated.channel_name' = 'validated',
    'memory_enriched.channel_name' = 'enriched',
    'memory_enriched.max_capacity' = '50000'
);

-- Stage 3: Aggregate by customer tier
CREATE STREAM tier_aggregates AS
SELECT
    tier,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    window_start
FROM memory_enriched
WINDOW TUMBLING(1h)
GROUP BY tier, window_start
INTO memory_aggregates
WITH (
    'memory_enriched.channel_name' = 'enriched',
    'memory_aggregates.channel_name' = 'aggregates'
);

-- Stage 4: Export to external systems
CREATE STREAM exports AS
SELECT * FROM memory_aggregates
INTO kafka_tier_stats
WITH (
    'memory_aggregates.channel_name' = 'aggregates',
    'kafka_tier_stats.config_file' = 'config/kafka_sink.yaml'
);
```

**Performance**: 3 intermediate stages, ~100µs total latency vs 30-300ms with Kafka

---

**End of RFC**
