# FR-072: IPC Channel Architecture for Low-Latency Stream Processing

## Table of Contents

1. [Summary](#summary)
2. [Problem Statement](#problem-statement)
3. [Use Cases](#use-cases)
    - [Real-Time Feature Engineering for Trading](#1-real-time-feature-engineering-for-trading)
    - [Multi-Stage Pipelines with Partitioning](#2-multi-stage-pipelines-with-partitioning)
    - [Python-First Analytics Pipeline](#3-python-first-analytics-pipeline)
4. [Performance Goals](#performance-goals)
5. [Platform Support and Windows Compatibility](#platform-support-and-windows-compatibility)
6. [Alternatives Considered](#alternatives-considered)
    - [Why Not Shared Memory Libraries?](#why-not-shared-memory-libraries)
    - [Why Not gRPC?](#why-not-grpc)
    - [Why Not ZeroMQ?](#why-not-zeromq)
7. [Security and Privacy Considerations](#security-and-privacy-considerations)
8. [Testing Strategy](#testing-strategy)
9. [Error Handling](#error-handling)
10. [Metrics and Monitoring](#metrics-and-monitoring)
11. [Design](#design)
    - [Architecture Overview](#architecture-overview-hybrid-transport-model)
    - [Memory Visibility and Concurrency Model](#memory-visibility-and-concurrency-model)
    - [Unix Socket Backpressure and Flow Control](#unix-socket-backpressure-and-flow-control)
    - [Socket Management and Lifecycle](#socket-management-and-lifecycle)
    - [Partition-Group Assignment Protocol](#partition-group-assignment-protocol-configuration-based)
    - [Core Components](#core-components)
12. [Dependency Management and Connection Resilience](#dependency-management-and-connection-resilience)
13. [Memory Safety and Resource Limits](#memory-safety-and-resource-limits)
14. [Simplified Implementation Strategies](#simplified-implementation-strategies)
15. [SQL Integration](#sql-integration)
16. [Python Client Integration](#python-client-integration)
17. [External Storage Integration](#external-storage-integration)
18. [Performance Considerations](#performance-considerations)
19. [Competitive Positioning](#competitive-positioning)
20. [Implementation Plan](#implementation-plan)
21. [Success Metrics](#success-metrics)
22. [Documentation Requirements](#documentation-requirements)
23. [Appendix: Complete End-to-End Example](#appendix-complete-end-to-end-example)
24. [References](#references)

---

## Summary

Velostream introduces IPC-based streaming channels — a zero-copy (in-process) or low-latency serialized (cross-process) communication layer that connects SQL pipelines and Python processes on a single machine, bridging durable storage systems (Kafka, S3) with microsecond-latency computation.

Unlike Kafka, which externalizes every stream boundary with 10-100ms network overhead, or Flink, which compiles monolithic DAGs with complex coordination, Velostream's design allows independently defined SQL queries and Python scripts to communicate through lightweight channels with automatic transport selection.

### Architecture Pattern: Durable Edges, Fast Middle

```
Kafka (durable)  ──10ms──►  Velostream IPC  ──<100µs──►  Velostream IPC  ──10ms──►  Kafka (durable)
   (external)              (Stage 1: in-process)        (Stage 2: in-process)      (external)
                                   or                           or
                          (Python via socket: 5-10µs)   (Python via socket: 5-10µs)
```

By combining the durability of Kafka/S3 at the boundaries with sub-microsecond latency for in-process stages and sub-10µs latency for cross-process stages, Velostream enables:

- **Real-time ML feature engineering** - Python in hot path: <10µs vs 1-10ms with network calls
- **Multi-stage streaming pipelines** - 3-10x lower latency than Flink
- **Zero-copy in-process** - Arc cloning: ~50ns between SQL stages
- **Fast cross-process** - Unix sockets + Arrow IPC: ~5µs to Python
- **Regulatory compliance** - Durable audit trail at edges, fast processing in middle

This makes it uniquely suited for mid-frequency quantitative finance, where:

- Sub-100µs processing enables real-time alpha signals
- Python flexibility enables rapid strategy iteration
- Kafka/S3 durability satisfies compliance requirements
- 40-50% TCO savings vs Kafka + Flink stacks

## Problem Statement

Current Velostream deployments require external systems (Kafka, Files) to link query stages together, incurring unnecessary latency:

```sql
-- Stage 1: Filter high-value orders (writes to Kafka)
CREATE STREAM filtered_orders AS
SELECT * FROM kafka_orders_source
WHERE amount > 1000
INTO kafka_filtered_sink;  -- 10ms write latency

-- Stage 2: Python feature engineering (reads from Kafka)
-- Python process must consume from Kafka: 10ms read latency
-- Total: 20ms+ just for data transport between stages

-- Stage 3: Aggregate (reads from Kafka again)
CREATE STREAM order_stats AS
SELECT customer_id, SUM(amount) as total
FROM kafka_filtered_source  -- Another 10ms read
GROUP BY customer_id
INTO file_stats_sink;
```

### Issues with Current Approach

1. **Latency overhead** - 10-100ms per stage (Kafka serialization + network)
2. **Resource waste** - Serialize data only to immediately deserialize it
3. **Complexity** - Must manage external topics for transient intermediate data
4. **Cost** - Kafka cluster + engineering overhead ($500K-1M/year)
5. **Python integration** - Network calls add 1-10ms overhead, preventing real-time ML

**The gap:** No good solution for low-latency multi-stage processing with Python in the hot path that preserves durability at the edges.

## Use Cases

### 1. Real-Time Feature Engineering for Trading

```sql
-- Durable input: Kafka (replayable for compliance)
CREATE STREAM market_data AS
SELECT * FROM kafka://market_feed
INTO ipc://hot_market_data;  -- Move to memory (~10ms initial read)

-- Stage 1: SQL preprocessing (in-process: <100µs)
CREATE STREAM normalized AS
SELECT symbol, price, volume, timestamp
FROM ipc://hot_market_data
WHERE price > 0 AND volume > 0
INTO ipc://validated;

-- Stage 2: Python feature engineering (cross-process: <10µs via Unix socket)
-- Python process:
--   reader = IPCReader("ipc://validated")  
--   for tick in reader:  # ~5µs per read
--       features = compute_ewma_volatility(tick)
--       writer.write("ipc://features", features)  # ~5µs write

-- Stage 3: SQL aggregation (in-process: <100µs)
CREATE STREAM signals AS
SELECT 
    symbol,
    AVG(feature_volatility) OVER (RANGE '1m') as avg_vol,
    SUM(feature_ofi) as total_ofi
FROM ipc://features
WINDOW TUMBLING(100ms)
WHERE regime = 'high_volatility'
INTO ipc://trading_signals;

-- Durable output: Kafka (for audit trail)
CREATE STREAM output AS
SELECT * FROM ipc://trading_signals
INTO kafka://signals_topic,      -- Real-time consumers
     s3://compliance/signals/;    -- Long-term archive
```

**Latency breakdown:**

- Kafka read (once): 10ms
- IPC stage 1 (SQL): 50µs
- Python processing (cross-process): 20µs (read 5µs + compute 10µs + write 5µs)
- IPC stage 2 (SQL): 50µs
- Kafka write (once): 10ms
- **Total: ~20ms end-to-end**

**vs Current (Kafka between every stage):**

- Stage 1 → Kafka: 10ms
- Kafka → Python: 10ms
- Python → Kafka: 10ms
- Kafka → Stage 3: 10ms
- Stage 3 → Kafka: 10ms
- **Total: ~50ms**

**Improvement: 2.5x faster, with same durability guarantees**

### 2. Multi-Stage Pipelines with Partitioning

```sql
-- Producer: Write to partitioned IPC channel
CREATE STREAM filtered_orders AS
SELECT order_id, customer_id, amount, timestamp
FROM kafka://orders_topic
WHERE amount > 100
INTO ipc://high_value_orders
WITH (
    'ipc://high_value_orders.partitions' = '4',
    'ipc://high_value_orders.partition_key' = 'customer_id',
    'ipc://high_value_orders.max_capacity' = '10000'
);

-- Consumer 1: Reads partitions 0,1 (via consumer group)
CREATE STREAM aggregated_1 AS
SELECT customer_id, SUM(amount) as total_amount
FROM ipc://high_value_orders
GROUP BY customer_id
INTO kafka://customer_stats
WITH (
    'ipc://high_value_orders.group_id' = 'aggregators',
    'ipc://high_value_orders.consumer_id' = 'aggregator-1'
);

-- Consumer 2: Reads partitions 2,3 (same group, automatic load balancing)
CREATE STREAM aggregated_2 AS
SELECT customer_id, SUM(amount) as total_amount
FROM ipc://high_value_orders
GROUP BY customer_id
INTO kafka://customer_stats
WITH (
    'ipc://high_value_orders.group_id' = 'aggregators',
    'ipc://high_value_orders.consumer_id' = 'aggregator-2'
);

-- Consumer 3: Different group (sees ALL data for fraud detection)
CREATE STREAM fraud_detection AS
SELECT order_id, customer_id, amount
FROM ipc://high_value_orders
WHERE fraud_score(amount, customer_id) > 0.8
INTO kafka://fraud_alerts
WITH (
    'ipc://high_value_orders.group_id' = 'fraud_detectors'
);
```

### 3. Python-First Analytics Pipeline

```python
from velostream import IPCReader, IPCWriter
import pandas as pd
import pyarrow as pa

# Read from IPC channel (Velostream SQL writes here)
reader = IPCReader("ipc://raw_events", group_id="python_processors")

# Batch processing with zero-copy Arrow
arrow_table = reader.read_batch(1000)  # ~1ms for 1000 rows
df = arrow_table.to_pandas(zero_copy_only=True)  # Zero-copy!

# Complex Python analytics
features = compute_complex_features(df)
predictions = ml_model.predict(features)

# Write back to Velostream
writer = IPCWriter("ipc://predictions")
writer.write_batch(predictions)  # ~1ms via Arrow IPC
```

```sql
-- Velostream SQL consumes and routes to Kafka
CREATE STREAM output AS
SELECT * FROM ipc://predictions
WHERE score > 0.8
INTO kafka://high_confidence_predictions;
```

## Performance Goals

| Metric | Current (Kafka) | Target (IPC) | Improvement |
|--------|----------------|--------------|-------------|
| In-process latency | 10-100ms | <100µs | 100-1000x |
| Cross-process latency (Python) | 1-10ms | <10µs | 100-1000x |
| End-to-end (4 stages) | 50-200ms | 20-50ms | 3-5x |
| Throughput (in-process) | 50K msg/sec | 5M msg/sec | 100x |
| Throughput (cross-process) | 50K msg/sec | 1-2M msg/sec | 20-40x |
| Serialization | Required | Zero-copy (in-process) | ∞ |
| Python integration | Network overhead | Native IPC | 100x faster |

## Platform Support and Windows Compatibility

### Current Design: Unix/Linux/macOS Only

**The IPC channel architecture as designed requires Unix domain sockets, which means:**

✅ **Supported platforms:**
- Linux (production target)
- macOS (development)
- WSL2 on Windows (Windows Subsystem for Linux)

❌ **Not supported:**
- Native Windows (Windows 10/11 without WSL)

### Why Windows Doesn't Work

**Unix domain sockets** are the foundation of the cross-process transport:
```rust
// This works on Unix/Linux/macOS
let socket_path = PathBuf::from("/tmp/velostream/channel.sock");
let listener = UnixListener::bind(socket_path)?; // ❌ Fails on Windows
```

**Windows alternatives have different APIs:**
- Named Pipes (different API, more complex)
- Memory-mapped files (no built-in signaling)
- TCP localhost (higher latency, more overhead)

### Options for Windows Support

#### Option 1: TCP Localhost Fallback (Simplest)

Use TCP sockets on `127.0.0.1` as a fallback transport on Windows:

```rust
pub enum CrossProcessTransport {
    UnixSocket(UnixStream),      // Unix/Linux/macOS
    TcpLocalhost(TcpStream),     // Windows fallback
}

#[cfg(unix)]
pub fn create_ipc_transport(channel_name: &str) -> Result<CrossProcessTransport> {
    let socket_path = format!("/tmp/velostream/{}.sock", channel_name);
    let stream = UnixStream::connect(socket_path)?;
    Ok(CrossProcessTransport::UnixSocket(stream))
}

#[cfg(windows)]
pub fn create_ipc_transport(channel_name: &str) -> Result<CrossProcessTransport> {
    // Use localhost TCP on Windows
    let port = hash_channel_name_to_port(channel_name); // e.g., 50000 + hash
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port))?;
    Ok(CrossProcessTransport::TcpLocalhost(stream))
}
```

**Pros:**
- Simple to implement (just swap transport)
- Same serialization (Arrow IPC) works
- Minimal code changes

**Cons:**
- Higher latency on Windows: ~50-100µs vs <10µs on Unix
- TCP overhead (more kernel work)
- Port management complexity (need to allocate ports dynamically)

**Performance impact:**
- Unix socket: <10µs latency
- TCP localhost: 50-100µs latency
- **5-10x slower on Windows**

#### Option 2: Windows Named Pipes (Better Performance)

Use Named Pipes on Windows for better performance:

```rust
#[cfg(windows)]
pub fn create_ipc_transport(channel_name: &str) -> Result<CrossProcessTransport> {
    use std::os::windows::io::AsRawHandle;
    
    let pipe_name = format!(r"\\.\pipe\velostream_{}", channel_name);
    
    // Windows Named Pipe API
    let pipe = ClientOptions::new()
        .open(pipe_name)?;
    
    Ok(CrossProcessTransport::WindowsNamedPipe(pipe))
}
```

**Pros:**
- Better performance than TCP: ~20-30µs latency
- Native Windows IPC mechanism
- No port management needed

**Cons:**
- Different API (need Windows-specific code)
- More complex implementation (~500 lines)
- Testing requires Windows machines

**Performance impact:**
- Unix socket: <10µs latency
- Named Pipes: 20-30µs latency
- **2-3x slower on Windows**

#### Option 3: Shared Memory (Best Performance, Most Complex)

Use memory-mapped files with synchronization primitives:

```rust
pub struct SharedMemoryChannel {
    mapping: MemoryMappedFile,
    semaphore: Semaphore,  // For signaling
    read_offset: AtomicUsize,
    write_offset: AtomicUsize,
}
```

**Pros:**
- Cross-platform (works on Unix and Windows)
- Best performance: <5µs latency
- True zero-copy

**Cons:**
- Very complex implementation (~2000 lines)
- Need custom synchronization protocol
- Hard to debug
- Memory management complexity

#### Option 4: Windows Not Supported (Simplest for Phase 1)

**Recommendation:** Explicitly don't support Windows initially.

**Rationale:**
1. **Target market is Linux** - Production streaming workloads run on Linux
2. **Development on macOS works** - Developers can use macOS or WSL2
3. **Reduces complexity** - Focus on getting Unix implementation right first
4. **Can add later** - Windows support can be Phase 4 or 5

**Documentation:**
```markdown
## Platform Support

**Supported:**
- ✅ Linux (production)
- ✅ macOS (development)
- ✅ WSL2 on Windows (development)

**Not Supported:**
- ❌ Native Windows (use WSL2 or run on Linux)

For Windows users, we recommend:
1. Use WSL2 (Windows Subsystem for Linux)
2. Deploy to Linux for production
3. Use Kafka for cross-machine communication
```

### Recommended Approach

**Phase 1-3: Unix only**
- Focus on Linux/macOS
- Document Windows limitation
- Suggest WSL2 for Windows users

**Phase 4: Add Windows support if needed**
- Start with TCP localhost fallback (simplest)
- Upgrade to Named Pipes if performance matters
- Only implement if customers ask for it

**Code structure to enable future Windows support:**

```rust
// Abstract transport trait
pub trait IPCTransport: Send + Sync {
    async fn write(&mut self, data: &[u8]) -> Result<()>;
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn set_timeout(&mut self, timeout: Duration) -> Result<()>;
}

// Unix implementation
#[cfg(unix)]
pub struct UnixSocketTransport {
    stream: UnixStream,
}

// Future Windows implementation
#[cfg(windows)]
pub struct WindowsTransport {
    inner: WindowsTransportImpl,
}

#[cfg(windows)]
enum WindowsTransportImpl {
    TcpLocalhost(TcpStream),
    NamedPipe(NamedPipe),
}

// Factory function
pub fn create_transport(channel_name: &str) -> Result<Box<dyn IPCTransport>> {
    #[cfg(unix)]
    return Ok(Box::new(UnixSocketTransport::new(channel_name)?));
    
    #[cfg(windows)]
    return Ok(Box::new(WindowsTransport::new(channel_name)?));
}
```

### WSL2 as Windows Solution

**WSL2 provides full Unix compatibility:**
- Full Linux kernel (not emulation)
- Unix domain sockets work natively
- Near-native performance
- Easy setup for developers

**Installation:**
```powershell
# Install WSL2
wsl --install

# Install Velostream in WSL2
wsl
sudo apt update
cargo install velostream
```

### Performance Comparison

| Platform | Transport | Latency | Throughput | Complexity |
|----------|-----------|---------|------------|------------|
| Linux | Unix socket | <10µs | 1-2M msg/sec | Low |
| macOS | Unix socket | <10µs | 1-2M msg/sec | Low |
| WSL2 | Unix socket | <15µs | 1-2M msg/sec | Low |
| Windows | TCP localhost | 50-100µs | 200K msg/sec | Medium |
| Windows | Named Pipes | 20-30µs | 500K msg/sec | High |
| Windows | Shared Memory | <5µs | 2M msg/sec | Very High |

### Decision Matrix

**Should you add Windows support?**

| Factor | Unix Only | + TCP Fallback | + Named Pipes |
|--------|-----------|----------------|---------------|
| Development time | 0 weeks | +2 weeks | +4 weeks |
| Code complexity | Low | Medium | High |
| Windows performance | N/A | Poor | Good |
| Maintenance burden | Low | Medium | High |
| Customer value | Medium | High | High |

**Recommendation:** Start Unix-only, add Windows support in Phase 4+ based on customer demand.

### Summary: Windows Support Strategy

**Phase 1 (Now): Unix only**
- Document limitation clearly
- Recommend WSL2 for Windows developers
- Focus on getting Unix implementation right

**Phase 2-3: Monitor demand**
- Track customer requests for Windows
- Evaluate competitive landscape
- Assess market need

**Phase 4: Add Windows if justified**
- Start with TCP localhost (2 weeks)
- Upgrade to Named Pipes if performance matters (4 weeks)
- Document performance differences

**Key insight:** IPC is inherently local/single-machine, and production streaming workloads overwhelmingly run on Linux. Windows support is "nice to have use WSL2"

### Architecture Overview: Hybrid Transport Model

**Core principle:** Automatic transport selection based on process boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                    External Durable Storage                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                │
│  │   Kafka    │  │     S3     │  │  QuestDB   │                │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘                │
└────────┼───────────────┼───────────────┼─────────────────────────┘
         │ 10ms          │ 10ms          │ 1-10ms
         ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│              Velostream Process (Rust Engine)                    │
│                                                                  │
│  ┌──────────────┐   ipc://   ┌──────────────┐   ipc://         │
│  │  SQL Query 1 │ ─────────► │  SQL Query 2 │ ─────────►       │
│  │  (FROM kafka)│   ~50ns    │              │   ~50ns          │
│  └──────────────┘   Arc clone└──────────────┘   Arc clone      │
│         │                            │                           │
│         │ ipc:// (Unix socket)      │ ipc:// (Unix socket)     │
│         │ ~5µs (serialize)           │ ~5µs (serialize)         │
│         ▼                            ▼                           │
│  ┌──────────────────────────────────────────────────────┐       │
│  │           IPC Channel Registry                       │       │
│  │  ┌────────────────────────────────────────────────┐ │       │
│  │  │  In-Process Transport (Same Process)           │ │       │
│  │  │  - Arc<SegQueue<Arc<StreamRecord>>>            │ │       │
│  │  │  - ~50ns Arc clone (pointer copy)              │ │       │
│  │  │  - Zero serialization                          │ │       │
│  │  │  - Partitioned: 4-16 partitions per channel   │ │       │
│  │  └────────────────────────────────────────────────┘ │       │
│  │  ┌────────────────────────────────────────────────┐ │       │
│  │  │  Cross-Process Transport (Unix Socket)         │ │       │
│  │  │  - /tmp/velostream/*.sock                      │ │       │
│  │  │  - ~5µs with Arrow IPC serialization           │ │       │
│  │  │  - Zero-copy to pandas/polars in Python        │ │       │
│  │  │  - Automatic batching (1000 records)           │ │       │
│  │  └────────────────────────────────────────────────┘ │       │
│  └──────────────────────────────────────────────────────┘       │
└──────────────────────────────────┬───────────────────────────────┘
         │ ipc:// (Unix socket)    │
         │ ~5µs (Arrow IPC)         │
         ▼                          │
┌─────────────────────────────────────────────────────────────────┐
│  Python Process (Feature Engineering / ML)                       │
│                                                                  │
│  from velostream import IPCReader, IPCWriter                    │
│                                                                  │
│  reader = IPCReader("ipc://market_data")  # ~5µs per read      │
│  for tick in reader:                                            │
│      features = compute_features(tick)    # Custom logic       │
│      writer.write(features)               # ~5µs per write     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
         │
         │ Results flow back to Velostream
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    External Durable Storage                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                │
│  │   Kafka    │  │     S3     │  │ PostgreSQL │                │
│  └────────────┘  └────────────┘  └────────────┘                │
└──────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Automatic transport selection** - In-process uses Arc (50ns), cross-process uses Unix socket (5µs)
2. **IPC for hot path** - Intermediate stages use IPC (<100µs total)
3. **External storage for durability** - Kafka/S3 at edges (10ms acceptable)
4. **Standard interfaces** - Works with existing Kafka, S3, cloud storage
5. **Kafka-style partitioning** - Consumer groups, partition assignment, load balancing
6. **Pluggable serialization** - Arrow IPC (Python), MessagePack, Bincode, Protobuf

### Memory Visibility and Concurrency Model

#### Explicit Guarantees

| Scope | Transport | Latency | Use Case |
|-------|-----------|---------|----------|
| Same process, same thread | Arc (in-process) | ~50ns | SQL → SQL on same async task |
| Same process, different threads | Arc (in-process) | ~50ns | SQL → SQL on thread pool |
| Different processes, same machine | Unix socket | ~5µs | SQL ↔ Python |
| Different machines | ❌ Not supported | N/A | Use Kafka for distributed |

#### Concurrency Semantics

**Same Process (50ns latency):**

```rust
// Channel is Arc-wrapped and shared across threads
pub struct InProcessChannel {
    name: String,
    partitions: Vec<Arc<Partition>>,
    partition_count: usize,
}

pub struct Partition {
    partition_id: usize,
    queue: Arc<SegQueue<Arc<StreamRecord>>>,  // Lock-free MPMC
    capacity: AtomicUsize,
    current_size: AtomicUsize,
}

// Multiple threads can read/write concurrently
// Arc<StreamRecord> enables zero-copy sharing
```

**Cross Process (5µs latency):**

```rust
// Unix domain socket for inter-process communication
pub struct CrossProcessChannel {
    name: String,
    socket_path: PathBuf,  // /tmp/velostream/channel_name.sock
    listener: Option<UnixListener>,
    streams: Arc<RwLock<Vec<UnixStream>>>,
    serializer: Arc<dyn Serializer>,  // Arrow IPC, MessagePack, etc.
    connection_manager: Arc<ConnectionManager>,
}

pub struct ConnectionManager {
    connections: Arc<RwLock<HashMap<String, ConnectionState>>>,
    cleanup_interval: Duration,  // Default: 60s
    connection_timeout: Duration,  // Default: 300s (5 minutes)
    last_cleanup: AtomicU64,
}

pub struct ConnectionState {
    stream: UnixStream,
    consumer_id: String,
    group_id: Option<String>,
    last_heartbeat: Instant,
    assigned_partitions: Vec<usize>,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}
```

#### Threading Model

- **Producer threads:** Multiple queries/processes write concurrently
    - Same process: Lock-free SegQueue push (~50ns)
    - Cross process: Socket write with serialization (~5µs)
    - Both are thread-safe

- **Consumer threads:** Multiple queries/processes read concurrently
    - Consumer groups provide load balancing (Kafka-style)
    - Each group sees all records (broadcast)
    - Within group, records distributed by partition assignment

- **Async task isolation:** Each SQL query executes as independent async task
    - Queries run on tokio thread pool
    - Channel operations are async fn for back-pressure

#### Safety Guarantees

- **Memory safety:** Rust ownership + Arc prevents data races
- **MPMC correctness:** Lock-free queue guarantees consistency
- **No data loss:** Bounded queue with back-pressure (blocks, not drops)
- **Ordering:** FIFO per partition (within consumer group)
- **Process isolation:** Unix sockets isolate process failures

### Unix Socket Backpressure and Flow Control

Unix domain sockets provide **automatic kernel-level backpressure** through send/receive buffer management:

#### How Unix Socket Backpressure Works

1. **Kernel buffer limits** - Each Unix socket has send and receive buffers (typically 4-8KB default, configurable up to several MB)
2. **Blocking writes** - When the receiver's buffer is full, `write()` blocks until space becomes available
3. **Non-blocking detection** - With non-blocking sockets, `EAGAIN`/`EWOULDBLOCK` signals backpressure
4. **Natural flow control** - Slow consumers automatically slow down fast producers at the OS level

```rust
// Example: Unix socket write with backpressure handling
pub async fn write_to_socket(
    stream: &mut UnixStream,
    data: &[u8],
    timeout: Duration
) -> Result<()> {
    // Set write timeout to detect hung connections
    stream.set_write_timeout(Some(timeout))?;
    
    match stream.write_all(data).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::WouldBlock => {
            // Backpressure detected - consumer is slow
            metrics::increment_counter!("ipc.backpressure.events");
            Err(Error::Backpressure { consumer_id: "..." })
        }
        Err(e) if e.kind() == ErrorKind::BrokenPipe => {
            // Consumer disconnected
            Err(Error::ConsumerDisconnected)
        }
        Err(e) => Err(e.into()),
    }
}
```

#### Advantages Over Application-Level Backpressure

- **Zero overhead** - OS kernel manages buffers, no application logic needed
- **Fair scheduling** - OS scheduler handles blocked processes efficiently
- **Automatic rate matching** - Producer speed naturally matches consumer speed
- **Simple implementation** - Just handle `EAGAIN` or blocking writes

#### Configuration

```yaml
# config/ipc_channel.yaml
cross_process:
  socket_buffer_size: 262144  # 256KB (SO_SNDBUF/SO_RCVBUF)
  write_timeout: 30s           # Block up to 30s before error
  read_timeout: 60s            # Detect dead producers
  non_blocking: false          # Use blocking writes (simpler)
```

### Socket Management and Lifecycle

#### Connection Lifecycle

```
┌─────────────┐
│   CREATED   │  Socket file created in /tmp/velostream/
└──────┬──────┘
       │ Client connects
       ▼
┌─────────────┐
│  CONNECTED  │  Handshake: exchange config (group_id, partitions)
└──────┬──────┘
       │ Partition assignment
       ▼
┌─────────────┐
│   ACTIVE    │  Data flowing, heartbeats every 30s
└──────┬──────┘
       │ Timeout/disconnect/error
       ▼
┌─────────────┐
│  CLEANING   │  Remove from consumer group, reassign partitions
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   CLOSED    │  Socket file remains for new connections
└─────────────┘
```

#### Socket File Management

**Creation:**
```rust
pub fn create_socket_file(channel_name: &str) -> Result<PathBuf> {
    let socket_dir = PathBuf::from("/tmp/velostream");
    
    // Create directory with proper permissions
    std::fs::create_dir_all(&socket_dir)?;
    std::fs::set_permissions(&socket_dir, Permissions::from_mode(0o755))?;
    
    let socket_path = socket_dir.join(format!("{}.sock", channel_name));
    
    // Remove stale socket file if exists
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    
    Ok(socket_path)
}
```

**Cleanup Strategy:**
- **On process exit** - Remove socket file in Drop implementation
- **On startup** - Remove stale socket files older than 1 hour
- **Periodic cleanup** - Background task removes abandoned sockets every 5 minutes
- **Graceful shutdown** - Signal handler (SIGTERM) ensures cleanup

```rust
impl Drop for CrossProcessChannel {
    fn drop(&mut self) {
        // Close all connections
        if let Ok(mut streams) = self.streams.write() {
            for stream in streams.drain(..) {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
        
        // Remove socket file
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
        
        info!("Cleaned up IPC channel: {}", self.name);
    }
}
```

#### Connection Timeout and Expiry

**Heartbeat Protocol:**
```rust
pub struct HeartbeatManager {
    interval: Duration,        // Default: 30s
    timeout: Duration,         // Default: 90s (3x interval)
    last_heartbeat: HashMap<String, Instant>,
}

impl HeartbeatManager {
    pub async fn check_timeouts(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut expired = Vec::new();
        
        for (consumer_id, last_hb) in &self.last_heartbeat {
            if now.duration_since(*last_hb) > self.timeout {
                expired.push(consumer_id.clone());
                warn!(
                    "Consumer {} timed out (no heartbeat for {:?})",
                    consumer_id,
                    now.duration_since(*last_hb)
                );
            }
        }
        
        expired
    }
    
    pub fn record_heartbeat(&mut self, consumer_id: String) {
        self.last_heartbeat.insert(consumer_id, Instant::now());
    }
}
```

**Dead Connection Detection:**
- **Write errors** - `BrokenPipe` or `ConnectionReset` indicates dead client
- **Read timeouts** - No data received within `read_timeout` period
- **Heartbeat timeout** - No heartbeat message within 3x heartbeat interval
- **OS signals** - `SIGPIPE` on write to closed socket

#### Other Complications

**1. Socket Buffer Exhaustion**
```rust
// Problem: Large batches can exceed socket buffer
// Solution: Chunked writes with size limits
pub async fn write_batch_chunked(
    stream: &mut UnixStream,
    records: &[Arc<StreamRecord>],
    max_chunk_size: usize  // Default: 64KB
) -> Result<()> {
    let serialized = serialize_batch(records)?;
    
    for chunk in serialized.chunks(max_chunk_size) {
        stream.write_all(chunk).await?;
    }
    
    Ok(())
}
```

**2. Partial Reads**
```rust
// Problem: read() may return partial message
// Solution: Length-prefixed protocol with buffered reads
pub async fn read_message(stream: &mut UnixStream) -> Result<Vec<u8>> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    
    // Read exact message length
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    
    Ok(data)
}
```

**3. Thundering Herd on Reconnect**
```rust
// Problem: All consumers reconnect simultaneously after server restart
// Solution: Exponential backoff with jitter
pub async fn connect_with_backoff(
    socket_path: &Path,
    max_attempts: u32
) -> Result<UnixStream> {
    let mut attempt = 0;
    let mut delay = Duration::from_millis(100);
    
    loop {
        match UnixStream::connect(socket_path).await {
            Ok(stream) => return Ok(stream),
            Err(e) if attempt < max_attempts => {
                attempt += 1;
                
                // Add jitter: delay * (0.5 to 1.5)
                let jitter = rand::random::<f64>() * delay.as_secs_f64();
                let jittered_delay = delay + Duration::from_secs_f64(jitter - delay.as_secs_f64() * 0.5);
                
                warn!("Connection failed (attempt {}), retrying in {:?}", attempt, jittered_delay);
                tokio::time::sleep(jittered_delay).await;
                
                delay = std::cmp::min(delay * 2, Duration::from_secs(30));
            }
            Err(e) => return Err(e.into()),
        }
    }
}
```

**4. File Descriptor Limits**
```rust
// Problem: Too many connections exhaust file descriptors
// Solution: Connection pooling and limits
pub struct ConnectionPool {
    max_connections: usize,  // Default: 1000
    active_connections: AtomicUsize,
}

impl ConnectionPool {
    pub fn accept_connection(&self) -> Result<bool> {
        let current = self.active_connections.load(Ordering::Relaxed);
        
        if current >= self.max_connections {
            warn!("Connection limit reached: {}/{}", current, self.max_connections);
            return Err(Error::TooManyConnections);
        }
        
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}
```

### Partition-Group Assignment Protocol (Configuration-Based)

**Philosophy: Explicit configuration over automatic coordination**

Instead of Kafka-style automatic rebalancing, use **declarative configuration** where operators explicitly define partition assignments. This is simpler, more debuggable, and matches the single-machine IPC use case.

#### Configuration-First Approach

**No runtime coordination needed** - all partition assignment defined in config files:

```yaml
# channel_config.yaml
channels:
  orders:
    partitions: 4
    partition_key: customer_id
    max_capacity_per_partition: 10000
    
    # Optional: Define consumer groups statically
    consumer_groups:
      processors:
        consumers:
          - id: processor-1
            partitions: [0, 1]
          - id: processor-2
            partitions: [2, 3]
      
      fraud_detection:
        consumers:
          - id: fraud-checker
            partitions: [0, 1, 2, 3]  # Reads all partitions
```

#### SQL Configuration Syntax

```sql
-- Producer: Create partitioned channel
CREATE STREAM orders AS
SELECT * FROM kafka://orders_topic
INTO ipc://orders
WITH (
    'ipc.partitions' = '4',
    'ipc.partition_key' = 'customer_id',
    'ipc.max_capacity' = '10000'
);

-- Consumer 1: Explicit partition assignment
CREATE STREAM process_orders_1 AS
SELECT * FROM ipc://orders
WITH (
    'ipc.partitions' = '0,1'  -- Explicit: read partitions 0,1
)
GROUP BY customer_id
INTO kafka://output;

-- Consumer 2: Explicit partition assignment
CREATE STREAM process_orders_2 AS
SELECT * FROM ipc://orders  
WITH (
    'ipc.partitions' = '2,3'  -- Explicit: read partitions 2,3
)
GROUP BY customer_id
INTO kafka://output;

-- Consumer 3: Read all partitions (broadcast)
CREATE STREAM fraud_detection AS
SELECT * FROM ipc://orders
WITH (
    'ipc.partitions' = 'ALL'  -- Read all partitions
)
WHERE amount > 10000
INTO kafka://fraud_alerts;
```

#### Python Configuration

```python
from velostream import IPCReader, IPCWriter

# Explicit partition assignment via constructor
reader = IPCReader(
    "ipc://orders",
    partitions=[0, 1],  # Hard-coded: read partitions 0,1
    consumer_id="processor-1"
)

# Or from config file
from velostream.config import load_config

config = load_config("consumer_config.yaml")
reader = IPCReader.from_config(config)
```

**consumer_config.yaml:**
```yaml
source:
  channel: orders
  partitions: [0, 1]
  consumer_id: processor-1
  
serialization:
  format: arrow_ipc
  batch_size: 1000
```

#### Handshake Protocol (Simplified)

No dynamic assignment - consumer declares its partitions, coordinator validates:

```rust
#[derive(Serialize, Deserialize)]
pub struct ConnectRequest {
    pub consumer_id: String,
    pub channel_name: String,
    pub requested_partitions: PartitionSpec,
    pub serialization_format: String,
}

#[derive(Serialize, Deserialize)]
pub enum PartitionSpec {
    Explicit(Vec<usize>),  // [0, 1, 2]
    All,                    // Read all partitions
    None,                   // Error - must specify
}

#[derive(Serialize, Deserialize)]
pub struct ConnectResponse {
    pub status: ConnectionStatus,
    pub assigned_partitions: Vec<usize>,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum ConnectionStatus {
    Accepted,
    Rejected { reason: String },
}
```

**Connection flow:**

```
Consumer                          Coordinator
   │                                    │
   │  CONNECT                          │
   │  - consumer_id: "proc-1"          │
   │  - partitions: [0, 1]             │
   ├───────────────────────────────────>│
   │                                    │ Validate:
   │                                    │ - Partitions exist?
   │                                    │ - Already claimed?
   │                                    │ - Permission granted?
   │                                    │
   │  ACCEPTED                          │
   │  - assigned: [0, 1]                │
   │<───────────────────────────────────┤
   │                                    │
   │  ACK                               │
   ├───────────────────────────────────>│
   │                                    │
   │  ... data flow ...                │
```

#### Implementation

```rust
pub struct ConfigBasedCoordinator {
    config: Arc<RwLock<ChannelConfig>>,
    active_assignments: Arc<RwLock<HashMap<String, Assignment>>>,
}

#[derive(Clone)]
pub struct Assignment {
    consumer_id: String,
    partitions: Vec<usize>,
    connected_at: Instant,
}

impl ConfigBasedCoordinator {
    pub fn register_consumer(
        &self,
        consumer_id: String,
        requested_partitions: PartitionSpec,
    ) -> Result<Vec<usize>> {
        let config = self.config.read().unwrap();
        
        // Resolve partition spec to concrete partition list
        let partitions = match requested_partitions {
            PartitionSpec::Explicit(parts) => {
                // Validate partition IDs exist
                for &p in &parts {
                    if p >= config.partition_count {
                        return Err(Error::InvalidPartition { 
                            partition: p, 
                            max: config.partition_count 
                        });
                    }
                }
                parts
            }
            PartitionSpec::All => {
                // Read all partitions (broadcast mode)
                (0..config.partition_count).collect()
            }
            PartitionSpec::None => {
                return Err(Error::PartitionsNotSpecified);
            }
        };
        
        // Optional: Check for conflicts (if exclusive mode enabled)
        if config.exclusive_partitions {
            let assignments = self.active_assignments.read().unwrap();
            for partition_id in &partitions {
                if self.is_partition_claimed(&assignments, *partition_id, &consumer_id) {
                    return Err(Error::PartitionAlreadyClaimed {
                        partition: *partition_id,
                        by: self.get_partition_owner(&assignments, *partition_id),
                    });
                }
            }
        }
        
        // Record assignment
        let assignment = Assignment {
            consumer_id: consumer_id.clone(),
            partitions: partitions.clone(),
            connected_at: Instant::now(),
        };
        
        self.active_assignments
            .write()
            .unwrap()
            .insert(consumer_id.clone(), assignment);
        
        info!(
            "Registered consumer {} with partitions {:?}",
            consumer_id, partitions
        );
        
        Ok(partitions)
    }
    
    pub fn unregister_consumer(&self, consumer_id: &str) {
        let mut assignments = self.active_assignments.write().unwrap();
        
        if let Some(assignment) = assignments.remove(consumer_id) {
            info!(
                "Unregistered consumer {} (partitions {:?})",
                consumer_id, assignment.partitions
            );
        }
    }
    
    // Helper: Check if partition is claimed by another consumer
    fn is_partition_claimed(
        &self,
        assignments: &HashMap<String, Assignment>,
        partition_id: usize,
        requesting_consumer: &str,
    ) -> bool {
        assignments.iter().any(|(id, assignment)| {
            id != requesting_consumer && assignment.partitions.contains(&partition_id)
        })
    }
}
```

#### Configuration Options

```yaml
channels:
  orders:
    partitions: 4
    partition_key: customer_id
    
    # Partition assignment mode
    assignment_mode: explicit  # explicit | broadcast | exclusive
    
    # exclusive: Only one consumer per partition (enforced)
    # broadcast: Multiple consumers can read same partition
    # explicit: No enforcement, trust config
    exclusive_partitions: false
    
    # Optional: Pre-defined consumer groups (for validation)
    allowed_consumers:
      - processor-1
      - processor-2
      - fraud-checker
```

#### Benefits of Configuration-Based Approach

**Pros:**
- ✅ **Zero coordination logic** - No rebalancing, no generation IDs, no race conditions
- ✅ **Explicit and auditable** - Can see partition assignment in config files
- ✅ **Predictable behavior** - No surprise rebalances during production
- ✅ **Easy debugging** - Know exactly which consumer should read which partition
- ✅ **GitOps-friendly** - Config changes go through version control
- ✅ **No split-brain** - Can't have two coordinators disagreeing
- ✅ **Simpler testing** - Just test config validation, not coordination protocol

**Cons:**
- ❌ No automatic failover (need to manually reassign if consumer dies)
- ❌ No automatic load balancing (need to manually adjust partition assignment)
- ❌ More operational overhead (must update config to scale)

**When this works well:**
- Single-machine deployments (IPC by definition)
- Static topology (2-4 SQL queries, 1-2 Python processes)
- Ops team comfortable with config management
- Prefer explicit control over automatic behavior

**When you might need dynamic assignment:**
- 10+ consumers that frequently scale up/down
- Dynamic workloads where failures are common
- Need automatic failover without human intervention
- Multi-tenant environments with many users

#### Hybrid Approach: Config with Fallback

For flexibility, support both modes:

```yaml
channels:
  orders:
    partitions: 4
    
    assignment_mode: config_with_fallback
    
    # Static assignments (preferred)
    static_assignments:
      processor-1: [0, 1]
      processor-2: [2, 3]
    
    # Fallback: If consumer not in static config, use this
    fallback_mode: broadcast  # broadcast | error | round_robin
```

```rust
impl ConfigBasedCoordinator {
    pub fn register_consumer(&self, consumer_id: String, ...) -> Result<Vec<usize>> {
        let config = self.config.read().unwrap();
        
        // Check static assignment first
        if let Some(partitions) = config.static_assignments.get(&consumer_id) {
            return Ok(partitions.clone());
        }
        
        // Fallback behavior
        match config.fallback_mode {
            FallbackMode::Broadcast => {
                // Read all partitions
                Ok((0..config.partition_count).collect())
            }
            FallbackMode::Error => {
                Err(Error::ConsumerNotConfigured { consumer_id })
            }
            FallbackMode::RoundRobin => {
                // Simple: assign partition based on hash(consumer_id)
                let partition = hash(&consumer_id) % config.partition_count;
                Ok(vec![partition])
            }
        }
    }
}
```

#### Example: Complete Configuration

**deployment.yaml:**
```yaml
velostream:
  channels:
    # High-value orders channel
    high_value_orders:
      partitions: 4
      partition_key: customer_id
      max_capacity_per_partition: 10000
      assignment_mode: explicit
      exclusive_partitions: true  # Enforce one consumer per partition
      
      consumers:
        aggregator-1:
          partitions: [0, 1]
        aggregator-2:
          partitions: [2, 3]
        fraud-checker:
          partitions: [0, 1, 2, 3]  # Can read all (non-exclusive group)
    
    # Market data channel
    market_data:
      partitions: 8
      partition_key: symbol
      assignment_mode: broadcast  # All consumers read all data
      exclusive_partitions: false
```

#### CLI for Introspection

```bash
# Show current partition assignments
$ velostream-cli partitions show orders
Channel: orders (4 partitions)

Partition 0: consumer processor-1 (connected 5m ago)
Partition 1: consumer processor-1 (connected 5m ago)
Partition 2: consumer processor-2 (connected 3m ago)
Partition 3: consumer processor-2 (connected 3m ago)

Consumer Groups:
  processors: 2 consumers, 4 partitions assigned
  fraud_detection: 1 consumer, 4 partitions (broadcast)

# Validate config before deployment
$ velostream-cli config validate deployment.yaml
✓ All partition assignments valid
✓ No partition conflicts detected
✓ All consumers defined
```

#### Summary: Configuration vs Automatic

| Feature | Configuration-Based | Automatic (Kafka-style) |
|---------|-------------------|------------------------|
| Complexity | Low | High |
| Code required | Validation only | Rebalancing protocol |
| Predictability | High | Medium |
| Debuggability | Easy | Harder |
| Failover | Manual | Automatic |
| Load balancing | Manual | Automatic |
| Best for | Static topologies | Dynamic scaling |
| Lines of code | ~200 | ~2000 |

**Recommendation for Phase 1:** Start with configuration-based. It's 10x simpler and matches the IPC use case perfectly.

## Core Components

### 1. Unified IPC Channel (Automatic Transport Selection)

```rust
pub struct IPCChannel {
    name: String,
    mode: ChannelMode,
    partitions: Vec<Arc<Partition>>,
    partition_count: usize,
    partition_strategy: PartitionStrategy,
    consumer_groups: Arc<RwLock<HashMap<String, Arc<ConsumerGroup>>>>,
    metrics: Arc<ChannelMetrics>,
}

pub enum ChannelMode {
    InProcess,      // Same Velostream process (~50ns)
    CrossProcess,   // Different processes (~5µs)
}

pub enum PartitionStrategy {
    Hash,           // Hash(key) % partition_count
    RoundRobin,     // Distribute evenly (no key)
}
```

### 2. Consumer Group (Kafka-Style)

```rust
pub struct ConsumerGroup {
    group_id: String,
    partition_count: usize,
    consumers: Arc<RwLock<Vec<ConsumerState>>>,
    partition_assignment: Arc<RwLock<HashMap<String, Vec<usize>>>>,
}

pub struct ConsumerState {
    consumer_id: String,
    registered_at: Instant,
    last_heartbeat: AtomicU64,
}
```

### 3. Backpressure Strategy

```rust
pub enum BackpressureStrategy {
    Block {
        timeout: Option<Duration>,
    },
    DropOldest,
    Error,
}
```

**Default:** Block with 30s timeout (no data loss, memory bounded)

### 4. Serialization: Arrow IPC (Default for Python)

```rust
pub trait Serializer: Send + Sync {
    fn serialize(&self, record: &StreamRecord) -> Result<Vec<u8>>;
    fn deserialize(&self, bytes: &[u8]) -> Result<StreamRecord>;
    fn serialize_batch(&self, records: &[Arc<StreamRecord>]) -> Result<Vec<u8>>;
    fn deserialize_batch(&self, bytes: &[u8]) -> Result<Vec<StreamRecord>>;
}

pub struct ArrowIPCSerializer {
    schema: Arc<ArrowSchema>,
    batch_size: usize,  // Auto-batch for efficiency
}
```

#### Serialization Performance

| Format | Single Record | Batch (1000) | Size | Zero-Copy Python |
|--------|---------------|--------------|------|------------------|
| Arrow IPC | 10µs | 1ms (1µs avg) | 150B | ✅ Yes |
| MessagePack | 1µs | 1ms (1µs avg) | 180B | ❌ No |
| Bincode | 500ns | 500µs | 160B | ❌ No |

**Default:** Arrow IPC with auto-batching (1µs per record amortized)

## Dependency Management and Connection Resilience

### The Challenge: Service Startup Dependencies

IPC channels create temporal dependencies between producers and consumers that must be handled gracefully to avoid:

- **Startup order dependencies** - Consumer fails if started before producer
- **Data loss** - Producer writes to channel with no consumers
- **Cascading failures** - Service restart breaks all dependent services
- **Operational complexity** - Manual coordination of service startup order

### Scenario 1: Consumer Starts Before Producer

```sql
-- Service A (consumer) starts FIRST
CREATE STREAM process AS
SELECT * FROM ipc://orders  -- ❌ Channel doesn't exist yet!
WHERE amount > 100;

-- Service B (producer) starts LATER
CREATE STREAM filtered AS
SELECT * FROM kafka://raw
INTO ipc://orders;  -- Creates channel
```

**Problem:** Consumer fails immediately with "Channel not found" error.

**Solution:** Retry with exponential backoff.

### Scenario 2: Producer Starts Before Consumer

```sql
-- Service A (producer) starts FIRST
CREATE STREAM filtered AS
SELECT * FROM kafka://raw
INTO ipc://orders
WITH ('ipc.backpressure_strategy' = 'block');

-- Service B (consumer) starts LATER (30 seconds delay)
CREATE STREAM process AS
SELECT * FROM ipc://orders;
```

**Problems:**
- **Block strategy:** Producer blocks forever waiting for consumers → Kafka consumer stalls
- **DropOldest strategy:** 30 seconds of data silently lost
- **Error strategy:** Producer crashes

**Solution:** Wait for first consumer or buffer startup records.

### Solution 1: Lazy Channel Creation with Auto-Recovery

```rust
pub struct IPCChannelManager {
    channels: Arc<RwLock<HashMap<String, Arc<IPCChannel>>>>,
}

impl IPCChannelManager {
    /// Get existing channel or create new one (thread-safe)
    pub fn get_or_create(
        &self,
        name: &str,
        config: ChannelConfig
    ) -> Arc<IPCChannel> {
        let mut channels = self.channels.write().unwrap();

        channels.entry(name.to_string())
            .or_insert_with(|| {
                log::info!("Creating IPC channel: {} (partitions: {})",
                    name, config.partition_count);
                Arc::new(IPCChannel::new(name, config))
            })
            .clone()
    }
}
```

**Key Benefit:** Both producer and consumer can call `get_or_create()` - whoever starts first creates the channel.

### Solution 2: Consumer Retry with Exponential Backoff

```rust
pub struct IPCConnectionConfig {
    /// Maximum number of connection attempts
    pub max_retries: u32,           // Default: unlimited (0)

    /// Initial retry delay
    pub initial_backoff: Duration,  // Default: 1s

    /// Maximum retry delay
    pub max_backoff: Duration,      // Default: 30s

    /// Total timeout for connection attempts
    pub connection_timeout: Duration, // Default: 5 minutes

    /// Whether to wait for channel to exist
    pub wait_for_channel: bool,     // Default: true
}

impl IPCDataSource {
    pub async fn create_reader(&self) -> Result<Box<dyn DataReader>> {
        if !self.connection_config.wait_for_channel {
            // Fail immediately if channel doesn't exist
            return self.try_create_reader().await;
        }

        // Retry with exponential backoff
        let mut attempt = 0;
        let mut backoff = self.connection_config.initial_backoff;
        let start = Instant::now();

        loop {
            match self.try_create_reader().await {
                Ok(reader) => {
                    log::info!(
                        "Connected to IPC channel '{}' after {} attempts ({:?})",
                        self.channel_name, attempt, start.elapsed()
                    );
                    return Ok(reader);
                }

                Err(e) if e.is_channel_not_found() => {
                    // Check timeout
                    if start.elapsed() > self.connection_config.connection_timeout {
                        return Err(IPCError::ConnectionTimeout {
                            channel: self.channel_name.clone(),
                            duration: self.connection_config.connection_timeout,
                            attempts: attempt,
                        });
                    }

                    attempt += 1;
                    log::warn!(
                        "IPC channel '{}' not found (attempt {}), retrying in {:?}",
                        self.channel_name, attempt, backoff
                    );

                    // Sleep with exponential backoff
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, self.connection_config.max_backoff);
                }

                Err(e) => return Err(e),  // Other errors are not retryable
            }
        }
    }
}
```

**SQL Configuration:**

```sql
CREATE STREAM consumer AS
SELECT * FROM ipc://orders
WITH (
    'ipc.wait_for_channel' = 'true',       -- Wait for channel to exist
    'ipc.connection_timeout' = '300s',     -- Give up after 5 minutes
    'ipc.initial_backoff' = '1s',          -- Start with 1s retry
    'ipc.max_backoff' = '30s'              -- Cap at 30s between retries
);
```

### Solution 3: Producer Wait Strategy

```rust
pub enum ProducerStartStrategy {
    /// Wait for at least one consumer to connect
    WaitForConsumer {
        timeout: Duration,  // Default: 5 minutes
    },

    /// Buffer records until first consumer arrives
    BufferUntilConsumer {
        max_buffer: usize,  // Default: 100K records
        overflow_strategy: OverflowStrategy,
    },

    /// Start producing immediately (may lose data if no consumers)
    StartImmediately,
}

impl IPCDataSink {
    pub async fn create_writer(&self) -> Result<Box<dyn DataWriter>> {
        let channel = self.channel_manager.get_or_create(
            &self.channel_name,
            self.channel_config.clone()
        );

        match self.start_strategy {
            ProducerStartStrategy::WaitForConsumer { timeout } => {
                self.wait_for_consumer(&channel, timeout).await?;
            }

            ProducerStartStrategy::BufferUntilConsumer { max_buffer, .. } => {
                channel.enable_startup_buffer(max_buffer);
            }

            ProducerStartStrategy::StartImmediately => {
                log::warn!(
                    "IPC producer starting with no consumers on '{}'",
                    self.channel_name
                );
            }
        }

        Ok(Box::new(IPCWriter::new(channel)))
    }

    async fn wait_for_consumer(
        &self,
        channel: &IPCChannel,
        timeout: Duration
    ) -> Result<()> {
        let start = Instant::now();

        while channel.consumer_count() == 0 {
            if start.elapsed() > timeout {
                return Err(IPCError::NoConsumersTimeout {
                    channel: self.channel_name.clone(),
                    timeout,
                });
            }

            log::info!(
                "Waiting for consumers on IPC channel '{}' ({:?} elapsed)",
                self.channel_name, start.elapsed()
            );

            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        log::info!(
            "First consumer connected to IPC channel '{}' after {:?}",
            self.channel_name, start.elapsed()
        );

        Ok(())
    }
}
```

**SQL Configuration:**

```sql
CREATE STREAM producer AS
SELECT * FROM kafka://raw
INTO ipc://orders
WITH (
    'ipc.producer_start_strategy' = 'wait_for_consumer',
    'ipc.producer_wait_timeout' = '300s',  -- Wait 5 minutes for consumers
    'ipc.backpressure_strategy' = 'block'
);
```

### Solution 4: Health Checks and Monitoring

```rust
#[derive(Debug, Clone)]
pub struct ChannelHealth {
    pub exists: bool,
    pub consumer_count: usize,
    pub producer_count: usize,
    pub queue_depth: usize,
    pub oldest_message_age: Option<Duration>,
    pub status: ChannelStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChannelStatus {
    Healthy,           // Producers and consumers connected
    ProducerOnly,      // No consumers (potential data loss)
    ConsumerOnly,      // No producers (waiting for data)
    Idle,              // No producers or consumers
    Backpressure,      // Queue full, blocking writes
}

impl IPCChannel {
    pub fn health_check(&self) -> ChannelHealth {
        let consumer_count = self.consumer_count();
        let producer_count = self.producer_count();
        let queue_depth = self.total_queue_depth();

        let status = match (producer_count, consumer_count, queue_depth) {
            (0, 0, _) => ChannelStatus::Idle,
            (_, 0, _) => ChannelStatus::ProducerOnly,
            (0, _, _) => ChannelStatus::ConsumerOnly,
            (_, _, depth) if depth >= self.capacity * 90 / 100 => {
                ChannelStatus::Backpressure
            }
            _ => ChannelStatus::Healthy,
        };

        ChannelHealth {
            exists: true,
            consumer_count,
            producer_count,
            queue_depth,
            oldest_message_age: self.oldest_message_age(),
            status,
        }
    }
}
```

**HTTP Health Endpoint:**

```bash
$ curl http://localhost:8080/health/ipc/orders
{
  "exists": true,
  "consumer_count": 2,
  "producer_count": 1,
  "queue_depth": 1250,
  "status": "Healthy",
  "oldest_message_age_ms": 45
}
```

### Solution 5: Declarative Dependency Management

```yaml
# deployment.yaml
velostream:
  services:
    # Producer service
    kafka_filter:
      type: sql_query
      query: |
        CREATE STREAM filtered AS
        SELECT * FROM kafka://orders
        WHERE amount > 100
        INTO ipc://high_value_orders

      ipc_producer_config:
        channel: high_value_orders
        start_strategy: wait_for_consumer
        wait_timeout: 300s

    # Consumer service 1
    aggregator:
      type: sql_query
      query: |
        CREATE STREAM aggregated AS
        SELECT customer_id, SUM(amount)
        FROM ipc://high_value_orders
        GROUP BY customer_id
        INTO kafka://stats

      depends_on:
        - kafka_filter  # Wait for producer to start

      ipc_consumer_config:
        channel: high_value_orders
        wait_for_channel: true
        connection_timeout: 300s
        initial_backoff: 1s

    # Consumer service 2 (Python)
    python_processor:
      type: python_script
      script: process_orders.py

      depends_on:
        - kafka_filter

      ipc_consumer_config:
        channel: high_value_orders
        wait_for_channel: true
```

**Orchestration ensures startup order:**
1. `kafka_filter` starts first (producer)
2. `aggregator` waits for `kafka_filter` readiness
3. `python_processor` waits for `kafka_filter` readiness

### Recommended Configuration Strategy

#### Development (Fast Feedback)

```yaml
ipc:
  consumer:
    wait_for_channel: false      # Fail fast if channel missing
    connection_timeout: 10s      # Quick timeout

  producer:
    start_strategy: start_immediately  # Don't wait for consumers
```

#### Production (Resilient)

```yaml
ipc:
  consumer:
    wait_for_channel: true       # Wait for channel to exist
    connection_timeout: 300s     # 5 minute timeout
    initial_backoff: 1s
    max_backoff: 30s

  producer:
    start_strategy: wait_for_consumer  # Wait for first consumer
    wait_timeout: 300s           # 5 minute timeout
    backpressure_strategy: block
```

### Resilience Summary

| Scenario | Solution | Default Behavior |
|----------|----------|------------------|
| **Consumer starts first** | Retry with backoff | Wait up to 5 minutes |
| **Producer starts first** | Wait for consumer | Wait up to 5 minutes |
| **Channel doesn't exist** | Auto-create on first access | Lazy creation |
| **Connection timeout** | Exponential backoff | 1s → 30s, max 5 min |
| **No consumers** | Producer buffering | Wait or buffer |
| **Service restart** | Auto-reconnect | Retry with backoff |
| **Health monitoring** | `/health/ipc/{channel}` | Check before critical ops |

**Design Principle:** Make IPC channels **self-healing by default**, with configuration options for strict failure modes in development. This ensures operational robustness while maintaining the low-latency benefits of IPC communication.

## SQL Integration

### Configuration Syntax

```sql
-- Default: In-process channel (zero-copy Arc)
CREATE STREAM filtered_orders AS
SELECT * FROM kafka://orders_topic
WHERE amount > 100
INTO ipc://high_value_orders
WITH (
    'ipc://high_value_orders.partitions' = '4',
    'ipc://high_value_orders.partition_key' = 'customer_id',
    'ipc://high_value_orders.max_capacity' = '10000',
    'ipc://high_value_orders.backpressure_strategy' = 'block',
    'ipc://high_value_orders.backpressure_timeout' = '30s'
);

-- Consumer with group
CREATE STREAM aggregated AS
SELECT customer_id, SUM(amount) as total_amount
FROM ipc://high_value_orders
GROUP BY customer_id
INTO kafka://customer_stats
WITH (
    'ipc://high_value_orders.group_id' = 'aggregators',
    'ipc://high_value_orders.consumer_id' = 'aggregator-1'
);
```

### Configuration Files

```yaml
# config/ipc_sink.yaml
type: ipc_sink
channel_name: high_value_orders
partitions: 4
partition_key: customer_id
max_capacity: 10000

# Backpressure configuration
backpressure:
  strategy: block  # block | drop_oldest | error
  timeout: 30s

# Serialization (for cross-process)
serialization:
  format: arrow_ipc  # arrow_ipc | messagepack | bincode
  batch_size: 1000
  batch_timeout: 10ms
```

```yaml
# config/ipc_source.yaml
type: ipc_source
channel_name: high_value_orders
group_id: aggregators
consumer_id: aggregator-1
```

## Python Client Integration

```python
# velostream/ipc.py

import socket
import struct
import pyarrow as pa
import pyarrow.ipc as ipc
from typing import Iterator, Dict, Any, List

class IPCReader:
    """Python client for reading from Velostream IPC channels"""
    
    def __init__(
        self,
        uri: str,
        group_id: str = None,
        consumer_id: str = None
    ):
        self.channel_name = uri.replace("ipc://", "")
        self.group_id = group_id
        self.consumer_id = consumer_id or f"python-{os.getpid()}"
        
        # Connect to Unix socket
        self.socket_path = f"/tmp/velostream/{self.channel_name}.sock"
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)
        
        # Send configuration (handshake)
        self._send_config({
            'type': 'consumer',
            'group_id': group_id,
            'consumer_id': self.consumer_id,
            'serialization_format': 'arrow_ipc'
        })
        
        # Receive partition assignment
        self.assigned_partitions = self._recv_assignment()
    
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over records (row-at-a-time)"""
        while True:
            arrow_table = self._read_arrow_batch()
            if arrow_table is None:
                break
            
            for record in arrow_table.to_pylist():
                yield record
    
    def read_batch(self, n: int = 1000) -> pa.Table:
        """Read batch as Arrow table (zero-copy to pandas/polars)"""
        return self._read_arrow_batch()

class IPCWriter:
    """Python client for writing to Velostream IPC channels"""
    
    def __init__(self, uri: str, partition_key: str = None):
        self.channel_name = uri.replace("ipc://", "")
        self.partition_key = partition_key
        
        # Connect to Unix socket
        self.socket_path = f"/tmp/velostream/{self.channel_name}.sock"
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)
        
        # Send configuration
        self._send_config({
            'type': 'producer',
            'partition_key': partition_key,
            'serialization_format': 'arrow_ipc'
        })
        
        # Buffer for batching
        self.buffer = []
        self.batch_size = 1000
    
    def write(self, record: Dict[str, Any]):
        """Write single record (auto-batches)"""
        self.buffer.append(record)
        
        if len(self.buffer) >= self.batch_size:
            self.flush()
    
    def write_batch(self, records: List[Dict[str, Any]]):
        """Write batch of records"""
        table = pa.Table.from_pylist(records)
        
        # Serialize with Arrow IPC
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        
        data = sink.getvalue().to_pybytes()
        
        # Send length-prefixed message
        self.sock.sendall(struct.pack('!I', len(data)))
        self.sock.sendall(data)
    
    def flush(self):
        if self.buffer:
            self.write_batch(self.buffer)
            self.buffer.clear()
```

## External Storage Integration

### Supported Connectors

**Input Sources:**

- `kafka://` - Apache Kafka topics
- `kinesis://` - AWS Kinesis streams
- `pubsub://` - GCP Pub/Sub topics
- `file://` - Local files (CSV, JSON, Parquet)
- `s3://` - S3 objects (batch read)

**Output Sinks:**

- `kafka://` - Apache Kafka topics
- `s3://` - S3 objects (batch write)
- `file://` - Local files
- `questdb://` - QuestDB tables (via ILP)
- `postgres://` - PostgreSQL tables
- `clickhouse://` - ClickHouse tables

### Integration Pattern

```sql
-- Durable → IPC → IPC → Durable
CREATE STREAM input AS
SELECT * FROM kafka://source_topic
INTO ipc://stage1;

CREATE STREAM process AS
SELECT * FROM ipc://stage1
WHERE valid = true
INTO ipc://stage2;

CREATE STREAM output AS
SELECT * FROM ipc://stage2
INTO kafka://output_topic,
     s3://archive/;
```

## Performance Considerations

### Latency Breakdown

| Operation | In-Process | Cross-Process | Kafka |
|-----------|-----------|---------------|-------|
| Write | 50ns | 5µs | 10ms |
| Read | 50ns | 5µs | 10ms |
| Serialization | None | 2µs (Arrow IPC) | 5-10ms |
| Network | None | None (Unix socket) | 5-50ms |
| **Total per stage** | **<100ns** | **<10µs** | **10-100ms** |

### Performance Targets

| Metric | Target |
|--------|--------|
| In-process throughput | >5M records/sec |
| Cross-process throughput (batched) | >1M records/sec |
| Cross-process throughput (unbatched) | >500K records/sec |
| In-process latency (p50) | <100ns |
| Cross-process latency (p50) | <10µs |
| End-to-end (4 stages) | 20-50ms |
| Python IPC latency | <10µs |

### Partition-Aware Benchmarks

| Partitions | Write Throughput | Read Throughput (per consumer) | Total (4 consumers) |
|------------|------------------|-------------------------------|---------------------|
| 1 | 5M/sec | 5M/sec | 5M/sec |
| 4 | 15M/sec | 4M/sec | 16M/sec |
| 16 | 40M/sec | 2.5M/sec | 40M/sec |

## Competitive Positioning

### vs Apache Flink

| Feature | Flink | Velostream IPC |
|---------|-------|----------------|
| Architecture | Distributed DAG | Local IPC + External storage |
| In-process latency | 10-100ms | <100µs |
| Cross-process latency | 10-100ms | <10µs (Unix socket) |
| Python integration | Py4J (~1-10ms) | Native IPC (<10µs) |
| Setup complexity | High (cluster) | Low (single process) |
| Ops complexity | High | Low |
| Engineering cost | 2-3 FTE | 0.5 FTE |
| Total cost | $1-2M/year | $300-800K/year |
| Use case | Distributed | Low-latency local |

**Positioning:** "Replace Flink with Velostream for 3-10x lower latency and 40-60% lower TCO. Keep Kafka for durability."

### vs kdb+

| Feature | kdb+ | Velostream IPC |
|---------|------|----------------|
| Language | q (proprietary) | SQL (standard) |
| Python integration | Poor | Excellent (<10µs IPC) |
| Cost | $640K-5M/year | $150-500K/year |
| In-process latency | <1µs | <100µs |
| Cross-process latency | N/A | <10µs |
| Learning curve | Steep | Easy |
| Ecosystem | Proprietary | Open (Kafka, S3, cloud) |

**Positioning:** "Modern alternative to kdb+ with standard SQL, better Python integration, and 50-70% lower cost."

### vs QuestDB

| Feature | QuestDB | Velostream IPC |
|---------|---------|----------------|
| Primary use case | Time-series storage | Stream processing |
| Architecture | Disk-backed database | Memory-first processor |
| Query latency | 1-10ms | N/A (not query engine) |
| Processing latency | N/A | 10-100µs (in-process) |
| Python integration | Network (psycopg2, 1-10ms) | Native IPC (<10µs) |
| Streaming SQL | No | Yes (CREATE STREAM AS) |

**Positioning:** "Complementary. Use QuestDB for historical storage, Velostream for real-time processing."

## Implementation Plan

## Simplified Implementation Strategies

For the initial implementation, we can use simple, proven strategies that avoid premature optimization while remaining production-ready.

### Simple Connection Management

**Strategy: One socket per channel, accept-loop with connection tracking**

```rust
pub struct SimpleConnectionManager {
    listener: UnixListener,
    connections: Arc<RwLock<Vec<ManagedConnection>>>,
    max_connections: usize,  // Default: 100
}

struct ManagedConnection {
    id: String,
    stream: UnixStream,
    created_at: Instant,
    last_activity: AtomicU64,  // Unix timestamp
}

impl SimpleConnectionManager {
    pub async fn accept_loop(&self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _addr)) => {
                    // Check connection limit
                    if self.connections.read().unwrap().len() >= self.max_connections {
                        warn!("Connection limit reached, rejecting new connection");
                        let _ = stream.shutdown(Shutdown::Both);
                        continue;
                    }
                    
                    // Spawn handler task
                    let conn_id = Uuid::new_v4().to_string();
                    let managed = ManagedConnection {
                        id: conn_id.clone(),
                        stream,
                        created_at: Instant::now(),
                        last_activity: AtomicU64::new(current_timestamp()),
                    };
                    
                    self.connections.write().unwrap().push(managed);
                    
                    tokio::spawn(handle_connection(conn_id, /* ... */));
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}
```

**Key simplifications:**
- **No pooling** - Just track active connections in a Vec
- **Linear cleanup** - Periodic scan of connections (acceptable for <1000 connections)
- **Simple ID generation** - Use UUID, no need for complex registry
- **One task per connection** - Tokio handles scheduling efficiently

### Simple Timeout Strategy

**Strategy: Activity-based timeouts with periodic cleanup task**

```rust
pub struct TimeoutConfig {
    read_timeout: Duration,   // Default: 60s (no data received)
    write_timeout: Duration,  // Default: 30s (write blocked)
    idle_timeout: Duration,   // Default: 300s (no activity at all)
    cleanup_interval: Duration,  // Default: 30s
}

pub async fn cleanup_task(
    connections: Arc<RwLock<Vec<ManagedConnection>>>,
    config: TimeoutConfig
) {
    let mut interval = tokio::time::interval(config.cleanup_interval);
    
    loop {
        interval.tick().await;
        
        let now = current_timestamp();
        let mut conns = connections.write().unwrap();
        
        // Remove stale connections (simple retain filter)
        conns.retain(|conn| {
            let last_activity = conn.last_activity.load(Ordering::Relaxed);
            let idle_duration = now - last_activity;
            
            if idle_duration > config.idle_timeout.as_secs() {
                warn!("Closing idle connection {} (idle for {}s)", conn.id, idle_duration);
                let _ = conn.stream.shutdown(Shutdown::Both);
                return false;  // Remove from list
            }
            
            true  // Keep
        });
        
        info!("Cleanup: {} active connections", conns.len());
    }
}

// Update activity timestamp on every read/write
async fn handle_connection(conn_id: String, stream: UnixStream) {
    let mut stream = stream;
    
    loop {
        // Set read timeout using tokio
        match tokio::time::timeout(Duration::from_secs(60), read_message(&mut stream)).await {
            Ok(Ok(msg)) => {
                // Update activity timestamp
                update_activity(&conn_id);
                process_message(msg).await;
            }
            Ok(Err(e)) => {
                error!("Read error on {}: {}", conn_id, e);
                break;
            }
            Err(_) => {
                warn!("Read timeout on {}", conn_id);
                break;
            }
        }
    }
}
```

**Key simplifications:**
- **Single idle timeout** - One timer for all inactivity (no separate read/write timeouts initially)
- **Periodic sweep** - Every 30s, scan all connections (O(n) but n is small)
- **Tokio timeout** - Use `tokio::time::timeout` instead of socket-level timeouts
- **No heartbeat initially** - Rely on data activity; add heartbeat only if needed

### Simple Coordination Strategy

**Strategy: Hard-coded partition assignment (simplest possible)**

For Phase 1, skip dynamic consumer groups entirely and use **explicit partition assignment**:

```rust
pub struct SimpleCoordinator {
    // No state needed! Consumers explicitly request partitions
}

impl SimpleCoordinator {
    // Consumer tells us what partitions it wants
    pub fn register_consumer(&self, consumer_id: String, partitions: Vec<usize>) -> Result<()> {
        // Just validate the partition IDs exist
        if partitions.iter().any(|&p| p >= PARTITION_COUNT) {
            return Err(Error::InvalidPartition);
        }
        
        info!("Consumer {} assigned to partitions {:?}", consumer_id, partitions);
        Ok(())
    }
}
```

**Configuration approach:**

```sql
-- Producer writes to partitioned channel
CREATE STREAM orders AS
SELECT * FROM kafka://orders_topic
INTO ipc://orders
WITH ('partitions' = '4', 'partition_key' = 'customer_id');

-- Consumer 1: Explicitly reads partitions 0,1
CREATE STREAM process_1 AS
SELECT * FROM ipc://orders
WHERE __partition_id IN (0, 1)  -- Hard-coded partition filter
INTO kafka://output;

-- Consumer 2: Explicitly reads partitions 2,3
CREATE STREAM process_2 AS
SELECT * FROM ipc://orders
WHERE __partition_id IN (2, 3)  -- Hard-coded partition filter
INTO kafka://output;
```

Or with configuration files:

```yaml
# consumer1.yaml
source:
  type: ipc_source
  channel: orders
  partitions: [0, 1]  # Hard-coded

# consumer2.yaml
source:
  type: ipc_source
  channel: orders
  partitions: [2, 3]  # Hard-coded
```

**Python example:**

```python
# Process A: Hard-coded to partitions 0,1
reader = IPCReader("ipc://orders", partitions=[0, 1])

# Process B: Hard-coded to partitions 2,3
reader = IPCReader("ipc://orders", partitions=[2, 3])
```

**Why this is better for Phase 1:**

1. **Zero coordination logic** - No group state, no rebalancing, no generation IDs
2. **Explicit and debuggable** - Clear which consumer reads which partitions
3. **No race conditions** - Consumers can't conflict (you configure them not to)
4. **Easier testing** - Just test partition filtering, not coordination
5. **Operations-friendly** - Operators understand "process 1 reads partitions 0,1"

**Limitations (acceptable for Phase 1):**

- Manual partition assignment (no automatic load balancing)
- No automatic failover (if consumer 1 dies, partitions 0,1 stop processing)
- Requires coordination in deployment config

**When to add consumer groups:**

Only add Kafka-style consumer groups when you **actually need** automatic load balancing:

- Multiple Python processes that dynamically scale
- Need automatic failover without reconfiguration
- >10 consumers where manual assignment is tedious

For most use cases (2-4 SQL queries, 1-2 Python processes), hard-coded assignment is **clearer and simpler**.

---

**Alternative: Hybrid approach (Phase 2)**

If you need some automation but not full Kafka-style coordination:

```rust
// Simple: Read ALL partitions if no filter specified
pub fn register_consumer(&self, consumer_id: String, partitions: Option<Vec<usize>>) -> Vec<usize> {
    match partitions {
        Some(explicit) => {
            // User specified partitions explicitly
            info!("Consumer {} using explicit partitions {:?}", consumer_id, explicit);
            explicit
        }
        None => {
            // No partitions specified: read ALL partitions
            let all_partitions: Vec<usize> = (0..PARTITION_COUNT).collect();
            info!("Consumer {} reading ALL partitions", consumer_id);
            all_partitions
        }
    }
}
```

This gives you:
- **Default behavior**: Read all partitions (broadcast mode)
- **Explicit control**: Specify partitions when you want parallelism
- **No coordination**: Still no group state or rebalancing

---

### Simplified Coordination Strategy (Updated)

**Strategy: Start with hard-coded partitions, add consumer groups in Phase 3**

```rust
pub struct SimpleCoordinator {
    // Phase 1: No state needed
    // Phase 3: Add when we need consumer groups
    groups: Option<Arc<RwLock<HashMap<String, GroupState>>>>,
}

impl SimpleCoordinator {
    pub fn register_consumer(
        &self,
        consumer_id: String,
        partitions: Option<Vec<usize>>,  // None = read all
        group_id: Option<String>,         // None = no group coordination
    ) -> Result<Vec<usize>> {
        match (partitions, group_id) {
            // Case 1: Explicit partitions (Phase 1)
            (Some(explicit), None) => {
                self.validate_partitions(&explicit)?;
                Ok(explicit)
            }
            
            // Case 2: Read all partitions (Phase 1)
            (None, None) => {
                Ok((0..PARTITION_COUNT).collect())
            }
            
            // Case 3: Consumer group (Phase 3 - not implemented yet)
            (_, Some(group_id)) => {
                Err(Error::NotImplemented("Consumer groups coming in Phase 3"))
            }
        }
    }
}

### Progressive Complexity Path

Start simple, add complexity only when needed:

#### Phase 1 (MVP): Basic but functional
```
✓ One socket per channel
✓ Linear connection tracking (Vec)
✓ Simple idle timeout (300s)
✓ Hard-coded partition assignment (no consumer groups)
✓ No heartbeat (rely on data activity)
✓ No rebalancing logic
```

#### Phase 2: Add observability
```
✓ Metrics for connection count
✓ Metrics for timeout events
✓ Logging for partition assignment
✓ Connection age histograms
```

#### Phase 3: Add consumer groups (if needed)
```
✓ Consumer group coordination
✓ Automatic rebalancing
✓ Heartbeat protocol
✓ Sticky assignment
```

#### Phase 4: Production hardening
```
✓ Graceful shutdown coordination
✓ Backpressure metrics
✓ Circuit breaker for failing consumers
✓ Admin API for manual operations
```

### Memory Safety and Resource Limits

**Critical concern:** Unbounded IPC channels can exhaust system memory and crash the OS.

#### How Memory Can Grow Unboundedly

1. **Fast producer, slow consumer** - Producer writes faster than consumer reads
2. **Consumer crash** - Producer keeps writing, queue grows forever
3. **Backpressure failure** - Socket buffers fill but producer doesn't stop
4. **Memory leak** - Records never freed due to Arc cycles

#### Memory Allocation Model

**In-Process Channel:**
```
┌─────────────────────────────────────────────────┐
│  InProcessChannel                               │
│  ├─ Partition 0: SegQueue<Arc<StreamRecord>>   │
│  │   └─ Max capacity: 10,000 records           │
│  │   └─ Estimated size: ~1MB (100 bytes/record)│
│  ├─ Partition 1: SegQueue<Arc<StreamRecord>>   │
│  ├─ Partition 2: SegQueue<Arc<StreamRecord>>   │
│  └─ Partition 3: SegQueue<Arc<StreamRecord>>   │
│                                                  │
│  Total memory per channel: ~4MB                 │
│  With 10 channels: ~40MB                        │
└─────────────────────────────────────────────────┘
```

**Cross-Process Channel:**
```
┌─────────────────────────────────────────────────┐
│  Unix Socket Buffers (kernel-managed)          │
│  ├─ Send buffer: 256KB (SO_SNDBUF)             │
│  ├─ Receive buffer: 256KB (SO_RCVBUF)          │
│  │                                              │
│  Per connection: ~512KB                         │
│  With 100 connections: ~50MB                    │
└─────────────────────────────────────────────────┘
```

#### Strategy 1: Bounded Queues with Backpressure (Primary Defense)

```rust
pub struct BoundedPartition {
    queue: Arc<SegQueue<Arc<StreamRecord>>>,
    capacity: usize,  // Hard limit
    current_size: AtomicUsize,
    backpressure_strategy: BackpressureStrategy,
}

impl BoundedPartition {
    pub async fn push(&self, record: Arc<StreamRecord>) -> Result<()> {
        loop {
            let current = self.current_size.load(Ordering::Relaxed);
            
            if current >= self.capacity {
                // Queue is full - apply backpressure
                match self.backpressure_strategy {
                    BackpressureStrategy::Block { timeout } => {
                        // Block producer until space available
                        metrics::increment_counter!("ipc.backpressure.blocks");
                        
                        match timeout {
                            Some(t) => {
                                tokio::time::timeout(t, self.wait_for_space()).await??;
                            }
                            None => {
                                self.wait_for_space().await?;
                            }
                        }
                    }
                    BackpressureStrategy::DropOldest => {
                        // Drop oldest record to make space
                        metrics::increment_counter!("ipc.backpressure.drops");
                        if let Some(_dropped) = self.queue.pop() {
                            self.current_size.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                    BackpressureStrategy::Error => {
                        // Return error to producer
                        return Err(Error::QueueFull);
                    }
                }
            }
            
            // Try to push
            if self.current_size.compare_exchange(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed
            ).is_ok() {
                self.queue.push(record);
                return Ok(());
            }
        }
    }
    
    async fn wait_for_space(&self) -> Result<()> {
        // Poll every 10ms until space available
        loop {
            if self.current_size.load(Ordering::Relaxed) < self.capacity {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
```

**Default limits:**
```yaml
memory:
  max_records_per_partition: 10000      # ~1MB per partition
  max_partitions_per_channel: 16       # Max 16 partitions
  max_channels: 100                     # Max 100 channels
  # Total memory: 10000 * 16 * 100 * 100 bytes = ~1.6GB worst case
  
backpressure:
  strategy: block      # Block producer (no data loss)
  timeout: 30s         # Error after 30s of backpressure
```

#### Strategy 2: Memory Budget per Channel

```rust
pub struct ChannelMemoryBudget {
    max_bytes: usize,  // e.g., 10MB per channel
    current_bytes: AtomicUsize,
    record_size_estimator: RecordSizeEstimator,
}

impl ChannelMemoryBudget {
    pub fn try_allocate(&self, record: &StreamRecord) -> Result<()> {
        let record_size = self.record_size_estimator.estimate(record);
        
        loop {
            let current = self.current_bytes.load(Ordering::Relaxed);
            
            if current + record_size > self.max_bytes {
                return Err(Error::MemoryBudgetExceeded {
                    current: current,
                    limit: self.max_bytes,
                    requested: record_size,
                });
            }
            
            if self.current_bytes.compare_exchange(
                current,
                current + record_size,
                Ordering::Relaxed,
                Ordering::Relaxed
            ).is_ok() {
                return Ok(());
            }
        }
    }
    
    pub fn release(&self, record_size: usize) {
        self.current_bytes.fetch_sub(record_size, Ordering::Relaxed);
    }
}

// Estimate record size (with overhead)
pub struct RecordSizeEstimator;

impl RecordSizeEstimator {
    pub fn estimate(&self, record: &StreamRecord) -> usize {
        // Base Arc overhead
        let arc_overhead = std::mem::size_of::<Arc<StreamRecord>>();
        
        // Record data size
        let data_size = record.estimate_size();
        
        // Queue node overhead (~24 bytes for SegQueue)
        let queue_overhead = 24;
        
        arc_overhead + data_size + queue_overhead
    }
}
```

**Configuration:**
```yaml
memory_budget:
  per_channel_mb: 10              # 10MB per channel
  per_partition_mb: 2             # 2MB per partition (fallback)
  total_ipc_memory_mb: 1000       # 1GB total for all IPC
  enforcement: soft               # soft | hard
```

#### Strategy 3: Per-Socket Memory Limits (Kernel-Level)

Unix sockets have **kernel-enforced** limits that prevent unbounded growth:

```rust
pub fn configure_socket_memory(stream: &UnixStream) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    use libc::{setsockopt, SOL_SOCKET, SO_SNDBUF, SO_RCVBUF};
    
    let fd = stream.as_raw_fd();
    
    // Set send buffer size (256KB)
    let send_buf_size: i32 = 256 * 1024;
    unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_SNDBUF,
            &send_buf_size as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
    }
    
    // Set receive buffer size (256KB)
    let recv_buf_size: i32 = 256 * 1024;
    unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_RCVBUF,
            &recv_buf_size as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
    }
    
    Ok(())
}
```

**Kernel limits prevent runaway growth:**
- When send buffer fills → `write()` blocks or returns `EAGAIN`
- Kernel manages memory, not application
- Per-socket limit is **hard enforced** by OS

**Typical socket buffer sizes:**
```
Default: 4-8KB per socket
Configurable: 4KB - 4MB per socket
Our setting: 256KB per socket
With 100 connections: 25MB total (bounded)
```

#### Strategy 4: System-Wide Resource Limits

```rust
pub struct SystemResourceLimits {
    max_memory_mb: usize,      // Total memory limit
    max_connections: usize,    // Total connection limit
    max_channels: usize,       // Total channel limit
    oom_handler: OomHandler,
}

impl SystemResourceLimits {
    pub fn check_before_allocate(&self) -> Result<()> {
        // Check current memory usage
        let current_memory_mb = get_process_memory_mb();
        
        if current_memory_mb > self.max_memory_mb {
            error!(
                "Memory limit exceeded: {} MB / {} MB",
                current_memory_mb,
                self.max_memory_mb
            );
            
            // Trigger emergency measures
            self.oom_handler.handle_oom();
            
            return Err(Error::MemoryLimitExceeded);
        }
        
        Ok(())
    }
}

pub enum OomHandler {
    Fail,           // Return error, stop accepting writes
    DropOldest,     // Drop oldest records across all channels
    Shutdown,       // Graceful shutdown
}

fn get_process_memory_mb() -> usize {
    // Read from /proc/self/status on Linux
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                // Parse RSS in KB
                if let Some(kb) = line.split_whitespace().nth(1) {
                    if let Ok(kb_val) = kb.parse::<usize>() {
                        return kb_val / 1024;  // Convert to MB
                    }
                }
            }
        }
    }
    0
}
```

#### Strategy 5: Memory Monitoring and Alerts

```rust
pub struct MemoryMonitor {
    check_interval: Duration,  // Default: 10s
    warn_threshold: f64,       // Default: 0.7 (70%)
    critical_threshold: f64,   // Default: 0.9 (90%)
}

impl MemoryMonitor {
    pub async fn monitoring_loop(&self) {
        let mut interval = tokio::time::interval(self.check_interval);
        
        loop {
            interval.tick().await;
            
            let memory_mb = get_process_memory_mb();
            let limit_mb = 1000;  // From config
            let usage_ratio = memory_mb as f64 / limit_mb as f64;
            
            // Update metrics
            metrics::gauge!("ipc.memory.usage_mb", memory_mb as f64);
            metrics::gauge!("ipc.memory.usage_ratio", usage_ratio);
            
            // Alert on thresholds
            if usage_ratio > self.critical_threshold {
                error!(
                    "CRITICAL: Memory usage at {:.1}% ({} MB / {} MB)",
                    usage_ratio * 100.0,
                    memory_mb,
                    limit_mb
                );
                
                // Take action
                self.trigger_emergency_cleanup();
            } else if usage_ratio > self.warn_threshold {
                warn!(
                    "WARNING: Memory usage at {:.1}% ({} MB / {} MB)",
                    usage_ratio * 100.0,
                    memory_mb,
                    limit_mb
                );
            }
        }
    }
    
    fn trigger_emergency_cleanup(&self) {
        // Drop oldest records across all channels
        // Force flush buffers
        // Reject new connections
        // Alert ops team
    }
}
```

#### Recommended Memory Configuration

**Conservative (1GB total IPC memory):**
```yaml
memory_limits:
  # Per-partition limits
  max_records_per_partition: 10000      # ~1MB @ 100 bytes/record
  
  # Per-channel limits (4 partitions default)
  partitions_per_channel: 4
  # = 4 * 1MB = 4MB per channel
  
  # System limits
  max_channels: 100
  # = 100 * 4MB = 400MB worst case
  
  # Socket buffers (100 connections)
  socket_buffer_size_kb: 256
  max_connections: 100
  # = 100 * 512KB = 50MB
  
  # Total: ~450MB + overhead = ~500MB realistic
  # With 2x safety margin = 1GB limit
  
  total_memory_limit_mb: 1000
  
backpressure:
  strategy: block
  timeout: 30s
  
monitoring:
  check_interval: 10s
  warn_threshold: 0.70      # 700MB
  critical_threshold: 0.90  # 900MB
```

**Aggressive (for high-throughput):**
```yaml
memory_limits:
  max_records_per_partition: 50000      # ~5MB per partition
  partitions_per_channel: 16
  max_channels: 50
  # = 50 * 16 * 5MB = 4GB
  
  total_memory_limit_mb: 5000           # 5GB limit
```

#### Could This Kill the OS?

**Short answer: No, with proper limits.**

**Without limits:**
- ❌ Yes - could exhaust system memory (OOM killer kills process or entire system)
- ❌ Fast producer could allocate GB/sec of memory
- ❌ Swap thrashing could freeze system

**With limits:**
- ✅ Bounded queues → Max memory predictable
- ✅ Socket buffers → Kernel enforces limits
- ✅ Backpressure → Producer blocked, can't allocate more
- ✅ System limits → Process killed before system death

**Defense in depth:**
1. **Application-level** - Bounded queues (10K records/partition)
2. **Kernel-level** - Socket buffers (256KB/connection)
3. **Process-level** - Memory monitoring (1GB limit)
4. **System-level** - Linux OOM killer (last resort)

#### Testing Memory Limits

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_bounded_queue_blocks_on_full() {
        let partition = BoundedPartition::new(100);  // 100 record limit
        
        // Fill queue
        for i in 0..100 {
            partition.push(create_record(i)).await.unwrap();
        }
        
        // Next push should block
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            partition.push(create_record(101))
        ).await;
        
        assert!(result.is_err());  // Should timeout (queue full)
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
    
    #[tokio::test]
    async fn test_memory_limit_enforced() {
        let config = MemoryConfig {
            max_memory_mb: 10,  // 10MB limit
        };
        
        let channel = IPCChannel::new("test", config);
        
        // Try to allocate 20MB (should fail)
        let large_records = create_large_records(20_000, 1024);  // 20MB
        
        let mut succeeded = 0;
        for record in large_records {
            if channel.push(record).await.is_ok() {
                succeeded += 1;
            } else {
                break;  // Hit memory limit
            }
        }
        
        // Should stop around 10MB worth of records
        assert!(succeeded < 11_000);  // Roughly 10MB
    }
}
```

#### Summary: Memory Safety Checklist

- [x] Bounded queues with configurable capacity
- [x] Backpressure on queue full (block/drop/error)
- [x] Socket buffer size limits (256KB default)
- [x] Per-channel memory budgets (10MB default)
- [x] System-wide memory limit (1GB default)
- [x] Memory monitoring with alerts (70% warn, 90% critical)
- [x] Emergency cleanup on OOM risk
- [x] Testing for memory limit enforcement

Based on typical streaming workloads:

```yaml
# config/ipc_defaults.yaml
connection:
  max_connections: 100
  idle_timeout: 300s        # 5 minutes
  cleanup_interval: 30s     # Check every 30s
  
socket:
  buffer_size: 262144       # 256KB
  write_timeout: 30s
  read_timeout: 60s
  
coordination:
  rebalance_delay: 3s       # Wait 3s after join before rebalancing
  partition_count: 4        # Default partitions per channel
  
monitoring:
  metrics_enabled: true
  log_rebalances: true
  log_timeouts: true
```

### Why These Strategies Work

1. **Connection Management**
    - 100 connections << 10,000+ typical FD limit
    - Vec operations are O(n) but n is small (< 100ms to scan 100 connections)
    - Tokio tasks are cheap (~2KB per task)

2. **Timeouts**
    - Single idle timeout is easier to reason about
    - 5 minutes is aggressive enough to catch dead clients, generous enough to handle pauses
    - 30s cleanup interval is frequent enough (6s max delay to detect timeout)

3. **Coordination**
    - In-memory state is fine for single machine (IPC is local by definition)
    - Eager rebalancing is simpler than Kafka's multi-phase protocol
    - Range assignment is predictable and easy to debug

### When to Optimize

Only optimize when you hit these limits:

- **>500 connections** → Add connection pooling
- **>10 rebalances/minute** → Add sticky assignment
- **>1% timeout false positives** → Add heartbeat protocol
- **Backpressure in metrics** → Add flow control tuning

### Phase 1: Core IPC Infrastructure (Week 1-2)

**Goals:** Basic IPC channel with in-process transport

**Tasks:**

- [ ] Create Partition (in-process queue)
- [ ] Implement IPCChannel (single partition, in-process only)
- [ ] IPCDataSource and IPCDataSink traits
- [ ] Arc-based zero-copy
- [ ] Unit tests

**Success Criteria:**

- Single partition in-process works
- 1M records/sec throughput
- <100ns latency

### Phase 2: Partitioning & Consumer Groups (Week 3-4)

**Goals:** Multiple partitions, consumer group assignment

**Tasks:**

- [ ] Multi-partition support (hash-based)
- [ ] ConsumerGroup implementation
- [ ] Round-robin partition assignment
- [ ] Integration tests

**Success Criteria:**

- 4 partitions work
- Consumer group load balancing works
- No duplicate reads within group

### Phase 3: Cross-Process Transport (Week 5-6)

**Goals:** Unix socket transport for Python

**Tasks:**

- [ ] Unix socket server/client
- [ ] Arrow IPC serialization
- [ ] Configuration handshake protocol
- [ ] Cross-process partition support

**Success Criteria:**

- Unix socket communication works
- <10µs cross-process latency
- Arrow IPC serialization verified

### Phase 4: Python Client (Week 7)

**Goals:** Python library with IPC support

**Tasks:**

- [ ] IPCReader implementation
- [ ] IPCWriter implementation
- [ ] Arrow IPC integration (zero-copy)
- [ ] Batch API
- [ ] PyPI package

**Success Criteria:**

- Python can read/write via IPC
- Zero-copy to pandas/polars works
- `pip install velostream` works

### Phase 5: Backpressure & Flow Control (Week 8)

**Goals:** Bounded queues with backpressure

**Tasks:**

- [ ] Capacity limits
- [ ] Overflow strategies
- [ ] Backpressure metrics
- [ ] Dead consumer detection

**Success Criteria:**

- Queue stays bounded
- Backpressure works (blocks)
- Metrics expose pressure

### Phase 6: External Storage Integration (Week 9)

**Goals:** S3 connector and multi-destination fan-out

**Tasks:**

- [x] Kafka source/sink (already exists)
- [ ] S3 source/sink
- [x] File source/sink (already exists)
- [ ] Multi-destination fan-out

**Success Criteria:**

- Kafka → IPC → Kafka works
- Fan-out to multiple sinks works

### Phase 7: SQL Integration (Week 10)

**Goals:** SQL syntax for IPC channels

**Tasks:**

- [ ] `ipc://` URI parser
- [ ] Configuration system
- [ ] Multi-stage pipelines
- [ ] Documentation

**Success Criteria:**

- Can use `ipc://` in SQL
- Multi-stage pipelines work
- Examples run successfully

### Phase 8: Observability (Week 11)

**Goals:** Metrics and monitoring

**Tasks:**

- [ ] Partition-level metrics
- [ ] Consumer group metrics
- [ ] Prometheus export
- [ ] CLI inspection tools

**Success Criteria:**

- Can see partition assignments
- Can monitor lag per partition
- Prometheus metrics available

## Success Metrics

### Performance Targets

| Metric | Target |
|--------|--------|
| In-process throughput | >5M records/sec |
| Cross-process throughput | >100K records/sec |
| In-process latency (p50) | <100ns |
| Cross-process latency (p50) | <10µs |
| End-to-end (4 stages) | 20-50ms |
| Python IPC latency | <10µs |

### Business Targets

| Metric | Year 1 | Year 2 | Year 3 |
|--------|--------|--------|--------|
| Customers | 10 | 30 | 60 |
| ARR | $3.6M | $16M | $33M |
| Avg deal size | $360K | $533K | $550K |
| TCO savings vs Flink | 40% | 50% | 50% |

### Functional Goals

- [ ] Zero-copy in-process (Arc-based)
- [ ] Sub-10µs Python IPC (Unix socket + Arrow)
- [ ] Kafka-style partitioning and consumer groups
- [ ] Backpressure handling (block strategy)
- [ ] External storage integration (Kafka, S3)
- [ ] Arrow IPC serialization (Python zero-copy)
- [ ] Prometheus metrics
- [ ] CLI inspection tools
- [ ] Python PyPI package

## Documentation Requirements

### User Documentation

- [ ] SQL syntax guide for `ipc://` URIs
- [ ] Python client API reference
- [ ] Partitioning guide (when to use partitions)
- [ ] Consumer groups guide (load balancing)
- [ ] Backpressure strategies
- [ ] Integration guides (Kafka, S3, QuestDB)
- [ ] Performance tuning guide

### Examples

- [ ] Basic producer/consumer (SQL + Python)
- [ ] Multi-stage pipeline (Kafka → IPC → Kafka)
- [ ] Real-time feature engineering (finance)
- [ ] Consumer groups and load balancing
- [ ] Python batch processing (Arrow zero-copy)

## Appendix: Complete End-to-End Example

### Multi-stage pipeline: Kafka → IPC stages → Kafka/S3

```sql
-- Stage 1: Durable input from Kafka
CREATE STREAM raw_orders AS
SELECT * FROM kafka://orders_topic
INTO ipc://raw_data;

-- Stage 2: SQL validation (in-process: <100µs)
CREATE STREAM validated_orders AS
SELECT order_id, customer_id, amount, order_time
FROM ipc://raw_data
WHERE amount > 0 AND customer_id IS NOT NULL
INTO ipc://validated
WITH (
    'ipc://validated.partitions' = '4',
    'ipc://validated.partition_key' = 'customer_id'
);
```

**Python feature engineering (Stage 3):**

```python
from velostream import IPCReader, IPCWriter

reader = IPCReader("ipc://validated", group_id="feature_processors")
writer = IPCWriter("ipc://features")

for order in reader:  # ~5µs per read
    features = {
        'order_id': order['order_id'],
        'customer_id': order['customer_id'],
        'amount': order['amount'],
        'feature_score': compute_proprietary_score(order),
        'fraud_probability': fraud_model.predict(order),
    }
    writer.write(features)  # ~5µs per write
```

```sql
-- Stage 4: SQL aggregation (in-process: <100µs)
CREATE STREAM enriched_orders AS
SELECT
    customer_id,
    AVG(feature_score) OVER (RANGE '1m') as avg_score,
    COUNT(*) OVER (RANGE '1m') as order_count
FROM ipc://features
WINDOW TUMBLING(1m)
WHERE feature_score > 0.5
INTO ipc://signals;

-- Stage 5: Fan-out to multiple destinations
CREATE STREAM outputs AS
SELECT * FROM ipc://signals
INTO kafka://high_value_customers,        -- Real-time
     s3://archive/signals/,               -- Compliance
     questdb://customer_signals_table;    -- Analytics
```

**End-to-end latency:** ~20ms (vs 100-200ms with Kafka between stages)

## References

- [Crossbeam Queue Documentation](https://docs.rs/crossbeam-queue/)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [Unix Domain Sockets](https://man7.org/linux/man-pages/man7/unix.7.html)
- [Flink Architecture](https://flink.apache.org/flink-architecture.html)
- [QuestDB Architecture](https://questdb.io/docs/concept/storage-model/)

---

**End of RFC**