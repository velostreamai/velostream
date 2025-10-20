# Node/Server Identification in Observability

## Problem Statement

Observability metrics, error reporting, and tracing currently lack contextual information about the node or server instance where they are running. This makes it difficult to:
- Identify which node emitted a metric in multi-node deployments
- Correlate errors across a distributed system
- Debug issues in specific nodes
- Aggregate metrics by node/instance

## Current Architecture Analysis

### Observability Components

**1. Metrics (Prometheus)**
- Location: `src/velostream/observability/metrics.rs`
- Current labels: `operation`, `status`, `source`, `message`
- Currently uses GaugeVec, IntCounterVec, HistogramVec with simple label dimensions
- No node/instance label on any existing metric

**2. Tracing (OpenTelemetry)**
- Location: `src/velostream/observability/telemetry.rs`
- Resource attributes: Only `service.name` and `service.version` (lines 65-74)
- Span attributes: `job.name`, `batch.id`, `messaging.system`, `db.system`, `db.operation`, etc.
- No node/instance identifier in resource or span attributes

**3. Profiling**
- Location: `src/velostream/observability/profiling.rs`
- Not yet analyzed in detail, but likely lacks node context

**4. Error Reporting**
- Location: `src/velostream/observability/error_tracker.rs`
- Error tracking: Messages, counts, timestamps
- No node/instance label in error metrics

### Label System (Current)

**Prometheus Label Patterns:**
```rust
// Streaming metrics
operations_total.with_label_values(&["operation"]).inc()
operation_duration.with_label_values(&["operation"]).observe(duration)
throughput.with_label_values(&["operation"]).set(value)

// Dynamic metrics (user-defined)
counters.with_label_values(&["status", "source"]).inc()

// Error metrics
error_message_gauge.with_label_values(&["message"]).set(count)
```

**OpenTelemetry Attributes:**
```rust
// Resource-level (global, set once at initialization)
KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_NAME, config.service_name)
KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_VERSION, "0.1.0")

// Span-level (set per span)
KeyValue::new("job.name", job_name)
KeyValue::new("batch.id", batch_id)
KeyValue::new("db.operation", operation_name)
```

## Node Identification Options

### Option 1: Environment Variable + Auto-Discovery ⭐ RECOMMENDED

**Pros:**
- Zero code changes needed (just add label to all metrics)
- Works in containerized environments (Kubernetes)
- Standard approach (used by Prometheus exporters)
- Easy to set in deployment manifests
- Supports both static IDs and dynamic discovery (hostname, pod name)

**Cons:**
- Requires environment variable to be set
- User responsibility to ensure uniqueness

**Implementation:**
```
NODE_ID=node-1
NODE_NAME=server.example.com
INSTANCE_ID=prod-us-east-1-i-abc123
```

---

### Option 2: Hostname-Based ⭐ STRONG ALTERNATIVE

**Pros:**
- Automatic, no configuration needed
- Unique in most deployments
- Works in Kubernetes (pod name)
- Works in VMs/bare metal (hostname)

**Cons:**
- May not be unique if pods are renamed
- Less control than explicit ID

**Implementation:**
```rust
let hostname = hostname::get()
    .unwrap_or_default()
    .to_string_lossy()
    .to_string();
```

---

### Option 3: Configuration File

**Pros:**
- Explicit, clear intent
- Can store multiple metadata

**Cons:**
- Extra configuration overhead
- Adds complexity
- Not ideal for containerized environments

---

### Option 4: Pod/Container Name (Kubernetes-only)

**Pros:**
- Perfect for Kubernetes

**Cons:**
- Doesn't work in non-Kubernetes environments
- K8s specific

**Implementation:**
```rust
std::env::var("HOSTNAME").unwrap_or_default() // Pod name in K8s
```

---

### Option 5: Service Discovery

**Pros:**
- Dynamic registration

**Cons:**
- Added complexity
- Extra network calls
- Overkill for most use cases

---

## Recommended Approach: Hybrid (Environment + Hostname Fallback)

### Strategy

1. **Try environment variable first**: `NODE_ID` (user-provided)
2. **Fall back to hostname**: `gethostname::gethostname()`
3. **Generate fallback**: UUID if neither available (extreme cases)

### Configuration

```rust
pub struct NodeConfig {
    /// Unique identifier for this node/server instance
    pub node_id: String,
    /// Optional node name for display purposes
    pub node_name: Option<String>,
    /// Optional region/zone for grouping
    pub region: Option<String>,
    /// Optional pod namespace (Kubernetes)
    pub namespace: Option<String>,
}

impl NodeConfig {
    pub fn new() -> Self {
        let node_id = std::env::var("NODE_ID")
            .or_else(|_| std::env::var("INSTANCE_ID"))
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| {
                format!("node-{}", uuid::Uuid::new_v4().to_string()[..8].to_string())
            });

        Self {
            node_id,
            node_name: std::env::var("NODE_NAME").ok(),
            region: std::env::var("NODE_REGION").ok(),
            namespace: std::env::var("NAMESPACE").ok(),
        }
    }
}
```

---

## Integration Points

### 1. Metrics (Prometheus) - ADD NODE_ID LABEL

**Current:**
```rust
velo_sql_queries_total{operation="select"} 42
velo_streaming_operations_total{operation="deserialization"} 100
```

**After:**
```rust
velo_sql_queries_total{node_id="node-1", operation="select"} 42
velo_streaming_operations_total{node_id="node-1", operation="deserialization"} 100
```

**Changes needed:**
- Add `node_id` to all MetricVec registrations
- Pass `node_id` to all `with_label_values()` calls
- Pre-append node_id as first label in all metrics

**Files affected:**
- `src/velostream/observability/metrics.rs` - Register node_id label on all metrics
- `src/velostream/server/stream_job_server.rs` - Pass node_id when recording metrics

---

### 2. Tracing (OpenTelemetry) - ADD NODE_ID TO RESOURCE

**Current resource:**
```
Resource {
  service.name: "velostream"
  service.version: "0.1.0"
}
```

**After:**
```
Resource {
  service.name: "velostream"
  service.version: "0.1.0"
  service.instance.id: "node-1"
  host.name: "server.example.com"
  host.id: "i-abc123"
}
```

**Changes needed:**
- Add `NodeConfig` to `TracingConfig`
- Add semantic convention attributes to Resource:
  - `service.instance.id` (primary node identifier)
  - `host.name` (hostname)
  - `host.id` (cloud instance ID)
  - `cloud.region` (AWS/GCP region)

**Files affected:**
- `src/velostream/observability/telemetry.rs` - Add node attributes to Resource
- `src/velostream/sql/execution/config.rs` - Add NodeConfig to TracingConfig

---

### 3. Error Reporting - ADD NODE_ID TO ERROR METRICS

**Current error message gauge:**
```rust
velo_error_message{message="Connection timeout"} 5
```

**After:**
```rust
velo_error_message{node_id="node-1", message="Connection timeout"} 5
```

**Changes needed:**
- Modify error metric registration to include node_id label
- Update error message gauge label set

**Files affected:**
- `src/velostream/observability/error_tracker.rs`
- `src/velostream/observability/metrics.rs` (error metric registration)

---

### 4. Profiling - ADD NODE_ID CONTEXT

**Changes needed:**
- Pass `NodeConfig` to `ProfilingProvider`
- Include node_id in profiling span attributes or metadata
- Tag profile samples with node identifier

**Files affected:**
- `src/velostream/observability/profiling.rs`
- `src/velostream/observability/mod.rs`

---

## Implementation Priority

### Phase 1 (Critical)
- [ ] Create `NodeConfig` struct with hybrid initialization
- [ ] Add `node_id` label to ALL Prometheus metrics
- [ ] Add `service.instance.id` to OpenTelemetry Resource
- [ ] Update error message metrics with `node_id` label

### Phase 2 (Important)
- [ ] Add span attributes for node context (optional secondary labels)
- [ ] Add node info to profiling metadata
- [ ] Create documentation/examples

### Phase 3 (Nice-to-have)
- [ ] Grafana dashboard filtering by node_id
- [ ] Alerting rules grouped by node
- [ ] Node health/availability checks

---

## Deployment Configuration Examples

### Docker
```dockerfile
ENV NODE_ID=production-node-1
ENV NODE_NAME=server.prod.example.com
ENV NODE_REGION=us-east-1
```

### Kubernetes
```yaml
env:
  - name: NODE_ID
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
```

### Docker Compose
```yaml
services:
  velostream:
    environment:
      NODE_ID: node-1
      NODE_NAME: ${HOSTNAME}
```

### Bare Metal / VM
```bash
export NODE_ID="production-server-1"
export NODE_REGION="us-west-2"
./velostream-server
```

---

## Query Examples (After Implementation)

### Prometheus

**All metrics from a specific node:**
```promql
{node_id="node-1"}
```

**Compare queries across nodes:**
```promql
sum(velo_sql_queries_total) by (node_id)
```

**Per-node error rate:**
```promql
rate(velo_sql_query_errors_total[5m]) by (node_id)
```

### Grafana Dashboard

**Variable:**
```
node_id: label_values(velo_sql_queries_total, node_id)
```

**Panel (Node Selector):**
```
sum(velo_sql_queries_total{node_id="$node_id"}) by (operation)
```

### Tempo / Jaeger (Tracing)

**Filter spans by node:**
```
service.instance.id = "node-1"
```

**Trace drill-down:**
- Service: velostream
- Node: node-1
- Job: market-data-1
- Trace duration: 125ms

---

## Semantic Conventions Reference

OpenTelemetry semantic conventions for node/instance identification:

| Attribute | Type | Example | Purpose |
|-----------|------|---------|---------|
| `service.instance.id` | string | `node-1` | **Primary node identifier** |
| `host.name` | string | `server.example.com` | Hostname |
| `host.id` | string | `i-abc123` | Cloud instance ID (AWS, GCP) |
| `host.arch` | string | `x86_64` | Architecture |
| `cloud.provider` | string | `aws` | Cloud provider |
| `cloud.region` | string | `us-east-1` | Cloud region |
| `cloud.availability_zone` | string | `us-east-1a` | Availability zone |

---

## Next Steps

1. **Decision**: Confirm hybrid approach (env var + hostname fallback)
2. **Create**: `NodeConfig` struct and initialization
3. **Integrate**: Add to `ObservabilityManager` and all sub-providers
4. **Update**: Metrics registration to include `node_id` label
5. **Update**: Telemetry resource to include node attributes
6. **Test**: Multi-node metrics aggregation
7. **Document**: Deployment configuration guide
