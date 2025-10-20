# Node Identification via SQL Annotations

## Overview

Node/server identification should be integrated into the SQL annotation system for:
1. **Documentation**: Visible in the SQL file what node this application runs on
2. **Explicitness**: Clear intent and configuration
3. **Environment Integration**: Support environment variable fallback for flexibility
4. **Consistency**: Same pattern as existing observability annotations

## Design

### New Annotation: `@deployment.node_id`

Follows existing pattern with semantic naming (`@deployment.*`):
- `@deployment.node_id` - Primary node identifier
- `@deployment.node_name` - Human-readable node name (optional)
- `@deployment.region` - Cloud region/zone (optional)

### Syntax

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0

-- Deployment configuration (node identification)
-- @deployment.node_id: ${NODE_ID}  -- Load from NODE_ID env var, fallback to hostname
-- @deployment.node_name: trading-server-1
-- @deployment.region: us-east-1

-- Observability configuration
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true
```

---

## Environment Variable Resolution

### Syntax: `${ENV_VAR_NAME}`

**Parsing logic:**
```
1. If value contains ${VAR}: resolve from environment, fallback to literal value
2. Special case: ${NODE_ID} can fallback to hostname if not set
3. If env var not found, use literal string after ${} (for defaults)
```

**Examples:**

```sql
-- Load from NODE_ID env var, fallback to literal "node-default"
-- @deployment.node_id: ${NODE_ID:node-default}

-- Load from NODE_ID, fallback to auto-discovered hostname
-- @deployment.node_id: ${NODE_ID}

-- Literal value (no env var)
-- @deployment.node_id: production-server-1

-- Complex: env var with fallback pattern
-- @deployment.region: ${AWS_REGION:us-east-1}
```

---

## Implementation Architecture

### 1. Parsing Layer (app_parser.rs)

**New enum for deployment config:**
```rust
#[derive(Debug, Clone)]
pub struct DeploymentConfig {
    /// Node identifier (resolved from env vars or literal)
    pub node_id: String,
    /// Optional node name
    pub node_name: Option<String>,
    /// Optional region/zone
    pub region: Option<String>,
}

impl DeploymentConfig {
    /// Resolve ${ENV_VAR} syntax with fallbacks
    fn resolve_value(value: &str) -> String {
        if value.starts_with("${") && value.ends_with("}") {
            let var_spec = &value[2..value.len()-1];

            // Handle format: VAR_NAME or VAR_NAME:default_value
            if let Some((var_name, default)) = var_spec.split_once(':') {
                std::env::var(var_name).unwrap_or_else(|_| {
                    // Special case: NODE_ID can fallback to hostname
                    if var_name == "NODE_ID" {
                        hostname::get()
                            .map(|h| h.to_string_lossy().to_string())
                            .unwrap_or_else(|_| default.to_string())
                    } else {
                        default.to_string()
                    }
                })
            } else {
                // Just VAR_NAME, no default specified
                std::env::var(var_spec).unwrap_or_else(|_| {
                    // Special case: NODE_ID falls back to hostname
                    if var_spec == "NODE_ID" {
                        hostname::get()
                            .map(|h| h.to_string_lossy().to_string())
                            .unwrap_or_else(|_| format!("node-{}", uuid::Uuid::new_v4().to_string()[..8].to_string()))
                    } else {
                        var_spec.to_string() // Return the var name as literal if not found
                    }
                })
            }
        } else {
            value.to_string()
        }
    }
}
```

**Add to ApplicationMetadata:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationMetadata {
    // ... existing fields ...

    // Deployment configuration
    pub deployment_node_id: Option<String>,      // @deployment.node_id
    pub deployment_node_name: Option<String>,    // @deployment.node_name
    pub deployment_region: Option<String>,       // @deployment.region
}
```

**Update extract_metadata():**
```rust
} else if line.starts_with("-- @deployment.node_id:") {
    let raw_value = line.replace("-- @deployment.node_id:", "").trim().to_string();
    deployment_node_id = Some(DeploymentConfig::resolve_value(&raw_value));
} else if line.starts_with("-- @deployment.node_name:") {
    let raw_value = line.replace("-- @deployment.node_name:", "").trim().to_string();
    deployment_node_name = Some(DeploymentConfig::resolve_value(&raw_value));
} else if line.starts_with("-- @deployment.region:") {
    let raw_value = line.replace("-- @deployment.region:", "").trim().to_string();
    deployment_region = Some(DeploymentConfig::resolve_value(&raw_value));
}
```

---

### 2. Configuration Layer (execution/config.rs)

**Add to StreamingConfig:**
```rust
pub struct StreamingConfig {
    // ... existing fields ...

    /// Deployment node configuration
    pub deployment_node_id: Option<String>,
    pub deployment_node_name: Option<String>,
    pub deployment_region: Option<String>,
}

impl StreamingConfig {
    pub fn with_deployment_config(
        mut self,
        node_id: Option<String>,
        node_name: Option<String>,
        region: Option<String>,
    ) -> Self {
        self.deployment_node_id = node_id;
        self.deployment_node_name = node_name;
        self.deployment_region = region;
        self
    }
}
```

---

### 3. Observability Integration (observability/mod.rs)

**Pass deployment config to all sub-providers:**

```rust
pub struct ObservabilityManager {
    config: StreamingConfig,
    deployment_node_id: Option<String>,
    telemetry: Option<telemetry::TelemetryProvider>,
    metrics: Option<metrics::MetricsProvider>,
    profiling: Option<profiling::ProfilingProvider>,
    // ...
}

impl ObservabilityManager {
    pub fn initialize(&mut self) -> Result<(), SqlError> {
        // Extract deployment config
        let deployment_node_id = self.config.deployment_node_id.clone();

        // Pass to metrics
        if let Some(ref mut metrics) = self.metrics {
            metrics.set_node_id(deployment_node_id.clone())?;
        }

        // Pass to telemetry
        if let Some(ref mut telemetry) = self.telemetry {
            telemetry.set_node_id(deployment_node_id.clone())?;
        }

        // ... etc
    }
}
```

---

### 4. Metrics Layer (observability/metrics.rs)

**Store and use node_id:**

```rust
pub struct MetricsProvider {
    config: PrometheusConfig,
    registry: Registry,
    node_id: Option<String>,  // NEW
    // ... existing fields ...
}

impl MetricsProvider {
    pub fn set_node_id(&mut self, node_id: Option<String>) -> Result<(), SqlError> {
        self.node_id = node_id;
        log::info!("📊 Metrics node context: {:?}", self.node_id);
        Ok(())
    }

    /// Record query with node context
    pub fn record_sql_query_with_node(
        &self,
        query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        if self.active {
            let node_id = self.node_id.as_deref().unwrap_or("unknown");
            self.sql_metrics.record_query_with_node(
                node_id,
                query_type,
                duration,
                success,
                record_count,
            );
        }
    }
}
```

**Update metric recording with node_id label:**

```rust
struct SqlMetrics {
    // Existing metrics (no change to registration)
    query_total: IntCounterVec,           // label: node_id, operation
    query_duration: HistogramVec,         // label: node_id, operation
    query_errors: IntCounterVec,          // label: node_id
    // ... etc
}

impl SqlMetrics {
    fn record_query_with_node(
        &self,
        node_id: &str,
        _query_type: &str,
        duration: Duration,
        success: bool,
        record_count: u64,
    ) {
        self.query_total.with_label_values(&[node_id, "all"]).inc();
        self.query_duration.with_label_values(&[node_id, "all"]).observe(duration.as_secs_f64());
        self.records_processed.with_label_values(&[node_id]).inc_by(record_count);

        if !success {
            self.query_errors.with_label_values(&[node_id]).inc();
        }
    }
}
```

---

### 5. Telemetry Layer (observability/telemetry.rs)

**Add node context to Resource:**

```rust
pub struct TelemetryProvider {
    config: TracingConfig,
    node_id: Option<String>,  // NEW
    active: bool,
}

impl TelemetryProvider {
    pub fn set_node_id(&mut self, node_id: Option<String>) -> Result<(), SqlError> {
        self.node_id = node_id;
        log::info!("🔍 Tracing node context: {:?}", self.node_id);
        Ok(())
    }

    pub async fn new(config: TracingConfig, node_id: Option<String>) -> Result<Self, SqlError> {
        // ... existing setup ...

        // Enhance resource with deployment context
        let mut resource_attrs = vec![
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                config.service_name.clone(),
            ),
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                "0.1.0",
            ),
        ];

        // Add node context
        if let Some(ref node_id) = node_id {
            resource_attrs.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }

        let resource = Resource::new(resource_attrs);

        // ... continue with provider setup ...
    }
}
```

---

## SQL Usage Examples

### Example 1: Development Environment

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0
-- Description: Real-time financial analytics
-- Author: Finance Team

-- Deployment: Load node ID from environment, fallback to hostname
-- @deployment.node_id: ${NODE_ID}
-- @deployment.node_name: trading-dev-1
-- @deployment.region: us-east-1

-- Observability configuration
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: dev
-- @observability.error_reporting.enabled: true

-- @job_name: market-data-1
-- Name: Market Data Processor
CREATE STREAM market_data AS
SELECT * FROM kafka_market_data
EMIT CHANGES;
```

### Example 2: Production with Kubernetes

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0

-- Kubernetes deployment: Use pod name from HOSTNAME env var
-- @deployment.node_id: ${HOSTNAME}
-- @deployment.node_name: ${POD_NAME}
-- @deployment.region: ${KUBE_REGION:us-east-1}

-- Production observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true
```

### Example 3: Multi-Node with AWS

```sql
-- SQL Application: Multi-Region Trading
-- Version: 2.0.0

-- AWS Deployment: Use instance ID with region fallback
-- @deployment.node_id: ${INSTANCE_ID:i-default}
-- @deployment.region: ${AWS_REGION}

-- Observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
```

---

## Monitoring & Querying

### Prometheus Queries

**All metrics from specific node:**
```promql
{node_id="trading-server-1"}
```

**Compare nodes:**
```promql
sum(rate(velo_sql_queries_total[5m])) by (node_id)
```

**Per-node error rate:**
```promql
rate(velo_sql_query_errors_total[5m]) by (node_id)
```

### Tempo/Jaeger Traces

**Filter by node:**
```
service.instance.id = "trading-server-1"
```

**Span attributes:**
```
Resource {
  service.name: "velostream"
  service.instance.id: "trading-server-1"
  service.version: "0.1.0"
}
```

---

## Environment Variable Configuration

### Docker

```dockerfile
ENV NODE_ID=production-node-1
ENV POD_NAME=trading-server-1
ENV KUBE_REGION=us-east-1
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velostream
spec:
  template:
    spec:
      containers:
      - name: velostream
        env:
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: KUBE_REGION
          value: "us-east-1"
```

### Docker Compose

```yaml
services:
  velostream:
    environment:
      NODE_ID: node-1
      HOSTNAME: ${HOSTNAME}
```

### Manual Deployment

```bash
export NODE_ID="production-server-1"
export KUBE_REGION="us-east-1"
./velostream-server
```

---

## Implementation Phases

### Phase 1: Core Parsing (Week 1)
- [ ] Add `@deployment.node_id/node_name/region` annotation parsing
- [ ] Implement `${ENV_VAR}` and `${ENV_VAR:default}` resolution
- [ ] Add to `ApplicationMetadata`
- [ ] Add tests for environment variable resolution

### Phase 2: Metrics Integration (Week 2)
- [ ] Pass `node_id` from `ApplicationMetadata` to `MetricsProvider`
- [ ] Add `node_id` label to all metric registrations
- [ ] Update metric recording methods to include node context
- [ ] Add tests for node-labeled metrics

### Phase 3: Tracing Integration (Week 3)
- [ ] Add `service.instance.id` to OpenTelemetry Resource
- [ ] Pass deployment config to `TelemetryProvider`
- [ ] Update span attributes with node context
- [ ] Add tests for span attributes

### Phase 4: Error Reporting & Profiling (Week 4)
- [ ] Update error metrics with node_id
- [ ] Pass node context to profiling provider
- [ ] End-to-end testing

### Phase 5: Documentation & Examples (Week 5)
- [ ] Update SQL annotation reference docs
- [ ] Create deployment guide with examples
- [ ] Add Grafana dashboard templates
- [ ] Create troubleshooting guide

---

## Benefits

✅ **Documentation**: Node ID is visible in SQL file
✅ **Flexibility**: Environment variable support + fallback
✅ **Consistency**: Follows existing annotation pattern
✅ **Debuggability**: Node context in all metrics, traces, errors
✅ **Multi-node**: Easy to aggregate/compare across nodes
✅ **Kubernetes-native**: Supports pod name, namespace, region
✅ **Zero Breaking Changes**: Fully backward compatible (optional)

---

## Related Files to Update

1. `src/velostream/sql/app_parser.rs` - Add parsing
2. `src/velostream/sql/execution/config.rs` - Add to StreamingConfig
3. `src/velostream/observability/mod.rs` - Pass to sub-providers
4. `src/velostream/observability/metrics.rs` - Add node_id label
5. `src/velostream/observability/telemetry.rs` - Add to Resource
6. `docs/sql/deployment/sql-application-annotations.md` - Update docs
7. `docs/observability/` - Create deployment guide

---

## Questions for Confirmation

1. Should we support only app-level `@deployment.node_id`? (Recommended: yes, simpler)
2. Should per-job overrides be supported? (Recommended: no, node ID is global)
3. Should we provide helper annotations like `@deployment.auto_detect_node_id`? (Recommended: no, `${NODE_ID}` is clear enough)
4. Default fallback order: `${NODE_ID}` → hostname → UUID?
