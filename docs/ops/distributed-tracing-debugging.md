# Distributed Tracing Debugging Guide

## Overview

This guide helps you debug OpenTelemetry distributed tracing when traces don't appear in Grafana/Tempo.

## Debugging Workflow

Follow this systematic approach to identify tracing issues:

### 1. Check Application Logs for Initialization

**What to look for:**
```bash
grep -E "(OpenTelemetry|OTLP|Phase 4.*tracing)" /path/to/app.log
```

**Expected output:**
```
[INFO] 🔍 Phase 4: Initializing OpenTelemetry distributed tracing for service 'your-service'
[INFO] ✅ OTLP exporter created successfully for http://localhost:4317
[INFO] ✅ OpenTelemetry tracer initialized - spans will be exported to Tempo
[INFO] ✅ Phase 4: Distributed tracing initialized
```

**Troubleshooting:**
- ❌ **No messages**: Tracing is not enabled in configuration
  - Check `enable_distributed_tracing` flag
  - Verify `tracing_config` is set in StreamingConfig
- ❌ **"Failed to create OTLP exporter"**: Connection issue to Tempo
  - Verify Tempo is running: `docker ps | grep tempo`
  - Check Tempo endpoint is accessible: `curl http://localhost:4317`
  - Verify firewall rules allow port 4317

### 2. Check for Span Creation (Requires DEBUG Logging)

**Enable debug logging:**
```bash
RUST_LOG=debug your_app [args]
```

**What to look for:**
```bash
grep "🔍.*span" /path/to/app.log | head -20
```

**Expected output (per-record spans):**
```
[DEBUG] 🔍 Started record span: process:my_job (exporting to Tempo)
[DEBUG] 🔍 Injecting traceparent header: 00-{trace_id}-{span_id}-01
```

**Note**: Velostream uses head-based per-record sampling. Only sampled records produce spans. If `sampling_ratio` is low, most records won't generate span messages — this is expected behavior. Check Prometheus metrics for full operational visibility regardless of sampling.

**Troubleshooting:**
- ❌ **No span messages**: Instrumentation code not being executed
  - Check that `obs_lock.telemetry()` returns `Some(...)`
  - Verify observability manager was initialized
  - Check `sampling_ratio` is > 0.0 (0.0 means no records sampled)
- ❌ **All records sampled unexpectedly**: Check `sampling_ratio` configuration
  - Records with existing `traceparent` flag `01` are always sampled regardless of ratio

### 3. Query Tempo API to Verify Trace Ingestion

**Check if Tempo is receiving traces:**
```bash
# Search for traces by service name
curl -s "http://localhost:3200/api/search?tags=service.name=YOUR_SERVICE_NAME&limit=10" | jq

# Check Tempo health
curl -s "http://localhost:3200/ready"

# Get trace by ID (if you have one)
curl -s "http://localhost:3200/api/traces/TRACE_ID" | jq
```

**Expected output:**
```json
{
  "traces": [
    {
      "traceID": "abc123...",
      "rootServiceName": "velo-sql-your-app",
      "rootTraceName": "sql_query:select",
      "startTimeUnixNano": "1234567890..."
    }
  ],
  "metrics": {
    "completedJobs": 1,
    "totalJobs": 1
  }
}
```

**Troubleshooting:**
- ❌ **`"traces": []`**: No traces in Tempo
  - Spans may not be exported yet (batching delay: up to 5 seconds)
  - Check OTLP exporter errors in application logs
  - Verify network connectivity: `telnet localhost 4317`
- ❌ **Connection refused**: Tempo not running
  - Start Tempo: `docker-compose up -d tempo`
  - Check Tempo logs: `docker logs velo-tempo`
- ❌ **404 Not Found**: Wrong Tempo version or API endpoint
  - Verify Tempo version supports `/api/search`
  - Check Tempo configuration file

### 4. Verify Grafana-Tempo Connection

**Access Grafana:**
```
http://localhost:3000
```

**Steps to verify:**
1. Navigate to **Configuration** → **Data Sources**
2. Find **Tempo** data source
3. Click **Save & Test**

**Expected result:**
```
✓ Data source is working
```

**Check Tempo data source configuration:**
```yaml
URL: http://tempo:3200
Access: Server (proxy)
```

**Troubleshooting:**
- ❌ **Data source connection failed**: DNS or network issue
  - Verify Grafana can reach Tempo: `docker exec velo-grafana ping tempo`
  - Check docker-compose network configuration
  - Try using IP address instead of hostname
- ❌ **Timeout**: Tempo is slow or not responding
  - Check Tempo resource usage: `docker stats velo-tempo`
  - Review Tempo logs: `docker logs velo-tempo | tail -50`

### 5. Query Traces in Grafana Explore

**Navigate to Explore:**
1. Click **Explore** (compass icon)
2. Select **Tempo** data source
3. Choose **Search** tab
4. Set **Service Name** = your service name
5. Click **Run Query**

**Expected result:**
- List of traces with trace IDs
- Click a trace to see span timeline

**Troubleshooting:**
- ❌ **No traces found**:
  - Check time range (default: last 6 hours)
  - Verify service name matches exactly (case-sensitive)
  - Confirm traces exist in Tempo (step 3)
- ❌ **Trace ID shown but no data**: Data ingestion issue
  - Check Tempo storage: `docker exec velo-tempo ls -la /tmp/tempo/blocks`
  - Review Tempo compaction settings

## Common Issues and Solutions

### Issue: Traces appear in Tempo API but not in Grafana

**Cause**: Grafana cache or query issue

**Solution**:
```bash
# Restart Grafana
docker restart velo-grafana

# Clear browser cache
# Use Incognito/Private mode to test
```

### Issue: Spans created but batching delay too long

**Cause**: Default batch exporter settings

**Solution**: Reduce batch timeout in telemetry.rs:
```rust
let provider = TracerProvider::builder()
    .with_batch_exporter(exporter, runtime::Tokio)
    .with_batch_config(
        opentelemetry_sdk::trace::BatchConfig::default()
            .with_max_export_timeout(Duration::from_secs(1))  // Reduce from default 30s
            .with_scheduled_delay(Duration::from_millis(500))  // Export every 500ms
    )
    // ...
```

### Issue: Too many traces causing performance issues

**Cause**: High sampling ratio in development

**Solution**: Use a named sampling mode to set the ratio and export pipeline together:

```bash
# Recommended: use --sampling-mode to set ratio + flush interval + export timeout
velo-sql deploy-app --file app.sql --enable-tracing --sampling-mode prod   # 1% sampling
velo-sql deploy-app --file app.sql --enable-tracing --sampling-mode staging # 25% (default)

# Override ratio while keeping the mode's flush/timeout settings
velo-sql deploy-app --file app.sql --enable-tracing --sampling-mode prod --sampling-ratio 0.05
```

| Mode | Ratio | Flush Interval | Export Timeout | Use Case |
|------|-------|----------------|----------------|----------|
| `debug` | 1.0 (100%) | 500ms | 5s | Full visibility, local dev |
| `dev` | 0.5 (50%) | 1000ms | 5s | Team dev environments |
| `staging` | 0.25 (25%) | 2000ms | 10s | Pre-prod validation (**default**) |
| `prod` | 0.01 (1%) | 5000ms | 30s | Live traffic |

**How sampling works**:
- Records with an existing `traceparent` flag `01` → always sampled (continue chain)
- Records with an existing `traceparent` flag `00` → never sampled (respect upstream decision)
- Records with no `traceparent` → probabilistic dice roll against `sampling_ratio`
- Non-sampled records get a `traceparent` with flag `00` injected to prevent downstream re-rolling

### Issue: Spans not linked across services

**Cause**: Missing trace context propagation

**Solution**: Velostream automatically handles W3C `traceparent` propagation through Kafka headers. For each sampled record, the processor creates a `RecordSpan` linked to the upstream trace and injects the new span's context into output records. This chain propagates across all processor types (Simple, Transactional, Partition, Join).

If you need to propagate trace context to **non-Kafka** downstream services (e.g., HTTP APIs), extract the `traceparent` header from the output record:
```sql
-- Read the auto-injected traceparent for forwarding to HTTP services
SELECT HEADER('traceparent') as trace_context, ...
FROM my_stream;
```

### Issue: Span drops under high load

**Cause**: Export queue full — spans are produced faster than the OTLP exporter can send them.

**Solution**: Velostream includes adaptive backpressure. When the span export queue exceeds 80% capacity, the effective sampling ratio is automatically reduced (floored at 5% of the base ratio). This is logged at `debug` level:

```
Adaptive sampling: pressure=0.85, base=0.25, effective=0.0375
```

You can also tune the export pipeline via `TracingConfig`:

| Field | Default | Description |
|-------|---------|-------------|
| `max_queue_size` | 65536 | Max spans queued before dropping |
| `max_export_batch_size` | 2048 | Spans per export HTTP call |
| `export_flush_interval_ms` | 2000 | Interval between batch flushes |
| `export_timeout_seconds` | 10 | Timeout per export call |

OTLP HTTP payloads are gzip-compressed automatically (typically 5-10x reduction).

## Performance Monitoring

### Check Span Export Rate

```bash
# Count spans in logs (with RUST_LOG=debug)
grep "span" /path/to/app.log | wc -l

# Monitor Tempo ingestion rate
curl -s http://localhost:3200/metrics | grep tempo_ingester_traces_created_total
```

### Monitor Trace Storage Growth

```bash
# Check Tempo storage size
du -sh /tmp/tempo/blocks

# Check Tempo memory usage
docker stats velo-tempo --no-stream
```

## Log Level Configuration

Different log levels show different tracing information:

| Level | What You See |
|-------|-------------|
| `ERROR` | Span export failures, critical errors |
| `WARN` | Failed spans with error details |
| `INFO` | Initialization messages, Phase 4 status |
| `DEBUG` | Individual span creation, completion times |
| `TRACE` | Detailed span attributes, execution flow |

**Recommended for debugging:**
```bash
RUST_LOG=debug                    # See all span activity
RUST_LOG=velostream=debug         # Only Velostream spans
RUST_LOG=velostream::observability=trace  # Extreme detail
```

## Quick Diagnostic Script

Save as `check_tracing.sh`:

```bash
#!/bin/bash

echo "=== Distributed Tracing Diagnostics ==="
echo

echo "1. Checking Tempo container..."
docker ps --filter "name=tempo" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo

echo "2. Checking Tempo health..."
curl -s http://localhost:3200/ready && echo " ✓ Tempo is ready" || echo " ✗ Tempo not responding"
echo

echo "3. Querying for traces..."
TRACES=$(curl -s "http://localhost:3200/api/search?limit=10" | jq '.traces | length')
echo "Found $TRACES recent traces"
echo

echo "4. Checking Grafana connection to Tempo..."
curl -s http://localhost:3000/api/datasources | jq '.[] | select(.type=="tempo") | {name, url, access}'
echo

echo "5. Recent Tempo logs..."
docker logs velo-tempo --tail 10
echo

echo "=== Diagnostic complete ==="
```

Run with: `chmod +x check_tracing.sh && ./check_tracing.sh`

## References

- [OpenTelemetry Rust SDK](https://github.com/open-telemetry/opentelemetry-rust)
- [Grafana Tempo Documentation](https://grafana.com/docs/tempo/latest/)
- [OTLP Protocol Specification](https://opentelemetry.io/docs/specs/otlp/)
- [Velostream Observability Architecture](../architecture/observability-architecture.md)
