# Performance Monitoring Integration

Performance monitoring has been successfully integrated into both SQL servers with HTTP endpoints for production monitoring.

## SQL Server (`ferris-sql`)

### Usage
```bash
# Start server with performance monitoring
cargo run --bin ferris-sql server --brokers localhost:9092 --port 8080 --enable-metrics

# Or with custom metrics port
cargo run --bin ferris-sql server --brokers localhost:9092 --port 8080 --enable-metrics --metrics-port 9080
```

### Endpoints
- **Main server**: Port 8080 (SQL operations)
- **Metrics server**: Port 9080 (default: main port + 1000)

| Endpoint | Description | Content-Type |
|----------|-------------|-------------|
| `GET /` | Server info and available endpoints | `application/json` |
| `GET /metrics` | Prometheus metrics export | `text/plain` |
| `GET /health` | Health check with performance status | `application/json` |
| `GET /report` | Detailed performance report | `text/plain` |

## StreamJobServer (`ferris-sql-multi`)

### Usage
```bash
# Start StreamJobServer with monitoring
cargo run --bin ferris-sql-multi server --brokers localhost:9092 --port 8080 --enable-metrics

# Deploy app with monitoring enabled
cargo run --bin ferris-sql-multi deploy-app --file app.sql --brokers localhost:9092 --default-topic orders
```

### Endpoints
Same as SQL server plus:

| Endpoint | Description | Content-Type |
|----------|-------------|-------------|
| `GET /jobs` | List all running jobs with metrics | `application/json` |

## Example Usage

### 1. Start Server with Monitoring
```bash
# Terminal 1: Start SQL server
cargo run --bin ferris-sql server --enable-metrics

# Terminal 2: Check metrics
curl http://localhost:9080/metrics
```

### 2. Health Check
```bash
curl http://localhost:9080/health | jq
```

Example response:
```json
{
  "status": "Healthy",
  "issues": [],
  "warnings": [],
  "metrics": {
    "memory": {
      "allocated_bytes": 1048576,
      "peak_memory_bytes": 2097152
    },
    "throughput": {
      "records_per_second": 1250.5,
      "bytes_per_second": 125000.0
    },
    "total_queries": 42
  }
}
```

### 3. Prometheus Integration
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'ferris-sql-server'
    static_configs:
      - targets: ['localhost:9080']
    scrape_interval: 15s
    metrics_path: /metrics
```

### 4. Performance Report
```bash
curl http://localhost:9080/report
```

Example output:
```
=====================================
FerrisStreams Performance Report
=====================================
Generated: 2025-01-24 10:30:00 UTC
Status: Healthy

=== Overall Metrics ===
Total Queries: 1,234
Total Records Processed: 125,000
Average Query Time: 45.2ms
P95 Query Time: 128.5ms
Throughput: 127.3 records/sec

=== Recent Performance (Last Hour) ===
Queries: 245
Average Time: 42.1ms
Throughput: 156.2 records/sec

=== Top Query Patterns ===
1. SELECT * FROM USERS WHERE ID = ? - 456 executions
2. SELECT COUNT(*) FROM ORDERS - 234 executions
3. SELECT * FROM PRODUCTS WHERE PRICE > ? - 123 executions

Health Issues: None
Warnings: None
```

## Integration Benefits

1. **Zero Overhead**: Monitoring is completely optional and disabled by default
2. **Production Ready**: Prometheus metrics format for Grafana dashboards  
3. **Real-time Health**: Automated health checks with configurable thresholds
4. **Query Analysis**: Pattern recognition and slow query identification
5. **Multi-Job Support**: Job-level metrics in the multi-server

## Metrics Available

- **Query Execution**: Total time, per-processor breakdown, percentiles
- **Throughput**: Records/sec, bytes/sec, queries/sec  
- **Memory Usage**: Allocated, peak, per-component (GROUP BY, JOIN, etc.)
- **System Health**: Automated warnings for high latency, low throughput
- **Job Metrics** (multi-server): Per-job performance and resource usage

The monitoring system provides the foundation for future optimization phases by establishing baseline performance measurements and identifying bottlenecks.