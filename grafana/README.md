# FerrisStreams Grafana Dashboard Setup

This directory contains the complete Grafana monitoring setup for FerrisStreams SQL Engine, including Prometheus configuration and a comprehensive dashboard.

## Quick Start

1. **Start the monitoring stack:**
   ```bash
   docker-compose up -d
   ```

2. **Access the dashboard:**
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090

3. **Start FerrisStreams with observability enabled:**
   ```rust
   let config = StreamingConfig {
       prometheus: Some(PrometheusConfig::default()), // Exposes metrics on :9091
       // ... other config
   };
   ```

## Dashboard Features

### Core Metrics Panels
- **SQL Query Rate**: Real-time query throughput
- **SQL Query Duration**: 50th and 95th percentile latencies
- **Streaming Operations Rate**: Streaming operation throughput
- **Streaming Throughput**: Current records per second
- **System CPU Usage**: Real-time CPU monitoring with thresholds
- **System Memory Usage**: Memory consumption tracking
- **Active Connections**: Connection pool monitoring
- **SQL Error Rate**: Error percentage over time
- **Record Processing Rate**: Combined SQL and streaming record rates
- **Streaming Operation Duration**: Streaming latency percentiles

### Key Features
- **5-second refresh rate** for real-time monitoring
- **Color-coded thresholds** (Green → Yellow → Red)
- **Percentile analysis** for performance characterization
- **Rate calculations** for throughput analysis
- **Auto-refreshing panels** with 1-hour time window

## Files Overview

```
grafana/
├── docker-compose.yml                 # Complete monitoring stack
├── prometheus.yml                     # Prometheus configuration
├── ferris-streams-dashboard.json      # Main Grafana dashboard
├── grafana-provisioning/              # Auto-provisioning config
│   ├── datasources/
│   │   └── prometheus.yml            # Prometheus datasource
│   └── dashboards/
│       └── ferris-streams.yml        # Dashboard provider config
└── README.md                         # This file
```

## Configuration Details

### Prometheus Setup
- **Scrape interval**: 5 seconds for high-resolution monitoring
- **Target**: `host.docker.internal:9091` (FerrisStreams metrics endpoint)
- **Retention**: Standard Prometheus defaults
- **Evaluation**: 15-second rule evaluation

### Grafana Setup
- **Admin credentials**: admin/admin (change in production)
- **Auto-provisioning**: Datasource and dashboard auto-loaded
- **Dashboard permissions**: Editable for customization
- **Time range**: Last 1 hour with 5-second refresh

## Customization

### Adding Custom Panels

1. **Edit the dashboard JSON** or use Grafana UI
2. **Add new metric queries** using Prometheus expressions
3. **Configure thresholds** and colors for your use case

Example custom panel query:
```promql
# Custom business metric
rate(your_custom_metric_total[5m])
```

### Modifying Thresholds

Current thresholds in `ferris-streams-dashboard.json`:
- **CPU Warning**: 70%, Critical: 85%
- **Memory Warning**: 1GB, Critical: 2GB  
- **Error Rate Critical**: >5%

### Adding Alerts

Create alert rules in Grafana:
```yaml
# Example: High error rate alert
- alert: HighSQLErrorRate
  expr: rate(ferris_sql_query_errors_total[5m]) / rate(ferris_sql_queries_total[5m]) * 100 > 5
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "High SQL error rate detected"
```

## Production Setup

### Security Considerations
1. **Change default credentials**:
   ```yaml
   environment:
     - GF_SECURITY_ADMIN_PASSWORD=your_secure_password
   ```

2. **Enable HTTPS** in Grafana configuration
3. **Restrict network access** to monitoring ports
4. **Use authentication** for Prometheus if exposed

### Resource Requirements
- **Memory**: 512MB for Grafana, 1GB for Prometheus
- **CPU**: Minimal for typical FerrisStreams workloads
- **Disk**: ~100MB/day for metrics storage
- **Network**: <1MB/s for metrics scraping

### High Availability
For production deployments:
- Use external Prometheus storage (e.g., Thanos, Cortex)
- Set up Grafana clustering
- Configure backup for dashboard definitions
- Use persistent volumes for data retention

## Troubleshooting

### Common Issues

1. **No data in dashboard**:
   ```bash
   # Check FerrisStreams is exposing metrics
   curl http://localhost:9091/metrics
   
   # Verify Prometheus is scraping
   curl http://localhost:9090/api/v1/targets
   ```

2. **Container startup issues**:
   ```bash
   # Check container logs
   docker-compose logs grafana
   docker-compose logs prometheus
   ```

3. **Dashboard not loading**:
   - Verify `ferris-streams-dashboard.json` syntax
   - Check Grafana provisioning logs
   - Ensure datasource connection is working

### Debug Commands

```bash
# Restart monitoring stack
docker-compose down && docker-compose up -d

# View Prometheus configuration
curl http://localhost:9090/api/v1/status/config

# Check Grafana datasources
curl -u admin:admin http://localhost:3000/api/datasources

# Validate dashboard JSON
cat ferris-streams-dashboard.json | jq '.'
```

### Performance Tuning

If experiencing performance issues:

1. **Reduce scrape frequency** in `prometheus.yml`:
   ```yaml
   scrape_interval: 15s  # Instead of 5s
   ```

2. **Increase dashboard refresh** in JSON:
   ```json
   "refresh": "30s"  # Instead of 5s
   ```

3. **Limit dashboard time range**:
   ```json
   "time": {
     "from": "now-30m",  # Instead of 1h
     "to": "now"
   }
   ```

## Integration Examples

### Adding Business Metrics

1. **Register metrics in FerrisStreams**:
   ```rust
   let business_counter = register_int_counter_with_registry!(
       Opts::new("ferris_orders_processed_total", "Orders processed"),
       &metrics_provider.registry
   )?;
   ```

2. **Add panel to dashboard**:
   ```json
   {
     "targets": [{
       "expr": "rate(ferris_orders_processed_total[5m])",
       "legendFormat": "Orders/sec"
     }]
   }
   ```

### Custom Recording Rules

Add to `prometheus.yml`:
```yaml
rule_files:
  - "ferris_rules.yml"
```

Create `ferris_rules.yml`:
```yaml
groups:
  - name: ferris.rules
    rules:
      - record: ferris:sql_success_rate
        expr: rate(ferris_sql_queries_total[5m]) - rate(ferris_sql_query_errors_total[5m])
```

## Support

For monitoring setup issues:
1. Check Docker Compose logs for startup errors
2. Verify network connectivity between containers
3. Ensure FerrisStreams observability is properly configured
4. Review Prometheus targets and Grafana datasource health
5. Check file permissions on provisioning directories

The dashboard is designed to provide comprehensive visibility into FerrisStreams performance with minimal setup effort.