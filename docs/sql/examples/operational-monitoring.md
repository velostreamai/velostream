# Operational Monitoring Queries

Copy-paste SQL queries for monitoring system health, application performance, and infrastructure metrics in real-time.

## System Health Monitoring

### Server Performance Metrics

```sql
-- Real-time server health dashboard
SELECT
    server_id,
    hostname,
    datacenter,
    metric_timestamp,
    -- Resource utilization
    cpu_usage_percent,
    memory_usage_percent,
    disk_usage_percent,
    network_in_mbps,
    network_out_mbps,
    -- Performance indicators
    load_average_1min,
    load_average_5min,
    load_average_15min,
    -- Health status classification
    CASE
        WHEN cpu_usage_percent > 90 OR memory_usage_percent > 95 THEN 'CRITICAL'
        WHEN cpu_usage_percent > 80 OR memory_usage_percent > 85 OR disk_usage_percent > 90 THEN 'WARNING'
        WHEN cpu_usage_percent > 70 OR memory_usage_percent > 75 OR disk_usage_percent > 80 THEN 'CAUTION'
        ELSE 'HEALTHY'
    END as health_status,
    -- Resource pressure indicators
    CASE
        WHEN load_average_1min > cpu_cores * 2 THEN 'HIGH_LOAD'
        WHEN load_average_1min > cpu_cores * 1.5 THEN 'MODERATE_LOAD'
        ELSE 'NORMAL_LOAD'
    END as load_status,
    -- Trend analysis
    cpu_usage_percent - LAG(cpu_usage_percent, 1) OVER (
        PARTITION BY server_id
        ORDER BY metric_timestamp
    ) as cpu_trend,
    memory_usage_percent - LAG(memory_usage_percent, 1) OVER (
        PARTITION BY server_id
        ORDER BY metric_timestamp
    ) as memory_trend
FROM server_metrics
WHERE metric_timestamp > NOW() - INTERVAL '30' MINUTES
ORDER BY server_id, metric_timestamp DESC;
```

### Application Performance Monitoring

```sql
-- Application performance metrics with anomaly detection
SELECT
    app_name,
    service_name,
    instance_id,
    metric_timestamp,
    -- Response time metrics
    avg_response_time_ms,
    p95_response_time_ms,
    p99_response_time_ms,
    -- Throughput metrics
    requests_per_second,
    successful_requests_per_second,
    error_requests_per_second,
    -- Error rates
    error_rate_percent,
    timeout_rate_percent,
    -- Anomaly detection
    CASE
        WHEN avg_response_time_ms > AVG(avg_response_time_ms) OVER (
            PARTITION BY app_name, service_name
            ORDER BY metric_timestamp
            ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
        ) * 3 THEN 'RESPONSE_TIME_ANOMALY'
        WHEN error_rate_percent > AVG(error_rate_percent) OVER (
            PARTITION BY app_name, service_name
            ORDER BY metric_timestamp
            ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
        ) * 2 + 5 THEN 'ERROR_RATE_ANOMALY'
        WHEN requests_per_second < AVG(requests_per_second) OVER (
            PARTITION BY app_name, service_name
            ORDER BY metric_timestamp
            ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
        ) * 0.5 THEN 'TRAFFIC_DROP_ANOMALY'
        ELSE 'NORMAL'
    END as anomaly_status,
    -- SLA compliance
    CASE
        WHEN p95_response_time_ms <= 200 AND error_rate_percent <= 1.0 THEN 'SLA_MET'
        WHEN p95_response_time_ms <= 500 AND error_rate_percent <= 2.0 THEN 'SLA_WARNING'
        ELSE 'SLA_BREACH'
    END as sla_status
FROM application_metrics
WHERE metric_timestamp > NOW() - INTERVAL '2' HOURS
ORDER BY app_name, service_name, metric_timestamp DESC;
```

## Database Performance Monitoring

### Database Health Metrics

```sql
-- Real-time database performance monitoring
SELECT
    database_name,
    instance_id,
    metric_timestamp,
    -- Connection metrics
    active_connections,
    max_connections,
    active_connections * 100.0 / max_connections as connection_utilization_pct,
    -- Query performance
    avg_query_time_ms,
    slow_queries_per_minute,
    queries_per_second,
    -- Lock analysis
    lock_waits_per_second,
    deadlocks_per_minute,
    avg_lock_wait_time_ms,
    -- Buffer pool metrics
    buffer_hit_rate_percent,
    buffer_pages_dirty_percent,
    -- I/O metrics
    disk_reads_per_second,
    disk_writes_per_second,
    log_writes_per_second,
    -- Health indicators
    CASE
        WHEN connection_utilization_pct > 90 THEN 'CONNECTION_PRESSURE'
        WHEN avg_query_time_ms > 1000 THEN 'SLOW_QUERIES'
        WHEN buffer_hit_rate_percent < 90 THEN 'BUFFER_INEFFICIENCY'
        WHEN deadlocks_per_minute > 5 THEN 'LOCK_CONTENTION'
        ELSE 'HEALTHY'
    END as performance_issue,
    -- Capacity alerts
    CASE
        WHEN connection_utilization_pct > 85 THEN 'HIGH'
        WHEN connection_utilization_pct > 70 THEN 'MEDIUM'
        ELSE 'LOW'
    END as capacity_alert_level
FROM database_metrics
WHERE metric_timestamp > NOW() - INTERVAL '1' HOUR
ORDER BY database_name, instance_id, metric_timestamp DESC;
```

### Query Performance Analysis

```sql
-- Identify problematic queries and performance patterns
SELECT
    database_name,
    query_fingerprint,
    -- Execution statistics
    execution_count,
    avg_execution_time_ms,
    max_execution_time_ms,
    total_execution_time_ms,
    -- Resource usage
    avg_rows_examined,
    avg_rows_returned,
    avg_cpu_time_ms,
    avg_io_wait_ms,
    -- Efficiency metrics
    avg_rows_returned * 100.0 / NULLIF(avg_rows_examined, 0) as selectivity_pct,
    total_execution_time_ms * 100.0 / SUM(total_execution_time_ms) OVER (
        PARTITION BY database_name
    ) as time_share_pct,
    -- Performance classification
    CASE
        WHEN avg_execution_time_ms > 10000 THEN 'VERY_SLOW'
        WHEN avg_execution_time_ms > 5000 THEN 'SLOW'
        WHEN avg_execution_time_ms > 1000 THEN 'MODERATE'
        ELSE 'FAST'
    END as performance_tier,
    -- Optimization priority
    (avg_execution_time_ms / 1000.0) * execution_count *
    (CASE WHEN avg_rows_examined / NULLIF(avg_rows_returned, 0) > 10 THEN 2 ELSE 1 END) as optimization_score
FROM query_performance_stats
WHERE last_seen > NOW() - INTERVAL '24' HOURS
  AND execution_count > 10
ORDER BY optimization_score DESC, total_execution_time_ms DESC
LIMIT 50;
```

## Infrastructure Monitoring

### Network Performance Tracking

```sql
-- Network performance and connectivity monitoring
SELECT
    source_host,
    target_host,
    connection_type,
    metric_timestamp,
    -- Latency metrics
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    p95_latency_ms,
    -- Packet loss and jitter
    packet_loss_percent,
    jitter_ms,
    -- Bandwidth utilization
    bandwidth_utilization_percent,
    throughput_mbps,
    -- Connection health
    successful_connections,
    failed_connections,
    connection_success_rate,
    -- Service quality assessment
    CASE
        WHEN packet_loss_percent > 5 OR avg_latency_ms > 200 THEN 'POOR'
        WHEN packet_loss_percent > 1 OR avg_latency_ms > 100 THEN 'DEGRADED'
        WHEN packet_loss_percent > 0.1 OR avg_latency_ms > 50 THEN 'FAIR'
        ELSE 'EXCELLENT'
    END as connection_quality,
    -- Trend analysis
    avg_latency_ms - AVG(avg_latency_ms) OVER (
        PARTITION BY source_host, target_host
        ORDER BY metric_timestamp
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) as latency_trend
FROM network_metrics
WHERE metric_timestamp > NOW() - INTERVAL '4' HOURS
ORDER BY source_host, target_host, metric_timestamp DESC;
```

### Storage System Monitoring

```sql
-- Storage performance and capacity monitoring
SELECT
    storage_system,
    volume_name,
    filesystem_type,
    metric_timestamp,
    -- Capacity metrics
    total_space_gb,
    used_space_gb,
    free_space_gb,
    used_space_gb * 100.0 / total_space_gb as usage_percent,
    -- Performance metrics
    read_iops,
    write_iops,
    read_throughput_mbps,
    write_throughput_mbps,
    avg_read_latency_ms,
    avg_write_latency_ms,
    -- Queue and utilization
    queue_depth,
    disk_utilization_percent,
    -- Capacity forecasting
    CASE
        WHEN used_space_gb * 100.0 / total_space_gb > 90 THEN
            (total_space_gb - used_space_gb) / NULLIF(
                (used_space_gb - LAG(used_space_gb, 24) OVER (
                    PARTITION BY storage_system, volume_name
                    ORDER BY metric_timestamp
                )) / 24.0, 0
            )
        ELSE NULL
    END as days_until_full,
    -- Performance alerts
    CASE
        WHEN avg_read_latency_ms > 50 OR avg_write_latency_ms > 50 THEN 'SLOW_IO'
        WHEN disk_utilization_percent > 90 THEN 'HIGH_UTILIZATION'
        WHEN used_space_gb * 100.0 / total_space_gb > 95 THEN 'CAPACITY_CRITICAL'
        WHEN used_space_gb * 100.0 / total_space_gb > 85 THEN 'CAPACITY_WARNING'
        ELSE 'HEALTHY'
    END as storage_status
FROM storage_metrics
WHERE metric_timestamp > NOW() - INTERVAL '6' HOURS
ORDER BY storage_system, volume_name, metric_timestamp DESC;
```

## Application Log Analysis

### Error Pattern Detection

```sql
-- Real-time error analysis and pattern detection
SELECT
    application,
    service,
    error_level,
    error_message_pattern,
    COUNT(*) as error_count,
    COUNT(DISTINCT user_id) as affected_users,
    COUNT(DISTINCT session_id) as affected_sessions,
    MIN(log_timestamp) as first_occurrence,
    MAX(log_timestamp) as last_occurrence,
    -- Error frequency analysis
    COUNT(*) * 60.0 / DATEDIFF('seconds', MIN(log_timestamp), MAX(log_timestamp)) as errors_per_minute,
    -- Error trend
    COUNT(CASE WHEN log_timestamp > NOW() - INTERVAL '15' MINUTES THEN 1 END) as recent_errors,
    COUNT(CASE WHEN log_timestamp BETWEEN NOW() - INTERVAL '30' MINUTES AND NOW() - INTERVAL '15' MINUTES THEN 1 END) as prev_period_errors,
    -- Impact assessment
    CASE
        WHEN error_level = 'CRITICAL' AND COUNT(*) > 100 THEN 'SYSTEM_OUTAGE'
        WHEN error_level = 'ERROR' AND COUNT(*) > 500 THEN 'SERVICE_DEGRADATION'
        WHEN COUNT(*) > 1000 THEN 'HIGH_ERROR_VOLUME'
        ELSE 'NORMAL'
    END as impact_level,
    -- Pattern classification
    CASE
        WHEN error_message_pattern LIKE '%timeout%' THEN 'TIMEOUT_ERRORS'
        WHEN error_message_pattern LIKE '%connection%' THEN 'CONNECTIVITY_ERRORS'
        WHEN error_message_pattern LIKE '%memory%' THEN 'MEMORY_ERRORS'
        WHEN error_message_pattern LIKE '%authentication%' THEN 'AUTH_ERRORS'
        ELSE 'GENERAL_ERRORS'
    END as error_category
FROM application_logs
WHERE log_timestamp > NOW() - INTERVAL '2' HOURS
  AND error_level IN ('ERROR', 'CRITICAL', 'FATAL')
GROUP BY application, service, error_level, error_message_pattern
HAVING COUNT(*) > 5
ORDER BY error_count DESC, last_occurrence DESC;
```

### Performance Log Analysis

```sql
-- Application performance insights from logs
SELECT
    application,
    operation_name,
    -- Performance metrics
    COUNT(*) as operation_count,
    AVG(duration_ms) as avg_duration_ms,
    MIN(duration_ms) as min_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    STDDEV(duration_ms) as duration_stddev,
    -- Percentile analysis
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms) as p50_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99_duration_ms,
    -- Success rate
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_operations,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*) as success_rate_pct,
    -- Performance classification
    CASE
        WHEN AVG(duration_ms) > 5000 THEN 'VERY_SLOW'
        WHEN AVG(duration_ms) > 2000 THEN 'SLOW'
        WHEN AVG(duration_ms) > 1000 THEN 'MODERATE'
        ELSE 'FAST'
    END as performance_tier,
    -- Anomaly detection
    CASE
        WHEN MAX(duration_ms) > AVG(duration_ms) * 10 THEN 'OUTLIER_DETECTED'
        WHEN STDDEV(duration_ms) > AVG(duration_ms) THEN 'HIGH_VARIABILITY'
        ELSE 'STABLE'
    END as performance_stability
FROM performance_logs
WHERE log_timestamp > NOW() - INTERVAL '1' HOUR
  AND operation_name IS NOT NULL
GROUP BY application, operation_name
HAVING COUNT(*) > 10
ORDER BY avg_duration_ms DESC;
```

## Security Monitoring

### Authentication and Access Monitoring

```sql
-- Security event monitoring and threat detection
SELECT
    event_type,
    source_ip,
    user_agent,
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as affected_users,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event,
    -- Geographic analysis
    COUNT(DISTINCT source_country) as countries_count,
    STRING_AGG(DISTINCT source_country, ', ') as countries_list,
    -- Threat indicators
    CASE
        WHEN event_type = 'FAILED_LOGIN' AND COUNT(*) > 50 THEN 'BRUTE_FORCE_ATTACK'
        WHEN COUNT(DISTINCT source_ip) = 1 AND COUNT(DISTINCT user_id) > 20 THEN 'CREDENTIAL_STUFFING'
        WHEN COUNT(DISTINCT source_country) > 5 AND COUNT(*) > 100 THEN 'DISTRIBUTED_ATTACK'
        WHEN event_type = 'PRIVILEGE_ESCALATION' THEN 'SECURITY_INCIDENT'
        ELSE 'NORMAL_ACTIVITY'
    END as threat_level,
    -- IP reputation analysis
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as activity_concentration_pct,
    -- Time-based patterns
    COUNT(CASE WHEN EXTRACT('HOUR', event_timestamp) BETWEEN 22 AND 6 THEN 1 END) as off_hours_events,
    COUNT(CASE WHEN EXTRACT('HOUR', event_timestamp) BETWEEN 22 AND 6 THEN 1 END) * 100.0 / COUNT(*) as off_hours_pct
FROM security_events
WHERE event_timestamp > NOW() - INTERVAL '24' HOURS
GROUP BY event_type, source_ip, user_agent, user_id
HAVING COUNT(*) > 5 OR event_type IN ('PRIVILEGE_ESCALATION', 'DATA_BREACH', 'UNAUTHORIZED_ACCESS')
ORDER BY event_count DESC, last_event DESC;
```

## Capacity Planning

### Resource Trend Analysis

```sql
-- Capacity planning and resource growth analysis
SELECT
    resource_type,
    resource_name,
    metric_date,
    -- Current utilization
    avg_utilization_percent,
    max_utilization_percent,
    -- Growth calculation
    avg_utilization_percent - LAG(avg_utilization_percent, 7) OVER (
        PARTITION BY resource_type, resource_name
        ORDER BY metric_date
    ) as weekly_growth_pct,
    avg_utilization_percent - LAG(avg_utilization_percent, 30) OVER (
        PARTITION BY resource_type, resource_name
        ORDER BY metric_date
    ) as monthly_growth_pct,
    -- Capacity forecasting
    CASE
        WHEN LAG(avg_utilization_percent, 30) OVER (
            PARTITION BY resource_type, resource_name
            ORDER BY metric_date
        ) IS NOT NULL THEN
            (95 - avg_utilization_percent) / NULLIF(
                (avg_utilization_percent - LAG(avg_utilization_percent, 30) OVER (
                    PARTITION BY resource_type, resource_name
                    ORDER BY metric_date
                )) / 30.0, 0
            )
        ELSE NULL
    END as days_to_95_percent,
    -- Capacity status
    CASE
        WHEN max_utilization_percent > 95 THEN 'CAPACITY_EXCEEDED'
        WHEN avg_utilization_percent > 85 THEN 'CAPACITY_WARNING'
        WHEN avg_utilization_percent > 70 THEN 'CAPACITY_WATCH'
        ELSE 'CAPACITY_ADEQUATE'
    END as capacity_status,
    -- Growth trend classification
    CASE
        WHEN (avg_utilization_percent - LAG(avg_utilization_percent, 30) OVER (
            PARTITION BY resource_type, resource_name
            ORDER BY metric_date
        )) > 20 THEN 'RAPID_GROWTH'
        WHEN (avg_utilization_percent - LAG(avg_utilization_percent, 30) OVER (
            PARTITION BY resource_type, resource_name
            ORDER BY metric_date
        )) > 10 THEN 'MODERATE_GROWTH'
        WHEN (avg_utilization_percent - LAG(avg_utilization_percent, 30) OVER (
            PARTITION BY resource_type, resource_name
            ORDER BY metric_date
        )) < -5 THEN 'DECLINING'
        ELSE 'STABLE'
    END as growth_trend
FROM (
    SELECT
        resource_type,
        resource_name,
        DATE(metric_timestamp) as metric_date,
        AVG(utilization_percent) as avg_utilization_percent,
        MAX(utilization_percent) as max_utilization_percent
    FROM resource_utilization
    WHERE metric_timestamp > NOW() - INTERVAL '90' DAYS
    GROUP BY resource_type, resource_name, DATE(metric_timestamp)
) daily_metrics
ORDER BY resource_type, resource_name, metric_date DESC;
```

## Performance Tips

1. **Use time-based partitioning** for metrics tables
2. **Index on timestamp columns** for efficient time-range queries
3. **Consider data retention policies** for historical metrics
4. **Use appropriate aggregation intervals** based on monitoring needs
5. **Implement efficient alerting thresholds** to avoid alert fatigue

## Common Monitoring Patterns

```sql
-- Pattern: Health Status Classification
CASE
    WHEN error_rate > 5 OR response_time > 2000 THEN 'CRITICAL'
    WHEN error_rate > 2 OR response_time > 1000 THEN 'WARNING'
    WHEN error_rate > 1 OR response_time > 500 THEN 'CAUTION'
    ELSE 'HEALTHY'
END as health_status;

-- Pattern: Anomaly Detection
CASE
    WHEN current_value > historical_avg * anomaly_threshold THEN 'ANOMALY_HIGH'
    WHEN current_value < historical_avg / anomaly_threshold THEN 'ANOMALY_LOW'
    ELSE 'NORMAL'
END as anomaly_status;

-- Pattern: Trend Analysis
CASE
    WHEN current_period > previous_period * 1.2 THEN 'INCREASING'
    WHEN current_period < previous_period * 0.8 THEN 'DECREASING'
    ELSE 'STABLE'
END as trend_direction;
```