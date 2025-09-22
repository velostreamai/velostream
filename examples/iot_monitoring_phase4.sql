-- SQL Application: IoT Sensor Monitoring Platform (Phase 1B-4 Features)  
-- Version: 3.0.0
-- Description: Advanced IoT monitoring showcasing Velostream Phase 1B-4 capabilities
-- Author: IoT Platform Team
-- Features: Event-time processing, Circuit breakers, Advanced analytics, Observability
-- Dependencies: Configuration files in configs/ directory using extends pattern  
-- Tag: environment:production
-- Tag: domain:iot
-- Tag: features:watermarks,circuit-breakers,advanced-sql,observability

-- ====================================================================================
-- PHASE 1B: EVENT-TIME PROCESSING - IoT Sensor Data
-- ====================================================================================
-- Handle out-of-order IoT sensor data with proper event-time semantics

CREATE STREAM iot_sensors_with_event_time AS
SELECT 
    device_id,
    sensor_type, 
    sensor_value,
    unit,
    location,
    sensor_timestamp,
    -- Extract event-time from when sensor reading was actually taken
    TIMESTAMP(sensor_timestamp) as event_time,
    device_status,
    battery_level,
    signal_strength,
    firmware_version
FROM sensor_data_raw
WITH (
    -- Phase 1B: Event-time configuration for IoT network delays
    'event.time.field' = 'sensor_timestamp', 
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '2m',  -- IoT can have significant delays
    'late.data.strategy' = 'include_in_next_window', -- Include delayed sensor readings
    'watermark.idle.timeout' = '5m',  -- Handle sparse sensors
    
    'config_file' = 'examples/configs/sensor_data_topic.yaml'
);

-- ====================================================================================
-- PHASE 3: ADVANCED WINDOW FUNCTIONS - Anomaly Detection
-- ====================================================================================  
-- Sophisticated sensor anomaly detection using statistical analysis

CREATE STREAM sensor_anomaly_detection AS
SELECT 
    device_id,
    sensor_type,
    sensor_value,
    event_time,
    location,
    battery_level,
    
    -- Phase 3: Advanced statistical window functions
    AVG(sensor_value) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as moving_avg_20,
    
    STDDEV(sensor_value) OVER (
        PARTITION BY device_id, sensor_type  
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as moving_stddev_20,
    
    -- Percentile analysis for outlier detection
    PERCENT_RANK() OVER (
        PARTITION BY sensor_type, location
        ORDER BY sensor_value
    ) as value_percentile,
    
    -- Trend analysis using LAG/LEAD
    LAG(sensor_value, 1) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time
    ) as prev_value,
    
    LAG(sensor_value, 5) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time  
    ) as value_5min_ago,
    
    -- Rate of change analysis
    (sensor_value - LAG(sensor_value, 1) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time
    )) / NULLIF(EXTRACT(EPOCH FROM (event_time - LAG(event_time, 1) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time  
    ))), 0) as rate_of_change,
    
    -- Z-score calculation for statistical anomalies
    ABS(sensor_value - AVG(sensor_value) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW  
    )) / NULLIF(STDDEV(sensor_value) OVER (
        PARTITION BY device_id, sensor_type
        ORDER BY event_time
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 0) as z_score,
    
    -- Device health ranking
    RANK() OVER (
        PARTITION BY location
        ORDER BY battery_level DESC, signal_strength DESC
    ) as device_health_rank,
    
    -- Complex anomaly classification
    CASE
        WHEN sensor_type = 'temperature' AND sensor_value > 80 THEN 'CRITICAL_OVERHEAT'
        WHEN sensor_type = 'temperature' AND sensor_value < -20 THEN 'CRITICAL_FREEZE'
        WHEN sensor_type = 'pressure' AND sensor_value > 1000 THEN 'PRESSURE_CRITICAL'
        WHEN sensor_type = 'humidity' AND sensor_value > 95 THEN 'HUMIDITY_CRITICAL'
        WHEN ABS(sensor_value - AVG(sensor_value) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time  
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(sensor_value) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) > 3.0 THEN 'STATISTICAL_ANOMALY'
        WHEN ABS((sensor_value - LAG(sensor_value, 1) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time
        )) / NULLIF(EXTRACT(EPOCH FROM (event_time - LAG(event_time, 1) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time
        ))), 0)) > 10 THEN 'RAPID_CHANGE'
        WHEN battery_level < 10 THEN 'LOW_BATTERY' 
        ELSE 'NORMAL'
    END as anomaly_type,
    
    -- Severity scoring
    CASE
        WHEN sensor_type = 'temperature' AND (sensor_value > 80 OR sensor_value < -20) THEN 10
        WHEN sensor_type IN ('pressure', 'humidity') AND sensor_value > 95 THEN 8
        WHEN ABS(sensor_value - AVG(sensor_value) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(sensor_value) OVER (
            PARTITION BY device_id, sensor_type
            ORDER BY event_time  
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) > 3.0 THEN 6
        WHEN battery_level < 10 THEN 4
        ELSE 1
    END as severity_score,
    
    NOW() as detection_time
FROM iot_sensors_with_event_time
-- Phase 1B: Event-time sliding windows (10-minute windows, 1-minute slide) 
WINDOW SLIDING (event_time, INTERVAL '10' MINUTE, INTERVAL '1' MINUTE)
-- Phase 3: Complex HAVING with correlated subqueries
HAVING (
    -- Anomalous readings
    sensor_value > (
        SELECT AVG(s2.sensor_value) + 2 * STDDEV(s2.sensor_value)
        FROM iot_sensors_with_event_time s2
        WHERE s2.sensor_type = iot_sensors_with_event_time.sensor_type
        AND s2.location = iot_sensors_with_event_time.location
        AND s2.event_time >= iot_sensors_with_event_time.event_time - INTERVAL '1' HOUR
    )
    OR sensor_value < (
        SELECT AVG(s2.sensor_value) - 2 * STDDEV(s2.sensor_value)  
        FROM iot_sensors_with_event_time s2
        WHERE s2.sensor_type = iot_sensors_with_event_time.sensor_type
        AND s2.location = iot_sensors_with_event_time.location
        AND s2.event_time >= iot_sensors_with_event_time.event_time - INTERVAL '1' HOUR
    )
    OR battery_level < 15
)
AND COUNT(*) >= 5  -- At least 5 readings in window
INTO sensor_anomaly_alerts
WITH (
    -- Phase 2: Resource management for IoT scale
    'max.memory.mb' = '16384',  -- Large scale IoT deployment
    'max.groups' = '1000000',   -- Many devices and sensors
    'spill.to.disk' = 'true', 
    'memory.pressure.threshold' = '0.75',
    
    -- Phase 2: Circuit breaker for IoT gateway connections
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '10',  -- IoT networks can be unreliable
    'circuit.breaker.success.threshold' = '5',
    'circuit.breaker.timeout' = '300s',  -- 5-minute timeout for IoT recovery
    'circuit.breaker.slow.call.threshold' = '30s',
    
    -- Phase 2: Aggressive retry for critical IoT data
    'retry.max.attempts' = '8',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '500ms', 
    'retry.max.delay' = '120s',
    'retry.jitter' = 'true',  -- Avoid thundering herd from many devices
    
    -- Phase 4: Comprehensive IoT observability
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'iot_anomaly_detection',
    'prometheus.histogram.buckets' = '0.5,1.0,5.0,10.0,30.0,60.0,300.0,1800.0',
    
    'config_file' = 'examples/configs/sensor_anomaly_alerts_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B+2+3: MULTI-STREAM JOINS - Device Health Monitoring
-- ====================================================================================
-- Complex time-based joins with device status and circuit breaker protection

CREATE STREAM device_status_with_event_time AS  
SELECT
    device_id,
    status,
    last_heartbeat,
    uptime_seconds,
    network_latency_ms,
    error_count,
    maintenance_due,
    TIMESTAMP(last_heartbeat) as event_time
FROM device_status_raw
WITH (
    'event.time.field' = 'last_heartbeat',
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS',
    'watermark.strategy' = 'bounded_out_of_orderness', 
    'watermark.max_out_of_orderness' = '1m',  -- Device status updates more reliable
    'late.data.strategy' = 'update_previous',  -- Update device status
    
    'config_file' = 'examples/configs/device_status_topic.yaml'
);

CREATE STREAM comprehensive_device_health AS
SELECT 
    s.device_id,
    s.location,
    s.anomaly_type,
    s.severity_score,
    s.battery_level,
    s.event_time as sensor_time,
    d.event_time as status_time,
    d.status as device_status,
    d.uptime_seconds,
    d.network_latency_ms,  
    d.error_count,
    
    -- Phase 3: Complex health scoring with window functions
    AVG(d.network_latency_ms) OVER (
        PARTITION BY s.device_id
        ORDER BY COALESCE(d.event_time, s.event_time)
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as avg_latency_10,
    
    SUM(d.error_count) OVER (
        PARTITION BY s.device_id
        ORDER BY COALESCE(d.event_time, s.event_time) 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_errors,
    
    -- Device reliability scoring
    CASE
        WHEN d.status = 'OFFLINE' THEN 0
        WHEN d.status = 'ERROR' OR d.error_count > 10 THEN 2
        WHEN s.battery_level < 10 THEN 3
        WHEN s.severity_score >= 8 THEN 4
        WHEN d.network_latency_ms > 1000 THEN 5
        WHEN s.severity_score >= 4 OR d.error_count > 5 THEN 6
        WHEN s.battery_level < 25 THEN 7
        ELSE 10
    END as health_score,
    
    -- Predictive maintenance scoring
    CASE
        WHEN d.maintenance_due = true THEN 'MAINTENANCE_DUE'
        WHEN d.uptime_seconds > 7776000 THEN 'MAINTENANCE_RECOMMENDED'  -- 90 days
        WHEN s.battery_level < 20 AND s.battery_level > LAG(s.battery_level, 1) OVER (
            PARTITION BY s.device_id ORDER BY s.event_time
        ) THEN 'BATTERY_REPLACEMENT_SOON'
        WHEN d.error_count > LAG(d.error_count, 1) OVER (
            PARTITION BY s.device_id ORDER BY COALESCE(d.event_time, s.event_time)  
        ) + 5 THEN 'HARDWARE_DEGRADATION'
        ELSE 'HEALTHY'
    END as maintenance_status,
    
    -- Time lag analysis for data freshness
    EXTRACT(EPOCH FROM (s.event_time - d.event_time)) as sensor_status_lag_seconds,
    
    NOW() as health_check_time
FROM sensor_anomaly_detection s
-- Phase 1B+3: Time-tolerant LEFT JOIN with event-time
LEFT JOIN device_status_with_event_time d ON s.device_id = d.device_id
    AND d.event_time BETWEEN s.event_time - INTERVAL '5' MINUTE
                         AND s.event_time + INTERVAL '5' MINUTE
-- Phase 1B: Session windows based on device activity (1-hour inactivity)
WINDOW SESSION (GREATEST(s.event_time, COALESCE(d.event_time, s.event_time)), INTERVAL '1' HOUR, s.device_id)
-- Phase 3: Complex HAVING with EXISTS subquery
HAVING (
    -- Critical conditions
    s.severity_score >= 6
    OR d.status IN ('ERROR', 'OFFLINE') 
    OR s.battery_level < 20
    OR d.error_count > 5
    OR 
    -- Devices with degrading patterns
    EXISTS (
        SELECT 1 FROM sensor_anomaly_detection s2
        WHERE s2.device_id = s.device_id
        AND s2.event_time >= s.event_time - INTERVAL '6' HOUR
        AND s2.severity_score >= 4
        GROUP BY s2.device_id
        HAVING COUNT(*) >= 3  -- 3+ anomalies in 6 hours
    )
)
AND SESSION_DURATION() >= INTERVAL '5' MINUTE  -- Session must be at least 5 minutes
INTO critical_device_alerts
WITH (
    -- Phase 2: Maximum resource allocation for critical monitoring
    'max.memory.mb' = '8192',
    'max.groups' = '500000', 
    'max.joins' = '1000000',
    'max.session.windows' = '100000',
    'spill.to.disk' = 'true',
    'join.timeout' = '300s',
    
    -- Phase 2: Ultra-sensitive circuit breaker for critical alerts
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '1',  -- Fail fast for critical systems
    'circuit.breaker.success.threshold' = '10',
    'circuit.breaker.timeout' = '60s',
    'circuit.breaker.slow.call.threshold' = '5s',
    
    -- Phase 2: Maximum retry attempts for critical data
    'retry.max.attempts' = '15',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '100ms',
    'retry.max.delay' = '30s',
    
    -- Dead letter queue for critical alert failures
    'dead.letter.queue.enabled' = 'true',
    'dead.letter.queue.topic' = 'critical-iot-failures',
    'dead.letter.queue.max.retries' = '5',
    
    -- Phase 4: Maximum observability for critical IoT monitoring
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'critical_device_health',
    'observability.alerts.enabled' = 'true',  -- Real-time alerting
    'prometheus.histogram.buckets' = '0.1,0.5,1.0,5.0,15.0,60.0,300.0,1800.0,3600.0',
    
    'config_file' = 'examples/configs/critical_alerts_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B-4 IoT MONITORING FEATURE SHOWCASE SUMMARY
-- ====================================================================================
-- This IoT monitoring demo demonstrates:

-- PHASE 1B: Watermarks & Time Semantics  
-- ✓ Event-time processing for sensor readings with network delay tolerance
-- ✓ Different watermark strategies for sensors vs device status
-- ✓ Session windows for device activity analysis  
-- ✓ Late data handling for unreliable IoT networks
-- ✓ Idle timeout handling for sparse sensor data

-- PHASE 2: Resource Management & Circuit Breakers
-- ✓ Large-scale resource allocation for millions of IoT devices
-- ✓ Circuit breakers for unreliable IoT gateway connections
-- ✓ Aggressive retry strategies for critical sensor data
-- ✓ Dead letter queue for critical alert delivery failures
-- ✓ Join timeouts for device status correlations

-- PHASE 3: Advanced Query Features
-- ✓ Statistical window functions: STDDEV, PERCENT_RANK for anomaly detection
-- ✓ Complex time-based joins with tolerance windows
-- ✓ Session windows with SESSION_DURATION analysis
-- ✓ Advanced subqueries in HAVING clauses for pattern detection
-- ✓ Multi-level CASE expressions for health scoring
-- ✓ Predictive maintenance analysis using trend functions

-- PHASE 4: Observability Integration
-- ✓ Comprehensive tracing for IoT data pipelines
-- ✓ IoT-specific Prometheus metrics with custom buckets
-- ✓ Performance profiling for large-scale device monitoring
-- ✓ Real-time alerting for critical device failures
-- ✓ End-to-end monitoring of IoT data freshness and quality

-- IoT-specific capabilities demonstrated:
-- - Statistical anomaly detection for sensor readings
-- - Device health scoring with predictive maintenance
-- - Network latency and reliability monitoring  
-- - Battery life and hardware degradation tracking
-- - Cross-correlation of sensor anomalies with device status
-- - Scalable processing for millions of IoT devices
-- - Fault-tolerant handling of unreliable IoT networks