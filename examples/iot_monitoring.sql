-- SQL Application: IoT Sensor Monitoring Platform
-- Version: 2.1.0
-- Description: Real-time IoT sensor data processing and alerting system
-- Author: IoT Platform Team
-- Dependencies: kafka-sensors, kafka-alerts, influxdb-connector
-- Tag: environment:production
-- Tag: domain:iot

-- Name: Temperature Alert System
-- Property: alert_threshold=80
-- Property: cooldown_period=5m
START JOB temperature_alerts AS
SELECT 
    device_id,
    sensor_type,
    temperature,
    location,
    timestamp() as alert_time,
    'TEMPERATURE_HIGH' as alert_type
FROM sensor_data
WHERE sensor_type = 'temperature' AND temperature > 80
WITH ('output.topic' = 'critical_alerts');

-- Name: Pressure Monitoring
-- Property: min_pressure=10
-- Property: alert_level=critical
START JOB pressure_monitoring AS
SELECT 
    device_id,
    sensor_type,
    pressure,
    location,
    timestamp() as alert_time,
    CASE 
        WHEN pressure < 5 THEN 'CRITICAL_LOW'
        WHEN pressure < 10 THEN 'WARNING_LOW'
        ELSE 'NORMAL'
    END as pressure_status
FROM sensor_data
WHERE sensor_type = 'pressure' AND pressure < 15
WITH ('output.topic' = 'pressure_alerts');

-- Name: Vibration Analysis
-- Property: vibration_threshold=5.0
-- Property: analysis_window=10m
START JOB vibration_analysis AS
SELECT 
    device_id,
    location,
    AVG(vibration_level) as avg_vibration,
    MAX(vibration_level) as peak_vibration,
    COUNT(*) as reading_count,
    CASE 
        WHEN MAX(vibration_level) > 8.0 THEN 'CRITICAL'
        WHEN AVG(vibration_level) > 5.0 THEN 'HIGH'
        ELSE 'NORMAL'
    END as vibration_status
FROM sensor_data
WHERE sensor_type = 'vibration'
GROUP BY device_id, location
WINDOW TUMBLING(10m)
WITH ('output.topic' = 'vibration_analytics');

-- Name: Battery Level Monitor
-- Property: low_battery_threshold=20
-- Property: critical_battery_threshold=5
START JOB battery_monitoring AS
SELECT 
    device_id,
    location,
    battery_level,
    last_charge_time,
    DATEDIFF('hours', last_charge_time, timestamp()) as hours_since_charge,
    CASE 
        WHEN battery_level < 5 THEN 'CRITICAL'
        WHEN battery_level < 20 THEN 'LOW'
        WHEN battery_level < 50 THEN 'MEDIUM'
        ELSE 'GOOD'
    END as battery_status
FROM device_status
WHERE battery_level IS NOT NULL
WITH ('output.topic' = 'battery_alerts');

-- Name: Sensor Health Check
-- Property: health_check_interval=1h
-- Property: timeout_threshold=15m
START JOB sensor_health_check AS
SELECT 
    device_id,
    location,
    sensor_type,
    COUNT(*) as reading_count,
    MAX(timestamp) as last_reading,
    DATEDIFF('minutes', MAX(timestamp), timestamp()) as minutes_since_last_reading,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OFFLINE'
        WHEN DATEDIFF('minutes', MAX(timestamp), timestamp()) > 15 THEN 'TIMEOUT'
        WHEN COUNT(*) < 10 THEN 'DEGRADED'
        ELSE 'HEALTHY'
    END as health_status
FROM sensor_data
GROUP BY device_id, location, sensor_type
WINDOW TUMBLING(1h)
WITH ('output.topic' = 'sensor_health_reports');