-- SQL Application: IoT Sensor Monitoring Platform
-- Version: 2.2.0
-- Description: Real-time IoT sensor data processing and alerting system
-- Author: IoT Platform Team
-- Dependencies: Configuration files in configs/ directory using extends pattern
-- Tag: environment:production
-- Tag: domain:iot

-- Temperature Alert System
-- Monitors temperature sensors for high temperature conditions
CREATE STREAM critical_alerts AS
SELECT
    device_id,
    sensor_type,
    temperature,
    location,
    NOW() as alert_time,
    'TEMPERATURE_HIGH' as alert_type
FROM sensor_data
WHERE sensor_type = 'temperature' AND temperature > 80;

-- Pressure Monitoring System
-- Monitors pressure sensors for critical low pressure conditions
CREATE STREAM pressure_alerts AS
SELECT
    device_id,
    sensor_type,
    pressure,
    location,
    NOW() as alert_time,
    CASE
        WHEN pressure < 5 THEN 'CRITICAL_LOW'
        WHEN pressure < 10 THEN 'WARNING_LOW'
        ELSE 'NORMAL'
    END as pressure_status
FROM sensor_data
WHERE sensor_type = 'pressure' AND pressure < 15;

-- Vibration Analysis System
-- Windowed analysis of vibration levels for predictive maintenance
CREATE STREAM vibration_analytics AS
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
WINDOW TUMBLING(10m);

-- Battery Level Monitor
-- Monitors device battery levels for maintenance alerts
CREATE STREAM battery_alerts AS
SELECT
    device_id,
    location,
    battery_level,
    last_charge_time,
    DATEDIFF('hours', last_charge_time, NOW()) as hours_since_charge,
    CASE
        WHEN battery_level < 5 THEN 'CRITICAL'
        WHEN battery_level < 20 THEN 'LOW'
        WHEN battery_level < 50 THEN 'MEDIUM'
        ELSE 'GOOD'
    END as battery_status
FROM device_status
WHERE battery_level IS NOT NULL;

-- Sensor Health Check System
-- Windowed health monitoring for sensor availability and performance
CREATE STREAM sensor_health_reports AS
SELECT
    device_id,
    location,
    sensor_type,
    COUNT(*) as reading_count,
    MAX(timestamp) as last_reading,
    DATEDIFF('minutes', MAX(timestamp), NOW()) as minutes_since_last_reading,
    CASE
        WHEN COUNT(*) = 0 THEN 'OFFLINE'
        WHEN DATEDIFF('minutes', MAX(timestamp), NOW()) > 15 THEN 'TIMEOUT'
        WHEN COUNT(*) < 10 THEN 'DEGRADED'
        ELSE 'HEALTHY'
    END as health_status
FROM sensor_data
GROUP BY device_id, location, sensor_type
WINDOW TUMBLING(1h);
