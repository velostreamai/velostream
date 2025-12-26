-- IoT Device Monitoring with SQL-Native Observability
-- Demonstrates FR-073 SQL-native metrics with nested field extraction
--
-- Metrics Defined:
-- 1. Device temperature gauges with nested metadata labels
-- 2. Alert counters for temperature/pressure thresholds
-- 3. Battery level monitoring
-- 4. Device connectivity tracking
-- 5. Sensor reading histograms
--
-- Run with: velo-sql deploy-app --file examples/iot_monitoring_with_metrics.sql

-- ===========================================================================
-- Stream 1: Device Temperature Monitoring
-- ===========================================================================

-- Gauge: Current temperature reading
-- @metric: velo_device_temperature_celsius
-- @metric_type: gauge
-- @metric_help: "Device temperature in Celsius"
-- @metric_field: temperature
-- @metric_labels: metadata.region, metadata.datacenter, metadata.zone, device_id
-- @metric_condition: temperature > -50 AND temperature < 150

-- Counter: Temperature alerts
-- @metric: velo_temperature_alerts_total
-- @metric_type: counter
-- @metric_help: "Temperature readings exceeding thresholds"
-- @metric_labels: metadata.region, metadata.datacenter, alert_severity, device_type
-- @metric_condition: temperature > 80 OR temperature < 0
CREATE STREAM temperature_monitoring AS
SELECT
    device_id,
    device_type,
    temperature,
    CASE
        WHEN temperature > 100 THEN 'critical'
        WHEN temperature > 80 THEN 'warning'
        WHEN temperature < 0 THEN 'warning'
        WHEN temperature < -20 THEN 'critical'
        ELSE 'normal'
    END as alert_severity,
    metadata,  -- Map field: {region, datacenter, zone, rack_id, etc.}
    event_time
FROM device_telemetry
WHERE temperature IS NOT NULL;

-- ===========================================================================
-- Stream 2: Device Pressure Monitoring
-- ===========================================================================

-- Gauge: Current pressure reading
-- @metric: velo_device_pressure_psi
-- @metric_type: gauge
-- @metric_help: "Device pressure in PSI"
-- @metric_field: pressure
-- @metric_labels: metadata.region, metadata.facility, device_type

-- Histogram: Pressure distribution
-- @metric: velo_pressure_distribution_psi
-- @metric_type: histogram
-- @metric_help: "Distribution of pressure readings"
-- @metric_field: pressure
-- @metric_labels: metadata.region, device_type
-- @metric_buckets: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200
CREATE STREAM pressure_monitoring AS
SELECT
    device_id,
    device_type,
    pressure,
    metadata,
    event_time
FROM device_telemetry
WHERE pressure IS NOT NULL;

-- ===========================================================================
-- Stream 3: Battery Level Tracking
-- ===========================================================================

-- Gauge: Current battery level
-- @metric: velo_device_battery_percent
-- @metric_type: gauge
-- @metric_help: "Device battery level percentage"
-- @metric_field: battery_percent
-- @metric_labels: metadata.region, metadata.datacenter, device_id, power_mode

-- Counter: Low battery alerts
-- @metric: velo_low_battery_alerts_total
-- @metric_type: counter
-- @metric_help: "Devices with low battery levels"
-- @metric_labels: metadata.region, metadata.datacenter, device_type, urgency
-- @metric_condition: battery_percent < 20
CREATE STREAM battery_monitoring AS
SELECT
    device_id,
    device_type,
    battery_percent,
    power_mode,
    CASE
        WHEN battery_percent < 5 THEN 'critical'
        WHEN battery_percent < 10 THEN 'urgent'
        ELSE 'warning'
    END as urgency,
    metadata,
    event_time
FROM device_telemetry
WHERE battery_percent IS NOT NULL;

-- ===========================================================================
-- Stream 4: Device Connectivity Monitoring
-- ===========================================================================

-- Counter: Connection state changes
-- @metric: velo_device_connections_total
-- @metric_type: counter
-- @metric_help: "Device connection state changes"
-- @metric_labels: metadata.region, metadata.network_type, connection_state, device_type

-- Gauge: Signal strength
-- @metric: velo_signal_strength_dbm
-- @metric_type: gauge
-- @metric_help: "Device signal strength in dBm"
-- @metric_field: signal_strength
-- @metric_labels: metadata.region, metadata.network_type, device_id

-- Histogram: Connection latency
-- @metric: velo_connection_latency_ms
-- @metric_type: histogram
-- @metric_help: "Device connectivity latency in milliseconds"
-- @metric_field: latency_ms
-- @metric_labels: metadata.region, metadata.network_type
-- @metric_buckets: 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000
CREATE STREAM connectivity_monitoring AS
SELECT
    device_id,
    device_type,
    connection_state,
    signal_strength,
    latency_ms,
    metadata,
    event_time
FROM device_network_events;

-- ===========================================================================
-- Stream 5: Device Error Tracking
-- ===========================================================================

-- Counter: Device errors by type
-- @metric: velo_device_errors_total
-- @metric_type: counter
-- @metric_help: "Device errors by type and severity"
-- @metric_labels: metadata.region, metadata.datacenter, error_type, severity, device_type

-- Counter: Critical errors requiring intervention
-- @metric: velo_critical_device_errors_total
-- @metric_type: counter
-- @metric_help: "Critical device errors requiring immediate attention"
-- @metric_labels: metadata.region, metadata.facility_id, error_type, device_type
-- @metric_condition: severity = 'critical' OR severity = 'fatal'
CREATE STREAM device_errors AS
SELECT
    device_id,
    device_type,
    error_type,
    error_code,
    severity,
    error_message,
    metadata,
    event_time
FROM device_logs
WHERE severity IN ('warning', 'error', 'critical', 'fatal');

-- ===========================================================================
-- Stream 6: Sensor Reading Quality Monitoring
-- ===========================================================================

-- Counter: Anomalous sensor readings
-- @metric: velo_anomalous_readings_total
-- @metric_type: counter
-- @metric_help: "Sensor readings outside expected ranges"
-- @metric_labels: metadata.region, sensor_type, anomaly_type, device_type
-- @metric_condition: is_anomalous = true

-- Histogram: Sensor accuracy deviation
-- @metric: velo_sensor_accuracy_deviation_percent
-- @metric_type: histogram
-- @metric_help: "Sensor accuracy deviation from calibration"
-- @metric_field: deviation_percent
-- @metric_labels: metadata.region, sensor_type
-- @metric_buckets: 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0
CREATE STREAM sensor_quality AS
SELECT
    device_id,
    device_type,
    sensor_type,
    reading_value,
    expected_value,
    ABS((reading_value - expected_value) / expected_value * 100) as deviation_percent,
    CASE
        WHEN ABS(reading_value - expected_value) > expected_value * 0.2 THEN 'severe'
        WHEN ABS(reading_value - expected_value) > expected_value * 0.1 THEN 'moderate'
        WHEN ABS(reading_value - expected_value) > expected_value * 0.05 THEN 'minor'
        ELSE 'normal'
    END as anomaly_type,
    (ABS(reading_value - expected_value) > expected_value * 0.05) as is_anomalous,
    metadata,
    event_time
FROM sensor_readings
WHERE expected_value IS NOT NULL AND expected_value != 0;

-- ===========================================================================
-- Stream 7: Device Uptime Tracking
-- ===========================================================================

-- Counter: Device restarts
-- @metric: velo_device_restarts_total
-- @metric_type: counter
-- @metric_help: "Device restart events"
-- @metric_labels: metadata.region, metadata.datacenter, device_type, restart_reason

-- Histogram: Uptime duration (hours)
-- @metric: velo_device_uptime_hours
-- @metric_type: histogram
-- @metric_help: "Device uptime in hours between restarts"
-- @metric_field: uptime_hours
-- @metric_labels: metadata.region, device_type
-- @metric_buckets: 1, 6, 12, 24, 48, 72, 168, 336, 720, 8760
CREATE STREAM uptime_tracking AS
SELECT
    device_id,
    device_type,
    restart_reason,
    EXTRACT(EPOCH FROM (current_time - last_restart_time)) / 3600 as uptime_hours,
    metadata,
    event_time
FROM device_lifecycle_events
WHERE last_restart_time IS NOT NULL;

-- ===========================================================================
-- Stream 8: Multi-Sensor Device Monitoring (Nested Metadata)
-- ===========================================================================

-- Gauge: Multi-level nested metadata extraction
-- @metric: velo_facility_ambient_temperature_celsius
-- @metric_type: gauge
-- @metric_help: "Facility ambient temperature with full location hierarchy"
-- @metric_field: ambient_temperature
-- @metric_labels: metadata.location.country, metadata.location.region, metadata.location.city, metadata.facility.name, metadata.facility.zone
CREATE STREAM facility_environment AS
SELECT
    facility_id,
    ambient_temperature,
    humidity_percent,
    metadata,  -- Map with nested location and facility objects
    event_time
FROM facility_sensors
WHERE ambient_temperature IS NOT NULL;

-- ===========================================================================
-- Stream 9: Data Quality Monitoring
-- ===========================================================================

-- Counter: Missing sensor data
-- @metric: velo_missing_sensor_data_total
-- @metric_type: counter
-- @metric_help: "Sensor readings with missing data"
-- @metric_labels: metadata.region, sensor_type, device_type
-- @metric_condition: has_missing_data = true

-- Gauge: Data completeness percentage
-- @metric: velo_data_completeness_percent
-- @metric_type: gauge
-- @metric_help: "Percentage of expected sensor readings received"
-- @metric_field: completeness_percent
-- @metric_labels: metadata.region, metadata.datacenter, sensor_type
CREATE STREAM data_quality AS
SELECT
    device_id,
    device_type,
    sensor_type,
    expected_reading_count,
    actual_reading_count,
    (CAST(actual_reading_count AS FLOAT) / CAST(expected_reading_count AS FLOAT) * 100) as completeness_percent,
    (actual_reading_count < expected_reading_count) as has_missing_data,
    metadata,
    event_time
FROM sensor_data_quality
WHERE expected_reading_count > 0;

-- ===========================================================================
-- Expected Prometheus Metrics Output
-- ===========================================================================

-- # HELP velo_device_temperature_celsius Device temperature in Celsius
-- # TYPE velo_device_temperature_celsius gauge
-- velo_device_temperature_celsius{region="us-east",datacenter="dc1",zone="zone-a",device_id="sensor-001"} 45.2

-- # HELP velo_temperature_alerts_total Temperature readings exceeding thresholds
-- # TYPE velo_temperature_alerts_total counter
-- velo_temperature_alerts_total{region="us-east",datacenter="dc1",alert_severity="warning",device_type="temperature_sensor"} 23

-- # HELP velo_device_battery_percent Device battery level percentage
-- # TYPE velo_device_battery_percent gauge
-- velo_device_battery_percent{region="us-east",datacenter="dc1",device_id="sensor-001",power_mode="battery"} 15.5

-- # HELP velo_low_battery_alerts_total Devices with low battery levels
-- # TYPE velo_low_battery_alerts_total counter
-- velo_low_battery_alerts_total{region="us-east",datacenter="dc1",device_type="temperature_sensor",urgency="urgent"} 5

-- # HELP velo_signal_strength_dbm Device signal strength in dBm
-- # TYPE velo_signal_strength_dbm gauge
-- velo_signal_strength_dbm{region="us-east",network_type="cellular",device_id="sensor-001"} -67.0

-- # HELP velo_device_errors_total Device errors by type and severity
-- # TYPE velo_device_errors_total counter
-- velo_device_errors_total{region="us-east",datacenter="dc1",error_type="sensor_failure",severity="critical",device_type="multi_sensor"} 3

-- ===========================================================================
-- Prometheus Alert Examples
-- ===========================================================================

-- Alert: Critical temperature in datacenter
-- velo_device_temperature_celsius{datacenter="dc1"} > 100

-- Alert: High rate of temperature alerts
-- rate(velo_temperature_alerts_total{alert_severity="critical"}[5m]) > 5

-- Alert: Multiple devices with low battery
-- sum by (datacenter) (velo_low_battery_alerts_total{urgency="critical"}) > 10

-- Alert: Poor signal strength
-- avg by (region) (velo_signal_strength_dbm) < -80

-- Alert: High error rate for specific device type
-- rate(velo_device_errors_total{device_type="multi_sensor",severity="critical"}[10m]) > 1

-- Alert: Low data completeness
-- avg by (region, sensor_type) (velo_data_completeness_percent) < 90

-- ===========================================================================
-- Operational Dashboards (Prometheus + Grafana)
-- ===========================================================================

-- Temperature Overview:
-- - avg by (region, datacenter) (velo_device_temperature_celsius)
-- - rate(velo_temperature_alerts_total[5m])
-- - histogram_quantile(0.95, rate(velo_device_temperature_celsius_bucket[5m]))

-- Device Health:
-- - avg by (region) (velo_device_battery_percent)
-- - sum by (urgency) (velo_low_battery_alerts_total)
-- - rate(velo_device_errors_total[5m])

-- Connectivity:
-- - avg by (network_type) (velo_signal_strength_dbm)
-- - histogram_quantile(0.99, rate(velo_connection_latency_ms_bucket[5m]))
-- - rate(velo_device_connections_total{connection_state="disconnected"}[5m])

-- Data Quality:
-- - avg by (sensor_type) (velo_data_completeness_percent)
-- - rate(velo_missing_sensor_data_total[10m])
-- - rate(velo_anomalous_readings_total[5m])

-- Note: SQL file contains streaming analytics patterns demonstrated with CREATE STREAM statements.
-- The parser supports SELECT, CREATE STREAM, and other DDL/DML statements.
SELECT 1;
