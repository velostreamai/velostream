# IoT Analytics Queries

Copy-paste SQL queries for analyzing IoT sensor data, device health, and environmental monitoring.

## Environmental Monitoring

### Temperature and Humidity Analysis

```sql
-- Real-time environmental conditions
SELECT
    device_id,
    location,
    temperature,
    humidity,
    pressure,
    reading_timestamp,
    -- Comfort index calculation
    CASE
        WHEN temperature BETWEEN 20 AND 24 AND humidity BETWEEN 40 AND 60 THEN 'Optimal'
        WHEN temperature BETWEEN 18 AND 26 AND humidity BETWEEN 30 AND 70 THEN 'Comfortable'
        WHEN temperature > 30 OR humidity > 80 THEN 'Uncomfortable_Hot'
        WHEN temperature < 16 OR humidity < 30 THEN 'Uncomfortable_Cold'
        ELSE 'Moderate'
    END as comfort_level,
    -- Alert conditions
    CASE
        WHEN temperature > 35 THEN 'HIGH_TEMP_ALERT'
        WHEN temperature < 5 THEN 'LOW_TEMP_ALERT'
        WHEN humidity > 85 THEN 'HIGH_HUMIDITY_ALERT'
        WHEN humidity < 20 THEN 'LOW_HUMIDITY_ALERT'
        ELSE 'NORMAL'
    END as alert_status
FROM environmental_sensors
WHERE reading_timestamp > NOW() - INTERVAL '1' HOUR
ORDER BY alert_status DESC, reading_timestamp DESC;

-- Hourly environmental trends
SELECT
    location,
    DATE_FORMAT(reading_timestamp, '%Y-%m-%d %H:00:00') as hour,
    AVG(temperature) as avg_temp,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp,
    AVG(humidity) as avg_humidity,
    COUNT(*) as reading_count
FROM environmental_sensors
WHERE reading_timestamp > NOW() - INTERVAL '24' HOURS
GROUP BY location, DATE_FORMAT(reading_timestamp, '%Y-%m-%d %H:00:00')
ORDER BY location, hour;
```

### Air Quality Monitoring

```sql
-- Air quality analysis with health indicators
SELECT
    device_id,
    location,
    pm25,
    pm10,
    co2_ppm,
    voc_level,
    reading_timestamp,
    -- Air quality index calculation (simplified)
    CASE
        WHEN pm25 <= 12 AND pm10 <= 25 AND co2_ppm <= 1000 THEN 'Good'
        WHEN pm25 <= 35 AND pm10 <= 50 AND co2_ppm <= 1500 THEN 'Moderate'
        WHEN pm25 <= 55 AND pm10 <= 75 AND co2_ppm <= 2000 THEN 'Unhealthy for Sensitive'
        WHEN pm25 <= 150 AND pm10 <= 150 AND co2_ppm <= 3000 THEN 'Unhealthy'
        ELSE 'Very Unhealthy'
    END as air_quality_index,
    -- Health recommendations
    CASE
        WHEN pm25 > 55 OR co2_ppm > 2000 THEN 'Limit outdoor activities'
        WHEN pm25 > 35 OR co2_ppm > 1500 THEN 'Sensitive groups should limit exposure'
        ELSE 'Normal activities OK'
    END as health_recommendation,
    -- Ventilation needs
    CASE
        WHEN co2_ppm > 1000 THEN 'Increase ventilation'
        WHEN voc_level > 300 THEN 'Check for pollutant sources'
        ELSE 'Adequate ventilation'
    END as ventilation_status
FROM air_quality_sensors
WHERE reading_timestamp > NOW() - INTERVAL '2' HOURS;
```

## Device Health and Maintenance

### Battery and Signal Monitoring

```sql
-- Device health dashboard
SELECT
    device_id,
    device_type,
    location,
    battery_level,
    signal_strength,
    last_heartbeat,
    DATEDIFF('minutes', last_heartbeat, NOW()) as minutes_since_heartbeat,
    -- Battery status
    CASE
        WHEN battery_level > 80 THEN 'Excellent'
        WHEN battery_level > 50 THEN 'Good'
        WHEN battery_level > 20 THEN 'Low'
        WHEN battery_level > 10 THEN 'Critical'
        ELSE 'Replace Battery'
    END as battery_status,
    -- Connectivity status
    CASE
        WHEN DATEDIFF('minutes', last_heartbeat, NOW()) < 5 THEN 'Online'
        WHEN DATEDIFF('minutes', last_heartbeat, NOW()) < 15 THEN 'Intermittent'
        ELSE 'Offline'
    END as connectivity_status,
    -- Maintenance priority
    CASE
        WHEN battery_level < 10 OR DATEDIFF('minutes', last_heartbeat, NOW()) > 60 THEN 'Urgent'
        WHEN battery_level < 20 OR DATEDIFF('minutes', last_heartbeat, NOW()) > 15 THEN 'High'
        WHEN battery_level < 50 THEN 'Medium'
        ELSE 'Low'
    END as maintenance_priority
FROM device_status
ORDER BY maintenance_priority DESC, battery_level ASC;

-- Predicted battery replacement schedule
SELECT
    device_id,
    location,
    current_battery_level,
    -- Calculate battery drain rate (last 7 days)
    (first_reading - current_battery_level) / NULLIF(days_elapsed, 0) as daily_drain_rate,
    -- Predict days until replacement needed
    CASE
        WHEN (first_reading - current_battery_level) / NULLIF(days_elapsed, 0) > 0
        THEN CEIL((current_battery_level - 10) / ((first_reading - current_battery_level) / NULLIF(days_elapsed, 0)))
        ELSE NULL
    END as days_until_replacement,
    -- Replacement urgency
    CASE
        WHEN CEIL((current_battery_level - 10) / ((first_reading - current_battery_level) / NULLIF(days_elapsed, 0))) < 7
        THEN 'Replace This Week'
        WHEN CEIL((current_battery_level - 10) / ((first_reading - current_battery_level) / NULLIF(days_elapsed, 0))) < 30
        THEN 'Replace This Month'
        ELSE 'Monitor'
    END as replacement_schedule
FROM (\n    SELECT\n        device_id,\n        location,\n        battery_level as current_battery_level,\n        FIRST_VALUE(battery_level) OVER (\n            PARTITION BY device_id\n            ORDER BY reading_timestamp\n            ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING\n        ) as first_reading,\n        7 as days_elapsed  -- Simplified to 7 days\n    FROM device_battery_history\n    WHERE reading_timestamp > NOW() - INTERVAL '7' DAYS\n    ORDER BY device_id, reading_timestamp DESC\n) battery_trends;\n```\n\n## Anomaly Detection\n\n### Sensor Reading Anomalies\n\n```sql\n-- Detect anomalous sensor readings\nSELECT\n    device_id,\n    sensor_type,\n    reading_value,\n    reading_timestamp,\n    -- Statistical analysis\n    AVG(reading_value) OVER (\n        PARTITION BY device_id, sensor_type\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n    ) as avg_last_20_readings,\n    STDDEV(reading_value) OVER (\n        PARTITION BY device_id, sensor_type\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n    ) as stddev_last_20_readings,\n    -- Z-score calculation\n    ABS(reading_value - AVG(reading_value) OVER (\n        PARTITION BY device_id, sensor_type\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n    )) / NULLIF(STDDEV(reading_value) OVER (\n        PARTITION BY device_id, sensor_type\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n    ), 0) as z_score,\n    -- Anomaly classification\n    CASE\n        WHEN ABS(reading_value - AVG(reading_value) OVER (\n            PARTITION BY device_id, sensor_type\n            ORDER BY reading_timestamp\n            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n        )) > 3 * STDDEV(reading_value) OVER (\n            PARTITION BY device_id, sensor_type\n            ORDER BY reading_timestamp\n            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n        ) THEN 'SEVERE_ANOMALY'\n        WHEN ABS(reading_value - AVG(reading_value) OVER (\n            PARTITION BY device_id, sensor_type\n            ORDER BY reading_timestamp\n            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n        )) > 2 * STDDEV(reading_value) OVER (\n            PARTITION BY device_id, sensor_type\n            ORDER BY reading_timestamp\n            ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING\n        ) THEN 'MODERATE_ANOMALY'\n        ELSE 'NORMAL'\n    END as anomaly_level\nFROM sensor_readings\nWHERE reading_timestamp > NOW() - INTERVAL '2' HOURS\nORDER BY anomaly_level DESC, z_score DESC;\n```\n\n## Energy Consumption Analysis\n\n### Smart Meter Analytics\n\n```sql\n-- Electricity consumption patterns\nSELECT\n    meter_id,\n    building_id,\n    DATE_FORMAT(reading_timestamp, '%Y-%m-%d') as date,\n    EXTRACT('HOUR', reading_timestamp) as hour,\n    AVG(power_consumption_kw) as avg_power_kw,\n    SUM(energy_consumed_kwh) as total_energy_kwh,\n    MAX(power_consumption_kw) as peak_power_kw,\n    -- Time-of-day classification\n    CASE\n        WHEN EXTRACT('HOUR', reading_timestamp) BETWEEN 7 AND 9 THEN 'Morning Peak'\n        WHEN EXTRACT('HOUR', reading_timestamp) BETWEEN 18 AND 20 THEN 'Evening Peak'\n        WHEN EXTRACT('HOUR', reading_timestamp) BETWEEN 10 AND 17 THEN 'Daytime'\n        WHEN EXTRACT('HOUR', reading_timestamp) BETWEEN 21 AND 6 THEN 'Night'\n        ELSE 'Transition'\n    END as time_period,\n    -- Cost calculation (simplified)\n    SUM(energy_consumed_kwh) * 0.12 as estimated_cost_usd\nFROM smart_meters\nWHERE reading_timestamp > NOW() - INTERVAL '7' DAYS\nGROUP BY meter_id, building_id, DATE_FORMAT(reading_timestamp, '%Y-%m-%d'), EXTRACT('HOUR', reading_timestamp)\nORDER BY building_id, date, hour;\n\n-- Energy efficiency analysis\nSELECT\n    building_id,\n    building_type,\n    floor_area_sqft,\n    DATE_FORMAT(reading_timestamp, '%Y-%m') as month,\n    SUM(energy_consumed_kwh) as monthly_consumption,\n    SUM(energy_consumed_kwh) / floor_area_sqft as kwh_per_sqft,\n    -- Efficiency benchmarking\n    CASE\n        WHEN building_type = 'office' AND SUM(energy_consumed_kwh) / floor_area_sqft < 15 THEN 'Efficient'\n        WHEN building_type = 'office' AND SUM(energy_consumed_kwh) / floor_area_sqft < 25 THEN 'Average'\n        WHEN building_type = 'residential' AND SUM(energy_consumed_kwh) / floor_area_sqft < 12 THEN 'Efficient'\n        WHEN building_type = 'residential' AND SUM(energy_consumed_kwh) / floor_area_sqft < 20 THEN 'Average'\n        ELSE 'Inefficient'\n    END as efficiency_rating\nFROM smart_meters s\nJOIN buildings b ON s.building_id = b.building_id\nWHERE reading_timestamp > NOW() - INTERVAL '12' MONTHS\nGROUP BY building_id, building_type, floor_area_sqft, DATE_FORMAT(reading_timestamp, '%Y-%m')\nORDER BY building_id, month;\n```\n\n## Industrial IoT Monitoring\n\n### Equipment Performance\n\n```sql\n-- Machine performance analysis\nSELECT\n    machine_id,\n    machine_type,\n    location,\n    temperature,\n    vibration_level,\n    pressure,\n    rpm,\n    power_consumption,\n    reading_timestamp,\n    -- Performance indicators\n    CASE\n        WHEN temperature > optimal_temp_max OR vibration_level > vibration_threshold\n        THEN 'Performance Degraded'\n        WHEN temperature < optimal_temp_min OR rpm < optimal_rpm_min\n        THEN 'Underperforming'\n        ELSE 'Normal Operation'\n    END as performance_status,\n    -- Maintenance alerts\n    CASE\n        WHEN temperature > critical_temp_max THEN 'URGENT: Overheating'\n        WHEN vibration_level > critical_vibration THEN 'URGENT: Excessive Vibration'\n        WHEN pressure > critical_pressure THEN 'URGENT: Over Pressure'\n        WHEN temperature > optimal_temp_max THEN 'Schedule Maintenance'\n        ELSE 'OK'\n    END as maintenance_alert,\n    -- Efficiency calculation\n    ROUND((actual_output / theoretical_max_output) * 100, 1) as efficiency_percent\nFROM machine_sensors m\nJOIN machine_specifications s ON m.machine_id = s.machine_id\nWHERE reading_timestamp > NOW() - INTERVAL '1' HOUR\nORDER BY maintenance_alert DESC, efficiency_percent ASC;\n\n-- Predictive maintenance scoring\nSELECT\n    machine_id,\n    -- Temperature trend (last 24 hours)\n    AVG(temperature) OVER (\n        PARTITION BY machine_id\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW\n    ) as avg_temp_24h,\n    temperature - AVG(temperature) OVER (\n        PARTITION BY machine_id\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING\n    ) as temp_deviation,\n    -- Vibration trend\n    AVG(vibration_level) OVER (\n        PARTITION BY machine_id\n        ORDER BY reading_timestamp\n        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW\n    ) as avg_vibration_24h,\n    -- Predictive maintenance score\n    (CASE WHEN temperature > 80 THEN 30 ELSE 0 END +\n     CASE WHEN vibration_level > 5 THEN 25 ELSE 0 END +\n     CASE WHEN temperature - AVG(temperature) OVER (\n         PARTITION BY machine_id\n         ORDER BY reading_timestamp\n         ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING\n     ) > 10 THEN 20 ELSE 0 END +\n     CASE WHEN power_consumption > normal_power * 1.2 THEN 15 ELSE 0 END +\n     CASE WHEN rpm < normal_rpm * 0.9 THEN 10 ELSE 0 END) as maintenance_score,\n    -- Maintenance recommendation\n    CASE\n        WHEN (CASE WHEN temperature > 80 THEN 30 ELSE 0 END +\n              CASE WHEN vibration_level > 5 THEN 25 ELSE 0 END) >= 70 THEN 'Immediate Inspection'\n        WHEN (CASE WHEN temperature > 80 THEN 30 ELSE 0 END +\n              CASE WHEN vibration_level > 5 THEN 25 ELSE 0 END) >= 40 THEN 'Schedule Maintenance'\n        WHEN (CASE WHEN temperature > 80 THEN 30 ELSE 0 END) >= 20 THEN 'Monitor Closely'\n        ELSE 'Normal Operation'\n    END as maintenance_recommendation\nFROM machine_sensors_with_specs\nWHERE reading_timestamp > NOW() - INTERVAL '1' HOUR;\n```\n\n## Agricultural IoT\n\n### Soil and Weather Monitoring\n\n```sql\n-- Agricultural conditions analysis\nSELECT\n    field_id,\n    sensor_location,\n    soil_moisture,\n    soil_temperature,\n    soil_ph,\n    air_temperature,\n    humidity,\n    rainfall_mm,\n    wind_speed,\n    reading_timestamp,\n    -- Irrigation recommendations\n    CASE\n        WHEN soil_moisture < 30 THEN 'Immediate Irrigation Needed'\n        WHEN soil_moisture < 50 THEN 'Schedule Irrigation'\n        WHEN soil_moisture > 80 THEN 'Reduce Irrigation'\n        ELSE 'Adequate Moisture'\n    END as irrigation_recommendation,\n    -- Growing conditions\n    CASE\n        WHEN soil_temperature BETWEEN 18 AND 25\n         AND soil_moisture BETWEEN 40 AND 70\n         AND soil_ph BETWEEN 6.0 AND 7.5\n        THEN 'Optimal Growing Conditions'\n        WHEN soil_temperature BETWEEN 15 AND 30\n         AND soil_moisture BETWEEN 30 AND 80\n        THEN 'Good Growing Conditions'\n        ELSE 'Suboptimal Conditions'\n    END as growing_conditions,\n    -- Weather alerts\n    CASE\n        WHEN air_temperature > 35 THEN 'Heat Stress Risk'\n        WHEN air_temperature < 5 THEN 'Frost Risk'\n        WHEN wind_speed > 25 THEN 'High Wind Warning'\n        WHEN rainfall_mm > 25 THEN 'Heavy Rain Alert'\n        ELSE 'Normal Weather'\n    END as weather_alert\nFROM agricultural_sensors\nWHERE reading_timestamp > NOW() - INTERVAL '6' HOURS\nORDER BY field_id, reading_timestamp DESC;\n\n-- Crop growth analysis\nSELECT\n    field_id,\n    crop_type,\n    planting_date,\n    DATEDIFF('days', planting_date, CURRENT_DATE) as days_since_planting,\n    AVG(soil_moisture) as avg_soil_moisture,\n    AVG(soil_temperature) as avg_soil_temp,\n    SUM(rainfall_mm) as total_rainfall,\n    AVG(air_temperature) as avg_air_temp,\n    -- Growth stage estimation\n    CASE\n        WHEN crop_type = 'corn' AND DATEDIFF('days', planting_date, CURRENT_DATE) < 30 THEN 'Germination'\n        WHEN crop_type = 'corn' AND DATEDIFF('days', planting_date, CURRENT_DATE) < 60 THEN 'Vegetative'\n        WHEN crop_type = 'corn' AND DATEDIFF('days', planting_date, CURRENT_DATE) < 90 THEN 'Reproductive'\n        WHEN crop_type = 'corn' AND DATEDIFF('days', planting_date, CURRENT_DATE) < 120 THEN 'Maturation'\n        ELSE 'Harvest Ready'\n    END as growth_stage,\n    -- Yield prediction factors\n    CASE\n        WHEN AVG(soil_moisture) BETWEEN 40 AND 70\n         AND SUM(rainfall_mm) BETWEEN 300 AND 600\n         AND AVG(air_temperature) BETWEEN 20 AND 28\n        THEN 'High Yield Potential'\n        WHEN AVG(soil_moisture) BETWEEN 30 AND 80\n         AND SUM(rainfall_mm) BETWEEN 200 AND 700\n        THEN 'Moderate Yield Potential'\n        ELSE 'Low Yield Potential'\n    END as yield_forecast\nFROM agricultural_sensors a\nJOIN crop_plantings c ON a.field_id = c.field_id\nWHERE reading_timestamp > NOW() - INTERVAL '30' DAYS\nGROUP BY field_id, crop_type, planting_date;\n```\n\n## Performance Optimization Tips\n\n### Efficient IoT Query Patterns\n\n```sql\n-- ✅ Good: Time-based partitioning\nSELECT device_id, AVG(temperature)\nFROM sensor_readings\nWHERE reading_timestamp > NOW() - INTERVAL '1' HOUR  -- Use time-based filters\nGROUP BY device_id;\n\n-- ✅ Good: Aggregate before complex calculations\nWITH hourly_averages AS (\n    SELECT\n        device_id,\n        DATE_FORMAT(reading_timestamp, '%Y-%m-%d %H:00:00') as hour,\n        AVG(temperature) as avg_temp\n    FROM sensor_readings\n    WHERE reading_timestamp > NOW() - INTERVAL '24' HOURS\n    GROUP BY device_id, DATE_FORMAT(reading_timestamp, '%Y-%m-%d %H:00:00')\n)\nSELECT device_id, AVG(avg_temp) as daily_avg\nFROM hourly_averages\nGROUP BY device_id;\n\n-- ⚠️ Consider data retention policies for large IoT datasets\n-- DELETE FROM sensor_readings WHERE reading_timestamp < NOW() - INTERVAL '90' DAYS;\n```\n\n## Quick Reference\n\n### Common IoT Patterns\n| Pattern | Use Case | Example |\n|---------|----------|--------|\n| **Threshold Alerts** | Equipment limits | `WHEN temperature > 80 THEN 'ALERT'` |\n| **Trend Analysis** | Performance degradation | `LAG(value, 24) OVER (ORDER BY timestamp)` |\n| **Statistical Anomalies** | Sensor malfunctions | `ABS(value - AVG(value)) > 2 * STDDEV(value)` |\n| **Predictive Maintenance** | Equipment failure prevention | `CASE WHEN score > 70 THEN 'Maintenance Due'` |\n| **Energy Efficiency** | Cost optimization | `consumption / area` ratios |\n\n### Sensor Data Types\n| Sensor Type | Metrics | Typical Alerts |\n|-------------|---------|----------------|\n| **Environmental** | Temperature, humidity, pressure | Comfort zones, extreme conditions |\n| **Air Quality** | PM2.5, CO2, VOCs | Health thresholds, ventilation needs |\n| **Energy** | kWh, power demand, voltage | Peak usage, efficiency targets |\n| **Vibration** | Frequency, amplitude | Equipment wear, imbalance |\n| **Fluid** | Flow rate, pressure, level | Leaks, blockages, capacity |\n\n---\n\n**💡 Pro Tip**: Always include time-based filtering in IoT queries and consider implementing data aggregation strategies to handle the high volume of sensor data efficiently."
}]