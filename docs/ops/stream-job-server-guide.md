# Velostream StreamJobServer Operations Guide

## Overview

The Velostream StreamJobServer is a production-ready streaming SQL engine that can execute multiple concurrent SQL jobs with full isolation. This guide covers how to operate the deployed server, manage jobs, and create SQL job definition files.

**Prerequisites**: 
- Velostream StreamJobServer deployed (via Docker or Kubernetes)
- Access to the server container or cluster
- Basic SQL knowledge

## Table of Contents

1. [Server Operations](#server-operations)
2. [SQL Job File Format](#sql-job-file-format)
3. [Job Management](#job-management)
4. [Configuration Options](#configuration-options)
5. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
6. [Production Examples](#production-examples)
7. [Best Practices](#best-practices)

## Server Operations

### Checking Server Status

```bash
# Check if StreamJobServer is running
docker exec velo-streams velo-sql-multi --version

# Check server health
curl http://localhost:8081/health

# List running services
docker ps | grep velo
```

### Starting the StreamJobServer

The StreamJobServer runs on port 8081 (by default) and provides these endpoints:

- **Port 8081**: Main API for job management
- **Port 9091**: Metrics and monitoring
- **REST API**: Job control and status endpoints

```bash
# Server is typically started as part of docker-compose
# Check server status
curl http://localhost:8081/status

# View server configuration
curl http://localhost:8081/config
```

### Streaming Engine Architecture

**Yes, the StreamJobServer runs a dedicated streaming engine per query/job:**

#### Per-Job Streaming Engine Isolation

```
┌─────────────────────────────────────────────────────────────────────┐
│                    StreamJobServer                                 │
├─────────────────────────────────────────────────────────────────────┤
│ Job 1: fraud_detection                                              │
│ ├── Dedicated Kafka Consumer (group: fraud_detection)              │
│ ├── Isolated SQL Execution Engine                                  │
│ ├── Private Memory Pool (512MB)                                    │
│ └── Independent State Management                                   │
├─────────────────────────────────────────────────────────────────────┤
│ Job 2: user_analytics                                               │
│ ├── Dedicated Kafka Consumer (group: user_analytics)               │
│ ├── Isolated SQL Execution Engine                                  │
│ ├── Private Memory Pool (1024MB)                                   │
│ └── Independent State Management                                   │
├─────────────────────────────────────────────────────────────────────┤
│ Job 3: iot_monitoring                                               │
│ ├── Dedicated Kafka Consumer (group: iot_monitoring)               │
│ ├── Isolated SQL Execution Engine                                  │
│ ├── Private Memory Pool (256MB)                                    │
│ └── Independent State Management                                   │
└─────────────────────────────────────────────────────────────────────┘
```

#### Engine Isolation Benefits

1. **Memory Isolation**: Each job has its own memory pool
   ```sql
   -- Job 1 gets 2GB memory
   -- JOB: heavy_processing
   -- MEMORY_LIMIT: 2048
   
   -- Job 2 gets 512MB memory  
   -- JOB: light_filtering
   -- MEMORY_LIMIT: 512
   ```

2. **State Isolation**: Window functions and aggregations don't interfere
   ```sql
   -- Job 1: 5-minute windows
   -- WINDOW: TUMBLING (INTERVAL 5 MINUTES)
   
   -- Job 2: 1-hour windows (independent state)
   -- WINDOW: TUMBLING (INTERVAL 1 HOUR)
   ```

3. **Performance Isolation**: One slow job doesn't affect others
   ```sql
   -- Complex job with timeout
   -- JOB: complex_analytics
   -- TIMEOUT: 60000  -- 60 seconds
   
   -- Fast job with short timeout
   -- JOB: real_time_alerts  
   -- TIMEOUT: 5000   -- 5 seconds
   ```

4. **Schema Isolation**: Each job handles its own data format
   ```sql
   -- Job 1: Avro processing
   -- FORMAT: avro
   -- SCHEMA: /app/schemas/orders.avsc
   
   -- Job 2: JSON processing (different engine config)
   -- FORMAT: json
   ```

#### Resource Allocation Model

```bash
# Global server resources
Total Memory: 16GB
Worker Threads: 20

# Per-job allocation
Job 1: 2GB memory, dedicated consumer thread, SQL engine instance
Job 2: 1GB memory, dedicated consumer thread, SQL engine instance  
Job 3: 512MB memory, dedicated consumer thread, SQL engine instance
...remaining resources available for new jobs
```

## SQL Job File Format

### Basic Structure

SQL job files define multiple streaming jobs that run concurrently. Each file can contain:
- Global configuration (affects all jobs)
- Individual job definitions with specific configurations
- SQL queries for stream processing

### File Format Syntax

```sql
-- ==============================================================================
-- VELOSTREAM STREAM JOB SERVER SQL FILE
-- ==============================================================================

-- GLOBAL CONFIGURATION SECTION
-- These settings apply to all jobs in this file
-- CONFIG: parameter_name = value

-- JOB DEFINITION SECTION  
-- Each job has configuration comments followed by SQL
-- JOB: job_name
-- TOPIC: kafka_topic
-- [additional job-specific configuration]
-- START JOB job_name AS
-- SQL_QUERY;

-- Example Structure:
-- CONFIG: max_memory_mb = 4096
-- CONFIG: worker_threads = 8

-- JOB: my_first_job
-- TOPIC: input_data
-- MEMORY_LIMIT: 1024
START JOB my_first_job AS
SELECT * FROM input_data WHERE condition = 'active';

-- JOB: my_second_job  
-- TOPIC: user_events
-- FORMAT: avro
-- SCHEMA: /app/schemas/users.avsc
START JOB my_second_job AS
SELECT user_id, COUNT(*) as events 
FROM user_events 
GROUP BY user_id;
```

### Configuration Comments Format

All configuration is specified using SQL comments with specific prefixes:

| Comment Prefix | Scope | Purpose |
|---------------|-------|---------|
| `-- CONFIG:` | Global | Applies to all jobs in the file |
| `-- JOB:` | Job | Defines job name (required) |
| `-- TOPIC:` | Job | Input Kafka topic (required) |
| `-- FORMAT:` | Job | Data format (json/avro/protobuf) |
| `-- SCHEMA:` | Job | Schema file path for Avro |
| `-- PROTO:` | Job | Proto file path for Protobuf |
| `-- GROUP:` | Job | Consumer group name |
| `-- MEMORY_LIMIT:` | Job | Memory limit in MB |
| `-- TIMEOUT:` | Job | Query timeout in milliseconds |
| `-- OUTPUT_TOPIC:` | Job | Output Kafka topic |
| `-- WINDOW:` | Job | Window specification |

### Multi-Topic Avro Schema Handling

The StreamJobServer handles different Avro schemas per topic by allowing each job to specify its own schema file:

#### Individual Schema Per Job
```sql
-- Job 1: Orders (uses orders schema)
-- JOB: order_processor
-- TOPIC: orders
-- FORMAT: avro
-- SCHEMA: /app/schemas/orders.avsc
START JOB order_processor AS
SELECT order_id, customer_id, amount FROM orders;

-- Job 2: Users (uses users schema)  
-- JOB: user_processor
-- TOPIC: users
-- FORMAT: avro
-- SCHEMA: /app/schemas/users.avsc
START JOB user_processor AS
SELECT user_id, email, signup_date FROM users;

-- Job 3: Products (uses products schema)
-- JOB: product_processor
-- TOPIC: products
-- FORMAT: avro
-- SCHEMA: /app/schemas/products.avsc
START JOB product_processor AS
SELECT product_id, name, price FROM products;
```

#### Schema File Organization
```bash
# Recommended schema directory structure
/app/schemas/
├── orders.avsc           # Orders topic schema
├── users.avsc            # Users topic schema  
├── products.avsc         # Products topic schema
├── financial/
│   ├── trades.avsc       # Trading data schema
│   └── positions.avsc    # Position data schema
└── iot/
    ├── sensors.avsc      # Sensor reading schema
    └── alerts.avsc       # Alert schema
```

#### Mixed Format Jobs
```sql
-- Different jobs can use different formats
-- JOB: json_processor
-- TOPIC: raw_events
-- FORMAT: json              -- No schema needed
START JOB json_processor AS
SELECT event_type, timestamp FROM raw_events;

-- JOB: avro_processor  
-- TOPIC: structured_data
-- FORMAT: avro
-- SCHEMA: /app/schemas/structured_data.avsc
START JOB avro_processor AS
SELECT user_id, action FROM structured_data;

-- JOB: protobuf_processor
-- TOPIC: high_frequency_data
-- FORMAT: protobuf
-- PROTO: /app/schemas/trades.proto
START JOB protobuf_processor AS
SELECT symbol, price FROM high_frequency_data;
```

## Job Management  

### Deploying Jobs from SQL Files

```bash
# Deploy all jobs from a SQL file
docker exec velo-streams velo-sql-multi deploy-app \
  --file /app/sql/my_jobs.sql \
  --brokers kafka:9092 \
  --continuous

# Deploy with custom configuration
docker exec velo-streams velo-sql-multi deploy-app \
  --file /app/sql/trading_jobs.sql \
  --brokers kafka:9092 \
  --job-prefix "prod_" \
  --group-prefix "production_" \
  --max-jobs 20 \
  --continuous
```

### Individual Job Control

```bash
# List all jobs
docker exec velo-streams velo-sql-multi list-jobs
docker exec velo-streams velo-sql-multi list-jobs --format table

# Start/stop specific jobs
docker exec velo-streams velo-sql-multi start-job --job-id fraud_detection
docker exec velo-streams velo-sql-multi stop-job --job-id fraud_detection
docker exec velo-streams velo-sql-multi pause-job --job-id fraud_detection  
docker exec velo-streams velo-sql-multi resume-job --job-id fraud_detection

# Get job status
docker exec velo-streams velo-sql-multi job-status --job-id fraud_detection
docker exec velo-streams velo-sql-multi job-status --job-id fraud_detection --format json

# View job output in real-time
docker exec -it velo-streams velo-sql-multi stream-job \
  --job-id fraud_detection \
  --output stdout \
  --format json-lines

# View logs for specific job
docker exec velo-streams velo-sql-multi job-logs \
  --job-id fraud_detection \
  --tail 100 \
  --follow
```

### Bulk Job Operations

```bash
# Start all stopped jobs
docker exec velo-streams velo-sql-multi start-all-jobs

# Stop all running jobs
docker exec velo-streams velo-sql-multi stop-all-jobs  

# Get status of all jobs
docker exec velo-streams velo-sql-multi list-jobs --status all --format json

# Stream output from all jobs
docker exec -it velo-streams velo-sql-multi stream-all \
  --output stdout \
  --format json-lines \
  --include-metadata
```

## Configuration Options

### Global Configuration Options

These settings in SQL files apply to all jobs and affect server behavior:

```sql
-- PERFORMANCE SETTINGS
-- CONFIG: max_memory_mb = 8192           -- Total memory limit for all jobs
-- CONFIG: worker_threads = 16            -- Number of worker threads
-- CONFIG: batch_size = 1000              -- Processing batch size
-- CONFIG: flush_interval_ms = 100        -- Output flush interval

-- CONSUMER SETTINGS  
-- CONFIG: group_prefix = "myapp_"        -- Prefix for consumer group names
-- CONFIG: job_prefix = "job_"            -- Prefix for job names
-- CONFIG: session_timeout_ms = 30000     -- Kafka session timeout
-- CONFIG: heartbeat_interval_ms = 10000  -- Kafka heartbeat interval

-- RELIABILITY SETTINGS
-- CONFIG: timeout_ms = 60000             -- Default job timeout
-- CONFIG: retry_attempts = 3             -- Number of retry attempts on failure
-- CONFIG: restart_policy = "on-failure"  -- Job restart policy (always/never/on-failure)

-- SERIALIZATION SETTINGS
-- CONFIG: default_format = "json"        -- Default serialization format
-- CONFIG: financial_precision = true     -- Enable financial precision arithmetic
-- CONFIG: decimal_places = 4             -- Default decimal precision

-- MONITORING SETTINGS
-- CONFIG: metrics_enabled = true         -- Enable detailed metrics collection
-- CONFIG: health_check_interval_ms = 30000 -- Health check frequency
```

### Per-Job Configuration Options

Each job can have specific configuration that overrides global settings:

```sql
-- JOB IDENTIFICATION (REQUIRED)
-- JOB: unique_job_name                   -- Must be unique within the file
-- TOPIC: input_kafka_topic               -- Source Kafka topic (required)

-- DATA FORMAT CONFIGURATION
-- FORMAT: json                           -- Data format: json/avro/protobuf
-- SCHEMA: /app/schemas/orders.avsc       -- Avro schema file path
-- PROTO: /app/schemas/trades.proto       -- Protobuf definition file

-- CONSUMER CONFIGURATION
-- GROUP: custom_consumer_group           -- Override auto-generated group name
-- PARTITION: 0                           -- Specific partition (optional)
-- OFFSET_RESET: earliest                 -- Offset reset policy: earliest/latest

-- PROCESSING CONFIGURATION
-- MEMORY_LIMIT: 1024                     -- Job memory limit in MB
-- TIMEOUT: 30000                         -- Job timeout in milliseconds  
-- BATCH_SIZE: 500                        -- Processing batch size for this job
-- BUFFER_SIZE: 10000                     -- Internal buffer size

-- WINDOWING CONFIGURATION
-- WINDOW: TUMBLING (INTERVAL 5 MINUTES)  -- Window specification
-- WINDOW: SLIDING (INTERVAL 10 MINUTES)  -- Sliding window
-- WINDOW: SESSION (TIMEOUT 30 MINUTES)   -- Session window

-- OUTPUT CONFIGURATION
-- OUTPUT_TOPIC: processed_data           -- Destination Kafka topic
-- OUTPUT_FORMAT: json                    -- Output format (can differ from input)
-- OUTPUT_PARTITION_KEY: customer_id      -- Partition key for output

-- MONITORING CONFIGURATION
-- ENABLE_METRICS: true                   -- Enable per-job metrics
-- LOG_LEVEL: INFO                        -- Job-specific log level (DEBUG/INFO/WARN/ERROR)
```

### Configuration Priority Order

Configuration is applied in this priority order (highest to lowest):

1. **Command line parameters** (highest priority)
2. **Per-job configuration** in SQL file comments  
3. **Global configuration** in SQL file
4. **Server default configuration** (lowest priority)

Example showing override behavior:
```sql
-- Global setting
-- CONFIG: timeout_ms = 60000

-- Job-specific override
-- JOB: quick_processing
-- TIMEOUT: 5000                          -- This job gets 5 second timeout
START JOB quick_processing AS SELECT * FROM fast_data;

-- Another job inherits global setting  
-- JOB: slow_processing                   -- This job gets 60 second timeout
START JOB slow_processing AS SELECT * FROM complex_data;
```

## Monitoring and Troubleshooting

### Job Status Monitoring

```bash
# Overview of all jobs
docker exec velo-streams velo-sql-multi list-jobs --format table

# Detailed job information
docker exec velo-streams velo-sql-multi job-status \
  --job-id my_job \
  --format json

# Real-time job metrics
curl http://localhost:9091/metrics/job/my_job

# Consumer group lag monitoring  
curl http://localhost:9091/metrics/consumer-lag
```

### Health Checks

```bash
# Overall server health
curl http://localhost:9091/health

# Individual job health
curl http://localhost:9091/health/job/my_job

# Server resource usage
curl http://localhost:9091/metrics/resources
```

### Log Management

```bash
# View job logs
docker exec velo-streams velo-sql-multi job-logs \
  --job-id my_job \
  --tail 50

# Follow logs in real-time
docker exec velo-streams velo-sql-multi job-logs \
  --job-id my_job \
  --follow

# Search logs for errors
docker exec velo-streams velo-sql-multi job-logs \
  --job-id my_job \
  --grep "ERROR"

# Export logs to file
docker exec velo-streams velo-sql-multi job-logs \
  --job-id my_job \
  --since "2024-01-01T00:00:00Z" > job_logs.txt
```

### Troubleshooting Common Issues

#### Job Won't Start
```bash
# Check job configuration
docker exec velo-streams velo-sql-multi job-status --job-id my_job

# Validate SQL file syntax
docker exec velo-streams velo-sql-multi validate-sql --file /app/sql/jobs.sql

# Check resource availability
curl http://localhost:9091/metrics/resources
```

#### High Memory Usage
```bash
# Check per-job memory usage
curl http://localhost:9091/metrics/memory

# Adjust job memory limits
# Edit SQL file and redeploy with lower MEMORY_LIMIT values
```

#### Consumer Lag Issues
```bash
# Check consumer lag metrics
curl http://localhost:9091/metrics/consumer-lag

# View job processing rates
docker exec velo-streams velo-sql-multi job-status --job-id my_job --format json
```

## Production Examples

### E-commerce Real-time Analytics

Create file `ecommerce_jobs.sql`:

```sql
-- ==============================================================================  
-- E-COMMERCE REAL-TIME ANALYTICS JOBS
-- ==============================================================================

-- GLOBAL CONFIGURATION
-- CONFIG: max_memory_mb = 8192
-- CONFIG: worker_threads = 16  
-- CONFIG: group_prefix = "ecommerce_"
-- CONFIG: financial_precision = true
-- CONFIG: metrics_enabled = true

-- ==============================================================================
-- JOB 1: ORDER PROCESSING
-- ==============================================================================
-- JOB: order_processor
-- TOPIC: orders
-- FORMAT: avro
-- SCHEMA: /app/schemas/orders.avsc
-- MEMORY_LIMIT: 2048
-- TIMEOUT: 15000
-- OUTPUT_TOPIC: processed_orders
-- ENABLE_METRICS: true
START JOB order_processor AS
SELECT 
    order_id,
    customer_id,
    amount,
    currency,
    status,
    created_at,
    CASE 
        WHEN amount > 1000.00 THEN 'VIP_ORDER'
        WHEN amount > 100.00 THEN 'STANDARD_ORDER'
        ELSE 'SMALL_ORDER'
    END as order_category,
    amount * 0.1 as estimated_profit
FROM orders 
WHERE status IN ('CONFIRMED', 'PROCESSING', 'SHIPPED');

-- ==============================================================================  
-- JOB 2: CUSTOMER SEGMENTATION  
-- ==============================================================================
-- JOB: customer_segments
-- TOPIC: orders
-- FORMAT: avro
-- SCHEMA: /app/schemas/orders.avsc  
-- WINDOW: TUMBLING (INTERVAL 1 HOUR)
-- MEMORY_LIMIT: 1536
-- TIMEOUT: 30000
-- OUTPUT_TOPIC: customer_segments
START JOB customer_segments AS
SELECT 
    customer_id,
    COUNT(*) as orders_this_hour,
    SUM(amount) as total_spent_this_hour,
    AVG(amount) as avg_order_value,
    MAX(amount) as highest_order,
    CASE 
        WHEN SUM(amount) > 5000.00 THEN 'PLATINUM'
        WHEN SUM(amount) > 1000.00 THEN 'GOLD'  
        WHEN SUM(amount) > 500.00 THEN 'SILVER'
        ELSE 'BRONZE'
    END as hourly_tier,
    CURRENT_TIMESTAMP as calculated_at
FROM orders
WINDOW TUMBLING(1h)  
GROUP BY customer_id
HAVING COUNT(*) >= 2;

-- ==============================================================================
-- JOB 3: INVENTORY ALERTS
-- ==============================================================================  
-- JOB: inventory_monitor
-- TOPIC: inventory_updates
-- FORMAT: json
-- MEMORY_LIMIT: 512
-- TIMEOUT: 10000
-- OUTPUT_TOPIC: inventory_alerts
-- LOG_LEVEL: DEBUG
START JOB inventory_monitor AS
SELECT 
    product_id,
    product_name,
    current_stock,
    reorder_level,
    supplier_id,
    last_updated,
    CASE 
        WHEN current_stock = 0 THEN 'OUT_OF_STOCK'
        WHEN current_stock <= reorder_level THEN 'REORDER_NOW'
        WHEN current_stock <= (reorder_level * 1.5) THEN 'LOW_STOCK_WARNING'
        ELSE 'STOCK_OK'
    END as alert_level,
    (reorder_level - current_stock) as units_to_order
FROM inventory_updates 
WHERE current_stock <= (reorder_level * 2.0);

-- ==============================================================================
-- JOB 4: FRAUD DETECTION
-- ==============================================================================
-- JOB: fraud_detector  
-- TOPIC: payments
-- FORMAT: json
-- MEMORY_LIMIT: 2048
-- TIMEOUT: 5000
-- OUTPUT_TOPIC: fraud_alerts
-- ENABLE_METRICS: true
-- LOG_LEVEL: WARN
START JOB fraud_detector AS
SELECT 
    transaction_id,
    customer_id, 
    amount,
    payment_method,
    merchant_id,
    country_code,
    timestamp,
    CASE 
        WHEN amount > 10000.00 THEN 'HIGH_AMOUNT_RISK'
        WHEN payment_method = 'CRYPTOCURRENCY' THEN 'CRYPTO_RISK'
        WHEN country_code IN ('XX', 'YY', 'ZZ') THEN 'COUNTRY_RISK'
        ELSE 'LOW_RISK'
    END as risk_level,
    amount * 0.001 as potential_loss
FROM payments 
WHERE amount > 100.00 
   OR payment_method IN ('CRYPTOCURRENCY', 'WIRE_TRANSFER')
   OR country_code NOT IN ('US', 'CA', 'UK', 'DE', 'FR');
```

Deploy the jobs:
```bash
# Deploy all e-commerce jobs
docker exec velo-streams velo-sql-multi deploy-app \
  --file /app/sql/ecommerce_jobs.sql \
  --brokers kafka:9092 \
  --continuous

# Monitor job status  
docker exec velo-streams velo-sql-multi list-jobs --format table

# View real-time results
docker exec -it velo-streams velo-sql-multi stream-all \
  --format json-lines \
  --include-metadata
```

### Financial Trading System

Create file `trading_jobs.sql`:

```sql
-- ==============================================================================
-- FINANCIAL TRADING SYSTEM JOBS  
-- ==============================================================================

-- GLOBAL CONFIGURATION
-- CONFIG: max_memory_mb = 16384
-- CONFIG: worker_threads = 20
-- CONFIG: group_prefix = "trading_"
-- CONFIG: financial_precision = true
-- CONFIG: decimal_places = 4
-- CONFIG: batch_size = 10000
-- CONFIG: flush_interval_ms = 50

-- ==============================================================================
-- JOB 1: REAL-TIME PRICE MONITORING
-- ==============================================================================
-- JOB: price_monitor
-- TOPIC: market_data
-- FORMAT: protobuf
-- PROTO: /app/schemas/trades.proto
-- MEMORY_LIMIT: 4096
-- TIMEOUT: 1000
-- OUTPUT_TOPIC: price_alerts
-- BATCH_SIZE: 50000
-- ENABLE_METRICS: true
START JOB price_monitor AS
SELECT 
    symbol,
    price,
    volume,
    timestamp,
    price * volume as notional_value,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as previous_price,
    CASE 
        WHEN price > (LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 1.05) THEN 'SPIKE_UP'
        WHEN price < (LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 0.95) THEN 'SPIKE_DOWN'
        ELSE 'NORMAL'
    END as price_movement
FROM market_data 
WHERE volume > 1000;

-- ==============================================================================
-- JOB 2: RISK CALCULATION
-- ==============================================================================  
-- JOB: risk_calculator
-- TOPIC: positions
-- FORMAT: protobuf  
-- PROTO: /app/schemas/positions.proto
-- WINDOW: SLIDING (INTERVAL 5 MINUTES)
-- MEMORY_LIMIT: 8192
-- TIMEOUT: 10000
-- OUTPUT_TOPIC: risk_metrics
START JOB risk_calculator AS
SELECT 
    portfolio_id,
    symbol,
    SUM(position_size * current_price) as total_exposure,
    COUNT(*) as position_count,
    STDDEV(price_change_pct) as volatility,
    MAX(ABS(position_size * current_price)) as max_single_exposure,
    SUM(CASE WHEN position_size > 0 THEN position_size * current_price ELSE 0 END) as long_exposure,
    SUM(CASE WHEN position_size < 0 THEN ABS(position_size * current_price) ELSE 0 END) as short_exposure,
    CURRENT_TIMESTAMP as risk_calc_time
FROM positions 
WINDOW SLIDING(5m, 1m)
GROUP BY portfolio_id, symbol
HAVING SUM(ABS(position_size * current_price)) > 100000.00;

-- ==============================================================================
-- JOB 3: COMPLIANCE MONITORING
-- ==============================================================================
-- JOB: compliance_check
-- TOPIC: trades
-- FORMAT: protobuf
-- PROTO: /app/schemas/trades.proto  
-- MEMORY_LIMIT: 2048
-- TIMEOUT: 15000
-- OUTPUT_TOPIC: compliance_violations
-- LOG_LEVEL: WARN
START JOB compliance_check AS
SELECT 
    trade_id,
    trader_id,
    symbol,
    quantity,
    price,
    trade_time,
    notional_amount,
    CASE 
        WHEN notional_amount > 1000000.00 THEN 'LARGE_TRADE_REPORTING'
        WHEN symbol IN ('RESTRICTED_SYMBOL_1', 'RESTRICTED_SYMBOL_2') THEN 'RESTRICTED_INSTRUMENT'
        WHEN trade_time NOT BETWEEN '09:30:00' AND '16:00:00' THEN 'AFTER_HOURS_TRADE'
        ELSE 'COMPLIANT'  
    END as compliance_status,
    notional_amount * 0.0001 as regulatory_fee
FROM trades 
WHERE notional_amount > 10000.00 
   OR symbol LIKE 'RESTRICTED_%'
   OR trade_time NOT BETWEEN '09:30:00' AND '16:00:00';
```

## Best Practices

### SQL File Organization

1. **Use Clear Job Names**: Make job names descriptive of their purpose
   ```sql  
   -- Good
   -- JOB: fraud_detection_real_time
   -- JOB: customer_segmentation_hourly
   
   -- Avoid
   -- JOB: job1
   -- JOB: process_data
   ```

2. **Group Related Jobs**: Keep related jobs in the same file
   ```sql
   -- File: trading_jobs.sql - all trading-related jobs
   -- File: analytics_jobs.sql - all analytics jobs  
   -- File: monitoring_jobs.sql - all monitoring jobs
   ```

3. **Set Appropriate Resource Limits**:
   ```sql
   -- Start conservative, tune based on monitoring
   -- MEMORY_LIMIT: 512      -- For simple filtering jobs
   -- MEMORY_LIMIT: 2048     -- For aggregation jobs  
   -- MEMORY_LIMIT: 4096     -- For complex windowing jobs
   ```

### Operational Best Practices

1. **Monitor Job Health**: Check job status regularly
   ```bash
   # Set up automated monitoring
   docker exec velo-streams velo-sql-multi list-jobs --status failed
   ```

2. **Use Staged Deployments**: Test jobs before production
   ```bash
   # Deploy to staging first
   docker exec velo-streams-staging velo-sql-multi deploy-app --file jobs.sql
   
   # Then production
   docker exec velo-streams-prod velo-sql-multi deploy-app --file jobs.sql
   ```

3. **Manage Consumer Groups**: Use meaningful prefixes
   ```sql
   -- CONFIG: group_prefix = "production_v2_"
   -- Results in: production_v2_fraud_detection, production_v2_analytics, etc.
   ```

4. **Implement Gradual Rollouts**: Start/stop jobs gradually
   ```bash
   # Start critical jobs first
   docker exec velo-streams velo-sql-multi start-job --job-id payment_processing
   
   # Then supporting jobs
   docker exec velo-streams velo-sql-multi start-job --job-id analytics_aggregation
   ```

### Multi-Topic Avro Example

Here's a complete example showing how to handle multiple topics with different Avro schemas:

Create the SQL file `multi_topic_avro.sql`:

```sql
-- ==============================================================================
-- MULTI-TOPIC AVRO PROCESSING EXAMPLE
-- ==============================================================================

-- GLOBAL CONFIGURATION
-- CONFIG: max_memory_mb = 4096
-- CONFIG: group_prefix = "multi_avro_"
-- CONFIG: financial_precision = true

-- ==============================================================================
-- JOB 1: E-COMMERCE ORDERS (orders.avsc)
-- ==============================================================================
-- JOB: ecommerce_orders
-- TOPIC: orders
-- FORMAT: avro
-- SCHEMA: /app/schemas/orders.avsc
-- MEMORY_LIMIT: 1024
-- OUTPUT_TOPIC: processed_orders
START JOB ecommerce_orders AS
SELECT 
    order_id,
    customer_id,
    amount,
    currency,
    status,
    created_at
FROM orders 
WHERE status = 'CONFIRMED';

-- ==============================================================================
-- JOB 2: FINANCIAL TRADES (trades.avsc)
-- ==============================================================================
-- JOB: financial_trades  
-- TOPIC: trades
-- FORMAT: avro
-- SCHEMA: /app/schemas/financial/trades.avsc
-- MEMORY_LIMIT: 2048
-- OUTPUT_TOPIC: processed_trades
-- TIMEOUT: 5000
START JOB financial_trades AS
SELECT 
    symbol,
    price,
    quantity,
    side,
    timestamp,
    price * quantity as notional_value
FROM trades 
WHERE price > 0.01;

-- ==============================================================================
-- JOB 3: IOT SENSORS (sensors.avsc)  
-- ==============================================================================
-- JOB: iot_sensors
-- TOPIC: sensor_readings
-- FORMAT: avro
-- SCHEMA: /app/schemas/iot/sensors.avsc
-- WINDOW: TUMBLING (INTERVAL 5 MINUTES)
-- MEMORY_LIMIT: 512
-- OUTPUT_TOPIC: sensor_alerts
START JOB iot_sensors AS
SELECT 
    device_id,
    sensor_type,
    AVG(value) as avg_value,
    MAX(value) as max_value,
    COUNT(*) as reading_count
FROM sensor_readings 
WINDOW TUMBLING(5m)
GROUP BY device_id, sensor_type
HAVING AVG(value) > 75.0;

-- ==============================================================================
-- JOB 4: USER EVENTS (users.avsc)
-- ==============================================================================
-- JOB: user_analytics
-- TOPIC: user_events  
-- FORMAT: avro
-- SCHEMA: /app/schemas/users.avsc
-- MEMORY_LIMIT: 1024
-- OUTPUT_TOPIC: user_segments
-- WINDOW: SESSION (TIMEOUT 30 MINUTES)
START JOB user_analytics AS
SELECT 
    user_id,
    COUNT(*) as event_count,
    MAX(timestamp) as last_activity,
    MIN(timestamp) as session_start
FROM user_events 
WINDOW SESSION (TIMEOUT 30 MINUTES)
GROUP BY user_id
HAVING COUNT(*) > 5;
```

Deploy with proper schema mounting:

```bash
# Ensure schemas are mounted in the container
docker run -d \
  -v $(pwd)/schemas:/app/schemas:ro \
  -v $(pwd)/multi_topic_avro.sql:/app/sql/jobs.sql \
  velostream:latest

# Deploy the multi-topic jobs
docker exec velo-streams velo-sql-multi deploy-app \
  --file /app/sql/multi_topic_avro.sql \
  --brokers kafka:9092 \
  --continuous

# Monitor all jobs
docker exec velo-streams velo-sql-multi list-jobs --format table
```

### Schema Evolution Support

Each job handles its own schema evolution independently:

```sql
-- Old version jobs continue with v1 schemas
-- JOB: legacy_orders
-- SCHEMA: /app/schemas/orders_v1.avsc

-- New version jobs use v2 schemas  
-- JOB: new_orders
-- SCHEMA: /app/schemas/orders_v2.avsc
```

This approach allows gradual migration of schemas without affecting other jobs.

This guide provides everything needed to operate the Velostream StreamJobServer in production environments without requiring access to source code.