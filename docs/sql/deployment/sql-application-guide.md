# SQL Application Deployment Guide

## ✅ **Yes, you can deploy a single .sql file with multiple SQL statements as an app of related queries!**

This guide demonstrates how to use the new **SQL Application** feature that allows you to deploy multiple related SQL statements from a single `.sql` file as a cohesive streaming application.

## 🎯 What are SQL Applications?

**SQL Applications** are collections of related SQL statements that work together as a cohesive unit. They enable you to:

- **Deploy Multiple Jobs**: From a single `.sql` file with multiple SQL statements
- **Manage Dependencies**: Track relationships between statements and resources
- **Version Control**: Manage application versions and metadata
- **Organize Logic**: Group related streaming queries into logical applications

## 📁 SQL Application File Format

### Basic Structure

```sql
-- SQL Application: Your Application Name
-- Version: 1.0.0
-- Description: Application description
-- Author: Your Name
-- Dependencies: kafka-topic1, kafka-topic2
-- Tag: environment:production
-- Tag: domain:analytics

-- Name: Job 1 Name
-- Property: priority=high
-- Property: replicas=3
START JOB job_name_1 AS
SELECT * FROM topic1 WHERE condition1;

-- Name: Job 2 Name  
-- Property: priority=medium
START JOB job_name_2 AS
SELECT * FROM topic2 WHERE condition2;
```

### Metadata Comments

| Comment | Required | Description |
|---------|----------|-------------|
| `-- SQL Application: <name>` | ✅ **Required** | Application name |
| `-- Version: <version>` | Optional | Version (defaults to 1.0.0) |
| `-- Description: <text>` | Optional | Application description |
| `-- Author: <name>` | Optional | Author information |
| `-- Dependencies: <list>` | Optional | Comma-separated dependencies |
| `-- Tag: <key>:<value>` | Optional | Custom tags for metadata |
| `-- @observability.metrics.enabled: <true\|false>` | Optional | Enable metrics collection for all jobs |
| `-- @observability.tracing.enabled: <true\|false>` | Optional | Enable distributed tracing for all jobs |
| `-- @observability.profiling.enabled: <true\|false>` | Optional | Enable profiling for all jobs |

**ℹ️ Note**: For complete documentation on SQL Application annotations, see [SQL Application Annotations Reference](./sql-application-annotations.md).

### Statement-Level Comments

| Comment | Description |
|---------|-------------|
| `-- Name: <job_name>` | Human-readable job name |
| `-- Property: <key>=<value>` | Job-specific properties |

## 🚀 Deployment Commands

### Option 1: Deploy SQL Application

```bash
# Deploy from a .sql application file
velo-sql-multi deploy-app \
  --file examples/ecommerce_analytics.sql \
  --brokers localhost:9092 \
  --default-topic orders
```

### Option 2: Deploy to Running Server

```bash
# Start StreamJobServer first
velo-sql-multi server \
  --brokers localhost:9092 \
  --max-jobs 20 \
  --port 8080

# Then deploy application (in another terminal)
velo-sql-multi deploy-app \
  --file examples/iot_monitoring.sql \
  --brokers localhost:9092 \
  --default-topic sensor_data
```

## 📊 Example Applications

### 1. E-commerce Analytics (`examples/ecommerce_analytics.sql`)

**5 Related Jobs:**
- **High Value Orders**: Process orders > $1000
- **User Activity Analytics**: Track user behaviors  
- **Fraud Detection**: Real-time fraud alerts
- **Customer Segmentation**: Categorize customers by spend
- **Product Analytics**: Track product performance

```bash
# Deploy the complete e-commerce analytics platform
velo-sql-multi deploy-app \
  --file examples/ecommerce_analytics.sql \
  --brokers kafka-prod:9092 \
  --default-topic orders
```

### 2. IoT Monitoring (`examples/iot_monitoring.sql`)

**5 Related Jobs:**
- **Temperature Alerts**: Monitor critical temperatures
- **Pressure Monitoring**: Track pressure thresholds
- **Vibration Analysis**: Analyze equipment vibrations
- **Battery Monitoring**: Track device battery levels
- **Sensor Health**: Monitor sensor connectivity

```bash
# Deploy the complete IoT monitoring platform
velo-sql-multi deploy-app \
  --file examples/iot_monitoring.sql \
  --brokers kafka-iot:9092 \
  --default-topic sensor_data
```

### 3. Financial Trading (`examples/financial_trading.sql`)

**5 Related Jobs:**
- **Price Movement Detection**: Monitor significant price changes
- **Volume Spike Analysis**: Detect unusual trading volumes
- **Risk Management**: Monitor position limits and P&L
- **Order Flow Imbalance**: Track buy/sell imbalances  
- **Arbitrage Detection**: Find cross-exchange opportunities

```bash
# Deploy the complete trading analytics platform
velo-sql-multi deploy-app \
  --file examples/financial_trading.sql \
  --brokers kafka-trading:9092 \
  --default-topic market_data
```

### 4. Social Media Analytics (`examples/social_media_analytics.sql`)

**5 Related Jobs:**
- **Trending Hashtags**: Monitor viral hashtags
- **Viral Content Detection**: Identify viral posts
- **Sentiment Analysis**: Analyze post sentiment
- **Influencer Monitoring**: Track influencer activity
- **Crisis Detection**: Detect emergency situations

```bash
# Deploy the complete social media analytics platform
velo-sql-multi deploy-app \
  --file examples/social_media_analytics.sql \
  --brokers kafka-social:9092 \
  --default-topic social_posts
```

## 🎛️ Application Lifecycle Management

### Deployment Output

When you deploy a SQL application, you'll see:

```
[INFO] Deploying SQL application from file: examples/ecommerce_analytics.sql
[INFO] Parsed SQL application 'E-commerce Analytics Platform' version '1.2.0' with 5 statements
[INFO] Successfully deployed job 'high_value_orders' from application
[INFO] Successfully deployed job 'user_activity_analytics' from application  
[INFO] Successfully deployed job 'fraud_detection' from application
[INFO] Successfully deployed job 'customer_segmentation' from application
[INFO] Successfully deployed job 'product_analytics' from application
[INFO] Successfully deployed 5 jobs from SQL application 'E-commerce Analytics Platform'

SQL application deployment completed!
Application: E-commerce Analytics Platform v1.2.0
Deployed 5 jobs: ["high_value_orders", "user_activity_analytics", "fraud_detection", "customer_segmentation", "product_analytics"]
Description: Complete e-commerce data processing pipeline for real-time analytics
Author: Analytics Team
Jobs are now running. Use Ctrl+C to stop.

[INFO] Application 'E-commerce Analytics Platform' - Active jobs: 5
[INFO]   Job 'high_value_orders' (orders): Running - 1250 records processed
[INFO]   Job 'user_activity_analytics' (user_events): Running - 3420 records processed
[INFO]   Job 'fraud_detection' (orders): Running - 89 records processed
[INFO]   Job 'customer_segmentation' (orders): Running - 156 records processed  
[INFO]   Job 'product_analytics' (product_events): Running - 2840 records processed
```

### Real-time Monitoring

Applications provide comprehensive monitoring:

```
[2024-01-15 14:25:15] Application 'E-commerce Analytics Platform' - Active jobs: 5
[2024-01-15 14:25:15]   Job 'high_value_orders' (orders): Running - 15430 records processed
[2024-01-15 14:25:15]   Job 'user_activity_analytics' (user_events): Running - 8920 records processed
[2024-01-15 14:25:15]   Job 'fraud_detection' (orders): Running - 1250 records processed
[2024-01-15 14:25:15]   Job 'customer_segmentation' (orders): Running - 2150 records processed
[2024-01-15 14:25:15]   Job 'product_analytics' (product_events): Running - 12300 records processed
```

## 🔧 Advanced Features

### Topic Resolution

The system intelligently resolves Kafka topics for each job:

1. **FROM clause analysis**: Extracts topics from `FROM table_name`
2. **JOIN clause analysis**: Extracts topics from `JOIN table_name`
3. **Default topic fallback**: Uses `--default-topic` if no topic found
4. **Dependency mapping**: Uses dependencies list from statement metadata

### Statement Dependencies

```sql
-- Name: Enriched Orders Stream
-- Property: depends_on=raw_orders,users
START JOB enriched_orders AS
SELECT 
    o.order_id,
    o.customer_id,
    u.user_name
FROM raw_orders o
JOIN users u ON o.customer_id = u.user_id;
```

Dependencies are automatically extracted from:
- `FROM` clauses
- `JOIN` clauses  
- Statement metadata properties

### Job Properties

```sql
-- Property: priority=high
-- Property: replicas=3
-- Property: memory_limit=2gb
-- Property: cpu_limit=1000m
-- Property: output.topic=custom_output
START JOB my_job AS SELECT * FROM input_stream;
```

Properties are passed to the job execution environment and can control:
- Resource allocation
- Scaling parameters
- Output routing
- Processing priorities

## 🔍 App-Level Observability Configuration

### Overview

Observability settings can be configured at the **application level** to apply consistently across all jobs in your SQL application. This eliminates repetitive per-job configuration and ensures uniform monitoring across related streams.

### Three Observability Dimensions

#### 1. **Metrics** (`@observability.metrics.enabled`)
Enables Prometheus-compatible metrics collection for all jobs:
- Query execution times
- Record processing rates
- Error counts and types
- Throughput statistics

#### 2. **Tracing** (`@observability.tracing.enabled`)
Enables distributed tracing for comprehensive observability:
- End-to-end query execution traces
- Span hierarchy for complex operations
- Latency analysis
- Integration with OpenTelemetry

#### 3. **Profiling** (`@observability.profiling.enabled`)
Enables performance profiling and bottleneck detection:
- CPU usage tracking
- Memory consumption analysis
- Automatic bottleneck detection
- Performance recommendations

### Basic App-Level Configuration

```sql
-- SQL Application: Financial Trading Analytics
-- Version: 1.0.0
-- Description: Real-time trading analytics platform
-- Author: Analytics Team
-- Dependencies: market_data, positions, orders
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false

-- Name: Market Data Stream
START JOB market_data_ts AS
SELECT * FROM market_data;

-- Name: Trading Positions Monitor
START JOB trading_positions_with_event_time AS
SELECT * FROM positions;
```

### Configuration Inheritance and Overrides

**Key Principles:**
- App-level observability settings apply to **all jobs** in the application
- Per-job settings **override** app-level settings when explicitly configured
- Per-job settings are checked before applying app-level defaults

```sql
-- SQL Application: Mixed Observability Requirements
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- Name: Standard Job (inherits app-level settings)
-- Inherits: metrics=true, tracing=true
START JOB standard_job AS SELECT * FROM stream1;

-- Name: Low-Overhead Job (overrides tracing)
-- WITH (observability.tracing.enabled = false)
-- Result: metrics=true, tracing=false
START JOB low_overhead_job AS SELECT * FROM stream2;

-- Name: High-Observability Job (adds profiling)
-- WITH (observability.profiling.enabled = true)
-- Result: metrics=true, tracing=true, profiling=true
START JOB high_observability_job AS SELECT * FROM stream3;
```

### Production Example

```sql
-- SQL Application: Production E-Commerce Platform
-- Version: 2.0.0
-- Description: Complete e-commerce analytics with unified observability
-- Author: Platform Engineering
-- Dependencies: orders, customers, products
-- Tag: environment:production
-- Tag: team:analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false

-- Name: High-Value Orders (critical job - inherits full observability)
START JOB high_value_orders AS
SELECT
    order_id,
    customer_id,
    total_amount,
    order_timestamp
FROM orders
WHERE total_amount > 1000;

-- Name: Real-Time Fraud Detection (high-sensitivity - keeps full observability)
START JOB fraud_detection AS
SELECT
    order_id,
    customer_id,
    risk_score,
    fraud_flags
FROM orders
WHERE risk_score > 0.8;

-- Name: Product Analytics Rollup (lower-priority - reduce overhead)
-- WITH (observability.tracing.enabled = false)
START JOB product_analytics AS
SELECT
    product_id,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue
FROM orders
GROUP BY product_id;
```

### Observability Benefits

**Without App-Level Configuration:**
```sql
-- Repetitive: Each job needs identical configuration
START JOB job1 AS SELECT * FROM stream1
WITH (
    observability.metrics.enabled = true,
    observability.tracing.enabled = true
);

START JOB job2 AS SELECT * FROM stream2
WITH (
    observability.metrics.enabled = true,
    observability.tracing.enabled = true
);

START JOB job3 AS SELECT * FROM stream3
WITH (
    observability.metrics.enabled = true,
    observability.tracing.enabled = true
);
```

**With App-Level Configuration:**
```sql
-- SQL Application: Unified Observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- Clean and concise: Settings inherited from app-level
START JOB job1 AS SELECT * FROM stream1;
START JOB job2 AS SELECT * FROM stream2;
START JOB job3 AS SELECT * FROM stream3;
```

### Deployment with Observability

```bash
# Deploy application with app-level observability enabled
./velo-sql-multi deploy-app \
  --file examples/financial_trading.sql \
  --brokers localhost:9092 \
  --default-topic market_data

# All jobs will inherit observability settings from the application metadata
# Check logs to verify observability configuration:
# [INFO] @observability.metrics.enabled: true
# [INFO] @observability.tracing.enabled: true
# [INFO] @observability.profiling.enabled: false
```

### Monitoring App-Level Observability

When an application is deployed with observability enabled, you'll see:

```
[INFO] Deploying SQL application from file: financial_trading.sql
[INFO] App-level observability configuration:
[INFO]   @observability.metrics.enabled: true
[INFO]   @observability.tracing.enabled: true
[INFO]   @observability.profiling.enabled: false
[INFO] Successfully deployed job 'market_data_ts' with app-level observability
[INFO] Successfully deployed job 'trading_positions_monitor' with app-level observability
[INFO] Successfully deployed 8 jobs from SQL application with unified observability
```

## 📈 Production Best Practices

### 1. Application Organization

```sql
-- SQL Application: Production Analytics Suite
-- Version: 2.1.0
-- Description: Complete production monitoring and alerting
-- Author: Platform Engineering Team
-- Dependencies: kafka-metrics, kafka-logs, kafka-traces
-- Tag: environment:production
-- Tag: criticality:high
-- Tag: team:platform
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: true
```

### 2. Resource Management

```sql
-- Name: Critical Alert Processor
-- Property: priority=critical
-- Property: replicas=5
-- Property: memory_limit=4gb
-- Property: cpu_limit=2000m
-- Property: restart_policy=always
START JOB critical_alerts AS
SELECT * FROM system_metrics WHERE severity = 'CRITICAL';
```

### 3. Error Handling

Applications continue deploying jobs even if individual jobs fail:

```
[WARN] Failed to deploy job 'optional_analytics' from application: Job limit exceeded
[INFO] Successfully deployed job 'critical_monitoring' from application
[INFO] Successfully deployed 4 out of 5 jobs from SQL application
```

## 🎉 Summary: SQL Applications Capability

### ✅ **What You Get**

1. **Multi-Statement Deployment**: Deploy multiple related SQL jobs from a single .sql file
2. **Application Metadata**: Version, description, author, dependencies, and custom tags
3. **Dependency Tracking**: Automatic topic resolution and dependency analysis
4. **Job Properties**: Fine-grained control over job execution parameters
5. **Lifecycle Management**: Deploy, monitor, and manage applications as cohesive units
6. **Production Ready**: Built for enterprise-scale concurrent job deployments

### ✅ **Use Cases Enabled**

- **Complete Analytics Platforms**: Deploy entire analytics suites (e-commerce, IoT, finance)
- **Microservice Processing**: Each service gets a complete set of related stream processors
- **Domain-Specific Applications**: Organize jobs by business domain or function
- **Multi-Tenant Deployments**: Each tenant gets their own application deployment
- **Complex Event Processing**: Deploy interdependent jobs that work together

**🎯 Result: You can now deploy a single .sql file containing multiple related SQL statements as a complete streaming application with proper dependency management, versioning, and enterprise-grade monitoring!**

## 🏃‍♂️ Quick Start

```bash
# 1. Build the StreamJobServer
cargo build --release --bin velo-sql-multi

# 2. Deploy a complete application
./target/release/velo-sql-multi deploy-app \
  --file examples/ecommerce_analytics.sql \
  --brokers localhost:9092 \
  --default-topic orders

# 3. Watch your application run!
# - 5 jobs processing different aspects of e-commerce data
# - Real-time monitoring and metrics
# - Complete isolation between jobs
# - Enterprise-grade stream processing
```

Your SQL application is now running with multiple concurrent jobs processing different data streams! 🚀