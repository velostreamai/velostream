# Multi-Server Coordination Guide

## Overview

Velostream enables horizontal scaling of streaming SQL applications across multiple JobServer instances with **automatic Kafka consumer group coordination**. Multiple servers running the same SQL application automatically share the workload without any explicit configuration.

## How It Works

When you deploy a SQL application with the `@application` annotation across multiple JobServer instances:

1. **Automatic Consumer Group Naming**
   - Each job gets a consumer group: `velo-{app_name}-{job_name}`
   - All servers share this group name

2. **Kafka Partition Distribution**
   - Kafka automatically distributes partitions across all servers in the group
   - When servers join/leave, partitions are automatically rebalanced

3. **Transparent Coordination**
   - No manual consumer group configuration needed
   - No service discovery required
   - Kafka's built-in rebalancing handles everything

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                   SQL Application File                      │
│                                                             │
│  @application trading_platform                             │
│  @phase production                                          │
│                                                             │
│  CREATE STREAM orders AS SELECT * FROM kafka_orders;       │
│  CREATE STREAM payments AS SELECT * FROM kafka_payments;   │
└──────────────────┬──────────────────────────────────────────┘
                   │
        ┌──────────┼──────────┐
        │          │          │
        ▼          ▼          ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ Server1 │ │ Server2 │ │ Server3 │
   │ :8081   │ │ :8082   │ │ :8083   │
   └────┬────┘ └────┬────┘ └────┬────┘
        │           │           │
        └───────────┼───────────┘
                    │
             ┌──────▼──────┐
             │   Kafka     │
             │             │
             │ Consumer    │
             │ Groups:     │
             │ - velo-     │
             │   trading_  │
             │   platform- │
             │   orders    │
             │ - velo-     │
             │   trading_  │
             │   platform- │
             │   payments  │
             └─────────────┘
```

## Quick Start: 3-Server Deployment

### Step 1: Create Your SQL Application

```sql
-- trading.sql
@application trading_platform
@phase production

CREATE STREAM orders AS
SELECT
    order_id,
    customer_id,
    amount,
    order_time
FROM kafka_orders
WHERE amount > 100
EMIT CHANGES;

CREATE STREAM payments AS
SELECT
    payment_id,
    order_id,
    status,
    payment_time
FROM kafka_payments
EMIT CHANGES;

CREATE SINK processed_orders WITH (
    brokers: localhost:9092,
    topic: processed_orders
) SELECT * FROM orders;
```

### Step 2: Deploy on Server 1

```bash
# On Server 1 (hostname: server1.example.com)
$ velostream-server \
    --app trading.sql \
    --port 8081 \
    --kafka-brokers kafka1:9092,kafka2:9092,kafka3:9092
```

**Consumer groups created:**
- `velo-trading_platform-orders` (partition distribution: 4 partitions × 1/3 = ~1.33 per server)
- `velo-trading_platform-payments` (partition distribution: 4 partitions × 1/3 = ~1.33 per server)

### Step 3: Deploy on Server 2

```bash
# On Server 2 (hostname: server2.example.com)
$ velostream-server \
    --app trading.sql \
    --port 8081 \
    --kafka-brokers kafka1:9092,kafka2:9092,kafka3:9092
```

**Kafka rebalancing happens automatically:**
- Each partition is now assigned to one of the 2 servers
- Updates consumer group metadata: `velo-trading_platform-orders` now has 2 members
- Partition distribution: 4 partitions ÷ 2 servers = 2 partitions per server

### Step 4: Deploy on Server 3

```bash
# On Server 3 (hostname: server3.example.com)
$ velostream-server \
    --app trading.sql \
    --port 8081 \
    --kafka-brokers kafka1:9092,kafka2:9092,kafka3:9092
```

**Final state:**
- 3 servers share partitions equally
- Each server processes ~1/3 of data (automatic balancing)
- Transparent to application code

## Consumer Group Naming

### Pattern

```
velo-{app_name}-{job_name}
```

### Examples

| Application | Job Name | Consumer Group |
|-------------|----------|----------------|
| `trading_platform` | `orders` | `velo-trading_platform-orders` |
| `trading_platform` | `payments` | `velo-trading_platform-payments` |
| `ecommerce_api` | `user_events` | `velo-ecommerce_api-user_events` |
| `fraud_detection` | `transaction_monitor` | `velo-fraud_detection-transaction_monitor` |

### Job Name Extraction

Job names come from (in priority order):

1. **Explicit `@name` annotation:**
   ```sql
   -- @name: custom_job_name
   CREATE STREAM data_processor AS SELECT * FROM source;
   ```

2. **Statement name:**
   ```sql
   CREATE STREAM my_custom_name AS SELECT * FROM source;
   -- Job name: my_custom_name
   ```

3. **Auto-generated name:**
   - Pattern: `{filename}_{snippet}_{timestamp}_{order}`
   - Example: `trading_stream_sel_1234567890_01`

## Configuration Examples

### Scenario 1: Financial Trading (3 Servers)

```sql
@application financial_trading
@phase production

CREATE STREAM market_data AS
SELECT symbol, price, volume, trade_time
FROM kafka_market_data
WITH (
    brokers: kafka1:9092,kafka2:9092,kafka3:9092,
    topic: market_data
)
EMIT CHANGES;

CREATE STREAM trade_aggregates AS
SELECT
    symbol,
    SUM(volume) as total_volume,
    AVG(price) as avg_price,
    MAX(price) as max_price
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL 1 MINUTE)
EMIT CHANGES;

CREATE SINK trading_results WITH (
    brokers: kafka1:9092,kafka2:9092,kafka3:9092,
    topic: trade_results
) SELECT * FROM trade_aggregates;
```

**Deployment:**
```bash
# All three servers run the same command
for server in server1 server2 server3; do
    ssh $server "cd /app && velostream-server --app financial_trading.sql"
done
```

**Result:**
- Consumer group: `velo-financial_trading-market_data` (3 members)
- Consumer group: `velo-financial_trading-trade_aggregates` (3 members)
- Transparent partition balancing
- Each server processes ~1/3 of market data

### Scenario 2: E-Commerce Platform (2 Servers)

```sql
@application ecommerce_api
@phase production

CREATE STREAM user_activity AS
SELECT
    user_id,
    event_type,
    event_time,
    metadata
FROM kafka_events
WITH (topic: user_events)
EMIT CHANGES;

CREATE STREAM purchase_stream AS
SELECT
    user_id,
    order_id,
    amount,
    purchase_time
FROM kafka_purchases
WITH (topic: user_purchases)
EMIT CHANGES;

-- Join for enrichment
CREATE STREAM enriched_purchases AS
SELECT
    p.order_id,
    p.user_id,
    p.amount,
    ua.event_type,
    ua.event_time
FROM purchase_stream p
JOIN user_activity ua ON p.user_id = ua.user_id
EMIT CHANGES;
```

**Deployment:**
```bash
# Server 1
velostream-server --app ecommerce_api.sql --port 8081

# Server 2
velostream-server --app ecommerce_api.sql --port 8082
```

**Result:**
- Each consumer group has 2 members
- Partitions evenly distributed between servers
- One server can handle ~500K events/sec
- Two servers handle ~1M events/sec total

## Scaling Patterns

### Horizontal Scaling (Add Servers)

**Initial state: 1 server, 4 partitions**
```
Server1: partition[0,1,2,3]
Throughput: 300K rec/sec
```

**Add Server 2**
```
Kafka rebalances automatically...
Server1: partition[0,2]
Server2: partition[1,3]
Throughput: 600K rec/sec (2x)
```

**Add Server 3**
```
Kafka rebalances automatically...
Server1: partition[0,3]
Server2: partition[1]
Server3: partition[2]
Throughput: 900K rec/sec (3x)
```

### Vertical Scaling (Larger Partitions)

If you want more parallelism on fewer servers:

1. Increase Kafka topic partitions: `kafka-topics --alter --topic events --partitions 16`
2. Redeploy application (automatic partition redistribution)

```
4 partitions × 1 server = 300K rec/sec
12 partitions × 3 servers = 1.2M rec/sec (4x improvement)
16 partitions × 4 servers = 1.6M rec/sec (4x improvement)
```

## Monitoring Consumer Groups

### View Consumer Group Status

```bash
# List all consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list | grep velo

# Expected output:
# velo-financial_trading-market_data
# velo-financial_trading-trade_aggregates
# velo-ecommerce_api-user_activity
```

### Inspect Group Members and Lag

```bash
# Show consumer group details
kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group velo-financial_trading-market_data \
    --describe

# Output:
# GROUP                                    TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG     CONSUMER-ID
# velo-financial_trading-market_data       market_data     0          10000           10500           500     server1-consumer-1
# velo-financial_trading-market_data       market_data     1          9800            10300           500     server2-consumer-1
# velo-financial_trading-market_data       market_data     2          9950            10450           500     server3-consumer-1
```

### Monitor Partition Distribution

```bash
# Check if partitions are balanced
kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group velo-financial_trading-market_data \
    --describe | awk '{print $2, $3, $4}' | sort | uniq -c
```

## Troubleshooting

### Problem: Consumer Group Not Created

**Symptom:** Kafka consumer group doesn't appear after deployment

**Causes:**
1. SQL has no `@application` annotation
2. Job creation failed (check server logs)
3. Kafka brokers are unreachable

**Solution:**
```sql
-- Make sure to add @application annotation:
@application my_app

CREATE STREAM events AS SELECT * FROM kafka_events;
```

### Problem: Unbalanced Partition Distribution

**Symptom:** One server gets 3/4 of partitions, others get 1/4

**Causes:**
1. Server hasn't finished initializing
2. Rebalancing is in progress
3. Sticky assignor configuration

**Solution:**
```bash
# Wait 30 seconds for rebalancing to complete
sleep 30

# Force rebalance by restarting consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group velo-financial_trading-market_data \
    --reset-offsets --to-earliest --execute
```

### Problem: High Consumer Lag

**Symptom:** LAG column shows large numbers (e.g., 100,000)

**Causes:**
1. Processing too slow
2. Batch size too large
3. Server CPU bottleneck

**Solution:**
```sql
-- Reduce batch size for lower latency
-- @job_mode: simple
-- @batch_size: 100  (was 1000)

CREATE STREAM fast_processor AS
SELECT * FROM kafka_events
EMIT CHANGES;
```

## Best Practices

### 1. Always Use @application Annotation

```sql
-- ✅ GOOD: Enables automatic coordination
@application my_app

CREATE STREAM events AS SELECT * FROM kafka;

-- ❌ BAD: No coordination, each server creates own consumer group
CREATE STREAM events AS SELECT * FROM kafka;
```

### 2. Use Same Application Name Across All Servers

```bash
# Server 1
velostream-server --app trading.sql

# Server 2 (MUST use same file or equivalent app name)
velostream-server --app trading.sql

# Server 3
velostream-server --app trading.sql
```

### 3. Monitor Consumer Lag

```bash
# Regular monitoring
watch -n 5 'kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group velo-trading_platform-orders \
    --describe | awk "{print \$1, \$3, \$7}"'
```

### 4. Plan Partition Count Based on Servers

```
Target throughput: 1M rec/sec
Per-server throughput: 300K rec/sec
Servers needed: 1M ÷ 300K = 3.33 → 4 servers
Partitions needed: 4 (min 1 per server)
→ Create topic with 8-12 partitions for headroom
```

### 5. Test Failover

```bash
# Deployment with 3 servers
Server1 | Server2 | Server3
  P0,P2   P1,P3     P4,P5

# Simulate Server1 failure (stop process)
# Wait 30 seconds...
# Kafka rebalances:
Server2 | Server3
 P0,P2,P4  P1,P3,P5

# Verify no data loss:
kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group velo-app-stream \
    --describe | grep -c "0$"  # Should be 0 (no lag)
```

## Related Documentation

- [Job Annotations Guide](job-annotations-guide.md) - Learn about @application annotation
- [Production Deployment Guide](../user-guides/production-deployment-guide.md) - Production setup
- [Job Processor Configuration Guide](job-processor-configuration-guide.md) - Job configuration
- [Kafka Configuration Guide](../developer/kafka-schema-configuration.md) - Kafka setup
