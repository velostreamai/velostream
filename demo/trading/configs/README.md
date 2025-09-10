# Trading Analytics Configuration Files (Refactored)

This directory contains a modular configuration system for the real-time trading analytics system, following the FR-047 pluggable datasources specification with DRY principles.

## Configuration Architecture

### Base Configurations (Common)

#### `common_kafka_source.yaml`
- **Purpose**: Base configuration for all Kafka data sources
- **Contains**: Standard consumer settings, schema configuration, performance profiles
- **URI Construction**: `kafka://{bootstrap_servers}/{topic_name}` (automatic)
- **Profiles**: `ultra_low_latency`, `high_throughput`, `balanced`

#### `common_kafka_sink.yaml`  
- **Purpose**: Base configuration for all Kafka data sinks
- **Contains**: Standard producer settings, topic properties, delivery profiles
- **URI Construction**: `kafka://{bootstrap_servers}/{topic_name}` (automatic)
- **Profiles**: `critical`, `high_priority`, `medium_priority`, `balanced`

### Source Topic Configurations

#### `market_data_topic.yaml`
- **Extends**: `common_kafka_source.yaml`
- **Profile**: `ultra_low_latency` (10ms fetch wait)
- **Partitions**: 12 (high parallelism)
- **Use Case**: High-frequency market data feed

#### `trading_positions_topic.yaml`
- **Extends**: `common_kafka_source.yaml`
- **Profile**: `balanced`
- **Partitions**: 8 (moderate parallelism)
- **Use Case**: Real-time trading positions

#### `order_book_topic.yaml`
- **Extends**: `common_kafka_source.yaml`
- **Profile**: `high_throughput` (2000 records/poll)
- **Partitions**: 16 (highest parallelism)
- **Special**: Compacted topics for state management

### Sink Topic Configurations

#### `price_alerts_topic.yaml`
- **Extends**: `common_kafka_sink.yaml`
- **Profile**: `high_priority`
- **Retention**: 7 days
- **Use Case**: Price movement alerts (>5% changes)

#### `volume_spikes_topic.yaml`
- **Extends**: `common_kafka_sink.yaml`
- **Profile**: `medium_priority`
- **Retention**: 3 days
- **Use Case**: Volume anomaly detection

#### `risk_alerts_topic.yaml`
- **Extends**: `common_kafka_sink.yaml`
- **Profile**: `critical` (exactly-once delivery)
- **Retention**: 30 days (compliance)
- **Integrations**: Email, Slack alerts

#### `order_imbalance_topic.yaml`
- **Extends**: `common_kafka_sink.yaml`
- **Profile**: `medium_priority`
- **Retention**: 2 days
- **Use Case**: Institutional flow detection

#### `arbitrage_opportunities_topic.yaml`
- **Extends**: `common_kafka_sink.yaml`
- **Profile**: `high_priority` with ultra-fast overrides (1ms linger)
- **Retention**: 1 day (short-lived opportunities)
- **Integrations**: Trading bot webhooks

### Multi-Source Configuration

#### `multi_source_risk_monitoring.yaml`
- **Extends**: `common_kafka_source.yaml`
- **Type**: Multi-stream JOIN configuration
- **Sources**: `trading_positions_topic.yaml` + `market_data_topic.yaml`
- **JOIN**: Inner join on symbol with 1-minute window

## Configuration Benefits

### DRY Principles
- **No Duplication**: Common settings defined once, URIs constructed automatically
- **Easy Maintenance**: Change base config affects all derived configs
- **Consistency**: All topics inherit standard settings

### Modularity
- **Topic-Specific**: Each topic only defines name + unique settings
- **Profile-Based**: Reusable performance/delivery profiles
- **Extensible**: Easy to add new topics or profiles
- **URI-Free**: Topic configs only specify names, URIs built automatically

### Performance Optimization
- **Profile Selection**: Choose optimal settings per use case
- **Override Capability**: Fine-tune specific settings when needed
- **Resource Efficiency**: Appropriate partitioning and retention

## Usage in SQL

```sql
INSERT INTO price_alerts
SELECT ...
FROM market_data_stream
WHERE ...
WITH (
    source_config = 'configs/market_data_topic.yaml',
    sink_config = 'configs/price_alerts_topic.yaml'
);
```

## File Structure

```
configs/
├── README.md
├── common_kafka_source.yaml          # Base source config
├── common_kafka_sink.yaml            # Base sink config
├── market_data_topic.yaml            # Market data source
├── trading_positions_topic.yaml      # Positions source
├── order_book_topic.yaml             # Order book source
├── multi_source_risk_monitoring.yaml # Multi-stream JOIN
├── price_alerts_topic.yaml           # Price alerts sink
├── volume_spikes_topic.yaml          # Volume alerts sink
├── risk_alerts_topic.yaml            # Risk alerts sink
├── order_imbalance_topic.yaml        # Order flow alerts sink
└── arbitrage_opportunities_topic.yaml # Arbitrage alerts sink
```

## Performance Profiles Summary

| Profile | Latency | Throughput | Use Case |
|---------|---------|------------|-----------|
| `ultra_low_latency` | 10ms | 1000 rec/poll | Market data |
| `high_throughput` | 50ms | 2000 rec/poll | Order book |
| `balanced` | 20ms | 1000 rec/poll | Positions |
| `critical` | 0ms linger | Exactly-once | Risk alerts |
| `high_priority` | 1ms linger | At-least-once | Price/arbitrage |
| `medium_priority` | 5ms linger | At-least-once | Volume/order flow |

This modular approach reduces configuration files from 8 monolithic configs to 2 base configs + 8 specialized topic configs, eliminating duplication while maintaining full customization capability.