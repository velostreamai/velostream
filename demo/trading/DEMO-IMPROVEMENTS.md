# Trading Demo Resilience & Debuggability Improvements

## Overview
This document describes the improvements made to make the trading demo more resilient and easier to debug.

## Key Improvements

### 1. Consumer Offset Strategy
**Problem**: Jobs showed "0 records processed" because `auto.offset.reset: latest` meant consumers only saw messages arriving AFTER subscription.

**Solution**: Changed to `auto.offset.reset: earliest` for demos to process all existing data from topic start.

**Location**: `configs/common_kafka_source.yaml`

```yaml
consumer_config:
  auto.offset.reset: "earliest"  # Process all data from start
```

**For Production**: Change back to `latest` to only process new messages.

### 2. Startup Orchestration Script
**Created**: `start-demo.sh`

**Features**:
- ✅ Validates Docker is running
- ✅ Starts Kafka and waits for readiness
- ✅ Ensures all required topics exist (creates if missing)
- ✅ Resets consumer groups for clean demo start
- ✅ Starts data generator with configurable duration
- ✅ Verifies data is flowing before deployment
- ✅ Deploys SQL application
- ✅ Provides clear status summary and monitoring commands

**Usage**:
```bash
./start-demo.sh          # 10-minute simulation (default)
./start-demo.sh 30       # 30-minute simulation
```

### 3. Health Check Script
**Created**: `check-demo-health.sh`

**Features**:
- ✅ Checks all Docker containers
- ✅ Validates Kafka broker connectivity
- ✅ Verifies all required topics exist with message counts
- ✅ Confirms data generator is running
- ✅ Checks SQL application status and job count
- ✅ Shows consumer group lag
- ✅ Returns exit code 0 (healthy) or 1 (unhealthy)

**Usage**:
```bash
./check-demo-health.sh
```

### 4. Enhanced Logging
**Location**: `/tmp/` directory

**Log Files**:
- `/tmp/velo_deployment.log` - SQL job deployment and execution logs
- `/tmp/trading_generator.log` - Data generator logs

**Monitoring**:
```bash
tail -f /tmp/velo_deployment.log   # Watch SQL jobs
tail -f /tmp/trading_generator.log  # Watch data generation
```

### 5. Clean Shutdown
**Existing**: `stop-demo.sh`

**Features**:
- ✅ Stops all Velostream processes
- ✅ Stops data generator
- ✅ Stops Docker containers
- ✅ Force kills if needed

## Common Issues & Solutions

### Issue: "0 records processed"

**Causes**:
1. Consumer offset strategy set to `latest` (fixed)
2. Data generator not running
3. Topics don't exist
4. Consumer groups positioned at end of topic

**Debug**:
```bash
# Check if data is in topics
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market_data_stream \
  --from-beginning \
  --max-messages 10

# Check consumer group lag
docker exec simple-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group velo-sql-financia_createstreammarket_data_tsasse_67121_00 \
  --describe
```

**Solution**: Use `start-demo.sh` which resets consumer groups for clean start.

### Issue: Topics don't exist

**Cause**: Topics not created before deployment.

**Solution**: `start-demo.sh` automatically creates all required topics:
- market_data_stream (12 partitions)
- market_data_ts (12 partitions)
- market_data_stream_a (12 partitions)
- market_data_stream_b (12 partitions)
- trading_positions_stream (8 partitions)
- order_book_stream (12 partitions)

### Issue: Kafka not ready

**Cause**: Deployment started before Kafka finished initializing.

**Solution**: `start-demo.sh` waits for Kafka broker to be responsive before proceeding.

### Issue: Old consumer group offsets

**Cause**: Previous demo runs left consumer groups with offsets at end of topics.

**Solution**: `start-demo.sh` deletes all existing `velo-sql-*` consumer groups before starting.

## Testing the Improvements

### Quick Test
```bash
# 1. Clean stop
./stop-demo.sh

# 2. Start with new script
./start-demo.sh 5

# 3. Check health
./check-demo-health.sh

# 4. Monitor logs
tail -f /tmp/velo_deployment.log
```

### Expected Output
After ~30 seconds, you should see:
```
Application 'Real-Time Trading Analytics' - Active jobs: 8
  Job 'financia_createstreammarket_data_tsasse_67121_00': Running - 1234 records processed
  Job 'financia_createstreamtick_bucketsassele_67121_01': Running - 567 records processed
  ...
```

## Architecture Improvements

### Resilience Features

1. **Idempotent Startup**: Can run `start-demo.sh` multiple times safely
2. **Automatic Recovery**: Topics created if missing
3. **Clean State**: Consumer groups reset each run
4. **Validation Gates**: Each step validated before proceeding
5. **Timeout Protection**: Operations have timeouts to prevent hanging

### Debuggability Features

1. **Structured Logs**: All output in `/tmp/*.log`
2. **Health Checks**: Single command to verify all components
3. **Consumer Lag Visibility**: Check processing backlog
4. **Message Count Tracking**: Verify data flow through pipeline
5. **Clear Status Messages**: Color-coded output for quick scanning

## Future Enhancements

### Potential Additions
- [ ] Prometheus metrics export for monitoring
- [ ] Grafana dashboards for visualization
- [ ] Alert system for unhealthy states
- [ ] Automatic restart on failure
- [ ] Performance benchmarking mode
- [ ] Data quality checks
- [ ] Schema registry integration testing

## Migration from Old Demo

### Before
```bash
docker-compose up -d
cargo build
./target/debug/trading_data_generator localhost:9092 10 &
../../target/debug/velo-sql-multi deploy-app --file sql/financial_trading.sql
# Wait and hope it works...
```

### After
```bash
./start-demo.sh 10
# Everything validated, logs available, health checkable
```

## Configuration Reference

### Kafka Consumer Settings
```yaml
# For demos (process all data)
auto.offset.reset: "earliest"

# For production (only new data)
auto.offset.reset: "latest"
```

### Topic Configuration
All topics use these defaults:
- **Replication Factor**: 1 (single-node demo)
- **Partitions**: 12 for high-throughput, 8 for moderate
- **Retention**: Default (7 days)

### Performance Tuning
For faster processing, increase in `common_kafka_source.yaml`:
```yaml
consumer_config:
  max.poll.records: 500        # From 100
  fetch.min.bytes: 10240       # From 1024
```

## Support

**Logs**: Check `/tmp/velo_deployment.log` and `/tmp/trading_generator.log`

**Health**: Run `./check-demo-health.sh`

**Cleanup**: Run `./stop-demo.sh`

**Full Reset**:
```bash
./stop-demo.sh
docker-compose -f kafka-compose.yml down -v  # Remove volumes
docker-compose -f kafka-compose.yml up -d
./start-demo.sh
```
