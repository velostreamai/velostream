# SQL-Native Metrics: Debugging Best Practices

**Target Audience**: Operators, SREs, Developers
**Last Updated**: 2025-10-10 (FR-073 Phase 2A)
**Difficulty**: Intermediate

---

## Overview

This guide provides systematic approaches for debugging SQL-native metrics in Velostream, based on real-world troubleshooting experiences from FR-073 implementation.

---

## Quick Reference: Common Issues

| Symptom | Likely Cause | Quick Check | Fix |
|---------|--------------|-------------|-----|
| Dashboard panel empty | Metric not being emitted | `curl localhost:9091/metrics \| grep metric_name` | Trace to source stream |
| Metric registered but not emitting | 0 records processed | Check logs for `0 records processed` | Fix data flow |
| Job "Running" but no data | Topic empty or schema issue | Check Kafka topic has messages | Verify schema files |
| Consumer offset warnings | Deserialization failure | Check schema configuration | Fix schema path |
| All metrics missing | Prometheus scrape failing | `curl localhost:9091/metrics` | Check metrics server |

---

## Systematic Debugging Process

### Step 1: Verify Metric Endpoint

**Goal**: Confirm Prometheus metrics endpoint is reachable and serving metrics.

```bash
# Check metrics endpoint is running
curl -f http://localhost:9091/metrics

# If this fails:
# - Check velo-sql process is running: ps aux | grep velo-sql
# - Check port binding: lsof -i :9091
# - Check firewall rules
```

**Expected Output**: HTTP 200 with Prometheus-format metrics

**Next Steps**:
- ✅ If successful → Proceed to Step 2
- ❌ If fails → Restart velostream service

---

### Step 2: Check Metric Exists

**Goal**: Determine if specific metric is being emitted.

```bash
# Search for your metric
curl -s http://localhost:9091/metrics | grep "^velo_trading_price_change_percent"

# Check all SQL-native metrics
curl -s http://localhost:9091/metrics | grep "^velo_trading_"

# Count unique metrics
curl -s http://localhost:9091/metrics | grep "^velo_" | cut -d' ' -f1 | sort -u | wc -l
```

**Interpretation**:
- **Metric found with data**: Problem is with Grafana or query syntax
- **Metric found but no values**: Metric registered but not emitting → Go to Step 3
- **Metric not found at all**: Registration issue → Go to Step 4

---

### Step 3: Trace Metric to Source Stream

**Goal**: Identify which SQL stream produces the metric.

```bash
# Find SQL file with metric annotation
grep -r "@metric: velo_trading_price_change_percent" demo/trading/sql/

# Example output:
# demo/trading/sql/financial_trading.sql:62:-- @metric: velo_trading_price_change_percent
```

**Extract Stream Information**:
```bash
# Find CREATE STREAM statement after metric annotation
grep -A 20 "@metric: velo_trading_price_change_percent" demo/trading/sql/financial_trading.sql | grep "CREATE STREAM"

# Example output:
# CREATE STREAM advanced_price_movement_alerts AS
```

**Key Info to Note**:
- Stream name: `advanced_price_movement_alerts`
- Source topic(s): Check FROM clause
- Metric type: counter, gauge, histogram
- Metric field: Which field is being measured

---

### Step 4: Check Stream Job Status

**Goal**: Verify the job processing this stream is running.

```bash
# List all jobs
curl -s http://localhost:8080/jobs | jq -r '.[] | "\(.name): \(.status)"'

# Find job for specific stream (use stream name pattern matching)
curl -s http://localhost:8080/jobs | jq -r '.[] | select(.name | contains("price_movement")) | {name: .name, status: .status}'
```

**Job Status Meanings**:
- **Running**: Job is active (but may not be processing data!)
- **Failed**: Job encountered error during startup
- **Stopped**: Job was manually stopped
- **Pending**: Job waiting to start

**Next Steps**:
- If status is "Failed" or "Stopped" → Check job logs for errors
- If status is "Running" → Go to Step 5 (verify data flow)

---

### Step 5: Verify Data Flow (Records Processed)

**Goal**: Confirm job is actually processing records, not just running idle.

```bash
# Check records processed for specific job
grep "job_name_pattern" /tmp/velo_deployment.log | grep "records processed" | tail -5

# Example for advanced_price_movement job:
grep "financia_phase3advancedwindowfunctionsp" /tmp/velo_deployment.log | grep "records processed" | tail -5

# Expected output (GOOD):
# Job 'job_id' (market_data_ts): Running - 1523 records processed

# Warning sign (BAD):
# Job 'job_id' (market_data_ts): Running - 0 records processed
```

**If 0 Records Processed**:
This means job is running but not receiving data. Common causes:
1. Source Kafka topic is empty
2. Schema deserialization failing
3. Consumer group offset issue
4. Source stream not producing data

→ **Go to Step 6**

---

### Step 6: Check Source Topic Data Flow

**Goal**: Verify the Kafka topic feeding this stream has data.

```bash
# Identify source topic from SQL (look for FROM clause)
grep -A 5 "CREATE STREAM stream_name" demo/trading/sql/financial_trading.sql

# Check if topic exists
docker exec simple-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep topic_name

# Check topic has messages
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic topic_name \
  --max-messages 3 \
  --timeout-ms 3000

# Check topic message count (approximate)
docker exec simple-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic topic_name \
  --time -1 | awk -F ":" '{sum += $3} END {print sum}'
```

**Interpretation**:
- **Topic doesn't exist**: Producer stream hasn't created it yet
- **Topic empty (0 messages)**: Producer not writing or cleared
- **Topic has messages**: Problem is deserialization → Go to Step 7

---

### Step 7: Verify Schema Configuration

**Goal**: Ensure Avro/Protobuf schemas are correctly configured.

```bash
# Find config file for source topic
grep -r "topic_name" demo/trading/configs/*.yaml

# Example: configs/market_data_ts_source.yaml
# Check schema configuration
cat demo/trading/configs/market_data_ts_source.yaml | grep -A 5 "schema:"

# Expected output:
# schema:
#   value.serializer: "avro"
#   value.schema.file: "schemas/market_data_ts.avsc"

# VERIFY SCHEMA FILE EXISTS
ls demo/trading/schemas/market_data_ts.avsc

# If file doesn't exist: THIS IS THE PROBLEM
```

**Common Schema Issues**:

1. **Missing Schema File**
   ```
   Error: ls: schemas/market_data_ts.avsc: No such file or directory
   Fix: Create the schema file or update config to reference existing schema
   ```

2. **Wrong Schema Path**
   ```yaml
   # Config says: schemas/market_data.avsc
   # But file is:  schemas/market_data_ts.avsc
   Fix: Update config path to match actual file
   ```

3. **Invalid Schema Syntax**
   ```
   Check logs: grep "schema" /tmp/velo_deployment.log | grep -i error
   ```

---

### Step 8: Check Metric Registration Logs

**Goal**: Verify metric was registered correctly during job startup.

```bash
# Search for metric registration
grep "Registered dynamic.*metric" /tmp/velo_deployment.log | grep "metric_name"

# Example:
grep "Registered dynamic histogram metric: name=velo_trading_price_change_percent" /tmp/velo_deployment.log

# Expected output:
# [INFO] 📊 Registered dynamic histogram metric: name=velo_trading_price_change_percent, labels=["symbol"]
```

**If Not Found**:
- Metric annotation may not have been parsed correctly
- Check SQL syntax: `grep "@metric: metric_name" sql_file`
- Verify annotation format matches spec (see FR-073 documentation)

**If Found**:
- Registration succeeded, problem is in metric emission
- Combine with Step 5 results to diagnose data flow

---

## Advanced Troubleshooting

### Issue: Histogram Buckets Not Appearing

**Symptom**: `histogram_quantile()` query returns no data in Grafana

**Debug Steps**:

1. Check histogram bucket metric exists:
   ```bash
   curl -s http://localhost:9091/metrics | grep "metric_name_bucket"
   ```

2. Verify buckets are defined in annotation:
   ```sql
   -- @metric_buckets: 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0
   ```

3. Confirm field is being measured:
   ```sql
   -- @metric_field: price_change_pct
   ```

4. Check if field exists in stream output:
   ```bash
   # Consume from sink topic to see what fields are present
   docker exec simple-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sink_topic_name \
     --max-messages 1
   ```

5. Check logs for histogram observations:
   ```bash
   grep "Observing histogram" /tmp/velo_deployment.log | grep "metric_name"
   ```

---

### Issue: Counter Not Incrementing

**Symptom**: Counter shows same value over time, doesn't increase

**Debug Steps**:

1. Verify it's actually a counter (not gauge):
   ```sql
   -- @metric_type: counter  # Should say "counter"
   ```

2. Check counter increments in logs:
   ```bash
   grep "Incrementing counter" /tmp/velo_deployment.log | grep "metric_name"
   ```

3. Verify records are flowing through stream:
   ```bash
   grep "job_id" /tmp/velo_deployment.log | grep "serialization to.*sink"
   # Should show increasing record counts
   ```

4. Check if metric condition is met:
   ```sql
   -- @metric_condition: movement_severity IN ('SIGNIFICANT', 'MODERATE')
   -- Counter only increments when condition is true
   ```

---

### Issue: Gauge Shows Stale Data

**Symptom**: Gauge metric doesn't update with latest values

**Debug Steps**:

1. Check gauge emission frequency:
   ```bash
   curl -s http://localhost:9091/metrics | grep "metric_name" | tail -10
   # Take timestamp, wait 10s, check again
   ```

2. Verify stream EMIT mode:
   ```sql
   -- EMIT CHANGES means continuous updates
   -- EMIT FINAL means only on window close
   ```

3. Check window configuration:
   ```sql
   WINDOW TUMBLING(event_time, INTERVAL '1' SECOND)
   -- Gauge updates every 1 second
   ```

4. Verify event_time field is being populated:
   ```bash
   # Check if event_time is null or missing
   grep "event_time" /tmp/velo_deployment.log | grep -i "null\|missing"
   ```

---

## Debugging Checklist

Use this checklist for systematic troubleshooting:

### Pre-Deployment Validation

- [ ] All `@metric` annotations have valid syntax
- [ ] All referenced schema files exist
- [ ] All source Kafka topics are producing data
- [ ] Test SQL query works in isolation
- [ ] Unit tests pass for metric parsing

### Post-Deployment Verification

- [ ] Metrics endpoint responds (HTTP 200)
- [ ] All expected metrics appear at `/metrics`
- [ ] All jobs show "Running" status
- [ ] Jobs show >0 records processed
- [ ] Grafana can query Prometheus successfully
- [ ] Dashboard panels show data

### When Issues Occur

- [ ] Check metrics endpoint availability
- [ ] Verify metric exists at endpoint
- [ ] Trace metric to source SQL stream
- [ ] Check job status via API
- [ ] Verify records processed > 0
- [ ] Check source topic has data
- [ ] Verify schema files exist and are valid
- [ ] Review metric registration logs
- [ ] Check for consumer offset warnings
- [ ] Verify Grafana query syntax

---

## Debugging Tools Reference

### Essential Commands

```bash
# Metrics endpoint
METRICS_URL="http://localhost:9091/metrics"
curl -s $METRICS_URL | grep "^velo_trading_"

# Jobs API
JOBS_URL="http://localhost:8080/jobs"
curl -s $JOBS_URL | jq '.[] | {name, status}'

# Logs
LOG_FILE="/tmp/velo_deployment.log"
grep "metric_name" $LOG_FILE | tail -50

# Kafka topics
KAFKA_CONTAINER="simple-kafka"
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:9092 --list
```

### Useful Aliases

Add to your `~/.bashrc` or `~/.zshrc`:

```bash
# Velostream debugging aliases
alias velo-metrics='curl -s http://localhost:9091/metrics | grep "^velo_"'
alias velo-jobs='curl -s http://localhost:8080/jobs | jq -r ".[] | \"\(.name): \(.status)\""'
alias velo-logs='tail -f /tmp/velo_deployment.log'
alias velo-kafka-topics='docker exec simple-kafka kafka-topics --bootstrap-server localhost:9092 --list'

# Metric-specific
velo-check-metric() {
  curl -s http://localhost:9091/metrics | grep "^$1"
}

velo-find-stream() {
  grep -r "@metric: $1" demo/trading/sql/
}

velo-job-records() {
  grep "$1" /tmp/velo_deployment.log | grep "records processed" | tail -10
}
```

Usage:
```bash
velo-check-metric velo_trading_price_change_percent
velo-find-stream velo_trading_price_change_percent
velo-job-records financia_phase3
```

---

## Common Pitfalls

### ❌ DON'T: Assume "Running" Means Healthy

**Bad**:
```bash
$ curl http://localhost:8080/jobs | jq '.[] | .status'
"Running"  # Looks good!
```

**Good**:
```bash
$ grep "job_id" /tmp/velo_deployment.log | grep "records processed"
Running - 0 records processed  # Actually broken!
```

**Lesson**: Always verify records are being processed, not just that job is running.

---

### ❌ DON'T: Trust Registration Logs Alone

**Bad**:
```log
[INFO] 📊 Registered dynamic counter metric: name=velo_trading_market_data_total
# Assume metric is working
```

**Good**:
```bash
$ curl -s http://localhost:9091/metrics | grep velo_trading_market_data_total
# Verify metric actually appears at endpoint
```

**Lesson**: Registration != Emission. Always verify metric exists at `/metrics` endpoint.

---

### ❌ DON'T: Skip Schema Validation

**Bad**:
```yaml
# configs/source.yaml
schema:
  value.schema.file: "schemas/my_schema.avsc"
# Deploy without checking if file exists
```

**Good**:
```bash
$ ls demo/trading/schemas/my_schema.avsc
# Verify file exists before deploying
```

**Lesson**: Missing schema files cause silent failures (0 records processed).

---

### ❌ DON'T: Ignore "0 records" Logs

**Bad**:
```log
[INFO] Job 'my_job': Running - 0 records processed
[INFO] Job 'my_job': Running - 0 records processed
[INFO] Job 'my_job': Running - 0 records processed
# Ignore these repeated messages
```

**Good**:
```log
[INFO] Job 'my_job': Running - 0 records processed
# Investigate immediately: Why no data flow?
```

**Lesson**: Repeated "0 records" indicates data starvation. Investigate source topic/schema.

---

## Escalation Path

### Level 1: Self-Service (This Guide)
- Use systematic debugging process
- Check metrics endpoint, jobs, logs
- Verify schema files and topic data

### Level 2: Team Collaboration
- Share debugging findings with team
- Check recent deployments for changes
- Review related stream dependencies

### Level 3: Engineering Support
- Provide full debugging output
- Include logs, metric queries, SQL definitions
- Document reproduction steps

**Required Information for Escalation**:
1. Metric name and type
2. Source SQL stream name
3. Job ID and status
4. Records processed count
5. Relevant log excerpts
6. Schema configuration
7. Steps already attempted

---

## Best Practices Summary

### ✅ DO

- **Validate schemas before deployment**: Check all referenced files exist
- **Monitor records processed**: Alert on 0 records for >60s
- **Verify metrics post-deployment**: Check `/metrics` endpoint shows expected metrics
- **Use structured logging**: Grep-friendly log messages for debugging
- **Document metric sources**: Keep mapping of metric → stream → topic
- **Test in isolation**: Verify each stream works independently before chaining

### ❌ DON'T

- **Don't trust "Running" status alone**: Verify data flow
- **Don't skip pre-flight checks**: Validate configuration before deploying
- **Don't ignore warnings**: "NoOffset" warnings indicate issues
- **Don't deploy without tests**: Run unit tests for metric parsing
- **Don't assume registration = emission**: Verify at `/metrics` endpoint
- **Don't debug in production first**: Test in local/dev environment

---

## Additional Resources

- [FR-073 Implementation Documentation](../feature/FR-073-IMPLEMENTATION.md)
- [SQL Metric Annotation Specification](../sql/metric-annotations.md)
- [Debugging Analysis Report](./FR-073-DEBUGGING-ANALYSIS.md)
- [Velostream Operator Guide](../operator/guide.md)

---

## Feedback

This guide is a living document based on real-world debugging experiences. If you encounter issues not covered here or have suggestions for improvement, please:

1. Create an issue in the project repository
2. Update this document with your findings
3. Share debugging tips with the team

**Last Updated**: 2025-10-10 (Based on FR-073 Phase 2A debugging session)
