# FR-073 SQL-Native Observability: Debugging Analysis

**Date**: 2025-10-10
**Phase**: FR-073 Phase 2A (Runtime Integration)
**Status**: Production debugging analysis complete

---

## Executive Summary

During Phase 2A implementation and verification, we discovered critical gaps in debugging SQL-native metrics that resulted in empty Grafana dashboard panels despite successful metric registration. This document analyzes the root causes and proposes systematic improvements.

---

## Incident Summary

### What Happened

**Symptom**: Grafana dashboard panel "Trading Latency Percentiles" showed no data despite:
- ✅ Metric annotations parsed correctly
- ✅ Metrics registered successfully
- ✅ Jobs showing "Running" status
- ✅ No obvious errors in logs

### Root Cause

Missing Avro schema file (`demo/trading/schemas/market_data_ts.avsc`) prevented 3 downstream SQL streams from reading the `market_data_ts` topic, causing:
- Zero records processed
- No histogram buckets emitted
- Empty dashboard panels
- Silent failure mode (no error logs)

### Affected Streams

| Stream | Job ID | Status | Records | Impact |
|--------|--------|--------|---------|--------|
| `market_data_ts` | financia_featureswatermarkscircuitbreak_14939_00 | ✅ Writing | 150+ rec/s | **Working** (producer) |
| `tick_buckets` | financia_fr073sqlnativeobservabilitytic_14939_01 | ❌ Reading | 0 rec/s | No tick metrics |
| `advanced_price_movement_alerts` | financia_phase3advancedwindowfunctionsp_14939_02 | ❌ Reading | 0 rec/s | **Empty latency panel** |
| `volume_spike_analysis` | financia_phase2resourcemanagementcircui_14939_03 | ❌ Reading | 0 rec/s | No spike metrics |

---

## Debugging Gaps Identified

### Gap 1: Kafka Topic Data Flow Visibility

**Problem**: Job shows "Running" but doesn't indicate data starvation.

**Evidence**:
```log
[INFO] Job 'financia_phase3advancedwindowfunctionsp_14939_02' (market_data_ts): Running - 0 records processed
[INFO] Job 'financia_phase3advancedwindowfunctionsp_14939_02': ✅ Metrics recorded: deserialization from 'source_0_market_data_ts' (0 records, 0.00 rec/s)
```

**Issue**: "Running" status implies health, but "0 records" is buried in logs and not exposed via metrics or dashboard.

**Proposed Solution**:
- Add `velo_stream_records_consumed_total` counter metric per job
- Add `velo_stream_data_starvation` alert when `records_processed = 0` for >60s
- Expose "Records/sec" in job status API response

---

### Gap 2: Schema Configuration Error Handling

**Problem**: Missing schema file reference causes silent failure.

**Evidence**:
```yaml
# market_data_ts_source.yaml
schema:
  value.schema.file: "schemas/market_data_ts.avsc"  # File doesn't exist
```

**Issue**: No error logged when schema file is missing. Job starts, tries to deserialize, gets 0 records, and continues silently.

**Proposed Solution**:
- **Fail Fast**: Validate schema file exists during job deployment (Phase 2B)
- **Clear Error Message**: "Schema file not found: schemas/market_data_ts.avsc"
- **Job Status**: Mark job as "Failed" (not "Running") when schema validation fails

---

### Gap 3: Metric Registration vs Emission Visibility

**Problem**: Metrics registered successfully but never emit data.

**Evidence**:
```log
[INFO] 📊 Registered dynamic histogram metric: name=velo_trading_price_change_percent, labels=["symbol"]
[INFO] Job 'financia_phase3advancedwindowfunctionsp_14939_02': Registered histogram metric 'velo_trading_price_change_percent' with labels ["symbol"]

# But metrics endpoint shows:
$ curl http://localhost:9091/metrics | grep velo_trading_price_change_percent
# (no results - metric never emitted because no records processed)
```

**Issue**: Registration log implies success, but metric never appears at `/metrics` endpoint.

**Proposed Solution**:
- Add `velo_metric_emissions_total{metric_name, job_id}` counter
- Alert when `registered_metrics > 0` but `emissions = 0` for >5 minutes
- Add "Last Emitted" timestamp to metric metadata

---

### Gap 4: Dashboard Panel to SQL Stream Traceability

**Problem**: No easy way to trace empty Grafana panel back to source SQL stream.

**Grafana Panel**:
```json
{
  "title": "Trading Latency Percentiles",
  "targets": [
    {"expr": "histogram_quantile(0.50, rate(velo_trading_price_change_percent_bucket[5m]))"}
  ]
}
```

**Challenge**: User sees empty panel but doesn't know:
- Which SQL stream produces `velo_trading_price_change_percent`
- Whether stream is running or failed
- What the data flow path is

**Proposed Solution**:
- Add metadata to metric annotations:
  ```sql
  -- @metric: velo_trading_price_change_percent
  -- @metric_type: histogram
  -- @metric_stream: advanced_price_movement_alerts  # NEW: Add stream name
  -- @metric_help: "Distribution of price changes"
  ```
- Create `/metrics/metadata` endpoint exposing metric → stream → job mapping
- Add Grafana template variable to filter by stream health

---

### Gap 5: Consumer Offset Warnings Clarity

**Problem**: Consumer offset warnings don't indicate root cause.

**Evidence**:
```log
[WARN] Job 'financia_fr073sqlnativeobservabilitytic_14939_01': Failed to commit source 'source_0_market_data_ts': ExecutionError { message: "Failed to commit source 'source_0_market_data_ts': Consumer commit error: NoOffset (Local: No offset stored)" }
```

**Issue**: "NoOffset" could mean:
1. Topic is empty (no messages produced yet)
2. Schema deserialization failing silently
3. Consumer group hasn't consumed anything yet
4. Topic partitions reassigned

**Proposed Solution**:
- Distinguish warning types:
  - `WARN: No offset (topic empty)` vs
  - `ERROR: No offset (deserialization failed)` vs
  - `INFO: No offset (consumer just started)`
- Add consumer lag metrics: `velo_kafka_consumer_lag{topic, job_id}`

---

## Investigation Timeline

**Step-by-step debugging process used:**

1. ✅ Checked metrics endpoint → Confirmed 2/5 metrics working
2. ✅ Ran unit tests → Confirmed parser works
3. ✅ Checked job status API → All jobs "Running" (false positive)
4. ✅ Searched logs for job names → Found "0 records processed"
5. ✅ Checked Kafka topics → `market_data_ts` topic exists
6. ✅ Checked config files → Found missing schema file reference
7. ✅ Traced data flow → Producer writes, consumers can't read
8. ✅ Identified root cause → Missing Avro schema file

**Time to Root Cause**: ~15 minutes (with manual log analysis)

---

## Proposed Debugging Improvements

### Immediate (Phase 2B)

1. **Schema Validation at Deployment**
   - Validate all schema file references before starting job
   - Fail fast with clear error message

2. **Data Starvation Alerts**
   - Alert when job runs for >60s with 0 records processed
   - Expose in `/health` endpoint

3. **Metric Emission Tracking**
   - Add `velo_metric_emissions_total` counter
   - Track first/last emission timestamp

### Medium-Term (Phase 3)

4. **Dashboard Panel Health Indicators**
   - Add status badge to Grafana panels
   - Link panels to source stream documentation

5. **Improved Log Messages**
   - Structured JSON logs with traceability
   - Clear distinction between INFO/WARN/ERROR

6. **Job Health Score**
   - Composite health metric (0-100)
   - Factors: records/sec, error rate, latency, metric emission

### Long-Term (Phase 4)

7. **Auto-Discovery and Documentation**
   - Generate metric catalog from SQL annotations
   - Auto-update Grafana dashboard metadata

8. **Debugging CLI Tool**
   ```bash
   $ velo debug metric velo_trading_price_change_percent
   Metric: velo_trading_price_change_percent
   Type: Histogram
   Stream: advanced_price_movement_alerts
   Job: financia_phase3advancedwindowfunctionsp_14939_02
   Status: ❌ NOT EMITTING (0 records processed)
   Root Cause: Source topic 'market_data_ts' schema file missing
   Fix: Create demo/trading/schemas/market_data_ts.avsc
   ```

9. **Visual Data Flow Diagram**
   - Generate DAG showing topic → stream → metrics
   - Highlight broken links in real-time

---

## Lessons Learned

### What Worked Well

✅ **Unit Tests**: Parser tests caught annotation parsing bugs early
✅ **Metric Registration Logs**: Clear indication when metrics are registered
✅ **Structured Job Naming**: Easy to grep logs for specific streams
✅ **Prometheus Integration**: Metrics endpoint made verification straightforward

### What Needs Improvement

❌ **Silent Failures**: Schema errors should fail loudly, not silently
❌ **Job Status Granularity**: "Running" doesn't indicate health
❌ **Traceability**: Hard to map dashboard panel → SQL stream → Kafka topic
❌ **Error Diagnostics**: Warnings don't explain root cause clearly

---

## Recommendations

### For FR-073 Phase 2B

1. **Priority 1**: Implement schema validation at deployment (prevents this class of bug)
2. **Priority 2**: Add data starvation alerts (catches issues early)
3. **Priority 3**: Create debugging CLI tool (reduces time-to-resolution)

### For Production Deployments

1. **Pre-Flight Checks**:
   - Validate all schema files exist
   - Verify Kafka topics are producing data
   - Test metric emission before deploying dashboards

2. **Monitoring**:
   - Alert on `records_processed = 0` for >60s
   - Alert on `registered_metrics - emitting_metrics > 0`
   - Monitor consumer lag per job

3. **Documentation**:
   - Document metric → stream mapping
   - Create troubleshooting runbook
   - Add debugging section to operator guide

---

## Appendix: Debugging Command Reference

### Check Job Status
```bash
curl -s http://localhost:8080/jobs | jq -r '.[] | "\(.name): \(.status)"'
```

### Check Metric Availability
```bash
curl -s http://localhost:9091/metrics | grep "^velo_trading_"
```

### Find Job Processing Specific Stream
```bash
grep "Deploying job" /tmp/velo_deployment.log | grep "stream_name"
```

### Check Records Processed
```bash
grep "job_id" /tmp/velo_deployment.log | grep "records processed"
```

### Verify Kafka Topic Has Data
```bash
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic topic_name \
  --max-messages 1 \
  --timeout-ms 3000
```

### Check Schema File Exists
```bash
ls demo/trading/schemas/*.avsc
```

---

## Conclusion

The missing Avro schema file revealed systematic gaps in debugging SQL-native metrics. By implementing the proposed improvements—especially schema validation, data starvation alerts, and better traceability—we can reduce time-to-resolution from 15 minutes to <2 minutes and prevent entire classes of silent failures.

**Next Steps**:
1. Create missing schema file to unblock current demo
2. Implement Phase 2B schema validation
3. Add debugging utilities to velostream CLI
4. Update operator documentation with troubleshooting guide
