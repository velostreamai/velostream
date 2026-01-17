# Lesson 7: Adding Observability (Metrics & Dashboards)

Production streaming applications need monitoring. The `annotate` command adds observability annotations and generates monitoring infrastructure.

## The `annotate` Command

```bash
# Generate annotated SQL with metrics
../../target/release/velo-test annotate hello_world.sql -y
```

This creates `hello_world.annotated.sql` with:
- Application metadata (`@app`, `@version`, `@description`)
- Metric annotations (`@metric`, `@metric_type`, `@metric_help`)
- Observability settings (`@observability.metrics.enabled`)

## What Gets Added

**Before (hello_world.sql):**
```sql
CREATE STREAM hello_world AS
SELECT * FROM input_data
EMIT CHANGES
WITH (...);
```

**After (hello_world.annotated.sql):**
```sql
-- =============================================================================
-- APPLICATION: hello_world
-- =============================================================================
-- @app: hello_world
-- @version: 1.0.0
-- @description: Passthrough streaming query
--
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- -----------------------------------------------------------------------------
-- METRICS for hello_world
-- -----------------------------------------------------------------------------
-- @metric: velo_hello_world_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by hello_world"
--
CREATE STREAM hello_world AS
SELECT * FROM input_data
EMIT CHANGES
WITH (...);
```

## Try It

```bash
# Annotate a SQL file
../../target/release/velo-test annotate 03_aggregate.sql -y

# View the annotated version
cat 03_aggregate.annotated.sql
```

## Generating Monitoring Infrastructure

Add `--monitoring` to generate Prometheus/Grafana configs:

```bash
# Create monitoring directory
mkdir -p monitoring

# Generate annotated SQL + monitoring stack
../../target/release/velo-test annotate 03_aggregate.sql \
  --monitoring ./monitoring \
  -y

# See what was generated
ls -la monitoring/
```

## Generated Monitoring Files

```
monitoring/
├── prometheus.yml              # Prometheus scrape config
├── grafana/
│   ├── dashboards/
│   │   └── 03_aggregate-dashboard.json   # Auto-generated dashboard
│   └── provisioning/
│       └── datasources/
│           └── prometheus.yml  # Grafana datasource config
└── tempo/
    └── tempo.yaml              # Distributed tracing config
```

## Auto-Generated Dashboard

The generated Grafana dashboard includes panels for:

- **Records Processed** - Counter of total records
- **Processing Rate** - Records per second
- **Error Rate** - Failed records (if any)
- **Latency Histogram** - Processing time distribution

## Running with Monitoring (Docker)

To use the generated monitoring stack:

```bash
# In a project with docker-compose.yml that includes Grafana/Prometheus:
docker-compose up -d

# Access dashboards
open http://localhost:3000  # Grafana (admin/admin)
open http://localhost:9090  # Prometheus
```

## Interactive Mode

Without `-y`, the annotate command asks questions:

```bash
../../target/release/velo-test annotate 04_window.sql

# Prompts:
# - Application name?
# - Version?
# - Enable metrics? [Y/n]
# - Enable tracing? [Y/n]
# - Generate monitoring configs? [y/N]
```

## Metrics Available

Each annotated query exposes these Prometheus metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `velo_{app}_{query}_records_total` | Counter | Total records processed |
| `velo_{app}_{query}_records_per_second` | Gauge | Current throughput |
| `velo_{app}_{query}_processing_time_seconds` | Histogram | Latency distribution |
| `velo_{app}_{query}_errors_total` | Counter | Processing errors |

## Next Steps

Now you've learned:
1. ✅ Basic SQL (hello_world → window)
2. ✅ Test specification generation (init)
3. ✅ Runner script generation (scaffold)
4. ✅ Observability (annotate + monitoring)

**Ready for production?** See:
- `demo/trading/` - Full production demo with Kafka + monitoring
- `docs/test-harness/LEARNING_PATH.md` - Complete learning progression
