# Lesson 8: Deploying to Production

You've tested your SQL with `velo-test`. Now let's deploy it to a real Kafka cluster using `velo-sql`.

## The Deployment Workflow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Develop   │────▶│    Test     │────▶│   Deploy    │────▶│   Monitor   │
│  (SQL files)│     │ (velo-test) │     │ (velo-sql)  │     │ (velo-cli)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Prerequisites

For deployment, you need:
- Kafka cluster running (Docker or remote)
- Built binaries: `velo-sql` and `velo-cli`

```bash
# Build deployment tools (one-time)
cargo build --release --bin velo-sql
cargo build --release --bin velo-cli
```

## Option 1: Deploy a Single App

Deploy a SQL file directly to Kafka:

```bash
# Deploy your annotated app
velo-sql deploy-app \
  --file 03_aggregate.annotated.sql \
  --brokers localhost:9092 \
  --enable-metrics
```

**What happens:**
1. Parses the SQL file
2. Creates Kafka consumers for input topics
3. Creates Kafka producers for output topics
4. Starts processing data continuously

## Option 2: Start a SQL Server

For multiple apps, run the StreamJobServer:

```bash
# Start the server
velo-sql server \
  --brokers localhost:9092 \
  --port 8080 \
  --max-jobs 10 \
  --enable-metrics

# Server endpoints:
# - http://localhost:8080/status  - Server status
# - http://localhost:8080/jobs    - List running jobs
# - http://localhost:9091/metrics - Prometheus metrics
```

## Monitoring with velo-cli

Once deployed, use `velo-cli` to monitor:

```bash
# Quick health check
velo-cli health

# Show all component status
velo-cli status

# Show running jobs
velo-cli jobs

# Show Kafka cluster info
velo-cli kafka

# Show SQL server info
velo-cli sql
```

## Example: Full Deployment Workflow

```bash
# 1. Validate SQL
velo-test validate 03_aggregate.sql

# 2. Generate annotated version with monitoring
velo-test annotate 03_aggregate.sql \
  --monitoring ./monitoring \
  -y

# 3. Start Kafka (if not running)
docker-compose up -d kafka

# 4. Deploy the app
velo-sql deploy-app \
  --file 03_aggregate.annotated.sql \
  --brokers localhost:9092 \
  --enable-metrics \
  --enable-tracing

# 5. Monitor
velo-cli jobs
```

## Deployment Options

### velo-sql deploy-app

| Option | Description |
|--------|-------------|
| `--file` | SQL application file (required) |
| `--brokers` | Kafka broker addresses (default: localhost:9092) |
| `--group-id` | Consumer group ID prefix |
| `--enable-metrics` | Enable Prometheus metrics |
| `--metrics-port` | Metrics port (default: 9091) |
| `--enable-tracing` | Enable OpenTelemetry tracing |
| `--sampling-mode` | Sampling mode: debug, dev, staging (default), prod |
| `--sampling-ratio` | Trace sampling ratio override (0.0-1.0) |
| `--no-monitor` | Exit after deploy (don't wait) |

### velo-sql server

| Option | Description |
|--------|-------------|
| `--port` | Server port (default: 8080) |
| `--max-jobs` | Max concurrent jobs (default: 10) |
| `--enable-metrics` | Enable performance monitoring |

## Production Deployment

For production, see:
- [docs/ops/stream-job-server-guide.md](../../docs/ops/stream-job-server-guide.md)
- [docs/ops/productionisation.md](../../docs/ops/productionisation.md)
- [docs/ops/operational-runbook.md](../../docs/ops/operational-runbook.md)

## Docker Deployment

Deploy with Docker:

```bash
# Build Docker image
docker build -t velostream:latest .

# Run with Docker
docker run -d \
  --name velo-sql \
  -p 8080:8080 \
  -p 9091:9091 \
  -e VELOSTREAM_KAFKA_BROKERS=kafka:9092 \
  velostream:latest \
  velo-sql server --enable-metrics
```

## Next Steps

Congratulations! You've completed the quickstart:

| ✅ | Lesson | What You Learned |
|----|--------|------------------|
| ✅ | SQL Basics | SELECT, WHERE, transformations |
| ✅ | Aggregations | GROUP BY, COUNT, SUM |
| ✅ | Window Functions | LAG, PARTITION BY |
| ✅ | Test Specs | `velo-test init` |
| ✅ | Runner Scripts | `velo-test scaffold` |
| ✅ | Observability | `velo-test annotate` |
| ✅ | Deployment | `velo-sql deploy-app` |

**Ready for more?**
- `demo/trading/` - Production-like financial trading demo
- `docs/ops/` - Operations and production guides
