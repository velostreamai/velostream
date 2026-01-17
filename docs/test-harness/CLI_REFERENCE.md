# CLI Reference

Complete reference for the `velo-test` command-line interface.

## Synopsis

```bash
velo-test <COMMAND> [OPTIONS]
```

## Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Enable verbose output |
| `--help` | `-h` | Print help |
| `--version` | `-V` | Print version |

## Commands Overview

Commands are listed in recommended workflow order:

| Command | Description |
|---------|-------------|
| `quickstart` | Interactive wizard - guides you through testing a SQL file |
| `validate` | Validate SQL syntax without execution |
| `init` | Generate test specification from SQL file |
| `infer-schema` | Infer schemas from sample data files |
| `run` | Run tests against a SQL application |
| `debug` | Interactive debugger with stepping and breakpoints |
| `stress` | Run stress/performance tests |
| `annotate` | Generate SQL annotations and monitoring configs |
| `scaffold` | Generate velo-test.sh runner script |

## Non-Interactive Mode

All commands that prompt for input support `-y` / `--yes` to skip prompts and use sensible defaults:

```bash
velo-test run app.sql -y           # Auto-discover spec, no prompts
velo-test init app.sql -y          # Generate with defaults
velo-test stress app.sql -y        # 100k records, 60s duration
velo-test annotate app.sql -y      # Infer name from filename
velo-test scaffold . -y            # Use all auto-detected values
```

---

## velo-test quickstart

Interactive wizard that guides new users through the complete testing workflow.

```bash
velo-test quickstart <SQL_FILE>
```

### What It Does

1. **Validates SQL syntax** - Checks for parse errors
2. **Discovers existing artifacts** - Finds specs, schemas, data files
3. **Generates test spec** - Creates or uses existing spec (with save option)
4. **Guides schema setup** - Explains how to add schemas if missing
5. **Runs tests** - Optionally executes tests immediately

### Example

```bash
$ velo-test quickstart my_app.sql

🚀 Velostream Test Harness - Quickstart Wizard
════════════════════════════════════════════════

📋 Step 1/5: Validating SQL syntax
✅ All 3 statements are valid

📋 Step 2/5: Checking existing test artifacts
○ No test spec found (will generate)
✓ Found schemas: ./schemas
○ No data dir found (optional)

📋 Step 3/5: Test specification
📝 Generated test spec with 2 queries
Save test spec to my_app.test.yaml? [Y/n]: y
✅ Saved: my_app.test.yaml

📋 Step 4/5: Schema generation (optional)
✓ Using existing schemas

📋 Step 5/5: Ready to test!
🎉 Setup complete! Run tests now? [Y/n]: y
```

---

## velo-test validate

Validate SQL syntax without running against infrastructure.

```bash
velo-test validate <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Show detailed validation output |

### Examples

```bash
velo-test validate app.sql
velo-test validate app.sql --verbose
```

---

## velo-test init

Generate a test specification from a SQL file.

```bash
velo-test init <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--output` | `-o` | Output path (default: prompts or `<name>.test.yaml`) |
| `--ai` | | Use AI to generate intelligent assertions |
| `--yes` | `-y` | Skip prompts, use defaults |

### Auto-Discovery

When `--output` is not specified:
- **Interactive mode**: Prompts for output path with sensible default
- **With `-y`**: Uses `<sqlname>.test.yaml` in SQL file directory

### Examples

```bash
# Interactive mode
velo-test init app.sql

# Non-interactive with defaults
velo-test init app.sql -y

# Specify output
velo-test init app.sql --output test_spec.yaml

# AI-assisted
velo-test init app.sql --ai --output test_spec.yaml
```

---

## velo-test infer-schema

Infer schema definitions from sample data files.

```bash
velo-test infer-schema <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--data-dir` | `-d` | Directory containing CSV/JSON files (auto-discovered) |
| `--output` | `-o` | Output directory for schemas (default: `schemas/`) |
| `--yes` | `-y` | Skip prompts, use defaults |

### Auto-Discovery

Automatically searches for data in:
- `./data/` (relative to SQL file)
- `../data/` (parent directory)

### Examples

```bash
# Auto-discover data directory
velo-test infer-schema app.sql

# Specify directories
velo-test infer-schema app.sql --data-dir ./sample_data --output ./schemas

# Non-interactive
velo-test infer-schema app.sql -y
```

---

## velo-test run

Run tests against a SQL application.

```bash
velo-test run <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--spec` | `-s` | Test specification file (auto-discovered) |
| `--schemas` | | Schema directory (auto-discovered) |
| `--query` | `-q` | Run only specific query by name |
| `--output` | `-o` | Output format: `text`, `json`, `junit` |
| `--timeout-ms` | | Timeout per query (default: 30000) |
| `--kafka` | | External Kafka bootstrap servers |
| `--keep-containers` | | Keep testcontainers running after test |
| `--step` | | Execute statements one at a time |
| `--yes` | `-y` | Skip prompts, use auto-discovered values |

### Auto-Discovery

When `--spec` is not provided:
1. Searches for `<sqlname>.test.yaml`, `<sqlname>.spec.yaml`, `test_spec.yaml`
2. If not found, offers to generate from SQL (with save option)
3. With `-y`: auto-generates without prompting

### Examples

```bash
# Auto-discover everything
velo-test run app.sql

# Non-interactive (CI/CD friendly)
velo-test run app.sql -y

# Specify spec file
velo-test run app.sql --spec test_spec.yaml

# Run single query
velo-test run app.sql --query my_query

# JUnit output for CI
velo-test run app.sql -y --output junit > results.xml

# Step-by-step execution
velo-test run app.sql --step

# External Kafka
velo-test run app.sql --kafka kafka:9092
```

---

## velo-test debug

Interactive SQL debugger with stepping, breakpoints, and inspection.

```bash
velo-test debug <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--spec` | `-s` | Test specification file (auto-discovered) |
| `--schemas` | | Schema directory (auto-discovered) |
| `--breakpoint` | `-b` | Set breakpoint on query (can repeat) |
| `--timeout-ms` | | Timeout per query (default: 30000) |
| `--kafka` | | External Kafka bootstrap servers |
| `--keep-containers` | | Keep containers running after exit |
| `--yes` | `-y` | Skip prompts, use auto-discovered values |

### Debug Commands

Once in the debugger, use these commands:

| Command | Description |
|---------|-------------|
| `step` / `s` | Execute next statement |
| `continue` / `c` | Run until breakpoint or end |
| `breakpoint <name>` | Set breakpoint on query |
| `list` / `l` | Show all statements |
| `show` | Show current statement |
| `output` | Show last output records |
| `filter <field> <op> <value>` | Filter output records |
| `export <format> <path>` | Export output (csv, json) |
| `help` | Show available commands |
| `quit` / `q` | Exit debugger |

### Examples

```bash
# Start debugger
velo-test debug app.sql

# With breakpoints
velo-test debug app.sql -b query1 -b query2

# Non-interactive setup
velo-test debug app.sql -y
```

---

## velo-test stress

Run stress/performance tests with high volume.

```bash
velo-test stress <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--spec` | `-s` | Test specification (auto-discovered) |
| `--records` | | Records per source (default: 100000) |
| `--duration` | | Max duration in seconds (default: 60) |
| `--output` | `-o` | Output format: `text`, `json` |
| `--yes` | `-y` | Skip prompts, use defaults |

### Examples

```bash
# Default stress test (100k records, 60s)
velo-test stress app.sql -y

# Custom volume
velo-test stress app.sql --records 1000000 --duration 120

# JSON output for analysis
velo-test stress app.sql --output json > stress.json
```

### Output Metrics

- **Throughput**: Records/second achieved
- **Latency**: P50, P95, P99 latencies
- **Memory**: Peak usage, growth rate
- **Errors**: Error count and rate

---

## velo-test annotate

Generate SQL annotations and monitoring infrastructure.

```bash
velo-test annotate <SQL_FILE> [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--output` | `-o` | Output path for annotated SQL |
| `--name` | `-n` | Application name (default: inferred from filename) |
| `--version` | | Application version (default: 1.0.0) |
| `--monitoring` | `-m` | Directory for monitoring configs |
| `--yes` | `-y` | Skip prompts, use defaults |

### Recommended Two-File Workflow

**Keep source and generated files separate:**

| File | Contains | Purpose |
|------|----------|---------|
| `app.sql` | SQL queries + header docs (NO metrics) | Source file - edit this |
| `app.annotated.sql` | SQL + auto-generated metrics | Production file - use this |

**Why this matters:**
- Metrics MUST be placed **immediately before** each `CREATE STREAM` statement
- The parser associates `@metric` annotations with the next CREATE statement it finds
- Manually managing metrics in headers is error-prone and causes them to be "detached"

**Workflow:**

```bash
# 1. Edit your source SQL (no metrics needed)
vim app.sql

# 2. Generate annotated version with metrics
velo-test annotate app.sql --output app.annotated.sql --monitoring ./monitoring

# 3. Run/deploy the annotated version
velo-test run app.annotated.sql
```

### What It Generates

**Annotated SQL** (`*.annotated.sql`):
- `@app`, `@version`, `@description` annotations (header)
- `@metric` annotations placed **immediately before each CREATE STREAM**
- `@observability` settings

**Monitoring configs** (when `--monitoring` specified):
- `prometheus.yml` - Scrape configuration
- `grafana/dashboards/*.json` - Auto-generated dashboards
- `grafana/provisioning/` - Datasource configs
- `tempo/tempo.yaml` - Tracing configuration

### Metric Placement (Critical)

The annotator places metrics correctly:

```sql
-- ✅ CORRECT: Metrics immediately before CREATE STREAM
-- -----------------------------------------------------------------------------
-- METRICS for market_data_ts
-- -----------------------------------------------------------------------------
-- @metric: velo_app_market_data_ts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed"
--
CREATE STREAM market_data_ts AS ...
```

**Never do this manually:**

```sql
-- ❌ WRONG: Metrics in header, far from CREATE STREAM
-- @metric: my_metric
-- ... 50 lines later ...
CREATE STREAM my_stream AS ...  -- Parser won't associate the metric!
```

### Examples

```bash
# Interactive mode
velo-test annotate app.sql

# Non-interactive
velo-test annotate app.sql -y

# Generate monitoring stack
velo-test annotate app.sql --monitoring ./monitoring

# Custom app name
velo-test annotate app.sql --name my_app --version 2.0.0

# Full workflow
velo-test annotate apps/my_app.sql \
  --output apps/my_app.annotated.sql \
  --monitoring monitoring
```

---

## velo-test scaffold

Generate a `velo-test.sh` runner script for a project.

```bash
velo-test scaffold [DIRECTORY] [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--output` | `-o` | Output script path (default: `<dir>/velo-test.sh`) |
| `--name` | `-n` | Project name (default: directory name) |
| `--style` | | Force style: `simple`, `tiered`, `minimal` |
| `--velo-test-path` | | Path to velo-test binary |
| `--yes` | `-y` | Skip prompts, use auto-detected values |

### Auto-Detection

The scaffold command analyzes your directory structure:

| Pattern | Detected Style |
|---------|----------------|
| `tier*_*/` directories | tiered |
| `apps/*.sql` + `tests/` | simple |
| Single SQL file | minimal |

### Examples

```bash
# Interactive mode
velo-test scaffold demo/trading/

# Non-interactive
velo-test scaffold demo/trading/ -y

# Force specific style
velo-test scaffold . --style tiered

# Custom output
velo-test scaffold . --output run-tests.sh --name "My Project"
```

### Generated Script Features

**Tiered style**:
- `./velo-test.sh run` - Run all tiers
- `./velo-test.sh tier1` - Run specific tier
- `./velo-test.sh validate` - Validate all SQL

**Simple style**:
- `./velo-test.sh` - List apps
- `./velo-test.sh <app>` - Run specific app
- `./velo-test.sh all` - Run all apps

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (debug, info, warn, error) |
| `VELOSTREAM_KAFKA_BROKERS` | Default Kafka bootstrap servers |
| `ANTHROPIC_API_KEY` | API key for AI features (required for `--ai` flag) |

### Logging Examples

```bash
# Debug all
RUST_LOG=debug velo-test run app.sql

# Debug specific module
RUST_LOG=velostream::test_harness=debug velo-test run app.sql
```

---

## Data Generation Annotations

Embed test data generation hints directly in SQL source files using `@data.*` annotations. These replace separate schema YAML files for simpler workflows.

### Syntax

```sql
-- Global hints
-- @data.source: in_market_data        -- Source stream name (required for multi-source)
-- @data.record_count: 1000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"
-- @data.seed: 42

-- Field hints (type is required)
-- @data.<field>.type: string|integer|float|decimal(N)|timestamp|date|uuid|boolean

-- Enum constraint
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT"], weights: [0.4, 0.3, 0.3]

-- Range with distribution
-- @data.price.type: decimal(4)
-- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02, group_by: symbol

-- Timestamp with sequential generation
-- @data.event_time.type: timestamp
-- @data.event_time: timestamp, sequential: true

-- Derived field (calculated from other fields)
-- @data.bid_price.type: decimal(4)
-- @data.bid_price.derived: "price * random(0.998, 0.9999)"

-- UUID for unique identifiers
-- @data.order_id.type: uuid

-- Boolean field
-- @data.is_active.type: boolean
```

### Available Distributions

| Distribution | Description | Parameters |
|--------------|-------------|------------|
| `uniform` | Equal probability (default) | None |
| `normal` | Bell curve | `mean`, `std_dev` |
| `log_normal` | Right-skewed | `mean`, `std_dev` |
| `zipf` | Power law | `exponent` |
| `random_walk` | GBM for realistic prices | `volatility`, `drift`, `group_by` |

### Priority Rules

1. Schema YAML files take precedence over SQL hints
2. All fields require explicit types via `@data.<field>.type`
3. Errors if hint field doesn't match any source field in SQL

### Example

```sql
-- =============================================================================
-- DATA GENERATION HINTS for in_market_data
-- =============================================================================
-- @data.record_count: 1000
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT", "AMZN"]
-- @data.price.type: decimal(4)
-- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02, group_by: symbol
-- @data.volume.type: integer
-- @data.volume: range [100, 50000], distribution: log_normal
-- @data.timestamp.type: timestamp
-- @data.timestamp: timestamp, sequential: true

CREATE STREAM market_data AS
SELECT symbol, price, volume, timestamp
FROM in_market_data
EMIT CHANGES;
```

---

## Common Workflows

### New User Workflow

```bash
# Use the quickstart wizard
velo-test quickstart my_app.sql
```

### Development Workflow

```bash
# 1. Validate during development
velo-test validate app.sql

# 2. Generate test spec
velo-test init app.sql

# 3. Run tests
velo-test run app.sql

# 4. Debug failures
velo-test debug app.sql
```

### CI/CD Workflow

```bash
# Non-interactive with JUnit output
velo-test run app.sql -y --output junit > results.xml
```

### Performance Workflow

```bash
# Run stress test
velo-test stress app.sql --records 100000 --output json > results.json
```

### Observability Workflow (Recommended)

```bash
# 1. Write your SQL (source file, no metrics)
vim apps/my_app.sql

# 2. Generate annotated SQL + monitoring stack
velo-test annotate apps/my_app.sql \
  --output apps/my_app.annotated.sql \
  --monitoring monitoring

# 3. Start monitoring
docker-compose -f monitoring/docker-compose.yml up -d

# 4. Run the annotated version
velo-test run apps/my_app.annotated.sql

# 5. View dashboards at http://localhost:3000
```

**Key principle:** Keep source `.sql` files clean (no metrics). Let `velo-test annotate` generate metrics automatically and place them correctly before each `CREATE STREAM`.
