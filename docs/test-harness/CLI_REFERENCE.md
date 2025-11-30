# CLI Reference

Complete reference for the `velo-test` command-line interface.

## Synopsis

```bash
velo-test <COMMAND> [OPTIONS]
```

## Commands

| Command | Description |
|---------|-------------|
| `validate` | Validate SQL syntax without running tests |
| `run` | Run tests against a SQL application |
| `init` | Generate test specification from SQL file |
| `infer-schema` | Infer schema from sample data |
| `stress` | Run stress/performance tests |

---

## velo-test validate

Validate SQL syntax without running against infrastructure.

```bash
velo-test validate <SQL_FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `SQL_FILE` | Path to SQL application file |

### Options

| Option | Description |
|--------|-------------|
| `--verbose`, `-v` | Show detailed validation output |
| `--strict` | Enable strict validation mode |

### Examples

```bash
# Basic validation
velo-test validate app.sql

# Verbose output
velo-test validate app.sql --verbose
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Validation passed |
| 1 | Validation failed |

---

## velo-test run

Run tests against a SQL application.

```bash
velo-test run <SQL_FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `SQL_FILE` | Path to SQL application file |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--spec` | `-s` | Path to test specification YAML file |
| `--query` | `-q` | Run only specific query by name |
| `--schemas` | | Directory containing schema files |
| `--data` | | Directory containing static test data |
| `--output` | `-o` | Output format: `text`, `json`, `junit` |
| `--timeout-ms` | | Global timeout override in milliseconds |
| `--records` | `-r` | Override default record count |
| `--verbose` | `-v` | Enable verbose output |
| `--no-topic-prefix` | | Don't prefix topics with run ID |
| `--bootstrap-servers` | | Kafka bootstrap servers (overrides spec) |
| `--seed` | | Random seed for reproducibility |

### Examples

```bash
# Run with test spec
velo-test run app.sql --spec test_spec.yaml

# Run specific query
velo-test run app.sql --spec test_spec.yaml --query my_query

# JSON output
velo-test run app.sql --spec test_spec.yaml --output json

# JUnit XML for CI
velo-test run app.sql --spec test_spec.yaml --output junit > results.xml

# Custom timeout
velo-test run app.sql --spec test_spec.yaml --timeout-ms 60000

# Override record count
velo-test run app.sql --spec test_spec.yaml --records 5000

# Verbose debugging
RUST_LOG=debug velo-test run app.sql --spec test_spec.yaml --verbose

# Custom Kafka
velo-test run app.sql --spec test_spec.yaml --bootstrap-servers kafka:9092

# Reproducible runs
velo-test run app.sql --spec test_spec.yaml --seed 42
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All tests passed |
| 1 | One or more tests failed |
| 2 | Configuration or runtime error |

---

## velo-test init

Generate a test specification from a SQL file.

```bash
velo-test init <SQL_FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `SQL_FILE` | Path to SQL application file |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--output` | `-o` | Output path for generated spec (default: stdout) |
| `--ai` | | Use AI to generate intelligent assertions |
| `--schemas` | | Directory containing existing schemas |
| `--infer-schemas` | | Infer schemas from SQL and sample data |
| `--data-dir` | | Directory with sample data for inference |

### Examples

```bash
# Generate basic spec
velo-test init app.sql --output test_spec.yaml

# AI-assisted generation
velo-test init app.sql --ai --output test_spec.yaml

# With existing schemas
velo-test init app.sql --schemas ./schemas --output test_spec.yaml

# Infer schemas from data
velo-test init app.sql --infer-schemas --data-dir ./data --output test_spec.yaml
```

### Generated Spec Structure

The generated spec includes:
- Detected queries from SQL
- Default input configurations
- Basic assertions (record_count, schema_contains, no_nulls)
- Default timeout values

---

## velo-test infer-schema

Infer schema definitions from sample data.

```bash
velo-test infer-schema <SQL_FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `SQL_FILE` | Path to SQL application file |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--data-dir` | `-d` | Directory containing sample data files |
| `--output` | `-o` | Output directory for generated schemas |
| `--format` | | Data file format: `csv`, `json`, `json_lines` |
| `--ai` | | Use AI for enhanced inference |
| `--sample-size` | | Number of records to sample (default: 1000) |

### Examples

```bash
# Infer from CSV files
velo-test infer-schema app.sql --data-dir ./data --output ./schemas

# JSON Lines format
velo-test infer-schema app.sql --data-dir ./data --output ./schemas --format json_lines

# AI-enhanced inference
velo-test infer-schema app.sql --data-dir ./data --output ./schemas --ai

# Custom sample size
velo-test infer-schema app.sql --data-dir ./data --output ./schemas --sample-size 5000
```

### Inference Capabilities

- **Type detection**: Integer, decimal, string, boolean, timestamp
- **Constraint inference**: Min/max ranges, enum values
- **Distribution analysis**: Uniform, normal, log-normal patterns
- **Pattern detection**: Email, UUID, phone number formats

---

## velo-test stress

Run stress/performance tests.

```bash
velo-test stress <SQL_FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `SQL_FILE` | Path to SQL application file |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--records` | `-r` | Total records to generate (default: 100000) |
| `--duration` | `-d` | Test duration in seconds (default: 60) |
| `--rate` | | Target records per second |
| `--schemas` | | Directory containing schemas |
| `--report-interval` | | Seconds between progress reports (default: 5) |
| `--output` | `-o` | Output format: `text`, `json` |
| `--memory-tracking` | | Enable memory tracking (may impact performance) |

### Examples

```bash
# Basic stress test
velo-test stress app.sql --records 100000 --duration 60

# Sustained rate test
velo-test stress app.sql --rate 10000 --duration 300

# With memory tracking
velo-test stress app.sql --records 100000 --memory-tracking

# JSON output for analysis
velo-test stress app.sql --records 100000 --output json > stress_results.json
```

### Output Metrics

- **Throughput**: Records/second achieved
- **Latency**: P50, P95, P99 latencies
- **Memory**: Peak usage, growth rate
- **Errors**: Error count and rate

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Logging level (debug, info, warn, error) |
| `VELO_TEST_KAFKA` | Default Kafka bootstrap servers |
| `VELO_TEST_TIMEOUT` | Default timeout in milliseconds |
| `OPENAI_API_KEY` | API key for AI features |

### Logging Examples

```bash
# Debug all modules
RUST_LOG=debug velo-test run app.sql --spec test_spec.yaml

# Debug specific module
RUST_LOG=velostream::test_harness=debug velo-test run app.sql --spec test_spec.yaml

# Info level with warnings
RUST_LOG=info,warn velo-test run app.sql --spec test_spec.yaml
```

---

## Configuration File

Default configuration can be set in `~/.config/velo-test/config.yaml`:

```yaml
# Default settings
defaults:
  timeout_ms: 30000
  records: 1000
  output: text

# Kafka settings
kafka:
  bootstrap_servers: localhost:9092

# AI settings (optional)
ai:
  model: gpt-4
  # API key should be in OPENAI_API_KEY env var
```

---

## Common Workflows

### Development Workflow

```bash
# 1. Validate SQL during development
velo-test validate app.sql

# 2. Generate initial test spec
velo-test init app.sql --output test_spec.yaml

# 3. Run tests locally
velo-test run app.sql --spec test_spec.yaml --verbose

# 4. Fix failures and re-run
velo-test run app.sql --spec test_spec.yaml --query failed_query
```

### CI/CD Workflow

```bash
# Run tests with JUnit output
velo-test run app.sql --spec test_spec.yaml --output junit > test-results.xml

# Exit code indicates pass/fail
if [ $? -eq 0 ]; then
  echo "Tests passed"
else
  echo "Tests failed"
  exit 1
fi
```

### Performance Workflow

```bash
# 1. Run baseline stress test
velo-test stress app.sql --records 100000 --output json > baseline.json

# 2. Make changes...

# 3. Run comparison stress test
velo-test stress app.sql --records 100000 --output json > after.json

# 4. Compare results
diff baseline.json after.json
```
