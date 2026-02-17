# Velostream

High-performance streaming SQL engine written in Rust. Velostream provides real-time data processing with pluggable serialization formats (JSON, Avro, Protobuf), optimized for financial analytics use cases.

## Features

- **Streaming SQL** &mdash; Tumbling, sliding, and session windows with continuous and emit-mode aggregations
- **Stream-to-stream joins** &mdash; Windowed joins across multiple Kafka topics
- **Materialized tables** &mdash; CTAS (CREATE TABLE AS SELECT) with point lookups and stream-table joins
- **Pluggable serialization** &mdash; JSON, Avro, and Protobuf with schema registry support
- **Financial precision** &mdash; ScaledInteger arithmetic (42x faster than f64, exact decimal results)
- **Observability** &mdash; Prometheus metrics, OpenTelemetry traces, distributed trace propagation
- **Multi-job server** &mdash; Run multiple SQL pipelines in a single process via `velo-sql`

## Quick Start

```bash
# Build all binaries
cargo build --release

# Run a SQL pipeline
echo "CREATE STREAM trades_agg AS
  SELECT symbol, SUM(quantity) as total_qty, AVG(price) as avg_price
  FROM trades
  GROUP BY symbol
  WINDOW TUMBLING(INTERVAL '1' MINUTE)
  EMIT FINAL;" | ./target/release/velo-sql --file -
```

## Binaries

| Binary | Description |
|--------|-------------|
| `velo-sql` | Multi-job SQL server &mdash; deploy streaming pipelines from `.sql` files |
| `velo-cli` | Interactive CLI for managing running jobs |
| `velo-sql-batch` | Batch SQL execution for testing and CI |

## Related Projects

| Project | Description | License |
|---------|-------------|---------|
| [velo-test](https://github.com/velostreamai/velo-test) | SQL application test harness with Kafka testcontainers | Proprietary |
| [velo-studio](https://github.com/velostreamai/velo-studio) | AI-powered SQL Studio web application | Proprietary |

## Documentation

- [SQL Grammar](docs/sql/PARSER_GRAMMAR.md) &mdash; Complete EBNF grammar and AST reference
- [SQL Examples](docs/sql/COPY_PASTE_EXAMPLES.md) &mdash; Working examples for all query types
- [SQL Annotations](docs/user-guides/sql-annotations.md) &mdash; `@job_mode`, `@metric`, `@observability` reference
- [Kafka Schema Config](docs/developer/kafka-schema-configuration.md) &mdash; Avro/Protobuf schema configuration
- [Benchmarks](docs/benchmarks/) &mdash; Performance testing methodology and scripts

## Development

```bash
# Run unit tests
cargo test --lib --no-default-features

# Run comprehensive tests (excludes integration/performance)
cargo test --tests --no-default-features -- --skip integration:: --skip performance::

# Pre-commit checks
./run-commit.sh
```

See [CLAUDE.md](CLAUDE.md) for the full development guide.

## License

Licensed under the [Business Source License 1.1](LICENSE). Production use is permitted except for competitive hosted/embedded offerings. Converts to Apache 2.0 four years after each version's publication.
