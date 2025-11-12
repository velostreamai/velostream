# StreamJobServer Configuration Usage Guide

## Overview

StreamJobServer uses a **layered configuration system** that supports multiple configuration sources with clear precedence rules. This guide explains how to use the configuration system for development, testing, and production scenarios.

## Configuration Hierarchy

The configuration system supports multiple sources with the following precedence (highest to lowest):

```
┌─────────────────────────────────────────┐
│  1. Environment Variables (highest)     │  VELOSTREAM_KAFKA_BROKERS=...
├─────────────────────────────────────────┤
│  2. Builder Pattern Methods             │  .with_max_jobs(50)
├─────────────────────────────────────────┤
│  3. Constructor Parameters              │  StreamJobServerConfig::new(...)
├─────────────────────────────────────────┤
│  4. Default Values (lowest)             │  localhost:9092, max_jobs=100
└─────────────────────────────────────────┘
```

## Quick Start

### Development (Default Behavior)

```rust
// Uses defaults: localhost:9092, 100 max jobs, no monitoring
let server = StreamJobServer::new("localhost:9092", "my-group", 10);
```

### Staging/Production (Environment Variables)

```bash
# Set environment variables
export VELOSTREAM_KAFKA_BROKERS="broker1:9092,broker2:9092,broker3:9092"
export VELOSTREAM_MAX_JOBS=500
export VELOSTREAM_ENABLE_MONITORING=true
export VELOSTREAM_JOB_TIMEOUT_SECS=172800

# Code doesn't change - uses environment configuration
let server = StreamJobServer::new("broker1:9092", "prod-group", 500);
```

### Testing (Configuration Objects)

```rust
// Pattern 1: Direct parameter override (backward compatible)
#[tokio::test]
async fn test_with_custom_broker() {
    let server = StreamJobServer::new(
        "test-broker:9092".to_string(),  // Override broker
        "test-group".to_string(),
        10
    );
    // Test your code...
}

// Pattern 2: Configuration object (recommended for tests)
#[tokio::test]
async fn test_with_config_object() {
    let config = StreamJobServerConfig::new("test-broker:9092", "test-group")
        .with_max_jobs(10);
    let server = StreamJobServer::with_config(config);
    // Test your code...
}
```

## Configuration Methods

### StreamJobServerConfig

The configuration struct centralizes all server settings:

```rust
pub struct StreamJobServerConfig {
    pub kafka_brokers: String,        // Kafka broker endpoints
    pub base_group_id: String,        // Consumer group ID base
    pub max_jobs: usize,              // Maximum concurrent jobs
    pub enable_monitoring: bool,      // Performance monitoring flag
    pub job_timeout: Duration,        // Job timeout
    pub table_cache_size: usize,      // Table registry cache size
}
```

### Constructor Methods

#### `new(kafka_brokers, base_group_id) -> Self`

Creates a configuration with explicit brokers and group ID. Other fields use defaults.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-group");
// kafka_brokers: "localhost:9092"
// base_group_id: "my-group"
// max_jobs: 100 (default)
// enable_monitoring: false (default)
// job_timeout: 24 hours (default)
// table_cache_size: 100 (default)
```

#### `from_env(base_group_id) -> Self`

Loads configuration from environment variables with fallback to defaults.

```rust
// Set environment (optional - will use defaults if not set)
std::env::set_var("VELOSTREAM_KAFKA_BROKERS", "prod-broker:9092");
std::env::set_var("VELOSTREAM_MAX_JOBS", "500");

let config = StreamJobServerConfig::from_env("prod-group");
// Reads from environment, falls back to defaults if not set
```

### Builder Pattern Methods

All builder methods return `Self` for method chaining:

#### `with_max_jobs(max_jobs: usize) -> Self`

Sets the maximum number of concurrent jobs.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "group")
    .with_max_jobs(50);
```

#### `with_monitoring(enable_monitoring: bool) -> Self`

Enables or disables performance monitoring.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "group")
    .with_monitoring(true);
```

#### `with_kafka_brokers(brokers: impl Into<String>) -> Self`

Sets Kafka broker endpoints.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "group")
    .with_kafka_brokers("prod1:9092,prod2:9092,prod3:9092");
```

#### `with_job_timeout(duration: Duration) -> Self`

Sets the job timeout duration.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "group")
    .with_job_timeout(Duration::from_secs(172800)); // 48 hours
```

#### `with_table_cache_size(size: usize) -> Self`

Sets the table registry cache size.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "group")
    .with_table_cache_size(500);
```

#### `with_dev_preset() -> Self`

Applies development presets:
- kafka_brokers: `localhost:9092`
- enable_monitoring: `false`
- max_jobs: `10`
- table_cache_size: `100`

```rust
let config = StreamJobServerConfig::default()
    .with_dev_preset();
```

#### `with_production_preset() -> Self`

Applies production presets:
- enable_monitoring: `true`
- job_timeout: `48 hours`
- max_jobs: `500`
- table_cache_size: `500`

```rust
let config = StreamJobServerConfig::default()
    .with_production_preset();
```

### Utility Methods

#### `summary() -> String`

Returns a formatted configuration summary for logging.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-group");
println!("{}", config.summary());
// Output: StreamJobServer Configuration: brokers=localhost:9092, group_id=my-group, ...
```

## Environment Variables

The configuration system supports 5 environment variables:

| Variable | Default | Format | Description |
|----------|---------|--------|-------------|
| `VELOSTREAM_KAFKA_BROKERS` | `localhost:9092` | Comma-separated | Kafka broker endpoints |
| `VELOSTREAM_MAX_JOBS` | `100` | Integer | Maximum concurrent jobs |
| `VELOSTREAM_ENABLE_MONITORING` | `false` | `true`/`false`/`1`/`0` | Performance monitoring |
| `VELOSTREAM_JOB_TIMEOUT_SECS` | `86400` | Integer (seconds) | Job timeout duration |
| `VELOSTREAM_TABLE_CACHE_SIZE` | `100` | Integer | Table registry cache |

### Example: Setting Environment Variables

```bash
# Development
export VELOSTREAM_KAFKA_BROKERS="localhost:9092"
export VELOSTREAM_MAX_JOBS="10"

# Staging
export VELOSTREAM_KAFKA_BROKERS="staging-broker1:9092,staging-broker2:9092"
export VELOSTREAM_MAX_JOBS="100"
export VELOSTREAM_ENABLE_MONITORING="true"

# Production
export VELOSTREAM_KAFKA_BROKERS="prod-broker1:9092,prod-broker2:9092,prod-broker3:9092"
export VELOSTREAM_MAX_JOBS="500"
export VELOSTREAM_ENABLE_MONITORING="true"
export VELOSTREAM_JOB_TIMEOUT_SECS="172800"
export VELOSTREAM_TABLE_CACHE_SIZE="500"
```

## StreamJobServer Creation Methods

Once you have a configuration, create the server using one of these methods:

### `with_config(config: StreamJobServerConfig) -> Self`

Creates a server with explicit configuration.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-group")
    .with_max_jobs(50);
let server = StreamJobServer::with_config(config);
```

### `with_config_and_monitoring(config: StreamJobServerConfig) -> Self` (async)

Creates a server with configuration and monitoring support.

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-group")
    .with_monitoring(true);
let server = StreamJobServer::with_config_and_monitoring(config).await;
```

### `with_config_and_observability(config: StreamJobServerConfig, streaming_config: StreamingConfig) -> Self` (async)

Creates a server with full observability configuration (tracing, metrics, profiling).

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-group");
let streaming_config = StreamingConfig::default()
    .with_prometheus_metrics()
    .with_distributed_tracing();
let server = StreamJobServer::with_config_and_observability(config, streaming_config).await;
```

## Usage Patterns by Scenario

### Pattern 1: Development (Backward Compatible)

Use the original constructor with direct parameters:

```rust
fn main() {
    let server = StreamJobServer::new(
        "localhost:9092".to_string(),
        "dev-group".to_string(),
        10
    );
    // Uses defaults for everything else
    // max_jobs: 10
    // monitoring: false
}
```

**When to use**: Local development, existing code that works as-is.

### Pattern 2: Environment-Driven (Recommended for Production)

Use environment variables for all configuration:

```rust
async fn main() {
    // No code changes - configuration from environment
    let config = StreamJobServerConfig::from_env("prod-group");
    let server = StreamJobServer::with_config_and_monitoring(config).await;
}
```

**When to use**: Production deployments, Docker containers, cloud platforms (AWS, GCP, Azure).

**Environment setup**:
```dockerfile
# Dockerfile
FROM rust:latest
# ... build steps ...
ENV VELOSTREAM_KAFKA_BROKERS="broker1:9092,broker2:9092"
ENV VELOSTREAM_MAX_JOBS="500"
ENV VELOSTREAM_ENABLE_MONITORING="true"
```

### Pattern 3: Builder Pattern (Recommended for Testing)

Use configuration objects with builder methods:

```rust
#[tokio::test]
async fn test_custom_broker() {
    let config = StreamJobServerConfig::new("test-broker:9092", "test-group")
        .with_max_jobs(5)
        .with_monitoring(true);
    let server = StreamJobServer::with_config(config);

    // Your test code here
}
```

**When to use**: Unit tests, integration tests, complex test scenarios.

### Pattern 4: Environment + Builder Pattern (Maximum Flexibility)

Combine environment variables with builder overrides:

```rust
#[tokio::test]
async fn test_with_env_override() {
    // Start with environment, override specific values
    let config = StreamJobServerConfig::from_env("test-group")
        .with_max_jobs(5)  // Override max jobs just for this test
        .with_monitoring(false);  // Disable monitoring

    let server = StreamJobServer::with_config(config);

    // Your test code here
}
```

**When to use**: Tests that need environmental configuration with specific overrides.

## Real-World Examples

### Example 1: Local Development

```rust
// src/main.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Uses defaults: localhost:9092
    let server = StreamJobServer::new(
        "localhost:9092".to_string(),
        "dev-app".to_string(),
        10
    );

    // Deploy a job
    server.deploy_job(
        "job1".to_string(),
        "1.0".to_string(),
        "SELECT * FROM events".to_string(),
        "events".to_string(),
    ).await?;

    Ok(())
}
```

### Example 2: Docker Production Deployment

```dockerfile
# Dockerfile
FROM rust:latest as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/velostream-server /usr/local/bin/

# Configuration from environment
ENV VELOSTREAM_KAFKA_BROKERS="kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
ENV VELOSTREAM_MAX_JOBS="500"
ENV VELOSTREAM_ENABLE_MONITORING="true"
ENV VELOSTREAM_JOB_TIMEOUT_SECS="172800"
ENV VELOSTREAM_TABLE_CACHE_SIZE="500"

ENTRYPOINT ["/usr/local/bin/velostream-server"]
```

Production code:
```rust
// src/main.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // All configuration from environment
    let config = StreamJobServerConfig::from_env("production");
    let server = StreamJobServer::with_config_and_monitoring(config).await;

    // Server uses:
    // - kafka_brokers from VELOSTREAM_KAFKA_BROKERS
    // - max_jobs from VELOSTREAM_MAX_JOBS
    // - monitoring from VELOSTREAM_ENABLE_MONITORING
    // - etc.

    Ok(())
}
```

### Example 3: Kubernetes Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velostream-server
spec:
  containers:
  - name: velostream
    image: velostream:latest
    env:
    - name: VELOSTREAM_KAFKA_BROKERS
      valueFrom:
        configMapKeyRef:
          name: velostream-config
          key: kafka-brokers
    - name: VELOSTREAM_MAX_JOBS
      valueFrom:
        configMapKeyRef:
          name: velostream-config
          key: max-jobs
    - name: VELOSTREAM_ENABLE_MONITORING
      valueFrom:
        configMapKeyRef:
          name: velostream-config
          key: enable-monitoring
```

### Example 4: Testing with Custom Brokers

```rust
#[tokio::test]
async fn test_stream_job_creation() {
    // Create config for test environment
    let config = StreamJobServerConfig::new("test-kafka:9092", "test-group")
        .with_max_jobs(5);

    let server = StreamJobServer::with_config(config);

    // Test job deployment
    let result = server.deploy_job(
        "test-job".to_string(),
        "1.0".to_string(),
        "SELECT COUNT(*) FROM test_topic".to_string(),
        "test_topic".to_string(),
    ).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_monitoring_enabled() {
    // Test with monitoring explicitly enabled
    let config = StreamJobServerConfig::new("test-kafka:9092", "test-group")
        .with_monitoring(true);

    let server = StreamJobServer::with_config_and_monitoring(config).await;

    // Verify monitoring is enabled
    assert!(server.has_performance_monitoring());
}
```

## Configuration Summary Logging

The `summary()` method provides formatted output for logging:

```rust
let config = StreamJobServerConfig::new("localhost:9092", "my-app")
    .with_max_jobs(50)
    .with_monitoring(true);

println!("{}", config.summary());
// Output:
// StreamJobServer Configuration: brokers=localhost:9092, group_id=my-app, max_jobs=50, monitoring=true, timeout=86400s, cache_size=100
```

## Best Practices

### 1. Development
- Use defaults or explicit parameters with `new()`
- Keep code as-is, don't change for development environments
- Example: `StreamJobServer::new("localhost:9092", "dev-group", 10)`

### 2. Testing
- Use configuration objects with builder pattern
- Create separate configs for different test scenarios
- Document custom broker addresses in test comments
- Example:
  ```rust
  let config = StreamJobServerConfig::new("test-broker:9092", "test-group")
      .with_max_jobs(5);
  ```

### 3. Staging/Production
- Use environment variables exclusively
- Document required environment variables in README
- Use Docker/Kubernetes ConfigMaps for configuration
- Example: `StreamJobServerConfig::from_env("prod-group")`

### 4. Monitoring
- Enable monitoring in production: `VELOSTREAM_ENABLE_MONITORING=true`
- Use appropriate timeouts: `VELOSTREAM_JOB_TIMEOUT_SECS=172800` for long jobs
- Configure cache size based on workload: `VELOSTREAM_TABLE_CACHE_SIZE=500`

### 5. Documentation
- Document all environment variables your deployment uses
- Provide example configuration files
- Document default values and ranges
- Explain when to override defaults

## Migration from Hardcoded Values

If you have hardcoded broker addresses, migrate using this pattern:

### Before (Hardcoded)
```rust
let server = StreamJobServer::new("localhost:9092".to_string(), "group", 10);
```

### After (Configurable)
```rust
// Option 1: Direct parameter (backward compatible)
let brokers = std::env::var("KAFKA_BROKERS")
    .unwrap_or_else(|_| "localhost:9092".to_string());
let server = StreamJobServer::new(brokers, "group", 10);

// Option 2: Use from_env() (recommended)
let config = StreamJobServerConfig::from_env("group");
let server = StreamJobServer::with_config(config);
```

## Troubleshooting

### Issue: Environment variables not being read

**Solution**: Ensure variables are exported before running:
```bash
export VELOSTREAM_KAFKA_BROKERS="broker:9092"
cargo run
```

### Issue: Invalid configuration values

**Solution**: Check format in environment:
- Brokers: comma-separated, e.g., `broker1:9092,broker2:9092`
- Max jobs: integer, e.g., `500`
- Monitoring: boolean, e.g., `true` or `1`
- Timeout: integer seconds, e.g., `172800`

### Issue: Tests failing with wrong broker

**Solution**: Use explicit configuration objects:
```rust
let config = StreamJobServerConfig::new("correct-test-broker:9092", "test-group");
let server = StreamJobServer::with_config(config);
```

## Summary

The layered configuration system provides:

- ✅ **Backward compatible**: Existing code works without changes
- ✅ **Flexible**: Supports environment variables, builders, and direct parameters
- ✅ **Testable**: Easy to create custom configurations for tests
- ✅ **Production ready**: Environment-driven configuration for deployment
- ✅ **Well-documented**: Clear documentation for each method and use case

Choose the pattern that best fits your use case:
- **Development**: Pattern 1 (direct parameters)
- **Testing**: Pattern 3 (builder pattern)
- **Production**: Pattern 2 (environment variables)
