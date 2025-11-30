# Test Harness Architecture

This document describes the architecture and design of the SQL Application Test Harness.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLI Layer                                       │
│  velo-test validate | run | init | infer-schema | stress                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Orchestration Layer                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌─────────────────┐    │
│  │ TestSpec    │  │ ConfigOver-  │  │ Report     │  │ AI Assistant    │    │
│  │ Parser      │  │ rides        │  │ Generator  │  │                 │    │
│  └─────────────┘  └──────────────┘  └────────────┘  └─────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Execution Layer                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌─────────────────┐    │
│  │ Query       │  │ Schema Data  │  │ Assertion  │  │ Stress Runner   │    │
│  │ Executor    │  │ Generator    │  │ Runner     │  │                 │    │
│  └─────────────┘  └──────────────┘  └────────────┘  └─────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Infrastructure Layer                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌─────────────────┐    │
│  │ Kafka       │  │ Schema       │  │ File I/O   │  │ DLQ Capture     │    │
│  │ Infra       │  │ Registry     │  │            │  │                 │    │
│  └─────────────┘  └──────────────┘  └────────────┘  └─────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Core Velostream Engine                               │
│  StreamJobServer | SQL Parser | Execution Engine | DataSource/DataSink      │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module Responsibilities

### CLI Layer (`cli.rs`, `src/bin/velo-test.rs`)

**Purpose:** Parse command-line arguments and dispatch to appropriate handlers.

```rust
pub struct CliArgs {
    pub command: Command,
    pub verbose: bool,
}

pub enum Command {
    Validate { sql_file: PathBuf },
    Run { sql_file: PathBuf, spec: PathBuf, ... },
    Init { sql_file: PathBuf, output: PathBuf, ... },
    InferSchema { sql_file: PathBuf, ... },
    Stress { sql_file: PathBuf, ... },
}
```

### Test Specification (`spec.rs`)

**Purpose:** Define and parse test specification structures.

```rust
pub struct TestSpec {
    pub application: String,
    pub description: Option<String>,
    pub default_timeout_ms: u64,
    pub default_records: usize,
    pub schemas: HashMap<String, PathBuf>,
    pub queries: Vec<QueryTest>,
}

pub struct QueryTest {
    pub name: String,
    pub inputs: Vec<InputConfig>,
    pub assertions: Vec<AssertionConfig>,
    pub timeout_ms: Option<u64>,
}

pub struct AssertionConfig {
    pub assertion_type: String,
    pub params: HashMap<String, serde_yaml::Value>,
}
```

### Query Executor (`executor.rs`)

**Purpose:** Execute SQL queries and capture output.

```rust
pub struct QueryExecutor {
    infra: TestHarnessInfra,
    server: Option<Arc<StreamJobServer>>,
}

impl QueryExecutor {
    pub async fn execute_query(
        &mut self,
        sql: &str,
        inputs: &[InputConfig],
        timeout_ms: u64,
    ) -> Result<ExecutionResult, TestHarnessError>;
}

pub struct ExecutionResult {
    pub query_name: String,
    pub success: bool,
    pub outputs: Vec<CapturedOutput>,
    pub execution_time_ms: u64,
    pub error: Option<String>,
}
```

### Data Generator (`generator.rs`)

**Purpose:** Generate test data from schema definitions.

```rust
pub struct SchemaDataGenerator {
    rng: StdRng,
    reference_data: HashMap<(String, String), Vec<FieldValue>>,
}

impl SchemaDataGenerator {
    pub fn generate(
        &mut self,
        schema: &Schema,
        count: usize,
    ) -> Result<Vec<StreamRecord>, GeneratorError>;

    pub fn load_reference_data(
        &mut self,
        table: &str,
        field: &str,
        values: Vec<FieldValue>,
    );
}
```

### Assertions (`assertions.rs`)

**Purpose:** Validate captured output against assertions.

```rust
pub struct AssertionRunner;

impl AssertionRunner {
    pub fn run_assertions(
        config: &AssertionConfig,
        output: &CapturedOutput,
    ) -> Vec<AssertionResult>;
}

pub struct AssertionResult {
    pub assertion_type: String,
    pub passed: bool,
    pub message: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
}
```

### Report Generator (`report.rs`)

**Purpose:** Generate test reports in multiple formats.

```rust
pub struct ReportGenerator {
    application: String,
    run_id: String,
    queries: Vec<QueryReport>,
}

impl ReportGenerator {
    pub fn add_query_result(
        &mut self,
        execution: &ExecutionResult,
        assertions: &[AssertionResult],
    );

    pub fn generate(&self) -> TestReport;
}

pub fn write_report(
    report: &TestReport,
    format: OutputFormat,
    writer: &mut dyn Write,
) -> io::Result<()>;
```

### Infrastructure (`infra.rs`)

**Purpose:** Manage test infrastructure (Kafka, topics, etc.).

```rust
pub struct TestHarnessInfra {
    kafka_bootstrap: String,
    admin_client: Option<AdminClient<DefaultClientContext>>,
    topics: HashSet<String>,
    temp_dir: Option<TempDir>,
}

impl TestHarnessInfra {
    pub fn with_kafka(bootstrap_servers: &str) -> Self;
    pub async fn start(&mut self) -> Result<(), InfraError>;
    pub async fn create_topic(&mut self, name: &str) -> Result<(), InfraError>;
    pub async fn delete_topic(&mut self, name: &str) -> Result<(), InfraError>;
    pub async fn stop(&mut self) -> Result<(), InfraError>;
}
```

## Data Flow

### Test Execution Flow

```
1. CLI parses arguments
   ↓
2. TestSpec loaded from YAML
   ↓
3. ConfigOverrides applied (topic prefixes, bootstrap servers)
   ↓
4. For each QueryTest:
   │
   ├── 4a. Generate input data from schemas
   │        SchemaDataGenerator → Vec<StreamRecord>
   │
   ├── 4b. Publish input to Kafka topics
   │        TestHarnessInfra.create_topic()
   │        Producer.send()
   │
   ├── 4c. Execute SQL query
   │        StreamJobServer.deploy_job()
   │        Wait for completion
   │
   ├── 4d. Capture output records
   │        Consumer.subscribe()
   │        Consumer.poll() → CapturedOutput
   │
   └── 4e. Run assertions
            AssertionRunner.run_assertions() → Vec<AssertionResult>
   ↓
5. Generate report
   ReportGenerator → TestReport
   ↓
6. Output results (text/json/junit)
```

### Data Generation Flow

```
Schema YAML
    ↓
┌─────────────────┐
│ Schema Parser   │
│ (schema.rs)     │
└────────┬────────┘
         ↓
┌─────────────────┐     ┌──────────────────┐
│ SchemaData-     │────▶│ Reference Data   │
│ Generator       │     │ (foreign keys)   │
└────────┬────────┘     └──────────────────┘
         ↓
┌─────────────────┐
│ Constraint      │
│ Evaluation      │
│ - enum          │
│ - range         │
│ - pattern       │
│ - distribution  │
│ - derived       │
└────────┬────────┘
         ↓
Vec<StreamRecord>
```

## Extension Points

### Adding New Assertion Types

1. Add variant to `AssertionType` enum in `spec.rs`
2. Implement validation in `assertions.rs`
3. Add parsing in `AssertionConfig::from_yaml()`
4. Document in user guide

### Adding New Data Types

1. Add constraint handling in `generator.rs`
2. Update schema parser in `schema.rs`
3. Add type conversion in `utils.rs`

### Adding New Infrastructure

1. Implement infrastructure trait in `infra.rs`
2. Add configuration in `spec.rs`
3. Integrate with `QueryExecutor`

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum TestHarnessError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Infrastructure error: {0}")]
    Infrastructure(#[from] InfraError),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Assertion error: {0}")]
    Assertion(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

## Threading Model

- **Main thread:** CLI, orchestration, report generation
- **Async runtime:** Kafka operations, query execution
- **StreamJobServer:** Internal thread pool for processing

```rust
#[tokio::main]
async fn main() {
    let args = parse_args();

    match args.command {
        Command::Run { .. } => {
            // Async execution
            run_tests().await?;
        }
        Command::Validate { .. } => {
            // Sync validation
            validate_sql()?;
        }
    }
}
```

## Configuration Precedence

```
1. CLI arguments (highest priority)
2. Environment variables
3. Test spec file
4. Default values (lowest priority)
```

```rust
impl ConfigOverrides {
    pub fn apply(&self, config: &mut HashMap<String, String>) {
        // CLI overrides
        if let Some(bootstrap) = &self.bootstrap_servers {
            config.insert("bootstrap.servers".into(), bootstrap.clone());
        }

        // Topic prefix
        if let Some(prefix) = &self.topic_prefix {
            for (key, value) in config.iter_mut() {
                if key.ends_with(".topic") {
                    *value = format!("{}{}", prefix, value);
                }
            }
        }
    }
}
```

## Performance Considerations

1. **Record Generation:** Uses `rand` with seedable RNG for reproducibility
2. **Kafka Operations:** Batched writes, async polling
3. **Memory:** Records held in memory during execution; use streaming for large tests
4. **Assertions:** Lazy evaluation where possible

## Testing the Test Harness

```bash
# Unit tests
cargo test --lib test_harness

# Integration tests (requires Docker)
cargo test --tests integration::test_harness_integration_test

# Specific module tests
cargo test --lib test_harness::generator
cargo test --lib test_harness::assertions
```
