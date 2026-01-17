# Test Harness Internals

Deep dive into the implementation details of the test harness.

## Module Dependencies

```
                    ┌──────────┐
                    │  cli.rs  │
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
              ▼          ▼          ▼
        ┌─────────┐ ┌─────────┐ ┌─────────┐
        │spec.rs  │ │stress.rs│ │ai.rs    │
        └────┬────┘ └────┬────┘ └────┬────┘
             │           │           │
             └─────┬─────┴─────┬─────┘
                   │           │
                   ▼           ▼
             ┌──────────┐ ┌──────────┐
             │executor  │ │spec_gen  │
             │.rs       │ │erator.rs │
             └────┬─────┘ └────┬─────┘
                  │            │
         ┌────────┴────────────┴────────┐
         │                               │
         ▼                               ▼
   ┌──────────┐                   ┌──────────┐
   │generator │                   │assertions│
   │.rs       │                   │.rs       │
   └────┬─────┘                   └────┬─────┘
        │                              │
        └──────────┬───────────────────┘
                   │
                   ▼
             ┌──────────┐
             │schema.rs │
             └────┬─────┘
                  │
   ┌──────────────┼──────────────┐
   │              │              │
   ▼              ▼              ▼
┌──────┐   ┌──────────┐   ┌──────────┐
│infra │   │report.rs │   │config_   │
│.rs   │   │          │   │override  │
└──────┘   └──────────┘   │.rs       │
                          └──────────┘
```

## Key Data Structures

### StreamRecord Integration

The test harness uses Velostream's `StreamRecord` for data:

```rust
// From src/velostream/sql/execution/types.rs
pub struct StreamRecord {
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: Option<DateTime<Utc>>,
    pub metadata: Option<RecordMetadata>,
}

pub enum FieldValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Timestamp(NaiveDateTime),
    ScaledInteger(i64, u8),  // For financial precision
    Array(Vec<FieldValue>),
    Map(HashMap<String, FieldValue>),
}
```

### Schema Representation

```rust
// From schema.rs
pub struct Schema {
    pub name: String,
    pub namespace: Option<String>,
    pub description: Option<String>,
    pub fields: Vec<FieldDefinition>,
}

pub struct FieldDefinition {
    pub name: String,
    pub field_type: String,  // "string", "integer", "decimal", etc.
    pub description: Option<String>,
    pub precision: Option<u8>,
    pub scale: Option<u8>,
    pub constraints: FieldConstraints,
}

pub struct FieldConstraints {
    pub enum_values: Option<Vec<String>>,
    pub weights: Option<Vec<f64>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub distribution: Option<String>,
    pub pattern: Option<String>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub range: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub probability: Option<f64>,
    pub derived: Option<String>,
    pub references: Option<ReferenceConstraint>,
}
```

### Test Specification

```rust
// From spec.rs
pub struct TestSpec {
    pub application: String,
    pub description: Option<String>,
    pub default_timeout_ms: u64,
    pub default_records: usize,
    pub config: HashMap<String, String>,
    pub schemas: HashMap<String, PathBuf>,
    pub queries: Vec<QueryTest>,
}

pub struct QueryTest {
    pub name: String,
    pub description: Option<String>,
    pub timeout_ms: Option<u64>,
    pub inputs: Vec<InputConfig>,
    pub assertions: Vec<AssertionConfig>,
}

pub struct InputConfig {
    pub source: String,
    pub records: Option<usize>,
    pub schema: Option<PathBuf>,
    pub from_file: Option<PathBuf>,
    pub from_previous: Option<bool>,
    pub source_type: Option<SourceTypeConfig>,
}

pub struct AssertionConfig {
    pub assertion_type: String,
    pub params: HashMap<String, serde_yaml::Value>,
    pub message: Option<String>,
}
```

## Data Generation Internals

### Random Number Generation

The generator uses a seeded RNG for reproducibility:

```rust
use rand::prelude::*;
use rand::distributions::{Distribution, Uniform, WeightedIndex};

pub struct SchemaDataGenerator {
    rng: StdRng,
    reference_data: HashMap<(String, String), Vec<FieldValue>>,
}

impl SchemaDataGenerator {
    pub fn new(seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };
        Self {
            rng,
            reference_data: HashMap::new(),
        }
    }
}
```

### Distribution Implementation

```rust
fn generate_with_distribution(
    &mut self,
    min: f64,
    max: f64,
    distribution: &str,
) -> f64 {
    match distribution {
        "uniform" => {
            let dist = Uniform::new(min, max);
            dist.sample(&mut self.rng)
        }
        "normal" => {
            // Box-Muller transform for normal distribution
            let mean = (min + max) / 2.0;
            let std_dev = (max - min) / 6.0;  // ~99.7% within range

            loop {
                let u1: f64 = self.rng.gen();
                let u2: f64 = self.rng.gen();

                let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let value = mean + z * std_dev;

                if value >= min && value <= max {
                    return value;
                }
            }
        }
        "log_normal" => {
            // Log-normal distribution (skewed toward min)
            let log_min = (min + 1.0).ln();
            let log_max = (max + 1.0).ln();

            let u: f64 = self.rng.gen();
            let log_value = log_min + u * (log_max - log_min);
            log_value.exp() - 1.0
        }
        _ => {
            // Default to uniform
            let dist = Uniform::new(min, max);
            dist.sample(&mut self.rng)
        }
    }
}
```

### Pattern Generation

```rust
fn generate_from_pattern(&mut self, pattern: &str) -> String {
    let mut result = String::new();
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '[' => {
                // Character class
                let mut class = Vec::new();
                while let Some(&next) = chars.peek() {
                    if next == ']' {
                        chars.next();
                        break;
                    }
                    if next == '-' && !class.is_empty() {
                        chars.next();
                        if let Some(&end) = chars.peek() {
                            let start = *class.last().unwrap();
                            for ch in (start as u8 + 1)..=(end as u8) {
                                class.push(ch as char);
                            }
                            chars.next();
                        }
                    } else {
                        class.push(chars.next().unwrap());
                    }
                }

                // Check for repetition
                let count = self.parse_repetition(&mut chars);
                for _ in 0..count {
                    let idx = self.rng.gen_range(0..class.len());
                    result.push(class[idx]);
                }
            }
            _ => result.push(c),
        }
    }

    result
}

fn parse_repetition(&mut self, chars: &mut Peekable<Chars>) -> usize {
    if chars.peek() == Some(&'{') {
        chars.next();
        let mut num_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                num_str.push(chars.next().unwrap());
            } else {
                break;
            }
        }
        if chars.peek() == Some(&'}') {
            chars.next();
            return num_str.parse().unwrap_or(1);
        }
    }
    1
}
```

## Assertion Evaluation

### Assertion Pipeline

```rust
pub fn run_assertions(
    config: &[AssertionConfig],
    output: &CapturedOutput,
    context: &AssertionContext,
) -> Vec<AssertionResult> {
    config.iter().map(|assertion| {
        let result = Self::evaluate_assertion(assertion, output, context);

        // Enhance with context
        AssertionResult {
            message: assertion.message.clone()
                .unwrap_or(result.message),
            ..result
        }
    }).collect()
}

fn evaluate_assertion(
    assertion: &AssertionConfig,
    output: &CapturedOutput,
    context: &AssertionContext,
) -> AssertionResult {
    match assertion.assertion_type.as_str() {
        "record_count" => Self::check_record_count(assertion, output),
        "schema_contains" => Self::check_schema_contains(assertion, output),
        "no_nulls" => Self::check_no_nulls(assertion, output),
        "field_values" => Self::check_field_values(assertion, output),
        "field_in_set" => Self::check_field_in_set(assertion, output),
        "aggregate_check" => Self::check_aggregate(assertion, output),
        "join_coverage" => Self::check_join_coverage(assertion, output, context),
        "execution_time" => Self::check_execution_time(assertion, output),
        "memory_usage" => Self::check_memory_usage(assertion, output),
        "file_exists" => Self::check_file_exists(assertion, context),
        "file_row_count" => Self::check_file_row_count(assertion, context),
        "file_contains" => Self::check_file_contains(assertion, context),
        "file_matches" => Self::check_file_matches(assertion, context),
        _ => AssertionResult::error(&format!(
            "Unknown assertion type: {}",
            assertion.assertion_type
        )),
    }
}
```

### Operator Evaluation

```rust
fn evaluate_comparison<T: PartialOrd + std::fmt::Display>(
    actual: T,
    operator: &str,
    expected: T,
    min: Option<T>,
    max: Option<T>,
) -> (bool, String) {
    let passed = match operator {
        "equals" | "eq" => actual == expected,
        "not_equals" | "ne" => actual != expected,
        "greater_than" | "gt" => actual > expected,
        "less_than" | "lt" => actual < expected,
        "greater_than_or_equal" | "gte" => actual >= expected,
        "less_than_or_equal" | "lte" => actual <= expected,
        "between" => {
            if let (Some(min), Some(max)) = (min, max) {
                actual >= min && actual <= max
            } else {
                false
            }
        }
        _ => false,
    };

    let message = if passed {
        format!("{} {} {} - PASSED", actual, operator, expected)
    } else {
        format!("{} {} {} - FAILED", actual, operator, expected)
    };

    (passed, message)
}
```

## Infrastructure Management

### Kafka Topic Lifecycle

```rust
impl TestHarnessInfra {
    pub async fn create_topic(&mut self, name: &str) -> Result<(), InfraError> {
        let admin = self.admin_client.as_ref()
            .ok_or(InfraError::NotStarted)?;

        let topic = NewTopic::new(name, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "delete")
            .set("retention.ms", "3600000");  // 1 hour

        admin.create_topics(&[topic], &AdminOptions::new())
            .await
            .map_err(|e| InfraError::TopicCreation(e.to_string()))?;

        self.topics.insert(name.to_string());
        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<(), InfraError> {
        if let Some(admin) = &self.admin_client {
            let topics: Vec<&str> = self.topics.iter()
                .map(|s| s.as_str())
                .collect();

            if !topics.is_empty() {
                admin.delete_topics(&topics, &AdminOptions::new())
                    .await
                    .map_err(|e| InfraError::TopicDeletion(e.to_string()))?;
            }
        }

        self.topics.clear();

        // Clean up temp directory
        if let Some(temp_dir) = self.temp_dir.take() {
            // TempDir drops automatically
        }

        Ok(())
    }
}
```

### Producer/Consumer Creation

```rust
impl TestHarnessInfra {
    pub fn create_producer(&self) -> Result<FutureProducer, InfraError> {
        let config: ClientConfig = ClientConfig::new()
            .set("bootstrap.servers", &self.kafka_bootstrap)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .clone();

        config.create()
            .map_err(|e| InfraError::ProducerCreation(e.to_string()))
    }

    pub fn create_consumer(&self, group_id: &str) -> Result<StreamConsumer, InfraError> {
        let config: ClientConfig = ClientConfig::new()
            .set("bootstrap.servers", &self.kafka_bootstrap)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .clone();

        config.create()
            .map_err(|e| InfraError::ConsumerCreation(e.to_string()))
    }
}
```

## Report Generation

### Text Report Formatting

```rust
fn write_text_report(report: &TestReport, writer: &mut dyn Write) -> io::Result<()> {
    // Header
    writeln!(writer, "\n  Velostream SQL Test Report")?;
    writeln!(writer, "")?;
    writeln!(writer, "Application: {}", report.application)?;
    writeln!(writer, "Run ID: {}", report.run_id)?;
    writeln!(writer, "Duration: {}ms", report.duration_ms)?;
    writeln!(writer)?;

    // Summary
    writeln!(writer, " Summary")?;
    writeln!(writer, "")?;
    writeln!(
        writer,
        "Queries: {} total, {} passed, {} failed, {} errors, {} skipped",
        report.summary.total,
        report.summary.passed,
        report.summary.failed,
        report.summary.errors,
        report.summary.skipped
    )?;

    // Per-query results
    for query in &report.queries {
        let icon = match query.status {
            QueryStatus::Passed => "",
            QueryStatus::Failed => "",
            QueryStatus::Error => "",
            QueryStatus::Skipped => "",
        };

        writeln!(
            writer,
            "\n{} {} ({}ms)",
            icon, query.name, query.duration_ms
        )?;

        for assertion in &query.assertions {
            let check = if assertion.passed { "" } else { "" };
            writeln!(writer, "   {} {}", check, assertion.message)?;
        }
    }

    Ok(())
}
```

## Memory Tracking

### Platform-Specific Implementation

```rust
// From stress.rs
pub struct MemoryTracker {
    #[cfg(target_os = "macos")]
    baseline: Option<mach_task_info::TaskInfo>,
    #[cfg(target_os = "linux")]
    baseline: Option<ProcMemInfo>,
    enabled: bool,
}

impl MemoryTracker {
    pub fn new(enabled: bool) -> Self {
        Self {
            baseline: None,
            enabled,
        }
    }

    pub fn capture_baseline(&mut self) {
        if self.enabled {
            self.baseline = Self::get_current_memory();
        }
    }

    pub fn get_growth(&self) -> Option<i64> {
        if !self.enabled {
            return None;
        }

        let current = Self::get_current_memory()?;
        let baseline = self.baseline.as_ref()?;

        Some(current.resident_size as i64 - baseline.resident_size as i64)
    }

    #[cfg(target_os = "macos")]
    fn get_current_memory() -> Option<mach_task_info::TaskInfo> {
        mach_task_info::task_info().ok()
    }

    #[cfg(target_os = "linux")]
    fn get_current_memory() -> Option<ProcMemInfo> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        // Parse VmRSS from status
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let kb: u64 = line.split_whitespace()
                    .nth(1)?
                    .parse().ok()?;
                return Some(ProcMemInfo { resident_size: kb * 1024 });
            }
        }
        None
    }
}
```

## Error Handling

### Error Hierarchy

```rust
#[derive(Debug, thiserror::Error)]
pub enum TestHarnessError {
    #[error("Configuration error: {message}")]
    Config {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Schema error: {message}")]
    Schema {
        message: String,
        field: Option<String>,
    },

    #[error("Generation error: {message}")]
    Generation {
        message: String,
        schema: Option<String>,
    },

    #[error("Infrastructure error: {message}")]
    Infrastructure {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Execution error: {message}")]
    Execution {
        message: String,
        query: Option<String>,
    },

    #[error("Assertion error: {message}")]
    Assertion {
        message: String,
        assertion_type: Option<String>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}
```

### Error Context

```rust
pub trait ErrorContext<T> {
    fn with_context<F, S>(self, f: F) -> Result<T, TestHarnessError>
    where
        F: FnOnce() -> S,
        S: Into<String>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> ErrorContext<T> for Result<T, E> {
    fn with_context<F, S>(self, f: F) -> Result<T, TestHarnessError>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        self.map_err(|e| TestHarnessError::Config {
            message: f().into(),
            source: Some(Box::new(e)),
        })
    }
}
```

## Utility Functions

### Field Value Conversion

```rust
// From utils.rs
pub fn field_value_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Null => "null".to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::String(s) => s.clone(),
        FieldValue::Timestamp(t) => t.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
        FieldValue::ScaledInteger(value, scale) => {
            format_scaled_integer(*value, *scale)
        }
        FieldValue::Array(arr) => {
            let items: Vec<_> = arr.iter()
                .map(field_value_to_string)
                .collect();
            format!("[{}]", items.join(", "))
        }
        FieldValue::Map(map) => {
            let items: Vec<_> = map.iter()
                .map(|(k, v)| format!("{}: {}", k, field_value_to_string(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

pub fn format_scaled_integer(value: i64, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }

    let divisor = 10_i64.pow(scale as u32);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).abs();

    let formatted = format!(
        "{}.{:0>width$}",
        integer_part,
        fractional_part,
        width = scale as usize
    );

    // Remove trailing zeros
    formatted.trim_end_matches('0').trim_end_matches('.').to_string()
}
```

### Path Resolution

```rust
pub fn resolve_path(path: &Path, base_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

pub fn get_base_dir(spec_path: &Path) -> PathBuf {
    spec_path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}
```
