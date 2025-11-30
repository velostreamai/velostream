# Extending the Test Harness

This guide explains how to extend the test harness with new features.

## Adding New Assertion Types

### Step 1: Define the Assertion Type

Add to `spec.rs`:

```rust
// In AssertionType enum (or create if not exists)
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssertionType {
    RecordCount { ... },
    SchemaContains { ... },
    // Add your new type
    MyCustomAssertion {
        field: String,
        expected_pattern: String,
        #[serde(default)]
        case_sensitive: bool,
    },
}
```

### Step 2: Implement the Assertion Logic

Add to `assertions.rs`:

```rust
impl AssertionRunner {
    pub fn run_assertion(
        assertion: &AssertionConfig,
        output: &CapturedOutput,
    ) -> AssertionResult {
        match assertion.assertion_type.as_str() {
            "record_count" => Self::check_record_count(assertion, output),
            "schema_contains" => Self::check_schema_contains(assertion, output),
            // Add your handler
            "my_custom_assertion" => Self::check_my_custom(assertion, output),
            _ => AssertionResult::error(&format!(
                "Unknown assertion type: {}",
                assertion.assertion_type
            )),
        }
    }

    fn check_my_custom(
        assertion: &AssertionConfig,
        output: &CapturedOutput,
    ) -> AssertionResult {
        let field = assertion.params.get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let expected_pattern = assertion.params.get("expected_pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let case_sensitive = assertion.params.get("case_sensitive")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let regex = if case_sensitive {
            Regex::new(expected_pattern)
        } else {
            Regex::new(&format!("(?i){}", expected_pattern))
        };

        let regex = match regex {
            Ok(r) => r,
            Err(e) => return AssertionResult::error(&format!("Invalid pattern: {}", e)),
        };

        let all_match = output.records.iter().all(|record| {
            record.get(field)
                .map(|v| regex.is_match(&field_value_to_string(v)))
                .unwrap_or(false)
        });

        AssertionResult {
            assertion_type: "my_custom_assertion".to_string(),
            passed: all_match,
            message: if all_match {
                format!("All {} values match pattern", field)
            } else {
                format!("Some {} values do not match pattern", field)
            },
            expected: Some(expected_pattern.to_string()),
            actual: Some(format!("{} records checked", output.records.len())),
        }
    }
}
```

### Step 3: Add Tests

Add to `tests/unit/test_harness/assertions_test.rs`:

```rust
#[test]
fn test_my_custom_assertion_passes() {
    let output = create_test_output(vec![
        create_record([("email", "user@example.com")]),
        create_record([("email", "admin@test.org")]),
    ]);

    let assertion = AssertionConfig {
        assertion_type: "my_custom_assertion".to_string(),
        params: hashmap! {
            "field" => "email",
            "expected_pattern" => r"^[\w.]+@[\w.]+\.\w+$",
        },
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(result.passed);
}
```

### Step 4: Document

Add to `docs/test-harness/ASSERTIONS.md`:

```markdown
### my_custom_assertion

Validates field values match a regex pattern.

```yaml
- type: my_custom_assertion
  field: email
  expected_pattern: "^[\\w.]+@[\\w.]+\\.\\w+$"
  case_sensitive: false
```

**Parameters:**
- `field` - Field name to validate
- `expected_pattern` - Regex pattern to match
- `case_sensitive` - Whether matching is case-sensitive (default: true)
```

## Adding New Data Generator Constraints

### Step 1: Define the Constraint

Update `schema.rs`:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct FieldConstraints {
    // Existing constraints
    pub enum_values: Option<Vec<String>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    // Add new constraint
    pub custom_generator: Option<CustomGeneratorConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CustomGeneratorConfig {
    pub generator_type: String,
    pub params: HashMap<String, serde_yaml::Value>,
}
```

### Step 2: Implement Generation Logic

Update `generator.rs`:

```rust
impl SchemaDataGenerator {
    fn generate_field_value(
        &mut self,
        field: &FieldDefinition,
    ) -> Result<FieldValue, GeneratorError> {
        // Check for custom generator first
        if let Some(custom) = &field.constraints.custom_generator {
            return self.generate_custom(custom, field);
        }

        // Existing generation logic...
        match field.field_type.as_str() {
            "string" => self.generate_string(field),
            "integer" => self.generate_integer(field),
            // ...
        }
    }

    fn generate_custom(
        &mut self,
        config: &CustomGeneratorConfig,
        field: &FieldDefinition,
    ) -> Result<FieldValue, GeneratorError> {
        match config.generator_type.as_str() {
            "uuid" => Ok(FieldValue::String(uuid::Uuid::new_v4().to_string())),
            "sequence" => {
                let start = config.params.get("start")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(1);
                // Implementation...
            }
            _ => Err(GeneratorError::UnknownGenerator(
                config.generator_type.clone()
            )),
        }
    }
}
```

### Step 3: Add Tests

```rust
#[test]
fn test_custom_uuid_generator() {
    let schema = parse_schema(r#"
        fields:
          - name: id
            type: string
            constraints:
              custom_generator:
                generator_type: uuid
    "#).unwrap();

    let mut gen = SchemaDataGenerator::new(Some(42));
    let records = gen.generate(&schema, 10).unwrap();

    for record in records {
        let id = record.get("id").unwrap();
        assert!(uuid::Uuid::parse_str(&field_value_to_string(id)).is_ok());
    }
}
```

## Adding New Infrastructure Types

### Step 1: Define the Infrastructure

Create `src/velostream/test_harness/infra_redis.rs`:

```rust
use async_trait::async_trait;

pub struct RedisInfra {
    client: Option<redis::Client>,
    url: String,
}

impl RedisInfra {
    pub fn new(url: &str) -> Self {
        Self {
            client: None,
            url: url.to_string(),
        }
    }

    pub async fn start(&mut self) -> Result<(), InfraError> {
        self.client = Some(redis::Client::open(&self.url)?);
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), InfraError> {
        self.client = None;
        Ok(())
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), InfraError> {
        let mut conn = self.client.as_ref()
            .ok_or(InfraError::NotStarted)?
            .get_async_connection()
            .await?;
        redis::cmd("SET").arg(key).arg(value).query_async(&mut conn).await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, InfraError> {
        let mut conn = self.client.as_ref()
            .ok_or(InfraError::NotStarted)?
            .get_async_connection()
            .await?;
        Ok(redis::cmd("GET").arg(key).query_async(&mut conn).await?)
    }
}
```

### Step 2: Integrate with Test Harness

Update `infra.rs`:

```rust
pub struct TestHarnessInfra {
    kafka: Option<KafkaInfra>,
    redis: Option<RedisInfra>,
    // ...
}

impl TestHarnessInfra {
    pub fn with_redis(url: &str) -> Self {
        Self {
            kafka: None,
            redis: Some(RedisInfra::new(url)),
            // ...
        }
    }

    pub fn with_kafka_and_redis(kafka: &str, redis: &str) -> Self {
        Self {
            kafka: Some(KafkaInfra::new(kafka)),
            redis: Some(RedisInfra::new(redis)),
            // ...
        }
    }
}
```

### Step 3: Add Configuration Support

Update `spec.rs`:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct InfraConfig {
    #[serde(default)]
    pub kafka: Option<KafkaConfig>,
    #[serde(default)]
    pub redis: Option<RedisConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    #[serde(default = "default_redis_timeout")]
    pub timeout_ms: u64,
}
```

## Adding New Output Formats

### Step 1: Define the Format

Update `report.rs`:

```rust
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Text,
    Json,
    Junit,
    // Add new format
    Markdown,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            "junit" | "xml" => Ok(Self::Junit),
            "markdown" | "md" => Ok(Self::Markdown),
            _ => Err(format!("Unknown format: {}", s)),
        }
    }
}
```

### Step 2: Implement the Writer

```rust
pub fn write_report(
    report: &TestReport,
    format: OutputFormat,
    writer: &mut dyn Write,
) -> io::Result<()> {
    match format {
        OutputFormat::Text => write_text_report(report, writer),
        OutputFormat::Json => write_json_report(report, writer),
        OutputFormat::Junit => write_junit_report(report, writer),
        OutputFormat::Markdown => write_markdown_report(report, writer),
    }
}

fn write_markdown_report(report: &TestReport, writer: &mut dyn Write) -> io::Result<()> {
    writeln!(writer, "# Test Report: {}", report.application)?;
    writeln!(writer)?;
    writeln!(writer, "**Run ID:** {}", report.run_id)?;
    writeln!(writer, "**Duration:** {}ms", report.duration_ms)?;
    writeln!(writer)?;

    writeln!(writer, "## Summary")?;
    writeln!(writer)?;
    writeln!(writer, "| Metric | Value |")?;
    writeln!(writer, "|--------|-------|")?;
    writeln!(writer, "| Total | {} |", report.summary.total)?;
    writeln!(writer, "| Passed | {} |", report.summary.passed)?;
    writeln!(writer, "| Failed | {} |", report.summary.failed)?;
    writeln!(writer)?;

    writeln!(writer, "## Query Results")?;
    writeln!(writer)?;

    for query in &report.queries {
        let status = match query.status {
            QueryStatus::Passed => "PASSED",
            QueryStatus::Failed => "FAILED",
            QueryStatus::Error => "ERROR",
            QueryStatus::Skipped => "SKIPPED",
        };
        writeln!(writer, "### {} - {}", query.name, status)?;
        writeln!(writer)?;
        writeln!(writer, "- **Duration:** {}ms", query.duration_ms)?;

        if !query.assertions.is_empty() {
            writeln!(writer)?;
            writeln!(writer, "**Assertions:**")?;
            for assertion in &query.assertions {
                let icon = if assertion.passed { "" } else { "" };
                writeln!(writer, "- {} {}", icon, assertion.message)?;
            }
        }
        writeln!(writer)?;
    }

    Ok(())
}
```

## Adding New CLI Commands

### Step 1: Define the Command

Update `cli.rs`:

```rust
#[derive(Debug, Subcommand)]
pub enum Command {
    // Existing commands...

    /// Generate a test coverage report
    Coverage {
        /// SQL file to analyze
        #[arg(required = true)]
        sql_file: PathBuf,

        /// Test spec file
        #[arg(short, long)]
        spec: PathBuf,

        /// Output format
        #[arg(short, long, default_value = "text")]
        output: String,
    },
}
```

### Step 2: Implement the Handler

Update `src/bin/velo-test.rs`:

```rust
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    match args.command {
        Command::Run { .. } => run_tests(args).await,
        Command::Validate { .. } => validate_sql(args),
        // Add handler
        Command::Coverage { sql_file, spec, output } => {
            generate_coverage_report(&sql_file, &spec, &output).await
        }
    }
}

async fn generate_coverage_report(
    sql_file: &Path,
    spec: &Path,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse SQL
    let sql = std::fs::read_to_string(sql_file)?;
    let queries = parse_sql(&sql)?;

    // Parse spec
    let spec_content = std::fs::read_to_string(spec)?;
    let test_spec: TestSpec = serde_yaml::from_str(&spec_content)?;

    // Calculate coverage
    let coverage = calculate_coverage(&queries, &test_spec);

    // Output
    match output {
        "json" => println!("{}", serde_json::to_string_pretty(&coverage)?),
        _ => print_coverage_text(&coverage),
    }

    Ok(())
}
```

## Best Practices for Extensions

1. **Follow existing patterns** - Match the style of existing code
2. **Add comprehensive tests** - Unit tests for all new functionality
3. **Update documentation** - Both user docs and developer docs
4. **Handle errors gracefully** - Use proper error types
5. **Make it configurable** - Allow customization via spec files
6. **Keep it backwards compatible** - Don't break existing specs
