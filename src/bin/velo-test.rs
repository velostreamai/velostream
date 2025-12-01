//! Velostream SQL Application Test Harness
//!
//! CLI tool for testing SQL applications with schema-driven data generation,
//! testcontainers-based execution, and comprehensive assertions.
//!
//! Usage:
//!   velo-test run app.sql --spec test_spec.yaml
//!   velo-test validate app.sql
//!   velo-test init app.sql --output test_spec.yaml
//!   velo-test infer-schema app.sql --data-dir data/ --output schemas/

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "velo-test")]
#[command(about = "Velostream SQL Application Test Harness")]
#[command(version = "0.1.0")]
#[command(author = "Velostream Team")]
struct Cli {
    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run tests for a SQL application
    Run {
        /// Path to the SQL file to test
        sql_file: PathBuf,

        /// Path to the test specification YAML file
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Directory containing schema definitions
        #[arg(long)]
        schemas: Option<PathBuf>,

        /// Run only a specific query by name
        #[arg(short, long)]
        query: Option<String>,

        /// Output format: text, json, junit
        #[arg(short, long, default_value = "text")]
        output: String,

        /// Timeout per query in milliseconds
        #[arg(long, default_value = "30000")]
        timeout_ms: u64,

        /// Use AI for failure analysis
        #[arg(long)]
        ai: bool,

        /// Use exact topic names from SQL config (no test prefix)
        /// Use this when testing against a running SQL job with fixed topic names
        #[arg(long)]
        no_topic_prefix: bool,
    },

    /// Validate SQL syntax without execution
    Validate {
        /// Path to the SQL file to validate
        sql_file: PathBuf,

        /// Show verbose validation output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Generate a test specification template from SQL file
    Init {
        /// Path to the SQL file to analyze
        sql_file: PathBuf,

        /// Output path for generated test_spec.yaml
        #[arg(short, long)]
        output: PathBuf,

        /// Use AI for intelligent assertion generation
        #[arg(long)]
        ai: bool,
    },

    /// Infer schemas from SQL and data files
    InferSchema {
        /// Path to the SQL file to analyze
        sql_file: PathBuf,

        /// Directory containing CSV/JSON data files
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output directory for generated schemas
        #[arg(short, long)]
        output: PathBuf,

        /// Use AI for intelligent constraint inference
        #[arg(long)]
        ai: bool,
    },

    /// Run stress tests with high volume
    Stress {
        /// Path to the SQL file to test
        sql_file: PathBuf,

        /// Path to the test specification YAML file
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Number of records to generate per source
        #[arg(long, default_value = "100000")]
        records: usize,

        /// Duration to run stress test in seconds
        #[arg(long, default_value = "60")]
        duration: u64,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        output: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            sql_file,
            spec,
            schemas,
            query,
            output,
            timeout_ms,
            ai,
            no_topic_prefix,
        } => {
            use std::time::Duration;
            use velostream::velostream::test_harness::SpecGenerator;
            use velostream::velostream::test_harness::assertions::AssertionRunner;
            use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
            use velostream::velostream::test_harness::executor::QueryExecutor;
            use velostream::velostream::test_harness::infra::TestHarnessInfra;
            use velostream::velostream::test_harness::report::{
                OutputFormat, ReportGenerator, write_report,
            };
            use velostream::velostream::test_harness::schema::SchemaRegistry;
            use velostream::velostream::test_harness::spec::TestSpec;

            println!("🧪 Velostream SQL Test Harness");
            println!("════════════════════════════════════════");
            println!("SQL File: {}", sql_file.display());
            if let Some(ref s) = spec {
                println!("Test Spec: {}", s.display());
            }
            if let Some(ref s) = schemas {
                println!("Schemas: {}", s.display());
            }
            if let Some(ref q) = query {
                println!("Query Filter: {}", q);
            }
            println!("Output Format: {}", output);
            println!("Timeout: {}ms", timeout_ms);
            if ai {
                println!("AI Analysis: enabled");
            }
            println!();

            // Step 1: Load or generate test spec
            let test_spec: TestSpec = if let Some(ref spec_path) = spec {
                let content = match std::fs::read_to_string(spec_path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("❌ Failed to read test spec: {}", e);
                        std::process::exit(1);
                    }
                };
                match serde_yaml::from_str(&content) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("❌ Failed to parse test spec: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                // Generate from SQL file
                let generator = SpecGenerator::new();
                match generator.generate_from_sql(&sql_file) {
                    Ok(s) => {
                        println!("📝 Auto-generated test spec from SQL");
                        s
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to analyze SQL file: {}", e);
                        std::process::exit(1);
                    }
                }
            };

            println!("📊 Test Configuration:");
            println!("   Application: {}", test_spec.application);
            println!("   Queries: {}", test_spec.queries.len());
            println!();

            // Step 2: Load schemas if provided
            let mut schema_registry = SchemaRegistry::new();
            if let Some(ref schema_dir) = schemas
                && schema_dir.exists()
            {
                println!("📁 Loading schemas from {}", schema_dir.display());
                if let Ok(entries) = std::fs::read_dir(schema_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path
                            .extension()
                            .is_some_and(|ext| ext == "yaml" || ext == "yml")
                        {
                            match std::fs::read_to_string(&path) {
                                Ok(content) => {
                                    match serde_yaml::from_str::<
                                        velostream::velostream::test_harness::Schema,
                                    >(&content)
                                    {
                                        Ok(schema) => {
                                            println!("   Loaded schema: {}", schema.name);
                                            schema_registry.register(schema);
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "   ⚠️  Failed to parse {}: {}",
                                                path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("   ⚠️  Failed to read {}: {}", path.display(), e);
                                }
                            }
                        }
                    }
                }
            }
            println!();

            // Step 3: Validate SQL and extract configuration
            println!("🔧 Initializing test infrastructure...");
            use velostream::velostream::sql::validator::SqlValidator;
            // Use current working directory for config file resolution
            // This allows SQL files to reference configs relative to CWD (e.g., 'configs/source.yaml')
            // rather than relative to the SQL file's directory
            let validator = SqlValidator::new();
            let validation_result = validator.validate_application_file(&sql_file);

            // Extract bootstrap.servers and schema files from the validated sources
            let mut bootstrap_servers: Option<String> = None;
            let sql_dir = sql_file.parent().unwrap_or(std::path::Path::new("."));

            for query_result in &validation_result.query_results {
                for source in &query_result.sources_found {
                    // Extract bootstrap.servers
                    if bootstrap_servers.is_none()
                        && let Some(bs) = source.properties.get("bootstrap.servers")
                    {
                        bootstrap_servers = Some(bs.clone());
                        log::info!(
                            "Found bootstrap.servers from source '{}': {}",
                            source.name,
                            bs
                        );
                    }

                    // Extract and load schema file if specified
                    // Supports both 'datasource.schema.value.schema.file' and shorter variants
                    let schema_file_keys = [
                        "datasource.schema.value.schema.file",
                        "schema.file",
                        "value.schema.file",
                    ];
                    for key in &schema_file_keys {
                        if let Some(schema_path) = source.properties.get(*key) {
                            let full_path = sql_dir.join(schema_path);
                            log::info!(
                                "Loading schema for source '{}' from: {}",
                                source.name,
                                full_path.display()
                            );
                            if full_path.exists() {
                                if let Ok(content) = std::fs::read_to_string(&full_path) {
                                    match serde_yaml::from_str::<
                                        velostream::velostream::test_harness::Schema,
                                    >(&content)
                                    {
                                        Ok(schema) => {
                                            println!(
                                                "   Loaded schema from SQL config: {}",
                                                schema.name
                                            );
                                            schema_registry.register(schema);
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "Failed to parse schema file '{}': {}",
                                                full_path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                            } else {
                                log::warn!(
                                    "Schema file not found: {} (resolved from '{}')",
                                    full_path.display(),
                                    schema_path
                                );
                            }
                            break; // Found a schema file key, stop checking others
                        }
                    }
                }
            }

            // Also check environment variable as fallback
            if bootstrap_servers.is_none()
                && let Ok(bs) = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            {
                bootstrap_servers = Some(bs);
                log::info!("Using bootstrap.servers from KAFKA_BOOTSTRAP_SERVERS env var");
            }

            // Create test infrastructure with Kafka if available
            let infra = if let Some(ref bs) = bootstrap_servers {
                println!("   Kafka: {}", bs);
                TestHarnessInfra::with_kafka(bs)
            } else {
                TestHarnessInfra::new()
            };

            // Check if Kafka is available
            if infra.bootstrap_servers().is_none() {
                eprintln!("⚠️  No Kafka bootstrap servers configured");
                eprintln!(
                    "   Configure 'bootstrap.servers' in config file or set KAFKA_BOOTSTRAP_SERVERS env var"
                );
                eprintln!();
                eprintln!("   Running in validation-only mode (no execution)");
                println!();

                // Use the validation result we already have
                let result = validation_result;

                if result.is_valid {
                    println!(
                        "✅ SQL validation passed ({} queries)",
                        result.valid_queries
                    );
                } else {
                    println!("❌ SQL validation failed");
                    std::process::exit(1);
                }
            } else {
                // Step 4: Create config overrides
                let run_id = format!(
                    "{:08x}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u32
                );
                let mut override_builder = ConfigOverrideBuilder::new(&run_id)
                    .bootstrap_servers(infra.bootstrap_servers().unwrap());

                // By default, use exact topic names from SQL config (no prefix)
                // This ensures the test harness and SQL job use the same topic names
                if !no_topic_prefix {
                    // no_topic_prefix=false means we should NOT add prefixes (default behavior)
                    override_builder = override_builder.no_topic_prefix();
                } else {
                    // no_topic_prefix=true is a legacy flag that now does nothing (already default)
                    println!("   Note: --no-topic-prefix is now the default behavior");
                }

                let overrides = override_builder.build();

                // Step 5: Create executor with StreamJobServer for SQL execution
                let executor = QueryExecutor::new(infra)
                    .with_timeout(Duration::from_millis(timeout_ms))
                    .with_overrides(overrides)
                    .with_schema_registry(schema_registry);

                // Initialize StreamJobServer for actual SQL execution
                // Pass CWD so the server can resolve relative config file paths
                // (configs are relative to the working directory, not the SQL file)
                let cwd = std::env::current_dir().unwrap_or_default();
                let mut executor = match executor.with_server(Some(cwd)).await {
                    Ok(e) => {
                        println!("   SQL execution: enabled (in-process StreamJobServer)");
                        e
                    }
                    Err(e) => {
                        eprintln!("⚠️  Warning: Failed to initialize SQL server: {}", e);
                        eprintln!("   Running in data-only mode (no SQL execution)");
                        // Can't recover the executor after with_server() fails, so recreate
                        QueryExecutor::new(TestHarnessInfra::with_kafka(
                            bootstrap_servers.as_ref().unwrap(),
                        ))
                        .with_timeout(Duration::from_millis(timeout_ms))
                    }
                };

                // Step 5b: Load and parse SQL file to get sink topic info
                if let Err(e) = executor.load_sql_file(&sql_file) {
                    eprintln!("⚠️  Warning: Failed to parse SQL file: {}", e);
                    // Continue anyway - will fall back to default topic naming
                }

                // Step 6: Filter queries if specified
                let queries_to_run: Vec<_> = if let Some(ref query_filter) = query {
                    test_spec
                        .queries
                        .iter()
                        .filter(|q| q.name == *query_filter)
                        .collect()
                } else {
                    test_spec.queries.iter().filter(|q| !q.skip).collect()
                };

                println!();
                println!("🚀 Running {} queries...", queries_to_run.len());
                println!();

                // Step 7: Execute queries and run assertions
                let mut report_gen = ReportGenerator::new(&test_spec.application, &run_id);
                let assertion_runner = AssertionRunner::new();

                for query_test in &queries_to_run {
                    println!("▶️  Executing: {}", query_test.name);

                    // Execute query
                    match executor.execute_query(query_test).await {
                        Ok(exec_result) => {
                            // Run assertions on captured output
                            let mut assertion_results = Vec::new();

                            for output in &exec_result.outputs {
                                let results =
                                    assertion_runner.run_assertions(output, &query_test.assertions);
                                assertion_results.extend(results);
                            }

                            // If no outputs captured, create empty output for assertions
                            if exec_result.outputs.is_empty() && !query_test.assertions.is_empty() {
                                use velostream::velostream::test_harness::executor::CapturedOutput;
                                let empty_output = CapturedOutput {
                                    query_name: query_test.name.clone(),
                                    sink_name: format!("{}_output", query_test.name),
                                    records: Vec::new(),
                                    execution_time_ms: 0,
                                    warnings: Vec::new(),
                                    memory_peak_bytes: None,
                                    memory_growth_bytes: None,
                                };
                                let results = assertion_runner
                                    .run_assertions(&empty_output, &query_test.assertions);
                                assertion_results.extend(results);
                            }

                            // Report results
                            let passed = assertion_results.iter().all(|a| a.passed);
                            if passed {
                                println!("   ✅ Passed ({} assertions)", assertion_results.len());
                            } else {
                                let failed_count =
                                    assertion_results.iter().filter(|a| !a.passed).count();
                                println!(
                                    "   ❌ Failed ({}/{} assertions)",
                                    failed_count,
                                    assertion_results.len()
                                );
                            }

                            report_gen.add_query_result(&exec_result, &assertion_results);
                        }
                        Err(e) => {
                            println!("   💥 Error: {}", e);
                            // Create error result
                            use velostream::velostream::test_harness::executor::ExecutionResult;
                            let error_result = ExecutionResult {
                                query_name: query_test.name.clone(),
                                success: false,
                                error: Some(e.to_string()),
                                outputs: Vec::new(),
                                execution_time_ms: 0,
                            };
                            report_gen.add_query_result(&error_result, &[]);
                        }
                    }
                }

                // Step 8: Generate and output report
                let report = report_gen.generate();
                let output_format: OutputFormat = output.parse().unwrap_or(OutputFormat::Text);

                println!();
                let mut stdout = std::io::stdout();
                if let Err(e) = write_report(&report, output_format, &mut stdout) {
                    eprintln!("❌ Failed to write report: {}", e);
                }

                // Exit with appropriate code
                if report.summary.failed > 0 || report.summary.errors > 0 {
                    std::process::exit(1);
                }
            }
        }

        Commands::Validate { sql_file, verbose } => {
            println!("🔍 Validating SQL: {}", sql_file.display());

            // Use existing SqlValidator
            use velostream::velostream::sql::validator::SqlValidator;

            let validator = SqlValidator::new();
            let result = validator.validate_application_file(&sql_file);

            println!();
            println!("📊 Validation Results");
            println!("═════════════════════");
            println!(
                "Application: {}",
                result.application_name.as_deref().unwrap_or("unknown")
            );
            let invalid = result.total_queries.saturating_sub(result.valid_queries);
            println!(
                "Total Queries: {}, Valid: {}, Invalid: {}",
                result.total_queries, result.valid_queries, invalid
            );
            println!();

            if result.is_valid {
                println!("✅ All queries valid!");
            } else {
                println!("❌ Validation failed");
                for query_result in &result.query_results {
                    if !query_result.is_valid {
                        println!(
                            "\n  Query #{} (Line {}): {}",
                            query_result.query_index + 1,
                            query_result.start_line,
                            if verbose {
                                &query_result.query_text
                            } else {
                                "..."
                            }
                        );
                        for error in &query_result.parsing_errors {
                            println!("    ❌ {}", error.message);
                        }
                    }
                }
            }

            if !result.is_valid {
                std::process::exit(1);
            }
        }

        Commands::Init {
            sql_file,
            output,
            ai,
        } => {
            use velostream::velostream::test_harness::SpecGenerator;
            use velostream::velostream::test_harness::ai::AiAssistant;

            println!("📝 Generating test specification");
            println!("SQL File: {}", sql_file.display());
            println!("Output: {}", output.display());
            if ai {
                println!("AI Generation: enabled");
            }
            println!();

            let generator = SpecGenerator::new();
            let mut spec_generated = false;

            // If AI mode is enabled, try AI-powered generation first
            if ai {
                let ai_assistant = AiAssistant::new();

                if !ai_assistant.is_available() {
                    eprintln!("⚠️  AI mode requested but ANTHROPIC_API_KEY not set");
                    eprintln!("   Falling back to rule-based generation");
                    eprintln!();
                } else {
                    println!("🤖 Using AI-powered test spec generation...");

                    // Read SQL content
                    let sql_content = match std::fs::read_to_string(&sql_file) {
                        Ok(content) => content,
                        Err(e) => {
                            eprintln!("❌ Failed to read SQL file: {}", e);
                            std::process::exit(1);
                        }
                    };

                    // Get app name from file stem
                    let app_name = sql_file
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "application".to_string());

                    println!("   SQL file: {} bytes", sql_content.len());
                    println!("   Application: {}", app_name);

                    // Call AI to generate test spec
                    match ai_assistant
                        .generate_test_spec(&sql_content, &app_name)
                        .await
                    {
                        Ok(spec) => {
                            // Write spec to output file
                            match generator.write_spec(&spec, &output) {
                                Ok(_) => {
                                    println!(
                                        "✅ AI Generated test specification: {}",
                                        output.display()
                                    );
                                    println!();
                                    println!("📊 Summary:");
                                    println!("   • Application: {}", spec.application);
                                    println!("   • Queries: {}", spec.queries.len());
                                    for query in &spec.queries {
                                        println!(
                                            "     - {} ({} assertions)",
                                            query.name,
                                            query.assertions.len()
                                        );
                                    }
                                    println!();
                                    println!(
                                        "💡 Edit {} to customize assertions and input configurations",
                                        output.display()
                                    );
                                    spec_generated = true;
                                }
                                Err(e) => {
                                    eprintln!("❌ Failed to write AI-generated spec: {}", e);
                                    eprintln!("   Falling back to rule-based generation");
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("  ❌ AI test spec generation failed: {}", e);
                            eprintln!("   Falling back to rule-based generation");
                        }
                    }
                }
            }

            // Rule-based generation (if AI didn't generate, or as fallback)
            if !spec_generated {
                match generator.generate_from_sql(&sql_file) {
                    Ok(spec) => {
                        // Write spec to output file
                        match generator.write_spec(&spec, &output) {
                            Ok(_) => {
                                println!("✅ Generated test specification: {}", output.display());
                                println!();
                                println!("📊 Summary:");
                                println!("   • Application: {}", spec.application);
                                println!("   • Queries: {}", spec.queries.len());
                                for query in &spec.queries {
                                    println!(
                                        "     - {} ({} assertions)",
                                        query.name,
                                        query.assertions.len()
                                    );
                                }
                                println!();
                                println!(
                                    "💡 Edit {} to customize assertions and input configurations",
                                    output.display()
                                );
                            }
                            Err(e) => {
                                eprintln!("❌ Failed to write test specification: {}", e);
                                std::process::exit(1);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to generate test specification: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }

        Commands::InferSchema {
            sql_file,
            data_dir,
            output,
            ai,
        } => {
            use velostream::velostream::test_harness::ai::{AiAssistant, CsvSample};
            use velostream::velostream::test_harness::inference::SchemaInferencer;

            println!("🔬 Inferring schemas");
            println!("SQL File: {}", sql_file.display());
            if let Some(ref d) = data_dir {
                println!("Data Dir: {}", d.display());
            }
            println!("Output: {}", output.display());
            if ai {
                println!("AI Inference: enabled");
            }
            println!();

            // Create output directory if it doesn't exist
            std::fs::create_dir_all(&output)?;

            let inferencer = SchemaInferencer::new();
            let mut schema_count = 0;

            // If AI mode is enabled, use Claude for intelligent inference
            if ai {
                let ai_assistant = AiAssistant::new();

                if !ai_assistant.is_available() {
                    eprintln!("⚠️  AI mode requested but ANTHROPIC_API_KEY not set");
                    eprintln!("   Falling back to rule-based inference");
                    eprintln!();
                } else {
                    println!("🤖 Using AI-powered schema inference...");

                    // Read SQL content
                    let sql_content = std::fs::read_to_string(&sql_file)?;

                    // Collect CSV samples
                    let mut csv_samples = Vec::new();
                    if let Some(ref data_path) = data_dir
                        && data_path.exists()
                        && let Ok(entries) = std::fs::read_dir(data_path)
                    {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().is_some_and(|ext| ext == "csv")
                                && let Ok(content) = std::fs::read_to_string(&path)
                            {
                                let total_rows = content.lines().count();
                                // Take first 50 lines as sample
                                let sample_content: String =
                                    content.lines().take(50).collect::<Vec<_>>().join("\n");
                                csv_samples.push(CsvSample {
                                    name: path
                                        .file_stem()
                                        .map(|s| s.to_string_lossy().to_string())
                                        .unwrap_or_default(),
                                    content: sample_content,
                                    total_rows,
                                });
                            }
                        }
                    }

                    println!("   SQL file: {} bytes", sql_content.len());
                    println!("   CSV samples: {}", csv_samples.len());

                    // Call AI to infer schema
                    match ai_assistant.infer_schema(&sql_content, &csv_samples).await {
                        Ok(schema) => {
                            let output_file = output.join(format!("{}.schema.yaml", schema.name));
                            match inferencer.write_schema(&schema, &output_file) {
                                Ok(_) => {
                                    println!(
                                        "  ✅ AI Generated {} ({} fields)",
                                        output_file.display(),
                                        schema.fields.len()
                                    );
                                    schema_count += 1;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "  ❌ Failed to write {}: {}",
                                        output_file.display(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("  ❌ AI schema inference failed: {}", e);
                            eprintln!("   Falling back to rule-based inference");
                        }
                    }
                }
            }

            // Rule-based inference (always runs, or as fallback)
            if schema_count == 0 {
                // Step 1: Infer schemas from SQL file
                println!("📄 Analyzing SQL file...");
                match inferencer.infer_from_sql(&sql_file) {
                    Ok(schemas) => {
                        for schema in &schemas {
                            let output_file = output.join(format!("{}.schema.yaml", schema.name));
                            match inferencer.write_schema(schema, &output_file) {
                                Ok(_) => {
                                    println!(
                                        "  ✅ Generated {} ({} fields)",
                                        output_file.display(),
                                        schema.fields.len()
                                    );
                                    schema_count += 1;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "  ❌ Failed to write {}: {}",
                                        output_file.display(),
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  ⚠️  Could not infer from SQL: {}", e);
                    }
                }

                // Step 2: Infer schemas from CSV files in data directory
                if let Some(ref data_path) = data_dir
                    && data_path.exists()
                {
                    println!("\n📊 Analyzing data files in {}...", data_path.display());
                    if let Ok(entries) = std::fs::read_dir(data_path) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().is_some_and(|ext| ext == "csv") {
                                match inferencer.infer_from_csv(&path) {
                                    Ok(schema) => {
                                        let output_file =
                                            output.join(format!("{}.schema.yaml", schema.name));
                                        match inferencer.write_schema(&schema, &output_file) {
                                            Ok(_) => {
                                                println!(
                                                    "  ✅ Generated {} ({} fields)",
                                                    output_file.display(),
                                                    schema.fields.len()
                                                );
                                                schema_count += 1;
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "  ❌ Failed to write {}: {}",
                                                    output_file.display(),
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "  ⚠️  Could not infer from {}: {}",
                                            path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            println!();
            if schema_count > 0 {
                println!(
                    "✅ Generated {} schema file(s) in {}",
                    schema_count,
                    output.display()
                );
            } else {
                println!("⚠️  No schemas were generated");
            }
        }

        Commands::Stress {
            sql_file,
            spec,
            records,
            duration,
            output,
        } => {
            use velostream::velostream::test_harness::SpecGenerator;
            use velostream::velostream::test_harness::stress::{StressConfig, StressRunner};

            println!("🔥 Stress Test Mode");
            println!("SQL File: {}", sql_file.display());
            if let Some(ref s) = spec {
                println!("Test Spec: {}", s.display());
            }
            println!("Records per source: {}", records);
            println!("Max Duration: {}s", duration);
            println!("Output Format: {}", output);
            println!();

            // Build stress config
            let stress_config = StressConfig::with_records(records)
                .batch_size(1000)
                .measure_latency(true);

            // Try to load test spec if provided, otherwise generate from SQL
            let spec_generator = SpecGenerator::new();
            let test_spec = if let Some(ref spec_path) = spec {
                // Load from file
                let content = std::fs::read_to_string(spec_path)?;
                serde_yaml::from_str(&content)?
            } else {
                // Generate from SQL file
                match spec_generator.generate_from_sql(&sql_file) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("❌ Failed to analyze SQL file: {}", e);
                        std::process::exit(1);
                    }
                }
            };

            // Get schema directory (same as SQL file directory or spec directory)
            let schema_dir = spec
                .as_ref()
                .and_then(|p| p.parent())
                .unwrap_or_else(|| sql_file.parent().unwrap_or(std::path::Path::new(".")));

            println!("📊 Running stress test...");
            println!("   Sources: {}", test_spec.queries.len());
            println!("   Schema dir: {}", schema_dir.display());
            println!();

            let mut runner = StressRunner::new(stress_config);

            // Load schemas for each input in queries
            for query in &test_spec.queries {
                for input in &query.inputs {
                    if let Some(schema_file) = &input.schema {
                        let schema_path = schema_dir.join(schema_file);
                        if schema_path.exists() {
                            if let Err(e) = runner.load_schema_file(&input.source, &schema_path) {
                                eprintln!(
                                    "⚠️  Failed to load schema {}: {}",
                                    schema_path.display(),
                                    e
                                );
                            } else {
                                println!("   Loaded schema: {}", input.source);
                            }
                        }
                    }
                }
            }

            // Run the stress test
            match runner.run() {
                Ok(metrics) => {
                    let report = runner.generate_report(&metrics);

                    match output.as_str() {
                        "json" => {
                            let json = serde_json::to_string_pretty(&metrics)?;
                            println!("{}", json);
                        }
                        _ => {
                            println!("{}", report);
                        }
                    }

                    println!("✅ Stress test completed successfully");
                }
                Err(e) => {
                    eprintln!("❌ Stress test failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
