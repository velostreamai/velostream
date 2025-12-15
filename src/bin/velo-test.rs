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
use serde_json;
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

        /// Auto-start Kafka using testcontainers (requires Docker)
        /// If not specified, will auto-start when no external Kafka is available
        #[arg(long)]
        use_testcontainers: bool,

        /// External Kafka bootstrap servers (overrides config file)
        #[arg(long)]
        kafka: Option<String>,

        /// Keep testcontainers running after test completion (for debugging)
        /// Use 'docker ps' to find the container, 'docker stop <id>' to clean up
        #[arg(long)]
        keep_containers: bool,

        /// Execute statements one at a time (step mode)
        /// Pauses after each statement and displays results
        #[arg(long)]
        step: bool,
    },

    /// Debug SQL application with interactive stepping and breakpoints
    Debug {
        /// Path to the SQL file to debug
        sql_file: PathBuf,

        /// Path to the test specification YAML file
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Directory containing schema definitions
        #[arg(long)]
        schemas: Option<PathBuf>,

        /// Set breakpoints on specific queries (can be used multiple times)
        #[arg(short, long)]
        breakpoint: Vec<String>,

        /// Timeout per query in milliseconds
        #[arg(long, default_value = "30000")]
        timeout_ms: u64,

        /// External Kafka bootstrap servers (overrides config file)
        #[arg(long)]
        kafka: Option<String>,

        /// Keep testcontainers running after completion
        #[arg(long)]
        keep_containers: bool,
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

/// Build information constants
const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUILD_TIME: &str = env!("BUILD_TIME");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_BRANCH: &str = env!("GIT_BRANCH");

/// Print version and build information
fn print_version_info() {
    println!(
        "velo-test v{} ({} @ {}) built {}",
        VERSION, GIT_HASH, GIT_BRANCH, BUILD_TIME
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Print version info at startup
    print_version_info();
    println!();

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
            use_testcontainers,
            kafka,
            keep_containers,
            step,
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
                                        Ok(mut schema) => {
                                            schema.source_path = Some(path.display().to_string());
                                            let key_info = schema
                                                .key_field
                                                .as_ref()
                                                .map(|k| format!(", key_field: {}", k))
                                                .unwrap_or_default();
                                            println!(
                                                "   ✓ {} ({} fields{}) ← {}",
                                                schema.name,
                                                schema.fields.len(),
                                                key_info,
                                                path.display()
                                            );
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
            // Use the SQL file's directory as base for resolving relative config_file paths
            // This allows SQL files to use paths like '../configs/source.yaml' correctly
            // Canonicalize to get absolute path, so relative paths like '../configs/' resolve correctly
            let sql_dir = sql_file
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .canonicalize()
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
            let validator = SqlValidator::with_base_dir(&sql_dir);
            let validation_result = validator.validate_application_file(&sql_file);

            // Extract bootstrap.servers and schema files from the validated sources
            let mut bootstrap_servers: Option<String> = None;

            // Keys to check for bootstrap servers (in order of preference)
            let bootstrap_keys = [
                "bootstrap.servers",
                "brokers",
                "datasource.consumer_config.bootstrap.servers",
                "datasource.config.bootstrap.servers",
                "consumer_config.bootstrap.servers",
            ];

            for query_result in &validation_result.query_results {
                for source in &query_result.sources_found {
                    // Extract bootstrap.servers from any of the possible key locations
                    if bootstrap_servers.is_none() {
                        for key in &bootstrap_keys {
                            if let Some(bs) = source.properties.get(*key) {
                                // Check if this is a ${VAR:default} pattern where the env var is not set
                                // If so, we should start testcontainers (leave bootstrap_servers as None)
                                let resolved = if bs.contains("${") && bs.contains("}") {
                                    // Use runtime env var substitution
                                    use velostream::velostream::sql::config::substitute_env_vars;
                                    let substituted = substitute_env_vars(bs);
                                    // If the substitution resulted in the default value (e.g. localhost:9092/9999),
                                    // it means the env var wasn't set - we should use testcontainers
                                    if substituted.starts_with("localhost:") {
                                        log::info!(
                                            "bootstrap.servers '{}' resolved to default '{}' - will use testcontainers",
                                            bs,
                                            substituted
                                        );
                                        None // Trigger testcontainers
                                    } else {
                                        log::info!(
                                            "bootstrap.servers '{}' resolved to '{}' from env var",
                                            bs,
                                            substituted
                                        );
                                        Some(substituted)
                                    }
                                } else {
                                    // Plain value, use as-is
                                    Some(bs.clone())
                                };

                                if let Some(bs_resolved) = resolved {
                                    bootstrap_servers = Some(bs_resolved.clone());
                                    log::info!(
                                        "Found bootstrap.servers from source '{}' (key: {}): {}",
                                        source.name,
                                        key,
                                        bs_resolved
                                    );
                                }
                                break;
                            }
                        }
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
                            // Use the testable helper function for path resolution
                            let full_path =
                                velostream::velostream::test_harness::utils::resolve_schema_path(
                                    schema_path,
                                );
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
                                        Ok(mut schema) => {
                                            schema.source_path =
                                                Some(full_path.display().to_string());
                                            let key_info = schema
                                                .key_field
                                                .as_ref()
                                                .map(|k| format!(", key_field: {}", k))
                                                .unwrap_or_default();
                                            println!(
                                                "   ✓ {} ({} fields{}) ← {} (from SQL config)",
                                                schema.name,
                                                schema.fields.len(),
                                                key_info,
                                                full_path.display()
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

            // Priority for Kafka connection:
            // 1. --kafka CLI flag (explicit override)
            // 2. Config file bootstrap.servers
            // 3. KAFKA_BOOTSTRAP_SERVERS env var
            // 4. --use-testcontainers flag (or auto-start if none of above)

            // Check CLI flag first
            if let Some(ref k) = kafka {
                bootstrap_servers = Some(k.clone());
                log::info!("Using bootstrap.servers from --kafka flag: {}", k);
            }

            // Check environment variable as fallback
            if bootstrap_servers.is_none()
                && let Ok(bs) = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            {
                bootstrap_servers = Some(bs);
                log::info!("Using bootstrap.servers from KAFKA_BOOTSTRAP_SERVERS env var");
            }

            // Create test infrastructure
            // If --use-testcontainers or no Kafka configured, start testcontainers
            let mut infra = if use_testcontainers || bootstrap_servers.is_none() {
                println!("🐳 Starting Kafka via testcontainers (requires Docker)...");
                match TestHarnessInfra::with_testcontainers().await {
                    Ok(infra) => {
                        println!(
                            "   Kafka: {} (testcontainers)",
                            infra.bootstrap_servers().unwrap_or("unknown")
                        );
                        infra
                    }
                    Err(e) => {
                        if bootstrap_servers.is_some() {
                            // Fall back to configured Kafka if testcontainers fails
                            eprintln!("⚠️  Testcontainers failed ({}), using configured Kafka", e);
                            TestHarnessInfra::with_kafka(bootstrap_servers.as_ref().unwrap())
                        } else {
                            eprintln!("❌ Failed to start testcontainers Kafka: {}", e);
                            eprintln!("   Make sure Docker is running and try again");
                            eprintln!();
                            eprintln!(
                                "   Alternatively, specify --kafka <bootstrap_servers> to use an external Kafka"
                            );
                            std::process::exit(1);
                        }
                    }
                }
            } else if let Some(ref bs) = bootstrap_servers {
                println!("   Kafka: {}", bs);
                TestHarnessInfra::with_kafka(bs)
            } else {
                TestHarnessInfra::new()
            };

            // Start the infrastructure (creates topics, etc.)
            if let Err(e) = infra.start().await {
                eprintln!("❌ Failed to start test infrastructure: {}", e);
                // Cleanup testcontainers before exiting (unless --keep-containers)
                if !keep_containers {
                    let _ = infra.stop().await;
                }
                std::process::exit(1);
            }

            // Set VELOSTREAM_KAFKA_BROKERS env var to override config file bootstrap.servers
            // This ensures SQL job sinks use the same Kafka as the test harness
            // SAFETY: This is safe because we are single-threaded at this point in startup,
            // before any async tasks or threads are spawned that might read env vars.
            if let Some(bs) = infra.bootstrap_servers() {
                unsafe {
                    std::env::set_var("VELOSTREAM_KAFKA_BROKERS", bs);
                }
                log::info!("Set VELOSTREAM_KAFKA_BROKERS={}", bs);
            }

            // Check if Kafka is available
            if infra.bootstrap_servers().is_none() {
                eprintln!("⚠️  No Kafka bootstrap servers configured");
                eprintln!("   Use --use-testcontainers to auto-start Kafka (requires Docker)");
                eprintln!("   Or specify --kafka <bootstrap_servers> to use an external Kafka");
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
                    // Cleanup testcontainers before exiting (unless --keep-containers)
                    if !keep_containers {
                        let _ = infra.stop().await;
                    }
                    std::process::exit(1);
                }
                // Cleanup infra even on success in validation-only mode (unless --keep-containers)
                if !keep_containers {
                    let _ = infra.stop().await;
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
                // Configure test infrastructure overrides (bootstrap servers, etc.)
                let overrides = ConfigOverrideBuilder::new(&run_id)
                    .bootstrap_servers(infra.bootstrap_servers().unwrap())
                    .build();

                // Step 5: Create executor with StreamJobServer for SQL execution
                let executor = QueryExecutor::new(infra)
                    .with_timeout(Duration::from_millis(timeout_ms))
                    .with_overrides(overrides)
                    .with_schema_registry(schema_registry);

                // Initialize StreamJobServer for actual SQL execution
                // Pass SQL file's parent directory so the server can resolve relative config file paths
                let sql_dir = sql_file
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .canonicalize()
                    .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
                let mut executor = match executor.with_server(Some(sql_dir)).await {
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

                // Handle step mode with StatementExecutor
                if step {
                    use std::io::{BufRead, Write};
                    use velostream::velostream::test_harness::statement_executor::{
                        ExecutionMode, SessionState, StatementExecutor,
                    };

                    println!();
                    println!("🐛 Step Mode - Execute statements one at a time");
                    println!("   Commands: [Enter]=step, q=quit, r=run all remaining");
                    println!();

                    let mut stmt_executor =
                        StatementExecutor::with_executor(executor).with_mode(ExecutionMode::Step);

                    // Set spec for input data generation
                    stmt_executor = stmt_executor.with_spec(test_spec.clone());

                    if let Err(e) = stmt_executor.load_sql(&sql_file) {
                        eprintln!("❌ Failed to load SQL: {}", e);
                        std::process::exit(1);
                    }

                    let statements = stmt_executor.statements();
                    println!("📝 Loaded {} statements:", statements.len());
                    for stmt in statements {
                        println!(
                            "   [{}] {} ({})",
                            stmt.index + 1,
                            stmt.name,
                            stmt.statement_type.display_name()
                        );
                    }
                    println!();

                    let stdin = std::io::stdin();
                    let mut stdout = std::io::stdout();
                    let mut failed = false;

                    loop {
                        // Check if completed
                        if matches!(
                            stmt_executor.state(),
                            SessionState::Completed | SessionState::Error
                        ) {
                            break;
                        }

                        // Show current statement
                        let idx = stmt_executor.current_index();
                        if idx < stmt_executor.statements().len() {
                            let stmt = &stmt_executor.statements()[idx];
                            println!(
                                "▶️  [{}/{}] {} ({})",
                                idx + 1,
                                stmt_executor.statements().len(),
                                stmt.name,
                                stmt.statement_type.display_name()
                            );
                        }

                        // Prompt for input
                        print!("Step> ");
                        stdout.flush().ok();

                        let mut input = String::new();
                        if stdin.lock().read_line(&mut input).is_err() {
                            break;
                        }

                        let input = input.trim();
                        match input {
                            "q" | "quit" | "exit" => {
                                println!("🛑 Exiting step mode");
                                break;
                            }
                            "r" | "run" => {
                                println!("🚀 Running all remaining statements...");
                                // Switch to full mode and execute all
                                match stmt_executor.continue_execution().await {
                                    Ok(results) => {
                                        for result in &results {
                                            let icon = if result.success { "✅" } else { "❌" };
                                            println!(
                                                "   {} {} ({}ms)",
                                                icon, result.name, result.execution_time_ms
                                            );
                                            if let Some(ref err) = result.error {
                                                println!("      Error: {}", err);
                                                failed = true;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Execution error: {}", e);
                                        failed = true;
                                    }
                                }
                                break;
                            }
                            "" | "s" | "step" => {
                                // Execute next statement
                                match stmt_executor.step_next().await {
                                    Ok(Some(result)) => {
                                        let icon = if result.success { "✅" } else { "❌" };
                                        println!(
                                            "   {} Completed in {}ms",
                                            icon, result.execution_time_ms
                                        );
                                        if let Some(ref err) = result.error {
                                            println!("      Error: {}", err);
                                            failed = true;
                                        }
                                        if let Some(ref output) = result.output {
                                            println!(
                                                "      Output: {} records to {}",
                                                output.records.len(),
                                                output.sink_name
                                            );
                                        }
                                        println!();
                                    }
                                    Ok(None) => {
                                        println!("✅ All statements executed");
                                        break;
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Execution error: {}", e);
                                        failed = true;
                                        break;
                                    }
                                }
                            }
                            _ => {
                                println!("Unknown command: {}", input);
                                println!("Commands: [Enter]=step, q=quit, r=run all remaining");
                            }
                        }
                    }

                    // Summary
                    println!();
                    println!("📊 Step Mode Summary");
                    println!("════════════════════");
                    let results = stmt_executor.results();
                    let passed = results.iter().filter(|r| r.success).count();
                    let total = results.len();
                    println!(
                        "   Executed: {}/{} statements",
                        total,
                        stmt_executor.statements().len()
                    );
                    println!("   Passed: {}/{}", passed, total);

                    // Cleanup
                    if !keep_containers && let Err(e) = stmt_executor.executor_mut().stop().await {
                        log::warn!("Failed to stop executor: {}", e);
                    }

                    if failed || passed < total {
                        std::process::exit(1);
                    }
                    return Ok(());
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

                // Track if we were interrupted
                let mut was_interrupted = false;

                for query_test in &queries_to_run {
                    println!("▶️  Executing: {}", query_test.name);

                    // Use select! to race query execution against Ctrl+C
                    // This allows us to cancel long-running operations
                    let exec_result = tokio::select! {
                        result = executor.execute_query(query_test) => result,
                        _ = tokio::signal::ctrl_c() => {
                            eprintln!("\n⚠️  Interrupted - cleaning up containers...");
                            was_interrupted = true;
                            break;
                        }
                    };

                    // If we broke out of select due to interrupt, exit loop
                    if was_interrupted {
                        break;
                    }

                    // Process the result
                    match exec_result {
                        Ok(exec_result) => {
                            // Run assertions on captured output
                            let mut assertion_results = Vec::new();

                            for output in &exec_result.outputs {
                                // Get assertions for this specific sink (includes fallback to top-level)
                                let sink_assertions: Vec<_> = query_test
                                    .assertions_for_sink(&output.sink_name)
                                    .into_iter()
                                    .cloned()
                                    .collect();
                                let results =
                                    assertion_runner.run_assertions(output, &sink_assertions);
                                assertion_results.extend(results);
                            }

                            // If no outputs captured, create empty output for assertions
                            // Use all_assertions() to include per-sink assertions from outputs[]
                            let all_assertions: Vec<_> =
                                query_test.all_assertions().into_iter().cloned().collect();
                            if exec_result.outputs.is_empty() && !all_assertions.is_empty() {
                                use velostream::velostream::test_harness::executor::CapturedOutput;
                                let empty_output = CapturedOutput {
                                    query_name: query_test.name.clone(),
                                    sink_name: format!("{}_output", query_test.name),
                                    topic: None, // No topic for empty output
                                    records: Vec::new(),
                                    message_keys: Vec::new(), // No keys for empty output
                                    execution_time_ms: 0,
                                    warnings: Vec::new(),
                                    memory_peak_bytes: None,
                                    memory_growth_bytes: None,
                                };
                                let results =
                                    assertion_runner.run_assertions(&empty_output, &all_assertions);
                                assertion_results.extend(results);
                            }

                            // Report results - single pass through assertions
                            let total = assertion_results.len();
                            let failed_count =
                                assertion_results.iter().filter(|a| !a.passed).count();

                            if failed_count == 0 {
                                println!("   ✅ Passed ({}/{} assertions)", total, total);
                            } else {
                                println!("   ❌ Failed ({}/{} assertions)", failed_count, total);
                            }

                            // Print details of all assertions
                            for result in &assertion_results {
                                let icon = if result.passed { "✅" } else { "❌" };
                                println!(
                                    "      {} {}: {}",
                                    icon, result.assertion_type, result.message
                                );
                                // Show expected/actual for failed assertions
                                if !result.passed
                                    && let (Some(expected), Some(actual)) =
                                        (&result.expected, &result.actual)
                                {
                                    println!("         Expected: {}", expected);
                                    println!("         Actual:   {}", actual);
                                }
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

                // Cleanup executor and infrastructure (stops testcontainers)
                // Always cleanup on interrupt, regardless of keep_containers flag
                if keep_containers && !was_interrupted {
                    println!();
                    println!("🐳 Keeping containers running for debugging (--keep-containers)");
                    println!("   Use 'docker ps' to find containers");
                    println!("   Use 'docker stop <id>' to clean up when done");
                } else {
                    if was_interrupted {
                        println!("🧹 Cleaning up containers after interrupt...");
                    }
                    if let Err(e) = executor.stop().await {
                        log::warn!("Failed to stop executor: {}", e);
                    }
                    if was_interrupted {
                        println!("✅ Containers cleaned up");
                        std::process::exit(130); // Standard exit code for SIGINT
                    }
                }

                // Exit with appropriate code
                if report.summary.failed > 0 || report.summary.errors > 0 {
                    std::process::exit(1);
                }
            }
        }

        Commands::Validate { sql_file, verbose } => {
            println!("🔍 Validating SQL: {}", sql_file.display());

            // Use existing SqlValidator with SQL file's directory as base for config_file resolution
            // Canonicalize to get absolute path, so relative paths like '../configs/' resolve correctly
            use velostream::velostream::sql::validator::SqlValidator;

            let sql_dir = sql_file
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .canonicalize()
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
            let validator = SqlValidator::with_base_dir(&sql_dir);
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
                                println!("   ✓ {} ← {}", input.source, schema_path.display());
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

        Commands::Debug {
            sql_file,
            spec,
            schemas,
            breakpoint,
            timeout_ms,
            kafka,
            keep_containers,
        } => {
            use std::io::{BufRead, Write};
            use std::time::Duration;
            use velostream::velostream::test_harness::SpecGenerator;
            use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
            use velostream::velostream::test_harness::infra::TestHarnessInfra;
            use velostream::velostream::test_harness::schema::SchemaRegistry;
            use velostream::velostream::test_harness::spec::TestSpec;
            use velostream::velostream::test_harness::statement_executor::{
                CommandResult, DebugCommand, DebugSession, ExecutionMode, ExportFormat,
                FilterOperator, SessionState, StatementExecutor,
            };

            println!("🐛 Velostream SQL Debugger");
            println!("════════════════════════════════════════");
            println!("SQL File: {}", sql_file.display());
            if let Some(ref s) = spec {
                println!("Test Spec: {}", s.display());
            }
            if !breakpoint.is_empty() {
                println!("Breakpoints: {:?}", breakpoint);
            }
            println!("Timeout: {}ms", timeout_ms);
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
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("❌ Failed to analyze SQL file: {}", e);
                        std::process::exit(1);
                    }
                }
            };

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
                            && let Ok(content) = std::fs::read_to_string(&path)
                            && let Ok(mut schema) = serde_yaml::from_str::<
                                velostream::velostream::test_harness::Schema,
                            >(&content)
                        {
                            schema.source_path = Some(path.display().to_string());
                            let key_info = schema
                                .key_field
                                .as_ref()
                                .map(|k| format!(", key_field: {}", k))
                                .unwrap_or_default();
                            println!(
                                "   ✓ {} ({} fields{}) ← {}",
                                schema.name,
                                schema.fields.len(),
                                key_info,
                                path.display()
                            );
                            schema_registry.register(schema);
                        }
                    }
                }
            }

            // Step 3: Create infrastructure
            println!("🔧 Initializing debug infrastructure...");

            // Check for Kafka from CLI or environment
            let bootstrap_servers = kafka
                .clone()
                .or_else(|| std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok());

            let mut infra = if let Some(ref bs) = bootstrap_servers {
                println!("   Kafka: {}", bs);
                TestHarnessInfra::with_kafka(bs)
            } else {
                println!("🐳 Starting Kafka via testcontainers...");
                match TestHarnessInfra::with_testcontainers().await {
                    Ok(i) => {
                        println!(
                            "   Kafka: {} (testcontainers)",
                            i.bootstrap_servers().unwrap_or("unknown")
                        );
                        i
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to start testcontainers: {}", e);
                        std::process::exit(1);
                    }
                }
            };

            if let Err(e) = infra.start().await {
                eprintln!("❌ Failed to start infrastructure: {}", e);
                std::process::exit(1);
            }

            // Set environment for SQL job
            if let Some(bs) = infra.bootstrap_servers() {
                unsafe {
                    std::env::set_var("VELOSTREAM_KAFKA_BROKERS", bs);
                }
            }

            // Step 4: Create executor
            let run_id = format!(
                "{:08x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u32
            );
            let overrides = ConfigOverrideBuilder::new(&run_id)
                .bootstrap_servers(infra.bootstrap_servers().unwrap_or("localhost:9092"))
                .build();

            use velostream::velostream::test_harness::executor::QueryExecutor;
            let executor = QueryExecutor::new(infra)
                .with_timeout(Duration::from_millis(timeout_ms))
                .with_overrides(overrides)
                .with_schema_registry(schema_registry);

            // Initialize with server
            let sql_dir = sql_file
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .canonicalize()
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());

            let executor = match executor.with_server(Some(sql_dir)).await {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("❌ Failed to initialize SQL server: {}", e);
                    std::process::exit(1);
                }
            };

            // Step 5: Create debug session
            let mut stmt_executor = StatementExecutor::with_executor(executor)
                .with_mode(ExecutionMode::Breakpoint)
                .with_spec(test_spec);

            // Add breakpoints
            for bp in &breakpoint {
                stmt_executor.add_breakpoint(bp);
                println!("   Breakpoint set: {}", bp);
            }

            // Load SQL file
            if let Err(e) = stmt_executor.load_sql(&sql_file) {
                eprintln!("❌ Failed to load SQL: {}", e);
                std::process::exit(1);
            }

            let mut session = DebugSession::new(stmt_executor);

            // Print statements with full SQL
            println!();
            println!("📝 SQL Statements:");
            let statements = session.executor().statements();
            for stmt in statements {
                let bp_marker = if session.executor().breakpoints().contains(&stmt.name) {
                    "*"
                } else {
                    " "
                };
                println!(
                    "  {} [{}] {} ({})",
                    bp_marker,
                    stmt.index + 1,
                    stmt.name,
                    stmt.statement_type.display_name()
                );
                // Show full SQL indented
                for sql_line in stmt.sql_text.lines() {
                    println!("      {}", sql_line);
                }
                println!(); // Blank line between statements
            }

            // Helper function to resolve statement number to name
            fn resolve_statement_arg(
                arg: &str,
                statements: &[velostream::velostream::test_harness::statement_executor::ParsedStatement],
            ) -> Option<String> {
                // First try to parse as a number (1-indexed)
                if let Ok(num) = arg.parse::<usize>() {
                    if num >= 1 && num <= statements.len() {
                        return Some(statements[num - 1].name.clone());
                    } else {
                        println!(
                            "Invalid statement number: {}. Valid range: 1-{}",
                            num,
                            statements.len()
                        );
                        return None;
                    }
                }
                // Otherwise use as name directly
                Some(arg.to_string())
            }

            // Helper function to prompt user to select a statement from a list
            fn prompt_statement_selection(
                statements: &[velostream::velostream::test_harness::statement_executor::ParsedStatement],
                stdin: &std::io::Stdin,
                stdout: &mut std::io::Stdout,
            ) -> Option<String> {
                use std::io::Write;

                if statements.is_empty() {
                    println!("No statements available.");
                    return None;
                }

                println!("Select a statement:");
                for (i, stmt) in statements.iter().enumerate() {
                    println!(
                        "  [{}] {} ({})",
                        i + 1,
                        stmt.name,
                        stmt.statement_type.display_name()
                    );
                    // Show full SQL indented
                    for sql_line in stmt.sql_text.lines() {
                        println!("      {}", sql_line);
                    }
                    println!(); // Blank line between statements
                }
                print!("Enter number [1-{}]: ", statements.len());
                let _ = stdout.flush();

                let mut input = String::new();
                if stdin.read_line(&mut input).is_ok() {
                    let input = input.trim();
                    if let Ok(num) = input.parse::<usize>()
                        && (1..=statements.len()).contains(&num)
                    {
                        return Some(statements[num - 1].name.clone());
                    }
                    if !input.is_empty() {
                        println!("Invalid selection: {}", input);
                    }
                }
                None
            }

            /// Parse a filter expression like "field=value" or "field > value"
            fn parse_filter_expr(expr: &str) -> (String, Option<FilterOperator>, String) {
                // Try operators in order of length (longest first to avoid partial matches)
                let operators = [
                    (">=", FilterOperator::Gte),
                    ("<=", FilterOperator::Lte),
                    ("!=", FilterOperator::Ne),
                    ("<>", FilterOperator::Ne),
                    ("==", FilterOperator::Eq),
                    ("=", FilterOperator::Eq),
                    (">", FilterOperator::Gt),
                    ("<", FilterOperator::Lt),
                    ("~", FilterOperator::Contains),
                ];

                for (op_str, op) in operators {
                    if let Some(idx) = expr.find(op_str) {
                        let field = expr[..idx].trim().to_string();
                        let value = expr[idx + op_str.len()..].trim().to_string();
                        return (field, Some(op), value);
                    }
                }

                // No operator found
                (expr.to_string(), None, String::new())
            }

            // Interactive debug loop
            println!();
            println!("🐛 Debug Commands:");
            println!("   s, step       - Execute next statement");
            println!("   c, continue   - Run until next breakpoint");
            println!("   r, run        - Run all remaining statements");
            println!("   b <N>         - Set breakpoint on statement N");
            println!("   u <N>         - Remove breakpoint from statement N");
            println!("   l, list       - List all statements");
            println!("   i <N>         - Inspect output from statement N");
            println!("   st, status    - Show current state");
            println!("   topics        - List all Kafka topics with offsets");
            println!("   schema <topic> - Show schema for topic (inferred)");
            println!("   consumers     - List all consumer groups");
            println!("   jobs          - List all jobs with source/sink status");
            println!();
            println!("📊 Data Visibility:");
            println!("   messages <topic|N> [--last N] [--first N] - Peek at topic messages");
            println!("      (Use topic number from 'topics' list, e.g., 'messages 1')");
            println!("   head <stmt> [-n N]   - Show first N records (default: 10)");
            println!("   tail <stmt> [-n N]   - Show last N records (default: 10)");
            println!("   filter <stmt> <field><op><value> - Filter records (op: =,!=,>,<,~)");
            println!("   export <stmt> <file> - Export records to JSON/CSV file");
            println!();
            println!("   q, quit       - Exit debugger");
            println!();

            let stdin = std::io::stdin();
            let mut stdout = std::io::stdout();

            let mut execution_finished = false;
            // Cache topic names for numbered references (e.g., "messages 1" instead of "messages my_long_topic")
            let mut last_topic_list: Vec<String> = Vec::new();

            loop {
                // Show state
                let state = session.executor().state();
                let idx = session.executor().current_index();
                let total = session.executor().statements().len();

                match state {
                    SessionState::Completed => {
                        if !execution_finished {
                            println!("✅ All statements completed");
                            println!();
                            println!("📊 Execution finished. You can still inspect data:");
                            println!("   l, list      - List all statements with SQL");
                            println!("   topics       - View topic state and offsets");
                            println!("   jobs         - View job details and statistics");
                            println!("   i <N>        - Inspect output from statement N");
                            println!("   messages <topic|N> - Peek at topic messages");
                            println!("   head <N>     - Show first records from statement N");
                            println!("   tail <N>     - Show last records from statement N");
                            println!("   filter <N> <expr> - Filter records (e.g., status=FAILED)");
                            println!("   export <N> <file> - Export to JSON/CSV");
                            println!("   q            - Exit and cleanup");
                            execution_finished = true;
                        }
                        println!();
                        print!("🏁 ");
                    }
                    SessionState::Error => {
                        if !execution_finished {
                            println!("❌ Execution stopped due to error");
                            println!();
                            println!("📊 You can still inspect data:");
                            println!("   l, list      - List all statements with SQL");
                            println!("   topics       - View topic state and offsets");
                            println!("   jobs         - View job details and statistics");
                            println!("   i <N>        - Inspect output from statement N");
                            println!("   messages <topic|N> - Peek at topic messages");
                            println!("   head <N>     - Show first records from statement N");
                            println!("   tail <N>     - Show last records from statement N");
                            println!("   filter <N> <expr> - Filter records (e.g., status=FAILED)");
                            println!("   export <N> <file> - Export to JSON/CSV");
                            println!("   q            - Exit and cleanup");
                            execution_finished = true;
                        }
                        println!();
                        print!("🔴 ");
                    }
                    SessionState::Paused(stmt_idx) => {
                        let stmt = &session.executor().statements()[*stmt_idx];
                        println!(
                            "⏸️  Paused at [{}/{}] {} ({})",
                            stmt_idx + 1,
                            total,
                            stmt.name,
                            stmt.statement_type.display_name()
                        );
                    }
                    SessionState::NotStarted => {
                        if idx < total {
                            let stmt = &session.executor().statements()[idx];
                            println!("▶️  Ready to execute [{}/{}] {}", idx + 1, total, stmt.name);
                        }
                    }
                    SessionState::Running => {
                        // Shouldn't happen in interactive mode
                    }
                }

                // Show appropriate prompt based on state
                if execution_finished {
                    print!("🏁 (inspect) ");
                } else {
                    print!("(debug) ");
                }
                stdout.flush().ok();

                let mut input = String::new();
                if stdin.lock().read_line(&mut input).is_err() {
                    break;
                }

                let parts: Vec<&str> = input.split_whitespace().collect();
                let cmd = parts.first().copied().unwrap_or("");

                let debug_cmd = match cmd {
                    "" | "s" | "step" => {
                        if execution_finished {
                            println!(
                                "Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit."
                            );
                            None
                        } else {
                            Some(DebugCommand::Step)
                        }
                    }
                    "c" | "continue" => {
                        if execution_finished {
                            println!(
                                "Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit."
                            );
                            None
                        } else {
                            Some(DebugCommand::Continue)
                        }
                    }
                    "r" | "run" => {
                        if execution_finished {
                            println!(
                                "Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit."
                            );
                            None
                        } else {
                            Some(DebugCommand::Run)
                        }
                    }
                    "b" | "break" => {
                        if execution_finished {
                            println!("Execution complete. Breakpoints are not available.");
                            None
                        } else if let Some(arg) = parts.get(1) {
                            let statements = session.executor().statements();
                            resolve_statement_arg(arg, statements).map(DebugCommand::Break)
                        } else {
                            println!("Usage: b <N>  (e.g., b 1)");
                            None
                        }
                    }
                    "u" | "unbreak" => {
                        if execution_finished {
                            println!("Execution complete. Breakpoints are not available.");
                            None
                        } else if let Some(arg) = parts.get(1) {
                            let statements = session.executor().statements();
                            resolve_statement_arg(arg, statements).map(DebugCommand::Unbreak)
                        } else {
                            println!("Usage: u <N>  (e.g., u 1)");
                            None
                        }
                    }
                    "cb" | "clear" => {
                        if execution_finished {
                            println!("Execution complete. Breakpoints are not available.");
                            None
                        } else {
                            Some(DebugCommand::Clear)
                        }
                    }
                    "l" | "list" => Some(DebugCommand::List),
                    "i" | "inspect" => {
                        if let Some(arg) = parts.get(1) {
                            let statements = session.executor().statements();
                            resolve_statement_arg(arg, statements).map(DebugCommand::Inspect)
                        } else {
                            println!("Usage: i <N>  (e.g., i 1)");
                            None
                        }
                    }
                    "ia" | "inspect-all" => Some(DebugCommand::InspectAll),
                    "hi" | "history" => Some(DebugCommand::History),
                    "st" | "status" => Some(DebugCommand::Status),
                    "topics" | "lt" | "list-topics" => Some(DebugCommand::ListTopics),
                    "consumers" | "lc" | "list-consumers" => Some(DebugCommand::ListConsumers),
                    "jobs" | "lj" | "list-jobs" => Some(DebugCommand::ListJobs),
                    "schema" | "sc" => {
                        if let Some(arg) = parts.get(1) {
                            // Resolve topic name - first check if it's a statement number
                            let statements = session.executor().statements();
                            if let Ok(num) = arg.parse::<usize>() {
                                // It's a number - try to get the sink topic from that statement
                                if num >= 1 && num <= statements.len() {
                                    let topic_name = statements[num - 1]
                                        .sink_topic
                                        .clone()
                                        .unwrap_or_else(|| arg.to_string());
                                    Some(DebugCommand::ShowSchema(topic_name))
                                } else {
                                    println!(
                                        "Invalid statement number: {}. Valid range: 1-{}",
                                        num,
                                        statements.len()
                                    );
                                    None
                                }
                            } else {
                                // It's a topic name
                                Some(DebugCommand::ShowSchema(arg.to_string()))
                            }
                        } else {
                            println!("Usage: schema <topic_name> or schema <N>");
                            None
                        }
                    }

                    // === Data Visibility Commands ===
                    "messages" | "msg" | "m" => {
                        if let Some(topic_arg) = parts.get(1) {
                            // Resolve topic: either a number (from topics list) or a name
                            let topic_name = if let Ok(num) = topic_arg.parse::<usize>() {
                                // It's a number - look up in cached topic list
                                if num == 0 || num > last_topic_list.len() {
                                    if last_topic_list.is_empty() {
                                        println!(
                                            "No topics cached. Run 'topics' first to see available topics."
                                        );
                                    } else {
                                        println!(
                                            "Topic number {} out of range (1-{}). Run 'topics' to see list.",
                                            num,
                                            last_topic_list.len()
                                        );
                                    }
                                    None
                                } else {
                                    Some(last_topic_list[num - 1].clone())
                                }
                            } else {
                                // It's a name - use directly
                                Some(topic_arg.to_string())
                            };

                            if let Some(topic) = topic_name {
                                // Parse options: --last N, --first N, --offset O, --partition P
                                let mut last: Option<usize> = None;
                                let mut first: Option<usize> = None;
                                let mut offset: Option<i64> = None;
                                let mut partition: Option<i32> = None;

                                let mut i = 2;
                                while i < parts.len() {
                                    match parts[i] {
                                        "--last" | "-l" => {
                                            if let Some(n) = parts.get(i + 1) {
                                                last = n.parse().ok();
                                                i += 1;
                                            }
                                        }
                                        "--first" | "-f" => {
                                            if let Some(n) = parts.get(i + 1) {
                                                first = n.parse().ok();
                                                i += 1;
                                            }
                                        }
                                        "--offset" | "-o" => {
                                            if let Some(o) = parts.get(i + 1) {
                                                offset = o.parse().ok();
                                                i += 1;
                                            }
                                        }
                                        "--partition" | "-p" => {
                                            if let Some(p) = parts.get(i + 1) {
                                                partition = p.parse().ok();
                                                i += 1;
                                            }
                                        }
                                        _ => {}
                                    }
                                    i += 1;
                                }

                                // Default to last 5 if no option specified
                                if last.is_none() && first.is_none() && offset.is_none() {
                                    last = Some(5);
                                }

                                Some(DebugCommand::Messages {
                                    topic,
                                    last,
                                    first,
                                    offset,
                                    partition,
                                })
                            } else {
                                None
                            }
                        } else {
                            println!(
                                "Usage: messages <topic|N> [--last N] [--first N] [--offset O] [--partition P]"
                            );
                            println!("  e.g., messages my_topic --last 10");
                            println!(
                                "  e.g., messages 1 --last 5  (uses topic #1 from 'topics' list)"
                            );
                            None
                        }
                    }

                    "head" | "hd" => {
                        let statements = session.executor().statements();
                        let limit = parts
                            .iter()
                            .position(|p| *p == "-n")
                            .and_then(|idx| parts.get(idx + 1))
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(10);

                        let stmt_name = if let Some(arg) = parts.get(1) {
                            // Skip if arg is "-n" (user typed "head -n 5")
                            if *arg == "-n" {
                                prompt_statement_selection(statements, &stdin, &mut stdout)
                            } else {
                                resolve_statement_arg(arg, statements)
                            }
                        } else {
                            prompt_statement_selection(statements, &stdin, &mut stdout)
                        };

                        stmt_name.map(|stmt| DebugCommand::Head {
                            statement: stmt,
                            limit,
                        })
                    }

                    "tail" | "tl" => {
                        let statements = session.executor().statements();
                        let limit = parts
                            .iter()
                            .position(|p| *p == "-n")
                            .and_then(|idx| parts.get(idx + 1))
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(10);

                        let stmt_name = if let Some(arg) = parts.get(1) {
                            // Skip if arg is "-n" (user typed "tail -n 5")
                            if *arg == "-n" {
                                prompt_statement_selection(statements, &stdin, &mut stdout)
                            } else {
                                resolve_statement_arg(arg, statements)
                            }
                        } else {
                            prompt_statement_selection(statements, &stdin, &mut stdout)
                        };

                        stmt_name.map(|stmt| DebugCommand::Tail {
                            statement: stmt,
                            limit,
                        })
                    }

                    "filter" | "f" => {
                        // Usage: filter <statement> <field>=<value>
                        // or: filter <statement> <field> <op> <value>
                        let statements = session.executor().statements();

                        if parts.len() >= 3 {
                            let arg = parts[1];

                            if let Some(statement) = resolve_statement_arg(arg, statements) {
                                let expr = parts[2..].join(" ");
                                // Parse expression: field=value or field > value etc.
                                let (field, operator, value) = parse_filter_expr(&expr);

                                if let Some(op) = operator {
                                    Some(DebugCommand::Filter {
                                        statement,
                                        field,
                                        operator: op,
                                        value,
                                    })
                                } else {
                                    println!(
                                        "Invalid filter expression. Use: field=value, field>value, field<value, field~value (contains)"
                                    );
                                    None
                                }
                            } else {
                                None
                            }
                        } else if parts.len() == 1 {
                            // No args - prompt for statement first, then expression
                            use std::io::Write;
                            if let Some(statement) =
                                prompt_statement_selection(statements, &stdin, &mut stdout)
                            {
                                print!("Filter expression (e.g., status=FAILED): ");
                                let _ = stdout.flush();
                                let mut expr_input = String::new();
                                if stdin.read_line(&mut expr_input).is_ok() {
                                    let expr = expr_input.trim();
                                    if !expr.is_empty() {
                                        let (field, operator, value) = parse_filter_expr(expr);
                                        if let Some(op) = operator {
                                            Some(DebugCommand::Filter {
                                                statement,
                                                field,
                                                operator: op,
                                                value,
                                            })
                                        } else {
                                            println!("Invalid filter expression.");
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            println!("Usage: filter <statement> <field>=<value>");
                            println!("  e.g., filter 1 status=FAILED");
                            println!("  Operators: = != > < >= <= ~ (contains)");
                            None
                        }
                    }

                    "export" | "ex" => {
                        let statements = session.executor().statements();

                        if parts.len() >= 3 {
                            let arg = parts[1];
                            let path_str = parts[2];

                            if let Some(statement) = resolve_statement_arg(arg, statements) {
                                let path = std::path::PathBuf::from(path_str);
                                let format = if path_str.ends_with(".csv") {
                                    ExportFormat::Csv
                                } else {
                                    ExportFormat::Json
                                };

                                Some(DebugCommand::Export {
                                    statement,
                                    path,
                                    format,
                                })
                            } else {
                                None
                            }
                        } else if parts.len() == 1 {
                            // No args - prompt for statement first, then filename
                            use std::io::Write;
                            if let Some(statement) =
                                prompt_statement_selection(statements, &stdin, &mut stdout)
                            {
                                print!("Export filename (e.g., results.json or data.csv): ");
                                let _ = stdout.flush();
                                let mut path_input = String::new();
                                if stdin.read_line(&mut path_input).is_ok() {
                                    let path_str = path_input.trim();
                                    if !path_str.is_empty() {
                                        let path = std::path::PathBuf::from(path_str);
                                        let format = if path_str.ends_with(".csv") {
                                            ExportFormat::Csv
                                        } else {
                                            ExportFormat::Json
                                        };
                                        Some(DebugCommand::Export {
                                            statement,
                                            path,
                                            format,
                                        })
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            println!("Usage: export <statement> <file.json|file.csv>");
                            println!("  e.g., export 1 results.json");
                            None
                        }
                    }

                    "q" | "quit" | "exit" => Some(DebugCommand::Quit),
                    "h" | "help" | "?" => {
                        if execution_finished {
                            println!("Inspection Commands (execution complete):");
                            println!("  l, list        - List all statements");
                            println!("  i <N>          - Inspect output from statement N");
                            println!("  ia, inspect-all - Inspect all captured outputs");
                            println!("  hi, history    - Show command history");
                            println!("  st, status     - Show current state");
                            println!("  topics         - List all Kafka topics with offsets");
                            println!(
                                "  schema <topic> - Show schema for topic (inferred from message)"
                            );
                            println!("  consumers      - List all consumer groups");
                            println!("  jobs           - List all jobs with source/sink status");
                            println!("  q, quit        - Exit and cleanup");
                        } else {
                            println!("Debug Commands:");
                            println!("  s, step        - Execute next statement");
                            println!("  c, continue    - Run until next breakpoint");
                            println!("  r, run         - Run all remaining statements");
                            println!("  b <N>          - Set breakpoint on statement N");
                            println!("  u <N>          - Remove breakpoint from statement N");
                            println!("  cb, clear      - Clear all breakpoints");
                            println!("  l, list        - List all statements");
                            println!("  i <N>          - Inspect output from statement N");
                            println!("  ia, inspect-all - Inspect all captured outputs");
                            println!("  hi, history    - Show command history");
                            println!("  st, status     - Show current state");
                            println!("  topics         - List all Kafka topics with offsets");
                            println!(
                                "  schema <topic> - Show schema for topic (inferred from message)"
                            );
                            println!("  consumers      - List all consumer groups");
                            println!("  jobs           - List all jobs with source/sink status");
                            println!();
                            println!("Data Visibility:");
                            println!(
                                "  messages <topic|N> [--last N] [--first N] - Peek at topic messages"
                            );
                            println!(
                                "     (Use topic number from 'topics' list, e.g., 'messages 1')"
                            );
                            println!("  head <stmt> [-n N]   - Show first N records (default: 10)");
                            println!("  tail <stmt> [-n N]   - Show last N records (default: 10)");
                            println!(
                                "  filter <stmt> <field><op><value> - Filter records (op: =,!=,>,<,~)"
                            );
                            println!("  export <stmt> <file> - Export records to JSON/CSV file");
                            println!();
                            println!("  q, quit        - Exit debugger");
                        }
                        None
                    }
                    _ => {
                        println!("Unknown command: {}. Type 'h' for help.", cmd);
                        None
                    }
                };

                if let Some(debug_cmd) = debug_cmd {
                    match session.execute_command(debug_cmd).await {
                        Ok(result) => match result {
                            CommandResult::StepResult(Some(res)) => {
                                let icon = if res.success { "✅" } else { "❌" };
                                println!(
                                    "   {} {} completed in {}ms",
                                    icon, res.name, res.execution_time_ms
                                );
                                if let Some(ref err) = res.error {
                                    println!("      Error: {}", err);
                                }
                                if let Some(ref out) = res.output {
                                    println!(
                                        "      Output: {} records to {}",
                                        out.records.len(),
                                        out.sink_name
                                    );
                                }

                                // Auto-display topic state after step
                                println!();
                                println!("   📋 Topic State:");
                                match session
                                    .execute_command(
                                        velostream::velostream::test_harness::DebugCommand::ListTopics,
                                    )
                                    .await
                                {
                                    Ok(CommandResult::TopicListing(topics)) => {
                                        if topics.is_empty() {
                                            println!("      (no topics)");
                                        } else {
                                            for topic in &topics {
                                                let test_marker = if topic.is_test_topic {
                                                    " [test]"
                                                } else {
                                                    ""
                                                };
                                                println!(
                                                    "      • {}{}: {} msgs",
                                                    topic.name, test_marker, topic.total_messages
                                                );
                                                for p in &topic.partitions {
                                                    if p.message_count > 0 {
                                                        println!(
                                                            "        P{}: {} msgs [{}..{}]",
                                                            p.partition,
                                                            p.message_count,
                                                            p.low_offset,
                                                            p.high_offset
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        println!("      (failed to fetch topics: {})", e);
                                    }
                                }
                            }
                            CommandResult::StepResult(None) => {
                                println!("✅ No more statements to execute");
                            }
                            CommandResult::ExecutionResults(results) => {
                                for res in &results {
                                    let icon = if res.success { "✅" } else { "❌" };
                                    println!(
                                        "   {} {} ({}ms)",
                                        icon, res.name, res.execution_time_ms
                                    );
                                    if let Some(ref err) = res.error {
                                        println!("      Error: {}", err);
                                    }
                                }
                                println!("   Executed {} statements", results.len());
                            }
                            CommandResult::Message(msg) => {
                                println!("   {}", msg);
                            }
                            CommandResult::Listing(lines) => {
                                for line in lines {
                                    println!("   {}", line);
                                }
                            }
                            CommandResult::Output(out) => {
                                println!("   Sink: {}", out.sink_name);
                                println!("   Records: {}", out.records.len());
                                println!("   Execution time: {}ms", out.execution_time_ms);
                                if !out.records.is_empty() {
                                    println!("   Sample (first 5):");
                                    for (i, rec) in out.records.iter().take(5).enumerate() {
                                        println!("     [{}] {:?}", i + 1, rec);
                                    }
                                }
                            }
                            CommandResult::AllOutputs(outputs) => {
                                println!("   {} captured outputs:", outputs.len());
                                for (name, out) in &outputs {
                                    println!(
                                        "   • {} → {} ({} records, {}ms)",
                                        name,
                                        out.sink_name,
                                        out.records.len(),
                                        out.execution_time_ms
                                    );
                                }
                            }
                            CommandResult::HistoryListing(lines) => {
                                println!("   Command history:");
                                for line in lines {
                                    println!("   {}", line);
                                }
                            }
                            CommandResult::TopicListing(topics) => {
                                // Store topic names for numbered references
                                last_topic_list = topics.iter().map(|t| t.name.clone()).collect();

                                println!("   📋 Topics ({}):", topics.len());
                                println!("   (Use number with 'messages N' for quick access)");
                                for (idx, topic) in topics.iter().enumerate() {
                                    let test_marker =
                                        if topic.is_test_topic { " [test]" } else { "" };
                                    println!(
                                        "   [{}] {}{} ({} messages)",
                                        idx + 1,
                                        topic.name,
                                        test_marker,
                                        topic.total_messages
                                    );
                                    for p in &topic.partitions {
                                        // Format timestamp if available
                                        let ts_str = p.latest_timestamp_ms.map(|ts| {
                                            use chrono::{TimeZone, Utc};
                                            Utc.timestamp_millis_opt(ts)
                                                .single()
                                                .map(|dt| dt.format("%H:%M:%S").to_string())
                                                .unwrap_or_else(|| format!("{}ms", ts))
                                        });

                                        // Format key if available (truncate if too long)
                                        let key_str = p.latest_key.as_ref().map(|k| {
                                            if k.len() > 20 {
                                                format!("{}...", &k[..17])
                                            } else {
                                                k.clone()
                                            }
                                        });

                                        // Build the last message info string
                                        // Always show key status (None if not set)
                                        let last_info = match (&key_str, &ts_str) {
                                            (Some(k), Some(t)) => {
                                                format!(" [last: key=\"{}\", @{}]", k, t)
                                            }
                                            (Some(k), None) => format!(" [last: key=\"{}\"]", k),
                                            (None, Some(t)) => format!(" [last: key=None, @{}]", t),
                                            (None, None) => {
                                                if p.message_count > 0 {
                                                    " [last: key=None]".to_string()
                                                } else {
                                                    String::new()
                                                }
                                            }
                                        };

                                        println!(
                                            "     └─ P{}: {} msgs (offsets: {}..{}){}",
                                            p.partition,
                                            p.message_count,
                                            p.low_offset,
                                            p.high_offset,
                                            last_info
                                        );
                                    }
                                }
                            }
                            CommandResult::ConsumerListing(consumers) => {
                                println!("   👥 Consumers ({}):", consumers.len());
                                for consumer in &consumers {
                                    println!("   • {} ({:?})", consumer.group_id, consumer.state);
                                    println!(
                                        "     Topics: {}",
                                        consumer.subscribed_topics.join(", ")
                                    );
                                    for pos in &consumer.positions {
                                        println!(
                                            "     └─ {}[P{}]: offset {} (lag: {})",
                                            pos.topic, pos.partition, pos.offset, pos.lag
                                        );
                                    }
                                }
                            }
                            CommandResult::JobListing(jobs) => {
                                use velostream::velostream::test_harness::{
                                    DataSinkType, DataSourceType, JobType,
                                };
                                println!("   🔧 Jobs ({}):", jobs.len());
                                for job in &jobs {
                                    let state_icon = match job.state {
                                        velostream::velostream::test_harness::JobState::Initializing => "🔄",
                                        velostream::velostream::test_harness::JobState::Running => "▶️",
                                        velostream::velostream::test_harness::JobState::Completed => "✅",
                                        velostream::velostream::test_harness::JobState::CompletedNoOutput => "⚠️",
                                        velostream::velostream::test_harness::JobState::Failed => "❌",
                                        velostream::velostream::test_harness::JobState::Paused => "⏸️",
                                        velostream::velostream::test_harness::JobState::Stopped => "⏹️",
                                    };
                                    let type_icon = match job.job_type {
                                        JobType::Stream => "🌊",
                                        JobType::Table => "📊",
                                        JobType::Unknown => "❓",
                                    };
                                    println!(
                                        "\n   {} {} {} [{}] ({:?})",
                                        state_icon,
                                        type_icon,
                                        job.name,
                                        job.job_type.display_name(),
                                        job.state
                                    );

                                    // Show SQL (truncated if too long)
                                    let sql_display = if job.sql.len() > 100 {
                                        format!("{}...", &job.sql[..100].replace('\n', " "))
                                    } else {
                                        job.sql.replace('\n', " ")
                                    };
                                    println!("     📝 SQL: {}", sql_display);

                                    // Show stats
                                    println!(
                                        "     📈 Stats: {} written, {} errors, {}ms",
                                        job.stats.records_written,
                                        job.stats.records_errored,
                                        job.stats.execution_time_ms
                                    );

                                    // Show partitioner
                                    if let Some(ref partitioner) = job.partitioner {
                                        println!("     🔀 Partitioner: {}", partitioner);
                                    }

                                    // Table-specific stats
                                    if job.job_type == JobType::Table {
                                        println!("     📊 Table State:");
                                        if let Some(count) = job.table_record_count {
                                            println!("       Records: {}", count);
                                        }
                                        if let Some(keys) = job.table_key_count {
                                            println!("       Unique keys: {}", keys);
                                        }
                                        if let Some(ref updated) = job.table_last_updated {
                                            println!("       Last updated: {}", updated);
                                        }
                                    }

                                    // Display detailed data sources
                                    if !job.sources.is_empty() {
                                        println!("     📥 Sources:");
                                        for src in &job.sources {
                                            let type_str = match &src.source_type {
                                                DataSourceType::Kafka => "Kafka",
                                                DataSourceType::File => "File",
                                                DataSourceType::Other(s) => s.as_str(),
                                            };
                                            println!("       • {} [{}]", src.name, type_str);
                                            // Show config details
                                            if let Some(ref servers) = src.bootstrap_servers {
                                                println!("         bootstrap.servers: {}", servers);
                                            }
                                            if let Some(ref group) = src.consumer_group {
                                                println!("         group.id: {}", group);
                                            }
                                            if let Some(ref reset) = src.auto_offset_reset {
                                                println!("         auto.offset.reset: {}", reset);
                                            }
                                            if let Some(ref format) = src.format {
                                                println!("         format: {}", format);
                                            }
                                            // Stats
                                            if src.records_read > 0 || src.bytes_read > 0 {
                                                println!(
                                                    "         stats: {} records, {} bytes",
                                                    src.records_read, src.bytes_read
                                                );
                                            }
                                            // Last record timestamp
                                            if let Some(ref ts) = src.last_record_time {
                                                println!(
                                                    "         last read: {}",
                                                    ts.format("%H:%M:%S")
                                                );
                                            }
                                            if let Some(lag) = src.lag {
                                                println!("         lag: {} messages", lag);
                                            }
                                            if let Some(ref offsets) = src.current_offsets {
                                                let offset_str: Vec<String> = offsets
                                                    .iter()
                                                    .map(|(p, o)| format!("P{}:{}", p, o))
                                                    .collect();
                                                println!(
                                                    "         offsets: {}",
                                                    offset_str.join(", ")
                                                );
                                            }
                                            if let Some(ref path) = src.file_path {
                                                let consumed = if src.fully_consumed == Some(true) {
                                                    " (EOF)"
                                                } else {
                                                    ""
                                                };
                                                println!("         path: {}{}", path, consumed);
                                            }
                                        }
                                    }

                                    // Display detailed data sinks
                                    if !job.sinks.is_empty() {
                                        println!("     📤 Sinks:");
                                        for sink in &job.sinks {
                                            let type_str = match &sink.sink_type {
                                                DataSinkType::Kafka => "Kafka",
                                                DataSinkType::File => "File",
                                                DataSinkType::Other(s) => s.as_str(),
                                            };
                                            println!("       • {} [{}]", sink.name, type_str);
                                            // Show config details
                                            if let Some(ref servers) = sink.bootstrap_servers {
                                                println!("         bootstrap.servers: {}", servers);
                                            }
                                            if let Some(ref format) = sink.format {
                                                println!("         format: {}", format);
                                            }
                                            if let Some(ref compression) = sink.compression {
                                                println!("         compression: {}", compression);
                                            }
                                            if let Some(ref acks) = sink.acks {
                                                println!("         acks: {}", acks);
                                            }
                                            // Stats - only show if there's data, and only show bytes if non-zero
                                            if sink.records_written > 0 || sink.bytes_written > 0 {
                                                if sink.bytes_written > 0 {
                                                    println!(
                                                        "         stats: {} records, {} bytes",
                                                        sink.records_written, sink.bytes_written
                                                    );
                                                } else {
                                                    println!(
                                                        "         stats: {} records",
                                                        sink.records_written
                                                    );
                                                }
                                            }
                                            // Last record timestamp
                                            if let Some(ref ts) = sink.last_record_time {
                                                println!(
                                                    "         last write: {}",
                                                    ts.format("%H:%M:%S")
                                                );
                                            }
                                            if let Some(ref offsets) = sink.produced_offsets {
                                                let offset_str: Vec<String> = offsets
                                                    .iter()
                                                    .map(|(p, o)| format!("P{}:{}", p, o))
                                                    .collect();
                                                println!(
                                                    "         produced: {}",
                                                    offset_str.join(", ")
                                                );
                                            }
                                            if let Some(ref path) = sink.file_path {
                                                let flushed = if sink.is_flushed == Some(true) {
                                                    " (flushed)"
                                                } else {
                                                    ""
                                                };
                                                println!("         path: {}{}", path, flushed);
                                            }
                                        }
                                    } else if !job.sink_topics.is_empty() {
                                        // Fallback to legacy sink_topics if sinks is empty
                                        println!("     📤 Sinks: {}", job.sink_topics.join(", "));
                                    }
                                }
                            }
                            CommandResult::SchemaDisplay(schema) => {
                                println!();
                                println!(
                                    "📋 Schema for '{}' (sampled {} record{}):",
                                    schema.topic,
                                    schema.records_sampled,
                                    if schema.records_sampled == 1 { "" } else { "s" }
                                );
                                if schema.fields.is_empty() {
                                    println!("   (no messages found or topic is empty)");
                                } else {
                                    println!(
                                        "   Key: {}",
                                        if schema.has_keys { "present" } else { "None" }
                                    );
                                    println!("   Fields:");
                                    for (name, type_name) in &schema.fields {
                                        println!("     • {}: {}", name, type_name);
                                    }
                                    if let Some(ref sample) = schema.sample_value {
                                        println!("\n   Sample value:");
                                        for line in sample.lines() {
                                            println!("     {}", line);
                                        }
                                    }
                                }
                                println!();
                            }
                            CommandResult::MessagesResult(messages) => {
                                println!();
                                if messages.is_empty() {
                                    println!("📨 No messages found");
                                } else {
                                    println!(
                                        "📨 {} message{}:",
                                        messages.len(),
                                        if messages.len() == 1 { "" } else { "s" }
                                    );
                                    for (idx, msg) in messages.iter().enumerate() {
                                        // Format timestamp
                                        let ts_str = msg.timestamp_ms.map(|ts| {
                                            use chrono::{TimeZone, Utc};
                                            Utc.timestamp_millis_opt(ts)
                                                .single()
                                                .map(|dt| {
                                                    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                                                })
                                                .unwrap_or_else(|| format!("{}ms", ts))
                                        });

                                        println!("  ─────────────────────────────────────────");
                                        println!(
                                            "  [{}] P{}:offset {}",
                                            idx + 1,
                                            msg.partition,
                                            msg.offset
                                        );
                                        if let Some(ts) = ts_str {
                                            println!("      timestamp: {}", ts);
                                        }
                                        println!(
                                            "      key: {}",
                                            msg.key.as_deref().unwrap_or("<null>")
                                        );
                                        if !msg.headers.is_empty() {
                                            println!("      headers:");
                                            for (hk, hv) in &msg.headers {
                                                // Truncate long header values
                                                let display_val = if hv.len() > 50 {
                                                    format!("{}...", &hv[..47])
                                                } else {
                                                    hv.clone()
                                                };
                                                println!("        {}: {}", hk, display_val);
                                            }
                                        }
                                        // Pretty print JSON value if possible
                                        let formatted_value =
                                            serde_json::from_str::<serde_json::Value>(&msg.value)
                                                .ok()
                                                .and_then(|v| serde_json::to_string_pretty(&v).ok())
                                                .unwrap_or_else(|| msg.value.clone());
                                        println!("      value:");
                                        for line in formatted_value.lines() {
                                            println!("        {}", line);
                                        }
                                    }
                                }
                                println!();
                            }
                            CommandResult::RecordsResult {
                                statement,
                                records,
                                total_count,
                                showing,
                            } => {
                                println!();
                                if records.is_empty() {
                                    println!("📊 No records found for statement '{}'", statement);
                                } else {
                                    println!(
                                        "📊 {} ({} of {} records) from '{}':",
                                        showing,
                                        records.len(),
                                        total_count,
                                        statement
                                    );
                                    for (idx, record) in records.iter().enumerate() {
                                        // Format record as JSON
                                        let json = serde_json::to_string(record)
                                            .unwrap_or_else(|_| format!("{:?}", record));
                                        println!("  [{}] {}", idx + 1, json);
                                    }
                                }
                                println!();
                            }
                            CommandResult::FilteredResult {
                                statement,
                                records,
                                total_count,
                                matched_count,
                                filter_expr,
                            } => {
                                println!();
                                println!("🔍 Filtered '{}' where {}:", statement, filter_expr);
                                println!(
                                    "   Found {} of {} records matching",
                                    matched_count, total_count
                                );
                                if records.is_empty() {
                                    println!("   (no matching records)");
                                } else {
                                    for (idx, record) in records.iter() {
                                        let json = serde_json::to_string(record)
                                            .unwrap_or_else(|_| format!("{:?}", record));
                                        println!("  [{}] {}", idx, json);
                                    }
                                }
                                println!();
                            }
                            CommandResult::ExportResult {
                                path,
                                record_count,
                                format,
                            } => {
                                println!();
                                println!(
                                    "✅ Exported {} records to {} ({})",
                                    record_count,
                                    path.display(),
                                    format
                                );
                                println!();
                            }
                            CommandResult::Quit => {
                                println!("👋 Exiting debugger");
                                break;
                            }
                        },
                        Err(e) => {
                            eprintln!("❌ Command error: {}", e);
                        }
                    }
                }

                println!();
            }

            // Cleanup
            if !keep_containers {
                println!("🧹 Cleaning up...");
                if let Err(e) = session.executor_mut().executor_mut().stop().await {
                    log::warn!("Failed to stop executor: {}", e);
                }
            } else {
                println!("🐳 Keeping containers running (--keep-containers)");
            }

            // Summary
            let results = session.executor().results();
            let passed = results.iter().filter(|r| r.success).count();
            let total_stmts = session.executor().statements().len();
            println!();
            println!("📊 Debug Session Summary");
            println!("   Executed: {}/{} statements", results.len(), total_stmts);
            println!("   Passed: {}/{}", passed, results.len());

            if passed < results.len() {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
