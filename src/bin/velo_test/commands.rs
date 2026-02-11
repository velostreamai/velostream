//! Command handlers for velo-test CLI
//!
//! This module contains the implementation of all velo-test subcommands,
//! extracted from the main() function for better organization and maintainability.

use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

// Test harness imports for Run command
use velostream::velostream::test_harness::SpecGenerator;
use velostream::velostream::test_harness::assertions::AssertionRunner;
use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
use velostream::velostream::test_harness::executor::QueryExecutor;
use velostream::velostream::test_harness::infra::TestHarnessInfra;
use velostream::velostream::test_harness::report::{OutputFormat, ReportGenerator, write_report};
use velostream::velostream::test_harness::schema::SchemaRegistry;
use velostream::velostream::test_harness::spec::TestSpec;

// Command handler functions will be added here
// Each function will handle one Commands:: variant from the CLI

/// Type alias for command results
pub type CommandResult = Result<(), Box<dyn Error>>;
/// Run tests for a SQL application
///
/// This function executes the full test harness workflow:
/// 1. Load or generate test specification
/// 2. Load schemas
/// 3. Validate SQL
/// 4. Create test infrastructure (Kafka via testcontainers or external)
/// 5. Execute queries
/// 6. Run assertions
/// 7. Generate report
/// 8. Cleanup infrastructure
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to test
/// * `spec` - Optional path to test specification YAML
/// * `schemas` - Optional directory containing schema definitions
/// * `query` - Optional filter to run only a specific query
/// * `output` - Output format (text, json, junit)
/// * `timeout_ms` - Timeout per query in milliseconds
/// * `ai` - Enable AI for failure analysis
/// * `use_testcontainers` - Auto-start Kafka using testcontainers
/// * `kafka` - External Kafka bootstrap servers
/// * `keep_containers` - Keep containers running after completion
/// * `reuse_containers` - Reuse existing containers
/// * `step` - Execute statements one at a time
/// * `data_only` - Generate test data without running SQL
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub async fn run(
    sql_file: PathBuf,
    spec: Option<PathBuf>,
    schemas: Option<PathBuf>,
    query: Option<String>,
    output: String,
    timeout_ms: u64,
    ai: bool,
    use_testcontainers: bool,
    kafka: Option<String>,
    keep_containers: bool,
    reuse_containers: bool,
    step: bool,
    data_only: bool,
    yes: bool,
) -> CommandResult {
    println!("🧪 Velostream SQL Test Harness");
    println!("════════════════════════════════════════");

    // Auto-resolve .sql extension if file doesn't exist
    let sql_file = if !sql_file.exists() && sql_file.extension().is_none() {
        let with_ext = sql_file.with_extension("sql");
        if with_ext.exists() {
            with_ext
        } else {
            sql_file
        }
    } else {
        sql_file
    };

    println!("SQL File: {}", sql_file.display());

    // Auto-discover spec file if not provided
    let spec = spec.or_else(|| {
        let discovered = crate::discovery::discover_spec(&sql_file);
        if let Some(ref p) = discovered {
            println!("📁 Auto-discovered spec: {}", p.display());
        }
        discovered
    });

    // Auto-discover schemas directory if not provided
    let schemas = schemas.or_else(|| {
        let discovered = crate::discovery::discover_schemas(&sql_file);
        if let Some(ref p) = discovered {
            println!("📁 Auto-discovered schemas: {}", p.display());
        }
        discovered
    });

    // Interactive mode: prompt for missing options if not using -y
    let spec = if spec.is_none() && !yes {
        let default_spec = crate::discovery::default_spec_output(&sql_file);
        if crate::discovery::prompt_yes_no("No test spec found. Generate from SQL?", true) {
            None // Will auto-generate later
        } else {
            let path = crate::discovery::prompt_with_default(
                "Spec file path",
                &default_spec.display().to_string(),
            );
            let p = PathBuf::from(path);
            if p.exists() { Some(p) } else { None }
        }
    } else {
        spec
    };

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
                // Offer to save the generated spec
                crate::discovery::offer_save_spec(&sql_file, &s, yes);
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
                            match serde_yaml::from_str::<velostream::velostream::test_harness::Schema>(
                                &content,
                            ) {
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
                                    eprintln!("   ⚠️  Failed to parse {}: {}", path.display(), e);
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

    // Step 2b: Generate schemas from @data.* hints in SQL file
    // Only generate for sources that don't already have a schema file
    {
        use velostream::velostream::test_harness::{DataHintParser, generate_schema_from_hints};

        let sql_content = std::fs::read_to_string(&sql_file).unwrap_or_default();
        let mut hint_parser = DataHintParser::new();

        if hint_parser.parse(&sql_content).is_ok() {
            let field_hints = hint_parser.get_field_hints();
            let global_hints = hint_parser.get_global_hints();

            if !field_hints.is_empty() {
                // Determine source name from hints or default
                let source_name = global_hints
                    .source_name
                    .clone()
                    .unwrap_or_else(|| "generated_source".to_string());

                // Only generate if no schema already exists for this source
                if schema_registry.get(&source_name).is_none() {
                    match generate_schema_from_hints(&source_name, global_hints, &field_hints) {
                        Ok(schema) => {
                            println!(
                                "   ✓ {} ({} fields) ← @data.* hints in SQL",
                                schema.name,
                                schema.fields.len()
                            );
                            schema_registry.register(schema);
                        }
                        Err(e) => {
                            eprintln!("   ⚠️  Failed to generate schema from hints: {}", e);
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
                            match serde_yaml::from_str::<velostream::velostream::test_harness::Schema>(
                                &content,
                            ) {
                                Ok(mut schema) => {
                                    schema.source_path = Some(full_path.display().to_string());
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

    // Try file-only execution (no Kafka needed)
    let spec_dir_path = spec
        .as_ref()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf());
    if crate::run_file_only_execution(
        &sql_file,
        &validation_result,
        timeout_ms,
        Some(&test_spec),
        spec_dir_path.as_deref(),
    )
    .await?
    {
        return Ok(());
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
        if reuse_containers {
            println!("♻️  Looking for reusable Kafka container...");
        } else {
            println!("🐳 Starting Kafka via testcontainers (requires Docker)...");
        }
        match TestHarnessInfra::with_testcontainers_reuse(reuse_containers).await {
            Ok(infra) => {
                let mode = if reuse_containers {
                    "reused"
                } else {
                    "testcontainers"
                };
                println!(
                    "   Kafka: {} ({})",
                    infra.bootstrap_servers().unwrap_or("unknown"),
                    mode
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
        return Ok(());
    } else {
        // Step 4: Create config overrides
        let run_id = format!(
            "{:08x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u32
        );
        // Get SQL file's parent directory for resolving relative config file paths
        let sql_dir = sql_file
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .canonicalize()
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());

        // Configure test infrastructure overrides (bootstrap servers, config paths, etc.)
        let overrides = ConfigOverrideBuilder::new(&run_id)
            .bootstrap_servers(infra.bootstrap_servers().unwrap())
            .base_dir(sql_dir.clone())
            .build();

        // Determine the spec directory for resolving relative file paths
        // If a spec file was provided, use its parent directory; otherwise use the SQL file's directory
        let spec_dir = spec
            .as_ref()
            .and_then(|p| p.parent())
            .unwrap_or(&sql_dir)
            .to_path_buf();

        // Step 5: Create executor with StreamJobServer for SQL execution
        let executor = QueryExecutor::new(infra)
            .with_timeout(Duration::from_millis(timeout_ms))
            .with_overrides(overrides)
            .with_schema_registry(schema_registry)
            .with_spec_dir(&spec_dir);

        // Check if any queries have metric assertions - if so, enable metrics collection
        let has_metric_assertions = test_spec
            .queries
            .iter()
            .any(|q| !q.metric_assertions.is_empty());

        // Initialize StreamJobServer for actual SQL execution (skip in data_only mode)
        // Pass SQL file's parent directory so the server can resolve relative config file paths
        // Enable metrics collection if any queries have metric_assertions
        let mut executor = if data_only {
            println!("   SQL execution: disabled (--data-only mode)");
            println!("   Data will be published to Kafka for external SQL processing");
            executor
        } else {
            match executor
                .with_server_and_observability(Some(sql_dir), has_metric_assertions)
                .await
            {
                Ok(e) => {
                    if has_metric_assertions {
                        println!(
                            "   SQL execution: enabled with metrics (in-process StreamJobServer)"
                        );
                    } else {
                        println!("   SQL execution: enabled (in-process StreamJobServer)");
                    }
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
                    .with_spec_dir(&spec_dir)
                }
            }
        };

        // Step 5b: Load and parse SQL file to get sink topic info
        if let Err(e) = executor.load_sql_file(&sql_file) {
            eprintln!("⚠️  Warning: Failed to parse SQL file: {}", e);
            // Continue anyway - will fall back to default topic naming
        }

        // Handle step mode with StatementExecutor (not available in data_only mode)
        if step && !data_only {
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
                                    // Show table snapshot for CREATE TABLE statements
                                    if let Some(ref snapshot) = result.table_snapshot {
                                        println!(
                                            "      📊 Table: {} rows",
                                            snapshot.stats.record_count
                                        );
                                    }
                                    // Show captured ERROR/WARN logs (top 5 per statement)
                                    if !result.captured_logs.is_empty() {
                                        let log_count = result.captured_logs.len();
                                        let display_count = log_count.min(5);
                                        println!(
                                            "      📋 Logs ({} captured, showing {}):",
                                            log_count, display_count
                                        );
                                        for entry in result.captured_logs.iter().take(5) {
                                            println!("         {}", entry.display_short());
                                        }
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
                                println!("   {} Completed in {}ms", icon, result.execution_time_ms);
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
                                // Show table snapshot for CREATE TABLE statements
                                if let Some(ref snapshot) = result.table_snapshot {
                                    println!(
                                        "      📊 Table: {} rows",
                                        snapshot.stats.record_count
                                    );
                                    if !snapshot.records.is_empty() {
                                        println!("      Sample (first 5):");
                                        for (idx, (key, fields)) in
                                            snapshot.records.iter().take(5).enumerate()
                                        {
                                            let field_str: String = fields
                                                .iter()
                                                .take(4)
                                                .map(|(k, v)| format!("{}={:?}", k, v))
                                                .collect::<Vec<_>>()
                                                .join(", ");
                                            println!(
                                                "         [{}] {} → {{{}}}",
                                                idx + 1,
                                                key,
                                                field_str
                                            );
                                        }
                                    }
                                }
                                // Show captured ERROR/WARN logs (top 5)
                                if !result.captured_logs.is_empty() {
                                    let log_count = result.captured_logs.len();
                                    let display_count = log_count.min(5);
                                    println!(
                                        "      📋 Logs ({} captured, showing {}):",
                                        log_count, display_count
                                    );
                                    for entry in result.captured_logs.iter().take(5) {
                                        println!("         {}", entry.display_short());
                                    }
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
        if data_only {
            println!(
                "📤 Publishing data for {} queries (data-only mode)...",
                queries_to_run.len()
            );
        } else {
            println!("🚀 Running {} queries...", queries_to_run.len());
        }
        println!();

        // Step 7: Execute queries and run assertions (or just publish data in data_only mode)
        let mut report_gen = ReportGenerator::new(&test_spec.application, &run_id);
        let assertion_runner = AssertionRunner::new();

        // Track if we were interrupted
        let mut was_interrupted = false;

        // Data-only mode: just publish inputs without SQL execution
        if data_only {
            let mut total_records = 0usize;
            for query_test in &queries_to_run {
                println!("▶️  Publishing inputs for: {}", query_test.name);

                let publish_result = tokio::select! {
                    result = executor.publish_query_inputs(query_test) => result,
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("\n⚠️  Interrupted - cleaning up...");
                        was_interrupted = true;
                        break;
                    }
                };

                if was_interrupted {
                    break;
                }

                match publish_result {
                    Ok(count) => {
                        total_records += count;
                        println!("   ✅ Published {} input records", count);
                    }
                    Err(e) => {
                        eprintln!("   ❌ Failed to publish: {}", e);
                    }
                }
            }

            if was_interrupted {
                println!("🧹 Cleaning up containers after interrupt...");
                if let Err(e) = executor.stop().await {
                    log::warn!("Failed to stop executor: {}", e);
                }
                println!("✅ Containers cleaned up");
                std::process::exit(130);
            }

            // Summary for data-only mode
            println!();
            println!("════════════════════════════════════════");
            println!("📊 Data-Only Mode Complete");
            println!("════════════════════════════════════════");
            println!("   Total records published: {}", total_records);
            println!("   Topics: Kafka input topics from test spec");
            println!();
            println!("💡 Data is now available for external SQL processing");
            println!("   Use 'velo-sql deploy-app' to run SQL jobs against this data");
            println!();

            // Cleanup
            if !keep_containers {
                if let Err(e) = executor.stop().await {
                    log::warn!("Failed to stop executor: {}", e);
                }
            } else {
                println!("🐳 Keeping containers running (--keep-containers)");
            }

            return Ok(());
        }

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
                        let results = assertion_runner.run_assertions(output, &sink_assertions);
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
                            execution_time_ms: 0,
                            warnings: Vec::new(),
                            memory_peak_bytes: None,
                            memory_growth_bytes: None,
                        };
                        let results =
                            assertion_runner.run_assertions(&empty_output, &all_assertions);
                        assertion_results.extend(results);
                    }

                    // Add metric assertion results (if any)
                    if !exec_result.metric_assertion_results.is_empty() {
                        assertion_results.extend(exec_result.metric_assertion_results.clone());
                    }

                    // Report results - single pass through assertions
                    let total = assertion_results.len();
                    let failed_count = assertion_results.iter().filter(|a| !a.passed).count();

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
                        metric_assertion_results: Vec::new(),
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
        // Always cleanup on interrupt, regardless of keep_containers/reuse_containers flags
        if (keep_containers || reuse_containers) && !was_interrupted {
            if reuse_containers {
                println!();
                println!("♻️  Keeping container running for reuse (--reuse-containers)");
                println!("   Next run with --reuse-containers will be faster");
                // Clean up topics but keep container
                if let Err(e) = executor.stop_with_reuse(true).await {
                    log::warn!("Failed to stop executor with reuse: {}", e);
                }
            } else {
                println!();
                println!("🐳 Keeping containers running for debugging (--keep-containers)");
                println!("   Use 'docker ps' to find containers");
                println!("   Use 'docker stop <id>' to clean up when done");
            }
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
        Ok(())
    }
}
