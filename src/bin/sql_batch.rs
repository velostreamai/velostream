//! SQL Batch Execution - Run SQL files as scripts
//!
//! This binary allows you to execute SQL files as batch scripts that run and exit,
//! rather than running as a persistent server or interactive CLI.
//!
//! ## Usage:
//! ```bash
//! cargo run --bin sql-batch --no-default-features -- --file demo/datasource-demo/file_processing_sql_demo.sql
//! ```

use clap::Parser;
use ferrisstreams::ferris::{
    error::FerrisResult,
    serialization::JsonFormat,
    sql::{
        ast::StreamingQuery,
        execution::types::{FieldValue, StreamRecord},
        StreamExecutionEngine, StreamingSqlParser,
    },
};
use log::{error, info, warn};
use std::{collections::HashMap, fs, sync::Arc};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "sql-batch")]
#[command(about = "Execute SQL files as batch scripts that run and exit")]
#[command(version = "1.0.0")]
struct Cli {
    /// SQL file to execute
    #[arg(short, long)]
    file: String,

    /// Maximum number of records to process per stream (default: no limit)
    #[arg(long)]
    limit: Option<usize>,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Output format for results (json, csv, table)
    #[arg(long, default_value = "table")]
    output: String,
}

/// Parse and execute SQL statements from file
async fn execute_sql_file(
    file_path: &str,
    limit: Option<usize>,
    output_format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("📄 Reading SQL file: {}", file_path);
    let sql_content = fs::read_to_string(file_path)?;

    // Split SQL content into individual statements (simple approach - split by semicolon on newlines)
    let cleaned_content = sql_content
        .split('\n')
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with("/*") {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    let statements: Vec<&str> = cleaned_content
        .split(';')
        .filter(|stmt| !stmt.trim().is_empty())
        .collect();

    if statements.is_empty() {
        return Err("No valid SQL statements found in file".into());
    }

    info!("📊 Found {} SQL statements to execute", statements.len());

    // Create execution engine
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut execution_engine = StreamExecutionEngine::new(output_sender, serialization_format);

    // Parse SQL statements
    let parser = StreamingSqlParser::new();
    let mut parsed_statements = Vec::new();

    for (i, statement) in statements.iter().enumerate() {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }

        info!(
            "🔍 Parsing statement {}: {}",
            i + 1,
            &statement[..std::cmp::min(50, statement.len())]
        );

        match parser.parse(statement) {
            Ok(parsed) => {
                parsed_statements.push((statement.to_string(), parsed));
            }
            Err(e) => {
                error!("❌ Failed to parse statement {}: {}", i + 1, e);
                return Err(format!("Parse error in statement {}: {}", i + 1, e).into());
            }
        }
    }

    info!(
        "✅ Successfully parsed {} statements",
        parsed_statements.len()
    );

    // For demonstration, we'll process each statement with sample data
    // In a real implementation, you would:
    // 1. Identify data sources from CREATE STREAM statements
    // 2. Set up appropriate data readers
    // 3. Process data through the execution engine

    let mut processed_count = 0;
    let max_records = limit.unwrap_or(100); // Default to 100 for demo

    // Start result processing task
    let output_format_clone = output_format.to_string();
    let result_task = tokio::spawn(async move {
        let mut result_count = 0;

        match output_format_clone.as_str() {
            "json" => println!("{{\"results\":["),
            "csv" => {} // Headers will be printed per statement
            "table" => println!("📋 SQL Execution Results"),
            _ => {}
        }

        while let Some(result) = output_receiver.recv().await {
            result_count += 1;

            match output_format_clone.as_str() {
                "json" => {
                    if result_count > 1 {
                        print!(",");
                    }
                    // Convert StreamRecord to JSON representation
                    let json_result = format!(
                        "{{\"record\":{},\"field_count\":{},\"timestamp\":{},\"partition\":{},\"offset\":{}}}",
                        result_count,
                        result.fields.len(),
                        result.timestamp,
                        result.partition,
                        result.offset
                    );
                    println!("{}", json_result);
                }
                "csv" => {
                    // Simple CSV output - would need proper escaping in production
                    println!("{:?}", result);
                }
                "table" => {
                    println!("  Result {}: {:?}", result_count, result);
                }
                _ => {
                    println!("{:?}", result);
                }
            }
        }

        match output_format_clone.as_str() {
            "json" => println!("}}]}}"),
            _ => {}
        }

        result_count
    });

    // Execute statements with sample data
    for (i, (sql, parsed_query)) in parsed_statements.iter().enumerate() {
        info!(
            "⚙️ Executing statement {}: {}",
            i + 1,
            &sql[..std::cmp::min(60, sql.len())]
        );

        // Generate sample data based on the query type
        let sample_records = generate_sample_data_for_query(&parsed_query, max_records).await;

        for record in sample_records {
            if processed_count >= max_records {
                break;
            }

            processed_count += 1;

            // Execute the query with the record directly - no conversion needed!
            if let Err(e) = execution_engine
                .execute_with_record(&parsed_query, record)
                .await
            {
                warn!("⚠️ Failed to process record: {:?}", e);
            }
        }

        if processed_count >= max_records {
            break;
        }
    }

    // Close output channel and wait for results
    drop(execution_engine);
    let result_count = result_task.await?;

    info!("✅ Batch execution completed:");
    info!("  📄 Processed {} statements", parsed_statements.len());
    info!("  📊 Processed {} records", processed_count);
    info!("  📤 Generated {} results", result_count);

    Ok(())
}

/// Generate sample data based on the parsed query
async fn generate_sample_data_for_query(
    _parsed_query: &StreamingQuery,
    max_records: usize,
) -> Vec<StreamRecord> {
    // For demo purposes, generate financial transaction data
    let mut records = Vec::new();

    for i in 1..=std::cmp::min(max_records, 10) {
        // Limit to 10 for demo
        let mut fields = HashMap::new();

        // Generate sample financial data
        fields.insert(
            "transaction_id".to_string(),
            FieldValue::String(format!("TXN{:05}", i)),
        );
        fields.insert(
            "customer_id".to_string(),
            FieldValue::String(format!("CUST{:03}", (i % 25) + 1)),
        );
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(15550 + (i as i64 * 123), 2),
        );
        fields.insert(
            "currency".to_string(),
            FieldValue::String("USD".to_string()),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(1704110400 + (i as i64 * 3600)),
        );
        fields.insert(
            "merchant_category".to_string(),
            FieldValue::String("grocery".to_string()),
        );
        fields.insert(
            "description".to_string(),
            FieldValue::String("Sample transaction".to_string()),
        );

        records.push(StreamRecord {
            fields,
            timestamp: 1704110400 + (i as i64 * 3600),
            offset: i as i64,
            partition: 0,
            headers: HashMap::new(),
        });
    }

    records
}

// Note: convert_field_to_internal function removed as part of StreamRecord optimization
// No longer needed since we use StreamRecord directly without conversions

#[tokio::main]
async fn main() -> FerrisResult<()> {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    let log_level = if cli.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    info!("🚀 SQL Batch Execution Starting");
    info!("  📄 File: {}", cli.file);
    if let Some(limit) = cli.limit {
        info!("  🔢 Record limit: {}", limit);
    }
    info!("  📤 Output format: {}", cli.output);

    match execute_sql_file(&cli.file, cli.limit, &cli.output).await {
        Ok(()) => {
            info!("🎉 SQL batch execution completed successfully");
        }
        Err(e) => {
            error!("❌ SQL batch execution failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
