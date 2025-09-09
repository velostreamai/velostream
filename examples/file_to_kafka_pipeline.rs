//! File to Kafka Pipeline Example
//!
//! This example demonstrates how to process CSV files and stream the data to Kafka
//! using the pluggable data sources architecture.
//!
//! Usage:
//! ```bash
//! cargo run --example file_to_kafka_pipeline -- \
//!   --input "file:///data/orders.csv?format=csv&header=true" \
//!   --output "kafka://localhost:9092/processed-orders"
//! ```

use clap::Parser;
use ferrisstreams::ferris::datasource::config::SourceConfig;
use ferrisstreams::ferris::datasource::{
    create_sink, create_source, file::FileDataSource, registry::register_global_source,
};
use std::error::Error;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input data source URI
    #[arg(short, long)]
    input: String,

    /// Output data sink URI
    #[arg(short, long)]
    output: String,

    /// Batch size for processing
    #[arg(short, long, default_value_t = 100)]
    batch_size: usize,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

/// Initialize the datasource registry with file source factory
fn initialize_registry() {
    register_global_source("file", |config: SourceConfig| {
        // The factory function gets called with the parsed SourceConfig
        // We need to create and initialize the FileDataSource here
        let source = PreInitializedFileDataSource { config };
        Ok(Box::new(source) as Box<dyn ferrisstreams::ferris::datasource::DataSource>)
    });
}

/// Wrapper that holds config until initialize() is called
struct PreInitializedFileDataSource {
    config: SourceConfig,
}

#[async_trait::async_trait]
impl ferrisstreams::ferris::datasource::DataSource for PreInitializedFileDataSource {
    async fn initialize(
        &mut self,
        _config: SourceConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Already have the config from factory
        Ok(())
    }

    async fn fetch_schema(
        &self,
    ) -> Result<ferrisstreams::ferris::schema::Schema, Box<dyn Error + Send + Sync>> {
        // Create and initialize a temporary FileDataSource to get schema
        let mut temp_source = FileDataSource::new();
        temp_source.initialize(self.config.clone()).await?;
        temp_source.fetch_schema().await
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn ferrisstreams::ferris::datasource::DataReader>, Box<dyn Error + Send + Sync>>
    {
        // Create and initialize a FileDataSource for reading
        let mut source = FileDataSource::new();
        source.initialize(self.config.clone()).await?;
        source.create_reader().await
    }

    fn supports_streaming(&self) -> bool {
        true // File sources support streaming
    }

    fn supports_batch(&self) -> bool {
        true // File sources support batch reading
    }

    fn metadata(&self) -> ferrisstreams::ferris::datasource::SourceMetadata {
        ferrisstreams::ferris::datasource::SourceMetadata {
            source_type: "file".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec!["read".to_string(), "batch".to_string()],
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize datasource registry
    initialize_registry();

    let args = Args::parse();

    // Initialize logging
    if args.verbose {
        env_logger::init();
        log::info!("Starting file to Kafka pipeline");
        log::info!("Input: {}", args.input);
        log::info!("Output: {}", args.output);
    }

    // Create source and sink
    let source = create_source(&args.input)?;
    let sink = create_sink(&args.output)?;

    // Get source metadata
    let source_metadata = source.metadata();
    println!(
        "📊 Source: {} v{}",
        source_metadata.source_type, source_metadata.version
    );
    println!("   Capabilities: {:?}", source_metadata.capabilities);

    // Get sink metadata
    let sink_metadata = sink.metadata();
    println!(
        "📤 Sink: {} v{}",
        sink_metadata.sink_type, sink_metadata.version
    );
    println!("   Capabilities: {:?}", sink_metadata.capabilities);

    // Discover and display schema
    let schema = source.fetch_schema().await?;
    println!("\n📋 Schema discovered: {} fields", schema.fields.len());
    for field in &schema.fields {
        println!(
            "   {}: {:?} (nullable: {})",
            field.name, field.data_type, field.nullable
        );
    }

    // Validate schema compatibility
    sink.validate_schema(&schema).await?;
    println!("✅ Schema validation passed");

    // Create reader and writer
    let mut reader = source.create_reader().await?;
    let mut writer = sink.create_writer().await?;

    // Process data
    println!("\n🔄 Starting data processing...");
    let mut total_records = 0;
    let mut batch_count = 0;

    loop {
        // Read batch
        let batch = reader.read().await?;
        if batch.is_empty() {
            break;
        }

        batch_count += 1;
        let batch_size = batch.len();
        total_records += batch_size;

        if args.verbose {
            log::info!(
                "Processing batch {} with {} records",
                batch_count,
                batch_size
            );
        }

        // Transform records (example: add processing timestamp)
        let transformed_batch: Vec<_> = batch
            .into_iter()
            .map(|mut record| {
                // Add metadata fields
                record.fields.insert(
                    "processed_at".to_string(),
                    ferrisstreams::ferris::sql::execution::types::FieldValue::String(
                        chrono::Utc::now().to_rfc3339(),
                    ),
                );
                record.fields.insert(
                    "batch_id".to_string(),
                    ferrisstreams::ferris::sql::execution::types::FieldValue::String(
                        batch_count.to_string(),
                    ),
                );
                record
            })
            .collect();

        // Write batch
        writer.write_batch(transformed_batch).await?;

        // Progress update
        if !args.verbose && total_records % 1000 == 0 {
            print!("\r📊 Processed {} records...", total_records);
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }

    // Finalize
    writer.flush().await?;
    writer.commit().await?;

    println!("\n✅ Pipeline completed successfully!");
    println!("📊 Total records processed: {}", total_records);
    println!("📦 Total batches: {}", batch_count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_csv_processing() {
        // Initialize registry for the test
        initialize_registry();

        // Create test CSV file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,name,value").unwrap();
        writeln!(file, "1,Alice,100").unwrap();
        writeln!(file, "2,Bob,200").unwrap();
        writeln!(file, "3,Charlie,300").unwrap();

        // Test with mock sink (would need mock implementation)
        let input_uri = format!(
            "file://{}?format=csv&header=true",
            file.path().to_string_lossy()
        );

        let source = create_source(&input_uri).unwrap();
        let schema = source.fetch_schema().await.unwrap();

        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
        assert_eq!(schema.fields[2].name, "value");
    }
}
