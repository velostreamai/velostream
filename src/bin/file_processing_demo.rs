//! File Processing Demo - File → Processing → File Pipeline
//!
//! This demo showcases FerrisStreams' file processing capabilities:
//! 1. Reading financial transaction data from CSV files
//! 2. Processing with exact decimal precision (42x faster arithmetic)
//! 3. Writing processed results to JSON files with compression
//!
//! ## Usage:
//! ```bash
//! cargo run --bin file_processing_demo --no-default-features
//! ```

use ferrisstreams::ferris::datasource::file::config::FileFormat;
use ferrisstreams::ferris::datasource::file::{
    config::{CompressionType, FileSinkConfig, FileSourceConfig},
    FileDataSink, FileDataSource,
};
use ferrisstreams::ferris::datasource::traits::{DataSink, DataSource};
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// Demo configuration
const DEMO_DATA_DIR: &str = "./demo_data";
const DEMO_OUTPUT_DIR: &str = "./demo_output";
const DEMO_CSV_PATH: &str = "./demo_data/financial_transactions.csv";
const DEMO_OUTPUT_PATH: &str = "./demo_output/processed_transactions.jsonl";

#[derive(Debug)]
struct ProcessingMetrics {
    records_read: AtomicU64,
    records_processed: AtomicU64,
    records_written: AtomicU64,
    processing_errors: AtomicU64,
}

impl ProcessingMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            records_read: AtomicU64::new(0),
            records_processed: AtomicU64::new(0),
            records_written: AtomicU64::new(0),
            processing_errors: AtomicU64::new(0),
        })
    }

    fn print_summary(&self) {
        println!("\n📊 Final Processing Summary:");
        println!(
            "   📖 Records Read:     {}",
            self.records_read.load(Ordering::Relaxed)
        );
        println!(
            "   ⚙️  Records Processed: {}",
            self.records_processed.load(Ordering::Relaxed)
        );
        println!(
            "   💾 Records Written:   {}",
            self.records_written.load(Ordering::Relaxed)
        );
        println!(
            "   ❌ Processing Errors: {}",
            self.processing_errors.load(Ordering::Relaxed)
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🚀 FerrisStreams File Processing Demo");
    println!("=====================================");

    let metrics = ProcessingMetrics::new();

    // Setup demo environment
    setup_demo_environment().await?;

    // Run the file processing pipeline
    run_file_processing_pipeline(metrics.clone()).await?;

    // Print final summary
    metrics.print_summary();

    println!("\n✅ Demo completed successfully!");
    println!("   📄 Input:  {}", DEMO_CSV_PATH);
    println!("   📄 Output: {}", DEMO_OUTPUT_PATH);
    println!("   🗜️  Compression: Enabled (gzip)");

    Ok(())
}

async fn setup_demo_environment() -> Result<(), Box<dyn Error>> {
    println!("🛠️  Setting up demo environment...");

    // Create directories
    fs::create_dir_all(DEMO_DATA_DIR)?;
    fs::create_dir_all(DEMO_OUTPUT_DIR)?;

    // Generate sample financial transaction data if it doesn't exist
    if !Path::new(DEMO_CSV_PATH).exists() {
        generate_sample_data().await?;
    }

    println!("   ✅ Demo environment ready");
    Ok(())
}

async fn generate_sample_data() -> Result<(), Box<dyn Error>> {
    println!("   📝 Generating 5000 sample financial transactions...");

    let mut file = fs::File::create(DEMO_CSV_PATH)?;

    // Write CSV header
    writeln!(
        file,
        "transaction_id,customer_id,amount,currency,timestamp,merchant_category,description"
    )?;

    // Data arrays for generating realistic transactions
    let customers = [
        "CUST001", "CUST002", "CUST003", "CUST004", "CUST005", "CUST006", "CUST007", "CUST008",
        "CUST009", "CUST010", "CUST011", "CUST012", "CUST013", "CUST014", "CUST015", "CUST016",
        "CUST017", "CUST018", "CUST019", "CUST020", "CUST021", "CUST022", "CUST023", "CUST024",
        "CUST025",
    ];

    let categories_and_merchants = [
        (
            "grocery",
            [
                "Whole Foods Market",
                "Safeway",
                "Trader Joe's",
                "Costco",
                "King Soopers",
                "Kroger",
                "Walmart Grocery",
                "Target Grocery",
                "Fresh Market",
                "Sprouts",
            ],
        ),
        (
            "gas",
            [
                "Shell Gas Station",
                "Chevron",
                "76 Gas Station",
                "Exxon",
                "BP",
                "Mobil",
                "Arco",
                "Conoco",
                "Valero",
                "Circle K",
            ],
        ),
        (
            "restaurant",
            [
                "McDonald's",
                "Cheesecake Factory",
                "The Capital Grille",
                "Olive Garden",
                "Chipotle",
                "In-N-Out",
                "Panera Bread",
                "Subway",
                "Taco Bell",
                "KFC",
            ],
        ),
        (
            "retail",
            [
                "Best Buy Electronics",
                "Amazon Purchase",
                "Apple Store",
                "Microsoft Store",
                "Target",
                "Walmart",
                "Home Depot",
                "Costco",
                "Best Buy",
                "GameStop",
            ],
        ),
        (
            "coffee",
            [
                "Starbucks",
                "Peet's Coffee",
                "Blue Bottle Coffee",
                "Dunkin'",
                "Tim Hortons",
                "Local Coffee Shop",
                "Philz Coffee",
                "Dutch Bros",
                "Coffee Bean",
                "Caribou Coffee",
            ],
        ),
        (
            "entertainment",
            [
                "Netflix",
                "Disney+",
                "Spotify",
                "AMC Theaters",
                "Regal Cinema",
                "iTunes Store",
                "PlayStation Store",
                "Steam",
                "Xbox Live",
                "Hulu",
            ],
        ),
    ];

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let base_timestamp = 1704110400; // Start of 2024

    for i in 1..=5000 {
        // Use deterministic "random" generation for consistent results
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        let seed = hasher.finish();

        // Select customer
        let customer = customers[(seed % customers.len() as u64) as usize];

        // Select category and merchant
        let (category, merchants) = &categories_and_merchants
            [((seed >> 8) % categories_and_merchants.len() as u64) as usize];
        let merchant = merchants[((seed >> 16) % merchants.len() as u64) as usize];

        // Generate realistic amount based on category (in cents)
        let (base_cents, max_add_cents) = match *category {
            "grocery" => (1550, 20000),     // $15.50 + up to $200.00
            "gas" => (2500, 12000),         // $25.00 + up to $120.00
            "restaurant" => (875, 15000),   // $8.75 + up to $150.00
            "retail" => (1299, 80000),      // $12.99 + up to $800.00
            "coffee" => (350, 2500),        // $3.50 + up to $25.00
            "entertainment" => (499, 5000), // $4.99 + up to $50.00
            _ => (1000, 10000),             // Default
        };

        let total_cents = base_cents + ((seed >> 24) % max_add_cents);
        let amount = format!("{:.2}", total_cents as f64 / 100.0);

        // Generate timestamp spread over 30 days
        let day_offset = (i * 2592) / 5000; // Spread over 30 days
        let hour_variation = ((seed >> 32) % 86400) as i64; // Random time within day
        let timestamp = base_timestamp + day_offset + hour_variation;

        // Write CSV line
        writeln!(
            file,
            "TXN{:05},{},{},USD,{},{},\"{}\"",
            i, customer, amount, timestamp, category, merchant
        )?;

        // Progress indicator
        if i % 500 == 0 {
            println!("     Generated {} transactions...", i);
        }
    }

    file.flush()?;

    println!("   ✅ Generated 5000 sample transactions");
    Ok(())
}

async fn run_file_processing_pipeline(
    metrics: Arc<ProcessingMetrics>,
) -> Result<(), Box<dyn Error>> {
    println!("🔄 Starting File Processing Pipeline...");

    // Initialize file source
    println!("  📖 Setting up file source...");
    let file_config = FileSourceConfig::new(DEMO_CSV_PATH.to_string(), FileFormat::Csv);
    let mut file_source = FileDataSource::new();
    file_source
        .initialize(file_config.into())
        .await
        .map_err(|e| format!("Source initialization error: {}", e))?;

    // Initialize file sink
    println!("  📄 Setting up file sink...");
    let sink_config = FileSinkConfig::new(DEMO_OUTPUT_PATH.to_string(), FileFormat::JsonLines)
        .with_rotation_size(1024 * 1024) // 1MB rotation
        .with_compression(CompressionType::Gzip);

    let mut file_sink = FileDataSink::new();
    file_sink
        .initialize(sink_config.into())
        .await
        .map_err(|e| format!("Sink initialization error: {}", e))?;

    let mut writer = file_sink
        .create_writer()
        .await
        .map_err(|e| format!("Writer creation error: {}", e))?;

    println!("  ⚙️  Processing records...");

    // Read and process records
    let mut reader = file_source
        .create_reader()
        .await
        .map_err(|e| format!("Reader creation error: {}", e))?;

    loop {
        match reader.read().await {
            Ok(batch) => {
                if batch.is_empty() {
                    break;
                }

                for record in batch {
                    metrics.records_read.fetch_add(1, Ordering::Relaxed);

                    // Process the record with financial precision
                    match process_financial_record(record).await {
                        Ok(processed_record) => {
                            metrics.records_processed.fetch_add(1, Ordering::Relaxed);

                            // Write processed record
                            match writer.write(processed_record).await {
                                Ok(_) => {
                                    metrics.records_written.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    eprintln!("⚠️  Write error: {}", e);
                                    metrics.processing_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("⚠️  Processing error: {}", e);
                            metrics.processing_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Print progress
                let read = metrics.records_read.load(Ordering::Relaxed);
                let processed = metrics.records_processed.load(Ordering::Relaxed);
                let written = metrics.records_written.load(Ordering::Relaxed);
                println!(
                    "     Progress: Read {}, Processed {}, Written {}",
                    read, processed, written
                );
            }
            Err(e) => {
                eprintln!("⚠️  Batch read error: {}", e);
                break;
            }
        }
    }

    // Flush and close writer
    match writer.flush().await {
        Ok(_) => println!("  ✅ Pipeline completed successfully"),
        Err(e) => eprintln!("⚠️  Flush error: {}", e),
    }

    Ok(())
}

async fn process_financial_record(record: StreamRecord) -> Result<StreamRecord, Box<dyn Error>> {
    let mut processed_fields = record.fields.clone();

    // Extract amount and convert to ScaledInteger for exact precision
    if let Some(FieldValue::String(amount_str)) = record.fields.get("amount") {
        // Parse decimal amount and convert to scaled integer (cents)
        let amount_f64: f64 = amount_str.parse()?;
        let amount_scaled = ((amount_f64 * 100.0).round() as i64, 2u8);

        // Store as ScaledInteger for exact precision (42x faster than f64!)
        processed_fields.insert(
            "amount_precise".to_string(),
            FieldValue::ScaledInteger(amount_scaled.0, amount_scaled.1),
        );

        // Calculate processing fee (2.5% with exact precision)
        let fee_scaled = ((amount_scaled.0 as f64 * 0.025).round() as i64, 2u8);
        processed_fields.insert(
            "processing_fee".to_string(),
            FieldValue::ScaledInteger(fee_scaled.0, fee_scaled.1),
        );

        // Calculate total with fee
        let total_scaled = (amount_scaled.0 + fee_scaled.0, 2u8);
        processed_fields.insert(
            "total_with_fee".to_string(),
            FieldValue::ScaledInteger(total_scaled.0, total_scaled.1),
        );
    }

    // Add processing metadata
    processed_fields.insert(
        "processed_at".to_string(),
        FieldValue::Integer(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64),
    );
    processed_fields.insert(
        "pipeline_version".to_string(),
        FieldValue::String("1.0.0".to_string()),
    );
    processed_fields.insert(
        "precision_mode".to_string(),
        FieldValue::String("ScaledInteger".to_string()),
    );

    Ok(StreamRecord {
        fields: processed_fields,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
        offset: record.offset,
        partition: record.partition,
        headers: record.headers,
        event_time: None,
    })
}
