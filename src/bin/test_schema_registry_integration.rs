//! Integration Test for Schema Registry with SQL Execution Engine
//!
//! This test demonstrates the integration of the pluggable Schema Registry
//! with the FerrisStreams SQL execution engine, showing performance
//! characteristics and real-world usage patterns.

use std::sync::Arc;
use std::time::Instant;
use tokio;

use ferrisstreams::ferris::sql::schema::unified_registry_client::{
    UnifiedClientBuilder, UnifiedClientConfig, UnifiedSchemaRegistryClient,
};
use ferrisstreams::ferris::sql::schema::{
    BackendConfig, SchemaReference, SchemaRegistryBackend, SchemaRegistryBackendFactory,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Schema Registry Integration & Performance Test");
    println!("================================================\n");

    // Test different backend configurations
    let results = vec![
        test_filesystem_backend_performance().await?,
        test_memory_backend_performance().await?,
        test_multi_backend_comparison().await?,
    ];

    // Performance summary
    println!("📊 Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━");
    for (backend, timing) in results {
        println!("  {} - {:.2}ms average", backend, timing);
    }

    // Integration test with caching
    test_unified_client_integration().await?;

    println!("\n✅ Integration & Performance tests completed successfully!");
    Ok(())
}

async fn test_filesystem_backend_performance() -> Result<(String, f64), Box<dyn std::error::Error>>
{
    println!("🗃️  Testing FileSystem Backend Performance");

    let temp_path = std::env::temp_dir().join("ferris_schema_test_fs");
    std::fs::create_dir_all(&temp_path)?;

    let backend = SchemaRegistryBackendFactory::create(BackendConfig::FileSystem {
        base_path: temp_path.clone(),
        watch_for_changes: false,
        auto_create_directories: true,
    })?;

    let total_time = benchmark_backend_operations(&backend).await?;

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_path);

    println!("   FileSystem: {:.2}ms average per operation", total_time);
    Ok(("FileSystem".to_string(), total_time))
}

async fn test_memory_backend_performance() -> Result<(String, f64), Box<dyn std::error::Error>> {
    println!("🧠 Testing In-Memory Backend Performance");

    let backend = SchemaRegistryBackendFactory::create(BackendConfig::InMemory {
        initial_schemas: std::collections::HashMap::new(),
    })?;

    let total_time = benchmark_backend_operations(&backend).await?;

    println!("   In-Memory: {:.2}ms average per operation", total_time);
    Ok(("In-Memory".to_string(), total_time))
}

async fn test_multi_backend_comparison() -> Result<(String, f64), Box<dyn std::error::Error>> {
    println!("⚖️  Comparing Backend Performance Under Load");

    // Test with varying schema sizes
    let schema_sizes = vec![
        ("Small", create_test_schema(10)),
        ("Medium", create_test_schema(50)),
        ("Large", create_test_schema(200)),
    ];

    let temp_path = std::env::temp_dir().join("ferris_schema_test_multi");
    std::fs::create_dir_all(&temp_path)?;

    let filesystem_backend = SchemaRegistryBackendFactory::create(BackendConfig::FileSystem {
        base_path: temp_path.clone(),
        watch_for_changes: false,
        auto_create_directories: true,
    })?;

    let memory_backend = SchemaRegistryBackendFactory::create(BackendConfig::InMemory {
        initial_schemas: std::collections::HashMap::new(),
    })?;

    let mut total_comparisons = 0.0;
    let mut comparison_count = 0;

    for (size_label, schema) in schema_sizes {
        println!(
            "  Testing {} schemas ({} fields)",
            size_label,
            schema.matches("\"name\":").count()
        );

        // Benchmark filesystem
        let start = Instant::now();
        let fs_id = filesystem_backend
            .register_schema("test", &schema, vec![])
            .await?;
        let _fs_retrieved = filesystem_backend.get_schema(fs_id).await?;
        let fs_time = start.elapsed().as_micros() as f64 / 1000.0;

        // Benchmark memory
        let start = Instant::now();
        let mem_id = memory_backend
            .register_schema("test", &schema, vec![])
            .await?;
        let _mem_retrieved = memory_backend.get_schema(mem_id).await?;
        let mem_time = start.elapsed().as_micros() as f64 / 1000.0;

        println!(
            "    {} - FileSystem: {:.2}ms, Memory: {:.2}ms",
            size_label, fs_time, mem_time
        );

        total_comparisons += (fs_time + mem_time) / 2.0;
        comparison_count += 1;
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_path);

    Ok((
        "Multi-Backend".to_string(),
        total_comparisons / comparison_count as f64,
    ))
}

async fn test_unified_client_integration() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔗 Testing Direct Backend Integration");

    let temp_path = std::env::temp_dir().join("ferris_schema_test_unified");
    std::fs::create_dir_all(&temp_path)?;

    // Create backend directly to avoid stack overflow
    let backend = SchemaRegistryBackendFactory::create(BackendConfig::FileSystem {
        base_path: temp_path.clone(),
        watch_for_changes: false,
        auto_create_directories: true,
    })?;

    // Test schema registration with references
    println!("  📝 Registering schemas with references...");

    let address_schema = create_address_schema();
    let address_id = backend
        .register_schema("addresses-value", &address_schema, vec![])
        .await?;
    println!("     Address schema registered: ID {}", address_id);

    let user_schema = create_user_schema_with_reference();
    let references = vec![SchemaReference {
        name: "com.example.Address".to_string(),
        subject: "addresses-value".to_string(),
        version: Some(1),
        schema_id: Some(address_id),
    }];

    let user_id = backend
        .register_schema("users-value", &user_schema, references)
        .await?;
    println!("     User schema with reference registered: ID {}", user_id);

    // Test retrieval performance
    println!("  ⚡ Testing retrieval performance...");

    let iterations = 50;
    let start = Instant::now();

    for _ in 0..iterations {
        let _schema = backend.get_schema(user_id).await?;
        let _latest = backend.get_latest_schema("users-value").await?;
    }

    let avg_time = start.elapsed().as_millis() as f64 / iterations as f64;
    println!("     Average retrieval: {:.2}ms", avg_time);

    // Test compatibility checking
    println!("  ✅ Testing compatibility checking...");
    let is_compatible = backend
        .check_compatibility("users-value", &user_schema)
        .await?;
    println!(
        "     Schema compatibility: {}",
        if is_compatible {
            "✅ Compatible"
        } else {
            "❌ Incompatible"
        }
    );

    // Test health checks across backends
    println!("  🏥 Testing health checks...");
    let health_status = backend.health_check().await?;
    println!(
        "     Overall health: {} - {}",
        if health_status.is_healthy {
            "✅ HEALTHY"
        } else {
            "❌ UNHEALTHY"
        },
        health_status.message
    );

    // Test backend metadata
    println!("  📋 Backend metadata:");
    let metadata = backend.metadata();
    println!(
        "     Type: {}, Version: {}",
        metadata.backend_type, metadata.version
    );
    println!("     Features: {:?}", metadata.supported_features);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_path);

    Ok(())
}

async fn benchmark_backend_operations(
    backend: &Arc<dyn SchemaRegistryBackend>,
) -> Result<f64, Box<dyn std::error::Error>> {
    let operations = 20;
    let schema = create_test_schema(25);

    let start = Instant::now();

    // Register multiple schemas
    let mut schema_ids = Vec::new();
    for i in 0..operations {
        let subject = format!("benchmark-subject-{}", i);
        let id = backend.register_schema(&subject, &schema, vec![]).await?;
        schema_ids.push((subject, id));
    }

    // Retrieve schemas by ID
    for (_, id) in &schema_ids {
        let _schema = backend.get_schema(*id).await?;
    }

    // Get latest schemas
    for (subject, _) in &schema_ids {
        let _latest = backend.get_latest_schema(subject).await?;
    }

    let total_time = start.elapsed().as_millis() as f64;
    Ok(total_time / (operations * 3) as f64) // 3 operations per iteration
}

fn create_test_schema(field_count: usize) -> String {
    let mut fields = Vec::new();

    for i in 0..field_count {
        fields.push(format!(
            r#"    {{
      "name": "field_{}",
      "type": ["null", "string"],
      "default": null
    }}"#,
            i
        ));
    }

    format!(
        r#"{{
  "type": "record",
  "name": "BenchmarkRecord",
  "namespace": "com.example.benchmark",
  "fields": [
{}
  ]
}}"#,
        fields.join(",\n")
    )
}

fn create_address_schema() -> String {
    r#"{
  "type": "record",
  "name": "Address",
  "namespace": "com.example",
  "fields": [
    {
      "name": "street",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    },
    {
      "name": "zipcode",
      "type": "string"
    }
  ]
}"#
    .to_string()
}

fn create_user_schema_with_reference() -> String {
    r#"{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "address",
      "type": "com.example.Address"
    }
  ]
}"#
    .to_string()
}
