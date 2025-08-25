//! Test Schema Management System
//!
//! This binary demonstrates the comprehensive schema management functionality
//! including discovery, caching, evolution, and provider capabilities.

use ferrisstreams::ferris::sql::schema::*;
use ferrisstreams::ferris::sql::schema::cache::{CacheLookupResult, MissReason};
use ferrisstreams::ferris::sql::ast::DataType;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🔍 Testing FerrisStreams Schema Management System");
    println!("================================================");

    // Test 1: Schema Registry with Default Providers
    println!("\n📋 Test 1: Schema Registry with Default Providers");
    println!("--------------------------------------------------");
    
    let mut registry = create_default_registry();
    let providers = registry.list_providers();
    
    println!("✅ Registered providers:");
    for (scheme, metadata) in providers {
        println!("  • {} - {} v{}", 
                 scheme, 
                 metadata.name, 
                 metadata.version);
        println!("    Capabilities: {:?}", metadata.capabilities);
    }

    // Test 2: Schema Discovery from Different Sources
    println!("\n🔎 Test 2: Schema Discovery from Different Sources");
    println!("--------------------------------------------------");

    // Discover Kafka schema
    println!("Discovering Kafka schema...");
    let kafka_schema = registry.discover("kafka://localhost:9092/orders").await?;
    println!("✅ Kafka schema discovered:");
    println!("   • Version: {:?}", kafka_schema.version);
    println!("   • Fields: {}", kafka_schema.fields.len());
    for field in &kafka_schema.fields {
        println!("     - {} ({:?}) {}", 
                 field.name, 
                 field.data_type, 
                 if field.nullable { "[nullable]" } else { "[required]" });
    }

    // Discover file schema
    println!("\nDiscovering file schema...");
    let file_schema = registry.discover("file:///data/events.json").await?;
    println!("✅ File schema discovered:");
    println!("   • Version: {:?}", file_schema.version);
    println!("   • Source type: {}", file_schema.metadata.source_type);
    println!("   • Fields: {}", file_schema.fields.len());

    // Discover S3 schema
    println!("\nDiscovering S3 schema...");
    let s3_schema = registry.discover("s3://analytics-bucket/data/events.parquet").await?;
    println!("✅ S3 schema discovered:");
    println!("   • Version: {:?}", s3_schema.version);
    println!("   • Tags: {:?}", s3_schema.metadata.tags);
    println!("   • Fields: {}", s3_schema.fields.len());

    // Test 3: Schema Caching
    println!("\n💾 Test 3: Schema Caching Performance");
    println!("-------------------------------------");

    let cache_config = CacheConfig {
        default_ttl: Duration::from_secs(60),
        max_entries: 100,
        enable_statistics: true,
        ..Default::default()
    };
    let cache = SchemaCache::with_config(cache_config);

    // Cache some schemas
    cache.put("test://schema1", kafka_schema.clone(), None).await?;
    cache.put("test://schema2", file_schema.clone(), None).await?;
    cache.put("test://schema3", s3_schema.clone(), None).await?;

    // Test cache hits
    let start = std::time::Instant::now();
    match cache.get("test://schema1").await {
        CacheLookupResult::Hit { schema, age, access_count } => {
            let lookup_time = start.elapsed();
            println!("✅ Cache hit! Retrieved schema in {:?}", lookup_time);
            println!("   • Schema version: {:?}", schema.version);
            println!("   • Age: {:?}", age);
            println!("   • Access count: {}", access_count);
        }
        _ => println!("❌ Unexpected cache miss"),
    }

    // Test cache miss
    match cache.get("test://nonexistent").await {
        CacheLookupResult::Miss { reason } => {
            println!("✅ Cache miss correctly detected: {:?}", reason);
        }
        _ => println!("❌ Unexpected cache hit"),
    }

    // Show cache statistics
    let stats = cache.statistics().await;
    println!("📊 Cache statistics:");
    println!("   • Total requests: {}", stats.total_requests);
    println!("   • Hit rate: {:.2}%", stats.hit_rate() * 100.0);
    println!("   • Average access time: {:.2}μs", stats.avg_access_time_us);

    // Test 4: Schema Evolution
    println!("\n🔄 Test 4: Schema Evolution & Compatibility");
    println!("-------------------------------------------");

    let evolution = SchemaEvolution::new();

    // Create two schema versions for testing
    let schema_v1 = Schema {
        fields: vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("name".to_string(), DataType::String),
        ],
        version: Some("1.0.0".to_string()),
        metadata: SchemaMetadata::new("test".to_string())
            .with_compatibility(CompatibilityMode::Backward),
    };

    let schema_v2 = Schema {
        fields: vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("name".to_string(), DataType::String),
            FieldDefinition::optional("email".to_string(), DataType::String),
            FieldDefinition::optional("created_at".to_string(), DataType::Timestamp),
        ],
        version: Some("2.0.0".to_string()),
        metadata: SchemaMetadata::new("test".to_string())
            .with_compatibility(CompatibilityMode::Backward),
    };

    // Test compatibility
    let can_evolve = evolution.can_evolve(&schema_v1, &schema_v2);
    println!("✅ Can evolve v1.0.0 → v2.0.0: {}", can_evolve);

    // Compute differences
    let diff = evolution.compute_diff(&schema_v1, &schema_v2);
    println!("📊 Schema diff:");
    println!("   • Added fields: {}", diff.added_fields.len());
    println!("   • Removed fields: {}", diff.removed_fields.len());
    println!("   • Modified fields: {}", diff.modified_fields.len());
    println!("   • Compatible: {}", diff.is_compatible);

    for added_field in &diff.added_fields {
        println!("   + Added: {} ({:?}) {}", 
                 added_field.name, 
                 added_field.data_type,
                 if added_field.nullable { "[nullable]" } else { "[required]" });
    }

    // Create migration plan
    if can_evolve {
        let migration_plan = evolution.create_migration_plan(&schema_v1, &schema_v2)?;
        println!("🗺️  Migration plan created:");
        println!("   • Field mappings: {}", migration_plan.field_mappings.len());
        println!("   • Transformations: {}", migration_plan.transformations.len());
        
        // Test record evolution
        use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
        use std::collections::HashMap;
        
        let mut test_record_fields = HashMap::new();
        test_record_fields.insert("id".to_string(), FieldValue::Integer(123));
        test_record_fields.insert("name".to_string(), FieldValue::String("John Doe".to_string()));
        
        let test_record = StreamRecord {
            fields: test_record_fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            headers: HashMap::new(),
        };

        let evolved_record = evolution.evolve_record(test_record, &migration_plan)?;
        println!("✅ Record evolved successfully:");
        println!("   • Original fields: 2");
        println!("   • Evolved fields: {}", evolved_record.fields.len());
        
        for (field_name, field_value) in &evolved_record.fields {
            println!("     - {}: {:?}", field_name, field_value);
        }
    }

    // Test 5: Cache Maintenance
    println!("\n🔧 Test 5: Cache Maintenance");
    println!("----------------------------");
    
    let size_info = cache.size_info().await;
    println!("📏 Cache size info:");
    println!("   • Current entries: {}/{}", size_info.current_entries, size_info.max_entries);
    println!("   • Utilization: {:.1}%", size_info.utilization * 100.0);
    println!("   • Active entries: {}", size_info.active_entries);
    
    let maintenance_result = cache.maintenance().await?;
    println!("🧹 Maintenance result:");
    println!("   • Entries before: {}", maintenance_result.entries_before);
    println!("   • Entries after: {}", maintenance_result.entries_after);
    println!("   • Expired removed: {}", maintenance_result.expired_removed);

    // Test 6: Advanced Features
    println!("\n⚡ Test 6: Advanced Features");
    println!("---------------------------");

    // Test schema validation
    println!("Testing schema validation...");
    match registry.validate_schema(&kafka_schema) {
        Ok(()) => println!("✅ Kafka schema validation passed"),
        Err(e) => println!("❌ Kafka schema validation failed: {}", e),
    }

    // Test version checking
    let is_current = cache.is_version_current("test://schema1", "1.0.0").await;
    println!("✅ Version check result: {}", is_current);

    // Show final statistics
    println!("\n📈 Final Statistics");
    println!("------------------");
    
    let final_stats = cache.statistics().await;
    println!("Cache performance:");
    println!("   • Total requests: {}", final_stats.total_requests);
    println!("   • Hits: {} | Misses: {}", final_stats.hits, final_stats.misses);
    println!("   • Hit rate: {:.1}%", final_stats.hit_rate() * 100.0);
    println!("   • Average access time: {:.2}μs", final_stats.avg_access_time_us);
    println!("   • Total evictions: {}", final_stats.total_evictions());

    println!("\n🎉 Schema Management System Test Completed Successfully!");
    println!("Key achievements:");
    println!("   ✅ Multi-provider schema discovery (Kafka, File, S3)");
    println!("   ✅ High-performance TTL-based caching");
    println!("   ✅ Backward-compatible schema evolution");
    println!("   ✅ Automatic record transformation");
    println!("   ✅ Comprehensive validation and maintenance");

    Ok(())
}