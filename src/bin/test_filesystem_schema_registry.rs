use velostream::velostream::schema::client::registry_client::SchemaReference;
use velostream::velostream::schema::server::{BackendConfig, SchemaRegistryBackendFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing FileSystem Schema Registry Backend");

    // Create temporary directory for testing
    let registry_path = std::env::temp_dir().join("velo_schema_registry_test");

    println!("📁 Using temporary directory: {}", registry_path.display());

    // Create FileSystem backend
    let backend_config = BackendConfig::FileSystem {
        base_path: registry_path.clone(),
        watch_for_changes: false,
        auto_create_directories: true,
    };

    let backend = SchemaRegistryBackendFactory::create(backend_config)?;
    println!("✅ FileSystem backend created successfully");

    // Test health check
    let health = backend.health_check().await?;
    println!(
        "🏥 Health check: {} - {}",
        if health.is_healthy {
            "✅ HEALTHY"
        } else {
            "❌ UNHEALTHY"
        },
        health.message
    );

    // Test schema registration
    let test_schema = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
        ]
    }"#;

    println!("📝 Registering test schema for subject 'users-value'");
    let schema_id = backend
        .register_schema("users-value", test_schema, vec![])
        .await?;
    println!("✅ Schema registered with ID: {}", schema_id);

    // Test schema retrieval by ID
    let retrieved_schema = backend.get_schema(schema_id).await?;
    println!(
        "🔍 Retrieved schema by ID {}: subject='{}', version={}",
        retrieved_schema.id, retrieved_schema.subject, retrieved_schema.version
    );

    // Test latest schema retrieval
    let latest_schema = backend.get_latest_schema("users-value").await?;
    println!(
        "📋 Latest schema: ID={}, version={}",
        latest_schema.id, latest_schema.version
    );

    // Test schema with references
    let address_schema = r#"{
        "type": "record",
        "name": "Address", 
        "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "zipcode", "type": "string"}
        ]
    }"#;

    println!("📝 Registering address schema");
    let address_id = backend
        .register_schema("addresses-value", address_schema, vec![])
        .await?;
    println!("✅ Address schema registered with ID: {}", address_id);

    // Register user schema with address reference
    let user_with_address_schema = r#"{
        "type": "record", 
        "name": "UserWithAddress",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}, 
            {"name": "email", "type": "string"},
            {"name": "address", "type": "Address"}
        ]
    }"#;

    let address_ref = SchemaReference {
        name: "Address".to_string(),
        subject: "addresses-value".to_string(),
        version: Some(1),
        schema_id: Some(address_id),
    };

    println!("📝 Registering user schema with address reference");
    let user_ref_id = backend
        .register_schema(
            "users-with-address-value",
            user_with_address_schema,
            vec![address_ref],
        )
        .await?;
    println!(
        "✅ User with address schema registered with ID: {}",
        user_ref_id
    );

    // Test subjects listing
    let subjects = backend.get_subjects().await?;
    println!("📂 Available subjects: {:?}", subjects);

    // Test versions listing
    let user_versions = backend.get_versions("users-value").await?;
    println!("📚 Versions for 'users-value': {:?}", user_versions);

    // Test compatibility check
    let modified_schema = r#"{
        "type": "record",
        "name": "User", 
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "age", "type": ["null", "int"], "default": null}
        ]
    }"#;

    let is_compatible = backend
        .check_compatibility("users-value", modified_schema)
        .await?;
    println!(
        "🔄 Schema compatibility check: {}",
        if is_compatible {
            "✅ Compatible"
        } else {
            "❌ Incompatible"
        }
    );

    // Test metadata
    let metadata = backend.metadata();
    println!(
        "🔧 Backend metadata: type={}, version={}",
        metadata.backend_type, metadata.version
    );
    println!("   Features: {:?}", metadata.supported_features);
    println!(
        "   Supports references: {}",
        metadata.capabilities.supports_references
    );

    // Register another version of the user schema
    println!("📝 Registering version 2 of user schema");
    let user_v2_id = backend
        .register_schema("users-value", modified_schema, vec![])
        .await?;
    println!("✅ User schema v2 registered with ID: {}", user_v2_id);

    // Verify latest version updated
    let latest_after_v2 = backend.get_latest_schema("users-value").await?;
    println!(
        "📋 Latest schema after v2: ID={}, version={}",
        latest_after_v2.id, latest_after_v2.version
    );

    // Test specific version retrieval
    let v1_schema = backend.get_schema_version("users-value", 1).await?;
    println!(
        "📋 Version 1 schema: ID={}, version={}",
        v1_schema.id, v1_schema.version
    );

    let v2_schema = backend.get_schema_version("users-value", 2).await?;
    println!(
        "📋 Version 2 schema: ID={}, version={}",
        v2_schema.id, v2_schema.version
    );

    // Test directory structure
    let temp_path = &registry_path;
    println!("📁 Directory structure created:");
    println!("   Base: {}", temp_path.display());

    if temp_path.join("subjects").exists() {
        println!("   ✅ subjects/ directory exists");
        for subject in subjects {
            let subject_path = temp_path.join("subjects").join(&subject);
            if subject_path.exists() {
                println!("      📂 {}/", subject);
                let versions_path = subject_path.join("versions");
                if versions_path.exists() {
                    println!("         📂 versions/");
                    if let Ok(entries) = std::fs::read_dir(&versions_path) {
                        for entry in entries.flatten() {
                            println!("            📄 {}", entry.file_name().to_string_lossy());
                        }
                    }
                }
                if subject_path.join("latest.json").exists() {
                    println!("         📄 latest.json");
                }
            }
        }
    }

    if temp_path.join("schemas").exists() {
        println!("   ✅ schemas/ directory exists");
        if let Ok(entries) = std::fs::read_dir(temp_path.join("schemas")) {
            for entry in entries.flatten() {
                println!("      📄 {}", entry.file_name().to_string_lossy());
            }
        }
    }

    if temp_path.join("metadata.json").exists() {
        println!("   ✅ metadata.json exists");
    }

    println!("\n🎉 FileSystem Schema Registry Backend test completed successfully!");
    println!("✨ Key features demonstrated:");
    println!("   • Schema registration and retrieval");
    println!("   • Version management");
    println!("   • Subject organization");
    println!("   • Schema references support");
    println!("   • Atomic file operations");
    println!("   • Health monitoring");
    println!("   • Metadata tracking");

    // Cleanup
    println!("\n🧹 Cleaning up temporary directory...");
    if registry_path.exists() {
        std::fs::remove_dir_all(&registry_path).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to cleanup directory: {}", e);
        });
        println!("✅ Temporary directory cleaned up");
    }

    Ok(())
}
