use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

use crate::ferris::schema::client::registry_client::SchemaReference;
use crate::ferris::schema::server::registry_backend::{
    BackendCapabilities, BackendMetadata, HealthStatus, SchemaRegistryBackend, SchemaResponse,
};
use crate::ferris::schema::{SchemaError, SchemaResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub id: u32,
    pub subject: String,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub metadata: HashMap<String, String>,
}

pub struct FileSystemSchemaRegistryBackend {
    base_path: PathBuf,
    watch_for_changes: bool,
    auto_create_directories: bool,
}

impl FileSystemSchemaRegistryBackend {
    pub fn new(
        base_path: PathBuf,
        watch_for_changes: bool,
        auto_create_directories: bool,
    ) -> SchemaResult<Self> {
        Ok(Self {
            base_path,
            watch_for_changes,
            auto_create_directories,
        })
    }

    async fn ensure_directories(&self) -> SchemaResult<()> {
        if self.auto_create_directories {
            let subjects_dir = self.base_path.join("subjects");
            let schemas_dir = self.base_path.join("schemas");

            fs::create_dir_all(&subjects_dir)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to create subjects directory: {}", e),
                })?;

            fs::create_dir_all(&schemas_dir)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to create schemas directory: {}", e),
                })?;
        }
        Ok(())
    }

    async fn write_schema_version(&self, schema_version: &SchemaVersion) -> SchemaResult<()> {
        self.ensure_directories().await?;

        // Write to subjects directory
        let subject_file = self
            .base_path
            .join("subjects")
            .join(format!("{}.json", schema_version.subject));

        let mut versions = self
            .load_subject_versions(&schema_version.subject)
            .await
            .unwrap_or_default();

        // Remove existing version if it exists
        versions.retain(|v| v.version != schema_version.version);
        versions.push(schema_version.clone());

        // Sort by version
        versions.sort_by_key(|v| v.version);

        let json_content =
            serde_json::to_string_pretty(&versions).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to serialize schema versions: {}", e),
            })?;

        fs::write(&subject_file, json_content)
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to write subject file: {}", e),
            })?;

        // Write to schemas directory by ID
        let schema_file = self
            .base_path
            .join("schemas")
            .join(format!("{}.json", schema_version.id));

        let json_content =
            serde_json::to_string_pretty(schema_version).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to serialize schema: {}", e),
            })?;

        fs::write(&schema_file, json_content)
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to write schema file: {}", e),
            })?;

        Ok(())
    }

    async fn load_subject_versions(&self, subject: &str) -> SchemaResult<Vec<SchemaVersion>> {
        let subject_file = self
            .base_path
            .join("subjects")
            .join(format!("{}.json", subject));

        if !subject_file.exists() {
            return Ok(Vec::new());
        }

        let content =
            fs::read_to_string(&subject_file)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to read subject file: {}", e),
                })?;

        serde_json::from_str(&content).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to parse subject file: {}", e),
        })
    }

    async fn get_next_id(&self) -> SchemaResult<u32> {
        self.ensure_directories().await?;

        let id_file = self.base_path.join("next_id.txt");

        let current_id = if id_file.exists() {
            let content =
                fs::read_to_string(&id_file)
                    .await
                    .map_err(|e| SchemaError::Provider {
                        source: "filesystem".to_string(),
                        message: format!("Failed to read ID file: {}", e),
                    })?;
            content.trim().parse::<u32>().unwrap_or(1)
        } else {
            1
        };

        let next_id = current_id + 1;
        fs::write(&id_file, next_id.to_string())
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to write ID file: {}", e),
            })?;

        Ok(current_id)
    }
}

#[async_trait]
impl SchemaRegistryBackend for FileSystemSchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let schema_file = self.base_path.join("schemas").join(format!("{}.json", id));

        if !schema_file.exists() {
            return Err(SchemaError::NotFound {
                source: format!("Schema with ID {} not found in filesystem", id),
            });
        }

        let content =
            fs::read_to_string(&schema_file)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to read schema file: {}", e),
                })?;

        let schema_version: SchemaVersion =
            serde_json::from_str(&content).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to parse schema file: {}", e),
            })?;

        Ok(SchemaResponse {
            id: schema_version.id,
            subject: schema_version.subject,
            version: schema_version.version,
            schema: schema_version.schema,
            references: schema_version.references,
            metadata: schema_version.metadata,
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let versions = self.load_subject_versions(subject).await?;

        if versions.is_empty() {
            return Err(SchemaError::NotFound {
                source: format!("Subject {} not found", subject),
            });
        }

        let latest = versions.iter().max_by_key(|v| v.version).unwrap();

        Ok(SchemaResponse {
            id: latest.id,
            subject: latest.subject.clone(),
            version: latest.version,
            schema: latest.schema.clone(),
            references: latest.references.clone(),
            metadata: latest.metadata.clone(),
        })
    }

    async fn get_schema_version(
        &self,
        subject: &str,
        version: i32,
    ) -> SchemaResult<SchemaResponse> {
        let versions = self.load_subject_versions(subject).await?;

        let schema_version = versions
            .iter()
            .find(|v| v.version == version)
            .ok_or_else(|| SchemaError::NotFound {
                source: format!("Version {} of subject {} not found", version, subject),
            })?;

        Ok(SchemaResponse {
            id: schema_version.id,
            subject: schema_version.subject.clone(),
            version: schema_version.version,
            schema: schema_version.schema.clone(),
            references: schema_version.references.clone(),
            metadata: schema_version.metadata.clone(),
        })
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32> {
        let id = self.get_next_id().await?;
        let versions = self
            .load_subject_versions(subject)
            .await
            .unwrap_or_default();
        let next_version = versions.iter().map(|v| v.version).max().unwrap_or(0) + 1;

        let schema_version = SchemaVersion {
            id,
            subject: subject.to_string(),
            version: next_version,
            schema: schema.to_string(),
            references,
            metadata: {
                let mut map = HashMap::new();
                map.insert(
                    "created_at".to_string(),
                    chrono::Utc::now().timestamp().to_string(),
                );
                map
            },
        };

        self.write_schema_version(&schema_version).await?;

        Ok(id)
    }

    async fn check_compatibility(&self, _subject: &str, _schema: &str) -> SchemaResult<bool> {
        // Simplified compatibility check - always return true for filesystem backend
        Ok(true)
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let versions = self.load_subject_versions(subject).await?;
        Ok(versions.iter().map(|v| v.version).collect())
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        let subjects_dir = self.base_path.join("subjects");

        if !subjects_dir.exists() {
            return Ok(Vec::new());
        }

        let mut subjects = Vec::new();
        let mut entries = fs::read_dir(&subjects_dir)
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read subjects directory: {}", e),
            })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read directory entry: {}", e),
            })?
        {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(".json") {
                    subjects.push(file_name[..file_name.len() - 5].to_string());
                }
            }
        }

        Ok(subjects)
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let mut versions = self.load_subject_versions(subject).await?;

        let schema_to_delete = versions
            .iter()
            .find(|v| v.version == version)
            .ok_or_else(|| SchemaError::NotFound {
                source: format!("Version {} of subject {} not found", version, subject),
            })?
            .clone();

        // Remove from versions list
        versions.retain(|v| v.version != version);

        // Update subject file
        let subject_file = self
            .base_path
            .join("subjects")
            .join(format!("{}.json", subject));

        if versions.is_empty() {
            // Remove subject file if no versions left
            fs::remove_file(&subject_file)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to remove subject file: {}", e),
                })?;
        } else {
            let json_content =
                serde_json::to_string_pretty(&versions).map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to serialize schema versions: {}", e),
                })?;

            fs::write(&subject_file, json_content)
                .await
                .map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to write subject file: {}", e),
                })?;
        }

        // Remove schema file by ID
        let schema_file = self
            .base_path
            .join("schemas")
            .join(format!("{}.json", schema_to_delete.id));

        fs::remove_file(&schema_file)
            .await
            .map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to remove schema file: {}", e),
            })?;

        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "filesystem".to_string(),
            version: "1.0.0".to_string(),
            supported_features: vec![
                "schema_registration".to_string(),
                "version_management".to_string(),
                "subject_listing".to_string(),
                "schema_deletion".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: false, // Simplified for filesystem backend
                supports_compatibility_check: true,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: Some(1024 * 1024), // 1MB limit
                max_references_per_schema: Some(100),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        let start_time = std::time::Instant::now();

        // Check if base directory exists and is writable
        let is_healthy = if self.base_path.exists() {
            // Try to write a test file
            let test_file = self.base_path.join(".health_check");
            match fs::write(&test_file, "ok").await {
                Ok(_) => {
                    // Clean up test file
                    let _ = fs::remove_file(&test_file).await;
                    true
                }
                Err(_) => false,
            }
        } else {
            self.auto_create_directories
        };

        let response_time = start_time.elapsed();

        let mut details = HashMap::new();
        details.insert(
            "base_path".to_string(),
            self.base_path.to_string_lossy().to_string(),
        );
        details.insert(
            "watch_for_changes".to_string(),
            self.watch_for_changes.to_string(),
        );
        details.insert(
            "auto_create_directories".to_string(),
            self.auto_create_directories.to_string(),
        );

        Ok(HealthStatus {
            is_healthy,
            message: if is_healthy {
                "Filesystem backend is healthy".to_string()
            } else {
                "Filesystem backend is not accessible".to_string()
            },
            response_time_ms: response_time.as_millis() as u64,
            details,
        })
    }
}
