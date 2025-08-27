//! In-Memory Schema Registry Backend Implementation
//!
//! Provides an in-memory implementation suitable for testing and development.
//! All data is stored in memory and lost when the process terminates.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::super::registry_client::SchemaReference;
use super::super::{SchemaError, SchemaResult};
use crate::ferris::sql::schema::registry_backend::{
    BackendCapabilities, BackendMetadata, HealthStatus, SchemaRegistryBackend, SchemaResponse,
};

/// Schema version for in-memory storage
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    pub id: u32,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub created_at: i64,
}

/// In-memory Schema Registry backend for testing
pub struct InMemorySchemaRegistryBackend {
    /// Storage: subject -> versions
    subjects: Arc<RwLock<HashMap<String, Vec<SchemaVersion>>>>,
    /// Reverse lookup: schema_id -> SchemaVersion
    schemas_by_id: Arc<RwLock<HashMap<u32, SchemaVersion>>>,
    /// Next schema ID to allocate
    next_id: Arc<std::sync::atomic::AtomicU32>,
}

impl InMemorySchemaRegistryBackend {
    pub fn new(initial_schemas: HashMap<String, Vec<SchemaVersion>>) -> Self {
        let mut schemas_by_id = HashMap::new();
        let mut max_id = 0;

        // Build reverse lookup and find max ID
        for versions in initial_schemas.values() {
            for version in versions {
                schemas_by_id.insert(version.id, version.clone());
                max_id = max_id.max(version.id);
            }
        }

        Self {
            subjects: Arc::new(RwLock::new(initial_schemas)),
            schemas_by_id: Arc::new(RwLock::new(schemas_by_id)),
            next_id: Arc::new(std::sync::atomic::AtomicU32::new(max_id + 1)),
        }
    }
}

#[async_trait]
impl SchemaRegistryBackend for InMemorySchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let schemas = self
            .schemas_by_id
            .read()
            .map_err(|_| SchemaError::Provider {
                source: "memory".to_string(),
                message: "Failed to acquire read lock".to_string(),
            })?;

        let schema = schemas.get(&id).ok_or_else(|| SchemaError::NotFound {
            source: format!("Schema with ID {} not found", id),
        })?;

        Ok(SchemaResponse {
            id: schema.id,
            subject: "unknown".to_string(), // In-memory doesn't track subject by ID
            version: schema.version,
            schema: schema.schema.clone(),
            references: schema.references.clone(),
            metadata: HashMap::new(),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let subjects = self.subjects.read().map_err(|_| SchemaError::Provider {
            source: "memory".to_string(),
            message: "Failed to acquire read lock".to_string(),
        })?;

        let versions = subjects.get(subject).ok_or_else(|| SchemaError::NotFound {
            source: format!("Subject {} not found", subject),
        })?;

        let latest =
            versions
                .iter()
                .max_by_key(|v| v.version)
                .ok_or_else(|| SchemaError::NotFound {
                    source: format!("No versions found for subject {}", subject),
                })?;

        Ok(SchemaResponse {
            id: latest.id,
            subject: subject.to_string(),
            version: latest.version,
            schema: latest.schema.clone(),
            references: latest.references.clone(),
            metadata: HashMap::new(),
        })
    }

    async fn get_schema_version(
        &self,
        subject: &str,
        version: i32,
    ) -> SchemaResult<SchemaResponse> {
        let subjects = self.subjects.read().map_err(|_| SchemaError::Provider {
            source: "memory".to_string(),
            message: "Failed to acquire read lock".to_string(),
        })?;

        let versions = subjects.get(subject).ok_or_else(|| SchemaError::NotFound {
            source: format!("Subject {} not found", subject),
        })?;

        let schema_version = versions
            .iter()
            .find(|v| v.version == version)
            .ok_or_else(|| SchemaError::NotFound {
                source: format!("Version {} not found for subject {}", version, subject),
            })?;

        Ok(SchemaResponse {
            id: schema_version.id,
            subject: subject.to_string(),
            version: schema_version.version,
            schema: schema_version.schema.clone(),
            references: schema_version.references.clone(),
            metadata: HashMap::new(),
        })
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32> {
        let schema_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Determine next version number
        let next_version = {
            let subjects = self.subjects.read().map_err(|_| SchemaError::Provider {
                source: "memory".to_string(),
                message: "Failed to acquire read lock".to_string(),
            })?;

            subjects
                .get(subject)
                .map(|versions| versions.iter().map(|v| v.version).max().unwrap_or(0) + 1)
                .unwrap_or(1)
        };

        let schema_version = SchemaVersion {
            id: schema_id,
            version: next_version,
            schema: schema.to_string(),
            references,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };

        // Add to subjects
        {
            let mut subjects = self.subjects.write().map_err(|_| SchemaError::Provider {
                source: "memory".to_string(),
                message: "Failed to acquire write lock".to_string(),
            })?;

            subjects
                .entry(subject.to_string())
                .or_insert_with(Vec::new)
                .push(schema_version.clone());
        }

        // Add to schemas_by_id
        {
            let mut schemas = self
                .schemas_by_id
                .write()
                .map_err(|_| SchemaError::Provider {
                    source: "memory".to_string(),
                    message: "Failed to acquire write lock".to_string(),
                })?;

            schemas.insert(schema_id, schema_version);
        }

        Ok(schema_id)
    }

    async fn check_compatibility(&self, _subject: &str, _schema: &str) -> SchemaResult<bool> {
        // In-memory backend always reports compatible
        Ok(true)
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let subjects = self.subjects.read().map_err(|_| SchemaError::Provider {
            source: "memory".to_string(),
            message: "Failed to acquire read lock".to_string(),
        })?;

        let versions = subjects
            .get(subject)
            .map(|versions| versions.iter().map(|v| v.version).collect())
            .unwrap_or_default();

        Ok(versions)
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        let subjects = self.subjects.read().map_err(|_| SchemaError::Provider {
            source: "memory".to_string(),
            message: "Failed to acquire read lock".to_string(),
        })?;

        Ok(subjects.keys().cloned().collect())
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        // Remove from subjects
        let schema_id = {
            let mut subjects = self.subjects.write().map_err(|_| SchemaError::Provider {
                source: "memory".to_string(),
                message: "Failed to acquire write lock".to_string(),
            })?;

            if let Some(versions) = subjects.get_mut(subject) {
                if let Some(pos) = versions.iter().position(|v| v.version == version) {
                    let removed = versions.remove(pos);
                    Some(removed.id)
                } else {
                    return Err(SchemaError::NotFound {
                        source: format!("Version {} not found for subject {}", version, subject),
                    });
                }
            } else {
                return Err(SchemaError::NotFound {
                    source: format!("Subject {} not found", subject),
                });
            }
        };

        // Remove from schemas_by_id
        if let Some(id) = schema_id {
            let mut schemas = self
                .schemas_by_id
                .write()
                .map_err(|_| SchemaError::Provider {
                    source: "memory".to_string(),
                    message: "Failed to acquire write lock".to_string(),
                })?;

            schemas.remove(&id);
        }

        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "memory".to_string(),
            version: "1.0.0".to_string(),
            supported_features: vec![
                "in_memory_storage".to_string(),
                "fast_access".to_string(),
                "testing_suitable".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: false, // No compatibility checking
                supports_compatibility_check: false,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: None, // No limits
                max_references_per_schema: None,
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        Ok(HealthStatus {
            is_healthy: true,
            message: "In-memory registry is always healthy".to_string(),
            response_time_ms: 0,
            details: HashMap::new(),
        })
    }
}
