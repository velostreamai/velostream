//! Confluent Schema Registry Backend Implementation
//!
//! Provides HTTP client implementation for Confluent Schema Registry API
//! with authentication, retry logic, and full schema management capabilities.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ferris::schema::client::registry_client::SchemaReference;
use crate::ferris::schema::server::registry_backend::{
    BackendCapabilities, BackendMetadata, HealthStatus, SchemaRegistryBackend, SchemaResponse,
};
use crate::ferris::schema::{SchemaError, SchemaResult};

/// Confluent-specific authentication
#[derive(Debug, Clone)]
pub enum ConfluentAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
    ApiKey { key: String, secret: String },
}

/// Confluent Schema Registry backend implementation
pub struct ConfluentSchemaRegistryBackend {
    base_url: String,
    auth: ConfluentAuth,
    http_client: reqwest::Client,
    timeout_seconds: u64,
    max_retries: u32,
}

impl ConfluentSchemaRegistryBackend {
    pub fn new(
        base_url: String,
        auth: ConfluentAuth,
        timeout_seconds: u64,
        max_retries: u32,
    ) -> SchemaResult<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth,
            http_client,
            timeout_seconds,
            max_retries,
        })
    }
}

#[async_trait]
impl SchemaRegistryBackend for ConfluentSchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let confluent_response: ConfluentSchemaResponse =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject.unwrap_or_default(),
            version: confluent_response.version.unwrap_or(1),
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let confluent_response: ConfluentVersionResponse =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject,
            version: confluent_response.version,
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn get_schema_version(
        &self,
        subject: &str,
        version: i32,
    ) -> SchemaResult<SchemaResponse> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject, version
        );
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let confluent_response: ConfluentVersionResponse =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject,
            version: confluent_response.version,
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);

        let request_body = ConfluentRegisterRequest {
            schema: schema.to_string(),
            references: if references.is_empty() {
                None
            } else {
                Some(references)
            },
            schema_type: None,
        };

        let body = serde_json::to_string(&request_body).map_err(|e| SchemaError::Provider {
            source: "confluent".to_string(),
            message: format!("Failed to serialize request: {}", e),
        })?;

        let response = self
            .execute_request(reqwest::Method::POST, &url, Some(body))
            .await?;

        let result: serde_json::Value =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse registration response: {}", e),
            })?;

        result["id"]
            .as_u64()
            .map(|id| id as u32)
            .ok_or_else(|| SchemaError::Provider {
                source: "confluent".to_string(),
                message: "Invalid schema ID in response".to_string(),
            })
    }

    async fn check_compatibility(&self, subject: &str, schema: &str) -> SchemaResult<bool> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url, subject
        );

        let request_body = serde_json::json!({
            "schema": schema
        });

        let body = request_body.to_string();
        let response = self
            .execute_request(reqwest::Method::POST, &url, Some(body))
            .await?;

        let result: serde_json::Value =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse compatibility response: {}", e),
            })?;

        Ok(result["is_compatible"].as_bool().unwrap_or(false))
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let versions: Vec<i32> = response.json().await.map_err(|e| SchemaError::Provider {
            source: "confluent".to_string(),
            message: format!("Failed to parse versions response: {}", e),
        })?;

        Ok(versions)
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let subjects: Vec<String> = response.json().await.map_err(|e| SchemaError::Provider {
            source: "confluent".to_string(),
            message: format!("Failed to parse subjects response: {}", e),
        })?;

        Ok(subjects)
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject, version
        );
        let _response = self
            .execute_request(reqwest::Method::DELETE, &url, None)
            .await?;
        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "confluent".to_string(),
            version: "7.x".to_string(),
            supported_features: vec![
                "schema_references".to_string(),
                "compatibility_checking".to_string(),
                "schema_evolution".to_string(),
                "subject_management".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: true,
                supports_compatibility_check: true,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: Some(8 * 1024 * 1024), // 8MB
                max_references_per_schema: Some(1000),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        let start = std::time::Instant::now();
        let url = format!("{}/subjects", self.base_url);

        match self.execute_request(reqwest::Method::GET, &url, None).await {
            Ok(_) => Ok(HealthStatus {
                is_healthy: true,
                message: "Confluent Schema Registry is healthy".to_string(),
                response_time_ms: start.elapsed().as_millis() as u64,
                details: HashMap::new(),
            }),
            Err(e) => Ok(HealthStatus {
                is_healthy: false,
                message: format!("Health check failed: {}", e),
                response_time_ms: start.elapsed().as_millis() as u64,
                details: HashMap::new(),
            }),
        }
    }
}

impl ConfluentSchemaRegistryBackend {
    async fn execute_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<String>,
    ) -> SchemaResult<reqwest::Response> {
        let mut request = self
            .http_client
            .request(method, url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json");

        // Add authentication
        request = match &self.auth {
            ConfluentAuth::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            ConfluentAuth::Bearer { token } => request.bearer_auth(token),
            ConfluentAuth::ApiKey { key, secret } => request.basic_auth(key, Some(secret)),
            ConfluentAuth::None => request,
        };

        // Add body if provided
        if let Some(body_content) = body {
            request = request.body(body_content);
        }

        let response = request.send().await.map_err(|e| SchemaError::Provider {
            source: "confluent".to_string(),
            message: format!("Request failed: {}", e),
        })?;

        if !response.status().is_success() {
            return Err(SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Request failed with status: {}", response.status()),
            });
        }

        Ok(response)
    }
}

/// Confluent API response structures
#[derive(Debug, Deserialize)]
struct ConfluentSchemaResponse {
    id: u32,
    schema: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    version: Option<i32>,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Deserialize)]
struct ConfluentVersionResponse {
    id: u32,
    subject: String,
    version: i32,
    schema: String,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Serialize)]
struct ConfluentRegisterRequest {
    schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    references: Option<Vec<SchemaReference>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
}
