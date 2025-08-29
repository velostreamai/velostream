//! Schema Registry Client Implementation
//!
//! Provides HTTP client for interacting with Confluent Schema Registry API
//! with support for schema references, caching, and authentication.

use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::ferris::schema::{SchemaError, SchemaResult};

/// Schema Registry client for managing schemas with reference support
pub struct SchemaRegistryClient {
    base_url: String,
    auth: Option<AuthConfig>,
    cache: Arc<RwLock<SchemaCache>>,
    reference_resolver: Option<Arc<SchemaReferenceResolver>>,
    http_client: Client,
    config: RegistryClientConfig,
}

/// Configuration for Schema Registry client
#[derive(Debug, Clone)]
pub struct RegistryClientConfig {
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Base retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Enable reference resolution
    pub resolve_references: bool,
}

/// Authentication configuration
#[derive(Debug, Clone)]
pub enum AuthConfig {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// Schema Registry compatibility levels and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub compatibility_level: CompatibilityLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

/// Schema reference information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<u32>,
}

/// Schema dependency information
#[derive(Debug, Clone)]
pub struct SchemaDependency {
    pub schema_id: u32,
    pub subject: String,
    pub version: i32,
    pub dependencies: Vec<u32>,
}

/// Resolved schema with all references expanded
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub root_schema: CachedSchema,
    pub dependencies: HashMap<u32, CachedSchema>,
    pub flattened_schema: String,
    pub resolution_path: Vec<u32>,
}

/// Cached schema entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSchema {
    pub id: u32,
    pub subject: String,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    #[serde(skip, default = "std::time::Instant::now")]
    pub cached_at: std::time::Instant,
    #[serde(skip, default)]
    pub access_count: u64,
}

/// Schema cache implementation
pub struct SchemaCache {
    schemas: HashMap<u32, CachedSchema>,
    subject_versions: HashMap<String, HashMap<i32, u32>>,
    resolved_schemas: HashMap<u32, ResolvedSchema>,
    dependency_graphs: HashMap<u32, DependencyGraph>,
    reference_index: HashMap<String, std::collections::HashSet<u32>>,
    resolution_cache: HashMap<u32, std::time::Instant>,
    config: RegistryClientConfig,
}

/// Dependency graph for schema references
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    pub root: u32,
    pub nodes: HashMap<u32, GraphNode>,
    pub edges: HashMap<u32, Vec<u32>>,
    pub resolution_order: Vec<u32>,
}

/// Graph node representing a schema in the dependency graph
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub schema_id: u32,
    pub subject: String,
    pub version: i32,
    pub in_degree: usize,
    pub depth: usize,
}

/// Schema reference resolver
pub struct SchemaReferenceResolver {
    registry: Arc<RwLock<SchemaRegistryClient>>,
    dependency_cache: Arc<RwLock<HashMap<u32, Vec<SchemaDependency>>>>,
}

/// API response for schema retrieval
#[derive(Debug, Deserialize)]
struct SchemaResponse {
    id: u32,
    schema: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
    subject: Option<String>,
    version: Option<i32>,
}

/// API response for subject versions
#[derive(Debug, Deserialize)]
struct SubjectVersionResponse {
    subject: String,
    version: i32,
    id: u32,
    schema: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

/// API request for schema registration
#[derive(Debug, Serialize)]
struct RegisterSchemaRequest {
    schema: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    references: Vec<SchemaReference>,
}

impl SchemaRegistryClient {
    /// Create a new Schema Registry client
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth: None,
            cache: Arc::new(RwLock::new(SchemaCache::new(
                RegistryClientConfig::default(),
            ))),
            reference_resolver: None, // Avoid circular dependency by making it optional
            http_client: Client::new(),
            config: RegistryClientConfig::default(),
        }
    }

    /// Create client with configuration
    pub fn with_config(base_url: String, config: RegistryClientConfig) -> Self {
        let mut client = Self::new(base_url);
        client.config = config.clone();
        client.cache = Arc::new(RwLock::new(SchemaCache::new(config)));
        client
    }

    /// Set authentication configuration
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Get schema by ID
    pub async fn get_schema(&self, id: u32) -> SchemaResult<CachedSchema> {
        // Check cache first
        if let Some(cached) = self.get_cached_schema(id).await {
            return Ok(cached);
        }

        // Fetch from registry
        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let schema_response: SchemaResponse =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: format!("Failed to parse schema response: {}", e),
            })?;

        let cached_schema = CachedSchema {
            id: schema_response.id,
            subject: schema_response.subject.unwrap_or_default(),
            version: schema_response.version.unwrap_or(1),
            schema: schema_response.schema,
            references: schema_response.references,
            cached_at: std::time::Instant::now(),
            access_count: 1,
        };

        // Cache the schema
        self.cache_schema(cached_schema.clone()).await;

        Ok(cached_schema)
    }

    /// Get latest schema for a subject
    pub async fn get_latest_schema(&self, subject: &str) -> SchemaResult<CachedSchema> {
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let version_response: SubjectVersionResponse =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: format!("Failed to parse version response: {}", e),
            })?;

        let cached_schema = CachedSchema {
            id: version_response.id,
            subject: version_response.subject,
            version: version_response.version,
            schema: version_response.schema,
            references: version_response.references,
            cached_at: std::time::Instant::now(),
            access_count: 1,
        };

        self.cache_schema(cached_schema.clone()).await;

        Ok(cached_schema)
    }

    /// Register a new schema
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);

        let request_body = RegisterSchemaRequest {
            schema: schema.to_string(),
            references,
        };

        let body = serde_json::to_string(&request_body).map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to serialize request: {}", e),
        })?;

        let response = self
            .execute_request(reqwest::Method::POST, &url, Some(body))
            .await?;

        let result: serde_json::Value =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: format!("Failed to parse registration response: {}", e),
            })?;

        result["id"]
            .as_u64()
            .map(|id| id as u32)
            .ok_or_else(|| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: "Invalid schema ID in response".to_string(),
            })
    }

    /// Resolve schema with all its references
    pub async fn resolve_schema_with_references(&self, id: u32) -> SchemaResult<ResolvedSchema> {
        if !self.config.resolve_references {
            let schema = self.get_schema(id).await?;
            return Ok(ResolvedSchema {
                root_schema: schema.clone(),
                dependencies: HashMap::new(),
                flattened_schema: schema.schema,
                resolution_path: vec![id],
            });
        }

        // Check if already resolved in cache
        if let Some(resolved) = self.get_cached_resolved_schema(id).await {
            return Ok(resolved);
        }

        // Build dependency graph and resolve
        let graph = self.build_dependency_graph(id).await?;
        let resolved = self.resolve_from_graph(id, &graph).await?;

        // Cache the resolved schema
        self.cache_resolved_schema(resolved.clone()).await;

        Ok(resolved)
    }

    /// Check schema compatibility
    pub async fn validate_schema_compatibility(
        &self,
        subject: &str,
        schema: &str,
    ) -> SchemaResult<bool> {
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
                source: "schema_registry".to_string(),
                message: format!("Failed to parse compatibility response: {}", e),
            })?;

        Ok(result["is_compatible"].as_bool().unwrap_or(false))
    }

    /// Get schema dependencies
    pub fn get_schema_dependencies<'a>(
        &'a self,
        id: u32,
    ) -> futures::future::BoxFuture<'a, SchemaResult<Vec<SchemaDependency>>> {
        Box::pin(async move {
            let schema = self.get_schema(id).await?;
            let mut dependencies = Vec::new();

            for reference in &schema.references {
                if let Some(ref_id) = reference.schema_id {
                    let ref_schema = self.get_schema(ref_id).await?;
                    let nested_deps = self.get_schema_dependencies(ref_id).await?;

                    dependencies.push(SchemaDependency {
                        schema_id: ref_id,
                        subject: ref_schema.subject.clone(),
                        version: ref_schema.version,
                        dependencies: nested_deps.into_iter().map(|d| d.schema_id).collect(),
                    });
                }
            }

            Ok(dependencies)
        })
    }

    /// Get schema references for a subject
    pub async fn get_schema_references(&self, subject: &str) -> SchemaResult<Vec<SchemaReference>> {
        let schema = self.get_latest_schema(subject).await?;
        Ok(schema.references)
    }

    /// List all subjects in the registry
    pub async fn list_subjects(&self) -> SchemaResult<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let subjects: Vec<String> = response.json().await.map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to parse subjects response: {}", e),
        })?;

        Ok(subjects)
    }

    /// List all versions of a subject
    pub async fn list_subject_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let versions: Vec<i32> = response.json().await.map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to parse versions response: {}", e),
        })?;

        Ok(versions)
    }

    /// Get a specific version of a subject's schema
    pub async fn get_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> SchemaResult<CachedSchema> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject, version
        );
        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let schema: CachedSchema = response.json().await.map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to parse schema response: {}", e),
        })?;

        // Cache the schema
        self.cache_schema(schema.clone()).await;

        Ok(schema)
    }

    /// Delete a specific version of a subject
    pub async fn delete_subject_version(&self, subject: &str, version: i32) -> SchemaResult<i32> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url, subject, version
        );
        let response = self
            .execute_request(reqwest::Method::DELETE, &url, None)
            .await?;

        let deleted_version: i32 = response.json().await.map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to parse delete response: {}", e),
        })?;

        // Invalidate cache for this subject/version
        self.invalidate_cache(subject, Some(version)).await;

        Ok(deleted_version)
    }

    /// Delete all versions of a subject
    pub async fn delete_subject(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let url = format!("{}/subjects/{}", self.base_url, subject);
        let response = self
            .execute_request(reqwest::Method::DELETE, &url, None)
            .await?;

        let deleted_versions: Vec<i32> =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: format!("Failed to parse delete response: {}", e),
            })?;

        // Invalidate cache for this subject
        self.invalidate_cache(subject, None).await;

        Ok(deleted_versions)
    }

    /// Get registry configuration
    pub async fn get_config(&self, subject: Option<&str>) -> SchemaResult<RegistryConfig> {
        let url = if let Some(subj) = subject {
            format!("{}/config/{}", self.base_url, subj)
        } else {
            format!("{}/config", self.base_url)
        };

        let response = self
            .execute_request(reqwest::Method::GET, &url, None)
            .await?;

        let config: RegistryConfig = response.json().await.map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to parse config response: {}", e),
        })?;

        Ok(config)
    }

    /// Update registry configuration
    pub async fn update_config(
        &self,
        subject: Option<&str>,
        config: &RegistryConfig,
    ) -> SchemaResult<RegistryConfig> {
        let url = if let Some(subj) = subject {
            format!("{}/config/{}", self.base_url, subj)
        } else {
            format!("{}/config", self.base_url)
        };

        let body = serde_json::to_string(config).map_err(|e| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: format!("Failed to serialize config: {}", e),
        })?;

        let response = self
            .execute_request(reqwest::Method::PUT, &url, Some(body))
            .await?;

        let updated_config: RegistryConfig =
            response.json().await.map_err(|e| SchemaError::Provider {
                source: "schema_registry".to_string(),
                message: format!("Failed to parse config update response: {}", e),
            })?;

        Ok(updated_config)
    }

    /// Check if the registry is healthy
    pub async fn health_check(&self) -> SchemaResult<bool> {
        // Try to list subjects as a health check
        match self.list_subjects().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Invalidate cache entries for a subject
    async fn invalidate_cache(&self, subject: &str, version: Option<i32>) {
        let mut cache = self.cache.write().await;
        cache.invalidate_subject(subject, version);
    }

    // Private helper methods

    async fn execute_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<String>,
    ) -> SchemaResult<Response> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            let mut request = self
                .http_client
                .request(method.clone(), url)
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .timeout(std::time::Duration::from_secs(self.config.timeout_seconds));

            // Add authentication
            if let Some(auth) = &self.auth {
                request = match auth {
                    AuthConfig::Basic { username, password } => {
                        request.basic_auth(username, Some(password))
                    }
                    AuthConfig::Bearer { token } => request.bearer_auth(token),
                    AuthConfig::None => request,
                };
            }

            // Add body if provided
            if let Some(body_content) = &body {
                request = request.body(body_content.clone());
            }

            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(response);
                    } else {
                        let status = response.status();
                        let error_text = response.text().await.unwrap_or_default();
                        last_error = Some(SchemaError::Provider {
                            source: "schema_registry".to_string(),
                            message: format!(
                                "Request failed with status {}: {}",
                                status, error_text
                            ),
                        });

                        // Don't retry on 4xx errors (client errors)
                        if status.as_u16() >= 400 && status.as_u16() < 500 {
                            break;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(SchemaError::Provider {
                        source: "schema_registry".to_string(),
                        message: format!("Request failed: {}", e),
                    });
                }
            }

            // Wait before retry (exponential backoff)
            if attempt < self.config.max_retries {
                let delay = self.config.retry_delay_ms * 2_u64.pow(attempt);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| SchemaError::Provider {
            source: "schema_registry".to_string(),
            message: "All retry attempts failed".to_string(),
        }))
    }

    async fn get_cached_schema(&self, id: u32) -> Option<CachedSchema> {
        let cache = self.cache.read().await;
        cache.get_schema(id)
    }

    async fn cache_schema(&self, schema: CachedSchema) {
        let mut cache = self.cache.write().await;
        cache.put_schema(schema);
    }

    async fn get_cached_resolved_schema(&self, id: u32) -> Option<ResolvedSchema> {
        let cache = self.cache.read().await;
        cache.get_resolved_schema(id)
    }

    async fn cache_resolved_schema(&self, resolved: ResolvedSchema) {
        let mut cache = self.cache.write().await;
        cache.put_resolved_schema(resolved);
    }

    async fn build_dependency_graph(&self, root_id: u32) -> SchemaResult<DependencyGraph> {
        let mut graph = DependencyGraph {
            root: root_id,
            nodes: HashMap::new(),
            edges: HashMap::new(),
            resolution_order: Vec::new(),
        };

        // Build graph recursively
        self.add_to_graph(root_id, &mut graph, 0).await?;

        // Calculate resolution order (topological sort)
        graph.resolution_order = graph.topological_sort()?;

        Ok(graph)
    }

    fn add_to_graph<'a>(
        &'a self,
        schema_id: u32,
        graph: &'a mut DependencyGraph,
        depth: usize,
    ) -> futures::future::BoxFuture<'a, SchemaResult<()>> {
        Box::pin(async move {
            if graph.nodes.contains_key(&schema_id) {
                return Ok(()); // Already processed
            }

            let schema = self.get_schema(schema_id).await?;

            graph.nodes.insert(
                schema_id,
                GraphNode {
                    schema_id,
                    subject: schema.subject.clone(),
                    version: schema.version,
                    in_degree: 0,
                    depth,
                },
            );

            let mut dependencies = Vec::new();
            for reference in &schema.references {
                if let Some(ref_id) = reference.schema_id {
                    dependencies.push(ref_id);
                    self.add_to_graph(ref_id, graph, depth + 1).await?;
                }
            }

            graph.edges.insert(schema_id, dependencies);

            Ok(())
        })
    }

    async fn resolve_from_graph(
        &self,
        root_id: u32,
        graph: &DependencyGraph,
    ) -> SchemaResult<ResolvedSchema> {
        let root_schema = self.get_schema(root_id).await?;
        let mut dependencies = HashMap::new();

        for node_id in &graph.resolution_order {
            if *node_id != root_id {
                let dep_schema = self.get_schema(*node_id).await?;
                dependencies.insert(*node_id, dep_schema);
            }
        }

        // TODO: Implement actual schema flattening logic
        let flattened_schema = root_schema.schema.clone();

        Ok(ResolvedSchema {
            root_schema,
            dependencies,
            flattened_schema,
            resolution_path: graph.resolution_order.clone(),
        })
    }
}

impl Default for RegistryClientConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            max_retries: 3,
            retry_delay_ms: 1000,
            cache_ttl_seconds: 300,
            resolve_references: true,
        }
    }
}

impl SchemaCache {
    fn new(config: RegistryClientConfig) -> Self {
        Self {
            schemas: HashMap::new(),
            subject_versions: HashMap::new(),
            resolved_schemas: HashMap::new(),
            dependency_graphs: HashMap::new(),
            reference_index: HashMap::new(),
            resolution_cache: HashMap::new(),
            config,
        }
    }

    fn get_schema(&self, id: u32) -> Option<CachedSchema> {
        self.schemas.get(&id).cloned()
    }

    fn put_schema(&mut self, schema: CachedSchema) {
        let id = schema.id;
        let subject = schema.subject.clone();
        let version = schema.version;

        self.schemas.insert(id, schema);

        self.subject_versions
            .entry(subject)
            .or_default()
            .insert(version, id);
    }

    fn get_resolved_schema(&self, id: u32) -> Option<ResolvedSchema> {
        self.resolved_schemas.get(&id).cloned()
    }

    fn put_resolved_schema(&mut self, resolved: ResolvedSchema) {
        let id = resolved.root_schema.id;
        self.resolved_schemas.insert(id, resolved);
        self.resolution_cache.insert(id, std::time::Instant::now());
    }

    fn invalidate_subject(&mut self, subject: &str, version: Option<i32>) {
        if let Some(version) = version {
            // Invalidate specific version
            if let Some(versions) = self.subject_versions.get(subject) {
                if let Some(&schema_id) = versions.get(&version) {
                    self.schemas.remove(&schema_id);
                    self.resolved_schemas.remove(&schema_id);
                    self.resolution_cache.remove(&schema_id);
                }
            }

            // Remove from subject_versions
            if let Some(versions) = self.subject_versions.get_mut(subject) {
                versions.remove(&version);
                if versions.is_empty() {
                    self.subject_versions.remove(subject);
                }
            }
        } else {
            // Invalidate all versions of the subject
            if let Some(versions) = self.subject_versions.remove(subject) {
                for (_version, schema_id) in versions {
                    self.schemas.remove(&schema_id);
                    self.resolved_schemas.remove(&schema_id);
                    self.resolution_cache.remove(&schema_id);
                }
            }
        }
    }
}

impl DependencyGraph {
    fn topological_sort(&self) -> SchemaResult<Vec<u32>> {
        let mut result = Vec::new();
        let mut in_degrees = HashMap::new();
        let mut queue = std::collections::VecDeque::new();

        // Calculate in-degrees
        for node in self.nodes.keys() {
            in_degrees.insert(*node, 0);
        }

        for deps in self.edges.values() {
            for dep in deps {
                *in_degrees.get_mut(dep).unwrap() += 1;
            }
        }

        // Start with nodes that have no incoming edges
        for (node, degree) in &in_degrees {
            if *degree == 0 {
                queue.push_back(*node);
            }
        }

        // Process queue
        while let Some(node) = queue.pop_front() {
            result.push(node);

            if let Some(deps) = self.edges.get(&node) {
                for dep in deps {
                    let degree = in_degrees.get_mut(dep).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*dep);
                    }
                }
            }
        }

        // Check for cycles
        if result.len() != self.nodes.len() {
            return Err(SchemaError::Evolution {
                from: self.root.to_string(),
                to: "resolved".to_string(),
                reason: "Circular reference detected in schema dependencies".to_string(),
            });
        }

        // Reverse to get dependency order (dependencies first)
        result.reverse();
        Ok(result)
    }

    fn detect_cycles(&self) -> Option<Vec<u32>> {
        // Simple cycle detection using DFS
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();
        let mut path = Vec::new();

        for node in self.nodes.keys() {
            if !visited.contains(node) {
                if self.has_cycle_dfs(*node, &mut visited, &mut rec_stack, &mut path) {
                    return Some(path);
                }
            }
        }

        None
    }

    fn has_cycle_dfs(
        &self,
        node: u32,
        visited: &mut std::collections::HashSet<u32>,
        rec_stack: &mut std::collections::HashSet<u32>,
        path: &mut Vec<u32>,
    ) -> bool {
        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        if let Some(deps) = self.edges.get(&node) {
            for dep in deps {
                if !visited.contains(dep) {
                    if self.has_cycle_dfs(*dep, visited, rec_stack, path) {
                        return true;
                    }
                } else if rec_stack.contains(dep) {
                    // Found a cycle
                    path.push(*dep);
                    return true;
                }
            }
        }

        rec_stack.remove(&node);
        path.pop();
        false
    }

    fn is_acyclic(&self) -> bool {
        self.detect_cycles().is_none()
    }
}

impl SchemaReferenceResolver {
    fn new() -> Self {
        Self {
            registry: Arc::new(RwLock::new(SchemaRegistryClient::new("".to_string()))),
            dependency_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn new_stub() -> Self {
        Self {
            registry: Arc::new(RwLock::new(SchemaRegistryClient::new("stub".to_string()))),
            dependency_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
