//! Schema Reference Resolution Engine
//!
//! Advanced reference resolution with circular dependency detection,
//! schema evolution tracking, and compatibility validation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::registry_client::{
    CachedSchema, DependencyGraph, GraphNode, ResolvedSchema, SchemaReference, SchemaRegistryClient,
};
use crate::ferris::schema::{SchemaError, SchemaResult};

/// Advanced schema reference resolver with evolution support
pub struct SchemaReferenceResolver {
    registry: Arc<SchemaRegistryClient>,
    dependency_cache: Arc<RwLock<HashMap<u32, Vec<ResolvedDependency>>>>,
    evolution_tracker: Arc<RwLock<SchemaEvolutionTracker>>,
    config: ResolverConfig,
}

/// Configuration for reference resolver
#[derive(Debug, Clone)]
pub struct ResolverConfig {
    /// Maximum depth for reference resolution
    pub max_depth: usize,
    /// Enable circular reference detection
    pub detect_cycles: bool,
    /// Cache resolved dependencies
    pub cache_dependencies: bool,
    /// Validate schema compatibility
    pub validate_compatibility: bool,
    /// Maximum number of references per schema
    pub max_references: usize,
}

/// Resolved dependency with metadata
#[derive(Debug, Clone)]
pub struct ResolvedDependency {
    pub schema_id: u32,
    pub subject: String,
    pub version: i32,
    pub depth: usize,
    pub is_circular: bool,
    pub resolution_time_ms: u64,
}

/// Schema evolution tracking
pub struct SchemaEvolutionTracker {
    evolution_history: HashMap<String, Vec<EvolutionRecord>>,
    compatibility_cache: HashMap<(u32, u32), CompatibilityResult>,
}

/// Evolution record for a schema
#[derive(Debug, Clone)]
pub struct EvolutionRecord {
    pub from_version: i32,
    pub to_version: i32,
    pub from_schema_id: u32,
    pub to_schema_id: u32,
    pub evolution_type: EvolutionType,
    pub timestamp: std::time::Instant,
}

/// Types of schema evolution
#[derive(Debug, Clone, PartialEq)]
pub enum EvolutionType {
    FieldAdded,
    FieldRemoved,
    FieldTypeChanged,
    FieldRenamed,
    DefaultAdded,
    ReferenceAdded,
    ReferenceRemoved,
    Compatible,
    Breaking,
}

/// Compatibility validation result
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    pub is_compatible: bool,
    pub compatibility_level: CompatibilityLevel,
    pub breaking_changes: Vec<BreakingChange>,
    pub warnings: Vec<String>,
}

/// Schema compatibility levels
#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityLevel {
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

/// Breaking change description
#[derive(Debug, Clone)]
pub struct BreakingChange {
    pub change_type: EvolutionType,
    pub field_name: Option<String>,
    pub description: String,
    pub severity: ChangeSeverity,
}

/// Severity of schema changes
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Schema migration plan
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub from_schema: ResolvedSchema,
    pub to_schema: ResolvedSchema,
    pub migration_steps: Vec<MigrationStep>,
    pub estimated_duration_ms: u64,
    pub risk_level: RiskLevel,
}

/// Individual migration step
#[derive(Debug, Clone)]
pub struct MigrationStep {
    pub step_number: usize,
    pub operation: MigrationOperation,
    pub description: String,
    pub is_reversible: bool,
}

/// Migration operations
#[derive(Debug, Clone)]
pub enum MigrationOperation {
    AddField {
        name: String,
        default_value: Option<String>,
    },
    RemoveField {
        name: String,
    },
    RenameField {
        from: String,
        to: String,
    },
    ChangeFieldType {
        name: String,
        from_type: String,
        to_type: String,
    },
    AddReference {
        reference: SchemaReference,
    },
    RemoveReference {
        reference: SchemaReference,
    },
    DataTransformation {
        field: String,
        transformation: String,
    },
}

/// Risk level for migrations
#[derive(Debug, Clone, PartialEq)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl SchemaReferenceResolver {
    /// Create a new reference resolver
    pub fn new(registry: Arc<SchemaRegistryClient>) -> Self {
        Self {
            registry,
            dependency_cache: Arc::new(RwLock::new(HashMap::new())),
            evolution_tracker: Arc::new(RwLock::new(SchemaEvolutionTracker::new())),
            config: ResolverConfig::default(),
        }
    }

    /// Create a stub resolver without registry (to avoid circular dependencies)
    pub fn new_stub() -> Self {
        let stub_registry = Arc::new(SchemaRegistryClient::new("stub".to_string()));
        Self {
            registry: stub_registry,
            dependency_cache: Arc::new(RwLock::new(HashMap::new())),
            evolution_tracker: Arc::new(RwLock::new(SchemaEvolutionTracker::new())),
            config: ResolverConfig::default(),
        }
    }

    /// Create resolver with custom configuration
    pub fn with_config(registry: Arc<SchemaRegistryClient>, config: ResolverConfig) -> Self {
        Self {
            registry,
            dependency_cache: Arc::new(RwLock::new(HashMap::new())),
            evolution_tracker: Arc::new(RwLock::new(SchemaEvolutionTracker::new())),
            config,
        }
    }

    /// Resolve all references for a schema
    pub async fn resolve_references(&self, schema_id: u32) -> SchemaResult<ResolvedSchema> {
        // Check cache first
        if self.config.cache_dependencies {
            if let Some(resolved) = self.get_cached_resolution(schema_id).await {
                return Ok(resolved);
            }
        }

        let start_time = std::time::Instant::now();

        // Build dependency graph
        let graph = self.build_dependency_graph(schema_id).await?;

        // Detect circular references if enabled
        if self.config.detect_cycles {
            self.detect_and_handle_cycles(&graph)?;
        }

        // Resolve schemas in dependency order
        let resolved = self.resolve_from_graph(schema_id, &graph).await?;

        // Cache the resolution
        if self.config.cache_dependencies {
            self.cache_resolution(
                schema_id,
                resolved.clone(),
                start_time.elapsed().as_millis() as u64,
            )
            .await;
        }

        Ok(resolved)
    }

    /// Build complete dependency graph
    pub async fn build_dependency_graph(&self, root_id: u32) -> SchemaResult<DependencyGraph> {
        let mut graph = DependencyGraph {
            root: root_id,
            nodes: HashMap::new(),
            edges: HashMap::new(),
            resolution_order: Vec::new(),
        };

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back((root_id, 0));

        while let Some((schema_id, depth)) = queue.pop_front() {
            if visited.contains(&schema_id) {
                continue;
            }

            if depth > self.config.max_depth {
                return Err(SchemaError::Evolution {
                    from: root_id.to_string(),
                    to: schema_id.to_string(),
                    reason: format!("Maximum reference depth {} exceeded", self.config.max_depth),
                });
            }

            visited.insert(schema_id);

            let schema = self.registry.get_schema(schema_id).await?;

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
            if schema.references.len() > self.config.max_references {
                return Err(SchemaError::Evolution {
                    from: root_id.to_string(),
                    to: schema_id.to_string(),
                    reason: format!("Maximum references {} exceeded", self.config.max_references),
                });
            }

            for reference in &schema.references {
                if let Some(ref_id) = reference.schema_id {
                    dependencies.push(ref_id);
                    if !visited.contains(&ref_id) {
                        queue.push_back((ref_id, depth + 1));
                    }
                }
            }

            graph.edges.insert(schema_id, dependencies);
        }

        // Calculate resolution order
        graph.resolution_order = self.topological_sort(&graph)?;

        Ok(graph)
    }

    /// Detect and handle circular references
    pub fn detect_and_handle_cycles(&self, graph: &DependencyGraph) -> SchemaResult<()> {
        let mut color = HashMap::new();
        let mut parent = HashMap::new();

        for node in graph.nodes.keys() {
            color.insert(*node, Color::White);
        }

        for node in graph.nodes.keys() {
            if color[node] == Color::White {
                if self.dfs_cycle_detection(*node, &graph, &mut color, &mut parent)? {
                    // Found a cycle, build the cycle path
                    let cycle_path = self.build_cycle_path(&parent);
                    return Err(SchemaError::Evolution {
                        from: graph.root.to_string(),
                        to: "resolved".to_string(),
                        reason: format!("Circular reference detected: {:?}", cycle_path),
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate reference compatibility
    pub async fn validate_reference_compatibility(
        &self,
        old_schema: &ResolvedSchema,
        new_schema: &ResolvedSchema,
    ) -> SchemaResult<CompatibilityResult> {
        let mut result = CompatibilityResult {
            is_compatible: true,
            compatibility_level: CompatibilityLevel::Full,
            breaking_changes: Vec::new(),
            warnings: Vec::new(),
        };

        // Check if old schema references are preserved in new schema
        for (id, _old_dep) in &old_schema.dependencies {
            if !new_schema.dependencies.contains_key(id) {
                result.breaking_changes.push(BreakingChange {
                    change_type: EvolutionType::ReferenceRemoved,
                    field_name: None,
                    description: format!("Reference to schema {} removed", id),
                    severity: ChangeSeverity::Error,
                });
                result.is_compatible = false;
            }
        }

        // Check for new references (usually compatible)
        for (id, _) in &new_schema.dependencies {
            if !old_schema.dependencies.contains_key(id) {
                result
                    .warnings
                    .push(format!("New reference to schema {} added", id));
            }
        }

        // Track the evolution
        let evolution = EvolutionRecord {
            from_version: old_schema.root_schema.version,
            to_version: new_schema.root_schema.version,
            from_schema_id: old_schema.root_schema.id,
            to_schema_id: new_schema.root_schema.id,
            evolution_type: if result.is_compatible {
                EvolutionType::Compatible
            } else {
                EvolutionType::Breaking
            },
            timestamp: std::time::Instant::now(),
        };

        self.track_evolution(old_schema.root_schema.subject.clone(), evolution)
            .await;

        Ok(result)
    }

    /// Generate migration plan between schemas
    pub async fn generate_migration_plan(
        &self,
        from_schema: &ResolvedSchema,
        to_schema: &ResolvedSchema,
    ) -> SchemaResult<MigrationPlan> {
        let mut steps = Vec::new();
        let mut risk_level = RiskLevel::Low;

        // Analyze field changes
        let from_fields = self.extract_fields(&from_schema.flattened_schema);
        let to_fields = self.extract_fields(&to_schema.flattened_schema);

        // Fields to remove
        for field in &from_fields {
            if !to_fields.contains(field) {
                steps.push(MigrationStep {
                    step_number: steps.len() + 1,
                    operation: MigrationOperation::RemoveField {
                        name: field.clone(),
                    },
                    description: format!("Remove field '{}'", field),
                    is_reversible: false,
                });
                risk_level = RiskLevel::High;
            }
        }

        // Fields to add
        for field in &to_fields {
            if !from_fields.contains(field) {
                steps.push(MigrationStep {
                    step_number: steps.len() + 1,
                    operation: MigrationOperation::AddField {
                        name: field.clone(),
                        default_value: Some("null".to_string()),
                    },
                    description: format!("Add field '{}' with default value", field),
                    is_reversible: true,
                });
                if risk_level == RiskLevel::Low {
                    risk_level = RiskLevel::Medium;
                }
            }
        }

        // Reference changes
        for (id, _) in &from_schema.dependencies {
            if !to_schema.dependencies.contains_key(id) {
                steps.push(MigrationStep {
                    step_number: steps.len() + 1,
                    operation: MigrationOperation::RemoveReference {
                        reference: SchemaReference {
                            name: format!("schema_{}", id),
                            subject: String::new(),
                            version: None,
                            schema_id: Some(*id),
                        },
                    },
                    description: format!("Remove reference to schema {}", id),
                    is_reversible: false,
                });
                risk_level = RiskLevel::Critical;
            }
        }

        let estimated_duration = 1000 * steps.len() as u64;

        Ok(MigrationPlan {
            from_schema: from_schema.clone(),
            to_schema: to_schema.clone(),
            migration_steps: steps,
            estimated_duration_ms: estimated_duration,
            risk_level,
        })
    }

    /// Create a rollout plan for gradual schema evolution
    pub async fn create_rollout_plan(
        &self,
        subject: &str,
        target_schema: &str,
        strategy: RolloutStrategy,
    ) -> SchemaResult<RolloutPlan> {
        let current_schema = self.registry.get_latest_schema(subject).await?;

        let plan = match strategy {
            RolloutStrategy::Canary { percentage } => {
                self.create_canary_plan(current_schema, target_schema, percentage)
                    .await?
            }
            RolloutStrategy::BlueGreen => {
                self.create_blue_green_plan(current_schema, target_schema)
                    .await?
            }
            RolloutStrategy::Rolling { batch_size } => {
                self.create_rolling_plan(current_schema, target_schema, batch_size)
                    .await?
            }
        };

        Ok(plan)
    }

    // Private helper methods

    async fn get_cached_resolution(&self, _schema_id: u32) -> Option<ResolvedSchema> {
        // TODO: Implement cache lookup
        None
    }

    async fn cache_resolution(&self, schema_id: u32, resolved: ResolvedSchema, duration_ms: u64) {
        let mut cache = self.dependency_cache.write().await;
        let dependency = ResolvedDependency {
            schema_id,
            subject: resolved.root_schema.subject.clone(),
            version: resolved.root_schema.version,
            depth: resolved.resolution_path.len(),
            is_circular: false,
            resolution_time_ms: duration_ms,
        };
        cache
            .entry(schema_id)
            .or_insert_with(Vec::new)
            .push(dependency);
    }

    async fn resolve_from_graph(
        &self,
        root_id: u32,
        graph: &DependencyGraph,
    ) -> SchemaResult<ResolvedSchema> {
        let root_schema = self.registry.get_schema(root_id).await?;
        let mut dependencies = HashMap::new();

        for node_id in &graph.resolution_order {
            if *node_id != root_id {
                let dep_schema = self.registry.get_schema(*node_id).await?;
                dependencies.insert(*node_id, dep_schema);
            }
        }

        // Flatten schema (simplified - real implementation would merge schemas)
        let flattened = self.flatten_schema(&root_schema, &dependencies)?;

        Ok(ResolvedSchema {
            root_schema,
            dependencies,
            flattened_schema: flattened,
            resolution_path: graph.resolution_order.clone(),
        })
    }

    fn flatten_schema(
        &self,
        root: &CachedSchema,
        dependencies: &HashMap<u32, CachedSchema>,
    ) -> SchemaResult<String> {
        // Simplified flattening - real implementation would properly merge schemas
        let mut flattened = root.schema.clone();

        for (_, dep) in dependencies {
            flattened.push_str(&format!("\n// Referenced from {}\n", dep.subject));
            flattened.push_str(&dep.schema);
        }

        Ok(flattened)
    }

    fn topological_sort(&self, graph: &DependencyGraph) -> SchemaResult<Vec<u32>> {
        let mut result = Vec::new();
        let mut in_degrees = HashMap::new();
        let mut queue = VecDeque::new();

        // Initialize in-degrees
        for node in graph.nodes.keys() {
            in_degrees.insert(*node, 0);
        }

        for deps in graph.edges.values() {
            for dep in deps {
                *in_degrees.get_mut(dep).unwrap() += 1;
            }
        }

        // Start with nodes that have no dependencies
        for (node, degree) in &in_degrees {
            if *degree == 0 {
                queue.push_back(*node);
            }
        }

        // Process queue
        while let Some(node) = queue.pop_front() {
            result.push(node);

            if let Some(deps) = graph.edges.get(&node) {
                for dep in deps {
                    let degree = in_degrees.get_mut(dep).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*dep);
                    }
                }
            }
        }

        if result.len() != graph.nodes.len() {
            return Err(SchemaError::Evolution {
                from: graph.root.to_string(),
                to: "resolved".to_string(),
                reason: "Circular dependency detected during topological sort".to_string(),
            });
        }

        result.reverse(); // Dependencies first
        Ok(result)
    }

    fn dfs_cycle_detection(
        &self,
        node: u32,
        graph: &DependencyGraph,
        color: &mut HashMap<u32, Color>,
        parent: &mut HashMap<u32, Option<u32>>,
    ) -> SchemaResult<bool> {
        color.insert(node, Color::Gray);

        if let Some(deps) = graph.edges.get(&node) {
            for dep in deps {
                parent.insert(*dep, Some(node));

                match color[dep] {
                    Color::Gray => return Ok(true), // Found a cycle
                    Color::White => {
                        if self.dfs_cycle_detection(*dep, graph, color, parent)? {
                            return Ok(true);
                        }
                    }
                    Color::Black => {} // Already processed
                }
            }
        }

        color.insert(node, Color::Black);
        Ok(false)
    }

    fn build_cycle_path(&self, parent: &HashMap<u32, Option<u32>>) -> Vec<u32> {
        // Simplified - would need to track the actual cycle
        parent.keys().copied().collect()
    }

    fn extract_fields(&self, schema: &str) -> HashSet<String> {
        // Simplified field extraction
        let mut fields = HashSet::new();
        for line in schema.lines() {
            if line.contains("\"name\"") {
                if let Some(field_name) = line.split('"').nth(3) {
                    fields.insert(field_name.to_string());
                }
            }
        }
        fields
    }

    async fn track_evolution(&self, subject: String, record: EvolutionRecord) {
        let mut tracker = self.evolution_tracker.write().await;
        tracker
            .evolution_history
            .entry(subject)
            .or_insert_with(Vec::new)
            .push(record);
    }

    async fn create_canary_plan(
        &self,
        _current: CachedSchema,
        _target: &str,
        percentage: u8,
    ) -> SchemaResult<RolloutPlan> {
        Ok(RolloutPlan {
            strategy: RolloutStrategy::Canary { percentage },
            phases: vec![
                RolloutPhase {
                    phase_number: 1,
                    description: format!("Deploy to {}% of consumers", percentage),
                    duration_minutes: 60,
                    rollback_trigger: Some("error_rate > 1%".to_string()),
                },
                RolloutPhase {
                    phase_number: 2,
                    description: "Deploy to remaining consumers".to_string(),
                    duration_minutes: 30,
                    rollback_trigger: None,
                },
            ],
            estimated_total_time_minutes: 90,
            risk_assessment: RiskLevel::Medium,
        })
    }

    async fn create_blue_green_plan(
        &self,
        _current: CachedSchema,
        _target: &str,
    ) -> SchemaResult<RolloutPlan> {
        Ok(RolloutPlan {
            strategy: RolloutStrategy::BlueGreen,
            phases: vec![
                RolloutPhase {
                    phase_number: 1,
                    description: "Deploy to green environment".to_string(),
                    duration_minutes: 30,
                    rollback_trigger: Some("validation_failed".to_string()),
                },
                RolloutPhase {
                    phase_number: 2,
                    description: "Switch traffic to green".to_string(),
                    duration_minutes: 5,
                    rollback_trigger: Some("error_spike".to_string()),
                },
            ],
            estimated_total_time_minutes: 35,
            risk_assessment: RiskLevel::Low,
        })
    }

    async fn create_rolling_plan(
        &self,
        _current: CachedSchema,
        _target: &str,
        batch_size: usize,
    ) -> SchemaResult<RolloutPlan> {
        Ok(RolloutPlan {
            strategy: RolloutStrategy::Rolling { batch_size },
            phases: vec![
                RolloutPhase {
                    phase_number: 1,
                    description: format!("Deploy to first {} instances", batch_size),
                    duration_minutes: 15,
                    rollback_trigger: Some("health_check_failed".to_string()),
                },
                RolloutPhase {
                    phase_number: 2,
                    description: "Continue rolling deployment".to_string(),
                    duration_minutes: 45,
                    rollback_trigger: Some("error_rate > 0.5%".to_string()),
                },
            ],
            estimated_total_time_minutes: 60,
            risk_assessment: RiskLevel::Medium,
        })
    }
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            max_depth: 10,
            detect_cycles: true,
            cache_dependencies: true,
            validate_compatibility: true,
            max_references: 100,
        }
    }
}

impl SchemaEvolutionTracker {
    fn new() -> Self {
        Self {
            evolution_history: HashMap::new(),
            compatibility_cache: HashMap::new(),
        }
    }
}

/// Rollout strategy for schema changes
#[derive(Debug, Clone)]
pub enum RolloutStrategy {
    Canary { percentage: u8 },
    BlueGreen,
    Rolling { batch_size: usize },
}

/// Schema rollout plan
#[derive(Debug, Clone)]
pub struct RolloutPlan {
    pub strategy: RolloutStrategy,
    pub phases: Vec<RolloutPhase>,
    pub estimated_total_time_minutes: u64,
    pub risk_assessment: RiskLevel,
}

/// Individual rollout phase
#[derive(Debug, Clone)]
pub struct RolloutPhase {
    pub phase_number: usize,
    pub description: String,
    pub duration_minutes: u64,
    pub rollback_trigger: Option<String>,
}

/// DFS colors for cycle detection
#[derive(Debug, Clone, Copy, PartialEq)]
enum Color {
    White, // Not visited
    Gray,  // Currently visiting
    Black, // Completely visited
}
