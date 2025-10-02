# Phase 4: Parallel Loading & Dependency Graph - Implementation Plan

**Status**: 🔄 **READY TO IMPLEMENT**
**Timeline**: 1-2 weeks
**Dependencies**: Phase 1-3 Complete ✅
**Priority**: **MEDIUM** - Performance optimization for multi-table scenarios

---

## 🎯 **Overview**

Implement dependency graph resolution and parallel loading optimization to enable efficient multi-table loading with proper dependency management.

**Key Benefits**:
- **Faster startup**: Load independent tables in parallel
- **Correct ordering**: Respect table dependencies automatically
- **Cycle detection**: Prevent circular dependency errors
- **Resource optimization**: Configurable parallelism limits

---

## 📋 **Feature 1: Dependency Graph Resolution**

### **Objective**
Build a directed acyclic graph (DAG) of table dependencies and compute optimal loading order using topological sort.

### **Architecture**

```rust
// File: src/velostream/server/dependency_graph.rs

use std::collections::{HashMap, HashSet, VecDeque};
use crate::velostream::sql::error::SqlError;

/// Represents a table node in the dependency graph
#[derive(Debug, Clone)]
pub struct TableNode {
    /// Table name
    pub name: String,

    /// Tables this table depends on (must load before this)
    pub dependencies: HashSet<String>,

    /// Optional metadata about the table
    pub metadata: Option<TableMetadata>,
}

/// Dependency graph for managing table loading order
#[derive(Debug, Clone)]
pub struct TableDependencyGraph {
    /// All table nodes indexed by name
    nodes: HashMap<String, TableNode>,
}

impl TableDependencyGraph {
    /// Create an empty dependency graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Add a table to the graph
    pub fn add_table(&mut self, name: String, dependencies: HashSet<String>) {
        self.nodes.insert(
            name.clone(),
            TableNode {
                name,
                dependencies,
                metadata: None,
            },
        );
    }

    /// Compute topological sort order for loading tables
    /// Returns tables in loading order (dependencies first)
    pub fn topological_load_order(&self) -> Result<Vec<String>, DependencyError> {
        // Kahn's algorithm for topological sorting

        // 1. Calculate in-degree for each node
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        for node in self.nodes.values() {
            in_degree.entry(node.name.clone()).or_insert(0);
            for dep in &node.dependencies {
                *in_degree.entry(dep.clone()).or_insert(0) += 0; // Ensure dep exists
            }
        }

        for node in self.nodes.values() {
            for dep in &node.dependencies {
                *in_degree.entry(node.name.clone()).or_insert(0) += 1;
            }
        }

        // 2. Initialize queue with nodes having in-degree 0
        let mut queue: VecDeque<String> = VecDeque::new();
        for (name, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(name.clone());
            }
        }

        // 3. Process nodes in topological order
        let mut sorted_order = Vec::new();

        while let Some(current) = queue.pop_front() {
            sorted_order.push(current.clone());

            // Find all nodes that depend on current
            for node in self.nodes.values() {
                if node.dependencies.contains(&current) {
                    let degree = in_degree.get_mut(&node.name).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(node.name.clone());
                    }
                }
            }
        }

        // 4. Check for cycles
        if sorted_order.len() != self.nodes.len() {
            // Cycle detected - find which tables are in the cycle
            let cycle_tables: Vec<String> = self.nodes.keys()
                .filter(|name| !sorted_order.contains(name))
                .cloned()
                .collect();

            return Err(DependencyError::CircularDependency {
                tables: cycle_tables,
                description: "Circular dependency detected in table definitions".to_string(),
            });
        }

        Ok(sorted_order)
    }

    /// Detect if there are any circular dependencies
    pub fn detect_cycles(&self) -> Result<(), DependencyError> {
        self.topological_load_order()?;
        Ok(())
    }

    /// Get all direct dependencies for a table
    pub fn get_dependencies(&self, table_name: &str) -> Option<&HashSet<String>> {
        self.nodes.get(table_name).map(|node| &node.dependencies)
    }

    /// Get all tables that depend on this table (reverse dependencies)
    pub fn get_dependents(&self, table_name: &str) -> Vec<String> {
        self.nodes
            .values()
            .filter(|node| node.dependencies.contains(table_name))
            .map(|node| node.name.clone())
            .collect()
    }

    /// Group tables into loading "waves" for parallel execution
    /// Each wave contains independent tables that can be loaded in parallel
    pub fn compute_loading_waves(&self) -> Result<Vec<Vec<String>>, DependencyError> {
        let sorted_order = self.topological_load_order()?;

        let mut waves: Vec<Vec<String>> = Vec::new();
        let mut loaded: HashSet<String> = HashSet::new();
        let mut remaining: HashSet<String> = sorted_order.iter().cloned().collect();

        while !remaining.is_empty() {
            let mut current_wave = Vec::new();

            // Find all tables whose dependencies are satisfied
            for table in &remaining {
                if let Some(node) = self.nodes.get(table) {
                    if node.dependencies.iter().all(|dep| loaded.contains(dep)) {
                        current_wave.push(table.clone());
                    }
                }
            }

            if current_wave.is_empty() {
                // This shouldn't happen if topological sort succeeded
                return Err(DependencyError::InternalError(
                    "Unable to compute loading waves".to_string()
                ));
            }

            // Mark these tables as loaded
            for table in &current_wave {
                loaded.insert(table.clone());
                remaining.remove(table);
            }

            waves.push(current_wave);
        }

        Ok(waves)
    }

    /// Validate that all dependencies exist in the graph
    pub fn validate_dependencies(&self) -> Result<(), DependencyError> {
        for node in self.nodes.values() {
            for dep in &node.dependencies {
                if !self.nodes.contains_key(dep) {
                    return Err(DependencyError::MissingDependency {
                        table: node.name.clone(),
                        missing_dependency: dep.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Errors that can occur in dependency graph operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum DependencyError {
    #[error("Circular dependency detected among tables: {tables:?}. {description}")]
    CircularDependency {
        tables: Vec<String>,
        description: String,
    },

    #[error("Table '{table}' depends on '{missing_dependency}' which does not exist")]
    MissingDependency {
        table: String,
        missing_dependency: String,
    },

    #[error("Internal error in dependency graph: {0}")]
    InternalError(String),
}

/// Optional metadata about a table
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub source_type: String,  // "kafka", "file", "sql", etc.
    pub estimated_size: Option<usize>,
    pub priority: i32,  // Higher priority loads first (within wave)
}
```

### **Implementation Steps**

#### **Step 1: Core Graph Structure (Day 1-2)**
- [ ] Create `src/velostream/server/dependency_graph.rs`
- [ ] Implement `TableNode` and `TableDependencyGraph` structs
- [ ] Implement basic graph operations (add_table, get_dependencies)
- [ ] Add `DependencyError` enum with proper error messages

#### **Step 2: Topological Sort (Day 2-3)**
- [ ] Implement Kahn's algorithm for topological sorting
- [ ] Add in-degree calculation
- [ ] Add cycle detection with detailed error reporting
- [ ] Implement `compute_loading_waves()` for parallel execution groups

#### **Step 3: Validation & Utilities (Day 3-4)**
- [ ] Implement `validate_dependencies()` to check for missing tables
- [ ] Add `get_dependents()` for reverse dependency lookup
- [ ] Add graph visualization/debugging utilities
- [ ] Implement metadata support for table prioritization

#### **Step 4: Testing (Day 4-5)**
- [ ] Unit tests for basic graph operations
- [ ] Tests for topological sort with various graph shapes
- [ ] Cycle detection tests (A→B→C→A)
- [ ] Missing dependency validation tests
- [ ] Complex multi-table dependency scenarios
- [ ] Loading waves computation tests

### **Test Cases**

```rust
// tests/unit/server/dependency_graph_test.rs

#[test]
fn test_simple_dependency_chain() {
    // A -> B -> C
    let mut graph = TableDependencyGraph::new();
    graph.add_table("C".to_string(), HashSet::new());
    graph.add_table("B".to_string(), vec!["C"].into_iter().collect());
    graph.add_table("A".to_string(), vec!["B"].into_iter().collect());

    let order = graph.topological_load_order().unwrap();
    assert_eq!(order, vec!["C", "B", "A"]);
}

#[test]
fn test_parallel_loading_opportunity() {
    // A -> C, B -> C (A and B can load in parallel)
    let mut graph = TableDependencyGraph::new();
    graph.add_table("C".to_string(), HashSet::new());
    graph.add_table("A".to_string(), vec!["C"].into_iter().collect());
    graph.add_table("B".to_string(), vec!["C"].into_iter().collect());

    let waves = graph.compute_loading_waves().unwrap();
    assert_eq!(waves.len(), 2);
    assert_eq!(waves[0], vec!["C"]);
    assert!(waves[1].contains(&"A".to_string()) && waves[1].contains(&"B".to_string()));
}

#[test]
fn test_circular_dependency_detection() {
    // A -> B -> C -> A (cycle)
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), vec!["C"].into_iter().collect());
    graph.add_table("B".to_string(), vec!["A"].into_iter().collect());
    graph.add_table("C".to_string(), vec!["B"].into_iter().collect());

    let result = graph.detect_cycles();
    assert!(result.is_err());
    match result.unwrap_err() {
        DependencyError::CircularDependency { tables, .. } => {
            assert_eq!(tables.len(), 3);
        }
        _ => panic!("Expected CircularDependency error"),
    }
}

#[test]
fn test_missing_dependency_validation() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), vec!["B"].into_iter().collect());
    // B is not added!

    let result = graph.validate_dependencies();
    assert!(result.is_err());
    match result.unwrap_err() {
        DependencyError::MissingDependency { table, missing_dependency } => {
            assert_eq!(table, "A");
            assert_eq!(missing_dependency, "B");
        }
        _ => panic!("Expected MissingDependency error"),
    }
}
```

---

## 📋 **Feature 2: Parallel Loading with Dependencies**

### **Objective**
Coordinate parallel table loading while respecting dependencies and resource limits.

### **Architecture**

```rust
// File: src/velostream/server/parallel_loader.rs

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, RwLock};
use tokio::task::JoinSet;
use futures::future::join_all;

use crate::velostream::server::dependency_graph::{TableDependencyGraph, DependencyError};
use crate::velostream::server::table_registry::TableRegistry;
use crate::velostream::server::progress_monitoring::ProgressMonitor;
use crate::velostream::sql::error::SqlError;

/// Configuration for parallel loading
#[derive(Debug, Clone)]
pub struct ParallelLoadingConfig {
    /// Maximum number of tables to load concurrently
    pub max_parallel: usize,

    /// Timeout for individual table loading
    pub table_load_timeout: Duration,

    /// Whether to stop loading on first failure
    pub fail_fast: bool,

    /// Whether to continue loading on dependency failures
    pub continue_on_dependency_failure: bool,

    /// Maximum total loading time
    pub total_timeout: Option<Duration>,
}

impl Default for ParallelLoadingConfig {
    fn default() -> Self {
        Self {
            max_parallel: 4,  // Conservative default
            table_load_timeout: Duration::from_secs(300),  // 5 minutes per table
            fail_fast: false,  // Try to load as many tables as possible
            continue_on_dependency_failure: false,  // Skip tables with failed dependencies
            total_timeout: Some(Duration::from_secs(1800)),  // 30 minutes total
        }
    }
}

/// Result of a parallel loading operation
#[derive(Debug, Clone)]
pub struct ParallelLoadingResult {
    /// Tables that loaded successfully
    pub successful: Vec<String>,

    /// Tables that failed to load
    pub failed: HashMap<String, String>,  // table_name -> error_message

    /// Tables that were skipped due to dependency failures
    pub skipped: Vec<String>,

    /// Total time taken for the loading operation
    pub total_duration: Duration,

    /// Statistics per wave
    pub wave_stats: Vec<WaveStats>,
}

#[derive(Debug, Clone)]
pub struct WaveStats {
    pub wave_number: usize,
    pub tables: Vec<String>,
    pub duration: Duration,
    pub successful: usize,
    pub failed: usize,
}

/// Parallel table loading coordinator
pub struct ParallelLoader {
    /// Table registry for managing tables
    table_registry: Arc<TableRegistry>,

    /// Progress monitor for real-time updates
    progress_monitor: Arc<ProgressMonitor>,

    /// Configuration
    config: ParallelLoadingConfig,
}

impl ParallelLoader {
    /// Create a new parallel loader
    pub fn new(
        table_registry: Arc<TableRegistry>,
        progress_monitor: Arc<ProgressMonitor>,
        config: ParallelLoadingConfig,
    ) -> Self {
        Self {
            table_registry,
            progress_monitor,
            config,
        }
    }

    /// Load multiple tables in parallel respecting dependencies
    pub async fn load_tables_with_dependencies(
        &self,
        tables: Vec<TableDefinition>,
    ) -> Result<ParallelLoadingResult, SqlError> {
        let start_time = Instant::now();

        // Build dependency graph
        let graph = self.build_dependency_graph(&tables)?;

        // Validate graph (check for cycles and missing dependencies)
        graph.validate_dependencies()
            .map_err(|e| SqlError::ConfigurationError(e.to_string()))?;

        // Compute loading waves
        let waves = graph.compute_loading_waves()
            .map_err(|e| SqlError::ConfigurationError(e.to_string()))?;

        log::info!("Computed {} loading waves for {} tables", waves.len(), tables.len());

        // Load tables wave by wave
        let mut result = ParallelLoadingResult {
            successful: Vec::new(),
            failed: HashMap::new(),
            skipped: Vec::new(),
            total_duration: Duration::ZERO,
            wave_stats: Vec::new(),
        };

        for (wave_num, wave_tables) in waves.iter().enumerate() {
            log::info!("Loading wave {} with {} tables: {:?}",
                wave_num + 1, wave_tables.len(), wave_tables);

            let wave_start = Instant::now();
            let wave_result = self.load_wave(wave_tables, &tables).await;
            let wave_duration = wave_start.elapsed();

            // Update overall result
            result.successful.extend(wave_result.successful.iter().cloned());
            result.failed.extend(wave_result.failed.clone());
            result.skipped.extend(wave_result.skipped.iter().cloned());

            result.wave_stats.push(WaveStats {
                wave_number: wave_num + 1,
                tables: wave_tables.clone(),
                duration: wave_duration,
                successful: wave_result.successful.len(),
                failed: wave_result.failed.len(),
            });

            // Check if we should continue
            if self.config.fail_fast && !wave_result.failed.is_empty() {
                log::warn!("Stopping parallel load due to fail_fast and {} failures",
                    wave_result.failed.len());
                break;
            }

            // Check total timeout
            if let Some(total_timeout) = self.config.total_timeout {
                if start_time.elapsed() > total_timeout {
                    log::warn!("Parallel loading exceeded total timeout of {:?}", total_timeout);
                    break;
                }
            }
        }

        result.total_duration = start_time.elapsed();

        log::info!("Parallel loading completed: {} successful, {} failed, {} skipped in {:?}",
            result.successful.len(), result.failed.len(), result.skipped.len(),
            result.total_duration);

        Ok(result)
    }

    /// Load a single wave of independent tables in parallel
    async fn load_wave(
        &self,
        wave_tables: &[String],
        all_tables: &[TableDefinition],
    ) -> WaveLoadResult {
        // Create semaphore to limit parallelism
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel));

        let mut join_set = JoinSet::new();

        for table_name in wave_tables {
            // Find table definition
            let table_def = all_tables.iter()
                .find(|t| t.name == *table_name)
                .cloned();

            if table_def.is_none() {
                log::warn!("Table definition not found for '{}'", table_name);
                continue;
            }

            let table_def = table_def.unwrap();
            let semaphore = semaphore.clone();
            let registry = self.table_registry.clone();
            let progress = self.progress_monitor.clone();
            let timeout = self.config.table_load_timeout;

            join_set.spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.unwrap();

                log::info!("Starting load for table '{}'", table_def.name);

                // Start progress tracking
                let tracker = progress.start_tracking(
                    table_def.name.clone(),
                    None,  // Unknown size
                ).await;

                // Load table with timeout
                let result = tokio::time::timeout(
                    timeout,
                    load_single_table(registry, table_def.clone())
                ).await;

                match result {
                    Ok(Ok(())) => {
                        tracker.set_completed().await;
                        log::info!("Successfully loaded table '{}'", table_def.name);
                        (table_def.name, Ok(()))
                    }
                    Ok(Err(e)) => {
                        tracker.set_error(e.to_string()).await;
                        log::error!("Failed to load table '{}': {}", table_def.name, e);
                        (table_def.name, Err(e.to_string()))
                    }
                    Err(_) => {
                        let timeout_msg = format!("Timeout after {:?}", timeout);
                        tracker.set_error(timeout_msg.clone()).await;
                        log::error!("Timeout loading table '{}'", table_def.name);
                        (table_def.name, Err(timeout_msg))
                    }
                }
            });
        }

        // Wait for all tasks to complete
        let mut wave_result = WaveLoadResult::default();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((table_name, Ok(()))) => {
                    wave_result.successful.push(table_name);
                }
                Ok((table_name, Err(error))) => {
                    wave_result.failed.insert(table_name, error);
                }
                Err(join_error) => {
                    log::error!("Task join error: {}", join_error);
                }
            }
        }

        wave_result
    }

    /// Build dependency graph from table definitions
    fn build_dependency_graph(
        &self,
        tables: &[TableDefinition],
    ) -> Result<TableDependencyGraph, SqlError> {
        let mut graph = TableDependencyGraph::new();

        for table in tables {
            // Extract dependencies from table definition
            // (This would parse the SQL to find referenced tables)
            let dependencies = extract_table_dependencies(&table.sql)?;

            graph.add_table(table.name.clone(), dependencies);
        }

        Ok(graph)
    }
}

#[derive(Debug, Clone, Default)]
struct WaveLoadResult {
    successful: Vec<String>,
    failed: HashMap<String, String>,
    skipped: Vec<String>,
}

/// Table definition for loading
#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub name: String,
    pub sql: String,
    pub properties: HashMap<String, String>,
}

/// Load a single table (placeholder - would integrate with actual table loading)
async fn load_single_table(
    registry: Arc<TableRegistry>,
    table_def: TableDefinition,
) -> Result<(), SqlError> {
    // This would integrate with the actual CTAS implementation
    // For now, simulate loading
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

/// Extract table dependencies from SQL (placeholder)
fn extract_table_dependencies(sql: &str) -> Result<HashSet<String>, SqlError> {
    // This would parse the SQL and extract referenced table names
    // For now, return empty set
    Ok(HashSet::new())
}
```

### **Implementation Steps**

#### **Step 1: Core Loader Structure (Day 1-2)**
- [ ] Create `src/velostream/server/parallel_loader.rs`
- [ ] Implement `ParallelLoader` struct with configuration
- [ ] Add `ParallelLoadingResult` and statistics structures
- [ ] Implement basic wave-based loading skeleton

#### **Step 2: Semaphore-Based Parallelism (Day 2-3)**
- [ ] Implement `load_wave()` with tokio Semaphore for concurrency control
- [ ] Add timeout handling per table
- [ ] Integrate progress monitoring for each table
- [ ] Handle task failures gracefully

#### **Step 3: Dependency Integration (Day 3-4)**
- [ ] Implement `build_dependency_graph()` to extract dependencies from SQL
- [ ] Integrate with SQL parser to find table references
- [ ] Add validation before starting load
- [ ] Implement fail-fast and continue-on-error strategies

#### **Step 4: Advanced Features (Day 4-5)**
- [ ] Add total timeout handling
- [ ] Implement table priority/ordering within waves
- [ ] Add detailed logging and metrics
- [ ] Error recovery and retry logic

#### **Step 5: Testing (Day 5-7)**
- [ ] Unit tests for wave loading
- [ ] Integration tests with actual table loading
- [ ] Performance tests with varying parallelism
- [ ] Failure scenario tests (timeouts, errors, cycles)

### **Test Cases**

```rust
// tests/unit/server/parallel_loader_test.rs

#[tokio::test]
async fn test_parallel_loading_basic() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::default();

    let loader = ParallelLoader::new(registry, monitor, config);

    let tables = vec![
        TableDefinition {
            name: "table_a".to_string(),
            sql: "SELECT * FROM source".to_string(),
            properties: HashMap::new(),
        },
        TableDefinition {
            name: "table_b".to_string(),
            sql: "SELECT * FROM source".to_string(),
            properties: HashMap::new(),
        },
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();
    assert_eq!(result.successful.len(), 2);
    assert_eq!(result.failed.len(), 0);
}

#[tokio::test]
async fn test_parallel_loading_with_dependencies() {
    // Test that dependencies are respected
    // table_b depends on table_a
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::default();

    let loader = ParallelLoader::new(registry, monitor, config);

    let tables = vec![
        TableDefinition {
            name: "table_a".to_string(),
            sql: "SELECT * FROM source".to_string(),
            properties: HashMap::new(),
        },
        TableDefinition {
            name: "table_b".to_string(),
            sql: "SELECT * FROM table_a".to_string(),  // Depends on table_a
            properties: HashMap::new(),
        },
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // table_a should load in wave 1, table_b in wave 2
    assert_eq!(result.wave_stats.len(), 2);
    assert_eq!(result.wave_stats[0].tables, vec!["table_a"]);
    assert_eq!(result.wave_stats[1].tables, vec!["table_b"]);
}

#[tokio::test]
async fn test_parallel_loading_concurrency_limit() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig {
        max_parallel: 2,  // Only 2 at a time
        ..Default::default()
    };

    let loader = ParallelLoader::new(registry, monitor, config);

    // 10 independent tables
    let tables: Vec<TableDefinition> = (0..10)
        .map(|i| TableDefinition {
            name: format!("table_{}", i),
            sql: "SELECT * FROM source".to_string(),
            properties: HashMap::new(),
        })
        .collect();

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // All should succeed
    assert_eq!(result.successful.len(), 10);

    // Verify that max 2 were running concurrently
    // (This would require more detailed instrumentation)
}
```

---

## 🔗 **Integration Points**

### **1. TableRegistry Integration**
```rust
// Add to src/velostream/server/table_registry.rs

impl TableRegistry {
    /// Load multiple tables in parallel with dependency management
    pub async fn load_tables_parallel(
        &self,
        tables: Vec<TableDefinition>,
        config: ParallelLoadingConfig,
    ) -> Result<ParallelLoadingResult, SqlError> {
        let loader = ParallelLoader::new(
            Arc::new(self.clone()),
            self.progress_monitor.clone(),
            config,
        );

        loader.load_tables_with_dependencies(tables).await
    }
}
```

### **2. CTAS Integration**
```rust
// Modify CTAS processor to use parallel loading for multiple tables

pub async fn execute_multiple_ctas(
    statements: Vec<CreateTableStatement>,
) -> Result<ParallelLoadingResult, SqlError> {
    let table_defs: Vec<TableDefinition> = statements
        .into_iter()
        .map(|stmt| TableDefinition {
            name: stmt.table_name,
            sql: stmt.query,
            properties: stmt.properties,
        })
        .collect();

    let config = ParallelLoadingConfig::default();
    let registry = get_global_table_registry();

    registry.load_tables_parallel(table_defs, config).await
}
```

### **3. SQL Parser Integration**
```rust
// Add dependency extraction to SQL parser

/// Extract all table references from a SQL query
pub fn extract_table_references(sql: &str) -> Result<HashSet<String>, SqlError> {
    let query = parse_streaming_query(sql)?;

    let mut references = HashSet::new();

    // Extract from FROM clause
    if let Some(source) = &query.source {
        match source {
            StreamSource::Topic(name) |
            StreamSource::Table(name) => {
                references.insert(name.clone());
            }
            _ => {}
        }
    }

    // Extract from JOINs
    for join in &query.joins {
        if let StreamSource::Table(name) = &join.source {
            references.insert(name.clone());
        }
    }

    Ok(references)
}
```

---

## 📊 **Success Metrics**

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Parallel Speedup** | 2-4x faster for 10+ tables | Compare to sequential loading |
| **Dependency Accuracy** | 100% correct ordering | Test with complex dependency graphs |
| **Cycle Detection** | 100% detection rate | Test with various circular patterns |
| **Resource Usage** | Stay within max_parallel limit | Monitor active tasks |
| **Failure Handling** | Graceful degradation | Test with intentional failures |
| **Test Coverage** | >90% for new code | Code coverage tools |

---

## 📅 **Timeline Estimate**

### **Week 1: Dependency Graph (5 days)**
- Day 1-2: Core graph structure
- Day 2-3: Topological sort implementation
- Day 3-4: Validation and utilities
- Day 4-5: Comprehensive testing

### **Week 2: Parallel Loader (5 days)**
- Day 1-2: Core loader structure
- Day 2-3: Semaphore-based parallelism
- Day 3-4: Dependency integration
- Day 4-5: Advanced features
- Day 5-7: Integration testing

**Total**: 10 working days (2 weeks)

---

## 🚀 **Production Usage Example**

```rust
use velostream::velostream::server::parallel_loader::{
    ParallelLoader, ParallelLoadingConfig, TableDefinition
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure parallel loading
    let config = ParallelLoadingConfig {
        max_parallel: 8,  // Load up to 8 tables at once
        table_load_timeout: Duration::from_secs(300),
        fail_fast: false,  // Continue on failures
        ..Default::default()
    };

    // Define tables to load
    let tables = vec![
        TableDefinition {
            name: "user_profiles".to_string(),
            sql: "SELECT * FROM kafka_users".to_string(),
            properties: Default::default(),
        },
        TableDefinition {
            name: "user_preferences".to_string(),
            sql: "SELECT * FROM kafka_preferences".to_string(),
            properties: Default::default(),
        },
        TableDefinition {
            name: "enriched_users".to_string(),
            // Depends on both user_profiles and user_preferences
            sql: "SELECT p.*, pref.* FROM user_profiles p JOIN user_preferences pref ON p.user_id = pref.user_id".to_string(),
            properties: Default::default(),
        },
    ];

    // Load with automatic dependency resolution
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let loader = ParallelLoader::new(registry, monitor, config);

    let result = loader.load_tables_with_dependencies(tables).await?;

    println!("Parallel loading completed:");
    println!("  Successful: {}", result.successful.len());
    println!("  Failed: {}", result.failed.len());
    println!("  Skipped: {}", result.skipped.len());
    println!("  Total time: {:?}", result.total_duration);

    for wave_stat in result.wave_stats {
        println!("Wave {}: {} tables in {:?}",
            wave_stat.wave_number,
            wave_stat.tables.len(),
            wave_stat.duration);
    }

    Ok(())
}
```

---

## ✅ **Completion Checklist**

### **Dependency Graph**
- [ ] Core graph structure implemented
- [ ] Topological sort working correctly
- [ ] Cycle detection with detailed errors
- [ ] Loading waves computation
- [ ] Dependency validation
- [ ] Unit tests passing (>90% coverage)
- [ ] Documentation complete

### **Parallel Loader**
- [ ] Semaphore-based concurrency control
- [ ] Wave-by-wave loading
- [ ] Timeout handling per table and total
- [ ] Progress monitoring integration
- [ ] Error handling and reporting
- [ ] Integration tests passing
- [ ] Performance benchmarks satisfactory

### **Integration**
- [ ] TableRegistry integration
- [ ] CTAS processor integration
- [ ] SQL parser dependency extraction
- [ ] End-to-end tests with real tables
- [ ] Documentation and examples
- [ ] Production readiness review

---

**Document Status**: ✅ Ready for implementation
**Last Updated**: October 1, 2024
