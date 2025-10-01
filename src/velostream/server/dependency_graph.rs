//! Dependency Graph for Table Loading Order Resolution
//!
//! Provides dependency graph management and topological sorting for determining
//! optimal table loading order. Supports cycle detection, dependency validation,
//! and wave-based parallel loading groups.

use std::collections::{HashMap, HashSet, VecDeque};

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

    /// Add a table with metadata
    pub fn add_table_with_metadata(
        &mut self,
        name: String,
        dependencies: HashSet<String>,
        metadata: TableMetadata,
    ) {
        self.nodes.insert(
            name.clone(),
            TableNode {
                name,
                dependencies,
                metadata: Some(metadata),
            },
        );
    }

    /// Compute topological sort order for loading tables
    /// Returns tables in loading order (dependencies first)
    ///
    /// Uses Kahn's algorithm for topological sorting with cycle detection.
    pub fn topological_load_order(&self) -> Result<Vec<String>, DependencyError> {
        // 1. Calculate in-degree for each node
        let mut in_degree: HashMap<String, usize> = HashMap::new();

        // Initialize all nodes with in-degree 0
        for node in self.nodes.values() {
            in_degree.entry(node.name.clone()).or_insert(0);
        }

        // Calculate actual in-degrees
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
            let cycle_tables: Vec<String> = self
                .nodes
                .keys()
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
                    "Unable to compute loading waves".to_string(),
                ));
            }

            // Sort wave by priority if metadata available
            current_wave.sort_by(|a, b| {
                let priority_a = self
                    .nodes
                    .get(a)
                    .and_then(|n| n.metadata.as_ref())
                    .map(|m| m.priority)
                    .unwrap_or(0);
                let priority_b = self
                    .nodes
                    .get(b)
                    .and_then(|n| n.metadata.as_ref())
                    .map(|m| m.priority)
                    .unwrap_or(0);
                priority_b.cmp(&priority_a) // Higher priority first
            });

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

    /// Get the number of tables in the graph
    pub fn table_count(&self) -> usize {
        self.nodes.len()
    }

    /// Check if a table exists in the graph
    pub fn contains_table(&self, table_name: &str) -> bool {
        self.nodes.contains_key(table_name)
    }

    /// Remove a table from the graph
    pub fn remove_table(&mut self, table_name: &str) -> Option<TableNode> {
        self.nodes.remove(table_name)
    }

    /// Get all table names
    pub fn table_names(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }
}

impl Default for TableDependencyGraph {
    fn default() -> Self {
        Self::new()
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
    /// Source type ("kafka", "file", "sql", etc.)
    pub source_type: String,

    /// Estimated size in records (if known)
    pub estimated_size: Option<usize>,

    /// Priority for loading (higher loads first within wave)
    pub priority: i32,
}

impl TableMetadata {
    /// Create new metadata with defaults
    pub fn new(source_type: String) -> Self {
        Self {
            source_type,
            estimated_size: None,
            priority: 0,
        }
    }

    /// Set estimated size
    pub fn with_estimated_size(mut self, size: usize) -> Self {
        self.estimated_size = Some(size);
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}
