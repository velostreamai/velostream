//! Tests for TableDependencyGraph
//!
//! Comprehensive test suite for dependency graph operations including
//! topological sorting, cycle detection, wave computation, and validation.

use std::collections::HashSet;
use velostream::velostream::server::dependency_graph::{
    DependencyError, TableDependencyGraph, TableMetadata,
};

#[test]
fn test_empty_graph() {
    let graph = TableDependencyGraph::new();
    assert_eq!(graph.table_count(), 0);

    let order = graph.topological_load_order().unwrap();
    assert_eq!(order.len(), 0);
}

#[test]
fn test_single_table_no_dependencies() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("table_a".to_string(), HashSet::new());

    let order = graph.topological_load_order().unwrap();
    assert_eq!(order, vec!["table_a"]);
}

#[test]
fn test_simple_dependency_chain() {
    // A -> B -> C (C must load before B, B before A)
    let mut graph = TableDependencyGraph::new();
    graph.add_table("C".to_string(), HashSet::new());
    graph.add_table("B".to_string(), vec!["C".to_string()].into_iter().collect());
    graph.add_table("A".to_string(), vec!["B".to_string()].into_iter().collect());

    let order = graph.topological_load_order().unwrap();
    assert_eq!(order, vec!["C", "B", "A"]);
}

#[test]
fn test_parallel_loading_opportunity() {
    // A -> C, B -> C (A and B can load in parallel, both depend on C)
    let mut graph = TableDependencyGraph::new();
    graph.add_table("C".to_string(), HashSet::new());
    graph.add_table("A".to_string(), vec!["C".to_string()].into_iter().collect());
    graph.add_table("B".to_string(), vec!["C".to_string()].into_iter().collect());

    let waves = graph.compute_loading_waves().unwrap();
    assert_eq!(waves.len(), 2);
    assert_eq!(waves[0], vec!["C"]);

    // Second wave should contain both A and B (order may vary)
    assert_eq!(waves[1].len(), 2);
    assert!(waves[1].contains(&"A".to_string()));
    assert!(waves[1].contains(&"B".to_string()));
}

#[test]
fn test_diamond_dependency() {
    // D depends on B and C, B and C depend on A
    //     A
    //    / \
    //   B   C
    //    \ /
    //     D
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), HashSet::new());
    graph.add_table("B".to_string(), vec!["A".to_string()].into_iter().collect());
    graph.add_table("C".to_string(), vec!["A".to_string()].into_iter().collect());
    graph.add_table(
        "D".to_string(),
        vec!["B".to_string(), "C".to_string()].into_iter().collect(),
    );

    let waves = graph.compute_loading_waves().unwrap();
    assert_eq!(waves.len(), 3);
    assert_eq!(waves[0], vec!["A"]);
    assert_eq!(waves[1].len(), 2); // B and C in parallel
    assert_eq!(waves[2], vec!["D"]);
}

#[test]
fn test_circular_dependency_detection() {
    // A -> B -> C -> A (cycle)
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), vec!["C".to_string()].into_iter().collect());
    graph.add_table("B".to_string(), vec!["A".to_string()].into_iter().collect());
    graph.add_table("C".to_string(), vec!["B".to_string()].into_iter().collect());

    let result = graph.detect_cycles();
    assert!(result.is_err());

    match result.unwrap_err() {
        DependencyError::CircularDependency { tables, .. } => {
            assert_eq!(tables.len(), 3);
            assert!(tables.contains(&"A".to_string()));
            assert!(tables.contains(&"B".to_string()));
            assert!(tables.contains(&"C".to_string()));
        }
        _ => panic!("Expected CircularDependency error"),
    }
}

#[test]
fn test_self_dependency_cycle() {
    // A depends on itself
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), vec!["A".to_string()].into_iter().collect());

    let result = graph.topological_load_order();
    assert!(result.is_err());

    match result.unwrap_err() {
        DependencyError::CircularDependency { tables, .. } => {
            assert_eq!(tables, vec!["A"]);
        }
        _ => panic!("Expected CircularDependency error"),
    }
}

#[test]
fn test_missing_dependency_validation() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), vec!["B".to_string()].into_iter().collect());
    // B is not added!

    let result = graph.validate_dependencies();
    assert!(result.is_err());

    match result.unwrap_err() {
        DependencyError::MissingDependency {
            table,
            missing_dependency,
        } => {
            assert_eq!(table, "A");
            assert_eq!(missing_dependency, "B");
        }
        _ => panic!("Expected MissingDependency error"),
    }
}

#[test]
fn test_multiple_missing_dependencies() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table(
        "A".to_string(),
        vec!["B".to_string(), "C".to_string()].into_iter().collect(),
    );
    // Neither B nor C exist

    let result = graph.validate_dependencies();
    assert!(result.is_err());
}

#[test]
fn test_get_dependencies() {
    let mut graph = TableDependencyGraph::new();
    let deps: HashSet<String> = vec!["B".to_string(), "C".to_string()].into_iter().collect();
    graph.add_table("A".to_string(), deps.clone());

    let retrieved_deps = graph.get_dependencies("A").unwrap();
    assert_eq!(*retrieved_deps, deps);

    assert!(graph.get_dependencies("NonExistent").is_none());
}

#[test]
fn test_get_dependents() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), HashSet::new());
    graph.add_table("B".to_string(), vec!["A".to_string()].into_iter().collect());
    graph.add_table("C".to_string(), vec!["A".to_string()].into_iter().collect());
    graph.add_table("D".to_string(), vec!["B".to_string()].into_iter().collect());

    let dependents_of_a = graph.get_dependents("A");
    assert_eq!(dependents_of_a.len(), 2);
    assert!(dependents_of_a.contains(&"B".to_string()));
    assert!(dependents_of_a.contains(&"C".to_string()));

    let dependents_of_b = graph.get_dependents("B");
    assert_eq!(dependents_of_b, vec!["D"]);
}

#[test]
fn test_complex_graph() {
    // Test with a more complex realistic scenario
    let mut graph = TableDependencyGraph::new();

    // Raw data sources (no dependencies)
    graph.add_table("raw_events".to_string(), HashSet::new());
    graph.add_table("raw_users".to_string(), HashSet::new());
    graph.add_table("raw_products".to_string(), HashSet::new());

    // Enriched tables (depend on raw)
    graph.add_table(
        "enriched_events".to_string(),
        vec!["raw_events".to_string()].into_iter().collect(),
    );
    graph.add_table(
        "user_profiles".to_string(),
        vec!["raw_users".to_string()].into_iter().collect(),
    );

    // Aggregated tables (depend on enriched)
    graph.add_table(
        "user_activity".to_string(),
        vec!["enriched_events".to_string(), "user_profiles".to_string()]
            .into_iter()
            .collect(),
    );

    let waves = graph.compute_loading_waves().unwrap();

    // Should have 3 waves
    assert_eq!(waves.len(), 3);

    // Wave 1: All raw sources (can load in parallel)
    assert_eq!(waves[0].len(), 3);
    assert!(waves[0].contains(&"raw_events".to_string()));
    assert!(waves[0].contains(&"raw_users".to_string()));
    assert!(waves[0].contains(&"raw_products".to_string()));

    // Wave 2: Enriched tables
    assert_eq!(waves[1].len(), 2);
    assert!(waves[1].contains(&"enriched_events".to_string()));
    assert!(waves[1].contains(&"user_profiles".to_string()));

    // Wave 3: Aggregated tables
    assert_eq!(waves[2], vec!["user_activity"]);
}

#[test]
fn test_table_metadata_priority() {
    let mut graph = TableDependencyGraph::new();

    // Add tables with different priorities
    graph.add_table_with_metadata(
        "high_priority".to_string(),
        HashSet::new(),
        TableMetadata::new("kafka".to_string()).with_priority(100),
    );
    graph.add_table_with_metadata(
        "low_priority".to_string(),
        HashSet::new(),
        TableMetadata::new("kafka".to_string()).with_priority(10),
    );
    graph.add_table_with_metadata(
        "normal_priority".to_string(),
        HashSet::new(),
        TableMetadata::new("file".to_string()).with_priority(50),
    );

    let waves = graph.compute_loading_waves().unwrap();

    // All independent tables should be in one wave, sorted by priority (high to low)
    assert_eq!(waves.len(), 1);
    assert_eq!(waves[0][0], "high_priority");
    assert_eq!(waves[0][1], "normal_priority");
    assert_eq!(waves[0][2], "low_priority");
}

#[test]
fn test_remove_table() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("A".to_string(), HashSet::new());
    graph.add_table("B".to_string(), HashSet::new());

    assert_eq!(graph.table_count(), 2);
    assert!(graph.contains_table("A"));

    let removed = graph.remove_table("A");
    assert!(removed.is_some());
    assert_eq!(graph.table_count(), 1);
    assert!(!graph.contains_table("A"));
}

#[test]
fn test_table_names() {
    let mut graph = TableDependencyGraph::new();
    graph.add_table("table1".to_string(), HashSet::new());
    graph.add_table("table2".to_string(), HashSet::new());
    graph.add_table("table3".to_string(), HashSet::new());

    let mut names = graph.table_names();
    names.sort();

    assert_eq!(names, vec!["table1", "table2", "table3"]);
}

#[test]
fn test_large_independent_set() {
    // Test with many independent tables
    let mut graph = TableDependencyGraph::new();

    for i in 0..100 {
        graph.add_table(format!("table_{}", i), HashSet::new());
    }

    let waves = graph.compute_loading_waves().unwrap();

    // All tables should be in one wave (all independent)
    assert_eq!(waves.len(), 1);
    assert_eq!(waves[0].len(), 100);
}

#[test]
fn test_deep_dependency_chain() {
    // Create a long chain: 0 -> 1 -> 2 -> ... -> 9
    let mut graph = TableDependencyGraph::new();
    graph.add_table("table_0".to_string(), HashSet::new());

    for i in 1..10 {
        graph.add_table(
            format!("table_{}", i),
            vec![format!("table_{}", i - 1)].into_iter().collect(),
        );
    }

    let waves = graph.compute_loading_waves().unwrap();

    // Should have 10 waves (one per table)
    assert_eq!(waves.len(), 10);
    for (i, wave) in waves.iter().enumerate() {
        assert_eq!(wave.len(), 1);
        assert_eq!(wave[0], format!("table_{}", i));
    }
}

#[test]
fn test_cycle_in_complex_graph() {
    // Create a complex graph with a cycle in the middle
    let mut graph = TableDependencyGraph::new();

    // Independent base tables
    graph.add_table("A".to_string(), HashSet::new());
    graph.add_table("B".to_string(), HashSet::new());

    // Cycle between C, D, E
    graph.add_table("C".to_string(), vec!["E".to_string()].into_iter().collect());
    graph.add_table("D".to_string(), vec!["C".to_string()].into_iter().collect());
    graph.add_table("E".to_string(), vec!["D".to_string()].into_iter().collect());

    // F depends on the cycle
    graph.add_table("F".to_string(), vec!["C".to_string()].into_iter().collect());

    let result = graph.topological_load_order();
    assert!(result.is_err());

    match result.unwrap_err() {
        DependencyError::CircularDependency { tables, .. } => {
            // Should detect C, D, E, and F in the cycle
            assert!(tables.len() >= 3);
            assert!(tables.contains(&"C".to_string()));
            assert!(tables.contains(&"D".to_string()));
            assert!(tables.contains(&"E".to_string()));
        }
        _ => panic!("Expected CircularDependency error"),
    }
}
