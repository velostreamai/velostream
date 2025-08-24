/*!
# Hash Join Performance Tests

Benchmarks to validate the performance improvements of hash join over nested loop join.
Tests various dataset sizes and join selectivities.
*/

use ferrisstreams::ferris::sql::execution::algorithms::{HashJoinBuilder, JoinStrategy, JoinStatistics};
use ferrisstreams::ferris::sql::execution::processors::{JoinProcessor, ProcessorContext};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamRecord};
use ferrisstreams::ferris::sql::ast::{JoinClause, JoinType, Expr, BinaryOperator, StreamSource};
use std::collections::HashMap;
use std::time::Instant;

#[cfg(test)]
mod hash_join_performance_tests {
    use super::*;

    /// Create test records for performance testing
    fn create_test_records(count: usize, prefix: &str) -> Vec<StreamRecord> {
        (0..count).map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("value".to_string(), FieldValue::String(format!("{}_value_{}", prefix, i)));
            
            StreamRecord {
                fields,
                timestamp: i as i64,
                offset: i as i64,
                partition: 0,
                headers: HashMap::new(),
            }
        }).collect()
    }

    /// Create simple equi-join condition
    fn create_join_condition() -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column("id".to_string())),
        }
    }

    /// Create join clause for testing
    fn create_join_clause() -> JoinClause {
        JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("right_table".to_string()),
            condition: create_join_condition(),
            right_alias: Some("r".to_string()),
            window: None,
        }
    }

    #[test]
    fn test_hash_join_small_datasets() {
        // Small datasets should prefer nested loop
        let left_records = create_test_records(10, "left");
        let right_records = create_test_records(10, "right");
        
        let join_clause = create_join_clause();
        let mut context = ProcessorContext::new("test");
        
        let start = Instant::now();
        let mut hash_executor = HashJoinBuilder::new()
            .with_strategy(JoinStrategy::HashJoin)
            .build(join_clause.clone())
            .unwrap();
        
        let hash_results = hash_executor.execute(
            left_records.clone(),
            right_records.clone(),
            &mut context
        ).unwrap();
        let hash_duration = start.elapsed();
        
        let start = Instant::now();
        let nested_results = JoinProcessor::execute_nested_loop_batch(
            left_records,
            right_records,
            &join_clause,
            &mut context
        ).unwrap();
        let nested_duration = start.elapsed();
        
        // Results should be the same
        assert_eq!(hash_results.len(), nested_results.len());
        
        println!("Small dataset (10x10):");
        println!("  Hash join: {:?}", hash_duration);
        println!("  Nested loop: {:?}", nested_duration);
        println!("  Results: {} records", hash_results.len());
    }

    #[test]
    fn test_hash_join_medium_datasets() {
        // Medium datasets should show hash join benefit
        let left_records = create_test_records(100, "left");
        let right_records = create_test_records(50, "right");
        
        let join_clause = create_join_clause();
        let mut context = ProcessorContext::new("test");
        
        let start = Instant::now();
        let mut hash_executor = HashJoinBuilder::new()
            .with_strategy(JoinStrategy::HashJoin)
            .build(join_clause.clone())
            .unwrap();
        
        let hash_results = hash_executor.execute(
            left_records.clone(),
            right_records.clone(),
            &mut context
        ).unwrap();
        let hash_duration = start.elapsed();
        
        let start = Instant::now();
        let nested_results = JoinProcessor::execute_nested_loop_batch(
            left_records,
            right_records,
            &join_clause,
            &mut context
        ).unwrap();
        let nested_duration = start.elapsed();
        
        // Results should be the same
        assert_eq!(hash_results.len(), nested_results.len());
        
        println!("Medium dataset (100x50):");
        println!("  Hash join: {:?}", hash_duration);
        println!("  Nested loop: {:?}", nested_duration);
        println!("  Results: {} records", hash_results.len());
        println!("  Speedup: {:.2}x", nested_duration.as_nanos() as f64 / hash_duration.as_nanos() as f64);
    }

    #[test]
    fn test_hash_join_large_datasets() {
        // Large datasets should show significant hash join benefit
        let left_records = create_test_records(1000, "left");
        let right_records = create_test_records(100, "right");
        
        let join_clause = create_join_clause();
        let mut context = ProcessorContext::new("test");
        
        let start = Instant::now();
        let mut hash_executor = HashJoinBuilder::new()
            .with_strategy(JoinStrategy::HashJoin)
            .build(join_clause.clone())
            .unwrap();
        
        let hash_results = hash_executor.execute(
            left_records.clone(),
            right_records.clone(),
            &mut context
        ).unwrap();
        let hash_duration = start.elapsed();
        
        let start = Instant::now();
        let nested_results = JoinProcessor::execute_nested_loop_batch(
            left_records,
            right_records,
            &join_clause,
            &mut context
        ).unwrap();
        let nested_duration = start.elapsed();
        
        // Results should be the same
        assert_eq!(hash_results.len(), nested_results.len());
        
        let speedup = nested_duration.as_nanos() as f64 / hash_duration.as_nanos() as f64;
        
        println!("Large dataset (1000x100):");
        println!("  Hash join: {:?}", hash_duration);
        println!("  Nested loop: {:?}", nested_duration);
        println!("  Results: {} records", hash_results.len());
        println!("  Speedup: {:.2}x", speedup);
        
        // Hash join should be faster for large datasets
        assert!(speedup > 1.0, "Hash join should be faster than nested loop for large datasets");
    }

    #[test]
    fn test_join_strategy_selection() {
        // Test automatic strategy selection
        let small_stats = JoinStatistics {
            left_cardinality: 10,
            right_cardinality: 5,
            available_memory: 1024 * 1024,
            selectivity: 0.5,
        };
        
        let large_stats = JoinStatistics {
            left_cardinality: 10000,
            right_cardinality: 100,
            available_memory: 10 * 1024 * 1024,
            selectivity: 0.01,
        };
        
        // Small datasets should use nested loop
        assert_eq!(small_stats.select_strategy(), JoinStrategy::NestedLoop);
        
        // Large datasets should use hash join
        assert_eq!(large_stats.select_strategy(), JoinStrategy::HashJoin);
        
        println!("Strategy selection working correctly:");
        println!("  Small dataset: {:?}", small_stats.select_strategy());
        println!("  Large dataset: {:?}", large_stats.select_strategy());
    }

    #[test]
    fn test_memory_estimation() {
        let stats = JoinStatistics {
            left_cardinality: 1000,
            right_cardinality: 100,
            available_memory: 1024 * 1024,
            selectivity: 0.1,
        };
        
        let estimated_memory = stats.estimate_memory_usage();
        
        // Should estimate memory for smaller relation (100 records * 100 bytes)
        assert_eq!(estimated_memory, 100 * 100);
        assert!(estimated_memory < stats.available_memory);
        
        println!("Memory estimation:");
        println!("  Estimated usage: {} bytes", estimated_memory);
        println!("  Available memory: {} bytes", stats.available_memory);
        println!("  Memory sufficient: {}", estimated_memory < stats.available_memory);
    }
}