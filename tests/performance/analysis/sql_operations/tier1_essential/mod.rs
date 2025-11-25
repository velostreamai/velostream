//! Tier 1: Essential SQL Operations (90-100% probability, must-have for competitive product)
//!
//! Operations measured:
//! - Operation #1: SELECT + WHERE (99% probability) - Pure filtering
//! - Operation #2: ROWS WINDOW (78% probability) - Sliding window analytics
//! - Operation #3: Stream-Table JOIN (94% probability) - Reference data enrichment
//! - Operation #4: GROUP BY Continuous (93% probability) - Streaming aggregation
//! - Operation #5: Windowed Aggregation - TUMBLING EMIT FINAL (92% probability)
//! - Operation #5b: Windowed Aggregation - TUMBLING EMIT CHANGES (92% probability, late data)

pub mod emit_changes;
pub mod group_by_continuous;
pub mod rows_window;
pub mod select_where;
pub mod stream_table_join;
pub mod tumbling_window;
