//! Tier 3: Advanced SQL Operations (30-59% probability, competitive advantage)
//!
//! Operations:
//! - Operation #10: EXISTS/NOT EXISTS (48% probability) - Correlated subqueries ✅
//! - Operation #11: Stream-Stream JOIN (42% probability) - Memory-intensive joins ✅
//! - Operation #12: IN/NOT IN Subquery (55% probability) - Set membership ✅
//! - Operation #13: Correlated Subquery (35% probability) - Row-by-row evaluation ✅

pub mod correlated_subquery;
pub mod exists_subquery;
pub mod in_subquery;
pub mod stream_stream_join;
