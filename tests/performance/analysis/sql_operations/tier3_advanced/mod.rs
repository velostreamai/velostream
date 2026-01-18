//! Tier 3: Advanced SQL Operations (30-59% probability, competitive advantage)
//!
//! Operations:
//! - Operation #10: EXISTS/NOT EXISTS (48% probability) - Correlated subqueries ✅
//! - Operation #11: Stream-Table JOIN (42% probability) - Hash-based joins ✅
//! - Operation #12: IN/NOT IN Subquery (55% probability) - Set membership ✅
//! - Operation #13: Correlated Subquery (35% probability) - Row-by-row evaluation ✅
//! - Operation #14: Interval Stream-Stream JOIN - Time-bounded stream correlation ✅

pub mod correlated_subquery;
pub mod exists_subquery;
pub mod in_subquery;
pub mod interval_stream_join;
pub mod stream_table_join;
