//! Tier 2: Common SQL Operations (60-89% probability, significantly differentiating)
//!
//! Operations:
//! - Operation #6: Scalar Subquery (71% probability) - Config/reference lookups ✅
//! - Operation #7: Scalar Subquery with EXISTS (68% probability) - Existence checks ✅
//! - Operation #8: Time-Based JOIN WITHIN (68% probability) - Temporal correlation ✅
//! - Operation #9: HAVING Clause (72% probability) - Post-aggregation filtering ✅

pub mod having_clause;
pub mod scalar_subquery;
pub mod scalar_subquery_with_exists;
pub mod timebased_join;
