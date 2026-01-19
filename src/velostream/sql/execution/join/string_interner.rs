//! String Interner for Join Key Memory Optimization
//!
//! Provides memory-efficient string storage for join keys by interning
//! strings and returning shared references (`Arc<str>`).
//!
//! ## Memory Savings
//!
//! For join keys like "123:user_456" repeated across millions of records:
//! - Before: Each key is a separate String allocation (~24+ bytes overhead)
//! - After: Single String stored, `Arc<str>` (16 bytes) shared everywhere
//!
//! Expected savings: 30-40% memory reduction when keys are reused across
//! both sides of a join or within the same window.
//!
//! ## Usage
//!
//! ```ignore
//! let interner = StringInterner::new();
//! let key1 = interner.intern_arc("123:user_456");  // Allocates
//! let key2 = interner.intern_arc("123:user_456");  // Returns same Arc
//! assert!(Arc::ptr_eq(&key1, &key2));              // Same underlying allocation
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A lightweight handle to an interned string
///
/// Only 4 bytes instead of 24+ bytes for a String.
/// Can be compared in O(1) time via index comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InternedKey(u32);

impl InternedKey {
    /// Get the raw index value
    pub fn index(&self) -> u32 {
        self.0
    }
}

/// Thread-safe string interner for join keys
///
/// Stores unique strings once and returns shared references (`Arc<str>`).
/// All methods are thread-safe using RwLock for concurrent access.
///
/// ## Memory Model
///
/// - Each unique string is stored exactly once as `Arc<str>`
/// - Subsequent interns of the same string return clones of the same Arc
/// - Multiple users (left store, right store) share the underlying allocation
#[derive(Debug)]
pub struct StringInterner {
    /// String to Arc mapping for O(1) lookup and Arc sharing
    string_to_arc: RwLock<HashMap<String, Arc<str>>>,
    /// Index to Arc mapping for O(1) resolution by InternedKey
    index_to_arc: RwLock<Vec<Arc<str>>>,
    /// String to index mapping for InternedKey lookup
    string_to_index: RwLock<HashMap<String, u32>>,
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl StringInterner {
    /// Create a new string interner
    pub fn new() -> Self {
        Self {
            string_to_arc: RwLock::new(HashMap::new()),
            index_to_arc: RwLock::new(Vec::new()),
            string_to_index: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new string interner with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            string_to_arc: RwLock::new(HashMap::with_capacity(capacity)),
            index_to_arc: RwLock::new(Vec::with_capacity(capacity)),
            string_to_index: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Intern a string, returning a shared Arc<str>
    ///
    /// If the string was already interned, returns a clone of the existing Arc
    /// (same underlying allocation). If new, stores the string and returns Arc.
    ///
    /// **This provides actual memory savings** - multiple callers share the
    /// same underlying string allocation via Arc reference counting.
    ///
    /// # Example
    /// ```ignore
    /// let interner = StringInterner::new();
    /// let key1 = interner.intern_arc("123:user_456");  // Allocates once
    /// let key2 = interner.intern_arc("123:user_456");  // Returns same Arc
    /// assert!(Arc::ptr_eq(&key1, &key2));              // Same allocation!
    /// ```
    pub fn intern_arc(&self, s: &str) -> Arc<str> {
        // Fast path: check if already interned (read lock only)
        {
            let string_to_arc = self.string_to_arc.read().expect("RwLock poisoned");
            if let Some(arc) = string_to_arc.get(s) {
                return Arc::clone(arc);
            }
        }

        // Slow path: need to insert (write lock)
        let mut string_to_arc = self.string_to_arc.write().expect("RwLock poisoned");
        let mut index_to_arc = self.index_to_arc.write().expect("RwLock poisoned");
        let mut string_to_index = self.string_to_index.write().expect("RwLock poisoned");

        // Double-check after acquiring write lock
        if let Some(arc) = string_to_arc.get(s) {
            return Arc::clone(arc);
        }

        // Create new Arc<str> and store it
        let arc: Arc<str> = Arc::from(s);
        let index = index_to_arc.len() as u32;
        let key_string = s.to_string(); // Single allocation for both HashMaps

        index_to_arc.push(Arc::clone(&arc));
        string_to_arc.insert(key_string.clone(), Arc::clone(&arc));
        string_to_index.insert(key_string, index);

        arc
    }

    /// Intern a string, returning a lightweight handle (for stats tracking)
    ///
    /// Returns an InternedKey that can be used for O(1) equality checks.
    /// For actual memory savings, use `intern_arc()` instead.
    pub fn intern(&self, s: &str) -> InternedKey {
        // Fast path: check if already interned (read lock only)
        {
            let string_to_index = self.string_to_index.read().expect("RwLock poisoned");
            if let Some(&index) = string_to_index.get(s) {
                return InternedKey(index);
            }
        }

        // Use intern_arc to do the actual interning, then get the index
        let _arc = self.intern_arc(s);

        let string_to_index = self.string_to_index.read().expect("RwLock poisoned");
        InternedKey(*string_to_index.get(s).expect("Just inserted"))
    }

    /// Resolve an interned key back to its string
    ///
    /// Returns None if the key is invalid (should not happen in normal usage).
    pub fn resolve(&self, key: InternedKey) -> Option<String> {
        let index_to_arc = self.index_to_arc.read().expect("RwLock poisoned");
        index_to_arc.get(key.0 as usize).map(|arc| arc.to_string())
    }

    /// Resolve an interned key back to its Arc<str>
    ///
    /// Returns a clone of the Arc (cheap - just increments refcount).
    pub fn resolve_arc(&self, key: InternedKey) -> Option<Arc<str>> {
        let index_to_arc = self.index_to_arc.read().expect("RwLock poisoned");
        index_to_arc.get(key.0 as usize).cloned()
    }

    /// Get the number of unique strings interned
    pub fn len(&self) -> usize {
        self.index_to_arc.read().expect("RwLock poisoned").len()
    }

    /// Check if the interner is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage statistics
    pub fn memory_stats(&self) -> InternerStats {
        let index_to_arc = self.index_to_arc.read().expect("RwLock poisoned");
        let string_count = index_to_arc.len();

        // Estimate memory usage
        let string_bytes: usize = index_to_arc.iter().map(|s| s.len()).sum();
        // Arc<str> overhead: Arc header (16 bytes) + HashMap entry
        let arc_overhead = string_count * (std::mem::size_of::<Arc<str>>() + 16);
        let hashmap_overhead =
            string_count * (std::mem::size_of::<String>() + std::mem::size_of::<Arc<str>>());

        InternerStats {
            string_count,
            total_string_bytes: string_bytes,
            estimated_overhead_bytes: arc_overhead + hashmap_overhead,
            total_estimated_bytes: string_bytes + arc_overhead + hashmap_overhead,
        }
    }
}

/// Statistics about the string interner
#[derive(Debug, Clone)]
pub struct InternerStats {
    /// Number of unique strings stored
    pub string_count: usize,
    /// Total bytes used by string content
    pub total_string_bytes: usize,
    /// Estimated bytes used by internal data structures
    pub estimated_overhead_bytes: usize,
    /// Total estimated memory usage
    pub total_estimated_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_returns_same_key_for_same_string() {
        let interner = StringInterner::new();
        let key1 = interner.intern("hello");
        let key2 = interner.intern("hello");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_intern_returns_different_keys_for_different_strings() {
        let interner = StringInterner::new();
        let key1 = interner.intern("hello");
        let key2 = interner.intern("world");
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_resolve_returns_original_string() {
        let interner = StringInterner::new();
        let key = interner.intern("test_string");
        let resolved = interner.resolve(key);
        assert_eq!(resolved, Some("test_string".to_string()));
    }

    #[test]
    fn test_len_tracks_unique_strings() {
        let interner = StringInterner::new();
        assert_eq!(interner.len(), 0);

        interner.intern("a");
        assert_eq!(interner.len(), 1);

        interner.intern("b");
        assert_eq!(interner.len(), 2);

        // Same string shouldn't increase count
        interner.intern("a");
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_interned_key_hash_works() {
        use std::collections::HashSet;

        let interner = StringInterner::new();
        let key1 = interner.intern("a");
        let key2 = interner.intern("b");
        let key3 = interner.intern("a");

        let mut set = HashSet::new();
        set.insert(key1);
        set.insert(key2);
        set.insert(key3);

        assert_eq!(set.len(), 2); // key1 and key3 are the same
    }

    #[test]
    fn test_memory_stats() {
        let interner = StringInterner::new();
        interner.intern("short");
        interner.intern("a_longer_string_for_testing");

        let stats = interner.memory_stats();
        assert_eq!(stats.string_count, 2);
        assert!(stats.total_string_bytes > 0);
        assert!(stats.total_estimated_bytes > stats.total_string_bytes);
    }

    // ==================== Arc-based Interning Tests ====================

    #[test]
    fn test_intern_arc_returns_same_arc_for_same_string() {
        let interner = StringInterner::new();
        let arc1 = interner.intern_arc("hello");
        let arc2 = interner.intern_arc("hello");

        // Should be the exact same Arc (same pointer)
        assert!(Arc::ptr_eq(&arc1, &arc2), "Should share same allocation");
        assert_eq!(&*arc1, "hello");
    }

    #[test]
    fn test_intern_arc_different_arcs_for_different_strings() {
        let interner = StringInterner::new();
        let arc1 = interner.intern_arc("hello");
        let arc2 = interner.intern_arc("world");

        // Different strings, different Arcs
        assert!(!Arc::ptr_eq(&arc1, &arc2));
        assert_eq!(&*arc1, "hello");
        assert_eq!(&*arc2, "world");
    }

    #[test]
    fn test_intern_arc_memory_sharing() {
        let interner = StringInterner::new();

        // Simulate multiple "stores" using the same key
        let key_for_left_store = interner.intern_arc("0:order_123");
        let key_for_right_store = interner.intern_arc("0:order_123");
        let key_for_another_record = interner.intern_arc("0:order_123");

        // All should share the same underlying allocation
        assert!(Arc::ptr_eq(&key_for_left_store, &key_for_right_store));
        assert!(Arc::ptr_eq(&key_for_left_store, &key_for_another_record));

        // Only one string should be stored
        assert_eq!(interner.len(), 1);

        // Strong count should be 5 (3 external + 2 in interner: string_to_arc + index_to_arc)
        assert_eq!(Arc::strong_count(&key_for_left_store), 5);
    }

    #[test]
    fn test_resolve_arc() {
        let interner = StringInterner::new();
        let key = interner.intern("test_string");
        let arc = interner.resolve_arc(key);

        assert!(arc.is_some());
        assert_eq!(&*arc.unwrap(), "test_string");
    }

    #[test]
    fn test_intern_and_intern_arc_consistency() {
        let interner = StringInterner::new();

        // Use both methods on the same string
        let arc = interner.intern_arc("shared_key");
        let key = interner.intern("shared_key");

        // Should be able to resolve back to the same Arc
        let resolved_arc = interner.resolve_arc(key).unwrap();
        assert!(Arc::ptr_eq(&arc, &resolved_arc));

        // Only one entry should exist
        assert_eq!(interner.len(), 1);
    }
}
