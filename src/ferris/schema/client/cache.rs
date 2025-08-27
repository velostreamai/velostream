//! Schema Cache
//!
//! TTL-based schema caching with version tracking and automatic invalidation.
//! Provides high-performance schema access with configurable eviction policies.

use crate::ferris::schema::{Schema, SchemaResult};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// High-performance schema cache with TTL and LRU eviction
pub struct SchemaCache {
    /// Cached schema entries
    entries: Arc<RwLock<HashMap<String, CacheEntry>>>,
    /// Cache configuration
    config: CacheConfig,
    /// Access order for LRU eviction
    access_order: Arc<RwLock<VecDeque<String>>>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStatistics>>,
}

/// Configuration for schema cache behavior
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Default TTL for cache entries
    pub default_ttl: Duration,
    /// Maximum number of cached schemas
    pub max_entries: usize,
    /// Whether to use LRU eviction when cache is full
    pub enable_lru_eviction: bool,
    /// Whether to automatically refresh entries before they expire
    pub enable_refresh_ahead: bool,
    /// Refresh ahead threshold (fraction of TTL)
    pub refresh_ahead_factor: f64,
    /// Whether to track access statistics
    pub enable_statistics: bool,
}

/// A cached schema entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    /// The cached schema
    schema: Schema,
    /// When this entry was created
    created_at: Instant,
    /// When this entry was last accessed
    last_accessed: Instant,
    /// TTL for this specific entry
    ttl: Duration,
    /// Number of times this entry has been accessed
    access_count: u64,
    /// Schema version for change detection
    version: String,
    /// Source URI this schema was loaded from
    source_uri: String,
}

/// Cache performance statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStatistics {
    /// Total number of cache lookups
    pub total_requests: u64,
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of entries evicted due to TTL expiry
    pub ttl_evictions: u64,
    /// Number of entries evicted due to LRU policy
    pub lru_evictions: u64,
    /// Total number of cache insertions
    pub insertions: u64,
    /// Total number of cache invalidations
    pub invalidations: u64,
    /// Average access time in microseconds
    pub avg_access_time_us: f64,
}

/// Cache lookup result with metadata
#[derive(Debug, Clone)]
pub enum CacheLookupResult {
    /// Schema found in cache
    Hit {
        schema: Schema,
        age: Duration,
        access_count: u64,
    },
    /// Schema not found or expired
    Miss { reason: MissReason },
    /// Schema found but needs refresh
    Stale {
        schema: Schema,
        age: Duration,
        needs_refresh: bool,
    },
}

/// Reasons for cache miss
#[derive(Debug, Clone, PartialEq)]
pub enum MissReason {
    /// Entry not found in cache
    NotFound,
    /// Entry expired based on TTL
    Expired,
    /// Entry was explicitly invalidated
    Invalidated,
}

/// Cache eviction policy
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionPolicy {
    /// Least Recently Used eviction
    LRU,
    /// Least Frequently Used eviction
    LFU,
    /// First In First Out eviction
    FIFO,
    /// Random eviction
    Random,
}

impl SchemaCache {
    /// Create a new schema cache with default configuration
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }

    /// Create a new schema cache with custom configuration
    pub fn with_config(config: CacheConfig) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            config,
            access_order: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(CacheStatistics::default())),
        }
    }

    /// Get schema from cache
    pub async fn get(&self, source_uri: &str) -> CacheLookupResult {
        let start_time = Instant::now();

        // Update statistics
        if self.config.enable_statistics {
            let mut stats = self.stats.write().await;
            stats.total_requests += 1;
        }

        let mut entries = self.entries.write().await;
        let result = if let Some(entry) = entries.get_mut(source_uri) {
            let age = start_time.duration_since(entry.created_at);

            // Check if entry has expired
            if age > entry.ttl {
                // Remove expired entry
                entries.remove(source_uri);
                self.update_access_order_remove(source_uri).await;

                if self.config.enable_statistics {
                    let mut stats = self.stats.write().await;
                    stats.misses += 1;
                    stats.ttl_evictions += 1;
                }

                CacheLookupResult::Miss {
                    reason: MissReason::Expired,
                }
            } else {
                // Update access metadata
                entry.last_accessed = start_time;
                entry.access_count += 1;

                // Update LRU order
                self.update_access_order_touch(source_uri).await;

                if self.config.enable_statistics {
                    let mut stats = self.stats.write().await;
                    stats.hits += 1;
                }

                // Check if refresh is needed
                let refresh_threshold = Duration::from_secs_f64(
                    entry.ttl.as_secs_f64() * self.config.refresh_ahead_factor,
                );
                let needs_refresh = self.config.enable_refresh_ahead && age > refresh_threshold;

                if needs_refresh {
                    CacheLookupResult::Stale {
                        schema: entry.schema.clone(),
                        age,
                        needs_refresh: true,
                    }
                } else {
                    CacheLookupResult::Hit {
                        schema: entry.schema.clone(),
                        age,
                        access_count: entry.access_count,
                    }
                }
            }
        } else {
            if self.config.enable_statistics {
                let mut stats = self.stats.write().await;
                stats.misses += 1;
            }

            CacheLookupResult::Miss {
                reason: MissReason::NotFound,
            }
        };

        // Update access time statistics
        if self.config.enable_statistics {
            let access_time = start_time.elapsed().as_micros() as f64;
            let mut stats = self.stats.write().await;
            stats.avg_access_time_us =
                (stats.avg_access_time_us * (stats.total_requests - 1) as f64 + access_time)
                    / stats.total_requests as f64;
        }

        result
    }

    /// Put schema into cache
    pub async fn put(
        &self,
        source_uri: &str,
        schema: Schema,
        ttl: Option<Duration>,
    ) -> SchemaResult<()> {
        let ttl = ttl.unwrap_or(self.config.default_ttl);
        let version = schema
            .version
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let entry = CacheEntry {
            schema,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            ttl,
            access_count: 0,
            version,
            source_uri: source_uri.to_string(),
        };

        let mut entries = self.entries.write().await;

        // Check if cache is full and eviction is needed
        if entries.len() >= self.config.max_entries && !entries.contains_key(source_uri) {
            self.evict_entries(&mut entries).await?;
        }

        // Insert new entry
        entries.insert(source_uri.to_string(), entry);
        self.update_access_order_add(source_uri).await;

        // Update statistics
        if self.config.enable_statistics {
            let mut stats = self.stats.write().await;
            stats.insertions += 1;
        }

        Ok(())
    }

    /// Remove schema from cache
    pub async fn invalidate(&self, source_uri: &str) -> bool {
        let mut entries = self.entries.write().await;
        let was_present = entries.remove(source_uri).is_some();

        if was_present {
            self.update_access_order_remove(source_uri).await;

            if self.config.enable_statistics {
                let mut stats = self.stats.write().await;
                stats.invalidations += 1;
            }
        }

        was_present
    }

    /// Clear all entries from cache
    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        let count = entries.len();
        entries.clear();

        let mut access_order = self.access_order.write().await;
        access_order.clear();

        if self.config.enable_statistics {
            let mut stats = self.stats.write().await;
            stats.invalidations += count as u64;
        }
    }

    /// Get cache statistics
    pub async fn statistics(&self) -> CacheStatistics {
        if self.config.enable_statistics {
            self.stats.read().await.clone()
        } else {
            CacheStatistics::default()
        }
    }

    /// Get cache size and capacity info
    pub async fn size_info(&self) -> CacheSizeInfo {
        let entries = self.entries.read().await;
        let expired_count = self.count_expired_entries(&entries).await;

        CacheSizeInfo {
            current_entries: entries.len(),
            max_entries: self.config.max_entries,
            expired_entries: expired_count,
            active_entries: entries.len() - expired_count,
            utilization: entries.len() as f64 / self.config.max_entries as f64,
        }
    }

    /// Perform cache maintenance (remove expired entries)
    pub async fn maintenance(&self) -> SchemaResult<MaintenanceResult> {
        let mut entries = self.entries.write().await;
        let initial_count = entries.len();

        let now = Instant::now();
        let mut expired_keys = Vec::new();

        // Find expired entries
        for (key, entry) in entries.iter() {
            if now.duration_since(entry.created_at) > entry.ttl {
                expired_keys.push(key.clone());
            }
        }

        // Remove expired entries
        for key in &expired_keys {
            entries.remove(key);
            self.update_access_order_remove(key).await;
        }

        // Update statistics
        if self.config.enable_statistics {
            let mut stats = self.stats.write().await;
            stats.ttl_evictions += expired_keys.len() as u64;
        }

        Ok(MaintenanceResult {
            entries_before: initial_count,
            entries_after: entries.len(),
            expired_removed: expired_keys.len(),
        })
    }

    /// Check if schema version has changed
    pub async fn is_version_current(&self, source_uri: &str, version: &str) -> bool {
        let entries = self.entries.read().await;
        entries
            .get(source_uri)
            .map(|entry| entry.version == version)
            .unwrap_or(false)
    }

    // Private helper methods

    async fn evict_entries(&self, entries: &mut HashMap<String, CacheEntry>) -> SchemaResult<()> {
        if !self.config.enable_lru_eviction {
            return Ok(());
        }

        // Simple LRU eviction - remove oldest accessed entry
        if let Some(lru_key) = self.find_lru_key(entries).await {
            entries.remove(&lru_key);
            self.update_access_order_remove(&lru_key).await;

            if self.config.enable_statistics {
                let mut stats = self.stats.write().await;
                stats.lru_evictions += 1;
            }
        }

        Ok(())
    }

    async fn find_lru_key(&self, entries: &HashMap<String, CacheEntry>) -> Option<String> {
        entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(key, _)| key.clone())
    }

    async fn update_access_order_add(&self, key: &str) {
        if self.config.enable_lru_eviction {
            let mut order = self.access_order.write().await;
            order.push_back(key.to_string());
        }
    }

    async fn update_access_order_remove(&self, key: &str) {
        if self.config.enable_lru_eviction {
            let mut order = self.access_order.write().await;
            order.retain(|k| k != key);
        }
    }

    async fn update_access_order_touch(&self, key: &str) {
        if self.config.enable_lru_eviction {
            let mut order = self.access_order.write().await;
            // Remove from current position and add to end
            order.retain(|k| k != key);
            order.push_back(key.to_string());
        }
    }

    async fn count_expired_entries(&self, entries: &HashMap<String, CacheEntry>) -> usize {
        let now = Instant::now();
        entries
            .values()
            .filter(|entry| now.duration_since(entry.created_at) > entry.ttl)
            .count()
    }
}

/// Information about cache size and utilization
#[derive(Debug, Clone)]
pub struct CacheSizeInfo {
    pub current_entries: usize,
    pub max_entries: usize,
    pub expired_entries: usize,
    pub active_entries: usize,
    pub utilization: f64,
}

/// Result of cache maintenance operation
#[derive(Debug, Clone)]
pub struct MaintenanceResult {
    pub entries_before: usize,
    pub entries_after: usize,
    pub expired_removed: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(300), // 5 minutes
            max_entries: 1000,
            enable_lru_eviction: true,
            enable_refresh_ahead: true,
            refresh_ahead_factor: 0.8, // Refresh when 80% of TTL has elapsed
            enable_statistics: true,
        }
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheStatistics {
    /// Calculate cache hit rate
    pub fn hit_rate(&self) -> f64 {
        if self.total_requests > 0 {
            self.hits as f64 / self.total_requests as f64
        } else {
            0.0
        }
    }

    /// Calculate cache miss rate
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    /// Get total evictions
    pub fn total_evictions(&self) -> u64 {
        self.ttl_evictions + self.lru_evictions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::ast::DataType;
    use crate::ferris::schema::{FieldDefinition, SchemaMetadata};

    fn create_test_schema(version: &str) -> Schema {
        Schema {
            fields: vec![FieldDefinition::required(
                "id".to_string(),
                DataType::Integer,
            )],
            version: Some(version.to_string()),
            metadata: SchemaMetadata::new("test".to_string()),
        }
    }

    #[tokio::test]
    async fn test_cache_put_and_get() {
        let cache = SchemaCache::new();
        let schema = create_test_schema("1.0.0");

        cache
            .put("test://schema", schema.clone(), None)
            .await
            .unwrap();

        match cache.get("test://schema").await {
            CacheLookupResult::Hit {
                schema: cached_schema,
                ..
            } => {
                assert_eq!(cached_schema.version, Some("1.0.0".to_string()));
            }
            _ => panic!("Expected cache hit"),
        }
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = SchemaCache::new();

        match cache.get("nonexistent://schema").await {
            CacheLookupResult::Miss { reason } => {
                assert_eq!(reason, MissReason::NotFound);
            }
            _ => panic!("Expected cache miss"),
        }
    }

    #[tokio::test]
    async fn test_cache_ttl_expiry() {
        let config = CacheConfig {
            default_ttl: Duration::from_millis(10), // Very short TTL
            ..Default::default()
        };

        let cache = SchemaCache::with_config(config);
        let schema = create_test_schema("1.0.0");

        cache.put("test://schema", schema, None).await.unwrap();

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(20)).await;

        match cache.get("test://schema").await {
            CacheLookupResult::Miss { reason } => {
                assert_eq!(reason, MissReason::Expired);
            }
            _ => panic!("Expected cache miss due to expiry"),
        }
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let cache = SchemaCache::new();
        let schema = create_test_schema("1.0.0");

        cache.put("test://schema", schema, None).await.unwrap();
        assert!(cache.invalidate("test://schema").await);

        match cache.get("test://schema").await {
            CacheLookupResult::Miss { reason } => {
                assert_eq!(reason, MissReason::NotFound);
            }
            _ => panic!("Expected cache miss after invalidation"),
        }
    }

    #[tokio::test]
    async fn test_cache_statistics() {
        let cache = SchemaCache::new();
        let schema = create_test_schema("1.0.0");

        cache.put("test://schema", schema, None).await.unwrap();
        cache.get("test://schema").await; // Hit
        cache.get("nonexistent://schema").await; // Miss

        let stats = cache.statistics().await;
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[tokio::test]
    async fn test_cache_size_management() {
        let config = CacheConfig {
            max_entries: 2,
            enable_lru_eviction: true,
            ..Default::default()
        };

        let cache = SchemaCache::with_config(config);

        // Fill cache to capacity
        cache
            .put("test://schema1", create_test_schema("1.0.0"), None)
            .await
            .unwrap();
        cache
            .put("test://schema2", create_test_schema("2.0.0"), None)
            .await
            .unwrap();

        let size_info = cache.size_info().await;
        assert_eq!(size_info.current_entries, 2);
        assert_eq!(size_info.utilization, 1.0);

        // Adding third entry should trigger eviction
        cache
            .put("test://schema3", create_test_schema("3.0.0"), None)
            .await
            .unwrap();

        let size_info_after = cache.size_info().await;
        assert_eq!(size_info_after.current_entries, 2); // Still at capacity
    }
}
