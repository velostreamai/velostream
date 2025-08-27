//! Enhanced Schema Caching System
//!
//! Multi-level caching with hot schema optimization, dependency prefetching,
//! and comprehensive performance metrics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use super::registry_client::{CachedSchema, ResolvedSchema, DependencyGraph};
use super::{SchemaError, SchemaResult};

/// Enhanced multi-level schema cache
pub struct EnhancedSchemaCache {
    /// L1 Cache: Hot schemas (frequently accessed)
    hot_cache: Arc<RwLock<LruCache<u32, CachedSchema>>>,
    
    /// L2 Cache: All schemas with TTL
    main_cache: Arc<RwLock<HashMap<u32, CachedEntry>>>,
    
    /// L3 Cache: Resolved schemas with dependencies
    resolved_cache: Arc<RwLock<HashMap<u32, ResolvedEntry>>>,
    
    /// Dependency index for prefetching
    dependency_index: Arc<RwLock<DependencyIndex>>,
    
    /// Cache metrics collector
    metrics: Arc<RwLock<CacheMetrics>>,
    
    /// Background refresh manager
    refresh_manager: Arc<RefreshManager>,
    
    /// Cache configuration
    config: CacheConfig,
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Size of hot cache (L1)
    pub hot_cache_size: usize,
    
    /// Maximum entries in main cache (L2)
    pub max_cache_size: usize,
    
    /// TTL for cached schemas in seconds
    pub schema_ttl_seconds: u64,
    
    /// TTL for resolved schemas in seconds
    pub resolved_ttl_seconds: u64,
    
    /// Enable dependency prefetching
    pub enable_prefetching: bool,
    
    /// Background refresh interval in seconds
    pub refresh_interval_seconds: u64,
    
    /// Maximum prefetch depth
    pub max_prefetch_depth: usize,
    
    /// Enable cache persistence
    pub enable_persistence: bool,
    
    /// Cache persistence path
    pub persistence_path: Option<String>,
}

/// LRU cache implementation
pub struct LruCache<K: std::hash::Hash + Eq + Clone, V: Clone> {
    capacity: usize,
    cache: HashMap<K, V>,
    order: VecDeque<K>,
    access_count: HashMap<K, u64>,
}

/// Cached entry with metadata
#[derive(Debug, Clone)]
struct CachedEntry {
    schema: CachedSchema,
    inserted_at: Instant,
    last_accessed: Instant,
    access_count: u64,
    ttl_seconds: u64,
    size_bytes: usize,
}

/// Resolved schema cache entry
#[derive(Debug, Clone)]
struct ResolvedEntry {
    resolved: ResolvedSchema,
    graph: DependencyGraph,
    inserted_at: Instant,
    last_accessed: Instant,
    resolution_time_ms: u64,
    dependency_count: usize,
}

/// Dependency index for efficient lookups
struct DependencyIndex {
    /// Schema ID -> dependencies
    dependencies: HashMap<u32, HashSet<u32>>,
    
    /// Schema ID -> dependents (reverse index)
    dependents: HashMap<u32, HashSet<u32>>,
    
    /// Frequently accessed together
    access_patterns: HashMap<u32, Vec<(u32, f64)>>, // (related_id, correlation_score)
}

/// Background refresh manager
struct RefreshManager {
    refresh_queue: Arc<RwLock<VecDeque<RefreshTask>>>,
    last_refresh: Arc<RwLock<HashMap<u32, Instant>>>,
    refresh_in_progress: Arc<RwLock<HashSet<u32>>>,
}

/// Refresh task for background updates
#[derive(Debug, Clone)]
struct RefreshTask {
    schema_id: u32,
    priority: RefreshPriority,
    scheduled_at: Instant,
    retry_count: u32,
}

/// Refresh priority levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RefreshPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Cache performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    /// Total cache hits
    pub total_hits: u64,
    
    /// Total cache misses
    pub total_misses: u64,
    
    /// L1 (hot) cache hits
    pub l1_hits: u64,
    
    /// L2 (main) cache hits
    pub l2_hits: u64,
    
    /// L3 (resolved) cache hits
    pub l3_hits: u64,
    
    /// Average lookup time in microseconds
    pub avg_lookup_time_us: f64,
    
    /// Total evictions
    pub total_evictions: u64,
    
    /// Current cache size in bytes
    pub current_size_bytes: usize,
    
    /// Prefetch success rate
    pub prefetch_success_rate: f64,
    
    /// Background refresh count
    pub refresh_count: u64,
    
    /// Cache hit rate
    pub hit_rate: f64,
    
    /// Memory pressure events
    pub memory_pressure_events: u64,
}

impl EnhancedSchemaCache {
    /// Create a new enhanced cache with default configuration
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }

    /// Create a new enhanced cache with custom configuration
    pub fn with_config(config: CacheConfig) -> Self {
        Self {
            hot_cache: Arc::new(RwLock::new(LruCache::new(config.hot_cache_size))),
            main_cache: Arc::new(RwLock::new(HashMap::new())),
            resolved_cache: Arc::new(RwLock::new(HashMap::new())),
            dependency_index: Arc::new(RwLock::new(DependencyIndex::new())),
            metrics: Arc::new(RwLock::new(CacheMetrics::default())),
            refresh_manager: Arc::new(RefreshManager::new()),
            config,
        }
    }

    /// Get a schema from cache (checks all levels)
    pub async fn get(&self, schema_id: u32) -> Option<CachedSchema> {
        let start_time = Instant::now();
        let mut metrics = self.metrics.write().await;

        // Check L1 (hot cache) first
        {
            let mut hot = self.hot_cache.write().await;
            if let Some(schema) = hot.get(&schema_id) {
                metrics.l1_hits += 1;
                metrics.total_hits += 1;
                self.update_access_pattern(schema_id).await;
                self.record_lookup_time(&mut metrics, start_time);
                return Some(schema.clone());
            }
        }

        // Check L2 (main cache)
        {
            let mut main = self.main_cache.write().await;
            if let Some(entry) = main.get_mut(&schema_id) {
                // Check TTL
                if entry.is_expired() {
                    main.remove(&schema_id);
                    metrics.total_misses += 1;
                    self.schedule_refresh(schema_id, RefreshPriority::High).await;
                    self.record_lookup_time(&mut metrics, start_time);
                    return None;
                }

                entry.last_accessed = Instant::now();
                entry.access_count += 1;
                
                let schema = entry.schema.clone();
                
                // Promote to hot cache if frequently accessed
                if entry.access_count > 10 {
                    self.promote_to_hot(schema_id, schema.clone()).await;
                }

                metrics.l2_hits += 1;
                metrics.total_hits += 1;
                
                // Prefetch dependencies if enabled
                if self.config.enable_prefetching {
                    self.prefetch_dependencies(schema_id).await;
                }
                
                self.update_access_pattern(schema_id).await;
                self.record_lookup_time(&mut metrics, start_time);
                return Some(schema);
            }
        }

        metrics.total_misses += 1;
        self.record_lookup_time(&mut metrics, start_time);
        None
    }

    /// Put a schema into cache
    pub async fn put(&self, schema: CachedSchema) {
        let schema_id = schema.id;
        let size_bytes = std::mem::size_of_val(&schema) + schema.schema.len();

        // Check cache size and evict if necessary
        self.ensure_cache_capacity().await;

        let entry = CachedEntry {
            schema: schema.clone(),
            inserted_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
            ttl_seconds: self.config.schema_ttl_seconds,
            size_bytes,
        };

        // Add to main cache
        {
            let mut main = self.main_cache.write().await;
            main.insert(schema_id, entry);
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.current_size_bytes += size_bytes;
        }

        // Update dependency index
        if !schema.references.is_empty() {
            self.update_dependency_index(schema_id, &schema.references).await;
        }
    }

    /// Get a resolved schema from cache
    pub async fn get_resolved(&self, schema_id: u32) -> Option<ResolvedSchema> {
        let mut resolved = self.resolved_cache.write().await;
        
        if let Some(entry) = resolved.get_mut(&schema_id) {
            if entry.is_expired(self.config.resolved_ttl_seconds) {
                resolved.remove(&schema_id);
                self.schedule_refresh(schema_id, RefreshPriority::Normal).await;
                return None;
            }

            entry.last_accessed = Instant::now();
            let mut metrics = self.metrics.write().await;
            metrics.l3_hits += 1;
            metrics.total_hits += 1;
            
            return Some(entry.resolved.clone());
        }

        None
    }

    /// Put a resolved schema into cache
    pub async fn put_resolved(&self, resolved: ResolvedSchema, graph: DependencyGraph, resolution_time_ms: u64) {
        let schema_id = resolved.root_schema.id;
        let dependency_count = resolved.dependencies.len();

        let entry = ResolvedEntry {
            resolved,
            graph,
            inserted_at: Instant::now(),
            last_accessed: Instant::now(),
            resolution_time_ms,
            dependency_count,
        };

        let mut cache = self.resolved_cache.write().await;
        cache.insert(schema_id, entry);
    }

    /// Invalidate a schema and its dependents
    pub async fn invalidate(&self, schema_id: u32) {
        // Remove from all cache levels
        {
            let mut hot = self.hot_cache.write().await;
            hot.remove(&schema_id);
        }

        {
            let mut main = self.main_cache.write().await;
            main.remove(&schema_id);
        }

        {
            let mut resolved = self.resolved_cache.write().await;
            resolved.remove(&schema_id);
        }

        // Invalidate dependents
        let dependents = self.get_dependents(schema_id).await;
        for dependent in dependents {
            self.invalidate_resolved(dependent).await;
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_evictions += 1;
        }
    }

    /// Clear all caches
    pub async fn clear(&self) {
        let mut hot = self.hot_cache.write().await;
        hot.clear();

        let mut main = self.main_cache.write().await;
        main.clear();

        let mut resolved = self.resolved_cache.write().await;
        resolved.clear();

        let mut index = self.dependency_index.write().await;
        index.clear();

        let mut metrics = self.metrics.write().await;
        *metrics = CacheMetrics::default();
    }

    /// Get current cache metrics
    pub async fn metrics(&self) -> CacheMetrics {
        let metrics = self.metrics.read().await;
        let mut result = metrics.clone();
        
        // Calculate hit rate
        let total_requests = result.total_hits + result.total_misses;
        result.hit_rate = if total_requests > 0 {
            (result.total_hits as f64) / (total_requests as f64)
        } else {
            0.0
        };

        result
    }

    /// Warm up cache with frequently accessed schemas
    pub async fn warm_up(&self, schema_ids: Vec<u32>) {
        for schema_id in schema_ids {
            self.schedule_refresh(schema_id, RefreshPriority::Low).await;
        }
    }

    /// Persist cache to disk (if enabled)
    pub async fn persist(&self) -> SchemaResult<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let path = self.config.persistence_path.as_ref()
            .ok_or_else(|| SchemaError::Cache {
                message: "Persistence path not configured".to_string(),
            })?;

        // Serialize cache state
        let cache_state = self.export_state().await;
        let json = serde_json::to_string_pretty(&cache_state)
            .map_err(|e| SchemaError::Cache {
                message: format!("Failed to serialize cache: {}", e),
            })?;

        // Write to file
        tokio::fs::write(path, json).await
            .map_err(|e| SchemaError::Cache {
                message: format!("Failed to write cache file: {}", e),
            })?;

        Ok(())
    }

    /// Load cache from disk (if enabled)
    pub async fn load(&self) -> SchemaResult<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let path = self.config.persistence_path.as_ref()
            .ok_or_else(|| SchemaError::Cache {
                message: "Persistence path not configured".to_string(),
            })?;

        // Read from file
        let json = tokio::fs::read_to_string(path).await
            .map_err(|e| SchemaError::Cache {
                message: format!("Failed to read cache file: {}", e),
            })?;

        // Deserialize cache state
        let cache_state: CacheState = serde_json::from_str(&json)
            .map_err(|e| SchemaError::Cache {
                message: format!("Failed to deserialize cache: {}", e),
            })?;

        // Import state
        self.import_state(cache_state).await;

        Ok(())
    }

    // Private helper methods

    async fn promote_to_hot(&self, schema_id: u32, schema: CachedSchema) {
        let mut hot = self.hot_cache.write().await;
        hot.put(schema_id, schema);
    }

    async fn ensure_cache_capacity(&self) {
        let main = self.main_cache.read().await;
        if main.len() >= self.config.max_cache_size {
            drop(main); // Release read lock
            self.evict_lru().await;
        }
    }

    async fn evict_lru(&self) {
        let mut main = self.main_cache.write().await;
        
        // Find least recently used entry
        if let Some((lru_id, _)) = main.iter()
            .min_by_key(|(_, entry)| entry.last_accessed) {
            
            let lru_id = *lru_id;
            if let Some(entry) = main.remove(&lru_id) {
                let mut metrics = self.metrics.write().await;
                metrics.total_evictions += 1;
                metrics.current_size_bytes -= entry.size_bytes;
            }
        }
    }

    async fn schedule_refresh(&self, schema_id: u32, priority: RefreshPriority) {
        let task = RefreshTask {
            schema_id,
            priority,
            scheduled_at: Instant::now(),
            retry_count: 0,
        };

        let mut queue = self.refresh_manager.refresh_queue.write().await;
        queue.push_back(task);
    }

    async fn prefetch_dependencies(&self, schema_id: u32) {
        let index = self.dependency_index.read().await;
        
        if let Some(deps) = index.dependencies.get(&schema_id) {
            for dep_id in deps.iter().take(self.config.max_prefetch_depth) {
                self.schedule_refresh(*dep_id, RefreshPriority::Low).await;
            }
        }

        // Also prefetch commonly accessed together
        if let Some(patterns) = index.access_patterns.get(&schema_id) {
            for (related_id, score) in patterns {
                if *score > 0.7 { // High correlation
                    self.schedule_refresh(*related_id, RefreshPriority::Low).await;
                }
            }
        }
    }

    async fn update_dependency_index(&self, schema_id: u32, references: &[super::registry_client::SchemaReference]) {
        let mut index = self.dependency_index.write().await;
        
        let mut deps = HashSet::new();
        for reference in references {
            if let Some(ref_id) = reference.schema_id {
                deps.insert(ref_id);
                
                // Update reverse index
                index.dependents
                    .entry(ref_id)
                    .or_insert_with(HashSet::new)
                    .insert(schema_id);
            }
        }
        
        index.dependencies.insert(schema_id, deps);
    }

    async fn update_access_pattern(&self, schema_id: u32) {
        // Track which schemas are accessed together
        // This is simplified - real implementation would use sliding window
        let mut index = self.dependency_index.write().await;
        
        // Update access patterns (simplified correlation tracking)
        index.access_patterns
            .entry(schema_id)
            .or_insert_with(Vec::new);
    }

    async fn get_dependents(&self, schema_id: u32) -> Vec<u32> {
        let index = self.dependency_index.read().await;
        index.dependents
            .get(&schema_id)
            .map(|deps| deps.iter().copied().collect())
            .unwrap_or_default()
    }

    async fn invalidate_resolved(&self, schema_id: u32) {
        let mut resolved = self.resolved_cache.write().await;
        resolved.remove(&schema_id);
    }

    fn record_lookup_time(&self, metrics: &mut CacheMetrics, start_time: Instant) {
        let duration = start_time.elapsed();
        let current_time_us = duration.as_micros() as f64;
        
        // Update moving average
        if metrics.avg_lookup_time_us == 0.0 {
            metrics.avg_lookup_time_us = current_time_us;
        } else {
            metrics.avg_lookup_time_us = 
                (metrics.avg_lookup_time_us * 0.95) + (current_time_us * 0.05);
        }
    }

    async fn export_state(&self) -> CacheState {
        CacheState {
            metrics: self.metrics().await,
            cache_entries: self.main_cache.read().await.len(),
            resolved_entries: self.resolved_cache.read().await.len(),
            timestamp_millis: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    async fn import_state(&self, _state: CacheState) {
        // Simplified - would restore cache entries from state
        self.clear().await;
    }
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: HashMap::new(),
            order: VecDeque::new(),
            access_count: HashMap::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.cache.contains_key(key) {
            // Move to front
            self.order.retain(|k| k != key);
            self.order.push_front(key.clone());
            
            // Update access count
            *self.access_count.entry(key.clone()).or_insert(0) += 1;
            
            self.cache.get(key)
        } else {
            None
        }
    }

    fn put(&mut self, key: K, value: V) {
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&key) {
            // Evict LRU
            if let Some(lru) = self.order.pop_back() {
                self.cache.remove(&lru);
                self.access_count.remove(&lru);
            }
        }

        self.cache.insert(key.clone(), value);
        self.order.push_front(key.clone());
        *self.access_count.entry(key).or_insert(0) += 1;
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.order.retain(|k| k != key);
        self.access_count.remove(key);
        self.cache.remove(key)
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.order.clear();
        self.access_count.clear();
    }
}

impl CachedEntry {
    fn is_expired(&self) -> bool {
        self.inserted_at.elapsed() > Duration::from_secs(self.ttl_seconds)
    }
}

impl ResolvedEntry {
    fn is_expired(&self, ttl_seconds: u64) -> bool {
        self.inserted_at.elapsed() > Duration::from_secs(ttl_seconds)
    }
}

impl DependencyIndex {
    fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
            access_patterns: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.dependencies.clear();
        self.dependents.clear();
        self.access_patterns.clear();
    }
}

impl RefreshManager {
    fn new() -> Self {
        Self {
            refresh_queue: Arc::new(RwLock::new(VecDeque::new())),
            last_refresh: Arc::new(RwLock::new(HashMap::new())),
            refresh_in_progress: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            hot_cache_size: 100,
            max_cache_size: 10000,
            schema_ttl_seconds: 300,
            resolved_ttl_seconds: 600,
            enable_prefetching: true,
            refresh_interval_seconds: 60,
            max_prefetch_depth: 3,
            enable_persistence: false,
            persistence_path: None,
        }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self {
            total_hits: 0,
            total_misses: 0,
            l1_hits: 0,
            l2_hits: 0,
            l3_hits: 0,
            avg_lookup_time_us: 0.0,
            total_evictions: 0,
            current_size_bytes: 0,
            prefetch_success_rate: 0.0,
            refresh_count: 0,
            hit_rate: 0.0,
            memory_pressure_events: 0,
        }
    }
}

/// Serializable cache state for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheState {
    metrics: CacheMetrics,
    cache_entries: usize,
    resolved_entries: usize,
    timestamp_millis: u64,
}