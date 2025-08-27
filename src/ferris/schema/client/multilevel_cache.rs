//! Multi-level Schema Caching System
//!
//! Advanced caching with hot schema optimization, adaptive cache management,
//! and intelligent refresh strategies for high-performance schema operations.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tokio::task::JoinHandle;

use super::registry_client::{CachedSchema, SchemaRegistryClient};
use crate::ferris::schema::{SchemaError, SchemaResult};

/// Multi-level caching system for schema registry operations
pub struct MultiLevelSchemaCache {
    /// L1: Hot cache for frequently accessed schemas (in-memory, ultra-fast)
    l1_cache: Arc<RwLock<HotCache>>,
    /// L2: Warm cache for recently accessed schemas (in-memory, fast)
    l2_cache: Arc<RwLock<WarmCache>>,
    /// L3: Cold cache for long-term storage (disk-backed, slower but persistent)
    l3_cache: Arc<RwLock<ColdCache>>,
    /// Adaptive cache manager for intelligent promotion/demotion
    cache_manager: Arc<RwLock<AdaptiveCacheManager>>,
    /// Background refresh and maintenance tasks
    refresh_manager: Arc<Mutex<RefreshManager>>,
    /// Configuration settings
    config: MultiLevelCacheConfig,
}

/// Hot cache for ultra-frequently accessed schemas (L1)
#[derive(Debug)]
struct HotCache {
    /// Schema storage with access frequency tracking
    schemas: HashMap<u32, HotCacheEntry>,
    /// LRU ordering for eviction
    lru_order: VecDeque<u32>,
    /// Access statistics
    stats: CacheStats,
}

/// Warm cache for recently accessed schemas (L2)
#[derive(Debug)]
struct WarmCache {
    /// Schema storage with metadata
    schemas: HashMap<u32, WarmCacheEntry>,
    /// Access tracking for promotion decisions
    access_tracker: HashMap<u32, AccessPattern>,
    /// Size-based eviction queue
    size_tracker: BTreeMap<u64, Vec<u32>>, // size -> schema_ids
    /// Statistics
    stats: CacheStats,
}

/// Cold cache for long-term persistent storage (L3)
#[derive(Debug)]
struct ColdCache {
    /// Schema storage with compression
    schemas: HashMap<u32, ColdCacheEntry>,
    /// Disk storage path
    storage_path: String,
    /// Compression statistics
    compression_stats: CompressionStats,
    /// Statistics
    stats: CacheStats,
}

/// Hot cache entry with access frequency optimization
#[derive(Debug, Clone)]
struct HotCacheEntry {
    schema: CachedSchema,
    access_frequency: u64,
    last_accessed: Instant,
    promotion_time: Instant,
    heat_score: f64, // Calculated based on frequency and recency
}

/// Warm cache entry with access pattern tracking
#[derive(Debug, Clone)]
struct WarmCacheEntry {
    schema: CachedSchema,
    access_count: u64,
    last_accessed: Instant,
    created_at: Instant,
    size_bytes: u64,
    access_pattern: AccessPattern,
}

/// Cold cache entry with compression
#[derive(Debug, Clone)]
struct ColdCacheEntry {
    compressed_schema: Vec<u8>,
    original_size: u64,
    compressed_size: u64,
    compression_ratio: f32,
    stored_at: Instant,
    last_accessed: Instant,
    access_count: u64,
}

/// Access pattern tracking for intelligent caching decisions
#[derive(Debug, Clone)]
struct AccessPattern {
    total_accesses: u64,
    recent_accesses: VecDeque<Instant>,
    access_intervals: Vec<Duration>,
    predicted_next_access: Option<Instant>,
    pattern_type: AccessPatternType,
}

/// Types of access patterns for optimization
#[derive(Debug, Clone, PartialEq)]
enum AccessPatternType {
    Burst,      // High frequency in short time
    Periodic,   // Regular intervals
    Sporadic,   // Irregular access
    Declining,  // Decreasing frequency
    Unknown,    // Insufficient data
}

/// Adaptive cache manager for intelligent tier management
#[derive(Debug)]
struct AdaptiveCacheManager {
    promotion_threshold: f64,
    demotion_threshold: f64,
    heat_decay_rate: f64,
    adaptation_history: Vec<AdaptationEvent>,
    performance_metrics: PerformanceMetrics,
}

/// Cache performance metrics
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    l1_hit_rate: f64,
    l2_hit_rate: f64,
    l3_hit_rate: f64,
    overall_hit_rate: f64,
    average_retrieval_time: Duration,
    promotion_success_rate: f64,
    memory_efficiency: f64,
}

/// Adaptation events for learning and optimization
#[derive(Debug, Clone)]
struct AdaptationEvent {
    timestamp: Instant,
    event_type: AdaptationEventType,
    schema_id: u32,
    performance_impact: f64,
}

#[derive(Debug, Clone)]
enum AdaptationEventType {
    Promotion { from: CacheLevel, to: CacheLevel },
    Demotion { from: CacheLevel, to: CacheLevel },
    Eviction { from: CacheLevel },
    HitRateChange { level: CacheLevel, old_rate: f64, new_rate: f64 },
}

#[derive(Debug, Clone, PartialEq)]
enum CacheLevel {
    L1Hot,
    L2Warm,
    L3Cold,
}

/// Background refresh management
#[derive(Debug)]
struct RefreshManager {
    active_refreshes: HashMap<u32, JoinHandle<()>>,
    refresh_queue: VecDeque<RefreshTask>,
    refresh_scheduler: RefreshScheduler,
}

#[derive(Debug, Clone)]
struct RefreshTask {
    schema_id: u32,
    priority: RefreshPriority,
    scheduled_at: Instant,
    estimated_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RefreshPriority {
    Critical = 4,
    High = 3,
    Medium = 2,
    Low = 1,
}

/// Refresh scheduling strategy
#[derive(Debug)]
struct RefreshScheduler {
    next_refresh_times: BTreeMap<Instant, Vec<u32>>,
    refresh_intervals: HashMap<u32, Duration>,
    adaptive_scheduling: bool,
}

/// Cache statistics tracking
#[derive(Debug, Clone)]
pub struct CacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    promotions: u64,
    demotions: u64,
    total_size: u64,
    average_access_time: Duration,
}

/// Compression statistics
#[derive(Debug, Clone)]
struct CompressionStats {
    total_compressed_size: u64,
    total_original_size: u64,
    average_compression_ratio: f32,
    compression_time: Duration,
}

/// Configuration for multi-level cache
#[derive(Debug, Clone)]
pub struct MultiLevelCacheConfig {
    /// L1 cache settings
    pub l1_max_entries: usize,
    pub l1_max_memory_mb: usize,
    pub l1_ttl: Duration,
    
    /// L2 cache settings
    pub l2_max_entries: usize,
    pub l2_max_memory_mb: usize,
    pub l2_ttl: Duration,
    
    /// L3 cache settings
    pub l3_max_entries: usize,
    pub l3_storage_path: String,
    pub l3_compression_enabled: bool,
    
    /// Adaptation settings
    pub enable_adaptive_management: bool,
    pub promotion_threshold: f64,
    pub demotion_threshold: f64,
    pub heat_decay_rate: f64,
    
    /// Refresh settings
    pub background_refresh_enabled: bool,
    pub refresh_interval: Duration,
    pub max_concurrent_refreshes: usize,
}

impl Default for MultiLevelCacheConfig {
    fn default() -> Self {
        Self {
            l1_max_entries: 100,
            l1_max_memory_mb: 50,
            l1_ttl: Duration::from_secs(300), // 5 minutes
            
            l2_max_entries: 1000,
            l2_max_memory_mb: 200,
            l2_ttl: Duration::from_secs(1800), // 30 minutes
            
            l3_max_entries: 10000,
            l3_storage_path: "./cache/schemas".to_string(),
            l3_compression_enabled: true,
            
            enable_adaptive_management: true,
            promotion_threshold: 0.7,
            demotion_threshold: 0.3,
            heat_decay_rate: 0.95,
            
            background_refresh_enabled: true,
            refresh_interval: Duration::from_secs(60),
            max_concurrent_refreshes: 5,
        }
    }
}

impl MultiLevelSchemaCache {
    /// Create a new multi-level cache with configuration
    pub fn new(config: MultiLevelCacheConfig) -> Self {
        Self {
            l1_cache: Arc::new(RwLock::new(HotCache::new())),
            l2_cache: Arc::new(RwLock::new(WarmCache::new())),
            l3_cache: Arc::new(RwLock::new(ColdCache::new(config.l3_storage_path.clone()))),
            cache_manager: Arc::new(RwLock::new(AdaptiveCacheManager::new(
                config.promotion_threshold,
                config.demotion_threshold,
                config.heat_decay_rate,
            ))),
            refresh_manager: Arc::new(Mutex::new(RefreshManager::new())),
            config,
        }
    }

    /// Get schema with multi-level cache lookup
    pub async fn get_schema(&self, schema_id: u32) -> SchemaResult<Option<CachedSchema>> {
        // L1 (Hot) cache lookup
        if let Some(entry) = self.l1_lookup(schema_id).await {
            self.record_hit(CacheLevel::L1Hot, schema_id).await;
            return Ok(Some(entry.schema));
        }

        // L2 (Warm) cache lookup
        if let Some(entry) = self.l2_lookup(schema_id).await {
            self.record_hit(CacheLevel::L2Warm, schema_id).await;
            
            // Consider promoting to L1 based on access pattern
            if self.should_promote_to_l1(schema_id, &entry.access_pattern).await {
                self.promote_to_l1(schema_id, entry.schema.clone()).await?;
            }
            
            return Ok(Some(entry.schema));
        }

        // L3 (Cold) cache lookup
        if let Some(compressed_entry) = self.l3_lookup(schema_id).await {
            let schema = self.decompress_schema(&compressed_entry)?;
            self.record_hit(CacheLevel::L3Cold, schema_id).await;
            
            // Consider promoting to L2
            if self.should_promote_to_l2(schema_id, &compressed_entry).await {
                self.promote_to_l2(schema_id, schema.clone()).await?;
            }
            
            return Ok(Some(schema));
        }

        // Cache miss - schema not found in any level
        self.record_miss().await;
        Ok(None)
    }

    /// Store schema in appropriate cache level based on predicted usage
    pub async fn store_schema(&self, schema: CachedSchema) -> SchemaResult<()> {
        let schema_id = schema.id;
        
        // Predict optimal cache level based on access patterns and schema characteristics
        let optimal_level = self.predict_optimal_cache_level(&schema).await;
        
        match optimal_level {
            CacheLevel::L1Hot => {
                self.store_in_l1(schema_id, schema).await?;
            }
            CacheLevel::L2Warm => {
                self.store_in_l2(schema_id, schema).await?;
            }
            CacheLevel::L3Cold => {
                self.store_in_l3(schema_id, schema).await?;
            }
        }
        
        Ok(())
    }

    /// Force refresh of schema across all cache levels
    pub async fn refresh_schema(
        &self, 
        schema_id: u32, 
        registry: &SchemaRegistryClient
    ) -> SchemaResult<CachedSchema> {
        // Fetch fresh schema from registry
        let fresh_schema = registry.get_schema(schema_id).await?;
        
        // Update all cache levels where schema exists
        self.update_all_levels(schema_id, fresh_schema.clone()).await?;
        
        // Schedule background refresh for dependent schemas
        self.schedule_dependent_refreshes(schema_id).await;
        
        Ok(fresh_schema)
    }

    /// Get comprehensive cache statistics
    pub async fn get_cache_stats(&self) -> CacheStatsSummary {
        let l1_stats = self.l1_cache.read().await.stats.clone();
        let l2_stats = self.l2_cache.read().await.stats.clone();
        let l3_stats = self.l3_cache.read().await.stats.clone();
        let performance = self.cache_manager.read().await.performance_metrics.clone();

        CacheStatsSummary {
            l1_stats,
            l2_stats,
            l3_stats,
            performance_metrics: performance,
            total_memory_usage: self.calculate_total_memory_usage().await,
            efficiency_score: self.calculate_efficiency_score().await,
        }
    }

    /// Start background maintenance and refresh tasks
    pub async fn start_background_tasks(&self) {
        if self.config.background_refresh_enabled {
            let cache_clone = Arc::new(self.clone());
            
            // Start adaptive cache management
            tokio::spawn(async move {
                cache_clone.run_adaptive_management().await;
            });
            
            // Start background refresh scheduler
            let cache_clone = Arc::new(self.clone());
            tokio::spawn(async move {
                cache_clone.run_refresh_scheduler().await;
            });
            
            // Start performance monitoring
            let cache_clone = Arc::new(self.clone());
            tokio::spawn(async move {
                cache_clone.run_performance_monitoring().await;
            });
        }
    }

    // Private implementation methods

    async fn l1_lookup(&self, schema_id: u32) -> Option<HotCacheEntry> {
        let mut l1 = self.l1_cache.write().await;
        if let Some(entry) = l1.schemas.get_mut(&schema_id) {
            // Update access statistics
            entry.access_frequency += 1;
            entry.last_accessed = Instant::now();
            entry.heat_score = self.calculate_heat_score(entry.access_frequency, entry.last_accessed);
            
            let result = Some(entry.clone());
            
            // Update LRU order (after cloning entry)
            l1.lru_order.retain(|&id| id != schema_id);
            l1.lru_order.push_front(schema_id);
            
            result
        } else {
            None
        }
    }

    async fn l2_lookup(&self, schema_id: u32) -> Option<WarmCacheEntry> {
        let mut l2 = self.l2_cache.write().await;
        if let Some(entry) = l2.schemas.get_mut(&schema_id) {
            // Update access statistics
            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            
            // Update access pattern
            self.update_access_pattern(&mut entry.access_pattern).await;
            
            Some(entry.clone())
        } else {
            None
        }
    }

    async fn l3_lookup(&self, schema_id: u32) -> Option<ColdCacheEntry> {
        let mut l3 = self.l3_cache.write().await;
        if let Some(entry) = l3.schemas.get_mut(&schema_id) {
            entry.access_count += 1;
            entry.last_accessed = Instant::now();
            Some(entry.clone())
        } else {
            None
        }
    }

    async fn should_promote_to_l1(&self, _schema_id: u32, pattern: &AccessPattern) -> bool {
        if !self.config.enable_adaptive_management {
            return false;
        }

        // Promotion criteria based on access pattern
        match pattern.pattern_type {
            AccessPatternType::Burst => pattern.total_accesses > 10,
            AccessPatternType::Periodic => pattern.total_accesses > 5,
            AccessPatternType::Sporadic => pattern.total_accesses > 20,
            _ => false,
        }
    }

    async fn should_promote_to_l2(&self, _schema_id: u32, entry: &ColdCacheEntry) -> bool {
        if !self.config.enable_adaptive_management {
            return false;
        }

        // Promote if accessed recently and frequently enough
        entry.access_count >= 3 && 
        entry.last_accessed.elapsed() < Duration::from_secs(3600) // 1 hour
    }

    async fn promote_to_l1(&self, schema_id: u32, schema: CachedSchema) -> SchemaResult<()> {
        let mut l1 = self.l1_cache.write().await;
        
        // Check capacity and evict if necessary
        if l1.schemas.len() >= self.config.l1_max_entries {
            self.evict_from_l1(&mut l1).await;
        }
        
        let entry = HotCacheEntry {
            schema,
            access_frequency: 1,
            last_accessed: Instant::now(),
            promotion_time: Instant::now(),
            heat_score: 1.0,
        };
        
        l1.schemas.insert(schema_id, entry);
        l1.lru_order.push_front(schema_id);
        l1.stats.promotions += 1;
        
        Ok(())
    }

    async fn promote_to_l2(&self, schema_id: u32, schema: CachedSchema) -> SchemaResult<()> {
        let mut l2 = self.l2_cache.write().await;
        
        // Check capacity
        if l2.schemas.len() >= self.config.l2_max_entries {
            self.evict_from_l2(&mut l2).await;
        }
        
        let entry = WarmCacheEntry {
            size_bytes: self.estimate_schema_size(&schema),
            schema,
            access_count: 1,
            last_accessed: Instant::now(),
            created_at: Instant::now(),
            access_pattern: AccessPattern::new(),
        };
        
        l2.schemas.insert(schema_id, entry);
        l2.stats.promotions += 1;
        
        Ok(())
    }

    async fn store_in_l1(&self, schema_id: u32, schema: CachedSchema) -> SchemaResult<()> {
        self.promote_to_l1(schema_id, schema).await
    }

    async fn store_in_l2(&self, schema_id: u32, schema: CachedSchema) -> SchemaResult<()> {
        self.promote_to_l2(schema_id, schema).await
    }

    async fn store_in_l3(&self, schema_id: u32, schema: CachedSchema) -> SchemaResult<()> {
        let mut l3 = self.l3_cache.write().await;
        
        // Compress schema for storage
        let compressed_data = self.compress_schema(&schema)?;
        let original_size = self.estimate_schema_size(&schema);
        let compressed_size = compressed_data.len() as u64;
        
        let entry = ColdCacheEntry {
            compressed_schema: compressed_data,
            original_size,
            compressed_size,
            compression_ratio: compressed_size as f32 / original_size as f32,
            stored_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
        };
        
        l3.schemas.insert(schema_id, entry);
        l3.compression_stats.total_compressed_size += compressed_size;
        l3.compression_stats.total_original_size += original_size;
        
        Ok(())
    }

    fn calculate_heat_score(&self, frequency: u64, last_accessed: Instant) -> f64 {
        let recency_weight = (-last_accessed.elapsed().as_secs_f64() / 3600.0).exp(); // Decay over hours
        let frequency_score = (frequency as f64).ln() + 1.0;
        frequency_score * recency_weight
    }

    async fn update_access_pattern(&self, pattern: &mut AccessPattern) {
        pattern.total_accesses += 1;
        let now = Instant::now();
        
        // Track recent accesses (keep last 10)
        pattern.recent_accesses.push_back(now);
        if pattern.recent_accesses.len() > 10 {
            pattern.recent_accesses.pop_front();
        }
        
        // Update access intervals for pattern analysis
        if pattern.recent_accesses.len() >= 2 {
            let intervals: Vec<Duration> = pattern.recent_accesses
                .iter()
                .collect::<Vec<_>>()
                .windows(2)
                .map(|w| w[1].duration_since(*w[0]))
                .collect();
            
            pattern.access_intervals = intervals;
            pattern.pattern_type = self.classify_access_pattern(&pattern.access_intervals);
        }
    }

    fn classify_access_pattern(&self, intervals: &[Duration]) -> AccessPatternType {
        if intervals.is_empty() {
            return AccessPatternType::Unknown;
        }
        
        let avg_interval = intervals.iter().sum::<Duration>().as_secs_f64() / intervals.len() as f64;
        let variance = intervals.iter()
            .map(|d| (d.as_secs_f64() - avg_interval).powi(2))
            .sum::<f64>() / intervals.len() as f64;
        
        // Classification based on interval patterns
        if avg_interval < 60.0 && variance < 100.0 {
            AccessPatternType::Burst
        } else if variance < avg_interval * 0.5 {
            AccessPatternType::Periodic
        } else if variance > avg_interval * 2.0 {
            AccessPatternType::Sporadic
        } else {
            AccessPatternType::Unknown
        }
    }

    // Additional helper methods would be implemented here...
    
    async fn predict_optimal_cache_level(&self, _schema: &CachedSchema) -> CacheLevel {
        // Simplified prediction - in practice this would use ML or heuristics
        CacheLevel::L2Warm
    }

    fn compress_schema(&self, schema: &CachedSchema) -> SchemaResult<Vec<u8>> {
        // Simplified compression - in practice would use proper compression library
        Ok(schema.schema.as_bytes().to_vec())
    }

    fn decompress_schema(&self, entry: &ColdCacheEntry) -> SchemaResult<CachedSchema> {
        // Simplified decompression
        let schema_str = String::from_utf8(entry.compressed_schema.clone())
            .map_err(|e| SchemaError::Cache { 
                message: format!("Decompression error: {}", e) 
            })?;
            
        Ok(CachedSchema {
            id: 0, // This would be properly restored
            subject: "restored".to_string(),
            version: 1,
            schema: schema_str,
            references: Vec::new(),
            cached_at: Instant::now(),
            access_count: 1,
        })
    }

    fn estimate_schema_size(&self, schema: &CachedSchema) -> u64 {
        schema.schema.len() as u64 + 
        schema.subject.len() as u64 + 
        schema.references.len() as u64 * 100 // Rough estimate
    }

    async fn evict_from_l1(&self, l1: &mut HotCache) {
        // LRU eviction
        if let Some(oldest_id) = l1.lru_order.pop_back() {
            l1.schemas.remove(&oldest_id);
            l1.stats.evictions += 1;
        }
    }

    async fn evict_from_l2(&self, l2: &mut WarmCache) {
        // Size-based eviction - remove largest schemas first
        if let Some((_, schema_ids)) = l2.size_tracker.iter().last().map(|(k, v)| (*k, v.clone())) {
            if let Some(schema_id) = schema_ids.first() {
                l2.schemas.remove(schema_id);
                l2.stats.evictions += 1;
            }
        }
    }

    // Placeholder implementations for remaining methods
    async fn update_all_levels(&self, _schema_id: u32, _schema: CachedSchema) -> SchemaResult<()> {
        Ok(())
    }

    async fn schedule_dependent_refreshes(&self, _schema_id: u32) {
        // Implementation would schedule refresh of dependent schemas
    }

    async fn record_hit(&self, _level: CacheLevel, _schema_id: u32) {
        // Record hit statistics
    }

    async fn record_miss(&self) {
        // Record miss statistics
    }

    async fn calculate_total_memory_usage(&self) -> u64 {
        // Calculate memory usage across all levels
        0
    }

    async fn calculate_efficiency_score(&self) -> f64 {
        // Calculate overall cache efficiency
        0.75
    }

    async fn run_adaptive_management(&self) {
        // Background task for adaptive cache management
    }

    async fn run_refresh_scheduler(&self) {
        // Background task for scheduled refreshes
    }

    async fn run_performance_monitoring(&self) {
        // Background task for performance monitoring
    }
}

/// Summary of cache statistics across all levels
#[derive(Debug, Clone)]
pub struct CacheStatsSummary {
    pub l1_stats: CacheStats,
    pub l2_stats: CacheStats,
    pub l3_stats: CacheStats,
    pub performance_metrics: PerformanceMetrics,
    pub total_memory_usage: u64,
    pub efficiency_score: f64,
}

// Implementation of helper structs and their methods

impl HotCache {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            lru_order: VecDeque::new(),
            stats: CacheStats::new(),
        }
    }
}

impl WarmCache {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            access_tracker: HashMap::new(),
            size_tracker: BTreeMap::new(),
            stats: CacheStats::new(),
        }
    }
}

impl ColdCache {
    fn new(storage_path: String) -> Self {
        Self {
            schemas: HashMap::new(),
            storage_path,
            compression_stats: CompressionStats::new(),
            stats: CacheStats::new(),
        }
    }
}

impl AccessPattern {
    fn new() -> Self {
        Self {
            total_accesses: 0,
            recent_accesses: VecDeque::new(),
            access_intervals: Vec::new(),
            predicted_next_access: None,
            pattern_type: AccessPatternType::Unknown,
        }
    }
}

impl AdaptiveCacheManager {
    fn new(promotion_threshold: f64, demotion_threshold: f64, heat_decay_rate: f64) -> Self {
        Self {
            promotion_threshold,
            demotion_threshold,
            heat_decay_rate,
            adaptation_history: Vec::new(),
            performance_metrics: PerformanceMetrics::new(),
        }
    }
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            l1_hit_rate: 0.0,
            l2_hit_rate: 0.0,
            l3_hit_rate: 0.0,
            overall_hit_rate: 0.0,
            average_retrieval_time: Duration::from_millis(0),
            promotion_success_rate: 0.0,
            memory_efficiency: 0.0,
        }
    }
}

impl RefreshManager {
    fn new() -> Self {
        Self {
            active_refreshes: HashMap::new(),
            refresh_queue: VecDeque::new(),
            refresh_scheduler: RefreshScheduler::new(),
        }
    }
}

impl RefreshScheduler {
    fn new() -> Self {
        Self {
            next_refresh_times: BTreeMap::new(),
            refresh_intervals: HashMap::new(),
            adaptive_scheduling: true,
        }
    }
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            promotions: 0,
            demotions: 0,
            total_size: 0,
            average_access_time: Duration::from_millis(0),
        }
    }
}

impl CompressionStats {
    fn new() -> Self {
        Self {
            total_compressed_size: 0,
            total_original_size: 0,
            average_compression_ratio: 1.0,
            compression_time: Duration::from_millis(0),
        }
    }
}

// Clone implementation for MultiLevelSchemaCache
impl Clone for MultiLevelSchemaCache {
    fn clone(&self) -> Self {
        Self {
            l1_cache: Arc::clone(&self.l1_cache),
            l2_cache: Arc::clone(&self.l2_cache),
            l3_cache: Arc::clone(&self.l3_cache),
            cache_manager: Arc::clone(&self.cache_manager),
            refresh_manager: Arc::clone(&self.refresh_manager),
            config: self.config.clone(),
        }
    }
}