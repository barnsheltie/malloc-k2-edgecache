//! Cache module - provides memory and disk caching for S3 objects

mod cache_key;
mod disk_cache;
mod memory_cache;

pub use cache_key::CacheKey;
pub use disk_cache::DiskCache;
pub use memory_cache::MemoryCache;

use crate::config::CacheConfig;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Cached object entry
#[derive(Debug, Clone)]
pub struct CachedObject {
    pub key: CacheKey,
    pub body: Bytes,
    pub headers: HashMap<String, String>,
    pub content_type: Option<String>,
    pub content_length: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub created_at: Instant,
    pub ttl: Duration,
    pub hit_count: u64,
}

impl CachedObject {
    pub fn new(key: CacheKey, body: Bytes, headers: HashMap<String, String>, ttl: Duration) -> Self {
        let content_type = headers.get("content-type").cloned();
        let content_length = body.len() as u64;
        let etag = headers.get("etag").cloned();
        let last_modified = headers.get("last-modified").cloned();

        Self {
            key,
            body,
            headers,
            content_type,
            content_length,
            etag,
            last_modified,
            created_at: Instant::now(),
            ttl,
            hit_count: 0,
        }
    }

    /// Check if the cached object has expired
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub memory_hits: u64,
    pub memory_misses: u64,
    pub disk_hits: u64,
    pub disk_misses: u64,
    pub memory_size_bytes: u64,
    pub disk_size_bytes: u64,
    pub total_objects: u64,
    /// Total items expired (memory + disk)
    pub expired_count: u64,
    /// Last expiration run timestamp (unix seconds)
    pub last_expiration_run: u64,
}

/// Main cache manager coordinating memory and disk caches
pub struct CacheManager {
    config: CacheConfig,
    memory_cache: MemoryCache,
    disk_cache: DiskCache,
    stats: Arc<RwLock<CacheStats>>,
}

impl CacheManager {
    pub fn new(config: &CacheConfig) -> Self {
        let memory_cache = MemoryCache::new(config.memory_max_size_mb * 1024 * 1024);
        let disk_cache = DiskCache::new(&config.disk_path, config.disk_max_size_gb * 1024 * 1024 * 1024);

        info!(
            "Initialized cache manager: memory={}MB, disk={}GB, ttl={}s",
            config.memory_max_size_mb, config.disk_max_size_gb, config.default_ttl_seconds
        );

        Self {
            config: config.clone(),
            memory_cache,
            disk_cache,
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Get an object from cache (tries memory first, then disk)
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        // Try memory cache first
        if let Some(obj) = self.memory_cache.get(key).await {
            if !obj.is_expired() {
                let mut stats = self.stats.write().await;
                stats.memory_hits += 1;
                debug!("Memory cache hit for {:?}", key);
                return Some(obj);
            } else {
                // Remove expired object
                self.memory_cache.remove(key).await;
            }
        }

        // Update memory miss stats
        {
            let mut stats = self.stats.write().await;
            stats.memory_misses += 1;
        }

        // Try disk cache
        if let Some(obj) = self.disk_cache.get(key).await {
            if !obj.is_expired() {
                let mut stats = self.stats.write().await;
                stats.disk_hits += 1;
                debug!("Disk cache hit for {:?}", key);

                // Promote to memory cache if small enough
                if obj.content_length < self.config.memory_max_size_mb * 1024 * 1024 / 10 {
                    self.memory_cache.put(obj.clone()).await;
                }

                return Some(obj);
            } else {
                // Remove expired object
                self.disk_cache.remove(key).await;
            }
        }

        // Update disk miss stats
        {
            let mut stats = self.stats.write().await;
            stats.disk_misses += 1;
        }

        debug!("Cache miss for {:?}", key);
        None
    }

    /// Store an object in cache
    pub async fn put(&self, key: CacheKey, body: Bytes, headers: HashMap<String, String>) {
        let ttl = Duration::from_secs(self.config.default_ttl_seconds);
        let obj = CachedObject::new(key.clone(), body, headers, ttl);

        // Store in memory if small enough
        let memory_threshold = self.config.memory_max_size_mb * 1024 * 1024 / 10;
        if obj.content_length < memory_threshold {
            self.memory_cache.put(obj.clone()).await;
            debug!("Stored in memory cache: {:?} ({}B)", key, obj.content_length);
        }

        // Always store to disk for persistence
        if let Err(e) = self.disk_cache.put(obj).await {
            warn!("Failed to store in disk cache: {}", e);
        } else {
            debug!("Stored in disk cache: {:?}", key);
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_objects += 1;
    }

    /// Remove an object from cache
    pub async fn remove(&self, key: &CacheKey) {
        self.memory_cache.remove(key).await;
        self.disk_cache.remove(key).await;
    }

    /// Clear all caches
    pub async fn clear(&self) {
        self.memory_cache.clear().await;
        if let Err(e) = self.disk_cache.clear().await {
            warn!("Failed to clear disk cache: {}", e);
        }

        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let mut stats = self.stats.read().await.clone();
        stats.memory_size_bytes = self.memory_cache.size().await;
        stats.disk_size_bytes = self.disk_cache.size().await;
        stats
    }

    /// Get default TTL
    pub fn default_ttl(&self) -> Duration {
        Duration::from_secs(self.config.default_ttl_seconds)
    }

    /// Remove all expired entries from both memory and disk caches
    /// Returns the total number of items removed
    pub async fn remove_expired(&self) -> u64 {
        let memory_expired = self.memory_cache.remove_expired().await;
        let disk_expired = self.disk_cache.remove_expired().await;
        let total = memory_expired + disk_expired;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.expired_count += total;
            stats.last_expiration_run = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        if total > 0 {
            info!("Expired {} items (memory: {}, disk: {})", total, memory_expired, disk_expired);
        }

        total
    }

    /// Start a background task that periodically removes expired entries
    /// Returns a handle that can be used to stop the task
    pub fn start_expiration_task(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        info!("Starting cache expiration task with interval {:?}", interval);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                self.remove_expired().await;
            }
        })
    }
}
