//! Local cache for object→owner mappings
//!
//! Caches `locate_response` results to avoid repeated federator queries.
//! Entries are invalidated when the federator sends an invalidation message.

use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Information about the owner of an object
#[derive(Debug, Clone)]
pub struct OwnerInfo {
    pub node_id: String,
    pub endpoint: String,
}

/// Cached route entry with TTL
#[derive(Debug, Clone)]
struct CachedRoute {
    owner: Option<OwnerInfo>,
    cached_at: Instant,
}

/// Local cache of object→owner mappings
pub struct RouteCache {
    cache: DashMap<String, CachedRoute>,
    ttl: Duration,
}

impl RouteCache {
    /// Create a new route cache with the specified TTL
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            cache: DashMap::new(),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get the cached owner for an object
    pub fn get(&self, bucket: &str, key: &str) -> Option<Option<OwnerInfo>> {
        let cache_key = format!("{}:{}", bucket, key);
        if let Some(entry) = self.cache.get(&cache_key) {
            if entry.cached_at.elapsed() < self.ttl {
                return Some(entry.owner.clone());
            }
            // Entry expired, remove it
            drop(entry);
            self.cache.remove(&cache_key);
        }
        None
    }

    /// Cache the owner info for an object
    pub fn set(&self, bucket: &str, key: &str, owner: Option<OwnerInfo>) {
        let cache_key = format!("{}:{}", bucket, key);
        self.cache.insert(
            cache_key,
            CachedRoute {
                owner,
                cached_at: Instant::now(),
            },
        );
    }

    /// Invalidate a cached route entry
    pub fn invalidate(&self, bucket: &str, key: &str) {
        let cache_key = format!("{}:{}", bucket, key);
        self.cache.remove(&cache_key);
    }

    /// Clear all cached routes
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Remove expired entries (called periodically)
    pub fn cleanup_expired(&self) {
        self.cache.retain(|_, entry| entry.cached_at.elapsed() < self.ttl);
    }
}

impl Default for RouteCache {
    fn default() -> Self {
        // Default TTL of 5 minutes
        Self::new(300)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_route_cache_basic() {
        let cache = RouteCache::new(60);

        // Initially empty
        assert!(cache.get("bucket", "key1").is_none());

        // Set an owner
        cache.set(
            "bucket",
            "key1",
            Some(OwnerInfo {
                node_id: "node-1".to_string(),
                endpoint: "http://node-1:9000".to_string(),
            }),
        );

        // Should be cached
        let owner = cache.get("bucket", "key1").unwrap();
        assert!(owner.is_some());
        let owner = owner.unwrap();
        assert_eq!(owner.node_id, "node-1");
    }

    #[test]
    fn test_route_cache_no_owner() {
        let cache = RouteCache::new(60);

        // Cache a "no owner" result
        cache.set("bucket", "key1", None);

        // Should return Some(None) - meaning "cached, but no owner"
        let result = cache.get("bucket", "key1");
        assert!(result.is_some());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_route_cache_invalidation() {
        let cache = RouteCache::new(60);

        cache.set(
            "bucket",
            "key1",
            Some(OwnerInfo {
                node_id: "node-1".to_string(),
                endpoint: "http://node-1:9000".to_string(),
            }),
        );

        assert!(cache.get("bucket", "key1").is_some());

        cache.invalidate("bucket", "key1");

        assert!(cache.get("bucket", "key1").is_none());
    }

    #[test]
    fn test_route_cache_expiration() {
        let cache = RouteCache::new(1); // 1 second TTL

        cache.set(
            "bucket",
            "key1",
            Some(OwnerInfo {
                node_id: "node-1".to_string(),
                endpoint: "http://node-1:9000".to_string(),
            }),
        );

        assert!(cache.get("bucket", "key1").is_some());

        // Wait for expiration
        sleep(Duration::from_millis(1100));

        // Should be expired
        assert!(cache.get("bucket", "key1").is_none());
    }
}
