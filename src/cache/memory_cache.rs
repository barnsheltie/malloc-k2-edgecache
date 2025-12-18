//! In-memory LRU cache implementation

use super::{CacheKey, CachedObject};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory LRU cache with size limits
pub struct MemoryCache {
    /// Maximum size in bytes
    max_size: u64,
    /// Current size in bytes
    current_size: AtomicU64,
    /// The actual cache storage
    data: DashMap<CacheKey, CachedObject>,
    /// LRU order tracking (most recent at back)
    lru_order: Mutex<VecDeque<CacheKey>>,
}

impl MemoryCache {
    /// Create a new memory cache with the specified max size in bytes
    pub fn new(max_size: u64) -> Self {
        Self {
            max_size,
            current_size: AtomicU64::new(0),
            data: DashMap::new(),
            lru_order: Mutex::new(VecDeque::new()),
        }
    }

    /// Get an object from the cache
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        if let Some(mut entry) = self.data.get_mut(key) {
            // Update hit count
            entry.hit_count += 1;

            // Move to end of LRU (most recently used)
            let mut lru = self.lru_order.lock();
            if let Some(pos) = lru.iter().position(|k| k == key) {
                lru.remove(pos);
                lru.push_back(key.clone());
            }

            return Some(entry.clone());
        }
        None
    }

    /// Store an object in the cache
    pub async fn put(&self, obj: CachedObject) {
        let obj_size = obj.content_length;

        // Evict items if we need space
        self.evict_if_needed(obj_size).await;

        let key = obj.key.clone();

        // Remove old entry if exists (to update size correctly)
        if let Some((_, old)) = self.data.remove(&key) {
            self.current_size.fetch_sub(old.content_length, Ordering::SeqCst);
        }

        // Insert new entry
        self.data.insert(key.clone(), obj);
        self.current_size.fetch_add(obj_size, Ordering::SeqCst);

        // Update LRU order
        let mut lru = self.lru_order.lock();
        // Remove if already present
        if let Some(pos) = lru.iter().position(|k| k == &key) {
            lru.remove(pos);
        }
        lru.push_back(key);
    }

    /// Remove an object from the cache
    pub async fn remove(&self, key: &CacheKey) {
        if let Some((_, obj)) = self.data.remove(key) {
            self.current_size.fetch_sub(obj.content_length, Ordering::SeqCst);

            let mut lru = self.lru_order.lock();
            if let Some(pos) = lru.iter().position(|k| k == key) {
                lru.remove(pos);
            }
        }
    }

    /// Clear all entries
    pub async fn clear(&self) {
        self.data.clear();
        self.current_size.store(0, Ordering::SeqCst);
        self.lru_order.lock().clear();
    }

    /// Get current size in bytes
    pub async fn size(&self) -> u64 {
        self.current_size.load(Ordering::SeqCst)
    }

    /// Get number of cached objects
    pub async fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if cache is empty
    pub async fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Evict least recently used items until we have enough space
    async fn evict_if_needed(&self, needed_size: u64) {
        while self.current_size.load(Ordering::SeqCst) + needed_size > self.max_size {
            // Get the least recently used key
            let key_to_remove = {
                let mut lru = self.lru_order.lock();
                lru.pop_front()
            };

            if let Some(key) = key_to_remove {
                if let Some((_, obj)) = self.data.remove(&key) {
                    self.current_size.fetch_sub(obj.content_length, Ordering::SeqCst);
                    tracing::debug!("Evicted from memory cache: {:?}", key);
                }
            } else {
                // No more items to evict
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::time::Duration;

    fn create_test_object(bucket: &str, key: &str, size: usize) -> CachedObject {
        let cache_key = CacheKey::new(bucket, key);
        let body = Bytes::from(vec![0u8; size]);
        CachedObject::new(cache_key, body, HashMap::new(), Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let cache = MemoryCache::new(1024 * 1024); // 1MB
        let obj = create_test_object("bucket", "key", 100);
        let key = obj.key.clone();

        cache.put(obj).await;
        let retrieved = cache.get(&key).await;

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().content_length, 100);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let cache = MemoryCache::new(250); // Very small cache

        // Add three objects
        let obj1 = create_test_object("bucket", "key1", 100);
        let obj2 = create_test_object("bucket", "key2", 100);
        let obj3 = create_test_object("bucket", "key3", 100);

        let key1 = obj1.key.clone();
        let key2 = obj2.key.clone();
        let key3 = obj3.key.clone();

        cache.put(obj1).await;
        cache.put(obj2).await;

        // Access key1 to make it more recent
        cache.get(&key1).await;

        // Add key3, should evict key2 (least recently used)
        cache.put(obj3).await;

        assert!(cache.get(&key1).await.is_some());
        assert!(cache.get(&key2).await.is_none());
        assert!(cache.get(&key3).await.is_some());
    }

    #[tokio::test]
    async fn test_remove() {
        let cache = MemoryCache::new(1024 * 1024);
        let obj = create_test_object("bucket", "key", 100);
        let key = obj.key.clone();

        cache.put(obj).await;
        assert!(cache.get(&key).await.is_some());

        cache.remove(&key).await;
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_clear() {
        let cache = MemoryCache::new(1024 * 1024);

        cache.put(create_test_object("bucket", "key1", 100)).await;
        cache.put(create_test_object("bucket", "key2", 100)).await;

        assert_eq!(cache.len().await, 2);

        cache.clear().await;
        assert_eq!(cache.len().await, 0);
        assert_eq!(cache.size().await, 0);
    }
}
