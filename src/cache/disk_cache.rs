//! Disk-based persistent cache implementation

use super::{CacheKey, CachedObject};
use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

/// Metadata stored alongside cached objects
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheMetadata {
    bucket: String,
    key: String,
    headers: HashMap<String, String>,
    content_type: Option<String>,
    content_length: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    created_at_unix: u64,
    ttl_seconds: u64,
    hit_count: u64,
}

/// Disk-based persistent cache
pub struct DiskCache {
    /// Root directory for cache storage
    root_path: PathBuf,
    /// Maximum size in bytes
    max_size: u64,
    /// Current size in bytes (approximate)
    current_size: AtomicU64,
}

impl DiskCache {
    /// Create a new disk cache at the specified path
    pub fn new(path: &str, max_size: u64) -> Self {
        let root_path = PathBuf::from(path);

        // Create cache directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&root_path) {
            warn!("Failed to create cache directory {}: {}", path, e);
        }

        // Calculate initial size by scanning directory
        let current_size = Self::calculate_dir_size(&root_path).unwrap_or(0);

        Self {
            root_path,
            max_size,
            current_size: AtomicU64::new(current_size),
        }
    }

    /// Get an object from the disk cache
    pub async fn get(&self, key: &CacheKey) -> Option<CachedObject> {
        let (data_path, meta_path) = self.get_paths(key);

        // Read metadata first
        let metadata: CacheMetadata = match fs::read_to_string(&meta_path).await {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(meta) => meta,
                Err(e) => {
                    debug!("Failed to parse cache metadata: {}", e);
                    return None;
                }
            },
            Err(_) => return None,
        };

        // Read data
        let body = match fs::read(&data_path).await {
            Ok(data) => Bytes::from(data),
            Err(e) => {
                debug!("Failed to read cache data: {}", e);
                return None;
            }
        };

        // Convert to CachedObject
        let created_at = Instant::now() - Duration::from_secs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - metadata.created_at_unix,
        );

        Some(CachedObject {
            key: key.clone(),
            body,
            headers: metadata.headers,
            content_type: metadata.content_type,
            content_length: metadata.content_length,
            etag: metadata.etag,
            last_modified: metadata.last_modified,
            created_at,
            ttl: Duration::from_secs(metadata.ttl_seconds),
            hit_count: metadata.hit_count,
        })
    }

    /// Store an object in the disk cache
    pub async fn put(&self, obj: CachedObject) -> Result<()> {
        let (data_path, meta_path) = self.get_paths(&obj.key);

        // Create parent directories
        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create cache directory")?;
        }

        // Evict if needed
        self.evict_if_needed(obj.content_length).await;

        // Create metadata
        let metadata = CacheMetadata {
            bucket: obj.key.bucket.clone(),
            key: obj.key.key.clone(),
            headers: obj.headers.clone(),
            content_type: obj.content_type.clone(),
            content_length: obj.content_length,
            etag: obj.etag.clone(),
            last_modified: obj.last_modified.clone(),
            created_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            ttl_seconds: obj.ttl.as_secs(),
            hit_count: obj.hit_count,
        };

        // Write metadata
        let meta_json = serde_json::to_string_pretty(&metadata)?;
        fs::write(&meta_path, meta_json)
            .await
            .context("Failed to write cache metadata")?;

        // Write data
        fs::write(&data_path, &obj.body)
            .await
            .context("Failed to write cache data")?;

        // Update size
        self.current_size.fetch_add(obj.content_length, Ordering::SeqCst);

        debug!("Stored in disk cache: {:?}", obj.key);
        Ok(())
    }

    /// Remove an object from the disk cache
    pub async fn remove(&self, key: &CacheKey) {
        let (data_path, meta_path) = self.get_paths(key);

        // Get size before removal
        if let Ok(metadata) = fs::metadata(&data_path).await {
            let size = metadata.len();
            self.current_size.fetch_sub(size, Ordering::SeqCst);
        }

        // Remove files
        let _ = fs::remove_file(&data_path).await;
        let _ = fs::remove_file(&meta_path).await;

        // Try to remove empty parent directories
        if let Some(parent) = data_path.parent() {
            let _ = fs::remove_dir(parent).await;
        }
    }

    /// Clear all cached objects
    pub async fn clear(&self) -> Result<()> {
        if self.root_path.exists() {
            fs::remove_dir_all(&self.root_path)
                .await
                .context("Failed to clear cache directory")?;
            fs::create_dir_all(&self.root_path)
                .await
                .context("Failed to recreate cache directory")?;
        }
        self.current_size.store(0, Ordering::SeqCst);
        Ok(())
    }

    /// Get current size in bytes
    pub async fn size(&self) -> u64 {
        self.current_size.load(Ordering::SeqCst)
    }

    /// Scan and remove all expired entries, returns count of removed items
    pub async fn remove_expired(&self) -> u64 {
        let mut removed = 0u64;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut expired_keys = Vec::new();

        // Walk the cache directory looking for expired entries
        if let Ok(mut entries) = fs::read_dir(&self.root_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(mut subentries) = fs::read_dir(&path).await {
                        while let Ok(Some(subentry)) = subentries.next_entry().await {
                            let subpath = subentry.path();
                            if subpath.extension().map(|e| e == "meta").unwrap_or(false) {
                                if let Ok(content) = fs::read_to_string(&subpath).await {
                                    if let Ok(meta) = serde_json::from_str::<CacheMetadata>(&content) {
                                        // Check if expired
                                        let expires_at = meta.created_at_unix + meta.ttl_seconds;
                                        if now > expires_at {
                                            expired_keys.push(CacheKey::new(&meta.bucket, &meta.key));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Remove expired entries
        for key in expired_keys {
            self.remove(&key).await;
            removed += 1;
        }

        if removed > 0 {
            debug!("Removed {} expired entries from disk cache", removed);
        }

        removed
    }

    /// Get paths for data and metadata files
    fn get_paths(&self, key: &CacheKey) -> (PathBuf, PathBuf) {
        // Use hash prefix for directory sharding to avoid too many files in one dir
        let prefix = key.hash_prefix(2);
        let hash = key.hash();

        let dir = self.root_path.join(prefix);
        let data_path = dir.join(format!("{}.data", hash));
        let meta_path = dir.join(format!("{}.meta", hash));

        (data_path, meta_path)
    }

    /// Calculate total size of a directory
    fn calculate_dir_size(path: &Path) -> Result<u64> {
        let mut size = 0u64;
        if path.exists() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    size += Self::calculate_dir_size(&path)?;
                } else if path.extension().map(|e| e == "data").unwrap_or(false) {
                    size += entry.metadata()?.len();
                }
            }
        }
        Ok(size)
    }

    /// Evict oldest items if we need space
    async fn evict_if_needed(&self, needed_size: u64) {
        // Simple eviction: if we're over max size, remove oldest files
        // In production, you'd want a more sophisticated LRU implementation
        while self.current_size.load(Ordering::SeqCst) + needed_size > self.max_size {
            // Find and remove the oldest file
            if let Some(oldest) = self.find_oldest_entry().await {
                let key = CacheKey::new(&oldest.bucket, &oldest.key);
                self.remove(&key).await;
                debug!("Evicted from disk cache: {:?}", key);
            } else {
                break;
            }
        }
    }

    /// Find the oldest cache entry
    async fn find_oldest_entry(&self) -> Option<CacheMetadata> {
        let mut oldest: Option<(u64, CacheMetadata)> = None;

        // Walk the cache directory
        if let Ok(mut entries) = fs::read_dir(&self.root_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(mut subentries) = fs::read_dir(&path).await {
                        while let Ok(Some(subentry)) = subentries.next_entry().await {
                            let subpath = subentry.path();
                            if subpath.extension().map(|e| e == "meta").unwrap_or(false) {
                                if let Ok(content) = fs::read_to_string(&subpath).await {
                                    if let Ok(meta) = serde_json::from_str::<CacheMetadata>(&content) {
                                        match &oldest {
                                            None => oldest = Some((meta.created_at_unix, meta)),
                                            Some((oldest_time, _)) if meta.created_at_unix < *oldest_time => {
                                                oldest = Some((meta.created_at_unix, meta));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        oldest.map(|(_, meta)| meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_object(bucket: &str, key: &str, size: usize) -> CachedObject {
        let cache_key = CacheKey::new(bucket, key);
        let body = Bytes::from(vec![b'X'; size]);
        CachedObject::new(cache_key, body, HashMap::new(), Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn test_disk_cache_put_get() {
        let temp_dir = tempdir().unwrap();
        let cache = DiskCache::new(
            temp_dir.path().to_str().unwrap(),
            1024 * 1024, // 1MB
        );

        let obj = create_test_object("bucket", "key", 100);
        let key = obj.key.clone();

        cache.put(obj).await.unwrap();

        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().content_length, 100);
    }

    #[tokio::test]
    async fn test_disk_cache_remove() {
        let temp_dir = tempdir().unwrap();
        let cache = DiskCache::new(
            temp_dir.path().to_str().unwrap(),
            1024 * 1024,
        );

        let obj = create_test_object("bucket", "key", 100);
        let key = obj.key.clone();

        cache.put(obj).await.unwrap();
        assert!(cache.get(&key).await.is_some());

        cache.remove(&key).await;
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_disk_cache_clear() {
        let temp_dir = tempdir().unwrap();
        let cache = DiskCache::new(
            temp_dir.path().to_str().unwrap(),
            1024 * 1024,
        );

        cache.put(create_test_object("bucket", "key1", 100)).await.unwrap();
        cache.put(create_test_object("bucket", "key2", 100)).await.unwrap();

        cache.clear().await.unwrap();

        let key1 = CacheKey::new("bucket", "key1");
        let key2 = CacheKey::new("bucket", "key2");
        assert!(cache.get(&key1).await.is_none());
        assert!(cache.get(&key2).await.is_none());
    }
}
