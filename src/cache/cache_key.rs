//! Cache key generation for S3 objects

use sha2::{Digest, Sha256};
use std::fmt;

/// Unique key for cached objects
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// S3 bucket name
    pub bucket: String,
    /// S3 object key
    pub key: String,
    /// Computed hash for filesystem storage
    hash: String,
}

impl CacheKey {
    /// Create a new cache key from bucket and object key
    pub fn new(bucket: &str, key: &str) -> Self {
        let combined = format!("{}/{}", bucket, key);
        let mut hasher = Sha256::new();
        hasher.update(combined.as_bytes());
        let hash = hex::encode(hasher.finalize());

        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            hash,
        }
    }

    /// Get the hash string for filesystem paths
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Get a short hash prefix for directory sharding
    pub fn hash_prefix(&self, len: usize) -> &str {
        &self.hash[..len.min(self.hash.len())]
    }

    /// Get the full path representation
    pub fn full_path(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket, self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_creation() {
        let key = CacheKey::new("mybucket", "path/to/object.txt");
        assert_eq!(key.bucket, "mybucket");
        assert_eq!(key.key, "path/to/object.txt");
        assert!(!key.hash.is_empty());
        assert_eq!(key.hash.len(), 64); // SHA256 produces 64 hex chars
    }

    #[test]
    fn test_cache_key_hash_consistency() {
        let key1 = CacheKey::new("bucket", "key");
        let key2 = CacheKey::new("bucket", "key");
        assert_eq!(key1.hash, key2.hash);
    }

    #[test]
    fn test_cache_key_hash_uniqueness() {
        let key1 = CacheKey::new("bucket1", "key");
        let key2 = CacheKey::new("bucket2", "key");
        assert_ne!(key1.hash, key2.hash);
    }

    #[test]
    fn test_hash_prefix() {
        let key = CacheKey::new("bucket", "key");
        assert_eq!(key.hash_prefix(2).len(), 2);
        assert_eq!(key.hash_prefix(4).len(), 4);
    }
}
