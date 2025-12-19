//! Multipart upload handling for S3
//!
//! Implements the S3 multipart upload API:
//! - InitiateMultipartUpload: POST /bucket/key?uploads
//! - UploadPart: PUT /bucket/key?partNumber=N&uploadId=ID
//! - CompleteMultipartUpload: POST /bucket/key?uploadId=ID
//! - AbortMultipartUpload: DELETE /bucket/key?uploadId=ID
//! - ListParts: GET /bucket/key?uploadId=ID

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

/// Default multipart upload expiration (24 hours)
const DEFAULT_UPLOAD_EXPIRATION_SECS: u64 = 86400;

/// Minimum part size (5MB) - AWS requirement except for last part
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum part size (5GB)
pub const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Maximum number of parts (10000)
pub const MAX_PARTS: u32 = 10000;

/// Information about an uploaded part
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPart {
    /// Part number (1-10000)
    pub part_number: u32,
    /// ETag (MD5 or custom hash)
    pub etag: String,
    /// Size in bytes
    pub size: u64,
    /// Path to temporary file storing the part
    pub temp_path: PathBuf,
    /// Upload timestamp
    pub uploaded_at: u64,
}

/// State of a multipart upload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadState {
    /// Upload is active and accepting parts
    Active,
    /// Upload is being completed
    Completing,
    /// Upload has been completed
    Completed,
    /// Upload has been aborted
    Aborted,
}

/// Multipart upload tracking object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    /// Unique upload ID
    pub upload_id: String,
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Current state
    pub state: UploadState,
    /// Uploaded parts indexed by part number
    pub parts: HashMap<u32, UploadPart>,
    /// Upload initiated timestamp (unix seconds)
    pub initiated_at: u64,
    /// Directory for storing parts
    pub storage_path: PathBuf,
    /// Whether this upload uses write-back
    pub writeback_enabled: bool,
    /// Original request headers to preserve
    pub request_headers: HashMap<String, String>,
    /// Content type (if specified)
    pub content_type: Option<String>,
}

impl MultipartUpload {
    /// Create a new multipart upload
    pub fn new(
        upload_id: String,
        bucket: String,
        key: String,
        storage_path: PathBuf,
        writeback_enabled: bool,
    ) -> Self {
        let initiated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            upload_id,
            bucket,
            key,
            state: UploadState::Active,
            parts: HashMap::new(),
            initiated_at,
            storage_path,
            writeback_enabled,
            request_headers: HashMap::new(),
            content_type: None,
        }
    }

    /// Check if the upload has expired
    pub fn is_expired(&self, expiration_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.initiated_at + expiration_secs
    }

    /// Add or update a part
    pub fn add_part(&mut self, part: UploadPart) {
        self.parts.insert(part.part_number, part);
    }

    /// Get all parts sorted by part number
    pub fn get_sorted_parts(&self) -> Vec<&UploadPart> {
        let mut parts: Vec<_> = self.parts.values().collect();
        parts.sort_by_key(|p| p.part_number);
        parts
    }

    /// Calculate total size of all parts
    pub fn total_size(&self) -> u64 {
        self.parts.values().map(|p| p.size).sum()
    }

    /// Get the number of uploaded parts
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }
}

/// Manager for tracking all active multipart uploads
pub struct MultipartUploadManager {
    /// Active uploads indexed by upload_id
    uploads: DashMap<String, MultipartUpload>,
    /// Base path for storing upload parts
    base_path: PathBuf,
    /// Upload expiration time in seconds
    expiration_secs: u64,
}

impl MultipartUploadManager {
    /// Create a new multipart upload manager
    pub fn new(base_path: &str) -> Self {
        let path = PathBuf::from(base_path).join("multipart");

        // Create base directory if needed
        if let Err(e) = std::fs::create_dir_all(&path) {
            warn!("Failed to create multipart upload directory: {}", e);
        }

        Self {
            uploads: DashMap::new(),
            base_path: path,
            expiration_secs: DEFAULT_UPLOAD_EXPIRATION_SECS,
        }
    }

    /// Generate a unique upload ID
    pub fn generate_upload_id(&self, bucket: &str, key: &str) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}:{}", bucket, key, timestamp));
        let result = hasher.finalize();

        // Use first 32 chars of hex for a readable ID
        hex::encode(&result[..16])
    }

    /// Initiate a new multipart upload
    pub async fn initiate_upload(
        &self,
        bucket: &str,
        key: &str,
        writeback_enabled: bool,
        headers: HashMap<String, String>,
    ) -> Result<MultipartUpload, MultipartError> {
        let upload_id = self.generate_upload_id(bucket, key);

        // Create storage directory for this upload
        let storage_path = self.base_path.join(&upload_id);
        fs::create_dir_all(&storage_path)
            .await
            .map_err(|e| MultipartError::StorageError(e.to_string()))?;

        let mut upload = MultipartUpload::new(
            upload_id.clone(),
            bucket.to_string(),
            key.to_string(),
            storage_path.clone(),
            writeback_enabled,
        );
        upload.request_headers = headers.clone();
        upload.content_type = headers.get("content-type").cloned();

        // Save metadata
        self.save_upload_metadata(&upload).await?;

        // Store in memory
        self.uploads.insert(upload_id.clone(), upload.clone());

        info!(
            "Initiated multipart upload {} for {}/{}",
            upload_id, bucket, key
        );

        Ok(upload)
    }

    /// Upload a part
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<UploadPart, MultipartError> {
        // Validate part number
        if part_number == 0 || part_number > MAX_PARTS {
            return Err(MultipartError::InvalidPartNumber(part_number));
        }

        // Get the upload
        let mut upload = self
            .uploads
            .get_mut(upload_id)
            .ok_or_else(|| MultipartError::UploadNotFound(upload_id.to_string()))?;

        if upload.state != UploadState::Active {
            return Err(MultipartError::UploadNotActive(upload_id.to_string()));
        }

        // Calculate ETag (using SHA256 for consistency)
        let mut hasher = Sha256::new();
        hasher.update(&body);
        let hash = hasher.finalize();
        let etag = format!("\"{}\"", hex::encode(&hash[..16]));

        // Store part to disk
        let part_path = upload.storage_path.join(format!("part_{:05}", part_number));
        fs::write(&part_path, &body)
            .await
            .map_err(|e| MultipartError::StorageError(e.to_string()))?;

        let part = UploadPart {
            part_number,
            etag: etag.clone(),
            size: body.len() as u64,
            temp_path: part_path,
            uploaded_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        upload.add_part(part.clone());

        // Update metadata on disk
        self.save_upload_metadata(&upload).await?;

        debug!(
            "Uploaded part {} for upload {} (size: {} bytes)",
            part_number,
            upload_id,
            body.len()
        );

        Ok(part)
    }

    /// Complete a multipart upload by assembling all parts
    pub async fn complete_upload(
        &self,
        upload_id: &str,
        part_list: Vec<(u32, String)>, // (part_number, etag)
    ) -> Result<(Bytes, HashMap<String, String>), MultipartError> {
        // Get and validate the upload
        let mut upload = self
            .uploads
            .get_mut(upload_id)
            .ok_or_else(|| MultipartError::UploadNotFound(upload_id.to_string()))?;

        if upload.state != UploadState::Active {
            return Err(MultipartError::UploadNotActive(upload_id.to_string()));
        }

        // Mark as completing
        upload.state = UploadState::Completing;

        // Validate all parts are present and ETags match
        for (part_num, etag) in &part_list {
            let part = upload
                .parts
                .get(part_num)
                .ok_or_else(|| MultipartError::PartNotFound(*part_num))?;

            // Compare ETags (with or without quotes)
            let expected = etag.trim_matches('"');
            let actual = part.etag.trim_matches('"');
            if expected != actual {
                return Err(MultipartError::ETagMismatch {
                    part_number: *part_num,
                    expected: etag.clone(),
                    actual: part.etag.clone(),
                });
            }
        }

        // Assemble parts in order
        let mut assembled = Vec::new();
        let sorted_parts = upload.get_sorted_parts();

        for part in sorted_parts {
            let data = fs::read(&part.temp_path)
                .await
                .map_err(|e| MultipartError::StorageError(e.to_string()))?;
            assembled.extend(data);
        }

        let final_body = Bytes::from(assembled);
        let headers = upload.request_headers.clone();

        // Mark as completed
        upload.state = UploadState::Completed;

        // Clone data before dropping the reference
        let upload_bucket = upload.bucket.clone();
        let upload_key = upload.key.clone();
        let storage_path = upload.storage_path.clone();

        drop(upload); // Release the lock

        info!(
            "Completed multipart upload {} for {}/{} (final size: {} bytes)",
            upload_id,
            upload_bucket,
            upload_key,
            final_body.len()
        );

        // Clean up storage asynchronously
        let upload_id_clone = upload_id.to_string();
        let self_uploads = self.uploads.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Err(e) = fs::remove_dir_all(&storage_path).await {
                warn!("Failed to cleanup multipart storage: {}", e);
            }
            self_uploads.remove(&upload_id_clone);
        });

        Ok((final_body, headers))
    }

    /// Abort a multipart upload
    pub async fn abort_upload(&self, upload_id: &str) -> Result<(), MultipartError> {
        let upload = self
            .uploads
            .get(upload_id)
            .ok_or_else(|| MultipartError::UploadNotFound(upload_id.to_string()))?;

        let storage_path = upload.storage_path.clone();
        let bucket = upload.bucket.clone();
        let key = upload.key.clone();

        drop(upload);

        // Remove from tracking
        self.uploads.remove(upload_id);

        // Clean up storage
        if let Err(e) = fs::remove_dir_all(&storage_path).await {
            warn!("Failed to cleanup aborted upload storage: {}", e);
        }

        info!("Aborted multipart upload {} for {}/{}", upload_id, bucket, key);

        Ok(())
    }

    /// List parts for an upload
    pub fn list_parts(&self, upload_id: &str) -> Result<Vec<UploadPart>, MultipartError> {
        let upload = self
            .uploads
            .get(upload_id)
            .ok_or_else(|| MultipartError::UploadNotFound(upload_id.to_string()))?;

        Ok(upload.get_sorted_parts().into_iter().cloned().collect())
    }

    /// Get upload info
    pub fn get_upload(&self, upload_id: &str) -> Option<MultipartUpload> {
        self.uploads.get(upload_id).map(|u| u.clone())
    }

    /// List all active uploads for a bucket
    pub fn list_uploads(&self, bucket: &str) -> Vec<MultipartUpload> {
        self.uploads
            .iter()
            .filter(|entry| entry.bucket == bucket && entry.state == UploadState::Active)
            .map(|entry| entry.clone())
            .collect()
    }

    /// Remove expired uploads
    pub async fn remove_expired(&self) -> u64 {
        let mut removed = 0u64;
        let mut expired_ids = Vec::new();

        for entry in self.uploads.iter() {
            if entry.is_expired(self.expiration_secs) {
                expired_ids.push(entry.upload_id.clone());
            }
        }

        for upload_id in expired_ids {
            if let Ok(()) = self.abort_upload(&upload_id).await {
                removed += 1;
            }
        }

        if removed > 0 {
            info!("Removed {} expired multipart uploads", removed);
        }

        removed
    }

    /// Save upload metadata to disk
    async fn save_upload_metadata(&self, upload: &MultipartUpload) -> Result<(), MultipartError> {
        let meta_path = upload.storage_path.join("metadata.json");
        let json = serde_json::to_string_pretty(upload)
            .map_err(|e| MultipartError::StorageError(e.to_string()))?;

        fs::write(&meta_path, json)
            .await
            .map_err(|e| MultipartError::StorageError(e.to_string()))?;

        Ok(())
    }

    /// Load uploads from disk on startup
    pub async fn load_from_disk(&self) -> Result<u64, MultipartError> {
        let mut loaded = 0u64;

        let mut entries = match fs::read_dir(&self.base_path).await {
            Ok(e) => e,
            Err(_) => return Ok(0), // Directory doesn't exist yet
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                let meta_path = path.join("metadata.json");
                if meta_path.exists() {
                    match fs::read_to_string(&meta_path).await {
                        Ok(content) => {
                            if let Ok(upload) = serde_json::from_str::<MultipartUpload>(&content) {
                                if upload.state == UploadState::Active
                                    && !upload.is_expired(self.expiration_secs)
                                {
                                    self.uploads.insert(upload.upload_id.clone(), upload);
                                    loaded += 1;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load upload metadata from {:?}: {}", meta_path, e);
                        }
                    }
                }
            }
        }

        if loaded > 0 {
            info!("Loaded {} multipart uploads from disk", loaded);
        }

        Ok(loaded)
    }

    /// Get statistics
    pub fn stats(&self) -> MultipartStats {
        let mut active = 0;
        let mut total_parts = 0;
        let mut total_bytes = 0u64;

        for entry in self.uploads.iter() {
            if entry.state == UploadState::Active {
                active += 1;
                total_parts += entry.parts.len();
                total_bytes += entry.total_size();
            }
        }

        MultipartStats {
            active_uploads: active,
            total_parts,
            total_bytes,
        }
    }
}

/// Statistics for multipart uploads
#[derive(Debug, Clone)]
pub struct MultipartStats {
    pub active_uploads: usize,
    pub total_parts: usize,
    pub total_bytes: u64,
}

/// Errors that can occur during multipart operations
#[derive(Debug, Clone)]
pub enum MultipartError {
    UploadNotFound(String),
    UploadNotActive(String),
    InvalidPartNumber(u32),
    PartNotFound(u32),
    ETagMismatch {
        part_number: u32,
        expected: String,
        actual: String,
    },
    StorageError(String),
    InvalidRequest(String),
}

impl std::fmt::Display for MultipartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultipartError::UploadNotFound(id) => write!(f, "Upload not found: {}", id),
            MultipartError::UploadNotActive(id) => write!(f, "Upload not active: {}", id),
            MultipartError::InvalidPartNumber(n) => write!(f, "Invalid part number: {}", n),
            MultipartError::PartNotFound(n) => write!(f, "Part not found: {}", n),
            MultipartError::ETagMismatch {
                part_number,
                expected,
                actual,
            } => write!(
                f,
                "ETag mismatch for part {}: expected {}, got {}",
                part_number, expected, actual
            ),
            MultipartError::StorageError(e) => write!(f, "Storage error: {}", e),
            MultipartError::InvalidRequest(e) => write!(f, "Invalid request: {}", e),
        }
    }
}

impl std::error::Error for MultipartError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_initiate_upload() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        assert_eq!(upload.bucket, "test-bucket");
        assert_eq!(upload.key, "test-key");
        assert_eq!(upload.state, UploadState::Active);
        assert!(upload.parts.is_empty());
    }

    #[tokio::test]
    async fn test_upload_part() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        let body = Bytes::from(vec![0u8; 1024]);
        let part = manager
            .upload_part(&upload.upload_id, 1, body.clone())
            .await
            .unwrap();

        assert_eq!(part.part_number, 1);
        assert_eq!(part.size, 1024);
        assert!(!part.etag.is_empty());
    }

    #[tokio::test]
    async fn test_complete_upload() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        // Upload two parts
        let body1 = Bytes::from(b"Hello, ".to_vec());
        let part1 = manager
            .upload_part(&upload.upload_id, 1, body1)
            .await
            .unwrap();

        let body2 = Bytes::from(b"World!".to_vec());
        let part2 = manager
            .upload_part(&upload.upload_id, 2, body2)
            .await
            .unwrap();

        // Complete with part list
        let part_list = vec![
            (1, part1.etag.clone()),
            (2, part2.etag.clone()),
        ];

        let (final_body, _headers) = manager
            .complete_upload(&upload.upload_id, part_list)
            .await
            .unwrap();

        assert_eq!(final_body.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_abort_upload() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        manager.abort_upload(&upload.upload_id).await.unwrap();

        // Should not be found after abort
        assert!(manager.get_upload(&upload.upload_id).is_none());
    }

    #[tokio::test]
    async fn test_list_parts() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        // Upload parts out of order
        manager
            .upload_part(&upload.upload_id, 3, Bytes::from("part3"))
            .await
            .unwrap();
        manager
            .upload_part(&upload.upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        manager
            .upload_part(&upload.upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        let parts = manager.list_parts(&upload.upload_id).unwrap();

        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 3);
    }

    #[tokio::test]
    async fn test_invalid_part_number() {
        let temp_dir = tempdir().unwrap();
        let manager = MultipartUploadManager::new(temp_dir.path().to_str().unwrap());

        let upload = manager
            .initiate_upload("test-bucket", "test-key", false, HashMap::new())
            .await
            .unwrap();

        // Part number 0 is invalid
        let result = manager
            .upload_part(&upload.upload_id, 0, Bytes::from("data"))
            .await;
        assert!(matches!(result, Err(MultipartError::InvalidPartNumber(0))));

        // Part number > 10000 is invalid
        let result = manager
            .upload_part(&upload.upload_id, 10001, Bytes::from("data"))
            .await;
        assert!(matches!(result, Err(MultipartError::InvalidPartNumber(10001))));
    }

    #[test]
    fn test_upload_expiration() {
        let mut upload = MultipartUpload::new(
            "test-id".to_string(),
            "bucket".to_string(),
            "key".to_string(),
            PathBuf::from("/tmp"),
            false,
        );

        // New upload should not be expired
        assert!(!upload.is_expired(3600));

        // Manually set old timestamp
        upload.initiated_at = 0;
        assert!(upload.is_expired(3600));
    }
}
