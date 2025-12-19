//! Write-back queue for deferred S3 uploads
//!
//! This module implements a write-back queue that allows PUT requests to be acknowledged
//! immediately while the actual upload to S3 happens asynchronously in the background.
//!
//! Key features:
//! - Immediate response to clients (low latency)
//! - Background upload to S3 with retries
//! - Configurable commit delay and retry count
//! - Trait-based S3 operations for testability

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::cache::CacheKey;

// ============================================================================
// S3 Operations Trait
// ============================================================================

/// Result of an S3 put operation
#[derive(Debug, Clone)]
pub struct PutObjectOutput {
    pub etag: Option<String>,
}

/// Error from S3 operations
#[derive(Debug, Clone)]
pub struct S3OperationError {
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for S3OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for S3OperationError {}

/// Trait for S3 operations - allows mocking for tests
#[async_trait]
pub trait S3Operations: Send + Sync {
    /// Upload an object to S3
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<&str>,
    ) -> Result<PutObjectOutput, S3OperationError>;
}

// ============================================================================
// Real S3 Client
// ============================================================================

/// Real S3 client using aws-sdk-s3
pub struct RealS3Client {
    client: aws_sdk_s3::Client,
}

impl RealS3Client {
    /// Create a new client from AWS config
    pub async fn new() -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self {
            client: aws_sdk_s3::Client::new(&config),
        }
    }

    /// Create from existing SDK client
    pub fn from_client(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl S3Operations for RealS3Client {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<&str>,
    ) -> Result<PutObjectOutput, S3OperationError> {
        let mut request = self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into());

        if let Some(ct) = content_type {
            request = request.content_type(ct);
        }

        match request.send().await {
            Ok(output) => Ok(PutObjectOutput {
                etag: output.e_tag().map(|s| s.to_string()),
            }),
            Err(e) => {
                let message = format!("{}", e);
                // Most S3 errors are retryable (network, throttling, etc.)
                let retryable = !message.contains("AccessDenied")
                    && !message.contains("InvalidBucket");
                Err(S3OperationError { message, retryable })
            }
        }
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the write-back queue
#[derive(Debug, Clone)]
pub struct WriteBackConfig {
    /// Maximum number of concurrent uploads
    pub max_concurrent_uploads: usize,
    /// Delay before committing to S3 (allows for overwrites)
    pub commit_delay: Duration,
    /// Maximum retry attempts before giving up
    pub max_retries: u32,
    /// Interval between queue processing runs
    pub process_interval: Duration,
    /// Maximum queue size (items)
    pub max_queue_size: usize,
}

impl Default for WriteBackConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: 64,
            commit_delay: Duration::from_secs(60),
            max_retries: 3,
            process_interval: Duration::from_secs(10),
            max_queue_size: 10000,
        }
    }
}

// ============================================================================
// Write-back Item
// ============================================================================

/// State of a write-back item
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteBackState {
    /// Pending commit (waiting for delay)
    Pending,
    /// Currently being uploaded
    InProgress,
    /// Successfully committed
    Committed,
    /// Failed after max retries
    Failed,
}

/// A single item in the write-back queue
#[derive(Debug, Clone)]
pub struct WriteBackItem {
    /// Cache key for the object
    pub cache_key: CacheKey,
    /// S3 bucket name
    pub bucket: String,
    /// S3 object key
    pub key: String,
    /// Object body
    pub body: Bytes,
    /// HTTP headers to include
    pub headers: HashMap<String, String>,
    /// Content type
    pub content_type: Option<String>,
    /// Time when item was queued
    pub queued_at: Instant,
    /// Time when item should be committed
    pub commit_after: Instant,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Current state
    pub state: WriteBackState,
    /// Last error message (if any)
    pub last_error: Option<String>,
}

impl WriteBackItem {
    pub fn new(
        cache_key: CacheKey,
        bucket: String,
        key: String,
        body: Bytes,
        headers: HashMap<String, String>,
        commit_delay: Duration,
    ) -> Self {
        let now = Instant::now();
        let content_type = headers.get("content-type").cloned();
        Self {
            cache_key,
            bucket,
            key,
            body,
            headers,
            content_type,
            queued_at: now,
            commit_after: now + commit_delay,
            retry_count: 0,
            state: WriteBackState::Pending,
            last_error: None,
        }
    }

    /// Check if this item is ready to be committed
    pub fn is_ready(&self) -> bool {
        self.state == WriteBackState::Pending && Instant::now() >= self.commit_after
    }

    /// Mark item as in-progress
    pub fn mark_in_progress(&mut self) {
        self.state = WriteBackState::InProgress;
    }

    /// Mark item as committed
    pub fn mark_committed(&mut self) {
        self.state = WriteBackState::Committed;
    }

    /// Mark item as failed with retry
    pub fn mark_failed(&mut self, error: String, max_retries: u32) {
        self.retry_count += 1;
        self.last_error = Some(error);
        if self.retry_count >= max_retries {
            self.state = WriteBackState::Failed;
        } else {
            // Reset to pending for retry with exponential backoff
            self.state = WriteBackState::Pending;
            let backoff = Duration::from_secs(2u64.pow(self.retry_count));
            self.commit_after = Instant::now() + backoff;
        }
    }
}

// ============================================================================
// Write-back Queue
// ============================================================================

/// Statistics for the write-back queue
#[derive(Debug, Clone, Default)]
pub struct WriteBackStats {
    pub pending_count: u64,
    pub in_progress_count: u64,
    pub committed_count: u64,
    pub failed_count: u64,
    pub total_bytes_pending: u64,
}

/// The write-back queue manager
pub struct WriteBackQueue {
    config: WriteBackConfig,
    /// Items indexed by cache key hash
    items: DashMap<String, WriteBackItem>,
    /// Channel to signal new items
    notify_tx: mpsc::Sender<()>,
    /// Statistics
    stats: Arc<RwLock<WriteBackStats>>,
    /// Counter for committed items
    committed_total: AtomicU64,
    /// Counter for failed items
    failed_total: AtomicU64,
    /// S3 endpoint for uploads
    s3_endpoint: String,
    /// AWS region
    aws_region: String,
}

impl WriteBackQueue {
    /// Create a new write-back queue
    pub fn new(config: WriteBackConfig, s3_endpoint: String, aws_region: String) -> (Self, mpsc::Receiver<()>) {
        let (notify_tx, notify_rx) = mpsc::channel(100);

        let queue = Self {
            config,
            items: DashMap::new(),
            notify_tx,
            stats: Arc::new(RwLock::new(WriteBackStats::default())),
            committed_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
            s3_endpoint,
            aws_region,
        };

        (queue, notify_rx)
    }

    /// Enqueue a new item for write-back
    pub async fn enqueue(
        &self,
        cache_key: CacheKey,
        bucket: String,
        key: String,
        body: Bytes,
        headers: HashMap<String, String>,
    ) -> Result<(), WriteBackError> {
        // Check queue size limit
        if self.items.len() >= self.config.max_queue_size {
            return Err(WriteBackError::QueueFull);
        }

        let item = WriteBackItem::new(
            cache_key.clone(),
            bucket.clone(),
            key.clone(),
            body.clone(),
            headers,
            self.config.commit_delay,
        );

        let key_hash = cache_key.hash().to_string();
        let body_len = body.len() as u64;

        // Insert or update (overwrites reset the timer)
        self.items.insert(key_hash.clone(), item);

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.pending_count = self.items.len() as u64;
            stats.total_bytes_pending += body_len;
        }

        // Notify processor
        let _ = self.notify_tx.try_send(());

        info!(
            bucket = %bucket,
            key = %key,
            size = body_len,
            "Enqueued for write-back"
        );

        Ok(())
    }

    /// Get an item by cache key
    pub fn get(&self, cache_key: &CacheKey) -> Option<WriteBackItem> {
        let key_hash = cache_key.hash().to_string();
        self.items.get(&key_hash).map(|r| r.clone())
    }

    /// Remove an item from the queue
    pub fn remove(&self, cache_key: &CacheKey) -> Option<WriteBackItem> {
        let key_hash = cache_key.hash().to_string();
        self.items.remove(&key_hash).map(|(_, v)| v)
    }

    /// Get queue statistics
    pub async fn stats(&self) -> WriteBackStats {
        let mut stats = self.stats.read().await.clone();
        stats.pending_count = self.items.iter()
            .filter(|r| r.state == WriteBackState::Pending)
            .count() as u64;
        stats.in_progress_count = self.items.iter()
            .filter(|r| r.state == WriteBackState::InProgress)
            .count() as u64;
        stats.committed_count = self.committed_total.load(Ordering::Relaxed);
        stats.failed_count = self.failed_total.load(Ordering::Relaxed);
        stats
    }

    /// Get items ready for commit
    pub fn get_ready_items(&self, limit: usize) -> Vec<WriteBackItem> {
        self.items
            .iter()
            .filter(|r| r.is_ready())
            .take(limit)
            .map(|r| r.clone())
            .collect()
    }

    /// Mark an item as in-progress
    pub fn mark_in_progress(&self, cache_key: &CacheKey) {
        let key_hash = cache_key.hash().to_string();
        if let Some(mut item) = self.items.get_mut(&key_hash) {
            item.mark_in_progress();
        }
    }

    /// Mark an item as committed and remove it
    pub fn mark_committed(&self, cache_key: &CacheKey) {
        let key_hash = cache_key.hash().to_string();
        if let Some((_, _)) = self.items.remove(&key_hash) {
            self.committed_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark an item as failed
    pub fn mark_failed(&self, cache_key: &CacheKey, error: String) {
        let key_hash = cache_key.hash().to_string();
        if let Some(mut item) = self.items.get_mut(&key_hash) {
            item.mark_failed(error, self.config.max_retries);
            if item.state == WriteBackState::Failed {
                self.failed_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get the S3 endpoint
    pub fn s3_endpoint(&self) -> &str {
        &self.s3_endpoint
    }

    /// Get the AWS region
    pub fn aws_region(&self) -> &str {
        &self.aws_region
    }

    /// Get the config
    pub fn config(&self) -> &WriteBackConfig {
        &self.config
    }

    /// Get count of pending items
    pub fn pending_count(&self) -> usize {
        self.items.iter()
            .filter(|r| r.state == WriteBackState::Pending)
            .count()
    }
}

/// Errors that can occur in the write-back queue
#[derive(Debug, Clone)]
pub enum WriteBackError {
    QueueFull,
    S3Error(String),
    NetworkError(String),
}

impl std::fmt::Display for WriteBackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteBackError::QueueFull => write!(f, "Write-back queue is full"),
            WriteBackError::S3Error(e) => write!(f, "S3 error: {}", e),
            WriteBackError::NetworkError(e) => write!(f, "Network error: {}", e),
        }
    }
}

impl std::error::Error for WriteBackError {}

// ============================================================================
// Write-back Processor
// ============================================================================

/// Background processor for the write-back queue
pub struct WriteBackProcessor<S: S3Operations> {
    queue: Arc<WriteBackQueue>,
    s3_client: Arc<S>,
}

impl WriteBackProcessor<RealS3Client> {
    /// Create a new processor with real S3 client (initialized lazily)
    pub fn new(queue: Arc<WriteBackQueue>) -> WriteBackProcessorBuilder {
        WriteBackProcessorBuilder { queue }
    }
}

/// Builder for WriteBackProcessor - handles async S3 client initialization
pub struct WriteBackProcessorBuilder {
    queue: Arc<WriteBackQueue>,
}

impl WriteBackProcessorBuilder {
    /// Build with real S3 client
    pub async fn build(self) -> WriteBackProcessor<RealS3Client> {
        let s3_client = Arc::new(RealS3Client::new().await);
        WriteBackProcessor {
            queue: self.queue,
            s3_client,
        }
    }

    /// Build with custom S3 client (for testing)
    pub fn build_with_client<S: S3Operations>(self, client: S) -> WriteBackProcessor<S> {
        WriteBackProcessor {
            queue: self.queue,
            s3_client: Arc::new(client),
        }
    }
}

impl<S: S3Operations + 'static> WriteBackProcessor<S> {
    /// Run the background processor
    pub async fn run(self, mut notify_rx: mpsc::Receiver<()>) {
        info!("Starting write-back processor");

        let process_interval = self.queue.config().process_interval;

        loop {
            // Wait for notification or timeout
            tokio::select! {
                _ = notify_rx.recv() => {
                    debug!("Write-back processor notified");
                }
                _ = tokio::time::sleep(process_interval) => {
                    debug!("Write-back processor interval tick");
                }
            }

            // Process ready items
            self.process_ready_items().await;
        }
    }

    /// Process items that are ready for commit (public for testing)
    pub async fn process_ready_items(&self) {
        let max_concurrent = self.queue.config().max_concurrent_uploads;
        let ready_items = self.queue.get_ready_items(max_concurrent);

        if ready_items.is_empty() {
            return;
        }

        info!("Processing {} write-back items", ready_items.len());

        // Process items concurrently
        let mut handles = Vec::new();
        for item in ready_items {
            let queue = Arc::clone(&self.queue);
            let s3_client = Arc::clone(&self.s3_client);

            let handle = tokio::spawn(async move {
                Self::commit_item(queue, s3_client, item).await;
            });
            handles.push(handle);
        }

        // Wait for all to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Commit a single item to S3
    async fn commit_item(
        queue: Arc<WriteBackQueue>,
        s3_client: Arc<S>,
        item: WriteBackItem,
    ) {
        let cache_key = item.cache_key.clone();

        // Mark as in-progress
        queue.mark_in_progress(&cache_key);

        debug!(
            bucket = %item.bucket,
            key = %item.key,
            retry = item.retry_count,
            "Committing write-back item"
        );

        // Upload to S3
        let result = s3_client
            .put_object(
                &item.bucket,
                &item.key,
                item.body.clone(),
                item.content_type.as_deref(),
            )
            .await;

        match result {
            Ok(_output) => {
                info!(
                    bucket = %item.bucket,
                    key = %item.key,
                    size = item.body.len(),
                    "Write-back committed successfully"
                );
                queue.mark_committed(&cache_key);
            }
            Err(e) => {
                warn!(
                    bucket = %item.bucket,
                    key = %item.key,
                    error = %e.message,
                    retry = item.retry_count,
                    "Write-back commit failed"
                );
                queue.mark_failed(&cache_key, e.message);
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // ========================================================================
    // Mock S3 Client
    // ========================================================================

    /// Mock S3 client for testing
    pub struct MockS3Client {
        /// Whether put_object should fail
        should_fail: bool,
        /// Error message to return on failure
        error_message: String,
        /// Whether the error is retryable
        retryable: bool,
        /// Count of put_object calls
        call_count: AtomicUsize,
        /// Delay before responding (simulates network latency)
        delay: Option<Duration>,
    }

    impl MockS3Client {
        /// Create a mock that succeeds
        pub fn success() -> Self {
            Self {
                should_fail: false,
                error_message: String::new(),
                retryable: false,
                call_count: AtomicUsize::new(0),
                delay: None,
            }
        }

        /// Create a mock that fails with a retryable error
        pub fn fail_retryable(message: &str) -> Self {
            Self {
                should_fail: true,
                error_message: message.to_string(),
                retryable: true,
                call_count: AtomicUsize::new(0),
                delay: None,
            }
        }

        /// Create a mock that fails with a non-retryable error
        pub fn fail_permanent(message: &str) -> Self {
            Self {
                should_fail: true,
                error_message: message.to_string(),
                retryable: false,
                call_count: AtomicUsize::new(0),
                delay: None,
            }
        }

        /// Add simulated network delay
        pub fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = Some(delay);
            self
        }

        /// Get the number of times put_object was called
        pub fn call_count(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl S3Operations for MockS3Client {
        async fn put_object(
            &self,
            _bucket: &str,
            _key: &str,
            _body: Bytes,
            _content_type: Option<&str>,
        ) -> Result<PutObjectOutput, S3OperationError> {
            self.call_count.fetch_add(1, Ordering::Relaxed);

            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }

            if self.should_fail {
                Err(S3OperationError {
                    message: self.error_message.clone(),
                    retryable: self.retryable,
                })
            } else {
                Ok(PutObjectOutput {
                    etag: Some("\"mock-etag-12345\"".to_string()),
                })
            }
        }
    }

    // ========================================================================
    // Queue Tests
    // ========================================================================

    #[tokio::test]
    async fn test_enqueue_and_get() {
        let config = WriteBackConfig::default();
        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );

        let cache_key = CacheKey::new("test-bucket", "test-key");
        let body = Bytes::from("test data");
        let headers = HashMap::new();

        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            body.clone(),
            headers,
        ).await.unwrap();

        let item = queue.get(&cache_key).unwrap();
        assert_eq!(item.bucket, "test-bucket");
        assert_eq!(item.key, "test-key");
        assert_eq!(item.body, body);
        assert_eq!(item.state, WriteBackState::Pending);
    }

    #[tokio::test]
    async fn test_overwrite_resets_timer() {
        let mut config = WriteBackConfig::default();
        config.commit_delay = Duration::from_secs(60);
        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );

        let cache_key = CacheKey::new("test-bucket", "test-key");
        let headers = HashMap::new();

        // First enqueue
        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("data1"),
            headers.clone(),
        ).await.unwrap();

        let item1 = queue.get(&cache_key).unwrap();
        let first_commit_time = item1.commit_after;

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Overwrite with new data
        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("data2"),
            headers,
        ).await.unwrap();

        let item2 = queue.get(&cache_key).unwrap();
        assert_eq!(item2.body, Bytes::from("data2"));
        // Timer should be reset (new commit time is later)
        assert!(item2.commit_after > first_commit_time);
    }

    #[tokio::test]
    async fn test_remove() {
        let config = WriteBackConfig::default();
        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );

        let cache_key = CacheKey::new("test-bucket", "test-key");

        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("test"),
            HashMap::new(),
        ).await.unwrap();

        assert!(queue.get(&cache_key).is_some());
        queue.remove(&cache_key);
        assert!(queue.get(&cache_key).is_none());
    }

    #[test]
    fn test_item_ready() {
        let cache_key = CacheKey::new("bucket", "key");
        let mut item = WriteBackItem::new(
            cache_key,
            "bucket".to_string(),
            "key".to_string(),
            Bytes::from("data"),
            HashMap::new(),
            Duration::from_millis(0), // No delay
        );

        // Should be ready immediately with 0 delay
        assert!(item.is_ready());

        // Mark in progress - should no longer be ready
        item.mark_in_progress();
        assert!(!item.is_ready());
    }

    #[test]
    fn test_retry_backoff() {
        let cache_key = CacheKey::new("bucket", "key");
        let mut item = WriteBackItem::new(
            cache_key,
            "bucket".to_string(),
            "key".to_string(),
            Bytes::from("data"),
            HashMap::new(),
            Duration::from_secs(0),
        );

        // First failure
        item.mark_failed("error 1".to_string(), 3);
        assert_eq!(item.retry_count, 1);
        assert_eq!(item.state, WriteBackState::Pending);

        // Second failure
        item.mark_failed("error 2".to_string(), 3);
        assert_eq!(item.retry_count, 2);
        assert_eq!(item.state, WriteBackState::Pending);

        // Third failure - should be marked as failed
        item.mark_failed("error 3".to_string(), 3);
        assert_eq!(item.retry_count, 3);
        assert_eq!(item.state, WriteBackState::Failed);
    }

    // ========================================================================
    // S3 Integration Tests (with Mock)
    // ========================================================================

    #[tokio::test]
    async fn test_processor_successful_upload() {
        let mut config = WriteBackConfig::default();
        config.commit_delay = Duration::from_millis(0); // No delay for testing
        config.max_retries = 3;

        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );
        let queue = Arc::new(queue);

        // Enqueue an item
        let cache_key = CacheKey::new("test-bucket", "test-key");
        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("hello world"),
            HashMap::new(),
        ).await.unwrap();

        // Create processor with mock client
        let mock_client = MockS3Client::success();
        let processor = WriteBackProcessor::new(Arc::clone(&queue))
            .build_with_client(mock_client);

        // Process the queue
        processor.process_ready_items().await;

        // Item should be removed (committed)
        assert!(queue.get(&cache_key).is_none());

        // Stats should show 1 committed
        let stats = queue.stats().await;
        assert_eq!(stats.committed_count, 1);
        assert_eq!(stats.failed_count, 0);
    }

    #[tokio::test]
    async fn test_processor_failed_upload_retries() {
        let mut config = WriteBackConfig::default();
        config.commit_delay = Duration::from_millis(0);
        config.max_retries = 3;

        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );
        let queue = Arc::new(queue);

        // Enqueue an item
        let cache_key = CacheKey::new("test-bucket", "test-key");
        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("hello world"),
            HashMap::new(),
        ).await.unwrap();

        // Create processor with failing mock client
        let mock_client = MockS3Client::fail_retryable("Connection timeout");
        let processor = WriteBackProcessor::new(Arc::clone(&queue))
            .build_with_client(mock_client);

        // Process the queue - first attempt
        processor.process_ready_items().await;

        // Item should still exist with retry_count = 1
        let item = queue.get(&cache_key).unwrap();
        assert_eq!(item.retry_count, 1);
        assert_eq!(item.state, WriteBackState::Pending); // Back to pending for retry
        assert_eq!(item.last_error, Some("Connection timeout".to_string()));
    }

    #[tokio::test]
    async fn test_processor_max_retries_exceeded() {
        let mut config = WriteBackConfig::default();
        config.commit_delay = Duration::from_millis(0);
        config.max_retries = 2;

        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );
        let queue = Arc::new(queue);

        // Enqueue an item
        let cache_key = CacheKey::new("test-bucket", "test-key");
        queue.enqueue(
            cache_key.clone(),
            "test-bucket".to_string(),
            "test-key".to_string(),
            Bytes::from("hello world"),
            HashMap::new(),
        ).await.unwrap();

        // Create processor with failing mock client
        let mock_client = MockS3Client::fail_retryable("Service unavailable");
        let processor = WriteBackProcessor::new(Arc::clone(&queue))
            .build_with_client(mock_client);

        // Simulate multiple retry attempts by setting retry count manually
        // (In real scenario, backoff timer would need to expire)
        {
            let key_hash = cache_key.hash().to_string();
            if let Some(mut item) = queue.items.get_mut(&key_hash) {
                item.retry_count = 1; // Already retried once
                item.commit_after = Instant::now(); // Make it ready
            }
        }

        // Process - this should be the final retry
        processor.process_ready_items().await;

        // Item should be marked as failed
        let item = queue.get(&cache_key).unwrap();
        assert_eq!(item.state, WriteBackState::Failed);
        assert_eq!(item.retry_count, 2);

        // Stats should show failure
        let stats = queue.stats().await;
        assert_eq!(stats.failed_count, 1);
    }

    #[tokio::test]
    async fn test_processor_multiple_items() {
        let mut config = WriteBackConfig::default();
        config.commit_delay = Duration::from_millis(0);
        config.max_concurrent_uploads = 10;

        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );
        let queue = Arc::new(queue);

        // Enqueue multiple items
        for i in 0..5 {
            let cache_key = CacheKey::new("test-bucket", &format!("key-{}", i));
            queue.enqueue(
                cache_key,
                "test-bucket".to_string(),
                format!("key-{}", i),
                Bytes::from(format!("data-{}", i)),
                HashMap::new(),
            ).await.unwrap();
        }

        assert_eq!(queue.pending_count(), 5);

        // Create processor with mock client
        let mock_client = MockS3Client::success();
        let processor = WriteBackProcessor::new(Arc::clone(&queue))
            .build_with_client(mock_client);

        // Process all items
        processor.process_ready_items().await;

        // All items should be committed
        assert_eq!(queue.pending_count(), 0);
        let stats = queue.stats().await;
        assert_eq!(stats.committed_count, 5);
    }

    #[tokio::test]
    async fn test_mock_client_call_count() {
        let mock_client = MockS3Client::success();

        // Make some calls
        mock_client.put_object("bucket", "key1", Bytes::from("data1"), None).await.unwrap();
        mock_client.put_object("bucket", "key2", Bytes::from("data2"), Some("text/plain")).await.unwrap();
        mock_client.put_object("bucket", "key3", Bytes::from("data3"), None).await.unwrap();

        assert_eq!(mock_client.call_count(), 3);
    }

    #[tokio::test]
    async fn test_queue_full_error() {
        let mut config = WriteBackConfig::default();
        config.max_queue_size = 2;

        let (queue, _rx) = WriteBackQueue::new(
            config,
            "https://s3.amazonaws.com".to_string(),
            "us-east-1".to_string(),
        );

        // Fill the queue
        queue.enqueue(
            CacheKey::new("bucket", "key1"),
            "bucket".to_string(),
            "key1".to_string(),
            Bytes::from("data1"),
            HashMap::new(),
        ).await.unwrap();

        queue.enqueue(
            CacheKey::new("bucket", "key2"),
            "bucket".to_string(),
            "key2".to_string(),
            Bytes::from("data2"),
            HashMap::new(),
        ).await.unwrap();

        // Third should fail
        let result = queue.enqueue(
            CacheKey::new("bucket", "key3"),
            "bucket".to_string(),
            "key3".to_string(),
            Bytes::from("data3"),
            HashMap::new(),
        ).await;

        assert!(matches!(result, Err(WriteBackError::QueueFull)));
    }
}
