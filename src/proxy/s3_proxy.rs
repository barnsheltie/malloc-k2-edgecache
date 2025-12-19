//! S3 Proxy implementation using Pingora

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use pingora_core::prelude::*;
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::cache::{CacheKey, CacheManager};
use crate::config::Config;
use crate::multipart::{MultipartUploadManager, MultipartError};
use crate::writeback::{WriteBackConfig, WriteBackProcessor, WriteBackQueue};

/// S3 Proxy server
pub struct S3Proxy {
    config: Config,
    cache: Arc<CacheManager>,
    writeback_queue: Arc<WriteBackQueue>,
    multipart_manager: Arc<MultipartUploadManager>,
}

impl S3Proxy {
    pub fn new(config: Config) -> Self {
        let cache = Arc::new(CacheManager::new(&config.cache));

        // Start cache expiration background task if interval > 0
        if config.cache.expiration_interval_seconds > 0 {
            let expiration_interval = Duration::from_secs(config.cache.expiration_interval_seconds);
            Arc::clone(&cache).start_expiration_task(expiration_interval);
        }

        // Create write-back queue with config
        let wb_config = WriteBackConfig {
            max_concurrent_uploads: config.writeback.max_concurrent_uploads,
            commit_delay: Duration::from_secs(config.writeback.commit_delay_seconds),
            max_retries: config.writeback.max_retries,
            process_interval: Duration::from_secs(config.writeback.process_interval_seconds),
            max_queue_size: config.writeback.max_queue_size,
        };
        let (writeback_queue, notify_rx) = WriteBackQueue::new(
            wb_config,
            config.s3.endpoint.clone(),
            config.s3.region.clone(),
        );
        let writeback_queue = Arc::new(writeback_queue);

        // Start the write-back processor in the background
        if config.writeback.enabled {
            let queue_clone = Arc::clone(&writeback_queue);
            tokio::spawn(async move {
                let processor = WriteBackProcessor::new(queue_clone).build().await;
                processor.run(notify_rx).await;
            });
            info!("Write-back processor started");
        }

        // Create multipart upload manager
        let multipart_manager = Arc::new(MultipartUploadManager::new(&config.cache.disk_path));
        info!("Multipart upload manager initialized");

        Self { config, cache, writeback_queue, multipart_manager }
    }

    /// Run the proxy server
    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Starting S3 proxy server...");

        // Create Pingora server
        let mut server = Server::new(None)?;
        server.bootstrap();

        // Create proxy service
        let handler = S3ProxyHandler {
            config: self.config.clone(),
            cache: self.cache.clone(),
            writeback_queue: Arc::clone(&self.writeback_queue),
            multipart_manager: Arc::clone(&self.multipart_manager),
        };

        let http_addr = format!("0.0.0.0:{}", self.config.server.http_port);

        // Use http_proxy_service helper
        let mut proxy_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            handler,
        );
        proxy_service.add_tcp(&http_addr);

        server.add_service(proxy_service);

        info!("S3 proxy listening on {}", http_addr);

        // Run the server
        server.run_forever();
    }

    /// Get a reference to the write-back queue for API access
    pub fn writeback_queue(&self) -> &Arc<WriteBackQueue> {
        &self.writeback_queue
    }
}

/// S3 Proxy request handler
pub struct S3ProxyHandler {
    config: Config,
    cache: Arc<CacheManager>,
    writeback_queue: Arc<WriteBackQueue>,
    multipart_manager: Arc<MultipartUploadManager>,
}

/// Multipart operation type
#[derive(Debug, Clone, PartialEq)]
pub enum MultipartOp {
    /// POST /bucket/key?uploads - Initiate multipart upload
    Initiate,
    /// PUT /bucket/key?partNumber=N&uploadId=ID - Upload part
    UploadPart { part_number: u32, upload_id: String },
    /// POST /bucket/key?uploadId=ID - Complete multipart upload
    Complete { upload_id: String },
    /// DELETE /bucket/key?uploadId=ID - Abort multipart upload
    Abort { upload_id: String },
    /// GET /bucket/key?uploadId=ID - List parts
    ListParts { upload_id: String },
}

/// Context for each request
pub struct S3ProxyCtx {
    bucket: Option<String>,
    key: Option<String>,
    cache_key: Option<CacheKey>,
    upstream_addr: String,
    /// Whether this request is cacheable (GET request with valid cache key)
    is_cacheable: bool,
    /// Accumulate response body for caching
    response_body: BytesMut,
    /// Response headers to cache
    response_headers: HashMap<String, String>,
    /// Response status code
    response_status: u16,
    /// Whether this is a write-back PUT request
    is_writeback_put: bool,
    /// Accumulate request body for write-back
    request_body: BytesMut,
    /// Request headers for write-back
    request_headers: HashMap<String, String>,
    /// Multipart operation (if any)
    multipart_op: Option<MultipartOp>,
}

#[async_trait]
impl ProxyHttp for S3ProxyHandler {
    type CTX = S3ProxyCtx;

    fn new_ctx(&self) -> Self::CTX {
        S3ProxyCtx {
            bucket: None,
            key: None,
            cache_key: None,
            upstream_addr: self.config.s3.endpoint.clone(),
            is_cacheable: false,
            response_body: BytesMut::new(),
            response_headers: HashMap::new(),
            response_status: 0,
            is_writeback_put: false,
            request_body: BytesMut::new(),
            request_headers: HashMap::new(),
            multipart_op: None,
        }
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Parse the request to extract bucket and key
        let uri = session.req_header().uri.clone();
        let path = uri.path();

        // Extract bucket name from host or path
        if let Some(host) = session.req_header().headers.get("host") {
            if let Ok(host_str) = host.to_str() {
                ctx.bucket = self.extract_bucket_from_host(host_str);
            }
        }

        // If bucket not in host, try path-style
        if ctx.bucket.is_none() {
            let (bucket, key) = self.parse_path_style(path);
            ctx.bucket = bucket;
            ctx.key = key;
        } else {
            ctx.key = Some(path.trim_start_matches('/').to_string());
        }

        // Generate cache key
        if let (Some(bucket), Some(key)) = (&ctx.bucket, &ctx.key) {
            ctx.cache_key = Some(CacheKey::new(bucket, key));
        }

        // Determine upstream address
        let upstream = if let Some(ref bucket) = ctx.bucket {
            let bucket_config = self.config.get_bucket_config(bucket);
            bucket_config
                .endpoint
                .unwrap_or_else(|| self.config.s3.endpoint.clone())
        } else {
            self.config.s3.endpoint.clone()
        };

        ctx.upstream_addr = upstream.clone();

        // Parse upstream URL
        let upstream_url = url::Url::parse(&upstream)
            .map_err(|e| Error::because(ErrorType::InternalError, "Invalid upstream URL", e))?;
        let host = upstream_url.host_str().unwrap_or("s3.amazonaws.com");
        let port = upstream_url.port().unwrap_or(443);
        let tls = upstream_url.scheme() == "https";

        debug!("Proxying to upstream: {}:{} (TLS: {})", host, port, tls);

        let peer = HttpPeer::new((host, port), tls, host.to_string());
        Ok(Box::new(peer))
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<bool> {
        let method = session.req_header().method.as_str();
        let uri = session.req_header().uri.clone();
        let query = uri.query().unwrap_or("");

        // Detect multipart operations from query string
        ctx.multipart_op = self.parse_multipart_op(method, query);

        // Handle multipart operations locally
        if let Some(mp_op) = ctx.multipart_op.clone() {
            if let (Some(bucket), Some(key)) = (ctx.bucket.clone(), ctx.key.clone()) {
                return self.handle_multipart_request(session, ctx, &bucket, &key, mp_op).await;
            }
        }

        // Handle GET requests - serve from cache if available
        if method == "GET" {
            ctx.is_cacheable = ctx.cache_key.is_some();

            if let Some(ref cache_key) = ctx.cache_key {
                // Try to serve from cache
                if let Some(cached) = self.cache.get(cache_key).await {
                    debug!("Cache hit for {:?}", cache_key);

                    // Build response header
                    let mut resp = ResponseHeader::build(200, None)?;

                    // Add cached headers
                    if let Some(ref content_type) = cached.content_type {
                        resp.insert_header("content-type", content_type)?;
                    }
                    resp.insert_header("content-length", cached.content_length.to_string())?;
                    if let Some(ref etag) = cached.etag {
                        resp.insert_header("etag", etag)?;
                    }
                    if let Some(ref last_modified) = cached.last_modified {
                        resp.insert_header("last-modified", last_modified)?;
                    }
                    resp.insert_header("x-cache", "HIT")?;

                    // Copy other cached headers
                    for (key, value) in cached.headers.iter() {
                        let key_lower = key.to_lowercase();
                        if !matches!(key_lower.as_str(), "content-type" | "content-length" | "etag" | "last-modified") {
                            if let Err(e) = resp.insert_header(key.clone(), value.clone()) {
                                debug!("Failed to insert header {}: {}", key, e);
                            }
                        }
                    }

                    // Send response header
                    session.write_response_header(Box::new(resp), false).await?;

                    // Send response body
                    session.write_response_body(Some(cached.body.clone()), true).await?;

                    info!("Served from cache: {:?} ({} bytes)", cache_key, cached.content_length);
                    return Ok(true);
                }
            }
        }

        // Handle PUT requests - check if write-back is enabled
        if method == "PUT" {
            if let (Some(ref bucket), Some(ref _key)) = (&ctx.bucket, &ctx.key) {
                // Check if write-back is enabled for this bucket
                if self.config.is_writeback_enabled(bucket) && ctx.cache_key.is_some() {
                    ctx.is_writeback_put = true;

                    // Collect request headers for write-back
                    for (name, value) in session.req_header().headers.iter() {
                        if let Ok(v) = value.to_str() {
                            ctx.request_headers.insert(name.to_string(), v.to_string());
                        }
                    }

                    debug!(
                        bucket = %bucket,
                        key = ?ctx.key,
                        "PUT request marked for write-back"
                    );
                }
            }
        }

        // Continue processing - go to upstream
        Ok(false)
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Handle multipart UploadPart operation
        if let Some(MultipartOp::UploadPart { part_number, ref upload_id }) = ctx.multipart_op {
            // Accumulate request body
            if let Some(ref chunk) = body {
                ctx.request_body.extend_from_slice(chunk);
            }

            if end_of_stream {
                let body_bytes = ctx.request_body.clone().freeze();

                match self
                    .multipart_manager
                    .upload_part(upload_id, part_number, body_bytes)
                    .await
                {
                    Ok(part) => {
                        let mut resp = ResponseHeader::build(200, None)?;
                        resp.insert_header("etag", part.etag.clone())?;
                        resp.insert_header("x-multipart", "part-uploaded")?;
                        session.write_response_header(Box::new(resp), true).await?;

                        info!(
                            upload_id = %upload_id,
                            part_number = part_number,
                            size = part.size,
                            "Uploaded multipart part"
                        );

                        *body = None;
                    }
                    Err(e) => {
                        warn!("Failed to upload multipart part: {}", e);
                        let _ = self.send_multipart_error(session, &e).await;
                        *body = None;
                    }
                }
            }
            return Ok(());
        }

        // Handle multipart Complete operation
        if let Some(MultipartOp::Complete { ref upload_id }) = ctx.multipart_op {
            // Accumulate request body (contains part list XML)
            if let Some(ref chunk) = body {
                ctx.request_body.extend_from_slice(chunk);
            }

            if end_of_stream {
                let body_str = String::from_utf8_lossy(&ctx.request_body);

                // Parse the part list from XML (simple regex-based parsing)
                let part_list = self.parse_complete_multipart_request(&body_str);

                match self
                    .multipart_manager
                    .complete_upload(upload_id, part_list)
                    .await
                {
                    Ok((final_body, headers)) => {
                        // Store the completed object in cache
                        if let (Some(ref bucket), Some(ref key)) = (&ctx.bucket, &ctx.key) {
                            let cache_key = CacheKey::new(bucket, key);
                            self.cache.put(cache_key.clone(), final_body.clone(), headers.clone()).await;

                            // If write-back is enabled, queue for S3
                            if self.config.is_writeback_enabled(bucket) {
                                if let Err(e) = self.writeback_queue.enqueue(
                                    cache_key.clone(),
                                    bucket.clone(),
                                    key.clone(),
                                    final_body.clone(),
                                    headers.clone(),
                                ).await {
                                    warn!("Failed to queue completed multipart for write-back: {}", e);
                                }
                            }

                            // Calculate ETag (simplified - real S3 uses a different algorithm)
                            use sha2::{Sha256, Digest};
                            let mut hasher = Sha256::new();
                            hasher.update(&final_body);
                            let hash = hasher.finalize();
                            let etag = format!("\"{}\"", hex::encode(&hash[..16]));

                            let xml = format!(
                                r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <ETag>{}</ETag>
</CompleteMultipartUploadResult>"#,
                                bucket, key, etag
                            );

                            let resp_body = Bytes::from(xml);
                            let mut resp = ResponseHeader::build(200, None)?;
                            resp.insert_header("content-type", "application/xml")?;
                            resp.insert_header("content-length", resp_body.len().to_string())?;
                            resp.insert_header("etag", etag.clone())?;
                            resp.insert_header("x-multipart", "completed")?;

                            session.write_response_header(Box::new(resp), false).await?;
                            session.write_response_body(Some(resp_body), true).await?;

                            info!(
                                bucket = %bucket,
                                key = %key,
                                upload_id = %upload_id,
                                final_size = final_body.len(),
                                "Completed multipart upload"
                            );
                        }
                        *body = None;
                    }
                    Err(e) => {
                        warn!("Failed to complete multipart upload: {}", e);
                        let _ = self.send_multipart_error(session, &e).await;
                        *body = None;
                    }
                }
            }
            return Ok(());
        }

        // Handle write-back PUT requests (non-multipart)
        if !ctx.is_writeback_put {
            return Ok(());
        }

        // Accumulate request body
        if let Some(ref chunk) = body {
            ctx.request_body.extend_from_slice(chunk);
        }

        // When stream ends, queue for write-back and send success response
        if end_of_stream {
            if let (Some(ref bucket), Some(ref key), Some(ref cache_key)) =
                (&ctx.bucket, &ctx.key, &ctx.cache_key)
            {
                let body_bytes = ctx.request_body.clone().freeze();
                let body_len = body_bytes.len();

                // Store in local cache immediately
                self.cache.put(
                    cache_key.clone(),
                    body_bytes.clone(),
                    ctx.request_headers.clone(),
                ).await;

                // Queue for write-back to S3
                if let Err(e) = self.writeback_queue.enqueue(
                    cache_key.clone(),
                    bucket.clone(),
                    key.clone(),
                    body_bytes,
                    ctx.request_headers.clone(),
                ).await {
                    warn!(
                        bucket = %bucket,
                        key = %key,
                        error = %e,
                        "Failed to queue write-back, proceeding to upstream"
                    );
                    // Don't short-circuit - let it go to upstream
                    return Ok(());
                }

                // Generate ETag (simple MD5 for compatibility)
                let etag = format!("\"{}\"", cache_key.hash());

                // Send success response to client
                let mut resp = ResponseHeader::build(200, None)?;
                resp.insert_header("etag", etag.clone())?;
                resp.insert_header("x-amz-request-id", "writeback-queued")?;
                resp.insert_header("x-writeback", "queued")?;

                session.write_response_header(Box::new(resp), true).await?;

                info!(
                    bucket = %bucket,
                    key = %key,
                    size = body_len,
                    etag = %etag,
                    "PUT queued for write-back"
                );

                // Clear body to prevent it from being sent upstream
                *body = None;
            }
        }

        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.response_status = upstream_response.status.as_u16();

        // Only cache successful GET responses
        if ctx.is_cacheable && ctx.response_status == 200 {
            debug!("Will cache response for {:?}", ctx.cache_key);

            // Extract headers for caching
            for (name, value) in upstream_response.headers.iter() {
                if let Ok(v) = value.to_str() {
                    ctx.response_headers.insert(name.to_string(), v.to_string());
                }
            }
        }

        // Add cache status header
        upstream_response.insert_header("x-cache", "MISS")?;

        Ok(())
    }

    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Only collect body if this is a cacheable request with 200 status
        if ctx.is_cacheable && ctx.response_status == 200 {
            // Accumulate body chunks
            if let Some(ref chunk) = body {
                ctx.response_body.extend_from_slice(chunk);
            }

            // When stream ends, store in cache
            if end_of_stream {
                if let Some(ref cache_key) = ctx.cache_key {
                    let body_bytes = ctx.response_body.clone().freeze();
                    let body_len = body_bytes.len();
                    let cache_key = cache_key.clone();
                    let headers = ctx.response_headers.clone();
                    let cache = self.cache.clone();

                    // Spawn async task to store in cache
                    tokio::spawn(async move {
                        cache.put(cache_key.clone(), body_bytes, headers).await;
                        info!("Cached response: {:?} ({} bytes)", cache_key, body_len);
                    });
                }
            }
        }

        Ok(())
    }

    async fn logging(
        &self,
        session: &mut Session,
        _e: Option<&pingora_core::Error>,
        ctx: &mut Self::CTX,
    ) {
        let method = session.req_header().method.as_str();
        let uri = session.req_header().uri.to_string();
        let status = session
            .response_written()
            .map(|r| r.status.as_u16())
            .unwrap_or(0);

        info!(
            method = %method,
            uri = %uri,
            status = %status,
            bucket = ?ctx.bucket,
            key = ?ctx.key,
            "Request completed"
        );
    }
}

impl S3ProxyHandler {
    /// Extract bucket name from virtual-hosted style hostname
    /// e.g., bucket.s3.amazonaws.com -> Some("bucket")
    fn extract_bucket_from_host(&self, host: &str) -> Option<String> {
        let host = host.split(':').next().unwrap_or(host);

        // Check for virtual-hosted style
        if host.ends_with(".s3.amazonaws.com") {
            let bucket = host.trim_end_matches(".s3.amazonaws.com");
            if !bucket.is_empty() && bucket != "s3" {
                return Some(bucket.to_string());
            }
        }

        // Check for regional virtual-hosted style
        // e.g., bucket.s3.us-west-2.amazonaws.com
        if host.contains(".s3.") && host.ends_with(".amazonaws.com") {
            let parts: Vec<&str> = host.split(".s3.").collect();
            if parts.len() == 2 && !parts[0].is_empty() {
                return Some(parts[0].to_string());
            }
        }

        None
    }

    /// Parse path-style S3 URL
    /// e.g., /bucket/key -> (Some("bucket"), Some("key"))
    fn parse_path_style(&self, path: &str) -> (Option<String>, Option<String>) {
        let path = path.trim_start_matches('/');
        let parts: Vec<&str> = path.splitn(2, '/').collect();

        match parts.len() {
            0 => (None, None),
            1 => {
                if parts[0].is_empty() {
                    (None, None)
                } else {
                    (Some(parts[0].to_string()), None)
                }
            }
            _ => (
                Some(parts[0].to_string()),
                Some(parts[1].to_string()),
            ),
        }
    }

    /// Parse multipart operation from HTTP method and query string
    fn parse_multipart_op(&self, method: &str, query: &str) -> Option<MultipartOp> {
        let params: HashMap<&str, &str> = query
            .split('&')
            .filter_map(|part| {
                let mut kv = part.splitn(2, '=');
                Some((kv.next()?, kv.next().unwrap_or("")))
            })
            .collect();

        match method {
            "POST" => {
                // POST ?uploads - Initiate multipart upload
                if query == "uploads" || params.contains_key("uploads") {
                    return Some(MultipartOp::Initiate);
                }
                // POST ?uploadId=ID - Complete multipart upload
                if let Some(upload_id) = params.get("uploadId") {
                    return Some(MultipartOp::Complete {
                        upload_id: upload_id.to_string(),
                    });
                }
            }
            "PUT" => {
                // PUT ?partNumber=N&uploadId=ID - Upload part
                if let (Some(part_num_str), Some(upload_id)) =
                    (params.get("partNumber"), params.get("uploadId"))
                {
                    if let Ok(part_number) = part_num_str.parse::<u32>() {
                        return Some(MultipartOp::UploadPart {
                            part_number,
                            upload_id: upload_id.to_string(),
                        });
                    }
                }
            }
            "DELETE" => {
                // DELETE ?uploadId=ID - Abort multipart upload
                if let Some(upload_id) = params.get("uploadId") {
                    return Some(MultipartOp::Abort {
                        upload_id: upload_id.to_string(),
                    });
                }
            }
            "GET" => {
                // GET ?uploadId=ID - List parts
                if let Some(upload_id) = params.get("uploadId") {
                    return Some(MultipartOp::ListParts {
                        upload_id: upload_id.to_string(),
                    });
                }
            }
            _ => {}
        }

        None
    }

    /// Handle multipart upload requests
    async fn handle_multipart_request(
        &self,
        session: &mut Session,
        ctx: &mut S3ProxyCtx,
        bucket: &str,
        key: &str,
        op: MultipartOp,
    ) -> Result<bool> {
        // Check if write-back (local handling) is enabled for this bucket
        let writeback_enabled = self.config.is_writeback_enabled(bucket);

        match op {
            MultipartOp::Initiate => {
                // Collect request headers
                let mut headers = HashMap::new();
                for (name, value) in session.req_header().headers.iter() {
                    if let Ok(v) = value.to_str() {
                        headers.insert(name.to_string(), v.to_string());
                    }
                }

                match self
                    .multipart_manager
                    .initiate_upload(bucket, key, writeback_enabled, headers)
                    .await
                {
                    Ok(upload) => {
                        let xml = format!(
                            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
                            bucket, key, upload.upload_id
                        );

                        let body = Bytes::from(xml);
                        let mut resp = ResponseHeader::build(200, None)?;
                        resp.insert_header("content-type", "application/xml")?;
                        resp.insert_header("content-length", body.len().to_string())?;
                        resp.insert_header("x-multipart", "initiated")?;

                        session.write_response_header(Box::new(resp), false).await?;
                        session.write_response_body(Some(body), true).await?;

                        info!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload.upload_id,
                            "Initiated multipart upload"
                        );
                        return Ok(true);
                    }
                    Err(e) => {
                        warn!("Failed to initiate multipart upload: {}", e);
                        return self.send_multipart_error(session, &e).await;
                    }
                }
            }

            MultipartOp::UploadPart {
                part_number,
                upload_id,
            } => {
                // For UploadPart, we need to collect the body first
                // Mark this for body collection and defer handling
                ctx.multipart_op = Some(MultipartOp::UploadPart {
                    part_number,
                    upload_id,
                });
                // Continue to request_body_filter to collect the body
                return Ok(false);
            }

            MultipartOp::Complete { upload_id } => {
                // For Complete, we need to parse the body for the part list
                // Mark this for body collection and defer handling
                ctx.multipart_op = Some(MultipartOp::Complete { upload_id });
                // Continue to request_body_filter to collect the body
                return Ok(false);
            }

            MultipartOp::Abort { upload_id } => {
                match self.multipart_manager.abort_upload(&upload_id).await {
                    Ok(()) => {
                        let mut resp = ResponseHeader::build(204, None)?;
                        resp.insert_header("x-multipart", "aborted")?;
                        session.write_response_header(Box::new(resp), true).await?;

                        info!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload_id,
                            "Aborted multipart upload"
                        );
                        return Ok(true);
                    }
                    Err(e) => {
                        warn!("Failed to abort multipart upload: {}", e);
                        return self.send_multipart_error(session, &e).await;
                    }
                }
            }

            MultipartOp::ListParts { upload_id } => {
                match self.multipart_manager.list_parts(&upload_id) {
                    Ok(parts) => {
                        let parts_xml: String = parts
                            .iter()
                            .map(|p| {
                                format!(
                                    "    <Part>\n        <PartNumber>{}</PartNumber>\n        <ETag>{}</ETag>\n        <Size>{}</Size>\n    </Part>",
                                    p.part_number, p.etag, p.size
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n");

                        let xml = format!(
                            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <UploadId>{}</UploadId>
    <PartNumberMarker>0</PartNumberMarker>
    <IsTruncated>false</IsTruncated>
{}
</ListPartsResult>"#,
                            bucket, key, upload_id, parts_xml
                        );

                        let body = Bytes::from(xml);
                        let mut resp = ResponseHeader::build(200, None)?;
                        resp.insert_header("content-type", "application/xml")?;
                        resp.insert_header("content-length", body.len().to_string())?;

                        session.write_response_header(Box::new(resp), false).await?;
                        session.write_response_body(Some(body), true).await?;

                        info!(
                            bucket = %bucket,
                            key = %key,
                            upload_id = %upload_id,
                            parts_count = parts.len(),
                            "Listed multipart parts"
                        );
                        return Ok(true);
                    }
                    Err(e) => {
                        warn!("Failed to list multipart parts: {}", e);
                        return self.send_multipart_error(session, &e).await;
                    }
                }
            }
        }
    }

    /// Parse CompleteMultipartUpload XML request body
    /// Returns a list of (part_number, etag) tuples
    fn parse_complete_multipart_request(&self, body: &str) -> Vec<(u32, String)> {
        let mut parts = Vec::new();

        // Simple parsing - look for <Part><PartNumber>N</PartNumber><ETag>...</ETag></Part>
        // In production, use a proper XML parser
        let mut current_part_num: Option<u32> = None;
        let mut current_etag: Option<String> = None;

        for line in body.lines() {
            let trimmed = line.trim();

            // Extract PartNumber
            if trimmed.starts_with("<PartNumber>") && trimmed.ends_with("</PartNumber>") {
                let num_str = trimmed
                    .trim_start_matches("<PartNumber>")
                    .trim_end_matches("</PartNumber>");
                current_part_num = num_str.parse().ok();
            }

            // Extract ETag
            if trimmed.starts_with("<ETag>") && trimmed.ends_with("</ETag>") {
                let etag = trimmed
                    .trim_start_matches("<ETag>")
                    .trim_end_matches("</ETag>");
                current_etag = Some(etag.to_string());
            }

            // If we have both, add to parts list
            if let (Some(part_num), Some(etag)) = (current_part_num, current_etag.take()) {
                parts.push((part_num, etag));
                current_part_num = None;
            }
        }

        // Sort by part number
        parts.sort_by_key(|(n, _)| *n);
        parts
    }

    /// Send an error response for multipart operations
    async fn send_multipart_error(
        &self,
        session: &mut Session,
        error: &MultipartError,
    ) -> Result<bool> {
        let (status, code, message) = match error {
            MultipartError::UploadNotFound(_) => (404, "NoSuchUpload", error.to_string()),
            MultipartError::UploadNotActive(_) => (400, "InvalidRequest", error.to_string()),
            MultipartError::InvalidPartNumber(_) => (400, "InvalidPart", error.to_string()),
            MultipartError::PartNotFound(_) => (400, "InvalidPart", error.to_string()),
            MultipartError::ETagMismatch { .. } => (400, "InvalidPart", error.to_string()),
            MultipartError::StorageError(_) => (500, "InternalError", error.to_string()),
            MultipartError::InvalidRequest(_) => (400, "InvalidRequest", error.to_string()),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
</Error>"#,
            code, message
        );

        let body = Bytes::from(xml);
        let mut resp = ResponseHeader::build(status, None)?;
        resp.insert_header("content-type", "application/xml")?;
        resp.insert_header("content-length", body.len().to_string())?;

        session.write_response_header(Box::new(resp), false).await?;
        session.write_response_body(Some(body), true).await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_handler() -> S3ProxyHandler {
        let config = Config::default();
        let cache = Arc::new(CacheManager::new(&config.cache));
        let wb_config = WriteBackConfig::default();
        let (writeback_queue, _rx) = WriteBackQueue::new(
            wb_config,
            config.s3.endpoint.clone(),
            config.s3.region.clone(),
        );
        let multipart_manager = Arc::new(MultipartUploadManager::new("/tmp/test-multipart"));
        S3ProxyHandler {
            config,
            cache,
            writeback_queue: Arc::new(writeback_queue),
            multipart_manager,
        }
    }

    #[test]
    fn test_extract_bucket_from_host() {
        let handler = create_test_handler();

        assert_eq!(
            handler.extract_bucket_from_host("mybucket.s3.amazonaws.com"),
            Some("mybucket".to_string())
        );

        assert_eq!(
            handler.extract_bucket_from_host("mybucket.s3.us-west-2.amazonaws.com"),
            Some("mybucket".to_string())
        );

        assert_eq!(
            handler.extract_bucket_from_host("s3.amazonaws.com"),
            None
        );
    }

    #[test]
    fn test_parse_path_style() {
        let handler = create_test_handler();

        assert_eq!(
            handler.parse_path_style("/bucket/path/to/object"),
            (Some("bucket".to_string()), Some("path/to/object".to_string()))
        );

        assert_eq!(
            handler.parse_path_style("/bucket"),
            (Some("bucket".to_string()), None)
        );

        assert_eq!(
            handler.parse_path_style("/"),
            (None, None)
        );
    }
}
