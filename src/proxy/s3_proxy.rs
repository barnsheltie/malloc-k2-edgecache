//! S3 Proxy implementation using Pingora

use async_trait::async_trait;
use pingora_core::prelude::*;
use pingora_core::services::background::background_service;
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};
use std::sync::Arc;
use tracing::{debug, info};

use crate::cache::{CacheKey, CacheManager};
use crate::config::Config;

/// S3 Proxy server
pub struct S3Proxy {
    config: Config,
    cache: Arc<CacheManager>,
}

impl S3Proxy {
    pub fn new(config: Config) -> Self {
        let cache = Arc::new(CacheManager::new(&config.cache));
        Self { config, cache }
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
}

/// S3 Proxy request handler
pub struct S3ProxyHandler {
    config: Config,
    cache: Arc<CacheManager>,
}

/// Context for each request
pub struct S3ProxyCtx {
    bucket: Option<String>,
    key: Option<String>,
    cache_key: Option<CacheKey>,
    upstream_addr: String,
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
        // Check if this is a cacheable GET request
        let method = session.req_header().method.as_str();

        if method == "GET" {
            if let Some(ref cache_key) = ctx.cache_key {
                // Try to serve from cache
                if let Some(_cached) = self.cache.get(cache_key).await {
                    debug!("Cache hit for {:?}", cache_key);

                    // TODO: Send cached response
                    // For now, just continue to upstream
                    return Ok(false);
                }
            }
        }

        // Continue processing
        Ok(false)
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Cache successful GET responses
        if let Some(ref cache_key) = ctx.cache_key {
            let status = upstream_response.status.as_u16();
            if status == 200 {
                debug!("Caching response for {:?}", cache_key);
                // TODO: Implement response body caching
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_from_host() {
        let config = Config::default();
        let cache = Arc::new(CacheManager::new(&config.cache));
        let handler = S3ProxyHandler { config, cache };

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
        let config = Config::default();
        let cache = Arc::new(CacheManager::new(&config.cache));
        let handler = S3ProxyHandler { config, cache };

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
