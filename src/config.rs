//! Configuration management for malloc-k2-edgecache

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub cache: CacheConfig,
    pub logging: LoggingConfig,
    pub s3: S3Config,
    #[serde(default)]
    pub buckets: Vec<BucketConfig>,
}

/// Server configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// HTTP proxy port
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// HTTPS proxy port
    #[serde(default = "default_https_port")]
    pub https_port: u16,

    /// REST API port
    #[serde(default = "default_api_port")]
    pub api_port: u16,

    /// Number of worker threads
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// TLS certificate path (optional)
    pub tls_cert: Option<String>,

    /// TLS key path (optional)
    pub tls_key: Option<String>,
}

/// Cache configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    /// Enable caching
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Disk cache path
    #[serde(default = "default_disk_path")]
    pub disk_path: String,

    /// Maximum disk cache size in GB
    #[serde(default = "default_disk_max_size")]
    pub disk_max_size_gb: u64,

    /// Maximum memory cache size in MB
    #[serde(default = "default_memory_max_size")]
    pub memory_max_size_mb: u64,

    /// Default TTL in seconds
    #[serde(default = "default_ttl")]
    pub default_ttl_seconds: u64,
}

/// Logging configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (json, text)
    #[serde(default = "default_log_format")]
    pub format: String,

    /// Enable syslog output
    #[serde(default)]
    pub syslog: bool,
}

/// S3 configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Config {
    /// Default S3 endpoint
    #[serde(default = "default_s3_endpoint")]
    pub endpoint: String,

    /// Default AWS region
    #[serde(default = "default_region")]
    pub region: String,

    /// Access key (can also use AWS_ACCESS_KEY_ID env var)
    pub access_key: Option<String>,

    /// Secret key (can also use AWS_SECRET_ACCESS_KEY env var)
    pub secret_key: Option<String>,
}

/// Per-bucket configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BucketConfig {
    /// Bucket name
    pub name: String,

    /// Custom endpoint for this bucket (optional)
    pub endpoint: Option<String>,

    /// Custom region for this bucket (optional)
    pub region: Option<String>,

    /// Cache TTL override for this bucket
    pub cache_ttl: Option<u64>,

    /// Access key override
    pub access_key: Option<String>,

    /// Secret key override
    pub secret_key: Option<String>,
}

impl Config {
    /// Load configuration from a TOML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        Ok(config)
    }

    /// Get bucket-specific configuration, falling back to defaults
    pub fn get_bucket_config(&self, bucket_name: &str) -> BucketConfig {
        self.buckets
            .iter()
            .find(|b| b.name == bucket_name)
            .cloned()
            .unwrap_or_else(|| BucketConfig {
                name: bucket_name.to_string(),
                endpoint: None,
                region: None,
                cache_ttl: None,
                access_key: None,
                secret_key: None,
            })
    }
}

// Default value functions
fn default_http_port() -> u16 { 9000 }
fn default_https_port() -> u16 { 9001 }
fn default_api_port() -> u16 { 14000 }
fn default_workers() -> usize { 4 }
fn default_true() -> bool { true }
fn default_disk_path() -> String { "/var/cache/malloc-k2-edgecache".to_string() }
fn default_disk_max_size() -> u64 { 100 }
fn default_memory_max_size() -> u64 { 1024 }
fn default_ttl() -> u64 { 3600 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "json".to_string() }
fn default_s3_endpoint() -> String { "https://s3.amazonaws.com".to_string() }
fn default_region() -> String { "us-east-1".to_string() }

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                http_port: default_http_port(),
                https_port: default_https_port(),
                api_port: default_api_port(),
                workers: default_workers(),
                tls_cert: None,
                tls_key: None,
            },
            cache: CacheConfig {
                enabled: true,
                disk_path: default_disk_path(),
                disk_max_size_gb: default_disk_max_size(),
                memory_max_size_mb: default_memory_max_size(),
                default_ttl_seconds: default_ttl(),
            },
            logging: LoggingConfig {
                level: default_log_level(),
                format: default_log_format(),
                syslog: false,
            },
            s3: S3Config {
                endpoint: default_s3_endpoint(),
                region: default_region(),
                access_key: None,
                secret_key: None,
            },
            buckets: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.http_port, 9000);
        assert_eq!(config.server.https_port, 9001);
        assert_eq!(config.server.api_port, 14000);
        assert!(config.cache.enabled);
    }
}
