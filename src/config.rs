//! Configuration management for malloc-k2-edgecache

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Node types in the hybrid architecture
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Cache,
    Storage,
}

impl Default for NodeType {
    fn default() -> Self {
        NodeType::Cache
    }
}

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub cache: CacheConfig,
    pub logging: LoggingConfig,
    pub s3: S3Config,
    #[serde(default)]
    pub writeback: WriteBackConfig,
    #[serde(default)]
    pub federator: FederatorConfig,
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

    /// Expiration scan interval in seconds (0 to disable background expiration)
    #[serde(default = "default_expiration_interval")]
    pub expiration_interval_seconds: u64,
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

/// Write-back queue configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WriteBackConfig {
    /// Enable write-back queue
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum concurrent uploads to S3
    #[serde(default = "default_wb_max_concurrent")]
    pub max_concurrent_uploads: usize,

    /// Delay before committing to S3 (seconds)
    #[serde(default = "default_wb_commit_delay")]
    pub commit_delay_seconds: u64,

    /// Maximum retry attempts
    #[serde(default = "default_wb_max_retries")]
    pub max_retries: u32,

    /// Queue processing interval (seconds)
    #[serde(default = "default_wb_process_interval")]
    pub process_interval_seconds: u64,

    /// Maximum queue size
    #[serde(default = "default_wb_max_queue_size")]
    pub max_queue_size: usize,
}

impl Default for WriteBackConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_uploads: default_wb_max_concurrent(),
            commit_delay_seconds: default_wb_commit_delay(),
            max_retries: default_wb_max_retries(),
            process_interval_seconds: default_wb_process_interval(),
            max_queue_size: default_wb_max_queue_size(),
        }
    }
}

/// Federator configuration for inter-node communication
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FederatorConfig {
    /// Enable federator connection
    #[serde(default)]
    pub enabled: bool,

    /// Federator WebSocket URL (wss://...)
    #[serde(default = "default_federator_url")]
    pub url: String,

    /// Unique node identifier (auto-generated if not set)
    pub node_id: Option<String>,

    /// Cluster identifier
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,

    /// Node type: cache or storage
    #[serde(default)]
    pub node_type: NodeType,

    /// HTTP endpoint for this node (required for storage nodes, used for proxying)
    pub endpoint: Option<String>,

    /// JWT token for authentication with federator
    pub jwt_token: Option<String>,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_seconds: u64,

    /// Reconnection delay in seconds
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,

    /// Buckets to register with federator (defaults to all configured buckets)
    pub buckets: Option<Vec<String>>,
}

impl Default for FederatorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: default_federator_url(),
            node_id: None,
            cluster_id: default_cluster_id(),
            node_type: NodeType::Cache,
            endpoint: None,
            jwt_token: None,
            heartbeat_interval_seconds: default_heartbeat_interval(),
            reconnect_delay_seconds: default_reconnect_delay(),
            buckets: None,
        }
    }
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

    /// Write-back enabled for this bucket (defaults to global setting)
    pub writeback_enabled: Option<bool>,
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
                writeback_enabled: None,
            })
    }

    /// Check if write-back is enabled for a bucket
    pub fn is_writeback_enabled(&self, bucket_name: &str) -> bool {
        let bucket_config = self.get_bucket_config(bucket_name);
        bucket_config.writeback_enabled.unwrap_or(self.writeback.enabled)
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
fn default_expiration_interval() -> u64 { 300 } // 5 minutes
fn default_wb_max_concurrent() -> usize { 64 }
fn default_wb_commit_delay() -> u64 { 60 }
fn default_wb_max_retries() -> u32 { 3 }
fn default_wb_process_interval() -> u64 { 10 }
fn default_wb_max_queue_size() -> usize { 10000 }
fn default_federator_url() -> String { "wss://edgecache-federator.workers.dev/ws".to_string() }
fn default_cluster_id() -> String { "default".to_string() }
fn default_heartbeat_interval() -> u64 { 30 }
fn default_reconnect_delay() -> u64 { 5 }

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
                expiration_interval_seconds: default_expiration_interval(),
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
            writeback: WriteBackConfig::default(),
            federator: FederatorConfig::default(),
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
