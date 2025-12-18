//! Proxy module - handles S3 request proxying

mod s3_proxy;

pub use s3_proxy::*;

use anyhow::Result;
use crate::config::Config;

/// Run the proxy server
pub async fn run(config: &Config) -> Result<()> {
    let proxy = S3Proxy::new(config.clone());
    proxy.run().await
}
