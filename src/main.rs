//! malloc-k2-edgecache - High-performance S3 caching proxy
//!
//! Built on Cloudflare Pingora for maximum performance and reliability.

use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod api;
mod auth;
mod cache;
mod config;
mod metrics;
mod proxy;

use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    init_logging();

    info!("Starting malloc-k2-edgecache v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "config/default.toml".to_string());

    let config = Config::load(&config_path)?;
    info!("Loaded configuration from {}", config_path);

    // Initialize metrics
    metrics::init();

    // Start the proxy server
    info!(
        "Starting proxy on HTTP:{} HTTPS:{} API:{}",
        config.server.http_port,
        config.server.https_port,
        config.server.api_port
    );

    // Run the proxy and API server
    tokio::select! {
        result = proxy::run(&config) => {
            if let Err(e) = result {
                tracing::error!("Proxy server error: {}", e);
            }
        }
        result = api::run(&config) => {
            if let Err(e) = result {
                tracing::error!("API server error: {}", e);
            }
        }
    }

    Ok(())
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(filter)
        .init();
}
