//! REST API module for management and monitoring

mod handlers;
mod routes;

use crate::cache::CacheManager;
use crate::config::Config;
use crate::federator::FederatorClient;
use crate::multipart::MultipartUploadManager;
use crate::writeback::WriteBackQueue;
use anyhow::Result;
use axum::Router;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

/// Shared application state
pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub cache: Arc<CacheManager>,
    pub writeback_queue: Option<Arc<WriteBackQueue>>,
    pub multipart_manager: Option<Arc<MultipartUploadManager>>,
    pub federator_client: Option<Arc<RwLock<FederatorClient>>>,
}

/// Run the REST API server
pub async fn run(config: &Config) -> Result<()> {
    run_with_components(config, None, None).await
}

/// Run the REST API server with optional write-back queue
pub async fn run_with_writeback(
    config: &Config,
    writeback_queue: Option<Arc<WriteBackQueue>>,
) -> Result<()> {
    run_with_components(config, writeback_queue, None).await
}

/// Run the REST API server with all optional components
pub async fn run_with_components(
    config: &Config,
    writeback_queue: Option<Arc<WriteBackQueue>>,
    multipart_manager: Option<Arc<MultipartUploadManager>>,
) -> Result<()> {
    run_with_all_components(config, writeback_queue, multipart_manager, None).await
}

/// Run the REST API server with all optional components including federator
pub async fn run_with_all_components(
    config: &Config,
    writeback_queue: Option<Arc<WriteBackQueue>>,
    multipart_manager: Option<Arc<MultipartUploadManager>>,
    federator_client: Option<Arc<RwLock<FederatorClient>>>,
) -> Result<()> {
    let cache = Arc::new(CacheManager::new(&config.cache));
    let state = Arc::new(AppState {
        config: Arc::new(RwLock::new(config.clone())),
        cache,
        writeback_queue,
        multipart_manager,
        federator_client,
    });

    // Build the router
    let app = create_router(state);

    // Start the server
    let addr = format!("0.0.0.0:{}", config.server.api_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("REST API server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

/// Create the API router with all routes
pub fn create_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .merge(routes::health_routes())
        .merge(routes::config_routes())
        .merge(routes::cache_routes())
        .merge(routes::bucket_routes())
        .merge(routes::writeback_routes())
        .merge(routes::multipart_routes())
        .merge(routes::federator_routes())
        .merge(routes::swagger_routes())
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
