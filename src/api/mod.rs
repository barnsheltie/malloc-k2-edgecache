//! REST API module for management and monitoring

mod handlers;
mod routes;

use crate::cache::CacheManager;
use crate::config::Config;
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
}

/// Run the REST API server
pub async fn run(config: &Config) -> Result<()> {
    let cache = Arc::new(CacheManager::new(&config.cache));
    let state = Arc::new(AppState {
        config: Arc::new(RwLock::new(config.clone())),
        cache,
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
        .merge(routes::swagger_routes())
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
