//! API route definitions

use super::handlers;
use super::AppState;
use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

/// Health check routes
pub fn health_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/health", get(handlers::health_check))
        .route("/api/version", get(handlers::get_version))
        .route("/api/ready", get(handlers::readiness_check))
}

/// Configuration routes
pub fn config_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/config", get(handlers::get_config))
        .route("/api/config/logging", get(handlers::get_logging))
        .route("/api/config/logging/:level", put(handlers::set_logging))
}

/// Cache management routes
pub fn cache_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/cache/stats", get(handlers::get_cache_stats))
        .route("/api/cache/clear", post(handlers::clear_cache))
        .route("/api/cache/objects", get(handlers::list_cached_objects))
}

/// Bucket management routes
pub fn bucket_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/buckets", get(handlers::list_buckets))
        .route("/api/buckets", post(handlers::register_bucket))
        .route("/api/buckets/:name", get(handlers::get_bucket))
        .route("/api/buckets/:name", delete(handlers::delete_bucket))
        .route("/api/buckets/:name/objects", get(handlers::list_bucket_objects))
        .route(
            "/api/buckets/:name/objects/*key",
            delete(handlers::delete_cached_object),
        )
}

/// Swagger UI routes
pub fn swagger_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/swagger-ui", get(handlers::swagger_ui))
        .route("/api/openapi.json", get(handlers::openapi_spec))
}
