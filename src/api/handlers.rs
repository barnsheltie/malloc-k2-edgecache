//! API request handlers

use super::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Response types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub version: String,
    pub build_time: String,
    pub git_commit: String,
}

#[derive(Debug, Serialize)]
pub struct CacheStatsResponse {
    pub memory_hits: u64,
    pub memory_misses: u64,
    pub disk_hits: u64,
    pub disk_misses: u64,
    pub memory_size_bytes: u64,
    pub disk_size_bytes: u64,
    pub total_objects: u64,
    pub hit_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct BucketInfo {
    pub name: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub cache_ttl: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterBucketRequest {
    pub name: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub cache_ttl: Option<u64>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

// ============================================================================
// Health handlers
// ============================================================================

/// Health check endpoint
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Version information
pub async fn get_version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_time: option_env!("BUILD_TIME").unwrap_or("unknown").to_string(),
        git_commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
    })
}

/// Readiness check (for Kubernetes)
pub async fn readiness_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Check if cache is accessible
    let stats = state.cache.stats().await;
    Json(serde_json::json!({
        "ready": true,
        "cache_initialized": true,
        "total_cached_objects": stats.total_objects
    }))
}

// ============================================================================
// Config handlers
// ============================================================================

/// Get current configuration (sensitive data redacted)
pub async fn get_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.read().await;

    // Return config with secrets redacted
    Json(serde_json::json!({
        "server": {
            "http_port": config.server.http_port,
            "https_port": config.server.https_port,
            "api_port": config.server.api_port,
            "workers": config.server.workers
        },
        "cache": {
            "enabled": config.cache.enabled,
            "disk_path": config.cache.disk_path,
            "disk_max_size_gb": config.cache.disk_max_size_gb,
            "memory_max_size_mb": config.cache.memory_max_size_mb,
            "default_ttl_seconds": config.cache.default_ttl_seconds
        },
        "logging": {
            "level": config.logging.level,
            "format": config.logging.format
        },
        "s3": {
            "endpoint": config.s3.endpoint,
            "region": config.s3.region,
            "credentials_configured": config.s3.access_key.is_some()
        }
    }))
}

/// Get current logging level
pub async fn get_logging(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.read().await;
    Json(serde_json::json!({
        "level": config.logging.level,
        "format": config.logging.format
    }))
}

/// Set logging level
pub async fn set_logging(
    State(state): State<Arc<AppState>>,
    Path(level): Path<String>,
) -> impl IntoResponse {
    let valid_levels = ["trace", "debug", "info", "warn", "error"];

    if !valid_levels.contains(&level.to_lowercase().as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: "invalid_level".to_string(),
                message: format!("Valid levels: {:?}", valid_levels),
            }),
        )
            .into_response();
    }

    let mut config = state.config.write().await;
    config.logging.level = level.clone();

    Json(serde_json::json!({
        "status": "updated",
        "level": level
    }))
    .into_response()
}

// ============================================================================
// Cache handlers
// ============================================================================

/// Get cache statistics
pub async fn get_cache_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.cache.stats().await;

    let total_requests = stats.memory_hits + stats.memory_misses;
    let total_hits = stats.memory_hits + stats.disk_hits;
    let hit_rate = if total_requests > 0 {
        total_hits as f64 / total_requests as f64
    } else {
        0.0
    };

    Json(CacheStatsResponse {
        memory_hits: stats.memory_hits,
        memory_misses: stats.memory_misses,
        disk_hits: stats.disk_hits,
        disk_misses: stats.disk_misses,
        memory_size_bytes: stats.memory_size_bytes,
        disk_size_bytes: stats.disk_size_bytes,
        total_objects: stats.total_objects,
        hit_rate,
    })
}

/// Clear all caches
pub async fn clear_cache(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.cache.clear().await;
    Json(serde_json::json!({
        "status": "cleared",
        "message": "All caches cleared successfully"
    }))
}

/// List cached objects (paginated)
pub async fn list_cached_objects(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // TODO: Implement listing of cached objects
    Json(serde_json::json!({
        "objects": [],
        "total": 0,
        "message": "Listing not yet implemented"
    }))
}

// ============================================================================
// Bucket handlers
// ============================================================================

/// List registered buckets
pub async fn list_buckets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.read().await;
    let buckets: Vec<BucketInfo> = config
        .buckets
        .iter()
        .map(|b| BucketInfo {
            name: b.name.clone(),
            endpoint: b.endpoint.clone(),
            region: b.region.clone(),
            cache_ttl: b.cache_ttl,
        })
        .collect();

    Json(serde_json::json!({
        "buckets": buckets,
        "total": buckets.len()
    }))
}

/// Register a new bucket
pub async fn register_bucket(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterBucketRequest>,
) -> impl IntoResponse {
    let mut config = state.config.write().await;

    // Check if bucket already exists
    if config.buckets.iter().any(|b| b.name == req.name) {
        return (
            StatusCode::CONFLICT,
            Json(ApiError {
                error: "bucket_exists".to_string(),
                message: format!("Bucket '{}' is already registered", req.name),
            }),
        )
            .into_response();
    }

    // Add the bucket
    config.buckets.push(crate::config::BucketConfig {
        name: req.name.clone(),
        endpoint: req.endpoint,
        region: req.region,
        cache_ttl: req.cache_ttl,
        access_key: req.access_key,
        secret_key: req.secret_key,
    });

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "created",
            "bucket": req.name
        })),
    )
        .into_response()
}

/// Get bucket info
pub async fn get_bucket(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let config = state.config.read().await;

    if let Some(bucket) = config.buckets.iter().find(|b| b.name == name) {
        Json(serde_json::json!({
            "name": bucket.name,
            "endpoint": bucket.endpoint,
            "region": bucket.region,
            "cache_ttl": bucket.cache_ttl,
            "credentials_configured": bucket.access_key.is_some()
        }))
        .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: "bucket_not_found".to_string(),
                message: format!("Bucket '{}' is not registered", name),
            }),
        )
            .into_response()
    }
}

/// Delete a bucket registration
pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut config = state.config.write().await;

    let initial_len = config.buckets.len();
    config.buckets.retain(|b| b.name != name);

    if config.buckets.len() < initial_len {
        Json(serde_json::json!({
            "status": "deleted",
            "bucket": name
        }))
        .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: "bucket_not_found".to_string(),
                message: format!("Bucket '{}' is not registered", name),
            }),
        )
            .into_response()
    }
}

/// List objects in a bucket cache
pub async fn list_bucket_objects(
    State(_state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    // TODO: Implement bucket object listing
    Json(serde_json::json!({
        "bucket": name,
        "objects": [],
        "total": 0,
        "message": "Listing not yet implemented"
    }))
}

/// Delete a cached object
pub async fn delete_cached_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let cache_key = crate::cache::CacheKey::new(&bucket, &key);
    state.cache.remove(&cache_key).await;

    Json(serde_json::json!({
        "status": "deleted",
        "bucket": bucket,
        "key": key
    }))
}

// ============================================================================
// Swagger handlers
// ============================================================================

/// Swagger UI HTML page
pub async fn swagger_ui() -> impl IntoResponse {
    Html(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>malloc-k2-edgecache API</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        SwaggerUIBundle({
            url: '/api/openapi.json',
            dom_id: '#swagger-ui',
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIBundle.SwaggerUIStandalonePreset
            ],
            layout: 'StandaloneLayout'
        });
    </script>
</body>
</html>"#,
    )
}

/// OpenAPI specification
pub async fn openapi_spec() -> impl IntoResponse {
    Json(serde_json::json!({
        "openapi": "3.0.3",
        "info": {
            "title": "malloc-k2-edgecache API",
            "description": "High-performance S3 caching proxy API",
            "version": env!("CARGO_PKG_VERSION")
        },
        "servers": [
            {
                "url": "/api",
                "description": "API server"
            }
        ],
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {
                        "200": {
                            "description": "Service is healthy"
                        }
                    }
                }
            },
            "/version": {
                "get": {
                    "summary": "Get version information",
                    "responses": {
                        "200": {
                            "description": "Version information"
                        }
                    }
                }
            },
            "/config": {
                "get": {
                    "summary": "Get current configuration",
                    "responses": {
                        "200": {
                            "description": "Current configuration"
                        }
                    }
                }
            },
            "/cache/stats": {
                "get": {
                    "summary": "Get cache statistics",
                    "responses": {
                        "200": {
                            "description": "Cache statistics"
                        }
                    }
                }
            },
            "/cache/clear": {
                "post": {
                    "summary": "Clear all caches",
                    "responses": {
                        "200": {
                            "description": "Caches cleared"
                        }
                    }
                }
            },
            "/buckets": {
                "get": {
                    "summary": "List registered buckets",
                    "responses": {
                        "200": {
                            "description": "List of buckets"
                        }
                    }
                },
                "post": {
                    "summary": "Register a new bucket",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": { "type": "string" },
                                        "endpoint": { "type": "string" },
                                        "region": { "type": "string" },
                                        "cache_ttl": { "type": "integer" }
                                    },
                                    "required": ["name"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Bucket registered"
                        }
                    }
                }
            }
        }
    }))
}
