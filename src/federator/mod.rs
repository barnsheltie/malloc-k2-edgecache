//! Federator client module for inter-node communication
//!
//! Provides WebSocket-based communication with the central federator
//! for cache invalidation, object routing, and cluster coordination.

mod connection;
mod handler;
mod health;
pub mod protocol;
pub mod route_cache;

use crate::cache::CacheManager;
use crate::config::FederatorConfig;
use crate::federator::connection::WebSocketConnection;
use crate::federator::handler::MessageHandler;
use crate::federator::health::{HeartbeatConfig, HeartbeatSender};
use crate::federator::protocol::{FederatorMessage, NodeMessage, NodeType};
use crate::federator::route_cache::{OwnerInfo, RouteCache};
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{error, info, warn};

/// State shared across the federator client components
struct FederatorState {
    cluster_id: Option<String>,
    registered: bool,
}

/// Client for communicating with the federator
pub struct FederatorClient {
    config: FederatorConfig,
    node_id: String,
    cache: Arc<CacheManager>,
    route_cache: Arc<RouteCache>,
    connection: Arc<RwLock<Option<WebSocketConnection>>>,
    state: Arc<RwLock<FederatorState>>,
    connected: Arc<AtomicBool>,
    outgoing_tx: Option<mpsc::Sender<NodeMessage>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl FederatorClient {
    /// Create a new federator client
    pub fn new(config: FederatorConfig, cache: Arc<CacheManager>) -> Self {
        let node_id = config
            .node_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let route_cache = Arc::new(RouteCache::new(300)); // 5 minute TTL

        Self {
            config,
            node_id,
            cache,
            route_cache,
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(FederatorState {
                cluster_id: None,
                registered: false,
            })),
            connected: Arc::new(AtomicBool::new(false)),
            outgoing_tx: None,
            shutdown_tx: None,
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Check if connected to the federator
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Get the route cache
    pub fn route_cache(&self) -> Arc<RouteCache> {
        self.route_cache.clone()
    }

    /// Connect to the federator and start background tasks
    pub async fn connect(&mut self) -> Result<()> {
        if !self.config.enabled {
            info!("Federator connection disabled");
            return Ok(());
        }

        info!("Connecting to federator at {}", self.config.url);

        // Create channels
        let (incoming_tx, incoming_rx) = mpsc::channel::<FederatorMessage>(100);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<NodeMessage>(100);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        self.outgoing_tx = Some(outgoing_tx.clone());
        self.shutdown_tx = Some(shutdown_tx);

        // Connect WebSocket
        let mut ws = WebSocketConnection::new(&self.config.url);
        ws.connect(incoming_tx.clone()).await?;

        self.connected.store(true, Ordering::SeqCst);
        *self.connection.write().await = Some(ws);

        // Send authentication
        let jwt = self.config.jwt_token.clone().unwrap_or_default();
        self.send(NodeMessage::Auth { jwt }).await?;

        // Start message handler
        let handler = MessageHandler::new(
            self.cache.clone(),
            self.route_cache.clone(),
            &self.node_id,
        );
        tokio::spawn(async move {
            handler.run(incoming_rx).await;
        });

        // Start heartbeat sender
        let heartbeat_config = HeartbeatConfig {
            interval: Duration::from_secs(self.config.heartbeat_interval_seconds),
            node_id: self.node_id.clone(),
        };
        let heartbeat = HeartbeatSender::new(heartbeat_config, self.cache.clone());
        let heartbeat_tx = outgoing_tx.clone();
        tokio::spawn(async move {
            heartbeat.run(heartbeat_tx).await;
        });

        // Start outgoing message forwarder
        let connection = self.connection.clone();
        let connected = self.connected.clone();
        tokio::spawn(async move {
            while let Some(msg) = outgoing_rx.recv().await {
                let conn_guard = connection.read().await;
                if let Some(ws) = conn_guard.as_ref() {
                    if let Err(e) = ws.send(msg).await {
                        error!("Failed to send message: {}", e);
                        connected.store(false, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        // Send registration
        self.register().await?;

        info!("Connected to federator as node {}", self.node_id);
        Ok(())
    }

    /// Register with the federator
    async fn register(&self) -> Result<()> {
        let buckets = self.config.buckets.clone().unwrap_or_default();

        self.send(NodeMessage::Register {
            node_id: self.node_id.clone(),
            node_type: self.config.node_type.clone(),
            buckets,
            endpoint: self.config.endpoint.clone(),
        })
        .await
    }

    /// Send a message to the federator
    pub async fn send(&self, msg: NodeMessage) -> Result<()> {
        if let Some(tx) = &self.outgoing_tx {
            tx.send(msg)
                .await
                .map_err(|e| anyhow!("Failed to queue message: {}", e))?;
            Ok(())
        } else {
            Err(anyhow!("Not connected"))
        }
    }

    /// Notify federator of an object PUT
    pub async fn send_object_put(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: u64,
        is_owner: bool,
    ) -> Result<()> {
        if !self.is_connected() {
            return Err(anyhow!("Federator not connected"));
        }

        self.send(NodeMessage::ObjectPut {
            bucket: bucket.to_string(),
            key: key.to_string(),
            etag: etag.to_string(),
            size,
            is_owner,
        })
        .await
    }

    /// Notify federator of an object DELETE
    pub async fn send_object_delete(&self, bucket: &str, key: &str) -> Result<()> {
        if !self.is_connected() {
            return Err(anyhow!("Federator not connected"));
        }

        self.send(NodeMessage::ObjectDelete {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
        .await
    }

    /// Locate the owner of an object
    ///
    /// First checks the local route cache, then queries the federator if not cached.
    pub async fn locate_owner(&self, bucket: &str, key: &str) -> Result<Option<OwnerInfo>> {
        // Check route cache first
        if let Some(cached) = self.route_cache.get(bucket, key) {
            return Ok(cached);
        }

        // Query federator
        if !self.is_connected() {
            return Err(anyhow!("Federator not connected"));
        }

        self.send(NodeMessage::LocateObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
        .await?;

        // Note: The response will be handled by the message handler and cached.
        // For now, return None and let the caller retry or use another method.
        // A more sophisticated implementation would wait for the response.
        Ok(None)
    }

    /// Invalidate a route in the local cache
    pub fn invalidate_route(&self, bucket: &str, key: &str) {
        self.route_cache.invalidate(bucket, key);
    }

    /// Disconnect from the federator
    pub async fn disconnect(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(ws) = self.connection.write().await.take() {
            // Connection will be dropped, closing the WebSocket
        }

        self.connected.store(false, Ordering::SeqCst);
        self.outgoing_tx = None;

        info!("Disconnected from federator");
    }
}

/// Get statistics about the federator connection
#[derive(Debug, Clone, serde::Serialize)]
pub struct FederatorStats {
    pub connected: bool,
    pub node_id: String,
    pub node_type: NodeType,
    pub route_cache_size: usize,
}

impl FederatorClient {
    /// Get connection statistics
    pub fn stats(&self) -> FederatorStats {
        FederatorStats {
            connected: self.is_connected(),
            node_id: self.node_id.clone(),
            node_type: self.config.node_type.clone(),
            route_cache_size: self.route_cache.len(),
        }
    }
}
