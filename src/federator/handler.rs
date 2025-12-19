//! Message handlers for incoming federator messages
//!
//! Processes invalidation, sync, and other messages from the federator.

use crate::cache::{CacheKey, CacheManager};
use crate::federator::protocol::{FederatorMessage, ObjectMeta};
use crate::federator::route_cache::RouteCache;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Handles incoming messages from the federator
pub struct MessageHandler {
    cache: Arc<CacheManager>,
    route_cache: Arc<RouteCache>,
    node_id: String,
}

impl MessageHandler {
    /// Create a new message handler
    pub fn new(cache: Arc<CacheManager>, route_cache: Arc<RouteCache>, node_id: &str) -> Self {
        Self {
            cache,
            route_cache,
            node_id: node_id.to_string(),
        }
    }

    /// Start processing messages from the channel
    pub async fn run(&self, mut rx: mpsc::Receiver<FederatorMessage>) {
        info!("Message handler started for node {}", self.node_id);

        while let Some(msg) = rx.recv().await {
            self.handle_message(msg).await;
        }

        info!("Message handler stopped");
    }

    /// Handle a single message
    pub async fn handle_message(&self, msg: FederatorMessage) {
        match msg {
            FederatorMessage::AuthOk => {
                info!("Authentication successful");
            }
            FederatorMessage::AuthFailed { reason } => {
                error!("Authentication failed: {}", reason);
            }
            FederatorMessage::Registered { cluster_id } => {
                info!("Registered with cluster: {}", cluster_id);
            }
            FederatorMessage::Invalidate {
                bucket,
                key,
                source_node,
            } => {
                self.handle_invalidation(&bucket, &key, &source_node).await;
            }
            FederatorMessage::BucketSync { bucket, objects } => {
                self.handle_bucket_sync(&bucket, objects).await;
            }
            FederatorMessage::NodeStatus { nodes } => {
                debug!("Received node status update: {} nodes", nodes.len());
            }
            FederatorMessage::LocateResponse {
                bucket,
                key,
                owner_node,
                endpoint,
            } => {
                self.handle_locate_response(&bucket, &key, owner_node, endpoint);
            }
            FederatorMessage::Error { code, message } => {
                error!("Federator error [{}]: {}", code, message);
            }
        }
    }

    /// Handle cache invalidation from another node
    async fn handle_invalidation(&self, bucket: &str, key: &str, source_node: &str) {
        debug!(
            "Invalidating cache for {}/{} (from {})",
            bucket, key, source_node
        );

        // Invalidate route cache
        self.route_cache.invalidate(bucket, key);

        // Remove from local cache
        let cache_key = CacheKey::new(bucket, key);
        self.cache.remove(&cache_key).await;
        debug!("Removed {}/{} from cache", bucket, key);
    }

    /// Handle bucket sync - update local metadata about objects in a bucket
    async fn handle_bucket_sync(&self, bucket: &str, objects: Vec<ObjectMeta>) {
        info!(
            "Received bucket sync for '{}': {} objects",
            bucket,
            objects.len()
        );

        // Update route cache with owner information
        for obj in objects {
            if let Some(owner_node) = &obj.owner_node {
                // We don't have endpoint info in the sync message, so we skip caching
                // The endpoint will be retrieved on first locate_object request
                debug!(
                    "Bucket {} has object {}/{} owned by {}",
                    bucket, obj.bucket, obj.key, owner_node
                );
            }
        }
    }

    /// Handle locate response - cache the owner information
    fn handle_locate_response(
        &self,
        bucket: &str,
        key: &str,
        owner_node: Option<String>,
        endpoint: Option<String>,
    ) {
        match (owner_node, endpoint) {
            (Some(node), Some(ep)) => {
                debug!("Caching route for {}/{}: {} at {}", bucket, key, node, ep);
                self.route_cache.set(
                    bucket,
                    key,
                    Some(crate::federator::route_cache::OwnerInfo {
                        node_id: node,
                        endpoint: ep,
                    }),
                );
            }
            (None, _) => {
                debug!("No owner for {}/{}", bucket, key);
                self.route_cache.set(bucket, key, None);
            }
            (Some(node), None) => {
                warn!(
                    "Owner {} for {}/{} has no endpoint",
                    node, bucket, key
                );
                // Don't cache incomplete info
            }
        }
    }
}
