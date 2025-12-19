//! Heartbeat sender for federator connection
//!
//! Sends periodic heartbeats with cache statistics to the federator.

use crate::cache::CacheManager;
use crate::federator::protocol::{CacheStats, NodeMessage};
use crate::metrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Heartbeat sender configuration
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub node_id: String,
}

/// Sends periodic heartbeats to the federator
pub struct HeartbeatSender {
    config: HeartbeatConfig,
    cache: Arc<CacheManager>,
}

impl HeartbeatSender {
    /// Create a new heartbeat sender
    pub fn new(config: HeartbeatConfig, cache: Arc<CacheManager>) -> Self {
        Self { config, cache }
    }

    /// Start sending heartbeats
    pub async fn run(&self, message_tx: mpsc::Sender<NodeMessage>) {
        info!(
            "Starting heartbeat sender with interval {:?}",
            self.config.interval
        );

        let mut interval = tokio::time::interval(self.config.interval);

        loop {
            interval.tick().await;

            let stats = self.collect_stats().await;
            let msg = NodeMessage::Heartbeat {
                node_id: self.config.node_id.clone(),
                stats,
            };

            debug!("Sending heartbeat");
            if let Err(e) = message_tx.send(msg).await {
                error!("Failed to send heartbeat: {}", e);
                break;
            }
        }

        info!("Heartbeat sender stopped");
    }

    /// Collect cache statistics for the heartbeat
    async fn collect_stats(&self) -> CacheStats {
        let cache_stats = self.cache.stats().await;
        let m = metrics::get();

        // Calculate hit rate
        let hits = m.cache_hits_total.get();
        let misses = m.cache_misses_total.get();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        CacheStats {
            objects_count: cache_stats.total_objects,
            total_size_bytes: cache_stats.memory_size_bytes + cache_stats.disk_size_bytes,
            hit_rate,
            requests_total: m.requests_total.get(),
        }
    }
}
