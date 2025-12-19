//! Protocol definitions for federator-node communication
//!
//! Message types match the TypeScript federator implementation.

use serde::{Deserialize, Serialize};

// Re-export NodeType from config
pub use crate::config::NodeType;

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Offline,
}

/// Cache statistics sent with heartbeats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub objects_count: u64,
    pub total_size_bytes: u64,
    pub hit_rate: f64,
    pub requests_total: u64,
}

/// Object metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_node: Option<String>,
    pub cache_nodes: Vec<String>,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub node_type: NodeType,
    pub buckets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub last_heartbeat: u64,
    pub status: NodeStatus,
}

// =============================================================================
// Node → Federator Messages
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeMessage {
    Auth {
        jwt: String,
    },
    Register {
        node_id: String,
        node_type: NodeType,
        buckets: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
    Heartbeat {
        node_id: String,
        stats: CacheStats,
    },
    ObjectPut {
        bucket: String,
        key: String,
        etag: String,
        size: u64,
        is_owner: bool,
    },
    ObjectDelete {
        bucket: String,
        key: String,
    },
    BucketAdd {
        bucket: String,
    },
    BucketRemove {
        bucket: String,
    },
    LocateObject {
        bucket: String,
        key: String,
    },
}

// =============================================================================
// Federator → Node Messages
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FederatorMessage {
    AuthOk,
    AuthFailed {
        reason: String,
    },
    Registered {
        cluster_id: String,
    },
    Invalidate {
        bucket: String,
        key: String,
        source_node: String,
    },
    BucketSync {
        bucket: String,
        objects: Vec<ObjectMeta>,
    },
    NodeStatus {
        nodes: Vec<NodeInfo>,
    },
    LocateResponse {
        bucket: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        owner_node: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
    Error {
        code: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_message_serialization() {
        let msg = NodeMessage::Auth {
            jwt: "test-token".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"auth\""));
        assert!(json.contains("\"jwt\":\"test-token\""));
    }

    #[test]
    fn test_register_message_serialization() {
        let msg = NodeMessage::Register {
            node_id: "node-1".to_string(),
            node_type: NodeType::Cache,
            buckets: vec!["bucket-1".to_string()],
            endpoint: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"register\""));
        assert!(json.contains("\"node_type\":\"cache\""));
    }

    #[test]
    fn test_federator_message_deserialization() {
        let json = r#"{"type":"auth_ok"}"#;
        let msg: FederatorMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, FederatorMessage::AuthOk));
    }

    #[test]
    fn test_invalidate_message_deserialization() {
        let json = r#"{"type":"invalidate","bucket":"test","key":"obj1","source_node":"node-2"}"#;
        let msg: FederatorMessage = serde_json::from_str(json).unwrap();
        if let FederatorMessage::Invalidate {
            bucket,
            key,
            source_node,
        } = msg
        {
            assert_eq!(bucket, "test");
            assert_eq!(key, "obj1");
            assert_eq!(source_node, "node-2");
        } else {
            panic!("Expected Invalidate message");
        }
    }
}
