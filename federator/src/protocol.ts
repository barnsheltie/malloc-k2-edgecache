/**
 * Protocol definitions for federator-node communication
 */

// Node types in the hybrid architecture
export type NodeType = 'cache' | 'storage';

// Node status
export type NodeStatus = 'healthy' | 'degraded' | 'offline';

// Cache statistics sent with heartbeats
export interface CacheStats {
  objects_count: number;
  total_size_bytes: number;
  hit_rate: number;
  requests_total: number;
}

// Object metadata
export interface ObjectMeta {
  bucket: string;
  key: string;
  etag: string;
  size: number;
  owner_node?: string;    // Storage node that owns this object (if any)
  cache_nodes: string[];  // Cache nodes that have a copy
}

// Node information
export interface NodeInfo {
  id: string;
  node_type: NodeType;
  buckets: string[];
  endpoint?: string;      // For storage nodes - HTTP endpoint for proxying
  last_heartbeat: number;
  status: NodeStatus;
}

// Bucket information
export interface BucketInfo {
  name: string;
  node_ids: string[];     // Nodes that have registered this bucket
  object_count: number;
}

// =============================================================================
// Node → Federator Messages
// =============================================================================

export interface AuthMessage {
  type: 'auth';
  jwt: string;
}

export interface RegisterMessage {
  type: 'register';
  node_id: string;
  node_type: NodeType;
  buckets: string[];
  endpoint?: string;      // Required for storage nodes
}

export interface HeartbeatMessage {
  type: 'heartbeat';
  node_id: string;
  stats: CacheStats;
}

export interface ObjectPutMessage {
  type: 'object_put';
  bucket: string;
  key: string;
  etag: string;
  size: number;
  is_owner: boolean;      // true if this is a storage node claiming ownership
}

export interface ObjectDeleteMessage {
  type: 'object_delete';
  bucket: string;
  key: string;
}

export interface BucketAddMessage {
  type: 'bucket_add';
  bucket: string;
}

export interface BucketRemoveMessage {
  type: 'bucket_remove';
  bucket: string;
}

export interface LocateObjectMessage {
  type: 'locate_object';
  bucket: string;
  key: string;
}

export type NodeMessage =
  | AuthMessage
  | RegisterMessage
  | HeartbeatMessage
  | ObjectPutMessage
  | ObjectDeleteMessage
  | BucketAddMessage
  | BucketRemoveMessage
  | LocateObjectMessage;

// =============================================================================
// Federator → Node Messages
// =============================================================================

export interface AuthOkMessage {
  type: 'auth_ok';
}

export interface AuthFailedMessage {
  type: 'auth_failed';
  reason: string;
}

export interface RegisteredMessage {
  type: 'registered';
  cluster_id: string;
}

export interface InvalidateMessage {
  type: 'invalidate';
  bucket: string;
  key: string;
  source_node: string;
}

export interface BucketSyncMessage {
  type: 'bucket_sync';
  bucket: string;
  objects: ObjectMeta[];
}

export interface NodeStatusMessage {
  type: 'node_status';
  nodes: NodeInfo[];
}

export interface LocateResponseMessage {
  type: 'locate_response';
  bucket: string;
  key: string;
  owner_node?: string;
  endpoint?: string;
}

export interface ErrorMessage {
  type: 'error';
  code: string;
  message: string;
}

export type FederatorMessage =
  | AuthOkMessage
  | AuthFailedMessage
  | RegisteredMessage
  | InvalidateMessage
  | BucketSyncMessage
  | NodeStatusMessage
  | LocateResponseMessage
  | ErrorMessage;

// =============================================================================
// Validation Functions
// =============================================================================

export function isNodeMessage(msg: unknown): msg is NodeMessage {
  if (typeof msg !== 'object' || msg === null) return false;
  const m = msg as Record<string, unknown>;
  if (typeof m.type !== 'string') return false;

  switch (m.type) {
    case 'auth':
      return typeof m.jwt === 'string';
    case 'register':
      return (
        typeof m.node_id === 'string' &&
        (m.node_type === 'cache' || m.node_type === 'storage') &&
        Array.isArray(m.buckets)
      );
    case 'heartbeat':
      return typeof m.node_id === 'string' && typeof m.stats === 'object';
    case 'object_put':
      return (
        typeof m.bucket === 'string' &&
        typeof m.key === 'string' &&
        typeof m.etag === 'string' &&
        typeof m.size === 'number' &&
        typeof m.is_owner === 'boolean'
      );
    case 'object_delete':
      return typeof m.bucket === 'string' && typeof m.key === 'string';
    case 'bucket_add':
    case 'bucket_remove':
      return typeof m.bucket === 'string';
    case 'locate_object':
      return typeof m.bucket === 'string' && typeof m.key === 'string';
    default:
      return false;
  }
}

// JWT payload structure
export interface NodeJwtPayload {
  node_id: string;
  cluster_id: string;
  iat: number;
  exp: number;
}
