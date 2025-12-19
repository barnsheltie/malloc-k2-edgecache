// Types matching the federator protocol

export type NodeType = 'cache' | 'storage';
export type NodeStatus = 'healthy' | 'degraded' | 'offline';

export interface CacheStats {
  objects_count: number;
  total_size_bytes: number;
  hit_rate: number;
  requests_total: number;
}

export interface NodeInfo {
  id: string;
  node_type: NodeType;
  buckets: string[];
  endpoint?: string;
  last_heartbeat: number;
  status: NodeStatus;
  stats?: CacheStats;
}

export interface BucketInfo {
  name: string;
  node_ids: string[];
  object_count: number;
}

export interface ClusterStats {
  nodes_total: number;
  nodes_healthy: number;
  nodes_degraded: number;
  nodes_offline: number;
  storage_nodes: number;
  cache_nodes: number;
  buckets_total: number;
  objects_total: number;
  total_size_bytes: number;
  avg_hit_rate: number;
  total_requests: number;
}

export interface FederatorEvent {
  timestamp: number;
  type: 'node_connected' | 'node_disconnected' | 'node_registered' | 'object_put' | 'object_delete' | 'invalidation' | 'bucket_sync';
  node_id?: string;
  details: string;
}
