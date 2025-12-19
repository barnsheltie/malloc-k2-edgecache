/**
 * ClusterFederator - Durable Object for coordinating cache/storage nodes
 */

import * as jose from 'jose';
import {
  NodeMessage,
  FederatorMessage,
  NodeInfo,
  ObjectMeta,
  BucketInfo,
  NodeType,
  NodeStatus,
  CacheStats,
  isNodeMessage,
  NodeJwtPayload,
} from './protocol';

interface ConnectedNode {
  websocket: WebSocket;
  info: NodeInfo;
  authenticated: boolean;
  stats?: CacheStats;
}

// Event types for dashboard
export interface FederatorEvent {
  timestamp: number;
  type: 'node_connected' | 'node_disconnected' | 'node_registered' | 'object_put' | 'object_delete' | 'invalidation' | 'bucket_sync';
  node_id?: string;
  details: string;
}

// Aggregate stats for dashboard
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

export class ClusterFederator implements DurableObject {
  private state: DurableObjectState;
  private env: Env;

  // In-memory state (not persisted)
  private nodes: Map<string, ConnectedNode> = new Map();
  private buckets: Map<string, BucketInfo> = new Map();
  private objectIndex: Map<string, ObjectMeta> = new Map(); // "bucket:key" → ObjectMeta
  private events: FederatorEvent[] = [];
  private maxEvents: number = 100;

  private heartbeatTimeoutSeconds: number = 90;
  private jwtSecret: Uint8Array | null = null;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;

    // Parse heartbeat timeout from env
    if (env.HEARTBEAT_TIMEOUT_SECONDS) {
      this.heartbeatTimeoutSeconds = parseInt(env.HEARTBEAT_TIMEOUT_SECONDS, 10);
    }

    // Start health check interval
    this.state.blockConcurrencyWhile(async () => {
      // Initialize JWT secret from environment
      if (env.JWT_SECRET) {
        this.jwtSecret = new TextEncoder().encode(env.JWT_SECRET);
      }
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket upgrade for node connections
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      this.handleWebSocket(server);

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    }

    // Health check endpoint
    if (url.pathname === '/health') {
      return new Response(
        JSON.stringify({
          status: 'ok',
          nodes: this.nodes.size,
          buckets: this.buckets.size,
          objects: this.objectIndex.size,
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Status endpoint (more detailed)
    if (url.pathname === '/status') {
      const nodeList: NodeInfo[] = [];
      for (const [, node] of this.nodes) {
        if (node.authenticated) {
          nodeList.push(node.info);
        }
      }

      return this.jsonResponse({
        nodes: nodeList,
        buckets: Array.from(this.buckets.values()),
        object_count: this.objectIndex.size,
      });
    }

    // Dashboard API endpoints
    if (url.pathname === '/api/stats') {
      return this.jsonResponse(this.getClusterStats());
    }

    if (url.pathname === '/api/nodes') {
      return this.jsonResponse(this.getNodeList());
    }

    if (url.pathname === '/api/buckets') {
      return this.jsonResponse(this.getBucketList());
    }

    if (url.pathname === '/api/events') {
      return this.jsonResponse(this.getEvents());
    }

    return new Response('Not Found', { status: 404 });
  }

  private jsonResponse(data: unknown): Response {
    return new Response(JSON.stringify(data), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  }

  private handleWebSocket(ws: WebSocket): void {
    ws.accept();

    const connectedNode: ConnectedNode = {
      websocket: ws,
      info: {
        id: '',
        node_type: 'cache',
        buckets: [],
        last_heartbeat: Date.now(),
        status: 'healthy',
      },
      authenticated: false,
    };

    ws.addEventListener('message', async (event) => {
      try {
        const data = typeof event.data === 'string' ? event.data : new TextDecoder().decode(event.data as ArrayBuffer);
        const msg = JSON.parse(data);

        if (!isNodeMessage(msg)) {
          this.sendError(ws, 'INVALID_MESSAGE', 'Invalid message format');
          return;
        }

        // First message must be auth
        if (!connectedNode.authenticated && msg.type !== 'auth') {
          this.sendError(ws, 'AUTH_REQUIRED', 'First message must be auth');
          ws.close(4001, 'Authentication required');
          return;
        }

        await this.handleMessage(ws, connectedNode, msg);
      } catch (err) {
        console.error('Error handling message:', err);
        this.sendError(ws, 'PARSE_ERROR', 'Failed to parse message');
      }
    });

    ws.addEventListener('close', () => {
      if (connectedNode.info.id) {
        this.handleNodeDisconnect(connectedNode.info.id);
      }
    });

    ws.addEventListener('error', (err) => {
      console.error('WebSocket error:', err);
      if (connectedNode.info.id) {
        this.handleNodeDisconnect(connectedNode.info.id);
      }
    });
  }

  private async handleMessage(
    ws: WebSocket,
    node: ConnectedNode,
    msg: NodeMessage
  ): Promise<void> {
    switch (msg.type) {
      case 'auth':
        await this.handleAuth(ws, node, msg.jwt);
        break;
      case 'register':
        this.handleRegister(ws, node, msg.node_id, msg.node_type, msg.buckets, msg.endpoint);
        break;
      case 'heartbeat':
        this.handleHeartbeat(node, msg.node_id, msg.stats);
        break;
      case 'object_put':
        this.handleObjectPut(node, msg.bucket, msg.key, msg.etag, msg.size, msg.is_owner);
        break;
      case 'object_delete':
        this.handleObjectDelete(node, msg.bucket, msg.key);
        break;
      case 'bucket_add':
        this.handleBucketAdd(node, msg.bucket);
        break;
      case 'bucket_remove':
        this.handleBucketRemove(node, msg.bucket);
        break;
      case 'locate_object':
        this.handleLocateObject(ws, msg.bucket, msg.key);
        break;
    }
  }

  private async handleAuth(ws: WebSocket, node: ConnectedNode, jwt: string): Promise<void> {
    if (!this.jwtSecret) {
      // No JWT secret configured, allow all connections (dev mode)
      console.warn('JWT_SECRET not configured, allowing unauthenticated connection');
      node.authenticated = true;
      this.send(ws, { type: 'auth_ok' });
      return;
    }

    try {
      const { payload } = await jose.jwtVerify(jwt, this.jwtSecret, {
        algorithms: ['HS256'],
      });

      const jwtPayload = payload as unknown as NodeJwtPayload;

      if (!jwtPayload.node_id || !jwtPayload.cluster_id) {
        this.send(ws, { type: 'auth_failed', reason: 'Invalid JWT payload' });
        ws.close(4003, 'Invalid JWT payload');
        return;
      }

      node.authenticated = true;
      node.info.id = jwtPayload.node_id;
      this.send(ws, { type: 'auth_ok' });
    } catch (err) {
      console.error('JWT verification failed:', err);
      this.send(ws, { type: 'auth_failed', reason: 'Invalid or expired token' });
      ws.close(4003, 'Authentication failed');
    }
  }

  private handleRegister(
    ws: WebSocket,
    node: ConnectedNode,
    nodeId: string,
    nodeType: NodeType,
    buckets: string[],
    endpoint?: string
  ): void {
    // Update node info
    node.info.id = nodeId;
    node.info.node_type = nodeType;
    node.info.buckets = buckets;
    node.info.endpoint = endpoint;
    node.info.last_heartbeat = Date.now();
    node.info.status = 'healthy';

    // Store in nodes map
    this.nodes.set(nodeId, node);

    // Register buckets
    for (const bucket of buckets) {
      let bucketInfo = this.buckets.get(bucket);
      if (!bucketInfo) {
        bucketInfo = { name: bucket, node_ids: [], object_count: 0 };
        this.buckets.set(bucket, bucketInfo);
      }
      if (!bucketInfo.node_ids.includes(nodeId)) {
        bucketInfo.node_ids.push(nodeId);
      }
    }

    // Send registered confirmation
    this.send(ws, { type: 'registered', cluster_id: 'default' });

    // Log registration event
    this.logEvent('node_registered', `${nodeType} node registered with ${buckets.length} buckets`, nodeId);

    // Send bucket sync for each registered bucket
    for (const bucket of buckets) {
      const objects = this.getObjectsForBucket(bucket);
      this.send(ws, { type: 'bucket_sync', bucket, objects });
      this.logEvent('bucket_sync', `synced ${objects.length} objects from "${bucket}"`, nodeId);
    }

    console.log(`Node registered: ${nodeId} (${nodeType}), buckets: ${buckets.join(', ')}`);
  }

  private handleHeartbeat(node: ConnectedNode, nodeId: string, stats: CacheStats): void {
    node.info.last_heartbeat = Date.now();
    node.info.status = 'healthy';
    node.stats = stats;
  }

  private handleObjectPut(
    node: ConnectedNode,
    bucket: string,
    key: string,
    etag: string,
    size: number,
    isOwner: boolean
  ): void {
    const objectKey = `${bucket}:${key}`;
    let meta = this.objectIndex.get(objectKey);

    if (!meta) {
      meta = {
        bucket,
        key,
        etag,
        size,
        cache_nodes: [],
      };
      this.objectIndex.set(objectKey, meta);
    }

    // Update metadata
    meta.etag = etag;
    meta.size = size;

    if (isOwner) {
      // Storage node claiming ownership
      meta.owner_node = node.info.id;
    } else {
      // Cache node has a copy
      if (!meta.cache_nodes.includes(node.info.id)) {
        meta.cache_nodes.push(node.info.id);
      }
    }

    // Log the event
    this.logEvent('object_put', `${bucket}/${key} (${this.formatBytes(size)})`, node.info.id);

    // Broadcast invalidation to all OTHER cache nodes
    this.broadcastInvalidation(bucket, key, node.info.id);
  }

  private handleObjectDelete(node: ConnectedNode, bucket: string, key: string): void {
    const objectKey = `${bucket}:${key}`;
    this.objectIndex.delete(objectKey);

    // Log the event
    this.logEvent('object_delete', `${bucket}/${key}`, node.info.id);

    // Broadcast invalidation to all OTHER nodes
    this.broadcastInvalidation(bucket, key, node.info.id);
  }

  private formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  }

  private handleBucketAdd(node: ConnectedNode, bucket: string): void {
    let bucketInfo = this.buckets.get(bucket);
    if (!bucketInfo) {
      bucketInfo = { name: bucket, node_ids: [], object_count: 0 };
      this.buckets.set(bucket, bucketInfo);
    }
    if (!bucketInfo.node_ids.includes(node.info.id)) {
      bucketInfo.node_ids.push(node.info.id);
    }
    if (!node.info.buckets.includes(bucket)) {
      node.info.buckets.push(bucket);
    }
  }

  private handleBucketRemove(node: ConnectedNode, bucket: string): void {
    const bucketInfo = this.buckets.get(bucket);
    if (bucketInfo) {
      bucketInfo.node_ids = bucketInfo.node_ids.filter((id) => id !== node.info.id);
      if (bucketInfo.node_ids.length === 0) {
        this.buckets.delete(bucket);
      }
    }
    node.info.buckets = node.info.buckets.filter((b) => b !== bucket);
  }

  private handleLocateObject(ws: WebSocket, bucket: string, key: string): void {
    const objectKey = `${bucket}:${key}`;
    const meta = this.objectIndex.get(objectKey);

    if (meta?.owner_node) {
      const ownerNode = this.nodes.get(meta.owner_node);
      this.send(ws, {
        type: 'locate_response',
        bucket,
        key,
        owner_node: meta.owner_node,
        endpoint: ownerNode?.info.endpoint,
      });
    } else {
      // No owner found
      this.send(ws, {
        type: 'locate_response',
        bucket,
        key,
      });
    }
  }

  private handleNodeDisconnect(nodeId: string): void {
    const node = this.nodes.get(nodeId);
    if (node) {
      // Update node status
      node.info.status = 'offline';

      // Remove from buckets
      for (const bucket of node.info.buckets) {
        const bucketInfo = this.buckets.get(bucket);
        if (bucketInfo) {
          bucketInfo.node_ids = bucketInfo.node_ids.filter((id) => id !== nodeId);
        }
      }

      // Remove node from nodes map
      this.nodes.delete(nodeId);

      // If this was a storage node, clear ownership of its objects
      if (node.info.node_type === 'storage') {
        for (const [key, meta] of this.objectIndex) {
          if (meta.owner_node === nodeId) {
            meta.owner_node = undefined;
          }
        }
      }

      // Remove from cache_nodes in object index
      for (const [, meta] of this.objectIndex) {
        meta.cache_nodes = meta.cache_nodes.filter((id) => id !== nodeId);
      }

      // Log the event
      this.logEvent('node_disconnected', `${node.info.node_type} node disconnected`, nodeId);

      console.log(`Node disconnected: ${nodeId}`);
    }
  }

  private broadcastInvalidation(bucket: string, key: string, sourceNodeId: string): void {
    const message: FederatorMessage = {
      type: 'invalidate',
      bucket,
      key,
      source_node: sourceNodeId,
    };

    let count = 0;
    for (const [nodeId, node] of this.nodes) {
      // Don't send to source node, only to cache nodes
      if (nodeId !== sourceNodeId && node.info.node_type === 'cache' && node.authenticated) {
        try {
          node.websocket.send(JSON.stringify(message));
          count++;
        } catch (err) {
          console.error(`Failed to send invalidation to ${nodeId}:`, err);
        }
      }
    }

    if (count > 0) {
      this.logEvent('invalidation', `${bucket}/${key} broadcast to ${count} nodes`, sourceNodeId);
    }
  }

  private getObjectsForBucket(bucket: string): ObjectMeta[] {
    const objects: ObjectMeta[] = [];
    for (const [key, meta] of this.objectIndex) {
      if (key.startsWith(`${bucket}:`)) {
        objects.push(meta);
      }
    }
    return objects;
  }

  private send(ws: WebSocket, msg: FederatorMessage): void {
    try {
      ws.send(JSON.stringify(msg));
    } catch (err) {
      console.error('Failed to send message:', err);
    }
  }

  private sendError(ws: WebSocket, code: string, message: string): void {
    this.send(ws, { type: 'error', code, message });
  }

  // ============================================================================
  // Dashboard API Methods
  // ============================================================================

  private logEvent(type: FederatorEvent['type'], details: string, nodeId?: string): void {
    this.events.push({
      timestamp: Date.now(),
      type,
      node_id: nodeId,
      details,
    });
    // Keep only the last N events
    if (this.events.length > this.maxEvents) {
      this.events.shift();
    }
  }

  getEvents(): FederatorEvent[] {
    return [...this.events].reverse(); // Most recent first
  }

  getClusterStats(): ClusterStats {
    let nodesHealthy = 0;
    let nodesDegraded = 0;
    let nodesOffline = 0;
    let storageNodes = 0;
    let cacheNodes = 0;
    let totalSizeBytes = 0;
    let totalHitRate = 0;
    let totalRequests = 0;
    let nodesWithStats = 0;

    const now = Date.now();
    for (const [, node] of this.nodes) {
      if (!node.authenticated) continue;

      // Determine status based on last heartbeat
      const timeSinceHeartbeat = (now - node.info.last_heartbeat) / 1000;
      if (timeSinceHeartbeat < 60) {
        nodesHealthy++;
      } else if (timeSinceHeartbeat < this.heartbeatTimeoutSeconds) {
        nodesDegraded++;
      } else {
        nodesOffline++;
      }

      if (node.info.node_type === 'storage') {
        storageNodes++;
      } else {
        cacheNodes++;
      }

      // Aggregate stats from heartbeats
      if (node.stats) {
        totalSizeBytes += node.stats.total_size_bytes;
        totalHitRate += node.stats.hit_rate;
        totalRequests += node.stats.requests_total;
        nodesWithStats++;
      }
    }

    return {
      nodes_total: this.nodes.size,
      nodes_healthy: nodesHealthy,
      nodes_degraded: nodesDegraded,
      nodes_offline: nodesOffline,
      storage_nodes: storageNodes,
      cache_nodes: cacheNodes,
      buckets_total: this.buckets.size,
      objects_total: this.objectIndex.size,
      total_size_bytes: totalSizeBytes,
      avg_hit_rate: nodesWithStats > 0 ? totalHitRate / nodesWithStats : 0,
      total_requests: totalRequests,
    };
  }

  getNodeList(): (NodeInfo & { stats?: CacheStats })[] {
    const nodeList: (NodeInfo & { stats?: CacheStats })[] = [];
    for (const [, node] of this.nodes) {
      if (node.authenticated) {
        nodeList.push({
          ...node.info,
          stats: node.stats,
        });
      }
    }
    return nodeList;
  }

  getBucketList(): BucketInfo[] {
    // Update object counts
    for (const [, bucket] of this.buckets) {
      bucket.object_count = 0;
    }
    for (const [key] of this.objectIndex) {
      const bucketName = key.split(':')[0];
      const bucket = this.buckets.get(bucketName);
      if (bucket) {
        bucket.object_count++;
      }
    }
    return Array.from(this.buckets.values());
  }
}

// Environment interface
interface Env {
  JWT_SECRET?: string;
  HEARTBEAT_TIMEOUT_SECONDS?: string;
}
