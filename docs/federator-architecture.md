# Inter-Node Communication via Cloud Federator

## Overview

Implement hub-spoke architecture where distributed cache nodes (3-300) communicate through a central **Cloudflare Workers + Durable Objects** federator.

## Architecture

```
                    ┌─────────────────────────────────┐
                    │   Cloudflare Durable Object     │
                    │        "ClusterFederator"       │
                    │  ┌───────────────────────────┐  │
                    │  │ - Node registry (w/ type) │  │
                    │  │ - Object→Owner mapping    │  │
                    │  │ - Object metadata cache   │  │
                    │  │ - Health status           │  │
                    │  │ - WebSocket connections   │  │
                    │  └───────────────────────────┘  │
                    └──────────────┬──────────────────┘
                                   │ WebSocket (wss://)
       ┌───────────────────────────┼───────────────────────────┐
       │                           │                           │
┌──────▼──────┐             ┌──────▼──────┐             ┌──────▼──────┐
│ STORAGE     │             │ CACHE Node  │             │ CACHE Node  │
│ Node        │◄────────────│  (Field)    │             │  (Field)    │
│ (Primary)   │  HTTP proxy │  Rust Edge  │             │  Rust Edge  │
└─────────────┘  on miss    └─────────────┘             └─────────────┘
      │
      │ Local disk
      ▼
  ┌─────────┐
  │ Objects │
  └─────────┘
```

**Flow**: Cache nodes query federator for object owner → proxy to storage node on miss

## Requirements Summary

- **Scale**: 3-300 nodes per cluster
- **Node Types**: Hybrid - supports both cache nodes and storage nodes
- **Sync Scope**: Cache invalidations + object metadata + bucket registry + health + object routing
- **Protocol**: WebSocket (persistent connection via Durable Objects)
- **Consistency**: Strict - writes fail if federator unreachable
- **Platform**: Cloudflare Workers + Durable Objects
- **State**: In-memory only (nodes re-register on restart)
- **Authentication**: JWT token per node
- **Topology**: One Durable Object (federator) per cluster

---

## Node Types (Hybrid Architecture)

### Cache Node
- **Purpose**: Caches objects fetched from upstream S3 or storage nodes
- **Source of truth**: Upstream S3 or designated storage node
- **On remote write**: Receives `invalidate` message, removes stale cached copy
- **On read miss**: Fetches from upstream S3 OR queries federator for storage node owner

### Storage Node
- **Purpose**: Primary storage owner for objects (no upstream S3)
- **Source of truth**: The node itself
- **On write**: Notifies federator of ownership (`object_put` with `is_owner: true`)
- **On remote read**: Other nodes proxy through federator to reach storage node
- **Replication**: Optional - federator can track replica nodes for durability

### Behavior Comparison

| Operation | Cache Node | Storage Node |
|-----------|-----------|--------------|
| **PUT** | Write to local cache + notify federator | Write to local storage + register ownership |
| **GET (hit)** | Serve from local cache | Serve from local storage |
| **GET (miss)** | Fetch from S3 OR storage node owner | N/A (storage node IS the owner) |
| **On invalidate** | Remove from local cache | Ignore (only cache nodes invalidate) |
| **DELETE** | Remove from cache + notify federator | Remove from storage + notify federator |

---

## Implementation Plan

### Phase 1: Cloudflare Federator (TypeScript/Workers)

**Location**: New repository or `federator/` subdirectory

#### 1.1 Durable Object: ClusterFederator

```typescript
// Core state maintained by federator
interface FederatorState {
  nodes: Map<string, NodeInfo>;           // node_id → status, buckets, last_seen
  buckets: Map<string, BucketInfo>;       // bucket_name → node_ids[], metadata
  objectIndex: Map<string, ObjectMeta>;   // bucket:key → etag, size, owner_node, cache_nodes[]
}

interface NodeInfo {
  id: string;
  websocket: WebSocket;
  nodeType: 'cache' | 'storage';          // NEW: Node type for hybrid architecture
  buckets: string[];
  endpoint?: string;                       // NEW: For storage nodes - HTTP endpoint for proxying
  lastHeartbeat: number;
  status: 'healthy' | 'degraded' | 'offline';
}

interface ObjectMeta {
  bucket: string;
  key: string;
  etag: string;
  size: number;
  ownerNode?: string;                      // NEW: Storage node that owns this object (if any)
  cacheNodes: string[];                    // NEW: Cache nodes that have a copy
}
```

#### 1.2 Message Protocol (JSON over WebSocket)

```typescript
// Node → Federator (first message must be auth)
type NodeMessage =
  | { type: 'auth'; jwt: string }  // JWT contains node_id, cluster_id, exp
  | { type: 'register'; node_id: string; node_type: 'cache' | 'storage'; buckets: string[]; endpoint?: string }
  | { type: 'heartbeat'; node_id: string; stats: CacheStats }
  | { type: 'object_put'; bucket: string; key: string; etag: string; size: number; is_owner: boolean }
  | { type: 'object_delete'; bucket: string; key: string }
  | { type: 'bucket_add'; bucket: string }
  | { type: 'bucket_remove'; bucket: string }
  | { type: 'locate_object'; bucket: string; key: string };  // NEW: Find storage node owner

// Federator → Node
type FederatorMessage =
  | { type: 'auth_ok' }
  | { type: 'auth_failed'; reason: string }
  | { type: 'registered'; cluster_id: string }
  | { type: 'invalidate'; bucket: string; key: string; source_node: string }
  | { type: 'bucket_sync'; bucket: string; objects: ObjectMeta[] }
  | { type: 'node_status'; nodes: NodeInfo[] }
  | { type: 'locate_response'; bucket: string; key: string; owner_node?: string; endpoint?: string }  // NEW
  | { type: 'error'; code: string; message: string };
```

#### 1.3 JWT Authentication

```typescript
// JWT payload structure
interface NodeJwtPayload {
  node_id: string;       // Unique node identifier
  cluster_id: string;    // Which cluster this node belongs to
  iat: number;           // Issued at
  exp: number;           // Expiration (e.g., 1 year)
}

// Federator validates JWT using shared secret per cluster
// Secret stored in Cloudflare Worker secrets (wrangler secret)
```

#### 1.4 Files to Create

| File | Purpose |
|------|---------|
| `federator/src/index.ts` | Worker entry point, routes to DO |
| `federator/src/federator.ts` | ClusterFederator Durable Object |
| `federator/src/protocol.ts` | Message types and validation |
| `federator/wrangler.toml` | Cloudflare deployment config |

---

### Phase 2: Rust Client Module (Cache Node Side)

**Location**: `src/federator/` in malloc-k2-edgecache

#### 2.1 New Module Structure

```
src/federator/
├── mod.rs           # Module exports, FederatorClient struct
├── connection.rs    # WebSocket connection management, reconnection logic
├── protocol.rs      # Message serialization (serde)
├── handler.rs       # Incoming message handlers (invalidation, sync)
├── health.rs        # Heartbeat sender, stats collection
└── route_cache.rs   # Local cache of object→owner mappings (invalidated on change)
```

#### 2.2 Core Components

**FederatorClient** (mod.rs):
```rust
pub struct FederatorClient {
    ws: Option<WebSocketConnection>,
    config: FederatorConfig,
    node_id: String,
    state: Arc<RwLock<FederatorState>>,
    route_cache: RouteCache,  // Local cache of object→owner mappings
}

impl FederatorClient {
    pub async fn connect(&mut self) -> Result<()>;
    pub async fn send_object_put(&self, bucket: &str, key: &str, etag: &str, size: u64, is_owner: bool) -> Result<()>;
    pub async fn send_object_delete(&self, bucket: &str, key: &str) -> Result<()>;
    pub async fn locate_owner(&self, bucket: &str, key: &str) -> Result<Option<OwnerInfo>>;  // Uses route cache
    pub fn invalidate_route(&self, bucket: &str, key: &str);  // Called on invalidate message
    pub fn is_connected(&self) -> bool;
}

// Route cache entry (cached locally to avoid repeated federator queries)
pub struct OwnerInfo {
    pub node_id: String,
    pub endpoint: String,
}
```

**Strict Consistency Enforcement** (in s3_proxy.rs):
```rust
// Before accepting a PUT:
if !self.federator_client.is_connected() {
    return Err(Error::FederatorUnavailable);
}
```

#### 2.3 Integration Points

| Existing File | Change |
|--------------|--------|
| `src/proxy/s3_proxy.rs` | Call federator on PUT/DELETE, reject if disconnected |
| `src/cache/mod.rs` | Hook invalidation handler to remove cached objects |
| `src/config.rs` | Add `FederatorConfig` (url, node_id, cluster_id) |
| `src/main.rs` | Initialize and start FederatorClient |
| `src/api/handlers.rs` | Add `/api/federator/status` endpoint |

---

### Phase 3: Configuration

#### 3.1 Config Additions (config.rs)

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FederatorConfig {
    /// Enable federator connection
    #[serde(default)]
    pub enabled: bool,

    /// Federator WebSocket URL (wss://...)
    pub url: String,

    /// Unique node identifier
    pub node_id: Option<String>,  // Auto-generated if not set

    /// Cluster identifier
    pub cluster_id: String,

    /// Node type: "cache" or "storage"
    #[serde(default = "default_node_type")]
    pub node_type: NodeType,

    /// HTTP endpoint for this node (required for storage nodes, used for proxying)
    pub endpoint: Option<String>,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_seconds: u64,

    /// Reconnection delay in seconds
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    #[default]
    Cache,
    Storage,
}
```

#### 3.2 Example TOML Config

**Cache Node:**
```toml
[federator]
enabled = true
url = "wss://edgecache-federator.your-domain.workers.dev/ws"
cluster_id = "prod-cluster-1"
node_type = "cache"
heartbeat_interval_seconds = 30
reconnect_delay_seconds = 5
```

**Storage Node:**
```toml
[federator]
enabled = true
url = "wss://edgecache-federator.your-domain.workers.dev/ws"
cluster_id = "prod-cluster-1"
node_type = "storage"
endpoint = "https://storage-node-1.example.com:9000"  # Required for storage nodes
heartbeat_interval_seconds = 30
reconnect_delay_seconds = 5
```

---

### Phase 4: Message Flows

#### PUT Object Flow - Cache Node (Strict Consistency)

```
1. Client → Cache Node: PUT /bucket/key
2. Cache Node checks: federator.is_connected()?
   - NO → Return 503 Service Unavailable
   - YES → Continue
3. Cache Node → Federator: { type: "object_put", bucket, key, etag, size, is_owner: false }
4. Federator broadcasts to all other cache nodes: { type: "invalidate", bucket, key }
5. Other cache nodes remove object from local cache
6. Cache Node stores object locally (and optionally writes to upstream S3)
7. Cache Node → Client: 200 OK
```

#### PUT Object Flow - Storage Node (Primary Owner)

```
1. Client → Storage Node: PUT /bucket/key
2. Storage Node checks: federator.is_connected()?
   - NO → Return 503 Service Unavailable
   - YES → Continue
3. Storage Node stores object to local disk
4. Storage Node → Federator: { type: "object_put", bucket, key, etag, size, is_owner: true }
5. Federator updates object index: owner_node = this storage node
6. Federator broadcasts to all cache nodes: { type: "invalidate", bucket, key }
7. Cache nodes remove stale copies
8. Storage Node → Client: 200 OK
```

#### GET Object Flow - Cache Node (Miss with Storage Node Owner)

```
1. Client → Cache Node: GET /bucket/key
2. Cache Node: local cache miss
3. Cache Node → Federator: { type: "locate_object", bucket, key }
4. Federator → Cache Node: { type: "locate_response", bucket, key, owner_node, endpoint }
5. If owner_node exists:
   - Cache Node → Storage Node endpoint: GET /bucket/key (HTTP proxy)
   - Storage Node → Cache Node: object data
   - Cache Node caches locally
   - Cache Node → Client: object data
6. If no owner_node:
   - Cache Node fetches from upstream S3 (if configured)
   - OR returns 404 Not Found
```

#### Node Startup Flow

```
1. Node starts, loads config
2. Connect WebSocket to federator
3. Send: { type: "register", node_id, node_type, buckets: [...], endpoint? }
4. Receive: { type: "registered", cluster_id }
5. Receive: { type: "bucket_sync", ... } for each registered bucket
6. Start heartbeat timer
```

---

## File Changes Summary

### New Files (Cloudflare Federator)
- `federator/src/index.ts`
- `federator/src/federator.ts`
- `federator/src/protocol.ts`
- `federator/wrangler.toml`
- `federator/package.json`

### New Files (Rust Client)
- `src/federator/mod.rs`
- `src/federator/connection.rs`
- `src/federator/protocol.rs`
- `src/federator/handler.rs`
- `src/federator/health.rs`
- `src/federator/route_cache.rs`

### Modified Files (Rust)
- `src/config.rs` - Add FederatorConfig
- `src/main.rs` - Initialize FederatorClient
- `src/lib.rs` - Add `mod federator`
- `src/proxy/s3_proxy.rs` - Integration with federator
- `src/cache/mod.rs` - Invalidation handler
- `src/api/handlers.rs` - Status endpoint
- `src/api/routes.rs` - Route for status
- `CLAUDE.md` - Update documentation

---

## Dependencies

### Rust (Cargo.toml)
```toml
tokio-tungstenite = "0.21"   # WebSocket client
futures-util = "0.3"          # Stream utilities
```

### Cloudflare (package.json)
```json
{
  "devDependencies": {
    "wrangler": "^3.0.0",
    "@cloudflare/workers-types": "^4.0.0"
  }
}
```

---

## Testing Strategy

1. **Unit Tests**: Protocol serialization, message validation
2. **Integration Tests**: Mock WebSocket server, test reconnection
3. **E2E Tests**: Deploy federator to Cloudflare, test with 2-3 nodes

---

## Resolved Decisions

1. ✅ **State persistence**: In-memory only (nodes re-register on restart)
2. ✅ **Authentication**: JWT token per node
3. ✅ **Multi-cluster**: One federator per cluster
4. ✅ **Node types**: Hybrid architecture supporting both cache and storage nodes
5. ✅ **Storage replication**: Single owner (may investigate multi-node replication later)
6. ✅ **Route caching**: Cache nodes cache `locate_response` results locally (invalidated on object change)
