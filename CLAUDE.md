# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

malloc-k2-edgecache is a high-performance S3 caching proxy built on Cloudflare's Pingora framework. It replicates the functionality of KodiakEdgeCache (located at `~/k2/KodiakEdgeCache`) but rewritten in Rust for improved performance and memory safety.

**Reference Implementation:** `~/k2/KodiakEdgeCache` (Node.js)

## Build Commands

```bash
# Build debug version
cargo build

# Build release version (optimized)
cargo build --release

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name

# Format code
cargo fmt

# Lint code
cargo clippy

# Run with debug logging
RUST_LOG=debug cargo run -- --config config/default.toml

# Run release build
./target/release/malloc-k2-edgecache --config config/default.toml
```

## Project Architecture

### Source Structure

```
src/
├── main.rs          # Application entry point
├── lib.rs           # Library exports
├── config.rs        # Configuration structures and loading (TOML-based)
├── api/             # REST API (axum-based)
│   ├── mod.rs       # API server setup
│   ├── routes.rs    # Route definitions
│   └── handlers.rs  # Request handlers
├── auth/            # Authentication
│   ├── mod.rs       # Auth manager
│   └── aws_sigv4.rs # AWS Signature V4 implementation
├── cache/           # Caching layer
│   ├── mod.rs       # Cache manager (coordinates memory + disk, expiration)
│   ├── cache_key.rs # Cache key generation (SHA256-based)
│   ├── memory_cache.rs # In-memory LRU cache
│   └── disk_cache.rs   # Disk-based persistent cache
├── metrics/         # Prometheus metrics
│   └── mod.rs
├── multipart/       # Multipart upload handling
│   └── mod.rs       # Upload manager, part tracking, assembly
├── proxy/           # Pingora proxy implementation
│   ├── mod.rs
│   └── s3_proxy.rs  # S3 proxy handler (ProxyHttp trait)
├── writeback/       # Write-back queue for deferred S3 uploads
│   └── mod.rs       # Queue, processor, S3Operations trait
└── federator/       # Inter-node communication with central federator
    ├── mod.rs       # FederatorClient for WebSocket communication
    ├── connection.rs # WebSocket connection management
    ├── handler.rs   # Incoming message handler (invalidations, syncs)
    ├── health.rs    # Heartbeat sender with cache statistics
    ├── protocol.rs  # Message types matching TypeScript federator
    └── route_cache.rs # Local cache for object→owner mappings
```

### Key Components

- **Proxy Layer** (`proxy/s3_proxy.rs`): Implements Pingora's `ProxyHttp` trait to intercept and cache S3 requests
- **Cache Manager** (`cache/mod.rs`): Two-tier caching with memory (LRU) and disk (persistent) layers
- **Auth Module** (`auth/aws_sigv4.rs`): AWS Signature V4 signing and verification
- **REST API** (`api/`): Management endpoints on port 14000
- **Federator Client** (`federator/`): WebSocket-based communication with central federator for cache invalidation, object routing, and cluster coordination

### Default Ports

| Port  | Purpose        |
|-------|----------------|
| 9000  | HTTP S3 proxy  |
| 9001  | HTTPS S3 proxy |
| 14000 | REST API       |

## Current Status

**Build:** ✅ Compiles successfully with Pingora 0.6
**Tests:** Run `cargo test` to verify

### Implemented
- [x] Project structure with all modules
- [x] Configuration (TOML-based)
- [x] Memory cache with LRU eviction
- [x] Disk cache with persistent storage
- [x] AWS SigV4 authentication (signing + verification)
- [x] REST API with health, config, cache, and bucket endpoints
- [x] Prometheus metrics definitions
- [x] Pingora proxy with S3 request parsing
- [x] GitHub repo: https://github.com/barnsheltie/malloc-k2-edgecache
- [x] Cache serving from request_filter (serves cached responses with x-cache: HIT header)
- [x] Response body caching via upstream_response_body_filter (caches GET 200 responses)
- [x] Write-back queue for deferred PUT uploads (src/writeback/mod.rs)
- [x] PUT request handling with immediate local cache + background S3 upload
- [x] Write-back API endpoints (/api/writeback/stats, /api/writeback/queue)
- [x] Cache expiration background task (configurable interval, memory + disk scanning)
- [x] Trait-based S3 operations for testability (S3Operations, MockS3Client)
- [x] Multipart upload handling (InitiateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts)
- [x] Multipart API endpoints (/api/multipart/stats, /api/multipart/uploads)
- [x] Federator client for inter-node communication (src/federator/)
- [x] WebSocket-based connection to central federator
- [x] Route cache for object→owner mappings with TTL
- [x] Heartbeat sender with cache statistics
- [x] Federator API endpoint (/api/federator/status)

### Future Enhancements
- [ ] Add Azure Blob Storage support
- [ ] Add HTTPS support with TLS certificates

## Federator Architecture

The federator enables distributed cache coordination using a hub-spoke architecture:

### Components
- **Federator (Hub)**: TypeScript service on Cloudflare Workers with Durable Objects (`federator/`)
- **Nodes (Spokes)**: Rust edge cache nodes that connect via WebSocket

### Node Types
- **Cache Node**: Read-heavy, caches objects from storage nodes
- **Storage Node**: Authoritative source, owns objects and handles writes

### Features
- **Cache Invalidation**: Federator broadcasts invalidation when objects change
- **Object Routing**: Nodes can locate object owners via federator
- **Heartbeats**: Nodes report cache stats periodically
- **JWT Authentication**: Per-node authentication tokens

### Federator Configuration
```toml
[federator]
enabled = true
url = "wss://federator.example.com/ws"
node_type = "cache"  # or "storage"
node_id = "node-us-east-1"
jwt_token = "eyJ..."
heartbeat_interval_seconds = 30
buckets = ["my-bucket"]
endpoint = "https://cache-us-east.example.com"
```

## Configuration

Configuration is TOML-based. See `config/default.toml` for all options.

Environment variables:
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region (default: us-east-1)
- `RUST_LOG` - Log level (trace, debug, info, warn, error)
- `CONFIG_PATH` - Config file path (default: config/default.toml)

## Dependencies

Key dependencies:
- **pingora** (0.6) - Cloudflare's proxy framework
- **tokio** - Async runtime
- **axum** (0.7) - REST API framework
- **hmac/sha2** - AWS authentication
- **dashmap** - Concurrent hash maps
- **prometheus** - Metrics
- **tokio-tungstenite** - WebSocket client for federator communication

## Reference: KodiakEdgeCache

The original Node.js implementation is at `~/k2/KodiakEdgeCache`. Key reference files:
- `usr/src/EdgeCache/EdgeCache_core.js` - Core architecture
- `usr/src/EdgeCache/aws/s3proxy.js` - S3 caching logic (406KB)
- `usr/src/EdgeCache/aws/s3auth.js` - AWS authentication
- `usr/src/EdgeCache/routes/EdgeCache_api.js` - REST API definitions

## Git Info

- **Remote:** https://github.com/barnsheltie/malloc-k2-edgecache
- **Branch:** master
