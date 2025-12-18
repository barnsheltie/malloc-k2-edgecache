# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

malloc-k2-edgecache is a high-performance S3 caching proxy built on Cloudflare's Pingora framework. It sits between clients and AWS S3 (or S3-compatible storage), caching objects locally to reduce latency, bandwidth costs, and load on origin storage.

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
├── config.rs        # Configuration structures and loading
├── api/             # REST API (axum-based)
│   ├── mod.rs       # API server setup
│   ├── routes.rs    # Route definitions
│   └── handlers.rs  # Request handlers
├── auth/            # Authentication
│   ├── mod.rs       # Auth manager
│   └── aws_sigv4.rs # AWS Signature V4 implementation
├── cache/           # Caching layer
│   ├── mod.rs       # Cache manager (coordinates memory + disk)
│   ├── cache_key.rs # Cache key generation (SHA256-based)
│   ├── memory_cache.rs # In-memory LRU cache
│   └── disk_cache.rs   # Disk-based persistent cache
├── metrics/         # Prometheus metrics
│   └── mod.rs
└── proxy/           # Pingora proxy implementation
    ├── mod.rs
    └── s3_proxy.rs  # S3 proxy handler (ProxyHttp trait)
```

### Key Components

- **Proxy Layer** (`proxy/s3_proxy.rs`): Implements Pingora's `ProxyHttp` trait to intercept and cache S3 requests
- **Cache Manager** (`cache/mod.rs`): Two-tier caching with memory (LRU) and disk (persistent) layers
- **Auth Module** (`auth/aws_sigv4.rs`): AWS Signature V4 signing and verification
- **REST API** (`api/`): Management endpoints on port 14000

### Default Ports

| Port  | Purpose        |
|-------|----------------|
| 9000  | HTTP S3 proxy  |
| 9001  | HTTPS S3 proxy |
| 14000 | REST API       |

## Configuration

Configuration is TOML-based. See `config/default.toml` for all options.

Environment variables:
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region (default: us-east-1)
- `RUST_LOG` - Log level (trace, debug, info, warn, error)
- `CONFIG_PATH` - Config file path (default: config/default.toml)

## Testing

```bash
# Run all tests
cargo test

# Run cache tests
cargo test cache::

# Run auth tests
cargo test auth::

# Run with coverage (requires cargo-tarpaulin)
cargo tarpaulin
```

## Dependencies

Key dependencies:
- **pingora** (0.4) - Cloudflare's proxy framework
- **tokio** - Async runtime
- **axum** - REST API framework
- **aws-sigv4** - AWS authentication
- **dashmap** - Concurrent hash maps
- **prometheus** - Metrics

## Adding New Features

1. **New cache backend**: Implement the cache trait pattern in `src/cache/`
2. **New auth method**: Add module under `src/auth/` and register in `AuthManager`
3. **New API endpoint**: Add route in `api/routes.rs` and handler in `api/handlers.rs`
4. **New metrics**: Add to `metrics/mod.rs` and call recording functions

## Reference

This project is inspired by KodiakEdgeCache and aims for feature parity:
- Full S3 API proxy support
- Memory + disk caching with TTL
- Write-back cache mode (planned)
- AWS SigV4 authentication
- REST API for management
- Prometheus metrics
