# malloc-k2-edgecache

A high-performance S3 caching proxy built on [Cloudflare Pingora](https://github.com/cloudflare/pingora).

## Overview

malloc-k2-edgecache is an edge caching proxy server that sits between clients and AWS S3 (or S3-compatible storage). It caches objects locally to reduce latency, bandwidth costs, and load on origin storage.

### Key Features

- **High Performance**: Built on Pingora, the same framework powering Cloudflare's edge network
- **Memory Safe**: Written in Rust for reliability and security
- **Dual-Layer Caching**: In-memory LRU cache + disk-based persistent cache
- **S3 Compatible**: Works with AWS S3 and S3-compatible services (MinIO, DigitalOcean Spaces, etc.)
- **AWS Signature V4**: Full support for S3 authentication
- **REST API**: Management interface with Swagger/OpenAPI documentation
- **Prometheus Metrics**: Built-in observability
- **Configurable**: TOML-based configuration with per-bucket settings

## Architecture

```
┌─────────────┐     ┌─────────────────────────────────────────┐     ┌─────────────┐
│   Clients   │────▶│         malloc-k2-edgecache             │────▶│   AWS S3    │
└─────────────┘     │  ┌─────────┐  ┌──────────┐  ┌────────┐  │     └─────────────┘
                    │  │ Memory  │  │   Disk   │  │ Proxy  │  │
                    │  │  Cache  │  │  Cache   │  │ Layer  │  │
                    │  └─────────┘  └──────────┘  └────────┘  │
                    │         ▲                               │
                    │         │      ┌──────────┐             │
                    │         └──────│ REST API │◀── :14000   │
                    │                └──────────┘             │
                    └─────────────────────────────────────────┘
                              ▲              ▲
                              │              │
                           :9000          :9001
                           (HTTP)        (HTTPS)
```

## Quick Start

### Prerequisites

- Rust 1.75+ (stable)
- Linux x86_64 or aarch64

### Build

```bash
# Clone the repository
git clone https://github.com/your-org/malloc-k2-edgecache.git
cd malloc-k2-edgecache

# Build release binary
cargo build --release

# Run
./target/release/malloc-k2-edgecache --config config/default.toml
```

### Docker

```bash
# Build image
docker build -t malloc-k2-edgecache -f packages/docker/Dockerfile .

# Run
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -p 14000:14000 \
  -v /var/cache/edgecache:/var/cache/malloc-k2-edgecache \
  malloc-k2-edgecache
```

## Configuration

Configuration is done via TOML files. See `config/default.toml` for all options.

```toml
[server]
http_port = 9000
https_port = 9001
api_port = 14000
workers = 4

[cache]
enabled = true
disk_path = "/var/cache/malloc-k2-edgecache"
disk_max_size_gb = 100
memory_max_size_mb = 1024
default_ttl_seconds = 3600

[s3]
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"

[[buckets]]
name = "my-bucket"
cache_ttl = 7200
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |
| `AWS_REGION` | AWS region | `us-east-1` |
| `RUST_LOG` | Log level | `info` |
| `CONFIG_PATH` | Config file path | `config/default.toml` |

## API Endpoints

The REST API runs on port 14000 by default.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/version` | GET | Version info |
| `/api/config` | GET | Current configuration |
| `/api/cache/stats` | GET | Cache statistics |
| `/api/cache/clear` | POST | Clear cache |
| `/api/buckets` | GET | List registered buckets |
| `/api/buckets` | POST | Register a bucket |
| `/swagger-ui` | GET | Swagger UI |

## Default Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9000 | HTTP | S3 proxy |
| 9001 | HTTPS | S3 proxy (TLS) |
| 14000 | HTTP | REST API |

## Performance

malloc-k2-edgecache is designed for high throughput:

- **Throughput**: 100k+ requests/second (single node)
- **Latency**: <1ms cache hits (memory), <5ms cache hits (disk)
- **Memory**: ~50MB base + cache size
- **Binary Size**: ~15MB (stripped)

## Development

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- --config config/default.toml

# Format code
cargo fmt

# Lint
cargo clippy
```

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Acknowledgments

- [Cloudflare Pingora](https://github.com/cloudflare/pingora) - The foundation of this project
- Original inspiration from [KodiakEdgeCache](https://github.com/kodiakdata/KodiakEdgeCache)
