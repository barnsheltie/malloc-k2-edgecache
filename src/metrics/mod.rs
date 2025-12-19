//! Metrics module for Prometheus monitoring

use prometheus::{
    Counter, Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry,
    TextEncoder,
};
use std::sync::OnceLock;

static REGISTRY: OnceLock<Metrics> = OnceLock::new();

/// Application metrics
pub struct Metrics {
    pub registry: Registry,

    // Request metrics
    pub requests_total: IntCounter,
    pub requests_by_method: prometheus::IntCounterVec,
    pub requests_by_status: prometheus::IntCounterVec,
    pub request_duration_seconds: Histogram,

    // Cache metrics
    pub cache_hits_total: IntCounter,
    pub cache_misses_total: IntCounter,
    pub cache_memory_bytes: IntGauge,
    pub cache_disk_bytes: IntGauge,
    pub cache_objects_total: IntGauge,

    // S3 upstream metrics
    pub s3_requests_total: IntCounter,
    pub s3_errors_total: IntCounter,
    pub s3_request_duration_seconds: Histogram,

    // Connection metrics
    pub active_connections: IntGauge,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();

        // Request metrics
        let requests_total = IntCounter::new("requests_total", "Total number of requests")
            .expect("metric can be created");
        registry.register(Box::new(requests_total.clone())).unwrap();

        let requests_by_method = prometheus::IntCounterVec::new(
            Opts::new("requests_by_method_total", "Requests by HTTP method"),
            &["method"],
        )
        .expect("metric can be created");
        registry
            .register(Box::new(requests_by_method.clone()))
            .unwrap();

        let requests_by_status = prometheus::IntCounterVec::new(
            Opts::new("requests_by_status_total", "Requests by HTTP status code"),
            &["status"],
        )
        .expect("metric can be created");
        registry
            .register(Box::new(requests_by_status.clone()))
            .unwrap();

        let request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new("request_duration_seconds", "Request duration in seconds")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        )
        .expect("metric can be created");
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .unwrap();

        // Cache metrics
        let cache_hits_total = IntCounter::new("cache_hits_total", "Total cache hits")
            .expect("metric can be created");
        registry
            .register(Box::new(cache_hits_total.clone()))
            .unwrap();

        let cache_misses_total = IntCounter::new("cache_misses_total", "Total cache misses")
            .expect("metric can be created");
        registry
            .register(Box::new(cache_misses_total.clone()))
            .unwrap();

        let cache_memory_bytes =
            IntGauge::new("cache_memory_bytes", "Memory cache size in bytes")
                .expect("metric can be created");
        registry
            .register(Box::new(cache_memory_bytes.clone()))
            .unwrap();

        let cache_disk_bytes = IntGauge::new("cache_disk_bytes", "Disk cache size in bytes")
            .expect("metric can be created");
        registry
            .register(Box::new(cache_disk_bytes.clone()))
            .unwrap();

        let cache_objects_total =
            IntGauge::new("cache_objects_total", "Total number of cached objects")
                .expect("metric can be created");
        registry
            .register(Box::new(cache_objects_total.clone()))
            .unwrap();

        // S3 upstream metrics
        let s3_requests_total =
            IntCounter::new("s3_requests_total", "Total S3 upstream requests")
                .expect("metric can be created");
        registry
            .register(Box::new(s3_requests_total.clone()))
            .unwrap();

        let s3_errors_total = IntCounter::new("s3_errors_total", "Total S3 upstream errors")
            .expect("metric can be created");
        registry
            .register(Box::new(s3_errors_total.clone()))
            .unwrap();

        let s3_request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "s3_request_duration_seconds",
                "S3 upstream request duration in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        )
        .expect("metric can be created");
        registry
            .register(Box::new(s3_request_duration_seconds.clone()))
            .unwrap();

        // Connection metrics
        let active_connections =
            IntGauge::new("active_connections", "Number of active connections")
                .expect("metric can be created");
        registry
            .register(Box::new(active_connections.clone()))
            .unwrap();

        Self {
            registry,
            requests_total,
            requests_by_method,
            requests_by_status,
            request_duration_seconds,
            cache_hits_total,
            cache_misses_total,
            cache_memory_bytes,
            cache_disk_bytes,
            cache_objects_total,
            s3_requests_total,
            s3_errors_total,
            s3_request_duration_seconds,
            active_connections,
        }
    }
}

/// Initialize metrics (call once at startup)
pub fn init() {
    REGISTRY.get_or_init(Metrics::new);
}

/// Get the global metrics instance
pub fn get() -> &'static Metrics {
    REGISTRY.get_or_init(Metrics::new)
}

/// Encode metrics in Prometheus text format
pub fn encode() -> String {
    let metrics = get();
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();

    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Record a cache hit
pub fn record_cache_hit() {
    get().cache_hits_total.inc();
}

/// Record a cache miss
pub fn record_cache_miss() {
    get().cache_misses_total.inc();
}

/// Record a request
pub fn record_request(method: &str, status: u16, duration_seconds: f64) {
    let m = get();
    m.requests_total.inc();
    m.requests_by_method.with_label_values(&[method]).inc();
    m.requests_by_status
        .with_label_values(&[&status.to_string()])
        .inc();
    m.request_duration_seconds.observe(duration_seconds);
}

/// Record an S3 upstream request
pub fn record_s3_request(duration_seconds: f64, is_error: bool) {
    let m = get();
    m.s3_requests_total.inc();
    m.s3_request_duration_seconds.observe(duration_seconds);
    if is_error {
        m.s3_errors_total.inc();
    }
}

/// Update cache size metrics
pub fn update_cache_sizes(memory_bytes: u64, disk_bytes: u64, total_objects: u64) {
    let m = get();
    m.cache_memory_bytes.set(memory_bytes as i64);
    m.cache_disk_bytes.set(disk_bytes as i64);
    m.cache_objects_total.set(total_objects as i64);
}

/// Update active connections count
pub fn set_active_connections(count: i64) {
    get().active_connections.set(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        init();
        let m = get();
        // Just verify metrics are accessible (can't check for 0 since tests run in parallel)
        let _ = m.requests_total.get();
        let _ = m.cache_hits_total.get();
        let _ = m.cache_misses_total.get();
    }

    #[test]
    fn test_record_request() {
        init();
        record_request("GET", 200, 0.1);
        let m = get();
        assert!(m.requests_total.get() >= 1);
    }

    #[test]
    fn test_encode_metrics() {
        init();
        let output = encode();
        assert!(output.contains("requests_total"));
        assert!(output.contains("cache_hits_total"));
    }
}
