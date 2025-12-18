//! malloc-k2-edgecache library
//!
//! This crate provides the core functionality for the S3 caching proxy.

pub mod api;
pub mod auth;
pub mod cache;
pub mod config;
pub mod metrics;
pub mod proxy;

pub use config::Config;
