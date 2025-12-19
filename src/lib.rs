//! malloc-k2-edgecache library
//!
//! This crate provides the core functionality for the S3 caching proxy.

pub mod config;
pub mod auth;
pub mod cache;
pub mod metrics;
pub mod multipart;
pub mod writeback;
pub mod federator;
pub mod api;
pub mod proxy;

pub use config::Config;
