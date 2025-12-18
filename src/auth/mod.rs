//! Authentication module for AWS S3 requests

mod aws_sigv4;

pub use aws_sigv4::{AwsCredentials, AwsSigV4, SignedRequest};

use crate::config::Config;
use std::sync::Arc;

/// Authentication manager
pub struct AuthManager {
    config: Arc<Config>,
}

impl AuthManager {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Get AWS credentials for a bucket
    pub fn get_credentials(&self, bucket: Option<&str>) -> Option<AwsCredentials> {
        // Check bucket-specific credentials first
        if let Some(bucket_name) = bucket {
            if let Some(bucket_cfg) = self.config.buckets.iter().find(|b| b.name == bucket_name) {
                if let (Some(access_key), Some(secret_key)) =
                    (&bucket_cfg.access_key, &bucket_cfg.secret_key)
                {
                    return Some(AwsCredentials {
                        access_key: access_key.clone(),
                        secret_key: secret_key.clone(),
                        region: bucket_cfg
                            .region
                            .clone()
                            .unwrap_or_else(|| self.config.s3.region.clone()),
                    });
                }
            }
        }

        // Fall back to default credentials
        let access_key = self.config.s3.access_key.clone().or_else(|| {
            std::env::var("AWS_ACCESS_KEY_ID").ok()
        })?;

        let secret_key = self.config.s3.secret_key.clone().or_else(|| {
            std::env::var("AWS_SECRET_ACCESS_KEY").ok()
        })?;

        Some(AwsCredentials {
            access_key,
            secret_key,
            region: std::env::var("AWS_REGION")
                .unwrap_or_else(|_| self.config.s3.region.clone()),
        })
    }

    /// Create a signer with the appropriate credentials
    pub fn create_signer(&self, bucket: Option<&str>) -> Option<AwsSigV4> {
        let creds = self.get_credentials(bucket)?;
        Some(AwsSigV4::new(creds))
    }
}
