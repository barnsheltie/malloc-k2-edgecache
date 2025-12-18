//! AWS Signature Version 4 implementation

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tracing::debug;

type HmacSha256 = Hmac<Sha256>;

/// AWS credentials
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

/// Signed request details
#[derive(Debug, Clone)]
pub struct SignedRequest {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
}

/// AWS Signature V4 signer
pub struct AwsSigV4 {
    credentials: AwsCredentials,
    service: String,
}

impl AwsSigV4 {
    /// Create a new signer with the given credentials
    pub fn new(credentials: AwsCredentials) -> Self {
        Self {
            credentials,
            service: "s3".to_string(),
        }
    }

    /// Sign an HTTP request
    pub fn sign_request(
        &self,
        method: &str,
        uri: &str,
        query_params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload: &[u8],
    ) -> SignedRequest {
        let now = Utc::now();
        self.sign_request_at(method, uri, query_params, headers, payload, now)
    }

    /// Sign an HTTP request at a specific time (useful for testing)
    pub fn sign_request_at(
        &self,
        method: &str,
        uri: &str,
        query_params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload: &[u8],
        datetime: DateTime<Utc>,
    ) -> SignedRequest {
        let date = datetime.format("%Y%m%d").to_string();
        let datetime_str = datetime.format("%Y%m%dT%H%M%SZ").to_string();

        // Calculate payload hash
        let payload_hash = sha256_hex(payload);

        // Build canonical request
        let (canonical_request, signed_headers) =
            self.build_canonical_request(method, uri, query_params, headers, &payload_hash, &datetime_str);

        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());
        debug!("Canonical request hash: {}", canonical_request_hash);

        // Build string to sign
        let string_to_sign =
            self.build_string_to_sign(&datetime_str, &date, &canonical_request_hash);
        debug!("String to sign:\n{}", string_to_sign);

        // Calculate signature
        let signing_key = self.derive_signing_key(&date);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        // Build authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request, SignedHeaders={}, Signature={}",
            self.credentials.access_key,
            date,
            self.credentials.region,
            self.service,
            signed_headers,
            signature
        );

        SignedRequest {
            authorization,
            x_amz_date: datetime_str,
            x_amz_content_sha256: payload_hash,
        }
    }

    /// Verify an incoming request signature
    pub fn verify_request(
        &self,
        method: &str,
        uri: &str,
        query_params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload: &[u8],
        provided_auth: &str,
    ) -> bool {
        // Parse the provided authorization header
        let (provided_signature, datetime_str) = match self.parse_auth_header(provided_auth, headers) {
            Some(result) => result,
            None => return false,
        };

        // Parse datetime
        let datetime = match DateTime::parse_from_str(&datetime_str, "%Y%m%dT%H%M%SZ") {
            Ok(dt) => dt.with_timezone(&Utc),
            Err(_) => return false,
        };

        // Check if request is within time tolerance (±15 minutes)
        let now = Utc::now();
        let diff = (now - datetime).num_minutes().abs();
        if diff > 15 {
            debug!("Request timestamp too old/new: {} minutes", diff);
            return false;
        }

        // Re-sign the request and compare
        let signed = self.sign_request_at(method, uri, query_params, headers, payload, datetime);

        // Extract signature from our signed request
        if let Some(our_signature) = signed.authorization.split("Signature=").nth(1) {
            our_signature == provided_signature
        } else {
            false
        }
    }

    /// Build canonical request string
    fn build_canonical_request(
        &self,
        method: &str,
        uri: &str,
        query_params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload_hash: &str,
        datetime: &str,
    ) -> (String, String) {
        // Canonical URI (URI-encode the path)
        let canonical_uri = if uri.is_empty() { "/" } else { uri };

        // Canonical query string (sorted)
        let canonical_query: String = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        // Build headers map with host and x-amz-date
        let mut canonical_headers_map: BTreeMap<String, String> = headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.trim().to_string()))
            .collect();

        // Ensure x-amz-date is included
        canonical_headers_map
            .entry("x-amz-date".to_string())
            .or_insert_with(|| datetime.to_string());

        // Ensure x-amz-content-sha256 is included
        canonical_headers_map
            .entry("x-amz-content-sha256".to_string())
            .or_insert_with(|| payload_hash.to_string());

        // Build canonical headers string
        let canonical_headers: String = canonical_headers_map
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        // Signed headers list
        let signed_headers: String = canonical_headers_map
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(";");

        // Build canonical request
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );

        (canonical_request, signed_headers)
    }

    /// Build string to sign
    fn build_string_to_sign(
        &self,
        datetime: &str,
        date: &str,
        canonical_request_hash: &str,
    ) -> String {
        format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
            datetime, date, self.credentials.region, self.service, canonical_request_hash
        )
    }

    /// Derive signing key
    fn derive_signing_key(&self, date: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.credentials.secret_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, self.credentials.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        hmac_sha256(&k_service, b"aws4_request")
    }

    /// Parse authorization header to extract signature and datetime
    fn parse_auth_header(
        &self,
        auth_header: &str,
        headers: &BTreeMap<String, String>,
    ) -> Option<(String, String)> {
        // Extract signature from auth header
        let signature = auth_header
            .split("Signature=")
            .nth(1)?
            .split(',')
            .next()?
            .trim()
            .to_string();

        // Get x-amz-date from headers
        let datetime = headers
            .get("x-amz-date")
            .or_else(|| headers.get("X-Amz-Date"))
            .cloned()?;

        Some((signature, datetime))
    }
}

/// Calculate SHA256 hash and return as hex string
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Calculate HMAC-SHA256
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// URL encode a string (AWS-style)
fn url_encode(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_credentials() -> AwsCredentials {
        AwsCredentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    #[test]
    fn test_sha256_hex() {
        let result = sha256_hex(b"");
        assert_eq!(
            result,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a/b/c"), "a%2Fb%2Fc");
    }

    #[test]
    fn test_sign_request() {
        let creds = test_credentials();
        let signer = AwsSigV4::new(creds);

        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), "examplebucket.s3.amazonaws.com".to_string());

        let query_params = BTreeMap::new();

        let signed = signer.sign_request(
            "GET",
            "/test.txt",
            &query_params,
            &headers,
            &[],
        );

        assert!(signed.authorization.starts_with("AWS4-HMAC-SHA256"));
        assert!(signed.authorization.contains("Credential=AKIAIOSFODNN7EXAMPLE"));
        assert!(!signed.x_amz_date.is_empty());
    }

    #[test]
    fn test_derive_signing_key() {
        let creds = test_credentials();
        let signer = AwsSigV4::new(creds);

        let key = signer.derive_signing_key("20130524");
        assert!(!key.is_empty());
        assert_eq!(key.len(), 32); // SHA256 produces 32 bytes
    }
}
