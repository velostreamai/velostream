//! Gzip-compressing HTTP client wrapper for OTLP span export.
//!
//! The `opentelemetry-otlp` v0.14 HTTP exporter does not support
//! `with_compression()` (only the gRPC/tonic exporter does). This module
//! provides a wrapper around `reqwest::Client` that transparently gzip-
//! compresses request bodies and sets the `Content-Encoding: gzip` header,
//! reducing OTLP export bandwidth significantly (typically 5-10x).

use async_trait::async_trait;
use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use std::fmt;
use std::io::Write;

/// Wraps `reqwest::Client` with transparent gzip compression for OTLP HTTP export.
pub struct GzipHttpClient {
    inner: reqwest::Client,
}

impl fmt::Debug for GzipHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GzipHttpClient").finish()
    }
}

impl GzipHttpClient {
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl HttpClient for GzipHttpClient {
    async fn send(&self, request: Request<Vec<u8>>) -> Result<Response<Bytes>, HttpError> {
        let (mut parts, body) = request.into_parts();

        let original_len = body.len();

        // Compress the body with gzip (fast level for low latency)
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&body)?;
        let compressed = encoder.finish()?;

        log::debug!(
            "GzipHttpClient: compressed {} -> {} bytes ({:.0}% of original)",
            original_len,
            compressed.len(),
            (compressed.len() as f64 / original_len.max(1) as f64) * 100.0
        );

        parts
            .headers
            .insert("content-encoding", "gzip".parse().unwrap());
        let request = Request::from_parts(parts, compressed);

        self.inner.send(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gzip_http_client_debug() {
        let client = GzipHttpClient::new();
        let debug_str = format!("{:?}", client);
        assert!(debug_str.contains("GzipHttpClient"));
    }
}
