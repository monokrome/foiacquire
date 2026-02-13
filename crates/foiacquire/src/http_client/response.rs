//! HTTP response wrappers.

use std::collections::HashMap;

use reqwest::{Response, StatusCode};

/// Response body source - either pending (reqwest) or already fetched (browser).
pub(crate) enum ResponseBody {
    /// Pending response from reqwest.
    Pending(Response),
    /// Already fetched content (from browser).
    Ready(Vec<u8>),
}

/// HTTP response wrapper.
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
    pub(crate) body: ResponseBody,
}

impl HttpResponse {
    /// Create from a reqwest response.
    pub(crate) fn from_reqwest(
        status: StatusCode,
        headers: HashMap<String, String>,
        response: Response,
    ) -> Self {
        Self {
            status,
            headers,
            body: ResponseBody::Pending(response),
        }
    }

    /// Create from already-fetched content (browser).
    pub(crate) fn from_bytes(
        status: StatusCode,
        headers: HashMap<String, String>,
        content: Vec<u8>,
    ) -> Self {
        Self {
            status,
            headers,
            body: ResponseBody::Ready(content),
        }
    }

    /// Check if the response is 304 Not Modified.
    pub fn is_not_modified(&self) -> bool {
        self.status == StatusCode::NOT_MODIFIED
    }

    /// Check if the response is successful.
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the response indicates rate limiting (429 or 503).
    pub fn is_rate_limited(&self) -> bool {
        let code = self.status.as_u16();
        code == 429 || code == 503
    }

    /// Get the ETag header.
    pub fn etag(&self) -> Option<&str> {
        self.headers.get("etag").map(|s| s.as_str())
    }

    /// Get the Last-Modified header.
    pub fn last_modified(&self) -> Option<&str> {
        self.headers.get("last-modified").map(|s| s.as_str())
    }

    /// Get the Content-Type header.
    pub fn content_type(&self) -> Option<&str> {
        self.headers.get("content-type").map(|s| s.as_str())
    }

    /// Get the Content-Length header.
    pub fn content_length(&self) -> Option<u64> {
        self.headers
            .get("content-length")
            .and_then(|s| s.parse().ok())
    }

    /// Get the filename from Content-Disposition header.
    pub fn content_disposition_filename(&self) -> Option<String> {
        self.headers
            .get("content-disposition")
            .and_then(|h| parse_content_disposition_filename(h))
    }

    /// Get response body as bytes.
    pub async fn bytes(self) -> Result<Vec<u8>, reqwest::Error> {
        match self.body {
            ResponseBody::Pending(response) => response.bytes().await.map(|b| b.to_vec()),
            ResponseBody::Ready(bytes) => Ok(bytes),
        }
    }

    /// Get response body as text.
    pub async fn text(self) -> Result<String, reqwest::Error> {
        match self.body {
            ResponseBody::Pending(response) => response.text().await,
            ResponseBody::Ready(bytes) => {
                // Best effort UTF-8 conversion
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            }
        }
    }

    /// Deserialize response body as JSON.
    ///
    /// Note: This should only be called on streaming (Pending) responses.
    /// Cached responses don't support JSON deserialization.
    pub async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T, reqwest::Error> {
        match self.body {
            ResponseBody::Pending(response) => response.json().await,
            ResponseBody::Ready(_) => {
                // JSON API responses are never cached, so this should never happen
                panic!("Cannot deserialize JSON from cached response - JSON responses should not be cached")
            }
        }
    }
}

/// HEAD response wrapper (no body, just headers).
pub struct HeadResponse {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
}

impl HeadResponse {
    /// Check if the response is 304 Not Modified.
    pub fn is_not_modified(&self) -> bool {
        self.status == StatusCode::NOT_MODIFIED
    }

    /// Check if the response is successful.
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the response indicates rate limiting (429 or 503).
    pub fn is_rate_limited(&self) -> bool {
        let code = self.status.as_u16();
        code == 429 || code == 503
    }

    /// Get the ETag header.
    pub fn etag(&self) -> Option<&str> {
        self.headers.get("etag").map(|s| s.as_str())
    }

    /// Get the Last-Modified header.
    pub fn last_modified(&self) -> Option<&str> {
        self.headers.get("last-modified").map(|s| s.as_str())
    }

    /// Get the Content-Type header.
    pub fn content_type(&self) -> Option<&str> {
        self.headers.get("content-type").map(|s| s.as_str())
    }

    /// Get the Content-Length header.
    pub fn content_length(&self) -> Option<u64> {
        self.headers
            .get("content-length")
            .and_then(|s| s.parse().ok())
    }

    /// Get the filename from Content-Disposition header.
    pub fn content_disposition_filename(&self) -> Option<String> {
        self.headers
            .get("content-disposition")
            .and_then(|h| parse_content_disposition_filename(h))
    }
}

/// Parse filename from Content-Disposition header value.
/// Parses both `filename="name.pdf"` and `filename*=UTF-8''name.pdf` formats.
pub fn parse_content_disposition_filename(header: &str) -> Option<String> {
    // Try filename*= first (RFC 5987 encoded)
    if let Some(start) = header.find("filename*=") {
        let rest = &header[start + 10..];
        if let Some(quote_start) = rest.find("''") {
            let encoded = rest[quote_start + 2..].split([';', ' ']).next()?;
            if let Ok(decoded) = urlencoding::decode(encoded) {
                let filename = decoded.trim().to_string();
                if !filename.is_empty() {
                    return Some(filename);
                }
            }
        }
    }

    // Try filename= (standard format)
    if let Some(start) = header.find("filename=") {
        let rest = &header[start + 9..];
        let filename = if let Some(quoted) = rest.strip_prefix('"') {
            quoted.split('"').next()
        } else {
            rest.split([';', ' ']).next()
        };

        if let Some(name) = filename {
            let name = name.trim().to_string();
            if !name.is_empty() {
                return Some(name);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_disposition_quoted() {
        let header = r#"attachment; filename="document.pdf""#;
        assert_eq!(
            parse_content_disposition_filename(header),
            Some("document.pdf".to_string())
        );
    }

    #[test]
    fn test_parse_content_disposition_unquoted() {
        let header = "attachment; filename=document.pdf";
        assert_eq!(
            parse_content_disposition_filename(header),
            Some("document.pdf".to_string())
        );
    }

    #[test]
    fn test_parse_content_disposition_rfc5987() {
        let header = "attachment; filename*=UTF-8''my%20document.pdf";
        assert_eq!(
            parse_content_disposition_filename(header),
            Some("my document.pdf".to_string())
        );
    }

    #[test]
    fn test_parse_content_disposition_both_formats() {
        // RFC 5987 should take precedence
        let header = r#"attachment; filename="fallback.pdf"; filename*=UTF-8''preferred.pdf"#;
        assert_eq!(
            parse_content_disposition_filename(header),
            Some("preferred.pdf".to_string())
        );
    }

    #[test]
    fn test_parse_content_disposition_none() {
        assert_eq!(parse_content_disposition_filename("attachment"), None);
        assert_eq!(parse_content_disposition_filename("inline"), None);
    }
}
