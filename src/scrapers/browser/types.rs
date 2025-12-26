//! Browser fetch response types.

/// Response from browser fetch.
#[derive(Debug, Clone)]
pub struct BrowserFetchResponse {
    pub url: String,
    pub final_url: String,
    pub status: u16,
    pub content: String,
    pub content_type: String,
    /// Cookies from the browser session (for subsequent HTTP requests).
    pub cookies: Vec<BrowserCookie>,
}

/// Cookie extracted from browser session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BrowserCookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
}

/// Response from binary fetch (PDF, images, etc).
#[derive(Debug, Clone)]
pub struct BinaryFetchResponse {
    pub url: String,
    pub status: u16,
    pub content_type: String,
    pub data: Vec<u8>,
    pub size: usize,
}
