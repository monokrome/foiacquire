//! Scraper implementations for FOIA document sources.

#![allow(dead_code)]

pub mod browser;
pub mod config;
pub mod configurable;
pub mod google_drive;
mod http_client;
pub mod rate_limit_backend;
#[cfg(feature = "redis-backend")]
pub mod rate_limit_redis;
pub mod rate_limit_sqlite;
pub mod rate_limiter;

#[cfg(feature = "browser")]
pub use browser::BrowserFetcher;
pub use browser::{BrowserEngineConfig, BrowserEngineType};
pub use config::ScraperConfig;
pub use configurable::ConfigurableScraper;
pub use http_client::HttpClient;
pub use rate_limiter::{load_rate_limit_state, save_rate_limit_state, RateLimiter};
// ScrapeStream is defined in this file, no need to re-export
#[allow(unused_imports)]
#[cfg(feature = "redis-backend")]
pub use rate_limit_redis::RedisRateLimitBackend;

use crate::models::{CrawlUrl, DiscoveryMethod};
use chrono::{DateTime, Utc};

/// Stream of scraper results with optional total count.
pub struct ScrapeStream {
    /// Receiver for scraper results.
    pub receiver: tokio::sync::mpsc::Receiver<ScraperResult>,
    /// Total count of documents (if known from API).
    pub total_count: Option<u64>,
}

/// Result of scraping a single document.
#[derive(Debug, Clone)]
pub struct ScraperResult {
    /// Source URL of the document.
    pub url: String,
    /// Document title or filename.
    pub title: String,
    /// Raw document bytes (None if 304 Not Modified).
    pub content: Option<Vec<u8>>,
    /// MIME type of the content.
    pub mime_type: String,
    /// Additional document-specific information.
    pub metadata: serde_json::Value,
    /// Timestamp of retrieval.
    pub fetched_at: DateTime<Utc>,
    /// ETag header from response.
    pub etag: Option<String>,
    /// Last-Modified header from response.
    pub last_modified: Option<String>,
    /// True if server returned 304 Not Modified.
    pub not_modified: bool,
    /// Original filename from Content-Disposition header.
    pub original_filename: Option<String>,
    /// Server date from Last-Modified header parsed as DateTime.
    pub server_date: Option<DateTime<Utc>>,
}

impl ScraperResult {
    /// Create a new scraper result.
    pub fn new(url: String, title: String, content: Vec<u8>, mime_type: String) -> Self {
        Self {
            url,
            title,
            content: Some(content),
            mime_type,
            metadata: serde_json::json!({}),
            fetched_at: Utc::now(),
            etag: None,
            last_modified: None,
            not_modified: false,
            original_filename: None,
            server_date: None,
        }
    }

    /// Create a 304 Not Modified result.
    pub fn not_modified(url: String, etag: Option<String>, last_modified: Option<String>) -> Self {
        Self {
            url: url.clone(),
            title: extract_title_from_url(&url),
            content: None,
            mime_type: String::new(),
            metadata: serde_json::json!({"not_modified": true}),
            fetched_at: Utc::now(),
            etag,
            last_modified,
            not_modified: true,
            original_filename: None,
            server_date: None,
        }
    }
}

/// Extract document title from URL.
pub fn extract_title_from_url(url: &str) -> String {
    let path = url.split('/').next_back().unwrap_or("untitled");
    let name = path
        .trim_end_matches(".pdf")
        .trim_end_matches(".PDF")
        .trim_end_matches(".doc")
        .trim_end_matches(".docx");
    name.replace(['_', '-'], " ")
}

/// Create a CrawlUrl for tracking.
pub fn create_crawl_url(
    url: &str,
    source_id: &str,
    discovery_method: DiscoveryMethod,
    parent_url: Option<&str>,
    depth: u32,
) -> CrawlUrl {
    CrawlUrl::new(
        url.to_string(),
        source_id.to_string(),
        discovery_method,
        parent_url.map(|s| s.to_string()),
        depth,
    )
}
