//! Scraper implementations for FOIA document sources.

#![allow(dead_code)]

pub mod archive;
pub mod config;
pub mod configurable;
pub mod discovery;
pub mod google_drive;
pub mod services;
#[allow(unused_imports)]
pub use archive::{ArchiveError, ArchiveRegistry, ArchiveSource, SnapshotInfo, WaybackSource};
#[allow(unused_imports)]
pub use config::ScraperConfig;
#[allow(unused_imports)]
pub use config::ViaMode;
pub use configurable::ConfigurableScraper;
#[cfg(feature = "browser")]
pub use foiacquire::browser::BrowserFetcher;
#[cfg(feature = "browser")]
pub use foiacquire::browser::{BrowserEngineConfig, BrowserEngineType};
pub use foiacquire::http_client::{HttpClient, HttpResponse};

// Rate limiting re-exports from foiacquire::rate_limit
#[cfg(feature = "redis-backend")]
pub use foiacquire::rate_limit::RedisRateLimitBackend;
#[allow(unused_imports)]
pub use foiacquire::rate_limit::{
    DieselRateLimitBackend, DomainRateState, InMemoryRateLimitBackend, RateLimitBackend,
    RateLimitError, RateLimiter,
};

/// Wayback Machine CDX API base URL (shared across archive and discovery modules).
pub const WAYBACK_CDX_API_URL: &str = "https://web.archive.org/cdx/search/cdx";

use std::path::Path;

use chrono::{DateTime, Utc};
use foiacquire::models::{CrawlUrl, DiscoveryMethod};
use foiacquire::repository::DieselDocumentRepository;
use foiacquire::storage::DocumentInput;

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
    /// Archive snapshot ID if this content was fetched from an archive.
    pub archive_snapshot_id: Option<i32>,
    /// When the archive captured this content (for provenance).
    pub archive_captured_at: Option<DateTime<Utc>>,
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
            archive_snapshot_id: None,
            archive_captured_at: None,
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
            archive_snapshot_id: None,
            archive_captured_at: None,
        }
    }

    /// Create a scraper result from an archive snapshot.
    ///
    /// Uses the archive's capture date as the server date for provenance.
    pub fn from_archive(
        url: String,
        title: String,
        content: Vec<u8>,
        mime_type: String,
        snapshot_id: i32,
        captured_at: DateTime<Utc>,
    ) -> Self {
        Self {
            url,
            title,
            content: Some(content),
            mime_type,
            metadata: serde_json::json!({"from_archive": true}),
            fetched_at: Utc::now(),
            etag: None,
            last_modified: None,
            not_modified: false,
            original_filename: None,
            server_date: Some(captured_at), // Use archive capture time as server date
            archive_snapshot_id: Some(snapshot_id),
            archive_captured_at: Some(captured_at),
        }
    }
}

impl From<&ScraperResult> for DocumentInput {
    fn from(result: &ScraperResult) -> Self {
        Self {
            url: result.url.clone(),
            title: result.title.clone(),
            mime_type: result.mime_type.clone(),
            metadata: result.metadata.clone(),
            original_filename: result.original_filename.clone(),
            server_date: result.server_date,
        }
    }
}

/// Save scraped document content to disk and database.
pub async fn save_scraped_document_async(
    doc_repo: &DieselDocumentRepository,
    content: &[u8],
    result: &ScraperResult,
    source_id: &str,
    documents_dir: &Path,
) -> anyhow::Result<bool> {
    foiacquire::storage::save_document_async(
        doc_repo,
        content,
        &DocumentInput::from(result),
        source_id,
        documents_dir,
    )
    .await
}

pub use foiacquire::utils::extract_title_from_url;

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
