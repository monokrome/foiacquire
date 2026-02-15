//! Download service types and events.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use tracing::warn;

use crate::config::ViaMode;
use foia::models::{CrawlUrl, Document, DocumentVersion, UrlStatus};
use foia::privacy::PrivacyConfig;
use foia::repository::{DieselCrawlRepository, DieselDocumentRepository};

/// Events emitted during download operations.
/// Fields are populated when events are created, even if consumers don't read all of them.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum DownloadEvent {
    /// Download started for a URL
    Started {
        worker_id: usize,
        url: String,
        filename: String,
    },
    /// Progress update (bytes downloaded)
    Progress {
        worker_id: usize,
        bytes: u64,
        total: Option<u64>,
    },
    /// Download completed successfully
    Completed {
        worker_id: usize,
        url: String,
        new_document: bool,
    },
    /// File deduplicated (identical content already exists)
    Deduplicated {
        worker_id: usize,
        url: String,
        existing_path: String,
    },
    /// Document unchanged (304 Not Modified)
    Unchanged { worker_id: usize, url: String },
    /// Download failed
    Failed {
        worker_id: usize,
        url: String,
        error: String,
    },
}

/// Result of a download operation.
/// Part of public API - consumers may use any field even if current CLI doesn't read all.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DownloadResult {
    pub downloaded: usize,
    pub deduplicated: usize,
    pub skipped: usize,
    pub failed: usize,
    pub remaining: u64,
}

/// Configuration for download service.
pub struct DownloadConfig {
    pub documents_dir: PathBuf,
    pub request_timeout: Duration,
    pub request_delay: Duration,
    /// Privacy configuration for HTTP requests.
    pub privacy: PrivacyConfig,
    /// URL rewriting for caching proxies.
    pub via: HashMap<String, String>,
    /// Via mode controlling when via mappings are used.
    pub via_mode: ViaMode,
}

/// Handle a download failure: update status, increment counter, send event.
pub async fn handle_download_failure(
    crawl_url: &CrawlUrl,
    crawl_repo: &Arc<DieselCrawlRepository>,
    failed: &Arc<AtomicUsize>,
    event_tx: &mpsc::Sender<DownloadEvent>,
    worker_id: usize,
    error: &str,
    increment_retry: bool,
) {
    let mut failed_url = crawl_url.clone();
    failed_url.status = UrlStatus::Failed;
    failed_url.last_error = Some(error.to_string());
    if increment_retry {
        failed_url.retry_count += 1;
    }
    if let Err(e) = crawl_repo.update_url(&failed_url).await {
        warn!(
            "Failed to update crawl URL status for {}: {}",
            crawl_url.url, e
        );
    }
    failed.fetch_add(1, Ordering::Relaxed);
    let _ = event_tx
        .send(DownloadEvent::Failed {
            worker_id,
            url: crawl_url.url.clone(),
            error: error.to_string(),
        })
        .await;
}

/// Send a failure event without updating crawl status (for local errors like IO).
pub async fn send_failure_event(
    url: &str,
    failed: &Arc<AtomicUsize>,
    event_tx: &mpsc::Sender<DownloadEvent>,
    worker_id: usize,
    error: &str,
) {
    failed.fetch_add(1, Ordering::Relaxed);
    let _ = event_tx
        .send(DownloadEvent::Failed {
            worker_id,
            url: url.to_string(),
            error: error.to_string(),
        })
        .await;
}

/// Mark a URL as unchanged (304 Not Modified).
pub async fn handle_unchanged(
    crawl_url: &CrawlUrl,
    crawl_repo: &Arc<DieselCrawlRepository>,
    skipped: &Arc<AtomicUsize>,
    event_tx: &mpsc::Sender<DownloadEvent>,
    worker_id: usize,
) {
    let mut fetched_url = crawl_url.clone();
    fetched_url.status = UrlStatus::Fetched;
    fetched_url.fetched_at = Some(chrono::Utc::now());
    if let Err(e) = crawl_repo.update_url(&fetched_url).await {
        warn!(
            "Failed to update crawl URL status for {}: {}",
            crawl_url.url, e
        );
    }
    skipped.fetch_add(1, Ordering::Relaxed);
    let _ = event_tx
        .send(DownloadEvent::Unchanged {
            worker_id,
            url: crawl_url.url.clone(),
        })
        .await;
}

/// Save a document version, either adding to existing document or creating new.
/// Returns whether this created a new document.
#[allow(clippy::too_many_arguments)]
pub async fn save_or_update_document(
    doc_repo: &Arc<DieselDocumentRepository>,
    url: &str,
    source_id: &str,
    title: String,
    version: DocumentVersion,
    metadata: serde_json::Value,
    discovery_method: &str,
) -> Result<bool, foia::repository::DieselError> {
    let existing = doc_repo.get_by_url(url).await?.into_iter().next();
    let new_document = existing.is_none();

    if let Some(mut doc) = existing {
        if doc.add_version(version) {
            doc_repo.save_with_versions(&doc).await?;
        }
    } else {
        let doc = Document::with_discovery_method(
            uuid::Uuid::new_v4().to_string(),
            source_id.to_string(),
            title,
            url.to_string(),
            version,
            metadata,
            discovery_method.to_string(),
        );
        doc_repo.save_with_versions(&doc).await?;
    }

    Ok(new_document)
}
