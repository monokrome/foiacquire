//! Document download service.
//!
//! Handles downloading pending documents from the crawl queue.
//! Separated from UI concerns - emits events for progress tracking.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::models::{Document, DocumentVersion, UrlStatus};
use crate::repository::{
    extract_filename_parts, sanitize_filename, CrawlRepository, DocumentRepository,
};
use crate::scrapers::{extract_title_from_url, HttpClient};

/// Events emitted during download operations.
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
#[derive(Debug)]
#[allow(dead_code)]
pub struct DownloadResult {
    pub downloaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub remaining: u64,
}

/// Configuration for download service.
pub struct DownloadConfig {
    pub documents_dir: PathBuf,
    pub request_timeout: Duration,
    pub request_delay: Duration,
}

/// Service for downloading documents from the crawl queue.
pub struct DownloadService {
    doc_repo: Arc<DocumentRepository>,
    crawl_repo: Arc<CrawlRepository>,
    config: DownloadConfig,
}

impl DownloadService {
    /// Create a new download service.
    pub fn new(
        doc_repo: Arc<DocumentRepository>,
        crawl_repo: Arc<CrawlRepository>,
        config: DownloadConfig,
    ) -> Self {
        Self {
            doc_repo,
            crawl_repo,
            config,
        }
    }

    /// Get the number of pending documents for a source (or all sources).
    #[allow(dead_code)]
    pub fn pending_count(&self, source_id: Option<&str>) -> anyhow::Result<u64> {
        if let Some(sid) = source_id {
            Ok(self.crawl_repo.get_crawl_state(sid)?.urls_pending)
        } else {
            // Aggregate across all sources - we need source repo for this
            // For now just return 0 if no source specified
            Ok(self
                .crawl_repo
                .get_crawl_state("all")
                .map(|s| s.urls_pending)
                .unwrap_or(0))
        }
    }

    /// Download pending documents.
    ///
    /// Returns a channel receiver for progress events and spawns worker tasks.
    /// Call `await` on the returned future to get the final result.
    pub async fn download(
        &self,
        source_id: Option<&str>,
        workers: usize,
        limit: Option<usize>,
        event_tx: mpsc::Sender<DownloadEvent>,
    ) -> anyhow::Result<DownloadResult> {
        let downloaded = Arc::new(AtomicUsize::new(0));
        let skipped = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(workers);

        for worker_id in 0..workers {
            let crawl_repo = self.crawl_repo.clone();
            let doc_repo = self.doc_repo.clone();
            let documents_dir = self.config.documents_dir.clone();
            let timeout = self.config.request_timeout;
            let delay = self.config.request_delay;
            let source_id = source_id.map(|s| s.to_string());
            let downloaded = downloaded.clone();
            let skipped = skipped.clone();
            let failed = failed.clone();
            let event_tx = event_tx.clone();

            let handle = tokio::spawn(async move {
                let client = HttpClient::new("download", timeout, delay);

                loop {
                    // Check limit
                    if let Some(max) = limit {
                        if downloaded.load(Ordering::Relaxed) >= max {
                            break;
                        }
                    }

                    // Claim a URL to process
                    let crawl_url = match crawl_repo.claim_pending_url(source_id.as_deref()) {
                        Ok(Some(url)) => url,
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            match crawl_repo.claim_pending_url(source_id.as_deref()) {
                                Ok(Some(url)) => url,
                                _ => break,
                            }
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    };

                    let url = crawl_url.url.clone();
                    let filename = extract_title_from_url(&url);

                    let _ = event_tx
                        .send(DownloadEvent::Started {
                            worker_id,
                            url: url.clone(),
                            filename: filename.clone(),
                        })
                        .await;

                    // Fetch the URL
                    let response = match client
                        .get(
                            &url,
                            crawl_url.etag.as_deref(),
                            crawl_url.last_modified.as_deref(),
                        )
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            let mut failed_url = crawl_url.clone();
                            failed_url.status = UrlStatus::Failed;
                            failed_url.last_error = Some(e.to_string());
                            failed_url.retry_count += 1;
                            let _ = crawl_repo.update_url(&failed_url);
                            failed.fetch_add(1, Ordering::Relaxed);
                            let _ = event_tx
                                .send(DownloadEvent::Failed {
                                    worker_id,
                                    url,
                                    error: e.to_string(),
                                })
                                .await;
                            continue;
                        }
                    };

                    if response.is_not_modified() {
                        let mut fetched_url = crawl_url.clone();
                        fetched_url.status = UrlStatus::Fetched;
                        fetched_url.fetched_at = Some(chrono::Utc::now());
                        let _ = crawl_repo.update_url(&fetched_url);
                        skipped.fetch_add(1, Ordering::Relaxed);
                        let _ = event_tx
                            .send(DownloadEvent::Unchanged { worker_id, url })
                            .await;
                        continue;
                    }

                    if !response.is_success() {
                        let mut failed_url = crawl_url.clone();
                        failed_url.status = UrlStatus::Failed;
                        failed_url.last_error = Some(format!("HTTP {}", response.status));
                        failed_url.retry_count += 1;
                        let _ = crawl_repo.update_url(&failed_url);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let _ = event_tx
                            .send(DownloadEvent::Failed {
                                worker_id,
                                url,
                                error: format!("HTTP {}", response.status),
                            })
                            .await;
                        continue;
                    }

                    // Extract metadata before consuming response
                    let disposition_filename = response.content_disposition_filename();
                    let title = disposition_filename
                        .clone()
                        .unwrap_or_else(|| extract_title_from_url(&url));
                    let mime_type = response
                        .content_type()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "application/octet-stream".to_string());
                    let etag = response.etag().map(|s| s.to_string());
                    let last_modified = response.last_modified().map(|s| s.to_string());
                    let server_date = last_modified.as_ref().and_then(|lm| {
                        chrono::DateTime::parse_from_rfc2822(lm)
                            .ok()
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                    });

                    let content = match response.bytes().await {
                        Ok(b) => b,
                        Err(e) => {
                            let mut failed_url = crawl_url.clone();
                            failed_url.status = UrlStatus::Failed;
                            failed_url.last_error = Some(e.to_string());
                            let _ = crawl_repo.update_url(&failed_url);
                            failed.fetch_add(1, Ordering::Relaxed);
                            let _ = event_tx
                                .send(DownloadEvent::Failed {
                                    worker_id,
                                    url,
                                    error: e.to_string(),
                                })
                                .await;
                            continue;
                        }
                    };

                    let _ = event_tx
                        .send(DownloadEvent::Progress {
                            worker_id,
                            bytes: content.len() as u64,
                            total: Some(content.len() as u64),
                        })
                        .await;

                    // Save document
                    let content_hash = DocumentVersion::compute_hash(&content);
                    let (basename, extension) = extract_filename_parts(&url, &title, &mime_type);
                    let filename = format!(
                        "{}-{}.{}",
                        sanitize_filename(&basename),
                        &content_hash[..8],
                        extension
                    );

                    let content_path = documents_dir.join(&content_hash[..2]).join(&filename);

                    if let Err(e) = std::fs::create_dir_all(content_path.parent().unwrap()) {
                        failed.fetch_add(1, Ordering::Relaxed);
                        let _ = event_tx
                            .send(DownloadEvent::Failed {
                                worker_id,
                                url,
                                error: e.to_string(),
                            })
                            .await;
                        continue;
                    }

                    if let Err(e) = std::fs::write(&content_path, &content) {
                        failed.fetch_add(1, Ordering::Relaxed);
                        let _ = event_tx
                            .send(DownloadEvent::Failed {
                                worker_id,
                                url,
                                error: e.to_string(),
                            })
                            .await;
                        continue;
                    }

                    let version = DocumentVersion::new_with_metadata(
                        &content,
                        content_path,
                        mime_type.clone(),
                        Some(url.clone()),
                        disposition_filename,
                        server_date,
                    );

                    // Check for existing document
                    let existing = doc_repo.get_by_url(&url).ok().flatten();
                    let new_document = existing.is_none();

                    if let Some(mut doc) = existing {
                        if doc.add_version(version) {
                            let _ = doc_repo.save(&doc);
                        }
                    } else {
                        let doc = Document::with_discovery_method(
                            uuid::Uuid::new_v4().to_string(),
                            crawl_url.source_id.clone(),
                            title,
                            url.clone(),
                            version,
                            serde_json::json!({}),
                            "crawl".to_string(),
                        );
                        let _ = doc_repo.save(&doc);
                    }

                    // Mark URL as fetched
                    let mut fetched_url = crawl_url.clone();
                    fetched_url.status = UrlStatus::Fetched;
                    fetched_url.fetched_at = Some(chrono::Utc::now());
                    fetched_url.etag = etag;
                    fetched_url.last_modified = last_modified;
                    fetched_url.content_hash = Some(content_hash);
                    let _ = crawl_repo.update_url(&fetched_url);

                    downloaded.fetch_add(1, Ordering::Relaxed);
                    let _ = event_tx
                        .send(DownloadEvent::Completed {
                            worker_id,
                            url,
                            new_document,
                        })
                        .await;
                }
            });

            handles.push(handle);
        }

        // Wait for all workers
        for handle in handles {
            let _ = handle.await;
        }

        // Get remaining count
        let remaining = if let Some(sid) = source_id {
            self.crawl_repo.get_crawl_state(sid)?.urls_pending
        } else {
            0
        };

        Ok(DownloadResult {
            downloaded: downloaded.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
            failed: failed.load(Ordering::Relaxed),
            remaining,
        })
    }
}
