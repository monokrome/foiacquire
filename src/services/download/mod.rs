//! Document download service.
//!
//! Handles downloading pending documents from the crawl queue.
//! Separated from UI concerns - emits events for progress tracking.

mod types;
mod youtube_download;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::cli::helpers::content_storage_path_with_name;
use crate::models::{DocumentVersion, UrlStatus};
use crate::repository::{extract_filename_parts, DieselCrawlRepository, DieselDocumentRepository};
use crate::scrapers::{extract_title_from_url, HttpClient};
use crate::services::youtube;

pub use types::{DownloadConfig, DownloadEvent, DownloadResult};
use types::{
    handle_download_failure, handle_unchanged, save_or_update_document, send_failure_event,
};
use youtube_download::download_youtube_video;

/// Service for downloading documents from the crawl queue.
pub struct DownloadService {
    doc_repo: Arc<DieselDocumentRepository>,
    crawl_repo: Arc<DieselCrawlRepository>,
    config: DownloadConfig,
}

impl DownloadService {
    /// Create a new download service.
    pub fn new(
        doc_repo: Arc<DieselDocumentRepository>,
        crawl_repo: Arc<DieselCrawlRepository>,
        config: DownloadConfig,
    ) -> Self {
        Self {
            doc_repo,
            crawl_repo,
            config,
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
        let deduplicated = Arc::new(AtomicUsize::new(0));
        let skipped = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(workers);

        for worker_id in 0..workers {
            let crawl_repo = self.crawl_repo.clone();
            let doc_repo = self.doc_repo.clone();
            let documents_dir = self.config.documents_dir.clone();
            let timeout = self.config.request_timeout;
            let delay = self.config.request_delay;
            let privacy = self.config.privacy.clone();
            let via = self.config.via.clone();
            let via_mode = self.config.via_mode;
            let source_id = source_id.map(|s| s.to_string());
            let downloaded = downloaded.clone();
            let deduplicated = deduplicated.clone();
            let skipped = skipped.clone();
            let failed = failed.clone();
            let event_tx = event_tx.clone();

            let handle = tokio::spawn(async move {
                let client =
                    match HttpClient::with_privacy("download", timeout, delay, None, &privacy) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("Failed to create HTTP client: {}", e);
                            return;
                        }
                    };

                // Apply via mappings for caching proxy support
                let client = if !via.is_empty() {
                    client.with_via_config(via, via_mode)
                } else {
                    client
                };

                loop {
                    // Check limit
                    if let Some(max) = limit {
                        if downloaded.load(Ordering::Relaxed) >= max {
                            break;
                        }
                    }

                    // Claim a URL to process
                    let crawl_url = match crawl_repo.claim_pending_url(source_id.as_deref()).await {
                        Ok(Some(url)) => url,
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            match crawl_repo.claim_pending_url(source_id.as_deref()).await {
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

                    // Handle YouTube URLs specially
                    if youtube::is_youtube_url(&url) {
                        let proxy_url = privacy.effective_proxy_url();
                        let yt_result = download_youtube_video(
                            &url,
                            &crawl_url,
                            &documents_dir,
                            &doc_repo,
                            &crawl_repo,
                            worker_id,
                            &event_tx,
                            &downloaded,
                            &failed,
                            proxy_url.as_deref(),
                        )
                        .await;

                        if yt_result {
                            continue;
                        }
                        // If YouTube download failed, continue to try regular HTTP
                    }

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
                            handle_download_failure(
                                &crawl_url,
                                &crawl_repo,
                                &failed,
                                &event_tx,
                                worker_id,
                                &e.to_string(),
                                true,
                            )
                            .await;
                            continue;
                        }
                    };

                    if response.is_not_modified() {
                        handle_unchanged(&crawl_url, &crawl_repo, &skipped, &event_tx, worker_id)
                            .await;
                        continue;
                    }

                    if !response.is_success() {
                        handle_download_failure(
                            &crawl_url,
                            &crawl_repo,
                            &failed,
                            &event_tx,
                            worker_id,
                            &format!("HTTP {}", response.status),
                            true,
                        )
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
                            handle_download_failure(
                                &crawl_url,
                                &crawl_repo,
                                &failed,
                                &event_tx,
                                worker_id,
                                &e.to_string(),
                                false,
                            )
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

                    // Compute dual hashes for deduplication
                    let hashes = DocumentVersion::compute_dual_hashes(&content);
                    let file_size = content.len() as i64;

                    // Check for existing file with same content
                    let (content_path, was_deduplicated) = match doc_repo
                        .find_existing_file(&hashes.sha256, &hashes.blake3, file_size)
                        .await
                    {
                        Ok(Some(existing_path)) => {
                            // File already exists, reuse it
                            deduplicated.fetch_add(1, Ordering::Relaxed);
                            let _ = event_tx
                                .send(DownloadEvent::Deduplicated {
                                    worker_id,
                                    url: url.clone(),
                                    existing_path: existing_path.clone(),
                                })
                                .await;
                            (std::path::PathBuf::from(existing_path), true)
                        }
                        Ok(None) | Err(_) => {
                            // No duplicate or dedup check failed - write new file
                            let (basename, extension) =
                                extract_filename_parts(&url, &title, &mime_type);
                            let new_path = content_storage_path_with_name(
                                &documents_dir,
                                &hashes.sha256,
                                &basename,
                                &extension,
                            );

                            if let Err(e) =
                                tokio::fs::create_dir_all(new_path.parent().unwrap()).await
                            {
                                send_failure_event(&url, &failed, &event_tx, worker_id, &e.to_string()).await;
                                continue;
                            }

                            if let Err(e) = tokio::fs::write(&new_path, &content).await {
                                send_failure_event(&url, &failed, &event_tx, worker_id, &e.to_string()).await;
                                continue;
                            }
                            (new_path, false)
                        }
                    };

                    let version = DocumentVersion::with_precomputed_hashes(
                        hashes.clone(),
                        file_size as u64,
                        content_path,
                        mime_type.clone(),
                        Some(url.clone()),
                        disposition_filename,
                        server_date,
                    );

                    // Save or update document
                    let new_document = save_or_update_document(
                        &doc_repo,
                        &url,
                        &crawl_url.source_id,
                        title,
                        version,
                        serde_json::json!({}),
                        "crawl",
                    )
                    .await;

                    // Mark URL as fetched
                    let mut fetched_url = crawl_url.clone();
                    fetched_url.status = UrlStatus::Fetched;
                    fetched_url.fetched_at = Some(chrono::Utc::now());
                    fetched_url.etag = etag;
                    fetched_url.last_modified = last_modified;
                    fetched_url.content_hash = Some(hashes.sha256.clone());
                    let _ = crawl_repo.update_url(&fetched_url).await;

                    // Only count as downloaded if we actually wrote a new file
                    if !was_deduplicated {
                        downloaded.fetch_add(1, Ordering::Relaxed);
                        let _ = event_tx
                            .send(DownloadEvent::Completed {
                                worker_id,
                                url,
                                new_document,
                            })
                            .await;
                    }
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
            self.crawl_repo.get_crawl_state(sid).await?.urls_pending
        } else {
            0
        };

        Ok(DownloadResult {
            downloaded: downloaded.load(Ordering::Relaxed),
            deduplicated: deduplicated.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
            failed: failed.load(Ordering::Relaxed),
            remaining,
        })
    }
}
