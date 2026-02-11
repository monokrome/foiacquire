//! YouTube video download handler.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::services::youtube;
use foiacquire::models::{CrawlUrl, DocumentVersion, UrlStatus};
use foiacquire::repository::{DieselCrawlRepository, DieselDocumentRepository};

use super::types::{handle_download_failure, save_or_update_document, DownloadEvent};

/// Download a YouTube video and store it as a document.
/// Returns true if handled (success or failure), false if should fall back to HTTP.
#[allow(clippy::too_many_arguments)]
pub async fn download_youtube_video(
    url: &str,
    crawl_url: &CrawlUrl,
    documents_dir: &Path,
    doc_repo: &Arc<DieselDocumentRepository>,
    crawl_repo: &Arc<DieselCrawlRepository>,
    worker_id: usize,
    event_tx: &mpsc::Sender<DownloadEvent>,
    downloaded: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    proxy_url: Option<&str>,
) -> bool {
    debug!("Attempting YouTube download: {}", url);

    // Download with yt-dlp
    let result = youtube::download_video(url, documents_dir, proxy_url).await;

    match result {
        Ok(yt_result) => {
            // Read the downloaded file
            let content = match tokio::fs::read(&yt_result.video_path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read downloaded video: {}", e);
                    handle_download_failure(
                        crawl_url,
                        crawl_repo,
                        failed,
                        event_tx,
                        worker_id,
                        &format!("Failed to read video: {}", e),
                        false,
                    )
                    .await;
                    return true;
                }
            };

            let _ = event_tx
                .send(DownloadEvent::Progress {
                    worker_id,
                    bytes: content.len() as u64,
                    total: Some(content.len() as u64),
                })
                .await;

            // Create document version
            let content_hash = DocumentVersion::compute_hash(&content);

            // Parse upload date if available
            let server_date = yt_result.metadata.upload_date.as_ref().and_then(|d| {
                chrono::NaiveDate::parse_from_str(d, "%Y%m%d")
                    .ok()
                    .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
            });

            let version = DocumentVersion::new_with_metadata(
                &content,
                "video/mp4".to_string(),
                Some(url.to_string()),
                Some(format!("{}.mp4", yt_result.metadata.title)),
                server_date,
            );

            // Build metadata
            let mut metadata = serde_json::json!({
                "youtube_id": yt_result.metadata.id,
                "uploader": yt_result.metadata.uploader,
                "duration": yt_result.metadata.duration,
                "view_count": yt_result.metadata.view_count,
            });

            if let Some(desc) = &yt_result.metadata.description {
                metadata["description"] = serde_json::Value::String(desc.clone());
            }

            // Save or update document
            let new_document = match save_or_update_document(
                doc_repo,
                url,
                &crawl_url.source_id,
                yt_result.metadata.title.clone(),
                version,
                metadata,
                "youtube",
            )
            .await
            {
                Ok(new_doc) => new_doc,
                Err(e) => {
                    handle_download_failure(
                        crawl_url,
                        crawl_repo,
                        failed,
                        event_tx,
                        worker_id,
                        &format!("Failed to save document: {}", e),
                        false,
                    )
                    .await;
                    return true;
                }
            };

            // Mark URL as fetched
            let mut fetched_url = crawl_url.clone();
            fetched_url.status = UrlStatus::Fetched;
            fetched_url.fetched_at = Some(chrono::Utc::now());
            fetched_url.content_hash = Some(content_hash);
            if let Err(e) = crawl_repo.update_url(&fetched_url).await {
                warn!("Failed to update crawl URL status for {}: {}", url, e);
            }

            downloaded.fetch_add(1, Ordering::Relaxed);
            let _ = event_tx
                .send(DownloadEvent::Completed {
                    worker_id,
                    url: url.to_string(),
                    new_document,
                })
                .await;

            true
        }
        Err(e) => {
            warn!("YouTube download failed for {}: {}", url, e);
            handle_download_failure(
                crawl_url,
                crawl_repo,
                failed,
                event_tx,
                worker_id,
                &format!("yt-dlp: {}", e),
                true,
            )
            .await;
            true
        }
    }
}
