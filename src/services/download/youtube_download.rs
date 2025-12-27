//! YouTube video download handler.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::models::{CrawlUrl, Document, DocumentVersion, UrlStatus};
use crate::repository::{DieselCrawlRepository, DieselDocumentRepository};
use crate::services::youtube;

use super::types::DownloadEvent;

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
) -> bool {
    debug!("Attempting YouTube download: {}", url);

    // Download with yt-dlp
    let result = youtube::download_video(url, documents_dir).await;

    match result {
        Ok(yt_result) => {
            // Read the downloaded file
            let content = match tokio::fs::read(&yt_result.video_path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read downloaded video: {}", e);
                    let mut failed_url = crawl_url.clone();
                    failed_url.status = UrlStatus::Failed;
                    failed_url.last_error = Some(format!("Failed to read video: {}", e));
                    let _ = crawl_repo.update_url(&failed_url).await;
                    failed.fetch_add(1, Ordering::Relaxed);
                    let _ = event_tx
                        .send(DownloadEvent::Failed {
                            worker_id,
                            url: url.to_string(),
                            error: e.to_string(),
                        })
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
                yt_result.video_path.clone(),
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

            // Check for existing document
            let existing = doc_repo.get_by_url(url).await.ok().and_then(|v| v.into_iter().next());
            let new_document = existing.is_none();

            if let Some(mut doc) = existing {
                if doc.add_version(version) {
                    let _ = doc_repo.save(&doc).await;
                }
            } else {
                let doc = Document::with_discovery_method(
                    uuid::Uuid::new_v4().to_string(),
                    crawl_url.source_id.clone(),
                    yt_result.metadata.title.clone(),
                    url.to_string(),
                    version,
                    metadata,
                    "youtube".to_string(),
                );
                let _ = doc_repo.save(&doc).await;
            }

            // Mark URL as fetched
            let mut fetched_url = crawl_url.clone();
            fetched_url.status = UrlStatus::Fetched;
            fetched_url.fetched_at = Some(chrono::Utc::now());
            fetched_url.content_hash = Some(content_hash);
            let _ = crawl_repo.update_url(&fetched_url).await;

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
            let mut failed_url = crawl_url.clone();
            failed_url.status = UrlStatus::Failed;
            failed_url.last_error = Some(format!("yt-dlp: {}", e));
            failed_url.retry_count += 1;
            let _ = crawl_repo.update_url(&failed_url).await;
            failed.fetch_add(1, Ordering::Relaxed);
            let _ = event_tx
                .send(DownloadEvent::Failed {
                    worker_id,
                    url: url.to_string(),
                    error: format!("yt-dlp: {}", e),
                })
                .await;
            true
        }
    }
}
