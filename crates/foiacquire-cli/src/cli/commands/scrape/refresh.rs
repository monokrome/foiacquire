//! Refresh metadata for documents.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use console::style;
use indicatif::ProgressBar;

use super::helpers::{process_get_response_for_refresh, RefreshResult};
use crate::cli::commands::helpers::truncate;
use foiacquire::config::{Config, Settings};
use foiacquire::models::Document;
use foiacquire::privacy::PrivacyConfig;
use foiacquire::repository::DieselDocumentRepository;

/// Shared GET request handling for refresh.
/// Returns (should_continue, should_skip_increment).
#[allow(clippy::too_many_arguments)]
async fn try_get_refresh(
    client: &foiacquire::http_client::HttpClient,
    url: &str,
    doc: &Document,
    current_version: &foiacquire::models::DocumentVersion,
    documents_dir: &std::path::Path,
    doc_repo: &Arc<DieselDocumentRepository>,
    pb: &ProgressBar,
    updated: &Arc<AtomicUsize>,
    redownloaded: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
) -> bool {
    match client.get(url, None, None).await {
        Ok(response) if response.is_success() => {
            let result =
                process_get_response_for_refresh(response, doc, current_version, documents_dir)
                    .await;
            handle_refresh_result(result, doc_repo, doc, pb, updated, redownloaded).await
        }
        _ => {
            skipped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// Handle a RefreshResult by saving the document and updating counters.
/// Returns true if processing should continue (skip), false otherwise.
async fn handle_refresh_result(
    result: RefreshResult,
    doc_repo: &Arc<DieselDocumentRepository>,
    doc: &Document,
    pb: &ProgressBar,
    updated: &Arc<AtomicUsize>,
    redownloaded: &Arc<AtomicUsize>,
) -> bool {
    match result {
        RefreshResult::Updated(updated_doc) => {
            if let Err(e) = doc_repo.save(&updated_doc).await {
                pb.println(format!(
                    "{} Failed to save {}: {}",
                    style("✗").red(),
                    truncate(&doc.title, 30),
                    e
                ));
            } else {
                updated.fetch_add(1, Ordering::Relaxed);
            }
            false
        }
        RefreshResult::Redownloaded(updated_doc) => {
            if let Err(e) = doc_repo.save_with_versions(&updated_doc).await {
                pb.println(format!(
                    "{} Failed to save {}: {}",
                    style("✗").red(),
                    truncate(&doc.title, 30),
                    e
                ));
            } else {
                redownloaded.fetch_add(1, Ordering::Relaxed);
            }
            false
        }
        RefreshResult::Skipped => true,
    }
}

/// Refresh metadata for documents.
pub async fn cmd_refresh(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    force: bool,
    privacy_config: &PrivacyConfig,
) -> anyhow::Result<()> {
    use tokio::sync::Semaphore;

    let repos = settings.repositories()?;
    let doc_repo = Arc::new(repos.documents);

    // Get documents that need metadata refresh
    let documents = if let Some(sid) = source_id {
        doc_repo.get_by_source(sid).await?
    } else {
        doc_repo.get_all().await?
    };

    // Filter to documents needing refresh (missing original_filename or server_date)
    let docs_needing_refresh: Vec<_> = documents
        .into_iter()
        .filter(|doc| {
            if force {
                return true;
            }
            if let Some(version) = doc.current_version() {
                version.original_filename.is_none() || version.server_date.is_none()
            } else {
                false
            }
        })
        .collect();

    let total = if limit > 0 {
        std::cmp::min(limit, docs_needing_refresh.len())
    } else {
        docs_needing_refresh.len()
    };

    if total == 0 {
        println!("{} All documents already have metadata", style("✓").green());
        return Ok(());
    }

    // Load config for via mappings
    let config = Config::load().await;
    let via_mappings = Arc::new(config.via);
    let via_mode = config.via_mode;

    println!(
        "{} Refreshing metadata for {} documents using {} workers",
        style("→").cyan(),
        total,
        workers
    );

    // Create work queue
    let work_queue: Arc<tokio::sync::Mutex<Vec<foiacquire::models::Document>>> = Arc::new(
        tokio::sync::Mutex::new(docs_needing_refresh.into_iter().take(total).collect()),
    );

    let updated = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let redownloaded = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(workers));

    // Progress bar
    let pb = indicatif::ProgressBar::new(total as u64);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut handles = Vec::new();
    let documents_dir = settings.documents_dir.clone();

    for _ in 0..workers {
        let work_queue = work_queue.clone();
        let doc_repo = doc_repo.clone();
        let documents_dir = documents_dir.clone();
        let updated = updated.clone();
        let skipped = skipped.clone();
        let redownloaded = redownloaded.clone();
        let semaphore = semaphore.clone();
        let pb = pb.clone();
        let privacy = privacy_config.clone();
        let via = via_mappings.clone();

        let handle = tokio::spawn(async move {
            let client = match foiacquire::http_client::HttpClient::builder(
                "refresh",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_millis(100),
            )
            .privacy(&privacy)
            .build()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to create HTTP client: {}", e);
                    return;
                }
            };

            // Apply via mappings for caching proxy support
            let client = if !via.is_empty() {
                client.with_via_config((*via).clone(), via_mode)
            } else {
                client
            };

            loop {
                let _permit = semaphore.acquire().await.unwrap();

                let doc = {
                    let mut queue = work_queue.lock().await;
                    queue.pop()
                };

                let doc = match doc {
                    Some(d) => d,
                    None => break,
                };

                pb.set_message(truncate(&doc.title, 40));

                let url = &doc.source_url;
                let current_version = match doc.current_version() {
                    Some(v) => v,
                    None => {
                        pb.inc(1);
                        continue;
                    }
                };

                // Try HEAD request first
                let head_result = client.head(url, None, None).await;

                match head_result {
                    Ok(head_response) if head_response.is_success() => {
                        let _head_etag = head_response.etag().map(|s| s.to_string());
                        let head_last_modified =
                            head_response.last_modified().map(|s| s.to_string());
                        let head_filename = head_response.content_disposition_filename();

                        // Parse server date from Last-Modified
                        let server_date = head_last_modified.as_ref().and_then(|lm| {
                            chrono::DateTime::parse_from_rfc2822(lm)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        });

                        // Check if we got useful metadata from HEAD
                        let got_metadata = head_filename.is_some() || server_date.is_some();

                        if got_metadata
                            && (head_filename.is_some()
                                || current_version.original_filename.is_some())
                            && (server_date.is_some() || current_version.server_date.is_some())
                        {
                            // We can update metadata without re-downloading
                            // Create updated version with new metadata
                            let mut updated_doc = doc.clone();
                            if let Some(version) = updated_doc.versions.first_mut() {
                                if version.original_filename.is_none() {
                                    version.original_filename = head_filename;
                                }
                                if version.server_date.is_none() {
                                    version.server_date = server_date;
                                }
                            }

                            if let Err(e) = doc_repo.save(&updated_doc).await {
                                pb.println(format!(
                                    "{} Failed to save {}: {}",
                                    style("✗").red(),
                                    truncate(&doc.title, 30),
                                    e
                                ));
                            } else {
                                updated.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            // Need to do full GET to get metadata
                            if try_get_refresh(
                                &client,
                                url,
                                &doc,
                                current_version,
                                &documents_dir,
                                &doc_repo,
                                &pb,
                                &updated,
                                &redownloaded,
                                &skipped,
                            )
                            .await
                            {
                                pb.inc(1);
                                continue;
                            }
                        }
                    }
                    _ => {
                        // HEAD failed or not supported, try GET
                        if try_get_refresh(
                            &client,
                            url,
                            &doc,
                            current_version,
                            &documents_dir,
                            &doc_repo,
                            &pb,
                            &updated,
                            &redownloaded,
                            &skipped,
                        )
                        .await
                        {
                            pb.inc(1);
                            continue;
                        }
                    }
                }

                pb.inc(1);
            }
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        let _ = handle.await;
    }

    pb.finish_with_message("done");

    let final_updated = updated.load(Ordering::Relaxed);
    let final_skipped = skipped.load(Ordering::Relaxed);
    let final_redownloaded = redownloaded.load(Ordering::Relaxed);

    println!(
        "{} Updated metadata for {} documents",
        style("✓").green(),
        final_updated
    );

    if final_redownloaded > 0 {
        println!(
            "  {} {} documents had content changes (new versions added)",
            style("↻").yellow(),
            final_redownloaded
        );
    }

    if final_skipped > 0 {
        println!(
            "  {} {} documents skipped (fetch failed)",
            style("→").dim(),
            final_skipped
        );
    }

    Ok(())
}
