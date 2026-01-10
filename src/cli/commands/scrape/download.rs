//! Download pending documents command.

use std::sync::Arc;
use std::time::Duration;

use console::style;

use crate::config::Settings;
use crate::privacy::PrivacyConfig;
use crate::repository::diesel_context::DieselDbContext;

/// Download pending documents from the queue.
pub async fn cmd_download(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    show_progress: bool,
    privacy_config: &PrivacyConfig,
) -> anyhow::Result<()> {
    use crate::cli::progress::DownloadProgress;
    use crate::services::{DownloadConfig, DownloadEvent, DownloadService};
    use tokio::sync::mpsc;

    settings.ensure_directories()?;

    let ctx = settings.create_db_context()?;
    let doc_repo = Arc::new(ctx.documents());
    let crawl_repo = Arc::new(ctx.crawl());

    // Check for pending work
    let initial_pending = get_pending_count(&ctx, source_id).await?;

    if initial_pending == 0 {
        println!("{} No pending documents to download", style("!").yellow());
        if let Some(sid) = source_id {
            println!(
                "  {} Run 'foiacquire crawl {}' to discover new URLs",
                style("→").dim(),
                sid
            );
        }
        return Ok(());
    }

    println!(
        "{} Starting {} download workers ({} pending documents)",
        style("→").cyan(),
        workers,
        initial_pending
    );

    // Create service
    let service = DownloadService::new(
        doc_repo,
        crawl_repo,
        DownloadConfig {
            documents_dir: settings.documents_dir.clone(),
            request_timeout: Duration::from_secs(settings.request_timeout),
            request_delay: Duration::from_millis(settings.request_delay_ms),
            privacy: privacy_config.clone(),
        },
    );

    // Event channel for progress updates
    let (event_tx, mut event_rx) = mpsc::channel::<DownloadEvent>(100);

    // Set up progress display (UI concern)
    let progress_display = if show_progress {
        Some(Arc::new(DownloadProgress::new(workers, initial_pending)))
    } else {
        None
    };

    // Spawn event handler task (UI layer)
    let progress_clone = progress_display.clone();
    let event_handler = tokio::spawn(async move {
        let mut downloaded = 0usize;
        let mut skipped = 0usize;

        while let Some(event) = event_rx.recv().await {
            match event {
                DownloadEvent::Started {
                    worker_id,
                    filename,
                    ..
                } => {
                    if let Some(ref progress) = progress_clone {
                        progress.start_download(worker_id, &filename, None).await;
                    }
                }
                DownloadEvent::Progress {
                    worker_id,
                    bytes,
                    total,
                } => {
                    if let Some(ref progress) = progress_clone {
                        if let Some(t) = total {
                            progress.start_download(worker_id, "", Some(t)).await;
                        }
                        progress.update_progress(worker_id, bytes).await;
                    }
                }
                DownloadEvent::Completed { worker_id, .. } => {
                    downloaded += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Deduplicated { worker_id, .. } => {
                    // Count deduplicated files as successful downloads
                    downloaded += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Unchanged { worker_id, .. } => {
                    skipped += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Failed { worker_id, url, error } => {
                    if let Some(ref progress) = progress_clone {
                        progress.println(&format!(
                            "{} Failed to download {}: {}",
                            console::style("✗").red(),
                            url,
                            error
                        ));
                        progress.finish_download(worker_id, false).await;
                    } else {
                        eprintln!(
                            "{} Failed to download {}: {}",
                            console::style("✗").red(),
                            url,
                            error
                        );
                    }
                }
            }
        }
    });

    // Run download service (business logic)
    let limit_opt = if limit > 0 { Some(limit) } else { None };
    let result = service
        .download(source_id, workers, limit_opt, event_tx)
        .await?;

    // Wait for event handler to finish
    if let Err(e) = event_handler.await {
        tracing::warn!("Event handler task failed: {}", e);
    }

    // Clean up progress display
    if let Some(ref progress) = progress_display {
        progress.finish().await;
    }

    // Print results (UI layer)
    println!(
        "{} Downloaded {} documents",
        style("✓").green(),
        result.downloaded
    );

    if result.skipped > 0 {
        println!(
            "  {} {} unchanged (304 Not Modified)",
            style("→").dim(),
            result.skipped
        );
    }

    if result.remaining > 0 {
        println!(
            "  {} {} URLs still pending",
            style("!").yellow(),
            result.remaining
        );
    }

    Ok(())
}

/// Get pending document count for a source or all sources.
async fn get_pending_count(ctx: &DieselDbContext, source_id: Option<&str>) -> anyhow::Result<u64> {
    let crawl_repo = ctx.crawl();

    if let Some(sid) = source_id {
        Ok(crawl_repo.get_crawl_state(sid).await?.urls_pending)
    } else {
        // Use bulk query to avoid N+1 pattern
        let all_stats = crawl_repo.get_all_stats().await?;
        Ok(all_stats.values().map(|s| s.urls_pending).sum())
    }
}
