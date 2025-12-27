//! Scrape and download commands.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use console::style;

use crate::config::{Config, Settings};
use crate::llm::LlmClient;
use crate::models::{Document, DocumentStatus, DocumentVersion, Source, SourceType};
use crate::repository::DbContext;
use crate::scrapers::{
    load_rate_limit_state, save_rate_limit_state, ConfigurableScraper, RateLimiter,
};

use super::helpers::{mime_to_extension, truncate};

/// Parse server date from Last-Modified header.
fn parse_server_date(last_modified: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    last_modified.and_then(|lm| {
        chrono::DateTime::parse_from_rfc2822(lm)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    })
}

/// Update document metadata without re-downloading content.
fn update_document_metadata(
    doc: &Document,
    filename: Option<String>,
    server_date: Option<chrono::DateTime<chrono::Utc>>,
) -> Document {
    let mut updated_doc = doc.clone();
    if let Some(version) = updated_doc.versions.first_mut() {
        if version.original_filename.is_none() {
            version.original_filename = filename;
        }
        if version.server_date.is_none() {
            version.server_date = server_date;
        }
    }
    updated_doc
}

/// Save new content and add a new version to the document.
#[allow(clippy::too_many_arguments)]
fn save_new_version(
    doc: &Document,
    content: &[u8],
    new_hash: &str,
    mime_type: &str,
    url: &str,
    filename: Option<String>,
    server_date: Option<chrono::DateTime<chrono::Utc>>,
    documents_dir: &Path,
) -> Document {
    let content_path = documents_dir.join(&new_hash[..2]).join(format!(
        "{}.{}",
        &new_hash[..8],
        mime_to_extension(mime_type)
    ));

    if let Some(parent) = content_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&content_path, content);

    let new_version = DocumentVersion::new_with_metadata(
        content,
        content_path,
        mime_type.to_string(),
        Some(url.to_string()),
        filename,
        server_date,
    );

    let mut updated_doc = doc.clone();
    updated_doc.add_version(new_version);
    updated_doc
}

/// Result of processing an HTTP response for refresh.
enum RefreshResult {
    Updated(Document),
    Redownloaded(Document),
    Skipped,
}

/// Process an HTTP GET response for metadata refresh.
async fn process_get_response_for_refresh(
    response: crate::scrapers::HttpResponse,
    doc: &Document,
    current_version: &DocumentVersion,
    documents_dir: &Path,
) -> RefreshResult {
    let filename = response.content_disposition_filename();
    let last_modified = response.last_modified().map(|s| s.to_string());
    let server_date = parse_server_date(last_modified.as_deref());

    let content = match response.bytes().await {
        Ok(b) => b,
        Err(_) => return RefreshResult::Skipped,
    };

    let new_hash = DocumentVersion::compute_hash(&content);
    let content_changed = new_hash != current_version.content_hash;

    if content_changed {
        let updated_doc = save_new_version(
            doc,
            &content,
            &new_hash,
            &current_version.mime_type,
            &doc.source_url,
            filename,
            server_date,
            documents_dir,
        );
        RefreshResult::Redownloaded(updated_doc)
    } else {
        let updated_doc = update_document_metadata(doc, filename, server_date);
        RefreshResult::Updated(updated_doc)
    }
}

/// Reload mode for daemon operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ReloadMode {
    /// Reload config at the start of each daemon cycle
    #[default]
    NextRun,
    /// Exit process when config file changes (for process manager restart)
    StopProcess,
    /// Watch config file and reload immediately when it changes
    Inplace,
}

/// Scrape documents from one or more sources.
#[allow(clippy::too_many_arguments)]
pub async fn cmd_scrape(
    settings: &Settings,
    source_ids: &[String],
    all: bool,
    workers: usize,
    limit: usize,
    show_progress: bool,
    daemon: bool,
    interval: u64,
    reload: ReloadMode,
) -> anyhow::Result<()> {
    // Set up config watcher for stop-process and inplace modes
    // Try file watching first, fall back to DB polling if no config file
    let mut config_watcher =
        if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

    // Create shared rate limiter and load persisted state
    let rate_limiter = Arc::new(RateLimiter::new());
    let db_path = settings.database_path();
    if let Err(e) = load_rate_limit_state(&rate_limiter, &db_path).await {
        tracing::warn!("Failed to load rate limit state: {}", e);
    }

    // Use DbContext for config history
    let ctx = settings.create_db_context();
    let config_history = ctx.config_history();

    // Initial config load for source list
    let config = Config::load().await;
    let mut current_config_hash = config.hash();

    // Determine initial sources to scrape
    let mut sources_to_scrape: Vec<String> = if all {
        config.scrapers.keys().cloned().collect()
    } else if source_ids.is_empty() {
        println!(
            "{} No sources specified. Use --all or provide source IDs.",
            style("✗").red()
        );
        println!(
            "Available sources: {}",
            config
                .scrapers
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    } else {
        source_ids.to_vec()
    };

    if daemon {
        println!(
            "{} Running in daemon mode (interval: {}s, reload: {:?})",
            style("→").cyan(),
            interval,
            reload
        );
    }

    loop {
        // For next-run and inplace modes, reload config to get updated source list
        if daemon && all && matches!(reload, ReloadMode::NextRun | ReloadMode::Inplace) {
            let new_config = Config::load().await;
            let new_sources: Vec<String> = new_config.scrapers.keys().cloned().collect();
            if new_sources != sources_to_scrape {
                println!(
                    "{} Config reloaded ({} sources)",
                    style("↻").cyan(),
                    new_sources.len()
                );
                sources_to_scrape = new_sources;
            }
        }
        // Initialize TUI with fixed status pane at top (1 header + 1 line per source)
        let num_status_lines = (sources_to_scrape.len() + 1).min(10) as u16; // Cap at 10 lines
        let tui_guard = crate::cli::tui::TuiGuard::new(num_status_lines)?;

        // Set header
        let _ = crate::cli::tui::set_status(
            0,
            &format!(
                "{} Scraping {} source{}...",
                style("→").cyan(),
                sources_to_scrape.len(),
                if sources_to_scrape.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ),
        );

        // Initialize status lines for each source
        let source_lines: std::collections::HashMap<String, u16> = sources_to_scrape
            .iter()
            .enumerate()
            .take(9) // Only show first 9 sources in status (line 0 is header)
            .map(|(i, s)| (s.clone(), (i + 1) as u16))
            .collect();

        for (source_id, line) in &source_lines {
            let _ = crate::cli::tui::set_status(
                *line,
                &format!("  {} {} waiting...", style("○").dim(), source_id),
            );
        }

        if sources_to_scrape.len() == 1 {
            // Single source - run directly
            let source_id = &sources_to_scrape[0];
            let line = source_lines.get(source_id).copied();
            cmd_scrape_single_tui(
                settings,
                source_id,
                workers,
                limit,
                show_progress,
                line,
                tui_guard.is_active(),
                Some(rate_limiter.clone()),
            )
            .await?;
        } else {
            // Multiple sources - run in parallel
            let mut handles = Vec::new();
            for source_id in &sources_to_scrape {
                let settings = settings.clone();
                let source_id_clone = source_id.clone();
                let line = source_lines.get(source_id).copied();
                let tui_active = tui_guard.is_active();
                let rate_limiter_clone = rate_limiter.clone();
                let handle = tokio::spawn(async move {
                    cmd_scrape_single_tui(
                        &settings,
                        &source_id_clone,
                        workers,
                        limit,
                        show_progress,
                        line,
                        tui_active,
                        Some(rate_limiter_clone),
                    )
                    .await
                });
                handles.push((source_id.clone(), handle));
            }

            // Wait for all to complete
            let mut errors = Vec::new();
            for (source_id, handle) in handles {
                match handle.await {
                    Ok(Ok(())) => {
                        if let Some(&line) = source_lines.get(&source_id) {
                            let _ = crate::cli::tui::set_status(
                                line,
                                &format!("  {} {} done", style("✓").green(), source_id),
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        if let Some(&line) = source_lines.get(&source_id) {
                            let _ = crate::cli::tui::set_status(
                                line,
                                &format!("  {} {} error", style("✗").red(), source_id),
                            );
                        }
                        errors.push(format!("{}: {}", source_id, e));
                    }
                    Err(e) => {
                        errors.push(format!("{}: task panicked: {}", source_id, e));
                    }
                }
            }

            if !errors.is_empty() {
                let _ = crate::cli::tui::log(&format!(
                    "\n{} Some scrapers failed:",
                    style("!").yellow()
                ));
                for err in &errors {
                    let _ = crate::cli::tui::log(&format!("  - {}", err));
                }
            }
        }

        // Update header to show complete
        let _ =
            crate::cli::tui::set_status(0, &format!("{} Scraping complete", style("✓").green()));

        // Save rate limit state to database
        if let Err(e) = save_rate_limit_state(&rate_limiter, &db_path).await {
            tracing::warn!("Failed to save rate limit state: {}", e);
        }

        // TUI cleanup happens automatically when tui_guard is dropped
        drop(tui_guard);

        if !daemon {
            break;
        }

        // Sleep with config watching for stop-process and inplace modes
        println!(
            "{} Sleeping for {}s before next check...",
            style("→").dim(),
            interval
        );

        if let Some(ref mut watcher) = config_watcher {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(interval)) => {}
                result = watcher.recv() => {
                    if result.is_some() {
                        match reload {
                            ReloadMode::StopProcess => {
                                println!(
                                    "{} Config file changed, exiting for restart...",
                                    style("↻").cyan()
                                );
                                return Ok(());
                            }
                            ReloadMode::Inplace => {
                                println!(
                                    "{} Config file changed, reloading...",
                                    style("↻").cyan()
                                );
                                // Config will be reloaded at start of next iteration
                            }
                            ReloadMode::NextRun => {}
                        }
                    }
                }
            }
        } else if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            // DB-based config polling (no config file available)
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

            // Check if config changed in DB
            if let Ok(Some(latest_hash)) = config_history.get_latest_hash().await {
                if latest_hash != current_config_hash {
                    match reload {
                        ReloadMode::StopProcess => {
                            println!(
                                "{} Config changed in database, exiting for restart...",
                                style("↻").cyan()
                            );
                            return Ok(());
                        }
                        ReloadMode::Inplace => {
                            println!(
                                "{} Config changed in database, reloading...",
                                style("↻").cyan()
                            );
                            current_config_hash = latest_hash;
                        }
                        ReloadMode::NextRun => {}
                    }
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        }
    }

    Ok(())
}

/// Scrape a single source with TUI status updates.
#[allow(clippy::too_many_arguments)]
async fn cmd_scrape_single_tui(
    settings: &Settings,
    source_id: &str,
    workers: usize,
    limit: usize,
    _show_progress: bool,
    status_line: Option<u16>,
    tui_active: bool,
    rate_limiter: Option<Arc<RateLimiter>>,
) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Helper to update status line or log
    let update_status = |msg: &str| {
        if let Some(line) = status_line {
            let _ = crate::cli::tui::set_status(line, &format!("  {} {}", style("●").cyan(), msg));
        }
    };

    let log_msg = |msg: &str| {
        if tui_active {
            let _ = crate::cli::tui::log(msg);
        } else {
            println!("{}", msg);
        }
    };

    // Load scraper config
    let config = Config::load().await;
    let mut scraper_config = match config.scrapers.get(source_id) {
        Some(c) => c.clone(),
        None => {
            log_msg(&format!(
                "{} No scraper configured for '{}'",
                style("✗").red(),
                source_id
            ));
            return Ok(());
        }
    };

    update_status(&format!("{} loading config...", source_id));

    // Expand search terms using LLM if configured
    if scraper_config.discovery.expand_search_terms
        && !scraper_config.discovery.search_queries.is_empty()
    {
        let llm_config = config.llm.clone();
        let llm = LlmClient::new(llm_config);

        if llm.is_available().await {
            update_status(&format!("{} expanding search terms...", source_id));
            let domain = scraper_config.name.as_deref().unwrap_or(source_id);
            if let Ok(expanded) = llm
                .expand_search_terms(&scraper_config.discovery.search_queries, domain)
                .await
            {
                let mut all_terms: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for term in &scraper_config.discovery.search_queries {
                    all_terms.insert(term.to_lowercase());
                }
                for term in expanded {
                    all_terms.insert(term.to_lowercase());
                }
                scraper_config.discovery.search_queries = all_terms.into_iter().collect();
            }
        }
    }

    let ctx = settings.create_db_context();
    let source_repo = ctx.sources();
    let doc_repo = ctx.documents();
    let crawl_repo = Arc::new(ctx.crawl());

    // Auto-register source if not in database
    let source = match source_repo.get(source_id).await? {
        Some(s) => s,
        None => {
            let new_source = Source::new(
                source_id.to_string(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&new_source).await?;
            new_source
        }
    };

    // Check crawl state and update config hash
    {
        let config_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let json = serde_json::to_string(&scraper_config).unwrap_or_default();
            let mut hasher = DefaultHasher::new();
            json.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        };

        let config_changed = crawl_repo
            .check_config_changed(source_id, &config_hash)
            .await?;
        if config_changed {
            crawl_repo
                .store_config_hash(source_id, &config_hash)
                .await?;
        }
    }

    update_status(&format!("{} starting...", source_id));

    // Create scraper and start streaming
    let refresh_ttl_days = config.get_refresh_ttl_days(source_id);
    // Clone rate limiter - RateLimiter uses Arc internally so cloning shares state
    let limiter_opt = rate_limiter.as_ref().map(|r| (**r).clone());
    let scraper = ConfigurableScraper::with_rate_limiter(
        source.clone(),
        scraper_config,
        Some(crawl_repo.clone()),
        Duration::from_millis(settings.request_delay_ms),
        refresh_ttl_days,
        limiter_opt,
    );

    let stream = scraper.scrape_stream(workers).await;
    let mut rx = stream.receiver;

    let mut count = 0u64;
    let mut new_this_session = 0u64;

    while let Some(result) = rx.recv().await {
        if result.not_modified {
            count += 1;
            update_status(&format!("{} {} processed", source_id, count));
            continue;
        }

        let content = match &result.content {
            Some(c) => c,
            None => continue,
        };

        // Save document using helper
        crate::cli::helpers::save_scraped_document_async(
            &doc_repo,
            content,
            &result,
            &source.id,
            &settings.documents_dir,
        )
        .await?;

        count += 1;
        new_this_session += 1;
        update_status(&format!(
            "{} {} processed ({} new)",
            source_id, count, new_this_session
        ));

        if limit > 0 && new_this_session as usize >= limit {
            break;
        }
    }

    // Update last scraped
    let mut source = source;
    source.last_scraped = Some(chrono::Utc::now());
    source_repo.save(&source).await?;

    // Final status
    if let Some(line) = status_line {
        let _ = crate::cli::tui::set_status(
            line,
            &format!("  {} {} {} docs", style("✓").green(), source_id, count),
        );
    }

    Ok(())
}

/// Download pending documents from the queue.
pub async fn cmd_download(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    show_progress: bool,
) -> anyhow::Result<()> {
    use crate::cli::progress::DownloadProgress;
    use crate::services::{DownloadConfig, DownloadEvent, DownloadService};
    use tokio::sync::mpsc;

    settings.ensure_directories()?;

    let ctx = settings.create_db_context();
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
                DownloadEvent::Unchanged { worker_id, .. } => {
                    skipped += 1;
                    if let Some(ref progress) = progress_clone {
                        progress.set_summary(downloaded, skipped);
                        progress.finish_download(worker_id, true).await;
                    }
                }
                DownloadEvent::Failed { worker_id, .. } => {
                    if let Some(ref progress) = progress_clone {
                        progress.finish_download(worker_id, false).await;
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
    let _ = event_handler.await;

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
async fn get_pending_count(ctx: &DbContext, source_id: Option<&str>) -> anyhow::Result<u64> {
    let crawl_repo = ctx.crawl();

    if let Some(sid) = source_id {
        Ok(crawl_repo.get_crawl_state(sid).await?.urls_pending)
    } else {
        // Use bulk query to avoid N+1 pattern
        let all_stats = crawl_repo.get_all_stats().await?;
        Ok(all_stats.values().map(|s| s.urls_pending).sum())
    }
}

/// Show overall system status.
pub async fn cmd_status(settings: &Settings) -> anyhow::Result<()> {
    let db_path = settings.database_path();

    // Check if database exists
    if !db_path.exists() {
        println!(
            "{} System not initialized. Run 'foiacquire init' first.",
            style("!").yellow()
        );
        return Ok(());
    }

    let ctx = settings.create_db_context();
    let doc_repo = ctx.documents();
    let source_repo = ctx.sources();

    println!("\n{}", style("FOIAcquire Status").bold());
    println!("{}", "-".repeat(40));
    println!("{:<20} {}", "Data Directory:", settings.data_dir.display());
    println!("{:<20} {}", "Sources:", source_repo.get_all().await?.len());
    println!("{:<20} {}", "Total Documents:", doc_repo.count().await?);

    // Count by status (single bulk query instead of N+1)
    let status_counts = doc_repo.count_all_by_status().await?;
    for status in [
        DocumentStatus::Pending,
        DocumentStatus::Downloaded,
        DocumentStatus::OcrComplete,
        DocumentStatus::Indexed,
        DocumentStatus::Failed,
    ] {
        if let Some(&count) = status_counts.get(status.as_str()) {
            if count > 0 {
                println!("{:<20} {}", format!("  {}:", status.as_str()), count);
            }
        }
    }

    Ok(())
}

/// Refresh metadata for documents.
pub async fn cmd_refresh(
    settings: &Settings,
    source_id: Option<&str>,
    workers: usize,
    limit: usize,
    force: bool,
) -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    let ctx = settings.create_db_context();
    let doc_repo = Arc::new(ctx.documents());

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

    println!(
        "{} Refreshing metadata for {} documents using {} workers",
        style("→").cyan(),
        total,
        workers
    );

    // Create work queue
    let work_queue: Arc<tokio::sync::Mutex<Vec<crate::models::Document>>> = Arc::new(
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

        let handle = tokio::spawn(async move {
            let client = crate::scrapers::HttpClient::new(
                "refresh",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_millis(100),
            );

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
                            match client.get(url, None, None).await {
                                Ok(response) if response.is_success() => {
                                    match process_get_response_for_refresh(
                                        response,
                                        &doc,
                                        current_version,
                                        &documents_dir,
                                    )
                                    .await
                                    {
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
                                        }
                                        RefreshResult::Redownloaded(updated_doc) => {
                                            if let Err(e) = doc_repo.save(&updated_doc).await {
                                                pb.println(format!(
                                                    "{} Failed to save {}: {}",
                                                    style("✗").red(),
                                                    truncate(&doc.title, 30),
                                                    e
                                                ));
                                            } else {
                                                redownloaded.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                        RefreshResult::Skipped => {
                                            pb.inc(1);
                                            continue;
                                        }
                                    }
                                }
                                _ => {
                                    skipped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    _ => {
                        // HEAD failed or not supported, try GET
                        match client.get(url, None, None).await {
                            Ok(response) if response.is_success() => {
                                match process_get_response_for_refresh(
                                    response,
                                    &doc,
                                    current_version,
                                    &documents_dir,
                                )
                                .await
                                {
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
                                    }
                                    RefreshResult::Redownloaded(updated_doc) => {
                                        if let Err(e) = doc_repo.save(&updated_doc).await {
                                            pb.println(format!(
                                                "{} Failed to save {}: {}",
                                                style("✗").red(),
                                                truncate(&doc.title, 30),
                                                e
                                            ));
                                        } else {
                                            redownloaded.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    RefreshResult::Skipped => {
                                        pb.inc(1);
                                        continue;
                                    }
                                }
                            }
                            _ => {
                                skipped.fetch_add(1, Ordering::Relaxed);
                            }
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
