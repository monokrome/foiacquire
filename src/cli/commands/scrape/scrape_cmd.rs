//! Main scrape command implementation.

use std::sync::Arc;
use std::time::Duration;

use console::style;

use crate::config::{Config, Settings};
use crate::llm::LlmClient;
use crate::models::{Source, SourceType};
use crate::scrapers::{
    load_rate_limit_state, save_rate_limit_state, ConfigurableScraper, RateLimiter,
};

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
    let ctx = settings.create_db_context()?;
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
            // Single source - run directly but catch errors in daemon mode
            let source_id = &sources_to_scrape[0];
            let line = source_lines.get(source_id).copied();
            let result = cmd_scrape_single_tui(
                settings,
                source_id,
                workers,
                limit,
                show_progress,
                line,
                tui_guard.is_active(),
                Some(rate_limiter.clone()),
            )
            .await;

            match result {
                Ok(()) => {
                    if let Some(&line) = source_lines.get(source_id) {
                        let _ = crate::cli::tui::set_status(
                            line,
                            &format!("  {} {} done", style("✓").green(), source_id),
                        );
                    }
                }
                Err(e) => {
                    if let Some(&line) = source_lines.get(source_id) {
                        let _ = crate::cli::tui::set_status(
                            line,
                            &format!("  {} {} error", style("✗").red(), source_id),
                        );
                    }
                    // Log error and continue - no reason to bail completely over one failure
                    let _ = crate::cli::tui::log(&format!(
                        "\n{} Scraper error: {}",
                        style("!").yellow(),
                        e
                    ));
                }
            }
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

    let ctx = settings.create_db_context()?;
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
