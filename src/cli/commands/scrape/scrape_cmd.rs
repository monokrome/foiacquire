//! Main scrape command implementation.

use std::sync::Arc;
use std::time::Duration;

use console::style;

use crate::cli::commands::RateLimitBackendType;
use crate::config::{Config, Settings};
use crate::llm::LlmClient;
use crate::models::{ScraperStats, ServiceStatus, Source, SourceType};
use crate::privacy::PrivacyConfig;
use crate::repository::DieselServiceStatusRepository;
use crate::scrapers::{
    ConfigurableScraper, DieselRateLimitBackend, InMemoryRateLimitBackend, RateLimiter,
};

/// Update service heartbeat if interval has elapsed.
async fn maybe_update_heartbeat(
    last_heartbeat: &mut std::time::Instant,
    heartbeat_interval: Duration,
    service_status: &mut ServiceStatus,
    service_status_repo: &DieselServiceStatusRepository,
    source_id: &str,
    count: u64,
    new_this_session: u64,
    errors_this_session: u64,
) {
    if last_heartbeat.elapsed() >= heartbeat_interval {
        service_status.update_scraper_stats(ScraperStats {
            session_processed: count,
            session_new: new_this_session,
            session_errors: errors_this_session,
            rate_per_min: None,
            queue_size: None,
            browser_failures: None,
        });
        service_status.current_task = Some(format!("Processing {}", source_id));
        let _ = service_status_repo.upsert(service_status).await;
        *last_heartbeat = std::time::Instant::now();
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
    rate_limit_backend_type: RateLimitBackendType,
    privacy_config: &PrivacyConfig,
) -> anyhow::Result<()> {
    // Set up config watcher for stop-process and inplace modes
    // Try file watching first, fall back to DB polling if no config file
    let mut config_watcher =
        if daemon && matches!(reload, ReloadMode::StopProcess | ReloadMode::Inplace) {
            prefer::watch("foiacquire").await.ok()
        } else {
            None
        };

    // Create rate limiter with selected backend
    let base_delay_ms = settings.request_delay_ms;
    let rate_limiter = match rate_limit_backend_type {
        RateLimitBackendType::Memory => {
            tracing::debug!("Using in-memory rate limit backend");
            let backend = Arc::new(InMemoryRateLimitBackend::new(base_delay_ms));
            Arc::new(RateLimiter::new(backend))
        }
        RateLimitBackendType::Database => {
            tracing::debug!("Using database rate limit backend");
            let ctx = settings.create_db_context()?;
            let backend = Arc::new(DieselRateLimitBackend::new(
                ctx.pool().clone(),
                base_delay_ms,
            ));
            Arc::new(RateLimiter::new(backend))
        }
        #[cfg(feature = "redis-backend")]
        RateLimitBackendType::Redis => {
            tracing::debug!("Using Redis rate limit backend");
            // TODO: Get Redis URL from config
            let redis_url =
                std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let backend = Arc::new(
                crate::scrapers::RedisRateLimitBackend::new(&redis_url, base_delay_ms).await?,
            );
            Arc::new(RateLimiter::new(backend))
        }
    };

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
                privacy_config,
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
                let privacy_config_clone = privacy_config.clone();
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
                        &privacy_config_clone,
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

        // TUI cleanup happens automatically when tui_guard is dropped
        // Note: Rate limit state is persisted automatically by the Diesel backend
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
    privacy_config: &PrivacyConfig,
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
    let service_status_repo = ctx.service_status();

    // Run external discovery if enabled
    if scraper_config.discovery.external.is_enabled() {
        update_status(&format!("{} running discovery...", source_id));

        if let Some(base_url) = &scraper_config.base_url {
            let discovery_urls = run_external_discovery(
                base_url,
                &scraper_config.discovery,
                source_id,
                privacy_config,
            )
            .await;

            if !discovery_urls.is_empty() {
                let mut added = 0usize;
                for discovered in discovery_urls {
                    let crawl_url = crate::models::CrawlUrl::new(
                        discovered.url.clone(),
                        source_id.to_string(),
                        discovered.source_method,
                        discovered.query_used.clone(),
                        0,
                    );
                    match crawl_repo.add_url(&crawl_url).await {
                        Ok(true) => added += 1,
                        Ok(false) => {} // Already exists
                        Err(e) => tracing::warn!("Failed to add discovered URL: {}", e),
                    }
                }
                if added > 0 {
                    log_msg(&format!(
                        "  {} Added {} URLs from external discovery",
                        style("→").cyan(),
                        added
                    ));
                }
            }
        }
    }

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

    // Register service status
    let mut service_status = ServiceStatus::new_scraper(source_id);
    service_status.set_running(Some(&format!("Starting scrape of {}", source_id)));
    if let Err(e) = service_status_repo.upsert(&service_status).await {
        tracing::warn!("Failed to register service status: {}", e);
    }

    // Create scraper and start streaming
    let refresh_ttl_days = config.get_refresh_ttl_days(source_id);
    // Clone rate limiter - RateLimiter uses Arc internally so cloning shares state
    let limiter_opt = rate_limiter.as_ref().map(|r| (**r).clone());
    let scraper = ConfigurableScraper::with_rate_limiter_and_privacy(
        source.clone(),
        scraper_config,
        Some(crawl_repo.clone()),
        Duration::from_millis(settings.request_delay_ms),
        refresh_ttl_days,
        limiter_opt,
        Some(privacy_config),
    )
    .map_err(|e| anyhow::anyhow!("Failed to create scraper: {}", e))?;

    // Apply via mappings for caching proxy support if configured
    let scraper = if !config.via.is_empty() {
        scraper.with_via_config(config.via.clone(), config.via_mode)
    } else {
        scraper
    };

    let stream = scraper.scrape_stream(workers).await;
    let mut rx = stream.receiver;

    let mut count = 0u64;
    let mut new_this_session = 0u64;
    let mut errors_this_session = 0u64;
    let mut last_heartbeat = std::time::Instant::now();
    let heartbeat_interval = std::time::Duration::from_secs(15);

    while let Some(result) = rx.recv().await {
        if result.not_modified {
            count += 1;
            update_status(&format!("{} {} processed", source_id, count));

            // Periodic heartbeat update
            maybe_update_heartbeat(
                &mut last_heartbeat,
                heartbeat_interval,
                &mut service_status,
                &service_status_repo,
                source_id,
                count,
                new_this_session,
                errors_this_session,
            )
            .await;
            continue;
        }

        let content = match &result.content {
            Some(c) => c,
            None => continue,
        };

        // Save document using helper
        if let Err(e) = crate::cli::helpers::save_scraped_document_async(
            &doc_repo,
            content,
            &result,
            &source.id,
            &settings.documents_dir,
        )
        .await
        {
            tracing::warn!("Failed to save document: {}", e);
            errors_this_session += 1;
            service_status.record_error(&e.to_string());
            let _ = service_status_repo.upsert(&service_status).await;
            continue;
        }

        count += 1;
        new_this_session += 1;
        update_status(&format!(
            "{} {} processed ({} new)",
            source_id, count, new_this_session
        ));

        // Periodic heartbeat update (every 15 seconds)
        maybe_update_heartbeat(
            &mut last_heartbeat,
            heartbeat_interval,
            &mut service_status,
            &service_status_repo,
            source_id,
            count,
            new_this_session,
            errors_this_session,
        )
        .await;

        if limit > 0 && new_this_session as usize >= limit {
            break;
        }
    }

    // Update last scraped
    let mut source = source;
    source.last_scraped = Some(chrono::Utc::now());
    source_repo.save(&source).await?;

    // Update service status to stopped with final stats
    service_status.update_scraper_stats(ScraperStats {
        session_processed: count,
        session_new: new_this_session,
        session_errors: errors_this_session,
        rate_per_min: None,
        queue_size: None,
        browser_failures: None,
    });
    service_status.set_stopped();
    let _ = service_status_repo.upsert(&service_status).await;

    // Final status
    if let Some(line) = status_line {
        let _ = crate::cli::tui::set_status(
            line,
            &format!("  {} {} {} docs", style("✓").green(), source_id, count),
        );
    }

    Ok(())
}

/// Run external discovery sources (sitemap, wayback, common paths, search engines).
async fn run_external_discovery(
    base_url: &str,
    discovery_config: &crate::scrapers::config::DiscoveryConfig,
    source_id: &str,
    privacy_config: &crate::privacy::PrivacyConfig,
) -> Vec<crate::discovery::DiscoveredUrl> {
    use crate::discovery::sources::{
        common_paths::CommonPathsSource, search::DuckDuckGoSource, sitemap::SitemapSource,
        wayback::WaybackSource,
    };
    use crate::discovery::{DiscoverySource, DiscoverySourceConfig};

    let external = &discovery_config.external;
    let mut all_urls = Vec::new();

    let config = DiscoverySourceConfig {
        max_results: 500, // Reasonable default per source
        privacy: privacy_config.clone(),
        ..Default::default()
    };

    // Sitemap discovery
    if external.enable_sitemap {
        tracing::debug!("Running sitemap discovery for {}", source_id);
        let source = SitemapSource::new();
        match source.discover(base_url, &[], &config).await {
            Ok(urls) => {
                tracing::info!(
                    "Sitemap discovery found {} URLs for {}",
                    urls.len(),
                    source_id
                );
                all_urls.extend(urls);
            }
            Err(e) => {
                tracing::warn!("Sitemap discovery failed for {}: {}", source_id, e);
            }
        }
    }

    // Wayback Machine discovery
    if external.enable_wayback {
        tracing::debug!("Running Wayback discovery for {}", source_id);
        let source = WaybackSource::new();
        match source.discover(base_url, &[], &config).await {
            Ok(urls) => {
                tracing::info!(
                    "Wayback discovery found {} URLs for {}",
                    urls.len(),
                    source_id
                );
                all_urls.extend(urls);
            }
            Err(e) => {
                tracing::warn!("Wayback discovery failed for {}: {}", source_id, e);
            }
        }
    }

    // Common paths discovery
    if !external.common_paths.is_empty() {
        tracing::debug!("Running common paths discovery for {}", source_id);
        let source = CommonPathsSource::new().with_custom_paths(external.common_paths.clone());
        match source.discover(base_url, &[], &config).await {
            Ok(urls) => {
                tracing::info!("Common paths found {} URLs for {}", urls.len(), source_id);
                all_urls.extend(urls);
            }
            Err(e) => {
                tracing::warn!("Common paths discovery failed for {}: {}", source_id, e);
            }
        }
    }

    // Search engine discovery
    for engine_config in external.enabled_search_engines() {
        tracing::debug!(
            "Running {} search discovery for {}",
            engine_config.engine,
            source_id
        );

        // Get search terms from config
        let terms = if !discovery_config.search_queries.is_empty() {
            discovery_config.search_queries.clone()
        } else {
            vec!["FOIA".to_string(), "documents".to_string()]
        };

        let engine_source_config = engine_config.to_source_config(privacy_config);

        match engine_config.engine.to_lowercase().as_str() {
            "duckduckgo" | "ddg" => {
                let source = DuckDuckGoSource::new();
                match source
                    .discover(base_url, &terms, &engine_source_config)
                    .await
                {
                    Ok(urls) => {
                        tracing::info!("DuckDuckGo found {} URLs for {}", urls.len(), source_id);
                        all_urls.extend(urls);
                    }
                    Err(e) => {
                        tracing::warn!("DuckDuckGo discovery failed for {}: {}", source_id, e);
                    }
                }
            }
            other => {
                tracing::warn!("Search engine '{}' not implemented yet", other);
            }
        }
    }

    // Deduplicate
    all_urls.sort_by(|a, b| a.url.cmp(&b.url));
    all_urls.dedup_by(|a, b| a.url == b.url);

    tracing::info!(
        "External discovery found {} total unique URLs for {}",
        all_urls.len(),
        source_id
    );

    all_urls
}
