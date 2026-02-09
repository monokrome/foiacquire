//! Main scrape command implementation.

use std::sync::Arc;
use std::time::Duration;

use console::style;

use crate::cli::commands::RateLimitBackendType;
use foiacquire::config::{Config, Settings};
use foiacquire::models::{ScraperStats, ServiceStatus};
use foiacquire::privacy::PrivacyConfig;
use foiacquire::repository::DieselServiceStatusRepository;
use foiacquire::scrapers::{DieselRateLimitBackend, InMemoryRateLimitBackend, RateLimiter};

use super::single_source::cmd_scrape_single_tui;

/// Update service heartbeat if interval has elapsed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn maybe_update_heartbeat(
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
            let redis_url =
                std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
            let backend = Arc::new(
                foiacquire::scrapers::RedisRateLimitBackend::new(&redis_url, base_delay_ms).await?,
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
