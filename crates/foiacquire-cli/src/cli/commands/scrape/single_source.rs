//! Single-source scraping with TUI status updates.

use std::sync::Arc;
use std::time::Duration;

use console::style;

use foiacquire::config::{Config, Settings};
use foiacquire::llm::LlmClient;
use foiacquire::models::{ScraperStats, ServiceStatus, Source, SourceType};
use foiacquire::privacy::PrivacyConfig;
use foiacquire_scrape::{ConfigurableScraper, RateLimiter};

use super::scrape_cmd::maybe_update_heartbeat;

/// Scrape a single source with TUI status updates.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cmd_scrape_single_tui(
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
        let llm = LlmClient::with_privacy(llm_config, privacy_config.clone());

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

    let repos = settings.repositories()?;
    let source_repo = repos.sources;
    let doc_repo = repos.documents;
    let crawl_repo = Arc::new(repos.crawl);
    let service_status_repo = repos.service_status;

    // Run external discovery if enabled
    if scraper_config.discovery.external.is_enabled() {
        update_status(&format!("{} running discovery...", source_id));

        if let Some(base_url) = &scraper_config.base_url {
            let discovery_urls = super::discovery::run_external_discovery(
                base_url,
                &scraper_config.discovery,
                source_id,
                privacy_config,
            )
            .await;

            if !discovery_urls.is_empty() {
                let mut added = 0usize;
                for discovered in discovery_urls {
                    let crawl_url = foiacquire::models::CrawlUrl::new(
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

    let stream = match scraper.scrape_stream(workers).await {
        Ok(s) => s,
        Err(e) => {
            service_status.record_error(&e.to_string());
            service_status.set_stopped();
            if let Err(status_err) = service_status_repo.upsert(&service_status).await {
                tracing::warn!("Failed to update service status: {}", status_err);
            }
            return Err(e);
        }
    };
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
            if let Err(e) = service_status_repo.upsert(&service_status).await {
                tracing::warn!("Failed to update service status on error: {}", e);
            }
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
    if let Err(e) = service_status_repo.upsert(&service_status).await {
        tracing::warn!("Failed to update final service status: {}", e);
    }

    // Final status
    if let Some(line) = status_line {
        let _ = crate::cli::tui::set_status(
            line,
            &format!("  {} {} {} docs", style("✓").green(), source_id, count),
        );
    }

    Ok(())
}
