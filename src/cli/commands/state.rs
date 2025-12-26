//! Crawl state management commands.

use std::sync::Arc;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::Mutex;

use crate::config::{Config, Settings};
use crate::models::{Source, SourceType};
use crate::repository::{CrawlRepository, SourceRepository};
use crate::scrapers::ConfigurableScraper;

use super::helpers::format_bytes;

/// Show crawl status for sources.
pub async fn cmd_crawl_status(
    settings: &Settings,
    source_id: Option<String>,
) -> anyhow::Result<()> {
    let source_repo = SourceRepository::new(&settings.database_path())?;
    let crawl_repo = CrawlRepository::new(&settings.database_path())?;

    let sources = match &source_id {
        Some(id) => source_repo.get(id)?.into_iter().collect(),
        None => source_repo.get_all()?,
    };

    if sources.is_empty() {
        println!("{} No sources found", style("!").yellow());
        return Ok(());
    }

    // Use bulk queries when loading all sources (avoids N+1)
    let (all_states, all_stats) = if source_id.is_none() {
        (
            crawl_repo.get_all_stats()?,
            crawl_repo.get_all_request_stats()?,
        )
    } else {
        (
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        )
    };

    for source in sources {
        // Use bulk-loaded data when available, otherwise fetch individually
        let state = if source_id.is_none() {
            all_states
                .get(&source.id)
                .cloned()
                .unwrap_or_else(|| crate::models::CrawlState {
                    source_id: source.id.clone(),
                    ..Default::default()
                })
        } else {
            crawl_repo.get_crawl_state(&source.id)?
        };

        let stats = if source_id.is_none() {
            all_stats.get(&source.id).cloned().unwrap_or_default()
        } else {
            crawl_repo.get_request_stats(&source.id)?
        };

        println!(
            "\n{}",
            style(format!("Crawl Status: {}", source.name)).bold()
        );
        println!("{}", "-".repeat(40));

        let status_str = if state.is_complete() {
            style("Complete").green().to_string()
        } else if state.needs_resume() {
            style("Needs Resume").yellow().to_string()
        } else {
            style("Not Started").dim().to_string()
        };

        println!("{:<20} {}", "Status:", status_str);

        if let Some(started) = state.last_crawl_started {
            println!(
                "{:<20} {}",
                "Last Started:",
                started.format("%Y-%m-%d %H:%M")
            );
        }
        if let Some(completed) = state.last_crawl_completed {
            println!(
                "{:<20} {}",
                "Last Completed:",
                completed.format("%Y-%m-%d %H:%M")
            );
        }

        println!("{:<20} {}", "URLs Discovered:", state.urls_discovered);
        println!("{:<20} {}", "URLs Fetched:", state.urls_fetched);
        println!("{:<20} {}", "URLs Pending:", state.urls_pending);
        println!("{:<20} {}", "URLs Failed:", state.urls_failed);

        if stats.total_requests > 0 {
            println!();
            println!("{:<20} {}", "Total Requests:", stats.total_requests);
            println!("{:<20} {}", "  Success (200):", stats.success_200);
            println!("{:<20} {}", "  Not Modified (304):", stats.not_modified_304);
            println!("{:<20} {}", "  Errors:", stats.errors);
            println!(
                "{:<20} {:.1}ms",
                "Avg Response Time:", stats.avg_duration_ms
            );
            println!(
                "{:<20} {}",
                "Total Downloaded:",
                format_bytes(stats.total_bytes)
            );
        }
    }

    Ok(())
}

/// Clear crawl state for a source.
pub async fn cmd_crawl_clear(
    settings: &Settings,
    source_id: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    if !confirm {
        println!(
            "{} This will clear ALL crawl state for '{}', including fetched URLs.",
            style("!").yellow(),
            source_id
        );
        println!("  The next crawl will start completely fresh.");
        println!("  Use --confirm to proceed.");
        return Ok(());
    }

    let crawl_repo = CrawlRepository::new(&settings.database_path())?;
    crawl_repo.clear_source_all(source_id)?;

    println!(
        "{} Cleared all crawl state for '{}'",
        style("✓").green(),
        source_id
    );

    Ok(())
}

/// Discover document URLs from a source (does not download).
pub async fn cmd_crawl(settings: &Settings, source_id: &str, _limit: usize) -> anyhow::Result<()> {
    settings.ensure_directories()?;

    // Load scraper config
    let config = Config::load().await;
    let scraper_config = match config.scrapers.get(source_id) {
        Some(c) => c.clone(),
        None => {
            println!(
                "{} No scraper configured for '{}'",
                style("✗").red(),
                source_id
            );
            return Ok(());
        }
    };

    let db_path = settings.database_path();
    let source_repo = SourceRepository::new(&db_path)?;
    let crawl_repo = Arc::new(Mutex::new(CrawlRepository::new(&db_path)?));

    // Auto-register source
    let source = match source_repo.get(source_id)? {
        Some(s) => s,
        None => {
            let new_source = Source::new(
                source_id.to_string(),
                SourceType::Custom,
                scraper_config.name_or(source_id),
                scraper_config.base_url_or(""),
            );
            source_repo.save(&new_source)?;
            crate::cli::progress::progress_println(&format!(
                "  {} Registered source: {}",
                style("✓").green(),
                new_source.name
            ));
            new_source
        }
    };

    // Check crawl state
    {
        let repo = crawl_repo.lock().await;
        let (config_changed, _has_pending_urls) =
            repo.check_config_changed(source_id, &scraper_config)?;

        // Update config hash (we never clear discovered URLs - they're valuable!)
        repo.store_config_hash(source_id, &scraper_config)?;

        let state = repo.get_crawl_state(source_id)?;
        if state.needs_resume() {
            println!(
                "{} Resuming crawl ({} pending URLs)",
                style("→").yellow(),
                state.urls_pending
            );
        }

        // Silence unused variable warning
        let _ = config_changed;
    }

    // Create scraper for discovery
    let refresh_ttl_days = config.get_refresh_ttl_days(source_id);
    let scraper = ConfigurableScraper::new(
        source.clone(),
        scraper_config,
        Some(crawl_repo.clone()),
        Duration::from_millis(settings.request_delay_ms),
        refresh_ttl_days,
    );

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.set_message(format!("Discovering URLs from {}...", source.name));

    let urls = scraper.discover().await;
    pb.finish_and_clear();

    let state = {
        let repo = crawl_repo.lock().await;
        repo.get_crawl_state(source_id)?
    };

    println!(
        "{} Discovered {} URLs from {} ({} pending)",
        style("✓").green(),
        urls.len(),
        source.name,
        state.urls_pending
    );

    if state.urls_pending > 0 {
        println!(
            "  {} Run 'foiacquire download {}' to download pending documents",
            style("→").dim(),
            source_id
        );
    }

    Ok(())
}
