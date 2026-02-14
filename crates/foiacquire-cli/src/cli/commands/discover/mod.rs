//! URL discovery and browser testing commands.

mod all;
#[cfg(feature = "browser")]
mod browser;
mod pattern;
mod search;
mod sources;

use console::style;

use foiacquire::config::Settings;
use foiacquire_scrape::discovery::DiscoveredUrl;

pub use all::cmd_discover_all;
#[cfg(feature = "browser")]
pub use browser::cmd_browser_test;
pub use pattern::cmd_discover_pattern;
pub use search::cmd_discover_search;
pub use sources::{cmd_discover_paths, cmd_discover_sitemap, cmd_discover_wayback};

/// Helper to get base URL for a source from config.
pub(super) async fn get_source_base_url(
    _settings: &Settings,
    source_id: &str,
) -> anyhow::Result<String> {
    use foiacquire::config::Config;

    let config = Config::load().await;
    let scraper = config
        .scrapers
        .get(source_id)
        .ok_or_else(|| anyhow::anyhow!("Source '{}' not found in configuration", source_id))?;

    scraper
        .base_url
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Source '{}' has no base_url configured", source_id))
}

/// Helper to add discovered URLs to the crawl queue.
pub(super) async fn add_discovered_urls(
    settings: &Settings,
    source_id: &str,
    urls: Vec<DiscoveredUrl>,
    dry_run: bool,
) -> anyhow::Result<usize> {
    use foiacquire::models::CrawlUrl;

    if dry_run {
        println!(
            "\n{} Dry run - would add {} URLs:",
            style("ℹ").blue(),
            urls.len()
        );

        // Show listing pages first
        let listings: Vec<_> = urls.iter().filter(|u| u.is_listing_page).collect();
        if !listings.is_empty() {
            println!("\n  {} Listing pages (high priority):", style("📁").cyan());
            for url in listings.iter().take(10) {
                println!("    {}", url.url);
            }
            if listings.len() > 10 {
                println!("    ... and {} more listing pages", listings.len() - 10);
            }
        }

        // Then documents
        let docs: Vec<_> = urls.iter().filter(|u| !u.is_listing_page).collect();
        if !docs.is_empty() {
            println!("\n  {} Document URLs:", style("📄").cyan());
            for url in docs.iter().take(10) {
                println!("    {}", url.url);
            }
            if docs.len() > 10 {
                println!("    ... and {} more document URLs", docs.len() - 10);
            }
        }

        return Ok(0);
    }

    let repos = settings.repositories()?;
    let crawl_repo = repos.crawl;

    let mut added = 0;
    for discovered in urls {
        let crawl_url = CrawlUrl::new(
            discovered.url.clone(),
            source_id.to_string(),
            discovered.source_method,
            discovered.query_used.clone(),
            0,
        );

        match crawl_repo.add_url(&crawl_url).await {
            Ok(true) => added += 1,
            Ok(false) => {} // Already exists
            Err(e) => tracing::warn!("Failed to add URL {}: {}", discovered.url, e),
        }
    }

    Ok(added)
}
