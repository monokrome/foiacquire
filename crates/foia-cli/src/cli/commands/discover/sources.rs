//! Sitemap, Wayback Machine, and common paths discovery commands.

use console::style;

use foia::config::Settings;
use foia_scrape::discovery::sources::{
    common_paths::CommonPathsSource, sitemap::SitemapSource, wayback::WaybackSource,
};
use foia_scrape::discovery::{DiscoverySource, DiscoverySourceConfig};

use super::{add_discovered_urls, get_source_base_url};

/// Discover URLs from sitemaps and robots.txt.
pub async fn cmd_discover_sitemap(
    settings: &Settings,
    source_id: &str,
    limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    let base_url = get_source_base_url(settings, source_id).await?;

    println!(
        "{} Sitemap discovery for {}",
        style("🗺").cyan(),
        style(&base_url).bold()
    );

    let source = SitemapSource::new();
    let config = DiscoverySourceConfig {
        max_results: limit,
        ..Default::default()
    };

    match source.discover(&base_url, &[], &config).await {
        Ok(urls) => {
            println!("  Found {} URLs in sitemaps", urls.len());

            let listings = urls.iter().filter(|u| u.is_listing_page).count();
            if listings > 0 {
                println!("  {} are listing pages", listings);
            }

            let added = add_discovered_urls(settings, source_id, urls, dry_run).await?;

            if !dry_run {
                println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
            }
        }
        Err(e) => {
            println!("{} Sitemap discovery failed: {}", style("✗").red(), e);
        }
    }

    Ok(())
}

/// Discover URLs from Wayback Machine.
pub async fn cmd_discover_wayback(
    settings: &Settings,
    source_id: &str,
    from: Option<&str>,
    to: Option<&str>,
    limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    let base_url = get_source_base_url(settings, source_id).await?;

    println!(
        "{} Wayback Machine discovery for {}",
        style("📜").cyan(),
        style(&base_url).bold()
    );

    if let Some(f) = from {
        println!("  From: {}", f);
    }
    if let Some(t) = to {
        println!("  To: {}", t);
    }

    let source = WaybackSource::new();
    let mut config = DiscoverySourceConfig {
        max_results: limit,
        ..Default::default()
    };

    // Add date range to custom params
    if let Some(f) = from {
        config
            .custom_params
            .insert("from".to_string(), serde_json::Value::String(f.to_string()));
    }
    if let Some(t) = to {
        config
            .custom_params
            .insert("to".to_string(), serde_json::Value::String(t.to_string()));
    }

    match source.discover(&base_url, &[], &config).await {
        Ok(urls) => {
            println!("  Found {} historical URLs", urls.len());

            let listings = urls.iter().filter(|u| u.is_listing_page).count();
            if listings > 0 {
                println!("  {} are listing pages", listings);
            }

            let added = add_discovered_urls(settings, source_id, urls, dry_run).await?;

            if !dry_run {
                println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
            }
        }
        Err(e) => {
            println!("{} Wayback discovery failed: {}", style("✗").red(), e);
        }
    }

    Ok(())
}

/// Discover URLs by checking common paths.
pub async fn cmd_discover_paths(
    settings: &Settings,
    source_id: &str,
    extra_paths: Option<&str>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let base_url = get_source_base_url(settings, source_id).await?;

    println!(
        "{} Common paths discovery for {}",
        style("📁").cyan(),
        style(&base_url).bold()
    );

    let mut source = CommonPathsSource::new();

    if let Some(paths) = extra_paths {
        let custom: Vec<String> = paths.split(',').map(|s| s.trim().to_string()).collect();
        source = source.with_custom_paths(custom);
    }

    let config = DiscoverySourceConfig::default();

    match source.discover(&base_url, &[], &config).await {
        Ok(urls) => {
            println!("  Found {} accessible paths", urls.len());

            let added = add_discovered_urls(settings, source_id, urls, dry_run).await?;

            if !dry_run {
                println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
            }
        }
        Err(e) => {
            println!("{} Path discovery failed: {}", style("✗").red(), e);
        }
    }

    Ok(())
}
