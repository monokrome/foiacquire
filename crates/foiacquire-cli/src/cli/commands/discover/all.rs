//! Run all discovery methods.

use console::style;

use foiacquire::config::Settings;
use foiacquire::discovery::sources::{
    common_paths::CommonPathsSource, search::DuckDuckGoSource, sitemap::SitemapSource,
    wayback::WaybackSource,
};
use foiacquire::discovery::{DiscoveredUrl, DiscoverySource, DiscoverySourceConfig};

use super::pattern::cmd_discover_pattern;
use super::{add_discovered_urls, get_source_base_url};

/// Run all discovery methods.
pub async fn cmd_discover_all(
    settings: &Settings,
    source_id: &str,
    dry_run: bool,
    limit: usize,
) -> anyhow::Result<()> {
    let base_url = get_source_base_url(settings, source_id).await?;

    println!(
        "{} Running all discovery methods for {}",
        style("🔍").cyan(),
        style(&base_url).bold()
    );

    let mut total_urls: Vec<DiscoveredUrl> = Vec::new();

    // 1. Pattern enumeration
    println!("\n{} Pattern enumeration...", style("→").cyan());
    // Note: Pattern enumeration uses the existing database, so we handle it separately
    // Just run the command directly
    cmd_discover_pattern(settings, source_id, limit, true, 3).await?;

    // 2. Sitemap discovery
    println!("\n{} Sitemap discovery...", style("→").cyan());
    let sitemap_source = SitemapSource::new();
    let config = DiscoverySourceConfig {
        max_results: limit,
        ..Default::default()
    };
    if let Ok(urls) = sitemap_source.discover(&base_url, &[], &config).await {
        println!("  Found {} sitemap URLs", urls.len());
        total_urls.extend(urls);
    }

    // 3. Wayback Machine
    println!("\n{} Wayback Machine discovery...", style("→").cyan());
    let wayback_source = WaybackSource::new();
    if let Ok(urls) = wayback_source.discover(&base_url, &[], &config).await {
        println!("  Found {} historical URLs", urls.len());
        total_urls.extend(urls);
    }

    // 4. Common paths
    println!("\n{} Common paths discovery...", style("→").cyan());
    let paths_source = CommonPathsSource::new();
    if let Ok(urls) = paths_source.discover(&base_url, &[], &config).await {
        println!("  Found {} valid paths", urls.len());
        total_urls.extend(urls);
    }

    // 5. Search (DuckDuckGo only for now)
    println!("\n{} Search engine discovery...", style("→").cyan());
    let search_source = DuckDuckGoSource::new();
    let terms = vec![
        "FOIA".to_string(),
        "documents".to_string(),
        "reports".to_string(),
    ];
    if let Ok(urls) = search_source.discover(&base_url, &terms, &config).await {
        println!("  Found {} search URLs", urls.len());
        total_urls.extend(urls);
    }

    // Deduplicate
    total_urls.sort_by(|a, b| a.url.cmp(&b.url));
    total_urls.dedup_by(|a, b| a.url == b.url);

    println!(
        "\n{} Total: {} unique URLs discovered",
        style("📊").cyan(),
        total_urls.len()
    );

    let listings = total_urls.iter().filter(|u| u.is_listing_page).count();
    println!(
        "  {} listing pages, {} document URLs",
        listings,
        total_urls.len() - listings
    );

    // Add to queue
    let added = add_discovered_urls(settings, source_id, total_urls, dry_run).await?;

    if !dry_run {
        println!("{} Added {} URLs to crawl queue", style("✓").green(), added);
        println!(
            "  Run {} to crawl discovered URLs",
            style(format!("foiacquire crawl {}", source_id)).cyan()
        );
    }

    Ok(())
}
