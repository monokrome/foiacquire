//! External discovery sources for scraping (sitemap, wayback, common paths, search).

use foia_scrape::discovery::sources::{
    common_paths::CommonPathsSource, search::DuckDuckGoSource, sitemap::SitemapSource,
    wayback::WaybackSource,
};
use foia_scrape::discovery::{DiscoveredUrl, DiscoverySource, DiscoverySourceConfig};

/// Run external discovery sources (sitemap, wayback, common paths, search engines).
pub(super) async fn run_external_discovery(
    base_url: &str,
    discovery_config: &foia_scrape::config::DiscoveryConfig,
    source_id: &str,
    privacy_config: &foia::privacy::PrivacyConfig,
) -> Vec<DiscoveredUrl> {
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
