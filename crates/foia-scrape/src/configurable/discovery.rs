//! Discovery dispatchers for the configurable scraper.

use std::sync::Arc;

use super::ConfigurableScraper;
use crate::config::ScraperConfig;
use crate::HttpClient;
#[cfg(feature = "browser")]
use foia::browser::BrowserEngineConfig;
use foia::repository::DieselCrawlRepository;

impl ConfigurableScraper {
    /// Streaming discovery that sends URLs as they're found (with browser support).
    #[cfg(feature = "browser")]
    pub(crate) async fn discover_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<DieselCrawlRepository>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
        browser_config: &Option<BrowserEngineConfig>,
    ) {
        match config.discovery.discovery_type.as_str() {
            "html_crawl" => {
                Self::discover_html_crawl_streaming(
                    config,
                    client,
                    source_id,
                    crawl_repo,
                    url_tx,
                    browser_config,
                )
                .await;
            }
            "api_paginated" => {
                Self::discover_api_paginated_streaming(
                    config, client, source_id, crawl_repo, url_tx,
                )
                .await;
            }
            "api_cursor" => {
                Self::discover_api_cursor_streaming(config, client, source_id, crawl_repo, url_tx)
                    .await;
            }
            _ => {}
        }
    }

    /// Streaming discovery that sends URLs as they're found (without browser).
    #[cfg(not(feature = "browser"))]
    pub(crate) async fn discover_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<DieselCrawlRepository>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
    ) {
        match config.discovery.discovery_type.as_str() {
            "html_crawl" => {
                Self::discover_html_crawl_streaming_no_browser(
                    config, client, source_id, crawl_repo, url_tx,
                )
                .await;
            }
            "api_paginated" => {
                Self::discover_api_paginated_streaming(
                    config, client, source_id, crawl_repo, url_tx,
                )
                .await;
            }
            "api_cursor" => {
                Self::discover_api_cursor_streaming(config, client, source_id, crawl_repo, url_tx)
                    .await;
            }
            _ => {}
        }
    }

    /// Discover document URLs (legacy non-streaming interface).
    pub async fn discover(&self) -> Vec<String> {
        match self.config.discovery.discovery_type.as_str() {
            "html_crawl" => self.discover_html_crawl().await,
            "api_paginated" => self.discover_api_paginated().await,
            "api_cursor" => self.discover_api_cursor().await,
            "api_nested" => self.discover_api_nested().await,
            _ => Vec::new(),
        }
    }
}
