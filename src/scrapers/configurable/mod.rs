//! Configuration-based scraper for FOIAcquire.
//!
//! This scraper reads its behavior from JSON configuration, allowing
//! flexible definition of discovery and fetching strategies without
//! writing custom code for each source.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "browser")]
use super::browser::BrowserEngineConfig;
use super::config::{ScraperConfig, ViaMode};
use super::HttpClient;
use crate::models::Source;
#[allow(unused_imports)]
use crate::privacy::PrivacyConfig;
use crate::rate_limit::RateLimiter;
use crate::repository::DieselCrawlRepository;

mod api;
mod discovery;
mod extract;
mod fetch;
mod html_crawl;
mod stream;

/// Configurable scraper driven by JSON configuration.
pub struct ConfigurableScraper {
    pub(crate) source: Source,
    pub(crate) config: ScraperConfig,
    pub(crate) client: HttpClient,
    pub(crate) crawl_repo: Option<Arc<DieselCrawlRepository>>,
    /// Refresh TTL in days - URLs older than this will be re-checked.
    pub(crate) refresh_ttl_days: u64,
    /// Browser fetcher for anti-bot protected sites (created lazily when needed).
    #[cfg(feature = "browser")]
    pub(crate) browser_config: Option<BrowserEngineConfig>,
}

impl ConfigurableScraper {
    /// Create a new configurable scraper.
    pub fn new(
        source: Source,
        config: ScraperConfig,
        crawl_repo: Option<Arc<DieselCrawlRepository>>,
        request_delay: Duration,
        refresh_ttl_days: u64,
    ) -> Self {
        Self::with_rate_limiter(
            source,
            config,
            crawl_repo,
            request_delay,
            refresh_ttl_days,
            None,
        )
    }

    /// Create a new configurable scraper with a shared rate limiter.
    /// Uses direct connections (no privacy routing).
    pub fn with_rate_limiter(
        source: Source,
        config: ScraperConfig,
        crawl_repo: Option<Arc<DieselCrawlRepository>>,
        request_delay: Duration,
        refresh_ttl_days: u64,
        rate_limiter: Option<RateLimiter>,
    ) -> Self {
        // None privacy config = Direct mode, which never fails
        Self::with_rate_limiter_and_privacy(
            source,
            config,
            crawl_repo,
            request_delay,
            refresh_ttl_days,
            rate_limiter,
            None,
        )
        .expect("Direct mode scraper creation should never fail")
    }

    /// Create a new configurable scraper with a shared rate limiter and privacy config.
    ///
    /// The effective privacy config is determined by:
    /// 1. Starting with the global privacy config
    /// 2. Applying per-source overrides from scraper config's `privacy` field
    ///
    /// # Errors
    /// Returns an error if Tor mode is requested but Tor is not available.
    pub fn with_rate_limiter_and_privacy(
        source: Source,
        config: ScraperConfig,
        crawl_repo: Option<Arc<DieselCrawlRepository>>,
        request_delay: Duration,
        refresh_ttl_days: u64,
        rate_limiter: Option<RateLimiter>,
        privacy_config: Option<&PrivacyConfig>,
    ) -> Result<Self, String> {
        // Apply per-source privacy overrides to global config
        let effective_privacy = privacy_config.map(|global| config.privacy.apply_to(global));

        let client = match (rate_limiter, effective_privacy.as_ref()) {
            (Some(limiter), Some(privacy)) => HttpClient::with_rate_limiter_and_privacy(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                limiter,
                config.user_agent.as_deref(),
                privacy,
            )?,
            (Some(limiter), None) => HttpClient::with_rate_limiter_and_user_agent(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                limiter,
                config.user_agent.as_deref(),
            ),
            (None, Some(privacy)) => HttpClient::with_privacy(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                config.user_agent.as_deref(),
                privacy,
            )?,
            (None, None) => HttpClient::with_user_agent(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                config.user_agent.as_deref(),
            ),
        };
        let client = if let Some(repo) = crawl_repo.clone() {
            client.with_crawl_repo(repo)
        } else {
            client
        };

        #[cfg(feature = "browser")]
        let browser_config = config
            .browser
            .as_ref()
            .filter(|b| b.enabled)
            .map(|b| b.to_engine_config());

        Ok(Self {
            source,
            config,
            client,
            crawl_repo,
            refresh_ttl_days,
            #[cfg(feature = "browser")]
            browser_config,
        })
    }

    /// Check if browser mode is enabled.
    pub fn uses_browser(&self) -> bool {
        #[cfg(feature = "browser")]
        {
            self.browser_config.is_some()
        }
        #[cfg(not(feature = "browser"))]
        {
            false
        }
    }

    /// Configure URL rewriting for caching proxies with mode.
    ///
    /// The via mappings allow routing requests through a CDN (like Cloudflare)
    /// while preserving the original URL for metadata and rate limiting.
    ///
    /// Example: mapping "https://cia.gov" -> "https://cia.monokro.me" will
    /// fetch URLs starting with cia.gov through the Cloudflare proxy instead.
    pub fn with_via_config(mut self, via: HashMap<String, String>, via_mode: ViaMode) -> Self {
        self.client = self.client.with_via_config(via, via_mode);
        self
    }

    /// Configure URL rewriting for caching proxies (uses Strict mode).
    #[deprecated(note = "Use with_via_config instead to also set via_mode")]
    pub fn with_via_mappings(mut self, via: HashMap<String, String>) -> Self {
        #[allow(deprecated)]
        {
            self.client = self.client.with_via_mappings(via);
        }
        self
    }
}
