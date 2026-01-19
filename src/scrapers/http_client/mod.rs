//! HTTP client with ETag and conditional request support.
//!
//! When `BROWSER_URL` environment variable is set, requests are routed through
//! the stealth browser for bot detection bypass.
//!
//! Privacy features:
//! - Routes requests through SOCKS proxy when `SOCKS_PROXY` env var is set
//! - Supports Tor with obfuscation (default) or direct Tor
//! - Can be configured to bypass proxy for specific sources

#![allow(dead_code)]
// This module is the privacy wrapper - it's allowed to use reqwest directly
#![allow(clippy::disallowed_methods)]

mod response;
mod user_agent;

#[allow(unused_imports)]
pub use response::{parse_content_disposition_filename, HeadResponse, HttpResponse};
#[allow(unused_imports)]
pub use user_agent::{resolve_user_agent, IMPERSONATE_USER_AGENTS, USER_AGENT};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::{Client, Proxy, StatusCode};
#[cfg(feature = "browser")]
use tracing::debug;

use super::config::ViaMode;
use super::rate_limiter::RateLimiter;
use super::InMemoryRateLimitBackend;
use crate::models::{CrawlRequest, CrawlUrl, UrlStatus};
use crate::privacy::{PrivacyConfig, PrivacyMode};
use crate::repository::DieselCrawlRepository;

#[cfg(feature = "browser")]
use super::browser::{BrowserPool, BrowserPoolConfig};

/// HTTP client with request logging and conditional request support.
///
/// When browser is configured (via `BROWSER_URL` env var), requests are
/// automatically routed through the browser pool. Multiple browsers can be
/// specified with comma-separated URLs for load balancing and failover.
///
/// Privacy routing:
/// - When `SOCKS_PROXY` is set, routes through that proxy
/// - When privacy config specifies Tor, routes through embedded Arti (if available)
/// - When direct mode is enabled, makes direct connections (with security warning)
///
/// URL rewriting (via):
/// - When `via_mappings` is configured, URLs matching a key prefix are rewritten
///   to fetch through a caching proxy (e.g., CloudFront, Cloudflare)
/// - The original URL is preserved in metadata for accurate record-keeping
#[derive(Clone)]
pub struct HttpClient {
    client: Client,
    crawl_repo: Option<Arc<DieselCrawlRepository>>,
    source_id: String,
    request_delay: Duration,
    referer: Option<String>,
    rate_limiter: RateLimiter,
    privacy_mode: PrivacyMode,
    /// URL rewriting mappings for caching proxies.
    /// Maps original base URLs to proxy URLs (e.g., "https://cia.gov" -> "https://cia.monokro.me")
    via_mappings: Arc<HashMap<String, String>>,
    /// Via mode controlling when via mappings are used for requests.
    via_mode: ViaMode,
    #[cfg(feature = "browser")]
    browser_pool: Option<Arc<BrowserPool>>,
}

impl HttpClient {
    /// Rewrite a URL using via mappings if a matching prefix is found.
    /// Returns the rewritten URL (for fetching) and whether it was rewritten.
    ///
    /// The original URL should be stored in metadata for accurate record-keeping,
    /// while the rewritten URL is used for the actual HTTP request.
    fn apply_via_rewrite(&self, url: &str) -> (String, bool) {
        for (from_prefix, to_prefix) in self.via_mappings.iter() {
            if url.starts_with(from_prefix) {
                let rewritten = format!("{}{}", to_prefix, &url[from_prefix.len()..]);
                tracing::debug!(
                    "Via rewrite: {} -> {} (via {})",
                    url,
                    rewritten,
                    to_prefix
                );
                return (rewritten, true);
            }
        }
        (url.to_string(), false)
    }

    /// Create browser pool from BROWSER_URL env var.
    /// Supports comma-separated URLs for multiple browsers.
    #[cfg(feature = "browser")]
    fn create_browser_pool() -> Option<Arc<BrowserPool>> {
        BrowserPoolConfig::from_env().map(|config| {
            debug!(
                "BROWSER_URL set, enabling browser pool with {} browser(s)",
                config.urls.len()
            );
            Arc::new(BrowserPool::new(config))
        })
    }

    /// Build a reqwest Client with the appropriate proxy settings.
    ///
    /// # Errors
    /// Returns an error if Tor mode is requested but no Tor is available
    /// (neither embedded Arti nor external SOCKS_PROXY). This enforces
    /// fail-closed security - we refuse to silently fall back to direct connections.
    fn build_client(
        user_agent: &str,
        timeout: Duration,
        privacy_config: Option<&PrivacyConfig>,
    ) -> Result<(Client, PrivacyMode), String> {
        let mut builder = Client::builder()
            .user_agent(user_agent)
            .timeout(timeout)
            .gzip(true)
            .brotli(true);

        let mode = privacy_config
            .map(|c| c.mode())
            .unwrap_or(PrivacyMode::Direct);

        // Configure proxy based on privacy mode
        match &mode {
            PrivacyMode::ExternalProxy => {
                // Use external SOCKS proxy from SOCKS_PROXY env var
                if let Some(config) = privacy_config {
                    if let Some(proxy_url) = config.proxy_url() {
                        // Validate SOCKS URL scheme
                        if !proxy_url.starts_with("socks5://")
                            && !proxy_url.starts_with("socks5h://")
                        {
                            return Err(format!(
                                "Invalid SOCKS proxy URL: '{}'. Must start with socks5:// or socks5h://",
                                proxy_url
                            ));
                        }
                        let proxy = Proxy::all(proxy_url).map_err(|e| {
                            format!("Invalid SOCKS proxy URL '{}': {}", proxy_url, e)
                        })?;
                        builder = builder.proxy(proxy);
                    } else {
                        return Err("ExternalProxy mode but no proxy URL configured".to_string());
                    }
                } else {
                    return Err("ExternalProxy mode but no privacy config provided".to_string());
                }
            }
            PrivacyMode::TorObfuscated(_) | PrivacyMode::TorDirect => {
                // Fail-closed: Tor mode requires Tor to be available
                // Check if embedded Arti is ready
                #[cfg(feature = "embedded-tor")]
                {
                    if let Some(proxy_url) = crate::privacy::get_arti_socks_url() {
                        let proxy = Proxy::all(&proxy_url)
                            .map_err(|e| format!("Failed to configure Arti proxy: {}", e))?;
                        builder = builder.proxy(proxy);
                    } else {
                        return Err("Tor mode requested but Arti is not bootstrapped. \
                             Either wait for Arti to initialize, set SOCKS_PROXY for an external \
                             Tor instance, or use --direct to disable privacy (not recommended)."
                            .to_string());
                    }
                }
                #[cfg(not(feature = "embedded-tor"))]
                {
                    return Err("Tor mode requested but embedded Tor is not available \
                         (compiled without 'embedded-tor' feature). Either set SOCKS_PROXY \
                         to an external Tor instance, or use --direct to disable privacy \
                         (not recommended)."
                        .to_string());
                }
            }
            PrivacyMode::Direct => {
                // No proxy - direct connection (user explicitly opted out of privacy)
            }
        }

        let client = builder
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;
        Ok((client, mode))
    }

    /// Create a new HTTP client with privacy configuration from environment.
    /// Respects SOCKS_PROXY env var if set, otherwise uses direct connections.
    /// This makes privacy opt-out instead of opt-in for better security.
    pub fn new(source_id: &str, timeout: Duration, request_delay: Duration) -> Self {
        Self::with_user_agent(source_id, timeout, request_delay, None)
    }

    /// Create a new HTTP client with custom user agent configuration.
    /// Respects SOCKS_PROXY env var if set, otherwise uses direct connections.
    /// - None: Use default FOIAcquire user agent
    /// - Some("impersonate"): Use random real browser user agent
    /// - Some(custom): Use custom user agent string
    pub fn with_user_agent(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        user_agent_config: Option<&str>,
    ) -> Self {
        let user_agent = resolve_user_agent(user_agent_config);

        // Read privacy config from environment (SOCKS_PROXY, etc.)
        // If no env vars set, this defaults to Direct mode (backward compatible)
        let default_privacy = PrivacyConfig::default().with_env_overrides();

        // Build client with env privacy config
        // If SOCKS_PROXY is set but invalid, fall back to direct mode with warning
        let (client, privacy_mode) = Self::build_client(&user_agent, timeout, Some(&default_privacy))
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to configure privacy from environment: {}", e);
                eprintln!("         Falling back to direct connections (no privacy)");
                Self::build_client(&user_agent, timeout, None)
                    .expect("Direct mode fallback should never fail")
            });

        // Default in-memory backend with request_delay as base
        let backend = Arc::new(InMemoryRateLimitBackend::new(
            request_delay.as_millis() as u64
        ));
        Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter: RateLimiter::new(backend),
            privacy_mode,
            via_mappings: Arc::new(HashMap::new()),
            via_mode: ViaMode::default(),
            #[cfg(feature = "browser")]
            browser_pool: Self::create_browser_pool(),
        }
    }

    /// Create a new HTTP client with privacy configuration.
    /// - None: Use default FOIAcquire user agent
    /// - Some("impersonate"): Use random real browser user agent
    /// - Some(custom): Use custom user agent string
    ///
    /// # Errors
    /// Returns an error if Tor mode is requested but Tor is not available.
    pub fn with_privacy(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        user_agent_config: Option<&str>,
        privacy_config: &PrivacyConfig,
    ) -> Result<Self, String> {
        let user_agent = resolve_user_agent(user_agent_config);
        let (client, privacy_mode) =
            Self::build_client(&user_agent, timeout, Some(privacy_config))?;

        // Default in-memory backend with request_delay as base
        let backend = Arc::new(InMemoryRateLimitBackend::new(
            request_delay.as_millis() as u64
        ));
        Ok(Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter: RateLimiter::new(backend),
            privacy_mode,
            via_mappings: Arc::new(HashMap::new()),
            via_mode: ViaMode::default(),
            #[cfg(feature = "browser")]
            browser_pool: Self::create_browser_pool(),
        })
    }

    /// Create a new HTTP client with via URL rewriting for caching proxies.
    ///
    /// # Arguments
    /// * `source_id` - Identifier for this source (for logging/rate limiting)
    /// * `timeout` - Request timeout
    /// * `request_delay` - Delay between requests
    /// * `user_agent_config` - User agent configuration
    /// * `privacy_config` - Privacy/proxy configuration
    /// * `via_mappings` - URL prefix mappings for caching proxies
    /// * `via_mode` - Controls when via mappings are used for requests
    ///
    /// # Errors
    /// Returns an error if Tor mode is requested but Tor is not available.
    pub fn with_via(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        user_agent_config: Option<&str>,
        privacy_config: &PrivacyConfig,
        via_mappings: HashMap<String, String>,
        via_mode: ViaMode,
    ) -> Result<Self, String> {
        let user_agent = resolve_user_agent(user_agent_config);
        let (client, privacy_mode) =
            Self::build_client(&user_agent, timeout, Some(privacy_config))?;

        // Default in-memory backend with request_delay as base
        let backend = Arc::new(InMemoryRateLimitBackend::new(
            request_delay.as_millis() as u64,
        ));

        if !via_mappings.is_empty() {
            tracing::info!(
                "HTTP client configured with {} via mapping(s) for caching proxy (mode: {:?})",
                via_mappings.len(),
                via_mode
            );
            for (from, to) in &via_mappings {
                tracing::debug!("  Via: {} -> {}", from, to);
            }
        }

        Ok(Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter: RateLimiter::new(backend),
            privacy_mode,
            via_mappings: Arc::new(via_mappings),
            via_mode,
            #[cfg(feature = "browser")]
            browser_pool: Self::create_browser_pool(),
        })
    }

    /// Create a new HTTP client with a shared rate limiter.
    /// Respects SOCKS_PROXY env var if set, otherwise uses direct connections.
    pub fn with_rate_limiter(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        rate_limiter: RateLimiter,
    ) -> Self {
        Self::with_rate_limiter_and_user_agent(
            source_id,
            timeout,
            request_delay,
            rate_limiter,
            None,
        )
    }

    /// Create a new HTTP client with a shared rate limiter and custom user agent.
    /// Respects SOCKS_PROXY env var if set, otherwise uses direct connections.
    pub fn with_rate_limiter_and_user_agent(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        rate_limiter: RateLimiter,
        user_agent_config: Option<&str>,
    ) -> Self {
        let user_agent = resolve_user_agent(user_agent_config);

        // Read privacy config from environment (SOCKS_PROXY, etc.)
        let default_privacy = PrivacyConfig::default().with_env_overrides();

        // Build client with env privacy config
        let (client, privacy_mode) = Self::build_client(&user_agent, timeout, Some(&default_privacy))
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to configure privacy from environment: {}", e);
                eprintln!("         Falling back to direct connections (no privacy)");
                Self::build_client(&user_agent, timeout, None)
                    .expect("Direct mode fallback should never fail")
            });

        Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter,
            privacy_mode,
            via_mappings: Arc::new(HashMap::new()),
            via_mode: ViaMode::default(),
            #[cfg(feature = "browser")]
            browser_pool: Self::create_browser_pool(),
        }
    }

    /// Create a new HTTP client with a shared rate limiter and privacy configuration.
    ///
    /// # Errors
    /// Returns an error if Tor mode is requested but Tor is not available.
    pub fn with_rate_limiter_and_privacy(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        rate_limiter: RateLimiter,
        user_agent_config: Option<&str>,
        privacy_config: &PrivacyConfig,
    ) -> Result<Self, String> {
        let user_agent = resolve_user_agent(user_agent_config);
        let (client, privacy_mode) =
            Self::build_client(&user_agent, timeout, Some(privacy_config))?;

        Ok(Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter,
            privacy_mode,
            via_mappings: Arc::new(HashMap::new()),
            via_mode: ViaMode::default(),
            #[cfg(feature = "browser")]
            browser_pool: Self::create_browser_pool(),
        })
    }

    /// Set the via mappings and mode for URL rewriting (caching proxy support).
    pub fn with_via_config(
        mut self,
        via: HashMap<String, String>,
        via_mode: ViaMode,
    ) -> Self {
        if !via.is_empty() {
            tracing::info!(
                "HTTP client configured with {} via mapping(s) for caching proxy (mode: {:?})",
                via.len(),
                via_mode
            );
            for (from, to) in &via {
                tracing::debug!("  Via: {} -> {}", from, to);
            }
        }
        self.via_mappings = Arc::new(via);
        self.via_mode = via_mode;
        self
    }

    /// Set the via mappings for URL rewriting (caching proxy support).
    /// Uses default via_mode (Strict).
    #[deprecated(note = "Use with_via_config instead to also set via_mode")]
    pub fn with_via_mappings(mut self, via: HashMap<String, String>) -> Self {
        if !via.is_empty() {
            tracing::info!(
                "HTTP client configured with {} via mapping(s) for caching proxy",
                via.len()
            );
            for (from, to) in &via {
                tracing::debug!("  Via: {} -> {}", from, to);
            }
        }
        self.via_mappings = Arc::new(via);
        self
    }

    /// Set the crawl repository for request logging.
    pub fn with_crawl_repo(mut self, repo: Arc<DieselCrawlRepository>) -> Self {
        self.crawl_repo = Some(repo);
        self
    }

    /// Set the Referer header for requests.
    pub fn with_referer(mut self, referer: String) -> Self {
        self.referer = Some(referer);
        self
    }

    /// Get the rate limiter for this client.
    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    /// Get the privacy mode for this client.
    pub fn privacy_mode(&self) -> PrivacyMode {
        self.privacy_mode
    }

    /// Check if this client is using a proxy (Tor or external).
    pub fn is_proxied(&self) -> bool {
        !matches!(self.privacy_mode, PrivacyMode::Direct)
    }

    /// Get the via mappings for URL rewriting detection.
    /// Useful for detecting when URLs will be rewritten to specific domains.
    pub fn via_mappings(&self) -> &HashMap<String, String> {
        &self.via_mappings
    }

    /// Make a GET request with optional conditional headers.
    /// Uses adaptive rate limiting per domain.
    /// When BROWSER_URL is configured, routes through browser pool.
    pub async fn get(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Check if browser mode is enabled
        #[cfg(feature = "browser")]
        if let Some(ref pool) = self.browser_pool {
            return self.get_via_browser_pool(pool, url).await;
        }

        self.get_via_reqwest(url, etag, last_modified).await
    }

    /// Fetch via browser pool (with load balancing and failover).
    /// Respects via_mode setting for URL rewriting behavior.
    #[cfg(feature = "browser")]
    async fn get_via_browser_pool(
        &self,
        pool: &Arc<BrowserPool>,
        url: &str,
    ) -> Result<HttpResponse, reqwest::Error> {
        let (via_url, has_via) = self.apply_via_rewrite(url);

        // Determine initial URL based on via_mode
        let (initial_url, can_fallback) = match self.via_mode {
            ViaMode::Strict => (url.to_string(), false),
            ViaMode::Fallback => (url.to_string(), has_via),
            ViaMode::Priority => {
                if has_via {
                    (via_url.clone(), true)
                } else {
                    (url.to_string(), false)
                }
            }
        };

        // Make first browser request
        if let Some(response) = self.do_browser_fetch(pool, &initial_url, url).await {
            let status = response.status.as_u16();
            let should_retry = can_fallback && (status == 429 || status == 503);

            if should_retry {
                let alternate_url = match self.via_mode {
                    ViaMode::Fallback => &via_url,
                    ViaMode::Priority => url,
                    ViaMode::Strict => return Ok(response),
                };

                tracing::info!(
                    "Via {:?} mode: retrying {} with alternate URL {}",
                    self.via_mode,
                    url,
                    alternate_url
                );

                tokio::time::sleep(self.request_delay).await;

                // Try alternate URL via browser
                if let Some(retry_response) =
                    self.do_browser_fetch(pool, alternate_url, url).await
                {
                    return Ok(retry_response);
                }
                // Browser retry failed, fall back to reqwest
                return self.get_via_reqwest(url, None, None).await;
            }

            return Ok(response);
        }

        // Browser failed completely, fall back to reqwest
        debug!("Browser pool exhausted, falling back to reqwest for {}", url);
        self.get_via_reqwest(url, None, None).await
    }

    /// Internal: perform a single browser fetch and handle logging/rate limiting.
    /// Returns None if browser fetch fails (caller should fall back to reqwest).
    #[cfg(feature = "browser")]
    async fn do_browser_fetch(
        &self,
        pool: &Arc<BrowserPool>,
        fetch_url: &str,
        original_url: &str,
    ) -> Option<HttpResponse> {
        let domain = self.rate_limiter.acquire(original_url).await;
        let start = Instant::now();

        let result = pool.fetch(fetch_url).await;
        let duration = start.elapsed();

        match result {
            Ok(browser_response) => {
                let status_code = browser_response.status;

                let mut request_log = CrawlRequest::new(
                    self.source_id.clone(),
                    original_url.to_string(),
                    "GET".to_string(),
                );
                request_log.response_at = Some(Utc::now());
                request_log.duration_ms = Some(duration.as_millis() as u64);
                request_log.response_status = Some(status_code);

                if let Some(repo) = &self.crawl_repo {
                    let _ = repo.log_request(&request_log).await;
                }

                if let Some(ref domain) = domain {
                    if (200..400).contains(&status_code) {
                        self.rate_limiter.report_success(domain).await;
                    } else if status_code == 429 || status_code == 503 {
                        self.rate_limiter
                            .report_rate_limit(domain, status_code)
                            .await;
                    }
                }

                tokio::time::sleep(self.request_delay).await;

                let mut headers = HashMap::new();
                headers.insert("content-type".to_string(), browser_response.content_type);

                Some(HttpResponse::from_bytes(
                    StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK),
                    headers,
                    browser_response.content.into_bytes(),
                ))
            }
            Err(e) => {
                debug!(
                    "Browser pool fetch failed for {}: {}",
                    original_url, e
                );

                if let Some(ref domain) = domain {
                    self.rate_limiter.report_server_error(domain).await;
                }

                None
            }
        }
    }

    /// Fetch via reqwest (direct HTTP).
    /// Respects via_mode setting for URL rewriting behavior.
    async fn get_via_reqwest(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HttpResponse, reqwest::Error> {
        let (via_url, has_via) = self.apply_via_rewrite(url);

        // Determine initial URL based on via_mode
        let (initial_url, can_fallback) = match self.via_mode {
            ViaMode::Strict => (url.to_string(), false),
            ViaMode::Fallback => (url.to_string(), has_via),
            ViaMode::Priority => {
                if has_via {
                    (via_url.clone(), true)
                } else {
                    (url.to_string(), false)
                }
            }
        };

        // Make first request
        let result = self
            .do_get_reqwest(&initial_url, url, etag, last_modified)
            .await?;

        // Check if we should retry with alternate URL
        let status = result.status.as_u16();
        let should_retry = can_fallback && (status == 429 || status == 503);

        if should_retry {
            let alternate_url = match self.via_mode {
                ViaMode::Fallback => &via_url, // Rate limited on original, try via
                ViaMode::Priority => url,      // Failed on via, try original
                ViaMode::Strict => return Ok(result), // Never retry in strict
            };

            tracing::info!(
                "Via {:?} mode: retrying {} with alternate URL {}",
                self.via_mode,
                url,
                alternate_url
            );

            // Small delay before retry
            tokio::time::sleep(self.request_delay).await;

            return self
                .do_get_reqwest(alternate_url, url, etag, last_modified)
                .await;
        }

        Ok(result)
    }

    /// Internal: perform a single GET request and handle logging/rate limiting.
    async fn do_get_reqwest(
        &self,
        fetch_url: &str,
        original_url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(original_url).await;

        let mut request = self.client.get(fetch_url);

        let mut headers = HashMap::new();

        // Add conditional request headers
        if let Some(etag) = etag {
            request = request.header("If-None-Match", etag);
            headers.insert("If-None-Match".to_string(), etag.to_string());
        }
        if let Some(lm) = last_modified {
            request = request.header("If-Modified-Since", lm);
            headers.insert("If-Modified-Since".to_string(), lm.to_string());
        }

        let was_conditional = etag.is_some() || last_modified.is_some();

        // Create request log (always log original URL for accurate records)
        let mut request_log = CrawlRequest::new(
            self.source_id.clone(),
            original_url.to_string(),
            "GET".to_string(),
        );
        request_log.request_headers = headers;
        request_log.was_conditional = was_conditional;

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);
        request_log.was_not_modified = response.status() == StatusCode::NOT_MODIFIED;

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter for adaptive backoff
        if let Some(ref domain) = domain {
            let has_retry_after = response_headers.contains_key("retry-after");

            if status_code == 429 || status_code == 503 {
                // Definite rate limit
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code == 403 {
                // Possible rate limit - needs pattern detection
                self.rate_limiter
                    .report_403(domain, original_url, has_retry_after)
                    .await;
            } else if status_code >= 500 {
                // Server error - mild backoff
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() || status_code == 304 {
                // Success - may recover from backoff
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay (rate limiter handles additional adaptive delay)
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse::from_reqwest(
            response.status(),
            response_headers,
            response,
        ))
    }

    /// Get page content as text.
    pub async fn get_text(&self, url: &str) -> Result<String, reqwest::Error> {
        let response = self.get(url, None, None).await?;
        response.text().await
    }

    /// GET request with custom headers.
    pub async fn get_with_headers(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Apply via rewriting if configured (fetch via caching proxy)
        let (fetch_url, _via_rewritten) = self.apply_via_rewrite(url);

        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(url).await;

        let mut request = self.client.get(&fetch_url);
        for (name, value) in &headers {
            request = request.header(name, value);
        }

        // Create request log
        let mut request_log =
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "GET".to_string());
        request_log.request_headers = headers.clone();

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter for adaptive backoff
        if let Some(ref domain) = domain {
            if status_code == 429 || status_code == 503 {
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code >= 500 {
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() {
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse::from_reqwest(
            response.status(),
            response_headers,
            response,
        ))
    }

    /// Make a POST request with form data.
    ///
    /// Note: Browser pool is not used for POST requests - they always go through reqwest.
    pub async fn post<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        form: &T,
    ) -> Result<HttpResponse, reqwest::Error> {
        self.post_via_reqwest(url, form).await
    }

    /// Make a POST request with JSON body.
    ///
    /// Note: Browser pool is not used for POST requests - they always go through reqwest.
    pub async fn post_json<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        json: &T,
    ) -> Result<HttpResponse, reqwest::Error> {
        self.post_json_via_reqwest(url, json).await
    }

    /// POST JSON request with custom headers.
    pub async fn post_json_with_headers<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        json: &T,
        headers: HashMap<String, String>,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Apply via rewriting if configured (fetch via caching proxy)
        let (fetch_url, _via_rewritten) = self.apply_via_rewrite(url);

        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(url).await;

        let mut request = self.client.post(&fetch_url).json(json);
        for (name, value) in &headers {
            request = request.header(name, value);
        }

        // Create request log (log original URL, not the via-rewritten one)
        let mut request_log =
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "POST".to_string());
        request_log.request_headers = headers.clone();

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter for adaptive backoff
        if let Some(ref domain) = domain {
            if status_code == 429 || status_code == 503 {
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code >= 500 {
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() {
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse::from_reqwest(
            response.status(),
            response_headers,
            response,
        ))
    }

    /// POST via reqwest (direct HTTP).
    async fn post_via_reqwest<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        form: &T,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Apply via rewriting if configured (fetch via caching proxy)
        let (fetch_url, _via_rewritten) = self.apply_via_rewrite(url);

        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(url).await;

        let request = self.client.post(&fetch_url).form(form);

        // Create request log
        let mut request_log =
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "POST".to_string());

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter for adaptive backoff
        if let Some(ref domain) = domain {
            let has_retry_after = response_headers.contains_key("retry-after");

            if status_code == 429 || status_code == 503 {
                // Definite rate limit
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code == 403 {
                // Possible rate limit - needs pattern detection
                self.rate_limiter
                    .report_403(domain, url, has_retry_after)
                    .await;
            } else if status_code >= 500 {
                // Server error - mild backoff
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() {
                // Success - may recover from backoff
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay (rate limiter handles additional adaptive delay)
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse::from_reqwest(
            response.status(),
            response_headers,
            response,
        ))
    }

    /// POST JSON via reqwest (direct HTTP).
    async fn post_json_via_reqwest<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        json: &T,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Apply via rewriting if configured (fetch via caching proxy)
        let (fetch_url, _via_rewritten) = self.apply_via_rewrite(url);

        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(url).await;

        let request = self.client.post(&fetch_url).json(json);

        // Create request log
        let mut request_log =
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "POST".to_string());

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter for adaptive backoff
        if let Some(ref domain) = domain {
            let has_retry_after = response_headers.contains_key("retry-after");

            if status_code == 429 || status_code == 503 {
                // Definite rate limit
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code == 403 {
                // Possible rate limit - needs pattern detection
                self.rate_limiter
                    .report_403(domain, url, has_retry_after)
                    .await;
            } else if status_code >= 500 {
                // Server error - mild backoff
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() {
                // Success - may recover from backoff
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay (rate limiter handles additional adaptive delay)
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse::from_reqwest(
            response.status(),
            response_headers,
            response,
        ))
    }

    /// Make a HEAD request to check headers without downloading content.
    /// Returns headers including ETag, Last-Modified, Content-Disposition, etc.
    pub async fn head(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HeadResponse, reqwest::Error> {
        // Apply via rewriting if configured (fetch via caching proxy)
        let (fetch_url, _via_rewritten) = self.apply_via_rewrite(url);

        // Wait for rate limiter before making request (use original URL for rate limiting)
        let domain = self.rate_limiter.acquire(url).await;

        let mut request = self.client.head(&fetch_url);

        let mut headers = HashMap::new();

        // Add conditional request headers
        if let Some(etag) = etag {
            request = request.header("If-None-Match", etag);
            headers.insert("If-None-Match".to_string(), etag.to_string());
        }
        if let Some(lm) = last_modified {
            request = request.header("If-Modified-Since", lm);
            headers.insert("If-Modified-Since".to_string(), lm.to_string());
        }

        let was_conditional = etag.is_some() || last_modified.is_some();

        // Create request log
        let mut request_log =
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "HEAD".to_string());
        request_log.request_headers = headers;
        request_log.was_conditional = was_conditional;

        let start = Instant::now();
        let response = request.send().await?;
        let duration = start.elapsed();

        let status_code = response.status().as_u16();

        // Update request log
        request_log.response_at = Some(Utc::now());
        request_log.duration_ms = Some(duration.as_millis() as u64);
        request_log.response_status = Some(status_code);
        request_log.was_not_modified = response.status() == StatusCode::NOT_MODIFIED;

        // Extract response headers
        let mut response_headers = HashMap::new();
        for (name, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(name.to_string(), v.to_string());
            }
        }
        request_log.response_headers = response_headers.clone();

        // Log the request
        if let Some(repo) = &self.crawl_repo {
            let _ = repo.log_request(&request_log).await;
        }

        // Report status to rate limiter
        if let Some(ref domain) = domain {
            let has_retry_after = response_headers.contains_key("retry-after");

            if status_code == 429 || status_code == 503 {
                self.rate_limiter
                    .report_rate_limit(domain, status_code)
                    .await;
            } else if status_code == 403 {
                self.rate_limiter
                    .report_403(domain, url, has_retry_after)
                    .await;
            } else if status_code >= 500 {
                self.rate_limiter.report_server_error(domain).await;
            } else if response.status().is_success() || status_code == 304 {
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay
        tokio::time::sleep(self.request_delay).await;

        Ok(HeadResponse {
            status: response.status(),
            headers: response_headers,
        })
    }

    /// Update crawl URL status to fetching.
    pub async fn mark_fetching(&self, url: &str) {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url).await {
                crawl_url.mark_fetching();
                let _ = repo.update_url(&crawl_url).await;
            }
        }
    }

    /// Update crawl URL status after successful fetch.
    pub async fn mark_fetched(
        &self,
        url: &str,
        content_hash: Option<String>,
        document_id: Option<String>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url).await {
                crawl_url.mark_fetched(content_hash, document_id, etag, last_modified);
                let _ = repo.update_url(&crawl_url).await;
            }
        }
    }

    /// Update crawl URL status after skip (304 Not Modified).
    pub async fn mark_skipped(&self, url: &str, reason: &str) {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url).await {
                crawl_url.mark_skipped(reason);
                let _ = repo.update_url(&crawl_url).await;
            }
        }
    }

    /// Update crawl URL status after failure.
    pub async fn mark_failed(&self, url: &str, error: &str) {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url).await {
                crawl_url.mark_failed(error, 3);
                let _ = repo.update_url(&crawl_url).await;
            }
        }
    }

    /// Track a discovered URL.
    pub async fn track_url(&self, crawl_url: &CrawlUrl) -> bool {
        if let Some(repo) = &self.crawl_repo {
            repo.add_url(crawl_url).await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Check if URL was already fetched.
    pub async fn is_fetched(&self, url: &str) -> bool {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(crawl_url)) = repo.get_url(&self.source_id, url).await {
                matches!(crawl_url.status, UrlStatus::Fetched | UrlStatus::Skipped)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Get cached headers for a URL.
    pub async fn get_cached_headers(&self, url: &str) -> (Option<String>, Option<String>) {
        if let Some(repo) = &self.crawl_repo {
            if let Ok(Some(crawl_url)) = repo.get_url(&self.source_id, url).await {
                return (crawl_url.etag, crawl_url.last_modified);
            }
        }
        (None, None)
    }
}
