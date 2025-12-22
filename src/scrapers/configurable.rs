//! Configuration-based scraper for FOIAcquire.
//!
//! This scraper reads its behavior from JSON configuration, allowing
//! flexible definition of discovery and fetching strategies without
//! writing custom code for each source.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use regex::Regex;
use scraper::{Html, Selector};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

use super::browser::BrowserEngineConfig;
#[cfg(feature = "browser")]
use super::browser::BrowserFetcher;
use super::config::{PaginationConfig, ScraperConfig, UrlExtractionConfig};
use super::rate_limiter::RateLimiter;
use super::{extract_title_from_url, HttpClient, ScrapeStream, ScraperResult};
use crate::models::{CrawlUrl, DiscoveryMethod, Source};
use crate::repository::CrawlRepository;

/// Configurable scraper driven by JSON configuration.
pub struct ConfigurableScraper {
    source: Source,
    config: ScraperConfig,
    client: HttpClient,
    crawl_repo: Option<Arc<Mutex<CrawlRepository>>>,
    /// Refresh TTL in days - URLs older than this will be re-checked.
    refresh_ttl_days: u64,
    /// Browser fetcher for anti-bot protected sites (created lazily when needed).
    #[cfg(feature = "browser")]
    browser_config: Option<BrowserEngineConfig>,
}

/// Default number of concurrent downloads.
const DEFAULT_CONCURRENCY: usize = 4;

/// Resolve a path to a full URL, handling both absolute and relative paths.
fn resolve_url(base_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else {
        format!("{}{}", base_url, path)
    }
}

impl ConfigurableScraper {
    /// Create a new configurable scraper.
    pub fn new(
        source: Source,
        config: ScraperConfig,
        crawl_repo: Option<Arc<Mutex<CrawlRepository>>>,
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
    pub fn with_rate_limiter(
        source: Source,
        config: ScraperConfig,
        crawl_repo: Option<Arc<Mutex<CrawlRepository>>>,
        request_delay: Duration,
        refresh_ttl_days: u64,
        rate_limiter: Option<RateLimiter>,
    ) -> Self {
        let client = if let Some(limiter) = rate_limiter {
            HttpClient::with_rate_limiter_and_user_agent(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                limiter,
                config.user_agent.as_deref(),
            )
        } else {
            HttpClient::with_user_agent(
                &source.id,
                Duration::from_secs(30),
                request_delay,
                config.user_agent.as_deref(),
            )
        };
        let client = if let Some(repo) = crawl_repo.clone() {
            client.with_crawl_repo(repo)
        } else {
            client
        };

        // Extract browser config if enabled
        #[cfg(feature = "browser")]
        let browser_config = config
            .browser
            .as_ref()
            .filter(|b| b.enabled)
            .map(|b| b.to_engine_config());

        Self {
            source,
            config,
            client,
            crawl_repo,
            refresh_ttl_days,
            #[cfg(feature = "browser")]
            browser_config,
        }
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

    /// Scrape documents from the source (legacy batch interface).
    pub async fn scrape(&self) -> Vec<ScraperResult> {
        let mut results = Vec::new();
        let stream = self.scrape_stream(DEFAULT_CONCURRENCY).await;
        let mut rx = stream.receiver;
        while let Some(result) = rx.recv().await {
            results.push(result);
        }
        results
    }

    /// Scrape documents with streaming results.
    /// Returns a ScrapeStream with the receiver and optional total count.
    pub async fn scrape_stream(&self, concurrency: usize) -> ScrapeStream {
        let (result_tx, result_rx) = tokio::sync::mpsc::channel::<ScraperResult>(100);
        let (url_tx, url_rx) = tokio::sync::mpsc::channel::<String>(500);

        // Query total count from API if available
        let total_count = self.get_api_total_count().await;

        // Spawn download workers
        let workers = self
            .spawn_download_workers(concurrency, url_rx, result_tx.clone())
            .await;

        // Spawn discovery task
        let discovery_handle = self.spawn_discovery_task(url_tx).await;

        // Spawn coordinator to clean up when done
        tokio::spawn(async move {
            // Wait for discovery to complete
            let _ = discovery_handle.await;
            // Workers will exit when url_rx is closed (all senders dropped)
            for worker in workers {
                let _ = worker.await;
            }
            // result_tx is dropped here, closing result_rx
        });

        ScrapeStream {
            receiver: result_rx,
            total_count,
        }
    }

    /// Query the total count from an API source.
    /// Returns None for non-API sources or if count is not available.
    async fn get_api_total_count(&self) -> Option<u64> {
        let api = self.config.discovery.api.as_ref()?;

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        // Build the first page URL to get the count
        let mut params: Vec<(String, String)> = Vec::new();
        params.push((api.pagination.page_param.clone(), "1".to_string()));
        if let Some(ref size_param) = api.pagination.page_size_param {
            params.push((size_param.clone(), "1".to_string())); // Just request 1 item to minimize data
        }

        let url_with_params = format!(
            "{}?{}",
            api_url,
            params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&")
        );

        let response = match self.client.get(&url_with_params, None, None).await {
            Ok(r) if r.is_success() => r,
            _ => return None,
        };

        let data: serde_json::Value = match response.text().await {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => return None,
        };

        // Try common count field names
        let count = data
            .get("count")
            .or_else(|| data.get("total"))
            .or_else(|| data.get("total_count"))
            .or_else(|| data.get("totalResults"))
            .and_then(|v| v.as_u64());

        if let Some(c) = count {
            debug!("API reports {} total documents", c);
        }

        count
    }

    /// Spawn worker tasks to download URLs concurrently.
    async fn spawn_download_workers(
        &self,
        count: usize,
        url_rx: tokio::sync::mpsc::Receiver<String>,
        result_tx: tokio::sync::mpsc::Sender<ScraperResult>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let url_rx = Arc::new(tokio::sync::Mutex::new(url_rx));
        let mut handles = Vec::with_capacity(count);

        // Get browser config if enabled
        #[cfg(feature = "browser")]
        let browser_config = self.browser_config.clone();

        // Get binary_fetch and context URL for PDF downloads
        let binary_fetch = self.config.fetch.binary_fetch;
        let context_url = self
            .config
            .base_url
            .clone()
            .or_else(|| self.config.discovery.base_url.clone());

        for _ in 0..count {
            let url_rx = url_rx.clone();
            let result_tx = result_tx.clone();
            let client = self.client.clone();
            #[cfg(feature = "browser")]
            let browser_config = browser_config.clone();
            let context_url = context_url.clone();

            let handle = tokio::spawn(async move {
                // Create browser fetcher for this worker if browser mode is enabled
                #[cfg(feature = "browser")]
                let mut browser_fetcher = browser_config
                    .as_ref()
                    .map(|cfg| BrowserFetcher::new(cfg.clone()));

                loop {
                    let url = {
                        let mut rx = url_rx.lock().await;
                        rx.recv().await
                    };

                    let url = match url {
                        Some(u) => u,
                        None => break, // Channel closed, exit worker
                    };

                    // Skip if already fetched
                    if client.is_fetched(&url).await {
                        continue;
                    }

                    // Mark as fetching
                    client.mark_fetching(&url).await;

                    // Fetch the document (using browser if configured)
                    #[cfg(feature = "browser")]
                    let fetch_result = if let Some(ref mut browser) = browser_fetcher {
                        // Use binary fetch for PDFs when configured (bypasses Akamai on PDF endpoints)
                        let is_pdf = url.to_lowercase().ends_with(".pdf");
                        if binary_fetch && is_pdf {
                            Self::fetch_url_with_browser_binary(
                                browser,
                                &url,
                                context_url.as_deref(),
                            )
                            .await
                        } else {
                            Self::fetch_url_with_browser(browser, &client, &url).await
                        }
                    } else {
                        Self::fetch_url(&client, &url).await
                    };

                    #[cfg(not(feature = "browser"))]
                    let fetch_result = Self::fetch_url(&client, &url).await;

                    match fetch_result {
                        Some(result) => {
                            // Mark as fetched
                            client
                                .mark_fetched(
                                    &url,
                                    None,
                                    None,
                                    result.etag.clone(),
                                    result.last_modified.clone(),
                                )
                                .await;
                            if result_tx.send(result).await.is_err() {
                                break; // Receiver dropped
                            }
                        }
                        None => {
                            // Mark as failed
                            client.mark_failed(&url, "fetch failed").await;
                        }
                    }
                }

                // Clean up browser if we have one
                #[cfg(feature = "browser")]
                if let Some(ref mut browser) = browser_fetcher {
                    browser.close().await;
                }
            });

            handles.push(handle);
        }

        handles
    }

    /// Spawn discovery task that feeds URLs to the download queue.
    async fn spawn_discovery_task(
        &self,
        url_tx: tokio::sync::mpsc::Sender<String>,
    ) -> tokio::task::JoinHandle<()> {
        let source_id = self.source.id.clone();
        let config = self.config.clone();
        let client = self.client.clone();
        let crawl_repo = self.crawl_repo.clone();
        let refresh_ttl_days = self.refresh_ttl_days;
        #[cfg(feature = "browser")]
        let browser_config = self.browser_config.clone();

        tokio::spawn(async move {
            // Phase 1: Process pending URLs from previous crawl
            if let Some(repo) = &crawl_repo {
                loop {
                    let pending = {
                        let repo = repo.lock().await;
                        repo.get_pending_urls(&source_id, 50).unwrap_or_default()
                    };

                    if pending.is_empty() {
                        break;
                    }

                    for crawl_url in pending {
                        if url_tx.send(crawl_url.url).await.is_err() {
                            return; // Receiver dropped
                        }
                    }
                }

                // Phase 2: Process retryable failed URLs
                let retryable = {
                    let repo = repo.lock().await;
                    repo.get_retryable_urls(&source_id, 50).unwrap_or_default()
                };

                for crawl_url in retryable {
                    if url_tx.send(crawl_url.url).await.is_err() {
                        return;
                    }
                }

                // Phase 3: Refresh stale URLs (older than TTL)
                let cutoff = chrono::Utc::now() - chrono::Duration::days(refresh_ttl_days as i64);
                loop {
                    let stale = {
                        let repo = repo.lock().await;
                        repo.get_urls_needing_refresh(&source_id, cutoff, 50)
                            .unwrap_or_default()
                    };

                    if stale.is_empty() {
                        break;
                    }

                    for crawl_url in stale {
                        // Mark as pending so it will be refetched with conditional GET
                        {
                            let repo = repo.lock().await;
                            let _ = repo.mark_url_for_refresh(&source_id, &crawl_url.url);
                        }
                        if url_tx.send(crawl_url.url).await.is_err() {
                            return;
                        }
                    }
                }
            }

            // Phase 4: Discover new URLs (streaming)
            #[cfg(feature = "browser")]
            Self::discover_streaming(
                &config,
                &client,
                &source_id,
                &crawl_repo,
                &url_tx,
                &browser_config,
            )
            .await;
            #[cfg(not(feature = "browser"))]
            Self::discover_streaming(&config, &client, &source_id, &crawl_repo, &url_tx).await;

            // url_tx is dropped here, signaling workers to finish
        })
    }

    /// Static fetch method for use in workers.
    async fn fetch_url(client: &HttpClient, url: &str) -> Option<ScraperResult> {
        debug!("Fetching: {}", url);

        // Get cached headers for conditional GET (refresh scenario)
        let (cached_etag, cached_last_modified) = client.get_cached_headers(url).await;

        let response = match client
            .get(url, cached_etag.as_deref(), cached_last_modified.as_deref())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!("Failed to fetch {}: {}", url, e);
                return None;
            }
        };

        if response.is_not_modified() {
            return Some(ScraperResult::not_modified(
                url.to_string(),
                response.etag().map(|s| s.to_string()),
                response.last_modified().map(|s| s.to_string()),
            ));
        }

        if !response.is_success() {
            debug!("HTTP {} for {}", response.status, url);
            return None;
        }

        // Extract headers before consuming response with bytes()
        let title = extract_title_from_url(url);
        let mime_type = response
            .content_type()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let etag = response.etag().map(|s| s.to_string());
        let last_modified = response.last_modified().map(|s| s.to_string());
        let original_filename = response.content_disposition_filename();

        // Parse Last-Modified into a DateTime
        let server_date = last_modified.as_ref().and_then(|lm| {
            chrono::DateTime::parse_from_rfc2822(lm)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        let content = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                debug!("Failed to read response for {}: {}", url, e);
                return None;
            }
        };

        let mut result = ScraperResult {
            url: url.to_string(),
            title,
            content: Some(content),
            mime_type,
            metadata: serde_json::json!({}),
            fetched_at: Utc::now(),
            etag,
            last_modified,
            not_modified: false,
            original_filename,
            server_date,
        };

        // Update metadata
        result.metadata = serde_json::json!({
            "fetched_at": result.fetched_at.to_rfc3339(),
        });

        Some(result)
    }

    /// Fetch URL using browser for anti-bot protected sites.
    #[cfg(feature = "browser")]
    async fn fetch_url_with_browser(
        browser: &mut BrowserFetcher,
        _client: &HttpClient,
        url: &str,
    ) -> Option<ScraperResult> {
        debug!("Fetching with browser: {}", url);

        let response = match browser.fetch(url).await {
            Ok(r) => r,
            Err(e) => {
                debug!("Browser fetch failed for {}: {}", url, e);
                return None;
            }
        };

        // For HTML pages fetched via browser, the content is the rendered HTML
        let title = extract_title_from_url(url);

        // Try to extract title from HTML content
        let title = if response.content.contains("<title>") {
            scraper::Html::parse_document(&response.content)
                .select(&scraper::Selector::parse("title").unwrap())
                .next()
                .map(|el| el.inner_html().trim().to_string())
                .unwrap_or(title)
        } else {
            title
        };

        let content = response.content.into_bytes();

        Some(ScraperResult {
            url: url.to_string(),
            title,
            content: Some(content),
            mime_type: response.content_type,
            metadata: serde_json::json!({
                "fetched_at": Utc::now().to_rfc3339(),
                "browser_fetch": true,
                "final_url": response.final_url,
            }),
            fetched_at: Utc::now(),
            etag: None,
            last_modified: None,
            not_modified: false,
            original_filename: None,
            server_date: None,
        })
    }

    /// Fetch binary URL (PDF, images) using JavaScript fetch from browser context.
    /// This bypasses Akamai/Cloudflare bot protection on PDF endpoints.
    #[cfg(feature = "browser")]
    async fn fetch_url_with_browser_binary(
        browser: &mut BrowserFetcher,
        url: &str,
        context_url: Option<&str>,
    ) -> Option<ScraperResult> {
        debug!("Fetching binary with browser: {}", url);

        let response = match browser.fetch_binary(url, context_url).await {
            Ok(r) => r,
            Err(e) => {
                debug!("Browser binary fetch failed for {}: {}", url, e);
                return None;
            }
        };

        // Extract filename from URL
        let filename = url.rsplit('/').next().map(|s| s.to_string());
        let title = filename
            .clone()
            .unwrap_or_else(|| extract_title_from_url(url));

        debug!(
            "Downloaded {} bytes from {} ({})",
            response.data.len(),
            url,
            response.content_type
        );

        Some(ScraperResult {
            url: url.to_string(),
            title,
            content: Some(response.data),
            mime_type: response.content_type,
            metadata: serde_json::json!({
                "fetched_at": Utc::now().to_rfc3339(),
                "browser_fetch": true,
                "binary_fetch": true,
                "size": response.size,
            }),
            fetched_at: Utc::now(),
            etag: None,
            last_modified: None,
            not_modified: false,
            original_filename: filename,
            server_date: None,
        })
    }

    /// Streaming discovery that sends URLs as they're found (with browser support).
    #[cfg(feature = "browser")]
    async fn discover_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
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
    async fn discover_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
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

    /// Streaming HTML crawl discovery with browser support.
    /// Performs recursive BFS crawling within the allowed domain.
    #[cfg(feature = "browser")]
    async fn discover_html_crawl_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
        browser_config: &Option<BrowserEngineConfig>,
    ) {
        use std::collections::{HashSet, VecDeque};

        let default_base = String::new();
        let base_url = config
            .discovery
            .base_url
            .as_ref()
            .or(config.base_url.as_ref())
            .unwrap_or(&default_base);

        // Parse base URL to get allowed root domain (for subdomain matching)
        // e.g., www.cia.gov -> cia.gov, so foia.cia.gov is also allowed
        let allowed_domain = base_url
            .parse::<Url>()
            .map(|u| {
                let host = u.host_str().unwrap_or("");
                // Extract root domain (last two parts, or last three for .co.uk etc.)
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() >= 2 {
                    // Simple heuristic: take last 2 parts (works for .gov, .com, .org)
                    parts[parts.len() - 2..].join(".")
                } else {
                    host.to_string()
                }
            })
            .unwrap_or_default();

        // Build document patterns - prefer direct config, fall back to levels
        let document_pattern_strs: Vec<String> = if !config.discovery.document_patterns.is_empty() {
            config.discovery.document_patterns.clone()
        } else {
            config
                .discovery
                .levels
                .last()
                .map(|l| l.document_patterns.clone())
                .unwrap_or_default()
        };
        let document_patterns: Vec<Regex> = document_pattern_strs
            .iter()
            .filter_map(|p| Regex::new(p).ok())
            .collect();

        // For BFS crawling, we always need to scan ALL links to find pages to visit.
        // document_links is used as a hint for where documents might be, but we crawl all links.
        let page_link_selector = "a".to_string();

        // document_links selectors help identify specific document links (optional, reserved for future use)
        let _document_link_selectors: Vec<String> = if !config.discovery.document_links.is_empty() {
            config.discovery.document_links.clone()
        } else {
            config
                .discovery
                .levels
                .last()
                .map(|l| l.link_selectors.clone())
                .unwrap_or_default()
        };

        // Use browser for discovery? Prefer direct config, fall back to levels
        let use_browser = config.discovery.use_browser
            || config
                .discovery
                .levels
                .first()
                .map(|l| l.use_browser)
                .unwrap_or(false);

        info!(
            "Crawler config: document_patterns={:?}, use_browser={}",
            document_pattern_strs, use_browser
        );

        // Create browser fetcher if configured
        let mut browser_fetcher = browser_config
            .as_ref()
            .map(|cfg| BrowserFetcher::new(cfg.clone()));

        // BFS frontier and visited set
        let mut frontier: VecDeque<(String, u32)> = VecDeque::new(); // (url, depth)
        let mut visited: HashSet<String> = HashSet::new();

        // Seed the frontier with start paths
        let start_paths = if config.discovery.start_paths.is_empty() {
            vec!["/".to_string()]
        } else {
            config.discovery.start_paths.clone()
        };

        for start_path in start_paths {
            let start_url = resolve_url(base_url, &start_path);
            if visited.insert(start_url.clone()) {
                frontier.push_back((start_url, 0));
            }
        }

        // Add search query URLs if configured
        if let Some(ref template) = config.discovery.search_url_template {
            for query in &config.discovery.search_queries {
                let encoded_query = urlencoding::encode(query);
                let search_path = template.replace("{query}", &encoded_query);
                let search_url = resolve_url(base_url, &search_path);
                if visited.insert(search_url.clone()) {
                    frontier.push_back((search_url, 0));
                }
            }
        }

        info!(
            "Starting recursive HTML crawl discovery with {} seed URLs",
            frontier.len()
        );

        let max_depth = config.discovery.max_depth.unwrap_or(10);
        let mut pages_crawled = 0u64;
        let mut docs_found = 0u64;
        let mut consecutive_browser_failures = 0u64;
        let mut total_browser_failures = 0u64;
        let initial_frontier_size = frontier.len();

        while let Some((current_url, depth)) = frontier.pop_front() {
            if depth > max_depth {
                continue;
            }

            // Track crawl URL
            let crawl_url = CrawlUrl::new(
                current_url.clone(),
                source_id.to_string(),
                if depth == 0 {
                    DiscoveryMethod::Seed
                } else {
                    DiscoveryMethod::HtmlLink
                },
                None,
                depth,
            );
            client.track_url(&crawl_url).await;

            // Fetch the page
            let html = if use_browser {
                if let Some(ref mut browser) = browser_fetcher {
                    match browser.fetch(&current_url).await {
                        Ok(resp) => {
                            consecutive_browser_failures = 0; // Reset on success
                            resp.content
                        }
                        Err(e) => {
                            consecutive_browser_failures += 1;
                            total_browser_failures += 1;

                            // Log as warning after a few failures, error if it looks like a connection issue
                            let err_str = e.to_string();
                            if err_str.contains("connect") || err_str.contains("Connection") {
                                warn!("Browser connection failed for {}: {} (failure #{} consecutive)",
                                      current_url, e, consecutive_browser_failures);
                            } else if consecutive_browser_failures > 3 {
                                warn!(
                                    "Browser fetch failed for {}: {} (failure #{} consecutive)",
                                    current_url, e, consecutive_browser_failures
                                );
                            } else {
                                debug!("Browser fetch failed for {}: {}", current_url, e);
                            }
                            continue;
                        }
                    }
                } else {
                    match client.get_text(&current_url).await {
                        Ok(html) => html,
                        Err(e) => {
                            debug!("Fetch failed for {}: {}", current_url, e);
                            continue;
                        }
                    }
                }
            } else {
                match client.get_text(&current_url).await {
                    Ok(html) => html,
                    Err(e) => {
                        debug!("Fetch failed for {}: {}", current_url, e);
                        continue;
                    }
                }
            };

            pages_crawled += 1;
            if pages_crawled.is_multiple_of(100) {
                info!(
                    "Crawled {} pages, found {} documents, {} in frontier",
                    pages_crawled,
                    docs_found,
                    frontier.len()
                );
            }

            // Parse and extract links (in a block to ensure document is dropped before async)
            let (doc_urls, page_urls) = {
                let document = Html::parse_document(&html);
                let mut doc_urls: Vec<String> = Vec::new();
                let mut page_urls: Vec<String> = Vec::new();

                // Use "a" selector to find ALL links on the page for BFS crawling
                let selector = match Selector::parse(&page_link_selector) {
                    Ok(s) => s,
                    Err(_) => {
                        warn!("Failed to parse link selector: {}", page_link_selector);
                        continue;
                    }
                };

                for element in document.select(&selector) {
                    let href = match element.value().attr("href") {
                        Some(h) => h,
                        None => continue,
                    };

                    // Skip empty, javascript, mailto, tel links
                    if href.is_empty()
                        || href.starts_with('#')
                        || href.starts_with("javascript:")
                        || href.starts_with("mailto:")
                        || href.starts_with("tel:")
                    {
                        continue;
                    }

                    // Resolve URL
                    let full_url = if href.starts_with("http://") || href.starts_with("https://") {
                        href.to_string()
                    } else if href.starts_with("/") {
                        if let Ok(parsed) = Url::parse(&current_url) {
                            format!(
                                "{}://{}{}",
                                parsed.scheme(),
                                parsed.host_str().unwrap_or(""),
                                href
                            )
                        } else {
                            format!("{}{}", base_url, href)
                        }
                    } else if href.starts_with("//") {
                        format!("https:{}", href)
                    } else {
                        // Relative URL
                        if let Ok(base) = Url::parse(&current_url) {
                            base.join(href).map(|u| u.to_string()).unwrap_or_default()
                        } else {
                            continue;
                        }
                    };

                    if full_url.is_empty() {
                        continue;
                    }

                    // Check if link should be followed:
                    // 1. Links to allowed domain (e.g., cia.gov) - always follow
                    // 2. Links to same host as current page - follow (for pagination on search engines)
                    let url_host = full_url
                        .parse::<Url>()
                        .map(|u| u.host_str().unwrap_or("").to_string())
                        .unwrap_or_default();
                    let current_host = current_url
                        .parse::<Url>()
                        .map(|u| u.host_str().unwrap_or("").to_string())
                        .unwrap_or_default();

                    let is_allowed_domain =
                        allowed_domain.is_empty() || url_host.ends_with(&allowed_domain);
                    let is_same_host = url_host == current_host;

                    if !is_allowed_domain && !is_same_host {
                        continue; // Skip external links
                    }

                    // Check if it's a document
                    let is_document = !document_patterns.is_empty()
                        && document_patterns.iter().any(|p| p.is_match(&full_url));

                    if is_document {
                        doc_urls.push(full_url);
                    } else {
                        // It's a page to crawl (if not a document pattern match and is HTML-ish)
                        let looks_like_page = !full_url.ends_with(".pdf")
                            && !full_url.ends_with(".jpg")
                            && !full_url.ends_with(".jpeg")
                            && !full_url.ends_with(".png")
                            && !full_url.ends_with(".gif")
                            && !full_url.ends_with(".zip")
                            && !full_url.ends_with(".doc")
                            && !full_url.ends_with(".docx")
                            && !full_url.ends_with(".xls");
                        if looks_like_page {
                            page_urls.push(full_url);
                        }
                    }
                }

                (doc_urls, page_urls)
            }; // document is dropped here

            // Send document URLs to download queue
            for full_url in doc_urls {
                if visited.insert(full_url.clone()) {
                    debug!("Found document: {}", full_url);
                    docs_found += 1;

                    // Track in crawl repo
                    if let Some(repo) = crawl_repo {
                        let crawl_url = CrawlUrl::new(
                            full_url.clone(),
                            source_id.to_string(),
                            DiscoveryMethod::HtmlLink,
                            Some(current_url.clone()),
                            depth + 1,
                        );
                        let repo = repo.lock().await;
                        let _ = repo.add_url(&crawl_url);
                    }

                    // Send to download queue
                    if url_tx.send(full_url).await.is_err() {
                        info!("Discovery complete: receiver dropped");
                        if let Some(ref mut browser) = browser_fetcher {
                            browser.close().await;
                        }
                        return;
                    }
                }
            }

            // Add page URLs to frontier
            for page_url in page_urls {
                if visited.insert(page_url.clone()) {
                    frontier.push_back((page_url, depth + 1));
                }
            }
        }

        // Report results with appropriate log level
        if pages_crawled == 0 && total_browser_failures > 0 {
            // All fetches failed - this is an error condition
            if total_browser_failures == initial_frontier_size as u64 {
                tracing::error!(
                    "Crawl failed: all {} URLs failed with browser errors. \
                     Check if the remote browser is running (ws://localhost:9222). \
                     Try: ./bin/stealth restart",
                    total_browser_failures
                );
            } else {
                tracing::error!(
                    "Crawl failed: {} pages crawled, {} documents found, {} browser failures",
                    pages_crawled,
                    docs_found,
                    total_browser_failures
                );
            }
        } else if total_browser_failures > 0 {
            warn!(
                "Crawl complete with errors: {} pages crawled, {} documents found, {} browser failures",
                pages_crawled, docs_found, total_browser_failures
            );
        } else {
            info!(
                "Recursive crawl complete: {} pages crawled, {} documents found",
                pages_crawled, docs_found
            );
        }

        // Clean up browser
        if let Some(ref mut browser) = browser_fetcher {
            browser.close().await;
        }
    }

    /// Streaming HTML crawl discovery without browser support.
    #[cfg(not(feature = "browser"))]
    async fn discover_html_crawl_streaming_no_browser(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
    ) {
        // Simplified version without browser support
        let default_base = String::new();
        let base_url = config
            .discovery
            .base_url
            .as_ref()
            .or(config.base_url.as_ref())
            .unwrap_or(&default_base);

        for start_path in &config.discovery.start_paths {
            let start_url = resolve_url(base_url, start_path);
            let html = match client.get_text(&start_url).await {
                Ok(html) => html,
                Err(_) => continue,
            };

            // Collect URLs synchronously (ElementRef is not Send)
            let found_urls = {
                let document = Html::parse_document(&html);
                let mut urls: Vec<String> = Vec::new();
                if let Some(level) = config.discovery.levels.last() {
                    for selector_str in &level.link_selectors {
                        let selector = match Selector::parse(selector_str) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };

                        for element in document.select(&selector) {
                            if let Some(href) = element.value().attr("href") {
                                let full_url = if href.starts_with("http") {
                                    href.to_string()
                                } else {
                                    format!("{}{}", base_url, href)
                                };
                                urls.push(full_url);
                            }
                        }
                    }
                }
                urls
            };

            // Now send URLs (after document is dropped)
            for full_url in found_urls {
                if url_tx.send(full_url).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Streaming API paginated discovery.
    async fn discover_api_paginated_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
    ) {
        let api = match &config.discovery.api {
            Some(api) => api,
            None => return,
        };

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        info!("Starting streaming API discovery from {}", api_url);

        let mut page = 1u32;
        let mut total_urls = 0;
        let mut rate_limited = false;
        let mut last_error: Option<String> = None;

        loop {
            let mut params: Vec<(String, String)> = Vec::new();
            params.push((api.pagination.page_param.clone(), page.to_string()));

            if let Some(ref size_param) = api.pagination.page_size_param {
                params.push((size_param.clone(), api.pagination.page_size.to_string()));
            }

            let url_with_params = format!(
                "{}?{}",
                api_url,
                params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&")
            );

            debug!("Fetching page {}: {}", page, url_with_params);

            let response = match client.get(&url_with_params, None, None).await {
                Ok(r) if r.is_success() => r,
                Ok(r) => {
                    let status = r.status.as_u16();
                    if status == 429 || status == 503 {
                        rate_limited = true;
                        last_error = Some(format!("Rate limited (HTTP {})", status));
                        tracing::error!(
                            "[{}] Rate limited (HTTP {}) on page {} - {}",
                            source_id,
                            status,
                            page,
                            url_with_params
                        );
                    } else {
                        last_error = Some(format!("HTTP {}", status));
                        warn!(
                            "[{}] API request failed (HTTP {}) - {}",
                            source_id, r.status, url_with_params
                        );
                    }
                    break;
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    warn!(
                        "[{}] API request error: {} - {}",
                        source_id, e, url_with_params
                    );
                    break;
                }
            };

            let data: serde_json::Value = match response.text().await {
                Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                Err(_) => break,
            };

            let results = extract_path(&data, &api.pagination.results_path);
            let results = match results.as_array() {
                Some(arr) => arr,
                None => {
                    warn!(
                        "No results array found at path '{}'",
                        api.pagination.results_path
                    );
                    break;
                }
            };

            if results.is_empty() {
                info!("No more results on page {}", page);
                break;
            }

            let mut page_urls = 0;
            for item in results {
                for url in extract_urls(item, &api.url_extraction) {
                    // Track URL in database
                    if let Some(repo) = crawl_repo {
                        let crawl_url = CrawlUrl::new(
                            url.clone(),
                            source_id.to_string(),
                            DiscoveryMethod::ApiResult,
                            Some(api_url.clone()),
                            1,
                        );
                        let repo = repo.lock().await;
                        let _ = repo.add_url(&crawl_url);
                    }

                    // Send URL to download queue
                    if url_tx.send(url).await.is_err() {
                        return; // Receiver dropped
                    }
                    page_urls += 1;
                    total_urls += 1;
                }
            }

            info!(
                "Page {}: found {} items, extracted {} URLs (total: {})",
                page,
                results.len(),
                page_urls,
                total_urls
            );

            if results.len() < api.pagination.page_size as usize {
                break;
            }

            page += 1;
        }

        // Report results with appropriate log level
        if rate_limited {
            tracing::error!(
                "[{}] Discovery stopped by rate limiting after {} URLs on {} pages. \
                 Wait and retry, or reduce request rate.",
                source_id,
                total_urls,
                page
            );
        } else if let Some(err) = last_error {
            tracing::error!(
                "[{}] Discovery failed after {} URLs: {}",
                source_id,
                total_urls,
                err
            );
        } else {
            info!(
                "[{}] Discovery complete: {} URLs found",
                source_id, total_urls
            );
        }
    }

    /// Streaming API cursor discovery.
    async fn discover_api_cursor_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
    ) {
        let api = match &config.discovery.api {
            Some(api) => api,
            None => return,
        };

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        let queries = if api.queries.is_empty() {
            vec![String::new()]
        } else {
            api.queries.clone()
        };

        let cursor_param = api.pagination.cursor_param.as_deref().unwrap_or("cursor");
        let cursor_path = api
            .pagination
            .cursor_response_path
            .as_deref()
            .unwrap_or("next_cursor");

        let mut total_urls = 0;
        let mut rate_limited = false;
        let mut last_error: Option<String> = None;

        for query in queries {
            let mut cursor: Option<String> = None;

            loop {
                let mut url = api_url.clone();
                let mut params = Vec::new();

                if !query.is_empty() {
                    if let Some(ref param) = api.query_param {
                        params.push(format!("{}={}", param, urlencoding::encode(&query)));
                    }
                }

                if let Some(ref c) = cursor {
                    params.push(format!("{}={}", cursor_param, urlencoding::encode(c)));
                }

                if !params.is_empty() {
                    url = format!("{}?{}", url, params.join("&"));
                }

                let response = match client.get(&url, None, None).await {
                    Ok(r) if r.is_success() => r,
                    Ok(r) => {
                        let status = r.status.as_u16();
                        if status == 429 || status == 503 {
                            rate_limited = true;
                            last_error = Some(format!("Rate limited (HTTP {})", status));
                            tracing::error!(
                                "[{}] Rate limited (HTTP {}) - {}",
                                source_id,
                                status,
                                url
                            );
                        } else {
                            last_error = Some(format!("HTTP {}", status));
                            warn!(
                                "[{}] API request failed (HTTP {}) - {}",
                                source_id, r.status, url
                            );
                        }
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e.to_string());
                        warn!("[{}] API request error: {} - {}", source_id, e, url);
                        break;
                    }
                };

                let data: serde_json::Value = match response.text().await {
                    Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                    Err(_) => break,
                };

                let results = extract_path(&data, &api.pagination.results_path);
                let results = match results.as_array() {
                    Some(arr) => arr,
                    None => break,
                };

                if results.is_empty() {
                    break;
                }

                for item in results {
                    for doc_url in extract_urls(item, &api.url_extraction) {
                        if let Some(repo) = crawl_repo {
                            let crawl_url = CrawlUrl::new(
                                doc_url.clone(),
                                source_id.to_string(),
                                DiscoveryMethod::ApiResult,
                                Some(url.clone()),
                                1,
                            );
                            let repo = repo.lock().await;
                            let _ = repo.add_url(&crawl_url);
                        }

                        if url_tx.send(doc_url).await.is_err() {
                            return;
                        }
                        total_urls += 1;
                    }
                }

                cursor = extract_path(&data, cursor_path)
                    .as_str()
                    .map(|s| s.to_string());

                if cursor.is_none() {
                    break;
                }
            }

            // If rate limited, don't continue to next query
            if rate_limited {
                break;
            }
        }

        // Report results with appropriate log level
        if rate_limited {
            tracing::error!(
                "[{}] Discovery stopped by rate limiting after {} URLs. \
                 Wait and retry, or reduce request rate.",
                source_id,
                total_urls
            );
        } else if let Some(err) = last_error {
            tracing::error!(
                "[{}] Discovery failed after {} URLs: {}",
                source_id,
                total_urls,
                err
            );
        } else {
            info!(
                "[{}] Cursor discovery complete: {} URLs found",
                source_id, total_urls
            );
        }
    }

    /// Discover document URLs.
    pub async fn discover(&self) -> Vec<String> {
        match self.config.discovery.discovery_type.as_str() {
            "html_crawl" => self.discover_html_crawl().await,
            "api_paginated" => self.discover_api_paginated().await,
            "api_cursor" => self.discover_api_cursor().await,
            "api_nested" => self.discover_api_nested().await,
            _ => Vec::new(),
        }
    }

    async fn discover_html_crawl(&self) -> Vec<String> {
        let mut urls = Vec::new();
        let default_base = String::new();
        let base_url = self
            .config
            .discovery
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);

        let start_paths = if self.config.discovery.start_paths.is_empty() {
            vec!["/".to_string()]
        } else {
            self.config.discovery.start_paths.clone()
        };

        for start_path in start_paths {
            let start_url = resolve_url(base_url, &start_path);

            // Track seed URL
            let crawl_url = CrawlUrl::new(
                start_url.clone(),
                self.source.id.clone(),
                DiscoveryMethod::Seed,
                None,
                0,
            );
            self.client.track_url(&crawl_url).await;

            urls.extend(self.crawl_level(&start_url, base_url, 0, None).await);
        }

        urls
    }

    fn crawl_level<'a>(
        &'a self,
        url: &'a str,
        base_url: &'a str,
        level_idx: usize,
        _parent_url: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<String>> + 'a>> {
        Box::pin(async move {
            let mut urls = Vec::new();

            let levels = &self.config.discovery.levels;
            if level_idx >= levels.len() {
                return urls;
            }

            let level = &levels[level_idx];
            let is_final_level = level_idx == levels.len() - 1;

            // Fetch the page
            let html = match self.client.get_text(url).await {
                Ok(html) => html,
                Err(_) => return urls,
            };

            let document = Html::parse_document(&html);

            // Extract links
            let link_selectors = if level.link_selectors.is_empty() {
                vec!["a".to_string()]
            } else {
                level.link_selectors.clone()
            };

            let link_pattern = level.link_pattern.as_ref().and_then(|p| Regex::new(p).ok());

            let document_patterns: Vec<Regex> = level
                .document_patterns
                .iter()
                .filter_map(|p| Regex::new(p).ok())
                .collect();

            // Collect links first to avoid borrowing issues
            let mut links_to_process: Vec<(String, bool)> = Vec::new();

            for selector_str in &link_selectors {
                let selector = match Selector::parse(selector_str) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                for element in document.select(&selector) {
                    let href = match element.value().attr("href") {
                        Some(h) => h,
                        None => continue,
                    };

                    // Apply link pattern filter
                    if let Some(ref pattern) = link_pattern {
                        if !pattern.is_match(href) {
                            continue;
                        }
                    }

                    // Resolve URL
                    let full_url = match Url::parse(base_url).and_then(|base| base.join(href)) {
                        Ok(u) => u.to_string(),
                        Err(_) => continue,
                    };

                    // Check if URL matches document patterns
                    let matches_doc = document_patterns.is_empty()
                        || document_patterns.iter().any(|p| p.is_match(href));

                    links_to_process.push((full_url, matches_doc));
                }
            }

            // Process links
            for (full_url, matches_doc) in links_to_process {
                // Track discovered URL
                let crawl_url = CrawlUrl::new(
                    full_url.clone(),
                    self.source.id.clone(),
                    DiscoveryMethod::HtmlLink,
                    Some(url.to_string()),
                    (level_idx + 1) as u32,
                );
                self.client.track_url(&crawl_url).await;

                if is_final_level {
                    if matches_doc {
                        urls.push(full_url);
                    }
                } else {
                    // Recurse to next level
                    urls.extend(
                        self.crawl_level(&full_url, base_url, level_idx + 1, Some(url))
                            .await,
                    );
                }
            }

            // Handle pagination
            if let Some(ref pagination) = level.pagination {
                if let Some(next_url) = self.find_next_page(&document, base_url, pagination) {
                    // Track pagination URL
                    let crawl_url = CrawlUrl::new(
                        next_url.clone(),
                        self.source.id.clone(),
                        DiscoveryMethod::Pagination,
                        Some(url.to_string()),
                        level_idx as u32,
                    );
                    self.client.track_url(&crawl_url).await;

                    urls.extend(
                        self.crawl_level(&next_url, base_url, level_idx, Some(url))
                            .await,
                    );
                }
            }

            urls
        })
    }

    fn find_next_page(
        &self,
        document: &Html,
        base_url: &str,
        pagination: &PaginationConfig,
    ) -> Option<String> {
        let selectors = if pagination.next_selectors.is_empty() {
            vec!["a[rel='next']".to_string(), ".pager-next a".to_string()]
        } else {
            pagination.next_selectors.clone()
        };

        for selector_str in selectors {
            if let Ok(selector) = Selector::parse(&selector_str) {
                if let Some(element) = document.select(&selector).next() {
                    if let Some(href) = element.value().attr("href") {
                        if let Ok(base) = Url::parse(base_url) {
                            if let Ok(url) = base.join(href) {
                                return Some(url.to_string());
                            }
                        }
                    }
                }
            }
        }

        None
    }

    async fn discover_api_paginated(&self) -> Vec<String> {
        let mut urls = Vec::new();

        let api = match &self.config.discovery.api {
            Some(api) => api,
            None => return urls,
        };

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        info!("Starting API paginated discovery from {}", api_url);

        let mut page = 1u32;
        loop {
            let mut params: Vec<(String, String)> = Vec::new();
            params.push((api.pagination.page_param.clone(), page.to_string()));

            if let Some(ref size_param) = api.pagination.page_size_param {
                params.push((size_param.clone(), api.pagination.page_size.to_string()));
            }

            let url_with_params = format!(
                "{}?{}",
                api_url,
                params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&")
            );

            debug!("Fetching page {}: {}", page, url_with_params);

            let response = match self.client.get(&url_with_params, None, None).await {
                Ok(r) if r.is_success() => r,
                Ok(r) => {
                    warn!("API request failed with status {}", r.status);
                    break;
                }
                Err(e) => {
                    warn!("API request error: {}", e);
                    break;
                }
            };

            let data: serde_json::Value = match response.text().await {
                Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                Err(_) => break,
            };

            let results = extract_path(&data, &api.pagination.results_path);
            let results = match results.as_array() {
                Some(arr) => arr,
                None => {
                    warn!(
                        "No results array found at path '{}'",
                        api.pagination.results_path
                    );
                    break;
                }
            };

            if results.is_empty() {
                info!("No more results on page {}", page);
                break;
            }

            let mut page_urls = 0;
            for item in results {
                for url in extract_urls(item, &api.url_extraction) {
                    let crawl_url = CrawlUrl::new(
                        url.clone(),
                        self.source.id.clone(),
                        DiscoveryMethod::ApiResult,
                        Some(api_url.clone()),
                        1,
                    );
                    self.client.track_url(&crawl_url).await;
                    urls.push(url);
                    page_urls += 1;
                }
            }

            info!(
                "Page {}: found {} items, extracted {} URLs (total: {})",
                page,
                results.len(),
                page_urls,
                urls.len()
            );

            if results.len() < api.pagination.page_size as usize {
                break;
            }

            page += 1;
        }

        urls
    }

    async fn discover_api_cursor(&self) -> Vec<String> {
        let mut urls = Vec::new();

        let api = match &self.config.discovery.api {
            Some(api) => api,
            None => return urls,
        };

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        let queries = if api.queries.is_empty() {
            vec![String::new()]
        } else {
            api.queries.clone()
        };

        let cursor_param = api.pagination.cursor_param.as_deref().unwrap_or("cursor");
        let cursor_path = api
            .pagination
            .cursor_response_path
            .as_deref()
            .unwrap_or("next_cursor");

        for query in queries {
            let mut cursor: Option<String> = None;

            loop {
                let mut params: Vec<(String, String)> = Vec::new();
                if !query.is_empty() {
                    let query_param = api.query_param.as_deref().unwrap_or("q");
                    params.push((query_param.to_string(), query.clone()));
                }
                if let Some(ref c) = cursor {
                    params.push((cursor_param.to_string(), c.clone()));
                }

                let url_with_params = if params.is_empty() {
                    api_url.clone()
                } else {
                    format!(
                        "{}?{}",
                        api_url,
                        params
                            .iter()
                            .map(|(k, v)| format!("{}={}", k, v))
                            .collect::<Vec<_>>()
                            .join("&")
                    )
                };

                let response = match self.client.get(&url_with_params, None, None).await {
                    Ok(r) if r.is_success() => r,
                    _ => break,
                };

                let data: serde_json::Value = match response.text().await {
                    Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                    Err(_) => break,
                };

                let results = extract_path(&data, &api.pagination.results_path);
                let results = match results.as_array() {
                    Some(arr) => arr,
                    None => break,
                };

                if results.is_empty() {
                    break;
                }

                for item in results {
                    if let Some(url) = extract_url(item, &api.url_extraction) {
                        let crawl_url = CrawlUrl::new(
                            url.clone(),
                            self.source.id.clone(),
                            DiscoveryMethod::ApiResult,
                            Some(api_url.clone()),
                            1,
                        );
                        self.client.track_url(&crawl_url).await;
                        urls.push(url);
                    }
                }

                cursor = extract_path(&data, cursor_path)
                    .as_str()
                    .map(|s| s.to_string());

                if cursor.is_none() {
                    break;
                }
            }
        }

        urls
    }

    async fn discover_api_nested(&self) -> Vec<String> {
        let mut urls = Vec::new();

        let api = match &self.config.discovery.api {
            Some(api) => api,
            None => return urls,
        };

        let parent = match &api.parent {
            Some(p) => p,
            None => return urls,
        };

        let child = match &api.child {
            Some(c) => c,
            None => return urls,
        };

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);
        let parent_url = format!("{}{}", base_url, parent.endpoint);

        let mut page = 1u32;
        loop {
            let url_with_params =
                format!("{}?{}={}", parent_url, parent.pagination.page_param, page);

            let response = match self.client.get(&url_with_params, None, None).await {
                Ok(r) if r.is_success() => r,
                _ => break,
            };

            let data: serde_json::Value = match response.text().await {
                Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                Err(_) => break,
            };

            let results = extract_path(&data, &parent.results_path);
            let results = match results.as_array() {
                Some(arr) => arr,
                None => break,
            };

            if results.is_empty() {
                break;
            }

            for item in results {
                let parent_id = extract_path(item, &parent.id_path);
                let parent_id = match parent_id
                    .as_str()
                    .or_else(|| parent_id.as_i64().map(|_| ""))
                {
                    Some(_) => parent_id.to_string().trim_matches('"').to_string(),
                    None => continue,
                };

                // Fetch child URLs
                let child_endpoint = child.endpoint_template.replace("{id}", &parent_id);
                let child_url = format!("{}{}", base_url, child_endpoint);

                let response = match self.client.get(&child_url, None, None).await {
                    Ok(r) if r.is_success() => r,
                    _ => continue,
                };

                let child_data: serde_json::Value = match response.text().await {
                    Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
                    Err(_) => continue,
                };

                let child_results = extract_path(&child_data, &child.results_path);
                let mut items: Vec<&serde_json::Value> = match child_results.as_array() {
                    Some(arr) => arr.iter().collect(),
                    None => continue,
                };

                // Handle nested items path
                if let Some(ref items_path) = child.url_extraction.items_path {
                    let mut nested_items = Vec::new();
                    for item in items {
                        let nested = extract_path(item, items_path);
                        if let Some(arr) = nested.as_array() {
                            nested_items.extend(arr.iter());
                        }
                    }
                    items = nested_items;
                }

                for item in items {
                    if let Some(url) = extract_url(item, &child.url_extraction) {
                        let crawl_url = CrawlUrl::new(
                            url.clone(),
                            self.source.id.clone(),
                            DiscoveryMethod::ApiNested,
                            Some(child_url.clone()),
                            2,
                        );
                        self.client.track_url(&crawl_url).await;
                        urls.push(url);
                    }
                }
            }

            if results.len() < parent.pagination.page_size as usize {
                break;
            }

            page += 1;
        }

        urls
    }

    /// Fetch a document with cached headers.
    async fn fetch_with_cache(&self, crawl_url: &CrawlUrl) -> Option<ScraperResult> {
        self.fetch_internal(
            &crawl_url.url,
            crawl_url.etag.as_deref(),
            crawl_url.last_modified.as_deref(),
        )
        .await
    }

    /// Fetch a document.
    pub async fn fetch(&self, url: &str) -> Option<ScraperResult> {
        let (etag, last_modified) = self.client.get_cached_headers(url).await;
        self.fetch_internal(url, etag.as_deref(), last_modified.as_deref())
            .await
    }

    async fn fetch_internal(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Option<ScraperResult> {
        self.client.mark_fetching(url).await;

        let response = match self.client.get(url, etag, last_modified).await {
            Ok(r) => r,
            Err(e) => {
                self.client.mark_failed(url, &e.to_string()).await;
                return None;
            }
        };

        // Handle 304 Not Modified
        if response.is_not_modified() {
            self.client.mark_skipped(url, "304 Not Modified").await;
            return Some(ScraperResult::not_modified(
                url.to_string(),
                etag.map(|s| s.to_string()),
                last_modified.map(|s| s.to_string()),
            ));
        }

        if !response.is_success() {
            self.client
                .mark_failed(url, &format!("HTTP {}", response.status))
                .await;
            return None;
        }

        let resp_etag = response.etag().map(|s| s.to_string());
        let resp_last_modified = response.last_modified().map(|s| s.to_string());
        let content_type = response
            .content_type()
            .map(|s| {
                s.split(';')
                    .next()
                    .unwrap_or("application/pdf")
                    .trim()
                    .to_string()
            })
            .unwrap_or_else(|| "application/pdf".to_string());
        let original_filename = response.content_disposition_filename();

        // Parse Last-Modified into a DateTime
        let server_date = resp_last_modified.as_ref().and_then(|lm| {
            chrono::DateTime::parse_from_rfc2822(lm)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        });

        let content = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                self.client.mark_failed(url, &e.to_string()).await;
                return None;
            }
        };

        let content_hash = crate::models::DocumentVersion::compute_hash(&content);

        self.client
            .mark_fetched(
                url,
                Some(content_hash),
                None,
                resp_etag.clone(),
                resp_last_modified.clone(),
            )
            .await;

        Some(ScraperResult {
            url: url.to_string(),
            title: extract_title_from_url(url),
            content: Some(content),
            mime_type: content_type,
            metadata: serde_json::json!({"source": self.source.name}),
            fetched_at: chrono::Utc::now(),
            etag: resp_etag,
            last_modified: resp_last_modified,
            not_modified: false,
            original_filename,
            server_date,
        })
    }
}

/// Extract a value from nested JSON using dot-notation path.
fn extract_path<'a>(data: &'a serde_json::Value, path: &str) -> &'a serde_json::Value {
    if path.is_empty() {
        return data;
    }

    let mut current = data;
    for key in path.split('.') {
        current = match current {
            serde_json::Value::Object(map) => map.get(key).unwrap_or(&serde_json::Value::Null),
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    arr.get(idx).unwrap_or(&serde_json::Value::Null)
                } else {
                    &serde_json::Value::Null
                }
            }
            _ => &serde_json::Value::Null,
        };
    }

    current
}

/// Extract URLs from an item using configured extraction rules.
/// Returns multiple URLs when nested_arrays is configured.
fn extract_urls(item: &serde_json::Value, extraction: &UrlExtractionConfig) -> Vec<String> {
    let mut urls = Vec::new();

    // If nested_arrays is configured, traverse the nested structure
    if !extraction.nested_arrays.is_empty() {
        extract_urls_nested(item, &extraction.nested_arrays, extraction, &mut urls);
        return urls;
    }

    // Simple extraction - single URL
    if let Some(url) = extract_single_url(item, extraction) {
        urls.push(url);
    }

    urls
}

/// Recursively extract URLs from nested arrays.
fn extract_urls_nested(
    item: &serde_json::Value,
    remaining_paths: &[String],
    extraction: &UrlExtractionConfig,
    urls: &mut Vec<String>,
) {
    if remaining_paths.is_empty() {
        // At the leaf - extract the URL
        if let Some(url) = extract_single_url(item, extraction) {
            urls.push(url);
        }
        return;
    }

    let current_path = &remaining_paths[0];
    let rest = &remaining_paths[1..];

    // Get the array at current path
    if let Some(arr) = item.get(current_path).and_then(|v| v.as_array()) {
        for nested_item in arr {
            extract_urls_nested(nested_item, rest, extraction, urls);
        }
    }
}

/// Extract a single URL from an item.
fn extract_single_url(
    item: &serde_json::Value,
    extraction: &UrlExtractionConfig,
) -> Option<String> {
    if let Some(s) = item.as_str() {
        return Some(s.to_string());
    }

    if let Some(obj) = item.as_object() {
        // Direct field extraction
        if let Some(url) = obj.get(&extraction.url_field).and_then(|v| v.as_str()) {
            return Some(url.to_string());
        }

        // Template-based URL construction
        if let Some(ref template) = extraction.url_template {
            let mut url = template.clone();
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    url = url.replace(&format!("{{{}}}", key), s);
                } else if let Some(n) = value.as_i64() {
                    url = url.replace(&format!("{{{}}}", key), &n.to_string());
                }
            }
            if !url.contains('{') {
                return Some(url);
            }
        }

        // Fallback field
        if let Some(ref fallback) = extraction.fallback_field {
            if let Some(url) = obj.get(fallback).and_then(|v| v.as_str()) {
                return Some(url.to_string());
            }
        }
    }

    None
}

/// Legacy single URL extraction for backward compatibility.
fn extract_url(item: &serde_json::Value, extraction: &UrlExtractionConfig) -> Option<String> {
    extract_urls(item, extraction).into_iter().next()
}
