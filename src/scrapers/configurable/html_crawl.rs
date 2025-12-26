//! HTML-based discovery methods (BFS crawl).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use regex::Regex;
use scraper::{Html, Selector};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

use super::super::browser::BrowserEngineConfig;
#[cfg(feature = "browser")]
use super::super::browser::BrowserFetcher;
use super::super::config::{PaginationConfig, ScraperConfig};
use super::super::google_drive::{
    extract_file_id, file_download_url, is_google_drive_file_url, is_google_drive_folder_url,
    DriveFolder,
};
use super::super::HttpClient;
use super::extract::resolve_url;
use super::ConfigurableScraper;
use crate::models::{CrawlUrl, DiscoveryMethod};
use crate::repository::CrawlRepository;

/// Configuration for the BFS HTML crawler, parsed from ScraperConfig.
struct CrawlerConfig {
    base_url: String,
    allowed_domain: String,
    document_patterns: Vec<Regex>,
    use_browser: bool,
    max_depth: u32,
}

impl CrawlerConfig {
    /// Build crawler configuration from ScraperConfig.
    fn from_scraper_config(config: &ScraperConfig) -> Self {
        let default_base = String::new();
        let base_url = config
            .discovery
            .base_url
            .as_ref()
            .or(config.base_url.as_ref())
            .unwrap_or(&default_base)
            .clone();

        // Parse base URL to get allowed root domain (for subdomain matching)
        let allowed_domain = base_url
            .parse::<Url>()
            .map(|u| {
                let host = u.host_str().unwrap_or("");
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() >= 2 {
                    parts[parts.len() - 2..].join(".")
                } else {
                    host.to_string()
                }
            })
            .unwrap_or_default();

        // Build document patterns
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

        // Use browser for discovery?
        let use_browser = config.discovery.use_browser
            || config
                .discovery
                .levels
                .first()
                .map(|l| l.use_browser)
                .unwrap_or(false);

        let max_depth = config.discovery.max_depth.unwrap_or(10);

        info!(
            "Crawler config: document_patterns={:?}, use_browser={}",
            document_pattern_strs, use_browser
        );

        Self {
            base_url,
            allowed_domain,
            document_patterns,
            use_browser,
            max_depth,
        }
    }
}

/// Initialize the BFS frontier with seed URLs.
fn seed_frontier(
    config: &ScraperConfig,
    base_url: &str,
    visited: &mut HashSet<String>,
) -> VecDeque<(String, u32)> {
    let mut frontier: VecDeque<(String, u32)> = VecDeque::new();

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

    frontier
}

/// Fetch a page using browser or HTTP client.
#[cfg(feature = "browser")]
async fn fetch_page_html(
    url: &str,
    use_browser: bool,
    browser_fetcher: &mut Option<BrowserFetcher>,
    client: &HttpClient,
    failure_stats: &mut (u64, u64), // (consecutive, total)
) -> Option<String> {
    if use_browser {
        if let Some(ref mut browser) = browser_fetcher {
            match browser.fetch(url).await {
                Ok(resp) => {
                    failure_stats.0 = 0; // Reset consecutive failures
                    return Some(resp.content);
                }
                Err(e) => {
                    failure_stats.0 += 1;
                    failure_stats.1 += 1;
                    warn!(
                        "Browser fetch failed for {}: {} (failure #{}/{})",
                        url, e, failure_stats.0, failure_stats.1
                    );
                    return None;
                }
            }
        }
    }
    // Fall back to HTTP client
    match client.get_text(url).await {
        Ok(html) => Some(html),
        Err(e) => {
            debug!("Fetch failed for {}: {}", url, e);
            None
        }
    }
}

/// Process Google Drive folder URLs and return direct file download URLs.
async fn enumerate_google_drive_folder(folder_url: &str, client: &HttpClient) -> Vec<String> {
    info!("Detected Google Drive folder: {}", folder_url);
    match DriveFolder::from_url(folder_url, client.clone()) {
        Ok(folder) => match folder.list_files_recursive().await {
            Ok(files) => {
                info!("Enumerated {} files from Google Drive folder", files.len());
                files
                    .into_iter()
                    .filter(|f| f.is_downloadable())
                    .map(|f| f.download_url)
                    .collect()
            }
            Err(e) => {
                warn!("Failed to enumerate Google Drive folder: {}", e);
                Vec::new()
            }
        },
        Err(e) => {
            warn!("Invalid Google Drive folder URL {}: {}", folder_url, e);
            Vec::new()
        }
    }
}

/// Convert Google Drive file URLs to proper download URLs.
fn convert_google_drive_file_url(url: String) -> String {
    if is_google_drive_file_url(&url) {
        if let Some(file_id) = extract_file_id(&url) {
            return file_download_url(&file_id);
        }
    }
    url
}

/// Send discovered document URLs to the channel and crawl repository.
async fn send_document_url(
    url: String,
    source_id: &str,
    parent_url: &str,
    depth: u32,
    discovery_method: DiscoveryMethod,
    crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
    url_tx: &tokio::sync::mpsc::Sender<String>,
    visited: &mut HashSet<String>,
) -> Result<(), ()> {
    if !visited.insert(url.clone()) {
        return Ok(());
    }

    if let Some(repo) = crawl_repo {
        let crawl_url = CrawlUrl::new(
            url.clone(),
            source_id.to_string(),
            discovery_method,
            Some(parent_url.to_string()),
            depth + 1,
        );
        let repo = repo.lock().await;
        let _ = repo.add_url(&crawl_url);
    }

    if url_tx.send(url).await.is_err() {
        return Err(());
    }
    Ok(())
}

/// Process Google Drive folder URLs, returning (gdrive_doc_urls, filtered_page_urls).
async fn process_google_drive_folders(
    page_urls: Vec<String>,
    client: &HttpClient,
) -> (Vec<String>, Vec<String>) {
    let mut gdrive_doc_urls: Vec<String> = Vec::new();
    let mut filtered_page_urls: Vec<String> = Vec::new();

    for url in page_urls {
        if is_google_drive_folder_url(&url) {
            let files = enumerate_google_drive_folder(&url, client).await;
            gdrive_doc_urls.extend(files);
        } else {
            filtered_page_urls.push(url);
        }
    }

    (gdrive_doc_urls, filtered_page_urls)
}

/// Close browser fetcher if present.
#[cfg(feature = "browser")]
async fn close_browser(browser_fetcher: &mut Option<BrowserFetcher>) {
    if let Some(ref mut browser) = browser_fetcher {
        browser.close().await;
    }
}

/// Report crawl results.
fn report_crawl_results(
    pages_crawled: u64,
    docs_found: u64,
    total_browser_failures: u64,
    initial_frontier_size: usize,
) {
    if pages_crawled == 0 && total_browser_failures > 0 {
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
}

impl ConfigurableScraper {
    /// Streaming HTML crawl discovery with browser support.
    /// Performs recursive BFS crawling within the allowed domain.
    #[cfg(feature = "browser")]
    pub(crate) async fn discover_html_crawl_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
        browser_config: &Option<BrowserEngineConfig>,
    ) {
        let crawler_config = CrawlerConfig::from_scraper_config(config);
        let page_link_selector = "a".to_string();

        // Create browser fetcher if configured
        let mut browser_fetcher = browser_config
            .as_ref()
            .map(|cfg| BrowserFetcher::new(cfg.clone()));

        // BFS frontier and visited set
        let mut visited: HashSet<String> = HashSet::new();
        let mut frontier = seed_frontier(config, &crawler_config.base_url, &mut visited);

        info!(
            "Starting recursive HTML crawl discovery with {} seed URLs",
            frontier.len()
        );

        let mut pages_crawled = 0u64;
        let mut docs_found = 0u64;
        let mut failure_stats = (0u64, 0u64); // (consecutive, total)
        let initial_frontier_size = frontier.len();

        while let Some((current_url, depth)) = frontier.pop_front() {
            if depth > crawler_config.max_depth {
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
            let html = match fetch_page_html(
                &current_url,
                crawler_config.use_browser,
                &mut browser_fetcher,
                client,
                &mut failure_stats,
            )
            .await
            {
                Some(html) => html,
                None => continue,
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

            // Parse and extract links
            let (doc_urls, page_urls) = extract_links_from_html(
                &html,
                &current_url,
                &crawler_config.base_url,
                &crawler_config.allowed_domain,
                &crawler_config.document_patterns,
                &page_link_selector,
            );

            // Process Google Drive folders and filter them from page URLs
            let (gdrive_doc_urls, page_urls) =
                process_google_drive_folders(page_urls, client).await;

            // Convert Google Drive file URLs to proper download URLs
            let doc_urls: Vec<String> = doc_urls
                .into_iter()
                .map(convert_google_drive_file_url)
                .collect();

            // Send document URLs to download queue
            for full_url in doc_urls {
                debug!("Found document: {}", full_url);
                if send_document_url(
                    full_url,
                    source_id,
                    &current_url,
                    depth,
                    DiscoveryMethod::HtmlLink,
                    crawl_repo,
                    url_tx,
                    &mut visited,
                )
                .await
                .is_err()
                {
                    info!("Discovery complete: receiver dropped");
                    close_browser(&mut browser_fetcher).await;
                    return;
                }
                docs_found += 1;
            }

            // Send Google Drive files to download queue
            for full_url in gdrive_doc_urls {
                debug!("Found Google Drive document: {}", full_url);
                if send_document_url(
                    full_url,
                    source_id,
                    &current_url,
                    depth,
                    DiscoveryMethod::GoogleDriveFolder,
                    crawl_repo,
                    url_tx,
                    &mut visited,
                )
                .await
                .is_err()
                {
                    info!("Discovery complete: receiver dropped");
                    close_browser(&mut browser_fetcher).await;
                    return;
                }
                docs_found += 1;
            }

            // Add page URLs to frontier
            for page_url in page_urls {
                if visited.insert(page_url.clone()) {
                    frontier.push_back((page_url, depth + 1));
                }
            }
        }

        report_crawl_results(
            pages_crawled,
            docs_found,
            failure_stats.1,
            initial_frontier_size,
        );
        close_browser(&mut browser_fetcher).await;
    }

    /// Streaming HTML crawl discovery without browser support.
    #[cfg(not(feature = "browser"))]
    pub(crate) async fn discover_html_crawl_streaming_no_browser(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        _crawl_repo: &Option<Arc<Mutex<CrawlRepository>>>,
        url_tx: &tokio::sync::mpsc::Sender<String>,
    ) {
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

            for full_url in found_urls {
                if url_tx.send(full_url).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Legacy HTML crawl discovery (non-streaming).
    pub(crate) async fn discover_html_crawl(&self) -> Vec<String> {
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

    pub(crate) fn crawl_level<'a>(
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

            let html = match self.client.get_text(url).await {
                Ok(html) => html,
                Err(_) => return urls,
            };

            let document = Html::parse_document(&html);

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

                    if let Some(ref pattern) = link_pattern {
                        if !pattern.is_match(href) {
                            continue;
                        }
                    }

                    let full_url = match Url::parse(base_url).and_then(|base| base.join(href)) {
                        Ok(u) => u.to_string(),
                        Err(_) => continue,
                    };

                    let matches_doc = document_patterns.is_empty()
                        || document_patterns.iter().any(|p| p.is_match(href));

                    links_to_process.push((full_url, matches_doc));
                }
            }

            for (full_url, matches_doc) in links_to_process {
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
                    urls.extend(
                        self.crawl_level(&full_url, base_url, level_idx + 1, Some(url))
                            .await,
                    );
                }
            }

            if let Some(ref pagination) = level.pagination {
                if let Some(next_url) = self.find_next_page(&document, base_url, pagination) {
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

    pub(crate) fn find_next_page(
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
}

/// Extract document and page links from HTML content.
fn extract_links_from_html(
    html: &str,
    current_url: &str,
    base_url: &str,
    allowed_domain: &str,
    document_patterns: &[Regex],
    page_link_selector: &str,
) -> (Vec<String>, Vec<String>) {
    let document = Html::parse_document(html);
    let mut doc_urls: Vec<String> = Vec::new();
    let mut page_urls: Vec<String> = Vec::new();

    let selector = match Selector::parse(page_link_selector) {
        Ok(s) => s,
        Err(_) => return (doc_urls, page_urls),
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
        } else if href.starts_with('/') {
            if let Ok(parsed) = Url::parse(current_url) {
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
        } else if let Ok(base) = Url::parse(current_url) {
            base.join(href).map(|u| u.to_string()).unwrap_or_default()
        } else {
            continue;
        };

        if full_url.is_empty() {
            continue;
        }

        // Check if link should be followed
        let url_host = full_url
            .parse::<Url>()
            .map(|u| u.host_str().unwrap_or("").to_string())
            .unwrap_or_default();
        let current_host = current_url
            .parse::<Url>()
            .map(|u| u.host_str().unwrap_or("").to_string())
            .unwrap_or_default();

        let is_allowed_domain = allowed_domain.is_empty() || url_host.ends_with(allowed_domain);
        let is_same_host = url_host == current_host;

        if !is_allowed_domain && !is_same_host {
            continue;
        }

        // Check if it's a document
        let is_document = !document_patterns.is_empty()
            && document_patterns.iter().any(|p| p.is_match(&full_url));

        if is_document {
            doc_urls.push(full_url);
        } else {
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
}
