//! Document fetching methods for the configurable scraper.

use chrono::Utc;
use tracing::debug;

use super::ConfigurableScraper;
use crate::{extract_title_from_url, HttpClient, ScraperResult};
#[cfg(feature = "browser")]
use foiacquire::browser::BrowserFetcher;

/// Error type distinguishing browser infrastructure failures from URL-specific failures.
#[cfg(feature = "browser")]
pub(crate) enum FetchError {
    /// The browser itself is unreachable (infrastructure failure).
    /// URLs should NOT be marked as failed — the problem is not URL-specific.
    BrowserUnavailable(String),
    /// The specific URL failed to fetch (URL-specific failure).
    /// URLs should be marked as failed normally.
    UrlFailed(String),
}

impl ConfigurableScraper {
    /// Static fetch method for use in workers.
    pub(crate) async fn fetch_url(client: &HttpClient, url: &str) -> Option<ScraperResult> {
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
            archive_snapshot_id: None,
            archive_captured_at: None,
        };

        // Update metadata
        result.metadata = serde_json::json!({
            "fetched_at": result.fetched_at.to_rfc3339(),
        });

        Some(result)
    }

    /// Fetch URL using browser for anti-bot protected sites.
    ///
    /// Returns `FetchError::BrowserUnavailable` if the browser itself can't be reached
    /// (infrastructure failure — URL should not be marked as failed), or
    /// `FetchError::UrlFailed` if the browser is fine but this URL couldn't be fetched.
    #[cfg(feature = "browser")]
    pub(crate) async fn fetch_url_with_browser(
        browser: &mut BrowserFetcher,
        _client: &HttpClient,
        url: &str,
    ) -> Result<ScraperResult, FetchError> {
        debug!("Fetching with browser: {}", url);

        // Check browser connectivity first — separate from URL-specific errors.
        // ensure_browser() is idempotent: returns Ok immediately if already connected.
        if let Err(e) = browser.ensure_browser().await {
            return Err(FetchError::BrowserUnavailable(e.to_string()));
        }

        let response = match browser.fetch(url).await {
            Ok(r) => r,
            Err(e) => {
                return Err(FetchError::UrlFailed(format!(
                    "Browser fetch failed for {}: {}",
                    url, e
                )));
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

        Ok(ScraperResult {
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
            archive_snapshot_id: None,
            archive_captured_at: None,
        })
    }

    /// Fetch binary URL (PDF, images) using JavaScript fetch from browser context.
    /// This bypasses Akamai/Cloudflare bot protection on PDF endpoints.
    ///
    /// Returns `FetchError::BrowserUnavailable` if the browser itself can't be reached,
    /// or `FetchError::UrlFailed` if the browser is fine but this URL couldn't be fetched.
    #[cfg(feature = "browser")]
    pub(crate) async fn fetch_url_with_browser_binary(
        browser: &mut BrowserFetcher,
        url: &str,
        context_url: Option<&str>,
    ) -> Result<ScraperResult, FetchError> {
        debug!("Fetching binary with browser: {}", url);

        if let Err(e) = browser.ensure_browser().await {
            return Err(FetchError::BrowserUnavailable(e.to_string()));
        }

        let response = match browser.fetch_binary(url, context_url).await {
            Ok(r) => r,
            Err(e) => {
                return Err(FetchError::UrlFailed(format!(
                    "Browser binary fetch failed for {}: {}",
                    url, e
                )));
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

        Ok(ScraperResult {
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
            archive_snapshot_id: None,
            archive_captured_at: None,
        })
    }

    /// Fetch a document with cached headers.
    pub(crate) async fn fetch_with_cache(
        &self,
        crawl_url: &foiacquire::models::CrawlUrl,
    ) -> Option<ScraperResult> {
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

    pub(crate) async fn fetch_internal(
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

        let content_hash = foiacquire::models::DocumentVersion::compute_hash(&content);

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
            archive_snapshot_id: None,
            archive_captured_at: None,
        })
    }
}
