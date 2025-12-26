//! HTTP client with ETag and conditional request support.

#![allow(dead_code)]

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
use reqwest::{Client, StatusCode};
use tokio::sync::Mutex;

use super::rate_limiter::RateLimiter;
use crate::models::{CrawlRequest, CrawlUrl, UrlStatus};
use crate::repository::CrawlRepository;

/// HTTP client with request logging and conditional request support.
#[derive(Clone)]
pub struct HttpClient {
    client: Client,
    crawl_repo: Option<Arc<Mutex<CrawlRepository>>>,
    source_id: String,
    request_delay: Duration,
    referer: Option<String>,
    rate_limiter: RateLimiter,
}

impl HttpClient {
    /// Create a new HTTP client.
    pub fn new(source_id: &str, timeout: Duration, request_delay: Duration) -> Self {
        Self::with_user_agent(source_id, timeout, request_delay, None)
    }

    /// Create a new HTTP client with custom user agent configuration.
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
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(timeout)
            .gzip(true)
            .brotli(true)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter: RateLimiter::new(),
        }
    }

    /// Create a new HTTP client with a shared rate limiter.
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
    pub fn with_rate_limiter_and_user_agent(
        source_id: &str,
        timeout: Duration,
        request_delay: Duration,
        rate_limiter: RateLimiter,
        user_agent_config: Option<&str>,
    ) -> Self {
        let user_agent = resolve_user_agent(user_agent_config);
        let client = Client::builder()
            .user_agent(&user_agent)
            .timeout(timeout)
            .gzip(true)
            .brotli(true)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            crawl_repo: None,
            source_id: source_id.to_string(),
            request_delay,
            referer: None,
            rate_limiter,
        }
    }

    /// Set the crawl repository for request logging.
    pub fn with_crawl_repo(mut self, repo: Arc<Mutex<CrawlRepository>>) -> Self {
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

    /// Make a GET request with optional conditional headers.
    /// Uses adaptive rate limiting per domain.
    pub async fn get(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HttpResponse, reqwest::Error> {
        // Wait for rate limiter before making request
        let domain = self.rate_limiter.acquire(url).await;

        let mut request = self.client.get(url);

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
            CrawlRequest::new(self.source_id.clone(), url.to_string(), "GET".to_string());
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
            let repo = repo.lock().await;
            let _ = repo.log_request(&request_log);
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
            } else if response.status().is_success() || status_code == 304 {
                // Success - may recover from backoff
                self.rate_limiter.report_success(domain).await;
            }
        }

        // Apply base delay (rate limiter handles additional adaptive delay)
        tokio::time::sleep(self.request_delay).await;

        Ok(HttpResponse {
            status: response.status(),
            headers: response_headers,
            response,
        })
    }

    /// Get page content as text.
    pub async fn get_text(&self, url: &str) -> Result<String, reqwest::Error> {
        let response = self.get(url, None, None).await?;
        response.response.text().await
    }

    /// Make a HEAD request to check headers without downloading content.
    /// Returns headers including ETag, Last-Modified, Content-Disposition, etc.
    pub async fn head(
        &self,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<HeadResponse, reqwest::Error> {
        // Wait for rate limiter before making request
        let domain = self.rate_limiter.acquire(url).await;

        let mut request = self.client.head(url);

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
            let repo = repo.lock().await;
            let _ = repo.log_request(&request_log);
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
            let repo = repo.lock().await;
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url) {
                crawl_url.mark_fetching();
                let _ = repo.update_url(&crawl_url);
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
            let repo = repo.lock().await;
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url) {
                crawl_url.mark_fetched(content_hash, document_id, etag, last_modified);
                let _ = repo.update_url(&crawl_url);
            }
        }
    }

    /// Update crawl URL status after skip (304 Not Modified).
    pub async fn mark_skipped(&self, url: &str, reason: &str) {
        if let Some(repo) = &self.crawl_repo {
            let repo = repo.lock().await;
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url) {
                crawl_url.mark_skipped(reason);
                let _ = repo.update_url(&crawl_url);
            }
        }
    }

    /// Update crawl URL status after failure.
    pub async fn mark_failed(&self, url: &str, error: &str) {
        if let Some(repo) = &self.crawl_repo {
            let repo = repo.lock().await;
            if let Ok(Some(mut crawl_url)) = repo.get_url(&self.source_id, url) {
                crawl_url.mark_failed(error, 3);
                let _ = repo.update_url(&crawl_url);
            }
        }
    }

    /// Track a discovered URL.
    pub async fn track_url(&self, crawl_url: &CrawlUrl) -> bool {
        if let Some(repo) = &self.crawl_repo {
            let repo = repo.lock().await;
            repo.add_url(crawl_url).unwrap_or(false)
        } else {
            false
        }
    }

    /// Check if URL was already fetched.
    pub async fn is_fetched(&self, url: &str) -> bool {
        if let Some(repo) = &self.crawl_repo {
            let repo = repo.lock().await;
            if let Ok(Some(crawl_url)) = repo.get_url(&self.source_id, url) {
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
            let repo = repo.lock().await;
            if let Ok(Some(crawl_url)) = repo.get_url(&self.source_id, url) {
                return (crawl_url.etag, crawl_url.last_modified);
            }
        }
        (None, None)
    }
}
