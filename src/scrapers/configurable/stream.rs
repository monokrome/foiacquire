//! Streaming scrape methods and worker management.

use std::sync::Arc;
use tracing::debug;

#[cfg(feature = "browser")]
use super::super::browser::BrowserFetcher;
use super::super::{ScrapeStream, ScraperResult};
use super::ConfigurableScraper;

/// Default number of concurrent downloads.
pub const DEFAULT_CONCURRENCY: usize = 4;

impl ConfigurableScraper {
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
            let _ = discovery_handle.await;
            for worker in workers {
                let _ = worker.await;
            }
        });

        ScrapeStream {
            receiver: result_rx,
            total_count,
        }
    }

    /// Query the total count from an API source.
    pub(crate) async fn get_api_total_count(&self) -> Option<u64> {
        let api = self.config.discovery.api.as_ref()?;

        let default_base = String::new();
        let base_url = api
            .base_url
            .as_ref()
            .or(self.config.base_url.as_ref())
            .unwrap_or(&default_base);
        let api_url = format!("{}{}", base_url, api.endpoint);

        let mut params: Vec<(String, String)> = Vec::new();
        params.push((api.pagination.page_param.clone(), "1".to_string()));
        if let Some(ref size_param) = api.pagination.page_size_param {
            params.push((size_param.clone(), "1".to_string()));
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
    pub(crate) async fn spawn_download_workers(
        &self,
        count: usize,
        url_rx: tokio::sync::mpsc::Receiver<String>,
        result_tx: tokio::sync::mpsc::Sender<ScraperResult>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let url_rx = Arc::new(tokio::sync::Mutex::new(url_rx));
        let mut handles = Vec::with_capacity(count);

        #[cfg(feature = "browser")]
        let browser_config = self.browser_config.clone();

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
                        None => break,
                    };

                    if client.is_fetched(&url).await {
                        continue;
                    }

                    client.mark_fetching(&url).await;

                    #[cfg(feature = "browser")]
                    let fetch_result = if let Some(ref mut browser) = browser_fetcher {
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
                                break;
                            }
                        }
                        None => {
                            client.mark_failed(&url, "fetch failed").await;
                        }
                    }
                }

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
    pub(crate) async fn spawn_discovery_task(
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
                            return;
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
        })
    }
}
