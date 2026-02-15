//! API-based discovery methods (paginated, cursor, nested).

use std::sync::Arc;
use tracing::{debug, info, warn};

use super::extract::{extract_path, extract_url, extract_urls};
use super::ConfigurableScraper;
use crate::config::ScraperConfig;
use crate::HttpClient;
use foia::models::{CrawlUrl, DiscoveryMethod};
use foia::repository::DieselCrawlRepository;

impl ConfigurableScraper {
    /// Streaming API paginated discovery.
    pub(crate) async fn discover_api_paginated_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<DieselCrawlRepository>>,
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
                    if r.is_rate_limited() {
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
                        let _ = repo.add_url(&crawl_url).await;
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
    pub(crate) async fn discover_api_cursor_streaming(
        config: &ScraperConfig,
        client: &HttpClient,
        source_id: &str,
        crawl_repo: &Option<Arc<DieselCrawlRepository>>,
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
                        if r.is_rate_limited() {
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
                            let _ = repo.add_url(&crawl_url).await;
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

    /// Legacy API paginated discovery (non-streaming).
    pub(crate) async fn discover_api_paginated(&self) -> Vec<String> {
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

    /// Legacy API cursor discovery (non-streaming).
    pub(crate) async fn discover_api_cursor(&self) -> Vec<String> {
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

    /// Legacy API nested discovery (non-streaming).
    pub(crate) async fn discover_api_nested(&self) -> Vec<String> {
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
}
