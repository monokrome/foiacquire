//! DuckDuckGo search engine discovery source.
//!
//! Uses DuckDuckGo HTML search to find URLs.

use async_trait::async_trait;
use scraper::{Html, Selector};
use tracing::{debug, warn};

use super::QueryBuilder;
use crate::discovery::sources::create_discovery_client;
use crate::discovery::url_utils::extract_domain;
use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use foia::models::DiscoveryMethod;

/// DuckDuckGo search URL.
const DDG_SEARCH_URL: &str = "https://html.duckduckgo.com/html/";

/// Discovery source using DuckDuckGo search.
#[derive(Default)]
pub struct DuckDuckGoSource {}

impl DuckDuckGoSource {
    /// Create a new DuckDuckGo source.
    pub fn new() -> Self {
        Self {}
    }

    /// Search DuckDuckGo and extract result URLs.
    async fn search(
        &self,
        query: &str,
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<SearchResult>, DiscoveryError> {
        debug!("DuckDuckGo search: {}", query);

        let client = create_discovery_client("duckduckgo", config, None, Some("impersonate"))?;

        let response = client
            .post(DDG_SEARCH_URL, &[("q", query), ("kl", "us-en")])
            .await
            .map_err(DiscoveryError::Http)?;

        if !response.status.is_success() {
            return Err(DiscoveryError::Unavailable(format!(
                "DuckDuckGo returned {}",
                response.status
            )));
        }

        let html = response
            .text()
            .await
            .map_err(|e| DiscoveryError::Parse(format!("Failed to read response text: {}", e)))?;
        self.parse_results(&html)
    }

    /// Parse search results from HTML.
    fn parse_results(&self, html: &str) -> Result<Vec<SearchResult>, DiscoveryError> {
        let document = Html::parse_document(html);

        // DuckDuckGo HTML results are in <a class="result__a"> elements
        let result_selector = Selector::parse("a.result__a")
            .map_err(|e| DiscoveryError::Parse(format!("Failed to parse selector: {:?}", e)))?;

        // Snippet is in <a class="result__snippet">
        let _snippet_selector = Selector::parse("a.result__snippet").ok();

        let mut results = Vec::new();

        for element in document.select(&result_selector) {
            // Get the href attribute
            if let Some(href) = element.value().attr("href") {
                // DuckDuckGo wraps URLs in a redirect, extract the actual URL
                let url = self.extract_url(href);

                if let Some(url) = url {
                    let title = element.text().collect::<String>().trim().to_string();

                    results.push(SearchResult {
                        url,
                        title: if title.is_empty() { None } else { Some(title) },
                        snippet: None,
                    });
                }
            }
        }

        debug!("Parsed {} results from DuckDuckGo", results.len());
        Ok(results)
    }

    /// Extract the actual URL from DuckDuckGo's redirect URL.
    fn extract_url(&self, href: &str) -> Option<String> {
        // DuckDuckGo sometimes uses direct URLs, sometimes redirects
        if href.starts_with("//duckduckgo.com/l/") {
            // Extract from redirect: //duckduckgo.com/l/?uddg=<encoded_url>&...
            if let Some(uddg_start) = href.find("uddg=") {
                let encoded = &href[uddg_start + 5..];
                let end = encoded.find('&').unwrap_or(encoded.len());
                let encoded_url = &encoded[..end];

                // URL decode
                urlencoding::decode(encoded_url)
                    .ok()
                    .map(|s| s.into_owned())
            } else {
                None
            }
        } else if href.starts_with("http://") || href.starts_with("https://") {
            Some(href.to_string())
        } else if href.starts_with("//") {
            Some(format!("https:{}", href))
        } else {
            None
        }
    }

    /// Check if URL belongs to the target domain.
    fn url_matches_domain(&self, url: &str, target_domain: &str) -> bool {
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                // Check if host ends with target domain
                return host == target_domain || host.ends_with(&format!(".{}", target_domain));
            }
        }
        false
    }

    /// Check if URL looks like a listing page.
    fn is_likely_listing(&self, url: &str) -> bool {
        crate::discovery::is_listing_url(url)
    }
}

/// A single search result.
struct SearchResult {
    url: String,
    title: Option<String>,
    snippet: Option<String>,
}

#[async_trait]
impl DiscoverySource for DuckDuckGoSource {
    fn name(&self) -> &str {
        "duckduckgo"
    }

    fn method(&self) -> DiscoveryMethod {
        DiscoveryMethod::SearchEngine
    }

    async fn discover(
        &self,
        target_domain: &str,
        search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        let domain = extract_domain(target_domain);

        let mut all_results: Vec<DiscoveredUrl> = Vec::new();
        let mut seen_urls = std::collections::HashSet::new();

        // If no search terms provided, use listing-focused default terms
        let terms: Vec<String> = if search_terms.is_empty() {
            vec![
                "reading room".to_string(),
                "FOIA".to_string(),
                "documents".to_string(),
                "publications".to_string(),
                "reports".to_string(),
            ]
        } else {
            search_terms.to_vec()
        };

        for term in &terms {
            // Build query with site: restriction
            let query = QueryBuilder::new().site(&domain).term(term).build();

            match self.search(&query, config).await {
                Ok(results) => {
                    for result in results {
                        // Filter to target domain
                        if !self.url_matches_domain(&result.url, &domain) {
                            continue;
                        }

                        // Skip duplicates
                        if seen_urls.contains(&result.url) {
                            continue;
                        }
                        seen_urls.insert(result.url.clone());

                        let is_listing = self.is_likely_listing(&result.url);
                        let mut discovered = DiscoveredUrl::new(
                            result.url,
                            DiscoveryMethod::SearchEngine,
                            "duckduckgo".to_string(),
                        )
                        .with_query(query.clone())
                        .with_metadata(result.title, result.snippet);

                        if is_listing {
                            discovered = discovered.listing_page();
                        }

                        discovered.detect_listing_page();
                        all_results.push(discovered);

                        // Check limit
                        if config.max_results > 0 && all_results.len() >= config.max_results {
                            return Ok(all_results);
                        }
                    }
                }
                Err(e) => {
                    warn!("DuckDuckGo search failed for '{}': {}", query, e);
                }
            }

            // Rate limit between queries
            if config.rate_limit_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(config.rate_limit_ms)).await;
            }
        }

        debug!(
            "DuckDuckGo discovery found {} URLs for {}",
            all_results.len(),
            domain
        );

        Ok(all_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_direct_url() {
        let source = DuckDuckGoSource::new();

        let url = source.extract_url("https://example.gov/doc.pdf");
        assert_eq!(url, Some("https://example.gov/doc.pdf".to_string()));
    }

    #[test]
    fn extract_protocol_relative_url() {
        let source = DuckDuckGoSource::new();

        let url = source.extract_url("//example.gov/doc.pdf");
        assert_eq!(url, Some("https://example.gov/doc.pdf".to_string()));
    }

    #[test]
    fn url_matches_domain() {
        let source = DuckDuckGoSource::new();

        assert!(source.url_matches_domain("https://fbi.gov/page", "fbi.gov"));
        assert!(source.url_matches_domain("https://vault.fbi.gov/page", "fbi.gov"));
        assert!(!source.url_matches_domain("https://cia.gov/page", "fbi.gov"));
    }

    #[test]
    fn is_likely_listing_detection() {
        let source = DuckDuckGoSource::new();

        assert!(source.is_likely_listing("https://fbi.gov/foia/reading-room/"));
        assert!(source.is_likely_listing("https://fbi.gov/documents/"));
        assert!(!source.is_likely_listing("https://fbi.gov/report.pdf"));
    }
}
