//! Sitemap and robots.txt discovery source.
//!
//! Parses sitemap.xml files and robots.txt to discover URLs.

use async_trait::async_trait;
use std::time::Duration;
use tracing::{debug, warn};

use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use crate::HttpClient;
use foiacquire::models::DiscoveryMethod;

/// Standard sitemap locations to check.
const SITEMAP_PATHS: &[&str] = &[
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap/sitemap.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap/index.xml",
];

/// Discovery source that parses sitemaps and robots.txt.
pub struct SitemapSource {}

impl SitemapSource {
    /// Create a new sitemap source.
    pub fn new() -> Self {
        Self {}
    }

    /// Parse robots.txt to find sitemap URLs.
    async fn parse_robots_txt(
        &self,
        base_url: &str,
        config: &DiscoverySourceConfig,
    ) -> Vec<String> {
        let robots_url = format!("{}/robots.txt", base_url.trim_end_matches('/'));
        debug!("Checking robots.txt at {}", robots_url);

        // Create HTTP client with privacy configuration
        let client = match HttpClient::with_privacy(
            "sitemap",
            Duration::from_secs(30),
            Duration::from_millis(config.rate_limit_ms),
            Some("Mozilla/5.0 (compatible; FOIAcquire/1.0)"),
            &config.privacy,
        ) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to create HTTP client: {}", e);
                return vec![];
            }
        };

        let text = match client.get_text(&robots_url).await {
            Ok(t) => t,
            Err(e) => {
                debug!("Failed to fetch robots.txt: {}", e);
                return vec![];
            }
        };

        // Parse Sitemap: directives
        text.lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.to_lowercase().starts_with("sitemap:") {
                    Some(line[8..].trim().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Fetch and parse a sitemap XML file (non-recursive).
    ///
    /// Uses a work queue to handle sitemap indexes without recursion.
    async fn parse_sitemap(
        &self,
        url: &str,
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<String>, DiscoveryError> {
        // Create HTTP client with privacy configuration
        let client = HttpClient::with_privacy(
            "sitemap",
            Duration::from_secs(30),
            Duration::from_millis(config.rate_limit_ms),
            Some("Mozilla/5.0 (compatible; FOIAcquire/1.0)"),
            &config.privacy,
        )
        .map_err(|e| DiscoveryError::Config(format!("Failed to create HTTP client: {}", e)))?;

        let mut all_urls = Vec::new();
        let mut pending_sitemaps = vec![url.to_string()];
        let mut processed = std::collections::HashSet::new();
        const MAX_SITEMAPS: usize = 100; // Prevent infinite loops

        while let Some(sitemap_url) = pending_sitemaps.pop() {
            if processed.contains(&sitemap_url) || processed.len() >= MAX_SITEMAPS {
                continue;
            }
            processed.insert(sitemap_url.clone());

            debug!("Fetching sitemap: {}", sitemap_url);

            let text = match client.get_text(&sitemap_url).await {
                Ok(t) => t,
                Err(e) => {
                    warn!("Failed to fetch sitemap {}: {}", sitemap_url, e);
                    continue;
                }
            };

            // Check if this is a sitemap index
            if text.contains("<sitemapindex") {
                // Extract sitemap URLs and add to pending
                for loc in self.extract_locs(&text) {
                    if !processed.contains(&loc) {
                        pending_sitemaps.push(loc);
                    }
                }
            } else {
                // Regular sitemap - extract URLs
                match self.extract_urls_from_sitemap(&text) {
                    Ok(urls) => all_urls.extend(urls),
                    Err(e) => warn!("Failed to parse sitemap {}: {}", sitemap_url, e),
                }
            }
        }

        Ok(all_urls)
    }

    /// Extract <loc> values from XML.
    fn extract_locs(&self, xml: &str) -> Vec<String> {
        let mut locs = Vec::new();
        for line in xml.lines() {
            if let Some(start) = line.find("<loc>") {
                if let Some(end) = line.find("</loc>") {
                    // Bounds check: end must be after start + 5 (length of "<loc>")
                    let content_start = start + 5;
                    if end > content_start {
                        let url = &line[content_start..end];
                        let url = url
                            .replace("&amp;", "&")
                            .replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&quot;", "\"")
                            .replace("&apos;", "'");
                        if !url.is_empty() {
                            locs.push(url);
                        }
                    }
                }
            }
        }
        locs
    }

    /// Extract URLs from a sitemap XML.
    fn extract_urls_from_sitemap(&self, xml: &str) -> Result<Vec<String>, DiscoveryError> {
        let mut urls = Vec::new();

        // Simple extraction of <loc> tags
        // Sitemaps use XML namespaces which scraper doesn't handle well,
        // so we use simple string parsing
        for line in xml.lines() {
            let line = line.trim();
            if let Some(start) = line.find("<loc>") {
                if let Some(end) = line.find("</loc>") {
                    // Bounds check: end must be after start + 5 (length of "<loc>")
                    let content_start = start + 5;
                    if end > content_start {
                        let url = &line[content_start..end];
                        // Unescape XML entities
                        let url = url
                            .replace("&amp;", "&")
                            .replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&quot;", "\"")
                            .replace("&apos;", "'");
                        if !url.is_empty() {
                            urls.push(url);
                        }
                    }
                }
            }
        }

        debug!("Extracted {} URLs from sitemap", urls.len());
        Ok(urls)
    }

    /// Check if a URL looks like a listing page vs a document.
    fn is_likely_listing(&self, url: &str) -> bool {
        crate::discovery::is_listing_url(url)
    }
}

impl Default for SitemapSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscoverySource for SitemapSource {
    fn name(&self) -> &str {
        "sitemap"
    }

    fn method(&self) -> DiscoveryMethod {
        DiscoveryMethod::Sitemap
    }

    fn requires_browser(&self) -> bool {
        false
    }

    async fn discover(
        &self,
        target_domain: &str,
        _search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        let base_url = if target_domain.starts_with("http") {
            target_domain.trim_end_matches('/').to_string()
        } else {
            format!("https://{}", target_domain.trim_end_matches('/'))
        };

        let mut all_urls = Vec::new();

        // First check robots.txt for sitemap URLs
        let robots_sitemaps = self.parse_robots_txt(&base_url, config).await;
        for sitemap_url in robots_sitemaps {
            match self.parse_sitemap(&sitemap_url, config).await {
                Ok(urls) => all_urls.extend(urls),
                Err(e) => warn!("Failed to parse sitemap from robots.txt: {}", e),
            }
        }

        // Try standard sitemap locations
        for path in SITEMAP_PATHS {
            let sitemap_url = format!("{}{}", base_url, path);
            match self.parse_sitemap(&sitemap_url, config).await {
                Ok(urls) => {
                    all_urls.extend(urls);
                    break; // Found a working sitemap, stop trying others
                }
                Err(_) => continue,
            }
        }

        // Deduplicate
        all_urls.sort();
        all_urls.dedup();

        // Apply limit
        if config.max_results > 0 && all_urls.len() > config.max_results {
            all_urls.truncate(config.max_results);
        }

        // Convert to DiscoveredUrl
        let discovered: Vec<DiscoveredUrl> = all_urls
            .into_iter()
            .map(|url| {
                let is_listing = self.is_likely_listing(&url);
                let mut discovered =
                    DiscoveredUrl::new(url, DiscoveryMethod::Sitemap, "sitemap".to_string());

                if is_listing {
                    discovered = discovered.listing_page();
                }

                discovered.detect_listing_page();
                discovered
            })
            .collect();

        debug!(
            "Sitemap discovery found {} URLs for {}",
            discovered.len(),
            target_domain
        );

        Ok(discovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_urls_from_simple_sitemap() {
        let source = SitemapSource::new();
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.gov/documents/report1.pdf</loc>
  </url>
  <url>
    <loc>https://example.gov/documents/report2.pdf</loc>
  </url>
  <url>
    <loc>https://example.gov/foia/reading-room/</loc>
  </url>
</urlset>"#;

        let urls = source.extract_urls_from_sitemap(xml).unwrap();
        assert_eq!(urls.len(), 3);
        assert!(urls.contains(&"https://example.gov/documents/report1.pdf".to_string()));
        assert!(urls.contains(&"https://example.gov/foia/reading-room/".to_string()));
    }

    #[test]
    fn extract_urls_with_xml_entities() {
        let source = SitemapSource::new();
        let xml = r#"<urlset>
  <url><loc>https://example.gov/search?q=test&amp;page=1</loc></url>
</urlset>"#;

        let urls = source.extract_urls_from_sitemap(xml).unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0], "https://example.gov/search?q=test&page=1");
    }

    #[test]
    fn is_likely_listing_detection() {
        let source = SitemapSource::new();

        // Listings
        assert!(source.is_likely_listing("https://example.gov/foia/reading-room/"));
        assert!(source.is_likely_listing("https://example.gov/documents/"));
        assert!(source.is_likely_listing("https://example.gov/reports/index.html"));

        // Not listings
        assert!(!source.is_likely_listing("https://example.gov/report.pdf"));
        assert!(!source.is_likely_listing("https://example.gov/data.xlsx"));
    }
}
