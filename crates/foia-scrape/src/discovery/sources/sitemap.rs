//! Sitemap and robots.txt discovery source.
//!
//! Parses sitemap.xml files and robots.txt to discover URLs.

use async_trait::async_trait;
use tracing::{debug, warn};

use super::create_discovery_client;
use crate::discovery::url_utils::{dedup_and_limit, extract_xml_locs, normalize_base_url};
use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use foia::models::DiscoveryMethod;

/// Standard sitemap locations to check.
const SITEMAP_PATHS: &[&str] = &[
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap/sitemap.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap/index.xml",
];

/// Discovery source that parses sitemaps and robots.txt.
#[derive(Default)]
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

        let client = match create_discovery_client("sitemap", config, None, None) {
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
        let client = create_discovery_client("sitemap", config, None, None)?;

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
        extract_xml_locs(xml)
    }

    /// Extract URLs from a sitemap XML.
    fn extract_urls_from_sitemap(&self, xml: &str) -> Result<Vec<String>, DiscoveryError> {
        let urls = extract_xml_locs(xml);
        debug!("Extracted {} URLs from sitemap", urls.len());
        Ok(urls)
    }

    /// Check if a URL looks like a listing page vs a document.
    fn is_likely_listing(&self, url: &str) -> bool {
        crate::discovery::is_listing_url(url)
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

    async fn discover(
        &self,
        target_domain: &str,
        _search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        let base_url = normalize_base_url(target_domain);

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

        dedup_and_limit(&mut all_urls, config.max_results);

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
