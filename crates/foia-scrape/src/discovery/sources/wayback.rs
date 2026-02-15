//! Wayback Machine CDX API discovery source.
//!
//! Uses the Internet Archive's CDX API to find historical URLs.

use async_trait::async_trait;
use std::time::Duration;
use tracing::debug;

use super::create_discovery_client;
use crate::cdx::{self, CdxQuery};
use crate::discovery::url_utils::{dedup_and_limit, extract_domain};
use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use foia::models::DiscoveryMethod;

/// Discovery source using Wayback Machine CDX API.
#[derive(Default)]
pub struct WaybackSource {}

impl WaybackSource {
    /// Create a new Wayback source.
    pub fn new() -> Self {
        Self {}
    }

    /// Check if URL looks like a listing page.
    fn is_likely_listing(url: &str) -> bool {
        crate::discovery::is_listing_url(url)
    }
}

#[async_trait]
impl DiscoverySource for WaybackSource {
    fn name(&self) -> &str {
        "wayback"
    }

    fn method(&self) -> DiscoveryMethod {
        DiscoveryMethod::WaybackMachine
    }

    async fn discover(
        &self,
        target_domain: &str,
        _search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        let domain = extract_domain(target_domain);

        let from_date = config
            .custom_params
            .get("from")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let to_date = config
            .custom_params
            .get("to")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut query = CdxQuery::new(format!("*.{}", domain))
            .fields(&["original", "mimetype", "statuscode", "timestamp"])
            .match_type("domain")
            .collapse("urlkey")
            .filter("statuscode:200");

        if let Some(from) = &from_date {
            query = query.from_date(from.as_str());
        }
        if let Some(to) = &to_date {
            query = query.to_date(to.as_str());
        }
        if config.max_results > 0 {
            query = query.limit(config.max_results);
        }

        let cdx_url = query.build();

        debug!("Querying Wayback CDX API: {}", cdx_url);

        let client =
            create_discovery_client("wayback", config, Some(Duration::from_secs(60)), None)?;

        let text = client
            .get_text(&cdx_url)
            .await
            .map_err(DiscoveryError::Http)?;

        let rows = cdx::parse_cdx_response(&text)
            .map_err(|e| DiscoveryError::Parse(format!("Failed to parse CDX response: {}", e)))?;

        let mut urls: Vec<String> = Vec::new();

        for row in &rows {
            let Some(original_url) = row.get("original") else {
                continue;
            };

            if let Some(mimetype) = row.get("mimetype") {
                if !foia::utils::is_document_mimetype(mimetype) {
                    continue;
                }
            }

            urls.push(original_url.to_string());
        }

        dedup_and_limit(&mut urls, 0);

        debug!(
            "Wayback CDX found {} unique URLs for {}",
            urls.len(),
            domain
        );

        let discovered: Vec<DiscoveredUrl> = urls
            .into_iter()
            .map(|url| {
                let is_listing = Self::is_likely_listing(&url);
                let mut discovered =
                    DiscoveredUrl::new(url, DiscoveryMethod::WaybackMachine, "wayback".to_string());

                if is_listing {
                    discovered = discovered.listing_page();
                }

                discovered.detect_listing_page();
                discovered
            })
            .collect();

        Ok(discovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_cdx_url_basic() {
        let url = CdxQuery::new("*.example.gov")
            .fields(&["original", "mimetype", "statuscode", "timestamp"])
            .match_type("domain")
            .collapse("urlkey")
            .filter("statuscode:200")
            .limit(100)
            .build();

        assert!(url.contains("url=*.example.gov"));
        assert!(url.contains("matchType=domain"));
        assert!(url.contains("output=json"));
        assert!(url.contains("limit=100"));
        assert!(url.contains("filter=statuscode:200"));
    }

    #[test]
    fn build_cdx_url_with_dates() {
        let url = CdxQuery::new("*.example.gov")
            .fields(&["original", "mimetype", "statuscode", "timestamp"])
            .match_type("domain")
            .collapse("urlkey")
            .filter("statuscode:200")
            .from_date("20200101")
            .to_date("20231231")
            .build();

        assert!(url.contains("from=20200101"));
        assert!(url.contains("to=20231231"));
        assert!(!url.contains("limit="));
    }

    #[test]
    fn is_likely_listing_test() {
        assert!(WaybackSource::is_likely_listing(
            "https://example.gov/foia/reading-room/"
        ));
        assert!(WaybackSource::is_likely_listing(
            "https://example.gov/documents/"
        ));

        assert!(!WaybackSource::is_likely_listing(
            "https://example.gov/report.pdf"
        ));
    }
}
