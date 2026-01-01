//! Wayback Machine CDX API discovery source.
//!
//! Uses the Internet Archive's CDX API to find historical URLs.

use async_trait::async_trait;
use tracing::debug;

use crate::discovery::{DiscoveredUrl, DiscoveryError, DiscoverySource, DiscoverySourceConfig};
use crate::models::DiscoveryMethod;

/// CDX API base URL.
const CDX_API_URL: &str = "https://web.archive.org/cdx/search/cdx";

/// Discovery source using Wayback Machine CDX API.
pub struct WaybackSource {
    client: reqwest::Client,
}

impl WaybackSource {
    /// Create a new Wayback source.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent("Mozilla/5.0 (compatible; FOIAcquire/1.0)")
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Build the CDX API URL with parameters.
    fn build_cdx_url(
        &self,
        domain: &str,
        from_date: Option<&str>,
        to_date: Option<&str>,
        limit: usize,
    ) -> String {
        let mut url = format!(
            "{}?url=*.{}&matchType=domain&output=json&fl=original,mimetype,statuscode,timestamp&collapse=urlkey",
            CDX_API_URL, domain
        );

        // Filter for successful responses only
        url.push_str("&filter=statuscode:200");

        // Date range filters
        if let Some(from) = from_date {
            url.push_str(&format!("&from={}", from));
        }
        if let Some(to) = to_date {
            url.push_str(&format!("&to={}", to));
        }

        // Limit results
        if limit > 0 {
            url.push_str(&format!("&limit={}", limit));
        }

        url
    }

    /// Check if a mimetype indicates a document.
    fn is_document_mimetype(mimetype: &str) -> bool {
        matches!(
            mimetype,
            "application/pdf"
                | "application/msword"
                | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                | "application/vnd.ms-excel"
                | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                | "text/html"
                | "application/xhtml+xml"
        )
    }

    /// Check if URL looks like a listing page.
    fn is_likely_listing(url: &str) -> bool {
        let url_lower = url.to_lowercase();

        // URLs ending in document extensions are not listings
        if url_lower.ends_with(".pdf")
            || url_lower.ends_with(".doc")
            || url_lower.ends_with(".docx")
            || url_lower.ends_with(".xls")
            || url_lower.ends_with(".xlsx")
        {
            return false;
        }

        // Listing patterns
        let listing_patterns = [
            "/index",
            "/browse",
            "/list",
            "/search",
            "/documents/",
            "/reports/",
            "/publications/",
            "/library/",
            "/reading-room",
            "/foia/",
            "/archive",
        ];

        listing_patterns.iter().any(|p| url_lower.contains(p))
    }
}

impl Default for WaybackSource {
    fn default() -> Self {
        Self::new()
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

    fn requires_browser(&self) -> bool {
        false
    }

    async fn discover(
        &self,
        target_domain: &str,
        _search_terms: &[String],
        config: &DiscoverySourceConfig,
    ) -> Result<Vec<DiscoveredUrl>, DiscoveryError> {
        // Extract domain from URL if needed
        let domain = if target_domain.starts_with("http") {
            url::Url::parse(target_domain)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_else(|| target_domain.to_string())
        } else {
            target_domain.to_string()
        };

        // Get date range from custom params if specified
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

        let cdx_url = self.build_cdx_url(
            &domain,
            from_date.as_deref(),
            to_date.as_deref(),
            config.max_results,
        );

        debug!("Querying Wayback CDX API: {}", cdx_url);

        let response = self
            .client
            .get(&cdx_url)
            .send()
            .await
            .map_err(DiscoveryError::Http)?;

        if !response.status().is_success() {
            return Err(DiscoveryError::Unavailable(format!(
                "CDX API returned {}",
                response.status()
            )));
        }

        let text = response.text().await.map_err(DiscoveryError::Http)?;

        // Parse JSON response
        // CDX returns array of arrays: [["original", "mimetype", "statuscode", "timestamp"], ...]
        let rows: Vec<Vec<String>> = serde_json::from_str(&text).map_err(|e| {
            DiscoveryError::Parse(format!("Failed to parse CDX response: {}", e))
        })?;

        // Skip header row
        let data_rows = if !rows.is_empty() && rows[0].get(0).map(|s| s.as_str()) == Some("original")
        {
            &rows[1..]
        } else {
            &rows[..]
        };

        let mut urls: Vec<String> = Vec::new();

        for row in data_rows {
            if row.len() >= 2 {
                let original_url = &row[0];
                let mimetype = row.get(1).map(|s| s.as_str()).unwrap_or("");

                // Filter by mimetype if we have it
                if !mimetype.is_empty() && !Self::is_document_mimetype(mimetype) {
                    continue;
                }

                urls.push(original_url.clone());
            }
        }

        // Deduplicate
        urls.sort();
        urls.dedup();

        debug!(
            "Wayback CDX found {} unique URLs for {}",
            urls.len(),
            domain
        );

        // Convert to DiscoveredUrl
        let discovered: Vec<DiscoveredUrl> = urls
            .into_iter()
            .map(|url| {
                let is_listing = Self::is_likely_listing(&url);
                let mut discovered = DiscoveredUrl::new(
                    url,
                    DiscoveryMethod::WaybackMachine,
                    "wayback".to_string(),
                );

                if is_listing {
                    discovered = discovered.as_listing_page();
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
        let source = WaybackSource::new();
        let url = source.build_cdx_url("example.gov", None, None, 100);

        assert!(url.contains("url=*.example.gov"));
        assert!(url.contains("matchType=domain"));
        assert!(url.contains("output=json"));
        assert!(url.contains("limit=100"));
        assert!(url.contains("filter=statuscode:200"));
    }

    #[test]
    fn build_cdx_url_with_dates() {
        let source = WaybackSource::new();
        let url = source.build_cdx_url("example.gov", Some("20200101"), Some("20231231"), 0);

        assert!(url.contains("from=20200101"));
        assert!(url.contains("to=20231231"));
        assert!(!url.contains("limit="));
    }

    #[test]
    fn is_document_mimetype_test() {
        assert!(WaybackSource::is_document_mimetype("application/pdf"));
        assert!(WaybackSource::is_document_mimetype("text/html"));
        assert!(WaybackSource::is_document_mimetype("application/msword"));

        assert!(!WaybackSource::is_document_mimetype("image/png"));
        assert!(!WaybackSource::is_document_mimetype("application/javascript"));
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
