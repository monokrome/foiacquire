//! Discovery result types.

use crate::models::DiscoveryMethod;

/// A discovered URL with metadata about how it was found.
#[derive(Debug, Clone)]
pub struct DiscoveredUrl {
    /// The discovered URL.
    pub url: String,

    /// How the URL was discovered.
    pub source_method: DiscoveryMethod,

    /// Name of the discovery source (e.g., "duckduckgo", "sitemap").
    pub discovery_source: String,

    /// Search query that found this URL (if applicable).
    pub query_used: Option<String>,

    /// Whether this appears to be a listing/index page.
    ///
    /// Listing pages are prioritized as they often lead to many documents.
    pub is_listing_page: bool,

    /// Confidence score for relevance (0.0 to 1.0).
    ///
    /// Higher scores indicate the URL is more likely to contain
    /// relevant documents.
    pub confidence: f32,

    /// Title or description from the search result.
    pub title: Option<String>,

    /// Snippet or description text.
    pub snippet: Option<String>,
}

impl DiscoveredUrl {
    /// Create a new discovered URL with minimal metadata.
    pub fn new(url: String, source_method: DiscoveryMethod, discovery_source: String) -> Self {
        Self {
            url,
            source_method,
            discovery_source,
            query_used: None,
            is_listing_page: false,
            confidence: 0.5,
            title: None,
            snippet: None,
        }
    }

    /// Set the search query that found this URL.
    pub fn with_query(mut self, query: String) -> Self {
        self.query_used = Some(query);
        self
    }

    /// Mark this URL as a listing page.
    pub fn as_listing_page(mut self) -> Self {
        self.is_listing_page = true;
        self.confidence = (self.confidence + 0.2).min(1.0);
        self
    }

    /// Set the confidence score.
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Set title and snippet from search result.
    pub fn with_metadata(mut self, title: Option<String>, snippet: Option<String>) -> Self {
        self.title = title;
        self.snippet = snippet;
        self
    }

    /// Check if this URL looks like a listing page based on patterns.
    pub fn detect_listing_page(&mut self) {
        let url_lower = self.url.to_lowercase();
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
            "page=",
            "?q=",
        ];

        // Check URL patterns
        if listing_patterns.iter().any(|p| url_lower.contains(p)) {
            self.is_listing_page = true;
            self.confidence = (self.confidence + 0.1).min(1.0);
        }

        // Check if URL doesn't end in a file extension (likely a page)
        if !url_lower.ends_with(".pdf")
            && !url_lower.ends_with(".doc")
            && !url_lower.ends_with(".docx")
            && !url_lower.ends_with(".xls")
            && !url_lower.ends_with(".xlsx")
        {
            self.confidence = (self.confidence + 0.05).min(1.0);
        }

        // Check title/snippet for listing indicators
        if let Some(ref title) = self.title {
            let title_lower = title.to_lowercase();
            if title_lower.contains("index")
                || title_lower.contains("list")
                || title_lower.contains("browse")
                || title_lower.contains("search results")
                || title_lower.contains("documents")
            {
                self.is_listing_page = true;
                self.confidence = (self.confidence + 0.15).min(1.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_discovered_url() {
        let url = DiscoveredUrl::new(
            "https://example.gov/doc.pdf".to_string(),
            DiscoveryMethod::SearchEngine,
            "duckduckgo".to_string(),
        );

        assert_eq!(url.url, "https://example.gov/doc.pdf");
        assert_eq!(url.discovery_source, "duckduckgo");
        assert!(!url.is_listing_page);
        assert_eq!(url.confidence, 0.5);
    }

    #[test]
    fn builder_methods() {
        let url = DiscoveredUrl::new(
            "https://example.gov/reports/".to_string(),
            DiscoveryMethod::SearchEngine,
            "google".to_string(),
        )
        .with_query("site:example.gov reports".to_string())
        .as_listing_page()
        .with_confidence(0.9);

        assert_eq!(url.query_used, Some("site:example.gov reports".to_string()));
        assert!(url.is_listing_page);
        assert_eq!(url.confidence, 0.9);
    }

    #[test]
    fn detect_listing_page_from_url() {
        let mut url = DiscoveredUrl::new(
            "https://example.gov/foia/reading-room/".to_string(),
            DiscoveryMethod::SearchEngine,
            "test".to_string(),
        );
        url.detect_listing_page();

        assert!(url.is_listing_page);
        assert!(url.confidence > 0.5);
    }

    #[test]
    fn detect_listing_page_from_title() {
        let mut url = DiscoveredUrl::new(
            "https://example.gov/page".to_string(),
            DiscoveryMethod::SearchEngine,
            "test".to_string(),
        )
        .with_metadata(Some("Document Index - Browse All".to_string()), None);

        url.detect_listing_page();

        assert!(url.is_listing_page);
    }

    #[test]
    fn pdf_url_not_listing() {
        let mut url = DiscoveredUrl::new(
            "https://example.gov/report.pdf".to_string(),
            DiscoveryMethod::SearchEngine,
            "test".to_string(),
        );
        url.detect_listing_page();

        // PDFs are not listing pages
        assert!(!url.is_listing_page);
    }
}
