//! URL extraction from text content.

#![allow(dead_code)]

use regex::Regex;
use std::collections::HashSet;
use url::Url;

/// Finds URLs in text content.
pub struct UrlFinder {
    /// Regex for matching URLs.
    url_regex: Regex,
    /// Allowed URL schemes.
    allowed_schemes: HashSet<String>,
    /// Domains to exclude.
    excluded_domains: HashSet<String>,
}

impl Default for UrlFinder {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlFinder {
    /// Create a new URL finder with default settings.
    pub fn new() -> Self {
        // Regex to match URLs - handles common URL patterns
        let url_regex = Regex::new(r"(?i)\b(https?://[^\s<>\[\]{}|\\^`\x00-\x1f\x7f]+)")
            .expect("URL regex should compile");

        let mut allowed_schemes = HashSet::new();
        allowed_schemes.insert("http".to_string());
        allowed_schemes.insert("https".to_string());

        // Common non-document domains to exclude
        let mut excluded_domains = HashSet::new();
        for domain in [
            "facebook.com",
            "twitter.com",
            "x.com",
            "instagram.com",
            "linkedin.com",
            "youtube.com",
            "google.com",
            "bing.com",
            "yahoo.com",
            "amazon.com",
            "ebay.com",
            "wikipedia.org",
            "reddit.com",
        ] {
            excluded_domains.insert(domain.to_string());
        }

        Self {
            url_regex,
            allowed_schemes,
            excluded_domains,
        }
    }

    /// Add domains to exclude from results.
    pub fn exclude_domains(mut self, domains: &[&str]) -> Self {
        for domain in domains {
            self.excluded_domains.insert(domain.to_string());
        }
        self
    }

    /// Find all URLs in the given text.
    pub fn find_urls(&self, text: &str) -> Vec<FoundUrl> {
        let mut found = Vec::new();
        let mut seen = HashSet::new();

        for cap in self.url_regex.captures_iter(text) {
            if let Some(url_match) = cap.get(1) {
                let url_str = url_match.as_str();

                // Clean up trailing punctuation
                let cleaned = self.clean_url(url_str);

                if seen.contains(&cleaned) {
                    continue;
                }

                // Validate and parse URL
                if let Some(found_url) = self.validate_url(&cleaned) {
                    if !self.is_excluded(&found_url.url) {
                        seen.insert(cleaned);
                        found.push(found_url);
                    }
                }
            }
        }

        found
    }

    /// Find URLs that look like document links.
    pub fn find_document_urls(&self, text: &str) -> Vec<FoundUrl> {
        self.find_urls(text)
            .into_iter()
            .filter(|u| u.is_likely_document())
            .collect()
    }

    /// Clean up a URL string by removing trailing punctuation.
    fn clean_url(&self, url: &str) -> String {
        let mut url = url.to_string();

        // Remove trailing punctuation that's not part of URLs
        while url.ends_with('.') || url.ends_with(',') || url.ends_with(')') || url.ends_with(']') {
            // Check for matching opening brackets - keep balanced parens/brackets
            let should_pop = match url.chars().last() {
                Some(')') => url.matches('(').count() < url.matches(')').count(),
                Some(']') => url.matches('[').count() < url.matches(']').count(),
                Some('.') | Some(',') => true,
                _ => false,
            };
            if should_pop {
                url.pop();
            } else {
                break;
            }
        }

        url
    }

    /// Validate a URL and create a FoundUrl if valid.
    fn validate_url(&self, url_str: &str) -> Option<FoundUrl> {
        let url = Url::parse(url_str).ok()?;

        // Check scheme
        if !self.allowed_schemes.contains(url.scheme()) {
            return None;
        }

        // Must have a host
        let host = url.host_str()?;

        // Infer document type from extension
        let doc_type = self.infer_document_type(url.path());

        Some(FoundUrl {
            url: url_str.to_string(),
            host: host.to_string(),
            document_type: doc_type,
        })
    }

    /// Check if a URL should be excluded.
    fn is_excluded(&self, url: &str) -> bool {
        if let Ok(parsed) = Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                // Check against excluded domains
                for excluded in &self.excluded_domains {
                    if host == *excluded || host.ends_with(&format!(".{}", excluded)) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Infer document type from URL path.
    fn infer_document_type(&self, path: &str) -> Option<DocumentType> {
        let lower_path = path.to_lowercase();

        if lower_path.ends_with(".pdf") {
            Some(DocumentType::Pdf)
        } else if lower_path.ends_with(".doc") || lower_path.ends_with(".docx") {
            Some(DocumentType::Word)
        } else if lower_path.ends_with(".xls") || lower_path.ends_with(".xlsx") {
            Some(DocumentType::Excel)
        } else if lower_path.ends_with(".ppt") || lower_path.ends_with(".pptx") {
            Some(DocumentType::PowerPoint)
        } else if lower_path.ends_with(".txt") {
            Some(DocumentType::Text)
        } else if lower_path.ends_with(".html") || lower_path.ends_with(".htm") {
            Some(DocumentType::Html)
        } else if lower_path.ends_with(".jpg")
            || lower_path.ends_with(".jpeg")
            || lower_path.ends_with(".png")
            || lower_path.ends_with(".gif")
            || lower_path.ends_with(".tiff")
            || lower_path.ends_with(".tif")
        {
            Some(DocumentType::Image)
        } else {
            None
        }
    }
}

/// A URL found in document text.
#[derive(Debug, Clone)]
pub struct FoundUrl {
    /// The full URL.
    pub url: String,
    /// The host portion of the URL.
    pub host: String,
    /// Inferred document type if any.
    pub document_type: Option<DocumentType>,
}

impl FoundUrl {
    /// Check if this URL likely points to a document.
    pub fn is_likely_document(&self) -> bool {
        self.document_type.is_some()
    }
}

/// Types of documents that can be inferred from URLs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentType {
    Pdf,
    Word,
    Excel,
    PowerPoint,
    Text,
    Html,
    Image,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_urls() {
        let finder = UrlFinder::new();
        let text =
            "Check out https://example.com/doc.pdf and http://gov.site/report.docx for more info.";

        let urls = finder.find_urls(text);
        assert_eq!(urls.len(), 2);
        assert!(urls[0].url.contains("example.com"));
        assert!(urls[1].url.contains("gov.site"));
    }

    #[test]
    fn test_find_document_urls() {
        let finder = UrlFinder::new();
        let text =
            "Links: https://example.com/doc.pdf https://example.com/page.html https://other.com/";

        let docs = finder.find_document_urls(text);
        assert_eq!(docs.len(), 2); // pdf and html
    }

    #[test]
    fn test_clean_url() {
        let finder = UrlFinder::new();
        assert_eq!(
            finder.clean_url("https://example.com/doc.pdf."),
            "https://example.com/doc.pdf"
        );
        assert_eq!(
            finder.clean_url("https://example.com/doc.pdf),"),
            "https://example.com/doc.pdf"
        );
    }

    #[test]
    fn test_excluded_domains() {
        let finder = UrlFinder::new();
        let text = "See https://facebook.com/page and https://gov.agency/doc.pdf";

        let urls = finder.find_urls(text);
        assert_eq!(urls.len(), 1);
        assert!(urls[0].url.contains("gov.agency"));
    }

    #[test]
    fn test_document_type_inference() {
        let finder = UrlFinder::new();
        let urls = finder.find_urls("https://example.com/report.pdf");

        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].document_type, Some(DocumentType::Pdf));
    }
}
