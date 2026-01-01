//! Template-based term extraction.
//!
//! Extracts terms from HTML using CSS selectors.

use async_trait::async_trait;
use scraper::{Html, Selector};
use std::collections::HashSet;
use tracing::debug;

use super::{ExtractionContext, TermExtractor};
use crate::discovery::DiscoveryError;

/// Default CSS selectors for term extraction.
const DEFAULT_SELECTORS: &[&str] = &[
    "title",
    "h1",
    "h2",
    "h3",
    "nav a",
    ".breadcrumb a",
    ".sidebar a",
    ".menu a",
    "meta[name='keywords']",
    "meta[name='description']",
];

/// Common stop words to filter out.
const STOP_WORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "from",
    "as", "is", "was", "are", "were", "been", "be", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "must", "shall", "can", "this", "that",
    "these", "those", "it", "its", "they", "their", "we", "our", "you", "your", "he", "she", "him",
    "her", "his", "all", "each", "every", "both", "few", "more", "most", "other", "some", "such",
    "no", "not", "only", "same", "so", "than", "too", "very", "just", "also", "now", "here",
    "there", "when", "where", "why", "how", "what", "which", "who", "whom", "about", "after",
    "before", "between", "into", "through", "during", "above", "below", "up", "down", "out", "off",
    "over", "under", "again", "further", "then", "once", "home", "contact", "about", "us", "menu",
    "search", "site", "page", "click", "here", "more", "read", "view", "see", "skip", "main",
    "content", "navigation", "footer", "header", "sidebar",
];

/// Template-based term extractor.
pub struct TemplateTermExtractor {
    /// CSS selectors to use.
    selectors: Vec<String>,
    /// Minimum term length.
    min_length: usize,
    /// Maximum number of terms to extract.
    max_terms: usize,
}

impl TemplateTermExtractor {
    /// Create a new template extractor with custom selectors.
    pub fn new(selectors: Vec<String>) -> Self {
        Self {
            selectors,
            min_length: 3,
            max_terms: 100,
        }
    }

    /// Create with default selectors.
    pub fn with_defaults() -> Self {
        Self {
            selectors: DEFAULT_SELECTORS.iter().map(|s| s.to_string()).collect(),
            min_length: 3,
            max_terms: 100,
        }
    }

    /// Set minimum term length.
    pub fn min_length(mut self, len: usize) -> Self {
        self.min_length = len;
        self
    }

    /// Set maximum terms to extract.
    pub fn max_terms(mut self, max: usize) -> Self {
        self.max_terms = max;
        self
    }

    /// Get effective selectors (custom or defaults).
    fn effective_selectors(&self) -> Vec<&str> {
        if self.selectors.is_empty() {
            DEFAULT_SELECTORS.to_vec()
        } else {
            self.selectors.iter().map(|s| s.as_str()).collect()
        }
    }

    /// Extract text from HTML using selectors.
    fn extract_text(&self, html: &str) -> Vec<String> {
        let document = Html::parse_document(html);
        let mut texts = Vec::new();

        for selector_str in self.effective_selectors() {
            // Handle meta tags specially
            if selector_str.starts_with("meta[") {
                if let Ok(selector) = Selector::parse(selector_str) {
                    for element in document.select(&selector) {
                        if let Some(content) = element.value().attr("content") {
                            texts.push(content.to_string());
                        }
                    }
                }
            } else if let Ok(selector) = Selector::parse(selector_str) {
                for element in document.select(&selector) {
                    let text: String = element.text().collect();
                    let text = text.trim();
                    if !text.is_empty() {
                        texts.push(text.to_string());
                    }
                }
            }
        }

        texts
    }

    /// Tokenize text into terms.
    fn tokenize(&self, text: &str) -> Vec<String> {
        // Split on whitespace and punctuation
        text.split(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == '|' || c == '/')
            .map(|s| {
                // Remove surrounding punctuation
                s.trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
                    .to_lowercase()
            })
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Filter out stop words and short terms.
    fn filter_terms(&self, terms: Vec<String>) -> Vec<String> {
        let stop_set: HashSet<&str> = STOP_WORDS.iter().copied().collect();

        terms
            .into_iter()
            .filter(|term| {
                term.len() >= self.min_length
                    && !stop_set.contains(term.as_str())
                    && !term.chars().all(|c| c.is_numeric())
            })
            .collect()
    }

    /// Count term frequencies and return top terms.
    fn top_terms(&self, terms: Vec<String>) -> Vec<String> {
        use std::collections::HashMap;

        let mut counts: HashMap<String, usize> = HashMap::new();
        for term in terms {
            *counts.entry(term).or_insert(0) += 1;
        }

        let mut sorted: Vec<_> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));

        sorted
            .into_iter()
            .take(self.max_terms)
            .map(|(term, _)| term)
            .collect()
    }
}

impl Default for TemplateTermExtractor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[async_trait]
impl TermExtractor for TemplateTermExtractor {
    fn name(&self) -> &str {
        "template"
    }

    async fn extract_terms(
        &self,
        _seed_terms: &[String],
        context: &ExtractionContext,
    ) -> Result<Vec<String>, DiscoveryError> {
        let html = match &context.html_content {
            Some(h) => h,
            None => {
                return Err(DiscoveryError::Config(
                    "Template extraction requires HTML content".to_string(),
                ))
            }
        };

        // Extract text from HTML
        let texts = self.extract_text(html);

        // Tokenize all extracted text
        let mut all_terms = Vec::new();
        for text in texts {
            all_terms.extend(self.tokenize(&text));
        }

        // Filter and dedupe
        let filtered = self.filter_terms(all_terms);
        let top = self.top_terms(filtered);

        debug!("Template extraction found {} terms", top.len());

        Ok(top)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_from_simple_html() {
        let extractor = TemplateTermExtractor::with_defaults();
        let html = r#"
            <html>
            <head>
                <title>FBI FOIA Reading Room</title>
                <meta name="keywords" content="fbi, foia, documents, declassified">
            </head>
            <body>
                <h1>Electronic Reading Room</h1>
                <nav>
                    <a href="/reports">Reports</a>
                    <a href="/investigations">Investigations</a>
                </nav>
            </body>
            </html>
        "#;

        let texts = extractor.extract_text(html);

        assert!(texts.iter().any(|t| t.contains("FBI")));
        assert!(texts.iter().any(|t| t.contains("Reading Room")));
        assert!(texts.iter().any(|t| t.contains("fbi, foia")));
    }

    #[test]
    fn tokenize_text() {
        let extractor = TemplateTermExtractor::with_defaults();

        let tokens = extractor.tokenize("FBI FOIA Documents, Reports | Investigations");

        assert!(tokens.contains(&"fbi".to_string()));
        assert!(tokens.contains(&"foia".to_string()));
        assert!(tokens.contains(&"documents".to_string()));
        assert!(tokens.contains(&"reports".to_string()));
    }

    #[test]
    fn filter_stop_words() {
        let extractor = TemplateTermExtractor::with_defaults();

        let terms = vec![
            "the".to_string(),
            "fbi".to_string(),
            "and".to_string(),
            "foia".to_string(),
            "a".to_string(),
        ];

        let filtered = extractor.filter_terms(terms);

        assert!(filtered.contains(&"fbi".to_string()));
        assert!(filtered.contains(&"foia".to_string()));
        assert!(!filtered.contains(&"the".to_string()));
        assert!(!filtered.contains(&"and".to_string()));
    }

    #[test]
    fn filter_short_terms() {
        let extractor = TemplateTermExtractor::with_defaults().min_length(3);

        let terms = vec!["ab".to_string(), "abc".to_string(), "abcd".to_string()];
        let filtered = extractor.filter_terms(terms);

        assert!(!filtered.contains(&"ab".to_string()));
        assert!(filtered.contains(&"abc".to_string()));
        assert!(filtered.contains(&"abcd".to_string()));
    }

    #[tokio::test]
    async fn full_extraction() {
        let extractor = TemplateTermExtractor::with_defaults();
        let context = ExtractionContext::for_domain("fbi.gov").with_html(
            r#"
            <html>
            <head><title>FBI FOIA Reading Room</title></head>
            <body>
                <h1>Declassified Documents</h1>
                <h2>Recent Publications</h2>
                <nav>
                    <a href="/vault">The Vault</a>
                    <a href="/reports">Reports</a>
                </nav>
            </body>
            </html>
        "#,
        );

        let terms = extractor.extract_terms(&[], &context).await.unwrap();

        assert!(!terms.is_empty());
        assert!(terms.iter().any(|t| t.contains("fbi") || t.contains("foia")));
    }
}
