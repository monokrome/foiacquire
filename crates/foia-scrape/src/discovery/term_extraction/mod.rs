//! Term extraction for discovery.
//!
//! Extracts search terms from documents and pages to improve discovery.

mod llm;
mod template;

pub use llm::LlmTermExtractor;
pub use template::TemplateTermExtractor;

use async_trait::async_trait;

use crate::discovery::url_utils::dedup_and_limit;
use crate::discovery::DiscoveryError;

/// Context for term extraction.
#[derive(Debug, Clone, Default)]
pub struct ExtractionContext {
    /// Target domain.
    pub domain: String,
    /// Description of the domain for LLM context.
    pub domain_description: Option<String>,
    /// HTML content for template extraction.
    pub html_content: Option<String>,
    /// Number of existing documents (for scaling expansion).
    pub document_count: usize,
}

impl ExtractionContext {
    /// Create a new context for a domain.
    pub fn for_domain(domain: &str) -> Self {
        Self {
            domain: domain.to_string(),
            ..Default::default()
        }
    }

    /// Add a domain description.
    pub fn with_description(mut self, desc: &str) -> Self {
        self.domain_description = Some(desc.to_string());
        self
    }

    /// Add HTML content for template extraction.
    pub fn with_html(mut self, html: &str) -> Self {
        self.html_content = Some(html.to_string());
        self
    }
}

/// Trait for term extraction strategies.
#[async_trait]
pub trait TermExtractor: Send + Sync {
    /// Extract search terms.
    ///
    /// # Arguments
    /// * `seed_terms` - Initial terms to expand from
    /// * `context` - Context for extraction (domain, HTML, etc.)
    ///
    /// # Returns
    /// List of extracted/expanded terms.
    async fn extract_terms(
        &self,
        seed_terms: &[String],
        context: &ExtractionContext,
    ) -> Result<Vec<String>, DiscoveryError>;

    /// Name of this extractor.
    fn name(&self) -> &str;
}

/// Combined extractor that uses multiple strategies.
pub struct CombinedTermExtractor {
    template: Option<TemplateTermExtractor>,
    llm: Option<LlmTermExtractor>,
}

impl CombinedTermExtractor {
    /// Create a new combined extractor.
    pub fn new() -> Self {
        Self {
            template: None,
            llm: None,
        }
    }

    /// Enable template-based extraction.
    pub fn with_template(mut self, selectors: Vec<String>) -> Self {
        self.template = Some(TemplateTermExtractor::new(selectors));
        self
    }

    /// Enable LLM-based expansion.
    pub fn with_llm(mut self, extractor: LlmTermExtractor) -> Self {
        self.llm = Some(extractor);
        self
    }

    /// Check if any extraction is enabled.
    pub fn is_enabled(&self) -> bool {
        self.template.is_some() || self.llm.is_some()
    }
}

impl Default for CombinedTermExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TermExtractor for CombinedTermExtractor {
    fn name(&self) -> &str {
        "combined"
    }

    async fn extract_terms(
        &self,
        seed_terms: &[String],
        context: &ExtractionContext,
    ) -> Result<Vec<String>, DiscoveryError> {
        let mut all_terms: Vec<String> = seed_terms.to_vec();

        // First, extract from templates if HTML is available
        if let Some(ref template) = self.template {
            if context.html_content.is_some() {
                let template_terms = template.extract_terms(seed_terms, context).await?;
                all_terms.extend(template_terms);
            }
        }

        // Then expand with LLM
        if let Some(ref llm) = self.llm {
            let llm_terms = llm.extract_terms(&all_terms, context).await?;
            all_terms.extend(llm_terms);
        }

        dedup_and_limit(&mut all_terms, 0);

        // Remove very short terms
        all_terms.retain(|t| t.len() >= 2);

        Ok(all_terms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extraction_context_builder() {
        let ctx = ExtractionContext::for_domain("fbi.gov")
            .with_description("FBI FOIA Reading Room")
            .with_html("<html>test</html>");

        assert_eq!(ctx.domain, "fbi.gov");
        assert_eq!(
            ctx.domain_description,
            Some("FBI FOIA Reading Room".to_string())
        );
        assert!(ctx.html_content.is_some());
    }

    #[test]
    fn combined_extractor_disabled_by_default() {
        let extractor = CombinedTermExtractor::new();
        assert!(!extractor.is_enabled());
    }
}
