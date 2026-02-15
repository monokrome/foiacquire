//! LLM-based term expansion.
//!
//! Wraps the existing LlmClient to expand search terms.

use async_trait::async_trait;
use tracing::{debug, warn};

use super::{ExtractionContext, TermExtractor};
use crate::discovery::DiscoveryError;
use foia::llm::{LlmClient, LlmConfig};

/// LLM-based term extractor.
///
/// Uses the existing LlmClient::expand_search_terms() to generate
/// related search terms from seed terms.
pub struct LlmTermExtractor {
    /// Maximum number of terms to generate.
    max_terms: usize,
}

impl LlmTermExtractor {
    /// Create a new LLM term extractor.
    pub fn new() -> Self {
        Self { max_terms: 50 }
    }

    /// Set maximum terms to generate.
    pub fn max_terms(mut self, max: usize) -> Self {
        self.max_terms = max;
        self
    }
}

impl Default for LlmTermExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TermExtractor for LlmTermExtractor {
    fn name(&self) -> &str {
        "llm"
    }

    async fn extract_terms(
        &self,
        seed_terms: &[String],
        context: &ExtractionContext,
    ) -> Result<Vec<String>, DiscoveryError> {
        if seed_terms.is_empty() {
            return Ok(Vec::new());
        }

        // Build domain description for LLM context
        let domain_desc = context
            .domain_description
            .clone()
            .unwrap_or_else(|| format!("Documents from {}", context.domain));

        debug!(
            "Expanding {} seed terms with LLM for {}",
            seed_terms.len(),
            context.domain
        );

        let client = LlmClient::new(LlmConfig::default());
        match client.expand_search_terms(seed_terms, &domain_desc).await {
            Ok(terms) => {
                let mut terms = terms;
                terms.truncate(self.max_terms);
                debug!("LLM expanded to {} terms", terms.len());
                Ok(terms)
            }
            Err(e) => {
                warn!("LLM term expansion failed: {}", e);
                // Return seed terms on failure
                Ok(seed_terms.to_vec())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn llm_extractor_default() {
        let extractor = LlmTermExtractor::new();
        assert_eq!(extractor.max_terms, 50);
    }

    #[test]
    fn llm_extractor_max_terms() {
        let extractor = LlmTermExtractor::new().max_terms(100);
        assert_eq!(extractor.max_terms, 100);
    }

    #[tokio::test]
    async fn llm_extractor_empty_seeds() {
        let extractor = LlmTermExtractor::new();
        let context = ExtractionContext::for_domain("example.gov");

        let terms = extractor.extract_terms(&[], &context).await.unwrap();
        assert!(terms.is_empty());
    }
}
