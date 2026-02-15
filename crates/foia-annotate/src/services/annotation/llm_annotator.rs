//! LLM summarization annotator — wraps `LlmClient::summarize()` behind the `Annotator` trait.

use async_trait::async_trait;

use foia::llm::{LlmClient, LlmConfig};
use foia::models::{Document, DocumentStatus};
use foia::repository::DieselDocumentRepository;

use super::annotator::{get_document_text, Annotator};
use super::types::{AnnotationError, AnnotationOutput};

/// Annotator that generates synopses and tags via an LLM service.
///
/// Unlike simpler annotators, this one also updates the document's
/// `synopsis`, `tags`, and `status` fields (setting status to `Indexed`).
pub struct LlmAnnotator {
    llm_client: LlmClient,
    config: LlmConfig,
}

impl LlmAnnotator {
    pub fn new(config: LlmConfig) -> Self {
        let llm_client = LlmClient::new(config.clone());
        Self { llm_client, config }
    }

    /// Get the underlying LLM config (for display in CLI).
    pub fn llm_config(&self) -> &LlmConfig {
        &self.config
    }
}

#[async_trait]
impl Annotator for LlmAnnotator {
    fn annotation_type(&self) -> &str {
        "llm_summary"
    }

    fn display_name(&self) -> &str {
        "LLM Summarization"
    }

    async fn is_available(&self) -> bool {
        self.llm_client.is_available().await
    }

    fn availability_hint(&self) -> String {
        self.config.availability_hint()
    }

    async fn annotate(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
    ) -> Result<AnnotationOutput, AnnotationError> {
        let text = match get_document_text(doc, doc_repo).await {
            Ok(t) => t,
            Err(output) => return Ok(output),
        };

        let result = self
            .llm_client
            .summarize(&text, &doc.title)
            .await
            .map_err(|e| AnnotationError::Failed(e.to_string()))?;

        // Update document with synopsis, tags, and status
        let mut updated_doc = doc.clone();
        updated_doc.synopsis = Some(result.synopsis.clone());
        updated_doc.tags = result.tags.clone();
        updated_doc.status = DocumentStatus::Indexed;
        updated_doc.updated_at = chrono::Utc::now();

        doc_repo
            .save(&updated_doc)
            .await
            .map_err(|e| AnnotationError::Database(format!("Save failed: {}", e)))?;

        let data = serde_json::json!({
            "synopsis_len": result.synopsis.len(),
            "tag_count": result.tags.len(),
        });

        Ok(AnnotationOutput::Data(data.to_string()))
    }
}
