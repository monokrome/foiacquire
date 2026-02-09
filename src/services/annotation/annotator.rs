//! Annotator trait — shared abstraction for annotation backends.

use async_trait::async_trait;

use crate::models::Document;
use crate::repository::DieselDocumentRepository;

use super::types::{AnnotationError, AnnotationOutput};

/// A backend that can annotate documents.
///
/// Implementations wrap a specific analysis (LLM summarization, date detection,
/// URL extraction) and expose it through a uniform interface so the
/// `AnnotationManager` can orchestrate them identically.
#[async_trait]
pub trait Annotator: Send + Sync {
    /// Key stored in `metadata.annotations[type]` via `record_annotation`.
    fn annotation_type(&self) -> &str;

    /// Human-readable name for CLI progress output.
    fn display_name(&self) -> &str;

    /// Schema version of this annotator's output.
    /// Bumping the version causes documents to be re-annotated.
    fn version(&self) -> i32;

    /// Whether the backend is ready to run.
    /// LLM checks service availability; date/URL always return true.
    async fn is_available(&self) -> bool;

    /// Human-readable reason when `is_available` returns false.
    fn availability_hint(&self) -> String;

    /// Annotate a single document.
    async fn annotate(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
    ) -> Result<AnnotationOutput, AnnotationError>;

    /// Post-processing hook called after annotation data is recorded.
    /// Used by NerAnnotator to populate the document_entities table.
    /// Default implementation is a no-op.
    async fn post_record(
        &self,
        _doc: &Document,
        _doc_repo: &DieselDocumentRepository,
        _output: &AnnotationOutput,
    ) -> Result<(), AnnotationError> {
        Ok(())
    }
}
