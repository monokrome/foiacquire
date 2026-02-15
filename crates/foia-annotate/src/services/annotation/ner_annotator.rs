//! Named Entity Recognition annotator — wraps a `NerBackend` behind the `Annotator` trait.

use async_trait::async_trait;

use crate::services::ner::{EntityType, NerBackend, NerResult, RegexNerBackend};
use foia::models::Document;
use foia::repository::models::NewDocumentEntity;
use foia::repository::DieselDocumentRepository;
#[cfg(feature = "gis")]
use foia::services::geolookup;

use super::annotator::{get_document_text, Annotator};
use super::types::{AnnotationError, AnnotationOutput};

/// Annotator that extracts named entities from document text.
///
/// Accepts any `NerBackend` implementation. Defaults to `RegexNerBackend`
/// (government/FOIA-tuned pattern matching). Future backends (rust-bert,
/// LLM-based) can be swapped in via `with_backend()`.
pub struct NerAnnotator {
    backend: Box<dyn NerBackend>,
}

impl NerAnnotator {
    pub fn new() -> Self {
        Self {
            backend: Box::new(RegexNerBackend::new()),
        }
    }

    #[allow(dead_code)]
    pub fn with_backend(backend: Box<dyn NerBackend>) -> Self {
        Self { backend }
    }
}

impl Default for NerAnnotator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Annotator for NerAnnotator {
    fn annotation_type(&self) -> &str {
        "ner_extraction"
    }

    fn display_name(&self) -> &str {
        "Named Entity Recognition"
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

        let result = self.backend.extract(&text);

        if result.entities.is_empty() {
            return Ok(AnnotationOutput::NoResult);
        }

        let data =
            serde_json::to_string(&result).map_err(|e| AnnotationError::Failed(e.to_string()))?;

        Ok(AnnotationOutput::Data(data))
    }

    async fn post_record(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
        output: &AnnotationOutput,
    ) -> Result<(), AnnotationError> {
        let data = match output {
            AnnotationOutput::Data(d) => d,
            _ => return Ok(()),
        };

        let ner_result: NerResult = serde_json::from_str(data)
            .map_err(|e| AnnotationError::Failed(format!("Failed to parse NER result: {}", e)))?;

        doc_repo
            .delete_document_entities(&doc.id)
            .await
            .map_err(|e| AnnotationError::Database(e.to_string()))?;

        let now = chrono::Utc::now().to_rfc3339();

        // Pre-compute normalized text to avoid borrowing temporaries
        let normalized: Vec<String> = ner_result
            .entities
            .iter()
            .map(|e| e.text.to_lowercase())
            .collect();

        let entity_rows: Vec<NewDocumentEntity<'_>> = ner_result
            .entities
            .iter()
            .zip(normalized.iter())
            .map(|(entity, norm_text)| {
                let entity_type_str = match entity.entity_type {
                    EntityType::Organization => "organization",
                    EntityType::Person => "person",
                    EntityType::FileNumber => "file_number",
                    EntityType::Location => "location",
                };

                let (latitude, longitude) = if entity.entity_type == EntityType::Location {
                    #[cfg(feature = "gis")]
                    {
                        geolookup::lookup(&entity.text)
                            .map(|(lat, lon)| (Some(lat), Some(lon)))
                            .unwrap_or((None, None))
                    }
                    #[cfg(not(feature = "gis"))]
                    {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                NewDocumentEntity {
                    document_id: &doc.id,
                    entity_type: entity_type_str,
                    entity_text: &entity.text,
                    normalized_text: norm_text,
                    latitude,
                    longitude,
                    created_at: &now,
                }
            })
            .collect();

        doc_repo
            .save_document_entities(&entity_rows)
            .await
            .map_err(|e| AnnotationError::Database(e.to_string()))?;

        Ok(())
    }
}
