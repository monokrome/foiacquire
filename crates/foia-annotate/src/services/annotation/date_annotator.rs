//! Date detection annotator — wraps `detect_date()` behind the `Annotator` trait.

use async_trait::async_trait;

use crate::services::date_detection::detect_date;
use foia::models::Document;
use foia::repository::DieselDocumentRepository;

use super::annotator::Annotator;
use super::types::{AnnotationError, AnnotationOutput};

/// Annotator that estimates document publication dates from metadata signals
/// (server headers, filename patterns, URL paths).
pub struct DateAnnotator {
    dry_run: bool,
}

impl DateAnnotator {
    pub fn new(dry_run: bool) -> Self {
        Self { dry_run }
    }
}

#[async_trait]
impl Annotator for DateAnnotator {
    fn annotation_type(&self) -> &str {
        "date_detection"
    }

    fn display_name(&self) -> &str {
        "Date Detection"
    }

    async fn annotate(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
    ) -> Result<AnnotationOutput, AnnotationError> {
        let version = doc.current_version();
        let filename = version.and_then(|v| v.original_filename.clone());
        let server_date = version.and_then(|v| v.server_date);
        let acquired_at = version.map(|v| v.acquired_at).unwrap_or(doc.created_at);
        let source_url = Some(doc.source_url.clone());

        let estimate = detect_date(
            server_date,
            acquired_at,
            filename.as_deref(),
            source_url.as_deref(),
        );

        match estimate {
            Some(est) => {
                if !self.dry_run {
                    doc_repo
                        .update_estimated_date(
                            &doc.id,
                            est.date,
                            est.confidence.as_str(),
                            est.source.as_str(),
                        )
                        .await
                        .map_err(|e| AnnotationError::Database(e.to_string()))?;
                }
                Ok(AnnotationOutput::Data(format!(
                    "detected:{}",
                    est.source.as_str()
                )))
            }
            None => Ok(AnnotationOutput::NoResult),
        }
    }
}
