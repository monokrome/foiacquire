//! URL extraction annotator — wraps `UrlFinder` behind the `Annotator` trait.

use async_trait::async_trait;

use crate::models::Document;
use crate::ocr::UrlFinder;
use crate::repository::DieselDocumentRepository;

use super::annotator::Annotator;
use super::types::{AnnotationError, AnnotationOutput};

/// Annotator that extracts document-like URLs from OCR text.
#[allow(dead_code)]
pub struct UrlAnnotator {
    finder: UrlFinder,
}

#[allow(dead_code)]
impl UrlAnnotator {
    pub fn new() -> Self {
        Self {
            finder: UrlFinder::new(),
        }
    }
}

impl Default for UrlAnnotator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Annotator for UrlAnnotator {
    fn annotation_type(&self) -> &str {
        "url_extraction"
    }

    fn display_name(&self) -> &str {
        "URL Extraction"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn is_available(&self) -> bool {
        true
    }

    fn availability_hint(&self) -> String {
        String::new()
    }

    async fn annotate(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
    ) -> Result<AnnotationOutput, AnnotationError> {
        let version_id = match doc.current_version() {
            Some(v) => v.id,
            None => return Ok(AnnotationOutput::Skipped),
        };

        let text = match doc_repo
            .get_combined_page_text(&doc.id, version_id as i32)
            .await
        {
            Ok(Some(t)) if !t.is_empty() => t,
            _ => return Ok(AnnotationOutput::Skipped),
        };

        let urls = self.finder.find_document_urls(&text);
        if urls.is_empty() {
            return Ok(AnnotationOutput::NoResult);
        }

        let url_strings: Vec<&str> = urls.iter().map(|u| u.url.as_str()).collect();
        let data = serde_json::json!({
            "urls": url_strings,
            "count": urls.len(),
        });

        Ok(AnnotationOutput::Data(data.to_string()))
    }
}
