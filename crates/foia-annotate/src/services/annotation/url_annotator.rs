//! URL extraction annotator — wraps `UrlFinder` behind the `Annotator` trait.

use async_trait::async_trait;

use foia::models::Document;
use foia::repository::DieselDocumentRepository;
use foia::utils::UrlFinder;

use super::annotator::{get_document_text, Annotator};
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

    async fn annotate(
        &self,
        doc: &Document,
        doc_repo: &DieselDocumentRepository,
    ) -> Result<AnnotationOutput, AnnotationError> {
        let text = match get_document_text(doc, doc_repo).await {
            Ok(t) => t,
            Err(output) => return Ok(output),
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
