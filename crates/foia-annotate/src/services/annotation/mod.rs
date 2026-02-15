//! Annotation pipeline — trait-based abstraction for document annotation backends.
//!
//! Each backend (LLM summarization, date detection, URL extraction) implements
//! the `Annotator` trait. The `AnnotationManager` provides a single batch loop
//! that works with any annotator.

mod annotator;
mod date_annotator;
mod llm_annotator;
mod manager;
mod ner_annotator;
mod types;
mod url_annotator;

pub use annotator::{get_document_text, Annotator};
pub use date_annotator::DateAnnotator;
pub use llm_annotator::LlmAnnotator;
pub use manager::AnnotationManager;
pub use ner_annotator::NerAnnotator;
pub use types::{AnnotationError, AnnotationEvent, AnnotationOutput, BatchAnnotationResult};
pub use url_annotator::UrlAnnotator;
