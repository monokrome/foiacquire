//! Service layer for FOIAcquire business logic.
//!
//! This module contains domain logic separated from UI concerns.
//! Services can be used by CLI, web server, or other interfaces.

pub mod analysis;
pub mod annotation;
pub mod date_detection;
pub mod download;
pub mod geolookup;
pub mod ner;
pub mod youtube;

#[allow(unused_imports)]
pub use analysis::{AnalysisEvent, AnalysisResult, AnalysisService};
#[allow(unused_imports)]
pub use annotation::{
    AnnotationError, AnnotationEvent, AnnotationManager, AnnotationOutput, BatchAnnotationResult,
    DateAnnotator, LlmAnnotator, NerAnnotator, UrlAnnotator,
};
#[allow(unused_imports)]
pub use date_detection::{detect_date, DateConfidence, DateEstimate, DateSource};
#[allow(unused_imports)]
pub use download::{DownloadConfig, DownloadEvent, DownloadResult, DownloadService};
#[allow(unused_imports)]
pub use ner::{NerBackend, NerResult, RegexNerBackend};
