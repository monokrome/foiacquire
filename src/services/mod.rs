//! Service layer for FOIAcquire business logic.
//!
//! This module contains domain logic separated from UI concerns.
//! Services can be used by CLI, web server, or other interfaces.

pub mod annotate;
pub mod date_detection;
pub mod download;
pub mod ocr;

#[allow(unused_imports)]
pub use annotate::{AnnotationEvent, AnnotationResult, AnnotationService};
#[allow(unused_imports)]
pub use date_detection::{detect_date, DateConfidence, DateEstimate, DateSource};
#[allow(unused_imports)]
pub use download::{DownloadConfig, DownloadEvent, DownloadResult, DownloadService};
#[allow(unused_imports)]
pub use ocr::{OcrEvent, OcrResult, OcrService};
