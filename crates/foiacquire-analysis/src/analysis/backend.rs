//! Analysis backend abstraction for text extraction from documents.
//!
//! Supports multiple analysis types:
//! - OCR: Page-level text extraction from images/PDFs
//! - Whisper: Document-level audio/video transcription
//! - Custom: User-defined commands per mimetype

use std::path::Path;
use thiserror::Error;

use crate::ocr::OcrError;

/// Errors from analysis backends.
#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("Backend not available: {0}")]
    BackendNotAvailable(String),

    #[error("Analysis failed: {0}")]
    AnalysisFailed(String),

    #[error("Unsupported mimetype: {0}")]
    UnsupportedMimetype(String),

    #[error("Command execution failed: {0}")]
    CommandFailed(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<OcrError> for AnalysisError {
    fn from(err: OcrError) -> Self {
        match err {
            OcrError::BackendNotAvailable(msg) => AnalysisError::BackendNotAvailable(msg),
            OcrError::Io(e) => AnalysisError::Io(e),
            OcrError::OcrFailed(msg) => AnalysisError::AnalysisFailed(msg),
            OcrError::RateLimited {
                backend,
                retry_after_secs,
            } => AnalysisError::AnalysisFailed(format!(
                "Rate limited by {}, retry after {:?}s",
                backend, retry_after_secs
            )),
            OcrError::ModelNotFound(msg) => AnalysisError::BackendNotAvailable(msg),
            OcrError::ImageError(msg) => AnalysisError::AnalysisFailed(msg),
        }
    }
}

/// Analysis granularity - determines how results are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisGranularity {
    /// Page-level analysis (OCR) - results stored per page
    Page,
    /// Document-level analysis (Whisper, custom) - results stored per document
    Document,
}

/// Result of analysis processing.
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Extracted text content.
    pub text: String,
    /// Confidence score (0.0 - 1.0), if available.
    pub confidence: Option<f32>,
    /// Which backend produced this result.
    pub backend: String,
    /// Which model was used (e.g., "gemini-1.5-flash", "llama-4-scout-17b").
    pub model: Option<String>,
    /// Processing time in milliseconds.
    pub processing_time_ms: u64,
    /// Additional metadata (language detected, etc.)
    pub metadata: Option<serde_json::Value>,
}

/// Available analysis types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AnalysisType {
    /// OCR text extraction
    Ocr,
    /// Audio/video transcription via Whisper
    Whisper,
    /// Custom command-based analysis
    Custom(String),
}

impl AnalysisType {
    /// Convert to string representation for storage.
    pub fn as_str(&self) -> String {
        match self {
            AnalysisType::Ocr => "ocr".to_string(),
            AnalysisType::Whisper => "whisper".to_string(),
            AnalysisType::Custom(name) => format!("custom:{}", name),
        }
    }

    /// Parse from string representation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "ocr" => Some(AnalysisType::Ocr),
            "whisper" => Some(AnalysisType::Whisper),
            s if s.starts_with("custom:") => {
                Some(AnalysisType::Custom(s.strip_prefix("custom:")?.to_string()))
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for AnalysisType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Trait for analysis backends (OCR, Whisper, custom commands).
pub trait AnalysisBackend: Send + Sync {
    /// Get the analysis type this backend provides.
    fn analysis_type(&self) -> AnalysisType;

    /// Get the backend identifier (e.g., "tesseract", "whisper-base", "my-extractor").
    fn backend_id(&self) -> &str;

    /// Check if this backend is available (dependencies installed, models present).
    fn is_available(&self) -> bool;

    /// Get a description of what's needed to make this backend available.
    fn availability_hint(&self) -> String;

    /// Get the analysis granularity (page-level or document-level).
    fn granularity(&self) -> AnalysisGranularity;

    /// Check if this backend supports the given mimetype.
    fn supports_mimetype(&self, mimetype: &str) -> bool;

    /// Analyze an entire file (for document-level analysis like Whisper).
    /// Returns error for page-level backends.
    fn analyze_file(&self, file_path: &Path) -> Result<AnalysisResult, AnalysisError>;

    /// Analyze a specific page of a PDF (for page-level analysis like OCR).
    /// Returns error for document-level backends.
    fn analyze_page(&self, file_path: &Path, page: u32) -> Result<AnalysisResult, AnalysisError>;

    /// Analyze an image file directly (for OCR backends).
    fn analyze_image(&self, _image_path: &Path) -> Result<AnalysisResult, AnalysisError> {
        Err(AnalysisError::UnsupportedOperation(
            "Image analysis not supported by this backend".to_string(),
        ))
    }
}

/// Check if a mimetype matches a pattern (supports wildcards like "audio/*").
pub fn mimetype_matches(pattern: &str, mimetype: &str) -> bool {
    if pattern == "*" || pattern == "*/*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("/*") {
        mimetype.starts_with(prefix) && mimetype.contains('/')
    } else {
        pattern == mimetype
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analysis_type_roundtrip() {
        let types = vec![
            AnalysisType::Ocr,
            AnalysisType::Whisper,
            AnalysisType::Custom("my-extractor".to_string()),
        ];

        for t in types {
            let s = t.as_str();
            let parsed = AnalysisType::from_str(&s).unwrap();
            assert_eq!(t, parsed);
        }
    }

    #[test]
    fn test_mimetype_matches() {
        assert!(mimetype_matches("audio/*", "audio/mp3"));
        assert!(mimetype_matches("audio/*", "audio/wav"));
        assert!(!mimetype_matches("audio/*", "video/mp4"));
        assert!(mimetype_matches("video/*", "video/mp4"));
        assert!(mimetype_matches("application/pdf", "application/pdf"));
        assert!(!mimetype_matches("application/pdf", "application/json"));
        assert!(mimetype_matches("*/*", "anything/here"));
        assert!(mimetype_matches("*", "anything/here"));
    }
}
