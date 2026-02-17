//! Adapter to wrap OcrBackend as AnalysisBackend.
//!
//! This allows existing OCR backends to be used with the unified analysis system.

use std::path::Path;
use std::sync::Arc;

use super::backend::{
    AnalysisBackend, AnalysisError, AnalysisGranularity, AnalysisResult, AnalysisType,
};
use crate::ocr::{OcrBackend, OcrBackendType};

/// Wraps an OcrBackend to implement AnalysisBackend.
pub struct OcrAnalysisAdapter {
    backend: Arc<dyn OcrBackend>,
}

impl OcrAnalysisAdapter {
    /// Create a new adapter wrapping an OCR backend.
    pub fn new<B: OcrBackend + 'static>(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    /// Create from an Arc'd backend.
    pub fn from_arc(backend: Arc<dyn OcrBackend>) -> Self {
        Self { backend }
    }

    /// Convert OcrResult to AnalysisResult.
    fn ocr_to_analysis(&self, result: crate::ocr::OcrResult) -> AnalysisResult {
        AnalysisResult {
            text: result.text,
            confidence: result.confidence,
            backend: self.backend_id().to_string(),
            model: result.model,
            processing_time_ms: result.processing_time_ms,
            metadata: None,
        }
    }
}

impl AnalysisBackend for OcrAnalysisAdapter {
    fn analysis_type(&self) -> AnalysisType {
        AnalysisType::Ocr
    }

    fn is_deferred(&self) -> bool {
        self.backend.is_deferred()
    }

    fn backend_id(&self) -> &str {
        match self.backend.backend_type() {
            OcrBackendType::Tesseract => "tesseract",
            OcrBackendType::Ocrs => "ocrs",
            OcrBackendType::PaddleOcr => "paddleocr",
            OcrBackendType::DeepSeek => "deepseek",
            OcrBackendType::Gemini => "gemini",
            OcrBackendType::Groq => "groq",
        }
    }

    fn is_available(&self) -> bool {
        self.backend.is_available()
    }

    fn availability_hint(&self) -> String {
        self.backend.availability_hint()
    }

    fn granularity(&self) -> AnalysisGranularity {
        AnalysisGranularity::Page
    }

    fn supports_mimetype(&self, mimetype: &str) -> bool {
        matches!(
            mimetype,
            "application/pdf"
                | "image/png"
                | "image/jpeg"
                | "image/jpg"
                | "image/tiff"
                | "image/gif"
                | "image/bmp"
                | "image/webp"
        )
    }

    fn analyze_file(&self, _file_path: &Path) -> Result<AnalysisResult, AnalysisError> {
        Err(AnalysisError::UnsupportedOperation(
            "OCR requires page-level analysis. Use analyze_page() instead.".to_string(),
        ))
    }

    fn analyze_page(&self, file_path: &Path, page: u32) -> Result<AnalysisResult, AnalysisError> {
        let result = self.backend.ocr_pdf_page(file_path, page)?;
        Ok(self.ocr_to_analysis(result))
    }

    fn analyze_image(&self, image_path: &Path) -> Result<AnalysisResult, AnalysisError> {
        let result = self.backend.ocr_image(image_path)?;
        Ok(self.ocr_to_analysis(result))
    }
}
