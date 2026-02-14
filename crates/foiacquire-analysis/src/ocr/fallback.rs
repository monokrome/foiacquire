//! Fallback OCR backend that tries multiple backends in sequence.
//!
//! When a backend fails due to rate limiting, the next backend in the
//! chain is tried. This allows using fast cloud APIs (Groq, Gemini)
//! with automatic fallback to local OCR (Tesseract) when limits are hit.

use std::path::Path;
use std::sync::Arc;

use tracing::{debug, info, warn};

use super::backend::{BackendConfig, OcrBackend, OcrBackendType, OcrError, OcrResult};
use super::deepseek::DeepSeekBackend;
use super::gemini::GeminiBackend;
use super::groq::GroqBackend;
use super::tesseract::TesseractBackend;

#[cfg(feature = "ocr-ocrs")]
use super::ocrs_backend::OcrsBackend;
#[cfg(feature = "ocr-paddle")]
use super::paddle_backend::PaddleBackend;

/// A fallback chain of OCR backends.
///
/// Tries each backend in order until one succeeds.
/// On rate limit errors, automatically falls back to the next backend.
pub struct FallbackOcrBackend {
    /// Ordered list of backends to try.
    backends: Vec<Arc<dyn OcrBackend>>,
}

impl FallbackOcrBackend {
    /// Create a new fallback backend from a list of backend names.
    ///
    /// # Arguments
    /// * `backend_names` - Ordered list of backend names to try (e.g., ["groq", "gemini"])
    /// * `config` - Backend configuration (OCR settings, privacy, etc.)
    pub fn from_names(backend_names: &[&str], config: BackendConfig) -> Self {
        let mut backends: Vec<Arc<dyn OcrBackend>> = Vec::new();

        for name in backend_names {
            if let Some(backend) = Self::create_backend(name, &config) {
                if backend.is_available() {
                    debug!("OCR fallback chain: added {} backend", name);
                    backends.push(backend);
                } else {
                    debug!(
                        "OCR fallback chain: {} not available ({})",
                        name,
                        backend.availability_hint()
                    );
                }
            } else {
                warn!("OCR fallback chain: unknown backend '{}'", name);
            }
        }

        // If no backends available, try tesseract as last resort
        if backends.is_empty() {
            let tesseract = Arc::new(TesseractBackend::from_backend_config(config));
            if tesseract.is_available() {
                backends.push(tesseract);
            }
        }

        info!(
            "OCR fallback chain initialized with {} backends",
            backends.len()
        );

        Self { backends }
    }

    /// Create a fallback backend for a single backend (no fallback).
    #[allow(dead_code)]
    pub fn single(backend_name: &str, config: BackendConfig) -> Self {
        Self::from_names(&[backend_name], config)
    }

    /// Create a backend by name.
    fn create_backend(name: &str, config: &BackendConfig) -> Option<Arc<dyn OcrBackend>> {
        match name.to_lowercase().as_str() {
            "tesseract" => Some(Arc::new(TesseractBackend::from_backend_config(
                config.clone(),
            ))),
            "groq" => Some(Arc::new(GroqBackend::from_backend_config(config.clone()))),
            "gemini" => Some(Arc::new(GeminiBackend::from_backend_config(config.clone()))),
            "deepseek" => Some(Arc::new(DeepSeekBackend::from_backend_config(
                config.clone(),
            ))),
            #[cfg(feature = "ocr-ocrs")]
            "ocrs" => Some(Arc::new(OcrsBackend::from_backend_config(config.clone()))),
            #[cfg(feature = "ocr-paddle")]
            "paddleocr" | "paddle" => {
                Some(Arc::new(PaddleBackend::from_backend_config(config.clone())))
            }
            _ => None,
        }
    }

    /// Check if a named backend is available (has required binaries/API keys).
    pub fn check_backend_available(name: &str) -> bool {
        let config = BackendConfig::default();
        Self::create_backend(name, &config)
            .map(|b| b.is_available())
            .unwrap_or(false)
    }

    /// Get the list of available backend types in the chain.
    #[allow(dead_code)]
    pub fn available_backends(&self) -> Vec<OcrBackendType> {
        self.backends.iter().map(|b| b.backend_type()).collect()
    }

    /// Check if the chain has any available backends.
    #[allow(dead_code)]
    pub fn has_backends(&self) -> bool {
        !self.backends.is_empty()
    }

    /// Run OCR with fallback chain.
    fn run_with_fallback<F>(&self, operation: F) -> Result<OcrResult, OcrError>
    where
        F: Fn(&dyn OcrBackend) -> Result<OcrResult, OcrError>,
    {
        let mut last_error: Option<OcrError> = None;

        for backend in &self.backends {
            match operation(backend.as_ref()) {
                Ok(result) => {
                    debug!("OCR succeeded with {} backend", backend.backend_type());
                    return Ok(result);
                }
                Err(OcrError::RateLimited {
                    backend: b,
                    retry_after_secs,
                }) => {
                    warn!(
                        "OCR backend {} rate limited (retry after {:?}s), trying next",
                        b, retry_after_secs
                    );
                    last_error = Some(OcrError::RateLimited {
                        backend: b,
                        retry_after_secs,
                    });
                    continue;
                }
                Err(e) => {
                    warn!("OCR backend {} failed: {}", backend.backend_type(), e);
                    last_error = Some(e);
                    // For non-rate-limit errors, still try next backend
                    continue;
                }
            }
        }

        // All backends failed
        Err(last_error.unwrap_or_else(|| {
            OcrError::BackendNotAvailable("No OCR backends available".to_string())
        }))
    }
}

impl OcrBackend for FallbackOcrBackend {
    fn backend_type(&self) -> OcrBackendType {
        // Return the type of the first backend in the chain
        self.backends
            .first()
            .map(|b| b.backend_type())
            .unwrap_or(OcrBackendType::Tesseract)
    }

    fn is_available(&self) -> bool {
        self.backends.iter().any(|b| b.is_available())
    }

    fn availability_hint(&self) -> String {
        if self.backends.is_empty() {
            "No OCR backends configured or available".to_string()
        } else {
            format!(
                "Fallback chain: {}",
                self.backends
                    .iter()
                    .map(|b| b.backend_type().to_string())
                    .collect::<Vec<_>>()
                    .join(" -> ")
            )
        }
    }

    fn run_ocr(&self, image_path: &Path) -> Result<String, OcrError> {
        self.ocr_image(image_path).map(|r| r.text)
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        self.run_with_fallback(|backend| backend.ocr_image(image_path))
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        self.run_with_fallback(|backend| backend.ocr_pdf_page(pdf_path, page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_config_defaults_to_tesseract() {
        let backend = FallbackOcrBackend::from_names(&[], BackendConfig::default());
        // Should have at least tesseract if available
        assert!(backend.backends.len() <= 1);
    }

    #[test]
    fn test_unknown_backend_ignored() {
        let backend = FallbackOcrBackend::from_names(
            &["unknown_backend", "tesseract"],
            BackendConfig::default(),
        );
        // Unknown backend should be skipped
        for b in &backend.backends {
            assert_ne!(b.backend_type().as_str(), "unknown_backend");
        }
    }

    #[test]
    fn test_single_backend() {
        let backend = FallbackOcrBackend::single("tesseract", BackendConfig::default());
        assert!(backend.backends.len() <= 1);
    }
}
