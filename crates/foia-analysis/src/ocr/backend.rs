//! OCR backend abstraction for A/B testing different OCR engines.
//!
//! Supports multiple OCR backends:
//! - Tesseract: Traditional OCR via command-line (CPU)
//! - Ocrs: Pure Rust OCR engine (CPU)
//! - PaddleOCR: CNN-based OCR via ONNX Runtime (CPU/GPU)
//! - DeepSeek: LLM-based OCR via subprocess (CPU/GPU)

#![allow(dead_code)]

use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use thiserror::Error;

use foia::http_client::HttpClient;
use foia::privacy::PrivacyConfig;

use super::model_utils::build_ocr_result;
use super::pdf_utils;

/// Errors from OCR backends.
#[derive(Debug, Error)]
pub enum OcrError {
    #[error("Backend not available: {0}")]
    BackendNotAvailable(String),

    #[error("OCR failed: {0}")]
    OcrFailed(String),

    #[error("Rate limited by {backend}, retry after {retry_after_secs:?}s")]
    RateLimited {
        backend: OcrBackendType,
        retry_after_secs: Option<u64>,
    },

    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Image error: {0}")]
    ImageError(String),
}

/// Result of OCR processing.
#[derive(Debug, Clone)]
pub struct OcrResult {
    /// Extracted text content.
    pub text: String,
    /// Confidence score (0.0 - 1.0), if available.
    pub confidence: Option<f32>,
    /// Which backend produced this result.
    pub backend: OcrBackendType,
    /// Which model was used (e.g., "gemini-1.5-flash", "llama-4-scout-17b").
    pub model: Option<String>,
    /// Processing time in milliseconds.
    pub processing_time_ms: u64,
}

/// Available OCR backend types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OcrBackendType {
    /// Tesseract OCR via command-line.
    Tesseract,
    /// Pure Rust OCR engine (ocrs crate).
    Ocrs,
    /// PaddleOCR via ONNX Runtime.
    PaddleOcr,
    /// DeepSeek VLM-based OCR via subprocess.
    DeepSeek,
    /// Google Gemini Vision API.
    Gemini,
    /// Groq Vision API (Llama 4 Scout/Maverick).
    Groq,
}

impl OcrBackendType {
    /// Whether this backend type sends work to a remote API rather than running locally.
    pub fn is_deferred(&self) -> bool {
        matches!(self, OcrBackendType::Gemini | OcrBackendType::Groq)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OcrBackendType::Tesseract => "tesseract",
            OcrBackendType::Ocrs => "ocrs",
            OcrBackendType::PaddleOcr => "paddleocr",
            OcrBackendType::DeepSeek => "deepseek",
            OcrBackendType::Gemini => "gemini",
            OcrBackendType::Groq => "groq",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "tesseract" => Some(OcrBackendType::Tesseract),
            "ocrs" => Some(OcrBackendType::Ocrs),
            "paddleocr" | "paddle" => Some(OcrBackendType::PaddleOcr),
            "deepseek" => Some(OcrBackendType::DeepSeek),
            "gemini" => Some(OcrBackendType::Gemini),
            "groq" => Some(OcrBackendType::Groq),
            _ => None,
        }
    }
}

impl std::fmt::Display for OcrBackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Trait for OCR backends.
pub trait OcrBackend: Send + Sync {
    /// Get the backend type.
    fn backend_type(&self) -> OcrBackendType;

    /// Check if this backend is available (dependencies installed, models present).
    fn is_available(&self) -> bool;

    /// Get a description of what's needed to make this backend available.
    fn availability_hint(&self) -> String;

    /// Core OCR: extract text from an image file.
    fn run_ocr(&self, image_path: &Path) -> Result<String, OcrError>;

    /// Whether this backend sends work to a remote API rather than running locally.
    /// Deferred backends can run concurrently with local stages in deep mode.
    fn is_deferred(&self) -> bool {
        self.backend_type().is_deferred()
    }

    /// Model name for this backend, if applicable.
    fn model_name(&self) -> Option<String> {
        None
    }

    /// Run OCR on an image file, returning a timed result.
    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_ocr(image_path)?;
        Ok(build_ocr_result(
            text,
            self.backend_type(),
            self.model_name(),
            start,
        ))
    }

    /// Run OCR on a specific page of a PDF file.
    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let temp_dir = TempDir::new()?;
        let image_path = pdf_utils::pdf_page_to_image(pdf_path, page, temp_dir.path())?;
        let text = self.run_ocr(&image_path)?;
        Ok(build_ocr_result(
            text,
            self.backend_type(),
            self.model_name(),
            start,
        ))
    }
}

/// Configuration for OCR backends (language, GPU, model paths).
#[derive(Debug, Clone)]
pub struct OcrConfig {
    /// Language for OCR (e.g., "eng", "chi_sim").
    pub language: String,
    /// Path to model files (for backends that need them).
    pub model_path: Option<std::path::PathBuf>,
    /// Whether to use GPU acceleration if available.
    pub use_gpu: bool,
    /// Device ID for GPU (if multiple GPUs).
    pub gpu_device_id: u32,
}

impl Default for OcrConfig {
    fn default() -> Self {
        Self {
            language: "eng".to_string(),
            model_path: None,
            use_gpu: false,
            gpu_device_id: 0,
        }
    }
}

/// Shared base configuration embedded by all OCR backends.
///
/// Bundles the OCR-specific settings with privacy configuration
/// so each backend doesn't need to define them independently.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub ocr: OcrConfig,
    pub privacy: Option<PrivacyConfig>,
}

impl BackendConfig {
    pub fn new() -> Self {
        Self {
            ocr: OcrConfig::default(),
            privacy: None,
        }
    }

    pub fn with_config(config: OcrConfig) -> Self {
        Self {
            ocr: config,
            privacy: None,
        }
    }

    pub fn with_privacy(mut self, privacy: PrivacyConfig) -> Self {
        self.privacy = Some(privacy);
        self
    }

    /// Create an HTTP client, applying privacy settings if configured.
    /// When privacy is None, HttpClient picks up env overrides (SOCKS_PROXY, etc.).
    pub fn create_http_client(&self, service_name: &str) -> Result<HttpClient, OcrError> {
        let mut builder = HttpClient::builder(
            service_name,
            Duration::from_secs(120),
            Duration::from_millis(0),
        );
        if let Some(ref privacy) = self.privacy {
            builder = builder.privacy(privacy);
        }
        builder
            .build()
            .map_err(|e| OcrError::OcrFailed(format!("Failed to create HTTP client: {}", e)))
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Manager for multiple OCR backends, enabling per-source backend selection.
pub struct OcrManager {
    backends: Vec<Box<dyn OcrBackend>>,
    primary: OcrBackendType,
}

impl OcrManager {
    /// Create a new OCR manager with the specified primary backend.
    pub fn new(primary: OcrBackendType) -> Self {
        Self {
            backends: Vec::new(),
            primary,
        }
    }

    /// Register a backend.
    pub fn register(&mut self, backend: Box<dyn OcrBackend>) {
        self.backends.push(backend);
    }

    /// Set the primary backend.
    pub fn set_primary(&mut self, backend_type: OcrBackendType) {
        self.primary = backend_type;
    }

    /// Get the primary backend.
    pub fn primary(&self) -> Option<&dyn OcrBackend> {
        self.backends
            .iter()
            .find(|b| b.backend_type() == self.primary)
            .map(|b| b.as_ref())
    }

    /// Get a specific backend by type.
    pub fn get(&self, backend_type: OcrBackendType) -> Option<&dyn OcrBackend> {
        self.backends
            .iter()
            .find(|b| b.backend_type() == backend_type)
            .map(|b| b.as_ref())
    }

    /// List all registered backends.
    pub fn backends(&self) -> impl Iterator<Item = &dyn OcrBackend> {
        self.backends.iter().map(|b| b.as_ref())
    }

    /// List available backends (those that can actually run).
    pub fn available_backends(&self) -> impl Iterator<Item = &dyn OcrBackend> {
        self.backends
            .iter()
            .filter(|b| b.is_available())
            .map(|b| b.as_ref())
    }

    /// Get the primary backend, validated and ready to use.
    fn get_ready_primary(&self) -> Result<&dyn OcrBackend, OcrError> {
        let backend = self.primary().ok_or_else(|| {
            OcrError::BackendNotAvailable(format!(
                "Primary backend {:?} not registered",
                self.primary
            ))
        })?;
        if !backend.is_available() {
            return Err(OcrError::BackendNotAvailable(backend.availability_hint()));
        }
        Ok(backend)
    }

    /// Get a specific backend, validated and ready to use.
    fn get_ready_backend(&self, backend_type: OcrBackendType) -> Result<&dyn OcrBackend, OcrError> {
        let backend = self.get(backend_type).ok_or_else(|| {
            OcrError::BackendNotAvailable(format!("Backend {:?} not registered", backend_type))
        })?;
        if !backend.is_available() {
            return Err(OcrError::BackendNotAvailable(backend.availability_hint()));
        }
        Ok(backend)
    }

    /// Run OCR using the primary backend.
    pub fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        self.get_ready_primary()?.ocr_image(image_path)
    }

    /// Run OCR on a PDF page using the primary backend.
    pub fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        self.get_ready_primary()?.ocr_pdf_page(pdf_path, page)
    }

    /// Run OCR using a specific backend.
    pub fn ocr_image_with(
        &self,
        image_path: &Path,
        backend_type: OcrBackendType,
    ) -> Result<OcrResult, OcrError> {
        self.get_ready_backend(backend_type)?.ocr_image(image_path)
    }

    /// Run OCR on a PDF page using a specific backend.
    pub fn ocr_pdf_page_with(
        &self,
        pdf_path: &Path,
        page: u32,
        backend_type: OcrBackendType,
    ) -> Result<OcrResult, OcrError> {
        self.get_ready_backend(backend_type)?
            .ocr_pdf_page(pdf_path, page)
    }
}
