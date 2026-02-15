//! Tesseract OCR backend implementation.
//!
//! Uses Tesseract OCR via command-line for text extraction.
//! This is the traditional, widely-available OCR option.

#![allow(dead_code)]

use std::path::Path;
use std::process::Command;

use super::backend::{BackendConfig, OcrBackend, OcrBackendType, OcrConfig, OcrError};
use super::model_utils::{check_binary, check_pdftoppm_hint};

/// Tesseract OCR backend.
pub struct TesseractBackend {
    config: BackendConfig,
}

impl TesseractBackend {
    /// Create a new Tesseract backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: BackendConfig::new(),
        }
    }

    /// Create a new Tesseract backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        Self {
            config: BackendConfig::with_config(config),
        }
    }

    /// Create a new Tesseract backend from a full backend configuration.
    pub fn from_backend_config(config: BackendConfig) -> Self {
        Self { config }
    }

    /// Run Tesseract on an image file.
    fn run_tesseract_impl(&self, image_path: &Path) -> Result<String, OcrError> {
        let output = Command::new("tesseract")
            .arg(image_path)
            .arg("stdout")
            .args(["-l", &self.config.ocr.language])
            .output();

        match output {
            Ok(output) => {
                if output.status.success() {
                    Ok(String::from_utf8_lossy(&output.stdout).to_string())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    Err(OcrError::OcrFailed(format!("tesseract failed: {}", stderr)))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(OcrError::BackendNotAvailable(
                    "tesseract not found (install tesseract-ocr)".to_string(),
                ))
            }
            Err(e) => Err(OcrError::Io(e)),
        }
    }
}

impl Default for TesseractBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for TesseractBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::Tesseract
    }

    fn is_available(&self) -> bool {
        check_binary("tesseract")
    }

    fn availability_hint(&self) -> String {
        if !check_binary("tesseract") {
            "Tesseract not installed. Install with: apt install tesseract-ocr".to_string()
        } else if let Some(hint) = check_pdftoppm_hint() {
            hint
        } else {
            "Tesseract is available".to_string()
        }
    }

    fn run_ocr(&self, image_path: &Path) -> Result<String, OcrError> {
        self.run_tesseract_impl(image_path)
    }
}
