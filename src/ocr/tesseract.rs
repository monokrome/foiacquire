//! Tesseract OCR backend implementation.
//!
//! Uses Tesseract OCR via command-line for text extraction.
//! This is the traditional, widely-available OCR option.

#![allow(dead_code)]

use std::path::Path;
use std::process::Command;
use std::time::Instant;
use tempfile::TempDir;

use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrResult};
use super::model_utils::check_binary;

/// Tesseract OCR backend.
pub struct TesseractBackend {
    config: OcrConfig,
}

impl TesseractBackend {
    /// Create a new Tesseract backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
        }
    }

    /// Create a new Tesseract backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        Self { config }
    }


    /// Run Tesseract on an image file.
    fn run_tesseract(&self, image_path: &Path) -> Result<String, OcrError> {
        let output = Command::new("tesseract")
            .arg(image_path)
            .arg("stdout")
            .args(["-l", &self.config.language])
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

    /// Convert a PDF page to an image.
    fn pdf_page_to_image(
        &self,
        pdf_path: &Path,
        page: u32,
        output_dir: &Path,
    ) -> Result<std::path::PathBuf, OcrError> {
        let page_str = page.to_string();
        let output_prefix = output_dir.join("page");

        let status = Command::new("pdftoppm")
            .args(["-png", "-r", "300", "-f", &page_str, "-l", &page_str])
            .arg(pdf_path)
            .arg(&output_prefix)
            .status();

        match status {
            Ok(s) if s.success() => {
                // Find the generated image
                self.find_page_image(output_dir, page).ok_or_else(|| {
                    OcrError::OcrFailed(format!("No image generated for page {}", page))
                })
            }
            Ok(_) => Err(OcrError::OcrFailed(
                "pdftoppm failed to convert PDF page".to_string(),
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(OcrError::BackendNotAvailable(
                    "pdftoppm not found (install poppler-utils)".to_string(),
                ))
            }
            Err(e) => Err(OcrError::Io(e)),
        }
    }

    /// Find the image file for a specific page number.
    fn find_page_image(&self, temp_path: &Path, page_num: u32) -> Option<std::path::PathBuf> {
        // pdftoppm names files like page-01.png, page-02.png, etc.
        for digits in [2, 3, 4] {
            let filename = format!("page-{:0width$}.png", page_num, width = digits);
            let path = temp_path.join(&filename);
            if path.exists() {
                return Some(path);
            }
        }
        None
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
        } else if !check_binary("pdftoppm") {
            "pdftoppm not installed. Install with: apt install poppler-utils".to_string()
        } else {
            "Tesseract is available".to_string()
        }
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_tesseract(image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None, // Tesseract can provide this but we're not parsing it yet
            backend: OcrBackendType::Tesseract,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();

        // Create temp directory for the image
        let temp_dir = TempDir::new()?;
        let image_path = self.pdf_page_to_image(pdf_path, page, temp_dir.path())?;

        // Run OCR on the image
        let text = self.run_tesseract(&image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::Tesseract,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}
