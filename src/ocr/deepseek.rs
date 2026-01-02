//! DeepSeek OCR backend implementation.
//!
//! Uses DeepSeek-OCR.rs via subprocess for LLM-based OCR.
//! This provides the highest accuracy for complex documents
//! but requires more resources (6-13GB RAM, GPU recommended).
//!
//! Install deepseek-ocr.rs from:
//! https://github.com/TimmyOVO/deepseek-ocr.rs
//!
//! ```bash
//! git clone https://github.com/TimmyOVO/deepseek-ocr.rs
//! cd deepseek-ocr.rs
//! cargo install --path crates/cli --features cuda  # or --features metal for Mac
//! ```

#![allow(dead_code)]

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;
use tempfile::TempDir;

use super::backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrResult};
use super::model_utils::check_binary;
use super::pdf_utils;

/// DeepSeek OCR backend using subprocess.
pub struct DeepSeekBackend {
    config: OcrConfig,
    /// Path to the deepseek-ocr binary.
    binary_path: PathBuf,
    /// Device to use (cpu, metal, cuda).
    device: String,
    /// Data type (f32, f16, bf16).
    dtype: String,
    /// Model to use (deepseek-ocr, paddleocr-vl, dots-ocr).
    model: String,
}

impl DeepSeekBackend {
    /// Create a new DeepSeek backend with default configuration.
    pub fn new() -> Self {
        Self {
            config: OcrConfig::default(),
            binary_path: PathBuf::from("deepseek-ocr-cli"),
            device: "cpu".to_string(),
            dtype: "f32".to_string(),
            model: "deepseek-ocr".to_string(),
        }
    }

    /// Create a new DeepSeek backend with custom configuration.
    pub fn with_config(config: OcrConfig) -> Self {
        let device = if config.use_gpu { "cuda" } else { "cpu" };
        let dtype = if config.use_gpu { "f16" } else { "f32" };

        Self {
            config,
            binary_path: PathBuf::from("deepseek-ocr-cli"),
            device: device.to_string(),
            dtype: dtype.to_string(),
            model: "deepseek-ocr".to_string(),
        }
    }

    /// Set the path to the deepseek-ocr binary.
    pub fn with_binary_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.binary_path = path.into();
        self
    }

    /// Set the device (cpu, metal, cuda).
    pub fn with_device(mut self, device: impl Into<String>) -> Self {
        self.device = device.into();
        self
    }

    /// Set the data type (f32, f16, bf16).
    pub fn with_dtype(mut self, dtype: impl Into<String>) -> Self {
        self.dtype = dtype.into();
        self
    }

    /// Set the model (deepseek-ocr, paddleocr-vl, dots-ocr).
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Check if the deepseek-ocr binary is available.
    fn is_binary_available(&self) -> bool {
        // First check if it's in PATH
        if check_binary(self.binary_path.to_str().unwrap_or("deepseek-ocr")) {
            return true;
        }

        // Check if it's a direct path that exists
        self.binary_path.exists()
    }

    /// Run DeepSeek OCR on an image.
    fn run_deepseek(&self, image_path: &Path) -> Result<String, OcrError> {
        // DeepSeek-OCR uses a prompt with <image> placeholder
        let prompt = "Extract all text from this image. Return only the extracted text, nothing else. <image>";

        let output = Command::new(&self.binary_path)
            .arg("--quiet") // Suppress logs, output only the result
            .args(["--prompt", prompt])
            .args(["--image", &image_path.to_string_lossy()])
            .args(["--device", &self.device])
            .args(["--dtype", &self.dtype])
            .args(["--model", &self.model])
            .args(["--max-new-tokens", "4096"])
            .output();

        match output {
            Ok(output) => {
                if output.status.success() {
                    Ok(String::from_utf8_lossy(&output.stdout).to_string())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    Err(OcrError::OcrFailed(format!("deepseek-ocr failed: {}", stderr)))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(OcrError::BackendNotAvailable(
                    "deepseek-ocr not found. Install from: https://github.com/TimmyOVO/deepseek-ocr.rs".to_string(),
                ))
            }
            Err(e) => Err(OcrError::Io(e)),
        }
    }
}

impl Default for DeepSeekBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OcrBackend for DeepSeekBackend {
    fn backend_type(&self) -> OcrBackendType {
        OcrBackendType::DeepSeek
    }

    fn is_available(&self) -> bool {
        self.is_binary_available()
    }

    fn availability_hint(&self) -> String {
        if !self.is_binary_available() {
            format!(
                "DeepSeek-OCR not found at '{}'. Install from: https://github.com/TimmyOVO/deepseek-ocr.rs\n\
                 git clone https://github.com/TimmyOVO/deepseek-ocr.rs && cd deepseek-ocr.rs\n\
                 cargo install --path crates/cli --features cuda  # or --features metal for Mac",
                self.binary_path.display()
            )
        } else if !check_binary("pdftoppm") {
            "pdftoppm not installed. Install with: apt install poppler-utils".to_string()
        } else {
            format!(
                "DeepSeek-OCR is available (device: {}, model: {})",
                self.device, self.model
            )
        }
    }

    fn ocr_image(&self, image_path: &Path) -> Result<OcrResult, OcrError> {
        let start = Instant::now();
        let text = self.run_deepseek(image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None, // DeepSeek doesn't provide confidence scores directly
            backend: OcrBackendType::DeepSeek,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }

    fn ocr_pdf_page(&self, pdf_path: &Path, page: u32) -> Result<OcrResult, OcrError> {
        let start = Instant::now();

        // Create temp directory for the image
        let temp_dir = TempDir::new()?;
        let image_path = pdf_utils::pdf_page_to_image(pdf_path, page, temp_dir.path())?;

        // Run OCR on the image
        let text = self.run_deepseek(&image_path)?;
        let elapsed = start.elapsed();

        Ok(OcrResult {
            text,
            confidence: None,
            backend: OcrBackendType::DeepSeek,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}
