//! Text extraction from documents using pdftotext and Tesseract.

#![allow(dead_code)]

use std::path::Path;
use std::process::Command;
use tempfile::TempDir;
use thiserror::Error;

use super::model_utils::check_binary;

/// Handle command output, extracting stdout on success or returning appropriate error.
fn handle_cmd_output(
    result: std::io::Result<std::process::Output>,
    tool_name: &str,
    error_prefix: &str,
) -> Result<String, ExtractionError> {
    match result {
        Ok(output) => {
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                Err(ExtractionError::ExtractionFailed(format!(
                    "{}: {}",
                    error_prefix, stderr
                )))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(ExtractionError::ToolNotFound(tool_name.to_string()))
        }
        Err(e) => Err(ExtractionError::Io(e)),
    }
}

/// Check command status, returning appropriate error on failure.
fn check_cmd_status(
    result: std::io::Result<std::process::ExitStatus>,
    tool_name: &str,
    error_msg: &str,
) -> Result<(), ExtractionError> {
    match result {
        Ok(s) if s.success() => Ok(()),
        Ok(_) => Err(ExtractionError::ExtractionFailed(error_msg.to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(ExtractionError::ToolNotFound(tool_name.to_string()))
        }
        Err(e) => Err(ExtractionError::Io(e)),
    }
}

/// Errors that can occur during text extraction.
#[derive(Debug, Error)]
pub enum ExtractionError {
    #[error("Unsupported file type: {0}")]
    UnsupportedFileType(String),

    #[error("External tool not found: {0}")]
    ToolNotFound(String),

    #[error("Extraction failed: {0}")]
    ExtractionFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result of text extraction.
#[derive(Debug)]
pub struct ExtractionResult {
    /// Extracted text content.
    pub text: String,
    /// Method used for extraction.
    pub method: ExtractionMethod,
    /// Number of pages processed (for PDFs).
    pub page_count: Option<u32>,
}

/// Method used to extract text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionMethod {
    /// Direct text extraction from PDF.
    PdfToText,
    /// OCR using Tesseract.
    TesseractOcr,
    /// Combined: pdftotext with OCR fallback for sparse pages.
    Hybrid,
}

/// Text extractor that uses external tools.
pub struct TextExtractor {
    /// Minimum characters per page to consider text extraction successful.
    min_chars_per_page: usize,
    /// Tesseract language setting.
    tesseract_lang: String,
}

impl Default for TextExtractor {
    fn default() -> Self {
        Self {
            min_chars_per_page: 100,
            tesseract_lang: "eng".to_string(),
        }
    }
}

impl TextExtractor {
    /// Create a new text extractor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum characters per page threshold.
    pub fn with_min_chars(mut self, min_chars: usize) -> Self {
        self.min_chars_per_page = min_chars;
        self
    }

    /// Set Tesseract language.
    pub fn with_language(mut self, lang: &str) -> Self {
        self.tesseract_lang = lang.to_string();
        self
    }

    /// Extract text from a file based on its MIME type.
    pub fn extract(
        &self,
        file_path: &Path,
        mime_type: &str,
    ) -> Result<ExtractionResult, ExtractionError> {
        match mime_type {
            "application/pdf" => self.extract_pdf(file_path),
            "image/png" | "image/jpeg" | "image/tiff" | "image/gif" | "image/bmp" => {
                self.extract_image(file_path)
            }
            "text/plain" | "text/html" => {
                // Read directly
                let text = std::fs::read_to_string(file_path)?;
                Ok(ExtractionResult {
                    text,
                    method: ExtractionMethod::PdfToText, // Not really, but direct read
                    page_count: None,
                })
            }
            _ => Err(ExtractionError::UnsupportedFileType(mime_type.to_string())),
        }
    }

    /// Extract text from a PDF file using per-page analysis.
    /// Both pdftotext and OCR are run on each page, keeping whichever has more content.
    fn extract_pdf(&self, file_path: &Path) -> Result<ExtractionResult, ExtractionError> {
        let page_count = self.get_pdf_page_count(file_path).unwrap_or(1);

        // For single-page PDFs or if we can't get page count, use simple approach
        if page_count <= 1 {
            return self.extract_pdf_simple(file_path, page_count);
        }

        // Convert entire PDF to images for OCR
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        let pdftoppm_status = Command::new("pdftoppm")
            .args(["-png", "-r", "300"])
            .arg(file_path)
            .arg(temp_path.join("page"))
            .status();

        let ocr_available = match pdftoppm_status {
            Ok(s) if s.success() => true,
            _ => {
                tracing::debug!("pdftoppm failed, falling back to pdftotext only");
                false
            }
        };

        // Process each page
        let mut page_texts: Vec<String> = Vec::with_capacity(page_count as usize);
        let mut used_ocr = false;

        for page_num in 1..=page_count {
            // Get pdftotext result for this page
            let pdf_text = self
                .extract_pdf_page_text(file_path, page_num)
                .unwrap_or_default();
            let pdf_chars: usize = pdf_text.chars().filter(|c| !c.is_whitespace()).count();

            // Try OCR for this page
            let mut final_text = pdf_text.clone();

            if ocr_available {
                // Find the image file for this page (pdftoppm names them page-01.png, page-02.png, etc.)
                let image_path = self.find_page_image(temp_path, page_num);

                if let Some(img_path) = image_path {
                    if let Ok(ocr_text) = self.run_tesseract(&img_path) {
                        let ocr_chars: usize =
                            ocr_text.chars().filter(|c| !c.is_whitespace()).count();

                        // Use OCR if it has significantly more content (>20% more chars)
                        if ocr_chars > pdf_chars + (pdf_chars / 5) {
                            final_text = ocr_text;
                            used_ocr = true;
                        }
                    }
                }
            }

            page_texts.push(final_text);
        }

        let combined_text = page_texts.join("\n\n");
        let method = if used_ocr {
            ExtractionMethod::Hybrid
        } else {
            ExtractionMethod::PdfToText
        };

        Ok(ExtractionResult {
            text: combined_text,
            method,
            page_count: Some(page_count),
        })
    }

    /// Find the image file for a specific page number.
    fn find_page_image(&self, temp_path: &Path, page_num: u32) -> Option<std::path::PathBuf> {
        // pdftoppm names files like page-01.png, page-02.png, etc.
        // For documents with many pages, it may use more digits: page-001.png
        for digits in [2, 3, 4] {
            let filename = format!("page-{:0width$}.png", page_num, width = digits);
            let path = temp_path.join(&filename);
            if path.exists() {
                return Some(path);
            }
        }
        None
    }

    /// Simple PDF extraction for single-page PDFs or fallback.
    fn extract_pdf_simple(
        &self,
        file_path: &Path,
        page_count: u32,
    ) -> Result<ExtractionResult, ExtractionError> {
        let pdftotext_result = self.run_pdftotext(file_path)?;
        let pdf_chars: usize = pdftotext_result
            .chars()
            .filter(|c| !c.is_whitespace())
            .count();

        // Always try OCR and compare results
        match self.ocr_pdf(file_path) {
            Ok(ocr_text) => {
                let ocr_chars: usize = ocr_text.chars().filter(|c| !c.is_whitespace()).count();

                // Use OCR if it has significantly more content (>20% more chars)
                if ocr_chars > pdf_chars + (pdf_chars / 5) {
                    Ok(ExtractionResult {
                        text: ocr_text,
                        method: ExtractionMethod::TesseractOcr,
                        page_count: Some(page_count),
                    })
                } else {
                    Ok(ExtractionResult {
                        text: pdftotext_result,
                        method: ExtractionMethod::PdfToText,
                        page_count: Some(page_count),
                    })
                }
            }
            Err(e) => {
                tracing::debug!("OCR failed: {}, using pdftotext result", e);
                Ok(ExtractionResult {
                    text: pdftotext_result,
                    method: ExtractionMethod::PdfToText,
                    page_count: Some(page_count),
                })
            }
        }
    }

    /// Run pdftotext on a PDF file.
    fn run_pdftotext(&self, file_path: &Path) -> Result<String, ExtractionError> {
        let output = Command::new("pdftotext")
            .args(["-layout", "-enc", "UTF-8"])
            .arg(file_path)
            .arg("-") // Output to stdout
            .output();

        handle_cmd_output(output, "pdftotext (install poppler-utils)", "pdftotext failed")
    }

    /// Run pdftotext on a single page of a PDF file.
    pub fn extract_pdf_page_text(
        &self,
        file_path: &Path,
        page: u32,
    ) -> Result<String, ExtractionError> {
        let page_str = page.to_string();
        let output = Command::new("pdftotext")
            .args(["-layout", "-enc", "UTF-8", "-f", &page_str, "-l", &page_str])
            .arg(file_path)
            .arg("-") // Output to stdout
            .output();

        handle_cmd_output(
            output,
            "pdftotext (install poppler-utils)",
            &format!("pdftotext failed on page {}", page),
        )
    }

    /// Get the page count of a PDF.
    pub fn get_pdf_page_count(&self, file_path: &Path) -> Option<u32> {
        let output = Command::new("pdfinfo").arg(file_path).output().ok()?;

        if !output.status.success() {
            return None;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if line.starts_with("Pages:") {
                return line.split_whitespace().nth(1).and_then(|s| s.parse().ok());
            }
        }
        None
    }

    /// OCR a PDF by converting pages to images and running Tesseract.
    fn ocr_pdf(&self, file_path: &Path) -> Result<String, ExtractionError> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();

        // Convert PDF to images using pdftoppm
        let status = Command::new("pdftoppm")
            .args(["-png", "-r", "300"]) // 300 DPI
            .arg(file_path)
            .arg(temp_path.join("page"))
            .status();

        check_cmd_status(
            status,
            "pdftoppm (install poppler-utils)",
            "pdftoppm failed to convert PDF",
        )?;

        // Find all generated images
        let mut images: Vec<_> = std::fs::read_dir(temp_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "png")
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();

        images.sort();

        if images.is_empty() {
            return Err(ExtractionError::ExtractionFailed(
                "No images generated from PDF".to_string(),
            ));
        }

        // OCR each image
        let mut all_text = String::new();
        for (i, image_path) in images.iter().enumerate() {
            match self.run_tesseract(image_path) {
                Ok(text) => {
                    if !all_text.is_empty() {
                        all_text.push_str("\n\n--- Page ");
                        all_text.push_str(&(i + 1).to_string());
                        all_text.push_str(" ---\n\n");
                    }
                    all_text.push_str(&text);
                }
                Err(e) => {
                    tracing::warn!("OCR failed for page {}: {}", i + 1, e);
                }
            }
        }

        Ok(all_text)
    }

    /// Extract text from an image file using Tesseract.
    fn extract_image(&self, file_path: &Path) -> Result<ExtractionResult, ExtractionError> {
        let text = self.run_tesseract(file_path)?;
        Ok(ExtractionResult {
            text,
            method: ExtractionMethod::TesseractOcr,
            page_count: Some(1),
        })
    }

    /// Run Tesseract OCR on an image.
    fn run_tesseract(&self, image_path: &Path) -> Result<String, ExtractionError> {
        let output = Command::new("tesseract")
            .arg(image_path)
            .arg("stdout")
            .args(["-l", &self.tesseract_lang])
            .output();

        handle_cmd_output(output, "tesseract (install tesseract-ocr)", "tesseract failed")
    }

    /// OCR a single page of a PDF file.
    /// Converts the specified page to an image and runs Tesseract on it.
    pub fn ocr_pdf_page(&self, file_path: &Path, page: u32) -> Result<String, ExtractionError> {
        let temp_dir = TempDir::new()?;
        let temp_path = temp_dir.path();
        let output_prefix = temp_path.join("page");

        // Convert just this page to an image using pdftoppm
        let page_str = page.to_string();
        let status = Command::new("pdftoppm")
            .args(["-png", "-r", "300", "-f", &page_str, "-l", &page_str])
            .arg(file_path)
            .arg(&output_prefix)
            .status();

        check_cmd_status(
            status,
            "pdftoppm (install poppler-utils)",
            &format!("pdftoppm failed to convert page {}", page),
        )?;

        // Find the generated image
        if let Some(image_path) = self.find_page_image(temp_path, page) {
            self.run_tesseract(&image_path)
        } else {
            Err(ExtractionError::ExtractionFailed(format!(
                "No image generated for page {}",
                page
            )))
        }
    }

    /// OCR an image file directly.
    pub fn ocr_image(&self, file_path: &Path) -> Result<String, ExtractionError> {
        self.run_tesseract(file_path)
    }

    /// Check if required tools are available.
    pub fn check_tools() -> Vec<(String, bool)> {
        ["pdftotext", "pdftoppm", "pdfinfo", "tesseract"]
            .iter()
            .map(|tool| (tool.to_string(), check_binary(tool)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_tools() {
        let tools = TextExtractor::check_tools();
        assert!(!tools.is_empty());
        for (tool, available) in tools {
            println!("{}: {}", tool, if available { "found" } else { "missing" });
        }
    }
}
