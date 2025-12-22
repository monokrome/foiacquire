//! OCR and text extraction module.
//!
//! Extracts text from documents using:
//! - pdftotext (Poppler) for PDF text extraction
//! - Tesseract OCR for image-based PDFs and image files (default)
//! - OCRS for pure-Rust OCR (feature: ocr-ocrs)
//! - PaddleOCR for CNN-based OCR via ONNX (feature: ocr-paddle)
//! - DeepSeek OCR for LLM-based text extraction (GPU recommended)
//!
//! Also includes URL extraction from extracted text.
//! And archive handling for processing files within zip archives.
//! And email parsing for extracting attachments from RFC822 emails.
//!
//! ## OCR Backends
//!
//! Tesseract is the default and recommended OCR backend.
//! Additional backends are available via feature flags:
//!
//! - **Tesseract**: Traditional OCR, widely available, CPU-based (default)
//! - **OCRS**: Pure Rust OCR, no external binaries (feature: ocr-ocrs)
//! - **PaddleOCR**: CNN-based, fast, GPU support via ONNX (feature: ocr-paddle)
//! - **DeepSeek**: LLM-based OCR, highest accuracy, GPU recommended
//!
//! Use `OcrManager` to compare results across backends.

// Allow unused exports - these are public API for per-source backend selection
#![allow(unused_imports)]

mod archive;
mod backend;
mod deepseek;
mod email;
mod extractor;
mod model_utils;
mod tesseract;
mod url_finder;

#[cfg(feature = "ocr-ocrs")]
mod ocrs_backend;
#[cfg(feature = "ocr-paddle")]
mod paddle_backend;

pub use archive::ArchiveExtractor;
pub use email::EmailExtractor;
pub use extractor::TextExtractor;
pub use url_finder::UrlFinder;

// OCR backend abstraction for A/B testing and per-source backend selection
pub use backend::{OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrManager, OcrResult};
pub use deepseek::DeepSeekBackend;
pub use tesseract::TesseractBackend;

#[cfg(feature = "ocr-ocrs")]
pub use ocrs_backend::OcrsBackend;
#[cfg(feature = "ocr-paddle")]
pub use paddle_backend::PaddleBackend;
