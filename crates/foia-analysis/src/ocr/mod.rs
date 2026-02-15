//! OCR and text extraction module.
//!
//! Extracts text from documents using:
//! - pdftotext (Poppler) for PDF text extraction
//! - Tesseract OCR for image-based PDFs and image files (default)
//! - OCRS for pure-Rust OCR (feature: ocr-ocrs)
//! - PaddleOCR for CNN-based OCR via ONNX (feature: ocr-paddle)
//! - DeepSeek OCR for LLM-based text extraction (GPU recommended)
//! - Gemini Vision for cloud-based LLM OCR (GEMINI_API_KEY)
//! - Groq Vision for fast cloud-based LLM OCR (GROQ_API_KEY)
//!
//! Also includes URL extraction from extracted text.
//! And archive handling for processing files within zip archives.
//! And email parsing for extracting attachments from RFC822 emails.
//!
//! ## OCR Backends
//!
//! Tesseract is the default and recommended OCR backend.
//! Additional backends are available via feature flags or API keys:
//!
//! - **Tesseract**: Traditional OCR, widely available, CPU-based (default)
//! - **OCRS**: Pure Rust OCR, no external binaries (feature: ocr-ocrs)
//! - **PaddleOCR**: CNN-based, fast, GPU support via ONNX (feature: ocr-paddle)
//! - **DeepSeek**: LLM-based OCR, highest accuracy, GPU recommended
//! - **Gemini**: Google's vision LLM, free tier 1,500 req/day (GEMINI_API_KEY)
//! - **Groq**: Fast inference, free tier 1,000 req/day (GROQ_API_KEY)
//!
//! Use `OcrManager` to compare results across backends.

// Allow unused exports - these are public API for per-source backend selection
#![allow(unused_imports)]

mod api_backend;
mod archive;
mod backend;
mod deepseek;
mod email;
mod extractor;
mod fallback;
mod gemini;
mod groq;
mod model_utils;
mod pdf_utils;
mod tesseract;

#[cfg(feature = "ocr-ocrs")]
mod ocrs_backend;
#[cfg(feature = "ocr-paddle")]
mod paddle_backend;

pub use archive::ArchiveExtractor;
pub use email::EmailExtractor;
pub use extractor::TextExtractor;
pub use foia::utils::UrlFinder;

// OCR backend abstraction for A/B testing and per-source backend selection
pub use backend::{
    BackendConfig, OcrBackend, OcrBackendType, OcrConfig, OcrError, OcrManager, OcrResult,
};
pub use deepseek::DeepSeekBackend;
pub use fallback::FallbackOcrBackend;
pub use gemini::GeminiBackend;
pub use groq::GroqBackend;
pub use tesseract::TesseractBackend;

#[cfg(feature = "ocr-ocrs")]
pub use ocrs_backend::OcrsBackend;
#[cfg(feature = "ocr-paddle")]
pub use paddle_backend::PaddleBackend;
