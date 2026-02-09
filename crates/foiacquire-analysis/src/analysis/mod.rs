//! Document analysis module.
//!
//! Provides a unified interface for various document analysis backends:
//! - OCR: Text extraction from images and scanned PDFs
//! - Whisper: Audio/video transcription
//! - Custom: User-defined analysis commands
//!
//! # Architecture
//!
//! The module is built around the [`AnalysisBackend`] trait, which all backends implement.
//! Backends have a granularity - either page-level (OCR) or document-level (Whisper).
//!
//! The [`AnalysisManager`] handles backend registration and selection based on
//! mimetype and requested methods.
//!
//! # Example
//!
//! ```ignore
//! use foiacquire::analysis::{AnalysisManager, AnalysisBackend};
//!
//! let manager = AnalysisManager::with_defaults();
//! let backends = manager.get_backends_for(&["ocr".to_string()], "application/pdf");
//!
//! for backend in backends {
//!     if backend.is_available() {
//!         let result = backend.analyze_page(&path, 1)?;
//!         println!("Extracted: {}", result.text);
//!     }
//! }
//! ```

// Analysis system is implemented but not yet fully integrated
#![allow(dead_code)]

mod backend;
mod custom;
mod manager;
mod ocr_adapter;
mod whisper;

pub use manager::AnalysisManager;
