//! Document page models for per-page text extraction.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// OCR processing status for a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageOcrStatus {
    /// Page has not been processed yet.
    Pending,
    /// PDF text extraction complete, OCR not yet attempted.
    TextExtracted,
    /// OCR has been completed for this page.
    OcrComplete,
    /// Page was skipped (e.g., has sufficient text).
    Skipped,
    /// Processing failed for this page.
    Failed,
}

impl PageOcrStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::TextExtracted => "text_extracted",
            Self::OcrComplete => "ocr_complete",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "text_extracted" => Some(Self::TextExtracted),
            "ocr_complete" => Some(Self::OcrComplete),
            "skipped" => Some(Self::Skipped),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A single page of a document with its extracted text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentPage {
    /// Database row ID.
    pub id: i64,
    /// Parent document ID.
    pub document_id: String,
    /// Document version ID this page belongs to.
    pub version_id: i64,
    /// Page number (1-indexed).
    pub page_number: u32,
    /// Text extracted via pdftotext.
    pub pdf_text: Option<String>,
    /// Text extracted via OCR (Tesseract).
    pub ocr_text: Option<String>,
    /// Final merged/chosen text for this page.
    pub final_text: Option<String>,
    /// OCR processing status.
    pub ocr_status: PageOcrStatus,
    /// When this page record was created.
    pub created_at: DateTime<Utc>,
    /// When this page was last updated.
    pub updated_at: DateTime<Utc>,
}

impl DocumentPage {
    /// Create a new document page.
    pub fn new(document_id: String, version_id: i64, page_number: u32) -> Self {
        let now = Utc::now();
        Self {
            id: 0, // Set by database
            document_id,
            version_id,
            page_number,
            pdf_text: None,
            ocr_text: None,
            final_text: None,
            ocr_status: PageOcrStatus::Pending,
            created_at: now,
            updated_at: now,
        }
    }

    /// Check if this page needs OCR based on pdf_text content.
    pub fn needs_ocr(&self, min_chars: usize) -> bool {
        match &self.pdf_text {
            None => true,
            Some(text) => {
                let char_count = text.chars().filter(|c| !c.is_whitespace()).count();
                char_count < min_chars
            }
        }
    }

    /// Compute final text by choosing the best result.
    /// Prefers OCR over extracted PDF text (unless OCR is empty).
    pub fn compute_final_text(&mut self) {
        let ocr_chars = self
            .ocr_text
            .as_ref()
            .map(|t| t.chars().filter(|c| !c.is_whitespace()).count())
            .unwrap_or(0);

        // Prefer OCR over extracted text (unless OCR is empty)
        self.final_text = if ocr_chars > 0 {
            self.ocr_text.clone()
        } else {
            self.pdf_text.clone()
        };
    }
}
