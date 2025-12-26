//! OCR service types and events.

/// Events emitted during document analysis.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum OcrEvent {
    /// Phase 0: MIME detection started
    MimeCheckStarted { total_documents: usize },
    /// Document MIME type was corrected
    MimeFixed {
        document_id: String,
        old_mime: String,
        new_mime: String,
    },
    /// Phase 0: MIME detection complete
    MimeCheckComplete { checked: usize, fixed: usize },

    /// Phase 1: Text extraction started
    Phase1Started { total_documents: usize },
    /// Document text extraction started
    DocumentStarted { document_id: String, title: String },
    /// Document text extraction completed
    DocumentCompleted {
        document_id: String,
        pages_extracted: usize,
    },
    /// Document extraction failed
    DocumentFailed { document_id: String, error: String },
    /// Phase 1 complete
    Phase1Complete {
        succeeded: usize,
        failed: usize,
        pages_created: usize,
    },

    /// Phase 2: OCR started
    Phase2Started { total_pages: usize },
    /// Page OCR started
    PageOcrStarted {
        document_id: String,
        page_number: u32,
    },
    /// Page OCR completed
    PageOcrCompleted {
        document_id: String,
        page_number: u32,
        improved: bool,
    },
    /// Page OCR failed
    PageOcrFailed {
        document_id: String,
        page_number: u32,
        error: String,
    },
    /// Document finalized (all pages complete)
    DocumentFinalized { document_id: String },
    /// Phase 2 complete
    Phase2Complete {
        improved: usize,
        skipped: usize,
        failed: usize,
    },
}

/// Result of document analysis.
#[derive(Debug)]
#[allow(dead_code)]
pub struct OcrResult {
    pub mime_checked: usize,
    pub mime_fixed: usize,
    pub phase1_succeeded: usize,
    pub phase1_failed: usize,
    pub pages_created: usize,
    pub phase2_improved: usize,
    pub phase2_skipped: usize,
    pub phase2_failed: usize,
}

/// Result of OCR on a single page.
pub struct PageOcrResult {
    /// Whether the OCR text was better than the PDF text.
    pub improved: bool,
    /// Whether this page completion triggered document finalization.
    #[allow(dead_code)]
    pub document_finalized: bool,
}
