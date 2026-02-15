//! Types shared across annotation backends.

use thiserror::Error;

/// Events emitted during annotation processing.
/// Used by the CLI to drive progress bars and status messages.
/// Fields are populated when events are created, even if consumers don't read all of them.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AnnotationEvent {
    Started {
        total_documents: usize,
    },
    DocumentStarted {
        document_id: String,
        title: String,
    },
    DocumentCompleted {
        document_id: String,
    },
    DocumentFailed {
        document_id: String,
        error: String,
    },
    DocumentSkipped {
        document_id: String,
    },
    Complete {
        succeeded: usize,
        failed: usize,
        skipped: usize,
        remaining: u64,
    },
}

/// Result of a single document annotation.
#[derive(Debug, Clone)]
pub enum AnnotationOutput {
    /// Annotation produced data to record.
    Data(String),
    /// No annotation could be produced (e.g., no date found).
    NoResult,
    /// Document was skipped (no text, no version, etc.).
    Skipped,
}

/// Result of a batch annotation run.
/// Part of public API — consumers may use any field.
#[derive(Debug)]
#[allow(dead_code)]
pub struct BatchAnnotationResult {
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    pub remaining: u64,
}

/// Errors from annotation backends.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AnnotationError {
    #[error("Backend not available: {0}")]
    BackendNotAvailable(String),

    #[error("Annotation failed: {0}")]
    Failed(String),

    #[error("Document has no text content")]
    NoText,

    #[error("Document has no version")]
    NoVersion,

    #[error("Database error: {0}")]
    Database(String),
}
