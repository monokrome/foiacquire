//! Virtual file model for files stored within archives.
//!
//! Virtual files represent files contained within archive formats (zip, tar, etc.)
//! that are not extracted to disk but can be accessed and processed on-demand.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A file contained within an archive that is not stored on disk.
///
/// Virtual files track their location within the parent archive and store
/// extracted text and summaries separately from the physical file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualFile {
    /// Unique identifier for this virtual file.
    pub id: String,
    /// ID of the parent document (the archive file).
    pub document_id: String,
    /// ID of the specific document version containing this file.
    pub version_id: i64,
    /// Path within the archive (e.g., "folder/document.pdf").
    pub archive_path: String,
    /// Filename within the archive.
    pub filename: String,
    /// MIME type of the file content.
    pub mime_type: String,
    /// Size in bytes (uncompressed).
    pub file_size: u64,
    /// OCR or extracted text content.
    pub extracted_text: Option<String>,
    /// LLM-generated synopsis.
    pub synopsis: Option<String>,
    /// LLM-generated tags.
    pub tags: Vec<String>,
    /// Processing status (pending, ocr_complete, failed).
    pub status: VirtualFileStatus,
    /// When this virtual file was discovered.
    pub created_at: DateTime<Utc>,
    /// When the virtual file was last processed.
    pub updated_at: DateTime<Utc>,
}

/// Processing status of a virtual file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualFileStatus {
    /// File discovered but not yet processed.
    Pending,
    /// Text extraction complete.
    OcrComplete,
    /// Text extraction failed.
    Failed,
    /// File type not supported for OCR.
    Unsupported,
}

impl VirtualFileStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::OcrComplete => "ocr_complete",
            Self::Failed => "failed",
            Self::Unsupported => "unsupported",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "ocr_complete" => Some(Self::OcrComplete),
            "failed" => Some(Self::Failed),
            "unsupported" => Some(Self::Unsupported),
            _ => None,
        }
    }
}

impl VirtualFile {
    /// Create a new virtual file entry.
    pub fn new(
        document_id: String,
        version_id: i64,
        archive_path: String,
        filename: String,
        mime_type: String,
        file_size: u64,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            document_id,
            version_id,
            archive_path,
            filename,
            mime_type,
            file_size,
            extracted_text: None,
            synopsis: None,
            tags: Vec::new(),
            status: VirtualFileStatus::Pending,
            created_at: now,
            updated_at: now,
        }
    }
}
