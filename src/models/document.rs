//! Document models for FOIA document storage and versioning.
//!
//! Documents are stored with content-addressable versioning, allowing
//! detection of updates from source agencies over time.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// Processing status of a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentStatus {
    Pending,
    Downloaded,
    OcrComplete,
    Indexed,
    Failed,
}

impl DocumentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Downloaded => "downloaded",
            Self::OcrComplete => "ocr_complete",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "downloaded" => Some(Self::Downloaded),
            "ocr_complete" => Some(Self::OcrComplete),
            "indexed" => Some(Self::Indexed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A specific version of a document's content.
///
/// Content is identified by SHA-256 hash, enabling detection of
/// changes when documents are re-downloaded from sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentVersion {
    /// Database row ID.
    pub id: i64,
    /// SHA-256 hash of the document content.
    pub content_hash: String,
    /// Path to the stored file.
    pub file_path: PathBuf,
    /// Size in bytes.
    pub file_size: u64,
    /// MIME type of the content.
    pub mime_type: String,
    /// When this version was downloaded by us.
    pub acquired_at: DateTime<Utc>,
    /// URL from which this version was fetched.
    pub source_url: Option<String>,
    /// Original filename from Content-Disposition header or URL.
    pub original_filename: Option<String>,
    /// Server-reported date (Last-Modified header), if available.
    pub server_date: Option<DateTime<Utc>>,
    /// Cached page count for PDFs (avoids needing to re-read file).
    pub page_count: Option<u32>,
}

impl DocumentVersion {
    /// Compute SHA-256 hash of content.
    pub fn compute_hash(content: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content);
        hex::encode(hasher.finalize())
    }

    /// Create a new document version.
    pub fn new(
        content: &[u8],
        file_path: PathBuf,
        mime_type: String,
        source_url: Option<String>,
    ) -> Self {
        Self {
            id: 0, // Set by database
            content_hash: Self::compute_hash(content),
            file_path,
            file_size: content.len() as u64,
            mime_type,
            acquired_at: Utc::now(),
            source_url,
            original_filename: None,
            server_date: None,
            page_count: None,
        }
    }

    /// Create a new document version with original filename and server date.
    pub fn new_with_metadata(
        content: &[u8],
        file_path: PathBuf,
        mime_type: String,
        source_url: Option<String>,
        original_filename: Option<String>,
        server_date: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id: 0, // Set by database
            content_hash: Self::compute_hash(content),
            file_path,
            file_size: content.len() as u64,
            mime_type,
            acquired_at: Utc::now(),
            source_url,
            original_filename,
            server_date,
            page_count: None,
        }
    }
}

/// A FOIA document with version history.
///
/// Documents track their origin source, all known versions,
/// extracted text, and processing status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique identifier for this document.
    pub id: String,
    /// Reference to the originating Source.
    pub source_id: String,
    /// Document title or filename.
    pub title: String,
    /// Canonical URL for this document.
    pub source_url: String,
    /// List of content versions, newest first.
    pub versions: Vec<DocumentVersion>,
    /// OCR or extracted text content.
    pub extracted_text: Option<String>,
    /// LLM-generated synopsis of the document.
    pub synopsis: Option<String>,
    /// LLM-generated tags for categorization.
    pub tags: Vec<String>,
    /// Current processing status.
    pub status: DocumentStatus,
    /// Additional document information.
    pub metadata: serde_json::Value,
    /// When the document was first seen.
    pub created_at: DateTime<Utc>,
    /// When the document was last modified.
    pub updated_at: DateTime<Utc>,
    /// How this document was discovered (import, crawl, discover).
    pub discovery_method: String,
}

impl Document {
    /// Create a new document.
    pub fn new(
        id: String,
        source_id: String,
        title: String,
        source_url: String,
        version: DocumentVersion,
        metadata: serde_json::Value,
    ) -> Self {
        Self::with_discovery_method(id, source_id, title, source_url, version, metadata, "import".to_string())
    }

    /// Create a new document with explicit discovery method.
    pub fn with_discovery_method(
        id: String,
        source_id: String,
        title: String,
        source_url: String,
        version: DocumentVersion,
        metadata: serde_json::Value,
        discovery_method: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id,
            source_id,
            title,
            source_url,
            versions: vec![version],
            extracted_text: None,
            synopsis: None,
            tags: Vec::new(),
            status: DocumentStatus::Downloaded,
            metadata,
            created_at: now,
            updated_at: now,
            discovery_method,
        }
    }

    /// Get the most recent version of this document.
    pub fn current_version(&self) -> Option<&DocumentVersion> {
        self.versions.first()
    }

    /// Add a new version if content differs from current.
    ///
    /// Returns true if a new version was added, false if content unchanged.
    pub fn add_version(&mut self, version: DocumentVersion) -> bool {
        if let Some(current) = self.current_version() {
            if current.content_hash == version.content_hash {
                return false;
            }
        }

        self.versions.insert(0, version);
        self.updated_at = Utc::now();
        true
    }
}

/// Display-ready document data for web views.
///
/// This replaces complex tuples in template functions with a named struct.
#[derive(Debug, Clone)]
pub struct DocumentDisplay {
    pub id: String,
    pub title: String,
    pub source_id: String,
    pub mime_type: String,
    pub size: u64,
    pub acquired_at: DateTime<Utc>,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
}

impl DocumentDisplay {
    /// Convert a document to display format.
    pub fn from_document(doc: &Document) -> Option<Self> {
        let version = doc.current_version()?;
        Some(Self {
            id: doc.id.clone(),
            title: version
                .original_filename
                .clone()
                .unwrap_or_else(|| doc.title.clone()),
            source_id: doc.source_id.clone(),
            mime_type: version.mime_type.clone(),
            size: version.file_size,
            acquired_at: version.acquired_at,
            synopsis: doc.synopsis.clone(),
            tags: doc.tags.clone(),
        })
    }

    /// Convert to tuple for backwards compatibility with existing templates.
    #[allow(clippy::type_complexity)]
    pub fn to_tuple(
        &self,
    ) -> (
        String,
        String,
        String,
        String,
        u64,
        DateTime<Utc>,
        Option<String>,
        Vec<String>,
    ) {
        (
            self.id.clone(),
            self.title.clone(),
            self.source_id.clone(),
            self.mime_type.clone(),
            self.size,
            self.acquired_at,
            self.synopsis.clone(),
            self.tags.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_hash() {
        let content = b"Hello, World!";
        let hash = DocumentVersion::compute_hash(content);
        assert_eq!(hash.len(), 64); // SHA-256 produces 64 hex chars
    }

    #[test]
    fn test_add_version_different_content() {
        let version1 = DocumentVersion::new(
            b"content v1",
            PathBuf::from("/tmp/v1"),
            "application/pdf".to_string(),
            None,
        );

        let mut doc = Document::new(
            "doc1".to_string(),
            "source1".to_string(),
            "Test Doc".to_string(),
            "https://example.com/doc.pdf".to_string(),
            version1,
            serde_json::json!({}),
        );

        let version2 = DocumentVersion::new(
            b"content v2",
            PathBuf::from("/tmp/v2"),
            "application/pdf".to_string(),
            None,
        );

        assert!(doc.add_version(version2));
        assert_eq!(doc.versions.len(), 2);
    }

    #[test]
    fn test_add_version_same_content() {
        let content = b"same content";
        let version1 = DocumentVersion::new(
            content,
            PathBuf::from("/tmp/v1"),
            "application/pdf".to_string(),
            None,
        );

        let mut doc = Document::new(
            "doc1".to_string(),
            "source1".to_string(),
            "Test Doc".to_string(),
            "https://example.com/doc.pdf".to_string(),
            version1,
            serde_json::json!({}),
        );

        let version2 = DocumentVersion::new(
            content,
            PathBuf::from("/tmp/v2"),
            "application/pdf".to_string(),
            None,
        );

        assert!(!doc.add_version(version2));
        assert_eq!(doc.versions.len(), 1);
    }
}
