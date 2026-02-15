//! Document models for FOIA document storage and versioning.
//!
//! Documents are stored with content-addressable versioning, allowing
//! detection of updates from source agencies over time.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Dual content hashes for collision-resistant deduplication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentHashes {
    pub sha256: String,
    pub blake3: String,
}

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
/// Content is identified by dual hashes (SHA-256 + BLAKE3) for
/// collision-resistant deduplication across crawls.
///
/// File paths are deterministic and computed at runtime from the content hash,
/// original filename, and dedup_index. Legacy records may have a stored
/// `file_path`; new records store `None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentVersion {
    /// Database row ID.
    pub id: i64,
    /// SHA-256 hash of the document content.
    pub content_hash: String,
    /// BLAKE3 hash of the document content (for deduplication verification).
    pub content_hash_blake3: Option<String>,
    /// Legacy stored file path. New records store None (path is deterministic).
    pub file_path: Option<PathBuf>,
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
    /// ID of the archive snapshot this version was fetched from (if from archive).
    pub archive_snapshot_id: Option<i32>,
    /// Earliest known archive date for this content (provenance verification).
    pub earliest_archived_at: Option<DateTime<Utc>>,
    /// Collision index for deterministic path computation. None means depth=2.
    pub dedup_index: Option<u32>,
}

impl DocumentVersion {
    /// Compute SHA-256 hash of content.
    pub fn compute_hash(content: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content);
        hex::encode(hasher.finalize())
    }

    /// Compute BLAKE3 hash of content.
    pub fn compute_hash_blake3(content: &[u8]) -> String {
        hex::encode(blake3::hash(content).as_bytes())
    }

    /// Compute both SHA-256 and BLAKE3 hashes for deduplication.
    pub fn compute_dual_hashes(content: &[u8]) -> ContentHashes {
        ContentHashes {
            sha256: Self::compute_hash(content),
            blake3: Self::compute_hash_blake3(content),
        }
    }

    /// Create a new document version (file_path is None for deterministic paths).
    pub fn new(content: &[u8], mime_type: String, source_url: Option<String>) -> Self {
        Self::new_with_metadata(content, mime_type, source_url, None, None)
    }

    /// Create a new document version with original filename and server date.
    pub fn new_with_metadata(
        content: &[u8],
        mime_type: String,
        source_url: Option<String>,
        original_filename: Option<String>,
        server_date: Option<DateTime<Utc>>,
    ) -> Self {
        let hashes = Self::compute_dual_hashes(content);
        Self {
            id: 0, // Set by database
            content_hash: hashes.sha256,
            content_hash_blake3: Some(hashes.blake3),
            file_path: None,
            file_size: content.len() as u64,
            mime_type,
            acquired_at: Utc::now(),
            source_url,
            original_filename,
            server_date,
            page_count: None,
            archive_snapshot_id: None,
            earliest_archived_at: None,
            dedup_index: None,
        }
    }

    /// Create a new document version with pre-computed hashes.
    pub fn with_precomputed_hashes(
        hashes: ContentHashes,
        file_size: u64,
        mime_type: String,
        source_url: Option<String>,
        original_filename: Option<String>,
        server_date: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id: 0, // Set by database
            content_hash: hashes.sha256,
            content_hash_blake3: Some(hashes.blake3),
            file_path: None,
            file_size,
            mime_type,
            acquired_at: Utc::now(),
            source_url,
            original_filename,
            server_date,
            page_count: None,
            archive_snapshot_id: None,
            earliest_archived_at: None,
            dedup_index: None,
        }
    }

    /// Resolve the absolute file path for this version.
    ///
    /// For legacy records with stored absolute paths, extracts the last 2
    /// components and joins with `documents_dir`. For records with relative
    /// paths, joins with `documents_dir`. For records with no stored path,
    /// computes the deterministic path.
    pub fn resolve_path(&self, documents_dir: &Path, url: &str, title: &str) -> PathBuf {
        match &self.file_path {
            Some(stored) if stored.is_absolute() => {
                // Legacy absolute path: extract last 2 components (e.g. "ab/report-abcdef12.pdf")
                let components: Vec<_> = stored.components().rev().take(2).collect();
                if components.len() == 2 {
                    let dir_name = components[1].as_os_str();
                    let file_name = components[0].as_os_str();
                    documents_dir.join(dir_name).join(file_name)
                } else {
                    // Fallback: just use the filename
                    let file_name = stored.file_name().unwrap_or_default();
                    documents_dir.join(file_name)
                }
            }
            Some(stored) => {
                // Relative path: join with documents_dir
                documents_dir.join(stored)
            }
            None => {
                // No stored path: compute deterministic path
                let relative = self.compute_storage_path(url, title);
                documents_dir.join(relative)
            }
        }
    }

    /// Compute the deterministic relative storage path.
    ///
    /// Format: `{hash[0..depth]}/{sanitized_basename}-{hash[0..8]}.{ext}`
    /// where depth = 2 + dedup_index.unwrap_or(0)
    pub fn compute_storage_path(&self, url: &str, title: &str) -> PathBuf {
        use crate::repository::{extract_filename_parts, sanitize_filename};
        use crate::storage::mime_to_extension;

        let (basename, extension) = if let Some(ref orig) = self.original_filename {
            // Use original_filename for basename + extension
            if let Some(dot_pos) = orig.rfind('.') {
                let base = &orig[..dot_pos];
                let ext = &orig[dot_pos + 1..];
                if !base.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_alphanumeric()) {
                    (base.to_string(), ext.to_lowercase())
                } else {
                    extract_filename_parts(url, title, &self.mime_type)
                }
            } else {
                (orig.clone(), mime_to_extension(&self.mime_type).to_string())
            }
        } else {
            extract_filename_parts(url, title, &self.mime_type)
        };

        let sanitized = sanitize_filename(&basename);
        let depth = 2 + self.dedup_index.unwrap_or(0) as usize;
        let prefix = &self.content_hash[..depth.min(self.content_hash.len())];
        let filename = format!("{}-{}.{}", sanitized, &self.content_hash[..8], extension);

        PathBuf::from(prefix).join(filename)
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
        Self::with_discovery_method(
            id,
            source_id,
            title,
            source_url,
            version,
            metadata,
            "import".to_string(),
        )
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
    /// Uses both SHA-256 and BLAKE3 hashes for collision-resistant comparison.
    pub fn add_version(&mut self, version: DocumentVersion) -> bool {
        if let Some(current) = self.current_version() {
            // Check both hashes match (if blake3 available on both)
            let sha_match = current.content_hash == version.content_hash;
            let blake_match = match (&current.content_hash_blake3, &version.content_hash_blake3) {
                (Some(a), Some(b)) => a == b,
                _ => true, // If either missing, rely on SHA-256 alone
            };
            if sha_match && blake_match {
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
        let version1 = DocumentVersion::new(b"content v1", "application/pdf".to_string(), None);

        let mut doc = Document::new(
            "doc1".to_string(),
            "source1".to_string(),
            "Test Doc".to_string(),
            "https://example.com/doc.pdf".to_string(),
            version1,
            serde_json::json!({}),
        );

        let version2 = DocumentVersion::new(b"content v2", "application/pdf".to_string(), None);

        assert!(doc.add_version(version2));
        assert_eq!(doc.versions.len(), 2);
    }

    #[test]
    fn test_add_version_same_content() {
        let content = b"same content";
        let version1 = DocumentVersion::new(content, "application/pdf".to_string(), None);

        let mut doc = Document::new(
            "doc1".to_string(),
            "source1".to_string(),
            "Test Doc".to_string(),
            "https://example.com/doc.pdf".to_string(),
            version1,
            serde_json::json!({}),
        );

        let version2 = DocumentVersion::new(content, "application/pdf".to_string(), None);

        assert!(!doc.add_version(version2));
        assert_eq!(doc.versions.len(), 1);
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_resolve_path_legacy_absolute() {
        let mut version =
            DocumentVersion::new(b"test content", "application/pdf".to_string(), None);
        version.file_path = Some(PathBuf::from("/opt/foia/documents/ab/report-abcdef12.pdf"));

        let resolved = version.resolve_path(
            Path::new("/mnt/documents"),
            "https://example.com/report.pdf",
            "report",
        );
        assert_eq!(
            resolved,
            PathBuf::from("/mnt/documents/ab/report-abcdef12.pdf")
        );
    }

    #[test]
    fn test_resolve_path_relative() {
        let mut version =
            DocumentVersion::new(b"test content", "application/pdf".to_string(), None);
        version.file_path = Some(PathBuf::from("ab/report-abcdef12.pdf"));

        let resolved = version.resolve_path(
            Path::new("/mnt/documents"),
            "https://example.com/report.pdf",
            "report",
        );
        assert_eq!(
            resolved,
            PathBuf::from("/mnt/documents/ab/report-abcdef12.pdf")
        );
    }

    #[test]
    fn test_resolve_path_none_computes_deterministic() {
        let version = DocumentVersion::new(
            b"test content",
            "application/pdf".to_string(),
            Some("https://example.com/report.pdf".to_string()),
        );
        assert!(version.file_path.is_none());

        let resolved = version.resolve_path(
            Path::new("/mnt/documents"),
            "https://example.com/report.pdf",
            "Report Title",
        );
        // Should be deterministic based on hash
        assert!(resolved.starts_with("/mnt/documents"));
        assert!(resolved.to_string_lossy().ends_with(".pdf"));
    }
}
