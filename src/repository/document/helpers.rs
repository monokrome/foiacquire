//! Shared types and helper utilities for the document repository.

use chrono::{DateTime, Utc};
use std::path::PathBuf;

use crate::models::{Document, DocumentStatus, DocumentVersion};

/// Partial document data loaded from a row, before versions are attached.
/// Used internally by bulk-load methods to avoid N+1 queries.
pub(crate) struct DocumentPartial {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub extracted_text: Option<String>,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub status: DocumentStatus,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub discovery_method: String,
}

impl DocumentPartial {
    pub fn with_versions(self, versions: Vec<DocumentVersion>) -> Document {
        Document {
            id: self.id,
            source_id: self.source_id,
            title: self.title,
            source_url: self.source_url,
            versions,
            extracted_text: self.extracted_text,
            synopsis: self.synopsis,
            tags: self.tags,
            status: self.status,
            metadata: self.metadata,
            created_at: self.created_at,
            updated_at: self.updated_at,
            discovery_method: self.discovery_method,
        }
    }
}

/// Lightweight document summary for listings (excludes extracted_text for memory efficiency).
#[derive(Debug, Clone)]
pub struct DocumentSummary {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_url: String,
    pub synopsis: Option<String>,
    pub tags: Vec<String>,
    pub status: DocumentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Current version info (if any)
    pub current_version: Option<VersionSummary>,
}

/// Lightweight version summary.
#[derive(Debug, Clone)]
pub struct VersionSummary {
    pub content_hash: String,
    pub file_path: PathBuf,
    pub file_size: u64,
    pub mime_type: String,
    pub acquired_at: DateTime<Utc>,
    pub original_filename: Option<String>,
    pub server_date: Option<DateTime<Utc>>,
}

/// Navigation context for a document within a filtered list.
/// Uses window functions to efficiently find prev/next documents.
#[derive(Debug, Clone)]
pub struct DocumentNavigation {
    pub prev_id: Option<String>,
    pub prev_title: Option<String>,
    pub next_id: Option<String>,
    pub next_title: Option<String>,
    pub position: u64,
    pub total: u64,
}

/// Result of cursor-based pagination browse query.
#[derive(Debug, Clone)]
pub struct BrowseResult {
    pub documents: Vec<Document>,
    /// ID of the first document on the previous page (for "Previous" link)
    pub prev_cursor: Option<String>,
    /// ID of the first document on the next page (for "Next" link)
    pub next_cursor: Option<String>,
    /// Position of first document on this page (1-indexed)
    pub start_position: u64,
    /// Total documents matching filters
    pub total: u64,
}

/// Extract filename parts (basename and extension) from URL, title, or mime type.
pub fn extract_filename_parts(url: &str, title: &str, mime_type: &str) -> (String, String) {
    // Try to get filename from URL path
    if let Some(filename) = url.split('/').next_back() {
        if let Some(dot_pos) = filename.rfind('.') {
            let basename = &filename[..dot_pos];
            let ext = &filename[dot_pos + 1..];
            // Only use if it looks like a real extension
            if !basename.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_alphanumeric()) {
                return (basename.to_string(), ext.to_lowercase());
            }
        }
    }

    // Fall back to title + mime type extension
    let ext = match mime_type {
        "application/pdf" => "pdf",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "text/html" => "html",
        "text/plain" => "txt",
        "image/jpeg" => "jpg",
        "image/png" => "png",
        _ => "bin",
    };

    let basename = if title.is_empty() { "document" } else { title };
    (basename.to_string(), ext.to_string())
}

/// Map MIME type to category.
pub fn mime_to_category(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "documents",
        m if m.contains("word") || m == "application/msword" => "documents",
        m if m.contains("rfc822") || m.contains("message") => "documents",
        m if m.starts_with("text/") && !m.contains("csv") => "documents",
        m if m.contains("excel")
            || m.contains("spreadsheet")
            || m == "text/csv"
            || m == "application/json"
            || m == "application/xml" =>
        {
            "data"
        }
        m if m.starts_with("image/") => "images",
        _ => "other",
    }
}

/// Sanitize a string for use as a filename.
pub fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    // Trim and limit length
    let trimmed = sanitized.trim().trim_matches('_');
    if trimmed.len() > 100 {
        trimmed[..100].to_string()
    } else if trimmed.is_empty() {
        "document".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_from_url() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/docs/report.pdf",
            "Some Title",
            "application/pdf",
        );
        assert_eq!(basename, "report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_fallback_to_mime() {
        let (basename, ext) = extract_filename_parts(
            "https://example.com/api/download?id=123",
            "Annual Report",
            "application/pdf",
        );
        assert_eq!(basename, "Annual Report");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_extract_filename_empty_title() {
        let (basename, ext) =
            extract_filename_parts("https://example.com/api/download", "", "application/pdf");
        assert_eq!(basename, "document");
        assert_eq!(ext, "pdf");
    }

    #[test]
    fn test_sanitize_filename_special_chars() {
        assert_eq!(
            sanitize_filename("file/with:bad*chars?"),
            "file_with_bad_chars"
        );
    }

    #[test]
    fn test_sanitize_filename_empty() {
        assert_eq!(sanitize_filename(""), "document");
    }

    #[test]
    fn test_sanitize_filename_long() {
        let long_name = "a".repeat(200);
        assert_eq!(sanitize_filename(&long_name).len(), 100);
    }
}
