//! Shared types and helper utilities for the document repository.

use chrono::{DateTime, Utc};
use rusqlite::Row;
use std::path::PathBuf;

use crate::models::{Document, DocumentStatus, DocumentVersion};
pub(crate) use crate::utils::mime_type_sql_condition as mime_type_condition;

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

/// Parse a version row into a DocumentVersion.
pub(crate) fn row_to_version(row: &Row) -> rusqlite::Result<DocumentVersion> {
    Ok(DocumentVersion {
        id: row.get("id")?,
        content_hash: row.get("content_hash")?,
        file_path: PathBuf::from(row.get::<_, String>("file_path")?),
        file_size: row.get::<_, i64>("file_size")? as u64,
        mime_type: row.get("mime_type")?,
        acquired_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("acquired_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        source_url: row.get("source_url")?,
        original_filename: row.get("original_filename")?,
        server_date: row
            .get::<_, Option<String>>("server_date")?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc)),
        page_count: row.get::<_, Option<i64>>("page_count")?.map(|c| c as u32),
    })
}

/// Parse a document row into a partial document (without versions).
/// Used by bulk load methods to avoid N+1 queries.
pub(crate) fn row_to_document_partial(row: &Row) -> rusqlite::Result<DocumentPartial> {
    let metadata_str: String = row.get("metadata")?;
    let tags: Vec<String> = row
        .get::<_, Option<String>>("tags")?
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    Ok(DocumentPartial {
        id: row.get("id")?,
        source_id: row.get("source_id")?,
        title: row.get("title")?,
        source_url: row.get("source_url")?,
        extracted_text: row.get("extracted_text")?,
        synopsis: row.get("synopsis")?,
        tags,
        status: DocumentStatus::from_str(&row.get::<_, String>("status")?)
            .unwrap_or(DocumentStatus::Pending),
        metadata: serde_json::from_str(&metadata_str)
            .unwrap_or(serde_json::Value::Object(Default::default())),
        created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        discovery_method: row.get("discovery_method")?,
    })
}

/// Convert a row to a Document with pre-loaded versions.
/// Used by bulk load methods to avoid N+1 queries.
pub(crate) fn row_to_document_with_versions(
    row: &Row,
    versions: Vec<DocumentVersion>,
) -> rusqlite::Result<Document> {
    let metadata_str: String = row.get("metadata")?;

    let tags: Vec<String> = row
        .get::<_, Option<String>>("tags")?
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    Ok(Document {
        id: row.get("id")?,
        source_id: row.get("source_id")?,
        title: row.get("title")?,
        source_url: row.get("source_url")?,
        versions,
        extracted_text: row.get("extracted_text")?,
        synopsis: row.get("synopsis")?,
        tags,
        status: DocumentStatus::from_str(&row.get::<_, String>("status")?)
            .unwrap_or(DocumentStatus::Pending),
        metadata: serde_json::from_str(&metadata_str).unwrap_or_default(),
        created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("created_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("updated_at")?)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        discovery_method: row.get("discovery_method")?,
    })
}

/// Extension trait to convert rusqlite errors for missing rows to Option.
pub(crate) trait OptionalExt<T> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for std::result::Result<T, rusqlite::Error> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
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
        let long_name = "a".repeat(150);
        let sanitized = sanitize_filename(&long_name);
        assert_eq!(sanitized.len(), 100);
    }

    #[test]
    fn test_sanitize_filename_only_special() {
        assert_eq!(sanitize_filename("///"), "document");
    }
}
